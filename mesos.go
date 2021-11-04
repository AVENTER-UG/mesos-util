package mesosutil

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	mesosproto "github.com/AVENTER-UG/mesos-util/proto"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/sirupsen/logrus"
)

// Service include all the current vars and global config
var config *FrameworkConfig

// Marshaler to serialize Protobuf Message to JSON
var marshaller = jsonpb.Marshaler{
	EnumsAsInts: false,
	Indent:      " ",
	OrigName:    true,
}

// SetConfig set the global config
func SetConfig(cfg *FrameworkConfig) {
	config = cfg
}

// Subscribe to the mesos backend
func Subscribe(
	handleoffers HandleOffers,
	restartfailedcontainer RestartFailedContainer,
	heartbeat Heartbeat) error {

	subscribeCall := &mesosproto.Call{
		FrameworkID: config.FrameworkInfo.ID,
		Type:        mesosproto.Call_SUBSCRIBE,
		Subscribe: &mesosproto.Call_Subscribe{
			FrameworkInfo: &config.FrameworkInfo,
		},
	}
	logrus.Debug(subscribeCall)
	body, _ := marshaller.MarshalToString(subscribeCall)
	logrus.Debug(body)
	client := &http.Client{}
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	protocol := "https"
	if !config.MesosSSL {
		protocol = "http"
	}
	req, _ := http.NewRequest("POST", protocol+"://"+config.MesosMasterServer+"/api/v1/scheduler", bytes.NewBuffer([]byte(body)))
	req.Close = true
	req.SetBasicAuth(config.Username, config.Password)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)

	if err != nil {
		logrus.Fatal(err)
	}

	reader := bufio.NewReader(res.Body)

	line, _ := reader.ReadString('\n')
	bytesCount, _ := strconv.Atoi(strings.Trim(line, "\n"))

	/*if config.MesosStreamID != "" {
		SearchMissingEtcd(true)
		SearchMissingK3SServer(true)
		SearchMissingK3SAgent(true)
	}*/

	for {
		// Read line from Mesos
		line, _ = reader.ReadString('\n')
		line = strings.Trim(line, "\n")
		// Read important data
		data := line[:bytesCount]
		// Rest data will be bytes of next message
		bytesCount, _ = strconv.Atoi((line[bytesCount:]))
		var event mesosproto.Event // Event as ProtoBuf
		err := jsonpb.UnmarshalString(data, &event)
		if err != nil {
			logrus.Error(err)
		}
		logrus.Debug("Subscribe Got: ", event.GetType())

		/*
			initStartEtcd()
				initStartK3SServer()
				initStartK3SAgent()
		*/

		switch event.Type {
		case mesosproto.Event_SUBSCRIBED:
			logrus.Debug(event)
			logrus.Info("Subscribed")
			logrus.Info("FrameworkId: ", event.Subscribed.GetFrameworkID())
			config.FrameworkInfo.ID = event.Subscribed.GetFrameworkID()
			config.MesosStreamID = res.Header.Get("Mesos-Stream-Id")
			// Save framework info
			persConf, _ := json.Marshal(&config)
			err = ioutil.WriteFile(config.FrameworkInfoFile, persConf, 0644)
			if err != nil {
				logrus.Error("Write FrameWork State File: ", err)
			}
		case mesosproto.Event_UPDATE:
			logrus.Debug("Update", HandleUpdate(&event))
		case mesosproto.Event_HEARTBEAT:
			heartbeat()
		case mesosproto.Event_OFFERS:
			// Search Failed containers and restart them
			restartfailedcontainer()
			logrus.Debug("Offer Got")
			err = handleoffers(event.Offers)
			if err != nil {
				logrus.Error("Switch Event HandleOffers: ", err)
			}
		default:
			logrus.Debug("DEFAULT EVENT: ", event.Offers)
		}
	}
}

// Call will send messages to mesos
func Call(message *mesosproto.Call) error {
	message.FrameworkID = config.FrameworkInfo.ID
	body, _ := marshaller.MarshalToString(message)

	client := &http.Client{}
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	protocol := "https"
	if !config.MesosSSL {
		protocol = "http"
	}
	req, _ := http.NewRequest("POST", protocol+"://"+config.MesosMasterServer+"/api/v1/scheduler", bytes.NewBuffer([]byte(body)))
	req.Close = true
	req.SetBasicAuth(config.Username, config.Password)
	req.Header.Set("Mesos-Stream-Id", config.MesosStreamID)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)

	if err != nil {
		logrus.Error("Call Message: ", err)
		return err
	}

	defer res.Body.Close()

	if res.StatusCode != 202 {
		_, err := io.Copy(os.Stderr, res.Body)
		if err != nil {
			logrus.Error("Call Handling: ", err)
		}
		return fmt.Errorf("Error %d", res.StatusCode)
	}

	return nil
}

// Reconcile will reconcile the task states after the framework was restarted
func Reconcile() {
	var oldTasks []mesosproto.Call_Reconcile_Task
	logrus.Debug("Reconcile Tasks")
	maxID := 0
	if config != nil {
		for _, t := range config.State {
			if t.Status != nil {
				oldTasks = append(oldTasks, mesosproto.Call_Reconcile_Task{
					TaskID:  t.Status.TaskID,
					AgentID: t.Status.AgentID,
				})
				numericID, err := strconv.Atoi(t.Status.TaskID.GetValue())
				if err == nil && numericID > maxID {
					maxID = numericID
				}
			}
		}
		atomic.StoreUint64(&config.TaskID, uint64(maxID))
		err := Call(&mesosproto.Call{
			Type:      mesosproto.Call_RECONCILE,
			Reconcile: &mesosproto.Call_Reconcile{Tasks: oldTasks},
		})

		if err != nil {
			logrus.Error("Call Reconcile: ", err)
		}
	}
}

// Revive will revive the mesos tasks to clean up
func Revive() {
	logrus.Debug("Revive Tasks")
	revive := &mesosproto.Call{
		Type: mesosproto.Call_REVIVE,
	}
	err := Call(revive)
	if err != nil {
		logrus.Error("Call Revive: ", err)
	}
}

// if all Tasks are running, suppress framework offers
func SuppressFramework() {
	logrus.Info("Framework Suppress")
	suppress := &mesosproto.Call{
		Type: mesosproto.Call_SUPPRESS,
	}
	err := Call(suppress)
	if err != nil {
		logrus.Error("Supress Framework Call: ")
	}
}

// Delete Failed Tasks from the config
func DeleteOldTask(taskID mesosproto.TaskID) {
	copy := make(map[string]State)

	if config.State != nil {
		for _, element := range config.State {
			if element.Status != nil {
				tmpID := element.Status.GetTaskID().Value
				if element.Status.TaskID != taskID {
					copy[tmpID] = element
				} else {
					logrus.Debug("Delete Task from config: ", tmpID)
				}
			}
		}

		config.State = copy
	}
}

// Kill a Task with the given taskID
func Kill(taskID string) error {
	task := config.State[taskID]

	logrus.Debug("Kill task ", taskID, task)

	// tell mesos to shutdonw the given task
	err := Call(&mesosproto.Call{
		Type: mesosproto.Call_KILL,
		Kill: &mesosproto.Call_Kill{
			TaskID:  task.Status.TaskID,
			AgentID: task.Status.AgentID,
		},
	})

	// remove deleted task from state
	if err == nil {
		deleteOldTask(task.Status.TaskID)
	}

	return err
}

func defaultResources(cmd Command) []mesosproto.Resource {
	CPU := "cpus"
	MEM := "mem"
	cpu := cmd.CPU
	mem := cmd.Memory
	PORT := "ports"

	res := []mesosproto.Resource{
		{
			Name:   CPU,
			Type:   mesosproto.SCALAR.Enum(),
			Scalar: &mesosproto.Value_Scalar{Value: cpu},
		},
		{
			Name:   MEM,
			Type:   mesosproto.SCALAR.Enum(),
			Scalar: &mesosproto.Value_Scalar{Value: mem},
		},
	}

	var portBegin, portEnd uint64

	if cmd.DockerPortMappings != nil {
		portBegin = uint64(cmd.DockerPortMappings[0].HostPort)
		portEnd = portBegin + 2

		res = []mesosproto.Resource{
			{
				Name:   CPU,
				Type:   mesosproto.SCALAR.Enum(),
				Scalar: &mesosproto.Value_Scalar{Value: cpu},
			},
			{
				Name:   MEM,
				Type:   mesosproto.SCALAR.Enum(),
				Scalar: &mesosproto.Value_Scalar{Value: mem},
			},
			{
				Name: PORT,
				Type: mesosproto.RANGES.Enum(),
				Ranges: &mesosproto.Value_Ranges{
					Range: []mesosproto.Value_Range{
						{
							Begin: portBegin,
							End:   portEnd,
						},
					},
				},
			},
		}
	}

	return res
}

func declineOffer(offerIds []mesosproto.OfferID) *mesosproto.Call {
	decline := &mesosproto.Call{
		Type:    mesosproto.Call_DECLINE,
		Decline: &mesosproto.Call_Decline{OfferIDs: offerIds},
	}
	return decline
}
