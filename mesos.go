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
		DeleteOldTask(task.Status.TaskID)
	}

	return err
}

func DeclineOffer(offerIds []mesosproto.OfferID) *mesosproto.Call {
	decline := &mesosproto.Call{
		Type:    mesosproto.Call_DECLINE,
		Decline: &mesosproto.Call_Decline{OfferIDs: offerIds},
	}
	return decline
}

// GetOffer get out the offer for the mesos task
func GetOffer(offers *mesosproto.Event_Offers, cmd Command) (mesosproto.Offer, []mesosproto.OfferID) {
	offerIds := []mesosproto.OfferID{}

	count := 0
	for n, offer := range offers.Offers {
		logrus.Debug("Got Offer From:", offer.GetHostname())
		offerIds = append(offerIds, offer.ID)

		count = n
	}

	return offers.Offers[count], offerIds

}

// PrepareTaskInfoExecuteContainer will make the mesos task object container ready
func PrepareTaskInfoExecuteContainer(agent mesosproto.AgentID, cmd Command, defaultresources DefaultResources) ([]mesosproto.TaskInfo, error) {
	contype := mesosproto.ContainerInfo_DOCKER.Enum()

	// Set Container Network Mode
	networkMode := mesosproto.ContainerInfo_DockerInfo_BRIDGE.Enum()

	if cmd.NetworkMode == "host" {
		networkMode = mesosproto.ContainerInfo_DockerInfo_HOST.Enum()
	}
	if cmd.NetworkMode == "none" {
		networkMode = mesosproto.ContainerInfo_DockerInfo_NONE.Enum()
	}
	if cmd.NetworkMode == "user" {
		networkMode = mesosproto.ContainerInfo_DockerInfo_USER.Enum()
	}
	if cmd.NetworkMode == "bridge" {
		networkMode = mesosproto.ContainerInfo_DockerInfo_BRIDGE.Enum()
	}

	// Save state of the new task
	newTaskID := cmd.TaskName + "_" + strconv.Itoa(int(cmd.TaskID))
	tmp := config.State[newTaskID]
	tmp.Command = cmd
	config.State[newTaskID] = tmp

	var msg mesosproto.TaskInfo

	msg.Name = cmd.TaskName
	msg.TaskID = mesosproto.TaskID{
		Value: newTaskID,
	}
	msg.AgentID = agent
	msg.Resources = defaultresources(cmd)

	if cmd.Command == "" {
		msg.Command = &mesosproto.CommandInfo{
			Shell:       &cmd.Shell,
			URIs:        cmd.Uris,
			Environment: &cmd.Environment,
		}
	} else {
		msg.Command = &mesosproto.CommandInfo{
			Shell:       &cmd.Shell,
			Value:       &cmd.Command,
			URIs:        cmd.Uris,
			Environment: &cmd.Environment,
		}
	}

	msg.Container = &mesosproto.ContainerInfo{
		Type:     contype,
		Volumes:  cmd.Volumes,
		Hostname: &cmd.Hostname,
		Docker: &mesosproto.ContainerInfo_DockerInfo{
			Image:        cmd.ContainerImage,
			Network:      networkMode,
			PortMappings: cmd.DockerPortMappings,
			Privileged:   &cmd.Privileged,
			Parameters:   cmd.DockerParameter,
		},
		NetworkInfos: cmd.NetworkInfo,
	}

	if cmd.Discovery != (mesosproto.DiscoveryInfo{}) {
		msg.Discovery = &cmd.Discovery
	}

	if cmd.Labels != nil {
		msg.Labels = &mesosproto.Labels{
			Labels: cmd.Labels,
		}
	}

	return []mesosproto.TaskInfo{msg}, nil
}

// HandleUpdate will handle the offers event of mesos
func HandleUpdate(event *mesosproto.Event) error {
	// unsuppress
	revive := &mesosproto.Call{
		Type: mesosproto.Call_REVIVE,
	}
	Call(revive)

	update := event.Update

	msg := &mesosproto.Call{
		Type: mesosproto.Call_ACKNOWLEDGE,
		Acknowledge: &mesosproto.Call_Acknowledge{
			AgentID: *update.Status.AgentID,
			TaskID:  update.Status.TaskID,
			UUID:    update.Status.UUID,
		},
	}

	// Save state of the task
	taskID := update.Status.GetTaskID().Value
	tmp := config.State[taskID]
	tmp.Status = &update.Status

	logrus.Debugf("HandleUpdate: %s Status %s ", taskID, update.Status.State.String())

	switch *update.Status.State {
	case mesosproto.TASK_FAILED:
		DeleteOldTask(tmp.Status.TaskID)
	case mesosproto.TASK_KILLED:
		DeleteOldTask(tmp.Status.TaskID)
	case mesosproto.TASK_LOST:
		DeleteOldTask(tmp.Status.TaskID)
	}

	// Update Framework State File
	config.State[taskID] = tmp
	persConf, _ := json.Marshal(&config)
	ioutil.WriteFile(config.FrameworkInfoFile, persConf, 0644)

	return Call(msg)
}
