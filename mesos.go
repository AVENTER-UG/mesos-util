package mesosutil

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"

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

// SuppressFramework if all Tasks are running, suppress framework offers
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

// Kill a Task with the given taskID
func Kill(taskID string, agentID string) error {

	logrus.Debug("Kill task ", taskID)
	// tell mesos to shutdonw the given task
	err := Call(&mesosproto.Call{
		Type: mesosproto.Call_KILL,
		Kill: &mesosproto.Call_Kill{
			TaskID: mesosproto.TaskID{
				Value: taskID,
			},
			AgentID: &mesosproto.AgentID{
				Value: agentID,
			},
		},
	})

	return err
}

// DeclineOffer will decline the given offers
func DeclineOffer(offerIds []mesosproto.OfferID) *mesosproto.Call {
	decline := &mesosproto.Call{
		Type:    mesosproto.Call_DECLINE,
		Decline: &mesosproto.Call_Decline{OfferIDs: offerIds},
	}
	return decline
}

// GetOffer get out the offer for the mesos task
func GetOffer(offers *mesosproto.Event_Offers, cmd Command) (mesosproto.Offer, []mesosproto.OfferID) {
	var offerIds []mesosproto.OfferID
	var offerret mesosproto.Offer

	for n, offer := range offers.Offers {
		logrus.Debug("Got Offer From:", offer.GetHostname())
		offerIds = append(offerIds, offer.ID)

		if cmd.TaskName == "" {
			continue
		}

		// if the ressources of this offer does not matched what the command need, the skip
		if !IsRessourceMatched(offer.Resources, cmd) {
			logrus.Debug("Could not found any matched ressources, get next offer")
			Call(DeclineOffer(offerIds))
			continue
		}
		offerret = offers.Offers[n]
	}
	return offerret, offerIds
}

// IsRessourceMatched - check if the ressources of the offer are matching the needs of the cmd
func IsRessourceMatched(ressource []mesosproto.Resource, cmd Command) bool {
	mem := false
	cpu := false
	ports := true

	for _, v := range ressource {
		if v.GetName() == "cpus" && v.Scalar.GetValue() >= cmd.CPU {
			logrus.Debug("Matched Offer CPU")
			cpu = true
		}
		if v.GetName() == "mem" && v.Scalar.GetValue() >= cmd.Memory {
			logrus.Debug("Matched Offer Memory")
			mem = true
		}
		if len(cmd.DockerPortMappings) > 0 {
			if v.GetName() == "ports" {
				for _, taskPort := range cmd.DockerPortMappings {
					for _, portRange := range v.GetRanges().Range {
						if taskPort.HostPort >= uint32(portRange.Begin) && taskPort.HostPort <= uint32(portRange.End) {
							logrus.Debug("Matched Offer TaskPort: ", taskPort.HostPort)
							logrus.Debug("Matched Offer RangePort: ", portRange)
							ports = ports || true
							break
						}
						ports = ports || false
					}
				}
			}
		}
	}

	return mem && cpu && ports
}

// GetAgentInfo get information about the agent
func GetAgentInfo(agentID string) MesosSlaves {
	client := &http.Client{}
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	protocol := "https"
	if !config.MesosSSL {
		protocol = "http"
	}
	req, _ := http.NewRequest("POST", protocol+"://"+config.MesosMasterServer+"/slaves/"+agentID, nil)
	req.Close = true
	req.SetBasicAuth(config.Username, config.Password)
	req.Header.Set("Mesos-Stream-Id", config.MesosStreamID)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)

	if res.StatusCode == http.StatusOK {

		if err != nil {
			logrus.WithField("func", "getAgentInfo").Error("Could not connect to agent: ", err.Error())
			return MesosSlaves{}
		}

		defer res.Body.Close()

		var agent MesosAgent
		err = json.NewDecoder(res.Body).Decode(&agent)
		if err != nil {
			logrus.WithField("func", "getAgentInfo").Error("Could not encode json result: ", err.Error())
			// if there is an error, dump out the res.Body as debug
			bodyBytes, err := io.ReadAll(res.Body)
			if err == nil {
				logrus.WithField("func", "getAgentInfo").Debug("response Body Dump: ", string(bodyBytes))
			}
			return MesosSlaves{}
		}

		// get the used agent info
		for _, a := range agent.Slaves {
			if a.ID == agentID {
				return a
			}
		}
	}

	return MesosSlaves{}
}

// GetNetworkInfo get network info of task
func GetNetworkInfo(taskID string) []mesosproto.NetworkInfo {
	client := &http.Client{}
	// #nosec G402
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	protocol := "https"
	if !config.MesosSSL {
		protocol = "http"
	}
	req, _ := http.NewRequest("POST", protocol+"://"+config.MesosMasterServer+"/tasks/?task_id="+taskID+"&framework_id="+config.FrameworkInfo.ID.GetValue(), nil)
	req.Close = true
	req.SetBasicAuth(config.Username, config.Password)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)

	if err != nil {
		logrus.WithField("func", "getNetworkInfo").Error("Could not connect to agent: ", err.Error())
		return []mesosproto.NetworkInfo{}
	}

	defer res.Body.Close()

	var task MesosTasks
	err = json.NewDecoder(res.Body).Decode(&task)
	if err != nil {
		logrus.WithField("func", "getAgentInfo").Error("Could not encode json result: ", err.Error())
		return []mesosproto.NetworkInfo{}
	}

	if len(task.Tasks) > 0 {
		for _, status := range task.Tasks[0].Statuses {
			if status.State == "TASK_RUNNING" {
				var netw []mesosproto.NetworkInfo
				netw = append(netw, status.ContainerStatus.NetworkInfos[0])
				// try to resolv the tasks hostname
				if task.Tasks[0].Container.Hostname != nil {
					netw = []mesosproto.NetworkInfo{}
					addr, err := net.LookupIP(*task.Tasks[0].Container.Hostname)
					if err == nil {
						hostNet := []mesosproto.NetworkInfo{{
							IPAddresses: []mesosproto.NetworkInfo_IPAddress{{
								IPAddress: func() *string { x := string(addr[0]); return &x }(),
							}}},
						}
						netw = append(netw, hostNet[0])
					}
				}
				return netw
			}
		}
	}
	return []mesosproto.NetworkInfo{}
}

// DecodeTask will decode the key into an mesos command struct
func DecodeTask(key string) Command {
	var task Command
	err := json.NewDecoder(strings.NewReader(key)).Decode(&task)
	if err != nil {
		logrus.WithField("func", "DecodeTask").Error("Could not decode task: ", err.Error())
		return Command{}
	}
	return task
}
