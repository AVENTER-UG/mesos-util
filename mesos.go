package mesosutil

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"

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
	var offerIds []mesosproto.OfferID
	var offerret mesosproto.Offer

	for n, offer := range offers.Offers {
		logrus.Debug("Got Offer From:", offer.GetHostname())
		offerIds = append(offerIds, offer.ID)

		// if the ressources of this offer does not matched what the command need, the skip
		if !isRessourceMatched(offer.Resources, cmd) {
			Call(DeclineOffer(offerIds))
			continue
		}
		offerret = offers.Offers[n]
	}
	return offerret, offerIds
}

// check if the ressources of the offer are matching the needs of the cmd
func isRessourceMatched(ressource []mesosproto.Resource, cmd Command) bool {
	mem := false
	cpu := false
	for _, v := range ressource {
		if v.GetName() == "cpus" && v.Scalar.GetValue() >= cmd.CPU {
			logrus.Debug("Matched Offer CPU")
			cpu = true
		}
		if v.GetName() == "mem" && v.Scalar.GetValue() >= cmd.Memory {
			logrus.Debug("Matched Offer Memory")
			mem = true
		}
	}

	return mem && cpu
}
