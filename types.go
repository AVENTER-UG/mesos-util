package mesosutil

import (
	"time"

	mesosproto "github.com/AVENTER-UG/mesos-util/proto"
)

type FrameworkConfig struct {
	FrameworkHostname     string
	FrameworkPort         string
	FrameworkBind         string
	FrameworkUser         string
	FrameworkName         string
	FrameworkRole         string
	FrameworkInfo         mesosproto.FrameworkInfo
	FrameworkInfoFile     string
	FrameworkInfoFilePath string
	PortRangeFrom         int
	PortRangeTo           int
	CommandChan           chan Command `json:"-"`
	Username              string
	Password              string
	MesosMasterServer     string
	MesosSSL              bool
	MesosStreamID         string
	MesosCNI              string
	TaskID                string
	SSL                   bool
	State                 map[string]State
}

// Command is a chan which include all the Information about the started tasks
type Command struct {
	ContainerImage     string                                            `json:"container_image,omitempty"`
	ContainerType      string                                            `json:"container_type,omitempty"`
	TaskName           string                                            `json:"task_name,omitempty"`
	Command            string                                            `json:"command,omitempty"`
	Hostname           string                                            `json:"hostname,omitempty"`
	Domain             string                                            `json:"domain,omitempty"`
	Privileged         bool                                              `json:"privileged,omitempty"`
	NetworkMode        string                                            `json:"network_mode,omitempty"`
	Volumes            []mesosproto.Volume                               `protobuf:"bytes,1,rep,name=volumes" json:"volumes,omitempty"`
	Shell              bool                                              `protobuf:"varint,2,opt,name=shell,def=1" json:"shell,omitempty"`
	Uris               []mesosproto.CommandInfo_URI                      `protobuf:"bytes,3,rep,name=uris" json:"uris,omitempty"`
	Environment        mesosproto.Environment                            `protobuf:"bytes,4,opt,name=environment" json:"environment,omitempty"`
	NetworkInfo        []mesosproto.NetworkInfo                          `protobuf:"bytes,5,opt,name=networkinfo" json:"networkinfo,omitempty"`
	DockerPortMappings []mesosproto.ContainerInfo_DockerInfo_PortMapping `protobuf:"bytes,6,rep,name=port_mappings,json=portMappings" json:"port_mappings,omitempty"`
	DockerParameter    []mesosproto.Parameter                            `protobuf:"bytes,7,rep,name=parameters" json:"parameters,omitempty"`
	Arguments          []string                                          `protobuf:"bytes,8,rep,name=arguments" json:"arguments,omitempty"`
	Discovery          mesosproto.DiscoveryInfo                          `protobuf:"bytes,9,opt,name=discovery" json:"discovery,omitempty"`
	Executor           mesosproto.ExecutorInfo                           `protobuf:"bytes,10,opt,name=executor" json:"executor,omitempty"`
	InternalID         int
	TaskID             string
	Memory             float64
	CPU                float64
	Disk               float64
	Agent              string
	Labels             []mesosproto.Label
	State              string
	StateTime          time.Time
}

// State will have the state of all tasks stated by this framework
type State struct {
	Command Command                `json:"command"`
	Status  *mesosproto.TaskStatus `json:"status"`
}
