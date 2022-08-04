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
	Instances          int
	LinuxInfo          mesosproto.LinuxInfo `protobuf:"bytes,11,opt,name=linux_info,json=linuxInfo" json:"linux_info,omitempty"`
	MesosAgent         MesosSlaves
}

// State will have the state of all tasks stated by this framework
type State struct {
	Command Command                `json:"command"`
	Status  *mesosproto.TaskStatus `json:"status"`
}

// MesosAgents
type MesosAgent struct {
	Slaves          []MesosSlaves `json:"slaves"`
	RecoveredSlaves []interface{} `json:"recovered_slaves"`
}

// MesosSlaves ..
type MesosSlaves struct {
	ID         string `json:"id"`
	Hostname   string `json:"hostname"`
	Port       int    `json:"port"`
	Attributes struct {
	} `json:"attributes"`
	Pid              string  `json:"pid"`
	RegisteredTime   float64 `json:"registered_time"`
	ReregisteredTime float64 `json:"reregistered_time"`
	Resources        struct {
		Disk  float64 `json:"disk"`
		Mem   float64 `json:"mem"`
		Gpus  float64 `json:"gpus"`
		Cpus  float64 `json:"cpus"`
		Ports string  `json:"ports"`
	} `json:"resources"`
	UsedResources struct {
		Disk  float64 `json:"disk"`
		Mem   float64 `json:"mem"`
		Gpus  float64 `json:"gpus"`
		Cpus  float64 `json:"cpus"`
		Ports string  `json:"ports"`
	} `json:"used_resources"`
	OfferedResources struct {
		Disk float64 `json:"disk"`
		Mem  float64 `json:"mem"`
		Gpus float64 `json:"gpus"`
		Cpus float64 `json:"cpus"`
	} `json:"offered_resources"`
	ReservedResources struct {
	} `json:"reserved_resources"`
	UnreservedResources struct {
		Disk  float64 `json:"disk"`
		Mem   float64 `json:"mem"`
		Gpus  float64 `json:"gpus"`
		Cpus  float64 `json:"cpus"`
		Ports string  `json:"ports"`
	} `json:"unreserved_resources"`
	Active                bool     `json:"active"`
	Deactivated           bool     `json:"deactivated"`
	Version               string   `json:"version"`
	Capabilities          []string `json:"capabilities"`
	ReservedResourcesFull struct {
	} `json:"reserved_resources_full"`
	UnreservedResourcesFull []struct {
		Name   string `json:"name"`
		Type   string `json:"type"`
		Scalar struct {
			Value float64 `json:"value"`
		} `json:"scalar,omitempty"`
		Role   string `json:"role"`
		Ranges struct {
			Range []struct {
				Begin int `json:"begin"`
				End   int `json:"end"`
			} `json:"range"`
		} `json:"ranges,omitempty"`
	} `json:"unreserved_resources_full"`
	UsedResourcesFull []struct {
		Name   string `json:"name"`
		Type   string `json:"type"`
		Scalar struct {
			Value float64 `json:"value"`
		} `json:"scalar,omitempty"`
		Role           string `json:"role"`
		AllocationInfo struct {
			Role string `json:"role"`
		} `json:"allocation_info"`
		Ranges struct {
			Range []struct {
				Begin int `json:"begin"`
				End   int `json:"end"`
			} `json:"range"`
		} `json:"ranges,omitempty"`
	} `json:"used_resources_full"`
	OfferedResourcesFull []interface{} `json:"offered_resources_full"`
}

// MesosTasks hold the information of the task
type MesosTasks struct {
	Tasks []struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		FrameworkID string `json:"framework_id"`
		ExecutorID  string `json:"executor_id"`
		SlaveID     string `json:"slave_id"`
		AgentID     string `json:"agent_id"`
		State       string `json:"state"`
		Resources   struct {
			Disk float64 `json:"disk"`
			Mem  float64 `json:"mem"`
			Gpus float64 `json:"gpus"`
			Cpus float64 `json:"cpus"`
		} `json:"resources"`
		Role     string `json:"role"`
		Statuses []struct {
			State           string  `json:"state"`
			Timestamp       float64 `json:"timestamp"`
			ContainerStatus struct {
				ContainerID struct {
					Value string `json:"value"`
				} `json:"container_id"`
				NetworkInfos []mesosproto.NetworkInfo `json:"network_infos"`
			} `json:"container_status,omitempty"`
		} `json:"statuses"`
		Discovery mesosproto.DiscoveryInfo `json:"discovery"`
		Container mesosproto.ContainerInfo `json:"container"`
	} `json:"tasks"`
}
