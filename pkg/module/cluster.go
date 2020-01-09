package module

type ClusterRuntimeInfo struct {
	ClusterName string            `json:"clusterName"`
	Nodes       []ClusterNodeInfo `json:"nodes"`
}

type ClusterNodeInfo struct {
	NodeRole
	State             string   `json:"state"`
	RunningComponents []string `json:"runningComponents"`
	LastKnown         Time     `json:"lastKnown"`
	Components []ComponentStatus `json:"components"`
}

type ComponentStatus struct {
	Name string	`json:"name"`
	DesireToRun bool `json:"desireToRun"`
	Status string `json:"status"`
	IsProcessing bool `json:"isProcessing"`
}

type ClusterConfig struct {
	ClusterName string     `json:"clusterName"`
	Nodes       []NodeRole `json:"nodes"`
	Password    string     `json:"password"`
}

type HostnameIp struct {
	Hostname string `json:"hostname"`
	Ip       string `json:"ip"`
}

type HostIpPort struct {
	HostnameIp
	Port int `json:"port"`
}

type NodeRole struct {
	HostnameIp
	Roles []string `json:"roles"`
}

type NodeStatus struct {
	Hostname  string `json:"hostname"`
	State     string `json:"state"`
	LastKnown Time   `json:"lastKnown"`
}

type NodeHeartBeat struct {
	Cluster  string `json:"cluster"`
	Hostname  string `json:"hostname"`
	RunningComponents []string `json:"runningComponents"`
}


type ClusterConf struct {
	Nodes []HostIpPort `json:"nodes"`
	Password string `json:"password,omitempty"`

	HdfsConfig *HdfsConfig `json:"hdfsConfig,omitempty"`
	HbaseConfig *HbaseConfig `json:"hbaseConfig,omitempty"`
}

type ClusteredHbaseConfig struct {
	Cid int `json:"cid"`
	Name string `json:"name"`
	RoleToHosts map[string][]HostnameIp `json:"roleToHosts"`
}





type HdfsConfig struct {
	//Name string `json:"name"`
	Zookeepers []HostnameIp `json:"zookeepers,omitempty"`
	ResourceManagers []HostnameIp `json:"resourceManagers,omitempty"`
	JournalNodes []HostnameIp `json:"journalNodes,omitempty"`
	NameNodes []HostnameIp `json:"nameNodes,omitempty"`
	DataNodes []HostnameIp `json:"dataNodes,omitempty"`
}

type HbaseConfig struct {
	//Name string `json:"name"`
	RegionServers []HostnameIp `json:"regionServers,omitempty"`
	Masters []HostnameIp `json:"masters,omitempty"`
}

type Endpoint struct {
	Name string
	HostnameIp
	port int
	Status string
}


