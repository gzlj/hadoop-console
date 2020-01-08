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
	Nodes []NodeRole `json:"nodes"`
	Password string `json:"password"`

	HdfsConfig HdfsConfig `json:"hdfsConfig"`
	HbaseConfig HbaseConfig `json:"hbaseConfig"`
}

type ClusteredHbaseConfig struct {
	Cid int `json:"cid"`
	Name string `json:"name"`
	RoleToHosts map[string][]HostnameIp `json:"roleToHosts"`
}





type HdfsConfig struct {
	//Name string `json:"name"`
	Zookeepers []HostnameIp `json:"zookeepers"`
	ResourceManagers []HostnameIp `json:"resourceManagers"`
	JournalNodes []HostnameIp `json:"journalNodes"`
	NameNodes []HostnameIp `json:"nameNodes"`
	DataNodes []HostnameIp `json:"dataNodes"`
}

type HbaseConfig struct {
	//Name string `json:"name"`
	RegionServers []HostnameIp `json:"regionServers"`
	Masters []HostnameIp `json:"masters"`
}

type Endpoint struct {
	Name string
	HostnameIp
	port int
	Status string
}


