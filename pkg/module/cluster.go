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
