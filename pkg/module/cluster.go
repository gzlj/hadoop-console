package module

import "time"

type ClusterRuntimeInfo struct {
	ClusterName string   `json:"clusterName"`
	Nodes []ClusterNodeInfo `json:"nodes"`
}

type ClusterNodeInfo struct {
	//Hostname string   `json:"hostname"`
	//Ip string   `json:"ip"`
	HostnameIp
	State string   `json:"state"`
	RunningComponents []string `json:"runningComponents"`
	LastKnown time.Time `json:"lastKnown"`
}

/*

type ClusteredComponentStatuses struct {
	ClusterName string   `json:"clusterName"`
	Host string   `json:"host"`
	RunningComponents []string `json:"runningComponents"`
}
 */

 type ClusterConfig struct {
	ClusterName string   `json:"clusterName"`
	Nodes []NodeRole  `json:"nodes"`
	Password string `json:"password"`
 }

 type HostnameIp struct {
	 Hostname string   `json:"hostname"`
	 Ip string   `json:"ip"`
 }

 type NodeRole struct {
	 HostnameIp
	 Roles []string `json:"roles"`
 }