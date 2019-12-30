package cluster

import (
	"github.com/gzlj/hadoop-console/pkg/infra/db"
	"github.com/gzlj/hadoop-console/pkg/module"
	"log"
	"encoding/json"
	"sort"
	"time"
)

var (
	ClusterRuntimeInfos map[string]module.ClusterRuntimeInfo = make(map[string]module.ClusterRuntimeInfo)
	HbChan chan module.NodeHeartBeat = make(chan module.NodeHeartBeat, 64)
)



func CheckNodeLoop() {

	timer1 := time.NewTimer(30 * time.Second)
	for {
		select {
		case <-timer1.C:
			checkNodeAlive()
			timer1.Reset(30 * time.Second)
		}
	}

}

func checkNodeAlive() {

}


func ListClusters()(dtos []*db.ClusterDto, err error){

	var (
		clusters []*db.Cluster
		cluster *db.Cluster
		dto *db.ClusterDto
	)
	clusters = db.ListClusters()
	for _, cluster = range clusters {
		dto, err = cluster.ToDto()
		if err != nil {
			continue
		}
		dtos = append(dtos, dto)
	}
	sort.Sort(db.ClusterList(dtos))

	return
}

func SyncClusterFromDb() (err error){
	//ClusterRuntimeInfos = make(map[string]module.ClusterRuntimeInfo)
	var (
		//cluster *db.Cluster
		//dtos []*db.ClusterDto
		clusters []*db.Cluster
		config module.ClusterConfig
		nodes []module.NodeRole
		infos []module.ClusterNodeInfo
	)
	//dtos, err = ListClusters()
	clusters = db.ListClusters()

	for _, c := range clusters {
		err := json.Unmarshal([]byte(c.Config), &config)
		if err != nil {
			log.Print("Failed to get cluster config when json Unmarshal: ", err.Error())
			continue
		}
		nodes = config.Nodes
		for _, nr := range nodes {
			infos = append(infos,module.ClusterNodeInfo{
				NodeRole: nr,
				State: "Unknown",
			})
		}
		ClusterRuntimeInfos[c.Name] = module.ClusterRuntimeInfo{
			ClusterName: c.Name,
			Nodes: infos,
		}
	}
	return
}

func SyncFromHeartBeat() {
	for hb := range HbChan {
		syncFromHeartBeat(hb)
	}
}

func syncFromHeartBeat(hb module.NodeHeartBeat) {
	clusterInfo, exists := ClusterRuntimeInfos[hb.Cluster]
	if ! exists {
		log.Println("Cluster not exists: ", hb.Cluster)
		return
	}
	nodes := clusterInfo.Nodes
	for i, _ := range nodes {
		if nodes[i].Hostname == hb.Hostname {
			nodes[i].RunningComponents = hb.RunningComponents
			nodes[i].LastKnown = module.Time(time.Now())
			nodes[i].State = "Ready"
		}
	}
}








