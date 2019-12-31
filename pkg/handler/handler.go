package handler

import (
	"github.com/gin-gonic/gin"
	"github.com/gzlj/hadoop-console/pkg/global"
	"github.com/gzlj/hadoop-console/pkg/infra/cluster"
	"github.com/gzlj/hadoop-console/pkg/infra/db"
	"github.com/gzlj/hadoop-console/pkg/module"
)

func HandleHeartBeat(c *gin.Context) {

}

func HandlerGetClusterInfo(c *gin.Context) {

	//cluster name
	var (
		//cluster string
		response global.Response
		//runtimeInfo module.ClusterRuntimeInfo
	)
	//cluster = c.Query("cluster")

	//cluster = strings.TrimSpace(cluster)



	//if len(cluster) == 0 {
	//	response = global.BuildResponse(400, "input is not valid.", nil)
	//	c.JSON(200,response)
	//	return
	//}
	response = global.BuildResponse(200, "ok", cluster.ClusterRuntimeInfos)
	c.JSON(200,response)

}

func Test(c *gin.Context) {
	info := module.ClusterNodeInfo{}
	info.Ip="192.168.35.100"
	info.Hostname = "hehe"
	info.State = "Ready"
	info.RunningComponents = nil
	c.JSON(200,info)

}

func HandlerCreateCluster(c *gin.Context) {
	var (
		dto db.ClusterDto
		err error
		cluster db.Cluster
		response global.Response
	)
	if err = c.ShouldBindJSON(&dto); err != nil {
		response = global.BuildResponse(400, "Requet body is not correct.", nil)
		c.JSON(200, response)
		return
	}

	if cluster, err = dto.ToCluster(); err != nil {
		response = global.BuildResponse(400, "Convert failed.Requet body is not correct.", nil)
		c.JSON(200, response)
		return
	}

	if err = db.ADDCluster(&cluster); err != nil {
		response = global.BuildResponse(500, "Cannot create cluster in db: " + err.Error(), nil)
		//c.JSON(500, "cannot create like in db.")
	}
	c.JSON(200,response)
}

func HandlerListCluster(c *gin.Context) {
	var (
		//cluster *db.Cluster
		response global.Response
		dtos []*db.ClusterDto
		err error
	)
	//clusters = db.ListClusters()
	//for _, cluster = range clusters {
	//	dto, err = cluster.ToDto()
	//	if err != nil {
	//		continue
	//	}
	//	dtos = append(dtos, dto)
	//}
	//sort.Sort(db.ClusterList(dtos))

	dtos, err = cluster.ListClusters()
	if err != nil {
		response = global.BuildResponse(500, err.Error(), nil)
	} else {
		response = global.BuildResponse(200, "OK", dtos)
	}
	c.JSON(200,response)

}

/*func HandlerUpdateStatus(c *gin.Context) {
	var (
		nss []*module.NodeStatus
		//err error
	)

	nss = append(nss, &module.NodeStatus{
		Hostname: "dn3",
		State: "Unknown",
		LastKnown: module.Time(time.Now()),
	})
	_ = db.UpdateStatus(1, nss)
	c.JSON(200,nil)
}*/

func HandlerHeartBeat(c *gin.Context) {
	var (
		hb module.NodeHeartBeat
		response global.Response
		err error
	)
	if err = c.ShouldBindJSON(&hb); err != nil {
		response = global.BuildResponse(400, "Requet body (heartbeat) is not correct.", nil)
		c.JSON(200, response)
		return
	}
	cluster.HbChan <- hb
	response = global.BuildResponse(200, "OK.", nil)
	c.JSON(200, response)
}



