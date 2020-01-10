package handler

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/gzlj/hadoop-console/pkg/global"
	"github.com/gzlj/hadoop-console/pkg/infra/cluster"
	"github.com/gzlj/hadoop-console/pkg/infra/db"
	"github.com/gzlj/hadoop-console/pkg/infra/job"
	"github.com/gzlj/hadoop-console/pkg/module"
	"log"
	"strconv"
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

	response = global.BuildResponse(200, "ok", cluster.ClusterRuntimeInfos)
	c.JSON(200, response)

}

func Test(c *gin.Context) {
	info := module.ClusterNodeInfo{}
	info.Ip = "192.168.35.100"
	info.Hostname = "hehe"
	info.State = "Ready"
	info.RunningComponents = nil
	c.JSON(200, info)
}

func HandlerCreateCluster(c *gin.Context) {
	var (
		dto           db.ClusterDto
		err           error
		hadoopCluster db.Cluster
		response      global.Response
	)
	if err = c.ShouldBindJSON(&dto); err != nil {
		response = global.BuildResponse(400, "Requet body is not correct.", nil)
		c.JSON(200, response)
		return
	}

	if hadoopCluster, err = dto.ToCluster(); err != nil {
		response = global.BuildResponse(400, "Convert failed.Requet body is not correct.", nil)
		c.JSON(200, response)
		return
	}

	if err = db.ADDCluster(&hadoopCluster); err != nil {
		response = global.BuildResponse(500, "Cannot create hadoopCluster in db: "+err.Error(), nil)
		//c.JSON(500, "cannot create like in db.")
	}

	// hadoop cluster ssh
	go cluster.RunSshByCluster(hadoopCluster)

	response = global.BuildResponse(200, "OK", nil)
	c.JSON(200, response)
}

func HandlerListCluster(c *gin.Context) {
	var (
		//cluster *db.Cluster
		response global.Response
		dtos     []*db.ClusterDto
		err      error
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
	c.JSON(200, response)

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
		hb       module.NodeHeartBeat
		response global.Response
		err      error
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

//InitHdfs
func HandlerInitHdfs(c *gin.Context) {
	var (
		//dto db.ClusterDto
		err error
		//record db.Cluster
		response global.Response
		id       int
		record   *db.Cluster
		config   module.ClusterConf
	)

	id, err = strconv.Atoi(c.Query("id"))
	if err != nil {
		response = global.BuildResponse(400, "Please specify cluster id.", nil)
		c.JSON(200, response)
		return
	}

	// get record from db
	record, err = db.QueryClusterById(id)
	if err != nil || record == nil {
		// not found
		response = global.BuildResponse(404, "Cluster not found.", nil)
		c.JSON(200, response)
		return
	}
	err = json.Unmarshal([]byte(record.Config), &config)
	if err != nil {
		response = global.BuildResponse(500, "Cluster recored is not correct as json Unmarshal failed.", nil)
		c.JSON(200, response)
		return
	}
	/*	if err = c.ShouldBindJSON(&dto); err != nil {
		response = global.BuildResponse(400, "Requet body is not correct.", nil)
		c.JSON(200, response)
		return
	}*/

	_, ok := global.G_JobExcutingInfo[record.Name+"-hdfs"]
	if ok {
		response = global.BuildResponse(400, "Job is already Running.Please wait to complete.", nil)
		c.JSON(200, response)
		return
	}

	go cluster.StartInitHdfs(id, record.Name, config)

	//record.HbChan <- hb
	response = global.BuildResponse(200, "OK.", nil)
	c.JSON(200, response)
}

func QueryJobLog(c *gin.Context) {
	var (
		response global.Response
	)
	log.Println("QueryJobLog response:", response)
	cluster := c.Query("cluster")
	if cluster == "" {
		response = global.BuildResponse(400, "Please specify cluster name.", nil)
		c.JSON(200, response)
		return
	}
	class := c.Query("class")
	if class == "" {
		response = global.BuildResponse(400, "Please specify component class name.", nil)
		c.JSON(200, response)
		return
	}
	//job name
	jobName := cluster + "-" + class
	c.String(200, job.QueryJobLogByName(jobName))
}

func QueryTasksByCluster(c *gin.Context) {

	var (
		response global.Response
		id       int
		err      error
		limit int
	)

	id, err = strconv.Atoi(c.Query("cid"))
	if err != nil {
		response = global.BuildResponse(400, "Please specify cluster id.", nil)
		c.JSON(200, response)
		return
	}
	limit, err = strconv.Atoi(c.Query("limit"))
	if err != nil || limit == 0 {
		limit = 15
	}
	tasks, _ := cluster.ListLatestTasksByClusterLimit(uint(id), uint(limit))
	response = global.BuildResponse(200, "OK", tasks)
	c.JSON(200, response)
}

func QueryLogByTask(c *gin.Context){

	var (
		response global.Response
		id       int
		err      error
		l   db.LogDto
	)
	id, err = strconv.Atoi(c.Query("tid"))
	if err != nil {
		response = global.BuildResponse(400, "Please specify task id.", nil)
		c.JSON(200, response)
		return
	}
	l, err = job.QueryLogByTaskId(uint(id))
	if err != nil {
		response = global.BuildResponse(400, err.Error(), nil)
		c.JSON(200, response)
	}
	/*

	(clusters []*Cluster) {

	G_db.Find(&clusters, "removed = ?", "0")
	 */

	/*var (
		//err error
		l   db.Log
		)
	//G_db.Find(&ss, "cid = ? AND removed = ?", id, "0")
	db.G_db.First(&l, "tid = ? AND removed = ?", 10, "0")*/
	//db.G_db.First(&l, "tid = ? AND removed = ?", 4, "0")
	response = global.BuildResponse(200, "OK", l)
	c.JSON(200, response)
}

//QueryServiceDetail
func QueryServiceDetail(c *gin.Context){
	var (
		response global.Response
		sid       int
		err      error
		detail *module.ServiceDetail
	)
	sid, err = strconv.Atoi(c.Query("sid"))
	if err != nil {
		response = global.BuildResponse(400, "Please specify service id.", nil)
		c.JSON(200, response)
		return
	}
	detail, err = cluster.QueryServiceDetail(uint(sid))
	if err != nil {
		response = global.BuildResponse(500, err.Error(), nil)
		c.JSON(200, response)
		return
	}
	response = global.BuildResponse(200, "OK", detail)
	c.JSON(200, response)
}

func AddService(c *gin.Context){
	/*
	var (
		dto      db.ClusterDto
		err      error
		cluster  db.Cluster
		response global.Response
	)
	if err = c.ShouldBindJSON(&dto); err != nil {
	 */
	var (
		config module.ClusteredServiceConfig
		err      error
		s *db.Service
		response global.Response
	)
	if err = c.ShouldBindJSON(&config); err != nil {
		response = global.BuildResponse(400, "Requet body is not correct.", nil)
		c.JSON(200, response)
		return
	}
	//log.Println("config:",config)
	s, err = cluster.AddService(config)
	if err != nil {
		response = global.BuildResponse(500, err.Error(), nil)
		c.JSON(200, response)
		return
	}

	// go start to install service in backgroud
	go cluster.InitService(s)
	response = global.BuildResponse(200, "OK", nil)
	c.JSON(200, response)
}

func InitService(c *gin.Context) {
	var (
		response global.Response
		//cid       int
		sid       int
		s *db.Service
		err      error
	)
	sid, err = strconv.Atoi(c.Query("sid"))
	s, err = cluster.QueryServiceById(uint(sid))
	if err != nil {
		response = global.BuildResponse(404, "Service Not found", nil)
		c.JSON(200, response)
	}
	go cluster.InitService(s)
	response = global.BuildResponse(200, "OK", nil)
	c.JSON(200, response)
}


func QueryService(c *gin.Context) {
	var (
		response global.Response
		//cid       int
		sid       int
		s *db.Service
		err      error
	)
	sid, err = strconv.Atoi(c.Query("sid"))
	s, err = cluster.QueryServiceById(uint(sid))
	if err != nil {
		response = global.BuildResponse(404, "Service Not found", nil)
		c.JSON(200, response)
	}
	go cluster.InitService(s)
	response = global.BuildResponse(200, "OK", nil)
	c.JSON(200, response)
}

func ListServiceByCluster(c *gin.Context) {
	var (
		response global.Response
		cid       int
		err      error
		ss []*db.Service
		dto *db.ServiceDto
		dtos []*db.ServiceDto
	)
	cid, err = strconv.Atoi(c.Query("cid"))
	if err != nil {
		response = global.BuildResponse(400, "Please specify cluster id.", nil)
		c.JSON(200, response)
		return
	}


	ss, err = cluster.GetServicesByCluster(uint(cid))
	if err != nil {
		response = global.BuildResponse(500, err.Error(), nil)
		c.JSON(200, response)
		return
	}

	for _, s := range ss {
		dto, err  = s.ToDto()
		if err != nil {
			continue
		}
		dtos = append(dtos, dto)
	}
	response = global.BuildResponse(200, "OK", dtos)
	c.JSON(200, response)
	/*
	func GetServicesByCluster(cid uint) (ss []*db.Service, err error) {
	ss, err = db.GetServiceByClusterId(cid)
	return
}

	 */



}

func GetHostsFile(c *gin.Context) {
	var (
		response global.Response
		sid       int
		err      error
		liens []string

	)
	sid, err = strconv.Atoi(c.Query("sid"))
	liens, _,err = cluster.GenerateHostsFileByCluster(uint(sid))
	if err != nil {
		log.Println(err)
		response = global.BuildResponse(500, err.Error(), liens)
	}
	response = global.BuildResponse(200, "OK", liens)
	c.JSON(200, response)
}