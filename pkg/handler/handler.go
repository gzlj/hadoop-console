package handler

import (
	"github.com/gin-gonic/gin"
	"github.com/gzlj/hadoop-console/pkg/global"
	"github.com/gzlj/hadoop-console/pkg/module"

	//"github.com/gzlj/hadoop-console/pkg/module"
	"strings"
)

func HandleHeartBeat(c *gin.Context) {

}

func HandlerGetClusterInfo(c *gin.Context) {

	//cluster name
	var (
		cluster string
		response global.Response
		//runtimeInfo module.ClusterRuntimeInfo
	)
	cluster = c.Query("cluster")

	cluster = strings.TrimSpace(cluster)
	if len(cluster) == 0 {
		response = global.BuildResponse(400, "input is not valid.", nil)
		c.JSON(200,response)
		return
	}

}

func Test(c *gin.Context) {
	info := module.ClusterNodeInfo{}
	info.Ip="192.168.35.100"
	info.Hostname = "hehe"
	info.State = "Ready"
	info.RunningComponents = nil
	c.JSON(200,info)

}

