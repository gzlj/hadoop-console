package main

import (
	"github.com/gin-gonic/gin"
	"github.com/gzlj/hadoop-console/pkg/global"
	"github.com/gzlj/hadoop-console/pkg/handler"
	"github.com/gzlj/hadoop-console/pkg/infra/cluster"
	"github.com/gzlj/hadoop-console/pkg/infra/db"
	"os"
	"runtime"
	"strconv"
)

type APIServer struct {
	engine *gin.Engine
	port string
}

func (s *APIServer) Run() {
	s.engine.Run(":" + s.port)
}

// 初始化线程数量
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func init() {
	initEnv()

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == ""{
		serverPort="18080"
	}

	mysqlHost := os.Getenv("MYSQL_HOST")
	if mysqlHost == ""{
		mysqlHost="localhost"
	}
	mysqlPort := os.Getenv("MYSQL_PORT")
	if mysqlPort == ""{
		mysqlPort="3306"
	}
	mysqlUser := os.Getenv("MYSQL_USER")
	if mysqlUser == ""{
		mysqlUser="root"
	}
	mysqlPassword := os.Getenv("MYSQL_PASSWORD")
	if mysqlPassword == ""{
		mysqlPassword="root_password"
	}

	syncToDbSecondsStr := os.Getenv("MYSQL_PASSWORD")
	if syncToDbSecondsStr == ""{
		syncToDbSecondsStr="300"
	}
	syncToDbSeconds, err :=strconv.Atoi(syncToDbSecondsStr)
	if err != nil {
		syncToDbSeconds = 300
	}
	global.InitConfig(serverPort, mysqlHost, mysqlPort, mysqlUser, mysqlPassword, syncToDbSeconds)
	db.InitDb()
	cluster.SyncClusterFromDb()
}

func (s *APIServer) registryApi() {
	registryBasicApis(s.engine)
}

func registryBasicApis(r *gin.Engine) {
	r.GET("/status", handler.HandlerGetClusterInfo)
	r.POST("/clusters", handler.HandlerCreateCluster)
	r.GET("/clusters", handler.HandlerListCluster)
	r.POST("/heartbeat", handler.HandlerHeartBeat)
	r.POST("/cluster/init", handler.HandlerInitHdfs)
	r.GET("/cluster/log", handler.QueryJobLog)

}

func main() {
	server := &APIServer{
		engine: gin.Default(),
		port: global.G_config.ServerPort,
	}
	go cluster.SyncFromHeartBeat()
	go cluster.SyncClusterStatusToDbLoop()
	go cluster.CalculateNodeStatusLoop()
	server.registryApi()
	server.Run()
}




