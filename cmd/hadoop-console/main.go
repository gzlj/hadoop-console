package main

import (
	"github.com/gin-gonic/gin"
	"github.com/gzlj/hadoop-console/pkg/global"
	"github.com/gzlj/hadoop-console/pkg/handler"
	"os"
	"runtime"
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
	//hostname , _ := infra.GetHostName()
	global.InitConfig(serverPort)
}

func (s *APIServer) registryApi() {
	registryBasicApis(s.engine)
}

func registryBasicApis(r *gin.Engine) {
	//r.POST("", handler.HandleHeartBeat)
	r.GET("/test", handler.Test)
}

func main() {
	server := &APIServer{
		engine: gin.Default(),
		port: global.G_config.ServerPort,
	}
	server.registryApi()
	server.Run()
}




