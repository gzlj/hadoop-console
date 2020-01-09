package util

import (
	"context"
	"fmt"
	"github.com/gzlj/hadoop-console/pkg/global"
	"github.com/gzlj/hadoop-console/pkg/infra/job"
	"github.com/gzlj/hadoop-console/pkg/module"
	"io"
	"log"
	"os/exec"
	"strconv"
	"strings"
)

func RunSsh(password string, hosts []string) (err error){
	hostsStr := strings.Trim(fmt.Sprint(hosts), "[]")
	log.Println("Run ssh command for hosts:", hostsStr)
	cmdStr := global.WORKING_DIR + "ssh-public-key.sh" + " '" + password + "' " + hostsStr
	err = RunAndWait(cmdStr)
	return
}

func RunAndWait(cmdStr string) (err error) {
	cmd := exec.CommandContext(context.TODO(), "bash", "-c", cmdStr)
	err = cmd.Run()
	return
}

func RunAndLog(tid uint, cmdStr string) (err error) {
	var (
		cmdStdoutPipe io.ReadCloser
	)
	cmd := exec.CommandContext(context.TODO(), "bash", "-c", cmdStr)
	cmdStdoutPipe, _ = cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout
	go job.SyncLogToDb(cmdStdoutPipe, tid)
	err = cmd.Start()
	if err != nil {
		log.Println("cmd.Start() errror: ", err)
		return
	}
	err = cmd.Wait()
	return
}

func BatchRunSsh(password string, Nodes []module.HostIpPort) (err error){

	for _, h := range  Nodes {
		port := strconv.Itoa(h.Port)
		cmdStr := global.WORKING_DIR + "ssh-public-key.sh" + " '" + password + "' " + port + " " + h.Ip
		if err = RunAndWait(cmdStr); err != nil {
			return
		}
	}
	return
}
