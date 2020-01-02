package util

import (
	"context"
	"fmt"
	"github.com/gzlj/hadoop-console/pkg/global"
	"log"
	"os/exec"
	"strings"
)

func RunSsh(password string, hosts []string) (err error){
	hostsStr := strings.Trim(fmt.Sprint(hosts), "[]")
	log.Println("Run ssh command for hosts:", hostsStr)
	cmdStr := global.WORKING_DIR + "ssh-public-key.sh " + password + " " + hostsStr
	err = RunAndWait(cmdStr)
	return
}

func RunAndWait(cmdStr string) (err error) {
	cmd := exec.CommandContext(context.TODO(), "bash", "-c", cmdStr)
	err = cmd.Run()
	return
}
