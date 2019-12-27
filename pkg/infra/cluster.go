package infra

import (
	"github.com/gzlj/hadoop-console/pkg/module"
	"time"
)

var (
	ClusterRuntimeInfos map[string]module.ClusterRuntimeInfo
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
