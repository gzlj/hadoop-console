package job

import (
	"encoding/json"
	"fmt"
	"github.com/gzlj/hadoop-console/pkg/global"
	"github.com/gzlj/hadoop-console/pkg/infra/db"
	"github.com/gzlj/hadoop-console/pkg/module"
	"log"
	"os"
	"strings"
)

func CreateStatusFile(jobName string) (err error) {
	var (
		statusFile string = global.STATUS_DIR + jobName + global.STATUS_FILE_SUFFIX
		status     module.Status
		bytes      []byte
	)

	f, err := os.OpenFile(statusFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return
	}
	defer f.Close()

	status = module.Status{
		Code:  200,
		Err:   "",
		Phase: "created",
		Job:    jobName,
	}
	if bytes, err = json.Marshal(status); err != nil {
	log.Println("Json Marshal failed when Create job status file.")
		return
	}
	f.WriteString(string(bytes))
	return
}

func ConstructFilesAndCmd(jobName string) (filesAndCmds module.TaskFilesAndCmds) {
	filesAndCmds.HostsFile = global.HOSTS_DIR + jobName +  global.HOSTS_FILE_SUFFIX
	filesAndCmds.Logfile = global.LOGS_DIR + jobName +  global.LOG_FILE_SUFFIX
	filesAndCmds.CoreCmdStr = "ansible-playbook " + global.HADOOP_HDFS_YAML_FILE + " -i " + filesAndCmds.HostsFile
	return
}

func GenerateHostFileContent(config db.ClusterConfig) (lines []string, err error) {
	/*
	[all:vars]
all_hosts="[{"hostname":"nn1","ip":"192.168.25.202"},{"hostname":"nn2","ip":"192.168.25.201"},{"hostname":"dn3","ip":"192.168.25.200"}]"
zookeeper_server_1=192.168.25.202
zookeeper_server_2=192.168.25.201
zookeeper_server_3=192.168.25.200
datanodes=nn1,nn2,dn3
journalnode_1=nn1
journalnode_2=nn2
journalnode_3=dn3
namenode_1=nn1
namenode_2=nn2
resource_manager_1=nn1
resource_manager_2=nn2
password=5743138
cluster=liujun

[nn]
192.168.25.202 nn_role=nn1
192.168.25.201 nn_role=nn2

[jn]
192.168.25.202
192.168.25.201
192.168.25.200

[dn]
192.168.25.202
192.168.25.201
192.168.25.200

[zk]
192.168.25.202 zk_role=zk1
192.168.25.201 zk_role=zk2
192.168.25.200 zk_role=zk3
	 */

	var (
		//nodes []module.NodeRole = config.Nodes
		//nodesStr string
		//hostAndip module.HostnameIp
		//hostAndips []module.HostnameIp
		bytes []byte
		ansibleConfig module.AnsibleHostsConfig

	)
	/*for _, n := range nodes {
		hostAndips = append(hostAndips, module.HostnameIp{
			Hostname: n.Hostname,
			Ip: n.Ip,
		})

	}
	bytes, err = json.Marshal(hostAndips)
	if err != nil {
		return
	}*/
	ansibleConfig = getAnsibleHostsConfig(config)
	bytes, err = json.Marshal(ansibleConfig.AllHostAndips)
	if err != nil {
		log.Println("json Marshal Error happend when generate ansible hosts file:", err)
		return
	}

	lines = []string{
		"[all:vars]",
		"all_hosts=\"" + string(bytes) + "\"",
		"zookeeper_server_1=" + ansibleConfig.Zk1,
		"zookeeper_server_2=" + ansibleConfig.Zk2,
		"zookeeper_server_3=" + ansibleConfig.Zk3,
		"datanodes=" + strings.Replace(strings.Trim(fmt.Sprint(ansibleConfig.HostNameForDataNodes), "[]"), " ", ",", -1),
		"journalnode_1=" + ansibleConfig.HostNameForJn1,
		"journalnode_2" + ansibleConfig.HostNameForJn2,
		"journalnode_3" + ansibleConfig.HostNameForJn3,
		"namenode_1=" + ansibleConfig.HostNameForNn1,
		"namenode_2=" + ansibleConfig.HostNameForNn2,
		"resource_manager_1" + ansibleConfig.HostNameForRm1,
		"resource_manager_2" + ansibleConfig.HostNameForRm2,
		"password=" + ansibleConfig.Password,
		"cluster=" + ansibleConfig.Cluster,
		"[nn]",
		ansibleConfig.Nn1 + " nn_role=nn1",
		ansibleConfig.Nn2 + " nn_role=nn2",

		"[jn]",
		ansibleConfig.Jn1,
		ansibleConfig.Jn2,
		ansibleConfig.Jn3,

		"[zk]",
		ansibleConfig.Zk1 + " zk_role=zk1",
		ansibleConfig.Zk2 + " zk_role=zk2",
		ansibleConfig.Zk3 + " zk_role=zk3",
	}

	lines = append(lines, "[dn]")
	for _, ip := range ansibleConfig.IpForDataNodes {
		lines = append(lines, ip)
	}

	return
}

func getAnsibleHostsConfig(config db.ClusterConfig) (ansibleConfig module.AnsibleHostsConfig){
	var node module.NodeRole
	var role string
	var hostAndips []module.HostnameIp
	/*

	for _, n := range nodes {
		hostAndips = append(hostAndips, module.HostnameIp{
			Hostname: n.Hostname,
			Ip: n.Ip,
		})

	}
	bytes, err = json.Marshal(hostAndips)
	if err != nil {
		return
	}
	 */


	for _, node = range config.Nodes {
		hostAndips = append(hostAndips, module.HostnameIp{
			Hostname: node.Hostname,
			Ip: node.Ip,
		})
		for role = range node.Roles {



			switch role {
			case global.ROLE_DATANODE:
				ansibleConfig.IpForDataNodes = append(ansibleConfig.IpForDataNodes, node.Ip)
				ansibleConfig.HostNameForDataNodes = append(ansibleConfig.HostNameForDataNodes, node.Hostname)
			case global.ROLE_ZOOKEEPER_SERVER_1:
				ansibleConfig.Zk1 = node.Ip

			case global.ROLE_ZOOKEEPER_SERVER_2:
				ansibleConfig.Zk2 = node.Ip
			case global.ROLE_ZOOKEEPER_SERVER_3:
				ansibleConfig.Zk3 = node.Ip

			case global.ROLE_JOURNALNODE_1:
			case global.ROLE_JOURNALNODE_2:
			case global.ROLE_JOURNALNODE_3:

			case global.ROLE_NAMENODE_1:
			case global.ROLE_NAMENODE_2:
			case global.ROLE_RESOURCE_MANAGER_1:
			case global.ROLE_RESOURCE_MANAGER_2:

			default:
				log.Println("Unknown Role: ", role)
			}
		}
	}

	//hostAndips
	ansibleConfig.AllHostAndips = hostAndips
	return
}
