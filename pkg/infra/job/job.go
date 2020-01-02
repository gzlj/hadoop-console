package job

import (
	"encoding/json"
	"fmt"
	"github.com/gzlj/hadoop-console/pkg/global"
	"github.com/gzlj/hadoop-console/pkg/infra/db"
	"github.com/gzlj/hadoop-console/pkg/module"
	"io"
	"io/ioutil"
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

func GenerateHostFileContent(cluster string, config db.ClusterConfig) (lines []string, err error) {
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
	ansibleConfig = getAnsibleHostsConfig(cluster, config)
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
		"journalnode_2=" + ansibleConfig.HostNameForJn2,
		"journalnode_3=" + ansibleConfig.HostNameForJn3,
		"namenode_1=" + ansibleConfig.HostNameForNn1,
		"namenode_2=" + ansibleConfig.HostNameForNn2,
		"resource_manager_1=" + ansibleConfig.HostNameForRm1,
		"resource_manager_2=" + ansibleConfig.HostNameForRm2,
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

func getAnsibleHostsConfig(cluster string, config db.ClusterConfig) (ansibleConfig module.AnsibleHostsConfig){
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
		for _, role = range node.Roles {



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
				ansibleConfig.Jn1 = node.Ip
				ansibleConfig.HostNameForJn1 = node.Hostname
			case global.ROLE_JOURNALNODE_2:
				ansibleConfig.Jn2 = node.Ip
				ansibleConfig.HostNameForJn2 = node.Hostname
			case global.ROLE_JOURNALNODE_3:
				ansibleConfig.Jn3 = node.Ip
				ansibleConfig.HostNameForJn3 = node.Hostname
			case global.ROLE_NAMENODE_1:
				ansibleConfig.Nn1 = node.Ip
				ansibleConfig.HostNameForNn1 = node.Hostname
			case global.ROLE_NAMENODE_2:
				ansibleConfig.Nn2 = node.Ip
				ansibleConfig.HostNameForNn2 = node.Hostname
			case global.ROLE_RESOURCE_MANAGER_1:
				ansibleConfig.Rm1 = node.Ip
				ansibleConfig.HostNameForRm1 = node.Hostname
			case global.ROLE_RESOURCE_MANAGER_2:
				ansibleConfig.Rm2 = node.Ip
				ansibleConfig.HostNameForRm2 = node.Hostname
			default:
				log.Println("Unknown Role: ", role)
			}
		}
	}

	//hostAndips
	ansibleConfig.AllHostAndips = hostAndips
	ansibleConfig.Password = config.Password
	ansibleConfig.Cluster = cluster
	return
}

/*
func UpdateStatusFile(status common.Status) (err error) {
	var (
		statusFile string = common.STATUS_DIR + "/" + status.Id + common.STATUS_FILE_SUFFIX
		bytes      []byte
	)

	f, err := os.OpenFile(statusFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return
	}
	defer f.Close()

	if bytes, err = json.Marshal(status); err != nil {
		fmt.Println("upload job status fail.")
		return
	}
	f.WriteString(string(bytes))
	return
}
 */
func UpdateStatusFile(status module.Status) (err error) {
	var (
		statusFile string = global.STATUS_DIR + "/" + status.Job + global.STATUS_FILE_SUFFIX
		bytes      []byte
	)
	f, err := os.OpenFile(statusFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return
	}
	defer f.Close()

	if bytes, err = json.Marshal(status); err != nil {
		log.Println("update job status fail for job:", status.Job)
		return
	}
	f.WriteString(string(bytes))
	return
}

func ConstructFinalStatus(job string, err error) (status module.Status) {
	if err != nil {
		/*
			Code:  500,
			Err:   "Some error happened.Please check log file.",
			Phase: "failed",
			Id:    jobId,
		 */
		status.Job = job
		status.Code = 500
		status.Err = "Some error happened.Please check log file: " + err.Error()
		status.Phase = "failed"
	} else {
	/*
		Code:  200,
			Err:   "",
			Phase: "exited",
			Id:    jobId,
	 */
		status.Job = job
		status.Code = 200
		status.Phase = "exited"
	}
	return
}

func SyncLog(reader io.ReadCloser, file string, append bool) {
	//fmt.Println("start syncLog()")
	/*	scanner := bufio.NewScanner(reader)
		for scanner.Scan() { // 命令在执行的过程中, 实时地获取其输出
			data, err := simplifiedchinese.GB18030.NewDecoder().Bytes(scanner.Bytes()) // 防止乱码
			if err != nil {
				fmt.Println("transfer error with bytes:", scanner.Bytes())
				continue
			}

			fmt.Printf("%s\n", string(data))
		}*/
	var (
		f *os.File
	)
	if append {
		f, _ = os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	} else {
		f, _ = os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	}
	defer f.Close()
	buf := make([]byte, 1024, 1024)
	for {
		strNum, err := reader.Read(buf)

		if err != nil {
			//读到结尾
			if err == io.EOF || strings.Contains(err.Error(), "file already closed") {
				//err = nil
				break
			}
		}
		outputByte := buf[:strNum]
		f.WriteString(string(outputByte))
	}
}

func QueryJobLogByName(jobName string) (result string) {
	var (
		bytes []byte
		err   error
	)

	if bytes, err = ioutil.ReadFile(global.LOGS_DIR + jobName + global.LOG_FILE_SUFFIX); err != nil {
		result = err.Error()
	}
	result = string(bytes)
	return
}
