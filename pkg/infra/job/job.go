package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gzlj/hadoop-console/pkg/global"
	"github.com/gzlj/hadoop-console/pkg/infra/db"
	"github.com/gzlj/hadoop-console/pkg/module"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
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

func ConstructFilesAndCmd(cluster string) (filesAndCmds module.TaskFilesAndCmds) {



	filesAndCmds.HostsFile = global.HOSTS_DIR + cluster +  global.HOSTS_FILE_SUFFIX

	filesAndCmds.HdfsJobName = cluster + "-hdfs"
	filesAndCmds.HdfsLogfile = global.LOGS_DIR + filesAndCmds.HdfsJobName +  global.LOG_FILE_SUFFIX
	filesAndCmds.HdfsCmdStr = "ansible-playbook " + global.HADOOP_HDFS_YAML_FILE + " -i " + filesAndCmds.HostsFile

	filesAndCmds.HbaseJobName = cluster + "-hbase"
	filesAndCmds.HbaseLogfile = global.LOGS_DIR + filesAndCmds.HbaseJobName +   global.LOG_FILE_SUFFIX
	filesAndCmds.HbaseCmdStr = "ansible-playbook " + global.HBASE_YAML_FILE + " -i " + filesAndCmds.HostsFile

	filesAndCmds.SparkJobName = cluster + "-spark"
	filesAndCmds.SparkLogfile = global.LOGS_DIR + filesAndCmds.SparkJobName +   global.LOG_FILE_SUFFIX
	filesAndCmds.SparkCmdStr = "ansible-playbook " + global.SPARK_YAML_FILE + " -i " + filesAndCmds.HostsFile

	filesAndCmds.HiveJobName = cluster + "-hive"
	filesAndCmds.HiveLogfile = global.LOGS_DIR + filesAndCmds.HiveJobName +   global.LOG_FILE_SUFFIX
	filesAndCmds.HiveCmdStr = "ansible-playbook " + global.HIVE_YAML_FILE + " -i " + filesAndCmds.HostsFile

	filesAndCmds.SqoopJobName = cluster + "-sqoop"
	filesAndCmds.SqoopLogfile = global.LOGS_DIR + filesAndCmds.SqoopJobName +   global.LOG_FILE_SUFFIX
	filesAndCmds.SqoopCmdStr = "ansible-playbook " + global.SQOOP_YAML_FILE + " -i " + filesAndCmds.HostsFile

	return
}

func GenerateHostFileContent(cluster string, config module.ClusterConf) (lines []string, err error) {


	var (
		bytes []byte
		ansibleConfig module.AnsibleHostsConfig
	)

	ansibleConfig = GetAnsibleHostsConfig(cluster, config)
	bytes, err = json.Marshal(ansibleConfig.AllHostAndips)
	if err != nil {
		log.Println("json Marshal Error happend when generate ansible hosts file:", err)
		return
	}

	var hbaseRegionHostnames, hbaseRegionIps []string
	for _, r := range ansibleConfig.HbaseRegionservers {
		hbaseRegionHostnames = append(hbaseRegionHostnames, r.Hostname)
	}
	for _, r := range ansibleConfig.HbaseRegionservers {
		hbaseRegionIps = append(hbaseRegionIps, r.Ip)
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
		"hbase_regionservers=" + strings.Replace(strings.Trim(fmt.Sprint(hbaseRegionHostnames), "[]"), " ", ",", -1),
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
	lines = append(lines, "[hbase]")

	for _, ip := range hbaseRegionIps {
		for _, node := range ansibleConfig.HbaseMasters {
			if node.Ip == ip {
				ip = ip + " hbase_role_master=true"
				break
			}

		}
		lines = append(lines, ip)
	}
	return
}

func GetAnsibleHostsConfig(cluster string, config module.ClusterConf) (ansibleConfig module.AnsibleHostsConfig){
	var ipMap map[string]module.HostnameIp = make(map[string]module.HostnameIp)
	var hostAndips []module.HostnameIp
	//zookeeper
	for _, n := range config.HdfsConfig.Zookeepers {
		if ansibleConfig.Zk1 == "" {
			ansibleConfig.Zk1 = n.Ip
			continue
		}
		if ansibleConfig.Zk2 == "" {
			ansibleConfig.Zk2 = n.Ip
			continue
		}
		if ansibleConfig.Zk3 == "" {
			ansibleConfig.Zk3 = n.Ip
			continue
		}
	}

	// datanode and namenode
	for _, n := range config.HdfsConfig.DataNodes {
		ansibleConfig.IpForDataNodes = append(ansibleConfig.IpForDataNodes, n.Ip)
		ansibleConfig.HostNameForDataNodes = append(ansibleConfig.HostNameForDataNodes, n.Hostname)
		_, ok := ipMap[n.Hostname]
		if ! ok {
			ipMap[n.Hostname] = n
		}

	}
	for i, n := range config.HdfsConfig.NameNodes {

		if i == 0 {
			ansibleConfig.Nn1 = n.Ip
			ansibleConfig.HostNameForNn1 = n.Hostname
		} else {
			ansibleConfig.Nn2 = n.Ip
			ansibleConfig.HostNameForNn2 = n.Hostname
		}
		_, ok := ipMap[n.Hostname]
		if ! ok {
			ipMap[n.Hostname] = n
		}
	}


	//jn
	for _, n := range config.HdfsConfig.JournalNodes {
		if ansibleConfig.Jn1 == "" {
			ansibleConfig.Jn1 = n.Ip
			ansibleConfig.HostNameForJn1 = n.Hostname
			continue
		}
		if ansibleConfig.Jn2 == "" {
			ansibleConfig.Jn2 = n.Ip
			ansibleConfig.HostNameForJn2 = n.Hostname
			continue
		}
		if ansibleConfig.Jn3 == "" {
			ansibleConfig.Jn3 = n.Ip
			ansibleConfig.HostNameForJn3 = n.Hostname
			continue
		}
		_, ok := ipMap[n.Hostname]
		if ! ok {
			ipMap[n.Hostname] = n
		}
	}

	//rm
	for i, n := range config.HdfsConfig.ResourceManagers {
		if i == 0 {
			ansibleConfig.Rm1 = n.Ip
			ansibleConfig.HostNameForRm1 = n.Hostname
		} else {
			ansibleConfig.Rm2 = n.Ip
			ansibleConfig.HostNameForRm2 = n.Hostname
		}
		_, ok := ipMap[n.Hostname]
		if ! ok {
			ipMap[n.Hostname] = n
		}
	}

	//hbase
	for _, n := range config.HbaseConfig.Masters {
		ansibleConfig.HbaseMasters = append(ansibleConfig.HbaseMasters, n)
		_, ok := ipMap[n.Hostname]
		if ! ok {
			ipMap[n.Hostname] = n
		}
	}
	for _, n := range config.HbaseConfig.RegionServers {
		ansibleConfig.HbaseRegionservers = append(ansibleConfig.HbaseMasters, n)
		_, ok := ipMap[n.Hostname]
		if ! ok {
			ipMap[n.Hostname] = n
		}
	}

	for _, node := range ipMap {
		hostAndips = append(hostAndips, module.HostnameIp{
			Hostname: node.Hostname,
			Ip: node.Ip,
		})
	}


	//ans
	// ibleConfig.IpForDataNodes =
	//ansibleConfig.HbaseMasters = append(ansibleConfig.HbaseMasters



/*
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
			case global.ROLE_HBASE_MASTER:
				ansibleConfig.HbaseMasters = append(ansibleConfig.HbaseMasters, module.HostnameIp{
					Hostname: node.Hostname,
					Ip: node.Ip,
				})
			case global.ROLE_HBASE_REGION_SERVER:
				ansibleConfig.HbaseRegionservers = append(ansibleConfig.HbaseRegionservers, module.HostnameIp{
					Hostname: node.Hostname,
					Ip: node.Ip,
				})

			default:
				log.Println("Unknown Role: ", role)
			}
		}
	}*/

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

func SyncLogToDb(reader io.ReadCloser, tid uint) {
	buf := make([]byte, 4096, 4096)
	for {
		strNum, err := reader.Read(buf)
		if err != nil {
			//读到结尾
			if err == io.EOF || strings.Contains(err.Error(), "file already closed") {
				//err = nil
				break
			}
		}
		//log.Println("SyncLogToDb: ", string(buf[:strNum]))
		//log.Println("SyncLogToDb all: ", string(buf))
		db.AppendSyncLog(tid, buf[:strNum])
		time.Sleep(time.Duration(2) * time.Second)
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

func QueryLogByTaskId(tid uint) (dto db.LogDto, err error) {
	var l db.Log
	db.G_db.First(&l, "tid = ? AND removed = ?", 4, "0")
	if l.ID == 0 {
		return dto, errors.New("Log not found.")
	}
	dto = l.ToDto()
	return
}
