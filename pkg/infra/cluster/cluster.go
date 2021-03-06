package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gzlj/hadoop-console/pkg/global"
	"github.com/gzlj/hadoop-console/pkg/infra/db"
	"github.com/gzlj/hadoop-console/pkg/infra/job"
	"github.com/gzlj/hadoop-console/pkg/infra/util"
	"github.com/gzlj/hadoop-console/pkg/module"
	"io"
	"log"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	ClusterRuntimeInfos map[string]module.ClusterRuntimeInfo = make(map[string]module.ClusterRuntimeInfo)
	HbChan chan module.NodeHeartBeat = make(chan module.NodeHeartBeat, 64)
)




// sync cluster status from memory to db periodly
func SyncClusterStatusToDbLoop() {

	timer1 := time.NewTimer(30 * time.Second)
	for {
		select {
		case <-timer1.C:
			syncClusterStatusToDb()
			timer1.Reset(30 * time.Second)
		}
	}

}

func syncClusterStatusToDb() {
	var (
		info module.ClusterRuntimeInfo
		cluster string
	)

	for _, info = range ClusterRuntimeInfos {
		var(
			nss      []*module.NodeStatus
			nodeInfo module.ClusterNodeInfo
			err error
		)
		cluster = info.ClusterName
		for _, nodeInfo = range info.Nodes {
			nss = append(nss, &module.NodeStatus{
				Hostname:  nodeInfo.Hostname,
				State:     nodeInfo.State,
				LastKnown: module.Time(nodeInfo.LastKnown),
			})
		}
		err = db.UpdateStatusByName(cluster, nss)
		if err != nil {
			log.Println("Failed to sync cluster status to db: ", err)
		}
	}
}


func ListClusters()(dtos []*db.ClusterDto, err error){

	var (
		clusters []*db.Cluster
		cluster *db.Cluster
		dto *db.ClusterDto
	)
	clusters = db.ListClusters()
	for _, cluster = range clusters {
		dto, err = cluster.ToDto()
		if err != nil {
			continue
		}
		dtos = append(dtos, dto)
	}
	sort.Sort(db.ClusterList(dtos))

	return
}

func QueryClusterById(id uint)(dto *db.ClusterDto, err error){
	var (
		c *db.Cluster
	)
	c, err = db.QueryClusterById(int(id))
	if err != nil {
		return
	}
	dto, err  = c.ToDto()
	//log.Printf("dto:", *dto)
	return
}




func ListTasksByCluster(cid uint) (dtos []*db.TaskDto, err error) {
	var (
		tasks []*db.Task
		t *db.Task
	dto *db.TaskDto
	)

	//QueryLatestTasksByClusterIdLimit
	tasks = db.QueryLatestTasksByClusterIdLimit(cid, 3)
	//tasks = db.QueryTasksByClusterId(cid)
	for _,t = range tasks {
		dto, err = t.ToDto()
		if err != nil {
			continue
		}
		dtos = append(dtos, dto)
	}
	//sort.Reverse(db.TaskList(dtos))
	//sort.Sort(db.TaskList(dtos))
	return
}

func ListLatestTasksByClusterLimit(cid, limit uint) (dtos []*db.TaskDto, err error) {
	var (
		tasks []*db.Task
		t *db.Task
		dto *db.TaskDto
	)

	//QueryLatestTasksByClusterIdLimit
	tasks = db.QueryLatestTasksByClusterIdLimit(cid, limit)
	//tasks = db.QueryTasksByClusterId(cid)
	for _,t = range tasks {
		dto, err = t.ToDto()
		if err != nil {
			continue
		}
		dtos = append(dtos, dto)
	}
	//sort.Reverse(db.TaskList(dtos))
	//sort.Sort(db.TaskList(dtos))
	return
}

func ListLatestTasksByServiceIdLimit(sid, limit uint) (dtos []*db.TaskDto, err error) {
	var (
		tasks []*db.Task
		t *db.Task
		dto *db.TaskDto
	)

	//QueryLatestTasksByClusterIdLimit
	log.Println("sid:", sid)
	tasks = db.QueryLatestTasksByServiceIdLimit(sid, limit)
	log.Println("tasks:", tasks)
	//tasks = db.QueryLatestTasksByClusterIdLimit(cid, limit)
	//tasks = db.QueryTasksByClusterId(cid)
	for _,t = range tasks {
		dto, err = t.ToDto()
		if err != nil {
			continue
		}
		dtos = append(dtos, dto)
	}
	//sort.Reverse(db.TaskList(dtos))
	//sort.Sort(db.TaskList(dtos))
	return
}

/*
func SyncClusterFromDb() (err error){
	//ClusterRuntimeInfos = make(map[string]module.ClusterRuntimeInfo)
	var (
		//cluster *db.Cluster
		//dtos []*db.ClusterDto
		clusters []*db.Cluster
		config module.ClusterConfig
		nodes []module.NodeRole
		infos []module.ClusterNodeInfo
	)
	//dtos, err = ListClusters()
	clusters = db.ListClusters()

	for _, c := range clusters {
		err := json.Unmarshal([]byte(c.Config), &config)
		if err != nil {
			log.Print("Failed to get cluster config when json Unmarshal: ", err.Error())
			continue
		}
		nodes = config.Nodes
		for _, nr := range nodes {
			infos = append(infos,module.ClusterNodeInfo{
				NodeRole: nr,
				State: "Unknown",
			})
		}
		var nss []*module.NodeStatus
		err = json.Unmarshal([]byte(c.Status), &nss)
		if err == nil {
			for _, ns := range nss {
				for i, _ := range infos {
					if infos[i].Hostname == ns.Hostname {
						//infos[i].State = ns.State
						infos[i].LastKnown = ns.LastKnown
					}
				}
			}
		} else {
			log.Println("Failed to Unmarshal cluster status when sync from db: ", err)
		}
		ClusterRuntimeInfos[c.Name] = module.ClusterRuntimeInfo{
			ClusterName: c.Name,
			Nodes: infos,
		}
	}
	return
}*/

func SyncFromHeartBeat() {
	for hb := range HbChan {
		syncFromHeartBeat(hb)
	}
}

func syncFromHeartBeat(hb module.NodeHeartBeat) {

	log.Println("accept hb: ----->", hb.Hostname)
	log.Println("accept hb: ----->", hb.RunningComponents)
	hb.RunningComponents = util.RemoveDuplicate(hb.RunningComponents)

	clusterInfo, exists := ClusterRuntimeInfos[hb.Cluster]
	if ! exists {
		var nodes []module.ClusterNodeInfo
		n := module.ClusterNodeInfo{
			LastKnown : module.Time(time.Now()),
			RunningComponents: hb.RunningComponents,
			State: "Ready",
		}
		n.Hostname = hb.Hostname
		nodes = append(nodes, n)
		clusterInfo = module.ClusterRuntimeInfo{
			ClusterName: hb.Cluster,
			Nodes: nodes,
		}
		ClusterRuntimeInfos[hb.Cluster] = clusterInfo
		log.Println("insert into memory:", ClusterRuntimeInfos[hb.Cluster])
		//log.Println("Cluster not exists: ", hb.Cluster)
		return
	}

	found := false
	nodes := clusterInfo.Nodes
	for i, _ := range nodes {
		//log.Println("already node: ", nodes[i].Hostname)
		//log.Println("nodes[i].Hostname: ", nodes[i].Hostname)
		//log.Println("hb.Hostname: ", hb.Hostname)
		if nodes[i].Hostname == hb.Hostname {
			//log.Println("hb.Hostname:  ->", hb.Hostname)
			//log.Println("nodes[i].Hostname == hb.Hostname:  ->", true)
			//log.Println("hb.RunningComponents", hb.RunningComponents)
			log.Println("hb.RunningComponents: ", hb.RunningComponents)
			nodes[i].RunningComponents = hb.RunningComponents
			nodes[i].LastKnown = module.Time(time.Now())
			nodes[i].State = "Ready"
			found = true
			break
		}
	}
	if !found {
		//log.Println("Not found .insert into memory:", hb.Hostname)
		log.Println("Not found .hb.RunningComponents: ", hb.RunningComponents)
		n := module.ClusterNodeInfo{
			LastKnown : module.Time(time.Now()),
			RunningComponents: hb.RunningComponents,
			State: "Ready",
		}
		n.Hostname = hb.Hostname
		clusterInfo.Nodes = append(clusterInfo.Nodes, n)
		ClusterRuntimeInfos[hb.Cluster] = clusterInfo
	}

	//log.Println("clusterInfo.Nodes: ",clusterInfo.Nodes)
	//log.Println("clusterInfo.Nodes for : ",hb.Cluster)
	log.Println("clusterInfo.Nodes: ",ClusterRuntimeInfos[hb.Cluster])
}

func CalculateNodeStatus(){
	var (
		cluster string
		info module.ClusterRuntimeInfo
		nodes       []module.ClusterNodeInfo
	)

	for cluster, info = range ClusterRuntimeInfos {
		nodes = info.Nodes
		var findUnknownNode = false
		for i := range nodes {
			//log.Println("tim gap: ", time.Now().Sub(time.Time(nodes[i].LastKnown)).Seconds())
			if time.Now().Sub(time.Time(nodes[i].LastKnown)).Seconds() >120 {
				log.Println(nodes[i].LastKnown)
				log.Println("Find unknown node: ", nodes[i].Hostname)
				nodes[i].State = "Unknown"
				findUnknownNode = true
			}
		}
		if findUnknownNode {
			ClusterRuntimeInfos[cluster] = info
		}
	}
}

func CalculateNodeStatusLoop() {
	timer1 := time.NewTimer(15 * time.Second)
	for {
		select {
		case <-timer1.C:
			CalculateNodeStatus()
			timer1.Reset(15 * time.Second)
		}
	}
}

func addClusterInitTask(clusterId uint) (tid uint, err error) {
	var (
		ss []*db.Service
		sid uint
	)
	ss ,err = db.GetServiceByNameAndClusterId("HDFS", clusterId)
	if err != nil {
		return
	}
	if len(ss) == 0 {
		log.Println("Failed to get services from cluster.")
	}

	if len(ss) > 0 {
		//s := ss[0]
		sid = ss[0].ID

		task := db.Task{
			Cid: clusterId,
			Sid: sid,
			Name: "Bootstrap HDFS",
			Status: "Running",
			Message: "",
		}
		err = db.G_db.Create(&task).Error
		if err != nil {
			log.Println("Failed to instert task record to tasks table:",err)
		}
		if err == nil {
			tid = task.ID
		}
	}
	return
}

func StartInitHdfs(id int, cluster string, config module.ClusterConf) {
	var (
		//config db.ClusterConfig = dto.Config
		jobName string
		filesAndCmds module.TaskFilesAndCmds
		err error
		lines []string
		f *os.File
		//allHosts []string
		//n module.NodeRole
		status module.Status
		cancelCtx, _ = context.WithCancel(context.TODO())
		cmdStdoutPipe io.ReadCloser
		//cmdStderrPipe io.ReadCloser
		cmd           *exec.Cmd
		tid uint
	    ss []*db.Service

		hbaseSid uint
		//sparkSid uint
		//sqoopSid uint
		servicesCount int
	)
	jobName = cluster +"-hdfs"

	//hdfs task  is created
	global.G_JobExcutingInfo[jobName]="created"
	defer delete(global.G_JobExcutingInfo, jobName)
	// insert a record to tasks table
	// db.UpdateTaskStatusByErr(tid, err)
	tid, err = addClusterInitTask(uint(id))
	if err != nil {
		log.Println("Failed to insert HDFS task to db for cluster:",cluster)
		goto FINISH
	}
	ss, err = GetServicesByCluster(uint(id))
	if err != nil {
		log.Println("Faild to get services for cluster:",cluster)
		goto FINISH
	}
	servicesCount = len(ss)

	log.Println("Job " + jobName + " is initializing")
	status = module.Status{
		Code:  200,
		Job:    jobName,
		Phase: "created",
	}
	job.UpdateStatusFile(status)

	job.CreateStatusFile(jobName)
	filesAndCmds = job.ConstructFilesAndCmd(cluster)
	// config --> lines --> target-hosts/{jobname}.hosts
	f, err = os.OpenFile(filesAndCmds.HostsFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	defer f.Close()
	if err != nil {
		log.Println(err)
		return
	}
	lines, err = job.GenerateHostFileContent(cluster, config)
	if err != nil {
		log.Println("GenerateHostFileContent failed.")
		return
	}
	f.WriteString(strings.Join(lines, "\r\n"))


	// ssh
/*	for _, n = range config.Nodes {
		allHosts = append(allHosts, n.Ip)
	}
	err = util.RunSsh(config.Password, allHosts)
	if err != nil {

		log.Println("Failed to sent ssh public key to target host:", err)
		goto FINISH
	}*/

	//hdfs task  is starting to run
	global.G_JobExcutingInfo[jobName]="Running"
	cmd = exec.CommandContext(cancelCtx, "bash", "-c", filesAndCmds.HdfsCmdStr)
	cmdStdoutPipe, _ = cmd.StdoutPipe()
	//cmdStderrPipe, _ = cmd.StderrPipe()
	//go job.SyncLog(cmdStdoutPipe, filesAndCmds.HdfsLogfile, false)
	//go job.SyncLog(cmdStderrPipe, filesAndCmds.HdfsLogfile, false)
	go job.SyncLogToDb(cmdStdoutPipe, tid)
	//go job.SyncLogToDb(cmdStderrPipe)
	log.Println("Job " + jobName + " is Running")
	status = module.Status{
		Code:  200,
		Job:    jobName,
		Phase: "Running",
	}
	job.UpdateStatusFile(status)
	err = cmd.Start()
	if err != nil {
		goto FINISH
	}
	err = cmd.Wait()

	if servicesCount == 0 {
		goto FINISH
	}

	// cluster --> get []service
	for _, s:= range ss {

		switch s.Name {
		case "HBASE":
			hbaseSid = s.ID
		case "SPARK":
			//hbaseSid = s.ID
		case "SQOOP":
			//hbaseSid = s.ID
		}
	}

	// hbase spark and so on
	//waitgroup
	if hbaseSid > 0 {
		go initComponent(id, int(hbaseSid), filesAndCmds.HbaseJobName, filesAndCmds.HbaseCmdStr, filesAndCmds.HbaseLogfile)
	}

FINISH:
	log.Println("Job is completely finished:", jobName)
	status = job.ConstructFinalStatus(jobName, err)
	job.UpdateStatusFile(status)
	db.UpdateTaskStatusByErr(tid, err)

}

/*func initHbase(dto db.ClusterDto, filesAndCmds module.TaskFilesAndCmds) {
var (
	cmdStdoutPipe io.ReadCloser
	cmdStderrPipe io.ReadCloser
	cmd           *exec.Cmd
	jobName string = dto.Name+"-hbase"
	status module.Status
	err error

	logFile string
	)
	global.G_JobExcutingInfo[dto.Name+"-hbase"]="created"

	cmd = exec.CommandContext(context.TODO(), "bash", "-c", filesAndCmds.HbaseLogfile)
	cmdStdoutPipe, _ = cmd.StdoutPipe()
	cmdStderrPipe, _ = cmd.StderrPipe()
	go job.SyncLog(cmdStdoutPipe, filesAndCmds.HdfsLogfile, false)
	go job.SyncLog(cmdStderrPipe, filesAndCmds.HdfsLogfile, false)
	log.Println("Job " + jobName + " is Running")
	status = module.Status{
		Code:  200,
		Job:    jobName,
		Phase: "Running",
	}
	job.UpdateStatusFile(status)
	err = cmd.Start()
	if err != nil {
		goto FINISH
	}
	err = cmd.Wait()

FINISH:
	status = job.ConstructFinalStatus(jobName, err)
	job.UpdateStatusFile(status)
	log.Println("Job is completely finished:", jobName)
}*/

func initComponent(cid int,sid int, jobName, cmdStr, logFile string) {
	var (
		cmdStdoutPipe io.ReadCloser
		//cmdStderrPipe io.ReadCloser
		cmd           *exec.Cmd
		status module.Status
		err error
		tid uint
	)
	global.G_JobExcutingInfo[jobName]="created"
	defer delete(global.G_JobExcutingInfo, jobName)
	/*
	global.G_JobExcutingInfo[jobName]="created"
	defer delete(global.G_JobExcutingInfo, jobName)
	// db.UpdateTaskStatusByErr(tid, err)
	tid, err = addClusterInitTask(uint(id))
	 */
	// add task record to db
	tid, err = AddTaskToDb(uint(cid), uint(sid) , jobName, "Running", "")
	if err != nil {
		log.Println("Failed to create task recored in db:", err)
		goto FINISH
	}

	cmd = exec.CommandContext(context.TODO(), "bash", "-c", cmdStr)
	cmdStdoutPipe, _ = cmd.StdoutPipe()
	//cmdStderrPipe, _ = cmd.StderrPipe()
	//go job.SyncLog(cmdStdoutPipe, logFile, false)
	//go job.SyncLog(cmdStderrPipe, logFile, false)
	/*

	cmdStdoutPipe, _ = cmd.StdoutPipe()
	//cmdStderrPipe, _ = cmd.StderrPipe()
	//go job.SyncLog(cmdStdoutPipe, filesAndCmds.HdfsLogfile, false)
	//go job.SyncLog(cmdStderrPipe, filesAndCmds.HdfsLogfile, false)
	go job.SyncLogToDb(cmdStdoutPipe, tid)
	 */
	//go job.SyncLogToDb(cmdStdoutPipe, tid)

	log.Println("Job " + jobName + " is Running")
	status = module.Status{
		Code:  200,
		Job:    jobName,
		Phase: "Running",
	}
	job.UpdateStatusFile(status)
	err = cmd.Start()
	if err != nil {
		goto FINISH
	}
	job.SyncLogToDb(cmdStdoutPipe, tid)
	err = cmd.Wait()

FINISH:
	status = job.ConstructFinalStatus(jobName, err)
	job.UpdateStatusFile(status)
	db.UpdateTaskStatusByErr(tid, err)
	log.Println("Job is completely finished:", jobName)
}


func AddTaskToDb(clusterId, servcieId uint, taskName, status, message string) (tid uint, err error) {

	// check the same task in the same cluster

	var (
		count  int
	)
	db.G_db.Model(db.Task{}).Where("cid = ? AND name = ? AND removed = ?", clusterId, taskName, "0").Not("status", []string{"Exited", "Failed"}).Count(&count)

	if count > 0  {
		err = errors.New("Running Task already exists: "+  taskName)
		return
	}

	task := db.Task{
		Cid: clusterId,
		Sid: servcieId,
		Name: taskName,
		Status: status,
		Message: message,
	}
	err = db.G_db.Create(&task).Error
	if err != nil {
		log.Println("Failed to instert task record to tasks table:",err)
	} else {
		tid = task.ID
	}
	return
}

/*


func addClusterInitTask(clusterId uint) (tid uint, err error) {
	var (
		ss []*db.Service
		sid uint
	)
	ss ,err = db.GetServiceByNameAndClusterId("HDFS", clusterId)
	if err != nil {
		return
	}
	if len(ss) == 0 {
		log.Println("Failed to get services from cluster.")
	}

	if len(ss) > 0 {
		//s := ss[0]
		sid = ss[0].ID

		task := db.Task{
			Cid: clusterId,
			Sid: sid,
			Name: "Bootstrap HDFS",
			Status: "Running",
			Message: "",
		}
		err = db.G_db.Create(&task).Error
		if err != nil {
			log.Println("Failed to instert task record to tasks table:",err)
		}
		if err == nil {
			tid = task.ID
		}
	}
	return
}
 */
func GetServicesByCluster(cid uint) (ss []*db.Service, err error) {
	ss, err = db.GetServiceByClusterId(cid)
	return
}

/*
for _, s = range ss {
		if err = tx.Create(s).Error; err != nil {
			tx.Rollback()
			break
		}
	}

 */

func AddService(config module.ClusteredServiceConfig) (s *db.Service, err error) {
	var (
		bytes []byte
		ss []*db.Service
		//s db.Service
	)

	if config.Cid == 0 {

	}

	//GetServiceByNameAndClusterId
	ss, err = db.GetServiceByClusterId(uint(config.Cid))
	if err != nil {
		return
	}

	for _, tmp := range ss {
		if tmp.Name == config.Name {
			err = errors.New("Service Already exists: " + config.Name)
			return
		}
	}


	bytes, err = json.Marshal(config.RoleToHosts)
	if err != nil {
		return
	}

	s = &db.Service{
		Cid: uint(config.Cid),
		Name: config.Name,
		Config: string(bytes),
	}
	err = db.AddService(s)

	return
}

func QueryServiceById(id uint) (s *db.Service, err error) {
	var service db.Service
	if err := db.G_db.Find(&service, id).Error; err != nil {
		return nil, err
	}
	return &service, nil
}


/**

 */
func RunSshByCluster(c db.Cluster) (err error) {

	if c.ID == 0 ||c.Name == "" {
		err = errors.New("Please specify a valied hadoop cluster.")
		return
	}

	var (
		lines []string
		f     *os.File
		fileName = global.HOSTS_DIR + c.Name + "-ssh" + global.HOSTS_FILE_SUFFIX
		allHosts []string
		allHostsAndIps string
		config   module.ClusterConf
		tid uint
		bytes []byte
		tStatus string
	)

	//json
	err = json.Unmarshal([]byte(c.Config), &config)
	if err != nil {
		log.Println("json Unmarshal failed for config of cluster:", c.Name)
		return
	}

	// insert ssh task record for cluster
	tid, err = AddTaskToDb(c.ID, 0, "hadoop-ssh", "Running", "")
	if err != nil {
		log.Println("err: ", err)
		log.Println("Inserting ssh task record failed for cluster:", c.Name)
		return
	}
	defer func() {
		if err == nil {
			tStatus = "Exited"
			db.UpdateTaskStatus(tid, tStatus, fmt.Sprint(allHosts))
		} else {
			tStatus = "Failed"
			if len(allHosts) == 0 {
				db.UpdateTaskStatus(tid, tStatus, err.Error())
				return
			}
			db.UpdateTaskStatus(tid, tStatus, fmt.Sprint(allHosts))
		}
	}()

	// ansible host ssh
	err = util.BatchRunSsh(config.Password, config.Nodes)
	if err != nil {
		log.Println("Failed to sent ssh public key to target host:", err)
		return
	}

	bytes, err = json.Marshal(config.Nodes)
	if err != nil {
		log.Println("Failed to generate ansible hosts file for cluster:", c.Name)
		return
	}
	for _, n := range config.Nodes {
		allHosts = append(allHosts, n.Ip)
	}

	// ansible hosts file for ssh play book
	allHostsAndIps = string(bytes)
	lines = []string{
		"[all:vars]",
		"all_hosts=\"" + allHostsAndIps + "\"",
		"password=" + config.Password,
	}
	lines = append(lines, "[ssh]")
	for _, n := range config.Nodes {
		port := strconv.Itoa(n.Port)
		if n.Port == 0 {
			port = "22"
		}
		str := n.Ip + " ansible_ssh_port=" + port + " hostname=" + n.Hostname
		lines = append(lines, str)
	}
	f, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	defer f.Close()
	if err != nil {
		log.Println("Cannot open file:", err)
		return
	}
	f.WriteString(strings.Join(lines, "\r\n"))

	// run ansible play book
	cmdStr := "ansible-playbook " + global.SSH_YAML_FILE + " -i " + fileName
	err = util.RunAndLog(tid, cmdStr)
	return
}

func GenerateHostsFileByCluster(sid uint) (lines []string, c *db.Cluster,err error){
	var (
		cid uint
		ss []*db.Service
		s *db.Service
		//c *db.Cluster
		config   module.ClusterConf
		bytes []byte
		//allHostsAndIps string
		
		dataNodeHostnames    []string
		dataNodeIps    []string
		//hdfsConfigStr        string
	    //hbaseRegionHostnames []string


		//hbaseRegionIps []string
		//hbaseMasterIps []string
		//zkIps []string

		journalNodeHostnames []string
		nameNodeHostnames []string
		nameNodeIps []string

	    hbaseRegionHostnames []string
		hbaseRegionIps []string
		hbaseMasterIps []string


	ansibleConfig module.AnsibleHostsConfig
	)

	s , err = QueryServiceById(sid)
	if err != nil {
		return
	}
	cid = s.Cid
	// cid --> cluster ---> []service ---> /etc/ansible/target-hosts/{cluster}.hosts
	c, err = db.QueryClusterById(int(cid))
	if err != nil {
		return
	}



	ss, err = GetServicesByCluster(cid)
	if err != nil {
		return
	}
	for _, tmp := range ss {
		log.Println("tmp name:", tmp.Name)
		switch tmp.Name {
		case "ZOOKEEPER":
			var roleToHosts  = make(map[string][]module.HostnameIp)
			//var roleToHosts2  = make(map[string][]interface{})
			err = json.Unmarshal([]byte(tmp.Config), &roleToHosts)
			if err != nil {
				return
			}
			if zks, ok :=roleToHosts["zookeepers"]; ok {
				//log.Println("has zookeepers----------")
				for _, zk := range zks {
					if ansibleConfig.Zk1 == "" {
						ansibleConfig.Zk1 = zk.Ip
						continue
					}
					if ansibleConfig.Zk2 == "" {
						ansibleConfig.Zk2 = zk.Ip
						continue
					}
					if ansibleConfig.Zk3 == "" {
						ansibleConfig.Zk3 = zk.Ip
						continue
					}
				}
			}
		case "HDFS":
			var roleToHosts  = make(map[string][]module.HostnameIp)
			//var roleToHosts2  = make(map[string][]interface{})
			err = json.Unmarshal([]byte(tmp.Config), &roleToHosts)
			if err != nil {
				return
			}
			//log.Println("roleToHosts in zookeeper:", roleToHosts)


			if rss, ok :=roleToHosts["journalNodes"]; ok {
				for _, tmp := range rss {
					if ansibleConfig.Jn1 == "" {
						ansibleConfig.Jn1 = tmp.Ip
						ansibleConfig.HostNameForJn1 = tmp.Hostname
						continue
					}
					if ansibleConfig.Jn2 == "" {
						ansibleConfig.Jn2 = tmp.Ip
						ansibleConfig.HostNameForJn2 = tmp.Hostname
						continue
					}
					if ansibleConfig.Jn3 == "" {
						ansibleConfig.Jn3 = tmp.Ip
						ansibleConfig.HostNameForJn3 = tmp.Hostname
						continue
					}
					journalNodeHostnames = append(journalNodeHostnames, tmp.Hostname)

				}
			}
			//nameNodes
			if rss, ok :=roleToHosts["nameNodes"]; ok {
				for i, tmp := range rss {
					if i == 0 {
						ansibleConfig.Nn1 = tmp.Ip
						ansibleConfig.HostNameForNn1 = tmp.Hostname
					} else {
						ansibleConfig.Nn2 = tmp.Ip
						ansibleConfig.HostNameForNn2 = tmp.Hostname
					}

					nameNodeHostnames = append(nameNodeHostnames, tmp.Hostname)
					nameNodeIps = append(nameNodeIps, tmp.Hostname)
				}
			}
			if rss, ok :=roleToHosts["dateNodes"]; ok {
				for _, tmp := range rss {

					ansibleConfig.IpForDataNodes = append(ansibleConfig.IpForDataNodes, tmp.Ip)
					ansibleConfig.HostNameForDataNodes = append(ansibleConfig.HostNameForDataNodes, tmp.Hostname)

					dataNodeHostnames = append(dataNodeHostnames, tmp.Hostname)
					dataNodeIps = append(dataNodeIps, tmp.Ip)
				}
			}
		/*case "YARN":
			var roleToHosts  = make(map[string][]module.HostnameIp)
			//var roleToHosts2  = make(map[string][]interface{})
			err = json.Unmarshal([]byte(tmp.Config), &roleToHosts)
			if err != nil {
				return
			}
			if rms, ok :=roleToHosts["resourceManagers"]; ok {
				for i, rm := range rms {
					if i == 0 {
						ansibleConfig.Rm1 = rm.Ip
						ansibleConfig.HostNameForRm1 = rm.Hostname
					} else {
						ansibleConfig.Rm2 = rm.Ip
						ansibleConfig.HostNameForRm2 = rm.Hostname
					}
				}
			}
			//nodeManagers
			if nms, ok :=roleToHosts["nodeManagers"]; ok {
				for _, nm := range nms {
					ansibleConfig.IpForNodeManagers = append(ansibleConfig.IpForNodeManagers, nm.Ip)
				}
			}*/
		}
	}





	log.Println("s name: ", s.Name)
	switch s.Name {
	case "HBASE":
		var roleToHosts  = make(map[string][]module.HostnameIp)
		err = json.Unmarshal([]byte(s.Config), &roleToHosts)
		if err != nil {
			return
		}
		log.Println("hbase config:", roleToHosts)
		if rss, ok :=roleToHosts["regionServers"]; ok {
			for _, rs := range rss {

				ansibleConfig.HbaseRegionservers = append(ansibleConfig.HbaseMasters, rs)

				//hbaseRegionHostnames = append(hbaseRegionHostnames, rs.Hostname)
				//hbaseRegionIps = append(hbaseRegionIps, rs.Ip)
			}
		}
		if masters, ok :=roleToHosts["masters"]; ok {
			for _, m := range masters {
				ansibleConfig.HbaseMasters = append(ansibleConfig.HbaseMasters, m)
				//hbaseMasterIps = append(hbaseMasterIps, m.Ip)
			}
		}
	case "YARN":
		var roleToHosts  = make(map[string][]module.HostnameIp)
		err = json.Unmarshal([]byte(s.Config), &roleToHosts)
		if err != nil {
			return
		}
		if rms, ok :=roleToHosts["resourceManagers"]; ok {
			for i, rm := range rms {
				if i == 0 {
					ansibleConfig.Rm1 = rm.Ip
					ansibleConfig.HostNameForRm1 = rm.Hostname
					ansibleConfig.IpForRm1 = rm.Ip
				} else {
					ansibleConfig.Rm2 = rm.Ip
					ansibleConfig.HostNameForRm2 = rm.Hostname
					ansibleConfig.IpForRm2 = rm.Ip
				}
			}
		}
		if nms, ok :=roleToHosts["nodeManagers"]; ok {
			for _, nm := range nms {
				ansibleConfig.IpForNodeManagers = append(ansibleConfig.IpForNodeManagers, nm.Ip)
			}
		}
	}





	err = json.Unmarshal([]byte(c.Config), &config)
	if err != nil {
		log.Println("json Unmarshal failed for config of cluster:", c.Name)
		return
	}

	bytes, err = json.Marshal(config.Nodes)
	if err != nil {
		log.Println("Failed to generate ansible hosts file for cluster:", c.Name)
		return
	}

	//ansibleConfig.IpForDataNodes = append(ansibleConfig.IpForDataNodes, n.Ip)

	var allHostAndips []module.HostnameIp
	for _, tmp :=range config.Nodes {
		allHostAndips = append(allHostAndips, module.HostnameIp{
			Ip: tmp.Ip,
			Hostname: tmp.Hostname,
		})
	}


	ansibleConfig.AllHostAndips = allHostAndips
	ansibleConfig.Password = config.Password
	ansibleConfig.Cluster = c.Name

	for _, r := range ansibleConfig.HbaseRegionservers {
		hbaseRegionHostnames = append(hbaseRegionHostnames, r.Hostname)
	}
	for _, r := range ansibleConfig.HbaseRegionservers {
		hbaseRegionIps = append(hbaseRegionIps, r.Ip)
	}

	for _, m := range ansibleConfig.HbaseMasters {
		hbaseMasterIps = append(hbaseMasterIps, m.Ip)
	}



	// ansible hosts file for ssh play book
	//allHostsAndIps = string(bytes)


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
	lines = append(lines, "[hbase-master]")

	for _, ip := range hbaseMasterIps {
		/*for _, node := range ansibleConfig.HbaseMasters {
			if node.Ip == ip {
				ip = ip + " hbase_role_master=true"
				break
			}

		}*/
		lines = append(lines, ip)
	}

	lines = append(lines, "[hbase-regionserver]")
	for _, ip := range hbaseRegionIps {
		lines = append(lines, ip)
	}


	lines = append(lines, "[yarn-resourcemanager]")
	lines = append(lines,ansibleConfig.IpForRm1)
	lines = append(lines,ansibleConfig.IpForRm2)

	lines = append(lines, "[yarn-nodemanager]")
	for _, ip := range ansibleConfig.IpForNodeManagers {
		lines = append(lines, ip)
	}


	//log.Println("---------------------ansibleConfig: ", ansibleConfig)

	return
}

func InitService(s *db.Service) (err error) {
	var (
		lines []string
		hostsFile string
		c *db.Cluster
		f     *os.File
		cmdStr string
		tid uint
	)
	if s == nil || s.ID <= 0 || s.Name == "" {
		err = errors.New("Invalid Service Object")
		return
	}

	lines, c, err = GenerateHostsFileByCluster(s.ID)
	if err != nil {
		return
	}
	switch s.Name {

	case "HDFS":

	case "HBASE":
		hostsFile = global.HOSTS_DIR + c.Name + "-hbase" + global.HOSTS_FILE_SUFFIX
		cmdStr = "ansible-playbook " + global.HBASE_YAML_FILE + " -i " + hostsFile
		tid, err = AddTaskToDb(c.ID, s.ID, "hbase-init", "Running", "")
		if err !=nil {
			log.Printf("Init service %s Failed: %s", s.Name, err)
			return
		}
	}
	f, err = os.OpenFile(hostsFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	defer func() {
		if (tid > 0) {
			db.UpdateTaskStatusByErr(tid, err)
		}
	} ()


	if err != nil {
		log.Println("Cannot open file:", err)
		return
	}
	defer f.Close()
	f.WriteString(strings.Join(lines, "\r\n"))

	// run ansible play book
	err = util.RunAndLog(tid, cmdStr)
	return
}

//ClusteredServiceConfig

func QueryServiceDetail(sid uint) (config *module.ServiceDetail, err error){

	var (
		s *db.Service
		cluster *db.Cluster
	)
	s , err = QueryServiceById(sid)
	if err != nil {
		return
	}
	var roleToHosts  = make(map[string][]module.HostnameIp)
	err = json.Unmarshal([]byte(s.Config), &roleToHosts)
	if err != nil {
		return
	}
	//log.Println("roleToHosts:", roleToHosts)
	c := module.ServiceDetail{}
	c.Id =  int(s.ID)
	c.Cid = int(s.Cid)
	c.Name = s.Name
	c.RoleToHosts = roleToHosts
	//log.Println("ServiceDetail: ->>>>>>>>", c)
	// end point
	cluster, err = db.QueryClusterById(int(s.Cid))
	if err != nil {
		log.Println("Cannot find cluster for service:", s.Name)
		return
	}
	eps := GetEndpoints(cluster.Name, s.Name, roleToHosts)
	c.Endpoints = eps
	config = &c
	return
}

func GetEndpoints(cluster, service string, m map[string][]module.HostnameIp) (eps map[string][]module.Endpoint){
	var (
		info module.ClusterRuntimeInfo
		nodes       []module.ClusterNodeInfo
		exists bool
		result = make(map[string][]module.Endpoint)
	)

	info, exists = ClusterRuntimeInfos[cluster]
	if ! exists {
		return
	}
	nodes = info.Nodes


	for name, hosts := range m {
		var tmpEps  []module.Endpoint
		switch name {
		case "nodeManagers":
			for _, h := range hosts {
				ep  :=module.Endpoint{
				}
				ep.Hostname = h.Hostname
				ep.Ip = h.Ip
				for _, n := range nodes {
					if n.Hostname == h.Hostname {
						cs := n.RunningComponents
						for _, c := range cs {
							if c == "NodeManager" {
								ep.Status = "Running"
								break
							}
						}

					}
				}
				tmpEps = append(tmpEps, ep)
			}
		case "resourceManagers":
			for _, h := range hosts {
				ep  :=module.Endpoint{
				}
				ep.Hostname = h.Hostname
				ep.Ip = h.Ip
				for _, n := range nodes {
					if n.Hostname == h.Hostname {
						cs := n.RunningComponents
						for _, c := range cs {
							if c == "ResourceManager" {
								ep.Status = "Running"
								break
							}
						}

					}
				}
				tmpEps = append(tmpEps, ep)
			}

			//dataNodes
		case "dataNodes":
			for _, h := range hosts {
				ep  :=module.Endpoint{
				}
				ep.Hostname = h.Hostname
				ep.Ip = h.Ip
				for _, n := range nodes {
					if n.Hostname == h.Hostname {
						cs := n.RunningComponents
						for _, c := range cs {
							if c == "DataNode" {
								ep.Status = "Running"
								break
							}
						}
					}
				}
				if ep.Status == "" {
					ep.Status = "Stopped"
				}
				tmpEps = append(tmpEps, ep)
			}

			//nameNodes
		case "nameNodes":
			for _, h := range hosts {
				ep  :=module.Endpoint{
				}
				ep.Hostname = h.Hostname
				ep.Ip = h.Ip
				for _, n := range nodes {
					if n.Hostname == h.Hostname {
						cs := n.RunningComponents
						for _, c := range cs {
							if c == "NameNode" {
								ep.Status = "Running"
								break
							}
						}
					}
				}
				if ep.Status == "" {
					ep.Status = "Stopped"
				}
				tmpEps = append(tmpEps, ep)
			}
		case "zookeepers":


		}
		result[name] = tmpEps
	}
	eps = result


	return
}

/*
switch s.Name {
	case "HBASE":
		var roleToHosts  = make(map[string][]module.HostnameIp)
		err = json.Unmarshal([]byte(s.Config), &roleToHosts)
		if err != nil {
			return
		}
		log.Println("hbase config:", roleToHosts)
		if rss, ok :=roleToHosts["regionServers"]; ok {
			for _, rs := range rss {

				ansibleConfig.HbaseRegionservers = append(ansibleConfig.HbaseMasters, rs)

				//hbaseRegionHostnames = append(hbaseRegionHostnames, rs.Hostname)
				//hbaseRegionIps = append(hbaseRegionIps, rs.Ip)
			}
		}
		if masters, ok :=roleToHosts["masters"]; ok {
			for _, m := range masters {
				ansibleConfig.HbaseMasters = append(ansibleConfig.HbaseMasters, m)
				//hbaseMasterIps = append(hbaseMasterIps, m.Ip)
			}
		}
	case "YARN":
		var roleToHosts  = make(map[string][]module.HostnameIp)
		err = json.Unmarshal([]byte(s.Config), &roleToHosts)
		if err != nil {
			return
		}
		if rms, ok :=roleToHosts["resourceManagers"]; ok {
			for i, rm := range rms {
				if i == 0 {
					ansibleConfig.Rm1 = rm.Ip
					ansibleConfig.HostNameForRm1 = rm.Hostname
					ansibleConfig.IpForRm1 = rm.Ip
				} else {
					ansibleConfig.Rm2 = rm.Ip
					ansibleConfig.HostNameForRm2 = rm.Hostname
					ansibleConfig.IpForRm2 = rm.Ip
				}
			}
		}
		if nms, ok :=roleToHosts["nodeManagers"]; ok {
			for _, nm := range nms {
				ansibleConfig.IpForNodeManagers = append(ansibleConfig.IpForNodeManagers, nm.Ip)
			}
		}
	}

 */












