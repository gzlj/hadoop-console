package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gzlj/hadoop-console/pkg/global"
	"github.com/gzlj/hadoop-console/pkg/infra/db"
	"github.com/gzlj/hadoop-console/pkg/infra/job"
	"github.com/gzlj/hadoop-console/pkg/module"
	"io"
	"log"
	"os"
	"os/exec"
	"sort"
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

func ListTasksByCluster(cid uint) (dtos []*db.TaskDto, err error) {
	var (
		tasks []*db.Task
		t *db.Task
	dto *db.TaskDto
	)
	tasks = db.QueryTasksByClusterId(cid)
	for _,t = range tasks {
		dto, err = t.ToDto()
		if err != nil {
			continue
		}
		dtos = append(dtos, dto)
	}
	sort.Sort(db.TaskList(dtos))
	return
}

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
}

func SyncFromHeartBeat() {
	for hb := range HbChan {
		syncFromHeartBeat(hb)
	}
}

func syncFromHeartBeat(hb module.NodeHeartBeat) {
	clusterInfo, exists := ClusterRuntimeInfos[hb.Cluster]
	if ! exists {
		log.Println("Cluster not exists: ", hb.Cluster)
		return
	}
	nodes := clusterInfo.Nodes
	for i, _ := range nodes {
		if nodes[i].Hostname == hb.Hostname {
			nodes[i].RunningComponents = hb.RunningComponents
			nodes[i].LastKnown = module.Time(time.Now())
			nodes[i].State = "Ready"

			/*var components []module.ComponentStatus

			//calculate component
			for _, r := range nodes[i].Roles {




				switch r {
			case global.ROLE_DATANODE:

			case global.ROLE_ZOOKEEPER_SERVER_1:
			case global.ROLE_ZOOKEEPER_SERVER_2:
			case global.ROLE_ZOOKEEPER_SERVER_3:
			case global.ROLE_JOURNALNODE_1:

			case global.ROLE_JOURNALNODE_2:

			case global.ROLE_JOURNALNODE_3:

			case global.ROLE_NAMENODE_1:

			case global.ROLE_NAMENODE_2:

			case global.ROLE_RESOURCE_MANAGER_1:

			case global.ROLE_RESOURCE_MANAGER_2:

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

			}*/

		}
	}
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
	// db.UpdateTaskStatus(tid, err)
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
	db.UpdateTaskStatus(tid, err)

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
	// db.UpdateTaskStatus(tid, err)
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
	go job.SyncLogToDb(cmdStdoutPipe, tid)

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
	db.UpdateTaskStatus(tid, err)
	log.Println("Job is completely finished:", jobName)
}


func AddTaskToDb(clusterId, servcieId uint, taskName, status, message string) (tid uint, err error) {
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

func AddService(config module.ClusteredHbaseConfig) (err error) {
	var (
		bytes []byte
		ss []*db.Service
		s db.Service
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

	s = db.Service{
		Cid: uint(config.Cid),
		Name: config.Name,
		Config: string(bytes),
	}
	err = db.AddService(&s)

	return
}












