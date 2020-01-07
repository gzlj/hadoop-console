package db

import (
	"encoding/json"
	"github.com/gzlj/hadoop-console/pkg/global"
	"github.com/gzlj/hadoop-console/pkg/module"
	"errors"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"log"
)

func InitDb() {
	var db *gorm.DB
	var err error
	var mysqlStr = global.G_config.MysqlUser + ":" + global.G_config.MysqlPassword + "@(" + global.G_config.MysqlHost + ":" + global.G_config.MysqlPort + ")/bigdata?charset=utf8&parseTime=True&loc=Local"
	log.Println("msyql:", mysqlStr)
	//if db, err = gorm.Open("mysql", "root:ps@(192.168.1.70:3307)/gotest?charset=utf8&parseTime=True&loc=Local"); err != nil {
	if db, err = gorm.Open("mysql", mysqlStr); err != nil {
		log.Fatal(err)
		return
	}

	if !db.HasTable(&Cluster{}) {
		if err = db.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8").CreateTable(&Cluster{}).Error; err != nil {
			panic(err)
		}
	}
	G_db = db
	log.Println("G_db:", db)
}

func ADDCluster(like *Cluster) (err error) {

	//count := G_db.Table("clusters").Where("name = ? AND removed = ?", like.Name, "0").RowsAffected
	var (
		count  int

		ss []*Service

		s *Service
	)
	G_db.Model(Cluster{
		Name: like.Name,
		Removed: 0,
	}).Where("name = ? AND removed = ?", like.Name, "0").Count(&count)

	log.Println("count: ", count)

	if count > 0  {
		return errors.New("Cluster already exists: "+  like.Name)
	}

	tx := G_db.Begin()

	if err = tx.Create(like).Error; err != nil {
		//log.Fatal(err)
		return
	}
	// cluster --> []service
	ss, err = GetServicesFromCluster(like)
	if err != nil {
		log.Println("Failed cluster for ", like.Name,":", err)
		return
	}

	for _, s = range ss {
		if err = tx.Create(s).Error; err != nil {
			tx.Rollback()
			break
		}
	}
	if err != nil {
		log.Println("Failed cluster for ", like.Name,":", err)
		return
	}
	log.Print("Create cluster successully:", like.Name)
	tx.Commit()
	return nil
}

func GetServiceByClusterId(id uint) (ss []*Service, err error){

	//G_db.Find(&clusters, "removed = ?", "0")
	G_db.Find(&Service{}, "cid = ? AND removed = ?", id, "0")
	return
}

func GetServiceByNameAndClusterId(name string, cid uint) (ss []*Service, err error){

	//G_db.Find(&clusters, "removed = ?", "0")
	G_db.Find(&ss, "name = ? AND cid = ? AND removed = ?",name, cid, "0")



	return
}

func GetServicesFromCluster(c *Cluster) (ss []*Service, err error) {
	var (
		config module.ClusterConf
		//hdfsConfig module.HdfsConfig
	//hbaseConfig module.HbaseConfig
		//tmp string
		bytes []byte
	)
	//var config module.ClusterConf
	err = json.Unmarshal([]byte(c.Config), &config)
	if err != nil {
		log.Println("Failed to get config from cluster becouse of error of json Unmarshal :", c.Name)
		return
	}
	//hdfsConfig = config.HdfsConfig

	bytes, err = json.Marshal(config.HdfsConfig)
	if err != nil {
		log.Println("Failed to get hdfs config from cluster config becouse of error of json Unmarshal :", c.Name)
		return
	}
	ss = append(ss, &Service{
		Cid:c.ID,
		Name: "HDFS",
		Config: string(bytes),
	})
	bytes, err = json.Marshal(config.HbaseConfig)
	if err != nil {
		log.Println("Failed to get hbase config from cluster config becouse of error of json Unmarshal :", c.Name)
		return make([]*Service, 0), err
	}
	ss = append(ss, &Service{
		Cid:c.ID,
		Name: "HBASE",
		Config: string(bytes),
	})
	return
}

func QueryById(id int) *Cluster {
	var c Cluster
	if err := G_db.Find(&c, id).Error; err != nil {
		//log.Fatal(err)
		return nil
	}
	return &c
}

func ListClusters() (clusters []*Cluster) {

	G_db.Find(&clusters, "removed = ?", "0")
	return
}

func UpdateStatus(id uint, nss []*module.NodeStatus) (err error){
	var (
		bytes []byte
	)
	bytes, err = json.Marshal(nss)
	if err != nil {
		log.Println("Failed to Marshal NodeStatus: ", err.Error())
		return
	}
	update := G_db.Model(Cluster{}).Where("id = ?", id).Update("status", bytes)
	err = update.Error
	log.Println("Errors happened when update cluster Status in db:", err)
	return
}

func UpdateStatusByName(cluster string, nss []*module.NodeStatus) (err error){
	var (
		bytes []byte
	)
	bytes, err = json.Marshal(nss)
	if err != nil {
		//log.Println("Failed to Marshal NodeStatus: ", err.Error())
		return
	}
	update := G_db.Model(Cluster{}).Where("name = ?", cluster).Update("status", bytes)
	err = update.Error
	//log.Println("UpdateStatus error: ", err)
	return
}

func AppendSyncLog(taskId uint, bytes []byte) {
	var (
		err error
		l   Log)
	G_db.Table("logs").Where("tid = ? AND removed = ?", taskId, "0").Find(&l)

	if l.ID > 0 {
		//append
		l.Content = l.Content + string(bytes)
		if err = G_db.Save(&l).Error; err != nil {
			log.Println("Failed to update record to logs table for task:", taskId, ":", err)
		}
		return
	}
	l.Tid = taskId
	l.Content = string(bytes)
	G_db.Create(&l)

	// create
	if err = G_db.Create(&l).Error; err != nil {
		log.Println("Failed to insert record to logs table for task:", taskId, ":", err)
	}

}
