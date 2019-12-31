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

func ADDCluster(like *Cluster) error {

	where := G_db.Where("name = ? AND removed = ?", like.Name, 0)
	if where.Error == nil {
		return errors.New("Cluster already exists: "+  like.Name)
	}
	if err := G_db.Create(like).Error; err != nil {
		//log.Fatal(err)
		return err
	}
	log.Print("Create cluster successully:", like.Name)
	return nil
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
