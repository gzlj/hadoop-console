package db

import (
	"encoding/json"
	"github.com/gzlj/hadoop-console/pkg/module"
	"github.com/jinzhu/gorm"
)

var (
	G_db *gorm.DB
)

type Cluster struct {
	gorm.Model
	Name string `gorm:"type:varchar(64);not null;"`
	Config string `gorm:"type:varchar(4096);not null;"`
	Removed int `gorm:"type:tinyint(1);not null;"`
	Status string `gorm:"type:varchar(4096);"`
}

type Service struct {
	gorm.Model
	Cid uint `gorm:"type:int(10) unsigned;not null;"`
	Name string `gorm:"type:varchar(64);not null;"`
	Config string `gorm:"type:varchar(4096);not null;"`
	Removed int `gorm:"type:tinyint(1);not null;"`
}

type ServiceDto struct {
	Id uint `json:"id"`
	Cid uint `gorm:"type:int(10) unsigned;not null;"`
	Name string `gorm:"type:varchar(64);not null;"`
	//Config string `gorm:"type:varchar(4096);not null;"`
	//Removed int `gorm:"type:tinyint(1);not null;"`
	CreatedAt module.Time `json:"createdAt"`
	UpdatedAt module.Time `json:"updatedAt"`
}

type Log struct {
	gorm.Model
	Tid uint `gorm:"type:int(10) unsigned;not null;"`
	Content string `gorm:"type:MEDIUMTEXT;not null;"`
	Removed int `gorm:"type:tinyint(1);not null;"`
}

type LogDto struct {
	Id uint `json:"id"`
	CreatedAt module.Time `json:"createdAt"`
	StopedAt module.Time `json:"stopedAt"`
	Content string `json:"content"`
}

type Task struct {
	gorm.Model
	Cid uint `gorm:"type:int(10) unsigned;not null;"`
	Sid uint `gorm:"type:int(10) unsigned;not null;"`
	Name string `gorm:"type:varchar(64);not null;"`
	Status string `gorm:"type:varchar(64);not null;"`
	Message string `gorm:"type:varchar(128);not null;"`
	Removed int `gorm:"type:tinyint(1);not null;"`
}

type TaskDto struct {
	Id uint `json:"id"`
	Name string `json:"name"`
	Status string `json:"status"`
	Message string `json:"message"`
	CreatedAt module.Time `json:"createdAt"`
	UpdatedAt module.Time `json:"updatedAt"`

	Removed int `json:"-"`
}



/*

type ClusterList []*ClusterDto

func (l ClusterList)  Len() int  {
	return len(l)
}

func (l ClusterList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l ClusterList)  Less(i, j int) bool {
	return l[j].Id > l[i].Id
}

 */

type TaskList []*TaskDto

func (t TaskList) Len() int  {
	return len(t)
}

func (t TaskList) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t TaskList) Less(i, j int) bool {
	return t[j].Id > t[i].Id
}


func (t *Task) ToDto() (dto *TaskDto, err error) {

	dto = &TaskDto{
		Id: t.ID,
		Name: t.Name,
		Status: t.Status,
		Message: t.Message,
		CreatedAt: module.Time(t.CreatedAt),
		UpdatedAt: module.Time(t.UpdatedAt),
	}
	return
}

func (s *Service) ToDto() (dto *ServiceDto, err error) {
	dto = &ServiceDto{
		Id: s.ID,
		Cid: s.Cid,
		Name: s.Name,
		CreatedAt: module.Time(s.CreatedAt),
		UpdatedAt: module.Time(s.UpdatedAt),
	}

	return
}


func (c *Cluster) ToDto() (dto *ClusterDto, err error) {
	var (
		config module.ClusterConf
		bytes []byte

	)
	bytes = []byte(c.Config)
	if err = json.Unmarshal(bytes, &config); err != nil {
		return
	}
	dto = &ClusterDto {
		Id: c.ID,
		Name: c.Name,
		Config:    config,
		CreatedAt: module.Time(c.CreatedAt),
		UpdatedAt: module.Time(c.UpdatedAt),
	}
	return
}

type ClusterDto struct {

	Id uint `json:"id"`
	Name string `json:"name"`
	Config module.ClusterConf `json:"config"`
	CreatedAt module.Time `json:"createdAt"`
	UpdatedAt module.Time `json:"updatedAt"`
	Removed int `json:"-"`
}




/*type Time time.Time

const (
	timeFormart = "2006-01-02 15:04:05"
)

func (t *Time) UnmarshalJSON(data []byte) (err error) {
	now, err := time.ParseInLocation(`"`+timeFormart+`"`, string(data), time.Local)
	*t = Time(now)
	return
}

func (t Time) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, len(timeFormart)+2)
	b = append(b, '"')
	b = time.Time(t).AppendFormat(b, timeFormart)
	b = append(b, '"')
	return b, nil
}

func (t Time) String() string {
	return time.Time(t).Format(timeFormart)
}*/



type ClusterList []*ClusterDto

func (l ClusterList)  Len() int  {
	return len(l)
}

func (l ClusterList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l ClusterList)  Less(i, j int) bool {
	return l[j].Id > l[i].Id
}



func (dto *ClusterDto) ToCluster() (c Cluster, err error){
	var (
		bytes []byte
	)
	bytes, err = json.Marshal(dto.Config)
	if err != nil {
		return
	}
	c = Cluster{
		Name: dto.Name,
		Config: string(bytes),
	}
	return
}

func (l *Log) ToDto() LogDto {

	return LogDto{
		Id: l.ID,
		Content: l.Content,
		CreatedAt: module.Time(l.CreatedAt),
		StopedAt: module.Time(l.UpdatedAt),
	}
}
