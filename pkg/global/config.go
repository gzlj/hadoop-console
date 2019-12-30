package global

type Config struct {
	ServerPort string   `json:"serverPort"`
	MysqlHost string   `json:"mysqlHost"`
	MysqlPort string   `json:"mysqlPort"`
	MysqlUser string   `json:"mysqlUser"`
	MysqlPassword string   `json:"mysqlPassword"`
}

var (
	// 单例
	G_config *Config
)


func InitConfig(serverPort, mysqlHost, mysqlPort, mysqlUser, mysqlPassword  string) (err error) {

	conf := Config{
		ServerPort: serverPort,
		MysqlHost: mysqlHost,
		MysqlPort: mysqlPort,
		MysqlUser: mysqlUser,
		MysqlPassword: mysqlPassword,
	}
	G_config = &conf
	return
}
