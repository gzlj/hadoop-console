package global

type Config struct {
	ServerPort string   `json:"serverPort"`
}

var (
	// 单例
	G_config *Config
)


func InitConfig(serverPort string) (err error) {


	conf := Config{
		ServerPort: serverPort,
	}
	G_config = &conf
	return
}
