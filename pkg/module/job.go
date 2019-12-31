package module

type Status struct {
	Err   string `json:"err"`
	Code  int64  `json:"code"`
	Phase string `json:"phase"` //created running stopping stoped exiting exited
	Job    string `json:"job"`
}

type TaskFilesAndCmds struct {
	HostsFile string
	Logfile string
	CoreCmdStr string
}

type AnsibleHostsConfig struct {
	Cluster              string `json:"cluster"`
	Zk1                  string `json:"zk1"`
	Zk2                  string `json:"zk2"`
	Zk3                  string `json:"zk3"`
	Nn1                  string `json:"nn1"`
	Nn2                  string `json:"nn2"`
	Jn1                  string `json:"jn1"`
	Jn2                  string `json:"jn2"`
	Jn3                  string `json:"jn3"`
	Rm1                  string `json:"rm1"`
	Rm2                  string `json:"rm2"`
	Password             string `json:"password"`
	HostNameForDataNodes []string `json:"hostNameForDataNodes"`
	IpForDataNodes []string `json:"dataNodes"`

	HostNameForJn1 string `json:"hostNameForJn1"`
	HostNameForJn2 string `json:"hostNameForJn2"`
	HostNameForJn3 string `json:"hostNameForJn3"`

	HostNameForNn1 string `json:"HostNameFornn1"`
	HostNameForNn2 string `json:"HostNameFornn2"`

	HostNameForRm1 string `json:"HostNameForRm1"`
	HostNameForRm2 string `json:"HostNameForRm2"`


	AllHostAndips []HostnameIp `json:"allHostAndips"`
}
