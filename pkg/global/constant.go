package global

const (
	ROLE_ZOOKEEPER_SERVER_1  = "zookeeper_server_1"
	ROLE_ZOOKEEPER_SERVER_2  = "zookeeper_server_2"
	ROLE_ZOOKEEPER_SERVER_3  = "zookeeper_server_3"


	ROLE_NAMENODE_1  = "namenode_1"
	ROLE_NAMENODE_2  = "namenode_2"

	ROLE_DATANODE  = "datanode"

	ROLE_JOURNALNODE_1 = "journalnode_1"
	ROLE_JOURNALNODE_2 = "journalnode_2"
	ROLE_JOURNALNODE_3 = "journalnode_3"

	ROLE_RESOURCE_MANAGER_1 = "resource_manager_1"
	ROLE_RESOURCE_MANAGER_2 = "resource_manager_2"

	ROLE_HBASE_MASTER = "hbase_role_master"
	ROLE_HBASE_REGION_SERVER = "hbase_role_region"



	WORKING_DIR = "/etc/ansible/"

	LOGS_DIR = WORKING_DIR + "/logs/"

	STATUS_DIR = WORKING_DIR + "status/"

	HOSTS_DIR = WORKING_DIR + "target-hosts/"

	STATUS_FILE_SUFFIX = ".status"
	LOG_FILE_SUFFIX = ".log"
	HOSTS_FILE_SUFFIX = ".hosts"

	//ha-master-boostrap.yaml
	HA_MASTER_BOOTSTRAP_YAML_FILE = WORKING_DIR + "ha-master-boostrap.yaml"
	HA_MASTER2_JOIN_YAML_FILE = WORKING_DIR + "ha-master2-join.yaml"
	HA_MASTER3_JOIN_YAML_FILE = WORKING_DIR + "ha-master3-join.yaml"
	HA_MASTER_JOIN_YAML_FILE = WORKING_DIR + "ha-master-join.yaml"

	// /etc/ansible/hdfs.yaml
	HADOOP_HDFS_YAML_FILE = WORKING_DIR + "hdfs.yaml"
	HBASE_YAML_FILE = WORKING_DIR + "hbase.yaml"
	SPARK_YAML_FILE = WORKING_DIR + "spark.yaml"
	SQOOP_YAML_FILE = WORKING_DIR + "sqoop.yaml"
	HIVE_YAML_FILE = WORKING_DIR + "hive.yaml"

	//single-master-bootstrap.yaml
	SINGLE_MASTER_BOOTSTRAP_YAML_FILE = WORKING_DIR + "/single-master-bootstrap.yaml"
	//worker-node-join.yaml
	WOKER_NODE_JOIN_YAML_FILE = WORKING_DIR + "/worker-node-join.yaml"
)
