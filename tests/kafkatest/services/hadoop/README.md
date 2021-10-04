# Hadoop as a Service in Ducktape

Setup IntelliJ IDEA CE 2021.2 for development and running the tests:
```shell
cd kafka/tests/
python3 -m venv venv
source venv/bin/activate
# you have to upgrade PIP and manually install cryptography libs
pip3 install --upgrade pip
pip3 install bcrypt ntlm-auth setuptools-rust cryptography
# Then, run the below script
python3 setup.py develop
```
- Goto platform settings in project structure(⌘;), then add new python SDK pointing to `kafka/tests/venv/bin/python`. 
- In Modules section, click '+' and import module 'kafka/tests', select 'create module from existing sources' and click next and finish
- In the imported 'tests' module, select 'Dependencies' tab and choose the Module SDK as 'Python 3.9'
- Make sure to remove the 'kafka/tests' entry from the python SDK 'Classpath' (to mark the folder as source instead of library)
- Click apply and ok

## Components
* Name node: stores and manages the filesystem metadata.
* Data node: stores the actual data or the dataset.
* YARN: It's for resource management and Job scheduling. It's mainly used for map-reduce jobs.
* Node manager: Runs on slave daemons and responsible for execution of tasks on every single data node.
* Resource manager: Runs on master daemon and manages the resource allocation in the cluster.
* History server: Allow the user to get status on finished applications.

We only need Hadoop namenode and datanode. All the other components are optional and not required to test the tiered-storage setup.

To achieve High availability for namenode, it requires:
- NFS (NAS) + ZooKeeper (or)
- Quorum Journal Manager

## Command to run System test
Make sure to increase the `docker-for-desktop` memory to at least 4GB when running ZK, Kafka, MiniKDC, Hadoop, 
verifiable Producer and verifiable Consumer in the test environment. Goto Docker Desktop -> Preferences -> Resources 
-> Memory to change it.

```shell
cd kafka
export KAFKA_NUM_CONTAINERS=14; TC_PATHS="tests/kafkatest/tests/core/tiered_replication_test.py" _DUCKTAPE_OPTIONS="--debug --no-teardown" bash tests/docker/run_tests.sh
# After each run, the ducker framework collects logs from each service and saves it in 'results' folder.
# You can check the 'results' folder to find out which service is running in which container. 
# Then, to login a container, use ducker-ak ssh script. 
# Config, data and log files are mounted in /mnt/kafka folder. 
# The kafka distribution is located in /opt/kafka-dev folder
tests/docker/ducker-ak ssh ducker06
# To stop the containers
tests/docker/ducker-ak down
```

```shell
# To run it directly inside the docker container
docker exec ducker01 bash -c "cd /opt/kafka-dev && ducktape --cluster-file /opt/kafka-dev/tests/docker/build/cluster.json  ./tests/kafkatest/tests/core/tiered_replication_test.py --debug --no-teardown"
```

To view the file contents in the HDFS docker container:
```shell
# Find the container where MiniHadoop service is running. Then, login to that container.
tests/docker/ducker-ak ssh ducker02
cd /opt/hadoop-3.2.2
# The hadoop config, log and data files are located in /mnt/hadoop folder
echo "Hello World" | cat > /tmp/hello.txt
./bin/hdfs --loglevel DEBUG --config /mnt/hadoop/config/etc/hadoop dfs -mkdir -p test/
./bin/hdfs  --config /mnt/hadoop/config/etc/hadoop dfs -copyFromLocal /tmp/hello.txt test/
./bin/hdfs  --config /mnt/hadoop/config/etc/hadoop dfs -ls
./bin/hdfs  --config /mnt/hadoop/config/etc/hadoop dfs -cat /user/ducker/test/hello.txt 
./bin/hdfs  --config /mnt/hadoop/config/etc/hadoop dfs -cat test/hello.txt
```

Environment Variables in HDFS
-----------------------------
The below environment variables will be set in your HDFS docker namenode and datanode containers:
```shell
export JAVA_HOME="/usr/local/openjdk-8"
export HADOOP_HOME="/opt/hadoop-3.2.2"
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export HADOOP_LOG_DIR="${HADOOP_HOME}/logs"
export HADOOP_PID_DIR="${HADOOP_HOME}/pid"
export HADOOP_DAEMON_ROOT_LOGGER="INFO,RFA",
export HADOOP_OS_TYPE="${HADOOP_OS_TYPE:-$(uname -s)}"
export HADOOP_GC_SETTINGS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps"
export HDFS_NAMENODE_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=1026 
${HADOOP_GC_SETTINGS} -Xloggc:${HADOOP_LOG_DIR}/gc-rm.log-$(date +'%Y%m%d%H%M') -Dhadoop.security.logger=INFO,RFAS"

# default values
export HDFS_NAMENODE_USER="${HDFS_NAMENODE_USER:-$(whoami)}"
export HDFS_DATANODE_USER="${HDFS_DATANODE_USER:-$(whoami)}"
export HDFS_SECONDARYNAMENODE_USER="$(whoami)"
export HDFS_DATANODE_OPTS="-Dhadoop.security.logger=ERROR,RFAS"
export HDFS_SECONDARYNAMENODE_OPTS="-Dhadoop.security.logger=INFO,RFAS"
```

### Reference
- [Hadoop Cluster Setup](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html)
- [Understand The Default Configuration](https://docs.bitnami.com/aws/apps/hadoop/get-started/understand-default-config/)
- [How to set up a Hadoop cluster in Docker?](https://clubhouse.io/developer-how-to/how-to-set-up-a-hadoop-cluster-in-docker/)
- [Hadoop bash commands](https://docs.google.com/document/d/19zTXNbvc_kcNONcWkTfHueh1pHyIOSo-b_njv7FpYxo/edit#heading=h.9hfmjh3vlbc4)
