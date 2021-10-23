# Introduction

This module provides HDFS implementation for `RemoteStorageManager`. It stores the respective logs of non-compacted 
topic partitions in the configured HDFS base directory.

```properties
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager
retention.ms=259200000

remote.log.storage.manager.impl.prefix=remote.log.storage.
remote.log.storage.hdfs.base.dir=kafka-remote-storage
remote.log.storage.hdfs.user=kloak
remote.log.storage.hdfs.keytab.path=/etc/kafka/kloak.keytab
```

# Steps to setup HDFS, ZK and Kafka in local machine

### Setup HDFS
Download the latest [Hadoop distribution 3.3.1](https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz) and start it in standalone mode:
```shell
$ wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
$ tar -xf hadoop-3.3.1.tar.gz
$ cd hadoop-3.3.1
$ export HADOOP_HOME=`pwd`
```

Configure the core-site.xml and hdfs-site.xml files under ${HADOOP_HOME}/etc/hadoop folder:
```xml
<!-- core-site.xml -->
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

```xml
<!-- hdfs-site.xml -->
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/tmp/data/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/tmp/data/dfs/data</value>
  </property>
</configuration>
```

Start the HDFS name-node and data-node:
```shell
# format the namenode directory
bin/hdfs namenode -format
# starts the namenode
bin/hdfs namenode
# starts the datanode
bin/hdfs datanode

# create 'kafka-remote-storage' log directory under which the topic data gets saved
bin/hdfs dfs -mkdir -p /user/<username>/kafka-remote-storage
# To list all the files in HDFS
bin/hdfs dfs -ls -R /

# Sample commands to test the HDFS read/write operation
echo Hello | cat > /tmp/hello.txt
bin/hdfs dfs -copyFromLocal /tmp/hello.txt kafka-remote-storage/
bin/hdfs dfs -cat kafka-remote-storage/hello.txt

# To delete all the contents under 'kafka-remote-storage' directory:
./bin/hdfs dfs -rm -R -skipTrash kafka-remote-storage
```

### Setup ZK and Kafka

Checkout and build the 2.8.x-tiering-dev branch:
```shell
$ git clone gitolite@code.uber.internal:data/kafka
$ cd kafka; git fetch origin 2.8.x-tiering-dev; git checkout -b 2.8.x-tiering-dev FETCH_HEAD
$ ./gradlew clean releaseTarGz -x test
$ cd core/build/distributions/ 
$ tar -xf kafka_2.13-2.8.0.tgz
```

In server.properties, configure the below properties to enable HDFS as remote log storage in broker:
```properties
############################# Remote Storage Configurations ############################
# Remote Storage Manager Config
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager
remote.log.storage.manager.class.path=<HADOOP_HOME>/etc/hadoop:<KAFKA_HOME>/external/hdfs/libs/*

remote.log.storage.manager.impl.prefix=remote.log.storage.
remote.log.storage.hdfs.base.dir=kafka-remote-storage
# remote.log.storage.hdfs.user=<user>
# remote.log.storage.hdfs.keytab.path=<user>.keytab

# Remote Log Metadata Manager Config
remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager
remote.log.metadata.manager.class.path=
remote.log.metadata.manager.listener.name=PLAINTEXT

remote.log.metadata.manager.impl.prefix=rlmm.config.
rlmm.config.remote.log.metadata.topic.num.partitions=5
rlmm.config.remote.log.metadata.topic.replication.factor=1
rlmm.config.remote.log.metadata.topic.retention.ms=-1
```

Run the ZooKeeper and Kafka server locally and create the topics with remote log storage:
```shell
$ cd <KAFKA_HOME>/bin; JMX_PORT=9990 nohup sh zookeeper-server-start.sh ../config/zookeeper.properties &
$ cd <KAFKA_HOME>; JMX_PORT=9991 nohup sh bin/kafka-server-start.sh config/server.properties &

# Remote storage should be enabled at system (cluster) level and at individual topic level
# The retention time for this topic is set to default 7 days. And, configured the local retention timeout to 60 seconds to quickly cleanup the log segments from local log directory. 
# Once the active segment gets rolled out, it takes 90 seconds (initial log cleaner thread delay is 30s) for the log cleaner thread to clean up the expired local log segments.  
$ sh kafka-topics.sh --bootstrap-server localhost:9092 --topic sample-tiered-storage-test --replication-factor 1 --partitions 5 --create --config local.retention.ms=60000 --config segment.bytes=1048576 --config remote.storage.enable=true

# Run producer perf script to generate load on the topic
$ cat test-producer.config
bootstrap-server=localhost:9092
acks=all

$ sh kafka-producer-perf-test.sh --topic sample-tiered-storage-test --num-records 100000 --throughput 100 --record-size 10240 --producer.config ../config/producer.properties

# To view the uploaded segments chronologically in HDFS:
$ bin/hdfs dfs -ls -R kafka-remote-storage/ | grep sample | sort -k6,7

# Verify the earliest and latest log offset using GetOffsetShell CLI tool once the local log segments are removed
# NOTE: This tool doesn't return the earliest local log offset (-3) at this time.
$ sh kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic sample-tiered-storage-test --time <-1/-2>

# Consume from the beginning of the topic to verify that the consumers are able to read all the messages from remote and local storage:
$ sh kafka-console-consumer.sh --bootstrap-server localhost:9092 localhost:9092 --topic sample-tiered-storage-test --partition 0 --from-beginning --property print.offset=true --property print.value=false
```

# Uber Setup Guide
Read [HDFS Get Started](https://engwiki.uberinternal.com/display/TEOHADOOP/HDFS+Get+Started) guide to know about the 
client usage and configuring the client to connect with cluster.

## Debug steps to check the connection with HDFS

Download the Hadoop distribution and copy it to your container to get the CLI access to production cluster with the 
predefined client and security configurations.
```shell
ssh -A <host>
wget https://archive.apache.org/dist/hadoop/core/hadoop-2.8.2/hadoop-2.8.2.tar.gz
sudo docker cp hadoop-2.8.2.tar.gz $(sudo docker ps -q --filter name=kafka\$):/home/udocker
sudo docker exec -it $(sudo docker ps -q --filter name=kafka\$) /bin/bash
cd /home/udocker
tar -xf hadoop-2.8.2.tar.gz
```

The below libraries are already baked in your Kafka base image. Adding them here for reference.
To generate the Kerberos config file, read [data/krb5-conf](https://code.uberinternal.com/diffusion/DAKRBJI) repository.
You can also [download](https://sourcegraph.uberinternal.com/search?q=repo:%5Ecode%5C.uber%5C.internal/data/piper%24+file:%5Econfig/+krb5-) 
the ready-made krb5 configuration for your region. Then, run the below commands inside the container.
```shell
sudo apt-get update && apt-get upgrade
# Search for the latest conf file version, then install it.
# sudo apt-cache search neon | tail -3
# sudo apt-cache search platinum | tail -3
sudo DEBIAN_FRONTEND=noninteractive apt-get install yarn-neon-secure-dca-conf-73 hdfs-platinum-phx-conf-67 krb5-user uber-data-krb5-conf -y
sudo chown -R udocker:udocker /opt/yarn /opt/hdfs
# To connect to HDFS PHX cluster, activate hdfs-platinum-phx-conf-67 conf
bash /opt/hdfs/hdfs-platinum-phx-conf-67/ACTIVATE
# To connect to HDFS DCA cluster, activate yarn-neon-secure-dca-conf-73 conf
bash /opt/yarn/yarn-neon-secure-dca-conf-73/ACTIVATE
# To generate the krb5.conf, run the below command:
/opt/uber-data-krb5-conf/scripts/setup.sh --region ${UBER_REGION} --env ${UBER_RUNTIME_ENVIRONMENT} --dest /etc/kafka/krb5.conf
```

Once the distribution and required libraries are installed in your container, then run the below commands to list, write, read and delete the files:
```shell
cp /opt/hdfs/conf/core-site.xml /home/udocker/hadoop-2.8.2/etc/hadoop/
cp /opt/hdfs/conf/hdfs-site.xml /home/udocker/hadoop-2.8.2/etc/hadoop/

# NOTE: KRB5_CONFIG is not applicable to HDFS, should export HADOOP_OPTS separately
export KRB5_CONFIG=/etc/kafka/krb5.conf
export HADOOP_OPTS="-Djava.security.krb5.conf=/etc/kafka/krb5.conf"
# The 'kloak' keytab will be mounted on the container in /langley/udocker/odin-kafka/current/kloak/odin_kafka/keytab/kloak.keytab path
kinit -kt /langley/udocker/odin-kafka/current/kloak/odin_kafka/keytab/kloak.keytab kloak
klist

cd /home/udocker/hadoop-2.8.2
# NOTE: default fs will be taken from etc/hadoop/hdfs-site.xml
# NOTE: To enable logging, use ‘log4j.configuration’ system property
# The data gets stored under directory /user/<keytab_user>/. In this case, /user/kloak/kafka-remote-storage/CLUSTER folder
./bin/hdfs --loglevel INFO dfs -ls -R kafka-remote-storage/kafka-staging1-dca

# Test a sample write operation. The data gets copied to the /user/<kloak>/kafka-remote-storage folder.
echo "Hello World" | cat > /tmp/hello.txt
./bin/hdfs dfs -mkdir -p kafka-remote-storage/
./bin/hdfs dfs -copyFromLocal /tmp/hello.txt kafka-remote-storage/
./bin/hdfs dfs -cat kafka-remote-storage/hello.txt

# To remove the files from HDFS, the user should run the operation with sufficient privileges. In this case, you need to
# be logged in with 'kloak' keytab.
./bin/hdfs dfs -rm -R -skipTrash kafka-remote-storage/hello.txt
```

You can also view the files from the Hadoop gateway machines:
```shell
ssh -A hadoopgw01-dca1
# Do kinit with your LDAP username
kinit
klist

hdfs dfs -fs hdfs://hadoopneonnamenode02-dca1.prod.uber.internal:8020 -ls -R /user/kloak/kafka-remote-storage/kafka-staging1-dca
```

## Test Java Client

Paste: [P283737](https://code.uberinternal.com/P283737)
```java
package org.apache.kafka.rsm.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;

public class TestHDFSDeletion {
    // arg0: fsURI, args1: user, args2: keytabPath
    public static void main(String[] args) throws IOException {
        URI fsURI = URI.create(args[0]);
        Configuration hadoopConf = new Configuration();
        String authentication = hadoopConf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);
        System.out.println("Authentication mechanism: " + authentication);
        if (authentication.equalsIgnoreCase("kerberos")) {
            String user = args[1];
            String keytabPath = args[2];
            try {
                UserGroupInformation.setConfiguration(hadoopConf);
                UserGroupInformation.loginUserFromKeytab(user, keytabPath);
            } catch (final Exception ex) {
                throw new RuntimeException(String.format("Unable to login as user: %s", user), ex);
            }
            FileSystem fileSystem = FileSystem.newInstance(fsURI, hadoopConf);
            String path = "kafka-remote-storage/kafka-staging1-dca/uberex.metrics0-7-_XApRouCTVOScfraIZXVBg/zxl-JmiJRKat_sGpVE912Q";
            boolean deleteStatus = fileSystem.delete(new Path(path), true);
            System.out.println("File path: " + path + ", delete status: " + deleteStatus);
        }
    }
}
```

Copy this file into docker container under folder: `/home/udocker`. To compile and run:
```shell
javac -d . -cp odin-kafka/external/hdfs/libs/*:odin-kafka/libs/* TestHDFSDeletion.java
nohup java -cp .:/opt/yarn/conf:/opt/hdfs/conf:odin-kafka/external/hdfs/libs/*:odin-kafka/libs/* -Dsun.security.krb5.debug=true -Djava.security.krb5.conf=/etc/kafka/krb5.conf org.apache.kafka.rsm.hdfs.TestHDFSDeletion hdfs://hadoopneonnamenode02-dca1.prod.uber.internal:8020 kloak /langley/udocker/odin-kafka/current/kloak/odin_kafka/keytab/kloak.keytab &
```

If you want to connect to HDFS with your LDAP username instead of 'kloak' keytab, then:
```shell
$ kinit kchandraprakash@CORP.UBER.COM
$ klist
$ kdestroy

$ ktutil
ktutil:  add_entry -password -p kchandraprakash@CORP.UBER.COM -k 1 -e aes256-cts
Password for kchandraprakash@CORP.UBER.COM:
ktutil:  wkt /tmp/kchandraprakash.keytab
ktutil:  quit

# To view the created keytab:
$ klist -kte /tmp/kchandraprakash.keytab
Keytab name: FILE:kchandraprakash.keytab
KVNO Timestamp         Principal
---- ----------------- --------------------------------------------------------
   1 04/12/21 04:03:13 kchandraprakash@CORP.UBER.COM (aes256-cts-hmac-sha1-96)

$ kinit -V -kt /tmp/kchandraprakash.keytab kchandraprakash@CORP.UBER.COM
$ klist

$ nohup java -cp .:/opt/yarn/conf:/opt/hdfs/conf:odin-kafka/external/hdfs/libs/*:odin-kafka/libs/* -Dsun.security.krb5.debug=true -Djava.security.krb5.conf=/etc/kafka/krb5.conf -Djava.security.krb5.realm=CORP.UBER.COM -Djavax.security.auth.useSubjectCredsOnly=false -Djava.security.krb5.kdc=activedirectory.local.uber.internal org.apache.kafka.rsm.hdfs.TestHDFSDeletion hdfs://hadoopneonnamenode02-dca1.prod.uber.internal:8020 kchandraprakash@CORP.UBER.COM /tmp/kchandraprakash.keytab &
```

## HDFS CLI Scripts
```shell
# To format the namenode: 
./bin/hdfs namenode -format test-cluster
# Configure the /etc/hadoop/workers with hostname to start the datanodes in those hosts: 
./sbin/start-dfs.sh
# Create a user directory: 
./bin/hdfs dfs -D dfs.replicationFactor=4 -mkdir -p /user/ducker
# Create a input directory for the user: 
./bin/hdfs dfs -D dfs.replicationFactor=4 -mkdir input
# To copy a local file to HDFS: (NOTE: Command ‘cp’ does not work, use copyFromLocal instead)
./bin/hdfs dfs -D dfs.replication=4  -copyFromLocal ./xyz.txt input 
# To list the files in directory: 
./bin/hdfs dfs -D dfs.replicationFactor=4 -ls /user/ducker/input
# To read the file contents: 
./bin/hdfs dfs -cat /user/ducker/input/xyz.txt
# To touch/create a file: 
./bin/hdfs dfs -touchz hello.txt
# To remove the file: (use skipTrash as trash policy might not set properly in the test cluster) 
./bin/hdfs dfs -skipTrash -rm /user/ducker/input/xyz.txt
# To view the disk usage: 
./bin/hdfs dfs -du /user/ducker
# To specify the filesystem URI: 
./bin/hdfs dfs --loglevel DEBUG -fs hdfs://<namenode_host>:<namenode_port> <actual_command>
```

## Reference
- [HDFS Get Started](https://engwiki.uberinternal.com/display/TEOHADOOP/HDFS+Get+Started)
- [HDFS Client Guide](https://engwiki.uberinternal.com/display/TEOHADOOP/HDFS+Client+Guide)
- [DCA Secure Guide](https://engwiki.uberinternal.com/display/TEOHADOOP/DCA+Secure+Guide)
- [HDFS Trash](http://t.uber.com/hdfs_trash)
- [Secure HDFS Client Example](https://henning.kropponline.de/2016/02/14/a-secure-hdfs-client-example/)
- [Hadoop Service Account Creation](https://team.uberinternal.com/display/IT/Hadoop+Service+Account+Creation+-+wiki)
- [Pullo-Kloak](https://pullo-frontend.uberinternal.com/groups/details/kloak)

Reach out to #hadoop-helpdesk in slack for any user-support in Hadoop.
