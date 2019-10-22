This module provides `RemoteStorageManager` implementation for HDFS. It stores the respective logs of non-compacted topic partitions in the configured HDFS directory.   

You can set the below properties with the respective values on Kafka broker's server.properties. These properties should be the same across all brokers in the cluster.  

```
remote.log.storage.enable=true
## this should be more than `log.retention.minutes`
remote.log.retention.minutes=50
remote.log.storage.manager.class.name=org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager
remote.log.storage.hdfs.fs.uri=hdfs://localhost:9000 
remote.log.storage.hdfs.base.dir=/kafka-remote-logs
```
