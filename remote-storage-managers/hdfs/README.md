This module provides `RemoteStorageManager` implementation for HDFS. It stores the respective logs of non-compacted topic partitions in the configured HDFS directory.   

You can set the below properties with the respective values on Kafka broker's server.properties. These properties should be the same across all brokers in the cluster.  

```
remote.log.storage.system.enable=true
retention.ms=259200000
remote.log.storage.manager.class.name=org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager
remote.log.storage.manager.impl.prefix="remote.log.storage."
remote.log.storage.hdfs.fs.uri=hdfs://localhost:9000
remote.log.storage.hdfs.base.dir=/kafka-remote-logs
remote.log.storage.hdfs.user=kafka_user@REALM
remote.log.storage.hdfs.keytab.path=/etc/security/keytabs/kafka_user.keytab
```
