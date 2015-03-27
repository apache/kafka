/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Properties

import kafka.consumer.ConsumerConfig
import kafka.message.{BrokerCompressionCodec, CompressionCodec, Message, MessageSet}
import kafka.utils.Utils
import org.apache.kafka.common.config.ConfigDef

import scala.collection.{JavaConversions, Map}

object Defaults {
  /** ********* Zookeeper Configuration ***********/
  val ZkSessionTimeoutMs = 6000
  val ZkSyncTimeMs = 2000

  /** ********* General Configuration ***********/
  val MaxReservedBrokerId = 1000
  val BrokerId = -1
  val MessageMaxBytes = 1000000 + MessageSet.LogOverhead
  val NumNetworkThreads = 3
  val NumIoThreads = 8
  val BackgroundThreads = 10
  val QueuedMaxRequests = 500

  /** ********* Socket Server Configuration ***********/
  val Port = 9092
  val HostName: String = new String("")
  val SocketSendBufferBytes: Int = 100 * 1024
  val SocketReceiveBufferBytes: Int = 100 * 1024
  val SocketRequestMaxBytes: Int = 100 * 1024 * 1024
  val MaxConnectionsPerIp: Int = Int.MaxValue
  val MaxConnectionsPerIpOverrides: String = ""
  val ConnectionsMaxIdleMs = 10 * 60 * 1000L

  /** ********* Log Configuration ***********/
  val NumPartitions = 1
  val LogDir = "/tmp/kafka-logs"
  val LogSegmentBytes = 1 * 1024 * 1024 * 1024
  val LogRollHours = 24 * 7
  val LogRollJitterHours = 0
  val LogRetentionHours = 24 * 7

  val LogRetentionBytes = -1L
  val LogCleanupIntervalMs = 5 * 60 * 1000L
  val Delete = "delete"
  val Compact = "compact"
  val LogCleanupPolicy = Delete
  val LogCleanerThreads = 1
  val LogCleanerIoMaxBytesPerSecond = Double.MaxValue
  val LogCleanerDedupeBufferSize = 500 * 1024 * 1024L
  val LogCleanerIoBufferSize = 512 * 1024
  val LogCleanerDedupeBufferLoadFactor = 0.9d
  val LogCleanerBackoffMs = 15 * 1000
  val LogCleanerMinCleanRatio = 0.5d
  val LogCleanerEnable = false
  val LogCleanerDeleteRetentionMs = 24 * 60 * 60 * 1000L
  val LogIndexSizeMaxBytes = 10 * 1024 * 1024
  val LogIndexIntervalBytes = 4096
  val LogFlushIntervalMessages = Long.MaxValue
  val LogDeleteDelayMs = 60000
  val LogFlushSchedulerIntervalMs = Long.MaxValue
  val LogFlushOffsetCheckpointIntervalMs = 60000
  val NumRecoveryThreadsPerDataDir = 1
  val AutoCreateTopicsEnable = true
  val MinInSyncReplicas = 1

  /** ********* Replication configuration ***********/
  val ControllerSocketTimeoutMs = 30000
  val ControllerMessageQueueSize = Int.MaxValue
  val DefaultReplicationFactor = 1
  val ReplicaLagTimeMaxMs = 10000L
  val ReplicaLagMaxMessages = 4000
  val ReplicaSocketTimeoutMs = ConsumerConfig.SocketTimeout
  val ReplicaSocketReceiveBufferBytes = ConsumerConfig.SocketBufferSize
  val ReplicaFetchMaxBytes = ConsumerConfig.FetchSize
  val ReplicaFetchWaitMaxMs = 500
  val ReplicaFetchMinBytes = 1
  val NumReplicaFetchers = 1
  val ReplicaFetchBackoffMs = 1000
  val ReplicaHighWatermarkCheckpointIntervalMs = 5000L
  val FetchPurgatoryPurgeIntervalRequests = 1000
  val ProducerPurgatoryPurgeIntervalRequests = 1000
  val AutoLeaderRebalanceEnable = true
  val LeaderImbalancePerBrokerPercentage = 10
  val LeaderImbalanceCheckIntervalSeconds = 300
  val UncleanLeaderElectionEnable = true

  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetries = 3
  val ControlledShutdownRetryBackoffMs = 5000
  val ControlledShutdownEnable = true

  /** ********* Offset management configuration ***********/
  val OffsetMetadataMaxSize = OffsetManagerConfig.DefaultMaxMetadataSize
  val OffsetsLoadBufferSize = OffsetManagerConfig.DefaultLoadBufferSize
  val OffsetsTopicReplicationFactor = OffsetManagerConfig.DefaultOffsetsTopicReplicationFactor
  val OffsetsTopicPartitions: Int = OffsetManagerConfig.DefaultOffsetsTopicNumPartitions
  val OffsetsTopicSegmentBytes: Int = OffsetManagerConfig.DefaultOffsetsTopicSegmentBytes
  val OffsetsTopicCompressionCodec: Int = OffsetManagerConfig.DefaultOffsetsTopicCompressionCodec.codec
  val OffsetsRetentionMinutes: Int = 24 * 60
  val OffsetsRetentionCheckIntervalMs: Long = OffsetManagerConfig.DefaultOffsetsRetentionCheckIntervalMs
  val OffsetCommitTimeoutMs = OffsetManagerConfig.DefaultOffsetCommitTimeoutMs
  val OffsetCommitRequiredAcks = OffsetManagerConfig.DefaultOffsetCommitRequiredAcks

  val DeleteTopicEnable = false

  val CompressionType = "producer"
}

object KafkaConfig {

  /** ********* Zookeeper Configuration ***********/
  val ZkConnectProp = "zookeeper.connect"
  val ZkSessionTimeoutMsProp = "zookeeper.session.timeout.ms"
  val ZkConnectionTimeoutMsProp = "zookeeper.connection.timeout.ms"
  val ZkSyncTimeMsProp = "zookeeper.sync.time.ms"
  /** ********* General Configuration ***********/
  val MaxReservedBrokerIdProp = "reserved.broker.max.id"
  val BrokerIdProp = "broker.id"
  val MessageMaxBytesProp = "message.max.bytes"
  val NumNetworkThreadsProp = "num.network.threads"
  val NumIoThreadsProp = "num.io.threads"
  val BackgroundThreadsProp = "background.threads"
  val QueuedMaxRequestsProp = "queued.max.requests"
  /** ********* Socket Server Configuration ***********/
  val PortProp = "port"
  val HostNameProp = "host.name"
  val AdvertisedHostNameProp: String = "advertised.host.name"
  val AdvertisedPortProp = "advertised.port"
  val SocketSendBufferBytesProp = "socket.send.buffer.bytes"
  val SocketReceiveBufferBytesProp = "socket.receive.buffer.bytes"
  val SocketRequestMaxBytesProp = "socket.request.max.bytes"
  val MaxConnectionsPerIpProp = "max.connections.per.ip"
  val MaxConnectionsPerIpOverridesProp = "max.connections.per.ip.overrides"
  val ConnectionsMaxIdleMsProp = "connections.max.idle.ms"
  /** ********* Log Configuration ***********/
  val NumPartitionsProp = "num.partitions"
  val LogDirsProp = "log.dirs"
  val LogDirProp = "log.dir"
  val LogSegmentBytesProp = "log.segment.bytes"

  val LogRollTimeMillisProp = "log.roll.ms"
  val LogRollTimeHoursProp = "log.roll.hours"

  val LogRollTimeJitterMillisProp = "log.roll.jitter.ms"
  val LogRollTimeJitterHoursProp = "log.roll.jitter.hours"

  val LogRetentionTimeMillisProp = "log.retention.ms"
  val LogRetentionTimeMinutesProp = "log.retention.minutes"
  val LogRetentionTimeHoursProp = "log.retention.hours"

  val LogRetentionBytesProp = "log.retention.bytes"
  val LogCleanupIntervalMsProp = "log.retention.check.interval.ms"
  val LogCleanupPolicyProp = "log.cleanup.policy"
  val LogCleanerThreadsProp = "log.cleaner.threads"
  val LogCleanerIoMaxBytesPerSecondProp = "log.cleaner.io.max.bytes.per.second"
  val LogCleanerDedupeBufferSizeProp = "log.cleaner.dedupe.buffer.size"
  val LogCleanerIoBufferSizeProp = "log.cleaner.io.buffer.size"
  val LogCleanerDedupeBufferLoadFactorProp = "log.cleaner.io.buffer.load.factor"
  val LogCleanerBackoffMsProp = "log.cleaner.backoff.ms"
  val LogCleanerMinCleanRatioProp = "log.cleaner.min.cleanable.ratio"
  val LogCleanerEnableProp = "log.cleaner.enable"
  val LogCleanerDeleteRetentionMsProp = "log.cleaner.delete.retention.ms"
  val LogIndexSizeMaxBytesProp = "log.index.size.max.bytes"
  val LogIndexIntervalBytesProp = "log.index.interval.bytes"
  val LogFlushIntervalMessagesProp = "log.flush.interval.messages"
  val LogDeleteDelayMsProp = "log.segment.delete.delay.ms"
  val LogFlushSchedulerIntervalMsProp = "log.flush.scheduler.interval.ms"
  val LogFlushIntervalMsProp = "log.flush.interval.ms"
  val LogFlushOffsetCheckpointIntervalMsProp = "log.flush.offset.checkpoint.interval.ms"
  val NumRecoveryThreadsPerDataDirProp = "num.recovery.threads.per.data.dir"
  val AutoCreateTopicsEnableProp = "auto.create.topics.enable"
  val MinInSyncReplicasProp = "min.insync.replicas"
  /** ********* Replication configuration ***********/
  val ControllerSocketTimeoutMsProp = "controller.socket.timeout.ms"
  val ControllerMessageQueueSizeProp = "controller.message.queue.size"
  val DefaultReplicationFactorProp = "default.replication.factor"
  val ReplicaLagTimeMaxMsProp = "replica.lag.time.max.ms"
  val ReplicaLagMaxMessagesProp = "replica.lag.max.messages"
  val ReplicaSocketTimeoutMsProp = "replica.socket.timeout.ms"
  val ReplicaSocketReceiveBufferBytesProp = "replica.socket.receive.buffer.bytes"
  val ReplicaFetchMaxBytesProp = "replica.fetch.max.bytes"
  val ReplicaFetchWaitMaxMsProp = "replica.fetch.wait.max.ms"
  val ReplicaFetchMinBytesProp = "replica.fetch.min.bytes"
  val ReplicaFetchBackoffMsProp = "replica.fetch.backoff.ms"
  val NumReplicaFetchersProp = "num.replica.fetchers"
  val ReplicaHighWatermarkCheckpointIntervalMsProp = "replica.high.watermark.checkpoint.interval.ms"
  val FetchPurgatoryPurgeIntervalRequestsProp = "fetch.purgatory.purge.interval.requests"
  val ProducerPurgatoryPurgeIntervalRequestsProp = "producer.purgatory.purge.interval.requests"
  val AutoLeaderRebalanceEnableProp = "auto.leader.rebalance.enable"
  val LeaderImbalancePerBrokerPercentageProp = "leader.imbalance.per.broker.percentage"
  val LeaderImbalanceCheckIntervalSecondsProp = "leader.imbalance.check.interval.seconds"
  val UncleanLeaderElectionEnableProp = "unclean.leader.election.enable"
  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetriesProp = "controlled.shutdown.max.retries"
  val ControlledShutdownRetryBackoffMsProp = "controlled.shutdown.retry.backoff.ms"
  val ControlledShutdownEnableProp = "controlled.shutdown.enable"
  /** ********* Offset management configuration ***********/
  val OffsetMetadataMaxSizeProp = "offset.metadata.max.bytes"
  val OffsetsLoadBufferSizeProp = "offsets.load.buffer.size"
  val OffsetsTopicReplicationFactorProp = "offsets.topic.replication.factor"
  val OffsetsTopicPartitionsProp = "offsets.topic.num.partitions"
  val OffsetsTopicSegmentBytesProp = "offsets.topic.segment.bytes"
  val OffsetsTopicCompressionCodecProp = "offsets.topic.compression.codec"
  val OffsetsRetentionMinutesProp = "offsets.retention.minutes"
  val OffsetsRetentionCheckIntervalMsProp = "offsets.retention.check.interval.ms"
  val OffsetCommitTimeoutMsProp = "offsets.commit.timeout.ms"
  val OffsetCommitRequiredAcksProp = "offsets.commit.required.acks"

  val DeleteTopicEnableProp = "delete.topic.enable"
  val CompressionTypeProp = "compression.type"


  /* Documentation */
  /** ********* Zookeeper Configuration ***********/
  val ZkConnectDoc = "Zookeeper host string"
  val ZkSessionTimeoutMsDoc = "Zookeeper session timeout"
  val ZkConnectionTimeoutMsDoc = "The max time that the client waits to establish a connection to zookeeper"
  val ZkSyncTimeMsDoc = "How far a ZK follower can be behind a ZK leader"
  /** ********* General Configuration ***********/
  val MaxReservedBrokerIdDoc = "Max number that can be used for a broker.id"
  val BrokerIdDoc = "The broker id for this server. " +
    "To avoid conflicts between zookeeper generated brokerId and user's config.brokerId " +
    "added MaxReservedBrokerId and zookeeper sequence starts from MaxReservedBrokerId + 1."
  val MessageMaxBytesDoc = "The maximum size of message that the server can receive"
  val NumNetworkThreadsDoc = "the number of network threads that the server uses for handling network requests"
  val NumIoThreadsDoc = "The number of io threads that the server uses for carrying out network requests"
  val BackgroundThreadsDoc = "The number of threads to use for various background processing tasks"
  val QueuedMaxRequestsDoc = "The number of queued requests allowed before blocking the network threads"
  /** ********* Socket Server Configuration ***********/
  val PortDoc = "the port to listen and accept connections on"
  val HostNameDoc = "hostname of broker. If this is set, it will only bind to this address. If this is not set, it will bind to all interfaces"
  val AdvertisedHostNameDoc = "Hostname to publish to ZooKeeper for clients to use. In IaaS environments, this may " +
    "need to be different from the interface to which the broker binds. If this is not set, " +
    "it will use the value for \"host.name\" if configured. Otherwise " +
    "it will use the value returned from java.net.InetAddress.getCanonicalHostName()."
  val AdvertisedPortDoc = "The port to publish to ZooKeeper for clients to use. In IaaS environments, this may " +
    "need to be different from the port to which the broker binds. If this is not set, " +
    "it will publish the same port that the broker binds to."
  val SocketSendBufferBytesDoc = "The SO_SNDBUF buffer of the socket sever sockets"
  val SocketReceiveBufferBytesDoc = "The SO_RCVBUF buffer of the socket sever sockets"
  val SocketRequestMaxBytesDoc = "The maximum number of bytes in a socket request"
  val MaxConnectionsPerIpDoc = "The maximum number of connections we allow from each ip address"
  val MaxConnectionsPerIpOverridesDoc = "Per-ip or hostname overrides to the default maximum number of connections"
  val ConnectionsMaxIdleMsDoc = "Idle connections timeout: the server socket processor threads close the connections that idle more than this"
  /** ********* Log Configuration ***********/
  val NumPartitionsDoc = "The default number of log partitions per topic"
  val LogDirDoc = "The directory in which the log data is kept (supplemental for " + LogDirsProp + " property)"
  val LogDirsDoc = "The directories in which the log data is kept"
  val LogSegmentBytesDoc = "The maximum size of a single log file"
  val LogRollTimeMillisDoc = "The maximum time before a new log segment is rolled out (in milliseconds)"
  val LogRollTimeHoursDoc = "The maximum time before a new log segment is rolled out (in hours), secondary to " + LogRollTimeMillisProp + " property"

  val LogRollTimeJitterMillisDoc = "The maximum jitter to subtract from logRollTimeMillis (in milliseconds)"
  val LogRollTimeJitterHoursDoc = "The maximum jitter to subtract from logRollTimeMillis (in hours), secondary to " + LogRollTimeJitterMillisProp + " property"

  val LogRetentionTimeMillisDoc = "The number of milliseconds to keep a log file before deleting it (in milliseconds)"
  val LogRetentionTimeMinsDoc = "The number of minutes to keep a log file before deleting it (in minutes), secondary to " + LogRetentionTimeMillisProp + " property"
  val LogRetentionTimeHoursDoc = "The number of hours to keep a log file before deleting it (in hours), tertiary to " + LogRetentionTimeMillisProp + " property"

  val LogRetentionBytesDoc = "The maximum size of the log before deleting it"
  val LogCleanupIntervalMsDoc = "The frequency in minutes that the log cleaner checks whether any log is eligible for deletion"
  val LogCleanupPolicyDoc = "The default cleanup policy for segments beyond the retention window, must be either \"delete\" or \"compact\""
  val LogCleanerThreadsDoc = "The number of background threads to use for log cleaning"
  val LogCleanerIoMaxBytesPerSecondDoc = "The log cleaner will be throttled so that the sum of its read and write i/o will be less than this value on average"
  val LogCleanerDedupeBufferSizeDoc = "The total memory used for log deduplication across all cleaner threads"
  val LogCleanerIoBufferSizeDoc = "The total memory used for log cleaner I/O buffers across all cleaner threads"
  val LogCleanerDedupeBufferLoadFactorDoc = "Log cleaner dedupe buffer load factor. The percentage full the dedupe buffer can become. A higher value " +
    "will allow more log to be cleaned at once but will lead to more hash collisions"
  val LogCleanerBackoffMsDoc = "The amount of time to sleep when there are no logs to clean"
  val LogCleanerMinCleanRatioDoc = "The minimum ratio of dirty log to total log for a log to eligible for cleaning"
  val LogCleanerEnableDoc = "Should we enable log cleaning?"
  val LogCleanerDeleteRetentionMsDoc = "How long are delete records retained?"
  val LogIndexSizeMaxBytesDoc = "The maximum size in bytes of the offset index"
  val LogIndexIntervalBytesDoc = "The interval with which we add an entry to the offset index"
  val LogFlushIntervalMessagesDoc = "The number of messages accumulated on a log partition before messages are flushed to disk "
  val LogDeleteDelayMsDoc = "The amount of time to wait before deleting a file from the filesystem"
  val LogFlushSchedulerIntervalMsDoc = "The frequency in ms that the log flusher checks whether any log needs to be flushed to disk"
  val LogFlushIntervalMsDoc = "The maximum time in ms that a message in any topic is kept in memory before flushed to disk"
  val LogFlushOffsetCheckpointIntervalMsDoc = "The frequency with which we update the persistent record of the last flush which acts as the log recovery point"
  val NumRecoveryThreadsPerDataDirDoc = "The number of threads per data directory to be used for log recovery at startup and flushing at shutdown"
  val AutoCreateTopicsEnableDoc = "Enable auto creation of topic on the server"
  val MinInSyncReplicasDoc = "define the minimum number of replicas in ISR needed to satisfy a produce request with required.acks=-1 (or all)"
  /** ********* Replication configuration ***********/
  val ControllerSocketTimeoutMsDoc = "The socket timeout for controller-to-broker channels"
  val ControllerMessageQueueSizeDoc = "The buffer size for controller-to-broker-channels"
  val DefaultReplicationFactorDoc = "default replication factors for automatically created topics"
  val ReplicaLagTimeMaxMsDoc = "If a follower hasn't sent any fetch requests during this time, the leader will remove the follower from isr"
  val ReplicaLagMaxMessagesDoc = "If the lag in messages between a leader and a follower exceeds this number, the leader will remove the follower from isr"
  val ReplicaSocketTimeoutMsDoc = "The socket timeout for network requests. Its value should be at least replica.fetch.wait.max.ms"
  val ReplicaSocketReceiveBufferBytesDoc = "The socket receive buffer for network requests"
  val ReplicaFetchMaxBytesDoc = "The number of byes of messages to attempt to fetch"
  val ReplicaFetchWaitMaxMsDoc = "max wait time for each fetcher request issued by follower replicas. This value should always be less than the " +
    "replica.lag.time.max.ms at all times to prevent frequent shrinking of ISR for low throughput topics"
  val ReplicaFetchMinBytesDoc = "Minimum bytes expected for each fetch response. If not enough bytes, wait up to replicaMaxWaitTimeMs"
  val NumReplicaFetchersDoc = "Number of fetcher threads used to replicate messages from a source broker. " +
    "Increasing this value can increase the degree of I/O parallelism in the follower broker."
  val ReplicaFetchBackoffMsDoc = "The amount of time to sleep when fetch partition error occurs."
  val ReplicaHighWatermarkCheckpointIntervalMsDoc = "The frequency with which the high watermark is saved out to disk"
  val FetchPurgatoryPurgeIntervalRequestsDoc = "The purge interval (in number of requests) of the fetch request purgatory"
  val ProducerPurgatoryPurgeIntervalRequestsDoc = "The purge interval (in number of requests) of the producer request purgatory"
  val AutoLeaderRebalanceEnableDoc = "Enables auto leader balancing. A background thread checks and triggers leader balance if required at regular intervals"
  val LeaderImbalancePerBrokerPercentageDoc = "The ratio of leader imbalance allowed per broker. The controller would trigger a leader balance if it goes above this value per broker. The value is specified in percentage."
  val LeaderImbalanceCheckIntervalSecondsDoc = "The frequency with which the partition rebalance check is triggered by the controller"
  val UncleanLeaderElectionEnableDoc = "Indicates whether to enable replicas not in the ISR set to be elected as leader as a last resort, even though doing so may result in data loss"
  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetriesDoc = "Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens"
  val ControlledShutdownRetryBackoffMsDoc = "Before each retry, the system needs time to recover from the state that caused the previous failure (Controller fail over, replica lag etc). This config determines the amount of time to wait before retrying."
  val ControlledShutdownEnableDoc = "Enable controlled shutdown of the server"
  /** ********* Offset management configuration ***********/
  val OffsetMetadataMaxSizeDoc = "The maximum size for a metadata entry associated with an offset commit"
  val OffsetsLoadBufferSizeDoc = "Batch size for reading from the offsets segments when loading offsets into the cache."
  val OffsetsTopicReplicationFactorDoc = "The replication factor for the offsets topic (set higher to ensure availability). " +
    "To ensure that the effective replication factor of the offsets topic is the configured value, " +
    "the number of alive brokers has to be at least the replication factor at the time of the " +
    "first request for the offsets topic. If not, either the offsets topic creation will fail or " +
    "it will get a replication factor of min(alive brokers, configured replication factor)"
  val OffsetsTopicPartitionsDoc = "The number of partitions for the offset commit topic (should not change after deployment)"
  val OffsetsTopicSegmentBytesDoc = "The offsets topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads"
  val OffsetsTopicCompressionCodecDoc = "Compression codec for the offsets topic - compression may be used to achieve \"atomic\" commits"
  val OffsetsRetentionMinutesDoc = "Log retention window in minutes for offsets topic"
  val OffsetsRetentionCheckIntervalMsDoc = "Frequency at which to check for stale offsets"
  val OffsetCommitTimeoutMsDoc = "Offset commit will be delayed until all replicas for the offsets topic receive the commit " +
    "or this timeout is reached. This is similar to the producer request timeout."
  val OffsetCommitRequiredAcksDoc = "The required acks before the commit can be accepted. In general, the default (-1) should not be overridden"
  val DeleteTopicEnableDoc = "Enables delete topic. Delete topic through the admin tool will have no effect if this config is turned off"
  val CompressionTypeDoc = "Specify the final compression type for a given topic. This configuration accepts the standard compression codecs " +
    "('gzip', 'snappy', lz4). It additionally accepts 'uncompressed' which is equivalent to no compression; and " +
    "'producer' which means retain the original compression codec set by the producer."


  private val configDef = {
    import ConfigDef.Range._
    import ConfigDef.ValidString._
    import ConfigDef.Type._
    import ConfigDef.Importance._
    import java.util.Arrays.asList

    new ConfigDef()

      /** ********* Zookeeper Configuration ***********/
      .define(ZkConnectProp, STRING, HIGH, ZkConnectDoc)
      .define(ZkSessionTimeoutMsProp, INT, Defaults.ZkSessionTimeoutMs, HIGH, ZkSessionTimeoutMsDoc)
      .define(ZkConnectionTimeoutMsProp, INT, HIGH, ZkConnectionTimeoutMsDoc, false)
      .define(ZkSyncTimeMsProp, INT, Defaults.ZkSyncTimeMs, LOW, ZkSyncTimeMsDoc)

      /** ********* General Configuration ***********/
      .define(MaxReservedBrokerIdProp, INT, Defaults.MaxReservedBrokerId, atLeast(0), MEDIUM, MaxReservedBrokerIdProp)
      .define(BrokerIdProp, INT, Defaults.BrokerId, HIGH, BrokerIdDoc)
      .define(MessageMaxBytesProp, INT, Defaults.MessageMaxBytes, atLeast(0), HIGH, MessageMaxBytesDoc)
      .define(NumNetworkThreadsProp, INT, Defaults.NumNetworkThreads, atLeast(1), HIGH, NumNetworkThreadsDoc)
      .define(NumIoThreadsProp, INT, Defaults.NumIoThreads, atLeast(1), HIGH, NumIoThreadsDoc)
      .define(BackgroundThreadsProp, INT, Defaults.BackgroundThreads, atLeast(1), HIGH, BackgroundThreadsDoc)
      .define(QueuedMaxRequestsProp, INT, Defaults.QueuedMaxRequests, atLeast(1), HIGH, QueuedMaxRequestsDoc)

      /** ********* Socket Server Configuration ***********/
      .define(PortProp, INT, Defaults.Port, HIGH, PortDoc)
      .define(HostNameProp, STRING, Defaults.HostName, HIGH, HostNameDoc)
      .define(AdvertisedHostNameProp, STRING, HIGH, AdvertisedHostNameDoc, false)
      .define(AdvertisedPortProp, INT, HIGH, AdvertisedPortDoc, false)
      .define(SocketSendBufferBytesProp, INT, Defaults.SocketSendBufferBytes, HIGH, SocketSendBufferBytesDoc)
      .define(SocketReceiveBufferBytesProp, INT, Defaults.SocketReceiveBufferBytes, HIGH, SocketReceiveBufferBytesDoc)
      .define(SocketRequestMaxBytesProp, INT, Defaults.SocketRequestMaxBytes, atLeast(1), HIGH, SocketRequestMaxBytesDoc)
      .define(MaxConnectionsPerIpProp, INT, Defaults.MaxConnectionsPerIp, atLeast(1), MEDIUM, MaxConnectionsPerIpDoc)
      .define(MaxConnectionsPerIpOverridesProp, STRING, Defaults.MaxConnectionsPerIpOverrides, MEDIUM, MaxConnectionsPerIpOverridesDoc)
      .define(ConnectionsMaxIdleMsProp, LONG, Defaults.ConnectionsMaxIdleMs, MEDIUM, ConnectionsMaxIdleMsDoc)

      /** ********* Log Configuration ***********/
      .define(NumPartitionsProp, INT, Defaults.NumPartitions, atLeast(1), MEDIUM, NumPartitionsDoc)
      .define(LogDirProp, STRING, Defaults.LogDir, HIGH, LogDirDoc)
      .define(LogDirsProp, STRING, HIGH, LogDirsDoc, false)
      .define(LogSegmentBytesProp, INT, Defaults.LogSegmentBytes, atLeast(Message.MinHeaderSize), HIGH, LogSegmentBytesDoc)

      .define(LogRollTimeMillisProp, LONG, HIGH, LogRollTimeMillisDoc, false)
      .define(LogRollTimeHoursProp, INT, Defaults.LogRollHours, atLeast(1), HIGH, LogRollTimeHoursDoc)

      .define(LogRollTimeJitterMillisProp, LONG, HIGH, LogRollTimeJitterMillisDoc, false)
      .define(LogRollTimeJitterHoursProp, INT, Defaults.LogRollJitterHours, atLeast(0), HIGH, LogRollTimeJitterHoursDoc)

      .define(LogRetentionTimeMillisProp, LONG, HIGH, LogRetentionTimeMillisDoc, false)
      .define(LogRetentionTimeMinutesProp, INT, HIGH, LogRetentionTimeMinsDoc, false)
      .define(LogRetentionTimeHoursProp, INT, Defaults.LogRetentionHours, atLeast(1), HIGH, LogRetentionTimeHoursDoc)

      .define(LogRetentionBytesProp, LONG, Defaults.LogRetentionBytes, HIGH, LogRetentionBytesDoc)
      .define(LogCleanupIntervalMsProp, LONG, Defaults.LogCleanupIntervalMs, atLeast(1), MEDIUM, LogCleanupIntervalMsDoc)
      .define(LogCleanupPolicyProp, STRING, Defaults.LogCleanupPolicy, in(Defaults.Compact, Defaults.Delete), MEDIUM, LogCleanupPolicyDoc)
      .define(LogCleanerThreadsProp, INT, Defaults.LogCleanerThreads, atLeast(0), MEDIUM, LogCleanerThreadsDoc)
      .define(LogCleanerIoMaxBytesPerSecondProp, DOUBLE, Defaults.LogCleanerIoMaxBytesPerSecond, MEDIUM, LogCleanerIoMaxBytesPerSecondDoc)
      .define(LogCleanerDedupeBufferSizeProp, LONG, Defaults.LogCleanerDedupeBufferSize, MEDIUM, LogCleanerDedupeBufferSizeDoc)
      .define(LogCleanerIoBufferSizeProp, INT, Defaults.LogCleanerIoBufferSize, atLeast(0), MEDIUM, LogCleanerIoBufferSizeDoc)
      .define(LogCleanerDedupeBufferLoadFactorProp, DOUBLE, Defaults.LogCleanerDedupeBufferLoadFactor, MEDIUM, LogCleanerDedupeBufferLoadFactorDoc)
      .define(LogCleanerBackoffMsProp, LONG, Defaults.LogCleanerBackoffMs, atLeast(0), MEDIUM, LogCleanerBackoffMsDoc)
      .define(LogCleanerMinCleanRatioProp, DOUBLE, Defaults.LogCleanerMinCleanRatio, MEDIUM, LogCleanerMinCleanRatioDoc)
      .define(LogCleanerEnableProp, BOOLEAN, Defaults.LogCleanerEnable, MEDIUM, LogCleanerEnableDoc)
      .define(LogCleanerDeleteRetentionMsProp, LONG, Defaults.LogCleanerDeleteRetentionMs, MEDIUM, LogCleanerDeleteRetentionMsDoc)
      .define(LogIndexSizeMaxBytesProp, INT, Defaults.LogIndexSizeMaxBytes, atLeast(4), MEDIUM, LogIndexSizeMaxBytesDoc)
      .define(LogIndexIntervalBytesProp, INT, Defaults.LogIndexIntervalBytes, atLeast(0), MEDIUM, LogIndexIntervalBytesDoc)
      .define(LogFlushIntervalMessagesProp, LONG, Defaults.LogFlushIntervalMessages, atLeast(1), HIGH, LogFlushIntervalMessagesDoc)
      .define(LogDeleteDelayMsProp, LONG, Defaults.LogDeleteDelayMs, atLeast(0), HIGH, LogDeleteDelayMsDoc)
      .define(LogFlushSchedulerIntervalMsProp, LONG, Defaults.LogFlushSchedulerIntervalMs, HIGH, LogFlushSchedulerIntervalMsDoc)
      .define(LogFlushIntervalMsProp, LONG, HIGH, LogFlushIntervalMsDoc, false)
      .define(LogFlushOffsetCheckpointIntervalMsProp, INT, Defaults.LogFlushOffsetCheckpointIntervalMs, atLeast(0), HIGH, LogFlushOffsetCheckpointIntervalMsDoc)
      .define(NumRecoveryThreadsPerDataDirProp, INT, Defaults.NumRecoveryThreadsPerDataDir, atLeast(1), HIGH, NumRecoveryThreadsPerDataDirDoc)
      .define(AutoCreateTopicsEnableProp, BOOLEAN, Defaults.AutoCreateTopicsEnable, HIGH, AutoCreateTopicsEnableDoc)
      .define(MinInSyncReplicasProp, INT, Defaults.MinInSyncReplicas, atLeast(1), HIGH, MinInSyncReplicasDoc)

      /** ********* Replication configuration ***********/
      .define(ControllerSocketTimeoutMsProp, INT, Defaults.ControllerSocketTimeoutMs, MEDIUM, ControllerSocketTimeoutMsDoc)
      .define(ControllerMessageQueueSizeProp, INT, Defaults.ControllerMessageQueueSize, MEDIUM, ControllerMessageQueueSizeDoc)
      .define(DefaultReplicationFactorProp, INT, Defaults.DefaultReplicationFactor, MEDIUM, DefaultReplicationFactorDoc)
      .define(ReplicaLagTimeMaxMsProp, LONG, Defaults.ReplicaLagTimeMaxMs, HIGH, ReplicaLagTimeMaxMsDoc)
      .define(ReplicaLagMaxMessagesProp, LONG, Defaults.ReplicaLagMaxMessages, HIGH, ReplicaLagMaxMessagesDoc)
      .define(ReplicaSocketTimeoutMsProp, INT, Defaults.ReplicaSocketTimeoutMs, HIGH, ReplicaSocketTimeoutMsDoc)
      .define(ReplicaSocketReceiveBufferBytesProp, INT, Defaults.ReplicaSocketReceiveBufferBytes, HIGH, ReplicaSocketReceiveBufferBytesDoc)
      .define(ReplicaFetchMaxBytesProp, INT, Defaults.ReplicaFetchMaxBytes, HIGH, ReplicaFetchMaxBytesDoc)
      .define(ReplicaFetchWaitMaxMsProp, INT, Defaults.ReplicaFetchWaitMaxMs, HIGH, ReplicaFetchWaitMaxMsDoc)
      .define(ReplicaFetchBackoffMsProp, INT, Defaults.ReplicaFetchBackoffMs, atLeast(0), MEDIUM, ReplicaFetchBackoffMsDoc)
      .define(ReplicaFetchMinBytesProp, INT, Defaults.ReplicaFetchMinBytes, HIGH, ReplicaFetchMinBytesDoc)
      .define(NumReplicaFetchersProp, INT, Defaults.NumReplicaFetchers, HIGH, NumReplicaFetchersDoc)
      .define(ReplicaHighWatermarkCheckpointIntervalMsProp, LONG, Defaults.ReplicaHighWatermarkCheckpointIntervalMs, HIGH, ReplicaHighWatermarkCheckpointIntervalMsDoc)
      .define(FetchPurgatoryPurgeIntervalRequestsProp, INT, Defaults.FetchPurgatoryPurgeIntervalRequests, MEDIUM, FetchPurgatoryPurgeIntervalRequestsDoc)
      .define(ProducerPurgatoryPurgeIntervalRequestsProp, INT, Defaults.ProducerPurgatoryPurgeIntervalRequests, MEDIUM, ProducerPurgatoryPurgeIntervalRequestsDoc)
      .define(AutoLeaderRebalanceEnableProp, BOOLEAN, Defaults.AutoLeaderRebalanceEnable, HIGH, AutoLeaderRebalanceEnableDoc)
      .define(LeaderImbalancePerBrokerPercentageProp, INT, Defaults.LeaderImbalancePerBrokerPercentage, HIGH, LeaderImbalancePerBrokerPercentageDoc)
      .define(LeaderImbalanceCheckIntervalSecondsProp, INT, Defaults.LeaderImbalanceCheckIntervalSeconds, HIGH, LeaderImbalanceCheckIntervalSecondsDoc)
      .define(UncleanLeaderElectionEnableProp, BOOLEAN, Defaults.UncleanLeaderElectionEnable, HIGH, UncleanLeaderElectionEnableDoc)

      /** ********* Controlled shutdown configuration ***********/
      .define(ControlledShutdownMaxRetriesProp, INT, Defaults.ControlledShutdownMaxRetries, MEDIUM, ControlledShutdownMaxRetriesDoc)
      .define(ControlledShutdownRetryBackoffMsProp, INT, Defaults.ControlledShutdownRetryBackoffMs, MEDIUM, ControlledShutdownRetryBackoffMsDoc)
      .define(ControlledShutdownEnableProp, BOOLEAN, Defaults.ControlledShutdownEnable, MEDIUM, ControlledShutdownEnableDoc)

      /** ********* Offset management configuration ***********/
      .define(OffsetMetadataMaxSizeProp, INT, Defaults.OffsetMetadataMaxSize, HIGH, OffsetMetadataMaxSizeDoc)
      .define(OffsetsLoadBufferSizeProp, INT, Defaults.OffsetsLoadBufferSize, atLeast(1), HIGH, OffsetsLoadBufferSizeDoc)
      .define(OffsetsTopicReplicationFactorProp, SHORT, Defaults.OffsetsTopicReplicationFactor, atLeast(1), HIGH, OffsetsTopicReplicationFactorDoc)
      .define(OffsetsTopicPartitionsProp, INT, Defaults.OffsetsTopicPartitions, atLeast(1), HIGH, OffsetsTopicPartitionsDoc)
      .define(OffsetsTopicSegmentBytesProp, INT, Defaults.OffsetsTopicSegmentBytes, atLeast(1), HIGH, OffsetsTopicSegmentBytesDoc)
      .define(OffsetsTopicCompressionCodecProp, INT, Defaults.OffsetsTopicCompressionCodec, HIGH, OffsetsTopicCompressionCodecDoc)
      .define(OffsetsRetentionMinutesProp, INT, Defaults.OffsetsRetentionMinutes, atLeast(1), HIGH, OffsetsRetentionMinutesDoc)
      .define(OffsetsRetentionCheckIntervalMsProp, LONG, Defaults.OffsetsRetentionCheckIntervalMs, atLeast(1), HIGH, OffsetsRetentionCheckIntervalMsDoc)
      .define(OffsetCommitTimeoutMsProp, INT, Defaults.OffsetCommitTimeoutMs, atLeast(1), HIGH, OffsetCommitTimeoutMsDoc)
      .define(OffsetCommitRequiredAcksProp, SHORT, Defaults.OffsetCommitRequiredAcks, HIGH, OffsetCommitRequiredAcksDoc)
      .define(DeleteTopicEnableProp, BOOLEAN, Defaults.DeleteTopicEnable, HIGH, DeleteTopicEnableDoc)
      .define(CompressionTypeProp, STRING, Defaults.CompressionType, HIGH, CompressionTypeDoc)
  }

  def configNames() = {
    import scala.collection.JavaConversions._
    configDef.names().toList.sorted
  }

  /**
   * Parse the given properties instance into a KafkaConfig object
   */
  def fromProps(props: Properties): KafkaConfig = {
    import kafka.utils.Utils.evaluateDefaults
    val parsed = configDef.parse(evaluateDefaults(props))
    new KafkaConfig(
      /** ********* Zookeeper Configuration ***********/
      zkConnect = parsed.get(ZkConnectProp).asInstanceOf[String],
      zkSessionTimeoutMs = parsed.get(ZkSessionTimeoutMsProp).asInstanceOf[Int],
      _zkConnectionTimeoutMs = Option(parsed.get(ZkConnectionTimeoutMsProp)).map(_.asInstanceOf[Int]),
      zkSyncTimeMs = parsed.get(ZkSyncTimeMsProp).asInstanceOf[Int],

      /** ********* General Configuration ***********/
      maxReservedBrokerId = parsed.get(MaxReservedBrokerIdProp).asInstanceOf[Int],
      brokerId = parsed.get(BrokerIdProp).asInstanceOf[Int],
      messageMaxBytes = parsed.get(MessageMaxBytesProp).asInstanceOf[Int],
      numNetworkThreads = parsed.get(NumNetworkThreadsProp).asInstanceOf[Int],
      numIoThreads = parsed.get(NumIoThreadsProp).asInstanceOf[Int],
      backgroundThreads = parsed.get(BackgroundThreadsProp).asInstanceOf[Int],
      queuedMaxRequests = parsed.get(QueuedMaxRequestsProp).asInstanceOf[Int],

      /** ********* Socket Server Configuration ***********/
      port = parsed.get(PortProp).asInstanceOf[Int],
      hostName = parsed.get(HostNameProp).asInstanceOf[String],
      _advertisedHostName = Option(parsed.get(AdvertisedHostNameProp)).map(_.asInstanceOf[String]),
      _advertisedPort = Option(parsed.get(AdvertisedPortProp)).map(_.asInstanceOf[Int]),
      socketSendBufferBytes = parsed.get(SocketSendBufferBytesProp).asInstanceOf[Int],
      socketReceiveBufferBytes = parsed.get(SocketReceiveBufferBytesProp).asInstanceOf[Int],
      socketRequestMaxBytes = parsed.get(SocketRequestMaxBytesProp).asInstanceOf[Int],
      maxConnectionsPerIp = parsed.get(MaxConnectionsPerIpProp).asInstanceOf[Int],
      _maxConnectionsPerIpOverrides = parsed.get(MaxConnectionsPerIpOverridesProp).asInstanceOf[String],
      connectionsMaxIdleMs = parsed.get(ConnectionsMaxIdleMsProp).asInstanceOf[Long],

      /** ********* Log Configuration ***********/
      numPartitions = parsed.get(NumPartitionsProp).asInstanceOf[Int],
      _logDir = parsed.get(LogDirProp).asInstanceOf[String],
      _logDirs = Option(parsed.get(LogDirsProp)).map(_.asInstanceOf[String]),

      logSegmentBytes = parsed.get(LogSegmentBytesProp).asInstanceOf[Int],
      logRollTimeHours = parsed.get(LogRollTimeHoursProp).asInstanceOf[Int],
      _logRollTimeMillis = Option(parsed.get(LogRollTimeMillisProp)).map(_.asInstanceOf[Long]),

      logRollTimeJitterHours = parsed.get(LogRollTimeJitterHoursProp).asInstanceOf[Int],
      _logRollTimeJitterMillis = Option(parsed.get(LogRollTimeJitterMillisProp)).map(_.asInstanceOf[Long]),

      logRetentionTimeHours = parsed.get(LogRetentionTimeHoursProp).asInstanceOf[Int],
      _logRetentionTimeMins = Option(parsed.get(LogRetentionTimeMinutesProp)).map(_.asInstanceOf[Int]),
      _logRetentionTimeMillis = Option(parsed.get(LogRetentionTimeMillisProp)).map(_.asInstanceOf[Long]),

      logRetentionBytes = parsed.get(LogRetentionBytesProp).asInstanceOf[Long],
      logCleanupIntervalMs = parsed.get(LogCleanupIntervalMsProp).asInstanceOf[Long],
      logCleanupPolicy = parsed.get(LogCleanupPolicyProp).asInstanceOf[String],
      logCleanerThreads = parsed.get(LogCleanerThreadsProp).asInstanceOf[Int],
      logCleanerIoMaxBytesPerSecond = parsed.get(LogCleanerIoMaxBytesPerSecondProp).asInstanceOf[Double],
      logCleanerDedupeBufferSize = parsed.get(LogCleanerDedupeBufferSizeProp).asInstanceOf[Long],
      logCleanerIoBufferSize = parsed.get(LogCleanerIoBufferSizeProp).asInstanceOf[Int],
      logCleanerDedupeBufferLoadFactor = parsed.get(LogCleanerDedupeBufferLoadFactorProp).asInstanceOf[Double],
      logCleanerBackoffMs = parsed.get(LogCleanerBackoffMsProp).asInstanceOf[Long],
      logCleanerMinCleanRatio = parsed.get(LogCleanerMinCleanRatioProp).asInstanceOf[Double],
      logCleanerEnable = parsed.get(LogCleanerEnableProp).asInstanceOf[Boolean],
      logCleanerDeleteRetentionMs = parsed.get(LogCleanerDeleteRetentionMsProp).asInstanceOf[Long],
      logIndexSizeMaxBytes = parsed.get(LogIndexSizeMaxBytesProp).asInstanceOf[Int],
      logIndexIntervalBytes = parsed.get(LogIndexIntervalBytesProp).asInstanceOf[Int],
      logFlushIntervalMessages = parsed.get(LogFlushIntervalMessagesProp).asInstanceOf[Long],
      logDeleteDelayMs = parsed.get(LogDeleteDelayMsProp).asInstanceOf[Long],
      logFlushSchedulerIntervalMs = parsed.get(LogFlushSchedulerIntervalMsProp).asInstanceOf[Long],
      _logFlushIntervalMs = Option(parsed.get(LogFlushIntervalMsProp)).map(_.asInstanceOf[Long]),
      logFlushOffsetCheckpointIntervalMs = parsed.get(LogFlushOffsetCheckpointIntervalMsProp).asInstanceOf[Int],
      numRecoveryThreadsPerDataDir = parsed.get(NumRecoveryThreadsPerDataDirProp).asInstanceOf[Int],
      autoCreateTopicsEnable = parsed.get(AutoCreateTopicsEnableProp).asInstanceOf[Boolean],
      minInSyncReplicas = parsed.get(MinInSyncReplicasProp).asInstanceOf[Int],

      /** ********* Replication configuration ***********/
      controllerSocketTimeoutMs = parsed.get(ControllerSocketTimeoutMsProp).asInstanceOf[Int],
      controllerMessageQueueSize = parsed.get(ControllerMessageQueueSizeProp).asInstanceOf[Int],
      defaultReplicationFactor = parsed.get(DefaultReplicationFactorProp).asInstanceOf[Int],
      replicaLagTimeMaxMs = parsed.get(ReplicaLagTimeMaxMsProp).asInstanceOf[Long],
      replicaLagMaxMessages = parsed.get(ReplicaLagMaxMessagesProp).asInstanceOf[Long],
      replicaSocketTimeoutMs = parsed.get(ReplicaSocketTimeoutMsProp).asInstanceOf[Int],
      replicaSocketReceiveBufferBytes = parsed.get(ReplicaSocketReceiveBufferBytesProp).asInstanceOf[Int],
      replicaFetchMaxBytes = parsed.get(ReplicaFetchMaxBytesProp).asInstanceOf[Int],
      replicaFetchWaitMaxMs = parsed.get(ReplicaFetchWaitMaxMsProp).asInstanceOf[Int],
      replicaFetchMinBytes = parsed.get(ReplicaFetchMinBytesProp).asInstanceOf[Int],
      replicaFetchBackoffMs = parsed.get(ReplicaFetchBackoffMsProp).asInstanceOf[Int],
      numReplicaFetchers = parsed.get(NumReplicaFetchersProp).asInstanceOf[Int],
      replicaHighWatermarkCheckpointIntervalMs = parsed.get(ReplicaHighWatermarkCheckpointIntervalMsProp).asInstanceOf[Long],
      fetchPurgatoryPurgeIntervalRequests = parsed.get(FetchPurgatoryPurgeIntervalRequestsProp).asInstanceOf[Int],
      producerPurgatoryPurgeIntervalRequests = parsed.get(ProducerPurgatoryPurgeIntervalRequestsProp).asInstanceOf[Int],
      autoLeaderRebalanceEnable = parsed.get(AutoLeaderRebalanceEnableProp).asInstanceOf[Boolean],
      leaderImbalancePerBrokerPercentage = parsed.get(LeaderImbalancePerBrokerPercentageProp).asInstanceOf[Int],
      leaderImbalanceCheckIntervalSeconds = parsed.get(LeaderImbalanceCheckIntervalSecondsProp).asInstanceOf[Int],
      uncleanLeaderElectionEnable = parsed.get(UncleanLeaderElectionEnableProp).asInstanceOf[Boolean],

      /** ********* Controlled shutdown configuration ***********/
      controlledShutdownMaxRetries = parsed.get(ControlledShutdownMaxRetriesProp).asInstanceOf[Int],
      controlledShutdownRetryBackoffMs = parsed.get(ControlledShutdownRetryBackoffMsProp).asInstanceOf[Int],
      controlledShutdownEnable = parsed.get(ControlledShutdownEnableProp).asInstanceOf[Boolean],

      /** ********* Offset management configuration ***********/
      offsetMetadataMaxSize = parsed.get(OffsetMetadataMaxSizeProp).asInstanceOf[Int],
      offsetsLoadBufferSize = parsed.get(OffsetsLoadBufferSizeProp).asInstanceOf[Int],
      offsetsTopicReplicationFactor = parsed.get(OffsetsTopicReplicationFactorProp).asInstanceOf[Short],
      offsetsTopicPartitions = parsed.get(OffsetsTopicPartitionsProp).asInstanceOf[Int],
      offsetsTopicSegmentBytes = parsed.get(OffsetsTopicSegmentBytesProp).asInstanceOf[Int],
      offsetsTopicCompressionCodec = Option(parsed.get(OffsetsTopicCompressionCodecProp)).map(_.asInstanceOf[Int]).map(value => CompressionCodec.getCompressionCodec(value)).orNull,
      offsetsRetentionMinutes = parsed.get(OffsetsRetentionMinutesProp).asInstanceOf[Int],
      offsetsRetentionCheckIntervalMs = parsed.get(OffsetsRetentionCheckIntervalMsProp).asInstanceOf[Long],
      offsetCommitTimeoutMs = parsed.get(OffsetCommitTimeoutMsProp).asInstanceOf[Int],
      offsetCommitRequiredAcks = parsed.get(OffsetCommitRequiredAcksProp).asInstanceOf[Short],
      deleteTopicEnable = parsed.get(DeleteTopicEnableProp).asInstanceOf[Boolean],
      compressionType = parsed.get(CompressionTypeProp).asInstanceOf[String]
    )
  }

  /**
   * Create a log config instance using the given properties and defaults
   */
  def fromProps(defaults: Properties, overrides: Properties): KafkaConfig = {
    val props = new Properties(defaults)
    props.putAll(overrides)
    fromProps(props)
  }

  /**
   * Check that property names are valid
   */
  def validateNames(props: Properties) {
    import JavaConversions._
    val names = configDef.names()
    for (name <- props.keys)
      require(names.contains(name), "Unknown configuration \"%s\".".format(name))
  }

  /**
   * Check that the given properties contain only valid kafka config names and that all values can be parsed and are valid
   */
  def validate(props: Properties) {
    validateNames(props)
    configDef.parse(props)

    // to bootstrap KafkaConfig.validateValues()
    KafkaConfig.fromProps(props)
  }
}

class KafkaConfig(/** ********* Zookeeper Configuration ***********/
                  val zkConnect: String,
                  val zkSessionTimeoutMs: Int = Defaults.ZkSessionTimeoutMs,
                  private val _zkConnectionTimeoutMs: Option[Int] = None,
                  val zkSyncTimeMs: Int = Defaults.ZkSyncTimeMs,

                  /** ********* General Configuration ***********/
                  val maxReservedBrokerId: Int = Defaults.MaxReservedBrokerId,
                  var brokerId: Int = Defaults.BrokerId,
                  val messageMaxBytes: Int = Defaults.MessageMaxBytes,
                  val numNetworkThreads: Int = Defaults.NumNetworkThreads,
                  val numIoThreads: Int = Defaults.NumIoThreads,
                  val backgroundThreads: Int = Defaults.BackgroundThreads,
                  val queuedMaxRequests: Int = Defaults.QueuedMaxRequests,

                  /** ********* Socket Server Configuration ***********/
                  val port: Int = Defaults.Port,
                  val hostName: String = Defaults.HostName,
                  private val _advertisedHostName: Option[String] = None,
                  private val _advertisedPort: Option[Int] = None,
                  val socketSendBufferBytes: Int = Defaults.SocketSendBufferBytes,
                  val socketReceiveBufferBytes: Int = Defaults.SocketReceiveBufferBytes,
                  val socketRequestMaxBytes: Int = Defaults.SocketRequestMaxBytes,
                  val maxConnectionsPerIp: Int = Defaults.MaxConnectionsPerIp,
                  private val _maxConnectionsPerIpOverrides: String = Defaults.MaxConnectionsPerIpOverrides,
                  val connectionsMaxIdleMs: Long = Defaults.ConnectionsMaxIdleMs,

                  /** ********* Log Configuration ***********/
                  val numPartitions: Int = Defaults.NumPartitions,
                  private val _logDir: String = Defaults.LogDir,
                  private val _logDirs: Option[String] = None,

                  val logSegmentBytes: Int = Defaults.LogSegmentBytes,

                  val logRollTimeHours: Int = Defaults.LogRollHours,
                  private val _logRollTimeMillis: Option[Long] = None,

                  val logRollTimeJitterHours: Int = Defaults.LogRollJitterHours,
                  private val _logRollTimeJitterMillis: Option[Long] = None,

                  val logRetentionTimeHours: Int = Defaults.LogRetentionHours,
                  private val _logRetentionTimeMins: Option[Int] = None,
                  private val _logRetentionTimeMillis: Option[Long] = None,

                  val logRetentionBytes: Long = Defaults.LogRetentionBytes,
                  val logCleanupIntervalMs: Long = Defaults.LogCleanupIntervalMs,
                  val logCleanupPolicy: String = Defaults.LogCleanupPolicy,
                  val logCleanerThreads: Int = Defaults.LogCleanerThreads,
                  val logCleanerIoMaxBytesPerSecond: Double = Defaults.LogCleanerIoMaxBytesPerSecond,
                  val logCleanerDedupeBufferSize: Long = Defaults.LogCleanerDedupeBufferSize,
                  val logCleanerIoBufferSize: Int = Defaults.LogCleanerIoBufferSize,
                  val logCleanerDedupeBufferLoadFactor: Double = Defaults.LogCleanerDedupeBufferLoadFactor,
                  val logCleanerBackoffMs: Long = Defaults.LogCleanerBackoffMs,
                  val logCleanerMinCleanRatio: Double = Defaults.LogCleanerMinCleanRatio,
                  val logCleanerEnable: Boolean = Defaults.LogCleanerEnable,
                  val logCleanerDeleteRetentionMs: Long = Defaults.LogCleanerDeleteRetentionMs,
                  val logIndexSizeMaxBytes: Int = Defaults.LogIndexSizeMaxBytes,
                  val logIndexIntervalBytes: Int = Defaults.LogIndexIntervalBytes,
                  val logFlushIntervalMessages: Long = Defaults.LogFlushIntervalMessages,
                  val logDeleteDelayMs: Long = Defaults.LogDeleteDelayMs,
                  val logFlushSchedulerIntervalMs: Long = Defaults.LogFlushSchedulerIntervalMs,
                  private val _logFlushIntervalMs: Option[Long] = None,
                  val logFlushOffsetCheckpointIntervalMs: Int = Defaults.LogFlushOffsetCheckpointIntervalMs,
                  val numRecoveryThreadsPerDataDir: Int = Defaults.NumRecoveryThreadsPerDataDir,
                  val autoCreateTopicsEnable: Boolean = Defaults.AutoCreateTopicsEnable,

                  val minInSyncReplicas: Int = Defaults.MinInSyncReplicas,

                  /** ********* Replication configuration ***********/
                  val controllerSocketTimeoutMs: Int = Defaults.ControllerSocketTimeoutMs,
                  val controllerMessageQueueSize: Int = Defaults.ControllerMessageQueueSize,
                  val defaultReplicationFactor: Int = Defaults.DefaultReplicationFactor,
                  val replicaLagTimeMaxMs: Long = Defaults.ReplicaLagTimeMaxMs,
                  val replicaLagMaxMessages: Long = Defaults.ReplicaLagMaxMessages,
                  val replicaSocketTimeoutMs: Int = Defaults.ReplicaSocketTimeoutMs,
                  val replicaSocketReceiveBufferBytes: Int = Defaults.ReplicaSocketReceiveBufferBytes,
                  val replicaFetchMaxBytes: Int = Defaults.ReplicaFetchMaxBytes,
                  val replicaFetchWaitMaxMs: Int = Defaults.ReplicaFetchWaitMaxMs,
                  val replicaFetchMinBytes: Int = Defaults.ReplicaFetchMinBytes,
                  val replicaFetchBackoffMs: Int = Defaults.ReplicaFetchBackoffMs,
                  val numReplicaFetchers: Int = Defaults.NumReplicaFetchers,
                  val replicaHighWatermarkCheckpointIntervalMs: Long = Defaults.ReplicaHighWatermarkCheckpointIntervalMs,
                  val fetchPurgatoryPurgeIntervalRequests: Int = Defaults.FetchPurgatoryPurgeIntervalRequests,
                  val producerPurgatoryPurgeIntervalRequests: Int = Defaults.ProducerPurgatoryPurgeIntervalRequests,
                  val autoLeaderRebalanceEnable: Boolean = Defaults.AutoLeaderRebalanceEnable,
                  val leaderImbalancePerBrokerPercentage: Int = Defaults.LeaderImbalancePerBrokerPercentage,
                  val leaderImbalanceCheckIntervalSeconds: Int = Defaults.LeaderImbalanceCheckIntervalSeconds,
                  val uncleanLeaderElectionEnable: Boolean = Defaults.UncleanLeaderElectionEnable,

                  /** ********* Controlled shutdown configuration ***********/
                  val controlledShutdownMaxRetries: Int = Defaults.ControlledShutdownMaxRetries,
                  val controlledShutdownRetryBackoffMs: Int = Defaults.ControlledShutdownRetryBackoffMs,
                  val controlledShutdownEnable: Boolean = Defaults.ControlledShutdownEnable,

                  /** ********* Offset management configuration ***********/
                  val offsetMetadataMaxSize: Int = Defaults.OffsetMetadataMaxSize,
                  val offsetsLoadBufferSize: Int = Defaults.OffsetsLoadBufferSize,
                  val offsetsTopicReplicationFactor: Short = Defaults.OffsetsTopicReplicationFactor,
                  val offsetsTopicPartitions: Int = Defaults.OffsetsTopicPartitions,
                  val offsetsTopicSegmentBytes: Int = Defaults.OffsetsTopicSegmentBytes,
                  val offsetsTopicCompressionCodec: CompressionCodec = CompressionCodec.getCompressionCodec(Defaults.OffsetsTopicCompressionCodec),
                  val offsetsRetentionMinutes: Int = Defaults.OffsetsRetentionMinutes,
                  val offsetsRetentionCheckIntervalMs: Long = Defaults.OffsetsRetentionCheckIntervalMs,
                  val offsetCommitTimeoutMs: Int = Defaults.OffsetCommitTimeoutMs,
                  val offsetCommitRequiredAcks: Short = Defaults.OffsetCommitRequiredAcks,

                  val deleteTopicEnable: Boolean = Defaults.DeleteTopicEnable,
                  val compressionType: String = Defaults.CompressionType
                   ) {

  val zkConnectionTimeoutMs: Int = _zkConnectionTimeoutMs.getOrElse(zkSessionTimeoutMs)

  val advertisedHostName: String = _advertisedHostName.getOrElse(hostName)
  val advertisedPort: Int = _advertisedPort.getOrElse(port)
  val logDirs = Utils.parseCsvList(_logDirs.getOrElse(_logDir))

  val logRollTimeMillis = _logRollTimeMillis.getOrElse(60 * 60 * 1000L * logRollTimeHours)
  val logRollTimeJitterMillis = _logRollTimeJitterMillis.getOrElse(60 * 60 * 1000L * logRollTimeJitterHours)
  val logRetentionTimeMillis = getLogRetentionTimeMillis

  val logFlushIntervalMs = _logFlushIntervalMs.getOrElse(logFlushSchedulerIntervalMs)

  private def getMap(propName: String, propValue: String): Map[String, String] = {
    try {
      Utils.parseCsvMap(propValue)
    } catch {
      case e: Exception => throw new IllegalArgumentException("Error parsing configuration property '%s': %s".format(propName, e.getMessage))
    }
  }

  val maxConnectionsPerIpOverrides: Map[String, Int] =
    getMap(KafkaConfig.MaxConnectionsPerIpOverridesProp, _maxConnectionsPerIpOverrides).map { case (k, v) => (k, v.toInt)}

  private def getLogRetentionTimeMillis: Long = {
    val millisInMinute = 60L * 1000L
    val millisInHour = 60L * millisInMinute

    _logRetentionTimeMillis.getOrElse(
      _logRetentionTimeMins match {
        case Some(mins) => millisInMinute * mins
        case None => millisInHour * logRetentionTimeHours
      }
    )
  }

  validateValues()

  private def validateValues() {
    require(brokerId >= -1 && brokerId <= maxReservedBrokerId, "broker.id must be equal or greater than -1 and not greater than reserved.broker.max.id")
    require(logRollTimeMillis >= 1, "log.roll.ms must be equal or greater than 1")
    require(logRollTimeJitterMillis >= 0, "log.roll.jitter.ms must be equal or greater than 0")
    require(logRetentionTimeMillis >= 1, "log.retention.ms must be equal or greater than 1")
    require(_logRetentionTimeMins.forall(_ >= 1), "log.retention.minutes must be equal or greater than 1")

    require(logDirs.size > 0)
    require(logCleanerDedupeBufferSize / logCleanerThreads > 1024 * 1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.")
    require(replicaFetchWaitMaxMs <= replicaSocketTimeoutMs, "replica.socket.timeout.ms should always be at least replica.fetch.wait.max.ms" +
      " to prevent unnecessary socket timeouts")
    require(replicaFetchMaxBytes >= messageMaxBytes, "replica.fetch.max.bytes should be equal or greater than message.max.bytes")
    require(replicaFetchWaitMaxMs <= replicaLagTimeMaxMs, "replica.fetch.wait.max.ms should always be at least replica.lag.time.max.ms" +
      " to prevent frequent changes in ISR")
    require(offsetCommitRequiredAcks >= -1 && offsetCommitRequiredAcks <= offsetsTopicReplicationFactor,
      "offsets.commit.required.acks must be greater or equal -1 and less or equal to offsets.topic.replication.factor")
    require(BrokerCompressionCodec.isValid(compressionType), "compression.type : " + compressionType + " is not valid." +
      " Valid options are " + BrokerCompressionCodec.brokerCompressionOptions.mkString(","))
  }

  def toProps: Properties = {
    val props = new Properties()
    import kafka.server.KafkaConfig._
    /** ********* Zookeeper Configuration ***********/
    props.put(ZkConnectProp, zkConnect)
    props.put(ZkSessionTimeoutMsProp, zkSessionTimeoutMs.toString)
    _zkConnectionTimeoutMs.foreach(value => props.put(ZkConnectionTimeoutMsProp, value.toString))
    props.put(ZkSyncTimeMsProp, zkSyncTimeMs.toString)

    /** ********* General Configuration ***********/
    props.put(MaxReservedBrokerIdProp, maxReservedBrokerId.toString)
    props.put(BrokerIdProp, brokerId.toString)
    props.put(MessageMaxBytesProp, messageMaxBytes.toString)
    props.put(NumNetworkThreadsProp, numNetworkThreads.toString)
    props.put(NumIoThreadsProp, numIoThreads.toString)
    props.put(BackgroundThreadsProp, backgroundThreads.toString)
    props.put(QueuedMaxRequestsProp, queuedMaxRequests.toString)

    /** ********* Socket Server Configuration ***********/
    props.put(PortProp, port.toString)
    props.put(HostNameProp, hostName)
    _advertisedHostName.foreach(props.put(AdvertisedHostNameProp, _))
    _advertisedPort.foreach(value => props.put(AdvertisedPortProp, value.toString))
    props.put(SocketSendBufferBytesProp, socketSendBufferBytes.toString)
    props.put(SocketReceiveBufferBytesProp, socketReceiveBufferBytes.toString)
    props.put(SocketRequestMaxBytesProp, socketRequestMaxBytes.toString)
    props.put(MaxConnectionsPerIpProp, maxConnectionsPerIp.toString)
    props.put(MaxConnectionsPerIpOverridesProp, _maxConnectionsPerIpOverrides)
    props.put(ConnectionsMaxIdleMsProp, connectionsMaxIdleMs.toString)

    /** ********* Log Configuration ***********/
    props.put(NumPartitionsProp, numPartitions.toString)
    props.put(LogDirProp, _logDir)
    _logDirs.foreach(value => props.put(LogDirsProp, value))
    props.put(LogSegmentBytesProp, logSegmentBytes.toString)

    props.put(LogRollTimeHoursProp, logRollTimeHours.toString)
    _logRollTimeMillis.foreach(v => props.put(LogRollTimeMillisProp, v.toString))

    props.put(LogRollTimeJitterHoursProp, logRollTimeJitterHours.toString)
    _logRollTimeJitterMillis.foreach(v => props.put(LogRollTimeJitterMillisProp, v.toString))


    props.put(LogRetentionTimeHoursProp, logRetentionTimeHours.toString)
    _logRetentionTimeMins.foreach(v => props.put(LogRetentionTimeMinutesProp, v.toString))
    _logRetentionTimeMillis.foreach(v => props.put(LogRetentionTimeMillisProp, v.toString))

    props.put(LogRetentionBytesProp, logRetentionBytes.toString)
    props.put(LogCleanupIntervalMsProp, logCleanupIntervalMs.toString)
    props.put(LogCleanupPolicyProp, logCleanupPolicy)
    props.put(LogCleanerThreadsProp, logCleanerThreads.toString)
    props.put(LogCleanerIoMaxBytesPerSecondProp, logCleanerIoMaxBytesPerSecond.toString)
    props.put(LogCleanerDedupeBufferSizeProp, logCleanerDedupeBufferSize.toString)
    props.put(LogCleanerIoBufferSizeProp, logCleanerIoBufferSize.toString)
    props.put(LogCleanerDedupeBufferLoadFactorProp, logCleanerDedupeBufferLoadFactor.toString)
    props.put(LogCleanerBackoffMsProp, logCleanerBackoffMs.toString)
    props.put(LogCleanerMinCleanRatioProp, logCleanerMinCleanRatio.toString)
    props.put(LogCleanerEnableProp, logCleanerEnable.toString)
    props.put(LogCleanerDeleteRetentionMsProp, logCleanerDeleteRetentionMs.toString)
    props.put(LogIndexSizeMaxBytesProp, logIndexSizeMaxBytes.toString)
    props.put(LogIndexIntervalBytesProp, logIndexIntervalBytes.toString)
    props.put(LogFlushIntervalMessagesProp, logFlushIntervalMessages.toString)
    props.put(LogDeleteDelayMsProp, logDeleteDelayMs.toString)
    props.put(LogFlushSchedulerIntervalMsProp, logFlushSchedulerIntervalMs.toString)
    _logFlushIntervalMs.foreach(v => props.put(LogFlushIntervalMsProp, v.toString))
    props.put(LogFlushOffsetCheckpointIntervalMsProp, logFlushOffsetCheckpointIntervalMs.toString)
    props.put(NumRecoveryThreadsPerDataDirProp, numRecoveryThreadsPerDataDir.toString)
    props.put(AutoCreateTopicsEnableProp, autoCreateTopicsEnable.toString)
    props.put(MinInSyncReplicasProp, minInSyncReplicas.toString)

    /** ********* Replication configuration ***********/
    props.put(ControllerSocketTimeoutMsProp, controllerSocketTimeoutMs.toString)
    props.put(ControllerMessageQueueSizeProp, controllerMessageQueueSize.toString)
    props.put(DefaultReplicationFactorProp, defaultReplicationFactor.toString)
    props.put(ReplicaLagTimeMaxMsProp, replicaLagTimeMaxMs.toString)
    props.put(ReplicaLagMaxMessagesProp, replicaLagMaxMessages.toString)
    props.put(ReplicaSocketTimeoutMsProp, replicaSocketTimeoutMs.toString)
    props.put(ReplicaSocketReceiveBufferBytesProp, replicaSocketReceiveBufferBytes.toString)
    props.put(ReplicaFetchMaxBytesProp, replicaFetchMaxBytes.toString)
    props.put(ReplicaFetchWaitMaxMsProp, replicaFetchWaitMaxMs.toString)
    props.put(ReplicaFetchMinBytesProp, replicaFetchMinBytes.toString)
    props.put(ReplicaFetchBackoffMsProp, replicaFetchBackoffMs.toString)
    props.put(NumReplicaFetchersProp, numReplicaFetchers.toString)
    props.put(ReplicaHighWatermarkCheckpointIntervalMsProp, replicaHighWatermarkCheckpointIntervalMs.toString)
    props.put(FetchPurgatoryPurgeIntervalRequestsProp, fetchPurgatoryPurgeIntervalRequests.toString)
    props.put(ProducerPurgatoryPurgeIntervalRequestsProp, producerPurgatoryPurgeIntervalRequests.toString)
    props.put(AutoLeaderRebalanceEnableProp, autoLeaderRebalanceEnable.toString)
    props.put(LeaderImbalancePerBrokerPercentageProp, leaderImbalancePerBrokerPercentage.toString)
    props.put(LeaderImbalanceCheckIntervalSecondsProp, leaderImbalanceCheckIntervalSeconds.toString)
    props.put(UncleanLeaderElectionEnableProp, uncleanLeaderElectionEnable.toString)

    /** ********* Controlled shutdown configuration ***********/
    props.put(ControlledShutdownMaxRetriesProp, controlledShutdownMaxRetries.toString)
    props.put(ControlledShutdownRetryBackoffMsProp, controlledShutdownRetryBackoffMs.toString)
    props.put(ControlledShutdownEnableProp, controlledShutdownEnable.toString)

    /** ********* Offset management configuration ***********/
    props.put(OffsetMetadataMaxSizeProp, offsetMetadataMaxSize.toString)
    props.put(OffsetsLoadBufferSizeProp, offsetsLoadBufferSize.toString)
    props.put(OffsetsTopicReplicationFactorProp, offsetsTopicReplicationFactor.toString)
    props.put(OffsetsTopicPartitionsProp, offsetsTopicPartitions.toString)
    props.put(OffsetsTopicSegmentBytesProp, offsetsTopicSegmentBytes.toString)
    props.put(OffsetsTopicCompressionCodecProp, offsetsTopicCompressionCodec.codec.toString)
    props.put(OffsetsRetentionMinutesProp, offsetsRetentionMinutes.toString)
    props.put(OffsetsRetentionCheckIntervalMsProp, offsetsRetentionCheckIntervalMs.toString)
    props.put(OffsetCommitTimeoutMsProp, offsetCommitTimeoutMs.toString)
    props.put(OffsetCommitRequiredAcksProp, offsetCommitRequiredAcks.toString)
    props.put(DeleteTopicEnableProp, deleteTopicEnable.toString)
    props.put(CompressionTypeProp, compressionType.toString)

    props
  }
}
