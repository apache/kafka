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

import kafka.api.ApiVersion
import kafka.cluster.EndPoint
import kafka.consumer.ConsumerConfig
import kafka.coordinator.OffsetConfig
import kafka.message.{BrokerCompressionCodec, CompressionCodec, Message, MessageSet}
import kafka.utils.CoreUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, SslConfigs}
import org.apache.kafka.common.metrics.MetricsReporter
import org.apache.kafka.common.protocol.SecurityProtocol

import scala.collection.{Map, immutable, JavaConverters}
import JavaConverters._

object Defaults {
  /** ********* Zookeeper Configuration ***********/
  val ZkSessionTimeoutMs = 6000
  val ZkSyncTimeMs = 2000
  val ZkEnableSecureAcls = false

  /** ********* General Configuration ***********/
  val BrokerIdGenerationEnable = true
  val MaxReservedBrokerId = 1000
  val BrokerId = -1
  val MessageMaxBytes = 1000000 + MessageSet.LogOverhead
  val NumNetworkThreads = 3
  val NumIoThreads = 8
  val BackgroundThreads = 10
  val QueuedMaxRequests = 500

  /************* Authorizer Configuration ***********/
  val AuthorizerClassName = ""

  /** ********* Socket Server Configuration ***********/
  val Port = 9092
  val HostName: String = new String("")
  val SocketSendBufferBytes: Int = 100 * 1024
  val SocketReceiveBufferBytes: Int = 100 * 1024
  val SocketRequestMaxBytes: Int = 100 * 1024 * 1024
  val MaxConnectionsPerIp: Int = Int.MaxValue
  val MaxConnectionsPerIpOverrides: String = ""
  val ConnectionsMaxIdleMs = 10 * 60 * 1000L
  val RequestTimeoutMs = 30000

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
  val LogCleanerDedupeBufferSize = 128 * 1024 * 1024L
  val LogCleanerIoBufferSize = 512 * 1024
  val LogCleanerDedupeBufferLoadFactor = 0.9d
  val LogCleanerBackoffMs = 15 * 1000
  val LogCleanerMinCleanRatio = 0.5d
  val LogCleanerEnable = true
  val LogCleanerDeleteRetentionMs = 24 * 60 * 60 * 1000L
  val LogIndexSizeMaxBytes = 10 * 1024 * 1024
  val LogIndexIntervalBytes = 4096
  val LogFlushIntervalMessages = Long.MaxValue
  val LogDeleteDelayMs = 60000
  val LogFlushSchedulerIntervalMs = Long.MaxValue
  val LogFlushOffsetCheckpointIntervalMs = 60000
  val LogPreAllocateEnable = false
  val NumRecoveryThreadsPerDataDir = 1
  val AutoCreateTopicsEnable = true
  val MinInSyncReplicas = 1

  /** ********* Replication configuration ***********/
  val ControllerSocketTimeoutMs = RequestTimeoutMs
  val ControllerMessageQueueSize = Int.MaxValue
  val DefaultReplicationFactor = 1
  val ReplicaLagTimeMaxMs = 10000L
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
  val InterBrokerSecurityProtocol = SecurityProtocol.PLAINTEXT.toString
  val InterBrokerProtocolVersion = ApiVersion.latestVersion.toString

  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetries = 3
  val ControlledShutdownRetryBackoffMs = 5000
  val ControlledShutdownEnable = true

  /** ********* Consumer coordinator configuration ***********/
  val ConsumerMinSessionTimeoutMs = 6000
  val ConsumerMaxSessionTimeoutMs = 30000

  /** ********* Offset management configuration ***********/
  val OffsetMetadataMaxSize = OffsetConfig.DefaultMaxMetadataSize
  val OffsetsLoadBufferSize = OffsetConfig.DefaultLoadBufferSize
  val OffsetsTopicReplicationFactor = OffsetConfig.DefaultOffsetsTopicReplicationFactor
  val OffsetsTopicPartitions: Int = OffsetConfig.DefaultOffsetsTopicNumPartitions
  val OffsetsTopicSegmentBytes: Int = OffsetConfig.DefaultOffsetsTopicSegmentBytes
  val OffsetsTopicCompressionCodec: Int = OffsetConfig.DefaultOffsetsTopicCompressionCodec.codec
  val OffsetsRetentionMinutes: Int = 24 * 60
  val OffsetsRetentionCheckIntervalMs: Long = OffsetConfig.DefaultOffsetsRetentionCheckIntervalMs
  val OffsetCommitTimeoutMs = OffsetConfig.DefaultOffsetCommitTimeoutMs
  val OffsetCommitRequiredAcks = OffsetConfig.DefaultOffsetCommitRequiredAcks

  /** ********* Quota Configuration ***********/
  val ProducerQuotaBytesPerSecondDefault = ClientQuotaManagerConfig.QuotaBytesPerSecondDefault
  val ConsumerQuotaBytesPerSecondDefault = ClientQuotaManagerConfig.QuotaBytesPerSecondDefault
  val NumQuotaSamples: Int = ClientQuotaManagerConfig.DefaultNumQuotaSamples
  val QuotaWindowSizeSeconds: Int = ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds

  val DeleteTopicEnable = false

  val CompressionType = "producer"

  /** ********* Kafka Metrics Configuration ***********/
  val MetricNumSamples = 2
  val MetricSampleWindowMs = 30000
  val MetricReporterClasses = ""

  /** ********* SSL configuration ***********/
  val PrincipalBuilderClass = SslConfigs.DEFAULT_PRINCIPAL_BUILDER_CLASS
  val SslProtocol = SslConfigs.DEFAULT_SSL_PROTOCOL
  val SslEnabledProtocols = SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS
  val SslKeystoreType = SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE
  val SslTruststoreType = SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE
  val SslKeyManagerAlgorithm = SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM
  val SslTrustManagerAlgorithm = SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM
  val SslClientAuthRequired = "required"
  val SslClientAuthRequested = "requested"
  val SslClientAuthNone = "none"
  val SslClientAuth = SslClientAuthNone

  /** ********* Sasl configuration ***********/
  val SaslKerberosKinitCmd = SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD
  val SaslKerberosTicketRenewWindowFactor = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR
  val SaslKerberosTicketRenewJitter = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER
  val SaslKerberosMinTimeBeforeRelogin = SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN
  val SaslKerberosPrincipalToLocalRules = SaslConfigs.DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES

}

object KafkaConfig {

  def main(args: Array[String]) {
    System.out.println(configDef.toHtmlTable)
  }

  /** ********* Zookeeper Configuration ***********/
  val ZkConnectProp = "zookeeper.connect"
  val ZkSessionTimeoutMsProp = "zookeeper.session.timeout.ms"
  val ZkConnectionTimeoutMsProp = "zookeeper.connection.timeout.ms"
  val ZkSyncTimeMsProp = "zookeeper.sync.time.ms"
  val ZkEnableSecureAclsProp = "zookeeper.set.acl"
  /** ********* General Configuration ***********/
  val BrokerIdGenerationEnableProp = "broker.id.generation.enable"
  val MaxReservedBrokerIdProp = "reserved.broker.max.id"
  val BrokerIdProp = "broker.id"
  val MessageMaxBytesProp = "message.max.bytes"
  val NumNetworkThreadsProp = "num.network.threads"
  val NumIoThreadsProp = "num.io.threads"
  val BackgroundThreadsProp = "background.threads"
  val QueuedMaxRequestsProp = "queued.max.requests"
  val RequestTimeoutMsProp = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG
  /************* Authorizer Configuration ***********/
  val AuthorizerClassNameProp = "authorizer.class.name"
  /** ********* Socket Server Configuration ***********/
  val PortProp = "port"
  val HostNameProp = "host.name"
  val ListenersProp = "listeners"
  val AdvertisedHostNameProp: String = "advertised.host.name"
  val AdvertisedPortProp = "advertised.port"
  val AdvertisedListenersProp = "advertised.listeners"
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
  val LogPreAllocateProp = "log.preallocate"
  val NumRecoveryThreadsPerDataDirProp = "num.recovery.threads.per.data.dir"
  val AutoCreateTopicsEnableProp = "auto.create.topics.enable"
  val MinInSyncReplicasProp = "min.insync.replicas"
  /** ********* Replication configuration ***********/
  val ControllerSocketTimeoutMsProp = "controller.socket.timeout.ms"
  val DefaultReplicationFactorProp = "default.replication.factor"
  val ReplicaLagTimeMaxMsProp = "replica.lag.time.max.ms"
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
  val InterBrokerSecurityProtocolProp = "security.inter.broker.protocol"
  val InterBrokerProtocolVersionProp = "inter.broker.protocol.version"
  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetriesProp = "controlled.shutdown.max.retries"
  val ControlledShutdownRetryBackoffMsProp = "controlled.shutdown.retry.backoff.ms"
  val ControlledShutdownEnableProp = "controlled.shutdown.enable"
  /** ********* Group coordinator configuration ***********/
  val GroupMinSessionTimeoutMsProp = "group.min.session.timeout.ms"
  val GroupMaxSessionTimeoutMsProp = "group.max.session.timeout.ms"
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
  /** ********* Quota Configuration ***********/
  val ProducerQuotaBytesPerSecondDefaultProp = "quota.producer.default"
  val ConsumerQuotaBytesPerSecondDefaultProp = "quota.consumer.default"
  val NumQuotaSamplesProp = "quota.window.num"
  val QuotaWindowSizeSecondsProp = "quota.window.size.seconds"

  val DeleteTopicEnableProp = "delete.topic.enable"
  val CompressionTypeProp = "compression.type"

  /** ********* Kafka Metrics Configuration ***********/
  val MetricSampleWindowMsProp = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG
  val MetricNumSamplesProp: String = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG
  val MetricReporterClassesProp: String = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG

  /** ********* SSL Configuration ****************/
  val PrincipalBuilderClassProp = SslConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG
  val SslProtocolProp = SslConfigs.SSL_PROTOCOL_CONFIG
  val SslProviderProp = SslConfigs.SSL_PROVIDER_CONFIG
  val SslCipherSuitesProp = SslConfigs.SSL_CIPHER_SUITES_CONFIG
  val SslEnabledProtocolsProp = SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG
  val SslKeystoreTypeProp = SslConfigs.SSL_KEYSTORE_TYPE_CONFIG
  val SslKeystoreLocationProp = SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG
  val SslKeystorePasswordProp = SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG
  val SslKeyPasswordProp = SslConfigs.SSL_KEY_PASSWORD_CONFIG
  val SslTruststoreTypeProp = SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG
  val SslTruststoreLocationProp = SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG
  val SslTruststorePasswordProp = SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG
  val SslKeyManagerAlgorithmProp = SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG
  val SslTrustManagerAlgorithmProp = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG
  val SslEndpointIdentificationAlgorithmProp = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG
  val SslClientAuthProp = SslConfigs.SSL_CLIENT_AUTH_CONFIG

  /** ********* SASL Configuration ****************/
  val SaslKerberosServiceNameProp = SaslConfigs.SASL_KERBEROS_SERVICE_NAME
  val SaslKerberosKinitCmdProp = SaslConfigs.SASL_KERBEROS_KINIT_CMD
  val SaslKerberosTicketRenewWindowFactorProp = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR
  val SaslKerberosTicketRenewJitterProp = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER
  val SaslKerberosMinTimeBeforeReloginProp = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN
  val SaslKerberosPrincipalToLocalRulesProp = SaslConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES

  /* Documentation */
  /** ********* Zookeeper Configuration ***********/
  val ZkConnectDoc = "Zookeeper host string"
  val ZkSessionTimeoutMsDoc = "Zookeeper session timeout"
  val ZkConnectionTimeoutMsDoc = "The max time that the client waits to establish a connection to zookeeper. If not set, the value in " + ZkSessionTimeoutMsProp + " is used"
  val ZkSyncTimeMsDoc = "How far a ZK follower can be behind a ZK leader"
  val ZkEnableSecureAclsDoc = "Set client to use secure ACLs"
  /** ********* General Configuration ***********/
  val BrokerIdGenerationEnableDoc = s"Enable automatic broker id generation on the server? When enabled the value configured for $MaxReservedBrokerIdProp should be reviewed."
  val MaxReservedBrokerIdDoc = "Max number that can be used for a broker.id"
  val BrokerIdDoc = "The broker id for this server. " +
  "To avoid conflicts between zookeeper generated brokerId and user's config.brokerId " +
  "added MaxReservedBrokerId and zookeeper sequence starts from MaxReservedBrokerId + 1."
  val MessageMaxBytesDoc = "The maximum size of message that the server can receive"
  val NumNetworkThreadsDoc = "the number of network threads that the server uses for handling network requests"
  val NumIoThreadsDoc = "The number of io threads that the server uses for carrying out network requests"
  val BackgroundThreadsDoc = "The number of threads to use for various background processing tasks"
  val QueuedMaxRequestsDoc = "The number of queued requests allowed before blocking the network threads"
  val RequestTimeoutMsDoc = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC
  /************* Authorizer Configuration ***********/
  val AuthorizerClassNameDoc = "The authorizer class that should be used for authorization"
  /** ********* Socket Server Configuration ***********/
  val PortDoc = "the port to listen and accept connections on"
  val HostNameDoc = "hostname of broker. If this is set, it will only bind to this address. If this is not set, it will bind to all interfaces"
  val ListenersDoc = "Listener List - Comma-separated list of URIs we will listen on and their protocols.\n" +
  " Specify hostname as 0.0.0.0 to bind to all interfaces.\n" +
  " Leave hostname empty to bind to default interface.\n" +
  " Examples of legal listener lists:\n" +
  " PLAINTEXT://myhost:9092,TRACE://:9091\n" +
  " PLAINTEXT://0.0.0.0:9092, TRACE://localhost:9093\n"
  val AdvertisedHostNameDoc = "Hostname to publish to ZooKeeper for clients to use. In IaaS environments, this may " +
  "need to be different from the interface to which the broker binds. If this is not set, " +
  "it will use the value for \"host.name\" if configured. Otherwise " +
  "it will use the value returned from java.net.InetAddress.getCanonicalHostName()."
  val AdvertisedPortDoc = "The port to publish to ZooKeeper for clients to use. In IaaS environments, this may " +
  "need to be different from the port to which the broker binds. If this is not set, " +
  "it will publish the same port that the broker binds to."
  val AdvertisedListenersDoc = "Listeners to publish to ZooKeeper for clients to use, if different than the listeners above." +
  " In IaaS environments, this may need to be different from the interface to which the broker binds." +
  " If this is not set, the value for \"listeners\" will be used."
  val SocketSendBufferBytesDoc = "The SO_SNDBUF buffer of the socket sever sockets"
  val SocketReceiveBufferBytesDoc = "The SO_RCVBUF buffer of the socket sever sockets"
  val SocketRequestMaxBytesDoc = "The maximum number of bytes in a socket request"
  val MaxConnectionsPerIpDoc = "The maximum number of connections we allow from each ip address"
  val MaxConnectionsPerIpOverridesDoc = "Per-ip or hostname overrides to the default maximum number of connections"
  val ConnectionsMaxIdleMsDoc = "Idle connections timeout: the server socket processor threads close the connections that idle more than this"
  /** ********* Log Configuration ***********/
  val NumPartitionsDoc = "The default number of log partitions per topic"
  val LogDirDoc = "The directory in which the log data is kept (supplemental for " + LogDirsProp + " property)"
  val LogDirsDoc = "The directories in which the log data is kept. If not set, the value in " + LogDirProp + " is used"
  val LogSegmentBytesDoc = "The maximum size of a single log file"
  val LogRollTimeMillisDoc = "The maximum time before a new log segment is rolled out (in milliseconds). If not set, the value in " + LogRollTimeHoursProp + " is used"
  val LogRollTimeHoursDoc = "The maximum time before a new log segment is rolled out (in hours), secondary to " + LogRollTimeMillisProp + " property"

  val LogRollTimeJitterMillisDoc = "The maximum jitter to subtract from logRollTimeMillis (in milliseconds). If not set, the value in " + LogRollTimeJitterHoursProp + " is used"
  val LogRollTimeJitterHoursDoc = "The maximum jitter to subtract from logRollTimeMillis (in hours), secondary to " + LogRollTimeJitterMillisProp + " property"

  val LogRetentionTimeMillisDoc = "The number of milliseconds to keep a log file before deleting it (in milliseconds), If not set, the value in " + LogRetentionTimeMinutesProp + " is used"
  val LogRetentionTimeMinsDoc = "The number of minutes to keep a log file before deleting it (in minutes), secondary to " + LogRetentionTimeMillisProp + " property. If not set, the value in " + LogRetentionTimeHoursProp + " is used"
  val LogRetentionTimeHoursDoc = "The number of hours to keep a log file before deleting it (in hours), tertiary to " + LogRetentionTimeMillisProp + " property"

  val LogRetentionBytesDoc = "The maximum size of the log before deleting it"
  val LogCleanupIntervalMsDoc = "The frequency in milliseconds that the log cleaner checks whether any log is eligible for deletion"
  val LogCleanupPolicyDoc = "The default cleanup policy for segments beyond the retention window, must be either \"delete\" or \"compact\""
  val LogCleanerThreadsDoc = "The number of background threads to use for log cleaning"
  val LogCleanerIoMaxBytesPerSecondDoc = "The log cleaner will be throttled so that the sum of its read and write i/o will be less than this value on average"
  val LogCleanerDedupeBufferSizeDoc = "The total memory used for log deduplication across all cleaner threads"
  val LogCleanerIoBufferSizeDoc = "The total memory used for log cleaner I/O buffers across all cleaner threads"
  val LogCleanerDedupeBufferLoadFactorDoc = "Log cleaner dedupe buffer load factor. The percentage full the dedupe buffer can become. A higher value " +
  "will allow more log to be cleaned at once but will lead to more hash collisions"
  val LogCleanerBackoffMsDoc = "The amount of time to sleep when there are no logs to clean"
  val LogCleanerMinCleanRatioDoc = "The minimum ratio of dirty log to total log for a log to eligible for cleaning"
  val LogCleanerEnableDoc = "Enable the log cleaner process to run on the server? Should be enabled if using any topics with a cleanup.policy=compact including the internal offsets topic. If disabled those topics will not be compacted and continually grow in size."
  val LogCleanerDeleteRetentionMsDoc = "How long are delete records retained?"
  val LogIndexSizeMaxBytesDoc = "The maximum size in bytes of the offset index"
  val LogIndexIntervalBytesDoc = "The interval with which we add an entry to the offset index"
  val LogFlushIntervalMessagesDoc = "The number of messages accumulated on a log partition before messages are flushed to disk "
  val LogDeleteDelayMsDoc = "The amount of time to wait before deleting a file from the filesystem"
  val LogFlushSchedulerIntervalMsDoc = "The frequency in ms that the log flusher checks whether any log needs to be flushed to disk"
  val LogFlushIntervalMsDoc = "The maximum time in ms that a message in any topic is kept in memory before flushed to disk. If not set, the value in " + LogFlushSchedulerIntervalMsProp + " is used"
  val LogFlushOffsetCheckpointIntervalMsDoc = "The frequency with which we update the persistent record of the last flush which acts as the log recovery point"
  val LogPreAllocateEnableDoc = "Should pre allocate file when create new segment? If you are using Kafka on Windows, you probably need to set it to true."
  val NumRecoveryThreadsPerDataDirDoc = "The number of threads per data directory to be used for log recovery at startup and flushing at shutdown"
  val AutoCreateTopicsEnableDoc = "Enable auto creation of topic on the server"
  val MinInSyncReplicasDoc = "define the minimum number of replicas in ISR needed to satisfy a produce request with required.acks=-1 (or all)"
  /** ********* Replication configuration ***********/
  val ControllerSocketTimeoutMsDoc = "The socket timeout for controller-to-broker channels"
  val ControllerMessageQueueSizeDoc = "The buffer size for controller-to-broker-channels"
  val DefaultReplicationFactorDoc = "default replication factors for automatically created topics"
  val ReplicaLagTimeMaxMsDoc = "If a follower hasn't sent any fetch requests or hasn't consumed up to the leaders log end offset for at least this time," +
  " the leader will remove the follower from isr"
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
  val InterBrokerSecurityProtocolDoc = "Security protocol used to communicate between brokers. Valid values are: " +
    s"${SecurityProtocol.nonTestingValues.asScala.toSeq.map(_.name).mkString(", ")}."
  val InterBrokerProtocolVersionDoc = "Specify which version of the inter-broker protocol will be used.\n" +
  " This is typically bumped after all brokers were upgraded to a new version.\n" +
  " Example of some valid values are: 0.8.0, 0.8.1, 0.8.1.1, 0.8.2, 0.8.2.0, 0.8.2.1, 0.9.0.0, 0.9.0.1 Check ApiVersion for the full list."
  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetriesDoc = "Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens"
  val ControlledShutdownRetryBackoffMsDoc = "Before each retry, the system needs time to recover from the state that caused the previous failure (Controller fail over, replica lag etc). This config determines the amount of time to wait before retrying."
  val ControlledShutdownEnableDoc = "Enable controlled shutdown of the server"
  /** ********* Consumer coordinator configuration ***********/
  val ConsumerMinSessionTimeoutMsDoc = "The minimum allowed session timeout for registered consumers"
  val ConsumerMaxSessionTimeoutMsDoc = "The maximum allowed session timeout for registered consumers"
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
  /** ********* Quota Configuration ***********/
  val ProducerQuotaBytesPerSecondDefaultDoc = "Any producer distinguished by clientId will get throttled if it produces more bytes than this value per-second"
  val ConsumerQuotaBytesPerSecondDefaultDoc = "Any consumer distinguished by clientId/consumer group will get throttled if it fetches more bytes than this value per-second"
  val NumQuotaSamplesDoc = "The number of samples to retain in memory"
  val QuotaWindowSizeSecondsDoc = "The time span of each sample"

  val DeleteTopicEnableDoc = "Enables delete topic. Delete topic through the admin tool will have no effect if this config is turned off"
  val CompressionTypeDoc = "Specify the final compression type for a given topic. This configuration accepts the standard compression codecs " +
  "('gzip', 'snappy', lz4). It additionally accepts 'uncompressed' which is equivalent to no compression; and " +
  "'producer' which means retain the original compression codec set by the producer."

  /** ********* Kafka Metrics Configuration ***********/
  val MetricSampleWindowMsDoc = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC
  val MetricNumSamplesDoc = CommonClientConfigs.METRICS_NUM_SAMPLES_DOC
  val MetricReporterClassesDoc = CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC

  /** ********* SSL Configuration ****************/
  val PrincipalBuilderClassDoc = SslConfigs.PRINCIPAL_BUILDER_CLASS_DOC
  val SslProtocolDoc = SslConfigs.SSL_PROTOCOL_DOC
  val SslProviderDoc = SslConfigs.SSL_PROVIDER_DOC
  val SslCipherSuitesDoc = SslConfigs.SSL_CIPHER_SUITES_DOC
  val SslEnabledProtocolsDoc = SslConfigs.SSL_ENABLED_PROTOCOLS_DOC
  val SslKeystoreTypeDoc = SslConfigs.SSL_KEYSTORE_TYPE_DOC
  val SslKeystoreLocationDoc = SslConfigs.SSL_KEYSTORE_LOCATION_DOC
  val SslKeystorePasswordDoc = SslConfigs.SSL_KEYSTORE_PASSWORD_DOC
  val SslKeyPasswordDoc = SslConfigs.SSL_KEY_PASSWORD_DOC
  val SslTruststoreTypeDoc = SslConfigs.SSL_TRUSTSTORE_TYPE_DOC
  val SslTruststorePasswordDoc = SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC
  val SslTruststoreLocationDoc = SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC
  val SslKeyManagerAlgorithmDoc = SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC
  val SslTrustManagerAlgorithmDoc = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC
  val SslEndpointIdentificationAlgorithmDoc = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC
  val SslClientAuthDoc = SslConfigs.SSL_CLIENT_AUTH_DOC

  /** ********* Sasl Configuration ****************/
  val SaslKerberosServiceNameDoc = SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC
  val SaslKerberosKinitCmdDoc = SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC
  val SaslKerberosTicketRenewWindowFactorDoc = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC
  val SaslKerberosTicketRenewJitterDoc = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC
  val SaslKerberosMinTimeBeforeReloginDoc = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC
  val SaslKerberosPrincipalToLocalRulesDoc = SaslConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC

  private val configDef = {
    import ConfigDef.Importance._
    import ConfigDef.Range._
    import ConfigDef.Type._
    import ConfigDef.ValidString._

    new ConfigDef()

      /** ********* Zookeeper Configuration ***********/
      .define(ZkConnectProp, STRING, HIGH, ZkConnectDoc)
      .define(ZkSessionTimeoutMsProp, INT, Defaults.ZkSessionTimeoutMs, HIGH, ZkSessionTimeoutMsDoc)
      .define(ZkConnectionTimeoutMsProp, INT, null, HIGH, ZkConnectionTimeoutMsDoc)
      .define(ZkSyncTimeMsProp, INT, Defaults.ZkSyncTimeMs, LOW, ZkSyncTimeMsDoc)
      .define(ZkEnableSecureAclsProp, BOOLEAN, Defaults.ZkEnableSecureAcls, HIGH, ZkEnableSecureAclsDoc)

      /** ********* General Configuration ***********/
      .define(BrokerIdGenerationEnableProp, BOOLEAN, Defaults.BrokerIdGenerationEnable, MEDIUM, BrokerIdGenerationEnableDoc)
      .define(MaxReservedBrokerIdProp, INT, Defaults.MaxReservedBrokerId, atLeast(0), MEDIUM, MaxReservedBrokerIdDoc)
      .define(BrokerIdProp, INT, Defaults.BrokerId, HIGH, BrokerIdDoc)
      .define(MessageMaxBytesProp, INT, Defaults.MessageMaxBytes, atLeast(0), HIGH, MessageMaxBytesDoc)
      .define(NumNetworkThreadsProp, INT, Defaults.NumNetworkThreads, atLeast(1), HIGH, NumNetworkThreadsDoc)
      .define(NumIoThreadsProp, INT, Defaults.NumIoThreads, atLeast(1), HIGH, NumIoThreadsDoc)
      .define(BackgroundThreadsProp, INT, Defaults.BackgroundThreads, atLeast(1), HIGH, BackgroundThreadsDoc)
      .define(QueuedMaxRequestsProp, INT, Defaults.QueuedMaxRequests, atLeast(1), HIGH, QueuedMaxRequestsDoc)
      .define(RequestTimeoutMsProp, INT, Defaults.RequestTimeoutMs, HIGH, RequestTimeoutMsDoc)

      /************* Authorizer Configuration ***********/
      .define(AuthorizerClassNameProp, STRING, Defaults.AuthorizerClassName, LOW, AuthorizerClassNameDoc)

      /** ********* Socket Server Configuration ***********/
      .define(PortProp, INT, Defaults.Port, HIGH, PortDoc)
      .define(HostNameProp, STRING, Defaults.HostName, HIGH, HostNameDoc)
      .define(ListenersProp, STRING, null, HIGH, ListenersDoc)
      .define(AdvertisedHostNameProp, STRING, null, HIGH, AdvertisedHostNameDoc)
      .define(AdvertisedPortProp, INT, null, HIGH, AdvertisedPortDoc)
      .define(AdvertisedListenersProp, STRING, null, HIGH, AdvertisedListenersDoc)
      .define(SocketSendBufferBytesProp, INT, Defaults.SocketSendBufferBytes, HIGH, SocketSendBufferBytesDoc)
      .define(SocketReceiveBufferBytesProp, INT, Defaults.SocketReceiveBufferBytes, HIGH, SocketReceiveBufferBytesDoc)
      .define(SocketRequestMaxBytesProp, INT, Defaults.SocketRequestMaxBytes, atLeast(1), HIGH, SocketRequestMaxBytesDoc)
      .define(MaxConnectionsPerIpProp, INT, Defaults.MaxConnectionsPerIp, atLeast(1), MEDIUM, MaxConnectionsPerIpDoc)
      .define(MaxConnectionsPerIpOverridesProp, STRING, Defaults.MaxConnectionsPerIpOverrides, MEDIUM, MaxConnectionsPerIpOverridesDoc)
      .define(ConnectionsMaxIdleMsProp, LONG, Defaults.ConnectionsMaxIdleMs, MEDIUM, ConnectionsMaxIdleMsDoc)

      /** ********* Log Configuration ***********/
      .define(NumPartitionsProp, INT, Defaults.NumPartitions, atLeast(1), MEDIUM, NumPartitionsDoc)
      .define(LogDirProp, STRING, Defaults.LogDir, HIGH, LogDirDoc)
      .define(LogDirsProp, STRING, null, HIGH, LogDirsDoc)
      .define(LogSegmentBytesProp, INT, Defaults.LogSegmentBytes, atLeast(Message.MinHeaderSize), HIGH, LogSegmentBytesDoc)

      .define(LogRollTimeMillisProp, LONG, null, HIGH, LogRollTimeMillisDoc)
      .define(LogRollTimeHoursProp, INT, Defaults.LogRollHours, atLeast(1), HIGH, LogRollTimeHoursDoc)

      .define(LogRollTimeJitterMillisProp, LONG, null, HIGH, LogRollTimeJitterMillisDoc)
      .define(LogRollTimeJitterHoursProp, INT, Defaults.LogRollJitterHours, atLeast(0), HIGH, LogRollTimeJitterHoursDoc)

      .define(LogRetentionTimeMillisProp, LONG, null, HIGH, LogRetentionTimeMillisDoc)
      .define(LogRetentionTimeMinutesProp, INT, null, HIGH, LogRetentionTimeMinsDoc)
      .define(LogRetentionTimeHoursProp, INT, Defaults.LogRetentionHours, HIGH, LogRetentionTimeHoursDoc)

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
      .define(LogFlushIntervalMsProp, LONG, null, HIGH, LogFlushIntervalMsDoc)
      .define(LogFlushOffsetCheckpointIntervalMsProp, INT, Defaults.LogFlushOffsetCheckpointIntervalMs, atLeast(0), HIGH, LogFlushOffsetCheckpointIntervalMsDoc)
      .define(LogPreAllocateProp, BOOLEAN, Defaults.LogPreAllocateEnable, MEDIUM, LogPreAllocateEnableDoc)
      .define(NumRecoveryThreadsPerDataDirProp, INT, Defaults.NumRecoveryThreadsPerDataDir, atLeast(1), HIGH, NumRecoveryThreadsPerDataDirDoc)
      .define(AutoCreateTopicsEnableProp, BOOLEAN, Defaults.AutoCreateTopicsEnable, HIGH, AutoCreateTopicsEnableDoc)
      .define(MinInSyncReplicasProp, INT, Defaults.MinInSyncReplicas, atLeast(1), HIGH, MinInSyncReplicasDoc)

      /** ********* Replication configuration ***********/
      .define(ControllerSocketTimeoutMsProp, INT, Defaults.ControllerSocketTimeoutMs, MEDIUM, ControllerSocketTimeoutMsDoc)
      .define(DefaultReplicationFactorProp, INT, Defaults.DefaultReplicationFactor, MEDIUM, DefaultReplicationFactorDoc)
      .define(ReplicaLagTimeMaxMsProp, LONG, Defaults.ReplicaLagTimeMaxMs, HIGH, ReplicaLagTimeMaxMsDoc)
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
      .define(LeaderImbalanceCheckIntervalSecondsProp, LONG, Defaults.LeaderImbalanceCheckIntervalSeconds, HIGH, LeaderImbalanceCheckIntervalSecondsDoc)
      .define(UncleanLeaderElectionEnableProp, BOOLEAN, Defaults.UncleanLeaderElectionEnable, HIGH, UncleanLeaderElectionEnableDoc)
      .define(InterBrokerSecurityProtocolProp, STRING, Defaults.InterBrokerSecurityProtocol, MEDIUM, InterBrokerSecurityProtocolDoc)
      .define(InterBrokerProtocolVersionProp, STRING, Defaults.InterBrokerProtocolVersion, MEDIUM, InterBrokerProtocolVersionDoc)

      /** ********* Controlled shutdown configuration ***********/
      .define(ControlledShutdownMaxRetriesProp, INT, Defaults.ControlledShutdownMaxRetries, MEDIUM, ControlledShutdownMaxRetriesDoc)
      .define(ControlledShutdownRetryBackoffMsProp, LONG, Defaults.ControlledShutdownRetryBackoffMs, MEDIUM, ControlledShutdownRetryBackoffMsDoc)
      .define(ControlledShutdownEnableProp, BOOLEAN, Defaults.ControlledShutdownEnable, MEDIUM, ControlledShutdownEnableDoc)

      /** ********* Consumer coordinator configuration ***********/
      .define(GroupMinSessionTimeoutMsProp, INT, Defaults.ConsumerMinSessionTimeoutMs, MEDIUM, ConsumerMinSessionTimeoutMsDoc)
      .define(GroupMaxSessionTimeoutMsProp, INT, Defaults.ConsumerMaxSessionTimeoutMs, MEDIUM, ConsumerMaxSessionTimeoutMsDoc)

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

      /** ********* Kafka Metrics Configuration ***********/
      .define(MetricNumSamplesProp, INT, Defaults.MetricNumSamples, atLeast(1), LOW, MetricNumSamplesDoc)
      .define(MetricSampleWindowMsProp, LONG, Defaults.MetricSampleWindowMs, atLeast(1), LOW, MetricSampleWindowMsDoc)
      .define(MetricReporterClassesProp, LIST, Defaults.MetricReporterClasses, LOW, MetricReporterClassesDoc)

      /** ********* Quota configuration ***********/
      .define(ProducerQuotaBytesPerSecondDefaultProp, LONG, Defaults.ProducerQuotaBytesPerSecondDefault, atLeast(1), HIGH, ProducerQuotaBytesPerSecondDefaultDoc)
      .define(ConsumerQuotaBytesPerSecondDefaultProp, LONG, Defaults.ConsumerQuotaBytesPerSecondDefault, atLeast(1), HIGH, ConsumerQuotaBytesPerSecondDefaultDoc)
      .define(NumQuotaSamplesProp, INT, Defaults.NumQuotaSamples, atLeast(1), LOW, NumQuotaSamplesDoc)
      .define(QuotaWindowSizeSecondsProp, INT, Defaults.QuotaWindowSizeSeconds, atLeast(1), LOW, QuotaWindowSizeSecondsDoc)


      /** ********* SSL Configuration ****************/
      .define(PrincipalBuilderClassProp, CLASS, Defaults.PrincipalBuilderClass, MEDIUM, PrincipalBuilderClassDoc)
      .define(SslProtocolProp, STRING, Defaults.SslProtocol, MEDIUM, SslProtocolDoc)
      .define(SslProviderProp, STRING, null, MEDIUM, SslProviderDoc)
      .define(SslEnabledProtocolsProp, LIST, Defaults.SslEnabledProtocols, MEDIUM, SslEnabledProtocolsDoc)
      .define(SslKeystoreTypeProp, STRING, Defaults.SslKeystoreType, MEDIUM, SslKeystoreTypeDoc)
      .define(SslKeystoreLocationProp, STRING, null, MEDIUM, SslKeystoreLocationDoc)
      .define(SslKeystorePasswordProp, PASSWORD, null, MEDIUM, SslKeystorePasswordDoc)
      .define(SslKeyPasswordProp, PASSWORD, null, MEDIUM, SslKeyPasswordDoc)
      .define(SslTruststoreTypeProp, STRING, Defaults.SslTruststoreType, MEDIUM, SslTruststoreTypeDoc)
      .define(SslTruststoreLocationProp, STRING, null, MEDIUM, SslTruststoreLocationDoc)
      .define(SslTruststorePasswordProp, PASSWORD, null, MEDIUM, SslTruststorePasswordDoc)
      .define(SslKeyManagerAlgorithmProp, STRING, Defaults.SslKeyManagerAlgorithm, MEDIUM, SslKeyManagerAlgorithmDoc)
      .define(SslTrustManagerAlgorithmProp, STRING, Defaults.SslTrustManagerAlgorithm, MEDIUM, SslTrustManagerAlgorithmDoc)
      .define(SslEndpointIdentificationAlgorithmProp, STRING, null, LOW, SslEndpointIdentificationAlgorithmDoc)
      .define(SslClientAuthProp, STRING, Defaults.SslClientAuth, in(Defaults.SslClientAuthRequired, Defaults.SslClientAuthRequested, Defaults.SslClientAuthNone), MEDIUM, SslClientAuthDoc)
      .define(SslCipherSuitesProp, LIST, null, MEDIUM, SslCipherSuitesDoc)

      /** ********* Sasl Configuration ****************/
      .define(SaslKerberosServiceNameProp, STRING, null, MEDIUM, SaslKerberosServiceNameDoc)
      .define(SaslKerberosKinitCmdProp, STRING, Defaults.SaslKerberosKinitCmd, MEDIUM, SaslKerberosKinitCmdDoc)
      .define(SaslKerberosTicketRenewWindowFactorProp, DOUBLE, Defaults.SaslKerberosTicketRenewWindowFactor, MEDIUM, SaslKerberosTicketRenewWindowFactorDoc)
      .define(SaslKerberosTicketRenewJitterProp, DOUBLE, Defaults.SaslKerberosTicketRenewJitter, MEDIUM, SaslKerberosTicketRenewJitterDoc)
      .define(SaslKerberosMinTimeBeforeReloginProp, LONG, Defaults.SaslKerberosMinTimeBeforeRelogin, MEDIUM, SaslKerberosMinTimeBeforeReloginDoc)
      .define(SaslKerberosPrincipalToLocalRulesProp, LIST, Defaults.SaslKerberosPrincipalToLocalRules, MEDIUM, SaslKerberosPrincipalToLocalRulesDoc)

  }

  def configNames() = {
    import scala.collection.JavaConversions._
    configDef.names().toList.sorted
  }

  /**
    * Check that property names are valid
    */
  def validateNames(props: Properties) {
    import scala.collection.JavaConversions._
    val names = configDef.names()
    for (name <- props.keys)
      require(names.contains(name), "Unknown configuration \"%s\".".format(name))
  }

  def fromProps(props: Properties): KafkaConfig =
    fromProps(props, true)

  def fromProps(props: Properties, doLog: Boolean): KafkaConfig =
    new KafkaConfig(props, doLog)

  def fromProps(defaults: Properties, overrides: Properties): KafkaConfig =
    fromProps(defaults, overrides, true)

  def fromProps(defaults: Properties, overrides: Properties, doLog: Boolean): KafkaConfig = {
    val props = new Properties()
    props.putAll(defaults)
    props.putAll(overrides)
    fromProps(props, doLog)
  }

  def apply(props: java.util.Map[_, _]): KafkaConfig = new KafkaConfig(props, true)

}

class KafkaConfig(val props: java.util.Map[_, _], doLog: Boolean) extends AbstractConfig(KafkaConfig.configDef, props, doLog) {

  def this(props: java.util.Map[_, _]) = this(props, true)

  /** ********* Zookeeper Configuration ***********/
  val zkConnect: String = getString(KafkaConfig.ZkConnectProp)
  val zkSessionTimeoutMs: Int = getInt(KafkaConfig.ZkSessionTimeoutMsProp)
  val zkConnectionTimeoutMs: Int =
    Option(getInt(KafkaConfig.ZkConnectionTimeoutMsProp)).map(_.toInt).getOrElse(getInt(KafkaConfig.ZkSessionTimeoutMsProp))
  val zkSyncTimeMs: Int = getInt(KafkaConfig.ZkSyncTimeMsProp)
  val zkEnableSecureAcls: Boolean = getBoolean(KafkaConfig.ZkEnableSecureAclsProp)

  /** ********* General Configuration ***********/
  val brokerIdGenerationEnable: Boolean = getBoolean(KafkaConfig.BrokerIdGenerationEnableProp)
  val maxReservedBrokerId: Int = getInt(KafkaConfig.MaxReservedBrokerIdProp)
  var brokerId: Int = getInt(KafkaConfig.BrokerIdProp)
  val numNetworkThreads = getInt(KafkaConfig.NumNetworkThreadsProp)
  val backgroundThreads = getInt(KafkaConfig.BackgroundThreadsProp)
  val queuedMaxRequests = getInt(KafkaConfig.QueuedMaxRequestsProp)
  val numIoThreads = getInt(KafkaConfig.NumIoThreadsProp)
  val messageMaxBytes = getInt(KafkaConfig.MessageMaxBytesProp)
  val requestTimeoutMs = getInt(KafkaConfig.RequestTimeoutMsProp)

  /************* Authorizer Configuration ***********/
  val authorizerClassName: String = getString(KafkaConfig.AuthorizerClassNameProp)

  /** ********* Socket Server Configuration ***********/
  val hostName = getString(KafkaConfig.HostNameProp)
  val port = getInt(KafkaConfig.PortProp)
  val advertisedHostName = Option(getString(KafkaConfig.AdvertisedHostNameProp)).getOrElse(hostName)
  val advertisedPort: java.lang.Integer = Option(getInt(KafkaConfig.AdvertisedPortProp)).getOrElse(port)

  val socketSendBufferBytes = getInt(KafkaConfig.SocketSendBufferBytesProp)
  val socketReceiveBufferBytes = getInt(KafkaConfig.SocketReceiveBufferBytesProp)
  val socketRequestMaxBytes = getInt(KafkaConfig.SocketRequestMaxBytesProp)
  val maxConnectionsPerIp = getInt(KafkaConfig.MaxConnectionsPerIpProp)
  val maxConnectionsPerIpOverrides: Map[String, Int] =
    getMap(KafkaConfig.MaxConnectionsPerIpOverridesProp, getString(KafkaConfig.MaxConnectionsPerIpOverridesProp)).map { case (k, v) => (k, v.toInt)}
  val connectionsMaxIdleMs = getLong(KafkaConfig.ConnectionsMaxIdleMsProp)

  /** ********* Log Configuration ***********/
  val autoCreateTopicsEnable = getBoolean(KafkaConfig.AutoCreateTopicsEnableProp)
  val numPartitions = getInt(KafkaConfig.NumPartitionsProp)
  val logDirs = CoreUtils.parseCsvList( Option(getString(KafkaConfig.LogDirsProp)).getOrElse(getString(KafkaConfig.LogDirProp)))
  val logSegmentBytes = getInt(KafkaConfig.LogSegmentBytesProp)
  val logFlushIntervalMessages = getLong(KafkaConfig.LogFlushIntervalMessagesProp)
  val logCleanerThreads = getInt(KafkaConfig.LogCleanerThreadsProp)
  val numRecoveryThreadsPerDataDir = getInt(KafkaConfig.NumRecoveryThreadsPerDataDirProp)
  val logFlushSchedulerIntervalMs = getLong(KafkaConfig.LogFlushSchedulerIntervalMsProp)
  val logFlushOffsetCheckpointIntervalMs = getInt(KafkaConfig.LogFlushOffsetCheckpointIntervalMsProp).toLong
  val logCleanupIntervalMs = getLong(KafkaConfig.LogCleanupIntervalMsProp)
  val logCleanupPolicy = getString(KafkaConfig.LogCleanupPolicyProp)
  val offsetsRetentionMinutes = getInt(KafkaConfig.OffsetsRetentionMinutesProp)
  val offsetsRetentionCheckIntervalMs = getLong(KafkaConfig.OffsetsRetentionCheckIntervalMsProp)
  val logRetentionBytes = getLong(KafkaConfig.LogRetentionBytesProp)
  val logCleanerDedupeBufferSize = getLong(KafkaConfig.LogCleanerDedupeBufferSizeProp)
  val logCleanerDedupeBufferLoadFactor = getDouble(KafkaConfig.LogCleanerDedupeBufferLoadFactorProp)
  val logCleanerIoBufferSize = getInt(KafkaConfig.LogCleanerIoBufferSizeProp)
  val logCleanerIoMaxBytesPerSecond = getDouble(KafkaConfig.LogCleanerIoMaxBytesPerSecondProp)
  val logCleanerDeleteRetentionMs = getLong(KafkaConfig.LogCleanerDeleteRetentionMsProp)
  val logCleanerBackoffMs = getLong(KafkaConfig.LogCleanerBackoffMsProp)
  val logCleanerMinCleanRatio = getDouble(KafkaConfig.LogCleanerMinCleanRatioProp)
  val logCleanerEnable = getBoolean(KafkaConfig.LogCleanerEnableProp)
  val logIndexSizeMaxBytes = getInt(KafkaConfig.LogIndexSizeMaxBytesProp)
  val logIndexIntervalBytes = getInt(KafkaConfig.LogIndexIntervalBytesProp)
  val logDeleteDelayMs = getLong(KafkaConfig.LogDeleteDelayMsProp)
  val logRollTimeMillis: java.lang.Long = Option(getLong(KafkaConfig.LogRollTimeMillisProp)).getOrElse(60 * 60 * 1000L * getInt(KafkaConfig.LogRollTimeHoursProp))
  val logRollTimeJitterMillis: java.lang.Long = Option(getLong(KafkaConfig.LogRollTimeJitterMillisProp)).getOrElse(60 * 60 * 1000L * getInt(KafkaConfig.LogRollTimeJitterHoursProp))
  val logFlushIntervalMs: java.lang.Long = Option(getLong(KafkaConfig.LogFlushIntervalMsProp)).getOrElse(getLong(KafkaConfig.LogFlushSchedulerIntervalMsProp))
  val logRetentionTimeMillis = getLogRetentionTimeMillis
  val minInSyncReplicas = getInt(KafkaConfig.MinInSyncReplicasProp)
  val logPreAllocateEnable: java.lang.Boolean = getBoolean(KafkaConfig.LogPreAllocateProp)

  /** ********* Replication configuration ***********/
  val controllerSocketTimeoutMs: Int = getInt(KafkaConfig.ControllerSocketTimeoutMsProp)
  val defaultReplicationFactor: Int = getInt(KafkaConfig.DefaultReplicationFactorProp)
  val replicaLagTimeMaxMs = getLong(KafkaConfig.ReplicaLagTimeMaxMsProp)
  val replicaSocketTimeoutMs = getInt(KafkaConfig.ReplicaSocketTimeoutMsProp)
  val replicaSocketReceiveBufferBytes = getInt(KafkaConfig.ReplicaSocketReceiveBufferBytesProp)
  val replicaFetchMaxBytes = getInt(KafkaConfig.ReplicaFetchMaxBytesProp)
  val replicaFetchWaitMaxMs = getInt(KafkaConfig.ReplicaFetchWaitMaxMsProp)
  val replicaFetchMinBytes = getInt(KafkaConfig.ReplicaFetchMinBytesProp)
  val replicaFetchBackoffMs = getInt(KafkaConfig.ReplicaFetchBackoffMsProp)
  val numReplicaFetchers = getInt(KafkaConfig.NumReplicaFetchersProp)
  val replicaHighWatermarkCheckpointIntervalMs = getLong(KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp)
  val fetchPurgatoryPurgeIntervalRequests = getInt(KafkaConfig.FetchPurgatoryPurgeIntervalRequestsProp)
  val producerPurgatoryPurgeIntervalRequests = getInt(KafkaConfig.ProducerPurgatoryPurgeIntervalRequestsProp)
  val autoLeaderRebalanceEnable = getBoolean(KafkaConfig.AutoLeaderRebalanceEnableProp)
  val leaderImbalancePerBrokerPercentage = getInt(KafkaConfig.LeaderImbalancePerBrokerPercentageProp)
  val leaderImbalanceCheckIntervalSeconds = getLong(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp)
  val uncleanLeaderElectionEnable: java.lang.Boolean = getBoolean(KafkaConfig.UncleanLeaderElectionEnableProp)
  val interBrokerSecurityProtocol = SecurityProtocol.valueOf(getString(KafkaConfig.InterBrokerSecurityProtocolProp))
  val interBrokerProtocolVersion = ApiVersion(getString(KafkaConfig.InterBrokerProtocolVersionProp))

  /** ********* Controlled shutdown configuration ***********/
  val controlledShutdownMaxRetries = getInt(KafkaConfig.ControlledShutdownMaxRetriesProp)
  val controlledShutdownRetryBackoffMs = getLong(KafkaConfig.ControlledShutdownRetryBackoffMsProp)
  val controlledShutdownEnable = getBoolean(KafkaConfig.ControlledShutdownEnableProp)

  /** ********* Group coordinator configuration ***********/
  val groupMinSessionTimeoutMs = getInt(KafkaConfig.GroupMinSessionTimeoutMsProp)
  val groupMaxSessionTimeoutMs = getInt(KafkaConfig.GroupMaxSessionTimeoutMsProp)

  /** ********* Offset management configuration ***********/
  val offsetMetadataMaxSize = getInt(KafkaConfig.OffsetMetadataMaxSizeProp)
  val offsetsLoadBufferSize = getInt(KafkaConfig.OffsetsLoadBufferSizeProp)
  val offsetsTopicReplicationFactor = getShort(KafkaConfig.OffsetsTopicReplicationFactorProp)
  val offsetsTopicPartitions = getInt(KafkaConfig.OffsetsTopicPartitionsProp)
  val offsetCommitTimeoutMs = getInt(KafkaConfig.OffsetCommitTimeoutMsProp)
  val offsetCommitRequiredAcks = getShort(KafkaConfig.OffsetCommitRequiredAcksProp)
  val offsetsTopicSegmentBytes = getInt(KafkaConfig.OffsetsTopicSegmentBytesProp)
  val offsetsTopicCompressionCodec = Option(getInt(KafkaConfig.OffsetsTopicCompressionCodecProp)).map(value => CompressionCodec.getCompressionCodec(value)).orNull

  /** ********* Metric Configuration **************/
  val metricNumSamples = getInt(KafkaConfig.MetricNumSamplesProp)
  val metricSampleWindowMs = getLong(KafkaConfig.MetricSampleWindowMsProp)
  val metricReporterClasses: java.util.List[MetricsReporter] = getConfiguredInstances(KafkaConfig.MetricReporterClassesProp, classOf[MetricsReporter])

  /** ********* SSL Configuration **************/
  val principalBuilderClass = getClass(KafkaConfig.PrincipalBuilderClassProp)
  val sslProtocol = getString(KafkaConfig.SslProtocolProp)
  val sslProvider = getString(KafkaConfig.SslProviderProp)
  val sslEnabledProtocols = getList(KafkaConfig.SslEnabledProtocolsProp)
  val sslKeystoreType = getString(KafkaConfig.SslKeystoreTypeProp)
  val sslKeystoreLocation = getString(KafkaConfig.SslKeystoreLocationProp)
  val sslKeystorePassword = getPassword(KafkaConfig.SslKeystorePasswordProp)
  val sslKeyPassword = getPassword(KafkaConfig.SslKeyPasswordProp)
  val sslTruststoreType = getString(KafkaConfig.SslTruststoreTypeProp)
  val sslTruststoreLocation = getString(KafkaConfig.SslTruststoreLocationProp)
  val sslTruststorePassword = getPassword(KafkaConfig.SslTruststorePasswordProp)
  val sslKeyManagerAlgorithm = getString(KafkaConfig.SslKeyManagerAlgorithmProp)
  val sslTrustManagerAlgorithm = getString(KafkaConfig.SslTrustManagerAlgorithmProp)
  val sslClientAuth = getString(KafkaConfig.SslClientAuthProp)
  val sslCipher = getList(KafkaConfig.SslCipherSuitesProp)

  /** ********* Sasl Configuration **************/
  val saslKerberosServiceName = getString(KafkaConfig.SaslKerberosServiceNameProp)
  val saslKerberosKinitCmd = getString(KafkaConfig.SaslKerberosKinitCmdProp)
  val saslKerberosTicketRenewWindowFactor = getDouble(KafkaConfig.SaslKerberosTicketRenewWindowFactorProp)
  val saslKerberosTicketRenewJitter = getDouble(KafkaConfig.SaslKerberosTicketRenewJitterProp)
  val saslKerberosMinTimeBeforeRelogin = getLong(KafkaConfig.SaslKerberosMinTimeBeforeReloginProp)
  val saslKerberosPrincipalToLocalRules = getList(KafkaConfig.SaslKerberosPrincipalToLocalRulesProp)

  /** ********* Quota Configuration **************/
  val producerQuotaBytesPerSecondDefault = getLong(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp)
  val consumerQuotaBytesPerSecondDefault = getLong(KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp)
  val numQuotaSamples = getInt(KafkaConfig.NumQuotaSamplesProp)
  val quotaWindowSizeSeconds = getInt(KafkaConfig.QuotaWindowSizeSecondsProp)

  val deleteTopicEnable = getBoolean(KafkaConfig.DeleteTopicEnableProp)
  val compressionType = getString(KafkaConfig.CompressionTypeProp)

  val listeners = getListeners
  val advertisedListeners = getAdvertisedListeners

  private def getLogRetentionTimeMillis: Long = {
    val millisInMinute = 60L * 1000L
    val millisInHour = 60L * millisInMinute

    val millis: java.lang.Long =
      Option(getLong(KafkaConfig.LogRetentionTimeMillisProp)).getOrElse(
        Option(getInt(KafkaConfig.LogRetentionTimeMinutesProp)) match {
          case Some(mins) => millisInMinute * mins
          case None => getInt(KafkaConfig.LogRetentionTimeHoursProp) * millisInHour
        })

    if (millis < 0) return -1
    millis
  }

  private def getMap(propName: String, propValue: String): Map[String, String] = {
    try {
      CoreUtils.parseCsvMap(propValue)
    } catch {
      case e: Exception => throw new IllegalArgumentException("Error parsing configuration property '%s': %s".format(propName, e.getMessage))
    }
  }

  private def validateUniquePortAndProtocol(listeners: String) {

    val endpoints = try {
      val listenerList = CoreUtils.parseCsvList(listeners)
      listenerList.map(listener => EndPoint.createEndPoint(listener))
    } catch {
      case e: Exception => throw new IllegalArgumentException("Error creating broker listeners from '%s': %s".format(listeners, e.getMessage))
    }
    // filter port 0 for unit tests
    val endpointsWithoutZeroPort = endpoints.map(ep => ep.port).filter(_ != 0)
    val distinctPorts = endpointsWithoutZeroPort.distinct
    val distinctProtocols = endpoints.map(ep => ep.protocolType).distinct

    require(distinctPorts.size == endpointsWithoutZeroPort.size, "Each listener must have a different port")
    require(distinctProtocols.size == endpoints.size, "Each listener must have a different protocol")
  }

  // If the user did not define listeners but did define host or port, let's use them in backward compatible way
  // If none of those are defined, we default to PLAINTEXT://:9092
  private def getListeners(): immutable.Map[SecurityProtocol, EndPoint] = {
    if (getString(KafkaConfig.ListenersProp) != null) {
      validateUniquePortAndProtocol(getString(KafkaConfig.ListenersProp))
      CoreUtils.listenerListToEndPoints(getString(KafkaConfig.ListenersProp))
    } else {
      CoreUtils.listenerListToEndPoints("PLAINTEXT://" + hostName + ":" + port)
    }
  }

  // If the user defined advertised listeners, we use those
  // If he didn't but did define advertised host or port, we'll use those and fill in the missing value from regular host / port or defaults
  // If none of these are defined, we'll use the listeners
  private def getAdvertisedListeners(): immutable.Map[SecurityProtocol, EndPoint] = {
    if (getString(KafkaConfig.AdvertisedListenersProp) != null) {
      validateUniquePortAndProtocol(getString(KafkaConfig.AdvertisedListenersProp))
      CoreUtils.listenerListToEndPoints(getString(KafkaConfig.AdvertisedListenersProp))
    } else if (getString(KafkaConfig.AdvertisedHostNameProp) != null || getInt(KafkaConfig.AdvertisedPortProp) != null) {
      CoreUtils.listenerListToEndPoints("PLAINTEXT://" + advertisedHostName + ":" + advertisedPort)
    } else {
      getListeners()
    }
  }

  validateValues()

  private def validateValues() {
    if(brokerIdGenerationEnable) {
      require(brokerId >= -1 && brokerId <= maxReservedBrokerId, "broker.id must be equal or greater than -1 and not greater than reserved.broker.max.id")
    } else {
      require(brokerId >= 0, "broker.id must be equal or greater than 0")
    }
    require(logRollTimeMillis >= 1, "log.roll.ms must be equal or greater than 1")
    require(logRollTimeJitterMillis >= 0, "log.roll.jitter.ms must be equal or greater than 0")
    require(logRetentionTimeMillis >= 1 || logRetentionTimeMillis == -1, "log.retention.ms must be unlimited (-1) or, equal or greater than 1")
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
    require(advertisedListeners.keySet.contains(interBrokerSecurityProtocol),
      s"${KafkaConfig.InterBrokerSecurityProtocolProp} must be a protocol in the configured set of ${KafkaConfig.AdvertisedListenersProp}. " +
      s"The valid options based on currently configured protocols are ${advertisedListeners.keySet}")
    require(advertisedListeners.keySet.subsetOf(listeners.keySet),
      s"${KafkaConfig.AdvertisedListenersProp} protocols must be equal to or a subset of ${KafkaConfig.ListenersProp} protocols. " +
      s"Found ${advertisedListeners.keySet}. The valid options based on currently configured protocols are ${listeners.keySet}"
    )
  }

}
