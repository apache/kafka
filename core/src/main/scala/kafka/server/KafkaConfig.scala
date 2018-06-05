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

import java.util
import java.util.{Collections, Properties}

import kafka.api.{ApiVersion, KAFKA_0_10_0_IV1}
import kafka.cluster.EndPoint
import kafka.consumer.ConsumerConfig
import kafka.coordinator.group.OffsetConfig
import kafka.coordinator.transaction.{TransactionLog, TransactionStateManager}
import kafka.message.{BrokerCompressionCodec, CompressionCodec, Message, MessageSet}
import kafka.utils.CoreUtils
import kafka.utils.Implicits._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.ConfigDef.{ConfigKey, ValidList}
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, SaslConfigs, SslConfigs, TopicConfig}
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.security.auth.SecurityProtocol

import scala.collection.JavaConverters._
import scala.collection.Map

object Defaults {
  /** ********* Zookeeper Configuration ***********/
  val ZkSessionTimeoutMs = 6000
  val ZkSyncTimeMs = 2000
  val ZkEnableSecureAcls = false
  val ZkMaxInFlightRequests = 10

  /** ********* General Configuration ***********/
  val BrokerIdGenerationEnable = true
  val MaxReservedBrokerId = 1000
  val BrokerId = -1
  val MessageMaxBytes = 1000000 + MessageSet.LogOverhead
  val NumNetworkThreads = 3
  val NumIoThreads = 8
  val BackgroundThreads = 10
  val QueuedMaxRequests = 500
  val QueuedMaxRequestBytes = -1

  /************* Authorizer Configuration ***********/
  val AuthorizerClassName = ""

  /** ********* Socket Server Configuration ***********/
  val Port = 9092
  val HostName: String = new String("")

  val ListenerSecurityProtocolMap: String = EndPoint.DefaultSecurityProtocolMap.map { case (listenerName, securityProtocol) =>
    s"${listenerName.value}:${securityProtocol.name}"
  }.mkString(",")

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
  val LogCleanerMinCompactionLagMs = 0L
  val LogIndexSizeMaxBytes = 10 * 1024 * 1024
  val LogIndexIntervalBytes = 4096
  val LogFlushIntervalMessages = Long.MaxValue
  val LogDeleteDelayMs = 60000
  val LogFlushSchedulerIntervalMs = Long.MaxValue
  val LogFlushOffsetCheckpointIntervalMs = 60000
  val LogFlushStartOffsetCheckpointIntervalMs = 60000
  val LogPreAllocateEnable = false
  // lazy val as `InterBrokerProtocolVersion` is defined later
  lazy val LogMessageFormatVersion = InterBrokerProtocolVersion
  val LogMessageTimestampType = "CreateTime"
  val LogMessageTimestampDifferenceMaxMs = Long.MaxValue
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
  val ReplicaFetchResponseMaxBytes = 10 * 1024 * 1024
  val NumReplicaFetchers = 1
  val ReplicaFetchBackoffMs = 1000
  val ReplicaHighWatermarkCheckpointIntervalMs = 5000L
  val FetchPurgatoryPurgeIntervalRequests = 1000
  val ProducerPurgatoryPurgeIntervalRequests = 1000
  val DeleteRecordsPurgatoryPurgeIntervalRequests = 1
  val AutoLeaderRebalanceEnable = true
  val LeaderImbalancePerBrokerPercentage = 10
  val LeaderImbalanceCheckIntervalSeconds = 300
  val UncleanLeaderElectionEnable = false
  val InterBrokerSecurityProtocol = SecurityProtocol.PLAINTEXT.toString
  val InterBrokerProtocolVersion = ApiVersion.latestVersion.toString

  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetries = 3
  val ControlledShutdownRetryBackoffMs = 5000
  val ControlledShutdownEnable = true

  /** ********* Group coordinator configuration ***********/
  val GroupMinSessionTimeoutMs = 6000
  val GroupMaxSessionTimeoutMs = 300000
  val GroupInitialRebalanceDelayMs = 3000

  /** ********* Offset management configuration ***********/
  val OffsetMetadataMaxSize = OffsetConfig.DefaultMaxMetadataSize
  val OffsetsLoadBufferSize = OffsetConfig.DefaultLoadBufferSize
  val OffsetsTopicReplicationFactor = OffsetConfig.DefaultOffsetsTopicReplicationFactor
  val OffsetsTopicPartitions: Int = OffsetConfig.DefaultOffsetsTopicNumPartitions
  val OffsetsTopicSegmentBytes: Int = OffsetConfig.DefaultOffsetsTopicSegmentBytes
  val OffsetsTopicCompressionCodec: Int = OffsetConfig.DefaultOffsetsTopicCompressionCodec.codec
  val OffsetsRetentionMinutes: Int = 7 * 24 * 60
  val OffsetsRetentionCheckIntervalMs: Long = OffsetConfig.DefaultOffsetsRetentionCheckIntervalMs
  val OffsetCommitTimeoutMs = OffsetConfig.DefaultOffsetCommitTimeoutMs
  val OffsetCommitRequiredAcks = OffsetConfig.DefaultOffsetCommitRequiredAcks

  /** ********* Transaction management configuration ***********/
  val TransactionalIdExpirationMs = TransactionStateManager.DefaultTransactionalIdExpirationMs
  val TransactionsMaxTimeoutMs = TransactionStateManager.DefaultTransactionsMaxTimeoutMs
  val TransactionsTopicMinISR = TransactionLog.DefaultMinInSyncReplicas
  val TransactionsLoadBufferSize = TransactionLog.DefaultLoadBufferSize
  val TransactionsTopicReplicationFactor = TransactionLog.DefaultReplicationFactor
  val TransactionsTopicPartitions = TransactionLog.DefaultNumPartitions
  val TransactionsTopicSegmentBytes = TransactionLog.DefaultSegmentBytes
  val TransactionsAbortTimedOutTransactionsCleanupIntervalMS = TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs
  val TransactionsRemoveExpiredTransactionsCleanupIntervalMS = TransactionStateManager.DefaultRemoveExpiredTransactionalIdsIntervalMs

  /** ********* Fetch Session Configuration **************/
  val MaxIncrementalFetchSessionCacheSlots = 1000

  /** ********* Quota Configuration ***********/
  val ProducerQuotaBytesPerSecondDefault = ClientQuotaManagerConfig.QuotaBytesPerSecondDefault
  val ConsumerQuotaBytesPerSecondDefault = ClientQuotaManagerConfig.QuotaBytesPerSecondDefault
  val NumQuotaSamples: Int = ClientQuotaManagerConfig.DefaultNumQuotaSamples
  val QuotaWindowSizeSeconds: Int = ClientQuotaManagerConfig.DefaultQuotaWindowSizeSeconds
  val NumReplicationQuotaSamples: Int = ReplicationQuotaManagerConfig.DefaultNumQuotaSamples
  val ReplicationQuotaWindowSizeSeconds: Int = ReplicationQuotaManagerConfig.DefaultQuotaWindowSizeSeconds
  val NumAlterLogDirsReplicationQuotaSamples: Int = ReplicationQuotaManagerConfig.DefaultNumQuotaSamples
  val AlterLogDirsReplicationQuotaWindowSizeSeconds: Int = ReplicationQuotaManagerConfig.DefaultQuotaWindowSizeSeconds

  /** ********* Transaction Configuration ***********/
  val TransactionalIdExpirationMsDefault = 604800000

  val DeleteTopicEnable = true

  val CompressionType = "producer"

  val MaxIdMapSnapshots = 2
  /** ********* Kafka Metrics Configuration ***********/
  val MetricNumSamples = 2
  val MetricSampleWindowMs = 30000
  val MetricReporterClasses = ""
  val MetricRecordingLevel = Sensor.RecordingLevel.INFO.toString()

  /** ********* SSL configuration ***********/
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
  val SaslMechanismInterBrokerProtocol = SaslConfigs.DEFAULT_SASL_MECHANISM
  val SaslEnabledMechanisms = SaslConfigs.DEFAULT_SASL_ENABLED_MECHANISMS
  val SaslKerberosKinitCmd = SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD
  val SaslKerberosTicketRenewWindowFactor = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR
  val SaslKerberosTicketRenewJitter = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER
  val SaslKerberosMinTimeBeforeRelogin = SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN
  val SaslKerberosPrincipalToLocalRules = BrokerSecurityConfigs.DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES
  val SaslLoginRefreshWindowFactor = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR
  val SaslLoginRefreshWindowJitter = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER
  val SaslLoginRefreshMinPeriodSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS
  val SaslLoginRefreshBufferSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS

  /** ********* Delegation Token configuration ***********/
  val DelegationTokenMaxLifeTimeMsDefault = 7 * 24 * 60 * 60 * 1000L
  val DelegationTokenExpiryTimeMsDefault = 24 * 60 * 60 * 1000L
  val DelegationTokenExpiryCheckIntervalMsDefault = 1 * 60 * 60 * 1000L

  /** ********* Password encryption configuration for dynamic configs *********/
  val PasswordEncoderCipherAlgorithm = "AES/CBC/PKCS5Padding"
  val PasswordEncoderKeyLength = 128
  val PasswordEncoderIterations = 4096
}

object KafkaConfig {

  private val LogConfigPrefix = "log."

  def main(args: Array[String]) {
    System.out.println(configDef.toHtmlTable(DynamicBrokerConfig.dynamicConfigUpdateModes))
  }

  /** ********* Zookeeper Configuration ***********/
  val ZkConnectProp = "zookeeper.connect"
  val ZkSessionTimeoutMsProp = "zookeeper.session.timeout.ms"
  val ZkConnectionTimeoutMsProp = "zookeeper.connection.timeout.ms"
  val ZkSyncTimeMsProp = "zookeeper.sync.time.ms"
  val ZkEnableSecureAclsProp = "zookeeper.set.acl"
  val ZkMaxInFlightRequestsProp = "zookeeper.max.in.flight.requests"
  /** ********* General Configuration ***********/
  val BrokerIdGenerationEnableProp = "broker.id.generation.enable"
  val MaxReservedBrokerIdProp = "reserved.broker.max.id"
  val BrokerIdProp = "broker.id"
  val MessageMaxBytesProp = "message.max.bytes"
  val NumNetworkThreadsProp = "num.network.threads"
  val NumIoThreadsProp = "num.io.threads"
  val BackgroundThreadsProp = "background.threads"
  val NumReplicaAlterLogDirsThreadsProp = "num.replica.alter.log.dirs.threads"
  val QueuedMaxRequestsProp = "queued.max.requests"
  val QueuedMaxBytesProp = "queued.max.request.bytes"
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
  val ListenerSecurityProtocolMapProp = "listener.security.protocol.map"
  val SocketSendBufferBytesProp = "socket.send.buffer.bytes"
  val SocketReceiveBufferBytesProp = "socket.receive.buffer.bytes"
  val SocketRequestMaxBytesProp = "socket.request.max.bytes"
  val MaxConnectionsPerIpProp = "max.connections.per.ip"
  val MaxConnectionsPerIpOverridesProp = "max.connections.per.ip.overrides"
  val ConnectionsMaxIdleMsProp = "connections.max.idle.ms"
  /***************** rack configuration *************/
  val RackProp = "broker.rack"
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
  val LogCleanerMinCompactionLagMsProp = "log.cleaner.min.compaction.lag.ms"
  val LogIndexSizeMaxBytesProp = "log.index.size.max.bytes"
  val LogIndexIntervalBytesProp = "log.index.interval.bytes"
  val LogFlushIntervalMessagesProp = "log.flush.interval.messages"
  val LogDeleteDelayMsProp = "log.segment.delete.delay.ms"
  val LogFlushSchedulerIntervalMsProp = "log.flush.scheduler.interval.ms"
  val LogFlushIntervalMsProp = "log.flush.interval.ms"
  val LogFlushOffsetCheckpointIntervalMsProp = "log.flush.offset.checkpoint.interval.ms"
  val LogFlushStartOffsetCheckpointIntervalMsProp = "log.flush.start.offset.checkpoint.interval.ms"
  val LogPreAllocateProp = "log.preallocate"
  val LogMessageFormatVersionProp = LogConfigPrefix + "message.format.version"
  val LogMessageTimestampTypeProp = LogConfigPrefix + "message.timestamp.type"
  val LogMessageTimestampDifferenceMaxMsProp = LogConfigPrefix + "message.timestamp.difference.max.ms"
  val LogMaxIdMapSnapshotsProp = LogConfigPrefix + "max.id.map.snapshots"
  val NumRecoveryThreadsPerDataDirProp = "num.recovery.threads.per.data.dir"
  val AutoCreateTopicsEnableProp = "auto.create.topics.enable"
  val MinInSyncReplicasProp = "min.insync.replicas"
  val CreateTopicPolicyClassNameProp = "create.topic.policy.class.name"
  val AlterConfigPolicyClassNameProp = "alter.config.policy.class.name"
  /** ********* Replication configuration ***********/
  val ControllerSocketTimeoutMsProp = "controller.socket.timeout.ms"
  val DefaultReplicationFactorProp = "default.replication.factor"
  val ReplicaLagTimeMaxMsProp = "replica.lag.time.max.ms"
  val ReplicaSocketTimeoutMsProp = "replica.socket.timeout.ms"
  val ReplicaSocketReceiveBufferBytesProp = "replica.socket.receive.buffer.bytes"
  val ReplicaFetchMaxBytesProp = "replica.fetch.max.bytes"
  val ReplicaFetchWaitMaxMsProp = "replica.fetch.wait.max.ms"
  val ReplicaFetchMinBytesProp = "replica.fetch.min.bytes"
  val ReplicaFetchResponseMaxBytesProp = "replica.fetch.response.max.bytes"
  val ReplicaFetchBackoffMsProp = "replica.fetch.backoff.ms"
  val NumReplicaFetchersProp = "num.replica.fetchers"
  val ReplicaHighWatermarkCheckpointIntervalMsProp = "replica.high.watermark.checkpoint.interval.ms"
  val FetchPurgatoryPurgeIntervalRequestsProp = "fetch.purgatory.purge.interval.requests"
  val ProducerPurgatoryPurgeIntervalRequestsProp = "producer.purgatory.purge.interval.requests"
  val DeleteRecordsPurgatoryPurgeIntervalRequestsProp = "delete.records.purgatory.purge.interval.requests"
  val AutoLeaderRebalanceEnableProp = "auto.leader.rebalance.enable"
  val LeaderImbalancePerBrokerPercentageProp = "leader.imbalance.per.broker.percentage"
  val LeaderImbalanceCheckIntervalSecondsProp = "leader.imbalance.check.interval.seconds"
  val UncleanLeaderElectionEnableProp = "unclean.leader.election.enable"
  val InterBrokerSecurityProtocolProp = "security.inter.broker.protocol"
  val InterBrokerProtocolVersionProp = "inter.broker.protocol.version"
  val InterBrokerListenerNameProp = "inter.broker.listener.name"
  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetriesProp = "controlled.shutdown.max.retries"
  val ControlledShutdownRetryBackoffMsProp = "controlled.shutdown.retry.backoff.ms"
  val ControlledShutdownEnableProp = "controlled.shutdown.enable"
  /** ********* Group coordinator configuration ***********/
  val GroupMinSessionTimeoutMsProp = "group.min.session.timeout.ms"
  val GroupMaxSessionTimeoutMsProp = "group.max.session.timeout.ms"
  val GroupInitialRebalanceDelayMsProp = "group.initial.rebalance.delay.ms"
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
  /** ********* Transaction management configuration ***********/
  val TransactionalIdExpirationMsProp = "transactional.id.expiration.ms"
  val TransactionsMaxTimeoutMsProp = "transaction.max.timeout.ms"
  val TransactionsTopicMinISRProp = "transaction.state.log.min.isr"
  val TransactionsLoadBufferSizeProp = "transaction.state.log.load.buffer.size"
  val TransactionsTopicPartitionsProp = "transaction.state.log.num.partitions"
  val TransactionsTopicSegmentBytesProp = "transaction.state.log.segment.bytes"
  val TransactionsTopicReplicationFactorProp = "transaction.state.log.replication.factor"
  val TransactionsAbortTimedOutTransactionCleanupIntervalMsProp = "transaction.abort.timed.out.transaction.cleanup.interval.ms"
  val TransactionsRemoveExpiredTransactionalIdCleanupIntervalMsProp = "transaction.remove.expired.transaction.cleanup.interval.ms"

  /** ********* Fetch Session Configuration **************/
  val MaxIncrementalFetchSessionCacheSlots = "max.incremental.fetch.session.cache.slots"

  /** ********* Quota Configuration ***********/
  val ProducerQuotaBytesPerSecondDefaultProp = "quota.producer.default"
  val ConsumerQuotaBytesPerSecondDefaultProp = "quota.consumer.default"
  val NumQuotaSamplesProp = "quota.window.num"
  val NumReplicationQuotaSamplesProp = "replication.quota.window.num"
  val NumAlterLogDirsReplicationQuotaSamplesProp = "alter.log.dirs.replication.quota.window.num"
  val QuotaWindowSizeSecondsProp = "quota.window.size.seconds"
  val ReplicationQuotaWindowSizeSecondsProp = "replication.quota.window.size.seconds"
  val AlterLogDirsReplicationQuotaWindowSizeSecondsProp = "alter.log.dirs.replication.quota.window.size.seconds"
  val ClientQuotaCallbackClassProp = "client.quota.callback.class"

  val DeleteTopicEnableProp = "delete.topic.enable"
  val CompressionTypeProp = "compression.type"

  /** ********* Kafka Metrics Configuration ***********/
  val MetricSampleWindowMsProp = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG
  val MetricNumSamplesProp: String = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG
  val MetricReporterClassesProp: String = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG
  val MetricRecordingLevelProp: String = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG

  /** ******** Common Security Configuration *************/
  val PrincipalBuilderClassProp = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG

  /** ********* SSL Configuration ****************/
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
  val SslSecureRandomImplementationProp = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG
  val SslClientAuthProp = BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG

  /** ********* SASL Configuration ****************/
  val SaslMechanismInterBrokerProtocolProp = "sasl.mechanism.inter.broker.protocol"
  val SaslJaasConfigProp = SaslConfigs.SASL_JAAS_CONFIG
  val SaslEnabledMechanismsProp = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG
  val SaslServerCallbackHandlerClassProp = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS
  val SaslClientCallbackHandlerClassProp = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS
  val SaslLoginClassProp = SaslConfigs.SASL_LOGIN_CLASS
  val SaslLoginCallbackHandlerClassProp = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS
  val SaslKerberosServiceNameProp = SaslConfigs.SASL_KERBEROS_SERVICE_NAME
  val SaslKerberosKinitCmdProp = SaslConfigs.SASL_KERBEROS_KINIT_CMD
  val SaslKerberosTicketRenewWindowFactorProp = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR
  val SaslKerberosTicketRenewJitterProp = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER
  val SaslKerberosMinTimeBeforeReloginProp = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN
  val SaslKerberosPrincipalToLocalRulesProp = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG
  val SaslLoginRefreshWindowFactorProp = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR
  val SaslLoginRefreshWindowJitterProp = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER
  val SaslLoginRefreshMinPeriodSecondsProp = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS
  val SaslLoginRefreshBufferSecondsProp = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS

  /** ********* Delegation Token Configuration ****************/
  val DelegationTokenMasterKeyProp = "delegation.token.master.key"
  val DelegationTokenMaxLifeTimeProp = "delegation.token.max.lifetime.ms"
  val DelegationTokenExpiryTimeMsProp = "delegation.token.expiry.time.ms"
  val DelegationTokenExpiryCheckIntervalMsProp = "delegation.token.expiry.check.interval.ms"

  /** ********* Password encryption configuration for dynamic configs *********/
  val PasswordEncoderSecretProp = "password.encoder.secret"
  val PasswordEncoderOldSecretProp = "password.encoder.old.secret"
  val PasswordEncoderKeyFactoryAlgorithmProp = "password.encoder.keyfactory.algorithm"
  val PasswordEncoderCipherAlgorithmProp = "password.encoder.cipher.algorithm"
  val PasswordEncoderKeyLengthProp =  "password.encoder.key.length"
  val PasswordEncoderIterationsProp =  "password.encoder.iterations"

  /* Documentation */
  /** ********* Zookeeper Configuration ***********/
  val ZkConnectDoc = "Zookeeper host string"
  val ZkSessionTimeoutMsDoc = "Zookeeper session timeout"
  val ZkConnectionTimeoutMsDoc = "The max time that the client waits to establish a connection to zookeeper. If not set, the value in " + ZkSessionTimeoutMsProp + " is used"
  val ZkSyncTimeMsDoc = "How far a ZK follower can be behind a ZK leader"
  val ZkEnableSecureAclsDoc = "Set client to use secure ACLs"
  val ZkMaxInFlightRequestsDoc = "The maximum number of unacknowledged requests the client will send to Zookeeper before blocking."
  /** ********* General Configuration ***********/
  val BrokerIdGenerationEnableDoc = s"Enable automatic broker id generation on the server. When enabled the value configured for $MaxReservedBrokerIdProp should be reviewed."
  val MaxReservedBrokerIdDoc = "Max number that can be used for a broker.id"
  val BrokerIdDoc = "The broker id for this server. If unset, a unique broker id will be generated." +
  "To avoid conflicts between zookeeper generated broker id's and user configured broker id's, generated broker ids " +
  "start from " + MaxReservedBrokerIdProp + " + 1."
  val MessageMaxBytesDoc = TopicConfig.MAX_MESSAGE_BYTES_DOC +
    s"<p>This can be set per topic with the topic level <code>${TopicConfig.MAX_MESSAGE_BYTES_CONFIG}</code> config.</p>"
  val NumNetworkThreadsDoc = "The number of threads that the server uses for receiving requests from the network and sending responses to the network"
  val NumIoThreadsDoc = "The number of threads that the server uses for processing requests, which may include disk I/O"
  val NumReplicaAlterLogDirsThreadsDoc = "The number of threads that can move replicas between log directories, which may include disk I/O"
  val BackgroundThreadsDoc = "The number of threads to use for various background processing tasks"
  val QueuedMaxRequestsDoc = "The number of queued requests allowed before blocking the network threads"
  val QueuedMaxRequestBytesDoc = "The number of queued bytes allowed before no more requests are read"
  val RequestTimeoutMsDoc = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC
  /************* Authorizer Configuration ***********/
  val AuthorizerClassNameDoc = "The authorizer class that should be used for authorization"
  /** ********* Socket Server Configuration ***********/
  val PortDoc = "DEPRECATED: only used when `listeners` is not set. " +
  "Use `listeners` instead. \n" +
  "the port to listen and accept connections on"
  val HostNameDoc = "DEPRECATED: only used when `listeners` is not set. " +
  "Use `listeners` instead. \n" +
  "hostname of broker. If this is set, it will only bind to this address. If this is not set, it will bind to all interfaces"
  val ListenersDoc = "Listener List - Comma-separated list of URIs we will listen on and the listener names." +
  s" If the listener name is not a security protocol, $ListenerSecurityProtocolMapProp must also be set.\n" +
  " Specify hostname as 0.0.0.0 to bind to all interfaces.\n" +
  " Leave hostname empty to bind to default interface.\n" +
  " Examples of legal listener lists:\n" +
  " PLAINTEXT://myhost:9092,SSL://:9091\n" +
  " CLIENT://0.0.0.0:9092,REPLICATION://localhost:9093\n"
  val AdvertisedHostNameDoc = "DEPRECATED: only used when `advertised.listeners` or `listeners` are not set. " +
  "Use `advertised.listeners` instead. \n" +
  "Hostname to publish to ZooKeeper for clients to use. In IaaS environments, this may " +
  "need to be different from the interface to which the broker binds. If this is not set, " +
  "it will use the value for `host.name` if configured. Otherwise " +
  "it will use the value returned from java.net.InetAddress.getCanonicalHostName()."
  val AdvertisedPortDoc = "DEPRECATED: only used when `advertised.listeners` or `listeners` are not set. " +
  "Use `advertised.listeners` instead. \n" +
  "The port to publish to ZooKeeper for clients to use. In IaaS environments, this may " +
  "need to be different from the port to which the broker binds. If this is not set, " +
  "it will publish the same port that the broker binds to."
  val AdvertisedListenersDoc = "Listeners to publish to ZooKeeper for clients to use, if different than the `listeners` config property." +
  " In IaaS environments, this may need to be different from the interface to which the broker binds." +
  " If this is not set, the value for `listeners` will be used." +
  " Unlike `listeners` it is not valid to advertise the 0.0.0.0 meta-address."
  val ListenerSecurityProtocolMapDoc = "Map between listener names and security protocols. This must be defined for " +
    "the same security protocol to be usable in more than one port or IP. For example, internal and " +
    "external traffic can be separated even if SSL is required for both. Concretely, the user could define listeners " +
    "with names INTERNAL and EXTERNAL and this property as: `INTERNAL:SSL,EXTERNAL:SSL`. As shown, key and value are " +
    "separated by a colon and map entries are separated by commas. Each listener name should only appear once in the map. " +
    "Different security (SSL and SASL) settings can be configured for each listener by adding a normalised " +
    "prefix (the listener name is lowercased) to the config name. For example, to set a different keystore for the " +
    "INTERNAL listener, a config with name `listener.name.internal.ssl.keystore.location` would be set. " +
    "If the config for the listener name is not set, the config will fallback to the generic config (i.e. `ssl.keystore.location`). "

  val SocketSendBufferBytesDoc = "The SO_SNDBUF buffer of the socket sever sockets. If the value is -1, the OS default will be used."
  val SocketReceiveBufferBytesDoc = "The SO_RCVBUF buffer of the socket sever sockets. If the value is -1, the OS default will be used."
  val SocketRequestMaxBytesDoc = "The maximum number of bytes in a socket request"
  val MaxConnectionsPerIpDoc = "The maximum number of connections we allow from each ip address. This can be set to 0 if there are overrides " +
    "configured using " + MaxConnectionsPerIpOverridesProp + " property"
  val MaxConnectionsPerIpOverridesDoc = "A comma-separated list of per-ip or hostname overrides to the default maximum number of connections. An example value is \"hostName:100,127.0.0.1:200\""
  val ConnectionsMaxIdleMsDoc = "Idle connections timeout: the server socket processor threads close the connections that idle more than this"
  /************* Rack Configuration **************/
  val RackDoc = "Rack of the broker. This will be used in rack aware replication assignment for fault tolerance. Examples: `RACK1`, `us-east-1d`"
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
  val LogCleanupPolicyDoc = "The default cleanup policy for segments beyond the retention window. A comma separated list of valid policies. Valid policies are: \"delete\" and \"compact\""
  val LogCleanerThreadsDoc = "The number of background threads to use for log cleaning"
  val LogCleanerIoMaxBytesPerSecondDoc = "The log cleaner will be throttled so that the sum of its read and write i/o will be less than this value on average"
  val LogCleanerDedupeBufferSizeDoc = "The total memory used for log deduplication across all cleaner threads"
  val LogCleanerIoBufferSizeDoc = "The total memory used for log cleaner I/O buffers across all cleaner threads"
  val LogCleanerDedupeBufferLoadFactorDoc = "Log cleaner dedupe buffer load factor. The percentage full the dedupe buffer can become. A higher value " +
  "will allow more log to be cleaned at once but will lead to more hash collisions"
  val LogCleanerBackoffMsDoc = "The amount of time to sleep when there are no logs to clean"
  val LogCleanerMinCleanRatioDoc = "The minimum ratio of dirty log to total log for a log to eligible for cleaning"
  val LogCleanerEnableDoc = "Enable the log cleaner process to run on the server. Should be enabled if using any topics with a cleanup.policy=compact including the internal offsets topic. If disabled those topics will not be compacted and continually grow in size."
  val LogCleanerDeleteRetentionMsDoc = "How long are delete records retained?"
  val LogCleanerMinCompactionLagMsDoc = "The minimum time a message will remain uncompacted in the log. Only applicable for logs that are being compacted."
  val LogIndexSizeMaxBytesDoc = "The maximum size in bytes of the offset index"
  val LogIndexIntervalBytesDoc = "The interval with which we add an entry to the offset index"
  val LogFlushIntervalMessagesDoc = "The number of messages accumulated on a log partition before messages are flushed to disk "
  val LogDeleteDelayMsDoc = "The amount of time to wait before deleting a file from the filesystem"
  val LogFlushSchedulerIntervalMsDoc = "The frequency in ms that the log flusher checks whether any log needs to be flushed to disk"
  val LogFlushIntervalMsDoc = "The maximum time in ms that a message in any topic is kept in memory before flushed to disk. If not set, the value in " + LogFlushSchedulerIntervalMsProp + " is used"
  val LogFlushOffsetCheckpointIntervalMsDoc = "The frequency with which we update the persistent record of the last flush which acts as the log recovery point"
  val LogFlushStartOffsetCheckpointIntervalMsDoc = "The frequency with which we update the persistent record of log start offset"
  val LogPreAllocateEnableDoc = "Should pre allocate file when create new segment? If you are using Kafka on Windows, you probably need to set it to true."
  val LogMessageFormatVersionDoc = "Specify the message format version the broker will use to append messages to the logs. The value should be a valid ApiVersion. " +
    "Some examples are: 0.8.2, 0.9.0.0, 0.10.0, check ApiVersion for more details. By setting a particular message format version, the " +
    "user is certifying that all the existing messages on disk are smaller or equal than the specified version. Setting this value incorrectly " +
    "will cause consumers with older versions to break as they will receive messages with a format that they don't understand."

  val LogMessageTimestampTypeDoc = "Define whether the timestamp in the message is message create time or log append time. The value should be either " +
    "`CreateTime` or `LogAppendTime`"

  val LogMessageTimestampDifferenceMaxMsDoc = "The maximum difference allowed between the timestamp when a broker receives " +
    "a message and the timestamp specified in the message. If log.message.timestamp.type=CreateTime, a message will be rejected " +
    "if the difference in timestamp exceeds this threshold. This configuration is ignored if log.message.timestamp.type=LogAppendTime." +
    "The maximum timestamp difference allowed should be no greater than log.retention.ms to avoid unnecessarily frequent log rolling."
  val NumRecoveryThreadsPerDataDirDoc = "The number of threads per data directory to be used for log recovery at startup and flushing at shutdown"
  val AutoCreateTopicsEnableDoc = "Enable auto creation of topic on the server"
  val MinInSyncReplicasDoc = "When a producer sets acks to \"all\" (or \"-1\"), " +
    "min.insync.replicas specifies the minimum number of replicas that must acknowledge " +
    "a write for the write to be considered successful. If this minimum cannot be met, " +
    "then the producer will raise an exception (either NotEnoughReplicas or " +
    "NotEnoughReplicasAfterAppend).<br>When used together, min.insync.replicas and acks " +
    "allow you to enforce greater durability guarantees. A typical scenario would be to " +
    "create a topic with a replication factor of 3, set min.insync.replicas to 2, and " +
    "produce with acks of \"all\". This will ensure that the producer raises an exception " +
    "if a majority of replicas do not receive a write."

  val CreateTopicPolicyClassNameDoc = "The create topic policy class that should be used for validation. The class should " +
    "implement the <code>org.apache.kafka.server.policy.CreateTopicPolicy</code> interface."
  val AlterConfigPolicyClassNameDoc = "The alter configs policy class that should be used for validation. The class should " +
    "implement the <code>org.apache.kafka.server.policy.AlterConfigPolicy</code> interface."

  /** ********* Replication configuration ***********/
  val ControllerSocketTimeoutMsDoc = "The socket timeout for controller-to-broker channels"
  val ControllerMessageQueueSizeDoc = "The buffer size for controller-to-broker-channels"
  val DefaultReplicationFactorDoc = "default replication factors for automatically created topics"
  val ReplicaLagTimeMaxMsDoc = "If a follower hasn't sent any fetch requests or hasn't consumed up to the leaders log end offset for at least this time," +
  " the leader will remove the follower from isr"
  val ReplicaSocketTimeoutMsDoc = "The socket timeout for network requests. Its value should be at least replica.fetch.wait.max.ms"
  val ReplicaSocketReceiveBufferBytesDoc = "The socket receive buffer for network requests"
  val ReplicaFetchMaxBytesDoc = "The number of bytes of messages to attempt to fetch for each partition. This is not an absolute maximum, " +
    "if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch will still be returned " +
    "to ensure that progress can be made. The maximum record batch size accepted by the broker is defined via " +
    "<code>message.max.bytes</code> (broker config) or <code>max.message.bytes</code> (topic config)."
  val ReplicaFetchWaitMaxMsDoc = "max wait time for each fetcher request issued by follower replicas. This value should always be less than the " +
  "replica.lag.time.max.ms at all times to prevent frequent shrinking of ISR for low throughput topics"
  val ReplicaFetchMinBytesDoc = "Minimum bytes expected for each fetch response. If not enough bytes, wait up to replicaMaxWaitTimeMs"
  val ReplicaFetchResponseMaxBytesDoc = "Maximum bytes expected for the entire fetch response. Records are fetched in batches, " +
    "and if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch " +
    "will still be returned to ensure that progress can be made. As such, this is not an absolute maximum. The maximum " +
    "record batch size accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or " +
    "<code>max.message.bytes</code> (topic config)."
  val NumReplicaFetchersDoc = "Number of fetcher threads used to replicate messages from a source broker. " +
  "Increasing this value can increase the degree of I/O parallelism in the follower broker."
  val ReplicaFetchBackoffMsDoc = "The amount of time to sleep when fetch partition error occurs."
  val ReplicaHighWatermarkCheckpointIntervalMsDoc = "The frequency with which the high watermark is saved out to disk"
  val FetchPurgatoryPurgeIntervalRequestsDoc = "The purge interval (in number of requests) of the fetch request purgatory"
  val ProducerPurgatoryPurgeIntervalRequestsDoc = "The purge interval (in number of requests) of the producer request purgatory"
  val DeleteRecordsPurgatoryPurgeIntervalRequestsDoc = "The purge interval (in number of requests) of the delete records request purgatory"
  val AutoLeaderRebalanceEnableDoc = "Enables auto leader balancing. A background thread checks and triggers leader balance if required at regular intervals"
  val LeaderImbalancePerBrokerPercentageDoc = "The ratio of leader imbalance allowed per broker. The controller would trigger a leader balance if it goes above this value per broker. The value is specified in percentage."
  val LeaderImbalanceCheckIntervalSecondsDoc = "The frequency with which the partition rebalance check is triggered by the controller"
  val UncleanLeaderElectionEnableDoc = "Indicates whether to enable replicas not in the ISR set to be elected as leader as a last resort, even though doing so may result in data loss"
  val InterBrokerSecurityProtocolDoc = "Security protocol used to communicate between brokers. Valid values are: " +
    s"${SecurityProtocol.names.asScala.mkString(", ")}. It is an error to set this and $InterBrokerListenerNameProp " +
    "properties at the same time."
  val InterBrokerProtocolVersionDoc = "Specify which version of the inter-broker protocol will be used.\n" +
  " This is typically bumped after all brokers were upgraded to a new version.\n" +
  " Example of some valid values are: 0.8.0, 0.8.1, 0.8.1.1, 0.8.2, 0.8.2.0, 0.8.2.1, 0.9.0.0, 0.9.0.1 Check ApiVersion for the full list."
  val InterBrokerListenerNameDoc = s"Name of listener used for communication between brokers. If this is unset, the listener name is defined by $InterBrokerSecurityProtocolProp. " +
    s"It is an error to set this and $InterBrokerSecurityProtocolProp properties at the same time."
  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetriesDoc = "Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens"
  val ControlledShutdownRetryBackoffMsDoc = "Before each retry, the system needs time to recover from the state that caused the previous failure (Controller fail over, replica lag etc). This config determines the amount of time to wait before retrying."
  val ControlledShutdownEnableDoc = "Enable controlled shutdown of the server"
  /** ********* Consumer coordinator configuration ***********/
  val GroupMinSessionTimeoutMsDoc = "The minimum allowed session timeout for registered consumers. Shorter timeouts result in quicker failure detection at the cost of more frequent consumer heartbeating, which can overwhelm broker resources."
  val GroupMaxSessionTimeoutMsDoc = "The maximum allowed session timeout for registered consumers. Longer timeouts give consumers more time to process messages in between heartbeats at the cost of a longer time to detect failures."
  val GroupInitialRebalanceDelayMsDoc = "The amount of time the group coordinator will wait for more consumers to join a new group before performing the first rebalance. A longer delay means potentially fewer rebalances, but increases the time until processing begins."
  /** ********* Offset management configuration ***********/
  val OffsetMetadataMaxSizeDoc = "The maximum size for a metadata entry associated with an offset commit"
  val OffsetsLoadBufferSizeDoc = "Batch size for reading from the offsets segments when loading offsets into the cache."
  val OffsetsTopicReplicationFactorDoc = "The replication factor for the offsets topic (set higher to ensure availability). " +
  "Internal topic creation will fail until the cluster size meets this replication factor requirement."
  val OffsetsTopicPartitionsDoc = "The number of partitions for the offset commit topic (should not change after deployment)"
  val OffsetsTopicSegmentBytesDoc = "The offsets topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads"
  val OffsetsTopicCompressionCodecDoc = "Compression codec for the offsets topic - compression may be used to achieve \"atomic\" commits"
  val OffsetsRetentionMinutesDoc = "Offsets older than this retention period will be discarded"
  val OffsetsRetentionCheckIntervalMsDoc = "Frequency at which to check for stale offsets"
  val OffsetCommitTimeoutMsDoc = "Offset commit will be delayed until all replicas for the offsets topic receive the commit " +
  "or this timeout is reached. This is similar to the producer request timeout."
  val OffsetCommitRequiredAcksDoc = "The required acks before the commit can be accepted. In general, the default (-1) should not be overridden"
  /** ********* Transaction management configuration ***********/
  val TransactionalIdExpirationMsDoc = "The maximum amount of time in ms that the transaction coordinator will wait before proactively expire a producer's transactional id without receiving any transaction status updates from it."
  val TransactionsMaxTimeoutMsDoc = "The maximum allowed timeout for transactions. " +
    "If a client’s requested transaction time exceed this, then the broker will return an error in InitProducerIdRequest. This prevents a client from too large of a timeout, which can stall consumers reading from topics included in the transaction."
  val TransactionsTopicMinISRDoc = "Overridden " + MinInSyncReplicasProp + " config for the transaction topic."
  val TransactionsLoadBufferSizeDoc = "Batch size for reading from the transaction log segments when loading producer ids and transactions into the cache."
  val TransactionsTopicReplicationFactorDoc = "The replication factor for the transaction topic (set higher to ensure availability). " +
    "Internal topic creation will fail until the cluster size meets this replication factor requirement."
  val TransactionsTopicPartitionsDoc = "The number of partitions for the transaction topic (should not change after deployment)."
  val TransactionsTopicSegmentBytesDoc = "The transaction topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads"
  val TransactionsAbortTimedOutTransactionsIntervalMsDoc = "The interval at which to rollback transactions that have timed out"
  val TransactionsRemoveExpiredTransactionsIntervalMsDoc = "The interval at which to remove transactions that have expired due to <code>transactional.id.expiration.ms<code> passing"

  /** ********* Fetch Session Configuration **************/
  val MaxIncrementalFetchSessionCacheSlotsDoc = "The maximum number of incremental fetch sessions that we will maintain."

  /** ********* Quota Configuration ***********/
  val ProducerQuotaBytesPerSecondDefaultDoc = "DEPRECATED: Used only when dynamic default quotas are not configured for <user>, <client-id> or <user, client-id> in Zookeeper. " +
  "Any producer distinguished by clientId will get throttled if it produces more bytes than this value per-second"
  val ConsumerQuotaBytesPerSecondDefaultDoc = "DEPRECATED: Used only when dynamic default quotas are not configured for <user, <client-id> or <user, client-id> in Zookeeper. " +
  "Any consumer distinguished by clientId/consumer group will get throttled if it fetches more bytes than this value per-second"
  val NumQuotaSamplesDoc = "The number of samples to retain in memory for client quotas"
  val NumReplicationQuotaSamplesDoc = "The number of samples to retain in memory for replication quotas"
  val NumAlterLogDirsReplicationQuotaSamplesDoc = "The number of samples to retain in memory for alter log dirs replication quotas"
  val QuotaWindowSizeSecondsDoc = "The time span of each sample for client quotas"
  val ReplicationQuotaWindowSizeSecondsDoc = "The time span of each sample for replication quotas"
  val AlterLogDirsReplicationQuotaWindowSizeSecondsDoc = "The time span of each sample for alter log dirs replication quotas"
  val ClientQuotaCallbackClassDoc = "The fully qualified name of a class that implements the ClientQuotaCallback interface, " +
    "which is used to determine quota limits applied to client requests. By default, <user, client-id>, <user> or <client-id> " +
    "quotas stored in ZooKeeper are applied. For any given request, the most specific quota that matches the user principal " +
    "of the session and the client-id of the request is applied."
  /** ********* Transaction Configuration ***********/
  val TransactionIdExpirationMsDoc = "The maximum time of inactivity before a transactional id is expired by the " +
    "transaction coordinator. Note that this also influences producer id expiration: Producer ids are guaranteed to expire " +
    "after expiration of this timeout from the last write by the producer id (they may expire sooner if the last write " +
    "from the producer id is deleted due to the topic's retention settings)."

  val DeleteTopicEnableDoc = "Enables delete topic. Delete topic through the admin tool will have no effect if this config is turned off"
  val CompressionTypeDoc = "Specify the final compression type for a given topic. This configuration accepts the standard compression codecs " +
  "('gzip', 'snappy', 'lz4'). It additionally accepts 'uncompressed' which is equivalent to no compression; and " +
  "'producer' which means retain the original compression codec set by the producer."

  /** ********* Kafka Metrics Configuration ***********/
  val MetricSampleWindowMsDoc = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC
  val MetricNumSamplesDoc = CommonClientConfigs.METRICS_NUM_SAMPLES_DOC
  val MetricReporterClassesDoc = CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC
  val MetricRecordingLevelDoc = CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC

  /** ******** Common Security Configuration *************/
  val PrincipalBuilderClassDoc = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC

  /** ********* SSL Configuration ****************/
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
  val SslSecureRandomImplementationDoc = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC
  val SslClientAuthDoc = BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC

  /** ********* Sasl Configuration ****************/
  val SaslMechanismInterBrokerProtocolDoc = "SASL mechanism used for inter-broker communication. Default is GSSAPI."
  val SaslJaasConfigDoc = SaslConfigs.SASL_JAAS_CONFIG_DOC
  val SaslEnabledMechanismsDoc = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC
  val SaslServerCallbackHandlerClassDoc = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC
  val SaslClientCallbackHandlerClassDoc = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC
  val SaslLoginClassDoc = SaslConfigs.SASL_LOGIN_CLASS_DOC
  val SaslLoginCallbackHandlerClassDoc = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC
  val SaslKerberosServiceNameDoc = SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC
  val SaslKerberosKinitCmdDoc = SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC
  val SaslKerberosTicketRenewWindowFactorDoc = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC
  val SaslKerberosTicketRenewJitterDoc = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC
  val SaslKerberosMinTimeBeforeReloginDoc = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC
  val SaslKerberosPrincipalToLocalRulesDoc = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC
  val SaslLoginRefreshWindowFactorDoc = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC
  val SaslLoginRefreshWindowJitterDoc = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC
  val SaslLoginRefreshMinPeriodSecondsDoc = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC
  val SaslLoginRefreshBufferSecondsDoc = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC

  /** ********* Delegation Token Configuration ****************/
  val DelegationTokenMasterKeyDoc = "Master/secret key to generate and verify delegation tokens. Same key must be configured across all the brokers. " +
    " If the key is not set or set to empty string, brokers will disable the delegation token support."
  val DelegationTokenMaxLifeTimeDoc = "The token has a maximum lifetime beyond which it cannot be renewed anymore. Default value 7 days."
  val DelegationTokenExpiryTimeMsDoc = "The token validity time in seconds before the token needs to be renewed. Default value 1 day."
  val DelegationTokenExpiryCheckIntervalDoc = "Scan interval to remove expired delegation tokens."

  /** ********* Password encryption configuration for dynamic configs *********/
  val PasswordEncoderSecretDoc = "The secret used for encoding dynamically configured passwords for this broker."
  val PasswordEncoderOldSecretDoc = "The old secret that was used for encoding dynamically configured passwords. " +
    "This is required only when the secret is updated. If specified, all dynamically encoded passwords are " +
    s"decoded using this old secret and re-encoded using $PasswordEncoderSecretProp when broker starts up."
  val PasswordEncoderKeyFactoryAlgorithmDoc = "The SecretKeyFactory algorithm used for encoding dynamically configured passwords. " +
    "Default is PBKDF2WithHmacSHA512 if available and PBKDF2WithHmacSHA1 otherwise."
  val PasswordEncoderCipherAlgorithmDoc = "The Cipher algorithm used for encoding dynamically configured passwords."
  val PasswordEncoderKeyLengthDoc =  "The key length used for encoding dynamically configured passwords."
  val PasswordEncoderIterationsDoc =  "The iteration count used for encoding dynamically configured passwords."

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
      .define(ZkMaxInFlightRequestsProp, INT, Defaults.ZkMaxInFlightRequests, atLeast(1), HIGH, ZkMaxInFlightRequestsDoc)

      /** ********* General Configuration ***********/
      .define(BrokerIdGenerationEnableProp, BOOLEAN, Defaults.BrokerIdGenerationEnable, MEDIUM, BrokerIdGenerationEnableDoc)
      .define(MaxReservedBrokerIdProp, INT, Defaults.MaxReservedBrokerId, atLeast(0), MEDIUM, MaxReservedBrokerIdDoc)
      .define(BrokerIdProp, INT, Defaults.BrokerId, HIGH, BrokerIdDoc)
      .define(MessageMaxBytesProp, INT, Defaults.MessageMaxBytes, atLeast(0), HIGH, MessageMaxBytesDoc)
      .define(NumNetworkThreadsProp, INT, Defaults.NumNetworkThreads, atLeast(1), HIGH, NumNetworkThreadsDoc)
      .define(NumIoThreadsProp, INT, Defaults.NumIoThreads, atLeast(1), HIGH, NumIoThreadsDoc)
      .define(NumReplicaAlterLogDirsThreadsProp, INT, null, HIGH, NumReplicaAlterLogDirsThreadsDoc)
      .define(BackgroundThreadsProp, INT, Defaults.BackgroundThreads, atLeast(1), HIGH, BackgroundThreadsDoc)
      .define(QueuedMaxRequestsProp, INT, Defaults.QueuedMaxRequests, atLeast(1), HIGH, QueuedMaxRequestsDoc)
      .define(QueuedMaxBytesProp, LONG, Defaults.QueuedMaxRequestBytes, MEDIUM, QueuedMaxRequestBytesDoc)
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
      .define(ListenerSecurityProtocolMapProp, STRING, Defaults.ListenerSecurityProtocolMap, LOW, ListenerSecurityProtocolMapDoc)
      .define(SocketSendBufferBytesProp, INT, Defaults.SocketSendBufferBytes, HIGH, SocketSendBufferBytesDoc)
      .define(SocketReceiveBufferBytesProp, INT, Defaults.SocketReceiveBufferBytes, HIGH, SocketReceiveBufferBytesDoc)
      .define(SocketRequestMaxBytesProp, INT, Defaults.SocketRequestMaxBytes, atLeast(1), HIGH, SocketRequestMaxBytesDoc)
      .define(MaxConnectionsPerIpProp, INT, Defaults.MaxConnectionsPerIp, atLeast(0), MEDIUM, MaxConnectionsPerIpDoc)
      .define(MaxConnectionsPerIpOverridesProp, STRING, Defaults.MaxConnectionsPerIpOverrides, MEDIUM, MaxConnectionsPerIpOverridesDoc)
      .define(ConnectionsMaxIdleMsProp, LONG, Defaults.ConnectionsMaxIdleMs, MEDIUM, ConnectionsMaxIdleMsDoc)

      /************ Rack Configuration ******************/
      .define(RackProp, STRING, null, MEDIUM, RackDoc)

      /** ********* Log Configuration ***********/
      .define(NumPartitionsProp, INT, Defaults.NumPartitions, atLeast(1), MEDIUM, NumPartitionsDoc)
      .define(LogDirProp, STRING, Defaults.LogDir, HIGH, LogDirDoc)
      .define(LogDirsProp, STRING, null, HIGH, LogDirsDoc)
      .define(LogSegmentBytesProp, INT, Defaults.LogSegmentBytes, atLeast(Message.MinMessageOverhead), HIGH, LogSegmentBytesDoc)

      .define(LogRollTimeMillisProp, LONG, null, HIGH, LogRollTimeMillisDoc)
      .define(LogRollTimeHoursProp, INT, Defaults.LogRollHours, atLeast(1), HIGH, LogRollTimeHoursDoc)

      .define(LogRollTimeJitterMillisProp, LONG, null, HIGH, LogRollTimeJitterMillisDoc)
      .define(LogRollTimeJitterHoursProp, INT, Defaults.LogRollJitterHours, atLeast(0), HIGH, LogRollTimeJitterHoursDoc)

      .define(LogRetentionTimeMillisProp, LONG, null, HIGH, LogRetentionTimeMillisDoc)
      .define(LogRetentionTimeMinutesProp, INT, null, HIGH, LogRetentionTimeMinsDoc)
      .define(LogRetentionTimeHoursProp, INT, Defaults.LogRetentionHours, HIGH, LogRetentionTimeHoursDoc)

      .define(LogRetentionBytesProp, LONG, Defaults.LogRetentionBytes, HIGH, LogRetentionBytesDoc)
      .define(LogCleanupIntervalMsProp, LONG, Defaults.LogCleanupIntervalMs, atLeast(1), MEDIUM, LogCleanupIntervalMsDoc)
      .define(LogCleanupPolicyProp, LIST, Defaults.LogCleanupPolicy, ValidList.in(Defaults.Compact, Defaults.Delete), MEDIUM, LogCleanupPolicyDoc)
      .define(LogCleanerThreadsProp, INT, Defaults.LogCleanerThreads, atLeast(0), MEDIUM, LogCleanerThreadsDoc)
      .define(LogCleanerIoMaxBytesPerSecondProp, DOUBLE, Defaults.LogCleanerIoMaxBytesPerSecond, MEDIUM, LogCleanerIoMaxBytesPerSecondDoc)
      .define(LogCleanerDedupeBufferSizeProp, LONG, Defaults.LogCleanerDedupeBufferSize, MEDIUM, LogCleanerDedupeBufferSizeDoc)
      .define(LogCleanerIoBufferSizeProp, INT, Defaults.LogCleanerIoBufferSize, atLeast(0), MEDIUM, LogCleanerIoBufferSizeDoc)
      .define(LogCleanerDedupeBufferLoadFactorProp, DOUBLE, Defaults.LogCleanerDedupeBufferLoadFactor, MEDIUM, LogCleanerDedupeBufferLoadFactorDoc)
      .define(LogCleanerBackoffMsProp, LONG, Defaults.LogCleanerBackoffMs, atLeast(0), MEDIUM, LogCleanerBackoffMsDoc)
      .define(LogCleanerMinCleanRatioProp, DOUBLE, Defaults.LogCleanerMinCleanRatio, MEDIUM, LogCleanerMinCleanRatioDoc)
      .define(LogCleanerEnableProp, BOOLEAN, Defaults.LogCleanerEnable, MEDIUM, LogCleanerEnableDoc)
      .define(LogCleanerDeleteRetentionMsProp, LONG, Defaults.LogCleanerDeleteRetentionMs, MEDIUM, LogCleanerDeleteRetentionMsDoc)
      .define(LogCleanerMinCompactionLagMsProp, LONG, Defaults.LogCleanerMinCompactionLagMs, MEDIUM, LogCleanerMinCompactionLagMsDoc)
      .define(LogIndexSizeMaxBytesProp, INT, Defaults.LogIndexSizeMaxBytes, atLeast(4), MEDIUM, LogIndexSizeMaxBytesDoc)
      .define(LogIndexIntervalBytesProp, INT, Defaults.LogIndexIntervalBytes, atLeast(0), MEDIUM, LogIndexIntervalBytesDoc)
      .define(LogFlushIntervalMessagesProp, LONG, Defaults.LogFlushIntervalMessages, atLeast(1), HIGH, LogFlushIntervalMessagesDoc)
      .define(LogDeleteDelayMsProp, LONG, Defaults.LogDeleteDelayMs, atLeast(0), HIGH, LogDeleteDelayMsDoc)
      .define(LogFlushSchedulerIntervalMsProp, LONG, Defaults.LogFlushSchedulerIntervalMs, HIGH, LogFlushSchedulerIntervalMsDoc)
      .define(LogFlushIntervalMsProp, LONG, null, HIGH, LogFlushIntervalMsDoc)
      .define(LogFlushOffsetCheckpointIntervalMsProp, INT, Defaults.LogFlushOffsetCheckpointIntervalMs, atLeast(0), HIGH, LogFlushOffsetCheckpointIntervalMsDoc)
      .define(LogFlushStartOffsetCheckpointIntervalMsProp, INT, Defaults.LogFlushStartOffsetCheckpointIntervalMs, atLeast(0), HIGH, LogFlushStartOffsetCheckpointIntervalMsDoc)
      .define(LogPreAllocateProp, BOOLEAN, Defaults.LogPreAllocateEnable, MEDIUM, LogPreAllocateEnableDoc)
      .define(NumRecoveryThreadsPerDataDirProp, INT, Defaults.NumRecoveryThreadsPerDataDir, atLeast(1), HIGH, NumRecoveryThreadsPerDataDirDoc)
      .define(AutoCreateTopicsEnableProp, BOOLEAN, Defaults.AutoCreateTopicsEnable, HIGH, AutoCreateTopicsEnableDoc)
      .define(MinInSyncReplicasProp, INT, Defaults.MinInSyncReplicas, atLeast(1), HIGH, MinInSyncReplicasDoc)
      .define(LogMessageFormatVersionProp, STRING, Defaults.LogMessageFormatVersion, MEDIUM, LogMessageFormatVersionDoc)
      .define(LogMessageTimestampTypeProp, STRING, Defaults.LogMessageTimestampType, in("CreateTime", "LogAppendTime"), MEDIUM, LogMessageTimestampTypeDoc)
      .define(LogMessageTimestampDifferenceMaxMsProp, LONG, Defaults.LogMessageTimestampDifferenceMaxMs, MEDIUM, LogMessageTimestampDifferenceMaxMsDoc)
      .define(CreateTopicPolicyClassNameProp, CLASS, null, LOW, CreateTopicPolicyClassNameDoc)
      .define(AlterConfigPolicyClassNameProp, CLASS, null, LOW, AlterConfigPolicyClassNameDoc)

      /** ********* Replication configuration ***********/
      .define(ControllerSocketTimeoutMsProp, INT, Defaults.ControllerSocketTimeoutMs, MEDIUM, ControllerSocketTimeoutMsDoc)
      .define(DefaultReplicationFactorProp, INT, Defaults.DefaultReplicationFactor, MEDIUM, DefaultReplicationFactorDoc)
      .define(ReplicaLagTimeMaxMsProp, LONG, Defaults.ReplicaLagTimeMaxMs, HIGH, ReplicaLagTimeMaxMsDoc)
      .define(ReplicaSocketTimeoutMsProp, INT, Defaults.ReplicaSocketTimeoutMs, HIGH, ReplicaSocketTimeoutMsDoc)
      .define(ReplicaSocketReceiveBufferBytesProp, INT, Defaults.ReplicaSocketReceiveBufferBytes, HIGH, ReplicaSocketReceiveBufferBytesDoc)
      .define(ReplicaFetchMaxBytesProp, INT, Defaults.ReplicaFetchMaxBytes, atLeast(0), MEDIUM, ReplicaFetchMaxBytesDoc)
      .define(ReplicaFetchWaitMaxMsProp, INT, Defaults.ReplicaFetchWaitMaxMs, HIGH, ReplicaFetchWaitMaxMsDoc)
      .define(ReplicaFetchBackoffMsProp, INT, Defaults.ReplicaFetchBackoffMs, atLeast(0), MEDIUM, ReplicaFetchBackoffMsDoc)
      .define(ReplicaFetchMinBytesProp, INT, Defaults.ReplicaFetchMinBytes, HIGH, ReplicaFetchMinBytesDoc)
      .define(ReplicaFetchResponseMaxBytesProp, INT, Defaults.ReplicaFetchResponseMaxBytes, atLeast(0), MEDIUM, ReplicaFetchResponseMaxBytesDoc)
      .define(NumReplicaFetchersProp, INT, Defaults.NumReplicaFetchers, HIGH, NumReplicaFetchersDoc)
      .define(ReplicaHighWatermarkCheckpointIntervalMsProp, LONG, Defaults.ReplicaHighWatermarkCheckpointIntervalMs, HIGH, ReplicaHighWatermarkCheckpointIntervalMsDoc)
      .define(FetchPurgatoryPurgeIntervalRequestsProp, INT, Defaults.FetchPurgatoryPurgeIntervalRequests, MEDIUM, FetchPurgatoryPurgeIntervalRequestsDoc)
      .define(ProducerPurgatoryPurgeIntervalRequestsProp, INT, Defaults.ProducerPurgatoryPurgeIntervalRequests, MEDIUM, ProducerPurgatoryPurgeIntervalRequestsDoc)
      .define(DeleteRecordsPurgatoryPurgeIntervalRequestsProp, INT, Defaults.DeleteRecordsPurgatoryPurgeIntervalRequests, MEDIUM, DeleteRecordsPurgatoryPurgeIntervalRequestsDoc)
      .define(AutoLeaderRebalanceEnableProp, BOOLEAN, Defaults.AutoLeaderRebalanceEnable, HIGH, AutoLeaderRebalanceEnableDoc)
      .define(LeaderImbalancePerBrokerPercentageProp, INT, Defaults.LeaderImbalancePerBrokerPercentage, HIGH, LeaderImbalancePerBrokerPercentageDoc)
      .define(LeaderImbalanceCheckIntervalSecondsProp, LONG, Defaults.LeaderImbalanceCheckIntervalSeconds, HIGH, LeaderImbalanceCheckIntervalSecondsDoc)
      .define(UncleanLeaderElectionEnableProp, BOOLEAN, Defaults.UncleanLeaderElectionEnable, HIGH, UncleanLeaderElectionEnableDoc)
      .define(InterBrokerSecurityProtocolProp, STRING, Defaults.InterBrokerSecurityProtocol, MEDIUM, InterBrokerSecurityProtocolDoc)
      .define(InterBrokerProtocolVersionProp, STRING, Defaults.InterBrokerProtocolVersion, MEDIUM, InterBrokerProtocolVersionDoc)
      .define(InterBrokerListenerNameProp, STRING, null, MEDIUM, InterBrokerListenerNameDoc)

      /** ********* Controlled shutdown configuration ***********/
      .define(ControlledShutdownMaxRetriesProp, INT, Defaults.ControlledShutdownMaxRetries, MEDIUM, ControlledShutdownMaxRetriesDoc)
      .define(ControlledShutdownRetryBackoffMsProp, LONG, Defaults.ControlledShutdownRetryBackoffMs, MEDIUM, ControlledShutdownRetryBackoffMsDoc)
      .define(ControlledShutdownEnableProp, BOOLEAN, Defaults.ControlledShutdownEnable, MEDIUM, ControlledShutdownEnableDoc)

      /** ********* Group coordinator configuration ***********/
      .define(GroupMinSessionTimeoutMsProp, INT, Defaults.GroupMinSessionTimeoutMs, MEDIUM, GroupMinSessionTimeoutMsDoc)
      .define(GroupMaxSessionTimeoutMsProp, INT, Defaults.GroupMaxSessionTimeoutMs, MEDIUM, GroupMaxSessionTimeoutMsDoc)
      .define(GroupInitialRebalanceDelayMsProp, INT, Defaults.GroupInitialRebalanceDelayMs, MEDIUM, GroupInitialRebalanceDelayMsDoc)

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

      /** ********* Transaction management configuration ***********/
      .define(TransactionalIdExpirationMsProp, INT, Defaults.TransactionalIdExpirationMs, atLeast(1), HIGH, TransactionalIdExpirationMsDoc)
      .define(TransactionsMaxTimeoutMsProp, INT, Defaults.TransactionsMaxTimeoutMs, atLeast(1), HIGH, TransactionsMaxTimeoutMsDoc)
      .define(TransactionsTopicMinISRProp, INT, Defaults.TransactionsTopicMinISR, atLeast(1), HIGH, TransactionsTopicMinISRDoc)
      .define(TransactionsLoadBufferSizeProp, INT, Defaults.TransactionsLoadBufferSize, atLeast(1), HIGH, TransactionsLoadBufferSizeDoc)
      .define(TransactionsTopicReplicationFactorProp, SHORT, Defaults.TransactionsTopicReplicationFactor, atLeast(1), HIGH, TransactionsTopicReplicationFactorDoc)
      .define(TransactionsTopicPartitionsProp, INT, Defaults.TransactionsTopicPartitions, atLeast(1), HIGH, TransactionsTopicPartitionsDoc)
      .define(TransactionsTopicSegmentBytesProp, INT, Defaults.TransactionsTopicSegmentBytes, atLeast(1), HIGH, TransactionsTopicSegmentBytesDoc)
      .define(TransactionsAbortTimedOutTransactionCleanupIntervalMsProp, INT, Defaults.TransactionsAbortTimedOutTransactionsCleanupIntervalMS, atLeast(1), LOW, TransactionsAbortTimedOutTransactionsIntervalMsDoc)
      .define(TransactionsRemoveExpiredTransactionalIdCleanupIntervalMsProp, INT, Defaults.TransactionsRemoveExpiredTransactionsCleanupIntervalMS, atLeast(1), LOW, TransactionsRemoveExpiredTransactionsIntervalMsDoc)

    /** ********* Fetch Session Configuration **************/
      .define(MaxIncrementalFetchSessionCacheSlots, INT, Defaults.MaxIncrementalFetchSessionCacheSlots, atLeast(0), MEDIUM, MaxIncrementalFetchSessionCacheSlotsDoc)

      /** ********* Kafka Metrics Configuration ***********/
      .define(MetricNumSamplesProp, INT, Defaults.MetricNumSamples, atLeast(1), LOW, MetricNumSamplesDoc)
      .define(MetricSampleWindowMsProp, LONG, Defaults.MetricSampleWindowMs, atLeast(1), LOW, MetricSampleWindowMsDoc)
      .define(MetricReporterClassesProp, LIST, Defaults.MetricReporterClasses, LOW, MetricReporterClassesDoc)
      .define(MetricRecordingLevelProp, STRING, Defaults.MetricRecordingLevel, LOW, MetricRecordingLevelDoc)

      /** ********* Quota configuration ***********/
      .define(ProducerQuotaBytesPerSecondDefaultProp, LONG, Defaults.ProducerQuotaBytesPerSecondDefault, atLeast(1), HIGH, ProducerQuotaBytesPerSecondDefaultDoc)
      .define(ConsumerQuotaBytesPerSecondDefaultProp, LONG, Defaults.ConsumerQuotaBytesPerSecondDefault, atLeast(1), HIGH, ConsumerQuotaBytesPerSecondDefaultDoc)
      .define(NumQuotaSamplesProp, INT, Defaults.NumQuotaSamples, atLeast(1), LOW, NumQuotaSamplesDoc)
      .define(NumReplicationQuotaSamplesProp, INT, Defaults.NumReplicationQuotaSamples, atLeast(1), LOW, NumReplicationQuotaSamplesDoc)
      .define(NumAlterLogDirsReplicationQuotaSamplesProp, INT, Defaults.NumAlterLogDirsReplicationQuotaSamples, atLeast(1), LOW, NumAlterLogDirsReplicationQuotaSamplesDoc)
      .define(QuotaWindowSizeSecondsProp, INT, Defaults.QuotaWindowSizeSeconds, atLeast(1), LOW, QuotaWindowSizeSecondsDoc)
      .define(ReplicationQuotaWindowSizeSecondsProp, INT, Defaults.ReplicationQuotaWindowSizeSeconds, atLeast(1), LOW, ReplicationQuotaWindowSizeSecondsDoc)
      .define(AlterLogDirsReplicationQuotaWindowSizeSecondsProp, INT, Defaults.AlterLogDirsReplicationQuotaWindowSizeSeconds, atLeast(1), LOW, AlterLogDirsReplicationQuotaWindowSizeSecondsDoc)
      .define(ClientQuotaCallbackClassProp, CLASS, null, LOW, ClientQuotaCallbackClassDoc)

      /** ********* SSL Configuration ****************/
      .define(PrincipalBuilderClassProp, CLASS, null, MEDIUM, PrincipalBuilderClassDoc)
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
      .define(SslSecureRandomImplementationProp, STRING, null, LOW, SslSecureRandomImplementationDoc)
      .define(SslClientAuthProp, STRING, Defaults.SslClientAuth, in(Defaults.SslClientAuthRequired, Defaults.SslClientAuthRequested, Defaults.SslClientAuthNone), MEDIUM, SslClientAuthDoc)
      .define(SslCipherSuitesProp, LIST, Collections.emptyList(), MEDIUM, SslCipherSuitesDoc)

      /** ********* Sasl Configuration ****************/
      .define(SaslMechanismInterBrokerProtocolProp, STRING, Defaults.SaslMechanismInterBrokerProtocol, MEDIUM, SaslMechanismInterBrokerProtocolDoc)
      .define(SaslJaasConfigProp, PASSWORD, null, MEDIUM, SaslJaasConfigDoc)
      .define(SaslEnabledMechanismsProp, LIST, Defaults.SaslEnabledMechanisms, MEDIUM, SaslEnabledMechanismsDoc)
      .define(SaslServerCallbackHandlerClassProp, CLASS, null, MEDIUM, SaslServerCallbackHandlerClassDoc)
      .define(SaslClientCallbackHandlerClassProp, CLASS, null, MEDIUM, SaslClientCallbackHandlerClassDoc)
      .define(SaslLoginClassProp, CLASS, null, MEDIUM, SaslLoginClassDoc)
      .define(SaslLoginCallbackHandlerClassProp, CLASS, null, MEDIUM, SaslLoginCallbackHandlerClassDoc)
      .define(SaslKerberosServiceNameProp, STRING, null, MEDIUM, SaslKerberosServiceNameDoc)
      .define(SaslKerberosKinitCmdProp, STRING, Defaults.SaslKerberosKinitCmd, MEDIUM, SaslKerberosKinitCmdDoc)
      .define(SaslKerberosTicketRenewWindowFactorProp, DOUBLE, Defaults.SaslKerberosTicketRenewWindowFactor, MEDIUM, SaslKerberosTicketRenewWindowFactorDoc)
      .define(SaslKerberosTicketRenewJitterProp, DOUBLE, Defaults.SaslKerberosTicketRenewJitter, MEDIUM, SaslKerberosTicketRenewJitterDoc)
      .define(SaslKerberosMinTimeBeforeReloginProp, LONG, Defaults.SaslKerberosMinTimeBeforeRelogin, MEDIUM, SaslKerberosMinTimeBeforeReloginDoc)
      .define(SaslKerberosPrincipalToLocalRulesProp, LIST, Defaults.SaslKerberosPrincipalToLocalRules, MEDIUM, SaslKerberosPrincipalToLocalRulesDoc)
      .define(SaslLoginRefreshWindowFactorProp, DOUBLE, Defaults.SaslLoginRefreshWindowFactor, MEDIUM, SaslLoginRefreshWindowFactorDoc)
      .define(SaslLoginRefreshWindowJitterProp, DOUBLE, Defaults.SaslLoginRefreshWindowJitter, MEDIUM, SaslLoginRefreshWindowJitterDoc)
      .define(SaslLoginRefreshMinPeriodSecondsProp, SHORT, Defaults.SaslLoginRefreshMinPeriodSeconds, MEDIUM, SaslLoginRefreshMinPeriodSecondsDoc)
      .define(SaslLoginRefreshBufferSecondsProp, SHORT, Defaults.SaslLoginRefreshBufferSeconds, MEDIUM, SaslLoginRefreshBufferSecondsDoc)
      /** ********* Delegation Token Configuration ****************/
      .define(DelegationTokenMasterKeyProp, PASSWORD, null, MEDIUM, DelegationTokenMasterKeyDoc)
      .define(DelegationTokenMaxLifeTimeProp, LONG, Defaults.DelegationTokenMaxLifeTimeMsDefault, atLeast(1), MEDIUM, DelegationTokenMaxLifeTimeDoc)
      .define(DelegationTokenExpiryTimeMsProp, LONG, Defaults.DelegationTokenExpiryTimeMsDefault, atLeast(1), MEDIUM, DelegationTokenExpiryTimeMsDoc)
      .define(DelegationTokenExpiryCheckIntervalMsProp, LONG, Defaults.DelegationTokenExpiryCheckIntervalMsDefault, atLeast(1), LOW, DelegationTokenExpiryCheckIntervalDoc)

      /** ********* Password encryption configuration for dynamic configs *********/
      .define(PasswordEncoderSecretProp, PASSWORD, null, MEDIUM, PasswordEncoderSecretDoc)
      .define(PasswordEncoderOldSecretProp, PASSWORD, null, MEDIUM, PasswordEncoderOldSecretDoc)
      .define(PasswordEncoderKeyFactoryAlgorithmProp, STRING, null, LOW, PasswordEncoderKeyFactoryAlgorithmDoc)
      .define(PasswordEncoderCipherAlgorithmProp, STRING, Defaults.PasswordEncoderCipherAlgorithm, LOW, PasswordEncoderCipherAlgorithmDoc)
      .define(PasswordEncoderKeyLengthProp, INT, Defaults.PasswordEncoderKeyLength, atLeast(8), LOW, PasswordEncoderKeyLengthDoc)
      .define(PasswordEncoderIterationsProp, INT, Defaults.PasswordEncoderIterations, atLeast(1024), LOW, PasswordEncoderIterationsDoc)
  }

  def configNames() = configDef.names().asScala.toList.sorted
  private[server] def defaultValues: Map[String, _] = configDef.defaultValues.asScala
  private[server] def configKeys: Map[String, ConfigKey] = configDef.configKeys.asScala

  def fromProps(props: Properties): KafkaConfig =
    fromProps(props, true)

  def fromProps(props: Properties, doLog: Boolean): KafkaConfig =
    new KafkaConfig(props, doLog)

  def fromProps(defaults: Properties, overrides: Properties): KafkaConfig =
    fromProps(defaults, overrides, true)

  def fromProps(defaults: Properties, overrides: Properties, doLog: Boolean): KafkaConfig = {
    val props = new Properties()
    props ++= defaults
    props ++= overrides
    fromProps(props, doLog)
  }

  def apply(props: java.util.Map[_, _]): KafkaConfig = new KafkaConfig(props, true)

}

class KafkaConfig(val props: java.util.Map[_, _], doLog: Boolean, dynamicConfigOverride: Option[DynamicBrokerConfig])
  extends AbstractConfig(KafkaConfig.configDef, props, doLog) {

  def this(props: java.util.Map[_, _]) = this(props, true, None)
  def this(props: java.util.Map[_, _], doLog: Boolean) = this(props, doLog, None)
  // Cache the current config to avoid acquiring read lock to access from dynamicConfig
  @volatile private var currentConfig = this
  private[server] val dynamicConfig = dynamicConfigOverride.getOrElse(new DynamicBrokerConfig(this))

  private[server] def updateCurrentConfig(newConfig: KafkaConfig): Unit = {
    this.currentConfig = newConfig
  }

  override def originals: util.Map[String, AnyRef] =
    if (this eq currentConfig) super.originals else currentConfig.originals
  override def values: util.Map[String, _] =
    if (this eq currentConfig) super.values else currentConfig.values
  override def originalsStrings: util.Map[String, String] =
    if (this eq currentConfig) super.originalsStrings else currentConfig.originalsStrings
  override def originalsWithPrefix(prefix: String): util.Map[String, AnyRef] =
    if (this eq currentConfig) super.originalsWithPrefix(prefix) else currentConfig.originalsWithPrefix(prefix)
  override def valuesWithPrefixOverride(prefix: String): util.Map[String, AnyRef] =
    if (this eq currentConfig) super.valuesWithPrefixOverride(prefix) else currentConfig.valuesWithPrefixOverride(prefix)
  override def get(key: String): AnyRef =
    if (this eq currentConfig) super.get(key) else currentConfig.get(key)

  //  During dynamic update, we use the values from this config, these are only used in DynamicBrokerConfig
  private[server] def originalsFromThisConfig: util.Map[String, AnyRef] = super.originals
  private[server] def valuesFromThisConfig: util.Map[String, _] = super.values
  private[server] def valuesFromThisConfigWithPrefixOverride(prefix: String): util.Map[String, AnyRef] =
    super.valuesWithPrefixOverride(prefix)

  /** ********* Zookeeper Configuration ***********/
  val zkConnect: String = getString(KafkaConfig.ZkConnectProp)
  val zkSessionTimeoutMs: Int = getInt(KafkaConfig.ZkSessionTimeoutMsProp)
  val zkConnectionTimeoutMs: Int =
    Option(getInt(KafkaConfig.ZkConnectionTimeoutMsProp)).map(_.toInt).getOrElse(getInt(KafkaConfig.ZkSessionTimeoutMsProp))
  val zkSyncTimeMs: Int = getInt(KafkaConfig.ZkSyncTimeMsProp)
  val zkEnableSecureAcls: Boolean = getBoolean(KafkaConfig.ZkEnableSecureAclsProp)
  val zkMaxInFlightRequests: Int = getInt(KafkaConfig.ZkMaxInFlightRequestsProp)
  /** ********* General Configuration ***********/
  val brokerIdGenerationEnable: Boolean = getBoolean(KafkaConfig.BrokerIdGenerationEnableProp)
  val maxReservedBrokerId: Int = getInt(KafkaConfig.MaxReservedBrokerIdProp)
  var brokerId: Int = getInt(KafkaConfig.BrokerIdProp)

  def numNetworkThreads = getInt(KafkaConfig.NumNetworkThreadsProp)
  def backgroundThreads = getInt(KafkaConfig.BackgroundThreadsProp)
  val queuedMaxRequests = getInt(KafkaConfig.QueuedMaxRequestsProp)
  val queuedMaxBytes = getLong(KafkaConfig.QueuedMaxBytesProp)
  def numIoThreads = getInt(KafkaConfig.NumIoThreadsProp)
  def messageMaxBytes = getInt(KafkaConfig.MessageMaxBytesProp)
  val requestTimeoutMs = getInt(KafkaConfig.RequestTimeoutMsProp)

  def getNumReplicaAlterLogDirsThreads: Int = {
    val numThreads: Integer = Option(getInt(KafkaConfig.NumReplicaAlterLogDirsThreadsProp)).getOrElse(logDirs.size)
    numThreads
  }

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

  /***************** rack configuration **************/
  val rack = Option(getString(KafkaConfig.RackProp))

  /** ********* Log Configuration ***********/
  val autoCreateTopicsEnable = getBoolean(KafkaConfig.AutoCreateTopicsEnableProp)
  val numPartitions = getInt(KafkaConfig.NumPartitionsProp)
  val logDirs = CoreUtils.parseCsvList(Option(getString(KafkaConfig.LogDirsProp)).getOrElse(getString(KafkaConfig.LogDirProp)))
  def logSegmentBytes = getInt(KafkaConfig.LogSegmentBytesProp)
  def logFlushIntervalMessages = getLong(KafkaConfig.LogFlushIntervalMessagesProp)
  val logCleanerThreads = getInt(KafkaConfig.LogCleanerThreadsProp)
  def numRecoveryThreadsPerDataDir = getInt(KafkaConfig.NumRecoveryThreadsPerDataDirProp)
  val logFlushSchedulerIntervalMs = getLong(KafkaConfig.LogFlushSchedulerIntervalMsProp)
  val logFlushOffsetCheckpointIntervalMs = getInt(KafkaConfig.LogFlushOffsetCheckpointIntervalMsProp).toLong
  val logFlushStartOffsetCheckpointIntervalMs = getInt(KafkaConfig.LogFlushStartOffsetCheckpointIntervalMsProp).toLong
  val logCleanupIntervalMs = getLong(KafkaConfig.LogCleanupIntervalMsProp)
  def logCleanupPolicy = getList(KafkaConfig.LogCleanupPolicyProp)
  val offsetsRetentionMinutes = getInt(KafkaConfig.OffsetsRetentionMinutesProp)
  val offsetsRetentionCheckIntervalMs = getLong(KafkaConfig.OffsetsRetentionCheckIntervalMsProp)
  def logRetentionBytes = getLong(KafkaConfig.LogRetentionBytesProp)
  val logCleanerDedupeBufferSize = getLong(KafkaConfig.LogCleanerDedupeBufferSizeProp)
  val logCleanerDedupeBufferLoadFactor = getDouble(KafkaConfig.LogCleanerDedupeBufferLoadFactorProp)
  val logCleanerIoBufferSize = getInt(KafkaConfig.LogCleanerIoBufferSizeProp)
  val logCleanerIoMaxBytesPerSecond = getDouble(KafkaConfig.LogCleanerIoMaxBytesPerSecondProp)
  def logCleanerDeleteRetentionMs = getLong(KafkaConfig.LogCleanerDeleteRetentionMsProp)
  def logCleanerMinCompactionLagMs = getLong(KafkaConfig.LogCleanerMinCompactionLagMsProp)
  val logCleanerBackoffMs = getLong(KafkaConfig.LogCleanerBackoffMsProp)
  def logCleanerMinCleanRatio = getDouble(KafkaConfig.LogCleanerMinCleanRatioProp)
  val logCleanerEnable = getBoolean(KafkaConfig.LogCleanerEnableProp)
  def logIndexSizeMaxBytes = getInt(KafkaConfig.LogIndexSizeMaxBytesProp)
  def logIndexIntervalBytes = getInt(KafkaConfig.LogIndexIntervalBytesProp)
  def logDeleteDelayMs = getLong(KafkaConfig.LogDeleteDelayMsProp)
  def logRollTimeMillis: java.lang.Long = Option(getLong(KafkaConfig.LogRollTimeMillisProp)).getOrElse(60 * 60 * 1000L * getInt(KafkaConfig.LogRollTimeHoursProp))
  def logRollTimeJitterMillis: java.lang.Long = Option(getLong(KafkaConfig.LogRollTimeJitterMillisProp)).getOrElse(60 * 60 * 1000L * getInt(KafkaConfig.LogRollTimeJitterHoursProp))
  def logFlushIntervalMs: java.lang.Long = Option(getLong(KafkaConfig.LogFlushIntervalMsProp)).getOrElse(getLong(KafkaConfig.LogFlushSchedulerIntervalMsProp))
  def minInSyncReplicas = getInt(KafkaConfig.MinInSyncReplicasProp)
  def logPreAllocateEnable: java.lang.Boolean = getBoolean(KafkaConfig.LogPreAllocateProp)
  // We keep the user-provided String as `ApiVersion.apply` can choose a slightly different version (eg if `0.10.0`
  // is passed, `0.10.0-IV0` may be picked)
  val logMessageFormatVersionString = getString(KafkaConfig.LogMessageFormatVersionProp)
  val logMessageFormatVersion = ApiVersion(logMessageFormatVersionString)
  def logMessageTimestampType = TimestampType.forName(getString(KafkaConfig.LogMessageTimestampTypeProp))
  def logMessageTimestampDifferenceMaxMs: Long = getLong(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp)

  /** ********* Replication configuration ***********/
  val controllerSocketTimeoutMs: Int = getInt(KafkaConfig.ControllerSocketTimeoutMsProp)
  val defaultReplicationFactor: Int = getInt(KafkaConfig.DefaultReplicationFactorProp)
  val replicaLagTimeMaxMs = getLong(KafkaConfig.ReplicaLagTimeMaxMsProp)
  val replicaSocketTimeoutMs = getInt(KafkaConfig.ReplicaSocketTimeoutMsProp)
  val replicaSocketReceiveBufferBytes = getInt(KafkaConfig.ReplicaSocketReceiveBufferBytesProp)
  val replicaFetchMaxBytes = getInt(KafkaConfig.ReplicaFetchMaxBytesProp)
  val replicaFetchWaitMaxMs = getInt(KafkaConfig.ReplicaFetchWaitMaxMsProp)
  val replicaFetchMinBytes = getInt(KafkaConfig.ReplicaFetchMinBytesProp)
  val replicaFetchResponseMaxBytes = getInt(KafkaConfig.ReplicaFetchResponseMaxBytesProp)
  val replicaFetchBackoffMs = getInt(KafkaConfig.ReplicaFetchBackoffMsProp)
  def numReplicaFetchers = getInt(KafkaConfig.NumReplicaFetchersProp)
  val replicaHighWatermarkCheckpointIntervalMs = getLong(KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp)
  val fetchPurgatoryPurgeIntervalRequests = getInt(KafkaConfig.FetchPurgatoryPurgeIntervalRequestsProp)
  val producerPurgatoryPurgeIntervalRequests = getInt(KafkaConfig.ProducerPurgatoryPurgeIntervalRequestsProp)
  val deleteRecordsPurgatoryPurgeIntervalRequests = getInt(KafkaConfig.DeleteRecordsPurgatoryPurgeIntervalRequestsProp)
  val autoLeaderRebalanceEnable = getBoolean(KafkaConfig.AutoLeaderRebalanceEnableProp)
  val leaderImbalancePerBrokerPercentage = getInt(KafkaConfig.LeaderImbalancePerBrokerPercentageProp)
  val leaderImbalanceCheckIntervalSeconds = getLong(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp)
  def uncleanLeaderElectionEnable: java.lang.Boolean = getBoolean(KafkaConfig.UncleanLeaderElectionEnableProp)

  // We keep the user-provided String as `ApiVersion.apply` can choose a slightly different version (eg if `0.10.0`
  // is passed, `0.10.0-IV0` may be picked)
  val interBrokerProtocolVersionString = getString(KafkaConfig.InterBrokerProtocolVersionProp)
  val interBrokerProtocolVersion = ApiVersion(interBrokerProtocolVersionString)

  /** ********* Controlled shutdown configuration ***********/
  val controlledShutdownMaxRetries = getInt(KafkaConfig.ControlledShutdownMaxRetriesProp)
  val controlledShutdownRetryBackoffMs = getLong(KafkaConfig.ControlledShutdownRetryBackoffMsProp)
  val controlledShutdownEnable = getBoolean(KafkaConfig.ControlledShutdownEnableProp)

  /** ********* Group coordinator configuration ***********/
  val groupMinSessionTimeoutMs = getInt(KafkaConfig.GroupMinSessionTimeoutMsProp)
  val groupMaxSessionTimeoutMs = getInt(KafkaConfig.GroupMaxSessionTimeoutMsProp)
  val groupInitialRebalanceDelay = getInt(KafkaConfig.GroupInitialRebalanceDelayMsProp)

  /** ********* Offset management configuration ***********/
  val offsetMetadataMaxSize = getInt(KafkaConfig.OffsetMetadataMaxSizeProp)
  val offsetsLoadBufferSize = getInt(KafkaConfig.OffsetsLoadBufferSizeProp)
  val offsetsTopicReplicationFactor = getShort(KafkaConfig.OffsetsTopicReplicationFactorProp)
  val offsetsTopicPartitions = getInt(KafkaConfig.OffsetsTopicPartitionsProp)
  val offsetCommitTimeoutMs = getInt(KafkaConfig.OffsetCommitTimeoutMsProp)
  val offsetCommitRequiredAcks = getShort(KafkaConfig.OffsetCommitRequiredAcksProp)
  val offsetsTopicSegmentBytes = getInt(KafkaConfig.OffsetsTopicSegmentBytesProp)
  val offsetsTopicCompressionCodec = Option(getInt(KafkaConfig.OffsetsTopicCompressionCodecProp)).map(value => CompressionCodec.getCompressionCodec(value)).orNull

  /** ********* Transaction management configuration ***********/
  val transactionalIdExpirationMs = getInt(KafkaConfig.TransactionalIdExpirationMsProp)
  val transactionMaxTimeoutMs = getInt(KafkaConfig.TransactionsMaxTimeoutMsProp)
  val transactionTopicMinISR = getInt(KafkaConfig.TransactionsTopicMinISRProp)
  val transactionsLoadBufferSize = getInt(KafkaConfig.TransactionsLoadBufferSizeProp)
  val transactionTopicReplicationFactor = getShort(KafkaConfig.TransactionsTopicReplicationFactorProp)
  val transactionTopicPartitions = getInt(KafkaConfig.TransactionsTopicPartitionsProp)
  val transactionTopicSegmentBytes = getInt(KafkaConfig.TransactionsTopicSegmentBytesProp)
  val transactionAbortTimedOutTransactionCleanupIntervalMs = getInt(KafkaConfig.TransactionsAbortTimedOutTransactionCleanupIntervalMsProp)
  val transactionRemoveExpiredTransactionalIdCleanupIntervalMs = getInt(KafkaConfig.TransactionsRemoveExpiredTransactionalIdCleanupIntervalMsProp)


  /** ********* Metric Configuration **************/
  val metricNumSamples = getInt(KafkaConfig.MetricNumSamplesProp)
  val metricSampleWindowMs = getLong(KafkaConfig.MetricSampleWindowMsProp)
  val metricRecordingLevel = getString(KafkaConfig.MetricRecordingLevelProp)

  /** ********* SSL/SASL Configuration **************/
  // Security configs may be overridden for listeners, so it is not safe to use the base values
  // Hence the base SSL/SASL configs are not fields of KafkaConfig, listener configs should be
  // retrieved using KafkaConfig#valuesWithPrefixOverride
  private def saslEnabledMechanisms(listenerName: ListenerName): Set[String] = {
    val value = valuesWithPrefixOverride(listenerName.configPrefix).get(KafkaConfig.SaslEnabledMechanismsProp)
    if (value != null)
      value.asInstanceOf[util.List[String]].asScala.toSet
    else
      Set.empty[String]
  }

  def interBrokerListenerName = getInterBrokerListenerNameAndSecurityProtocol._1
  def interBrokerSecurityProtocol = getInterBrokerListenerNameAndSecurityProtocol._2
  def saslMechanismInterBrokerProtocol = getString(KafkaConfig.SaslMechanismInterBrokerProtocolProp)
  val saslInterBrokerHandshakeRequestEnable = interBrokerProtocolVersion >= KAFKA_0_10_0_IV1

  /** ********* DelegationToken Configuration **************/
  val delegationTokenMasterKey = getPassword(KafkaConfig.DelegationTokenMasterKeyProp)
  val tokenAuthEnabled = (delegationTokenMasterKey != null && !delegationTokenMasterKey.value.isEmpty)
  val delegationTokenMaxLifeMs = getLong(KafkaConfig.DelegationTokenMaxLifeTimeProp)
  val delegationTokenExpiryTimeMs = getLong(KafkaConfig.DelegationTokenExpiryTimeMsProp)
  val delegationTokenExpiryCheckIntervalMs = getLong(KafkaConfig.DelegationTokenExpiryCheckIntervalMsProp)

  /** ********* Password encryption configuration for dynamic configs *********/
  def passwordEncoderSecret = Option(getPassword(KafkaConfig.PasswordEncoderSecretProp))
  def passwordEncoderOldSecret = Option(getPassword(KafkaConfig.PasswordEncoderOldSecretProp))
  def passwordEncoderCipherAlgorithm = getString(KafkaConfig.PasswordEncoderCipherAlgorithmProp)
  def passwordEncoderKeyFactoryAlgorithm = Option(getString(KafkaConfig.PasswordEncoderKeyFactoryAlgorithmProp))
  def passwordEncoderKeyLength = getInt(KafkaConfig.PasswordEncoderKeyLengthProp)
  def passwordEncoderIterations = getInt(KafkaConfig.PasswordEncoderIterationsProp)

  /** ********* Quota Configuration **************/
  val producerQuotaBytesPerSecondDefault = getLong(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp)
  val consumerQuotaBytesPerSecondDefault = getLong(KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp)
  val numQuotaSamples = getInt(KafkaConfig.NumQuotaSamplesProp)
  val quotaWindowSizeSeconds = getInt(KafkaConfig.QuotaWindowSizeSecondsProp)
  val numReplicationQuotaSamples = getInt(KafkaConfig.NumReplicationQuotaSamplesProp)
  val replicationQuotaWindowSizeSeconds = getInt(KafkaConfig.ReplicationQuotaWindowSizeSecondsProp)
  val numAlterLogDirsReplicationQuotaSamples = getInt(KafkaConfig.NumAlterLogDirsReplicationQuotaSamplesProp)
  val alterLogDirsReplicationQuotaWindowSizeSeconds = getInt(KafkaConfig.AlterLogDirsReplicationQuotaWindowSizeSecondsProp)

  /** ********* Transaction Configuration **************/
  val transactionIdExpirationMs = getInt(KafkaConfig.TransactionalIdExpirationMsProp)

  /** ********* Fetch Session Configuration **************/
  val maxIncrementalFetchSessionCacheSlots = getInt(KafkaConfig.MaxIncrementalFetchSessionCacheSlots)

  val deleteTopicEnable = getBoolean(KafkaConfig.DeleteTopicEnableProp)
  def compressionType = getString(KafkaConfig.CompressionTypeProp)

  def addReconfigurable(reconfigurable: Reconfigurable): Unit = {
    dynamicConfig.addReconfigurable(reconfigurable)
  }

  def logRetentionTimeMillis: Long = {
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

  // If the user did not define listeners but did define host or port, let's use them in backward compatible way
  // If none of those are defined, we default to PLAINTEXT://:9092
  def listeners: Seq[EndPoint] = {
    Option(getString(KafkaConfig.ListenersProp)).map { listenerProp =>
      CoreUtils.listenerListToEndPoints(listenerProp, listenerSecurityProtocolMap)
    }.getOrElse(CoreUtils.listenerListToEndPoints("PLAINTEXT://" + hostName + ":" + port, listenerSecurityProtocolMap))
  }

  // If the user defined advertised listeners, we use those
  // If he didn't but did define advertised host or port, we'll use those and fill in the missing value from regular host / port or defaults
  // If none of these are defined, we'll use the listeners
  def advertisedListeners: Seq[EndPoint] = {
    val advertisedListenersProp = getString(KafkaConfig.AdvertisedListenersProp)
    if (advertisedListenersProp != null)
      CoreUtils.listenerListToEndPoints(advertisedListenersProp, listenerSecurityProtocolMap)
    else if (getString(KafkaConfig.AdvertisedHostNameProp) != null || getInt(KafkaConfig.AdvertisedPortProp) != null)
      CoreUtils.listenerListToEndPoints("PLAINTEXT://" + advertisedHostName + ":" + advertisedPort, listenerSecurityProtocolMap)
    else
      listeners
  }

  private def getInterBrokerListenerNameAndSecurityProtocol: (ListenerName, SecurityProtocol) = {
    Option(getString(KafkaConfig.InterBrokerListenerNameProp)) match {
      case Some(_) if originals.containsKey(KafkaConfig.InterBrokerSecurityProtocolProp) =>
        throw new ConfigException(s"Only one of ${KafkaConfig.InterBrokerListenerNameProp} and " +
          s"${KafkaConfig.InterBrokerSecurityProtocolProp} should be set.")
      case Some(name) =>
        val listenerName = ListenerName.normalised(name)
        val securityProtocol = listenerSecurityProtocolMap.getOrElse(listenerName,
          throw new ConfigException(s"Listener with name ${listenerName.value} defined in " +
            s"${KafkaConfig.InterBrokerListenerNameProp} not found in ${KafkaConfig.ListenerSecurityProtocolMapProp}."))
        (listenerName, securityProtocol)
      case None =>
        val securityProtocol = getSecurityProtocol(getString(KafkaConfig.InterBrokerSecurityProtocolProp),
          KafkaConfig.InterBrokerSecurityProtocolProp)
        (ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
    }
  }

  private def getSecurityProtocol(protocolName: String, configName: String): SecurityProtocol = {
    try SecurityProtocol.forName(protocolName)
    catch {
      case _: IllegalArgumentException =>
        throw new ConfigException(s"Invalid security protocol `$protocolName` defined in $configName")
    }
  }

  def listenerSecurityProtocolMap: Map[ListenerName, SecurityProtocol] = {
    getMap(KafkaConfig.ListenerSecurityProtocolMapProp, getString(KafkaConfig.ListenerSecurityProtocolMapProp))
      .map { case (listenerName, protocolName) =>
      ListenerName.normalised(listenerName) -> getSecurityProtocol(protocolName, KafkaConfig.ListenerSecurityProtocolMapProp)
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
    require(logDirs.nonEmpty, "At least one log directory must be defined via log.dirs or log.dir.")
    require(logCleanerDedupeBufferSize / logCleanerThreads > 1024 * 1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.")
    require(replicaFetchWaitMaxMs <= replicaSocketTimeoutMs, "replica.socket.timeout.ms should always be at least replica.fetch.wait.max.ms" +
      " to prevent unnecessary socket timeouts")
    require(replicaFetchWaitMaxMs <= replicaLagTimeMaxMs, "replica.fetch.wait.max.ms should always be less than or equal to replica.lag.time.max.ms" +
      " to prevent frequent changes in ISR")
    require(offsetCommitRequiredAcks >= -1 && offsetCommitRequiredAcks <= offsetsTopicReplicationFactor,
      "offsets.commit.required.acks must be greater or equal -1 and less or equal to offsets.topic.replication.factor")
    require(BrokerCompressionCodec.isValid(compressionType), "compression.type : " + compressionType + " is not valid." +
      " Valid options are " + BrokerCompressionCodec.brokerCompressionOptions.mkString(","))

    val advertisedListenerNames = advertisedListeners.map(_.listenerName).toSet
    val listenerNames = listeners.map(_.listenerName).toSet
    require(advertisedListenerNames.contains(interBrokerListenerName),
      s"${KafkaConfig.InterBrokerListenerNameProp} must be a listener name defined in ${KafkaConfig.AdvertisedListenersProp}. " +
      s"The valid options based on currently configured listeners are ${advertisedListenerNames.map(_.value).mkString(",")}")
    require(advertisedListenerNames.subsetOf(listenerNames),
      s"${KafkaConfig.AdvertisedListenersProp} listener names must be equal to or a subset of the ones defined in ${KafkaConfig.ListenersProp}. " +
      s"Found ${advertisedListenerNames.map(_.value).mkString(",")}. The valid options based on the current configuration " +
      s"are ${listenerNames.map(_.value).mkString(",")}"
    )
    require(!advertisedListeners.exists(endpoint => endpoint.host=="0.0.0.0"),
      s"${KafkaConfig.AdvertisedListenersProp} cannot use the nonroutable meta-address 0.0.0.0. "+
      s"Use a routable IP address.")

    val recordVersion = logMessageFormatVersion.recordVersion
    require(interBrokerProtocolVersion.recordVersion.value >= recordVersion.value,
      s"log.message.format.version $logMessageFormatVersionString can only be used when inter.broker.protocol.version " +
      s"is set to version ${ApiVersion.minSupportedFor(recordVersion).shortVersion} or higher")

    val interBrokerUsesSasl = interBrokerSecurityProtocol == SecurityProtocol.SASL_PLAINTEXT || interBrokerSecurityProtocol == SecurityProtocol.SASL_SSL
    require(!interBrokerUsesSasl || saslInterBrokerHandshakeRequestEnable || saslMechanismInterBrokerProtocol == SaslConfigs.GSSAPI_MECHANISM,
      s"Only GSSAPI mechanism is supported for inter-broker communication with SASL when inter.broker.protocol.version is set to $interBrokerProtocolVersionString")
    require(!interBrokerUsesSasl || saslEnabledMechanisms(interBrokerListenerName).contains(saslMechanismInterBrokerProtocol),
      s"${KafkaConfig.SaslMechanismInterBrokerProtocolProp} must be included in ${KafkaConfig.SaslEnabledMechanismsProp} when SASL is used for inter-broker communication")
    require(queuedMaxBytes <= 0 || queuedMaxBytes >= socketRequestMaxBytes,
      s"${KafkaConfig.QueuedMaxBytesProp} must be larger or equal to ${KafkaConfig.SocketRequestMaxBytesProp}")

    if (maxConnectionsPerIp == 0)
      require(!maxConnectionsPerIpOverrides.isEmpty, s"${KafkaConfig.MaxConnectionsPerIpProp} can be set to zero only if" +
        s" ${KafkaConfig.MaxConnectionsPerIpOverridesProp} property is set.")
  }
}
