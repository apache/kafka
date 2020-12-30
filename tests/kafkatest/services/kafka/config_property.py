# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Define Kafka configuration property names here.
"""

BROKER_ID = "broker.id"
PORT = "port"
ADVERTISED_HOSTNAME = "advertised.host.name"

NUM_NETWORK_THREADS = "num.network.threads"
NUM_IO_THREADS = "num.io.threads"
SOCKET_SEND_BUFFER_BYTES = "socket.send.buffer.bytes"
SOCKET_RECEIVE_BUFFER_BYTES = "socket.receive.buffer.bytes"
SOCKET_REQUEST_MAX_BYTES = "socket.request.max.bytes"
LOG_DIRS = "log.dirs"
NUM_PARTITIONS = "num.partitions"
NUM_RECOVERY_THREADS_PER_DATA_DIR = "num.recovery.threads.per.data.dir"

LOG_RETENTION_HOURS = "log.retention.hours"
LOG_SEGMENT_BYTES = "log.segment.bytes"
LOG_RETENTION_CHECK_INTERVAL_MS = "log.retention.check.interval.ms"
LOG_RETENTION_MS = "log.retention.ms"
LOG_CLEANER_ENABLE = "log.cleaner.enable"

AUTO_CREATE_TOPICS_ENABLE = "auto.create.topics.enable"

ZOOKEEPER_CONNECT = "zookeeper.connect"
ZOOKEEPER_SSL_CLIENT_ENABLE = "zookeeper.ssl.client.enable"
ZOOKEEPER_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket"
ZOOKEEPER_CONNECTION_TIMEOUT_MS = "zookeeper.connection.timeout.ms"
ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms"
INTER_BROKER_PROTOCOL_VERSION = "inter.broker.protocol.version"
MESSAGE_FORMAT_VERSION = "log.message.format.version"
MESSAGE_TIMESTAMP_TYPE = "message.timestamp.type"
THROTTLING_REPLICATION_RATE_LIMIT = "replication.quota.throttled.rate"

LOG_FLUSH_INTERVAL_MESSAGE = "log.flush.interval.messages"
REPLICA_HIGHWATERMARK_CHECKPOINT_INTERVAL_MS = "replica.high.watermark.checkpoint.interval.ms"
LOG_ROLL_TIME_MS = "log.roll.ms"
OFFSETS_TOPIC_NUM_PARTITIONS = "offsets.topic.num.partitions"

DELEGATION_TOKEN_MAX_LIFETIME_MS="delegation.token.max.lifetime.ms"
DELEGATION_TOKEN_EXPIRY_TIME_MS="delegation.token.expiry.time.ms"
DELEGATION_TOKEN_SECRET_KEY="delegation.token.secret.key"
SASL_ENABLED_MECHANISMS="sasl.enabled.mechanisms"


"""
From KafkaConfig.scala

  /** ********* General Configuration ***********/
  val MaxReservedBrokerIdProp = "reserved.broker.max.id"
  val MessageMaxBytesProp = "message.max.bytes"
  val NumIoThreadsProp = "num.io.threads"
  val BackgroundThreadsProp = "background.threads"
  val QueuedMaxRequestsProp = "queued.max.requests"
  /** ********* Socket Server Configuration ***********/
  val PortProp = "port"
  val HostNameProp = "host.name"
  val ListenersProp = "listeners"
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
  /** ********* Consumer coordinator configuration ***********/
  val ConsumerMinSessionTimeoutMsProp = "consumer.min.session.timeout.ms"
  val ConsumerMaxSessionTimeoutMsProp = "consumer.max.session.timeout.ms"
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
  val PrincipalBuilderClassProp = SSLConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG
  val SSLProtocolProp = SSLConfigs.SSL_PROTOCOL_CONFIG
  val SSLProviderProp = SSLConfigs.SSL_PROVIDER_CONFIG
  val SSLCipherSuitesProp = SSLConfigs.SSL_CIPHER_SUITES_CONFIG
  val SSLEnabledProtocolsProp = SSLConfigs.SSL_ENABLED_PROTOCOLS_CONFIG
  val SSLKeystoreTypeProp = SSLConfigs.SSL_KEYSTORE_TYPE_CONFIG
  val SSLKeystoreLocationProp = SSLConfigs.SSL_KEYSTORE_LOCATION_CONFIG
  val SSLKeystorePasswordProp = SSLConfigs.SSL_KEYSTORE_PASSWORD_CONFIG
  val SSLKeyPasswordProp = SSLConfigs.SSL_KEY_PASSWORD_CONFIG
  val SSLTruststoreTypeProp = SSLConfigs.SSL_TRUSTSTORE_TYPE_CONFIG
  val SSLTruststoreLocationProp = SSLConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG
  val SSLTruststorePasswordProp = SSLConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG
  val SSLKeyManagerAlgorithmProp = SSLConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG
  val SSLTrustManagerAlgorithmProp = SSLConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG
  val SSLEndpointIdentificationAlgorithmProp = SSLConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG
  val SSLSecureRandomImplementationProp = SSLConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG
  val SSLClientAuthProp = SSLConfigs.SSL_CLIENT_AUTH_CONFIG
"""


