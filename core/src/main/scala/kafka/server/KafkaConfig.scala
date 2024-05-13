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

import java.{lang, util}
import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}
import kafka.cluster.EndPoint
import kafka.utils.CoreUtils.parseCsvList
import kafka.utils.{CoreUtils, Logging}
import kafka.utils.Implicits._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, ConfigResource, SaslConfigs, TopicConfig}
import org.apache.kafka.common.config.ConfigDef.{ConfigKey, ValidList}
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.{CompressionType, LegacyRecord, Records, TimestampType}
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.group.ConsumerGroupMigrationPolicy
import org.apache.kafka.coordinator.group.Group.GroupType
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor
import org.apache.kafka.coordinator.transaction.{TransactionLogConfigs, TransactionStateManagerConfigs}
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.security.authorizer.AuthorizerUtils
import org.apache.kafka.security.PasswordEncoderConfigs
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{MetadataVersion, MetadataVersionValidator}
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.config.{Defaults, KRaftConfigs, KafkaSecurityConfigs, QuotaConfigs, ReplicationConfigs, ServerLogConfigs, ServerTopicConfigSynonyms, ZkConfigs}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.metrics.MetricConfigs
import org.apache.kafka.server.record.BrokerCompressionType
import org.apache.kafka.server.util.Csv
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig}
import org.apache.kafka.storage.internals.log.LogConfig.MessageFormatVersion
import org.apache.zookeeper.client.ZKClientConfig

import scala.annotation.nowarn
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq}

object KafkaConfig {

  def main(args: Array[String]): Unit = {
    System.out.println(configDef.toHtml(4, (config: String) => "brokerconfigs_" + config,
      DynamicBrokerConfig.dynamicConfigUpdateModes))
  }

  private[kafka] def zooKeeperClientProperty(clientConfig: ZKClientConfig, kafkaPropName: String): Option[String] = {
    Option(clientConfig.getProperty(ZkConfigs.ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(kafkaPropName)))
  }

  private[kafka] def setZooKeeperClientProperty(clientConfig: ZKClientConfig, kafkaPropName: String, kafkaPropValue: Any): Unit = {
    clientConfig.setProperty(ZkConfigs.ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(kafkaPropName),
      kafkaPropName match {
        case ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG => (kafkaPropValue.toString.toUpperCase == "HTTPS").toString
        case ZkConfigs.ZK_SSL_ENABLED_PROTOCOLS_CONFIG | ZkConfigs.ZK_SSL_CIPHER_SUITES_CONFIG => kafkaPropValue match {
          case list: java.util.List[_] => list.asScala.mkString(",")
          case _ => kafkaPropValue.toString
        }
        case _ => kafkaPropValue.toString
    })
  }

  // For ZooKeeper TLS client authentication to be enabled the client must (at a minimum) configure itself as using TLS
  // with both a client connection socket and a key store location explicitly set.
  private[kafka] def zkTlsClientAuthEnabled(zkClientConfig: ZKClientConfig): Boolean = {
    zooKeeperClientProperty(zkClientConfig, ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG).contains("true") &&
      zooKeeperClientProperty(zkClientConfig, ZkConfigs.ZK_CLIENT_CNXN_SOCKET_CONFIG).isDefined &&
      zooKeeperClientProperty(zkClientConfig, ZkConfigs.ZK_SSL_KEY_STORE_LOCATION_CONFIG).isDefined
  }

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
  val ConnectionSetupTimeoutMsProp = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG
  val ConnectionSetupTimeoutMaxMsProp = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG
  val DeleteTopicEnableProp = "delete.topic.enable"
  val CompressionTypeProp = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.COMPRESSION_TYPE_CONFIG)

  /************* Authorizer Configuration ***********/
  val AuthorizerClassNameProp = "authorizer.class.name"
  val EarlyStartListenersProp = "early.start.listeners"

  /***************** rack configuration *************/
  val RackProp = "broker.rack"

  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetriesProp = "controlled.shutdown.max.retries"
  val ControlledShutdownRetryBackoffMsProp = "controlled.shutdown.retry.backoff.ms"
  val ControlledShutdownEnableProp = "controlled.shutdown.enable"

  /** ********* Fetch Configuration **************/
  val MaxIncrementalFetchSessionCacheSlots = "max.incremental.fetch.session.cache.slots"
  val FetchMaxBytes = "fetch.max.bytes"

  /** ********* Request Limit Configuration **************/
  val MaxRequestPartitionSizeLimit = "max.request.partition.size.limit"

  /** ********* Delegation Token Configuration ****************/
  val DelegationTokenSecretKeyAliasProp = "delegation.token.master.key"
  val DelegationTokenSecretKeyProp = "delegation.token.secret.key"
  val DelegationTokenMaxLifeTimeProp = "delegation.token.max.lifetime.ms"
  val DelegationTokenExpiryTimeMsProp = "delegation.token.expiry.time.ms"
  val DelegationTokenExpiryCheckIntervalMsProp = "delegation.token.expiry.check.interval.ms"

  /** Internal Configurations **/
  val UnstableApiVersionsEnableProp = "unstable.api.versions.enable"
  val UnstableMetadataVersionsEnableProp = "unstable.metadata.versions.enable"

  /* Documentation */
  /** ********* General Configuration ***********/
  val BrokerIdGenerationEnableDoc = s"Enable automatic broker id generation on the server. When enabled the value configured for $MaxReservedBrokerIdProp should be reviewed."
  val MaxReservedBrokerIdDoc = "Max number that can be used for a broker.id"
  val BrokerIdDoc = "The broker id for this server. If unset, a unique broker id will be generated." +
  "To avoid conflicts between ZooKeeper generated broker id's and user configured broker id's, generated broker ids " +
  "start from " + MaxReservedBrokerIdProp + " + 1."
  val MessageMaxBytesDoc = TopicConfig.MAX_MESSAGE_BYTES_DOC +
    s"This can be set per topic with the topic level <code>${TopicConfig.MAX_MESSAGE_BYTES_CONFIG}</code> config."
  val NumNetworkThreadsDoc = s"The number of threads that the server uses for receiving requests from the network and sending responses to the network. Noted: each listener (except for controller listener) creates its own thread pool."
  val NumIoThreadsDoc = "The number of threads that the server uses for processing requests, which may include disk I/O"
  val NumReplicaAlterLogDirsThreadsDoc = "The number of threads that can move replicas between log directories, which may include disk I/O"
  val BackgroundThreadsDoc = "The number of threads to use for various background processing tasks"
  val QueuedMaxRequestsDoc = "The number of queued requests allowed for data-plane, before blocking the network threads"
  val QueuedMaxRequestBytesDoc = "The number of queued bytes allowed before no more requests are read"
  val RequestTimeoutMsDoc = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC
  val ConnectionSetupTimeoutMsDoc = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC
  val ConnectionSetupTimeoutMaxMsDoc = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC
  val DeleteTopicEnableDoc = "Enables delete topic. Delete topic through the admin tool will have no effect if this config is turned off"
  val CompressionTypeDoc = "Specify the final compression type for a given topic. This configuration accepts the standard compression codecs " +
    "('gzip', 'snappy', 'lz4', 'zstd'). It additionally accepts 'uncompressed' which is equivalent to no compression; and " +
    "'producer' which means retain the original compression codec set by the producer."

  /************* Authorizer Configuration ***********/
  val AuthorizerClassNameDoc = s"The fully qualified name of a class that implements <code>${classOf[Authorizer].getName}</code>" +
    " interface, which is used by the broker for authorization."
  val EarlyStartListenersDoc = "A comma-separated list of listener names which may be started before the authorizer has finished " +
   "initialization. This is useful when the authorizer is dependent on the cluster itself for bootstrapping, as is the case for " +
   "the StandardAuthorizer (which stores ACLs in the metadata log.) By default, all listeners included in controller.listener.names " +
   "will also be early start listeners. A listener should not appear in this list if it accepts external traffic."

  /************* Rack Configuration **************/
  val RackDoc = "Rack of the broker. This will be used in rack aware replication assignment for fault tolerance. Examples: <code>RACK1</code>, <code>us-east-1d</code>"

  /** ********* Controlled shutdown configuration ***********/
  val ControlledShutdownMaxRetriesDoc = "Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens"
  val ControlledShutdownRetryBackoffMsDoc = "Before each retry, the system needs time to recover from the state that caused the previous failure (Controller fail over, replica lag etc). This config determines the amount of time to wait before retrying."
  val ControlledShutdownEnableDoc = "Enable controlled shutdown of the server."

  /** ********* Fetch Configuration **************/
  val MaxIncrementalFetchSessionCacheSlotsDoc = "The maximum number of total incremental fetch sessions that we will maintain. FetchSessionCache is sharded into 8 shards and the limit is equally divided among all shards. Sessions are allocated to each shard in round-robin. Only entries within a shard are considered eligible for eviction."
  val FetchMaxBytesDoc = "The maximum number of bytes we will return for a fetch request. Must be at least 1024."

  /** ********* Request Limit Configuration **************/
  val MaxRequestPartitionSizeLimitDoc = "The maximum number of partitions can be served in one request."


  /** ********* Delegation Token Configuration ****************/
  val DelegationTokenSecretKeyAliasDoc = s"DEPRECATED: An alias for $DelegationTokenSecretKeyProp, which should be used instead of this config."
  val DelegationTokenSecretKeyDoc = "Secret key to generate and verify delegation tokens. The same key must be configured across all the brokers. " +
    " If using Kafka with KRaft, the key must also be set across all controllers. " +
    " If the key is not set or set to empty string, brokers will disable the delegation token support."
  val DelegationTokenMaxLifeTimeDoc = "The token has a maximum lifetime beyond which it cannot be renewed anymore. Default value 7 days."
  val DelegationTokenExpiryTimeMsDoc = "The token validity time in milliseconds before the token needs to be renewed. Default value 1 day."
  val DelegationTokenExpiryCheckIntervalDoc = "Scan interval to remove expired delegation tokens."

  @nowarn("cat=deprecation")
  val configDef = {
    import ConfigDef.Importance._
    import ConfigDef.Range._
    import ConfigDef.Type._
    import ConfigDef.ValidString._

    new ConfigDef()

      /** ********* Zookeeper Configuration ***********/
      .define(ZkConfigs.ZK_CONNECT_CONFIG, STRING, null, HIGH, ZkConfigs.ZK_CONNECT_DOC)
      .define(ZkConfigs.ZK_SESSION_TIMEOUT_MS_CONFIG, INT, ZkConfigs.ZK_SESSION_TIMEOUT_MS, HIGH, ZkConfigs.ZK_SESSION_TIMEOUT_MS_DOC)
      .define(ZkConfigs.ZK_CONNECTION_TIMEOUT_MS_CONFIG, INT, null, HIGH, ZkConfigs.ZK_CONNECTION_TIMEOUT_MS_DOC)
      .define(ZkConfigs.ZK_ENABLE_SECURE_ACLS_CONFIG, BOOLEAN, ZkConfigs.ZK_ENABLE_SECURE_ACLS, HIGH, ZkConfigs.ZK_ENABLE_SECURE_ACLS_DOC)
      .define(ZkConfigs.ZK_MAX_IN_FLIGHT_REQUESTS_CONFIG, INT, ZkConfigs.ZK_MAX_IN_FLIGHT_REQUESTS, atLeast(1), HIGH, ZkConfigs.ZK_MAX_IN_FLIGHT_REQUESTS_DOC)
      .define(ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG, BOOLEAN, ZkConfigs.ZK_SSL_CLIENT_ENABLE, MEDIUM, ZkConfigs.ZK_SSL_CLIENT_ENABLE_DOC)
      .define(ZkConfigs.ZK_CLIENT_CNXN_SOCKET_CONFIG, STRING, null, MEDIUM, ZkConfigs.ZK_CLIENT_CNXN_SOCKET_DOC)
      .define(ZkConfigs.ZK_SSL_KEY_STORE_LOCATION_CONFIG, STRING, null, MEDIUM, ZkConfigs.ZK_SSL_KEY_STORE_LOCATION_DOC)
      .define(ZkConfigs.ZK_SSL_KEY_STORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, ZkConfigs.ZK_SSL_KEY_STORE_PASSWORD_DOC)
      .define(ZkConfigs.ZK_SSL_KEY_STORE_TYPE_CONFIG, STRING, null, MEDIUM, ZkConfigs.ZK_SSL_KEY_STORE_TYPE_DOC)
      .define(ZkConfigs.ZK_SSL_TRUST_STORE_LOCATION_CONFIG, STRING, null, MEDIUM, ZkConfigs.ZK_SSL_TRUST_STORE_LOCATION_DOC)
      .define(ZkConfigs.ZK_SSL_TRUST_STORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, ZkConfigs.ZK_SSL_TRUST_STORE_PASSWORD_DOC)
      .define(ZkConfigs.ZK_SSL_TRUST_STORE_TYPE_CONFIG, STRING, null, MEDIUM, ZkConfigs.ZK_SSL_TRUST_STORE_TYPE_DOC)
      .define(ZkConfigs.ZK_SSL_PROTOCOL_CONFIG, STRING, ZkConfigs.ZK_SSL_PROTOCOL, LOW, ZkConfigs.ZK_SSL_PROTOCOL_DOC)
      .define(ZkConfigs.ZK_SSL_ENABLED_PROTOCOLS_CONFIG, LIST, null, LOW, ZkConfigs.ZK_SSL_ENABLED_PROTOCOLS_DOC)
      .define(ZkConfigs.ZK_SSL_CIPHER_SUITES_CONFIG, LIST, null, LOW, ZkConfigs.ZK_SSL_CIPHER_SUITES_DOC)
      .define(ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, STRING, ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, LOW, ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
      .define(ZkConfigs.ZK_SSL_CRL_ENABLE_CONFIG, BOOLEAN, ZkConfigs.ZK_SSL_CRL_ENABLE, LOW, ZkConfigs.ZK_SSL_CRL_ENABLE_DOC)
      .define(ZkConfigs.ZK_SSL_OCSP_ENABLE_CONFIG, BOOLEAN, ZkConfigs.ZK_SSL_OCSP_ENABLE, LOW, ZkConfigs.ZK_SSL_OCSP_ENABLE_DOC)

      /** ********* General Configuration ***********/
      .define(BrokerIdGenerationEnableProp, BOOLEAN, Defaults.BROKER_ID_GENERATION_ENABLE, MEDIUM, BrokerIdGenerationEnableDoc)
      .define(MaxReservedBrokerIdProp, INT, Defaults.MAX_RESERVED_BROKER_ID, atLeast(0), MEDIUM, MaxReservedBrokerIdDoc)
      .define(BrokerIdProp, INT, Defaults.BROKER_ID, HIGH, BrokerIdDoc)
      .define(MessageMaxBytesProp, INT, LogConfig.DEFAULT_MAX_MESSAGE_BYTES, atLeast(0), HIGH, MessageMaxBytesDoc)
      .define(NumNetworkThreadsProp, INT, Defaults.NUM_NETWORK_THREADS, atLeast(1), HIGH, NumNetworkThreadsDoc)
      .define(NumIoThreadsProp, INT, Defaults.NUM_IO_THREADS, atLeast(1), HIGH, NumIoThreadsDoc)
      .define(NumReplicaAlterLogDirsThreadsProp, INT, null, HIGH, NumReplicaAlterLogDirsThreadsDoc)
      .define(BackgroundThreadsProp, INT, Defaults.BACKGROUND_THREADS, atLeast(1), HIGH, BackgroundThreadsDoc)
      .define(QueuedMaxRequestsProp, INT, Defaults.QUEUED_MAX_REQUESTS, atLeast(1), HIGH, QueuedMaxRequestsDoc)
      .define(QueuedMaxBytesProp, LONG, Defaults.QUEUED_MAX_REQUEST_BYTES, MEDIUM, QueuedMaxRequestBytesDoc)
      .define(RequestTimeoutMsProp, INT, Defaults.REQUEST_TIMEOUT_MS, HIGH, RequestTimeoutMsDoc)
      .define(ConnectionSetupTimeoutMsProp, LONG, Defaults.CONNECTION_SETUP_TIMEOUT_MS, MEDIUM, ConnectionSetupTimeoutMsDoc)
      .define(ConnectionSetupTimeoutMaxMsProp, LONG, Defaults.CONNECTION_SETUP_TIMEOUT_MAX_MS, MEDIUM, ConnectionSetupTimeoutMaxMsDoc)

      /*
       * KRaft mode configs.
       */
      .define(KRaftConfigs.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG, LONG, KRaftConfigs.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES, atLeast(1), HIGH, KRaftConfigs.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_DOC)
      .define(KRaftConfigs.METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG, LONG, KRaftConfigs.METADATA_SNAPSHOT_MAX_INTERVAL_MS_DEFAULT, atLeast(0), HIGH, KRaftConfigs.METADATA_SNAPSHOT_MAX_INTERVAL_MS_DOC)
      .define(KRaftConfigs.PROCESS_ROLES_CONFIG, LIST, Collections.emptyList(), ValidList.in("broker", "controller"), HIGH, KRaftConfigs.PROCESS_ROLES_DOC)
      .define(KRaftConfigs.NODE_ID_CONFIG, INT, KRaftConfigs.EMPTY_NODE_ID, null, HIGH, KRaftConfigs.NODE_ID_DOC)
      .define(KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG, INT, KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DEFAULT, null, MEDIUM, KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_DOC)
      .define(KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG, INT, KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_DEFAULT, null, MEDIUM, KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_DOC)
      .define(KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG, INT, KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_DEFAULT, null, MEDIUM, KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_DOC)
      .define(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, STRING, null, null, HIGH, KRaftConfigs.CONTROLLER_LISTENER_NAMES_DOC)
      .define(KRaftConfigs.SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG, STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, null, HIGH, KRaftConfigs.SASL_MECHANISM_CONTROLLER_PROTOCOL_DOC)
      .define(KRaftConfigs.METADATA_LOG_DIR_CONFIG, STRING, null, null, HIGH, KRaftConfigs.METADATA_LOG_DIR_DOC)
      .define(KRaftConfigs.METADATA_LOG_SEGMENT_BYTES_CONFIG, INT, LogConfig.DEFAULT_SEGMENT_BYTES, atLeast(Records.LOG_OVERHEAD), HIGH, KRaftConfigs.METADATA_LOG_SEGMENT_BYTES_DOC)
      .defineInternal(KRaftConfigs.METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG, INT, 8 * 1024 * 1024, atLeast(Records.LOG_OVERHEAD), HIGH, KRaftConfigs.METADATA_LOG_SEGMENT_MIN_BYTES_DOC)
      .define(KRaftConfigs.METADATA_LOG_SEGMENT_MILLIS_CONFIG, LONG, LogConfig.DEFAULT_SEGMENT_MS, null, HIGH, KRaftConfigs.METADATA_LOG_SEGMENT_MILLIS_DOC)
      .define(KRaftConfigs.METADATA_MAX_RETENTION_BYTES_CONFIG, LONG, KRaftConfigs.METADATA_MAX_RETENTION_BYTES_DEFAULT, null, HIGH, KRaftConfigs.METADATA_MAX_RETENTION_BYTES_DOC)
      .define(KRaftConfigs.METADATA_MAX_RETENTION_MILLIS_CONFIG, LONG, LogConfig.DEFAULT_RETENTION_MS, null, HIGH, KRaftConfigs.METADATA_MAX_RETENTION_MILLIS_DOC)
      .define(KRaftConfigs.METADATA_MAX_IDLE_INTERVAL_MS_CONFIG, INT, KRaftConfigs.METADATA_MAX_IDLE_INTERVAL_MS_DEFAULT, atLeast(0), LOW, KRaftConfigs.METADATA_MAX_IDLE_INTERVAL_MS_DOC)
      .defineInternal(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG, LONG, KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_DEFAULT, atLeast(0), MEDIUM, KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_DOC)
      .define(KRaftConfigs.MIGRATION_ENABLED_CONFIG, BOOLEAN, false, HIGH, KRaftConfigs.MIGRATION_ENABLED_DOC)
      .define(KRaftConfigs.ELR_ENABLED_CONFIG, BOOLEAN, false, HIGH, KRaftConfigs.ELR_ENABLED_DOC)
      .defineInternal(KRaftConfigs.MIGRATION_METADATA_MIN_BATCH_SIZE_CONFIG, INT, KRaftConfigs.MIGRATION_METADATA_MIN_BATCH_SIZE_DEFAULT, atLeast(1),
        MEDIUM, KRaftConfigs.MIGRATION_METADATA_MIN_BATCH_SIZE_DOC)

      /************* Authorizer Configuration ***********/
      .define(AuthorizerClassNameProp, STRING, Defaults.AUTHORIZER_CLASS_NAME, new ConfigDef.NonNullValidator(), LOW, AuthorizerClassNameDoc)
      .define(EarlyStartListenersProp, STRING, null,  HIGH, EarlyStartListenersDoc)

      /** ********* Socket Server Configuration ***********/
      .define(SocketServerConfigs.LISTENERS_CONFIG, STRING, SocketServerConfigs.LISTENERS_DEFAULT, HIGH, SocketServerConfigs.LISTENERS_DOC)
      .define(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, STRING, null, HIGH, SocketServerConfigs.ADVERTISED_LISTENERS_DOC)
      .define(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, STRING, SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT, LOW, SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_DOC)
      .define(SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG, STRING, null, HIGH, SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_DOC)
      .define(SocketServerConfigs.SOCKET_SEND_BUFFER_BYTES_CONFIG, INT, SocketServerConfigs.SOCKET_SEND_BUFFER_BYTES_DEFAULT, HIGH, SocketServerConfigs.SOCKET_SEND_BUFFER_BYTES_DOC)
      .define(SocketServerConfigs.SOCKET_RECEIVE_BUFFER_BYTES_CONFIG, INT, SocketServerConfigs.SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT, HIGH, SocketServerConfigs.SOCKET_RECEIVE_BUFFER_BYTES_DOC)
      .define(SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_CONFIG, INT, SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_DEFAULT, atLeast(1), HIGH, SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_DOC)
      .define(SocketServerConfigs.SOCKET_LISTEN_BACKLOG_SIZE_CONFIG, INT, SocketServerConfigs.SOCKET_LISTEN_BACKLOG_SIZE_DEFAULT, atLeast(1), MEDIUM, SocketServerConfigs.SOCKET_LISTEN_BACKLOG_SIZE_DOC)
      .define(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG, INT, SocketServerConfigs.MAX_CONNECTIONS_PER_IP_DEFAULT, atLeast(0), MEDIUM, SocketServerConfigs.MAX_CONNECTIONS_PER_IP_DOC)
      .define(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG, STRING, SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_DEFAULT, MEDIUM, SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_DOC)
      .define(SocketServerConfigs.MAX_CONNECTIONS_CONFIG, INT, SocketServerConfigs.MAX_CONNECTIONS_DEFAULT, atLeast(0), MEDIUM, SocketServerConfigs.MAX_CONNECTIONS_DOC)
      .define(SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG, INT, SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_DEFAULT, atLeast(0), MEDIUM, SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_DOC)
      .define(SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, LONG, SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_DEFAULT, MEDIUM, SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
      .define(SocketServerConfigs.FAILED_AUTHENTICATION_DELAY_MS_CONFIG, INT, SocketServerConfigs.FAILED_AUTHENTICATION_DELAY_MS_DEFAULT, atLeast(0), LOW, SocketServerConfigs.FAILED_AUTHENTICATION_DELAY_MS_DOC)

      /************ Rack Configuration ******************/
      .define(RackProp, STRING, null, MEDIUM, RackDoc)

      /** ********* Log Configuration ***********/
      .define(ServerLogConfigs.NUM_PARTITIONS_CONFIG, INT, ServerLogConfigs.NUM_PARTITIONS_DEFAULT, atLeast(1), MEDIUM, ServerLogConfigs.NUM_PARTITIONS_DOC)
      .define(ServerLogConfigs.LOG_DIR_CONFIG, STRING, ServerLogConfigs.LOG_DIR_DEFAULT, HIGH, ServerLogConfigs.LOG_DIR_DOC)
      .define(ServerLogConfigs.LOG_DIRS_CONFIG, STRING, null, HIGH, ServerLogConfigs.LOG_DIRS_DOC)
      .define(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG, INT, LogConfig.DEFAULT_SEGMENT_BYTES, atLeast(LegacyRecord.RECORD_OVERHEAD_V0), HIGH, ServerLogConfigs.LOG_SEGMENT_BYTES_DOC)

      .define(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, LONG, null, HIGH, ServerLogConfigs.LOG_ROLL_TIME_MILLIS_DOC)
      .define(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, INT, TimeUnit.MILLISECONDS.toHours(LogConfig.DEFAULT_SEGMENT_MS).toInt, atLeast(1), HIGH, ServerLogConfigs.LOG_ROLL_TIME_HOURS_DOC)

      .define(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG, LONG, null, HIGH, ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_DOC)
      .define(ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG, INT, TimeUnit.MILLISECONDS.toHours(LogConfig.DEFAULT_SEGMENT_JITTER_MS).toInt, atLeast(0), HIGH, ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_DOC)

      .define(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, LONG, null, HIGH, ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_DOC)
      .define(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, INT, null, HIGH, ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_DOC)
      .define(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, INT, TimeUnit.MILLISECONDS.toHours(LogConfig.DEFAULT_RETENTION_MS).toInt, HIGH, ServerLogConfigs.LOG_RETENTION_TIME_HOURS_DOC)

      .define(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, LONG, ServerLogConfigs.LOG_RETENTION_BYTES_DEFAULT, HIGH, ServerLogConfigs.LOG_RETENTION_BYTES_DOC)
      .define(ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, LONG, ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_DOC)
      .define(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG, LIST, ServerLogConfigs.LOG_CLEANUP_POLICY_DEFAULT, ValidList.in(TopicConfig.CLEANUP_POLICY_COMPACT, TopicConfig.CLEANUP_POLICY_DELETE), MEDIUM, ServerLogConfigs.LOG_CLEANUP_POLICY_DOC)
      .define(CleanerConfig.LOG_CLEANER_THREADS_PROP, INT, CleanerConfig.LOG_CLEANER_THREADS, atLeast(0), MEDIUM, CleanerConfig.LOG_CLEANER_THREADS_DOC)
      .define(CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP, DOUBLE, CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND, MEDIUM, CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_DOC)
      .define(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, LONG, CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE, MEDIUM, CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_DOC)
      .define(CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP, INT, CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE, atLeast(0), MEDIUM, CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_DOC)
      .define(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP, DOUBLE, CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR, MEDIUM, CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_DOC)
      .define(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP, LONG, CleanerConfig.LOG_CLEANER_BACKOFF_MS, atLeast(0), MEDIUM, CleanerConfig.LOG_CLEANER_BACKOFF_MS_DOC)
      .define(CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP, DOUBLE, LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO, between(0, 1), MEDIUM, CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_DOC)
      .define(CleanerConfig.LOG_CLEANER_ENABLE_PROP, BOOLEAN, CleanerConfig.LOG_CLEANER_ENABLE, MEDIUM, CleanerConfig.LOG_CLEANER_ENABLE_DOC)
      .define(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP, LONG, LogConfig.DEFAULT_DELETE_RETENTION_MS, atLeast(0), MEDIUM, CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_DOC)
      .define(CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP, LONG, LogConfig.DEFAULT_MIN_COMPACTION_LAG_MS, atLeast(0), MEDIUM, CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_DOC)
      .define(CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP, LONG, LogConfig.DEFAULT_MAX_COMPACTION_LAG_MS, atLeast(1), MEDIUM, CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_DOC)
      .define(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG, INT, ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_DEFAULT, atLeast(4), MEDIUM, ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_DOC)
      .define(ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_CONFIG, INT, ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_DOC)
      .define(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG, LONG, ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_DEFAULT, atLeast(1), HIGH, ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_DOC)
      .define(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, LONG, ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT, atLeast(0), HIGH, ServerLogConfigs.LOG_DELETE_DELAY_MS_DOC)
      .define(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG, LONG, ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_DEFAULT, HIGH, ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_DOC)
      .define(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG, LONG, null, HIGH, ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_DOC)
      .define(ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG, INT, ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_DEFAULT, atLeast(0), HIGH, ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_DOC)
      .define(ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG, INT, ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_DEFAULT, atLeast(0), HIGH, ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_DOC)
      .define(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG, BOOLEAN, LogConfig.DEFAULT_PREALLOCATE, MEDIUM, ServerLogConfigs.LOG_PRE_ALLOCATE_ENABLE_DOC)
      .define(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG, INT, ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_DEFAULT, atLeast(1), HIGH, ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_DOC)
      .define(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, BOOLEAN, ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_DEFAULT, HIGH, ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_DOC)
      .define(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, INT, ServerLogConfigs.MIN_IN_SYNC_REPLICAS_DEFAULT, atLeast(1), HIGH, ServerLogConfigs.MIN_IN_SYNC_REPLICAS_DOC)
      .define(ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_CONFIG, STRING, ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_DEFAULT, new MetadataVersionValidator(), MEDIUM, ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_DOC)
      .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG, STRING, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_DEFAULT, ConfigDef.ValidString.in("CreateTime", "LogAppendTime"), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_DOC)
      .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC)
      .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC)
      .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC)
      .define(ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG, CLASS, null, LOW, ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_DOC)
      .define(ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG, CLASS, null, LOW, ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_DOC)
      .define(ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, BOOLEAN, ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_DEFAULT, LOW, ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_DOC)
      .defineInternal(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, LONG, ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DEFAULT, atLeast(0), LOW, ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DOC)

      /** ********* Replication configuration ***********/
      .define(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG, INT, ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_DEFAULT, MEDIUM, ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_DOC)
      .define(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, INT, ReplicationConfigs.REPLICATION_FACTOR_DEFAULT, MEDIUM, ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_DOC)
      .define(ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_CONFIG, LONG, ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DEFAULT, HIGH, ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_DOC)
      .define(ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG, INT, ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_DEFAULT, HIGH, ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_DOC)
      .define(ReplicationConfigs.REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_CONFIG, INT, ReplicationConfigs.REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT, HIGH, ReplicationConfigs.REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_DOC)
      .define(ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_CONFIG, INT, ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_DEFAULT, atLeast(0), MEDIUM, ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_DOC)
      .define(ReplicationConfigs.REPLICA_FETCH_WAIT_MAX_MS_CONFIG, INT, ReplicationConfigs.REPLICA_FETCH_WAIT_MAX_MS_DEFAULT, HIGH, ReplicationConfigs.REPLICA_FETCH_WAIT_MAX_MS_DOC)
      .define(ReplicationConfigs.REPLICA_FETCH_BACKOFF_MS_CONFIG, INT, ReplicationConfigs.REPLICA_FETCH_BACKOFF_MS_DEFAULT, atLeast(0), MEDIUM, ReplicationConfigs.REPLICA_FETCH_BACKOFF_MS_DOC)
      .define(ReplicationConfigs.REPLICA_FETCH_MIN_BYTES_CONFIG, INT, ReplicationConfigs.REPLICA_FETCH_MIN_BYTES_DEFAULT, HIGH, ReplicationConfigs.REPLICA_FETCH_MIN_BYTES_DOC)
      .define(ReplicationConfigs.REPLICA_FETCH_RESPONSE_MAX_BYTES_CONFIG, INT, ReplicationConfigs.REPLICA_FETCH_RESPONSE_MAX_BYTES_DEFAULT, atLeast(0), MEDIUM, ReplicationConfigs.REPLICA_FETCH_RESPONSE_MAX_BYTES_DOC)
      .define(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG, INT, ReplicationConfigs.NUM_REPLICA_FETCHERS_DEFAULT, HIGH, ReplicationConfigs.NUM_REPLICA_FETCHERS_DOC)
      .define(ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_CONFIG, LONG, ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_DEFAULT, HIGH, ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_DOC)
      .define(ReplicationConfigs.FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG, INT, ReplicationConfigs.FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT, MEDIUM, ReplicationConfigs.FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC)
      .define(ReplicationConfigs.PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG, INT, ReplicationConfigs.PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT, MEDIUM, ReplicationConfigs.PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC)
      .define(ReplicationConfigs.DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG, INT, ReplicationConfigs.DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT, MEDIUM, ReplicationConfigs.DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC)
      .define(ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, BOOLEAN, ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_DEFAULT, HIGH, ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_DOC)
      .define(ReplicationConfigs.LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_CONFIG, INT, ReplicationConfigs.LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_DEFAULT, HIGH, ReplicationConfigs.LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_DOC)
      .define(ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_CONFIG, LONG, ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_DEFAULT, atLeast(1), HIGH, ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_DOC)
      .define(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, BOOLEAN, LogConfig.DEFAULT_UNCLEAN_LEADER_ELECTION_ENABLE, HIGH, ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_DOC)
      .define(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG, STRING, ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_DEFAULT, in(Utils.enumOptions(classOf[SecurityProtocol]):_*), MEDIUM, ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_DOC)
      .define(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, STRING, ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_DEFAULT, new MetadataVersionValidator(), MEDIUM, ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_DOC)
      .define(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, STRING, null, MEDIUM, ReplicationConfigs.INTER_BROKER_LISTENER_NAME_DOC)
      .define(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG, STRING, null, MEDIUM, ReplicationConfigs.REPLICA_SELECTOR_CLASS_DOC)


      /** ********* Controlled shutdown configuration ***********/
      .define(ControlledShutdownMaxRetriesProp, INT, Defaults.CONTROLLED_SHUTDOWN_MAX_RETRIES, MEDIUM, ControlledShutdownMaxRetriesDoc)
      .define(ControlledShutdownRetryBackoffMsProp, LONG, Defaults.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS, MEDIUM, ControlledShutdownRetryBackoffMsDoc)
      .define(ControlledShutdownEnableProp, BOOLEAN, Defaults.CONTROLLED_SHUTDOWN_ENABLE, MEDIUM, ControlledShutdownEnableDoc)

      /** ********* Group coordinator configuration ***********/
      .define(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT, MEDIUM, GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_DOC)
      .define(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT, MEDIUM, GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_DOC)
      .define(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, INT, GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT, MEDIUM, GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_DOC)
      .define(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, INT, GroupCoordinatorConfig.GROUP_MAX_SIZE_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.GROUP_MAX_SIZE_DOC)

      /** New group coordinator configs */
      .define(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, LIST, GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DEFAULT,
        ConfigDef.ValidList.in(Utils.enumOptions(classOf[GroupType]):_*), MEDIUM, GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DOC)
      .define(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG, INT, GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_DOC)
      // Internal configuration used by integration and system tests.
      .defineInternal(GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, BOOLEAN, GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_DEFAULT, null, MEDIUM, GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_DOC)

      /** Consumer groups configs */
      .define(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC)
      .define(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DOC)
      .define(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DOC)
      .define(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC)
      .define(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DOC)
      .define(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DOC)
      .define(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_DOC)
      .define(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, LIST, GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_DEFAULT, null, MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_DOC)
      .defineInternal(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, STRING, GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_DEFAULT, ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(classOf[ConsumerGroupMigrationPolicy]): _*), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_DOC)

      /** ********* Offset management configuration ***********/
      .define(GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_CONFIG, INT, GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_DEFAULT, HIGH, GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_DOC)
      .define(GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_CONFIG, INT, GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_DOC)
      .define(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_DOC)
      .define(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, INT, GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_DOC)
      .define(GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG, INT, GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_DOC)
      .define(GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG, INT, GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_DEFAULT.id.toInt, HIGH, GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_DOC)
      .define(GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG, INT, GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_DOC)
      .define(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, LONG, GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_DOC)
      .define(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_DEFAULT, atLeast(1), HIGH, GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_DOC)
      .define(GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_CONFIG, SHORT, GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_DEFAULT, HIGH, GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_DOC)
      .define(DeleteTopicEnableProp, BOOLEAN, Defaults.DELETE_TOPIC_ENABLE, HIGH, DeleteTopicEnableDoc)
      .define(CompressionTypeProp, STRING, LogConfig.DEFAULT_COMPRESSION_TYPE, in(BrokerCompressionType.names.asScala.toSeq:_*), HIGH, CompressionTypeDoc)

      /** ********* Transaction management configuration ***********/
      .define(TransactionStateManagerConfigs.TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG, INT, TransactionStateManagerConfigs.TRANSACTIONAL_ID_EXPIRATION_MS_DEFAULT, atLeast(1), HIGH, TransactionStateManagerConfigs.TRANSACTIONAL_ID_EXPIRATION_MS_DOC)
      .define(TransactionStateManagerConfigs.TRANSACTIONS_MAX_TIMEOUT_MS_CONFIG, INT, TransactionStateManagerConfigs.TRANSACTIONS_MAX_TIMEOUT_MS_DEFAULT, atLeast(1), HIGH, TransactionStateManagerConfigs.TRANSACTIONS_MAX_TIMEOUT_MS_DOC)
      .define(TransactionLogConfigs.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, INT, TransactionLogConfigs.TRANSACTIONS_TOPIC_MIN_ISR_DEFAULT, atLeast(1), HIGH, TransactionLogConfigs.TRANSACTIONS_TOPIC_MIN_ISR_DOC)
      .define(TransactionLogConfigs.TRANSACTIONS_LOAD_BUFFER_SIZE_CONFIG, INT, TransactionLogConfigs.TRANSACTIONS_LOAD_BUFFER_SIZE_DEFAULT, atLeast(1), HIGH, TransactionLogConfigs.TRANSACTIONS_LOAD_BUFFER_SIZE_DOC)
      .define(TransactionLogConfigs.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, TransactionLogConfigs.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, TransactionLogConfigs.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_DOC)
      .define(TransactionLogConfigs.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, INT, TransactionLogConfigs.TRANSACTIONS_TOPIC_PARTITIONS_DEFAULT, atLeast(1), HIGH, TransactionLogConfigs.TRANSACTIONS_TOPIC_PARTITIONS_DOC)
      .define(TransactionLogConfigs.TRANSACTIONS_TOPIC_SEGMENT_BYTES_CONFIG, INT, TransactionLogConfigs.TRANSACTIONS_TOPIC_SEGMENT_BYTES_DEFAULT, atLeast(1), HIGH, TransactionLogConfigs.TRANSACTIONS_TOPIC_SEGMENT_BYTES_DOC)
      .define(TransactionStateManagerConfigs.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, INT, TransactionStateManagerConfigs.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_DEFAULT, atLeast(1), LOW, TransactionStateManagerConfigs.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTIONS_INTERVAL_MS_DOC)
      .define(TransactionStateManagerConfigs.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG, INT, TransactionStateManagerConfigs.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_DEFAULT, atLeast(1), LOW, TransactionStateManagerConfigs.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONS_INTERVAL_MS_DOC)

      .define(TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG, BOOLEAN, TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_DEFAULT, LOW, TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_DOC)

      .define(TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_CONFIG, INT, TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_DEFAULT, atLeast(1), LOW, TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_DOC)
      // Configuration for testing only as default value should be sufficient for typical usage
      .defineInternal(TransactionLogConfigs.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_CONFIG, INT, TransactionLogConfigs.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT, atLeast(1), LOW, TransactionLogConfigs.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DOC)

      /** ********* Fetch Configuration **************/
      .define(MaxIncrementalFetchSessionCacheSlots, INT, Defaults.MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS, atLeast(0), MEDIUM, MaxIncrementalFetchSessionCacheSlotsDoc)
      .define(FetchMaxBytes, INT, Defaults.FETCH_MAX_BYTES, atLeast(1024), MEDIUM, FetchMaxBytesDoc)

      /** ********* Request Limit Configuration ***********/
      .define(MaxRequestPartitionSizeLimit, INT, Defaults.MAX_REQUEST_PARTITION_SIZE_LIMIT, atLeast(1), MEDIUM, MaxRequestPartitionSizeLimitDoc)

      /** ********* Kafka Metrics Configuration ***********/
      .define(MetricConfigs.METRIC_NUM_SAMPLES_CONFIG, INT, MetricConfigs.METRIC_NUM_SAMPLES_DEFAULT, atLeast(1), LOW, MetricConfigs.METRIC_NUM_SAMPLES_DOC)
      .define(MetricConfigs.METRIC_SAMPLE_WINDOW_MS_CONFIG, LONG, MetricConfigs.METRIC_SAMPLE_WINDOW_MS_DEFAULT, atLeast(1), LOW, MetricConfigs.METRIC_SAMPLE_WINDOW_MS_DOC)
      .define(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, LIST, MetricConfigs.METRIC_REPORTER_CLASSES_DEFAULT, LOW, MetricConfigs.METRIC_REPORTER_CLASSES_DOC)
      .define(MetricConfigs.METRIC_RECORDING_LEVEL_CONFIG, STRING, MetricConfigs.METRIC_RECORDING_LEVEL_DEFAULT, LOW, MetricConfigs.METRIC_RECORDING_LEVEL_DOC)
      .define(MetricConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG, BOOLEAN, MetricConfigs.AUTO_INCLUDE_JMX_REPORTER_DEFAULT, LOW, MetricConfigs.AUTO_INCLUDE_JMX_REPORTER_DOC)

      /** ********* Kafka Yammer Metrics Reporter Configuration for docs ***********/
      .define(MetricConfigs.KAFKA_METRICS_REPORTER_CLASSES_CONFIG, LIST, MetricConfigs.KAFKA_METRIC_REPORTER_CLASSES_DEFAULT, LOW, MetricConfigs.KAFKA_METRICS_REPORTER_CLASSES_DOC)
      .define(MetricConfigs.KAFKA_METRICS_POLLING_INTERVAL_SECONDS_CONFIG, INT, MetricConfigs.KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DEFAULT, atLeast(1), LOW, MetricConfigs.KAFKA_METRICS_POLLING_INTERVAL_SECONDS_DOC)

      /** ********* Kafka Client Telemetry Metrics Configuration ***********/
      .define(MetricConfigs.CLIENT_TELEMETRY_MAX_BYTES_CONFIG, INT, MetricConfigs.CLIENT_TELEMETRY_MAX_BYTES_DEFAULT, atLeast(1), LOW, MetricConfigs.CLIENT_TELEMETRY_MAX_BYTES_DOC)

      /** ********* Quota configuration ***********/
      .define(QuotaConfigs.NUM_QUOTA_SAMPLES_CONFIG, INT, QuotaConfigs.NUM_QUOTA_SAMPLES_DEFAULT, atLeast(1), LOW, QuotaConfigs.NUM_QUOTA_SAMPLES_DOC)
      .define(QuotaConfigs.NUM_REPLICATION_QUOTA_SAMPLES_CONFIG, INT, QuotaConfigs.NUM_QUOTA_SAMPLES_DEFAULT, atLeast(1), LOW, QuotaConfigs.NUM_REPLICATION_QUOTA_SAMPLES_DOC)
      .define(QuotaConfigs.NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_CONFIG, INT, QuotaConfigs.NUM_QUOTA_SAMPLES_DEFAULT, atLeast(1), LOW, QuotaConfigs.NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_DOC)
      .define(QuotaConfigs.NUM_CONTROLLER_QUOTA_SAMPLES_CONFIG, INT, QuotaConfigs.NUM_QUOTA_SAMPLES_DEFAULT, atLeast(1), LOW, QuotaConfigs.NUM_CONTROLLER_QUOTA_SAMPLES_DOC)
      .define(QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_CONFIG, INT, QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT, atLeast(1), LOW, QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_DOC)
      .define(QuotaConfigs.REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG, INT, QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT, atLeast(1), LOW, QuotaConfigs.REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC)
      .define(QuotaConfigs.ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG, INT, QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT, atLeast(1), LOW, QuotaConfigs.ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC)
      .define(QuotaConfigs.CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_CONFIG, INT, QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT, atLeast(1), LOW, QuotaConfigs.CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_DOC)
      .define(QuotaConfigs.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, CLASS, null, LOW, QuotaConfigs.CLIENT_QUOTA_CALLBACK_CLASS_DOC)

      /** ********* General Security Configuration ****************/
      .define(KafkaSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG, LONG, KafkaSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DEFAULT, MEDIUM, KafkaSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DOC)
      .define(KafkaSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG, INT, KafkaSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DOC)
      .define(KafkaSecurityConfigs.SECURITY_PROVIDER_CLASS_CONFIG, STRING, null, LOW, KafkaSecurityConfigs.SECURITY_PROVIDERS_DOC)

      /** ********* SSL Configuration ****************/
      .define(KafkaSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, CLASS, KafkaSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DEFAULT, MEDIUM, KafkaSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC)
      .define(KafkaSecurityConfigs.SSL_PROTOCOL_CONFIG, STRING, KafkaSecurityConfigs.SSL_PROTOCOL_DEFAULT, MEDIUM, KafkaSecurityConfigs.SSL_PROTOCOL_DOC)
      .define(KafkaSecurityConfigs.SSL_PROVIDER_CONFIG, STRING, null, MEDIUM, KafkaSecurityConfigs.SSL_PROVIDER_DOC)
      .define(KafkaSecurityConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, LIST, KafkaSecurityConfigs.SSL_ENABLED_PROTOCOLS_DEFAULTS, MEDIUM, KafkaSecurityConfigs.SSL_ENABLED_PROTOCOLS_DOC)
      .define(KafkaSecurityConfigs.SSL_KEYSTORE_TYPE_CONFIG, STRING, KafkaSecurityConfigs.SSL_KEYSTORE_TYPE_DEFAULT, MEDIUM, KafkaSecurityConfigs.SSL_KEYSTORE_TYPE_DOC)
      .define(KafkaSecurityConfigs.SSL_KEYSTORE_LOCATION_CONFIG, STRING, null, MEDIUM, KafkaSecurityConfigs.SSL_KEYSTORE_LOCATION_DOC)
      .define(KafkaSecurityConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SSL_KEYSTORE_PASSWORD_DOC)
      .define(KafkaSecurityConfigs.SSL_KEY_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SSL_KEY_PASSWORD_DOC)
      .define(KafkaSecurityConfigs.SSL_KEYSTORE_KEY_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SSL_KEYSTORE_KEY_DOC)
      .define(KafkaSecurityConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC)
      .define(KafkaSecurityConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, STRING, KafkaSecurityConfigs.SSL_TRUSTSTORE_TYPE_DEFAULT, MEDIUM, KafkaSecurityConfigs.SSL_TRUSTSTORE_TYPE_DOC)
      .define(KafkaSecurityConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, STRING, null, MEDIUM, KafkaSecurityConfigs.SSL_TRUSTSTORE_LOCATION_DOC)
      .define(KafkaSecurityConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SSL_TRUSTSTORE_PASSWORD_DOC)
      .define(KafkaSecurityConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC)
      .define(KafkaSecurityConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, STRING, KafkaSecurityConfigs.SSL_KEYMANAGER_ALGORITHM_DEFAULT, MEDIUM, KafkaSecurityConfigs.SSL_KEYMANAGER_ALGORITHM_DOC)
      .define(KafkaSecurityConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, STRING, KafkaSecurityConfigs.SSL_TRUSTMANAGER_ALGORITHM_DEFAULT, MEDIUM, KafkaSecurityConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC)
      .define(KafkaSecurityConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, STRING, KafkaSecurityConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DEFAULT, LOW, KafkaSecurityConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
      .define(KafkaSecurityConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, STRING, null, LOW, KafkaSecurityConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC)
      .define(KafkaSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, STRING, KafkaSecurityConfigs.SSL_CLIENT_AUTH_DEFAULT, in(KafkaSecurityConfigs.SSL_CLIENT_AUTHENTICATION_VALID_VALUES:_*), MEDIUM, KafkaSecurityConfigs.SSL_CLIENT_AUTH_DOC)
      .define(KafkaSecurityConfigs.SSL_CIPHER_SUITES_CONFIG, LIST, Collections.emptyList(), MEDIUM, KafkaSecurityConfigs.SSL_CIPHER_SUITES_DOC)
      .define(KafkaSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG, STRING, KafkaSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DEFAULT, LOW, KafkaSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DOC)
      .define(KafkaSecurityConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, CLASS, null, LOW, KafkaSecurityConfigs.SSL_ENGINE_FACTORY_CLASS_DOC)
      .define(KafkaSecurityConfigs.SSL_ALLOW_DN_CHANGES_CONFIG, BOOLEAN, KafkaSecurityConfigs.SSL_ALLOW_DN_CHANGES_DEFAULT, LOW, KafkaSecurityConfigs.SSL_ALLOW_DN_CHANGES_DOC)
      .define(KafkaSecurityConfigs.SSL_ALLOW_SAN_CHANGES_CONFIG, BOOLEAN, KafkaSecurityConfigs.SSL_ALLOW_SAN_CHANGES_DEFAULT, LOW, KafkaSecurityConfigs.SSL_ALLOW_SAN_CHANGES_DOC)

      /** ********* Sasl Configuration ****************/
      .define(KafkaSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, STRING, KafkaSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_DOC)
      .define(KafkaSecurityConfigs.SASL_JAAS_CONFIG, PASSWORD, null, MEDIUM, KafkaSecurityConfigs.SASL_JAAS_CONFIG_DOC)
      .define(KafkaSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, LIST, KafkaSecurityConfigs.SASL_ENABLED_MECHANISMS_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC)
      .define(KafkaSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG, CLASS, null, MEDIUM, KafkaSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC)
      .define(KafkaSecurityConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_CONFIG, CLASS, null, MEDIUM, KafkaSecurityConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC)
      .define(KafkaSecurityConfigs.SASL_LOGIN_CLASS_CONFIG, CLASS, null, MEDIUM, KafkaSecurityConfigs.SASL_LOGIN_CLASS_DOC)
      .define(KafkaSecurityConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_CONFIG, CLASS, null, MEDIUM, KafkaSecurityConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC)
      .define(KafkaSecurityConfigs.SASL_KERBEROS_SERVICE_NAME_CONFIG, STRING, null, MEDIUM, KafkaSecurityConfigs.SASL_KERBEROS_SERVICE_NAME_DOC)
      .define(KafkaSecurityConfigs.SASL_KERBEROS_KINIT_CMD_CONFIG, STRING, KafkaSecurityConfigs.SASL_KERBEROS_KINIT_CMD_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_KERBEROS_KINIT_CMD_DOC)
      .define(KafkaSecurityConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG, DOUBLE, KafkaSecurityConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC)
      .define(KafkaSecurityConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG, DOUBLE, KafkaSecurityConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC)
      .define(KafkaSecurityConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG, LONG, KafkaSecurityConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC)
      .define(KafkaSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG, LIST, KafkaSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC)
      .define(KafkaSecurityConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_CONFIG, DOUBLE, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC)
      .define(KafkaSecurityConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_CONFIG, DOUBLE, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC)
      .define(KafkaSecurityConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_CONFIG, SHORT, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC)
      .define(KafkaSecurityConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_CONFIG, SHORT, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DEFAULT, MEDIUM, KafkaSecurityConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC)
      .define(KafkaSecurityConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_CONFIG, INT, null, LOW, KafkaSecurityConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC)
      .define(KafkaSecurityConfigs.SASL_LOGIN_READ_TIMEOUT_MS_CONFIG, INT, null, LOW, KafkaSecurityConfigs.SASL_LOGIN_READ_TIMEOUT_MS_DOC)
      .define(KafkaSecurityConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_CONFIG, LONG, KafkaSecurityConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DEFAULT, LOW, KafkaSecurityConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC)
      .define(KafkaSecurityConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_CONFIG, LONG, KafkaSecurityConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DEFAULT, LOW, KafkaSecurityConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DOC)
      .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_CONFIG, STRING, KafkaSecurityConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DEFAULT, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC)
      .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_CONFIG, STRING, KafkaSecurityConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DEFAULT, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC)
      .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_CONFIG, STRING, null, MEDIUM, KafkaSecurityConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC)
      .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_CONFIG, STRING, null, MEDIUM, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC)
      .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_CONFIG, LONG, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DEFAULT, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC)
      .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_CONFIG, LONG, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DEFAULT, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC)
      .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_CONFIG, LONG, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DEFAULT, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC)
      .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_CONFIG, INT, KafkaSecurityConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DEFAULT, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC)
      .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_CONFIG, LIST, null, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC)
      .define(KafkaSecurityConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_CONFIG, STRING, null, LOW, KafkaSecurityConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC)

      /** ********* Delegation Token Configuration ****************/
      .define(DelegationTokenSecretKeyAliasProp, PASSWORD, null, MEDIUM, DelegationTokenSecretKeyAliasDoc)
      .define(DelegationTokenSecretKeyProp, PASSWORD, null, MEDIUM, DelegationTokenSecretKeyDoc)
      .define(DelegationTokenMaxLifeTimeProp, LONG, Defaults.DELEGATION_TOKEN_MAX_LIFE_TIME_MS, atLeast(1), MEDIUM, DelegationTokenMaxLifeTimeDoc)
      .define(DelegationTokenExpiryTimeMsProp, LONG, Defaults.DELEGATION_TOKEN_EXPIRY_TIME_MS, atLeast(1), MEDIUM, DelegationTokenExpiryTimeMsDoc)
      .define(DelegationTokenExpiryCheckIntervalMsProp, LONG, Defaults.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS, atLeast(1), LOW, DelegationTokenExpiryCheckIntervalDoc)

      /** ********* Password encryption configuration for dynamic configs *********/
      .define(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG, PASSWORD, null, MEDIUM, PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_DOC)
      .define(PasswordEncoderConfigs.PASSWORD_ENCODER_OLD_SECRET_CONFIG, PASSWORD, null, MEDIUM, PasswordEncoderConfigs.PASSWORD_ENCODER_OLD_SECRET_DOC)
      .define(PasswordEncoderConfigs.PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_CONFIG, STRING, null, LOW, PasswordEncoderConfigs.PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_DOC)
      .define(PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_CONFIG, STRING, PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_DEFAULT, LOW, PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_DOC)
      .define(PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_CONFIG, INT, PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_DEFAULT, atLeast(8), LOW, PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_DOC)
      .define(PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_CONFIG, INT, PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_DEFAULT, atLeast(1024), LOW, PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_DOC)

      /** ********* Raft Quorum Configuration *********/
      .define(QuorumConfig.QUORUM_VOTERS_CONFIG, LIST, QuorumConfig.DEFAULT_QUORUM_VOTERS, new QuorumConfig.ControllerQuorumVotersValidator(), HIGH, QuorumConfig.QUORUM_VOTERS_DOC)
      .define(QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_ELECTION_TIMEOUT_MS, null, HIGH, QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_DOC)
      .define(QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_FETCH_TIMEOUT_MS, null, HIGH, QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_DOC)
      .define(QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_ELECTION_BACKOFF_MAX_MS, null, HIGH, QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_DOC)
      .define(QuorumConfig.QUORUM_LINGER_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_LINGER_MS, null, MEDIUM, QuorumConfig.QUORUM_LINGER_MS_DOC)
      .define(QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_REQUEST_TIMEOUT_MS, null, MEDIUM, QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_DOC)
      .define(QuorumConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_RETRY_BACKOFF_MS, null, LOW, QuorumConfig.QUORUM_RETRY_BACKOFF_MS_DOC)

      /** Internal Configurations **/
      // This indicates whether unreleased APIs should be advertised by this node.
      .defineInternal(UnstableApiVersionsEnableProp, BOOLEAN, false, HIGH)
      // This indicates whether unreleased MetadataVersions should be enabled on this node.
      .defineInternal(UnstableMetadataVersionsEnableProp, BOOLEAN, false, HIGH)
  }

  /** ********* Remote Log Management Configuration *********/
  RemoteLogManagerConfig.CONFIG_DEF.configKeys().values().forEach(key => configDef.define(key))

  def configNames: Seq[String] = configDef.names.asScala.toBuffer.sorted
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

  def apply(props: java.util.Map[_, _], doLog: Boolean = true): KafkaConfig = new KafkaConfig(props, doLog)

  private def typeOf(name: String): Option[ConfigDef.Type] = Option(configDef.configKeys.get(name)).map(_.`type`)

  def configType(configName: String): Option[ConfigDef.Type] = {
    val configType = configTypeExact(configName)
    if (configType.isDefined) {
      return configType
    }
    typeOf(configName) match {
      case Some(t) => Some(t)
      case None =>
        DynamicBrokerConfig.brokerConfigSynonyms(configName, matchListenerOverride = true).flatMap(typeOf).headOption
    }
  }

  private def configTypeExact(exactName: String): Option[ConfigDef.Type] = {
    val configType = typeOf(exactName).orNull
    if (configType != null) {
      Some(configType)
    } else {
      val configKey = DynamicConfig.Broker.brokerConfigDef.configKeys().get(exactName)
      if (configKey != null) {
        Some(configKey.`type`)
      } else {
        None
      }
    }
  }

  def maybeSensitive(configType: Option[ConfigDef.Type]): Boolean = {
    // If we can't determine the config entry type, treat it as a sensitive config to be safe
    configType.isEmpty || configType.contains(ConfigDef.Type.PASSWORD)
  }

  def loggableValue(resourceType: ConfigResource.Type, name: String, value: String): String = {
    val maybeSensitive = resourceType match {
      case ConfigResource.Type.BROKER => KafkaConfig.maybeSensitive(KafkaConfig.configType(name))
      case ConfigResource.Type.TOPIC => KafkaConfig.maybeSensitive(LogConfig.configType(name).asScala)
      case ConfigResource.Type.BROKER_LOGGER => false
      case ConfigResource.Type.CLIENT_METRICS => false
      case _ => true
    }
    if (maybeSensitive) Password.HIDDEN else value
  }

  /**
   * Copy a configuration map, populating some keys that we want to treat as synonyms.
   */
  def populateSynonyms(input: util.Map[_, _]): util.Map[Any, Any] = {
    val output = new util.HashMap[Any, Any](input)
    val brokerId = output.get(KafkaConfig.BrokerIdProp)
    val nodeId = output.get(KRaftConfigs.NODE_ID_CONFIG)
    if (brokerId == null && nodeId != null) {
      output.put(KafkaConfig.BrokerIdProp, nodeId)
    } else if (brokerId != null && nodeId == null) {
      output.put(KRaftConfigs.NODE_ID_CONFIG, brokerId)
    }
    output
  }
}

class KafkaConfig private(doLog: Boolean, val props: java.util.Map[_, _], dynamicConfigOverride: Option[DynamicBrokerConfig])
  extends AbstractConfig(KafkaConfig.configDef, props, Utils.castToStringObjectMap(props), doLog) with Logging {

  def this(props: java.util.Map[_, _]) = this(true, KafkaConfig.populateSynonyms(props), None)
  def this(props: java.util.Map[_, _], doLog: Boolean) = this(doLog, KafkaConfig.populateSynonyms(props), None)
  def this(props: java.util.Map[_, _], doLog: Boolean, dynamicConfigOverride: Option[DynamicBrokerConfig]) =
    this(doLog, KafkaConfig.populateSynonyms(props), dynamicConfigOverride)

  // Cache the current config to avoid acquiring read lock to access from dynamicConfig
  @volatile private var currentConfig = this
  val processRoles: Set[ProcessRole] = parseProcessRoles()
  private[server] val dynamicConfig = dynamicConfigOverride.getOrElse(new DynamicBrokerConfig(this))

  private[server] def updateCurrentConfig(newConfig: KafkaConfig): Unit = {
    this.currentConfig = newConfig
  }

  // The following captures any system properties impacting ZooKeeper TLS configuration
  // and defines the default values this instance will use if no explicit config is given.
  // We make it part of each instance rather than the object to facilitate testing.
  private val zkClientConfigViaSystemProperties = new ZKClientConfig()

  override def originals: util.Map[String, AnyRef] =
    if (this eq currentConfig) super.originals else currentConfig.originals
  override def values: util.Map[String, _] =
    if (this eq currentConfig) super.values else currentConfig.values
  override def nonInternalValues: util.Map[String, _] =
    if (this eq currentConfig) super.nonInternalValues else currentConfig.nonInternalValues
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
  def valuesFromThisConfigWithPrefixOverride(prefix: String): util.Map[String, AnyRef] =
    super.valuesWithPrefixOverride(prefix)

  /** ********* Zookeeper Configuration ***********/
  val zkConnect: String = getString(ZkConfigs.ZK_CONNECT_CONFIG)
  val zkSessionTimeoutMs: Int = getInt(ZkConfigs.ZK_SESSION_TIMEOUT_MS_CONFIG)
  val zkConnectionTimeoutMs: Int =
    Option(getInt(ZkConfigs.ZK_CONNECTION_TIMEOUT_MS_CONFIG)).map(_.toInt).getOrElse(getInt(ZkConfigs.ZK_SESSION_TIMEOUT_MS_CONFIG))
  val zkEnableSecureAcls: Boolean = getBoolean(ZkConfigs.ZK_ENABLE_SECURE_ACLS_CONFIG)
  val zkMaxInFlightRequests: Int = getInt(ZkConfigs.ZK_MAX_IN_FLIGHT_REQUESTS_CONFIG)

  private val _remoteLogManagerConfig = new RemoteLogManagerConfig(this)
  def remoteLogManagerConfig = _remoteLogManagerConfig

  private def zkBooleanConfigOrSystemPropertyWithDefaultValue(propKey: String): Boolean = {
    // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
    // Need to translate any system property value from true/false (String) to true/false (Boolean)
    val actuallyProvided = originals.containsKey(propKey)
    if (actuallyProvided) getBoolean(propKey) else {
      val sysPropValue = KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey)
      sysPropValue match {
        case Some("true") => true
        case Some(_) => false
        case _ => getBoolean(propKey) // not specified so use the default value
      }
    }
  }

  private def zkStringConfigOrSystemPropertyWithDefaultValue(propKey: String): String = {
    // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
    val actuallyProvided = originals.containsKey(propKey)
    if (actuallyProvided) getString(propKey) else {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey) match {
        case Some(v) => v
        case _ => getString(propKey) // not specified so use the default value
      }
    }
  }

  private def zkOptionalStringConfigOrSystemProperty(propKey: String): Option[String] = {
    Option(getString(propKey)).orElse {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey)
    }
  }
  private def zkPasswordConfigOrSystemProperty(propKey: String): Option[Password] = {
    Option(getPassword(propKey)).orElse {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).map(new Password(_))
    }
  }
  private def zkListConfigOrSystemProperty(propKey: String): Option[util.List[String]] = {
    Option(getList(propKey)).orElse {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).map { sysProp =>
        sysProp.split("\\s*,\\s*").toBuffer.asJava
      }
    }
  }

  val zkSslClientEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG)
  val zkClientCnxnSocketClassName = zkOptionalStringConfigOrSystemProperty(ZkConfigs.ZK_CLIENT_CNXN_SOCKET_CONFIG)
  val zkSslKeyStoreLocation = zkOptionalStringConfigOrSystemProperty(ZkConfigs.ZK_SSL_KEY_STORE_LOCATION_CONFIG)
  val zkSslKeyStorePassword = zkPasswordConfigOrSystemProperty(ZkConfigs.ZK_SSL_KEY_STORE_PASSWORD_CONFIG)
  val zkSslKeyStoreType = zkOptionalStringConfigOrSystemProperty(ZkConfigs.ZK_SSL_KEY_STORE_TYPE_CONFIG)
  val zkSslTrustStoreLocation = zkOptionalStringConfigOrSystemProperty(ZkConfigs.ZK_SSL_TRUST_STORE_LOCATION_CONFIG)
  val zkSslTrustStorePassword = zkPasswordConfigOrSystemProperty(ZkConfigs.ZK_SSL_TRUST_STORE_PASSWORD_CONFIG)
  val zkSslTrustStoreType = zkOptionalStringConfigOrSystemProperty(ZkConfigs.ZK_SSL_TRUST_STORE_TYPE_CONFIG)
  val ZkSslProtocol = zkStringConfigOrSystemPropertyWithDefaultValue(ZkConfigs.ZK_SSL_PROTOCOL_CONFIG)
  val ZkSslEnabledProtocols = zkListConfigOrSystemProperty(ZkConfigs.ZK_SSL_ENABLED_PROTOCOLS_CONFIG)
  val ZkSslCipherSuites = zkListConfigOrSystemProperty(ZkConfigs.ZK_SSL_CIPHER_SUITES_CONFIG)
  val ZkSslEndpointIdentificationAlgorithm = {
    // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
    // Need to translate any system property value from true/false to HTTPS/<blank>
    val kafkaProp = ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG
    val actuallyProvided = originals.containsKey(kafkaProp)
    if (actuallyProvided)
      getString(kafkaProp)
    else {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, kafkaProp) match {
        case Some("true") => "HTTPS"
        case Some(_) => ""
        case None => getString(kafkaProp) // not specified so use the default value
      }
    }
  }
  val ZkSslCrlEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(ZkConfigs.ZK_SSL_CRL_ENABLE_CONFIG)
  val ZkSslOcspEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(ZkConfigs.ZK_SSL_OCSP_ENABLE_CONFIG)
  /** ********* General Configuration ***********/
  val brokerIdGenerationEnable: Boolean = getBoolean(KafkaConfig.BrokerIdGenerationEnableProp)
  val maxReservedBrokerId: Int = getInt(KafkaConfig.MaxReservedBrokerIdProp)
  var brokerId: Int = getInt(KafkaConfig.BrokerIdProp)
  val nodeId: Int = getInt(KRaftConfigs.NODE_ID_CONFIG)
  val initialRegistrationTimeoutMs: Int = getInt(KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG)
  val brokerHeartbeatIntervalMs: Int = getInt(KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG)
  val brokerSessionTimeoutMs: Int = getInt(KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG)

  def requiresZookeeper: Boolean = processRoles.isEmpty
  def usesSelfManagedQuorum: Boolean = processRoles.nonEmpty

  val migrationEnabled: Boolean = getBoolean(KRaftConfigs.MIGRATION_ENABLED_CONFIG)
  val migrationMetadataMinBatchSize: Int = getInt(KRaftConfigs.MIGRATION_METADATA_MIN_BATCH_SIZE_CONFIG)

  val elrEnabled: Boolean = getBoolean(KRaftConfigs.ELR_ENABLED_CONFIG)

  private def parseProcessRoles(): Set[ProcessRole] = {
    val roles = getList(KRaftConfigs.PROCESS_ROLES_CONFIG).asScala.map {
      case "broker" => ProcessRole.BrokerRole
      case "controller" => ProcessRole.ControllerRole
      case role => throw new ConfigException(s"Unknown process role '$role'" +
        " (only 'broker' and 'controller' are allowed roles)")
    }

    val distinctRoles: Set[ProcessRole] = roles.toSet

    if (distinctRoles.size != roles.size) {
      throw new ConfigException(s"Duplicate role names found in `${KRaftConfigs.PROCESS_ROLES_CONFIG}`: $roles")
    }

    distinctRoles
  }

  def isKRaftCombinedMode: Boolean = {
    processRoles == Set(ProcessRole.BrokerRole, ProcessRole.ControllerRole)
  }

  def metadataLogDir: String = {
    Option(getString(KRaftConfigs.METADATA_LOG_DIR_CONFIG)) match {
      case Some(dir) => dir
      case None => logDirs.head
    }
  }

  def metadataLogSegmentBytes = getInt(KRaftConfigs.METADATA_LOG_SEGMENT_BYTES_CONFIG)
  def metadataLogSegmentMillis = getLong(KRaftConfigs.METADATA_LOG_SEGMENT_MILLIS_CONFIG)
  def metadataRetentionBytes = getLong(KRaftConfigs.METADATA_MAX_RETENTION_BYTES_CONFIG)
  def metadataRetentionMillis = getLong(KRaftConfigs.METADATA_MAX_RETENTION_MILLIS_CONFIG)
  def metadataNodeIDConfig = getInt(KRaftConfigs.NODE_ID_CONFIG)
  def metadataLogSegmentMinBytes = getInt(KRaftConfigs.METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG)
  val serverMaxStartupTimeMs = getLong(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG)

  def numNetworkThreads = getInt(KafkaConfig.NumNetworkThreadsProp)
  def backgroundThreads = getInt(KafkaConfig.BackgroundThreadsProp)
  val queuedMaxRequests = getInt(KafkaConfig.QueuedMaxRequestsProp)
  val queuedMaxBytes = getLong(KafkaConfig.QueuedMaxBytesProp)
  def numIoThreads = getInt(KafkaConfig.NumIoThreadsProp)
  def messageMaxBytes = getInt(KafkaConfig.MessageMaxBytesProp)
  val requestTimeoutMs = getInt(KafkaConfig.RequestTimeoutMsProp)
  val connectionSetupTimeoutMs = getLong(KafkaConfig.ConnectionSetupTimeoutMsProp)
  val connectionSetupTimeoutMaxMs = getLong(KafkaConfig.ConnectionSetupTimeoutMaxMsProp)

  def getNumReplicaAlterLogDirsThreads: Int = {
    val numThreads: Integer = Option(getInt(KafkaConfig.NumReplicaAlterLogDirsThreadsProp)).getOrElse(logDirs.size)
    numThreads
  }

  /************* Metadata Configuration ***********/
  val metadataSnapshotMaxNewRecordBytes = getLong(KRaftConfigs.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG)
  val metadataSnapshotMaxIntervalMs = getLong(KRaftConfigs.METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG)
  val metadataMaxIdleIntervalNs: Option[Long] = {
    val value = TimeUnit.NANOSECONDS.convert(getInt(KRaftConfigs.METADATA_MAX_IDLE_INTERVAL_MS_CONFIG).toLong, TimeUnit.MILLISECONDS)
    if (value > 0) Some(value) else None
  }

  /************* Authorizer Configuration ***********/
  def createNewAuthorizer(): Option[Authorizer] = {
    val className = getString(KafkaConfig.AuthorizerClassNameProp)
    if (className == null || className.isEmpty)
      None
    else {
      Some(AuthorizerUtils.createAuthorizer(className))
    }
  }

  val earlyStartListeners: Set[ListenerName] = {
    val listenersSet = listeners.map(_.listenerName).toSet
    val controllerListenersSet = controllerListeners.map(_.listenerName).toSet
    Option(getString(KafkaConfig.EarlyStartListenersProp)) match {
      case None => controllerListenersSet
      case Some(str) =>
        str.split(",").map(_.trim()).filterNot(_.isEmpty).map { str =>
          val listenerName = new ListenerName(str)
          if (!listenersSet.contains(listenerName) && !controllerListenersSet.contains(listenerName))
            throw new ConfigException(s"${KafkaConfig.EarlyStartListenersProp} contains " +
              s"listener ${listenerName.value()}, but this is not contained in " +
              s"${SocketServerConfigs.LISTENERS_CONFIG} or ${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG}")
          listenerName
        }.toSet
    }
  }

  /** ********* Socket Server Configuration ***********/
  val socketSendBufferBytes = getInt(SocketServerConfigs.SOCKET_SEND_BUFFER_BYTES_CONFIG)
  val socketReceiveBufferBytes = getInt(SocketServerConfigs.SOCKET_RECEIVE_BUFFER_BYTES_CONFIG)
  val socketRequestMaxBytes = getInt(SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_CONFIG)
  val socketListenBacklogSize = getInt(SocketServerConfigs.SOCKET_LISTEN_BACKLOG_SIZE_CONFIG)
  val maxConnectionsPerIp = getInt(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG)
  val maxConnectionsPerIpOverrides: Map[String, Int] =
    getMap(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG, getString(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG)).map { case (k, v) => (k, v.toInt)}
  def maxConnections = getInt(SocketServerConfigs.MAX_CONNECTIONS_CONFIG)
  def maxConnectionCreationRate = getInt(SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG)
  val connectionsMaxIdleMs = getLong(SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG)
  val failedAuthenticationDelayMs = getInt(SocketServerConfigs.FAILED_AUTHENTICATION_DELAY_MS_CONFIG)

  /***************** rack configuration **************/
  val rack = Option(getString(KafkaConfig.RackProp))
  val replicaSelectorClassName = Option(getString(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG))

  /** ********* Log Configuration ***********/
  val autoCreateTopicsEnable = getBoolean(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG)
  val numPartitions = getInt(ServerLogConfigs.NUM_PARTITIONS_CONFIG)
  val logDirs = CoreUtils.parseCsvList(Option(getString(ServerLogConfigs.LOG_DIRS_CONFIG)).getOrElse(getString(ServerLogConfigs.LOG_DIR_CONFIG)))
  def logSegmentBytes = getInt(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG)
  def logFlushIntervalMessages = getLong(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG)
  val logCleanerThreads = getInt(CleanerConfig.LOG_CLEANER_THREADS_PROP)
  def numRecoveryThreadsPerDataDir = getInt(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG)
  val logFlushSchedulerIntervalMs = getLong(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG)
  val logFlushOffsetCheckpointIntervalMs = getInt(ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG).toLong
  val logFlushStartOffsetCheckpointIntervalMs = getInt(ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG).toLong
  val logCleanupIntervalMs = getLong(ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG)
  def logCleanupPolicy = getList(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG)

  val offsetsRetentionMinutes = getInt(GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG)
  val offsetsRetentionCheckIntervalMs = getLong(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG)
  def logRetentionBytes = getLong(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG)
  val logCleanerDedupeBufferSize = getLong(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP)
  val logCleanerDedupeBufferLoadFactor = getDouble(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP)
  val logCleanerIoBufferSize = getInt(CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP)
  val logCleanerIoMaxBytesPerSecond = getDouble(CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP)
  def logCleanerDeleteRetentionMs = getLong(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP)
  def logCleanerMinCompactionLagMs = getLong(CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP)
  def logCleanerMaxCompactionLagMs = getLong(CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP)
  val logCleanerBackoffMs = getLong(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP)
  def logCleanerMinCleanRatio = getDouble(CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP)
  val logCleanerEnable = getBoolean(CleanerConfig.LOG_CLEANER_ENABLE_PROP)
  def logIndexSizeMaxBytes = getInt(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG)
  def logIndexIntervalBytes = getInt(ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_CONFIG)
  def logDeleteDelayMs = getLong(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG)
  def logRollTimeMillis: java.lang.Long = Option(getLong(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG)).getOrElse(60 * 60 * 1000L * getInt(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG))
  def logRollTimeJitterMillis: java.lang.Long = Option(getLong(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG)).getOrElse(60 * 60 * 1000L * getInt(ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG))
  def logFlushIntervalMs: java.lang.Long = Option(getLong(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG)).getOrElse(getLong(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG))
  def minInSyncReplicas = getInt(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG)
  def logPreAllocateEnable: java.lang.Boolean = getBoolean(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG)
  def logInitialTaskDelayMs: java.lang.Long = Option(getLong(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG)).getOrElse(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DEFAULT)

  // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version (eg if `0.10.0`
  // is passed, `0.10.0-IV0` may be picked)
  @nowarn("cat=deprecation")
  private val logMessageFormatVersionString = getString(ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_CONFIG)

  /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
  @deprecated("3.0")
  lazy val logMessageFormatVersion =
    if (LogConfig.shouldIgnoreMessageFormatVersion(interBrokerProtocolVersion))
      MetadataVersion.fromVersionString(ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_DEFAULT)
    else MetadataVersion.fromVersionString(logMessageFormatVersionString)

  def logMessageTimestampType = TimestampType.forName(getString(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG))

  /* See `TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG` for details */
  @deprecated("3.6")
  def logMessageTimestampDifferenceMaxMs: Long = getLong(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG)

  // In the transition period before logMessageTimestampDifferenceMaxMs is removed, to maintain backward compatibility,
  // we are using its value if logMessageTimestampBeforeMaxMs default value hasn't changed.
  // See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for deprecation details
  @nowarn("cat=deprecation")
  def logMessageTimestampBeforeMaxMs: Long = {
    val messageTimestampBeforeMaxMs: Long = getLong(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG)
    if (messageTimestampBeforeMaxMs != ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DEFAULT) {
      messageTimestampBeforeMaxMs
    } else {
      logMessageTimestampDifferenceMaxMs
    }
  }

  // In the transition period before logMessageTimestampDifferenceMaxMs is removed, to maintain backward compatibility,
  // we are using its value if logMessageTimestampAfterMaxMs default value hasn't changed.
  // See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for deprecation details
  @nowarn("cat=deprecation")
  def logMessageTimestampAfterMaxMs: Long = {
    val messageTimestampAfterMaxMs: Long = getLong(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG)
    if (messageTimestampAfterMaxMs != Long.MaxValue) {
      messageTimestampAfterMaxMs
    } else {
      logMessageTimestampDifferenceMaxMs
    }
  }

  def logMessageDownConversionEnable: Boolean = getBoolean(ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_CONFIG)

  /** ********* Replication configuration ***********/
  val controllerSocketTimeoutMs: Int = getInt(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG)
  val defaultReplicationFactor: Int = getInt(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG)
  val replicaLagTimeMaxMs = getLong(ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_CONFIG)
  val replicaSocketTimeoutMs = getInt(ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG)
  val replicaSocketReceiveBufferBytes = getInt(ReplicationConfigs.REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_CONFIG)
  val replicaFetchMaxBytes = getInt(ReplicationConfigs.REPLICA_FETCH_MAX_BYTES_CONFIG)
  val replicaFetchWaitMaxMs = getInt(ReplicationConfigs.REPLICA_FETCH_WAIT_MAX_MS_CONFIG)
  val replicaFetchMinBytes = getInt(ReplicationConfigs.REPLICA_FETCH_MIN_BYTES_CONFIG)
  val replicaFetchResponseMaxBytes = getInt(ReplicationConfigs.REPLICA_FETCH_RESPONSE_MAX_BYTES_CONFIG)
  val replicaFetchBackoffMs = getInt(ReplicationConfigs.REPLICA_FETCH_BACKOFF_MS_CONFIG)
  def numReplicaFetchers = getInt(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG)
  val replicaHighWatermarkCheckpointIntervalMs = getLong(ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_CONFIG)
  val fetchPurgatoryPurgeIntervalRequests = getInt(ReplicationConfigs.FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG)
  val producerPurgatoryPurgeIntervalRequests = getInt(ReplicationConfigs.PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG)
  val deleteRecordsPurgatoryPurgeIntervalRequests = getInt(ReplicationConfigs.DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG)
  val autoLeaderRebalanceEnable = getBoolean(ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG)
  val leaderImbalancePerBrokerPercentage = getInt(ReplicationConfigs.LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_CONFIG)
  val leaderImbalanceCheckIntervalSeconds: Long = getLong(ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_CONFIG)
  def uncleanLeaderElectionEnable: java.lang.Boolean = getBoolean(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG)

  // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version (eg if `0.10.0`
  // is passed, `0.10.0-IV0` may be picked)
  val interBrokerProtocolVersionString = getString(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG)
  val interBrokerProtocolVersion = if (processRoles.isEmpty) {
    MetadataVersion.fromVersionString(interBrokerProtocolVersionString)
  } else {
    if (originals.containsKey(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG)) {
      // A user-supplied IBP was given
      val configuredVersion = MetadataVersion.fromVersionString(interBrokerProtocolVersionString)
      if (!configuredVersion.isKRaftSupported) {
        throw new ConfigException(s"A non-KRaft version $interBrokerProtocolVersionString given for ${ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG}. " +
          s"The minimum version is ${MetadataVersion.MINIMUM_KRAFT_VERSION}")
      } else {
        warn(s"${ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG} is deprecated in KRaft mode as of 3.3 and will only " +
          s"be read when first upgrading from a KRaft prior to 3.3. See kafka-storage.sh help for details on setting " +
          s"the metadata.version for a new KRaft cluster.")
      }
    }
    // In KRaft mode, we pin this value to the minimum KRaft-supported version. This prevents inadvertent usage of
    // the static IBP config in broker components running in KRaft mode
    MetadataVersion.MINIMUM_KRAFT_VERSION
  }

  /** ********* Controlled shutdown configuration ***********/
  val controlledShutdownMaxRetries = getInt(KafkaConfig.ControlledShutdownMaxRetriesProp)
  val controlledShutdownRetryBackoffMs = getLong(KafkaConfig.ControlledShutdownRetryBackoffMsProp)
  val controlledShutdownEnable = getBoolean(KafkaConfig.ControlledShutdownEnableProp)

  /** ********* Feature configuration ***********/
  def isFeatureVersioningSupported = interBrokerProtocolVersion.isFeatureVersioningSupported

  /** ********* Group coordinator configuration ***********/
  val groupMinSessionTimeoutMs = getInt(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG)
  val groupMaxSessionTimeoutMs = getInt(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG)
  val groupInitialRebalanceDelay = getInt(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG)
  val groupMaxSize = getInt(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG)

  /** New group coordinator configs */
  val groupCoordinatorRebalanceProtocols = {
    val protocols = getList(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG)
      .asScala.map(_.toUpperCase).map(GroupType.valueOf).toSet
    if (!protocols.contains(GroupType.CLASSIC)) {
      throw new ConfigException(s"Disabling the '${GroupType.CLASSIC}' protocol is not supported.")
    }
    if (protocols.contains(GroupType.CONSUMER)) {
      warn(s"The new '${GroupType.CONSUMER}' rebalance protocol is enabled along with the new group coordinator. " +
        "This is part of the early access of KIP-848 and MUST NOT be used in production.")
    }
    protocols
  }
  // The new group coordinator is enabled in two cases: 1) The internal configuration to enable
  // it is explicitly set; or 2) the consumer rebalance protocol is enabled.
  val isNewGroupCoordinatorEnabled = getBoolean(GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG) ||
    groupCoordinatorRebalanceProtocols.contains(GroupType.CONSUMER)
  val groupCoordinatorNumThreads = getInt(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG)

  /** Consumer group configs */
  val consumerGroupSessionTimeoutMs = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG)
  val consumerGroupMinSessionTimeoutMs = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG)
  val consumerGroupMaxSessionTimeoutMs = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG)
  val consumerGroupHeartbeatIntervalMs = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG)
  val consumerGroupMinHeartbeatIntervalMs = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG)
  val consumerGroupMaxHeartbeatIntervalMs = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG)
  val consumerGroupMaxSize = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG)
  val consumerGroupAssignors = getConfiguredInstances(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, classOf[PartitionAssignor])
  val consumerGroupMigrationPolicy = ConsumerGroupMigrationPolicy.parse(getString(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG))

  /** ********* Offset management configuration ***********/
  val offsetMetadataMaxSize = getInt(GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_CONFIG)
  val offsetsLoadBufferSize = getInt(GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_CONFIG)
  val offsetsTopicReplicationFactor = getShort(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG)
  val offsetsTopicPartitions = getInt(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG)
  val offsetCommitTimeoutMs = getInt(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG)
  @deprecated("3.8")
  val offsetCommitRequiredAcks = getShort(GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_CONFIG)
  val offsetsTopicSegmentBytes = getInt(GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG)
  val offsetsTopicCompressionType = Option(getInt(GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG)).map(value => CompressionType.forId(value)).orNull

  /** ********* Transaction management configuration ***********/
  val transactionalIdExpirationMs = getInt(TransactionStateManagerConfigs.TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG)
  val transactionMaxTimeoutMs = getInt(TransactionStateManagerConfigs.TRANSACTIONS_MAX_TIMEOUT_MS_CONFIG)
  val transactionTopicMinISR = getInt(TransactionLogConfigs.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG)
  val transactionsLoadBufferSize = getInt(TransactionLogConfigs.TRANSACTIONS_LOAD_BUFFER_SIZE_CONFIG)
  val transactionTopicReplicationFactor = getShort(TransactionLogConfigs.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG)
  val transactionTopicPartitions = getInt(TransactionLogConfigs.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG)
  val transactionTopicSegmentBytes = getInt(TransactionLogConfigs.TRANSACTIONS_TOPIC_SEGMENT_BYTES_CONFIG)
  val transactionAbortTimedOutTransactionCleanupIntervalMs = getInt(TransactionStateManagerConfigs.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG)
  val transactionRemoveExpiredTransactionalIdCleanupIntervalMs = getInt(TransactionStateManagerConfigs.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG)

  def transactionPartitionVerificationEnable = getBoolean(TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG)

  def producerIdExpirationMs = getInt(TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_CONFIG)
  val producerIdExpirationCheckIntervalMs = getInt(TransactionLogConfigs.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_CONFIG)

  /** ********* Metric Configuration **************/
  val metricNumSamples = getInt(MetricConfigs.METRIC_NUM_SAMPLES_CONFIG)
  val metricSampleWindowMs = getLong(MetricConfigs.METRIC_SAMPLE_WINDOW_MS_CONFIG)
  val metricRecordingLevel = getString(MetricConfigs.METRIC_RECORDING_LEVEL_CONFIG)

  /** ********* Kafka Client Telemetry Metrics Configuration ***********/
  val clientTelemetryMaxBytes: Int = getInt(MetricConfigs.CLIENT_TELEMETRY_MAX_BYTES_CONFIG)

  /** ********* SSL/SASL Configuration **************/
  // Security configs may be overridden for listeners, so it is not safe to use the base values
  // Hence the base SSL/SASL configs are not fields of KafkaConfig, listener configs should be
  // retrieved using KafkaConfig#valuesWithPrefixOverride
  private def saslEnabledMechanisms(listenerName: ListenerName): Set[String] = {
    val value = valuesWithPrefixOverride(listenerName.configPrefix).get(KafkaSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG)
    if (value != null)
      value.asInstanceOf[util.List[String]].asScala.toSet
    else
      Set.empty[String]
  }

  def interBrokerListenerName = getInterBrokerListenerNameAndSecurityProtocol._1
  def interBrokerSecurityProtocol = getInterBrokerListenerNameAndSecurityProtocol._2
  def controlPlaneListenerName = getControlPlaneListenerNameAndSecurityProtocol.map { case (listenerName, _) => listenerName }
  def controlPlaneSecurityProtocol = getControlPlaneListenerNameAndSecurityProtocol.map { case (_, securityProtocol) => securityProtocol }
  def saslMechanismInterBrokerProtocol = getString(KafkaSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG)
  val saslInterBrokerHandshakeRequestEnable = interBrokerProtocolVersion.isSaslInterBrokerHandshakeRequestEnabled

  /** ********* DelegationToken Configuration **************/
  val delegationTokenSecretKey = Option(getPassword(KafkaConfig.DelegationTokenSecretKeyProp))
    .getOrElse(getPassword(KafkaConfig.DelegationTokenSecretKeyAliasProp))
  val tokenAuthEnabled = delegationTokenSecretKey != null && delegationTokenSecretKey.value.nonEmpty
  val delegationTokenMaxLifeMs = getLong(KafkaConfig.DelegationTokenMaxLifeTimeProp)
  val delegationTokenExpiryTimeMs = getLong(KafkaConfig.DelegationTokenExpiryTimeMsProp)
  val delegationTokenExpiryCheckIntervalMs = getLong(KafkaConfig.DelegationTokenExpiryCheckIntervalMsProp)

  /** ********* Password encryption configuration for dynamic configs *********/
  def passwordEncoderSecret = Option(getPassword(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG))
  def passwordEncoderOldSecret = Option(getPassword(PasswordEncoderConfigs.PASSWORD_ENCODER_OLD_SECRET_CONFIG))
  def passwordEncoderCipherAlgorithm = getString(PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_CONFIG)
  def passwordEncoderKeyFactoryAlgorithm = getString(PasswordEncoderConfigs.PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_CONFIG)
  def passwordEncoderKeyLength = getInt(PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_CONFIG)
  def passwordEncoderIterations = getInt(PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_CONFIG)

  /** ********* Quota Configuration **************/
  val numQuotaSamples = getInt(QuotaConfigs.NUM_QUOTA_SAMPLES_CONFIG)
  val quotaWindowSizeSeconds = getInt(QuotaConfigs.QUOTA_WINDOW_SIZE_SECONDS_CONFIG)
  val numReplicationQuotaSamples = getInt(QuotaConfigs.NUM_REPLICATION_QUOTA_SAMPLES_CONFIG)
  val replicationQuotaWindowSizeSeconds = getInt(QuotaConfigs.REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG)
  val numAlterLogDirsReplicationQuotaSamples = getInt(QuotaConfigs.NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_CONFIG)
  val alterLogDirsReplicationQuotaWindowSizeSeconds = getInt(QuotaConfigs.ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG)
  val numControllerQuotaSamples = getInt(QuotaConfigs.NUM_CONTROLLER_QUOTA_SAMPLES_CONFIG)
  val controllerQuotaWindowSizeSeconds = getInt(QuotaConfigs.CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_CONFIG)

  /** ********* Fetch Configuration **************/
  val maxIncrementalFetchSessionCacheSlots = getInt(KafkaConfig.MaxIncrementalFetchSessionCacheSlots)
  val fetchMaxBytes = getInt(KafkaConfig.FetchMaxBytes)

  /** ********* Request Limit Configuration ***********/
  val maxRequestPartitionSizeLimit = getInt(KafkaConfig.MaxRequestPartitionSizeLimit)

  val deleteTopicEnable = getBoolean(KafkaConfig.DeleteTopicEnableProp)
  def compressionType = getString(KafkaConfig.CompressionTypeProp)

  /** ********* Raft Quorum Configuration *********/
  val quorumVoters = getList(QuorumConfig.QUORUM_VOTERS_CONFIG)
  val quorumElectionTimeoutMs = getInt(QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG)
  val quorumFetchTimeoutMs = getInt(QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG)
  val quorumElectionBackoffMs = getInt(QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG)
  val quorumLingerMs = getInt(QuorumConfig.QUORUM_LINGER_MS_CONFIG)
  val quorumRequestTimeoutMs = getInt(QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG)
  val quorumRetryBackoffMs = getInt(QuorumConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG)

  /** Internal Configurations **/
  val unstableApiVersionsEnabled = getBoolean(KafkaConfig.UnstableApiVersionsEnableProp)
  val unstableMetadataVersionsEnabled = getBoolean(KafkaConfig.UnstableMetadataVersionsEnableProp)

  def addReconfigurable(reconfigurable: Reconfigurable): Unit = {
    dynamicConfig.addReconfigurable(reconfigurable)
  }

  def removeReconfigurable(reconfigurable: Reconfigurable): Unit = {
    dynamicConfig.removeReconfigurable(reconfigurable)
  }

  def logRetentionTimeMillis: Long = {
    val millisInMinute = 60L * 1000L
    val millisInHour = 60L * millisInMinute

    val millis: java.lang.Long =
      Option(getLong(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG)).getOrElse(
        Option(getInt(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG)) match {
          case Some(mins) => millisInMinute * mins
          case None => getInt(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG) * millisInHour
        })

    if (millis < 0) return -1
    millis
  }

  private def getMap(propName: String, propValue: String): Map[String, String] = {
    try {
      Csv.parseCsvMap(propValue).asScala
    } catch {
      case e: Exception => throw new IllegalArgumentException("Error parsing configuration property '%s': %s".format(propName, e.getMessage))
    }
  }

  def listeners: Seq[EndPoint] =
    CoreUtils.listenerListToEndPoints(getString(SocketServerConfigs.LISTENERS_CONFIG), effectiveListenerSecurityProtocolMap)

  def controllerListenerNames: Seq[String] = {
    val value = Option(getString(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG)).getOrElse("")
    if (value.isEmpty) {
      Seq.empty
    } else {
      value.split(",")
    }
  }

  def controllerListeners: Seq[EndPoint] =
    listeners.filter(l => controllerListenerNames.contains(l.listenerName.value()))

  def saslMechanismControllerProtocol: String = getString(KRaftConfigs.SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG)

  def controlPlaneListener: Option[EndPoint] = {
    controlPlaneListenerName.map { listenerName =>
      listeners.filter(endpoint => endpoint.listenerName.value() == listenerName.value()).head
    }
  }

  def dataPlaneListeners: Seq[EndPoint] = {
    listeners.filterNot { listener =>
      val name = listener.listenerName.value()
      name.equals(getString(SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG)) ||
        controllerListenerNames.contains(name)
    }
  }

  // Use advertised listeners if defined, fallback to listeners otherwise
  def effectiveAdvertisedListeners: Seq[EndPoint] = {
    val advertisedListenersProp = getString(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG)
    if (advertisedListenersProp != null)
      CoreUtils.listenerListToEndPoints(advertisedListenersProp, effectiveListenerSecurityProtocolMap, requireDistinctPorts=false)
    else
      listeners.filterNot(l => controllerListenerNames.contains(l.listenerName.value()))
  }

  private def getInterBrokerListenerNameAndSecurityProtocol: (ListenerName, SecurityProtocol) = {
    Option(getString(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG)) match {
      case Some(_) if originals.containsKey(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG) =>
        throw new ConfigException(s"Only one of ${ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG} and " +
          s"${ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG} should be set.")
      case Some(name) =>
        val listenerName = ListenerName.normalised(name)
        val securityProtocol = effectiveListenerSecurityProtocolMap.getOrElse(listenerName,
          throw new ConfigException(s"Listener with name ${listenerName.value} defined in " +
            s"${ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG} not found in ${SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG}."))
        (listenerName, securityProtocol)
      case None =>
        val securityProtocol = getSecurityProtocol(getString(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG),
          ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG)
        (ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
    }
  }

  private def getControlPlaneListenerNameAndSecurityProtocol: Option[(ListenerName, SecurityProtocol)] = {
    Option(getString(SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG)) match {
      case Some(name) =>
        val listenerName = ListenerName.normalised(name)
        val securityProtocol = effectiveListenerSecurityProtocolMap.getOrElse(listenerName,
          throw new ConfigException(s"Listener with ${listenerName.value} defined in " +
            s"${SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG} not found in ${SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG}."))
        Some(listenerName, securityProtocol)

      case None => None
   }
  }

  private def getSecurityProtocol(protocolName: String, configName: String): SecurityProtocol = {
    try SecurityProtocol.forName(protocolName)
    catch {
      case _: IllegalArgumentException =>
        throw new ConfigException(s"Invalid security protocol `$protocolName` defined in $configName")
    }
  }

  def effectiveListenerSecurityProtocolMap: Map[ListenerName, SecurityProtocol] = {
    val mapValue = getMap(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, getString(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG))
      .map { case (listenerName, protocolName) =>
        ListenerName.normalised(listenerName) -> getSecurityProtocol(protocolName, SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG)
      }
    if (usesSelfManagedQuorum && !originals.containsKey(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG)) {
      // Nothing was specified explicitly for listener.security.protocol.map, so we are using the default value,
      // and we are using KRaft.
      // Add PLAINTEXT mappings for controller listeners as long as there is no SSL or SASL_{PLAINTEXT,SSL} in use
      def isSslOrSasl(name: String): Boolean = name.equals(SecurityProtocol.SSL.name) || name.equals(SecurityProtocol.SASL_SSL.name) || name.equals(SecurityProtocol.SASL_PLAINTEXT.name)
      // check controller listener names (they won't appear in listeners when process.roles=broker)
      // as well as listeners for occurrences of SSL or SASL_*
      if (controllerListenerNames.exists(isSslOrSasl) ||
        parseCsvList(getString(SocketServerConfigs.LISTENERS_CONFIG)).exists(listenerValue => isSslOrSasl(EndPoint.parseListenerName(listenerValue)))) {
        mapValue // don't add default mappings since we found something that is SSL or SASL_*
      } else {
        // add the PLAINTEXT mappings for all controller listener names that are not explicitly PLAINTEXT
        mapValue ++ controllerListenerNames.filterNot(SecurityProtocol.PLAINTEXT.name.equals(_)).map(
          new ListenerName(_) -> SecurityProtocol.PLAINTEXT)
      }
    } else {
      mapValue
    }
  }

  // Topic IDs are used with all self-managed quorum clusters and ZK cluster with IBP greater than or equal to 2.8
  def usesTopicId: Boolean =
    usesSelfManagedQuorum || interBrokerProtocolVersion.isTopicIdsSupported


  val isRemoteLogStorageSystemEnabled: lang.Boolean = getBoolean(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP)
  def logLocalRetentionBytes: java.lang.Long = getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP)

  def logLocalRetentionMs: java.lang.Long = getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP)

  validateValues()

  @nowarn("cat=deprecation")
  private def validateValues(): Unit = {
    if (nodeId != brokerId) {
      throw new ConfigException(s"You must set `${KRaftConfigs.NODE_ID_CONFIG}` to the same value as `${KafkaConfig.BrokerIdProp}`.")
    }
    if (requiresZookeeper) {
      if (zkConnect == null) {
        throw new ConfigException(s"Missing required configuration `${ZkConfigs.ZK_CONNECT_CONFIG}` which has no default value.")
      }
      if (brokerIdGenerationEnable) {
        require(brokerId >= -1 && brokerId <= maxReservedBrokerId, "broker.id must be greater than or equal to -1 and not greater than reserved.broker.max.id")
      } else {
        require(brokerId >= 0, "broker.id must be greater than or equal to 0")
      }
    } else {
      // KRaft-based metadata quorum
      if (nodeId < 0) {
        throw new ConfigException(s"Missing configuration `${KRaftConfigs.NODE_ID_CONFIG}` which is required " +
          s"when `process.roles` is defined (i.e. when running in KRaft mode).")
      }
      if (migrationEnabled) {
        if (zkConnect == null) {
          throw new ConfigException(s"If using `${KRaftConfigs.MIGRATION_ENABLED_CONFIG}` in KRaft mode, `${ZkConfigs.ZK_CONNECT_CONFIG}` must also be set.")
        }
      }
    }
    require(logRollTimeMillis >= 1, "log.roll.ms must be greater than or equal to 1")
    require(logRollTimeJitterMillis >= 0, "log.roll.jitter.ms must be greater than or equal to 0")
    require(logRetentionTimeMillis >= 1 || logRetentionTimeMillis == -1, "log.retention.ms must be unlimited (-1) or, greater than or equal to 1")
    require(logDirs.nonEmpty, "At least one log directory must be defined via log.dirs or log.dir.")
    if (isRemoteLogStorageSystemEnabled && logDirs.size > 1) {
      throw new ConfigException(s"Multiple log directories `${logDirs.mkString(",")}` are not supported when remote log storage is enabled")
    }
    require(logCleanerDedupeBufferSize / logCleanerThreads > 1024 * 1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.")
    require(replicaFetchWaitMaxMs <= replicaSocketTimeoutMs, "replica.socket.timeout.ms should always be at least replica.fetch.wait.max.ms" +
      " to prevent unnecessary socket timeouts")
    require(replicaFetchWaitMaxMs <= replicaLagTimeMaxMs, "replica.fetch.wait.max.ms should always be less than or equal to replica.lag.time.max.ms" +
      " to prevent frequent changes in ISR")
    require(offsetCommitRequiredAcks >= -1 && offsetCommitRequiredAcks <= offsetsTopicReplicationFactor,
      "offsets.commit.required.acks must be greater or equal -1 and less or equal to offsets.topic.replication.factor")
    val advertisedListenerNames = effectiveAdvertisedListeners.map(_.listenerName).toSet

    // validate KRaft-related configs
    val voterIds = QuorumConfig.parseVoterIds(quorumVoters)
    def validateNonEmptyQuorumVotersForKRaft(): Unit = {
      if (voterIds.isEmpty) {
        throw new ConfigException(s"If using ${KRaftConfigs.PROCESS_ROLES_CONFIG}, ${QuorumConfig.QUORUM_VOTERS_CONFIG} must contain a parseable set of voters.")
      }
    }
    def validateNonEmptyQuorumVotersForMigration(): Unit = {
      if (voterIds.isEmpty) {
        throw new ConfigException(s"If using ${KRaftConfigs.MIGRATION_ENABLED_CONFIG}, ${QuorumConfig.QUORUM_VOTERS_CONFIG} must contain a parseable set of voters.")
      }
    }
    def validateControlPlaneListenerEmptyForKRaft(): Unit = {
      require(controlPlaneListenerName.isEmpty,
        s"${SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG} is not supported in KRaft mode.")
    }
    def validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker(): Unit = {
      require(!advertisedListenerNames.exists(aln => controllerListenerNames.contains(aln.value())),
        s"The advertised.listeners config must not contain KRaft controller listeners from ${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} when ${KRaftConfigs.PROCESS_ROLES_CONFIG} contains the broker role because Kafka clients that send requests via advertised listeners do not send requests to KRaft controllers -- they only send requests to KRaft brokers.")
    }
    def validateControllerQuorumVotersMustContainNodeIdForKRaftController(): Unit = {
      require(voterIds.contains(nodeId),
        s"If ${KRaftConfigs.PROCESS_ROLES_CONFIG} contains the 'controller' role, the node id $nodeId must be included in the set of voters ${QuorumConfig.QUORUM_VOTERS_CONFIG}=${voterIds.asScala.toSet}")
    }
    def validateControllerListenerExistsForKRaftController(): Unit = {
      require(controllerListeners.nonEmpty,
        s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must contain at least one value appearing in the '${SocketServerConfigs.LISTENERS_CONFIG}' configuration when running the KRaft controller role")
    }
    def validateControllerListenerNamesMustAppearInListenersForKRaftController(): Unit = {
      val listenerNameValues = listeners.map(_.listenerName.value).toSet
      require(controllerListenerNames.forall(cln => listenerNameValues.contains(cln)),
        s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must only contain values appearing in the '${SocketServerConfigs.LISTENERS_CONFIG}' configuration when running the KRaft controller role")
    }
    def validateAdvertisedListenersNonEmptyForBroker(): Unit = {
      require(advertisedListenerNames.nonEmpty,
        "There must be at least one advertised listener." + (
          if (processRoles.contains(ProcessRole.BrokerRole)) s" Perhaps all listeners appear in ${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG}?" else ""))
    }
    if (processRoles == Set(ProcessRole.BrokerRole)) {
      // KRaft broker-only
      validateNonEmptyQuorumVotersForKRaft()
      validateControlPlaneListenerEmptyForKRaft()
      validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker()
      // nodeId must not appear in controller.quorum.voters
      require(!voterIds.contains(nodeId),
        s"If ${KRaftConfigs.PROCESS_ROLES_CONFIG} contains just the 'broker' role, the node id $nodeId must not be included in the set of voters ${QuorumConfig.QUORUM_VOTERS_CONFIG}=${voterIds.asScala.toSet}")
      // controller.listener.names must be non-empty...
      require(controllerListenerNames.nonEmpty,
        s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must contain at least one value when running KRaft with just the broker role")
      // controller.listener.names are forbidden in listeners...
      require(controllerListeners.isEmpty,
        s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must not contain a value appearing in the '${SocketServerConfigs.LISTENERS_CONFIG}' configuration when running KRaft with just the broker role")
      // controller.listener.names must all appear in listener.security.protocol.map
      controllerListenerNames.foreach { name =>
        val listenerName = ListenerName.normalised(name)
        if (!effectiveListenerSecurityProtocolMap.contains(listenerName)) {
          throw new ConfigException(s"Controller listener with name ${listenerName.value} defined in " +
            s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} not found in ${SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG}  (an explicit security mapping for each controller listener is required if ${SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG} is non-empty, or if there are security protocols other than PLAINTEXT in use)")
        }
      }
      // warn that only the first controller listener is used if there is more than one
      if (controllerListenerNames.size > 1) {
        warn(s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} has multiple entries; only the first will be used since ${KRaftConfigs.PROCESS_ROLES_CONFIG}=broker: ${controllerListenerNames.asJava}")
      }
      validateAdvertisedListenersNonEmptyForBroker()
    } else if (processRoles == Set(ProcessRole.ControllerRole)) {
      // KRaft controller-only
      validateNonEmptyQuorumVotersForKRaft()
      validateControlPlaneListenerEmptyForKRaft()
      // advertised listeners must be empty when only the controller is configured
      require(
        getString(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG) == null,
        s"The ${SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG} config must be empty when ${KRaftConfigs.PROCESS_ROLES_CONFIG}=controller"
      )
      // listeners should only contain listeners also enumerated in the controller listener
      require(
        effectiveAdvertisedListeners.isEmpty,
        s"The ${SocketServerConfigs.LISTENERS_CONFIG} config must only contain KRaft controller listeners from ${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} when ${KRaftConfigs.PROCESS_ROLES_CONFIG}=controller"
      )
      validateControllerQuorumVotersMustContainNodeIdForKRaftController()
      validateControllerListenerExistsForKRaftController()
      validateControllerListenerNamesMustAppearInListenersForKRaftController()
    } else if (isKRaftCombinedMode) {
      // KRaft combined broker and controller
      validateNonEmptyQuorumVotersForKRaft()
      validateControlPlaneListenerEmptyForKRaft()
      validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker()
      validateControllerQuorumVotersMustContainNodeIdForKRaftController()
      validateControllerListenerExistsForKRaftController()
      validateControllerListenerNamesMustAppearInListenersForKRaftController()
      validateAdvertisedListenersNonEmptyForBroker()
    } else {
      // ZK-based
      if (migrationEnabled) {
        validateNonEmptyQuorumVotersForMigration()
        require(controllerListenerNames.nonEmpty,
          s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must not be empty when running in ZooKeeper migration mode: ${controllerListenerNames.asJava}")
        require(interBrokerProtocolVersion.isMigrationSupported, s"Cannot enable ZooKeeper migration without setting " +
          s"'${ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG}' to 3.4 or higher")
        if (logDirs.size > 1) {
          require(interBrokerProtocolVersion.isDirectoryAssignmentSupported,
            s"Cannot enable ZooKeeper migration with multiple log directories (aka JBOD) without setting " +
            s"'${ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG}' to ${MetadataVersion.IBP_3_7_IV2} or higher")
        }
      } else {
        // controller listener names must be empty when not in KRaft mode
        require(controllerListenerNames.isEmpty,
          s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must be empty when not running in KRaft mode: ${controllerListenerNames.asJava}")
      }
      validateAdvertisedListenersNonEmptyForBroker()
    }

    val listenerNames = listeners.map(_.listenerName).toSet
    if (processRoles.isEmpty || processRoles.contains(ProcessRole.BrokerRole)) {
      // validations for all broker setups (i.e. ZooKeeper and KRaft broker-only and KRaft co-located)
      validateAdvertisedListenersNonEmptyForBroker()
      require(advertisedListenerNames.contains(interBrokerListenerName),
        s"${ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG} must be a listener name defined in ${SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG}. " +
          s"The valid options based on currently configured listeners are ${advertisedListenerNames.map(_.value).mkString(",")}")
      require(advertisedListenerNames.subsetOf(listenerNames),
        s"${SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG} listener names must be equal to or a subset of the ones defined in ${SocketServerConfigs.LISTENERS_CONFIG}. " +
          s"Found ${advertisedListenerNames.map(_.value).mkString(",")}. The valid options based on the current configuration " +
          s"are ${listenerNames.map(_.value).mkString(",")}"
      )
    }

    require(!effectiveAdvertisedListeners.exists(endpoint => endpoint.host=="0.0.0.0"),
      s"${SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG} cannot use the nonroutable meta-address 0.0.0.0. "+
      s"Use a routable IP address.")

    // validate control.plane.listener.name config
    if (controlPlaneListenerName.isDefined) {
      require(advertisedListenerNames.contains(controlPlaneListenerName.get),
        s"${SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG} must be a listener name defined in ${SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG}. " +
        s"The valid options based on currently configured listeners are ${advertisedListenerNames.map(_.value).mkString(",")}")
      // controlPlaneListenerName should be different from interBrokerListenerName
      require(!controlPlaneListenerName.get.value().equals(interBrokerListenerName.value()),
        s"${SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG}, when defined, should have a different value from the inter broker listener name. " +
        s"Currently they both have the value ${controlPlaneListenerName.get}")
    }

    val messageFormatVersion = new MessageFormatVersion(logMessageFormatVersionString, interBrokerProtocolVersionString)
    if (messageFormatVersion.shouldWarn)
      warn(createBrokerWarningMessage)

    val recordVersion = logMessageFormatVersion.highestSupportedRecordVersion
    require(interBrokerProtocolVersion.highestSupportedRecordVersion().value >= recordVersion.value,
      s"log.message.format.version $logMessageFormatVersionString can only be used when inter.broker.protocol.version " +
      s"is set to version ${MetadataVersion.minSupportedFor(recordVersion).shortVersion} or higher")

    if (offsetsTopicCompressionType == CompressionType.ZSTD)
      require(interBrokerProtocolVersion.highestSupportedRecordVersion().value >= IBP_2_1_IV0.highestSupportedRecordVersion().value,
        "offsets.topic.compression.codec zstd can only be used when inter.broker.protocol.version " +
        s"is set to version ${IBP_2_1_IV0.shortVersion} or higher")

    val interBrokerUsesSasl = interBrokerSecurityProtocol == SecurityProtocol.SASL_PLAINTEXT || interBrokerSecurityProtocol == SecurityProtocol.SASL_SSL
    require(!interBrokerUsesSasl || saslInterBrokerHandshakeRequestEnable || saslMechanismInterBrokerProtocol == SaslConfigs.GSSAPI_MECHANISM,
      s"Only GSSAPI mechanism is supported for inter-broker communication with SASL when inter.broker.protocol.version is set to $interBrokerProtocolVersionString")
    require(!interBrokerUsesSasl || saslEnabledMechanisms(interBrokerListenerName).contains(saslMechanismInterBrokerProtocol),
      s"${KafkaSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG} must be included in ${KafkaSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG} when SASL is used for inter-broker communication")
    require(queuedMaxBytes <= 0 || queuedMaxBytes >= socketRequestMaxBytes,
      s"${KafkaConfig.QueuedMaxBytesProp} must be larger or equal to ${SocketServerConfigs.SOCKET_RECEIVE_BUFFER_BYTES_CONFIG}")

    if (maxConnectionsPerIp == 0)
      require(maxConnectionsPerIpOverrides.nonEmpty, s"${SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG} can be set to zero only if" +
        s" ${SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG} property is set.")

    val invalidAddresses = maxConnectionsPerIpOverrides.keys.filterNot(address => Utils.validHostPattern(address))
    if (invalidAddresses.nonEmpty)
      throw new IllegalArgumentException(s"${SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG} contains invalid addresses : ${invalidAddresses.mkString(",")}")

    if (connectionsMaxIdleMs >= 0)
      require(failedAuthenticationDelayMs < connectionsMaxIdleMs,
        s"${SocketServerConfigs.FAILED_AUTHENTICATION_DELAY_MS_CONFIG}=$failedAuthenticationDelayMs should always be less than" +
        s" ${SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG}=$connectionsMaxIdleMs to prevent failed" +
        s" authentication responses from timing out")

    val principalBuilderClass = getClass(KafkaSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG)
    require(principalBuilderClass != null, s"${KafkaSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG} must be non-null")
    require(classOf[KafkaPrincipalSerde].isAssignableFrom(principalBuilderClass),
      s"${KafkaSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG} must implement KafkaPrincipalSerde")

    // New group coordinator configs validation.
    require(consumerGroupMaxHeartbeatIntervalMs >= consumerGroupMinHeartbeatIntervalMs,
      s"${GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG} must be greater than or equals " +
      s"to ${GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG}")
    require(consumerGroupHeartbeatIntervalMs >= consumerGroupMinHeartbeatIntervalMs,
      s"${GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG} must be greater than or equals " +
      s"to ${GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG}")
    require(consumerGroupHeartbeatIntervalMs <= consumerGroupMaxHeartbeatIntervalMs,
      s"${GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG} must be less than or equals " +
      s"to ${GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG}")

    require(consumerGroupMaxSessionTimeoutMs >= consumerGroupMinSessionTimeoutMs,
      s"${GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG} must be greater than or equals " +
      s"to ${GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG}")
    require(consumerGroupSessionTimeoutMs >= consumerGroupMinSessionTimeoutMs,
      s"${GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG} must be greater than or equals " +
      s"to ${GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG}")
    require(consumerGroupSessionTimeoutMs <= consumerGroupMaxSessionTimeoutMs,
      s"${GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG} must be less than or equals " +
      s"to ${GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG}")

    if (originals.containsKey(GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_CONFIG)) {
      warn(s"${GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_CONFIG} is deprecated and it will be removed in Apache Kafka 4.0.")
    }
  }

  /**
   * Copy the subset of properties that are relevant to Logs. The individual properties
   * are listed here since the names are slightly different in each Config class...
   */
  @nowarn("cat=deprecation")
  def extractLogConfigMap: java.util.Map[String, Object] = {
    val logProps = new java.util.HashMap[String, Object]()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, logSegmentBytes)
    logProps.put(TopicConfig.SEGMENT_MS_CONFIG, logRollTimeMillis)
    logProps.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, logRollTimeJitterMillis)
    logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, logIndexSizeMaxBytes)
    logProps.put(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, logFlushIntervalMessages)
    logProps.put(TopicConfig.FLUSH_MS_CONFIG, logFlushIntervalMs)
    logProps.put(TopicConfig.RETENTION_BYTES_CONFIG, logRetentionBytes)
    logProps.put(TopicConfig.RETENTION_MS_CONFIG, logRetentionTimeMillis: java.lang.Long)
    logProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, messageMaxBytes)
    logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, logIndexIntervalBytes)
    logProps.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, logCleanerDeleteRetentionMs)
    logProps.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, logCleanerMinCompactionLagMs)
    logProps.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, logCleanerMaxCompactionLagMs)
    logProps.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, logDeleteDelayMs)
    logProps.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, logCleanerMinCleanRatio)
    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, logCleanupPolicy)
    logProps.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSyncReplicas)
    logProps.put(TopicConfig.COMPRESSION_TYPE_CONFIG, compressionType)
    logProps.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, uncleanLeaderElectionEnable)
    logProps.put(TopicConfig.PREALLOCATE_CONFIG, logPreAllocateEnable)
    logProps.put(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, logMessageFormatVersion.version)
    logProps.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, logMessageTimestampType.name)
    logProps.put(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, logMessageTimestampDifferenceMaxMs: java.lang.Long)
    logProps.put(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, logMessageTimestampBeforeMaxMs: java.lang.Long)
    logProps.put(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, logMessageTimestampAfterMaxMs: java.lang.Long)
    logProps.put(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, logMessageDownConversionEnable: java.lang.Boolean)
    logProps.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, logLocalRetentionMs)
    logProps.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, logLocalRetentionBytes)
    logProps
  }

  @nowarn("cat=deprecation")
  private def createBrokerWarningMessage: String = {
    s"Broker configuration ${ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_CONFIG} with value $logMessageFormatVersionString is ignored " +
      s"because the inter-broker protocol version `$interBrokerProtocolVersionString` is greater or equal than 3.0. " +
      "This configuration is deprecated and it will be removed in Apache Kafka 4.0."
  }
}
