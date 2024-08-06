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
import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}
import kafka.cluster.EndPoint
import kafka.utils.{CoreUtils, Logging}
import kafka.utils.Implicits._
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, ConfigResource, SaslConfigs, SecurityConfig, SslClientAuth, SslConfigs, TopicConfig}
import org.apache.kafka.common.config.ConfigDef.{CaseInsensitiveValidString, ConfigKey, ValidList, ValidString}
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.{CompressionType, LegacyRecord, Records, TimestampType}
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.group.ConsumerGroupMigrationPolicy
import org.apache.kafka.coordinator.group.Group.GroupType
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor
import org.apache.kafka.coordinator.transaction.{TransactionLogConfigs, TransactionStateManagerConfigs}
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.security.authorizer.AuthorizerUtils
import org.apache.kafka.security.PasswordEncoderConfigs
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{MetadataVersion, MetadataVersionValidator}
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.config.{DelegationTokenManagerConfigs, KRaftConfigs, ServerConfigs, QuotaConfigs, ReplicationConfigs, ServerLogConfigs, ZkConfigs}
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

  @nowarn("cat=deprecation")
  val configDef = {
    import ConfigDef.Importance._
    import ConfigDef.Range._
    import ConfigDef.Type._

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
      .define(ServerConfigs.BROKER_ID_GENERATION_ENABLE_CONFIG, BOOLEAN, ServerConfigs.BROKER_ID_GENERATION_ENABLE_DEFAULT, MEDIUM, ServerConfigs.BROKER_ID_GENERATION_ENABLE_DOC)
      .define(ServerConfigs.RESERVED_BROKER_MAX_ID_CONFIG, INT, ServerConfigs.RESERVED_BROKER_MAX_ID_DEFAULT, atLeast(0), MEDIUM, ServerConfigs.RESERVED_BROKER_MAX_ID_DOC)
      .define(ServerConfigs.BROKER_ID_CONFIG, INT, ServerConfigs.BROKER_ID_DEFAULT, HIGH, ServerConfigs.BROKER_ID_DOC)
      .define(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, INT, LogConfig.DEFAULT_MAX_MESSAGE_BYTES, atLeast(0), HIGH, ServerConfigs.MESSAGE_MAX_BYTES_DOC)
      .define(ServerConfigs.NUM_NETWORK_THREADS_CONFIG, INT, ServerConfigs.NUM_NETWORK_THREADS_DEFAULT, atLeast(1), HIGH, ServerConfigs.NUM_NETWORK_THREADS_DOC)
      .define(ServerConfigs.NUM_IO_THREADS_CONFIG, INT, ServerConfigs.NUM_IO_THREADS_DEFAULT, atLeast(1), HIGH, ServerConfigs.NUM_IO_THREADS_DOC)
      .define(ServerConfigs.NUM_REPLICA_ALTER_LOG_DIRS_THREADS_CONFIG, INT, null, HIGH, ServerConfigs.NUM_REPLICA_ALTER_LOG_DIRS_THREADS_DOC)
      .define(ServerConfigs.BACKGROUND_THREADS_CONFIG, INT, ServerConfigs.BACKGROUND_THREADS_DEFAULT, atLeast(1), HIGH, ServerConfigs.BACKGROUND_THREADS_DOC)
      .define(ServerConfigs.QUEUED_MAX_REQUESTS_CONFIG, INT, ServerConfigs.QUEUED_MAX_REQUESTS_DEFAULT, atLeast(1), HIGH, ServerConfigs.QUEUED_MAX_REQUESTS_DOC)
      .define(ServerConfigs.QUEUED_MAX_BYTES_CONFIG, LONG, ServerConfigs.QUEUED_MAX_REQUEST_BYTES_DEFAULT, MEDIUM, ServerConfigs.QUEUED_MAX_REQUEST_BYTES_DOC)
      .define(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG, INT, ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT, HIGH, ServerConfigs.REQUEST_TIMEOUT_MS_DOC)
      .define(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, LONG, ServerConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS, MEDIUM, ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
      .define(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, LONG, ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS, MEDIUM, ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)

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
      .define(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, STRING, ServerConfigs.AUTHORIZER_CLASS_NAME_DEFAULT, new ConfigDef.NonNullValidator(), LOW, ServerConfigs.AUTHORIZER_CLASS_NAME_DOC)
      .define(ServerConfigs.EARLY_START_LISTENERS_CONFIG, STRING, null,  HIGH, ServerConfigs.EARLY_START_LISTENERS_DOC)

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
      .define(ServerConfigs.BROKER_RACK_CONFIG, STRING, null, MEDIUM, ServerConfigs.BROKER_RACK_DOC)

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
      .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG, STRING, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_DEFAULT, ValidString.in("CreateTime", "LogAppendTime"), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_DOC)
      .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC)
      .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC)
      .define(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, LONG, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DEFAULT, atLeast(0), MEDIUM, ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC)
      .define(ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG, CLASS, null, LOW, ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_DOC)
      .define(ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG, CLASS, null, LOW, ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_DOC)
      .define(ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, BOOLEAN, ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_DEFAULT, LOW, ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_DOC)
      .defineInternal(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, LONG, ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DEFAULT, atLeast(0), LOW, ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DOC)
      .define(ServerLogConfigs.LOG_DIR_FAILURE_TIMEOUT_MS_CONFIG, LONG, ServerLogConfigs.LOG_DIR_FAILURE_TIMEOUT_MS_DEFAULT, atLeast(1), LOW, ServerLogConfigs.LOG_DIR_FAILURE_TIMEOUT_MS_DOC)

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
      .define(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG, STRING, ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_DEFAULT, ValidString.in(Utils.enumOptions(classOf[SecurityProtocol]):_*), MEDIUM, ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_DOC)
      .define(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, STRING, ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_DEFAULT, new MetadataVersionValidator(), MEDIUM, ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_DOC)
      .define(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, STRING, null, MEDIUM, ReplicationConfigs.INTER_BROKER_LISTENER_NAME_DOC)
      .define(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG, STRING, null, MEDIUM, ReplicationConfigs.REPLICA_SELECTOR_CLASS_DOC)


      /** ********* Controlled shutdown configuration ***********/
      .define(ServerConfigs.CONTROLLED_SHUTDOWN_MAX_RETRIES_CONFIG, INT, ServerConfigs.CONTROLLED_SHUTDOWN_MAX_RETRIES_DEFAULT, MEDIUM, ServerConfigs.CONTROLLED_SHUTDOWN_MAX_RETRIES_DOC)
      .define(ServerConfigs.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_CONFIG, LONG, ServerConfigs.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_DEFAULT, MEDIUM, ServerConfigs.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_DOC)
      .define(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, BOOLEAN, ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_DEFAULT, MEDIUM, ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_DOC)

      /** ********* Group coordinator configuration ***********/
      .define(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT, MEDIUM, GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_DOC)
      .define(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT, MEDIUM, GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_DOC)
      .define(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, INT, GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT, MEDIUM, GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_DOC)
      .define(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, INT, GroupCoordinatorConfig.GROUP_MAX_SIZE_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.GROUP_MAX_SIZE_DOC)

      /** New group coordinator configs */
      .define(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, LIST, GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DEFAULT,
        ValidList.in(Utils.enumOptions(classOf[GroupType]):_*), MEDIUM, GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DOC)
      .define(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG, INT, GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_DOC)
      .define(GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, INT, GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_DEFAULT, atLeast(0), MEDIUM, GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_DOC)
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
      .define(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, STRING, GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_DEFAULT, CaseInsensitiveValidString.in(Utils.enumOptions(classOf[ConsumerGroupMigrationPolicy]): _*), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_DOC)

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
      .define(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, BOOLEAN, ServerConfigs.DELETE_TOPIC_ENABLE_DEFAULT, HIGH, ServerConfigs.DELETE_TOPIC_ENABLE_DOC)
      .define(ServerConfigs.COMPRESSION_TYPE_CONFIG, STRING, LogConfig.DEFAULT_COMPRESSION_TYPE, ValidString.in(BrokerCompressionType.names.asScala.toSeq:_*), HIGH, ServerConfigs.COMPRESSION_TYPE_DOC)
      .define(ServerConfigs.COMPRESSION_GZIP_LEVEL_CONFIG, INT, CompressionType.GZIP.defaultLevel, CompressionType.GZIP.levelValidator, MEDIUM, ServerConfigs.COMPRESSION_GZIP_LEVEL_DOC)
      .define(ServerConfigs.COMPRESSION_LZ4_LEVEL_CONFIG, INT, CompressionType.LZ4.defaultLevel, CompressionType.LZ4.levelValidator, MEDIUM, ServerConfigs.COMPRESSION_LZ4_LEVEL_DOC)
      .define(ServerConfigs.COMPRESSION_ZSTD_LEVEL_CONFIG, INT, CompressionType.ZSTD.defaultLevel, CompressionType.ZSTD.levelValidator, MEDIUM, ServerConfigs.COMPRESSION_ZSTD_LEVEL_DOC)

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
      .define(ServerConfigs.MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_CONFIG, INT, ServerConfigs.MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_DEFAULT, atLeast(0), MEDIUM, ServerConfigs.MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_DOC)
      .define(ServerConfigs.FETCH_MAX_BYTES_CONFIG, INT, ServerConfigs.FETCH_MAX_BYTES_DEFAULT, atLeast(1024), MEDIUM, ServerConfigs.FETCH_MAX_BYTES_DOC)

      /** ********* Request Limit Configuration ***********/
      .define(ServerConfigs.MAX_REQUEST_PARTITION_SIZE_LIMIT_CONFIG, INT, ServerConfigs.MAX_REQUEST_PARTITION_SIZE_LIMIT_DEFAULT, atLeast(1), MEDIUM, ServerConfigs.MAX_REQUEST_PARTITION_SIZE_LIMIT_DOC)

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
      .define(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG, LONG, BrokerSecurityConfigs.DEFAULT_CONNECTIONS_MAX_REAUTH_MS, MEDIUM, BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DOC)
      .define(BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG, INT, BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE, MEDIUM, BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_DOC)
      .define(SecurityConfig.SECURITY_PROVIDERS_CONFIG, STRING, null, LOW, SecurityConfig.SECURITY_PROVIDERS_DOC)

      /** ********* SSL Configuration ****************/
      .define(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, CLASS, BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DEFAULT, MEDIUM, BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC)
      .define(SslConfigs.SSL_PROTOCOL_CONFIG, STRING, SslConfigs.DEFAULT_SSL_PROTOCOL, MEDIUM, SslConfigs.SSL_PROTOCOL_DOC)
      .define(SslConfigs.SSL_PROVIDER_CONFIG, STRING, null, MEDIUM, SslConfigs.SSL_PROVIDER_DOC)
      .define(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, LIST, SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS, MEDIUM, SslConfigs.SSL_ENABLED_PROTOCOLS_DOC)
      .define(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, STRING, SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE, MEDIUM, SslConfigs.SSL_KEYSTORE_TYPE_DOC)
      .define(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, STRING, null, MEDIUM, SslConfigs.SSL_KEYSTORE_LOCATION_DOC)
      .define(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_KEYSTORE_PASSWORD_DOC)
      .define(SslConfigs.SSL_KEY_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_KEY_PASSWORD_DOC)
      .define(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_KEYSTORE_KEY_DOC)
      .define(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC)
      .define(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, STRING, SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE, MEDIUM, SslConfigs.SSL_TRUSTSTORE_TYPE_DOC)
      .define(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, STRING, null, MEDIUM, SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC)
      .define(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC)
      .define(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, PASSWORD, null, MEDIUM, SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC)
      .define(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, STRING, SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM, MEDIUM, SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC)
      .define(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, STRING, SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM, MEDIUM, SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC)
      .define(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, STRING, SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, LOW, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC)
      .define(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, STRING, null, LOW, SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC)
      .define(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, STRING, BrokerSecurityConfigs.SSL_CLIENT_AUTH_DEFAULT, ValidString.in(Utils.enumOptions(classOf[SslClientAuth]):_*), MEDIUM, BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC)
      .define(SslConfigs.SSL_CIPHER_SUITES_CONFIG, LIST, Collections.emptyList(), MEDIUM, SslConfigs.SSL_CIPHER_SUITES_DOC)
      .define(BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG, STRING, BrokerSecurityConfigs.DEFAULT_SSL_PRINCIPAL_MAPPING_RULES, LOW, BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DOC)
      .define(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, CLASS, null, LOW, SslConfigs.SSL_ENGINE_FACTORY_CLASS_DOC)
      .define(BrokerSecurityConfigs.SSL_ALLOW_DN_CHANGES_CONFIG, BOOLEAN, BrokerSecurityConfigs.DEFAULT_SSL_ALLOW_DN_CHANGES_VALUE, LOW, BrokerSecurityConfigs.SSL_ALLOW_DN_CHANGES_DOC)
      .define(BrokerSecurityConfigs.SSL_ALLOW_SAN_CHANGES_CONFIG, BOOLEAN, BrokerSecurityConfigs.DEFAULT_SSL_ALLOW_SAN_CHANGES_VALUE, LOW, BrokerSecurityConfigs.SSL_ALLOW_SAN_CHANGES_DOC)

      /** ********* Sasl Configuration ****************/
      .define(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, STRING, SaslConfigs.DEFAULT_SASL_MECHANISM, MEDIUM, BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_DOC)
      .define(SaslConfigs.SASL_JAAS_CONFIG, PASSWORD, null, MEDIUM, SaslConfigs.SASL_JAAS_CONFIG_DOC)
      .define(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, LIST, BrokerSecurityConfigs.DEFAULT_SASL_ENABLED_MECHANISMS, MEDIUM, BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC)
      .define(BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG, CLASS, null, MEDIUM, BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC)
      .define(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, CLASS, null, MEDIUM, SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC)
      .define(SaslConfigs.SASL_LOGIN_CLASS, CLASS, null, MEDIUM, SaslConfigs.SASL_LOGIN_CLASS_DOC)
      .define(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, CLASS, null, MEDIUM, SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC)
      .define(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, STRING, null, MEDIUM, SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC)
      .define(SaslConfigs.SASL_KERBEROS_KINIT_CMD, STRING, SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD, MEDIUM, SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC)
      .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, MEDIUM, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC)
      .define(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, DOUBLE, SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER, MEDIUM, SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC)
      .define(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, LONG, SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN, MEDIUM, SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC)
      .define(BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG, LIST, BrokerSecurityConfigs.DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES, MEDIUM, BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC)
      .define(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, DOUBLE, SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR, MEDIUM, SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC)
      .define(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, DOUBLE, SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER, MEDIUM, SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC)
      .define(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, SHORT, SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS, MEDIUM, SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC)
      .define(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, SHORT, SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS, MEDIUM, SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC)
      .define(SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS, INT, null, LOW, SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC)
      .define(SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS, INT, null, LOW, SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS_DOC)
      .define(SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS, LONG, SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS, LOW, SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC)
      .define(SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS, LONG, SaslConfigs.DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS, LOW, SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DOC)
      .define(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, STRING, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, LOW, SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC)
      .define(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME, STRING, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME, LOW, SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC)
      .define(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, STRING, null, MEDIUM, SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC)
      .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, STRING, null, MEDIUM, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC)
      .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, LONG, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, LOW, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC)
      .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, LONG, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, LOW, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC)
      .define(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, LONG, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, LOW, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC)
      .define(SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, INT, SaslConfigs.DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, LOW, SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC)
      .define(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE, LIST, null, LOW, SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC)
      .define(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, STRING, null, LOW, SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC)

      /** ********* Delegation Token Configuration ****************/
      .define(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_ALIAS_CONFIG, PASSWORD, null, MEDIUM, DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_ALIAS_DOC)
      .define(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG, PASSWORD, null, MEDIUM, DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_DOC)
      .define(DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFETIME_CONFIG, LONG, DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFE_TIME_MS_DEFAULT, atLeast(1), MEDIUM, DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFE_TIME_DOC)
      .define(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_TIME_MS_CONFIG, LONG, DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_TIME_MS_DEFAULT, atLeast(1), MEDIUM, DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_TIME_MS_DOC)
      .define(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_CONFIG, LONG, DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_DEFAULT, atLeast(1), LOW, DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_DOC)

      /** ********* Password encryption configuration for dynamic configs *********/
      .define(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG, PASSWORD, null, MEDIUM, PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_DOC)
      .define(PasswordEncoderConfigs.PASSWORD_ENCODER_OLD_SECRET_CONFIG, PASSWORD, null, MEDIUM, PasswordEncoderConfigs.PASSWORD_ENCODER_OLD_SECRET_DOC)
      .define(PasswordEncoderConfigs.PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_CONFIG, STRING, null, LOW, PasswordEncoderConfigs.PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_DOC)
      .define(PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_CONFIG, STRING, PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_DEFAULT, LOW, PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_DOC)
      .define(PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_CONFIG, INT, PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_DEFAULT, atLeast(8), LOW, PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_DOC)
      .define(PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_CONFIG, INT, PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_DEFAULT, atLeast(1024), LOW, PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_DOC)

      /** ********* Raft Quorum Configuration *********/
      .define(QuorumConfig.QUORUM_VOTERS_CONFIG, LIST, QuorumConfig.DEFAULT_QUORUM_VOTERS, new QuorumConfig.ControllerQuorumVotersValidator(), HIGH, QuorumConfig.QUORUM_VOTERS_DOC)
      .defineInternal(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, LIST, QuorumConfig.DEFAULT_QUORUM_BOOTSTRAP_SERVERS, new QuorumConfig.ControllerQuorumBootstrapServersValidator(), HIGH, QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_DOC)
      .define(QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_ELECTION_TIMEOUT_MS, null, HIGH, QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_DOC)
      .define(QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_FETCH_TIMEOUT_MS, null, HIGH, QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_DOC)
      .define(QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_ELECTION_BACKOFF_MAX_MS, null, HIGH, QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_DOC)
      .define(QuorumConfig.QUORUM_LINGER_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_LINGER_MS, null, MEDIUM, QuorumConfig.QUORUM_LINGER_MS_DOC)
      .define(QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_REQUEST_TIMEOUT_MS, null, MEDIUM, QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_DOC)
      .define(QuorumConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG, INT, QuorumConfig.DEFAULT_QUORUM_RETRY_BACKOFF_MS, null, LOW, QuorumConfig.QUORUM_RETRY_BACKOFF_MS_DOC)

      /** Internal Configurations **/
      // This indicates whether unreleased APIs should be advertised by this node.
      .defineInternal(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, BOOLEAN, false, HIGH)
      // This indicates whether unreleased MetadataVersions or other feature versions should be enabled on this node.
      .defineInternal(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, BOOLEAN, false, HIGH)
  }

  /** ********* Remote Log Management Configuration *********/
  RemoteLogManagerConfig.configDef().configKeys().values().forEach(key => configDef.define(key))

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
    val brokerId = output.get(ServerConfigs.BROKER_ID_CONFIG)
    val nodeId = output.get(KRaftConfigs.NODE_ID_CONFIG)
    if (brokerId == null && nodeId != null) {
      output.put(ServerConfigs.BROKER_ID_CONFIG, nodeId)
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

  private val _remoteLogManagerConfig = new RemoteLogManagerConfig(props)
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
  val brokerIdGenerationEnable: Boolean = getBoolean(ServerConfigs.BROKER_ID_GENERATION_ENABLE_CONFIG)
  val maxReservedBrokerId: Int = getInt(ServerConfigs.RESERVED_BROKER_MAX_ID_CONFIG)
  var brokerId: Int = getInt(ServerConfigs.BROKER_ID_CONFIG)
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

  def numNetworkThreads = getInt(ServerConfigs.NUM_NETWORK_THREADS_CONFIG)
  def backgroundThreads = getInt(ServerConfigs.BACKGROUND_THREADS_CONFIG)
  val queuedMaxRequests = getInt(ServerConfigs.QUEUED_MAX_REQUESTS_CONFIG)
  val queuedMaxBytes = getLong(ServerConfigs.QUEUED_MAX_BYTES_CONFIG)
  def numIoThreads = getInt(ServerConfigs.NUM_IO_THREADS_CONFIG)
  def messageMaxBytes = getInt(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG)
  val requestTimeoutMs = getInt(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG)
  val connectionSetupTimeoutMs = getLong(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)
  val connectionSetupTimeoutMaxMs = getLong(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)

  def getNumReplicaAlterLogDirsThreads: Int = {
    val numThreads: Integer = Option(getInt(ServerConfigs.NUM_REPLICA_ALTER_LOG_DIRS_THREADS_CONFIG)).getOrElse(logDirs.size)
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
    val className = getString(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG)
    if (className == null || className.isEmpty)
      None
    else {
      Some(AuthorizerUtils.createAuthorizer(className))
    }
  }

  val earlyStartListeners: Set[ListenerName] = {
    val listenersSet = listeners.map(_.listenerName).toSet
    val controllerListenersSet = controllerListeners.map(_.listenerName).toSet
    Option(getString(ServerConfigs.EARLY_START_LISTENERS_CONFIG)) match {
      case None => controllerListenersSet
      case Some(str) =>
        str.split(",").map(_.trim()).filterNot(_.isEmpty).map { str =>
          val listenerName = new ListenerName(str)
          if (!listenersSet.contains(listenerName) && !controllerListenersSet.contains(listenerName))
            throw new ConfigException(s"${ServerConfigs.EARLY_START_LISTENERS_CONFIG} contains " +
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
  val rack = Option(getString(ServerConfigs.BROKER_RACK_CONFIG))
  val replicaSelectorClassName = Option(getString(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG))

  /** ********* Log Configuration ***********/
  val autoCreateTopicsEnable = getBoolean(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG)
  val numPartitions = getInt(ServerLogConfigs.NUM_PARTITIONS_CONFIG)
  val logDirs: Seq[String] = Csv.parseCsvList(Option(getString(ServerLogConfigs.LOG_DIRS_CONFIG)).getOrElse(getString(ServerLogConfigs.LOG_DIR_CONFIG))).asScala
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

  def logDirFailureTimeoutMs: Long = getLong(ServerLogConfigs.LOG_DIR_FAILURE_TIMEOUT_MS_CONFIG)

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
  val controlledShutdownMaxRetries = getInt(ServerConfigs.CONTROLLED_SHUTDOWN_MAX_RETRIES_CONFIG)
  val controlledShutdownRetryBackoffMs = getLong(ServerConfigs.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_CONFIG)
  val controlledShutdownEnable = getBoolean(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG)

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
      if (processRoles.isEmpty) {
        throw new ConfigException(s"The new '${GroupType.CONSUMER}' rebalance protocol is only supported in KRaft cluster.")
      }
      warn(s"The new '${GroupType.CONSUMER}' rebalance protocol is enabled along with the new group coordinator. " +
        "This is part of the preview of KIP-848 and MUST NOT be used in production.")
    }
    protocols
  }
  // The new group coordinator is enabled in two cases: 1) The internal configuration to enable
  // it is explicitly set; or 2) the consumer rebalance protocol is enabled.
  val isNewGroupCoordinatorEnabled = getBoolean(GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG) ||
    groupCoordinatorRebalanceProtocols.contains(GroupType.CONSUMER)
  val groupCoordinatorNumThreads = getInt(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG)
  val groupCoordinatorAppendLingerMs = getInt(GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG)

  /** Consumer group configs */
  val consumerGroupSessionTimeoutMs = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG)
  val consumerGroupMinSessionTimeoutMs = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG)
  val consumerGroupMaxSessionTimeoutMs = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG)
  val consumerGroupHeartbeatIntervalMs = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG)
  val consumerGroupMinHeartbeatIntervalMs = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG)
  val consumerGroupMaxHeartbeatIntervalMs = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG)
  val consumerGroupMaxSize = getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG)
  val consumerGroupAssignors = getConfiguredInstances(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, classOf[ConsumerGroupPartitionAssignor])
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
    val value = valuesWithPrefixOverride(listenerName.configPrefix).get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG)
    if (value != null)
      value.asInstanceOf[util.List[String]].asScala.toSet
    else
      Set.empty[String]
  }

  def interBrokerListenerName = getInterBrokerListenerNameAndSecurityProtocol._1
  def interBrokerSecurityProtocol = getInterBrokerListenerNameAndSecurityProtocol._2
  def controlPlaneListenerName = getControlPlaneListenerNameAndSecurityProtocol.map { case (listenerName, _) => listenerName }
  def controlPlaneSecurityProtocol = getControlPlaneListenerNameAndSecurityProtocol.map { case (_, securityProtocol) => securityProtocol }
  def saslMechanismInterBrokerProtocol = getString(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG)
  val saslInterBrokerHandshakeRequestEnable = interBrokerProtocolVersion.isSaslInterBrokerHandshakeRequestEnabled

  /** ********* DelegationToken Configuration **************/
  val delegationTokenSecretKey = Option(getPassword(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG))
    .getOrElse(getPassword(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_ALIAS_CONFIG))
  val tokenAuthEnabled = delegationTokenSecretKey != null && delegationTokenSecretKey.value.nonEmpty
  val delegationTokenMaxLifeMs = getLong(DelegationTokenManagerConfigs.DELEGATION_TOKEN_MAX_LIFETIME_CONFIG)
  val delegationTokenExpiryTimeMs = getLong(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_TIME_MS_CONFIG)
  val delegationTokenExpiryCheckIntervalMs = getLong(DelegationTokenManagerConfigs.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_CONFIG)

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
  val maxIncrementalFetchSessionCacheSlots = getInt(ServerConfigs.MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_CONFIG)
  val fetchMaxBytes = getInt(ServerConfigs.FETCH_MAX_BYTES_CONFIG)

  /** ********* Request Limit Configuration ***********/
  val maxRequestPartitionSizeLimit = getInt(ServerConfigs.MAX_REQUEST_PARTITION_SIZE_LIMIT_CONFIG)

  val deleteTopicEnable = getBoolean(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG)
  def compressionType = getString(ServerConfigs.COMPRESSION_TYPE_CONFIG)

  def gzipCompressionLevel = getInt(ServerConfigs.COMPRESSION_GZIP_LEVEL_CONFIG)
  def lz4CompressionLevel = getInt(ServerConfigs.COMPRESSION_LZ4_LEVEL_CONFIG)
  def zstdCompressionLevel = getInt(ServerConfigs.COMPRESSION_ZSTD_LEVEL_CONFIG)

  /** ********* Raft Quorum Configuration *********/
  val quorumVoters = getList(QuorumConfig.QUORUM_VOTERS_CONFIG)
  val quorumBootstrapServers = getList(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG)
  val quorumElectionTimeoutMs = getInt(QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG)
  val quorumFetchTimeoutMs = getInt(QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG)
  val quorumElectionBackoffMs = getInt(QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG)
  val quorumLingerMs = getInt(QuorumConfig.QUORUM_LINGER_MS_CONFIG)
  val quorumRequestTimeoutMs = getInt(QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG)
  val quorumRetryBackoffMs = getInt(QuorumConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG)

  /** Internal Configurations **/
  val unstableApiVersionsEnabled = getBoolean(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG)
  val unstableFeatureVersionsEnabled = getBoolean(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG)

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
        Csv.parseCsvList(getString(SocketServerConfigs.LISTENERS_CONFIG)).asScala.exists(listenerValue => isSslOrSasl(EndPoint.parseListenerName(listenerValue)))) {
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

  def logLocalRetentionBytes: java.lang.Long = getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP)

  def logLocalRetentionMs: java.lang.Long = getLong(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP)

  def remoteFetchMaxWaitMs = getInt(RemoteLogManagerConfig.REMOTE_FETCH_MAX_WAIT_MS_PROP)

  def remoteLogIndexFileCacheTotalSizeBytes: Long = getLong(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP)

  def remoteLogManagerCopyMaxBytesPerSecond: Long = getLong(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP)

  def remoteLogManagerFetchMaxBytesPerSecond: Long = getLong(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP)

  validateValues()

  @nowarn("cat=deprecation")
  private def validateValues(): Unit = {
    if (nodeId != brokerId) {
      throw new ConfigException(s"You must set `${KRaftConfigs.NODE_ID_CONFIG}` to the same value as `${ServerConfigs.BROKER_ID_CONFIG}`.")
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
      s"${BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG} must be included in ${BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG} when SASL is used for inter-broker communication")
    require(queuedMaxBytes <= 0 || queuedMaxBytes >= socketRequestMaxBytes,
      s"${ServerConfigs.QUEUED_MAX_BYTES_CONFIG} must be larger or equal to ${SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_CONFIG}")

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

    val principalBuilderClass = getClass(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG)
    require(principalBuilderClass != null, s"${BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG} must be non-null")
    require(classOf[KafkaPrincipalSerde].isAssignableFrom(principalBuilderClass),
      s"${BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG} must implement KafkaPrincipalSerde")

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
   * Validate some configurations for new MetadataVersion. A new MetadataVersion can take place when
   * a FeatureLevelRecord for "metadata.version" is read from the cluster metadata.
   */
  def validateWithMetadataVersion(metadataVersion: MetadataVersion): Unit = {
    if (processRoles.contains(ProcessRole.BrokerRole) && logDirs.size > 1) {
      require(metadataVersion.isDirectoryAssignmentSupported,
        s"Multiple log directories (aka JBOD) are not supported in the current MetadataVersion ${metadataVersion}. " +
          s"Need ${MetadataVersion.IBP_3_7_IV2} or higher")
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
    logProps.put(TopicConfig.COMPRESSION_GZIP_LEVEL_CONFIG, gzipCompressionLevel)
    logProps.put(TopicConfig.COMPRESSION_LZ4_LEVEL_CONFIG, lz4CompressionLevel)
    logProps.put(TopicConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, zstdCompressionLevel)
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
