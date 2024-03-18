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
import java.util.Properties
import kafka.cluster.EndPoint
import kafka.utils.CoreUtils.parseCsvList
import kafka.utils.{CoreUtils, Logging}
import kafka.utils.Implicits._
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.{AbstractConfig, ConfigException, SaslConfigs, TopicConfig}
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.{CompressionType, TimestampType}
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.group.Group.GroupType
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.security.authorizer.AuthorizerUtils
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.config.KafkaConfig.populateSynonyms
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.util.Csv
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig}
import org.apache.kafka.storage.internals.log.LogConfig.MessageFormatVersion
import org.apache.zookeeper.client.ZKClientConfig

import scala.annotation.nowarn
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq}

object KafkaConfig {
  import org.apache.kafka.server.config.KafkaConfig._

  def main(args: Array[String]): Unit = {
    System.out.println(CONFIG_DEF.toHtml(4, (config: String) => "brokerconfigs_" + config,
      DynamicBrokerConfig.dynamicConfigUpdateModes))
  }

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
}

class KafkaConfig private(doLog: Boolean, val props: java.util.Map[_, _], dynamicConfigOverride: Option[DynamicBrokerConfig])
  extends AbstractConfig(org.apache.kafka.server.config.KafkaConfig.CONFIG_DEF, props, doLog) with Logging {

  import org.apache.kafka.server.config.KafkaConfig._
  def this(props: java.util.Map[_, _]) = this(true, populateSynonyms(props), None)
  def this(props: java.util.Map[_, _], doLog: Boolean) = this(doLog, populateSynonyms(props), None)
  def this(props: java.util.Map[_, _], doLog: Boolean, dynamicConfigOverride: Option[DynamicBrokerConfig]) =
    this(doLog, populateSynonyms(props), dynamicConfigOverride)

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
  val zkConnect: String = getString(ZK_CONNECT_PROP)
  val zkSessionTimeoutMs: Int = getInt(ZK_SESSION_TIMEOUT_MS_PROP)
  val zkConnectionTimeoutMs: Int =
    Option(getInt(ZK_CONNECTION_TIMEOUT_MS_PROP)).map(_.toInt).getOrElse(getInt(ZK_SESSION_TIMEOUT_MS_PROP))
  val zkEnableSecureAcls: Boolean = getBoolean(ZK_ENABLE_SECURE_ACLS_PROP)
  val zkMaxInFlightRequests: Int = getInt(ZK_MAX_IN_FLIGHT_REQUESTS_PROP)

  private val _remoteLogManagerConfig = new RemoteLogManagerConfig(this)
  def remoteLogManagerConfig = _remoteLogManagerConfig

  private def zkBooleanConfigOrSystemPropertyWithDefaultValue(propKey: String): Boolean = {
    // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
    // Need to translate any system property value from true/false (String) to true/false (Boolean)
    val actuallyProvided = originals.containsKey(propKey)
    if (actuallyProvided) getBoolean(propKey) else {
      val sysPropValue = zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).asScala
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
      zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).asScala match {
        case Some(v) => v
        case _ => getString(propKey) // not specified so use the default value
      }
    }
  }

  private def zkOptionalStringConfigOrSystemProperty(propKey: String): Option[String] = {
    Option(getString(propKey)).orElse {
      zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).asScala
    }
  }
  private def zkPasswordConfigOrSystemProperty(propKey: String): Option[Password] = {
    Option(getPassword(propKey)).orElse {
      zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).map(new Password(_)).asScala
    }
  }
  private def zkListConfigOrSystemProperty(propKey: String): Option[util.List[String]] = {
    Option(getList(propKey)).orElse {
      zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).asScala.map { sysProp =>
        sysProp.split("\\s*,\\s*").toBuffer.asJava
      }
    }
  }

  val zkSslClientEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(ZK_SSL_CLIENT_ENABLE_PROP)
  val zkClientCnxnSocketClassName = zkOptionalStringConfigOrSystemProperty(ZK_CLIENT_CNXN_SOCKET_PROP)
  val zkSslKeyStoreLocation = zkOptionalStringConfigOrSystemProperty(ZK_SSL_KEYSTORE_LOCATION_PROP)
  val zkSslKeyStorePassword = zkPasswordConfigOrSystemProperty(ZK_SSL_KEYSTORE_PASSWORD_PROP)
  val zkSslKeyStoreType = zkOptionalStringConfigOrSystemProperty(ZK_SSL_KEYSTORE_TYPE_PROP)
  val zkSslTrustStoreLocation = zkOptionalStringConfigOrSystemProperty(ZK_SSL_TRUSTSTORE_LOCATION_PROP)
  val zkSslTrustStorePassword = zkPasswordConfigOrSystemProperty(ZK_SSL_TRUSTSTORE_PASSWORD_PROP)
  val zkSslTrustStoreType = zkOptionalStringConfigOrSystemProperty(ZK_SSL_TRUSTSTORE_TYPE_PROP)
  val ZkSslProtocol = zkStringConfigOrSystemPropertyWithDefaultValue(ZK_SSL_PROTOCOL_PROP)
  val ZkSslEnabledProtocols = zkListConfigOrSystemProperty(ZK_SSL_ENABLED_PROTOCOLS_PROP)
  val ZkSslCipherSuites = zkListConfigOrSystemProperty(ZK_SSL_CIPHER_SUITES_PROP)
  val ZkSslEndpointIdentificationAlgorithm = {
    // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
    // Need to translate any system property value from true/false to HTTPS/<blank>
    val kafkaProp = ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP
    val actuallyProvided = originals.containsKey(kafkaProp)
    if (actuallyProvided)
      getString(kafkaProp)
    else {
      zooKeeperClientProperty(zkClientConfigViaSystemProperties, kafkaProp).asScala match {
        case Some("true") => "HTTPS"
        case Some(_) => ""
        case None => getString(kafkaProp) // not specified so use the default value
      }
    }
  }
  val ZkSslCrlEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(ZK_SSL_CRL_ENABLE_PROP)
  val ZkSslOcspEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(ZK_SSL_OCSP_ENABLE_PROP)
  /** ********* General Configuration ***********/
  val brokerIdGenerationEnable: Boolean = getBoolean(BROKER_ID_GENERATION_ENABLE_PROP)
  val maxReservedBrokerId: Int = getInt(MAX_RESERVED_BROKER_ID_PROP)
  var brokerId: Int = getInt(BROKER_ID_PROP)
  val nodeId: Int = getInt(NODE_ID_PROP)
  val initialRegistrationTimeoutMs: Int = getInt(INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_PROP)
  val brokerHeartbeatIntervalMs: Int = getInt(BROKER_HEARTBEAT_INTERVAL_MS_PROP)
  val brokerSessionTimeoutMs: Int = getInt(BROKER_SESSION_TIMEOUT_MS_PROP)

  def requiresZookeeper: Boolean = processRoles.isEmpty
  def usesSelfManagedQuorum: Boolean = processRoles.nonEmpty

  val migrationEnabled: Boolean = getBoolean(MIGRATION_ENABLED_PROP)
  val migrationMetadataMinBatchSize: Int = getInt(MIGRATION_METADATA_MIN_BATCH_SIZE_PROP)

  val elrEnabled: Boolean = getBoolean(ELR_ENABLED_PROP)

  private def parseProcessRoles(): Set[ProcessRole] = {
    val roles = getList(PROCESS_ROLES_PROP).asScala.map {
      case "broker" => ProcessRole.BrokerRole
      case "controller" => ProcessRole.ControllerRole
      case role => throw new ConfigException(s"Unknown process role '$role'" +
        " (only 'broker' and 'controller' are allowed roles)")
    }

    val distinctRoles: Set[ProcessRole] = roles.toSet

    if (distinctRoles.size != roles.size) {
      throw new ConfigException(s"Duplicate role names found in `${PROCESS_ROLES_PROP}`: $roles")
    }

    distinctRoles
  }

  def isKRaftCombinedMode: Boolean = {
    processRoles == Set(ProcessRole.BrokerRole, ProcessRole.ControllerRole)
  }

  def metadataLogDir: String = {
    Option(getString(METADATA_LOG_DIR_PROP)) match {
      case Some(dir) => dir
      case None => logDirs.head
    }
  }

  def metadataLogSegmentBytes = getInt(METADATA_LOG_SEGMENT_BYTES_PROP)
  def metadataLogSegmentMillis = getLong(METADATA_LOG_SEGMENT_MILLIS_PROP)
  def metadataRetentionBytes = getLong(METADATA_MAX_RETENTION_BYTES_PROP)
  def metadataRetentionMillis = getLong(METADATA_MAX_RETENTION_MILLIS_PROP)
  val serverMaxStartupTimeMs = getLong(SERVER_MAX_STARTUP_TIME_MS_PROP)

  def numNetworkThreads = getInt(NUM_NETWORK_THREADS_PROP)
  def backgroundThreads = getInt(BACKGROUND_THREADS_PROP)
  val queuedMaxRequests = getInt(QUEUED_MAX_REQUESTS_PROP)
  val queuedMaxBytes = getLong(QUEUED_MAX_BYTES_PROP)
  def numIoThreads = getInt(NUM_IO_THREADS_PROP)
  def messageMaxBytes = getInt(MESSAGE_MAX_BYTES_PROP)
  val requestTimeoutMs = getInt(REQUEST_TIMEOUT_MS_PROP)
  val connectionSetupTimeoutMs = getLong(CONNECTION_SETUP_TIMEOUT_MS_PROP)
  val connectionSetupTimeoutMaxMs = getLong(CONNECTION_SETUP_TIMEOUT_MAX_MS_PROP)

  def getNumReplicaAlterLogDirsThreads: Int = {
    val numThreads: Integer = Option(getInt(NUM_REPLICA_ALTER_LOG_DIRS_THREADS_PROP)).getOrElse(logDirs.size)
    numThreads
  }

  /************* Metadata Configuration ***********/
  val metadataSnapshotMaxNewRecordBytes = getLong(METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_PROP)
  val metadataSnapshotMaxIntervalMs = getLong(METADATA_SNAPSHOT_MAX_INTERVAL_MS_PROP)
  val metadataMaxIdleIntervalNs: Option[Long] = {
    val value = TimeUnit.NANOSECONDS.convert(getInt(METADATA_MAX_IDLE_INTERVAL_MS_PROP).toLong, TimeUnit.MILLISECONDS)
    if (value > 0) Some(value) else None
  }

  /************* Authorizer Configuration ***********/
  def createNewAuthorizer(): Option[Authorizer] = {
    val className = getString(AUTHORIZER_CLASS_NAME_PROP)
    if (className == null || className.isEmpty)
      None
    else {
      Some(AuthorizerUtils.createAuthorizer(className))
    }
  }

  val earlyStartListeners: Set[ListenerName] = {
    val listenersSet = listeners.map(_.listenerName).toSet
    val controllerListenersSet = controllerListeners.map(_.listenerName).toSet
    Option(getString(EARLY_START_LISTENERS_PROP)) match {
      case None => controllerListenersSet
      case Some(str) =>
        str.split(",").map(_.trim()).filterNot(_.isEmpty).map { str =>
          val listenerName = new ListenerName(str)
          if (!listenersSet.contains(listenerName) && !controllerListenersSet.contains(listenerName))
            throw new ConfigException(s"${EARLY_START_LISTENERS_PROP} contains " +
              s"listener ${listenerName.value()}, but this is not contained in " +
              s"${LISTENERS_PROP} or ${CONTROLLER_LISTENER_NAMES_PROP}")
          listenerName
        }.toSet
    }
  }

  /** ********* Socket Server Configuration ***********/
  val socketSendBufferBytes = getInt(SOCKET_SEND_BUFFER_BYTES_PROP)
  val socketReceiveBufferBytes = getInt(SOCKET_RECEIVE_BUFFER_BYTES_PROP)
  val socketRequestMaxBytes = getInt(SOCKET_REQUEST_MAX_BYTES_PROP)
  val socketListenBacklogSize = getInt(SOCKET_LISTEN_BACKLOG_SIZE_PROP)
  val maxConnectionsPerIp = getInt(MAX_CONNECTIONS_PER_IP_PROP)
  val maxConnectionsPerIpOverrides: Map[String, Int] =
    getMap(MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP, getString(MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP)).map { case (k, v) => (k, v.toInt)}
  def maxConnections = getInt(MAX_CONNECTIONS_PROP)
  def maxConnectionCreationRate = getInt(MAX_CONNECTION_CREATION_RATE_PROP)
  val connectionsMaxIdleMs = getLong(CONNECTIONS_MAX_IDLE_MS_PROP)
  val failedAuthenticationDelayMs = getInt(FAILED_AUTHENTICATION_DELAY_MS_PROP)

  /***************** rack configuration **************/
  val rack = Option(getString(RACK_PROP))
  val replicaSelectorClassName = Option(getString(REPLICA_SELECTOR_CLASS_PROP))

  /** ********* Log Configuration ***********/
  val autoCreateTopicsEnable = getBoolean(AUTO_CREATE_TOPICS_ENABLE_PROP)
  val numPartitions = getInt(NUM_PARTITIONS_PROP)
  val logDirs = CoreUtils.parseCsvList(Option(getString(LOG_DIRS_PROP)).getOrElse(getString(LOG_DIR_PROP)))
  def logSegmentBytes = getInt(LOG_SEGMENT_BYTES_PROP)
  def logFlushIntervalMessages = getLong(LOG_FLUSH_INTERVAL_MESSAGES_PROP)
  val logCleanerThreads = getInt(CleanerConfig.LOG_CLEANER_THREADS_PROP)
  def numRecoveryThreadsPerDataDir = getInt(NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP)
  val logFlushSchedulerIntervalMs = getLong(LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP)
  val logFlushOffsetCheckpointIntervalMs = getInt(LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_PROP).toLong
  val logFlushStartOffsetCheckpointIntervalMs = getInt(LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP).toLong
  val logCleanupIntervalMs = getLong(LOG_CLEANUP_INTERVAL_MS_PROP)
  def logCleanupPolicy = getList(LOG_CLEANUP_POLICY_PROP)
  val offsetsRetentionMinutes = getInt(OFFSETS_RETENTION_MINUTES_PROP)
  val offsetsRetentionCheckIntervalMs = getLong(OFFSETS_RETENTION_CHECK_INTERVAL_MS_PROP)
  def logRetentionBytes = getLong(LOG_RETENTION_BYTES_PROP)
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
  def logIndexSizeMaxBytes = getInt(LOG_INDEX_SIZE_MAX_BYTES_PROP)
  def logIndexIntervalBytes = getInt(LOG_INDEX_INTERVAL_BYTES_PROP)
  def logDeleteDelayMs = getLong(LOG_DELETE_DELAY_MS_PROP)
  def logRollTimeMillis: java.lang.Long = Option(getLong(LOG_ROLL_TIME_MILLIS_PROP)).getOrElse(60 * 60 * 1000L * getInt(LOG_ROLL_TIME_HOURS_PROP))
  def logRollTimeJitterMillis: java.lang.Long = Option(getLong(LOG_ROLL_TIME_JITTER_MILLIS_PROP)).getOrElse(60 * 60 * 1000L * getInt(LOG_ROLL_TIME_JITTER_HOURS_PROP))
  def logFlushIntervalMs: java.lang.Long = Option(getLong(LOG_FLUSH_INTERVAL_MS_PROP)).getOrElse(getLong(LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP))
  def minInSyncReplicas = getInt(MIN_IN_SYNC_REPLICAS_PROP)
  def logPreAllocateEnable: java.lang.Boolean = getBoolean(LOG_PRE_ALLOCATE_PROP)

  // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version (eg if `0.10.0`
  // is passed, `0.10.0-IV0` may be picked)
  @nowarn("cat=deprecation")
  private val logMessageFormatVersionString = getString(LOG_MESSAGE_FORMAT_VERSION_PROP)

  /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
  @deprecated("3.0")
  lazy val logMessageFormatVersion =
    if (LogConfig.shouldIgnoreMessageFormatVersion(interBrokerProtocolVersion))
      MetadataVersion.fromVersionString(LogConfig.DEFAULT_MESSAGE_FORMAT_VERSION)
    else MetadataVersion.fromVersionString(logMessageFormatVersionString)

  def logMessageTimestampType = TimestampType.forName(getString(LOG_MESSAGE_TIMESTAMP_TYPE_PROP))

  /* See `TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG` for details */
  @deprecated("3.6")
  def logMessageTimestampDifferenceMaxMs: Long = getLong(LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP)

  // In the transition period before logMessageTimestampDifferenceMaxMs is removed, to maintain backward compatibility,
  // we are using its value if logMessageTimestampBeforeMaxMs default value hasn't changed.
  // See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for deprecation details
  @nowarn("cat=deprecation")
  def logMessageTimestampBeforeMaxMs: Long = {
    val messageTimestampBeforeMaxMs: Long = getLong(LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_PROP)
    if (messageTimestampBeforeMaxMs != LogConfig.DEFAULT_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS) {
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
    val messageTimestampAfterMaxMs: Long = getLong(LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_PROP)
    if (messageTimestampAfterMaxMs != Long.MaxValue) {
      messageTimestampAfterMaxMs
    } else {
      logMessageTimestampDifferenceMaxMs
    }
  }

  def logMessageDownConversionEnable: Boolean = getBoolean(LOG_MESSAGE_DOWN_CONVERSION_ENABLE_PROP)

  /** ********* Replication configuration ***********/
  val controllerSocketTimeoutMs: Int = getInt(CONTROLLER_SOCKET_TIMEOUT_MS_PROP)
  val defaultReplicationFactor: Int = getInt(DEFAULT_REPLICATION_FACTOR_PROP)
  val replicaLagTimeMaxMs = getLong(REPLICA_LAG_TIME_MAX_MS_PROP)
  val replicaSocketTimeoutMs = getInt(REPLICA_SOCKET_TIMEOUT_MS_PROP)
  val replicaSocketReceiveBufferBytes = getInt(REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_PROP)
  val replicaFetchMaxBytes = getInt(REPLICA_FETCH_MAX_BYTES_PROP)
  val replicaFetchWaitMaxMs = getInt(REPLICA_FETCH_WAIT_MAX_MS_PROP)
  val replicaFetchMinBytes = getInt(REPLICA_FETCH_MIN_BYTES_PROP)
  val replicaFetchResponseMaxBytes = getInt(REPLICA_FETCH_RESPONSE_MAX_BYTES_PROP)
  val replicaFetchBackoffMs = getInt(REPLICA_FETCH_BACKOFF_MS_PROP)
  def numReplicaFetchers = getInt(NUM_REPLICA_FETCHERS_PROP)
  val replicaHighWatermarkCheckpointIntervalMs = getLong(REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_PROP)
  val fetchPurgatoryPurgeIntervalRequests = getInt(FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP)
  val producerPurgatoryPurgeIntervalRequests = getInt(PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP)
  val deleteRecordsPurgatoryPurgeIntervalRequests = getInt(DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP)
  val autoLeaderRebalanceEnable = getBoolean(AUTO_LEADER_REBALANCE_ENABLE_PROP)
  val leaderImbalancePerBrokerPercentage = getInt(LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_PROP)
  val leaderImbalanceCheckIntervalSeconds: Long = getLong(LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_PROP)
  def uncleanLeaderElectionEnable: java.lang.Boolean = getBoolean(UNCLEAN_LEADER_ELECTION_ENABLE_PROP)

  // We keep the user-provided String as `MetadataVersion.fromVersionString` can choose a slightly different version (eg if `0.10.0`
  // is passed, `0.10.0-IV0` may be picked)
  val interBrokerProtocolVersionString = getString(INTER_BROKER_PROTOCOL_VERSION_PROP)
  val interBrokerProtocolVersion = if (processRoles.isEmpty) {
    MetadataVersion.fromVersionString(interBrokerProtocolVersionString)
  } else {
    if (originals.containsKey(INTER_BROKER_PROTOCOL_VERSION_PROP)) {
      // A user-supplied IBP was given
      val configuredVersion = MetadataVersion.fromVersionString(interBrokerProtocolVersionString)
      if (!configuredVersion.isKRaftSupported) {
        throw new ConfigException(s"A non-KRaft version $interBrokerProtocolVersionString given for ${INTER_BROKER_PROTOCOL_VERSION_PROP}. " +
          s"The minimum version is ${MetadataVersion.MINIMUM_KRAFT_VERSION}")
      } else {
        warn(s"${INTER_BROKER_PROTOCOL_VERSION_PROP} is deprecated in KRaft mode as of 3.3 and will only " +
          s"be read when first upgrading from a KRaft prior to 3.3. See kafka-storage.sh help for details on setting " +
          s"the metadata version for a new KRaft cluster.")
      }
    }
    // In KRaft mode, we pin this value to the minimum KRaft-supported version. This prevents inadvertent usage of
    // the static IBP config in broker components running in KRaft mode
    MetadataVersion.MINIMUM_KRAFT_VERSION
  }

  /** ********* Controlled shutdown configuration ***********/
  val controlledShutdownMaxRetries = getInt(CONTROLLED_SHUTDOWN_MAX_RETRIES_PROP)
  val controlledShutdownRetryBackoffMs = getLong(CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_PROP)
  val controlledShutdownEnable = getBoolean(CONTROLLED_SHUTDOWN_ENABLE_PROP)

  /** ********* Feature configuration ***********/
  def isFeatureVersioningSupported = interBrokerProtocolVersion.isFeatureVersioningSupported

  /** ********* Group coordinator configuration ***********/
  val groupMinSessionTimeoutMs = getInt(GROUP_MIN_SESSION_TIMEOUT_MS_PROP)
  val groupMaxSessionTimeoutMs = getInt(GROUP_MAX_SESSION_TIMEOUT_MS_PROP)
  val groupInitialRebalanceDelay = getInt(GROUP_INITIAL_REBALANCE_DELAY_MS_PROP)
  val groupMaxSize = getInt(GROUP_MAX_SIZE_PROP)

  /** New group coordinator configs */
  val groupCoordinatorRebalanceProtocols = {
    val protocols = getList(GROUP_COORDINATOR_REBALANCE_PROTOCOLS_PROP)
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
  val isNewGroupCoordinatorEnabled = getBoolean(NEW_GROUP_COORDINATOR_ENABLE_PROP) ||
    groupCoordinatorRebalanceProtocols.contains(GroupType.CONSUMER)
  val groupCoordinatorNumThreads = getInt(GROUP_COORDINATOR_NUM_THREADS_PROP)

  /** Consumer group configs */
  val consumerGroupSessionTimeoutMs = getInt(CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP)
  val consumerGroupMinSessionTimeoutMs = getInt(CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP)
  val consumerGroupMaxSessionTimeoutMs = getInt(CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP)
  val consumerGroupHeartbeatIntervalMs = getInt(CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP)
  val consumerGroupMinHeartbeatIntervalMs = getInt(CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP)
  val consumerGroupMaxHeartbeatIntervalMs = getInt(CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP)
  val consumerGroupMaxSize = getInt(CONSUMER_GROUP_MAX_SIZE_PROP)
  val consumerGroupAssignors = getConfiguredInstances(CONSUMER_GROUP_ASSIGNORS_PROP, classOf[PartitionAssignor])

  /** ********* Offset management configuration ***********/
  val offsetMetadataMaxSize = getInt(OFFSET_METADATA_MAX_SIZE_PROP)
  val offsetsLoadBufferSize = getInt(OFFSETS_LOAD_BUFFER_SIZE_PROP)
  val offsetsTopicReplicationFactor = getShort(OFFSETS_TOPIC_REPLICATION_FACTOR_PROP)
  val offsetsTopicPartitions = getInt(OFFSETS_TOPIC_PARTITIONS_PROP)
  val offsetCommitTimeoutMs = getInt(OFFSET_COMMIT_TIMEOUT_MS_PROP)
  val offsetCommitRequiredAcks = getShort(OFFSET_COMMIT_REQUIRED_ACKS_PROP)
  val offsetsTopicSegmentBytes = getInt(OFFSETS_TOPIC_SEGMENT_BYTES_PROP)
  val offsetsTopicCompressionType = Option(getInt(OFFSETS_TOPIC_COMPRESSION_CODEC_PROP)).map(value => CompressionType.forId(value)).orNull

  /** ********* Transaction management configuration ***********/
  val transactionalIdExpirationMs = getInt(TRANSACTIONAL_ID_EXPIRATION_MS_PROP)
  val transactionMaxTimeoutMs = getInt(TRANSACTIONS_MAX_TIMEOUT_MS_PROP)
  val transactionTopicMinISR = getInt(TRANSACTIONS_TOPIC_MIN_ISR_PROP)
  val transactionsLoadBufferSize = getInt(TRANSACTIONS_LOAD_BUFFER_SIZE_PROP)
  val transactionTopicReplicationFactor = getShort(TRANSACTIONS_TOPIC_REPLICATION_FACTOR_PROP)
  val transactionTopicPartitions = getInt(TRANSACTIONS_TOPIC_PARTITIONS_PROP)
  val transactionTopicSegmentBytes = getInt(TRANSACTIONS_TOPIC_SEGMENT_BYTES_PROP)
  val transactionAbortTimedOutTransactionCleanupIntervalMs = getInt(TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_PROP)
  val transactionRemoveExpiredTransactionalIdCleanupIntervalMs = getInt(TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_PROP)

  def transactionPartitionVerificationEnable = getBoolean(TRANSACTION_PARTITION_VERIFICATION_ENABLE_PROP)

  def producerIdExpirationMs = getInt(PRODUCER_ID_EXPIRATION_MS_PROP)
  val producerIdExpirationCheckIntervalMs = getInt(PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_PROP)

  /** ********* Metric Configuration **************/
  val metricNumSamples = getInt(METRIC_NUM_SAMPLES_PROP)
  val metricSampleWindowMs = getLong(METRIC_SAMPLE_WINDOW_MS_PROP)
  val metricRecordingLevel = getString(METRIC_RECORDING_LEVEL_PROP)

  /** ********* Kafka Client Telemetry Metrics Configuration ***********/
  val clientTelemetryMaxBytes: Int = getInt(CLIENT_TELEMETRY_MAX_BYTES_PROP)

  /** ********* SSL/SASL Configuration **************/
  // Security configs may be overridden for listeners, so it is not safe to use the base values
  // Hence the base SSL/SASL configs are not fields of KafkaConfig, listener configs should be
  // retrieved using KafkaConfig#valuesWithPrefixOverride
  private def saslEnabledMechanisms(listenerName: ListenerName): Set[String] = {
    val value = valuesWithPrefixOverride(listenerName.configPrefix).get(SASL_ENABLED_MECHANISMS_PROP)
    if (value != null)
      value.asInstanceOf[util.List[String]].asScala.toSet
    else
      Set.empty[String]
  }

  def interBrokerListenerName = getInterBrokerListenerNameAndSecurityProtocol._1
  def interBrokerSecurityProtocol = getInterBrokerListenerNameAndSecurityProtocol._2
  def controlPlaneListenerName = getControlPlaneListenerNameAndSecurityProtocol.map { case (listenerName, _) => listenerName }
  def controlPlaneSecurityProtocol = getControlPlaneListenerNameAndSecurityProtocol.map { case (_, securityProtocol) => securityProtocol }
  def saslMechanismInterBrokerProtocol = getString(SASL_MECHANISM_INTER_BROKER_PROTOCOL_PROP)
  val saslInterBrokerHandshakeRequestEnable = interBrokerProtocolVersion.isSaslInterBrokerHandshakeRequestEnabled

  /** ********* DelegationToken Configuration **************/
  val delegationTokenSecretKey = Option(getPassword(DELEGATION_TOKEN_SECRET_KEY_PROP))
    .getOrElse(getPassword(DELEGATION_TOKEN_SECRET_KEY_ALIAS_PROP))
  val tokenAuthEnabled = delegationTokenSecretKey != null && delegationTokenSecretKey.value.nonEmpty
  val delegationTokenMaxLifeMs = getLong(DELEGATION_TOKEN_MAX_LIFE_TIME_PROP)
  val delegationTokenExpiryTimeMs = getLong(DELEGATION_TOKEN_EXPIRY_TIME_MS_PROP)
  val delegationTokenExpiryCheckIntervalMs = getLong(DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_PROP)

  /** ********* Password encryption configuration for dynamic configs *********/
  def passwordEncoderSecret = Option(getPassword(PASSWORD_ENCODER_SECRET_PROP))
  def passwordEncoderOldSecret = Option(getPassword(PASSWORD_ENCODER_OLD_SECRET_PROP))
  def passwordEncoderCipherAlgorithm = getString(PASSWORD_ENCODER_CIPHER_ALGORITHM_PROP)
  def passwordEncoderKeyFactoryAlgorithm = getString(PASSWORD_ENCODER_KEY_FACTORY_ALGORITHM_PROP)
  def passwordEncoderKeyLength = getInt(PASSWORD_ENCODER_KEY_LENGTH_PROP)
  def passwordEncoderIterations = getInt(PASSWORD_ENCODER_ITERATIONS_PROP)

  /** ********* Quota Configuration **************/
  val numQuotaSamples = getInt(NUM_QUOTA_SAMPLES_PROP)
  val quotaWindowSizeSeconds = getInt(QUOTA_WINDOW_SIZE_SECONDS_PROP)
  val numReplicationQuotaSamples = getInt(NUM_REPLICATION_QUOTA_SAMPLES_PROP)
  val replicationQuotaWindowSizeSeconds = getInt(REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_PROP)
  val numAlterLogDirsReplicationQuotaSamples = getInt(NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_PROP)
  val alterLogDirsReplicationQuotaWindowSizeSeconds = getInt(ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_PROP)
  val numControllerQuotaSamples = getInt(NUM_CONTROLLER_QUOTA_SAMPLES_PROP)
  val controllerQuotaWindowSizeSeconds = getInt(CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_PROP)

  /** ********* Fetch Configuration **************/
  val maxIncrementalFetchSessionCacheSlots = getInt(MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS_PROP)
  val fetchMaxBytes = getInt(FETCH_MAX_BYTES_PROP)

  /** ********* Request Limit Configuration ***********/
  val maxRequestPartitionSizeLimit = getInt(MAX_REQUEST_PARTITION_SIZE_LIMIT_PROP)

  val deleteTopicEnable = getBoolean(DELETE_TOPIC_ENABLE_PROP)
  def compressionType = getString(COMPRESSION_TYPE_PROP)

  /** ********* Raft Quorum Configuration *********/
  val quorumVoters = getList(RaftConfig.QUORUM_VOTERS_CONFIG)
  val quorumElectionTimeoutMs = getInt(RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG)
  val quorumFetchTimeoutMs = getInt(RaftConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG)
  val quorumElectionBackoffMs = getInt(RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG)
  val quorumLingerMs = getInt(RaftConfig.QUORUM_LINGER_MS_CONFIG)
  val quorumRequestTimeoutMs = getInt(RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG)
  val quorumRetryBackoffMs = getInt(RaftConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG)

  /** Internal Configurations **/
  val unstableApiVersionsEnabled = getBoolean(UNSTABLE_API_VERSIONS_ENABLE_PROP)
  val unstableMetadataVersionsEnabled = getBoolean(UNSTABLE_METADATA_VERSIONS_ENABLE_PROP)

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
      Option(getLong(LOG_RETENTION_TIME_MILLIS_PROP)).getOrElse(
        Option(getInt(LOG_RETENTION_TIME_MINUTES_PROP)) match {
          case Some(mins) => millisInMinute * mins
          case None => getInt(LOG_RETENTION_TIME_HOURS_PROP) * millisInHour
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
    CoreUtils.listenerListToEndPoints(getString(LISTENERS_PROP), effectiveListenerSecurityProtocolMap)

  def controllerListenerNames: Seq[String] = {
    val value = Option(getString(CONTROLLER_LISTENER_NAMES_PROP)).getOrElse("")
    if (value.isEmpty) {
      Seq.empty
    } else {
      value.split(",")
    }
  }

  def controllerListeners: Seq[EndPoint] =
    listeners.filter(l => controllerListenerNames.contains(l.listenerName.value()))

  def saslMechanismControllerProtocol: String = getString(SASL_MECHANISM_CONTROLLER_PROTOCOL_PROP)

  def controlPlaneListener: Option[EndPoint] = {
    controlPlaneListenerName.map { listenerName =>
      listeners.filter(endpoint => endpoint.listenerName.value() == listenerName.value()).head
    }
  }

  def dataPlaneListeners: Seq[EndPoint] = {
    listeners.filterNot { listener =>
      val name = listener.listenerName.value()
      name.equals(getString(CONTROL_PLANE_LISTENER_NAME_PROP)) ||
        controllerListenerNames.contains(name)
    }
  }

  // Use advertised listeners if defined, fallback to listeners otherwise
  def effectiveAdvertisedListeners: Seq[EndPoint] = {
    val advertisedListenersProp = getString(ADVERTISED_LISTENERS_PROP)
    if (advertisedListenersProp != null)
      CoreUtils.listenerListToEndPoints(advertisedListenersProp, effectiveListenerSecurityProtocolMap, requireDistinctPorts=false)
    else
      listeners.filterNot(l => controllerListenerNames.contains(l.listenerName.value()))
  }

  private def getInterBrokerListenerNameAndSecurityProtocol: (ListenerName, SecurityProtocol) = {
    Option(getString(INTER_BROKER_LISTENER_NAME_PROP)) match {
      case Some(_) if originals.containsKey(INTER_BROKER_SECURITY_PROTOCOL_PROP) =>
        throw new ConfigException(s"Only one of ${INTER_BROKER_LISTENER_NAME_PROP} and " +
          s"${INTER_BROKER_SECURITY_PROTOCOL_PROP} should be set.")
      case Some(name) =>
        val listenerName = ListenerName.normalised(name)
        val securityProtocol = effectiveListenerSecurityProtocolMap.getOrElse(listenerName,
          throw new ConfigException(s"Listener with name ${listenerName.value} defined in " +
            s"${INTER_BROKER_LISTENER_NAME_PROP} not found in ${LISTENER_SECURITY_PROTOCOL_MAP_PROP}."))
        (listenerName, securityProtocol)
      case None =>
        val securityProtocol = getSecurityProtocol(getString(INTER_BROKER_SECURITY_PROTOCOL_PROP),
          INTER_BROKER_SECURITY_PROTOCOL_PROP)
        (ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
    }
  }

  private def getControlPlaneListenerNameAndSecurityProtocol: Option[(ListenerName, SecurityProtocol)] = {
    Option(getString(CONTROL_PLANE_LISTENER_NAME_PROP)) match {
      case Some(name) =>
        val listenerName = ListenerName.normalised(name)
        val securityProtocol = effectiveListenerSecurityProtocolMap.getOrElse(listenerName,
          throw new ConfigException(s"Listener with ${listenerName.value} defined in " +
            s"${CONTROL_PLANE_LISTENER_NAME_PROP} not found in ${LISTENER_SECURITY_PROTOCOL_MAP_PROP}."))
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
    val mapValue = getMap(LISTENER_SECURITY_PROTOCOL_MAP_PROP, getString(LISTENER_SECURITY_PROTOCOL_MAP_PROP))
      .map { case (listenerName, protocolName) =>
        ListenerName.normalised(listenerName) -> getSecurityProtocol(protocolName, LISTENER_SECURITY_PROTOCOL_MAP_PROP)
      }
    if (usesSelfManagedQuorum && !originals.containsKey(LISTENER_SECURITY_PROTOCOL_MAP_PROP)) {
      // Nothing was specified explicitly for listener.security.protocol.map, so we are using the default value,
      // and we are using KRaft.
      // Add PLAINTEXT mappings for controller listeners as long as there is no SSL or SASL_{PLAINTEXT,SSL} in use
      def isSslOrSasl(name: String): Boolean = name.equals(SecurityProtocol.SSL.name) || name.equals(SecurityProtocol.SASL_SSL.name) || name.equals(SecurityProtocol.SASL_PLAINTEXT.name)
      // check controller listener names (they won't appear in listeners when process.roles=broker)
      // as well as listeners for occurrences of SSL or SASL_*
      if (controllerListenerNames.exists(isSslOrSasl) ||
        parseCsvList(getString(LISTENERS_PROP)).exists(listenerValue => isSslOrSasl(EndPoint.parseListenerName(listenerValue)))) {
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
      throw new ConfigException(s"You must set `${NODE_ID_PROP}` to the same value as `${BROKER_ID_PROP}`.")
    }
    if (requiresZookeeper) {
      if (zkConnect == null) {
        throw new ConfigException(s"Missing required configuration `${ZK_CONNECT_PROP}` which has no default value.")
      }
      if (brokerIdGenerationEnable) {
        require(brokerId >= -1 && brokerId <= maxReservedBrokerId, "broker.id must be greater than or equal to -1 and not greater than reserved.broker.max.id")
      } else {
        require(brokerId >= 0, "broker.id must be greater than or equal to 0")
      }
    } else {
      // KRaft-based metadata quorum
      if (nodeId < 0) {
        throw new ConfigException(s"Missing configuration `${NODE_ID_PROP}` which is required " +
          s"when `process.roles` is defined (i.e. when running in KRaft mode).")
      }
      if (migrationEnabled) {
        if (zkConnect == null) {
          throw new ConfigException(s"If using `${MIGRATION_ENABLED_PROP}` in KRaft mode, `${ZK_CONNECT_PROP}` must also be set.")
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
    val voterAddressSpecsByNodeId = RaftConfig.parseVoterConnections(quorumVoters)
    def validateNonEmptyQuorumVotersForKRaft(): Unit = {
      if (voterAddressSpecsByNodeId.isEmpty) {
        throw new ConfigException(s"If using ${PROCESS_ROLES_PROP}, ${QUORUM_VOTERS_PROP} must contain a parseable set of voters.")
      }
    }
    def validateNonEmptyQuorumVotersForMigration(): Unit = {
      if (voterAddressSpecsByNodeId.isEmpty) {
        throw new ConfigException(s"If using ${MIGRATION_ENABLED_PROP}, ${QUORUM_VOTERS_PROP} must contain a parseable set of voters.")
      }
    }
    def validateControlPlaneListenerEmptyForKRaft(): Unit = {
      require(controlPlaneListenerName.isEmpty,
        s"${CONTROL_PLANE_LISTENER_NAME_PROP} is not supported in KRaft mode.")
    }
    def validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker(): Unit = {
      require(!advertisedListenerNames.exists(aln => controllerListenerNames.contains(aln.value())),
        s"The advertised.listeners config must not contain KRaft controller listeners from ${CONTROLLER_LISTENER_NAMES_PROP} when ${PROCESS_ROLES_PROP} contains the broker role because Kafka clients that send requests via advertised listeners do not send requests to KRaft controllers -- they only send requests to KRaft brokers.")
    }
    def validateControllerQuorumVotersMustContainNodeIdForKRaftController(): Unit = {
      require(voterAddressSpecsByNodeId.containsKey(nodeId),
        s"If ${PROCESS_ROLES_PROP} contains the 'controller' role, the node id $nodeId must be included in the set of voters ${QUORUM_VOTERS_PROP}=${voterAddressSpecsByNodeId.asScala.keySet.toSet}")
    }
    def validateControllerListenerExistsForKRaftController(): Unit = {
      require(controllerListeners.nonEmpty,
        s"${CONTROLLER_LISTENER_NAMES_PROP} must contain at least one value appearing in the '${LISTENERS_PROP}' configuration when running the KRaft controller role")
    }
    def validateControllerListenerNamesMustAppearInListenersForKRaftController(): Unit = {
      val listenerNameValues = listeners.map(_.listenerName.value).toSet
      require(controllerListenerNames.forall(cln => listenerNameValues.contains(cln)),
        s"${CONTROLLER_LISTENER_NAMES_PROP} must only contain values appearing in the '${LISTENERS_PROP}' configuration when running the KRaft controller role")
    }
    def validateAdvertisedListenersNonEmptyForBroker(): Unit = {
      require(advertisedListenerNames.nonEmpty,
        "There must be at least one advertised listener." + (
          if (processRoles.contains(ProcessRole.BrokerRole)) s" Perhaps all listeners appear in $CONTROLLER_LISTENER_NAMES_PROP?" else ""))
    }
    if (processRoles == Set(ProcessRole.BrokerRole)) {
      // KRaft broker-only
      validateNonEmptyQuorumVotersForKRaft()
      validateControlPlaneListenerEmptyForKRaft()
      validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker()
      // nodeId must not appear in controller.quorum.voters
      require(!voterAddressSpecsByNodeId.containsKey(nodeId),
        s"If ${PROCESS_ROLES_PROP} contains just the 'broker' role, the node id $nodeId must not be included in the set of voters ${QUORUM_VOTERS_PROP}=${voterAddressSpecsByNodeId.asScala.keySet.toSet}")
      // controller.listener.names must be non-empty...
      require(controllerListenerNames.nonEmpty,
        s"${CONTROLLER_LISTENER_NAMES_PROP} must contain at least one value when running KRaft with just the broker role")
      // controller.listener.names are forbidden in listeners...
      require(controllerListeners.isEmpty,
        s"${CONTROLLER_LISTENER_NAMES_PROP} must not contain a value appearing in the '${LISTENERS_PROP}' configuration when running KRaft with just the broker role")
      // controller.listener.names must all appear in listener.security.protocol.map
      controllerListenerNames.foreach { name =>
        val listenerName = ListenerName.normalised(name)
        if (!effectiveListenerSecurityProtocolMap.contains(listenerName)) {
          throw new ConfigException(s"Controller listener with name ${listenerName.value} defined in " +
            s"${CONTROLLER_LISTENER_NAMES_PROP} not found in ${LISTENER_SECURITY_PROTOCOL_MAP_PROP}  (an explicit security mapping for each controller listener is required if ${LISTENER_SECURITY_PROTOCOL_MAP_PROP} is non-empty, or if there are security protocols other than PLAINTEXT in use)")
        }
      }
      // warn that only the first controller listener is used if there is more than one
      if (controllerListenerNames.size > 1) {
        warn(s"${CONTROLLER_LISTENER_NAMES_PROP} has multiple entries; only the first will be used since ${PROCESS_ROLES_PROP}=broker: ${controllerListenerNames.asJava}")
      }
      validateAdvertisedListenersNonEmptyForBroker()
    } else if (processRoles == Set(ProcessRole.ControllerRole)) {
      // KRaft controller-only
      validateNonEmptyQuorumVotersForKRaft()
      validateControlPlaneListenerEmptyForKRaft()
      // advertised listeners must be empty when only the controller is configured
      require(
        getString(ADVERTISED_LISTENERS_PROP) == null,
        s"The ${ADVERTISED_LISTENERS_PROP} config must be empty when ${PROCESS_ROLES_PROP}=controller"
      )
      // listeners should only contain listeners also enumerated in the controller listener
      require(
        effectiveAdvertisedListeners.isEmpty,
        s"The ${LISTENERS_PROP} config must only contain KRaft controller listeners from ${CONTROLLER_LISTENER_NAMES_PROP} when ${PROCESS_ROLES_PROP}=controller"
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
          s"${CONTROLLER_LISTENER_NAMES_PROP} must not be empty when running in ZooKeeper migration mode: ${controllerListenerNames.asJava}")
        require(interBrokerProtocolVersion.isMigrationSupported, s"Cannot enable ZooKeeper migration without setting " +
          s"'${INTER_BROKER_PROTOCOL_VERSION_PROP}' to 3.4 or higher")
        if (logDirs.size > 1) {
          require(interBrokerProtocolVersion.isDirectoryAssignmentSupported,
            s"Cannot enable ZooKeeper migration with multiple log directories (aka JBOD) without setting " +
            s"'${INTER_BROKER_PROTOCOL_VERSION_PROP}' to ${MetadataVersion.IBP_3_7_IV2} or higher")
        }
      } else {
        // controller listener names must be empty when not in KRaft mode
        require(controllerListenerNames.isEmpty,
          s"${CONTROLLER_LISTENER_NAMES_PROP} must be empty when not running in KRaft mode: ${controllerListenerNames.asJava}")
      }
      validateAdvertisedListenersNonEmptyForBroker()
    }

    val listenerNames = listeners.map(_.listenerName).toSet
    if (processRoles.isEmpty || processRoles.contains(ProcessRole.BrokerRole)) {
      // validations for all broker setups (i.e. ZooKeeper and KRaft broker-only and KRaft co-located)
      validateAdvertisedListenersNonEmptyForBroker()
      require(advertisedListenerNames.contains(interBrokerListenerName),
        s"${INTER_BROKER_LISTENER_NAME_PROP} must be a listener name defined in ${ADVERTISED_LISTENERS_PROP}. " +
          s"The valid options based on currently configured listeners are ${advertisedListenerNames.map(_.value).mkString(",")}")
      require(advertisedListenerNames.subsetOf(listenerNames),
        s"${ADVERTISED_LISTENERS_PROP} listener names must be equal to or a subset of the ones defined in ${LISTENERS_PROP}. " +
          s"Found ${advertisedListenerNames.map(_.value).mkString(",")}. The valid options based on the current configuration " +
          s"are ${listenerNames.map(_.value).mkString(",")}"
      )
    }

    require(!effectiveAdvertisedListeners.exists(endpoint => endpoint.host=="0.0.0.0"),
      s"${ADVERTISED_LISTENERS_PROP} cannot use the nonroutable meta-address 0.0.0.0. "+
      s"Use a routable IP address.")

    // validate control.plane.listener.name config
    if (controlPlaneListenerName.isDefined) {
      require(advertisedListenerNames.contains(controlPlaneListenerName.get),
        s"${CONTROL_PLANE_LISTENER_NAME_PROP} must be a listener name defined in ${ADVERTISED_LISTENERS_PROP}. " +
        s"The valid options based on currently configured listeners are ${advertisedListenerNames.map(_.value).mkString(",")}")
      // controlPlaneListenerName should be different from interBrokerListenerName
      require(!controlPlaneListenerName.get.value().equals(interBrokerListenerName.value()),
        s"${CONTROL_PLANE_LISTENER_NAME_PROP}, when defined, should have a different value from the inter broker listener name. " +
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
      s"${SASL_MECHANISM_INTER_BROKER_PROTOCOL_PROP} must be included in ${SASL_ENABLED_MECHANISMS_PROP} when SASL is used for inter-broker communication")
    require(queuedMaxBytes <= 0 || queuedMaxBytes >= socketRequestMaxBytes,
      s"${QUEUED_MAX_BYTES_PROP} must be larger or equal to ${SOCKET_REQUEST_MAX_BYTES_PROP}")

    if (maxConnectionsPerIp == 0)
      require(maxConnectionsPerIpOverrides.nonEmpty, s"${MAX_CONNECTIONS_PER_IP_PROP} can be set to zero only if" +
        s" ${MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP} property is set.")

    val invalidAddresses = maxConnectionsPerIpOverrides.keys.filterNot(address => Utils.validHostPattern(address))
    if (invalidAddresses.nonEmpty)
      throw new IllegalArgumentException(s"${MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP} contains invalid addresses : ${invalidAddresses.mkString(",")}")

    if (connectionsMaxIdleMs >= 0)
      require(failedAuthenticationDelayMs < connectionsMaxIdleMs,
        s"${FAILED_AUTHENTICATION_DELAY_MS_PROP}=$failedAuthenticationDelayMs should always be less than" +
        s" ${CONNECTIONS_MAX_IDLE_MS_PROP}=$connectionsMaxIdleMs to prevent failed" +
        s" authentication responses from timing out")

    val principalBuilderClass = getClass(PRINCIPAL_BUILDER_CLASS_PROP)
    require(principalBuilderClass != null, s"${PRINCIPAL_BUILDER_CLASS_PROP} must be non-null")
    require(classOf[KafkaPrincipalSerde].isAssignableFrom(principalBuilderClass),
      s"${PRINCIPAL_BUILDER_CLASS_PROP} must implement KafkaPrincipalSerde")

    // New group coordinator configs validation.
    require(consumerGroupMaxHeartbeatIntervalMs >= consumerGroupMinHeartbeatIntervalMs,
      s"${CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP} must be greater than or equals " +
      s"to ${CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP}")
    require(consumerGroupHeartbeatIntervalMs >= consumerGroupMinHeartbeatIntervalMs,
      s"${CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP} must be greater than or equals " +
      s"to ${CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP}")
    require(consumerGroupHeartbeatIntervalMs <= consumerGroupMaxHeartbeatIntervalMs,
      s"${CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP} must be less than or equals " +
      s"to ${CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP}")

    require(consumerGroupMaxSessionTimeoutMs >= consumerGroupMinSessionTimeoutMs,
      s"${CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP} must be greater than or equals " +
      s"to ${CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP}")
    require(consumerGroupSessionTimeoutMs >= consumerGroupMinSessionTimeoutMs,
      s"${CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP} must be greater than or equals " +
      s"to ${CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP}")
    require(consumerGroupSessionTimeoutMs <= consumerGroupMaxSessionTimeoutMs,
      s"${CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP} must be less than or equals " +
      s"to ${CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP}")
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
    s"Broker configuration ${LOG_MESSAGE_FORMAT_VERSION_PROP} with value $logMessageFormatVersionString is ignored " +
      s"because the inter-broker protocol version `$interBrokerProtocolVersionString` is greater or equal than 3.0. " +
      "This configuration is deprecated and it will be removed in Apache Kafka 4.0."
  }
}
