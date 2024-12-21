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
import java.util.Properties
import kafka.cluster.EndPoint
import kafka.utils.{CoreUtils, Logging}
import kafka.utils.Implicits._
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.{ConfigDef, ConfigException, ConfigResource, SaslConfigs, TopicConfig}
import org.apache.kafka.common.config.ConfigDef.ConfigKey
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.{CompressionType, TimestampType}
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.group.Group.GroupType
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig
import org.apache.kafka.coordinator.group.{GroupConfig, GroupCoordinatorConfig}
import org.apache.kafka.coordinator.transaction.{TransactionLogConfig, TransactionStateManagerConfig}
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.security.authorizer.AuthorizerUtils
import org.apache.kafka.security.PasswordEncoderConfigs
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.config.{AbstractKafkaConfig, DelegationTokenManagerConfigs, KRaftConfigs, QuotaConfig, ReplicationConfigs, ServerConfigs, ServerLogConfigs, ShareCoordinatorConfig, ZkConfigs}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.metrics.MetricConfigs
import org.apache.kafka.server.util.Csv
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig}
import org.apache.kafka.storage.internals.log.LogConfig.MessageFormatVersion
import org.apache.zookeeper.client.ZKClientConfig

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq}
import scala.jdk.OptionConverters.RichOptional

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

  val configDef = AbstractKafkaConfig.CONFIG_DEF

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
      val configKey = DynamicConfig.Broker.configKeys.get(exactName)
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
      case ConfigResource.Type.TOPIC => KafkaConfig.maybeSensitive(LogConfig.configType(name).toScala)
      case ConfigResource.Type.GROUP => KafkaConfig.maybeSensitive(GroupConfig.configType(name).toScala)
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

/**
 * The class extend {@link AbstractKafkaConfig} which will be the future KafkaConfig.
 * When add any new methods if it doesn't depend on anything in Core, then move it to org.apache.kafka.server.config.KafkaConfig instead of here.
 * Any code depends on kafka.server.KafkaConfig will keep for using kafka.server.KafkaConfig for the time being until we move it out of core
 * For more details check KAFKA-15853
 */
class KafkaConfig private(doLog: Boolean, val props: util.Map[_, _])
  extends AbstractKafkaConfig(KafkaConfig.configDef, props, Utils.castToStringObjectMap(props), doLog) with Logging {

  def this(props: java.util.Map[_, _]) = this(true, KafkaConfig.populateSynonyms(props))
  def this(props: java.util.Map[_, _], doLog: Boolean) = this(doLog, KafkaConfig.populateSynonyms(props))

  // Cache the current config to avoid acquiring read lock to access from dynamicConfig
  @volatile private var currentConfig = this
  val processRoles: Set[ProcessRole] = parseProcessRoles()
  private[server] val dynamicConfig = new DynamicBrokerConfig(this)

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

  private val _quorumConfig = new QuorumConfig(this)
  def quorumConfig: QuorumConfig = _quorumConfig

  private val _groupCoordinatorConfig = new GroupCoordinatorConfig(this)

  def groupCoordinatorConfig: GroupCoordinatorConfig = _groupCoordinatorConfig

  private val _shareGroupConfig = new ShareGroupConfig(this)
  def shareGroupConfig: ShareGroupConfig = _shareGroupConfig

  private val _shareCoordinatorConfig = new ShareCoordinatorConfig(this)
  def shareCoordinatorConfig: ShareCoordinatorConfig = _shareCoordinatorConfig

  private val _transactionLogConfig = new TransactionLogConfig(this)
  private val _transactionStateManagerConfig = new TransactionStateManagerConfig(this)
  def transactionLogConfig: TransactionLogConfig = _transactionLogConfig
  def transactionStateManagerConfig: TransactionStateManagerConfig = _transactionStateManagerConfig

  private val _quotaConfig = new QuotaConfig(this)
  def quotaConfig: QuotaConfig = _quotaConfig


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

  def backgroundThreads = getInt(ServerConfigs.BACKGROUND_THREADS_CONFIG)
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
  val queuedMaxRequests = getInt(SocketServerConfigs.QUEUED_MAX_REQUESTS_CONFIG)
  val queuedMaxBytes = getLong(SocketServerConfigs.QUEUED_MAX_BYTES_CONFIG)
  def numNetworkThreads = getInt(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG)

  /***************** rack configuration **************/
  val rack = Option(getString(ServerConfigs.BROKER_RACK_CONFIG))
  val replicaSelectorClassName = Option(getString(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG))

  /** ********* Log Configuration ***********/
  val autoCreateTopicsEnable = getBoolean(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG)
  val numPartitions = getInt(ServerLogConfigs.NUM_PARTITIONS_CONFIG)
  val logDirs: Seq[String] = Csv.parseCsvList(Option(getString(ServerLogConfigs.LOG_DIRS_CONFIG)).getOrElse(getString(ServerLogConfigs.LOG_DIR_CONFIG))).asScala
  def logSegmentBytes = getInt(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG)
  def logFlushIntervalMessages = getLong(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG)
  def logCleanerThreads = getInt(CleanerConfig.LOG_CLEANER_THREADS_PROP)
  def numRecoveryThreadsPerDataDir = getInt(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG)
  val logFlushSchedulerIntervalMs = getLong(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG)
  val logFlushOffsetCheckpointIntervalMs = getInt(ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG).toLong
  val logFlushStartOffsetCheckpointIntervalMs = getInt(ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG).toLong
  val logCleanupIntervalMs = getLong(ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG)
  def logCleanupPolicy = getList(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG)

  def logRetentionBytes = getLong(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG)
  def logCleanerDedupeBufferSize = getLong(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP)
  def logCleanerDedupeBufferLoadFactor = getDouble(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP)
  def logCleanerIoBufferSize = getInt(CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP)
  def logCleanerIoMaxBytesPerSecond = getDouble(CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP)
  def logCleanerDeleteRetentionMs = getLong(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP)
  def logCleanerMinCompactionLagMs = getLong(CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP)
  def logCleanerMaxCompactionLagMs = getLong(CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP)
  def logCleanerBackoffMs = getLong(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP)
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

  def logMessageTimestampBeforeMaxMs: Long = getLong(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG)

  def logMessageTimestampAfterMaxMs: Long = getLong(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG)

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
  val uncleanLeaderElectionCheckIntervalMs: Long = getLong(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_INTERVAL_MS_CONFIG)
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

  /** New group coordinator configs */
  val isNewGroupCoordinatorEnabled = getBoolean(GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG)
  val groupCoordinatorRebalanceProtocols = {
    val protocols = getList(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG)
      .asScala.map(_.toUpperCase).map(GroupType.valueOf).toSet
    if (!protocols.contains(GroupType.CLASSIC)) {
      throw new ConfigException(s"Disabling the '${GroupType.CLASSIC}' protocol is not supported.")
    }
    if (protocols.contains(GroupType.CONSUMER)) {
      if (processRoles.isEmpty || !isNewGroupCoordinatorEnabled) {
        warn(s"The new '${GroupType.CONSUMER}' rebalance protocol is only supported in KRaft cluster with the new group coordinator.")
      }
    }
    if (protocols.contains(GroupType.SHARE)) {
      if (processRoles.isEmpty || !isNewGroupCoordinatorEnabled) {
        warn(s"The new '${GroupType.SHARE}' rebalance protocol is only supported in KRaft cluster with the new group coordinator.")
      }
      warn(s"Share groups and the new '${GroupType.SHARE}' rebalance protocol are enabled. " +
        "This is part of the early access of KIP-932 and MUST NOT be used in production.")
    }
    protocols
  }

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
  val delegationTokenSecretKey = getPassword(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG)
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

  def effectiveAdvertisedControllerListeners: Seq[EndPoint] = {
    val controllerAdvertisedListeners = advertisedListeners.filter(l => controllerListenerNames.contains(l.listenerName.value()))
    val controllerListenersValue = controllerListeners

    controllerListenerNames.flatMap { name =>
      controllerAdvertisedListeners
        .find(endpoint => endpoint.listenerName.equals(ListenerName.normalised(name)))
        .orElse(controllerListenersValue.find(endpoint => endpoint.listenerName.equals(ListenerName.normalised(name))))
    }
  }

  def effectiveAdvertisedBrokerListeners: Seq[EndPoint] = {
    // Only expose broker listeners
    advertisedListeners.filterNot(l => controllerListenerNames.contains(l.listenerName.value()))
  }

  // Use advertised listeners if defined, fallback to listeners otherwise
  private def advertisedListeners: Seq[EndPoint] = {
    val advertisedListenersProp = getString(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG)
    if (advertisedListenersProp != null) {
      CoreUtils.listenerListToEndPoints(advertisedListenersProp, effectiveListenerSecurityProtocolMap, requireDistinctPorts=false)
    } else {
      listeners
    }
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

    val advertisedBrokerListenerNames = effectiveAdvertisedBrokerListeners.map(_.listenerName).toSet

    // validate KRaft-related configs
    val voterIds = QuorumConfig.parseVoterIds(quorumConfig.voters)
    def validateQuorumVotersAndQuorumBootstrapServerForKRaft(): Unit = {
      if (voterIds.isEmpty && quorumConfig.bootstrapServers.isEmpty) {
        throw new ConfigException(
          s"""If using ${KRaftConfigs.PROCESS_ROLES_CONFIG}, either ${QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG} must
          |contain the set of bootstrap controllers or ${QuorumConfig.QUORUM_VOTERS_CONFIG} must contain a parseable
          |set of controllers.""".stripMargin.replace("\n", " ")
        )
      }
    }

    def validateControlPlaneListenerEmptyForKRaft(): Unit = {
      require(controlPlaneListenerName.isEmpty,
        s"${SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG} is not supported in KRaft mode.")
    }
    def validateControllerQuorumVotersMustContainNodeIdForKRaftController(): Unit = {
      require(voterIds.isEmpty || voterIds.contains(nodeId),
        s"If ${KRaftConfigs.PROCESS_ROLES_CONFIG} contains the 'controller' role, the node id $nodeId must be included in the set of voters ${QuorumConfig.QUORUM_VOTERS_CONFIG}=${voterIds.asScala.toSet}")
    }
    def validateAdvertisedControllerListenersNonEmptyForKRaftController(): Unit = {
      require(effectiveAdvertisedControllerListeners.nonEmpty,
        s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must contain at least one value appearing in the '${SocketServerConfigs.LISTENERS_CONFIG}' configuration when running the KRaft controller role")
    }
    def validateControllerListenerNamesMustAppearInListenersForKRaftController(): Unit = {
      val listenerNameValues = listeners.map(_.listenerName.value).toSet
      require(controllerListenerNames.forall(cln => listenerNameValues.contains(cln)),
        s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must only contain values appearing in the '${SocketServerConfigs.LISTENERS_CONFIG}' configuration when running the KRaft controller role")
    }
    def validateAdvertisedBrokerListenersNonEmptyForBroker(): Unit = {
      require(advertisedBrokerListenerNames.nonEmpty,
        "There must be at least one broker advertised listener." + (
          if (processRoles.contains(ProcessRole.BrokerRole)) s" Perhaps all listeners appear in ${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG}?" else ""))
    }
    if (processRoles == Set(ProcessRole.BrokerRole)) {
      // KRaft broker-only
      validateQuorumVotersAndQuorumBootstrapServerForKRaft()
      validateControlPlaneListenerEmptyForKRaft()
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
    } else if (processRoles == Set(ProcessRole.ControllerRole)) {
      // KRaft controller-only
      validateQuorumVotersAndQuorumBootstrapServerForKRaft()
      validateControlPlaneListenerEmptyForKRaft()
      // listeners should only contain listeners also enumerated in the controller listener
      require(
        effectiveAdvertisedControllerListeners.size == listeners.size,
        s"The ${SocketServerConfigs.LISTENERS_CONFIG} config must only contain KRaft controller listeners from ${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} when ${KRaftConfigs.PROCESS_ROLES_CONFIG}=controller"
      )
      // controller.listener.names must not contain inter.broker.listener.name when inter.broker.listener.name is explicitly set
      if (Option(getString(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG)).isDefined) {
        require(
          !controllerListenerNames.contains(interBrokerListenerName.value()),
          s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must not contain an explicitly set ${ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG} configuration value when ${KRaftConfigs.PROCESS_ROLES_CONFIG}=controller'"
        )
      }
      validateControllerQuorumVotersMustContainNodeIdForKRaftController()
      validateAdvertisedControllerListenersNonEmptyForKRaftController()
      validateControllerListenerNamesMustAppearInListenersForKRaftController()
    } else if (isKRaftCombinedMode) {
      // KRaft combined broker and controller
      validateQuorumVotersAndQuorumBootstrapServerForKRaft()
      validateControlPlaneListenerEmptyForKRaft()
      validateControllerQuorumVotersMustContainNodeIdForKRaftController()
      validateAdvertisedControllerListenersNonEmptyForKRaftController()
      validateControllerListenerNamesMustAppearInListenersForKRaftController()
    } else {
      // controller listener names must be empty when not in KRaft mode
      require(controllerListenerNames.isEmpty,
        s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must be empty when not running in KRaft mode: ${controllerListenerNames.asJava}")
    }

    val listenerNames = listeners.map(_.listenerName).toSet
    if (processRoles.isEmpty || processRoles.contains(ProcessRole.BrokerRole)) {
      // validations for all broker setups (i.e. ZooKeeper and KRaft broker-only and KRaft co-located)
      validateAdvertisedBrokerListenersNonEmptyForBroker()
      require(advertisedBrokerListenerNames.contains(interBrokerListenerName),
        s"${ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG} must be a listener name defined in ${SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG}. " +
          s"The valid options based on currently configured listeners are ${advertisedBrokerListenerNames.map(_.value).mkString(",")}")
      require(advertisedBrokerListenerNames.subsetOf(listenerNames),
        s"${SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG} listener names must be equal to or a subset of the ones defined in ${SocketServerConfigs.LISTENERS_CONFIG}. " +
          s"Found ${advertisedBrokerListenerNames.map(_.value).mkString(",")}. The valid options based on the current configuration " +
          s"are ${listenerNames.map(_.value).mkString(",")}"
      )
    }

    require(!effectiveAdvertisedBrokerListeners.exists(endpoint => endpoint.host=="0.0.0.0"),
      s"${SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG} cannot use the nonroutable meta-address 0.0.0.0. "+
      s"Use a routable IP address.")

    require(!effectiveAdvertisedControllerListeners.exists(endpoint => endpoint.host=="0.0.0.0"),
      s"${SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG} cannot use the nonroutable meta-address 0.0.0.0. "+
      s"Use a routable IP address.")

    // validate control.plane.listener.name config
    if (controlPlaneListenerName.isDefined) {
      require(advertisedBrokerListenerNames.contains(controlPlaneListenerName.get),
        s"${SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG} must be a listener name defined in ${SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG}. " +
        s"The valid options based on currently configured listeners are ${advertisedBrokerListenerNames.map(_.value).mkString(",")}")
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

    if (groupCoordinatorConfig.offsetTopicCompressionType == CompressionType.ZSTD)
      require(interBrokerProtocolVersion.highestSupportedRecordVersion().value >= IBP_2_1_IV0.highestSupportedRecordVersion().value,
        "offsets.topic.compression.codec zstd can only be used when inter.broker.protocol.version " +
        s"is set to version ${IBP_2_1_IV0.shortVersion} or higher")

    val interBrokerUsesSasl = interBrokerSecurityProtocol == SecurityProtocol.SASL_PLAINTEXT || interBrokerSecurityProtocol == SecurityProtocol.SASL_SSL
    require(!interBrokerUsesSasl || saslInterBrokerHandshakeRequestEnable || saslMechanismInterBrokerProtocol == SaslConfigs.GSSAPI_MECHANISM,
      s"Only GSSAPI mechanism is supported for inter-broker communication with SASL when inter.broker.protocol.version is set to $interBrokerProtocolVersionString")
    require(!interBrokerUsesSasl || saslEnabledMechanisms(interBrokerListenerName).contains(saslMechanismInterBrokerProtocol),
      s"${BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG} must be included in ${BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG} when SASL is used for inter-broker communication")
    require(queuedMaxBytes <= 0 || queuedMaxBytes >= socketRequestMaxBytes,
      s"${SocketServerConfigs.QUEUED_MAX_BYTES_CONFIG} must be larger or equal to ${SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_CONFIG}")

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
    logProps.put(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, logMessageTimestampBeforeMaxMs: java.lang.Long)
    logProps.put(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, logMessageTimestampAfterMaxMs: java.lang.Long)
    logProps.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, remoteLogManagerConfig.logLocalRetentionMs: java.lang.Long)
    logProps.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, remoteLogManagerConfig.logLocalRetentionBytes: java.lang.Long)
    logProps
  }

  @nowarn("cat=deprecation")
  private def createBrokerWarningMessage: String = {
    s"Broker configuration ${ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_CONFIG} with value $logMessageFormatVersionString is ignored " +
      s"because the inter-broker protocol version `$interBrokerProtocolVersionString` is greater or equal than 3.0. " +
      "This configuration is deprecated and it will be removed in Apache Kafka 4.0."
  }
}
