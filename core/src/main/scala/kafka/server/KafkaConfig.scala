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
import kafka.utils.Logging
import kafka.utils.Implicits._
import org.apache.kafka.common.{Endpoint, Reconfigurable}
import org.apache.kafka.common.config.{ConfigDef, ConfigException, ConfigResource, TopicConfig}
import org.apache.kafka.common.config.ConfigDef.ConfigKey
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.internals.Plugin
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.group.Group.GroupType
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig
import org.apache.kafka.coordinator.group.{GroupConfig, GroupCoordinatorConfig}
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.{KRaftConfigs, MetadataLogConfig, QuorumConfig}
import org.apache.kafka.security.authorizer.AuthorizerUtils
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.config.AbstractKafkaConfig.getMap
import org.apache.kafka.server.config.{AbstractKafkaConfig, DynamicConfig, QuotaConfig, ReplicationConfigs, ServerConfigs, ServerLogConfigs, DynamicBrokerConfig => JDynamicBrokerConfig}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.metrics.MetricConfigs
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig}

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq}
import scala.jdk.OptionConverters.RichOptional

object KafkaConfig {

  def main(args: Array[String]): Unit = {
    System.out.println(configDef.toHtml(4, (config: String) => "brokerconfigs_" + config,
      JDynamicBrokerConfig.dynamicConfigUpdateModes))
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
        JDynamicBrokerConfig.brokerConfigSynonyms(configName, true).asScala.flatMap(typeOf).headOption
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

  // for testing
  private[server] def updateCurrentConfig(props: util.Map[_, _]): Unit = {
    this.currentConfig = new KafkaConfig(props)
  }

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

  private val _quotaConfig = new QuotaConfig(this)
  def quotaConfig: QuotaConfig = _quotaConfig

  /** ********* General Configuration ***********/
  val brokerSessionTimeoutMs: Int = getInt(KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG)
  val controllerPerformanceSamplePeriodMs: Long = getLong(KRaftConfigs.CONTROLLER_PERFORMANCE_SAMPLE_PERIOD_MS)
  val controllerPerformanceAlwaysLogThresholdMs: Long = getLong(KRaftConfigs.CONTROLLER_PERFORMANCE_ALWAYS_LOG_THRESHOLD_MS)

  private def parseProcessRoles(): Set[ProcessRole] = {
    val roles = getList(KRaftConfigs.PROCESS_ROLES_CONFIG).asScala.map {
      case "broker" => ProcessRole.BrokerRole
      case "controller" => ProcessRole.ControllerRole
      case role => throw new ConfigException(s"Unknown process role '$role'" +
        " (only 'broker' and 'controller' are allowed roles)")
    }
    roles.toSet
  }

  def isKRaftCombinedMode: Boolean = {
    processRoles == Set(ProcessRole.BrokerRole, ProcessRole.ControllerRole)
  }

  def metadataLogDir: String = {
    Option(getString(MetadataLogConfig.METADATA_LOG_DIR_CONFIG)) match {
      case Some(dir) => dir
      case None => logDirs.get(0)
    }
  }

  val serverMaxStartupTimeMs = getLong(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG)

  def messageMaxBytes = getInt(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG)
  val connectionSetupTimeoutMs = getLong(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG)
  val connectionSetupTimeoutMaxMs = getLong(ServerConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG)

  def getNumReplicaAlterLogDirsThreads: Int = {
    val numThreads: Integer = Option(getInt(ServerConfigs.NUM_REPLICA_ALTER_LOG_DIRS_THREADS_CONFIG)).getOrElse(logDirs.size)
    numThreads
  }

  /************* Metadata Configuration ***********/
  val metadataSnapshotMaxNewRecordBytes = getLong(MetadataLogConfig.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG)
  val metadataSnapshotMaxIntervalMs = getLong(MetadataLogConfig.METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG)
  val metadataMaxIdleIntervalNs: Option[Long] = {
    val value = TimeUnit.NANOSECONDS.convert(getInt(MetadataLogConfig.METADATA_MAX_IDLE_INTERVAL_MS_CONFIG).toLong, TimeUnit.MILLISECONDS)
    if (value > 0) Some(value) else None
  }

  /************* Authorizer Configuration ***********/
  def createNewAuthorizer(metrics: Metrics, role: String): Option[Plugin[Authorizer]] = {
    val className = getString(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG)
    if (className == null || className.isEmpty)
      None
    else {
      Some(AuthorizerUtils.createAuthorizer(className, originals, metrics, ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG, role))
    }
  }

  val earlyStartListeners: Set[ListenerName] = {
    val listenersSet = listeners.map(l => ListenerName.normalised(l.listener)).toSet
    val controllerListenersSet = controllerListeners.map(l => ListenerName.normalised(l.listener)).toSet
    Option(getList(ServerConfigs.EARLY_START_LISTENERS_CONFIG)) match {
      case None => controllerListenersSet
      case Some(list) =>
        list.asScala.map(_.trim()).filterNot(_.isEmpty).map { str =>
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
  def maxConnectionsPerIp = getInt(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG)
  def maxConnectionsPerIpOverrides: Map[String, Int] =
    getMap(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG, getString(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG)).asScala.map { case (k, v) => (k, v.toInt)}
  def maxConnections = getInt(SocketServerConfigs.MAX_CONNECTIONS_CONFIG)
  def maxConnectionCreationRate = getInt(SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG)
  val connectionsMaxIdleMs = getLong(SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG)
  val failedAuthenticationDelayMs = getInt(SocketServerConfigs.FAILED_AUTHENTICATION_DELAY_MS_CONFIG)
  val queuedMaxRequests = getInt(SocketServerConfigs.QUEUED_MAX_REQUESTS_CONFIG)
  val queuedMaxBytes = getLong(SocketServerConfigs.QUEUED_MAX_BYTES_CONFIG)
  def numNetworkThreads = getInt(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG)

  /***************** rack configuration **************/
  val replicaSelectorClassName = Option(getString(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG))

  /** ********* Log Configuration ***********/
  val autoCreateTopicsEnable = getBoolean(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG)
  val numPartitions = getInt(ServerLogConfigs.NUM_PARTITIONS_CONFIG)
  def logSegmentBytes = getInt(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG)
  def logFlushIntervalMessages = getLong(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG)
  def logCleanerThreads = getInt(CleanerConfig.LOG_CLEANER_THREADS_PROP)
  val logFlushSchedulerIntervalMs = getLong(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG)
  val logFlushOffsetCheckpointIntervalMs = getInt(ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG).toLong
  val logFlushStartOffsetCheckpointIntervalMs = getInt(ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG).toLong
  val logCleanupIntervalMs = getLong(ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG)
  def logCleanupPolicy = getList(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG)

  def logRetentionBytes = getLong(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG)
  def logCleanerDedupeBufferSize = getLong(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP)
  def logCleanerDeleteRetentionMs = getLong(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP)
  def logCleanerMinCompactionLagMs = getLong(CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP)
  def logCleanerMaxCompactionLagMs = getLong(CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP)
  def logCleanerMinCleanRatio = getDouble(CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP)
  def logIndexSizeMaxBytes = getInt(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG)
  def logIndexIntervalBytes = getInt(ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_CONFIG)
  def logDeleteDelayMs = getLong(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG)
  def logRollTimeMillis: java.lang.Long = Option(getLong(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG)).getOrElse(60 * 60 * 1000L * getInt(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG))
  def logRollTimeJitterMillis: java.lang.Long = Option(getLong(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG)).getOrElse(60 * 60 * 1000L * getInt(ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG))
  def logFlushIntervalMs: java.lang.Long = Option(getLong(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG)).getOrElse(getLong(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG))
  def minInSyncReplicas = getInt(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG)
  def logPreAllocateEnable: java.lang.Boolean = getBoolean(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG)
  def logInitialTaskDelayMs: java.lang.Long = Option(getLong(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG)).getOrElse(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DEFAULT)

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
  val replicaHighWatermarkCheckpointIntervalMs = getLong(ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_CONFIG)
  val fetchPurgatoryPurgeIntervalRequests = getInt(ReplicationConfigs.FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG)
  val producerPurgatoryPurgeIntervalRequests = getInt(ReplicationConfigs.PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG)
  val deleteRecordsPurgatoryPurgeIntervalRequests = getInt(ReplicationConfigs.DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG)
  val autoLeaderRebalanceEnable = getBoolean(ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG)
  val leaderImbalanceCheckIntervalSeconds: Long = getLong(ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_CONFIG)
  val uncleanLeaderElectionCheckIntervalMs: Long = getLong(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_INTERVAL_MS_CONFIG)
  def uncleanLeaderElectionEnable: java.lang.Boolean = getBoolean(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG)
  def followerFetchLastTieredOffsetEnable: java.lang.Boolean = getBoolean(ReplicationConfigs.FOLLOWER_FETCH_LAST_TIERED_OFFSET_ENABLE_CONFIG);

  /** ********* Controlled shutdown configuration ***********/
  val controlledShutdownEnable = getBoolean(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG)

  /** Group coordinator configs */
  val groupCoordinatorRebalanceProtocols = {
    val protocols = getList(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG)
      .asScala.map(_.toUpperCase).map(GroupType.valueOf).toSet
    if (!protocols.contains(GroupType.CLASSIC)) {
      throw new ConfigException(s"Disabling the '${GroupType.CLASSIC}' protocol is not supported.")
    }
    if (protocols.contains(GroupType.SHARE)) {
      warn(s"'${GroupType.SHARE}' in ${GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG} is deprecated. " +
        s"Share groups are controlled by the 'share.version' feature; this broker config will be ignored in a future release.")
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

  def saslMechanismInterBrokerProtocol = getString(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG)

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

  override def addReconfigurable(reconfigurable: Reconfigurable): Unit = {
    dynamicConfig.addReconfigurable(reconfigurable)
  }

  override def removeReconfigurable(reconfigurable: Reconfigurable): Unit = {
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

  def listeners: Seq[Endpoint] =
    AbstractKafkaConfig.listenerListToEndPoints(getList(SocketServerConfigs.LISTENERS_CONFIG), effectiveListenerSecurityProtocolMap).asScala

  def controllerListeners: Seq[Endpoint] =
    listeners.filter(l => controllerListenerNames.contains(l.listener))

  def saslMechanismControllerProtocol: String = getString(KRaftConfigs.SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG)

  def dataPlaneListeners: Seq[Endpoint] = {
    listeners.filterNot { listener =>
      val name = listener.listener
        controllerListenerNames.contains(name)
    }
  }

  def effectiveAdvertisedControllerListeners: Seq[Endpoint] = {
    val advertisedListenersProp = getList(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG)
    val controllerAdvertisedListeners = if (advertisedListenersProp != null) {
      AbstractKafkaConfig.listenerListToEndPoints(advertisedListenersProp, effectiveListenerSecurityProtocolMap, false).asScala
        .filter(l => controllerListenerNames.contains(l.listener))
    } else {
      Seq.empty
    }
    val controllerListenersValue = controllerListeners

    controllerListenerNames.asScala.flatMap { name =>
      controllerAdvertisedListeners
        .find(endpoint => ListenerName.normalised(endpoint.listener).equals(ListenerName.normalised(name)))
        .orElse(
          // If users don't define advertised.listeners, the advertised controller listeners inherit from listeners configuration
          // which match listener names in controller.listener.names.
          // Removing "0.0.0.0" host to avoid validation errors. This is to be compatible with the old behavior before 3.9.
          // The null or "" host does a reverse lookup in ListenerInfo#withWildcardHostnamesResolved.
          controllerListenersValue
            .find(endpoint => ListenerName.normalised(endpoint.listener).equals(ListenerName.normalised(name)))
            .map(endpoint => if (endpoint.host == "0.0.0.0") {
              new Endpoint(endpoint.listener, endpoint.securityProtocol, null, endpoint.port)
            } else {
              endpoint
            })
        )
    }
  }

  def effectiveAdvertisedBrokerListeners: Seq[Endpoint] = {
    // Use advertised listeners if defined, fallback to listeners otherwise
    val advertisedListenersProp = getList(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG)
    val advertisedListeners = if (advertisedListenersProp != null) {
      AbstractKafkaConfig.listenerListToEndPoints(advertisedListenersProp, effectiveListenerSecurityProtocolMap, false).asScala
    } else {
      listeners
    }
    // Only expose broker listeners
    advertisedListeners.filterNot(l => controllerListenerNames.contains(l.listener))
  }

  validateValues()

  private def validateValues(): Unit = {
    if (nodeId != brokerId) {
      throw new ConfigException(s"You must set `${KRaftConfigs.NODE_ID_CONFIG}` to the same value as `${ServerConfigs.BROKER_ID_CONFIG}`.")
    }
    require(logRollTimeMillis >= 1, "log.roll.ms must be greater than or equal to 1")
    require(logRollTimeJitterMillis >= 0, "log.roll.jitter.ms must be greater than or equal to 0")
    require(logRetentionTimeMillis >= 1 || logRetentionTimeMillis == -1, "log.retention.ms must be unlimited (-1) or, greater than or equal to 1")
    require(logDirs.size > 0, "At least one log directory must be defined via log.dirs or log.dir.")
    require(logCleanerDedupeBufferSize / logCleanerThreads > 1024 * 1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.")
    require(replicaFetchWaitMaxMs <= replicaSocketTimeoutMs, "replica.socket.timeout.ms should always be at least replica.fetch.wait.max.ms" +
      " to prevent unnecessary socket timeouts")
    require(replicaFetchWaitMaxMs <= replicaLagTimeMaxMs, "replica.fetch.wait.max.ms should always be less than or equal to replica.lag.time.max.ms" +
      " to prevent frequent changes in ISR")

    if (brokerHeartbeatIntervalMs * 2 > brokerSessionTimeoutMs) {
      error(s"${KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG} ($brokerHeartbeatIntervalMs ms) must be less than or equal to half of the ${KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG} ($brokerSessionTimeoutMs ms). " +
        s"The ${KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG} is configured on controller. The ${KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG} is configured on broker. " +
        s"If a broker doesn't send heartbeat request within ${KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG}, it loses broker lease. " +
        s"Please increase ${KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG} or decrease ${KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG}.")
    }

    val advertisedBrokerListenerNames = effectiveAdvertisedBrokerListeners.map(l => ListenerName.normalised(l.listener)).toSet

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

    def validateControllerQuorumVotersMustContainNodeIdForKRaftController(): Unit = {
      require(voterIds.isEmpty || voterIds.contains(nodeId),
        s"If ${KRaftConfigs.PROCESS_ROLES_CONFIG} contains the 'controller' role, the node id $nodeId must be included in the set of voters ${QuorumConfig.QUORUM_VOTERS_CONFIG}=${voterIds.asScala.toSet}")
    }
    def validateAdvertisedControllerListenersNonEmptyForKRaftController(): Unit = {
      require(effectiveAdvertisedControllerListeners.nonEmpty,
        s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must contain at least one value appearing in the '${SocketServerConfigs.LISTENERS_CONFIG}' configuration when running the KRaft controller role")
    }
    def validateControllerListenerNamesMustAppearInListenersForKRaftController(): Unit = {
      val listenerNameValues = listeners.map(_.listener).toSet
      require(controllerListenerNames.stream().allMatch(cln => listenerNameValues.contains(cln)),
        s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must only contain values appearing in the '${SocketServerConfigs.LISTENERS_CONFIG}' configuration when running the KRaft controller role")
    }
    def validateAdvertisedBrokerListenersNonEmptyForBroker(): Unit = {
      require(advertisedBrokerListenerNames.nonEmpty,
        "There must be at least one broker advertised listener." + (
          if (processRoles.contains(ProcessRole.BrokerRole)) s" Perhaps all listeners appear in ${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG}?" else ""))
    }
    def warnIfConfigDefinedInWrongRole(expectedRole: ProcessRole, configName: String): Unit = {
      if (originals.containsKey(configName)) {
        warn(s"$configName is defined in ${processRoles.mkString(", ")}. It should be defined in the $expectedRole role.")
      }
    }
    if (processRoles == Set(ProcessRole.BrokerRole)) {
      // KRaft broker-only
      validateQuorumVotersAndQuorumBootstrapServerForKRaft()
      // nodeId must not appear in controller.quorum.voters
      require(!voterIds.contains(nodeId),
        s"If ${KRaftConfigs.PROCESS_ROLES_CONFIG} contains just the 'broker' role, the node id $nodeId must not be included in the set of voters ${QuorumConfig.QUORUM_VOTERS_CONFIG}=${voterIds.asScala.toSet}")
      // controller.listener.names must be non-empty...
      require(controllerListenerNames.size() > 0,
        s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must contain at least one value when running KRaft with just the broker role")
      // controller.listener.names are forbidden in listeners...
      require(controllerListeners.isEmpty,
        s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must not contain a value appearing in the '${SocketServerConfigs.LISTENERS_CONFIG}' configuration when running KRaft with just the broker role")
      // controller.listener.names must all appear in listener.security.protocol.map
      controllerListenerNames.forEach { name =>
        val listenerName = ListenerName.normalised(name)
        if (!effectiveListenerSecurityProtocolMap.containsKey(listenerName)) {
          throw new ConfigException(s"Controller listener with name ${listenerName.value} defined in " +
            s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} not found in ${SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG}  (an explicit security mapping for each controller listener is required if ${SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG} is non-empty, or if there are security protocols other than PLAINTEXT in use)")
        }
      }
      // controller.quorum.auto.join.enable must be false for KRaft broker-only
      require(!quorumConfig.autoJoin,
        s"${QuorumConfig.QUORUM_AUTO_JOIN_ENABLE_CONFIG} is only supported when ${KRaftConfigs.PROCESS_ROLES_CONFIG} contains the 'controller' role.")
      // warn that only the first controller listener is used if there is more than one
      if (controllerListenerNames.size > 1) {
        warn(s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} has multiple entries; only the first will be used since ${KRaftConfigs.PROCESS_ROLES_CONFIG}=broker: ${controllerListenerNames}")
      }
      // warn if create.topic.policy.class.name or alter.config.policy.class.name is defined in the broker role
      warnIfConfigDefinedInWrongRole(ProcessRole.ControllerRole, ServerLogConfigs.CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG)
      warnIfConfigDefinedInWrongRole(ProcessRole.ControllerRole, ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG)
    } else if (processRoles == Set(ProcessRole.ControllerRole)) {
      // KRaft controller-only
      validateQuorumVotersAndQuorumBootstrapServerForKRaft()
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
      validateControllerQuorumVotersMustContainNodeIdForKRaftController()
      validateAdvertisedControllerListenersNonEmptyForKRaftController()
      validateControllerListenerNamesMustAppearInListenersForKRaftController()
    }

    val listenerNames = listeners.map(l => ListenerName.normalised(l.listener)).toSet
    if (processRoles.contains(ProcessRole.BrokerRole)) {
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

    val interBrokerUsesSasl = interBrokerSecurityProtocol == SecurityProtocol.SASL_PLAINTEXT || interBrokerSecurityProtocol == SecurityProtocol.SASL_SSL
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
   * Copy the subset of properties that are relevant to Logs. The individual properties
   * are listed here since the names are slightly different in each Config class...
   */
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
    logProps.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, logMessageTimestampType.name)
    logProps.put(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, logMessageTimestampBeforeMaxMs: java.lang.Long)
    logProps.put(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, logMessageTimestampAfterMaxMs: java.lang.Long)
    logProps.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, remoteLogManagerConfig.logLocalRetentionMs: java.lang.Long)
    logProps.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, remoteLogManagerConfig.logLocalRetentionBytes: java.lang.Long)
    logProps
  }
}
