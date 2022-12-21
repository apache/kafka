/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import kafka.log.LogConfig.configDef
import kafka.server.{KafkaConfig, ThrottledReplicaListValidator}
import kafka.utils.Implicits._
import org.apache.kafka.common.config.ConfigDef.{ConfigKey, ValidList, Validator}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, TopicConfig}
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.record.{LegacyRecord, RecordVersion, TimestampType}
import org.apache.kafka.common.utils.{ConfigUtils, Utils}
import org.apache.kafka.metadata.ConfigSynonym
import org.apache.kafka.metadata.ConfigSynonym.{HOURS_TO_MILLISECONDS, MINUTES_TO_MILLISECONDS}

import java.util.Arrays.asList
import java.util.{Collections, Locale, Properties}
import org.apache.kafka.server.common.{MetadataVersion, MetadataVersionValidator}
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.record.BrokerCompressionType

import scala.annotation.nowarn
import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters._

object Defaults {
  val SegmentSize = kafka.server.Defaults.LogSegmentBytes
  val SegmentMs = kafka.server.Defaults.LogRollHours * 60 * 60 * 1000L
  val SegmentJitterMs = kafka.server.Defaults.LogRollJitterHours * 60 * 60 * 1000L
  val FlushInterval = kafka.server.Defaults.LogFlushIntervalMessages
  val FlushMs = kafka.server.Defaults.LogFlushSchedulerIntervalMs
  val RetentionSize = kafka.server.Defaults.LogRetentionBytes
  val RetentionMs = kafka.server.Defaults.LogRetentionHours * 60 * 60 * 1000L
  val RemoteLogStorageEnable = false
  val LocalRetentionBytes = -2 // It indicates the value to be derived from RetentionSize
  val LocalRetentionMs = -2 // It indicates the value to be derived from RetentionMs
  val MaxMessageSize = kafka.server.Defaults.MessageMaxBytes
  val MaxIndexSize = kafka.server.Defaults.LogIndexSizeMaxBytes
  val IndexInterval = kafka.server.Defaults.LogIndexIntervalBytes
  val FileDeleteDelayMs = kafka.server.Defaults.LogDeleteDelayMs
  val DeleteRetentionMs = kafka.server.Defaults.LogCleanerDeleteRetentionMs
  val MinCompactionLagMs = kafka.server.Defaults.LogCleanerMinCompactionLagMs
  val MaxCompactionLagMs = kafka.server.Defaults.LogCleanerMaxCompactionLagMs
  val MinCleanableDirtyRatio = kafka.server.Defaults.LogCleanerMinCleanRatio
  val CleanupPolicy = kafka.server.Defaults.LogCleanupPolicy
  val UncleanLeaderElectionEnable = kafka.server.Defaults.UncleanLeaderElectionEnable
  val MinInSyncReplicas = kafka.server.Defaults.MinInSyncReplicas
  val CompressionType = kafka.server.Defaults.CompressionType
  val PreAllocateEnable = kafka.server.Defaults.LogPreAllocateEnable

  /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
  @deprecated("3.0")
  val MessageFormatVersion = kafka.server.Defaults.LogMessageFormatVersion

  val MessageTimestampType = kafka.server.Defaults.LogMessageTimestampType
  val MessageTimestampDifferenceMaxMs = kafka.server.Defaults.LogMessageTimestampDifferenceMaxMs
  val LeaderReplicationThrottledReplicas = Collections.emptyList[String]()
  val FollowerReplicationThrottledReplicas = Collections.emptyList[String]()
  val MessageDownConversionEnable = kafka.server.Defaults.MessageDownConversionEnable
}

case class LogConfig(props: java.util.Map[_, _], overriddenConfigs: Set[String] = Set.empty)
  extends AbstractConfig(LogConfig.configDef, props, false) {
  /**
   * Important note: Any configuration parameter that is passed along from KafkaConfig to LogConfig
   * should also go in [[LogConfig.extractLogConfigMap()]].
   */
  val segmentSize = getInt(TopicConfig.SEGMENT_BYTES_CONFIG)
  val segmentMs = getLong(TopicConfig.SEGMENT_MS_CONFIG)
  val segmentJitterMs = getLong(TopicConfig.SEGMENT_JITTER_MS_CONFIG)
  val maxIndexSize = getInt(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG)
  val flushInterval = getLong(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG)
  val flushMs = getLong(TopicConfig.FLUSH_MS_CONFIG)
  val retentionSize = getLong(TopicConfig.RETENTION_BYTES_CONFIG)
  val retentionMs = getLong(TopicConfig.RETENTION_MS_CONFIG)
  val maxMessageSize = getInt(TopicConfig.MAX_MESSAGE_BYTES_CONFIG)
  val indexInterval = getInt(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG)
  val fileDeleteDelayMs = getLong(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG)
  val deleteRetentionMs = getLong(TopicConfig.DELETE_RETENTION_MS_CONFIG)
  val compactionLagMs = getLong(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG)
  val maxCompactionLagMs = getLong(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG)
  val minCleanableRatio = getDouble(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG)
  val compact = getList(TopicConfig.CLEANUP_POLICY_CONFIG).asScala.map(_.toLowerCase(Locale.ROOT)).contains(TopicConfig.CLEANUP_POLICY_COMPACT)
  val delete = getList(TopicConfig.CLEANUP_POLICY_CONFIG).asScala.map(_.toLowerCase(Locale.ROOT)).contains(TopicConfig.CLEANUP_POLICY_DELETE)
  val uncleanLeaderElectionEnable = getBoolean(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG)
  val minInSyncReplicas = getInt(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)
  val compressionType = getString(TopicConfig.COMPRESSION_TYPE_CONFIG).toLowerCase(Locale.ROOT)
  val preallocate = getBoolean(TopicConfig.PREALLOCATE_CONFIG)

  /* See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for details */
  @deprecated("3.0")
  val messageFormatVersion = MetadataVersion.fromVersionString(getString(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG))

  val messageTimestampType = TimestampType.forName(getString(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG))
  val messageTimestampDifferenceMaxMs = getLong(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG).longValue
  val LeaderReplicationThrottledReplicas = getList(LogConfig.LeaderReplicationThrottledReplicasProp)
  val FollowerReplicationThrottledReplicas = getList(LogConfig.FollowerReplicationThrottledReplicasProp)
  val messageDownConversionEnable = getBoolean(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG)

  class RemoteLogConfig {
    val remoteStorageEnable = getBoolean(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG)

    val localRetentionMs: Long = {
      val localLogRetentionMs = getLong(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG)

      // -2 indicates to derive value from retentionMs property.
      if(localLogRetentionMs == -2) retentionMs
      else {
        // Added validation here to check the effective value should not be more than RetentionMs.
        if(localLogRetentionMs == -1 && retentionMs != -1) {
          throw new ConfigException(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, localLogRetentionMs, s"Value must not be -1 as ${TopicConfig.RETENTION_MS_CONFIG} value is set as $retentionMs.")
        }

        if (localLogRetentionMs > retentionMs) {
          throw new ConfigException(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, localLogRetentionMs, s"Value must not be more than property: ${TopicConfig.RETENTION_MS_CONFIG} value.")
        }

        localLogRetentionMs
      }
    }

    val localRetentionBytes: Long = {
      val localLogRetentionBytes = getLong(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG)

      // -2 indicates to derive value from retentionSize property.
      if(localLogRetentionBytes == -2) retentionSize
      else {
        // Added validation here to check the effective value should not be more than RetentionBytes.
        if(localLogRetentionBytes == -1 && retentionSize != -1) {
          throw new ConfigException(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, localLogRetentionBytes, s"Value must not be -1 as ${TopicConfig.RETENTION_BYTES_CONFIG} value is set as $retentionSize.")
        }

        if (localLogRetentionBytes > retentionSize) {
          throw new ConfigException(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, localLogRetentionBytes, s"Value must not be more than property: ${TopicConfig.RETENTION_BYTES_CONFIG} value.")
        }

        localLogRetentionBytes
      }
    }
  }

  private val _remoteLogConfig = new RemoteLogConfig()
  def remoteLogConfig = _remoteLogConfig

  @nowarn("cat=deprecation")
  def recordVersion = messageFormatVersion.highestSupportedRecordVersion

  def randomSegmentJitter: Long =
    if (segmentJitterMs == 0) 0 else Utils.abs(scala.util.Random.nextInt()) % math.min(segmentJitterMs, segmentMs)

  def maxSegmentMs: Long = {
    if (compact && maxCompactionLagMs > 0) math.min(maxCompactionLagMs, segmentMs)
    else segmentMs
  }

  def initFileSize: Int = {
    if (preallocate)
      segmentSize
    else
      0
  }

  def overriddenConfigsAsLoggableString: String = {
    val overriddenTopicProps = props.asScala.collect {
      case (k: String, v) if overriddenConfigs.contains(k) => (k, v.asInstanceOf[AnyRef])
    }
    ConfigUtils.configMapToRedactedString(overriddenTopicProps.asJava, configDef)
  }
}

object LogConfig {

  def main(args: Array[String]): Unit = {
    println(configDef.toHtml(4, (config: String) => "topicconfigs_" + config))
  }

  // Leave these out of TopicConfig for now as they are replication quota configs
  val LeaderReplicationThrottledReplicasProp = "leader.replication.throttled.replicas"
  val FollowerReplicationThrottledReplicasProp = "follower.replication.throttled.replicas"

  val LeaderReplicationThrottledReplicasDoc = "A list of replicas for which log replication should be throttled on " +
    "the leader side. The list should describe a set of replicas in the form " +
    "[PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle " +
    "all replicas for this topic."
  val FollowerReplicationThrottledReplicasDoc = "A list of replicas for which log replication should be throttled on " +
    "the follower side. The list should describe a set of " + "replicas in the form " +
    "[PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle " +
    "all replicas for this topic."

  private[log] val ServerDefaultHeaderName = "Server Default Property"

  val configsWithNoServerDefaults: Set[String] = Set(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG,
    TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG)

  // Package private for testing
  private[log] class LogConfigDef(base: ConfigDef) extends ConfigDef(base) {
    def this() = this(new ConfigDef)

    private final val serverDefaultConfigNames = mutable.Map[String, String]()
    base match {
      case b: LogConfigDef => serverDefaultConfigNames ++= b.serverDefaultConfigNames
      case _ =>
    }

    def define(name: String, defType: ConfigDef.Type, defaultValue: Any, validator: Validator,
               importance: ConfigDef.Importance, doc: String, serverDefaultConfigName: String): LogConfigDef = {
      super.define(name, defType, defaultValue, validator, importance, doc)
      serverDefaultConfigNames.put(name, serverDefaultConfigName)
      this
    }

    def define(name: String, defType: ConfigDef.Type, defaultValue: Any, importance: ConfigDef.Importance,
               documentation: String, serverDefaultConfigName: String): LogConfigDef = {
      super.define(name, defType, defaultValue, importance, documentation)
      serverDefaultConfigNames.put(name, serverDefaultConfigName)
      this
    }

    def define(name: String, defType: ConfigDef.Type, importance: ConfigDef.Importance, documentation: String,
               serverDefaultConfigName: String): LogConfigDef = {
      super.define(name, defType, importance, documentation)
      serverDefaultConfigNames.put(name, serverDefaultConfigName)
      this
    }

    override def headers = List("Name", "Description", "Type", "Default", "Valid Values", ServerDefaultHeaderName,
      "Importance").asJava

    override def getConfigValue(key: ConfigKey, headerName: String): String = {
      headerName match {
        case ServerDefaultHeaderName => serverDefaultConfigNames.getOrElse(key.name, null)
        case _ => super.getConfigValue(key, headerName)
      }
    }

    def serverConfigName(configName: String): Option[String] = serverDefaultConfigNames.get(configName)
  }

  // Package private for testing, return a copy since it's a mutable global variable
  private[kafka] def configDefCopy: LogConfigDef = new LogConfigDef(configDef)

  private val configDef: LogConfigDef = {
    import org.apache.kafka.common.config.ConfigDef.Importance._
    import org.apache.kafka.common.config.ConfigDef.Range._
    import org.apache.kafka.common.config.ConfigDef.Type._
    import org.apache.kafka.common.config.ConfigDef.ValidString._

    @nowarn("cat=deprecation")
    val logConfigDef = new LogConfigDef()
      .define(TopicConfig.SEGMENT_BYTES_CONFIG, INT, Defaults.SegmentSize, atLeast(LegacyRecord.RECORD_OVERHEAD_V0), MEDIUM,
        TopicConfig.SEGMENT_BYTES_DOC, KafkaConfig.LogSegmentBytesProp)
      .define(TopicConfig.SEGMENT_MS_CONFIG, LONG, Defaults.SegmentMs, atLeast(1), MEDIUM,
        TopicConfig.SEGMENT_MS_DOC, KafkaConfig.LogRollTimeMillisProp)
      .define(TopicConfig.SEGMENT_JITTER_MS_CONFIG, LONG, Defaults.SegmentJitterMs, atLeast(0), MEDIUM,
        TopicConfig.SEGMENT_JITTER_MS_DOC, KafkaConfig.LogRollTimeJitterMillisProp)
      .define(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, INT, Defaults.MaxIndexSize, atLeast(4), MEDIUM,
        TopicConfig.SEGMENT_INDEX_BYTES_DOC, KafkaConfig.LogIndexSizeMaxBytesProp)
      .define(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, LONG, Defaults.FlushInterval, atLeast(1), MEDIUM,
        TopicConfig.FLUSH_MESSAGES_INTERVAL_DOC, KafkaConfig.LogFlushIntervalMessagesProp)
      .define(TopicConfig.FLUSH_MS_CONFIG, LONG, Defaults.FlushMs, atLeast(0), MEDIUM,
        TopicConfig.FLUSH_MS_DOC, KafkaConfig.LogFlushIntervalMsProp)
      // can be negative. See kafka.log.LogManager.cleanupSegmentsToMaintainSize
      .define(TopicConfig.RETENTION_BYTES_CONFIG, LONG, Defaults.RetentionSize, MEDIUM,
        TopicConfig.RETENTION_BYTES_DOC, KafkaConfig.LogRetentionBytesProp)
      // can be negative. See kafka.log.LogManager.cleanupExpiredSegments
      .define(TopicConfig.RETENTION_MS_CONFIG, LONG, Defaults.RetentionMs, atLeast(-1), MEDIUM,
        TopicConfig.RETENTION_MS_DOC, KafkaConfig.LogRetentionTimeMillisProp)
      .define(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, INT, Defaults.MaxMessageSize, atLeast(0), MEDIUM,
        TopicConfig.MAX_MESSAGE_BYTES_DOC, KafkaConfig.MessageMaxBytesProp)
      .define(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, INT, Defaults.IndexInterval, atLeast(0), MEDIUM,
        TopicConfig.INDEX_INTERVAL_BYTES_DOC, KafkaConfig.LogIndexIntervalBytesProp)
      .define(TopicConfig.DELETE_RETENTION_MS_CONFIG, LONG, Defaults.DeleteRetentionMs, atLeast(0), MEDIUM,
        TopicConfig.DELETE_RETENTION_MS_DOC, KafkaConfig.LogCleanerDeleteRetentionMsProp)
      .define(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, LONG, Defaults.MinCompactionLagMs, atLeast(0), MEDIUM,
        TopicConfig.MIN_COMPACTION_LAG_MS_DOC, KafkaConfig.LogCleanerMinCompactionLagMsProp)
      .define(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, LONG, Defaults.MaxCompactionLagMs, atLeast(1), MEDIUM,
        TopicConfig.MAX_COMPACTION_LAG_MS_DOC, KafkaConfig.LogCleanerMaxCompactionLagMsProp)
      .define(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, LONG, Defaults.FileDeleteDelayMs, atLeast(0), MEDIUM,
        TopicConfig.FILE_DELETE_DELAY_MS_DOC, KafkaConfig.LogDeleteDelayMsProp)
      .define(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, DOUBLE, Defaults.MinCleanableDirtyRatio, between(0, 1), MEDIUM,
        TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_DOC, KafkaConfig.LogCleanerMinCleanRatioProp)
      .define(TopicConfig.CLEANUP_POLICY_CONFIG, LIST, Defaults.CleanupPolicy, ValidList.in(TopicConfig.CLEANUP_POLICY_COMPACT, TopicConfig.CLEANUP_POLICY_DELETE), MEDIUM,
        TopicConfig.CLEANUP_POLICY_DOC, KafkaConfig.LogCleanupPolicyProp)
      .define(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, BOOLEAN, Defaults.UncleanLeaderElectionEnable,
        MEDIUM, TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_DOC, KafkaConfig.UncleanLeaderElectionEnableProp)
      .define(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, INT, Defaults.MinInSyncReplicas, atLeast(1), MEDIUM,
        TopicConfig.MIN_IN_SYNC_REPLICAS_DOC, KafkaConfig.MinInSyncReplicasProp)
      .define(TopicConfig.COMPRESSION_TYPE_CONFIG, STRING, Defaults.CompressionType, in(BrokerCompressionType.names.asScala.toSeq:_*),
        MEDIUM, TopicConfig.COMPRESSION_TYPE_DOC, KafkaConfig.CompressionTypeProp)
      .define(TopicConfig.PREALLOCATE_CONFIG, BOOLEAN, Defaults.PreAllocateEnable, MEDIUM, TopicConfig.PREALLOCATE_DOC,
        KafkaConfig.LogPreAllocateProp)
      .define(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, STRING, Defaults.MessageFormatVersion, new MetadataVersionValidator(), MEDIUM,
        TopicConfig.MESSAGE_FORMAT_VERSION_DOC, KafkaConfig.LogMessageFormatVersionProp)
      .define(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, STRING, Defaults.MessageTimestampType, in("CreateTime", "LogAppendTime"), MEDIUM,
        TopicConfig.MESSAGE_TIMESTAMP_TYPE_DOC, KafkaConfig.LogMessageTimestampTypeProp)
      .define(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, LONG, Defaults.MessageTimestampDifferenceMaxMs,
        atLeast(0), MEDIUM, TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC, KafkaConfig.LogMessageTimestampDifferenceMaxMsProp)
      .define(LeaderReplicationThrottledReplicasProp, LIST, Defaults.LeaderReplicationThrottledReplicas, ThrottledReplicaListValidator, MEDIUM,
        LeaderReplicationThrottledReplicasDoc, LeaderReplicationThrottledReplicasProp)
      .define(FollowerReplicationThrottledReplicasProp, LIST, Defaults.FollowerReplicationThrottledReplicas, ThrottledReplicaListValidator, MEDIUM,
        FollowerReplicationThrottledReplicasDoc, FollowerReplicationThrottledReplicasProp)
      .define(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, BOOLEAN, Defaults.MessageDownConversionEnable, LOW,
        TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_DOC, KafkaConfig.LogMessageDownConversionEnableProp)

    // RemoteLogStorageEnableProp, LocalLogRetentionMsProp, LocalLogRetentionBytesProp do not have server default
    // config names.
    logConfigDef
      // This define method is not overridden in LogConfig as these configs do not have server defaults yet.
      .defineInternal(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, BOOLEAN, Defaults.RemoteLogStorageEnable, null, MEDIUM,
        TopicConfig.REMOTE_LOG_STORAGE_ENABLE_DOC)
      .defineInternal(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, LONG, Defaults.LocalRetentionMs, atLeast(-2), MEDIUM,
        TopicConfig.LOCAL_LOG_RETENTION_MS_DOC)
      .defineInternal(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, LONG, Defaults.LocalRetentionBytes, atLeast(-2), MEDIUM,
        TopicConfig.LOCAL_LOG_RETENTION_BYTES_DOC)

    logConfigDef
  }

  def apply(): LogConfig = LogConfig(new Properties())

  def configNames: Seq[String] = configDef.names.asScala.toSeq.sorted

  def serverConfigName(configName: String): Option[String] = configDef.serverConfigName(configName)

  def configType(configName: String): Option[ConfigDef.Type] = {
    Option(configDef.configKeys.get(configName)).map(_.`type`)
  }

  /**
   * Create a log config instance using the given properties and defaults
   */
  def fromProps(defaults: java.util.Map[_ <: Object, _ <: Object], overrides: Properties): LogConfig = {
    val props = new Properties()
    defaults.forEach { (k, v) => props.put(k, v) }
    props ++= overrides
    val overriddenKeys = overrides.keySet.asScala.map(_.asInstanceOf[String]).toSet
    new LogConfig(props, overriddenKeys)
  }

  /**
   * Check that property names are valid
   */
  def validateNames(props: Properties): Unit = {
    val names = configNames
    for(name <- props.asScala.keys)
      if (!names.contains(name))
        throw new InvalidConfigurationException(s"Unknown topic config name: $name")
  }

  private[kafka] def configKeys: Map[String, ConfigKey] = configDef.configKeys.asScala

  def validateValues(props: java.util.Map[_, _]): Unit = {
    val minCompactionLag =  props.get(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG).asInstanceOf[Long]
    val maxCompactionLag =  props.get(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG).asInstanceOf[Long]
    if (minCompactionLag > maxCompactionLag) {
      throw new InvalidConfigurationException(s"conflict topic config setting ${TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG} " +
        s"($minCompactionLag) > ${TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG} ($maxCompactionLag)")
    }
  }

  /**
   * Check that the given properties contain only valid log config names and that all values can be parsed and are valid
   */
  def validate(props: Properties): Unit = {
    validateNames(props)
    val valueMaps = configDef.parse(props)
    validateValues(valueMaps)
  }

  /**
   * Maps topic configurations to their equivalent broker configurations.
   *
   * Topics can be configured either by setting their dynamic topic configurations, or by
   * setting equivalent broker configurations. For historical reasons, the equivalent broker
   * configurations have different names. This table maps each topic configuration to its
   * equivalent broker configurations.
   *
   * In some cases, the equivalent broker configurations must be transformed before they
   * can be used. For example, log.roll.hours must be converted to milliseconds before it
   * can be used as the value of segment.ms.
   *
   * The broker configurations will be used in the order specified here. In other words, if
   * both the first and the second synonyms are configured, we will use only the value of
   * the first synonym and ignore the second.
   */
  @nowarn("cat=deprecation")
  val AllTopicConfigSynonyms = Map(
    TopicConfig.SEGMENT_BYTES_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogSegmentBytesProp)),
    TopicConfig.SEGMENT_MS_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogRollTimeMillisProp),
      new ConfigSynonym(KafkaConfig.LogRollTimeHoursProp, HOURS_TO_MILLISECONDS)),
    TopicConfig.SEGMENT_JITTER_MS_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogRollTimeJitterMillisProp),
      new ConfigSynonym(KafkaConfig.LogRollTimeJitterHoursProp, HOURS_TO_MILLISECONDS)),
    TopicConfig.SEGMENT_INDEX_BYTES_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogIndexSizeMaxBytesProp)),
    TopicConfig.SEGMENT_INDEX_BYTES_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogFlushIntervalMessagesProp)),
    TopicConfig.FLUSH_MS_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogFlushIntervalMsProp),
      new ConfigSynonym(KafkaConfig.LogFlushSchedulerIntervalMsProp)),
    TopicConfig.RETENTION_BYTES_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogRetentionBytesProp)),
    TopicConfig.RETENTION_MS_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogRetentionTimeMillisProp),
      new ConfigSynonym(KafkaConfig.LogRetentionTimeMinutesProp, MINUTES_TO_MILLISECONDS),
      new ConfigSynonym(KafkaConfig.LogRetentionTimeHoursProp, HOURS_TO_MILLISECONDS)),
    TopicConfig.MAX_MESSAGE_BYTES_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.MessageMaxBytesProp)),
    TopicConfig.INDEX_INTERVAL_BYTES_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogIndexIntervalBytesProp)),
    TopicConfig.DELETE_RETENTION_MS_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogCleanerDeleteRetentionMsProp)),
    TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogCleanerMinCompactionLagMsProp)),
    TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogCleanerMaxCompactionLagMsProp)),
    TopicConfig.FILE_DELETE_DELAY_MS_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogDeleteDelayMsProp)),
    TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogCleanerMinCleanRatioProp)),
    TopicConfig.CLEANUP_POLICY_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogCleanupPolicyProp)),
    TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.UncleanLeaderElectionEnableProp)),
    TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.MinInSyncReplicasProp)),
    TopicConfig.COMPRESSION_TYPE_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.CompressionTypeProp)),
    TopicConfig.PREALLOCATE_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogPreAllocateProp)),
    TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogMessageFormatVersionProp)),
    TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogMessageTimestampTypeProp)),
    TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp)),
    TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG -> asList(
      new ConfigSynonym(KafkaConfig.LogMessageDownConversionEnableProp)),
  ).asJava

  /**
   * Map topic config to the broker config with highest priority. Some of these have additional synonyms
   * that can be obtained using [[kafka.server.DynamicBrokerConfig#brokerConfigSynonyms]]
   * or using [[AllTopicConfigSynonyms]]
   */
  val TopicConfigSynonyms = AllTopicConfigSynonyms.asScala.map {
    case (k, v) => k -> v.get(0).name()
  }

  /**
   * Copy the subset of properties that are relevant to Logs. The individual properties
   * are listed here since the names are slightly different in each Config class...
   */
  @nowarn("cat=deprecation")
  def extractLogConfigMap(
    kafkaConfig: KafkaConfig
  ): java.util.Map[String, Object] = {
    val logProps = new java.util.HashMap[String, Object]()
    logProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, kafkaConfig.logSegmentBytes)
    logProps.put(TopicConfig.SEGMENT_MS_CONFIG, kafkaConfig.logRollTimeMillis)
    logProps.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, kafkaConfig.logRollTimeJitterMillis)
    logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, kafkaConfig.logIndexSizeMaxBytes)
    logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, kafkaConfig.logFlushIntervalMessages)
    logProps.put(TopicConfig.FLUSH_MS_CONFIG, kafkaConfig.logFlushIntervalMs)
    logProps.put(TopicConfig.RETENTION_BYTES_CONFIG, kafkaConfig.logRetentionBytes)
    logProps.put(TopicConfig.RETENTION_MS_CONFIG, kafkaConfig.logRetentionTimeMillis: java.lang.Long)
    logProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, kafkaConfig.messageMaxBytes)
    logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, kafkaConfig.logIndexIntervalBytes)
    logProps.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, kafkaConfig.logCleanerDeleteRetentionMs)
    logProps.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, kafkaConfig.logCleanerMinCompactionLagMs)
    logProps.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, kafkaConfig.logCleanerMaxCompactionLagMs)
    logProps.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, kafkaConfig.logDeleteDelayMs)
    logProps.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, kafkaConfig.logCleanerMinCleanRatio)
    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, kafkaConfig.logCleanupPolicy)
    logProps.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, kafkaConfig.minInSyncReplicas)
    logProps.put(TopicConfig.COMPRESSION_TYPE_CONFIG, kafkaConfig.compressionType)
    logProps.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, kafkaConfig.uncleanLeaderElectionEnable)
    logProps.put(TopicConfig.PREALLOCATE_CONFIG, kafkaConfig.logPreAllocateEnable)
    logProps.put(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG, kafkaConfig.logMessageFormatVersion.version)
    logProps.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, kafkaConfig.logMessageTimestampType.name)
    logProps.put(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, kafkaConfig.logMessageTimestampDifferenceMaxMs: java.lang.Long)
    logProps.put(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, kafkaConfig.logMessageDownConversionEnable: java.lang.Boolean)
    logProps
  }

  def shouldIgnoreMessageFormatVersion(interBrokerProtocolVersion: MetadataVersion): Boolean =
    interBrokerProtocolVersion.isAtLeast(IBP_3_0_IV1)

  class MessageFormatVersion(messageFormatVersionString: String, interBrokerProtocolVersionString: String) {
    val messageFormatVersion = MetadataVersion.fromVersionString(messageFormatVersionString)
    private val interBrokerProtocolVersion = MetadataVersion.fromVersionString(interBrokerProtocolVersionString)

    def shouldIgnore: Boolean = shouldIgnoreMessageFormatVersion(interBrokerProtocolVersion)

    def shouldWarn: Boolean =
      interBrokerProtocolVersion.isAtLeast(IBP_3_0_IV1) && messageFormatVersion.highestSupportedRecordVersion.precedes(RecordVersion.V2)

    @nowarn("cat=deprecation")
    def topicWarningMessage(topicName: String): String = {
      s"Topic configuration ${TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG} with value `$messageFormatVersionString` is ignored " +
      s"for `$topicName` because the inter-broker protocol version `$interBrokerProtocolVersionString` is " +
      "greater or equal than 3.0. This configuration is deprecated and it will be removed in Apache Kafka 4.0."
    }

    @nowarn("cat=deprecation")
    def brokerWarningMessage: String = {
      s"Broker configuration ${KafkaConfig.LogMessageFormatVersionProp} with value $messageFormatVersionString is ignored " +
      s"because the inter-broker protocol version `$interBrokerProtocolVersionString` is greater or equal than 3.0. " +
      "This configuration is deprecated and it will be removed in Apache Kafka 4.0."
    }
  }

}
