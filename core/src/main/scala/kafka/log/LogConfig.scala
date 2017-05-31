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

import java.util.{Collections, Locale, Properties}

import scala.collection.JavaConverters._
import kafka.api.ApiVersion
import kafka.message.{BrokerCompressionCodec, Message}
import kafka.server.{KafkaConfig, ThrottledReplicaListValidator}
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, TopicConfigs}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable
import org.apache.kafka.common.config.ConfigDef.{ConfigKey, ValidList, Validator}

object Defaults {
  val SegmentSize = kafka.server.Defaults.LogSegmentBytes
  val SegmentMs = kafka.server.Defaults.LogRollHours * 60 * 60 * 1000L
  val SegmentJitterMs = kafka.server.Defaults.LogRollJitterHours * 60 * 60 * 1000L
  val FlushInterval = kafka.server.Defaults.LogFlushIntervalMessages
  val FlushMs = kafka.server.Defaults.LogFlushSchedulerIntervalMs
  val RetentionSize = kafka.server.Defaults.LogRetentionBytes
  val RetentionMs = kafka.server.Defaults.LogRetentionHours * 60 * 60 * 1000L
  val MaxMessageSize = kafka.server.Defaults.MessageMaxBytes
  val MaxIndexSize = kafka.server.Defaults.LogIndexSizeMaxBytes
  val IndexInterval = kafka.server.Defaults.LogIndexIntervalBytes
  val FileDeleteDelayMs = kafka.server.Defaults.LogDeleteDelayMs
  val DeleteRetentionMs = kafka.server.Defaults.LogCleanerDeleteRetentionMs
  val MinCompactionLagMs = kafka.server.Defaults.LogCleanerMinCompactionLagMs
  val MinCleanableDirtyRatio = kafka.server.Defaults.LogCleanerMinCleanRatio
  val Compact = kafka.server.Defaults.LogCleanupPolicy
  val UncleanLeaderElectionEnable = kafka.server.Defaults.UncleanLeaderElectionEnable
  val MinInSyncReplicas = kafka.server.Defaults.MinInSyncReplicas
  val CompressionType = kafka.server.Defaults.CompressionType
  val PreAllocateEnable = kafka.server.Defaults.LogPreAllocateEnable
  val MessageFormatVersion = kafka.server.Defaults.LogMessageFormatVersion
  val MessageTimestampType = kafka.server.Defaults.LogMessageTimestampType
  val MessageTimestampDifferenceMaxMs = kafka.server.Defaults.LogMessageTimestampDifferenceMaxMs
  val LeaderReplicationThrottledReplicas = Collections.emptyList[String]()
  val FollowerReplicationThrottledReplicas = Collections.emptyList[String]()
  val MaxIdMapSnapshots = kafka.server.Defaults.MaxIdMapSnapshots
}

case class LogConfig(props: java.util.Map[_, _]) extends AbstractConfig(LogConfig.configDef, props, false) {
  /**
   * Important note: Any configuration parameter that is passed along from KafkaConfig to LogConfig
   * should also go in [[kafka.server.KafkaServer.copyKafkaConfigToLog]].
   */
  val segmentSize = getInt(LogConfig.SegmentBytesProp)
  val segmentMs = getLong(LogConfig.SegmentMsProp)
  val segmentJitterMs = getLong(LogConfig.SegmentJitterMsProp)
  val maxIndexSize = getInt(LogConfig.SegmentIndexBytesProp)
  val flushInterval = getLong(LogConfig.FlushMessagesProp)
  val flushMs = getLong(LogConfig.FlushMsProp)
  val retentionSize = getLong(LogConfig.RetentionBytesProp)
  val retentionMs = getLong(LogConfig.RetentionMsProp)
  val maxMessageSize = getInt(LogConfig.MaxMessageBytesProp)
  val indexInterval = getInt(LogConfig.IndexIntervalBytesProp)
  val fileDeleteDelayMs = getLong(LogConfig.FileDeleteDelayMsProp)
  val deleteRetentionMs = getLong(LogConfig.DeleteRetentionMsProp)
  val compactionLagMs = getLong(LogConfig.MinCompactionLagMsProp)
  val minCleanableRatio = getDouble(LogConfig.MinCleanableDirtyRatioProp)
  val compact = getList(LogConfig.CleanupPolicyProp).asScala.map(_.toLowerCase(Locale.ROOT)).contains(LogConfig.Compact)
  val delete = getList(LogConfig.CleanupPolicyProp).asScala.map(_.toLowerCase(Locale.ROOT)).contains(LogConfig.Delete)
  val uncleanLeaderElectionEnable = getBoolean(LogConfig.UncleanLeaderElectionEnableProp)
  val minInSyncReplicas = getInt(LogConfig.MinInSyncReplicasProp)
  val compressionType = getString(LogConfig.CompressionTypeProp).toLowerCase(Locale.ROOT)
  val preallocate = getBoolean(LogConfig.PreAllocateEnableProp)
  val messageFormatVersion = ApiVersion(getString(LogConfig.MessageFormatVersionProp))
  val messageTimestampType = TimestampType.forName(getString(LogConfig.MessageTimestampTypeProp))
  val messageTimestampDifferenceMaxMs = getLong(LogConfig.MessageTimestampDifferenceMaxMsProp).longValue
  val LeaderReplicationThrottledReplicas = getList(LogConfig.LeaderReplicationThrottledReplicasProp)
  val FollowerReplicationThrottledReplicas = getList(LogConfig.FollowerReplicationThrottledReplicasProp)

  def randomSegmentJitter: Long =
    if (segmentJitterMs == 0) 0 else Utils.abs(scala.util.Random.nextInt()) % math.min(segmentJitterMs, segmentMs)
}

object LogConfig {

  def main(args: Array[String]) {
    println(configDef.toHtmlTable)
  }

  val SegmentBytesProp = TopicConfigs.SEGMENT_BYTES
  val SegmentMsProp = TopicConfigs.SEGMENT_MS
  val SegmentJitterMsProp = TopicConfigs.SEGMENT_JITTER_MS
  val SegmentIndexBytesProp = TopicConfigs.SEGMENT_INDEX_BYTES
  val FlushMessagesProp = TopicConfigs.FLUSH_MESSAGES_INTERVAL
  val FlushMsProp = TopicConfigs.FLUSH_MS
  val RetentionBytesProp = TopicConfigs.RETENTION_BYTES
  val RetentionMsProp = TopicConfigs.RETENTION_MS
  val MaxMessageBytesProp = TopicConfigs.MAX_MESSAGE_BYTES
  val IndexIntervalBytesProp = TopicConfigs.INDEX_INTERVAL_BYTES
  val DeleteRetentionMsProp = TopicConfigs.DELETE_RETENTION_MS
  val MinCompactionLagMsProp = TopicConfigs.MIN_COMPACTION_LAG_MS
  val FileDeleteDelayMsProp = TopicConfigs.FILE_DELETE_DELAY_MS
  val MinCleanableDirtyRatioProp = TopicConfigs.MIN_CLEANABLE_DIRTY_RATIO
  val CleanupPolicyProp = TopicConfigs.CLEANUP_POLICY
  val Delete = TopicConfigs.CLEANUP_POLICY_DELETE
  val Compact = TopicConfigs.CLEANUP_POLICY_COMPACT
  val UncleanLeaderElectionEnableProp = TopicConfigs.UNCLEAN_LEADER_ELECTION_ENABLE
  val MinInSyncReplicasProp = TopicConfigs.MIN_IN_SYNC_REPLICAS
  val CompressionTypeProp = TopicConfigs.COMPRESSION_TYPE
  val PreAllocateEnableProp = TopicConfigs.PREALLOCATE
  val MessageFormatVersionProp = TopicConfigs.MESSAGE_FORMAT_VERSION
  val MessageTimestampTypeProp = TopicConfigs.MESSAGE_TIMESTAMP_TYPE
  val MessageTimestampDifferenceMaxMsProp = TopicConfigs.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS
  val LeaderReplicationThrottledReplicasProp = TopicConfigs.LEADER_REPLICATION_THROTTLED_REPLICAS
  val FollowerReplicationThrottledReplicasProp = TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS

  val SegmentSizeDoc = TopicConfigs.SEGMENT_BYTES_DOC
  val SegmentMsDoc = TopicConfigs.SEGMENT_MS_DOC
  val SegmentJitterMsDoc = TopicConfigs.SEGMENT_JITTER_MS_DOC
  val MaxIndexSizeDoc = TopicConfigs.SEGMENT_INDEX_BYTES_DOC
  val FlushIntervalDoc = TopicConfigs.FLUSH_MESSAGES_INTERVAL_DOC
  val FlushMsDoc = TopicConfigs.FLUSH_MS_DOC
  val RetentionSizeDoc = TopicConfigs.RETENTION_BYTES_DOC
  val RetentionMsDoc = TopicConfigs.RETENTION_MS_DOC
  val MaxMessageSizeDoc = TopicConfigs.MAX_MESSAGE_BYTES_DOC
  val IndexIntervalDoc = TopicConfigs.INDEX_INTERVAL_BYTES_DOCS
  val FileDeleteDelayMsDoc = TopicConfigs.FILE_DELETE_DELAY_MS_DOC
  val DeleteRetentionMsDoc = TopicConfigs.DELETE_RETENTION_MS_DOC
  val MinCompactionLagMsDoc = TopicConfigs.MIN_COMPACTION_LAG_MS_DOC
  val MinCleanableRatioDoc = TopicConfigs.MIN_CLEANABLE_DIRTY_RATIO_DOC
  val CompactDoc = TopicConfigs.CLEANUP_POLICY_DOC
  val UncleanLeaderElectionEnableDoc = TopicConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_DOC
  val MinInSyncReplicasDoc = TopicConfigs.MIN_IN_SYNC_REPLICAS_DOC
  val CompressionTypeDoc = TopicConfigs.COMPRESSION_TYPE_DOC
  val PreAllocateEnableDoc = TopicConfigs.PREALLOCATE_DOC
  val MessageFormatVersionDoc = TopicConfigs.MESSAGE_FORMAT_VERSION_DOC
  val MessageTimestampTypeDoc = TopicConfigs.MESSAGE_TIMESTAMP_TYPE_DOC
  val MessageTimestampDifferenceMaxMsDoc = TopicConfigs.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC
  val LeaderReplicationThrottledReplicasDoc = TopicConfigs.LEADER_REPLICATION_THROTTLED_REPLICAS_DOC
  val FollowerReplicationThrottledReplicasDoc = TopicConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_DOC

  private class LogConfigDef extends ConfigDef {

    private final val serverDefaultConfigNames = mutable.Map[String, String]()

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

    override def headers = List("Name", "Description", "Type", "Default", "Valid Values", "Server Default Property", "Importance").asJava

    override def getConfigValue(key: ConfigKey, headerName: String): String = {
      headerName match {
        case "Server Default Property" => serverDefaultConfigNames.get(key.name).get
        case _ => super.getConfigValue(key, headerName)
      }
    }

    def serverConfigName(configName: String): Option[String] = serverDefaultConfigNames.get(configName)
  }

  private val configDef: LogConfigDef = {
    import org.apache.kafka.common.config.ConfigDef.Importance._
    import org.apache.kafka.common.config.ConfigDef.Range._
    import org.apache.kafka.common.config.ConfigDef.Type._
    import org.apache.kafka.common.config.ConfigDef.ValidString._

    new LogConfigDef()
      .define(SegmentBytesProp, INT, Defaults.SegmentSize, atLeast(Message.MinMessageOverhead), MEDIUM,
        SegmentSizeDoc, KafkaConfig.LogSegmentBytesProp)
      .define(SegmentMsProp, LONG, Defaults.SegmentMs, atLeast(0), MEDIUM, SegmentMsDoc,
        KafkaConfig.LogRollTimeMillisProp)
      .define(SegmentJitterMsProp, LONG, Defaults.SegmentJitterMs, atLeast(0), MEDIUM, SegmentJitterMsDoc,
        KafkaConfig.LogRollTimeJitterMillisProp)
      .define(SegmentIndexBytesProp, INT, Defaults.MaxIndexSize, atLeast(0), MEDIUM, MaxIndexSizeDoc,
        KafkaConfig.LogIndexSizeMaxBytesProp)
      .define(FlushMessagesProp, LONG, Defaults.FlushInterval, atLeast(0), MEDIUM, FlushIntervalDoc,
        KafkaConfig.LogFlushIntervalMessagesProp)
      .define(FlushMsProp, LONG, Defaults.FlushMs, atLeast(0), MEDIUM, FlushMsDoc,
        KafkaConfig.LogFlushIntervalMsProp)
      // can be negative. See kafka.log.LogManager.cleanupSegmentsToMaintainSize
      .define(RetentionBytesProp, LONG, Defaults.RetentionSize, MEDIUM, RetentionSizeDoc,
        KafkaConfig.LogRetentionBytesProp)
      // can be negative. See kafka.log.LogManager.cleanupExpiredSegments
      .define(RetentionMsProp, LONG, Defaults.RetentionMs, MEDIUM, RetentionMsDoc,
        KafkaConfig.LogRetentionTimeMillisProp)
      .define(MaxMessageBytesProp, INT, Defaults.MaxMessageSize, atLeast(0), MEDIUM, MaxMessageSizeDoc,
        KafkaConfig.MessageMaxBytesProp)
      .define(IndexIntervalBytesProp, INT, Defaults.IndexInterval, atLeast(0), MEDIUM, IndexIntervalDoc,
        KafkaConfig.LogIndexIntervalBytesProp)
      .define(DeleteRetentionMsProp, LONG, Defaults.DeleteRetentionMs, atLeast(0), MEDIUM,
        DeleteRetentionMsDoc, KafkaConfig.LogCleanerDeleteRetentionMsProp)
      .define(MinCompactionLagMsProp, LONG, Defaults.MinCompactionLagMs, atLeast(0), MEDIUM, MinCompactionLagMsDoc,
        KafkaConfig.LogCleanerMinCompactionLagMsProp)
      .define(FileDeleteDelayMsProp, LONG, Defaults.FileDeleteDelayMs, atLeast(0), MEDIUM, FileDeleteDelayMsDoc,
        KafkaConfig.LogDeleteDelayMsProp)
      .define(MinCleanableDirtyRatioProp, DOUBLE, Defaults.MinCleanableDirtyRatio, between(0, 1), MEDIUM,
        MinCleanableRatioDoc, KafkaConfig.LogCleanerMinCleanRatioProp)
      .define(CleanupPolicyProp, LIST, Defaults.Compact, ValidList.in(LogConfig.Compact, LogConfig.Delete), MEDIUM, CompactDoc,
        KafkaConfig.LogCleanupPolicyProp)
      .define(UncleanLeaderElectionEnableProp, BOOLEAN, Defaults.UncleanLeaderElectionEnable,
        MEDIUM, UncleanLeaderElectionEnableDoc, KafkaConfig.UncleanLeaderElectionEnableProp)
      .define(MinInSyncReplicasProp, INT, Defaults.MinInSyncReplicas, atLeast(1), MEDIUM, MinInSyncReplicasDoc,
        KafkaConfig.MinInSyncReplicasProp)
      .define(CompressionTypeProp, STRING, Defaults.CompressionType, in(BrokerCompressionCodec.brokerCompressionOptions:_*),
        MEDIUM, CompressionTypeDoc, KafkaConfig.CompressionTypeProp)
      .define(PreAllocateEnableProp, BOOLEAN, Defaults.PreAllocateEnable, MEDIUM, PreAllocateEnableDoc,
        KafkaConfig.LogPreAllocateProp)
      .define(MessageFormatVersionProp, STRING, Defaults.MessageFormatVersion, MEDIUM, MessageFormatVersionDoc,
        KafkaConfig.LogMessageFormatVersionProp)
      .define(MessageTimestampTypeProp, STRING, Defaults.MessageTimestampType, MEDIUM, MessageTimestampTypeDoc,
        KafkaConfig.LogMessageTimestampTypeProp)
      .define(MessageTimestampDifferenceMaxMsProp, LONG, Defaults.MessageTimestampDifferenceMaxMs,
        atLeast(0), MEDIUM, MessageTimestampDifferenceMaxMsDoc, KafkaConfig.LogMessageTimestampDifferenceMaxMsProp)
      .define(LeaderReplicationThrottledReplicasProp, LIST, Defaults.LeaderReplicationThrottledReplicas, ThrottledReplicaListValidator, MEDIUM,
        LeaderReplicationThrottledReplicasDoc, LeaderReplicationThrottledReplicasProp)
      .define(FollowerReplicationThrottledReplicasProp, LIST, Defaults.FollowerReplicationThrottledReplicas, ThrottledReplicaListValidator, MEDIUM,
        FollowerReplicationThrottledReplicasDoc, FollowerReplicationThrottledReplicasProp)
  }

  def apply(): LogConfig = LogConfig(new Properties())

  def configNames: Seq[String] = configDef.names.asScala.toSeq.sorted

  def serverConfigName(configName: String): Option[String] = configDef.serverConfigName(configName)

  /**
   * Create a log config instance using the given properties and defaults
   */
  def fromProps(defaults: java.util.Map[_ <: Object, _ <: Object], overrides: Properties): LogConfig = {
    val props = new Properties()
    props.putAll(defaults)
    props.putAll(overrides)
    LogConfig(props)
  }

  /**
   * Check that property names are valid
   */
  def validateNames(props: Properties) {
    val names = configNames
    for(name <- props.asScala.keys)
      if (!names.contains(name))
        throw new InvalidConfigurationException(s"Unknown topic config name: $name")
  }

  /**
   * Check that the given properties contain only valid log config names and that all values can be parsed and are valid
   */
  def validate(props: Properties) {
    validateNames(props)
    configDef.parse(props)
  }

}
