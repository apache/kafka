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
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
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

  val Delete = "delete"
  val Compact = "compact"

  val SegmentBytesProp = "segment.bytes"
  val SegmentMsProp = "segment.ms"
  val SegmentJitterMsProp = "segment.jitter.ms"
  val SegmentIndexBytesProp = "segment.index.bytes"
  val FlushMessagesProp = "flush.messages"
  val FlushMsProp = "flush.ms"
  val RetentionBytesProp = "retention.bytes"
  val RetentionMsProp = "retention.ms"
  val MaxMessageBytesProp = "max.message.bytes"
  val IndexIntervalBytesProp = "index.interval.bytes"
  val DeleteRetentionMsProp = "delete.retention.ms"
  val MinCompactionLagMsProp = "min.compaction.lag.ms"
  val FileDeleteDelayMsProp = "file.delete.delay.ms"
  val MinCleanableDirtyRatioProp = "min.cleanable.dirty.ratio"
  val CleanupPolicyProp = "cleanup.policy"
  val UncleanLeaderElectionEnableProp = "unclean.leader.election.enable"
  val MinInSyncReplicasProp = "min.insync.replicas"
  val CompressionTypeProp = "compression.type"
  val PreAllocateEnableProp = "preallocate"
  val MessageFormatVersionProp = "message.format.version"
  val MessageTimestampTypeProp = "message.timestamp.type"
  val MessageTimestampDifferenceMaxMsProp = "message.timestamp.difference.max.ms"
  val LeaderReplicationThrottledReplicasProp = "leader.replication.throttled.replicas"
  val FollowerReplicationThrottledReplicasProp = "follower.replication.throttled.replicas"

  val SegmentSizeDoc = "This configuration controls the segment file size for " +
    "the log. Retention and cleaning is always done a file at a time so a larger " +
    "segment size means fewer files but less granular control over retention."
  val SegmentMsDoc = "This configuration controls the period of time after " +
    "which Kafka will force the log to roll even if the segment file isn't full " +
    "to ensure that retention can delete or compact old data."
  val SegmentJitterMsDoc = "The maximum random jitter subtracted from the scheduled segment roll time to avoid" +
    " thundering herds of segment rolling"
  val FlushIntervalDoc = "This setting allows specifying an interval at which we " +
    "will force an fsync of data written to the log. For example if this was set to 1 " +
    "we would fsync after every message; if it were 5 we would fsync after every five " +
    "messages. In general we recommend you not set this and use replication for " +
    "durability and allow the operating system's background flush capabilities as it " +
    "is more efficient. This setting can be overridden on a per-topic basis (see <a " +
    "href=\"#topic-config\">the per-topic configuration section</a>)."
  val FlushMsDoc = "This setting allows specifying a time interval at which we will " +
    "force an fsync of data written to the log. For example if this was set to 1000 " +
    "we would fsync after 1000 ms had passed. In general we recommend you not set " +
    "this and use replication for durability and allow the operating system's background " +
    "flush capabilities as it is more efficient."
  val RetentionSizeDoc = "This configuration controls the maximum size a log can grow " +
    "to before we will discard old log segments to free up space if we are using the " +
    "\"delete\" retention policy. By default there is no size limit only a time limit."
  val RetentionMsDoc = "This configuration controls the maximum time we will retain a " +
    "log before we will discard old log segments to free up space if we are using the " +
    "\"delete\" retention policy. This represents an SLA on how soon consumers must read " +
    "their data."
  val MaxIndexSizeDoc = "This configuration controls the size of the index that maps " +
    "offsets to file positions. We preallocate this index file and shrink it only after log " +
    "rolls. You generally should not need to change this setting."
  val MaxMessageSizeDoc = "This is largest message size Kafka will allow to be appended. Note that if you increase" +
    " this size you must also increase your consumer's fetch size so they can fetch messages this large."
  val IndexIntervalDoc = "This setting controls how frequently Kafka adds an index " +
    "entry to it's offset index. The default setting ensures that we index a message " +
    "roughly every 4096 bytes. More indexing allows reads to jump closer to the exact " +
    "position in the log but makes the index larger. You probably don't need to change " +
    "this."
  val FileDeleteDelayMsDoc = "The time to wait before deleting a file from the filesystem"
  val DeleteRetentionMsDoc = "The amount of time to retain delete tombstone markers " +
    "for <a href=\"#compaction\">log compacted</a> topics. This setting also gives a bound " +
    "on the time in which a consumer must complete a read if they begin from offset 0 " +
    "to ensure that they get a valid snapshot of the final stage (otherwise delete " +
    "tombstones may be collected before they complete their scan)."
  val MinCompactionLagMsDoc = "The minimum time a message will remain uncompacted in the log. " +
    "Only applicable for logs that are being compacted."
  val MinCleanableRatioDoc = "This configuration controls how frequently the log " +
    "compactor will attempt to clean the log (assuming <a href=\"#compaction\">log " +
    "compaction</a> is enabled). By default we will avoid cleaning a log where more than " +
    "50% of the log has been compacted. This ratio bounds the maximum space wasted in " +
    "the log by duplicates (at 50% at most 50% of the log could be duplicates). A " +
    "higher ratio will mean fewer, more efficient cleanings but will mean more wasted " +
    "space in the log."
  val CompactDoc = "A string that is either \"delete\" or \"compact\". This string " +
    "designates the retention policy to use on old log segments. The default policy " +
    "(\"delete\") will discard old segments when their retention time or size limit has " +
    "been reached. The \"compact\" setting will enable <a href=\"#compaction\">log " +
    "compaction</a> on the topic."
  val UncleanLeaderElectionEnableDoc = "Indicates whether to enable replicas not in the ISR set to be elected as" +
    " leader as a last resort, even though doing so may result in data loss"
  val MinInSyncReplicasDoc = KafkaConfig.MinInSyncReplicasDoc
  val CompressionTypeDoc = "Specify the final compression type for a given topic. This configuration accepts the " +
    "standard compression codecs ('gzip', 'snappy', lz4). It additionally accepts 'uncompressed' which is equivalent to " +
    "no compression; and 'producer' which means retain the original compression codec set by the producer."
  val PreAllocateEnableDoc ="Should pre allocate file when create new segment?"
  val MessageFormatVersionDoc = KafkaConfig.LogMessageFormatVersionDoc
  val MessageTimestampTypeDoc = KafkaConfig.LogMessageTimestampTypeDoc
  val MessageTimestampDifferenceMaxMsDoc = "The maximum difference allowed between the timestamp when a broker receives " +
    "a message and the timestamp specified in the message. If message.timestamp.type=CreateTime, a message will be rejected " +
    "if the difference in timestamp exceeds this threshold. This configuration is ignored if message.timestamp.type=LogAppendTime."
  val LeaderReplicationThrottledReplicasDoc = "A list of replicas for which log replication should be throttled on the leader side. The list should describe a set of " +
    "replicas in the form [PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle all replicas for this topic."
  val FollowerReplicationThrottledReplicasDoc = "A list of replicas for which log replication should be throttled on the follower side. The list should describe a set of " +
    "replicas in the form [PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle all replicas for this topic."

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
        throw new InvalidConfigurationException(s"Unknown Log configuration $name.")
  }

  /**
    * Check that the property values are valid relative to each other
    */
  def validateValues(props: Properties) {
    val segmentBytes = if (props.getProperty(SegmentBytesProp) == null) Defaults.SegmentSize else props.getProperty(SegmentBytesProp).toLong
    val retentionBytes = if (props.getProperty(RetentionBytesProp) == null) Defaults.RetentionSize else props.getProperty(RetentionBytesProp).toLong
    if (segmentBytes > retentionBytes && retentionBytes != -1)
      throw new InvalidConfigurationException(s"segment.bytes ${segmentBytes} is not less than or equal to retention.bytes ${retentionBytes}")
  }

  /**
   * Check that the given properties contain only valid log config names and that all values can be parsed and are valid
   */
  def validate(props: Properties) {
    validateNames(props)
    configDef.parse(props)
    validateValues(props)
  }

}
