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

import java.util.Properties

import scala.collection.JavaConverters._

import kafka.api.ApiVersion
import kafka.message.{BrokerCompressionCodec, Message}
import kafka.server.KafkaConfig
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable

object Defaults {
  val SegmentSize = kafka.server.Defaults.LogSegmentBytes
  val SegmentMs = kafka.server.Defaults.LogRollHours * 60 * 60 * 1000L
  val SegmentHours = kafka.server.Defaults.LogRollHours
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
  val MinCleanableDirtyRatio = kafka.server.Defaults.LogCleanerMinCleanRatio
  val Compact = kafka.server.Defaults.LogCleanupPolicy
  val UncleanLeaderElectionEnable = kafka.server.Defaults.UncleanLeaderElectionEnable
  val MinInSyncReplicas = kafka.server.Defaults.MinInSyncReplicas
  val CompressionType = kafka.server.Defaults.CompressionType
  val PreAllocateEnable = kafka.server.Defaults.LogPreAllocateEnable
  val MessageFormatVersion = kafka.server.Defaults.MessageFormatVersion
  val MessageTimestampType = kafka.server.Defaults.MessageTimestampType
  val MessageTimestampDifferenceMaxMs = kafka.server.Defaults.MessageTimestampDifferenceMaxMs
}

case class LogConfig(props: java.util.Map[_, _]) extends AbstractConfig(LogConfig.configDef, props, false) {
  /**
   * Important note: Any configuration parameter that is passed along from KafkaConfig to LogConfig
   * should also go in [[kafka.server.KafkaServer.copyKafkaConfigToLog]].
   */
  val segmentSize = getInt(LogConfig.SegmentBytesProp)
  val segmentMs = getLong(LogConfig.SegmentMsProp)
  val segmentJitterMs = Option(getLong(LogConfig.DeprecatedSegmentJitterMsProp))
    .getOrElse(getLong(LogConfig.SegmentJitterMsProp))
  val maxIndexSize = getInt(LogConfig.SegmentIndexBytesProp)
  val flushInterval = getLong(LogConfig.FlushMessagesProp)
  val flushMs = Option(getLong(LogConfig.DeprecatedFlushMsProp)).getOrElse(getLong(LogConfig.FlushMsProp))
  val retentionSize = getLong(LogConfig.RetentionBytesProp)
  val retentionMs = getLong(LogConfig.RetentionMsProp)
  val maxMessageSize = getInt(LogConfig.MaxMessageBytesProp)
  val indexInterval = getInt(LogConfig.IndexIntervalBytesProp)
  val fileDeleteDelayMs = getLong(LogConfig.FileDeleteDelayMsProp)
  val deleteRetentionMs = getLong(LogConfig.DeleteRetentionMsProp)
  val minCleanableRatio = Option(getDouble(LogConfig.DeprecatedMinCleanableDirtyRatioProp))
    .getOrElse(getDouble(LogConfig.MinCleanableDirtyRatioProp))
  val compact = getString(LogConfig.CleanupPolicyProp).toLowerCase != LogConfig.Delete
  val uncleanLeaderElectionEnable = getBoolean(LogConfig.UncleanLeaderElectionEnableProp)
  val minInSyncReplicas = getInt(LogConfig.MinInSyncReplicasProp)
  val compressionType = getString(LogConfig.CompressionTypeProp).toLowerCase
  val preallocate = getBoolean(LogConfig.PreAllocateEnableProp)
  val messageFormatVersion = ApiVersion(getString(LogConfig.MessageFormatVersionProp))
  val messageTimestampType = TimestampType.forName(getString(LogConfig.MessageTimestampTypeProp))
  val messageTimestampDifferenceMaxMs = getLong(LogConfig.MessageTimestampDifferenceMaxMsProp).longValue

  def randomSegmentJitter: Long =
    if (segmentJitterMs == 0) 0 else Utils.abs(scala.util.Random.nextInt()) % math.min(segmentJitterMs, segmentMs)
}

object LogConfig {

  def main(args: Array[String]) {
    println(configDef.toHtmlTable)
    println("<p>The following table provides the equivalent default server configuration properties. A given server" +
      " default config value only applies to a topic if it does not have an explicit topic config override.</p>")
    println(configDef.serverDefaultConfigNamesToHtmlTable())
  }

  val Delete = "delete"
  val Compact = "compact"

  val SegmentBytesProp = "segment.bytes"
  val DeprecatedSegmentMsProp = "segment.ms"
  val SegmentMsProp = "roll.ms"
  val SegmentHoursProp = "roll.hours"
  val DeprecatedSegmentJitterMsProp = "segment.jitter.ms"
  val SegmentJitterMsProp = "roll.jitter.ms"
  val SegmentIndexBytesProp = "segment.index.bytes"
  val DeprecatedFlushMessagesProp = "flush.messages"
  val FlushMessagesProp = "flush.interval.messages"
  val DeprecatedFlushMsProp = "flush.ms"
  val FlushMsProp = "flush.interval.ms"
  val RetentionBytesProp = "retention.bytes"
  val RetentionMsProp = "retention.ms"
  val MaxMessageBytesProp = "max.message.bytes"
  val IndexIntervalBytesProp = "index.interval.bytes"
  val DeleteRetentionMsProp = "delete.retention.ms"
  val FileDeleteDelayMsProp = "file.delete.delay.ms"
  val DeprecatedMinCleanableDirtyRatioProp = "min.cleanable.dirty.ratio"
  val MinCleanableDirtyRatioProp = "cleaner.min.cleanable.ratio"
  val CleanupPolicyProp = "cleanup.policy"
  val UncleanLeaderElectionEnableProp = "unclean.leader.election.enable"
  val MinInSyncReplicasProp = "min.insync.replicas"
  val CompressionTypeProp = "compression.type"
  val PreAllocateEnableProp = "preallocate"
  val MessageFormatVersionProp = "message.format.version"
  val MessageTimestampTypeProp = "message.timestamp.type"
  val MessageTimestampDifferenceMaxMsProp = "message.timestamp.difference.max.ms"

  val SegmentSizeDoc = "This configuration controls the segment file size for the log. Retention and cleaning is" +
    " always done a file at a time so a larger segment size means fewer files but less granular control over retention."
  val SegmentHoursDoc = "This configuration controls the period of time after which Kafka will force the log to roll" +
    " even if the segment file isn't full to ensure that retention can delete or compact old data."
  val SegmentMsDoc = s"$SegmentHoursDoc If not set, the value in $SegmentHoursProp is used."
  val DeprecatedSegmentMsDoc = s"${ConfigDef.deprecatesDoc(SegmentMsProp)} $SegmentMsDoc"
  val SegmentJitterMsDoc = "The maximum random jitter subtracted from the scheduled segment roll time to avoid" +
    " thundering herds of segment rolling."
  val DeprecatedSegmentJitterMsDoc = s"${ConfigDef.deprecatesDoc(SegmentJitterMsProp)} $SegmentJitterMsDoc"
  val FlushIntervalDoc = "This setting allows specifying an interval at which we will force an fsync of data written" +
    " to the log. For example if this was set to 1 we would fsync after every message; if it were 5 we would fsync" +
    " after every five messages. In general we recommend you not set this and use replication for durability and" +
    " allow the operating system's background flush capabilities as it is more efficient. This setting can be" +
    " overridden on a per-topic basis (see <a href=\"#topic-config\">the per-topic configuration section</a>)."
  val DeprecatedFlushIntervalDoc = s"${ConfigDef.deprecatesDoc(FlushMessagesProp)} $FlushIntervalDoc"
  val FlushMsDoc = "This setting allows specifying a time interval at which we will force an fsync of data written to" +
    " the log. For example if this was set to 1000 we would fsync after 1000 ms had passed. In general we recommend" +
    " you not set this and use replication for durability and allow the operating system's background flush" +
    " capabilities as it is more efficient."
  val DeprecatedFlushMsDoc = s"${ConfigDef.deprecatesDoc(FlushMsProp)} $FlushMsDoc"
  val RetentionSizeDoc = "This configuration controls the maximum size a log can grow to before we will discard old" +
    " log segments to free up space if we are using the \"delete\" retention policy. By default there is no size" +
    " limit only a time limit."
  val RetentionMsDoc = "This configuration controls the maximum time we will retain a log before we will discard" +
    " old log segments to free up space if we are using the \"delete\" retention policy. This represents an SLA on" +
    " how soon consumers must read their data."
  val MaxIndexSizeDoc = "This configuration controls the size of the index that maps offsets to file positions. We" +
    " preallocate this index file and shrink it only after log rolls. You generally should not need to change this" +
    " setting."
  val MaxMessageSizeDoc = "This is largest message size Kafka will allow to be appended. Note that if you increase" +
    " this size you must also increase your consumer's fetch size so they can fetch messages this large."
  val IndexIntervalDoc = "This setting controls how frequently Kafka adds an index entry to it's offset index. The" +
    " default setting ensures that we index a message roughly every 4096 bytes. More indexing allows reads to jump" +
    " closer to the exact position in the log but makes the index larger. You probably don't need to change this."
  val FileDeleteDelayMsDoc = "The time to wait before deleting a file from the filesystem"
  val DeleteRetentionMsDoc = "The amount of time to retain delete tombstone markers for <a href=\"#compaction\">log" +
    " compacted</a> topics. This setting also gives a bound on the time in which a consumer must complete a read if" +
    " they begin from offset 0 to ensure that they get a valid snapshot of the final stage (otherwise delete" +
    " tombstones may be collected before they complete their scan). Only applicable for logs that are being compacted."
  val MinCleanableRatioDoc = "This configuration controls how frequently the log compactor will attempt to clean the" +
    " log (assuming <a href=\"#compaction\">log compaction</a> is enabled). By default we will avoid cleaning a log" +
    " where more than 50% of the log has been compacted. This ratio bounds the maximum space wasted in the log by" +
    " duplicates (at 50% at most 50% of the log could be duplicates). A higher ratio will mean fewer, more efficient" +
    " cleanings but will mean more wasted space in the log."
  val DeprecatedMinCleanableRatioDoc = s"${ConfigDef.deprecatesDoc(MinCleanableDirtyRatioProp)} $MinCleanableRatioDoc"
  val CompactDoc = "A string that is either \"delete\" or \"compact\". This string designates the retention policy to" +
    " use on old log segments. The default policy (\"delete\") will discard old segments when their retention time or" +
    " size limit has been reached. The \"compact\" setting will enable <a href=\"#compaction\">log compaction</a> on" +
    " the topic."
  val UncleanLeaderElectionEnableDoc = "Indicates whether to enable replicas not in the ISR set to be elected as" +
    " leader as a last resort, even though doing so may result in data loss"
  val MinInSyncReplicasDoc = "When a producer sets acks to \"all\", min.insync.replicas specifies the minimum number" +
    " of replicas that must acknowledge a write for the write to be considered successful. If this minimum cannot be" +
    " met, then the producer will raise an exception (either NotEnoughReplicas or NotEnoughReplicasAfterAppend). When" +
    " used together, min.insync.replicas and acks allow you to enforce greater durability guarantees. A typical" +
    " scenario would be to create a topic with a replication factor of 3, set min.insync.replicas to 2, and produce" +
    " with acks of \"all\". This will ensure that the producer raises an exception if a majority of replicas do not" +
    " receive a write."
  val CompressionTypeDoc = "Specify the final compression type for a given topic. This configuration accepts the" +
    " standard compression codecs ('gzip', 'snappy', lz4). It additionally accepts 'uncompressed' which is equivalent" +
    " to no compression; and 'producer' which means retain the original compression codec set by the producer."
  val PreAllocateEnableDoc = "Should pre allocate file when create new segment? If you are using Kafka on Windows," +
    " you probably need to set it to true."
  val MessageFormatVersionDoc = "Specify the message format version the broker will use to append messages to the" +
    " log. The value should be a valid ApiVersion. Some examples are: 0.8.2, 0.9.0.0, 0.10.0; check ApiVersion for" +
    " more details. By setting a particular message format version, the user is certifying that all the existing" +
    " messages on disk are smaller or equal than the specified version. Setting this value incorrectly will cause" +
    " consumers with older versions to break as they will receive messages with a format that they don't understand."
  val MessageTimestampTypeDoc = "Define whether the timestamp in the message is message create time or log append" +
    " time. The value should be either `CreateTime` or `LogAppendTime`"
  val MessageTimestampDifferenceMaxMsDoc = "The maximum difference allowed between the timestamp when a broker" +
    " receives a message and the timestamp specified in the message. If message.timestamp.type=CreateTime, a message" +
    " will be rejected if the difference in timestamp exceeds this threshold. This configuration is ignored if" +
    " message.timestamp.type=LogAppendTime."

  private class LogConfigDef extends ConfigDef {

    private final val serverDefaultConfigNames = mutable.Map[String, String]()

    private def recordServerDefaultConfig(name: String, serverDefaultConfigName: String) {
      require(serverDefaultConfigName.startsWith("log."), s"Server default configuration $serverDefaultConfigName (for log config $name) is not prefixed with log.")
      serverDefaultConfigNames.put(name, serverDefaultConfigName)
    }

    def define(name: String, defType: ConfigDef.Type, defaultValue: Object, validator: ConfigDef.Validator,
               importance: ConfigDef.Importance, doc: String, serverDefaultConfigName: String) = {
      super.define(name, defType, defaultValue, validator, importance, doc)
      recordServerDefaultConfig(name, serverDefaultConfigName)
      this
    }

    def define(name: String, defType: ConfigDef.Type, defaultValue: Object, importance: ConfigDef.Importance,
               documentation: String, serverDefaultConfigName: String) = {
      super.define(name, defType, defaultValue, importance, documentation)
      recordServerDefaultConfig(name, serverDefaultConfigName)
      this
    }

    def define(name: String, defType: ConfigDef.Type, importance: ConfigDef.Importance, documentation: String,
               serverDefaultConfigName: String) = {
      super.define(name, defType, importance, documentation)
      recordServerDefaultConfig(name, serverDefaultConfigName)
      this
    }

    def serverDefaultConfigNamesToHtmlTable() = {
      val sb = new StringBuilder
      sb.append("<table class=\"data-table\"><tbody>\n")
      sb.append("<tr>\n")
      sb.append("<th>Topic config name</th>\n")
      sb.append("<th>Server default config name</th>\n")
      sb.append("</tr>\n")
      serverDefaultConfigNames.foreach { case(logConfig, serverConfig) =>
        sb.append("<tr>\n")
        sb.append(s"<td>$logConfig</td><td>$serverConfig</td>\n")
        sb.append("</tr>\n")
      }
      sb.append("</tbody></table>")
      sb.toString()
    }
  }

  private val configDef: LogConfigDef = {
    import org.apache.kafka.common.config.ConfigDef.Importance._
    import org.apache.kafka.common.config.ConfigDef.Range._
    import org.apache.kafka.common.config.ConfigDef.Type._
    import org.apache.kafka.common.config.ConfigDef.ValidString._

    new LogConfigDef()
      .define(SegmentBytesProp, INT, Int.box(Defaults.SegmentSize), atLeast(Message.MinMessageOverhead), MEDIUM,
        SegmentSizeDoc, KafkaConfig.LogSegmentBytesProp)
      .define(DeprecatedSegmentMsProp, LONG, null, MEDIUM, DeprecatedSegmentMsDoc, KafkaConfig.LogRollTimeMillisProp)
      .define(SegmentMsProp, LONG, Long.box(Defaults.SegmentMs), atLeast(0), MEDIUM, SegmentMsDoc,
        KafkaConfig.LogRollTimeMillisProp)
      .define(SegmentHoursProp, LONG, Long.box(Defaults.SegmentHours), atLeast(0), MEDIUM, SegmentHoursDoc,
        KafkaConfig.LogRollTimeHoursProp)
      .define(DeprecatedSegmentJitterMsProp, LONG, null, MEDIUM, DeprecatedSegmentJitterMsDoc,
        KafkaConfig.LogRollTimeJitterMillisProp)
      .define(SegmentJitterMsProp, LONG, Long.box(Defaults.SegmentJitterMs), atLeast(0), MEDIUM, SegmentJitterMsDoc,
        KafkaConfig.LogRollTimeJitterMillisProp)
      .define(SegmentIndexBytesProp, INT, Int.box(Defaults.MaxIndexSize), atLeast(0), MEDIUM, MaxIndexSizeDoc,
        KafkaConfig.LogIndexSizeMaxBytesProp)
      .define(DeprecatedFlushMessagesProp, LONG, null, MEDIUM, DeprecatedFlushIntervalDoc,
        KafkaConfig.LogFlushIntervalMessagesProp)
      .define(FlushMessagesProp, LONG, Long.box(Defaults.FlushInterval), atLeast(0), MEDIUM, FlushIntervalDoc,
        KafkaConfig.LogFlushIntervalMessagesProp)
      .define(DeprecatedFlushMsProp, LONG, null, MEDIUM, DeprecatedFlushMsDoc, KafkaConfig.LogFlushIntervalMsProp)
      .define(FlushMsProp, LONG, Long.box(Defaults.FlushMs), atLeast(0), MEDIUM, FlushMsDoc,
        KafkaConfig.LogFlushIntervalMsProp)
      // can be negative. See kafka.log.LogManager.cleanupSegmentsToMaintainSize
      .define(RetentionBytesProp, LONG, Long.box(Defaults.RetentionSize), MEDIUM, RetentionSizeDoc,
        KafkaConfig.LogRetentionBytesProp)
      // can be negative. See kafka.log.LogManager.cleanupExpiredSegments
      .define(RetentionMsProp, LONG, Long.box(Defaults.RetentionMs), MEDIUM, RetentionMsDoc,
        KafkaConfig.LogRetentionTimeMillisProp)
      .define(MaxMessageBytesProp, INT, Int.box(Defaults.MaxMessageSize), atLeast(0), MEDIUM, MaxMessageSizeDoc,
        KafkaConfig.MessageMaxBytesProp)
      .define(IndexIntervalBytesProp, INT, Int.box(Defaults.IndexInterval), atLeast(0), MEDIUM, IndexIntervalDoc,
        KafkaConfig.LogIndexIntervalBytesProp)
      .define(DeleteRetentionMsProp, LONG, Long.box(Defaults.DeleteRetentionMs), atLeast(0), MEDIUM,
        DeleteRetentionMsDoc, KafkaConfig.LogCleanerDeleteRetentionMsProp)
      .define(FileDeleteDelayMsProp, LONG, Long.box(Defaults.FileDeleteDelayMs), atLeast(0), MEDIUM, FileDeleteDelayMsDoc,
        KafkaConfig.LogDeleteDelayMsProp)
      .define(DeprecatedMinCleanableDirtyRatioProp, DOUBLE, null, MEDIUM, DeprecatedMinCleanableRatioDoc,
        KafkaConfig.LogCleanerMinCleanRatioProp)
      .define(MinCleanableDirtyRatioProp, DOUBLE, Double.box(Defaults.MinCleanableDirtyRatio), between(0, 1), MEDIUM,
        MinCleanableRatioDoc, KafkaConfig.LogCleanerMinCleanRatioProp)
      .define(CleanupPolicyProp, STRING, Defaults.Compact, in(Compact, Delete), MEDIUM, CompactDoc,
        KafkaConfig.LogCleanupPolicyProp)
      .define(UncleanLeaderElectionEnableProp, BOOLEAN, Boolean.box(Defaults.UncleanLeaderElectionEnable),
        MEDIUM, UncleanLeaderElectionEnableDoc, KafkaConfig.UncleanLeaderElectionEnableProp)
      .define(MinInSyncReplicasProp, INT, Int.box(Defaults.MinInSyncReplicas), atLeast(1), MEDIUM, MinInSyncReplicasDoc,
        KafkaConfig.MinInSyncReplicasProp)
      .define(CompressionTypeProp, STRING, Defaults.CompressionType, in(BrokerCompressionCodec.brokerCompressionOptions:_*),
        MEDIUM, CompressionTypeDoc, KafkaConfig.CompressionTypeProp)
      .define(PreAllocateEnableProp, BOOLEAN, Boolean.box(Defaults.PreAllocateEnable), MEDIUM, PreAllocateEnableDoc,
        KafkaConfig.LogPreAllocateProp)
      .define(MessageFormatVersionProp, STRING, Defaults.MessageFormatVersion, MEDIUM, MessageFormatVersionDoc,
        KafkaConfig.MessageFormatVersionProp)
      .define(MessageTimestampTypeProp, STRING, Defaults.MessageTimestampType, MEDIUM, MessageTimestampTypeDoc,
        KafkaConfig.MessageTimestampTypeProp)
      .define(MessageTimestampDifferenceMaxMsProp, LONG, Long.box(Defaults.MessageTimestampDifferenceMaxMs),
        atLeast(0), MEDIUM, MessageTimestampDifferenceMaxMsDoc, KafkaConfig.MessageTimestampDifferenceMaxMsProp)
  }

  def apply(): LogConfig = LogConfig(new Properties())

  def configNames: Seq[String] = configDef.names.asScala.toSeq.sorted

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
    for (name <- props.keys.asScala) require(names.contains(name), s"Unknown configuration `$name`.")
  }

  /**
   * Check that the given properties contain only valid log config names and that all values can be parsed and are valid
   */
  def validate(props: Properties) {
    validateNames(props)
    configDef.parse(props)
  }

}
