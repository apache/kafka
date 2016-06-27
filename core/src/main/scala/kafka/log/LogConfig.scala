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
import java.util.Locale

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
  val MinCleanableDirtyRatio = kafka.server.Defaults.LogCleanerMinCleanRatio
  val Compact = kafka.server.Defaults.LogCleanupPolicy
  val UncleanLeaderElectionEnable = kafka.server.Defaults.UncleanLeaderElectionEnable
  val MinInSyncReplicas = kafka.server.Defaults.MinInSyncReplicas
  val CompressionType = kafka.server.Defaults.CompressionType
  val PreAllocateEnable = kafka.server.Defaults.LogPreAllocateEnable
  val MessageFormatVersion = kafka.server.Defaults.LogMessageFormatVersion
  val MessageTimestampType = kafka.server.Defaults.LogMessageTimestampType
  val MessageTimestampDifferenceMaxMs = kafka.server.Defaults.LogMessageTimestampDifferenceMaxMs
}

case class LogConfig(props: java.util.Map[_, _]) extends AbstractConfig(LogConfig.configDef, props, false) {
  /**
   * Important note: Any configuration parameter that is passed along from KafkaConfig to LogConfig
   * should also go in copyKafkaConfigToLog.
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
  val minCleanableRatio = getDouble(LogConfig.MinCleanableDirtyRatioProp)
  val compact = getString(LogConfig.CleanupPolicyProp).toLowerCase(Locale.ROOT) != LogConfig.Delete
  val uncleanLeaderElectionEnable = getBoolean(LogConfig.UncleanLeaderElectionEnableProp)
  val minInSyncReplicas = getInt(LogConfig.MinInSyncReplicasProp)
  val compressionType = getString(LogConfig.CompressionTypeProp).toLowerCase(Locale.ROOT)
  val preallocate = getBoolean(LogConfig.PreAllocateEnableProp)
  val messageFormatVersion = ApiVersion(getString(LogConfig.MessageFormatVersionProp))
  val messageTimestampType = TimestampType.forName(getString(LogConfig.MessageTimestampTypeProp))
  val messageTimestampDifferenceMaxMs = getLong(LogConfig.MessageTimestampDifferenceMaxMsProp).longValue

  def randomSegmentJitter: Long =
    if (segmentJitterMs == 0) 0 else Utils.abs(scala.util.Random.nextInt()) % math.min(segmentJitterMs, segmentMs)
}

object LogConfig {

  def main(args: Array[String]) {
    System.out.println(configDef.toHtmlTable)
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

  val SegmentSizeDoc = "The hard maximum for the size of a segment file in the log"
  val SegmentMsDoc = "The soft maximum on the amount of time before a new log segment is rolled"
  val SegmentJitterMsDoc = "The maximum random jitter subtracted from segmentMs to avoid thundering herds of segment" +
    " rolling"
  val FlushIntervalDoc = "The number of messages that can be written to the log before a flush is forced"
  val FlushMsDoc = "The amount of time the log can have dirty data before a flush is forced"
  val RetentionSizeDoc = "The approximate total number of bytes this log can use"
  val RetentionMsDoc = "The approximate maximum age of the last segment that is retained"
  val MaxIndexSizeDoc = "The maximum size of an index file"
  val MaxMessageSizeDoc = "The maximum size of a message"
  val IndexIntervalDoc = "The approximate number of bytes between index entries"
  val FileDeleteDelayMsDoc = "The time to wait before deleting a file from the filesystem"
  val DeleteRetentionMsDoc = "The time to retain delete markers in the log. Only applicable for logs that are being" +
    " compacted."
  val MinCleanableRatioDoc = "The ratio of bytes that are available for cleaning to the bytes already cleaned"
  val CompactDoc = "Should old segments in this log be deleted or deduplicated?"
  val UncleanLeaderElectionEnableDoc = "Indicates whether unclean leader election is enabled"
  val MinInSyncReplicasDoc = "If number of insync replicas drops below this number, we stop accepting writes with" +
    " -1 (or all) required acks"
  val CompressionTypeDoc = "Specify the final compression type for a given topic. This configuration accepts the " +
    "standard compression codecs ('gzip', 'snappy', lz4). It additionally accepts 'uncompressed' which is equivalent to " +
    "no compression; and 'producer' which means retain the original compression codec set by the producer."
  val PreAllocateEnableDoc ="Should pre allocate file when create new segment?"
  val MessageFormatVersionDoc = KafkaConfig.LogMessageFormatVersionDoc
  val MessageTimestampTypeDoc = KafkaConfig.LogMessageTimestampTypeDoc
  val MessageTimestampDifferenceMaxMsDoc = KafkaConfig.LogMessageTimestampDifferenceMaxMsDoc

  private val configDef = {
    import org.apache.kafka.common.config.ConfigDef.Importance._
    import org.apache.kafka.common.config.ConfigDef.Range._
    import org.apache.kafka.common.config.ConfigDef.Type._
    import org.apache.kafka.common.config.ConfigDef.ValidString._

    new ConfigDef()
      .define(SegmentBytesProp, INT, Defaults.SegmentSize, atLeast(Message.MinMessageOverhead), MEDIUM, SegmentSizeDoc)
      .define(SegmentMsProp, LONG, Defaults.SegmentMs, atLeast(0), MEDIUM, SegmentMsDoc)
      .define(SegmentJitterMsProp, LONG, Defaults.SegmentJitterMs, atLeast(0), MEDIUM, SegmentJitterMsDoc)
      .define(SegmentIndexBytesProp, INT, Defaults.MaxIndexSize, atLeast(0), MEDIUM, MaxIndexSizeDoc)
      .define(FlushMessagesProp, LONG, Defaults.FlushInterval, atLeast(0), MEDIUM, FlushIntervalDoc)
      .define(FlushMsProp, LONG, Defaults.FlushMs, atLeast(0), MEDIUM, FlushMsDoc)
      // can be negative. See kafka.log.LogManager.cleanupSegmentsToMaintainSize
      .define(RetentionBytesProp, LONG, Defaults.RetentionSize, MEDIUM, RetentionSizeDoc)
      // can be negative. See kafka.log.LogManager.cleanupExpiredSegments
      .define(RetentionMsProp, LONG, Defaults.RetentionMs, MEDIUM, RetentionMsDoc)
      .define(MaxMessageBytesProp, INT, Defaults.MaxMessageSize, atLeast(0), MEDIUM, MaxMessageSizeDoc)
      .define(IndexIntervalBytesProp, INT, Defaults.IndexInterval, atLeast(0), MEDIUM,  IndexIntervalDoc)
      .define(DeleteRetentionMsProp, LONG, Defaults.DeleteRetentionMs, atLeast(0), MEDIUM, DeleteRetentionMsDoc)
      .define(FileDeleteDelayMsProp, LONG, Defaults.FileDeleteDelayMs, atLeast(0), MEDIUM, FileDeleteDelayMsDoc)
      .define(MinCleanableDirtyRatioProp, DOUBLE, Defaults.MinCleanableDirtyRatio, between(0, 1), MEDIUM,
        MinCleanableRatioDoc)
      .define(CleanupPolicyProp, STRING, Defaults.Compact, in(Compact, Delete), MEDIUM,
        CompactDoc)
      .define(UncleanLeaderElectionEnableProp, BOOLEAN, Defaults.UncleanLeaderElectionEnable,
        MEDIUM, UncleanLeaderElectionEnableDoc)
      .define(MinInSyncReplicasProp, INT, Defaults.MinInSyncReplicas, atLeast(1), MEDIUM, MinInSyncReplicasDoc)
      .define(CompressionTypeProp, STRING, Defaults.CompressionType, in(BrokerCompressionCodec.brokerCompressionOptions:_*), MEDIUM, CompressionTypeDoc)
      .define(PreAllocateEnableProp, BOOLEAN, Defaults.PreAllocateEnable,
        MEDIUM, PreAllocateEnableDoc)
      .define(MessageFormatVersionProp, STRING, Defaults.MessageFormatVersion, MEDIUM, MessageFormatVersionDoc)
      .define(MessageTimestampTypeProp, STRING, Defaults.MessageTimestampType, MEDIUM, MessageTimestampTypeDoc)
      .define(MessageTimestampDifferenceMaxMsProp, LONG, Defaults.MessageTimestampDifferenceMaxMs, atLeast(0), MEDIUM, MessageTimestampDifferenceMaxMsDoc)
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
