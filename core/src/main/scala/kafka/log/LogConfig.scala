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
import org.apache.kafka.common.utils.Utils
import scala.collection._
import org.apache.kafka.common.config.ConfigDef
import kafka.common._
import scala.collection.JavaConversions._
import kafka.message.BrokerCompressionCodec

object Defaults {
  val SegmentSize = 1024 * 1024
  val SegmentMs = Long.MaxValue
  val SegmentJitterMs = 0L
  val FlushInterval = Long.MaxValue
  val FlushMs = Long.MaxValue
  val RetentionSize = Long.MaxValue
  val RetentionMs = Long.MaxValue
  val MaxMessageSize = Int.MaxValue
  val MaxIndexSize = 1024 * 1024
  val IndexInterval = 4096
  val FileDeleteDelayMs = 60 * 1000L
  val DeleteRetentionMs = 24 * 60 * 60 * 1000L
  val MinCleanableDirtyRatio = 0.5
  val Compact = false
  val UncleanLeaderElectionEnable = true
  val MinInSyncReplicas = 1
  val CompressionType = "producer"
}

/**
 * Configuration settings for a log
 * @param segmentSize The hard maximum for the size of a segment file in the log
 * @param segmentMs The soft maximum on the amount of time before a new log segment is rolled
 * @param segmentJitterMs The maximum random jitter subtracted from segmentMs to avoid thundering herds of segment rolling
 * @param flushInterval The number of messages that can be written to the log before a flush is forced
 * @param flushMs The amount of time the log can have dirty data before a flush is forced
 * @param retentionSize The approximate total number of bytes this log can use
 * @param retentionMs The approximate maximum age of the last segment that is retained
 * @param maxIndexSize The maximum size of an index file
 * @param indexInterval The approximate number of bytes between index entries
 * @param fileDeleteDelayMs The time to wait before deleting a file from the filesystem
 * @param deleteRetentionMs The time to retain delete markers in the log. Only applicable for logs that are being compacted.
 * @param minCleanableRatio The ratio of bytes that are available for cleaning to the bytes already cleaned
 * @param compact Should old segments in this log be deleted or deduplicated?
 * @param uncleanLeaderElectionEnable Indicates whether unclean leader election is enabled
 * @param minInSyncReplicas If number of insync replicas drops below this number, we stop accepting writes with -1 (or all) required acks
 * @param compressionType compressionType for a given topic
 *
 */
case class LogConfig(val segmentSize: Int = Defaults.SegmentSize,
                     val segmentMs: Long = Defaults.SegmentMs,
                     val segmentJitterMs: Long = Defaults.SegmentJitterMs,
                     val flushInterval: Long = Defaults.FlushInterval,
                     val flushMs: Long = Defaults.FlushMs,
                     val retentionSize: Long = Defaults.RetentionSize,
                     val retentionMs: Long = Defaults.RetentionMs,
                     val maxMessageSize: Int = Defaults.MaxMessageSize,
                     val maxIndexSize: Int = Defaults.MaxIndexSize,
                     val indexInterval: Int = Defaults.IndexInterval,
                     val fileDeleteDelayMs: Long = Defaults.FileDeleteDelayMs,
                     val deleteRetentionMs: Long = Defaults.DeleteRetentionMs,
                     val minCleanableRatio: Double = Defaults.MinCleanableDirtyRatio,
                     val compact: Boolean = Defaults.Compact,
                     val uncleanLeaderElectionEnable: Boolean = Defaults.UncleanLeaderElectionEnable,
                     val minInSyncReplicas: Int = Defaults.MinInSyncReplicas,
                     val compressionType: String = Defaults.CompressionType) {

  def toProps: Properties = {
    val props = new Properties()
    import LogConfig._
    props.put(SegmentBytesProp, segmentSize.toString)
    props.put(SegmentMsProp, segmentMs.toString)
    props.put(SegmentJitterMsProp, segmentJitterMs.toString)
    props.put(SegmentIndexBytesProp, maxIndexSize.toString)
    props.put(FlushMessagesProp, flushInterval.toString)
    props.put(FlushMsProp, flushMs.toString)
    props.put(RetentionBytesProp, retentionSize.toString)
    props.put(RententionMsProp, retentionMs.toString)
    props.put(MaxMessageBytesProp, maxMessageSize.toString)
    props.put(IndexIntervalBytesProp, indexInterval.toString)
    props.put(DeleteRetentionMsProp, deleteRetentionMs.toString)
    props.put(FileDeleteDelayMsProp, fileDeleteDelayMs.toString)
    props.put(MinCleanableDirtyRatioProp, minCleanableRatio.toString)
    props.put(CleanupPolicyProp, if(compact) "compact" else "delete")
    props.put(UncleanLeaderElectionEnableProp, uncleanLeaderElectionEnable.toString)
    props.put(MinInSyncReplicasProp, minInSyncReplicas.toString)
    props.put(CompressionTypeProp, compressionType)
    props
  }

  def randomSegmentJitter: Long =
    if (segmentJitterMs == 0) 0 else Utils.abs(scala.util.Random.nextInt()) % math.min(segmentJitterMs, segmentMs)
}

object LogConfig {

  val Delete = "delete"
  val Compact = "compact"

  val SegmentBytesProp = "segment.bytes"
  val SegmentMsProp = "segment.ms"
  val SegmentJitterMsProp = "segment.jitter.ms"
  val SegmentIndexBytesProp = "segment.index.bytes"
  val FlushMessagesProp = "flush.messages"
  val FlushMsProp = "flush.ms"
  val RetentionBytesProp = "retention.bytes"
  val RententionMsProp = "retention.ms"
  val MaxMessageBytesProp = "max.message.bytes"
  val IndexIntervalBytesProp = "index.interval.bytes"
  val DeleteRetentionMsProp = "delete.retention.ms"
  val FileDeleteDelayMsProp = "file.delete.delay.ms"
  val MinCleanableDirtyRatioProp = "min.cleanable.dirty.ratio"
  val CleanupPolicyProp = "cleanup.policy"
  val UncleanLeaderElectionEnableProp = "unclean.leader.election.enable"
  val MinInSyncReplicasProp = "min.insync.replicas"
  val CompressionTypeProp = "compression.type"

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

  private val configDef = {
    import ConfigDef.Range._
    import ConfigDef.ValidString._
    import ConfigDef.Type._
    import ConfigDef.Importance._
    import java.util.Arrays.asList

    new ConfigDef()
      .define(SegmentBytesProp, INT, Defaults.SegmentSize, atLeast(0), MEDIUM, SegmentSizeDoc)
      .define(SegmentMsProp, LONG, Defaults.SegmentMs, atLeast(0), MEDIUM, SegmentMsDoc)
      .define(SegmentJitterMsProp, LONG, Defaults.SegmentJitterMs, atLeast(0), MEDIUM, SegmentJitterMsDoc)
      .define(SegmentIndexBytesProp, INT, Defaults.MaxIndexSize, atLeast(0), MEDIUM, MaxIndexSizeDoc)
      .define(FlushMessagesProp, LONG, Defaults.FlushInterval, atLeast(0), MEDIUM, FlushIntervalDoc)
      .define(FlushMsProp, LONG, Defaults.FlushMs, atLeast(0), MEDIUM, FlushMsDoc)
      // can be negative. See kafka.log.LogManager.cleanupSegmentsToMaintainSize
      .define(RetentionBytesProp, LONG, Defaults.RetentionSize, MEDIUM, RetentionSizeDoc)
      .define(RententionMsProp, LONG, Defaults.RetentionMs, atLeast(0), MEDIUM, RetentionMsDoc)
      .define(MaxMessageBytesProp, INT, Defaults.MaxMessageSize, atLeast(0), MEDIUM, MaxMessageSizeDoc)
      .define(IndexIntervalBytesProp, INT, Defaults.IndexInterval, atLeast(0), MEDIUM,  IndexIntervalDoc)
      .define(DeleteRetentionMsProp, LONG, Defaults.DeleteRetentionMs, atLeast(0), MEDIUM, DeleteRetentionMsDoc)
      .define(FileDeleteDelayMsProp, LONG, Defaults.FileDeleteDelayMs, atLeast(0), MEDIUM, FileDeleteDelayMsDoc)
      .define(MinCleanableDirtyRatioProp, DOUBLE, Defaults.MinCleanableDirtyRatio, between(0, 1), MEDIUM,
        MinCleanableRatioDoc)
      .define(CleanupPolicyProp, STRING, if (Defaults.Compact) Compact else Delete, in(Compact, Delete), MEDIUM,
        CompactDoc)
      .define(UncleanLeaderElectionEnableProp, BOOLEAN, Defaults.UncleanLeaderElectionEnable,
        MEDIUM, UncleanLeaderElectionEnableDoc)
      .define(MinInSyncReplicasProp, INT, Defaults.MinInSyncReplicas, atLeast(1), MEDIUM, MinInSyncReplicasDoc)
      .define(CompressionTypeProp, STRING, Defaults.CompressionType, in(BrokerCompressionCodec.brokerCompressionOptions:_*), MEDIUM, CompressionTypeDoc)
  }

  def configNames() = {
    import JavaConversions._
    configDef.names().toList.sorted
  }


  /**
   * Parse the given properties instance into a LogConfig object
   */
  def fromProps(props: Properties): LogConfig = {
    import kafka.utils.Utils.evaluateDefaults
    val parsed = configDef.parse(evaluateDefaults(props))
    new LogConfig(segmentSize = parsed.get(SegmentBytesProp).asInstanceOf[Int],
                  segmentMs = parsed.get(SegmentMsProp).asInstanceOf[Long],
                  segmentJitterMs = parsed.get(SegmentJitterMsProp).asInstanceOf[Long],
                  maxIndexSize = parsed.get(SegmentIndexBytesProp).asInstanceOf[Int],
                  flushInterval = parsed.get(FlushMessagesProp).asInstanceOf[Long],
                  flushMs = parsed.get(FlushMsProp).asInstanceOf[Long],
                  retentionSize = parsed.get(RetentionBytesProp).asInstanceOf[Long],
                  retentionMs = parsed.get(RententionMsProp).asInstanceOf[Long],
                  maxMessageSize = parsed.get(MaxMessageBytesProp).asInstanceOf[Int],
                  indexInterval = parsed.get(IndexIntervalBytesProp).asInstanceOf[Int],
                  fileDeleteDelayMs = parsed.get(FileDeleteDelayMsProp).asInstanceOf[Long],
                  deleteRetentionMs = parsed.get(DeleteRetentionMsProp).asInstanceOf[Long],
                  minCleanableRatio = parsed.get(MinCleanableDirtyRatioProp).asInstanceOf[Double],
                  compact = parsed.get(CleanupPolicyProp).asInstanceOf[String].toLowerCase != Delete,
                  uncleanLeaderElectionEnable = parsed.get(UncleanLeaderElectionEnableProp).asInstanceOf[Boolean],
                  minInSyncReplicas = parsed.get(MinInSyncReplicasProp).asInstanceOf[Int],
                  compressionType = parsed.get(CompressionTypeProp).asInstanceOf[String].toLowerCase())
  }

  /**
   * Create a log config instance using the given properties and defaults
   */
  def fromProps(defaults: Properties, overrides: Properties): LogConfig = {
    val props = new Properties(defaults)
    props.putAll(overrides)
    fromProps(props)
  }

  /**
   * Check that property names are valid
   */
  def validateNames(props: Properties) {
    import JavaConversions._
    val names = configDef.names()
    for(name <- props.keys)
      require(names.contains(name), "Unknown configuration \"%s\".".format(name))
  }

  /**
   * Check that the given properties contain only valid log config names and that all values can be parsed and are valid
   */
  def validate(props: Properties) {
    validateNames(props)
    configDef.parse(props)
  }

}
