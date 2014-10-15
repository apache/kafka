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
import kafka.common._

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
}

/**
 * Configuration settings for a log
 * @param segmentSize The soft maximum for the size of a segment file in the log
 * @param segmentMs The soft maximum on the amount of time before a new log segment is rolled
 * @param segmentJitterMs The maximum random jitter subtracted from segmentMs to avoid thundering herds of segment rolling
 * @param flushInterval The number of messages that can be written to the log before a flush is forced
 * @param flushMs The amount of time the log can have dirty data before a flush is forced
 * @param retentionSize The approximate total number of bytes this log can use
 * @param retentionMs The age approximate maximum age of the last segment that is retained
 * @param maxIndexSize The maximum size of an index file
 * @param indexInterval The approximate number of bytes between index entries
 * @param fileDeleteDelayMs The time to wait before deleting a file from the filesystem
 * @param deleteRetentionMs The time to retain delete markers in the log. Only applicable for logs that are being compacted.
 * @param minCleanableRatio The ratio of bytes that are available for cleaning to the bytes already cleaned
 * @param compact Should old segments in this log be deleted or deduplicated?
 * @param uncleanLeaderElectionEnable Indicates whether unclean leader election is enabled; actually a controller-level property
 *                                    but included here for topic-specific configuration validation purposes
 * @param minInSyncReplicas If number of insync replicas drops below this number, we stop accepting writes with -1 (or all) required acks
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
                     val minInSyncReplicas: Int = Defaults.MinInSyncReplicas) {

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
    props
  }

  def randomSegmentJitter: Long =
    if (segmentJitterMs == 0) 0 else Utils.abs(scala.util.Random.nextInt()) % math.min(segmentJitterMs, segmentMs)
}

object LogConfig {
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

  val ConfigNames = Set(SegmentBytesProp,
                        SegmentMsProp,
                        SegmentJitterMsProp,
                        SegmentIndexBytesProp,
                        FlushMessagesProp,
                        FlushMsProp,
                        RetentionBytesProp,
                        RententionMsProp,
                        MaxMessageBytesProp,
                        IndexIntervalBytesProp,
                        FileDeleteDelayMsProp,
                        DeleteRetentionMsProp,
                        MinCleanableDirtyRatioProp,
                        CleanupPolicyProp,
                        UncleanLeaderElectionEnableProp,
                        MinInSyncReplicasProp)

  /**
   * Parse the given properties instance into a LogConfig object
   */
  def fromProps(props: Properties): LogConfig = {
    new LogConfig(segmentSize = props.getProperty(SegmentBytesProp, Defaults.SegmentSize.toString).toInt,
                  segmentMs = props.getProperty(SegmentMsProp, Defaults.SegmentMs.toString).toLong,
                  segmentJitterMs = props.getProperty(SegmentJitterMsProp, Defaults.SegmentJitterMs.toString).toLong,
                  maxIndexSize = props.getProperty(SegmentIndexBytesProp, Defaults.MaxIndexSize.toString).toInt,
                  flushInterval = props.getProperty(FlushMessagesProp, Defaults.FlushInterval.toString).toLong,
                  flushMs = props.getProperty(FlushMsProp, Defaults.FlushMs.toString).toLong,
                  retentionSize = props.getProperty(RetentionBytesProp, Defaults.RetentionSize.toString).toLong,
                  retentionMs = props.getProperty(RententionMsProp, Defaults.RetentionMs.toString).toLong,
                  maxMessageSize = props.getProperty(MaxMessageBytesProp, Defaults.MaxMessageSize.toString).toInt,
                  indexInterval = props.getProperty(IndexIntervalBytesProp, Defaults.IndexInterval.toString).toInt,
                  fileDeleteDelayMs = props.getProperty(FileDeleteDelayMsProp, Defaults.FileDeleteDelayMs.toString).toInt,
                  deleteRetentionMs = props.getProperty(DeleteRetentionMsProp, Defaults.DeleteRetentionMs.toString).toLong,
                  minCleanableRatio = props.getProperty(MinCleanableDirtyRatioProp,
                    Defaults.MinCleanableDirtyRatio.toString).toDouble,
                  compact = props.getProperty(CleanupPolicyProp, if(Defaults.Compact) "compact" else "delete")
                    .trim.toLowerCase != "delete",
                  uncleanLeaderElectionEnable = props.getProperty(UncleanLeaderElectionEnableProp,
                    Defaults.UncleanLeaderElectionEnable.toString).toBoolean,
                  minInSyncReplicas = props.getProperty(MinInSyncReplicasProp,Defaults.MinInSyncReplicas.toString).toInt)
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
    for(name <- props.keys)
      require(LogConfig.ConfigNames.contains(name), "Unknown configuration \"%s\".".format(name))
  }

  /**
   * Check that the given properties contain only valid log config names, and that all values can be parsed.
   */
  def validate(props: Properties) {
    validateNames(props)
    validateMinInSyncReplicas(props)
    LogConfig.fromProps(LogConfig().toProps, props) // check that we can parse the values
  }

  /**
   * Check that MinInSyncReplicas is reasonable
   * Unfortunately, we can't validate its smaller than number of replicas
   * since we don't have this information here
   */
  private def validateMinInSyncReplicas(props: Properties) {
    val minIsr = props.getProperty(MinInSyncReplicasProp)
    if (minIsr != null && minIsr.toInt < 1) {
      throw new InvalidConfigException("Wrong value " + minIsr + " of min.insync.replicas in topic configuration; " +
        " Valid values are at least 1")
    }
  }

}