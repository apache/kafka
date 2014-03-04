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
import scala.collection._
import kafka.common._

/**
 * Configuration settings for a log
 * @param segmentSize The soft maximum for the size of a segment file in the log
 * @param segmentMs The soft maximum on the amount of time before a new log segment is rolled
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
 */
case class LogConfig(val segmentSize: Int = 1024*1024, 
                     val segmentMs: Long = Long.MaxValue,
                     val flushInterval: Long = Long.MaxValue, 
                     val flushMs: Long = Long.MaxValue,
                     val retentionSize: Long = Long.MaxValue,
                     val retentionMs: Long = Long.MaxValue,
                     val maxMessageSize: Int = Int.MaxValue,
                     val maxIndexSize: Int = 1024*1024,
                     val indexInterval: Int = 4096,
                     val fileDeleteDelayMs: Long = 60*1000,
                     val deleteRetentionMs: Long = 24 * 60 * 60 * 1000L,
                     val minCleanableRatio: Double = 0.5,
                     val compact: Boolean = false) {
  
  def toProps: Properties = {
    val props = new Properties()
    import LogConfig._
    props.put(SegmentBytesProp, segmentSize.toString)
    props.put(SegmentMsProp, segmentMs.toString)
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
    props
  }
  
}

object LogConfig {
  val SegmentBytesProp = "segment.bytes"
  val SegmentMsProp = "segment.ms"
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
  
  val ConfigNames = Set(SegmentBytesProp, 
                        SegmentMsProp, 
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
                        CleanupPolicyProp)
    
  
  /**
   * Parse the given properties instance into a LogConfig object
   */
  def fromProps(props: Properties): LogConfig = {
    new LogConfig(segmentSize = props.getProperty(SegmentBytesProp).toInt,
                  segmentMs = props.getProperty(SegmentMsProp).toLong,
                  maxIndexSize = props.getProperty(SegmentIndexBytesProp).toInt,
                  flushInterval = props.getProperty(FlushMessagesProp).toLong,
                  flushMs = props.getProperty(FlushMsProp).toLong,
                  retentionSize = props.getProperty(RetentionBytesProp).toLong,
                  retentionMs = props.getProperty(RententionMsProp).toLong,
                  maxMessageSize = props.getProperty(MaxMessageBytesProp).toInt,
                  indexInterval = props.getProperty(IndexIntervalBytesProp).toInt,
                  fileDeleteDelayMs = props.getProperty(FileDeleteDelayMsProp).toInt,
                  deleteRetentionMs = props.getProperty(DeleteRetentionMsProp).toLong,
                  minCleanableRatio = props.getProperty(MinCleanableDirtyRatioProp).toDouble,
                  compact = props.getProperty(CleanupPolicyProp).trim.toLowerCase != "delete")
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
    LogConfig.fromProps(LogConfig().toProps, props) // check that we can parse the values
  }
  
}
                      
                     