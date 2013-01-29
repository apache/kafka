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

import java.io.File
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
 * @param minCleanableRatio The ratio of bytes that are available for cleaning to the bytes already cleaned
 * @param dedupe Should old segments in this log be deleted or deduplicated?
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
                     val minCleanableRatio: Double = 0.5,
                     val dedupe: Boolean = false)
                      
                     