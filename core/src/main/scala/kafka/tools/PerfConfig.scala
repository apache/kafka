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

package kafka.tools

import joptsimple.OptionParser


class PerfConfig(args: Array[String]) {
  val parser = new OptionParser
  val numMessagesOpt = parser.accepts("messages", "REQUIRED: The number of messages to send or consume")
    .withRequiredArg
    .describedAs("count")
    .ofType(classOf[java.lang.Long])
  val reportingIntervalOpt = parser.accepts("reporting-interval", "Interval at which to print progress info.")
    .withRequiredArg
    .describedAs("size")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(5000)
  val dateFormatOpt = parser.accepts("date-format", "The date format to use for formatting the time field. " +
    "See java.text.SimpleDateFormat for options.")
    .withRequiredArg
    .describedAs("date format")
    .ofType(classOf[String])
    .defaultsTo("yyyy-MM-dd HH:mm:ss:SSS")
  val showDetailedStatsOpt = parser.accepts("show-detailed-stats", "If set, stats are reported for each reporting " +
    "interval as configured by reporting-interval")
  val hideHeaderOpt = parser.accepts("hide-header", "If set, skips printing the header for the stats ")
  val messageSizeOpt = parser.accepts("message-size", "The size of each message.")
    .withRequiredArg
    .describedAs("size")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(100)
  val batchSizeOpt = parser.accepts("batch-size", "Number of messages to write in a single batch.")
    .withRequiredArg
    .describedAs("size")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(200)
  val compressionCodecOpt = parser.accepts("compression-codec", "If set, messages are sent compressed")
    .withRequiredArg
    .describedAs("supported codec: NoCompressionCodec as 0, GZIPCompressionCodec as 1, SnappyCompressionCodec as 2, LZ4CompressionCodec as 3")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(0)
  val helpOpt = parser.accepts("help", "Print usage.")
}
