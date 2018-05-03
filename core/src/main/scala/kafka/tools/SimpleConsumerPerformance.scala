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

import java.net.URI
import java.text.SimpleDateFormat

import com.typesafe.scalalogging.LazyLogging
import kafka.api.{FetchRequestBuilder, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.consumer.SimpleConsumer
import kafka.utils._
import kafka.common.TopicAndPartition
import org.apache.kafka.common.utils.Time


/**
 * Performance test for the simple consumer
 */
@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
object SimpleConsumerPerformance extends LazyLogging {

  def main(args: Array[String]): Unit = {
    logger.warn("WARNING: SimpleConsumerPerformance is deprecated and will be dropped in a future release following 0.11.0.0.")

    val config = new ConsumerPerfConfig(args)
    logger.info("Starting SimpleConsumer...")

    if(!config.hideHeader) {
      if(!config.showDetailedStats)
        println("start.time, end.time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec")
      else
        println("time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec")
    }

    val consumer = new SimpleConsumer(config.url.getHost, config.url.getPort, 30*1000, 2*config.fetchSize, config.clientId)

    // reset to latest or smallest offset
    val topicAndPartition = TopicAndPartition(config.topic, config.partition)
    val request = OffsetRequest(Map(
      topicAndPartition -> PartitionOffsetRequestInfo(if (config.fromLatest) OffsetRequest.LatestTime else OffsetRequest.EarliestTime, 1)
      ))
    var offset: Long = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head

    val startMs = System.currentTimeMillis
    var done = false
    var totalBytesRead = 0L
    var totalMessagesRead = 0L
    var consumedInterval = 0
    var lastReportTime: Long = startMs
    var lastBytesRead = 0L
    var lastMessagesRead = 0L
    while(!done) {
      // TODO: add in the maxWait and minBytes for performance
      val request = new FetchRequestBuilder()
        .clientId(config.clientId)
        .addFetch(config.topic, config.partition, offset, config.fetchSize)
        .build()
      val fetchResponse = consumer.fetch(request)

      var messagesRead = 0
      var bytesRead = 0
      val messageSet = fetchResponse.messageSet(config.topic, config.partition)
      for (message <- messageSet) {
        messagesRead += 1
        bytesRead += message.message.payloadSize
      }

      if(messagesRead == 0 || totalMessagesRead > config.numMessages)
        done = true
      else
        // we only did one fetch so we find the offset for the first (head) messageset
        offset = messageSet.last.nextOffset

      totalBytesRead += bytesRead
      totalMessagesRead += messagesRead
      consumedInterval += messagesRead

      if(consumedInterval > config.reportingInterval) {
        if(config.showDetailedStats) {
          val reportTime = System.currentTimeMillis
          val elapsed = (reportTime - lastReportTime)/1000.0
          val totalMBRead = ((totalBytesRead-lastBytesRead)*1.0)/(1024*1024)
          println("%s, %d, %.4f, %.4f, %d, %.4f".format(config.dateFormat.format(reportTime), config.fetchSize,
            (totalBytesRead*1.0)/(1024*1024), totalMBRead/elapsed,
            totalMessagesRead, (totalMessagesRead-lastMessagesRead)/elapsed))
        }
        lastReportTime = Time.SYSTEM.milliseconds
        lastBytesRead = totalBytesRead
        lastMessagesRead = totalMessagesRead
        consumedInterval = 0
      }
    }
    val reportTime = System.currentTimeMillis
    val elapsed = (reportTime - startMs) / 1000.0

    if(!config.showDetailedStats) {
      val totalMBRead = (totalBytesRead*1.0)/(1024*1024)
      println("%s, %s, %d, %.4f, %.4f, %d, %.4f".format(config.dateFormat.format(startMs),
        config.dateFormat.format(reportTime), config.fetchSize, totalMBRead, totalMBRead/elapsed,
        totalMessagesRead, totalMessagesRead/elapsed))
    }
    Exit.exit(0)
  }

  class ConsumerPerfConfig(args: Array[String]) extends PerfConfig(args) {
    val urlOpt = parser.accepts("server", "REQUIRED: The hostname of the server to connect to.")
                           .withRequiredArg
                           .describedAs("kafka://hostname:port")
                           .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val resetBeginningOffsetOpt = parser.accepts("from-latest", "If the consumer does not already have an established " +
      "offset to consume from, start with the latest message present in the log rather than the earliest message.")
    val partitionOpt = parser.accepts("partition", "The topic partition to consume from.")
                           .withRequiredArg
                           .describedAs("partition")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(0)
    val fetchSizeOpt = parser.accepts("fetch-size", "REQUIRED: The fetch size to use for consumption.")
                           .withRequiredArg
                           .describedAs("bytes")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1024*1024)
    val clientIdOpt = parser.accepts("clientId", "The ID of this client.")
                           .withRequiredArg
                           .describedAs("clientId")
                           .ofType(classOf[String])
                           .defaultsTo("SimpleConsumerPerformanceClient")
    val showDetailedStatsOpt = parser.accepts("show-detailed-stats", "If set, stats are reported for each reporting " +
      "interval as configured by reporting-interval")

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, urlOpt, numMessagesOpt)

    val url = new URI(options.valueOf(urlOpt))
    val fetchSize = options.valueOf(fetchSizeOpt).intValue
    val fromLatest = options.has(resetBeginningOffsetOpt)
    val partition = options.valueOf(partitionOpt).intValue
    val topic = options.valueOf(topicOpt)
    val numMessages = options.valueOf(numMessagesOpt).longValue
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    val showDetailedStats = options.has(showDetailedStatsOpt)
    val dateFormat = new SimpleDateFormat(options.valueOf(dateFormatOpt))
    val hideHeader = options.has(hideHeaderOpt)
    val clientId = options.valueOf(clientIdOpt).toString
  }
}
