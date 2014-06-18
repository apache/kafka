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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import java.nio.channels.ClosedByInterruptException
import org.apache.log4j.Logger
import kafka.message.Message
import kafka.utils.{ZkUtils, CommandLineUtils}
import java.util.{ Random, Properties }
import kafka.consumer._
import java.text.SimpleDateFormat

/**
 * Performance test for the full zookeeper consumer
 */
object ConsumerPerformance {
  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]): Unit = {

    val config = new ConsumerPerfConfig(args)
    logger.info("Starting consumer...")
    var totalMessagesRead = new AtomicLong(0)
    var totalBytesRead = new AtomicLong(0)

    if (!config.hideHeader) {
      if (!config.showDetailedStats)
        println("start.time, end.time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec")
      else
        println("time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec")
    }

    // clean up zookeeper state for this group id for every perf run
    ZkUtils.maybeDeletePath(config.consumerConfig.zkConnect, "/consumers/" + config.consumerConfig.groupId)

    val consumerConnector: ConsumerConnector = Consumer.create(config.consumerConfig)

    val topicMessageStreams = consumerConnector.createMessageStreams(Map(config.topic -> config.numThreads))
    var threadList = List[ConsumerPerfThread]()
    for ((topic, streamList) <- topicMessageStreams)
      for (i <- 0 until streamList.length)
        threadList ::= new ConsumerPerfThread(i, "kafka-zk-consumer-" + i, streamList(i), config,
          totalMessagesRead, totalBytesRead)

    logger.info("Sleeping for 1 second.")
    Thread.sleep(1000)
    logger.info("starting threads")
    val startMs = System.currentTimeMillis
    for (thread <- threadList)
      thread.start

    for (thread <- threadList)
      thread.join

    val endMs = System.currentTimeMillis
    val elapsedSecs = (endMs - startMs - config.consumerConfig.consumerTimeoutMs) / 1000.0
    if (!config.showDetailedStats) {
      val totalMBRead = (totalBytesRead.get * 1.0) / (1024 * 1024)
      println(("%s, %s, %d, %.4f, %.4f, %d, %.4f").format(config.dateFormat.format(startMs), config.dateFormat.format(endMs),
        config.consumerConfig.fetchMessageMaxBytes, totalMBRead, totalMBRead / elapsedSecs, totalMessagesRead.get,
        totalMessagesRead.get / elapsedSecs))
    }
    System.exit(0)
  }

  class ConsumerPerfConfig(args: Array[String]) extends PerfConfig(args) {
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val groupIdOpt = parser.accepts("group", "The group id to consume on.")
      .withRequiredArg
      .describedAs("gid")
      .defaultsTo("perf-consumer-" + new Random().nextInt(100000))
      .ofType(classOf[String])
    val fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1024 * 1024)
    val resetBeginningOffsetOpt = parser.accepts("from-latest", "If the consumer does not already have an established " +
      "offset to consume from, start with the latest message present in the log rather than the earliest message.")
    val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(2 * 1024 * 1024)
    val numThreadsOpt = parser.accepts("threads", "Number of processing threads.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10)
    val numFetchersOpt = parser.accepts("num-fetch-threads", "Number of fetcher threads.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)

    val options = parser.parse(args: _*)

    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, zkConnectOpt)

    val props = new Properties
    props.put("group.id", options.valueOf(groupIdOpt))
    props.put("socket.receive.buffer.bytes", options.valueOf(socketBufferSizeOpt).toString)
    props.put("fetch.message.max.bytes", options.valueOf(fetchSizeOpt).toString)
    props.put("auto.offset.reset", if (options.has(resetBeginningOffsetOpt)) "largest" else "smallest")
    props.put("zookeeper.connect", options.valueOf(zkConnectOpt))
    props.put("consumer.timeout.ms", "5000")
    props.put("num.consumer.fetchers", options.valueOf(numFetchersOpt).toString)
    val consumerConfig = new ConsumerConfig(props)
    val numThreads = options.valueOf(numThreadsOpt).intValue
    val topic = options.valueOf(topicOpt)
    val numMessages = options.valueOf(numMessagesOpt).longValue
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    val showDetailedStats = options.has(showDetailedStatsOpt)
    val dateFormat = new SimpleDateFormat(options.valueOf(dateFormatOpt))
    val hideHeader = options.has(hideHeaderOpt)
  }

  class ConsumerPerfThread(threadId: Int, name: String, stream: KafkaStream[Array[Byte], Array[Byte]],
    config: ConsumerPerfConfig, totalMessagesRead: AtomicLong, totalBytesRead: AtomicLong)
    extends Thread(name) {

    override def run() {
      var bytesRead = 0L
      var messagesRead = 0L
      val startMs = System.currentTimeMillis
      var lastReportTime: Long = startMs
      var lastBytesRead = 0L
      var lastMessagesRead = 0L

      try {
        val iter = stream.iterator
        while (iter.hasNext && messagesRead < config.numMessages) {
          val messageAndMetadata = iter.next
          messagesRead += 1
          bytesRead += messageAndMetadata.message.length

          if (messagesRead % config.reportingInterval == 0) {
            if (config.showDetailedStats)
              printMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, lastReportTime, System.currentTimeMillis)
            lastReportTime = System.currentTimeMillis
            lastMessagesRead = messagesRead
            lastBytesRead = bytesRead
          }
        }
      } catch {
        case _: InterruptedException =>
        case _: ClosedByInterruptException =>
        case _: ConsumerTimeoutException =>
        case e: Throwable => e.printStackTrace()
      }
      totalMessagesRead.addAndGet(messagesRead)
      totalBytesRead.addAndGet(bytesRead)
      if (config.showDetailedStats)
        printMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, startMs, System.currentTimeMillis)
    }

    private def printMessage(id: Int, bytesRead: Long, lastBytesRead: Long, messagesRead: Long, lastMessagesRead: Long,
      startMs: Long, endMs: Long) = {
      val elapsedMs = endMs - startMs
      val totalMBRead = (bytesRead * 1.0) / (1024 * 1024)
      val mbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024)
      println(("%s, %d, %d, %.4f, %.4f, %d, %.4f").format(config.dateFormat.format(endMs), id,
        config.consumerConfig.fetchMessageMaxBytes, totalMBRead,
        1000.0 * (mbRead / elapsedMs), messagesRead, ((messagesRead - lastMessagesRead) / elapsedMs) * 1000.0))
    }
  }

}
