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

import java.text.SimpleDateFormat
import java.time.Duration
import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.{Properties, Random}

import com.typesafe.scalalogging.LazyLogging
import joptsimple.OptionException
import kafka.utils.{CommandLineUtils, ToolsUtils}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}

import scala.jdk.CollectionConverters._
import scala.collection.mutable

/**
 * Performance test for the full zookeeper consumer
 */
object ConsumerPerformance extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val config = new ConsumerPerfConfig(args)
    logger.info("Starting consumer...")
    val totalMessagesRead = new AtomicLong(0)
    val totalBytesRead = new AtomicLong(0)
    var metrics: mutable.Map[MetricName, _ <: Metric] = null
    val joinGroupTimeInMs = new AtomicLong(0)

    if (!config.hideHeader)
      printHeader(config.showDetailedStats)

    var startMs, endMs = 0L
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config.props)
    startMs = System.currentTimeMillis
    consume(consumer, List(config.topic), config.numMessages, config.recordFetchTimeoutMs, config, totalMessagesRead, totalBytesRead, joinGroupTimeInMs, startMs)
    endMs = System.currentTimeMillis

    if (config.printMetrics) {
      metrics = consumer.metrics.asScala
    }
    consumer.close()
    val elapsedSecs = (endMs - startMs) / 1000.0
    val fetchTimeInMs = (endMs - startMs) - joinGroupTimeInMs.get
    if (!config.showDetailedStats) {
      val totalMBRead = (totalBytesRead.get * 1.0) / (1024 * 1024)
      println("%s, %s, %.4f, %.4f, %d, %.4f, %d, %d, %.4f, %.4f".format(
        config.dateFormat.format(startMs),
        config.dateFormat.format(endMs),
        totalMBRead,
        totalMBRead / elapsedSecs,
        totalMessagesRead.get,
        totalMessagesRead.get / elapsedSecs,
        joinGroupTimeInMs.get,
        fetchTimeInMs,
        totalMBRead / (fetchTimeInMs / 1000.0),
        totalMessagesRead.get / (fetchTimeInMs / 1000.0)
      ))
    }

    if (metrics != null) {
      ToolsUtils.printMetrics(metrics)
    }

  }

  private[tools] def printHeader(showDetailedStats: Boolean): Unit = {
    val newFieldsInHeader = ", rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec"
    if (!showDetailedStats)
      println("start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec" + newFieldsInHeader)
    else
      println("time, threadId, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec" + newFieldsInHeader)
  }

  def consume(consumer: KafkaConsumer[Array[Byte], Array[Byte]],
              topics: List[String],
              count: Long,
              timeout: Long,
              config: ConsumerPerfConfig,
              totalMessagesRead: AtomicLong,
              totalBytesRead: AtomicLong,
              joinTime: AtomicLong,
              testStartTime: Long): Unit = {
    var bytesRead = 0L
    var messagesRead = 0L
    var lastBytesRead = 0L
    var lastMessagesRead = 0L
    var joinStart = System.currentTimeMillis
    var joinTimeMsInSingleRound = 0L

    consumer.subscribe(topics.asJava, new ConsumerRebalanceListener {
      def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        joinTime.addAndGet(System.currentTimeMillis - joinStart)
        joinTimeMsInSingleRound += System.currentTimeMillis - joinStart
      }
      def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        joinStart = System.currentTimeMillis
      }})

    // Now start the benchmark
    var currentTimeMillis = System.currentTimeMillis
    var lastReportTime: Long = currentTimeMillis
    var lastConsumedTime = currentTimeMillis

    while (messagesRead < count && currentTimeMillis - lastConsumedTime <= timeout) {
      val records = consumer.poll(Duration.ofMillis(100)).asScala
      currentTimeMillis = System.currentTimeMillis
      if (records.nonEmpty)
        lastConsumedTime = currentTimeMillis
      for (record <- records) {
        messagesRead += 1
        if (record.key != null)
          bytesRead += record.key.size
        if (record.value != null)
          bytesRead += record.value.size

        if (currentTimeMillis - lastReportTime >= config.reportingInterval) {
          if (config.showDetailedStats)
            printConsumerProgress(0, bytesRead, lastBytesRead, messagesRead, lastMessagesRead,
              lastReportTime, currentTimeMillis, config.dateFormat, joinTimeMsInSingleRound)
          joinTimeMsInSingleRound = 0L
          lastReportTime = currentTimeMillis
          lastMessagesRead = messagesRead
          lastBytesRead = bytesRead
        }
      }
    }

    if (messagesRead < count)
      println(s"WARNING: Exiting before consuming the expected number of messages: timeout ($timeout ms) exceeded. " +
        "You can use the --timeout option to increase the timeout.")
    totalMessagesRead.set(messagesRead)
    totalBytesRead.set(bytesRead)
  }

  def printConsumerProgress(id: Int,
                               bytesRead: Long,
                               lastBytesRead: Long,
                               messagesRead: Long,
                               lastMessagesRead: Long,
                               startMs: Long,
                               endMs: Long,
                               dateFormat: SimpleDateFormat,
                               periodicJoinTimeInMs: Long): Unit = {
    printBasicProgress(id, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, startMs, endMs, dateFormat)
    printExtendedProgress(bytesRead, lastBytesRead, messagesRead, lastMessagesRead, startMs, endMs, periodicJoinTimeInMs)
    println()
  }

  private def printBasicProgress(id: Int,
                                 bytesRead: Long,
                                 lastBytesRead: Long,
                                 messagesRead: Long,
                                 lastMessagesRead: Long,
                                 startMs: Long,
                                 endMs: Long,
                                 dateFormat: SimpleDateFormat): Unit = {
    val elapsedMs: Double = (endMs - startMs).toDouble
    val totalMbRead = (bytesRead * 1.0) / (1024 * 1024)
    val intervalMbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024)
    val intervalMbPerSec = 1000.0 * intervalMbRead / elapsedMs
    val intervalMessagesPerSec = ((messagesRead - lastMessagesRead) / elapsedMs) * 1000.0
    print("%s, %d, %.4f, %.4f, %d, %.4f".format(dateFormat.format(endMs), id, totalMbRead,
      intervalMbPerSec, messagesRead, intervalMessagesPerSec))
  }

  private def printExtendedProgress(bytesRead: Long,
                                    lastBytesRead: Long,
                                    messagesRead: Long,
                                    lastMessagesRead: Long,
                                    startMs: Long,
                                    endMs: Long,
                                    periodicJoinTimeInMs: Long): Unit = {
    val fetchTimeMs = endMs - startMs - periodicJoinTimeInMs
    val intervalMbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024)
    val intervalMessagesRead = messagesRead - lastMessagesRead
    val (intervalMbPerSec, intervalMessagesPerSec) = if (fetchTimeMs <= 0)
      (0.0, 0.0)
    else
      (1000.0 * intervalMbRead / fetchTimeMs, 1000.0 * intervalMessagesRead / fetchTimeMs)
    print(", %d, %d, %.4f, %.4f".format(periodicJoinTimeInMs, fetchTimeMs, intervalMbPerSec, intervalMessagesPerSec))
  }

  class ConsumerPerfConfig(args: Array[String]) extends PerfConfig(args) {
    val brokerListOpt = parser.accepts("broker-list", "DEPRECATED, use --bootstrap-server instead; ignored if --bootstrap-server is specified.  The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
      .withRequiredArg
      .describedAs("broker-list")
      .ofType(classOf[String])
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED unless --broker-list(deprecated) is specified. The server(s) to connect to.")
      .requiredUnless("broker-list")
      .withRequiredArg
      .describedAs("server to connect to")
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
    val numThreadsOpt = parser.accepts("threads", "DEPRECATED AND IGNORED: Number of processing threads.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10)
    val numFetchersOpt = parser.accepts("num-fetch-threads", "DEPRECATED AND IGNORED: Number of fetcher threads.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)
    val consumerConfigOpt = parser.accepts("consumer.config", "Consumer config properties file.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val printMetricsOpt = parser.accepts("print-metrics", "Print out the metrics.")
    val showDetailedStatsOpt = parser.accepts("show-detailed-stats", "If set, stats are reported for each reporting " +
      "interval as configured by reporting-interval")
    val recordFetchTimeoutOpt = parser.accepts("timeout", "The maximum allowed time in milliseconds between returned records.")
      .withOptionalArg()
      .describedAs("milliseconds")
      .ofType(classOf[Long])
      .defaultsTo(10000)

    try
      options = parser.parse(args: _*)
    catch {
      case e: OptionException =>
        CommandLineUtils.printUsageAndDie(parser, e.getMessage)
    }

    if(options.has(numThreadsOpt) || options.has(numFetchersOpt))
      println("WARNING: option [threads] and [num-fetch-threads] have been deprecated and will be ignored by the test")

    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool helps in performance test for the full zookeeper consumer")

    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, numMessagesOpt)

    val printMetrics = options.has(printMetricsOpt)

    val props = if (options.has(consumerConfigOpt))
      Utils.loadProps(options.valueOf(consumerConfigOpt))
    else
      new Properties

    import org.apache.kafka.clients.consumer.ConsumerConfig

    val brokerHostsAndPorts = options.valueOf(if (options.has(bootstrapServerOpt)) bootstrapServerOpt else brokerListOpt)
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHostsAndPorts)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(groupIdOpt))
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, options.valueOf(socketBufferSizeOpt).toString)
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, options.valueOf(fetchSizeOpt).toString)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, if (options.has(resetBeginningOffsetOpt)) "latest" else "earliest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
    props.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false")
    if (props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) == null)
      props.put(ConsumerConfig.CLIENT_ID_CONFIG, "perf-consumer-client")

    val numThreads = options.valueOf(numThreadsOpt).intValue
    val topic = options.valueOf(topicOpt)
    val numMessages = options.valueOf(numMessagesOpt).longValue
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    if (reportingInterval <= 0)
      throw new IllegalArgumentException("Reporting interval must be greater than 0.")
    val showDetailedStats = options.has(showDetailedStatsOpt)
    val dateFormat = new SimpleDateFormat(options.valueOf(dateFormatOpt))
    val hideHeader = options.has(hideHeaderOpt)
    val recordFetchTimeoutMs = options.valueOf(recordFetchTimeoutOpt).longValue()
  }

}
