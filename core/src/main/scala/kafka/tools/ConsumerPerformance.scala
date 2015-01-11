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

import scala.collection.JavaConversions._
import java.util.concurrent.atomic.AtomicLong
import java.nio.channels.ClosedByInterruptException
import org.apache.log4j.Logger
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import kafka.message.Message
import kafka.utils.{ZkUtils, CommandLineUtils}
import java.util.{ Random, Properties }
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConnector
import kafka.consumer.KafkaStream
import kafka.consumer.ConsumerTimeoutException
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
        println("start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec")
      else
        println("time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec")
    }

    var startMs, endMs = 0L
    if(config.useNewConsumer) {
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config.props)
      consumer.subscribe(config.topic)
      startMs = System.currentTimeMillis
      consume(consumer, config.numMessages, 1000, config, totalMessagesRead, totalBytesRead)
      endMs = System.currentTimeMillis
    } else {
      import kafka.consumer.ConsumerConfig
      val consumerConfig = new ConsumerConfig(config.props)
      val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
      val topicMessageStreams = consumerConnector.createMessageStreams(Map(config.topic -> config.numThreads))
      var threadList = List[ConsumerPerfThread]()
      for ((topic, streamList) <- topicMessageStreams)
        for (i <- 0 until streamList.length)
          threadList ::= new ConsumerPerfThread(i, "kafka-zk-consumer-" + i, streamList(i), config, totalMessagesRead, totalBytesRead)

      logger.info("Sleeping for 1 second.")
      Thread.sleep(1000)
      logger.info("starting threads")
      startMs = System.currentTimeMillis
      for (thread <- threadList)
        thread.start
      for (thread <- threadList)
        thread.join
      endMs = System.currentTimeMillis - consumerConfig.consumerTimeoutMs 
    }
    val elapsedSecs = (endMs - startMs) / 1000.0
    if (!config.showDetailedStats) {
      val totalMBRead = (totalBytesRead.get * 1.0) / (1024 * 1024)
      println(("%s, %s, %.4f, %.4f, %d, %.4f").format(config.dateFormat.format(startMs), config.dateFormat.format(endMs),
        totalMBRead, totalMBRead / elapsedSecs, totalMessagesRead.get, totalMessagesRead.get / elapsedSecs))
    }
    System.exit(0)
  }
  
  def consume(consumer: KafkaConsumer[Array[Byte], Array[Byte]], count: Long, timeout: Long, config: ConsumerPerfConfig, totalMessagesRead: AtomicLong, totalBytesRead: AtomicLong) {
    var bytesRead = 0L
    var messagesRead = 0L
    val startMs = System.currentTimeMillis
    var lastReportTime: Long = startMs
    var lastBytesRead = 0L
    var lastMessagesRead = 0L
    var lastConsumed = System.currentTimeMillis
    while(messagesRead < count && lastConsumed >= System.currentTimeMillis - timeout) {
      val records = consumer.poll(100)
      if(records.count() > 0)
        lastConsumed = System.currentTimeMillis
      for(record <- records) {
        messagesRead += 1
        if(record.key != null)
          bytesRead += record.key.size
        if(record.value != null)
          bytesRead += record.value.size 
      
        if (messagesRead % config.reportingInterval == 0) {
          if (config.showDetailedStats)
            printProgressMessage(0, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, lastReportTime, System.currentTimeMillis, config.dateFormat)
          lastReportTime = System.currentTimeMillis
          lastMessagesRead = messagesRead
          lastBytesRead = bytesRead
        }
      }
    }
    totalMessagesRead.set(messagesRead)
    totalBytesRead.set(bytesRead)
  }
  
  def printProgressMessage(id: Int, bytesRead: Long, lastBytesRead: Long, messagesRead: Long, lastMessagesRead: Long,
    startMs: Long, endMs: Long, dateFormat: SimpleDateFormat) = {
    val elapsedMs: Double = endMs - startMs
    val totalMBRead = (bytesRead * 1.0) / (1024 * 1024)
    val mbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024)
    println(("%s, %d, %.4f, %.4f, %d, %.4f").format(dateFormat.format(endMs), id, totalMBRead,
        1000.0 * (mbRead / elapsedMs), messagesRead, ((messagesRead - lastMessagesRead) / elapsedMs) * 1000.0))
  }

  class ConsumerPerfConfig(args: Array[String]) extends PerfConfig(args) {
    val zkConnectOpt = parser.accepts("zookeeper", "The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over. This option is only used with the old consumer.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
    val bootstrapServersOpt = parser.accepts("broker-list", "A broker list to use for connecting if using the new consumer.")
      .withRequiredArg()
      .describedAs("host")
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
    val useNewConsumerOpt = parser.accepts("new-consumer", "Use the new consumer implementation.")

    val options = parser.parse(args: _*)

    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt)
   
    val useNewConsumer = options.has(useNewConsumerOpt)
    
    val props = new Properties
    if(useNewConsumer) {
      import org.apache.kafka.clients.consumer.ConsumerConfig
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServersOpt))
      props.put(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(groupIdOpt))
      props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, options.valueOf(socketBufferSizeOpt).toString)
      props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, options.valueOf(fetchSizeOpt).toString)
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, if (options.has(resetBeginningOffsetOpt)) "latest" else "earliest")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer])
      props.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false")
    } else {
      CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)
      props.put("group.id", options.valueOf(groupIdOpt))
      props.put("socket.receive.buffer.bytes", options.valueOf(socketBufferSizeOpt).toString)
      props.put("fetch.message.max.bytes", options.valueOf(fetchSizeOpt).toString)
      props.put("auto.offset.reset", if (options.has(resetBeginningOffsetOpt)) "largest" else "smallest")
      props.put("zookeeper.connect", options.valueOf(zkConnectOpt))
      props.put("consumer.timeout.ms", "1000")
      props.put("num.consumer.fetchers", options.valueOf(numFetchersOpt).toString)
    }
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
              printProgressMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, lastReportTime, System.currentTimeMillis, config.dateFormat)
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
        printProgressMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, startMs, System.currentTimeMillis, config.dateFormat)
    }

  }

}
