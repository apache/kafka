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

package kafka.perf

import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.atomic.AtomicLong
import kafka.producer._
import org.apache.log4j.Logger
import kafka.message.{CompressionCodec, Message}
import java.text.SimpleDateFormat
import kafka.serializer._
import java.util._
import collection.immutable.List
import kafka.utils.{VerifiableProperties, Logging}
import kafka.metrics.KafkaMetricsReporter


/**
 * Load test for the producer
 */
object ProducerPerformance extends Logging {

  def main(args: Array[String]) {

    val logger = Logger.getLogger(getClass)
    val config = new ProducerPerfConfig(args)
    if(!config.isFixSize)
      logger.info("WARN: Throughput will be slower due to changing message size per request")

    val totalBytesSent = new AtomicLong(0)
    val totalMessagesSent = new AtomicLong(0)
    val executor = Executors.newFixedThreadPool(config.numThreads)
    val allDone = new CountDownLatch(config.numThreads)
    val startMs = System.currentTimeMillis
    val rand = new java.util.Random

    if(!config.hideHeader)
        println("start.time, end.time, compression, message.size, batch.size, total.data.sent.in.MB, MB.sec, " +
                        "total.data.sent.in.nMsg, nMsg.sec")

    for(i <- 0 until config.numThreads) {
      executor.execute(new ProducerThread(i, config, totalBytesSent, totalMessagesSent, allDone, rand))
    }

    allDone.await()
    val endMs = System.currentTimeMillis
    val elapsedSecs = (endMs - startMs) / 1000.0
    val totalMBSent = (totalBytesSent.get * 1.0)/ (1024 * 1024)
    println(("%s, %s, %d, %d, %d, %.2f, %.4f, %d, %.4f").format(
      config.dateFormat.format(startMs), config.dateFormat.format(endMs),
      config.compressionCodec.codec, config.messageSize, config.batchSize, totalMBSent,
      totalMBSent/elapsedSecs, totalMessagesSent.get, totalMessagesSent.get/elapsedSecs))
    System.exit(0)
  }

  class ProducerPerfConfig(args: Array[String]) extends PerfConfig(args) {
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: broker info (the list of broker host and port for bootstrap.")
            .withRequiredArg
            .describedAs("hostname:port,..,hostname:port")
            .ofType(classOf[String])
    val topicsOpt = parser.accepts("topics", "REQUIRED: The comma separated list of topics to produce to")
      .withRequiredArg
      .describedAs("topic1,topic2..")
      .ofType(classOf[String])
    val producerRequestTimeoutMsOpt = parser.accepts("request-timeout-ms", "The produce request timeout in ms")
            .withRequiredArg()
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(3000)
    val producerNumRetriesOpt = parser.accepts("producer-num-retries", "The producer retries number")
            .withRequiredArg()
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(3)
    val producerRetryBackOffMsOpt = parser.accepts("producer-retry-backoff-ms", "The producer retry backoff time in milliseconds")
            .withRequiredArg()
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(100)
    val producerRequestRequiredAcksOpt = parser.accepts("request-num-acks", "Number of acks required for producer request " +
            "to complete")
            .withRequiredArg()
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(-1)
    val varyMessageSizeOpt = parser.accepts("vary-message-size", "If set, message size will vary up to the given maximum.")
    val syncOpt = parser.accepts("sync", "If set, messages are sent synchronously.")
    val numThreadsOpt = parser.accepts("threads", "Number of sending threads.")
            .withRequiredArg
            .describedAs("number of threads")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(1)
    val initialMessageIdOpt = parser.accepts("initial-message-id", "The is used for generating test data, If set, messages will be tagged with an " +
            "ID and sent by producer starting from this ID sequentially. Message content will be String type and " +
            "in the form of 'Message:000...1:xxx...'")
            .withRequiredArg()
            .describedAs("initial message id")
            .ofType(classOf[java.lang.Integer])
    val messageSendGapMsOpt = parser.accepts("message-send-gap-ms", "If set, the send thread will wait for specified time between two sends")
            .withRequiredArg()
            .describedAs("message send time gap")
            .ofType(classOf[java.lang.Integer])
            .defaultsTo(0)
    val csvMetricsReporterEnabledOpt = parser.accepts("csv-reporter-enabled", "If set, the CSV metrics reporter will be enabled")
    val metricsDirectoryOpt = parser.accepts("metrics-dir", "If csv-reporter-enable is set, and this parameter is" +
            "set, the csv metrics will be outputed here")
      .withRequiredArg
      .describedAs("metrics dictory")
      .ofType(classOf[java.lang.String])

    val options = parser.parse(args : _*)
    for(arg <- List(topicsOpt, brokerListOpt, numMessagesOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
    val topicsStr = options.valueOf(topicsOpt)
    val topics = topicsStr.split(",")
    val numMessages = options.valueOf(numMessagesOpt).longValue
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    val dateFormat = new SimpleDateFormat(options.valueOf(dateFormatOpt))
    val hideHeader = options.has(hideHeaderOpt)
    val brokerList = options.valueOf(brokerListOpt)
    val messageSize = options.valueOf(messageSizeOpt).intValue
    var isFixSize = !options.has(varyMessageSizeOpt)
    var isSync = options.has(syncOpt)
    var batchSize = options.valueOf(batchSizeOpt).intValue
    var numThreads = options.valueOf(numThreadsOpt).intValue
    val compressionCodec = CompressionCodec.getCompressionCodec(options.valueOf(compressionCodecOpt).intValue)
    val seqIdMode = options.has(initialMessageIdOpt)
    var initialMessageId: Int = 0
    if(seqIdMode)
      initialMessageId = options.valueOf(initialMessageIdOpt).intValue()
    val producerRequestTimeoutMs = options.valueOf(producerRequestTimeoutMsOpt).intValue()
    val producerRequestRequiredAcks = options.valueOf(producerRequestRequiredAcksOpt).intValue()
    val producerNumRetries = options.valueOf(producerNumRetriesOpt).intValue()
    val producerRetryBackoffMs = options.valueOf(producerRetryBackOffMsOpt).intValue()

    val csvMetricsReporterEnabled = options.has(csvMetricsReporterEnabledOpt)

    if (csvMetricsReporterEnabled) {
      val props = new Properties()
      props.put("kafka.metrics.polling.interval.secs", "1")
      props.put("kafka.metrics.reporters", "kafka.metrics.KafkaCSVMetricsReporter")
      if (options.has(metricsDirectoryOpt))
        props.put("kafka.csv.metrics.dir", options.valueOf(metricsDirectoryOpt))
      else
        props.put("kafka.csv.metrics.dir", "kafka_metrics")
      props.put("kafka.csv.metrics.reporter.enabled", "true")
      val verifiableProps = new VerifiableProperties(props)
      KafkaMetricsReporter.startReporters(verifiableProps)
    }

    val messageSendGapMs = options.valueOf(messageSendGapMsOpt).intValue()
  }

  class ProducerThread(val threadId: Int,
                       val config: ProducerPerfConfig,
                       val totalBytesSent: AtomicLong,
                       val totalMessagesSent: AtomicLong,
                       val allDone: CountDownLatch,
                       val rand: Random) extends Runnable {
    val props = new Properties()
    props.put("broker.list", config.brokerList)
    props.put("compression.codec", config.compressionCodec.codec.toString)
    props.put("reconnect.interval", Integer.MAX_VALUE.toString)
    props.put("send.buffer.bytes", (64*1024).toString)
    if(!config.isSync) {
      props.put("producer.type","async")
      props.put("batch.num.messages", config.batchSize.toString)
      props.put("queue.enqueue.timeout.ms", "-1")
    }
    props.put("client.id", "ProducerPerformance")
    props.put("request.required.acks", config.producerRequestRequiredAcks.toString)
    props.put("request.timeout.ms", config.producerRequestTimeoutMs.toString)
    props.put("message.send.max.retries", config.producerNumRetries.toString)
    props.put("retry.backoff.ms", config.producerRetryBackoffMs.toString)
    props.put("serializer.class", classOf[DefaultEncoder].getName.toString)
    props.put("key.serializer.class", classOf[NullEncoder[Long]].getName.toString)

    
    val producerConfig = new ProducerConfig(props)
    val producer = new Producer[Long, Array[Byte]](producerConfig)
    val seqIdNumDigit = 10   // no. of digits for max int value

    val messagesPerThread = config.numMessages / config.numThreads
    debug("Messages per thread = " + messagesPerThread)

    // generate the sequential message ID
    private val SEP            = ":"              // message field separator
    private val messageIdLabel = "MessageID"
    private val threadIdLabel  = "ThreadID"
    private val topicLabel     = "Topic"
    private var leftPaddedSeqId : String = ""

    private def generateMessageWithSeqId(topic: String, msgId: Long, msgSize: Int): Array[Byte] = {
      // Each thread gets a unique range of sequential no. for its ids.
      // Eg. 1000 msg in 10 threads => 100 msg per thread
      // thread 0 IDs :   0 ~  99
      // thread 1 IDs : 100 ~ 199
      // thread 2 IDs : 200 ~ 299
      // . . .
      leftPaddedSeqId = String.format("%0"+seqIdNumDigit+"d", long2Long(msgId))

      val msgHeader = topicLabel      + SEP +
              topic           + SEP +
              threadIdLabel   + SEP +
              threadId        + SEP +
              messageIdLabel  + SEP +
              leftPaddedSeqId + SEP

      val seqMsgString = String.format("%1$-"+msgSize+"s", msgHeader).replace(' ', 'x')
      debug(seqMsgString)
      return seqMsgString.getBytes()
    }

    private def generateProducerData(topic: String, messageId: Long): (KeyedMessage[Long, Array[Byte]], Int) = {
      val msgSize = if(config.isFixSize) config.messageSize else 1 + rand.nextInt(config.messageSize)
      val message =
        if(config.seqIdMode) {
          val seqId = config.initialMessageId + (messagesPerThread * threadId) + messageId
          generateMessageWithSeqId(topic, seqId, msgSize)
        } else {
          new Array[Byte](msgSize)
        }
      (new KeyedMessage[Long, Array[Byte]](topic, messageId, message), message.length)
    }

    override def run {
      var bytesSent = 0L
      var nSends = 0
      var j: Long = 0L
      while(j < messagesPerThread) {
        try {
          config.topics.foreach(
            topic => {
              val (producerData, bytesSent_) = generateProducerData(topic, j)
              bytesSent += bytesSent_
              producer.send(producerData)
              nSends += 1
              if(config.messageSendGapMs > 0)
                Thread.sleep(config.messageSendGapMs)
            }
          )
        } catch {
          case e: Exception => error("Error sending messages", e)
        }
        j += 1
      }
      producer.close()
      totalBytesSent.addAndGet(bytesSent)
      totalMessagesSent.addAndGet(nSends)
      allDone.countDown()
    }
  }
}
