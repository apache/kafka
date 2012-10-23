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
import java.util.{Random, Properties}
import kafka.utils.{VerifiableProperties, Logging}
import kafka.metrics.{KafkaCSVMetricsReporter, KafkaMetricsReporterMBean, KafkaMetricsReporter, KafkaMetricsConfig}


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

    if(!config.hideHeader) {
      if(!config.showDetailedStats)
        println("start.time, end.time, compression, message.size, batch.size, total.data.sent.in.MB, MB.sec, " +
          "total.data.sent.in.nMsg, nMsg.sec")
      else
        println("time, compression, thread.id, message.size, batch.size, total.data.sent.in.MB, MB.sec, " +
          "total.data.sent.in.nMsg, nMsg.sec")
    }

    for(i <- 0 until config.numThreads) {
      executor.execute(new ProducerThread(i, config, totalBytesSent, totalMessagesSent, allDone, rand))
    }

    allDone.await()
    val endMs = System.currentTimeMillis
    val elapsedSecs = (endMs - startMs) / 1000.0
    if(!config.showDetailedStats) {
      val totalMBSent = (totalBytesSent.get * 1.0)/ (1024 * 1024)
      println(("%s, %s, %d, %d, %d, %.2f, %.4f, %d, %.4f").format(config.dateFormat.format(startMs),
        config.dateFormat.format(endMs), config.compressionCodec.codec, config.messageSize, config.batchSize,
        totalMBSent, totalMBSent/elapsedSecs, totalMessagesSent.get, totalMessagesSent.get/elapsedSecs))
    }
    System.exit(0)
  }

  class ProducerPerfConfig(args: Array[String]) extends PerfConfig(args) {
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: the broker list must be specified.")
      .withRequiredArg
      .describedAs("hostname:port")
      .ofType(classOf[String])
    val produceRequestTimeoutMsOpt = parser.accepts("request-timeout-ms", "The produce request timeout in ms")
      .withRequiredArg()
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(3000)
    val produceRequestRequiredAcksOpt = parser.accepts("request-num-acks", "Number of acks required for producer request " +
      "to complete")
      .withRequiredArg()
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(-1)
    val messageSizeOpt = parser.accepts("message-size", "The size of each message.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100)
    val varyMessageSizeOpt = parser.accepts("vary-message-size", "If set, message size will vary up to the given maximum.")
    val asyncOpt = parser.accepts("async", "If set, messages are sent asynchronously.")
    val batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(200)
    val numThreadsOpt = parser.accepts("threads", "Number of sending threads.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)
    val compressionCodecOption = parser.accepts("compression-codec", "If set, messages are sent compressed")
      .withRequiredArg
      .describedAs("compression codec ")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(0)
    val initialMessageIdOpt = parser.accepts("initial-message-id", "If set, messages will be tagged with an " + 
        "ID and sent by producer starting from this ID sequentially. Message content will be String type and " + 
        "in the form of 'Message:000...1:xxx...'")
      .withRequiredArg()
      .describedAs("initial message id")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(0)
    val csvMetricsReporterEnabledOpt = parser.accepts("csv-reporter-enabled", "If set, the CSV metrics reporter will be enabled")
    val metricsDirectoryOpt = parser.accepts("metrics-dir", "If csv-reporter-enable is set, and this parameter is" +
            "set, the csv metrics will be outputed here")
      .withRequiredArg
      .describedAs("metrics dictory")
      .ofType(classOf[java.lang.String])

    val options = parser.parse(args : _*)
    for(arg <- List(topicOpt, brokerListOpt, numMessagesOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
    val topic = options.valueOf(topicOpt)
    val numMessages = options.valueOf(numMessagesOpt).longValue
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    val showDetailedStats = options.has(showDetailedStatsOpt)
    val dateFormat = new SimpleDateFormat(options.valueOf(dateFormatOpt))
    val hideHeader = options.has(hideHeaderOpt)
    val brokerList = options.valueOf(brokerListOpt)
    val messageSize = options.valueOf(messageSizeOpt).intValue
    var isFixSize = !options.has(varyMessageSizeOpt)
    var isAsync = options.has(asyncOpt)
    var batchSize = options.valueOf(batchSizeOpt).intValue
    var numThreads = options.valueOf(numThreadsOpt).intValue
    val compressionCodec = CompressionCodec.getCompressionCodec(options.valueOf(compressionCodecOption).intValue)
    val initialMessageId = options.valueOf(initialMessageIdOpt).intValue()
    val seqIdMode = options.has(initialMessageIdOpt)
    val produceRequestTimeoutMs = options.valueOf(produceRequestTimeoutMsOpt).intValue()
    val produceRequestRequiredAcks = options.valueOf(produceRequestRequiredAcksOpt).intValue()

    val csvMetricsReporterEnabled = options.has(csvMetricsReporterEnabledOpt)

    if (csvMetricsReporterEnabled) {
      val props = new Properties()
      props.put("kafka.metrics.polling.interval.secs", "5")
      props.put("kafka.metrics.reporters", "kafka.metrics.KafkaCSVMetricsReporter")
      if (options.has(metricsDirectoryOpt))
        props.put("kafka.csv.metrics.dir", options.valueOf(metricsDirectoryOpt))
      else
        props.put("kafka.csv.metrics.dir", "kafka_metrics")
      props.put("kafka.csv.metrics.reporter.enabled", "true")
      val verifiableProps = new VerifiableProperties(props)
      KafkaCSVMetricsReporter.startCSVMetricReporter(verifiableProps)
    }

    // override necessary flags in seqIdMode
    if (seqIdMode) { 
      batchSize = 1
      isFixSize = true

      warn("seqIdMode - isAsync is overridden to:" + isAsync)
      warn("seqIdMode - batchSize is overridden to:" + batchSize)
      warn("seqIdMode - sFixSize is overridden to: " + isFixSize)
    }
  }

  private def getStringOfLength(len: Int) : String = {
    val strArray = new Array[Char](len)
    for (i <- 0 until len)
      strArray(i) = 'x'
    return new String(strArray)
  }

  private def getByteArrayOfLength(len: Int): Array[Byte] = {
    //new Array[Byte](len)
    new Array[Byte]( if (len == 0) 5 else len )
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
    props.put("buffer.size", (64*1024).toString)
    if(config.isAsync) {
      props.put("producer.type","async")
      props.put("batch.size", config.batchSize.toString)
      props.put("queue.enqueueTimeout.ms", "-1")
    }
    props.put("producer.request.required.acks", config.produceRequestRequiredAcks.toString)
    props.put("producer.request.timeout.ms", config.produceRequestTimeoutMs.toString)

    val producerConfig = new ProducerConfig(props)
    val producer = new Producer[Message, Message](producerConfig)
    val seqIdNumDigit = 10   // no. of digits for max int value

    override def run {
      var bytesSent = 0L
      var lastBytesSent = 0L
      var nSends = 0
      var lastNSends = 0
      var message = new Message(new Array[Byte](config.messageSize))
      var reportTime = System.currentTimeMillis()
      var lastReportTime = reportTime
      val messagesPerThread = if(!config.isAsync) config.numMessages / config.numThreads / config.batchSize
                              else config.numMessages / config.numThreads
      debug("Messages per thread = " + messagesPerThread)

      // generate the sequential message ID
      val SEP            = ":"              // message field separator
      val messageIdLabel = "MessageID"
      val threadIdLabel  = "ThreadID"
      val topicLabel     = "Topic"
      var leftPaddedSeqId : String = ""
      
      var j: Long = 0L
      while(j < messagesPerThread) {
        var strLength = config.messageSize
        
        if (config.seqIdMode) {
          // Each thread gets a unique range of sequential no. for its ids.
          // Eg. 1000 msg in 10 threads => 100 msg per thread
          // thread 0 IDs :   0 ~  99
          // thread 1 IDs : 100 ~ 199
          // thread 2 IDs : 200 ~ 299
          // . . .
          
          val msgId = config.initialMessageId + (messagesPerThread * threadId) + j
          leftPaddedSeqId = String.format("%0"+seqIdNumDigit+"d", long2Long(msgId))
          
          val msgHeader = topicLabel      + SEP + 
                          config.topic    + SEP + 
                          threadIdLabel   + SEP + 
                          threadId        + SEP + 
                          messageIdLabel  + SEP + 
                          leftPaddedSeqId + SEP
                             
          val seqMsgString = String.format("%1$-"+config.messageSize+"s", msgHeader).replace(' ', 'x')
          
          debug(seqMsgString)
          message = new Message(seqMsgString.getBytes())
        }

        var messageSet: List[Message] = Nil
        if(config.isFixSize) {
          for(k <- 0 until config.batchSize) {
            messageSet ::= message
          }
        }

        if (!config.isFixSize) {
          for(k <- 0 until config.batchSize) {
            strLength = rand.nextInt(config.messageSize)
            messageSet ::= message
            bytesSent += message.payloadSize
          }
        }else if(!config.isAsync) {
          bytesSent += config.batchSize*message.payloadSize
        }
        try  {
          if(!config.isAsync) {
            producer.send(new ProducerData[Message,Message](config.topic, null, messageSet))
            if(!config.isFixSize) messageSet = Nil
            nSends += config.batchSize
          }else {
            if(!config.isFixSize) {
              strLength = rand.nextInt(config.messageSize)
              val messageBytes = getByteArrayOfLength(strLength)
              rand.nextBytes(messageBytes)
              val message = new Message(messageBytes)
              producer.send(new ProducerData[Message,Message](config.topic, message))
              bytesSent += message.payloadSize
            }else {
              producer.send(new ProducerData[Message,Message](config.topic, message))
              bytesSent += message.payloadSize
            }
            nSends += 1
          }
        }catch {
          case e: Exception => error("Error sending messages", e)
        }
        if(nSends % config.reportingInterval == 0) {
          reportTime = System.currentTimeMillis()
          val elapsed = (reportTime - lastReportTime)/ 1000.0
          val mbBytesSent = ((bytesSent - lastBytesSent) * 1.0)/(1024 * 1024)
          val numMessagesPerSec = (nSends - lastNSends) / elapsed
          val mbPerSec = mbBytesSent / elapsed
          val formattedReportTime = config.dateFormat.format(reportTime)
          if(config.showDetailedStats)
            println(("%s, %d, %d, %d, %d, %.2f, %.4f, %d, %.4f").format(formattedReportTime, config.compressionCodec.codec,
              threadId, config.messageSize, config.batchSize, (bytesSent*1.0)/(1024 * 1024), mbPerSec, nSends, numMessagesPerSec))
          lastReportTime = reportTime
          lastBytesSent = bytesSent
          lastNSends = nSends
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
