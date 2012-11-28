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
import kafka.utils.Logging

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
    val brokerInfoOpt = parser.accepts("brokerinfo", "REQUIRED: broker info (either from zookeeper or a list.")
      .withRequiredArg
      .describedAs("broker.list=brokerid:hostname:port or zk.connect=host:port")
      .ofType(classOf[String])
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
      .defaultsTo(10)
    val compressionCodecOption = parser.accepts("compression-codec", "If set, messages are sent compressed")
      .withRequiredArg
      .describedAs("compression codec ")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(0)

    val options = parser.parse(args : _*)
    for(arg <- List(topicOpt, brokerInfoOpt, numMessagesOpt)) {
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
    val brokerInfo = options.valueOf(brokerInfoOpt)
    val messageSize = options.valueOf(messageSizeOpt).intValue
    val isFixSize = !options.has(varyMessageSizeOpt)
    val isAsync = options.has(asyncOpt)
    var batchSize = options.valueOf(batchSizeOpt).intValue
    val numThreads = options.valueOf(numThreadsOpt).intValue
    val compressionCodec = CompressionCodec.getCompressionCodec(options.valueOf(compressionCodecOption).intValue)
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
    val brokerInfoList = config.brokerInfo.split("=")
    if (brokerInfoList(0) == "zk.connect") {
      props.put("zk.connect", brokerInfoList(1))
      props.put("zk.sessiontimeout.ms", "300000")
    }
    else
      props.put("broker.list", brokerInfoList(1))
    props.put("compression.codec", config.compressionCodec.codec.toString)
    props.put("reconnect.interval", Integer.MAX_VALUE.toString)
    props.put("buffer.size", (64*1024).toString)
    if(config.isAsync) {
      props.put("producer.type","async")
      props.put("batch.size", config.batchSize.toString)
      props.put("queue.enqueueTimeout.ms", "-1")
    }
    val producerConfig = new ProducerConfig(props)
    val producer = new Producer[Message, Message](producerConfig)

    override def run {
      var bytesSent = 0L
      var lastBytesSent = 0L
      var nSends = 0
      var lastNSends = 0
      val message = new Message(new Array[Byte](config.messageSize))
      var reportTime = System.currentTimeMillis()
      var lastReportTime = reportTime
      val messagesPerThread = if(!config.isAsync) config.numMessages / config.numThreads / config.batchSize
                              else config.numMessages / config.numThreads
      debug("Messages per thread = " + messagesPerThread)
      var messageSet: List[Message] = Nil
      if(config.isFixSize) {
        for(k <- 0 until config.batchSize) {
          messageSet ::= message
        }
      }
      var j: Long = 0L
      while(j < messagesPerThread) {
        var strLength = config.messageSize
        if (!config.isFixSize) {
          for(k <- 0 until config.batchSize) {
            strLength = rand.nextInt(config.messageSize)
            val message = new Message(getByteArrayOfLength(strLength))
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
              debug(config.topic + "-checksum:" + message.checksum)
              bytesSent += message.payloadSize
            }else {
              producer.send(new ProducerData[Message,Message](config.topic, message))
              debug(config.topic + "-checksum:" + message.checksum)
              bytesSent += message.payloadSize
            }
            nSends += 1
          }
        }catch {
          case e: Exception => e.printStackTrace
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
