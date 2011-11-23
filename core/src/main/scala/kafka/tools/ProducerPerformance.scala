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

import kafka.utils.{Utils, Logging}
import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.atomic.AtomicLong
import kafka.producer._
import async.DefaultEventHandler
import kafka.serializer.StringEncoder
import joptsimple.{OptionSet, OptionParser}
import java.util.{Random, Properties}
import kafka.message.{CompressionCodec, Message, ByteBufferMessageSet}

/**
 * Load test for the producer
 */
object ProducerPerformance extends Logging {

  def main(args: Array[String]) {

    val config = new PerfConfig(args)
    if(!config.isFixSize)
      info("WARN: Throughput will be slower due to changing message size per request")

    val totalBytesSent = new AtomicLong(0)
    val totalMessagesSent = new AtomicLong(0)
    val executor = Executors.newFixedThreadPool(config.numThreads)
    val allDone = new CountDownLatch(config.numThreads)
    val startMs = System.currentTimeMillis
    val rand = new java.util.Random

    for(i <- 0 until config.numThreads) {
      if(config.isAsync)
        executor.execute(new AsyncProducerThread(i, config, totalBytesSent, totalMessagesSent, allDone, rand))
      else
        executor.execute(new SyncProducerThread(i, config, totalBytesSent, totalMessagesSent, allDone, rand))
    }

    allDone.await()
    val elapsedSecs = (System.currentTimeMillis - startMs) / 1000.0
    info("Total Num Messages: " + totalMessagesSent.get + " bytes: " + totalBytesSent.get + " in " + elapsedSecs + " secs")
    info("Messages/sec: " + (1.0 * totalMessagesSent.get / elapsedSecs).formatted("%.4f"))
    info("MB/sec: " + (totalBytesSent.get / elapsedSecs / (1024.0*1024.0)).formatted("%.4f"))
    System.exit(0)
  }

  class PerfConfig(args: Array[String]) {
    val parser = new OptionParser
    val brokerInfoOpt = parser.accepts("brokerinfo", "REQUIRED: broker info (either from zookeeper or a list.")
      .withRequiredArg
      .describedAs("broker.list=brokerid:hostname:port or zk.connect=host:port")
      .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val numMessagesOpt = parser.accepts("messages", "REQUIRED: The number of messages to send.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
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
    val reportingIntervalOpt = parser.accepts("reporting-interval", "Interval at which to print progress info.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(5000)
    val compressionCodecOption = parser.accepts("compression-codec", "If set, messages are sent compressed")
      .withRequiredArg
      .describedAs("compression codec ")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(0)

    val options = parser.parse(args : _*)
    for(arg <- List(brokerInfoOpt, topicOpt, numMessagesOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
    val brokerInfo = options.valueOf(brokerInfoOpt)
    val numMessages = options.valueOf(numMessagesOpt).intValue
    val messageSize = options.valueOf(messageSizeOpt).intValue
    val isFixSize = !options.has(varyMessageSizeOpt)
    val isAsync = options.has(asyncOpt)
    var batchSize = options.valueOf(batchSizeOpt).intValue
    val numThreads = options.valueOf(numThreadsOpt).intValue
    val topic = options.valueOf(topicOpt)
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    val compressionCodec = CompressionCodec.getCompressionCodec(options.valueOf(compressionCodecOption).intValue)
  }

  private def getStringOfLength(len: Int) : String = {
    val strArray = new Array[Char](len)
    for (i <- 0 until len)
      strArray(i) = 'x'
    return new String(strArray)
  }

  class AsyncProducerThread(val threadId: Int,
                            val config: PerfConfig,
                            val totalBytesSent: AtomicLong,
                            val totalMessagesSent: AtomicLong,
                            val allDone: CountDownLatch,
                            val rand: Random) extends Runnable with Logging {
    val brokerInfoList = config.brokerInfo.split("=")
    val props = new Properties()
    if (brokerInfoList(0) == "zk.connect")
      props.put("zk.connect", brokerInfoList(1))
    else
      props.put("broker.list", brokerInfoList(1))

    props.put("compression.codec", config.compressionCodec.codec.toString)
    props.put("producer.type","async")
    props.put("batch.size", config.batchSize.toString)
    props.put("reconnect.interval", Integer.MAX_VALUE.toString)
    props.put("buffer.size", (64*1024).toString)
    props.put("queue.enqueueTimeout.ms", "-1")
    info("Producer properties = " + props.toString)

    val producerConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](producerConfig, new StringEncoder,
      new DefaultEventHandler[String](producerConfig, null), null, new DefaultPartitioner[String])

    override def run {
      var bytesSent = 0L
      var lastBytesSent = 0L
      var nSends = 0
      var lastNSends = 0
      var message = getStringOfLength(config.messageSize)
      var reportTime = System.currentTimeMillis()
      var lastReportTime = reportTime
      val messagesPerThread = config.numMessages / config.numThreads
      info("Messages per thread = " + messagesPerThread)
      for(j <- 0 until messagesPerThread) {
        var strLength = config.messageSize
        if (!config.isFixSize) {
            strLength = rand.nextInt(config.messageSize)
            message = getStringOfLength(strLength)
            bytesSent += strLength
        }else
          bytesSent += config.messageSize
        try  {
          producer.send(new ProducerData[String,String](config.topic, message))
          nSends += 1
        }catch {
          case e: Exception => e.printStackTrace
        }
        if(nSends % config.reportingInterval == 0) {
          reportTime = System.currentTimeMillis()
          info("thread " + threadId + ": " + nSends + " messages sent "
            + (1000.0 * (nSends - lastNSends) / (reportTime - lastReportTime)).formatted("%.4f") + " nMsg/sec "
            + (1000.0 * (bytesSent - lastBytesSent) / (reportTime - lastReportTime) / (1024 * 1024)).formatted("%.4f") + " MBs/sec")
          lastReportTime = reportTime
          lastBytesSent = bytesSent
          lastNSends = nSends
        }
      }
      producer.close()
      totalBytesSent.addAndGet(bytesSent)
      totalMessagesSent.addAndGet(nSends)
      allDone.countDown()
    }
  }

  class SyncProducerThread(val threadId: Int,
                           val config: PerfConfig,
                           val totalBytesSent: AtomicLong,
                           val totalMessagesSent: AtomicLong,
                           val allDone: CountDownLatch,
                           val rand: Random) extends Runnable with Logging {
    val props = new Properties()
    val brokerInfoList = config.brokerInfo.split("=")
    if (brokerInfoList(0) == "zk.connect")
      props.put("zk.connect", brokerInfoList(1))
    else
      props.put("broker.list", brokerInfoList(1))
    props.put("compression.codec", config.compressionCodec.codec.toString)
    props.put("reconnect.interval", Integer.MAX_VALUE.toString)
    props.put("buffer.size", (64*1024).toString)

    val producerConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](producerConfig, new StringEncoder,
      new DefaultEventHandler[String](producerConfig, null), null, new DefaultPartitioner[String])

    override def run {
      var bytesSent = 0L
      var lastBytesSent = 0L
      var nSends = 0
      var lastNSends = 0
      val message = getStringOfLength(config.messageSize)
      var reportTime = System.currentTimeMillis()
      var lastReportTime = reportTime
      val messagesPerThread = config.numMessages / config.numThreads / config.batchSize
      info("Messages per thread = " + messagesPerThread)
      var messageSet: List[String] = Nil
      for(k <- 0 until config.batchSize) {
        messageSet ::= message
      }
      for(j <- 0 until messagesPerThread) {
        var strLength = config.messageSize
        if (!config.isFixSize) {
          for(k <- 0 until config.batchSize) {
            strLength = rand.nextInt(config.messageSize)
            messageSet ::= getStringOfLength(strLength)
            bytesSent += strLength
          }
        }else
          bytesSent += config.batchSize*config.messageSize
        try  {
          producer.send(new ProducerData[String,String](config.topic, messageSet))
          nSends += 1
        }catch {
          case e: Exception => e.printStackTrace
        }
        if(nSends % config.reportingInterval == 0) {
          reportTime = System.currentTimeMillis()
          info("thread " + threadId + ": " + nSends + " messages sent "
            + (1000.0 * (nSends - lastNSends) * config.batchSize / (reportTime - lastReportTime)).formatted("%.4f") + " nMsg/sec "
            + (1000.0 * (bytesSent - lastBytesSent) / (reportTime - lastReportTime) / (1024 * 1024)).formatted("%.4f") + " MBs/sec")
          lastReportTime = reportTime
          lastBytesSent = bytesSent
          lastNSends = nSends
        }
      }
      producer.close()
      totalBytesSent.addAndGet(bytesSent)
      totalMessagesSent.addAndGet(nSends*config.batchSize)
      allDone.countDown()
    }
  }
}
