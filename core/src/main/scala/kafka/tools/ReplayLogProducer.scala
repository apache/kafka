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
import java.util.concurrent.{Executors, CountDownLatch}
import java.util.Properties
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.consumer._
import kafka.utils.{Logging, ZkUtils}
import kafka.api.OffsetRequest
import kafka.message.CompressionCodec

object ReplayLogProducer extends Logging {

  private val GroupId: String = "replay-log-producer"

  def main(args: Array[String]) {
    val config = new Config(args)

    val executor = Executors.newFixedThreadPool(config.numThreads)
    val allDone = new CountDownLatch(config.numThreads)

    // if there is no group specified then avoid polluting zookeeper with persistent group data, this is a hack
    ZkUtils.maybeDeletePath(config.zkConnect, "/consumers/" + GroupId)
    Thread.sleep(500)

    // consumer properties
    val consumerProps = new Properties
    consumerProps.put("group.id", GroupId)
    consumerProps.put("zk.connect", config.zkConnect)
    consumerProps.put("consumer.timeout.ms", "10000")
    consumerProps.put("auto.offset.reset", OffsetRequest.SmallestTimeString)
    consumerProps.put("fetch.message.max.bytes", (1024*1024).toString)
    consumerProps.put("socket.receive.buffer.bytes", (2 * 1024 * 1024).toString)
    val consumerConfig = new ConsumerConfig(consumerProps)
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val topicMessageStreams = consumerConnector.createMessageStreams(Predef.Map(config.inputTopic -> config.numThreads))
    var threadList = List[ZKConsumerThread]()
    for ((topic, streamList) <- topicMessageStreams)
      for (stream <- streamList)
        threadList ::= new ZKConsumerThread(config, stream)

    for (thread <- threadList)
      thread.start

    threadList.foreach(_.shutdown)
    consumerConnector.shutdown
  }

  class Config(args: Array[String]) {
    val parser = new OptionParser
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("zookeeper url")
      .ofType(classOf[String])
      .defaultsTo("127.0.0.1:2181")
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: the broker list must be specified.")
      .withRequiredArg
      .describedAs("hostname:port")
      .ofType(classOf[String])
    val inputTopicOpt = parser.accepts("inputtopic", "REQUIRED: The topic to consume from.")
      .withRequiredArg
      .describedAs("input-topic")
      .ofType(classOf[String])
    val outputTopicOpt = parser.accepts("outputtopic", "REQUIRED: The topic to produce to")
      .withRequiredArg
      .describedAs("output-topic")
      .ofType(classOf[String])
    val numMessagesOpt = parser.accepts("messages", "The number of messages to send.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(-1)
    val asyncOpt = parser.accepts("async", "If set, messages are sent asynchronously.")
    val delayMSBtwBatchOpt = parser.accepts("delay-btw-batch-ms", "Delay in ms between 2 batch sends.")
      .withRequiredArg
      .describedAs("ms")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(0)
    val batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch.")
      .withRequiredArg
      .describedAs("batch size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(200)
    val numThreadsOpt = parser.accepts("threads", "Number of sending threads.")
      .withRequiredArg
      .describedAs("threads")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)
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
    for(arg <- List(brokerListOpt, inputTopicOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
    val zkConnect = options.valueOf(zkConnectOpt)
    val brokerList = options.valueOf(brokerListOpt)
    val numMessages = options.valueOf(numMessagesOpt).intValue
    val isAsync = options.has(asyncOpt)
    val delayedMSBtwSend = options.valueOf(delayMSBtwBatchOpt).longValue
    var batchSize = options.valueOf(batchSizeOpt).intValue
    val numThreads = options.valueOf(numThreadsOpt).intValue
    val inputTopic = options.valueOf(inputTopicOpt)
    val outputTopic = options.valueOf(outputTopicOpt)
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    val compressionCodec = CompressionCodec.getCompressionCodec(options.valueOf(compressionCodecOption).intValue)
  }

  class ZKConsumerThread(config: Config, stream: KafkaStream[Array[Byte], Array[Byte]]) extends Thread with Logging {
    val shutdownLatch = new CountDownLatch(1)
    val props = new Properties()
    props.put("broker.list", config.brokerList)
    props.put("reconnect.interval", Integer.MAX_VALUE.toString)
    props.put("send.buffer.bytes", (64*1024).toString)
    props.put("compression.codec", config.compressionCodec.codec.toString)
    props.put("batch.num.messages", config.batchSize.toString)
    props.put("queue.enqueue.timeout.ms", "-1")
    
    if(config.isAsync)
      props.put("producer.type", "async")

    val producerConfig = new ProducerConfig(props)
    val producer = new Producer[Array[Byte], Array[Byte]](producerConfig)

    override def run() {
      info("Starting consumer thread..")
      var messageCount: Int = 0
      try {
        val iter =
          if(config.numMessages >= 0)
            stream.slice(0, config.numMessages)
          else
            stream
        for (messageAndMetadata <- iter) {
          try {
            producer.send(new KeyedMessage[Array[Byte], Array[Byte]](config.outputTopic, messageAndMetadata.message))
            if (config.delayedMSBtwSend > 0 && (messageCount + 1) % config.batchSize == 0)
              Thread.sleep(config.delayedMSBtwSend)
            messageCount += 1
          }catch {
            case ie: Exception => error("Skipping this message", ie)
          }
        }
      }catch {
        case e: ConsumerTimeoutException => error("consumer thread timing out", e)
      }
      info("Sent " + messageCount + " messages")
      shutdownLatch.countDown
      info("thread finished execution !" )
    }

    def shutdown() {
      shutdownLatch.await
      producer.close
    }

  }
}
