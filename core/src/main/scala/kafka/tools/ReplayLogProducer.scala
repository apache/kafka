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
import java.util.concurrent.CountDownLatch
import java.util.Properties
import kafka.consumer._
import kafka.utils.{ToolsUtils, CommandLineUtils, Logging, ZkUtils}
import kafka.api.OffsetRequest
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import scala.collection.JavaConverters._

object ReplayLogProducer extends Logging {

  private val GroupId: String = "replay-log-producer"

  def main(args: Array[String]) {
    val config = new Config(args)

    // if there is no group specified then avoid polluting zookeeper with persistent group data, this is a hack
    ZkUtils.maybeDeletePath(config.zkConnect, "/consumers/" + GroupId)
    Thread.sleep(500)

    // consumer properties
    val consumerProps = new Properties
    consumerProps.put("group.id", GroupId)
    consumerProps.put("zookeeper.connect", config.zkConnect)
    consumerProps.put("consumer.timeout.ms", "10000")
    consumerProps.put("auto.offset.reset", OffsetRequest.SmallestTimeString)
    consumerProps.put("fetch.message.max.bytes", (1024*1024).toString)
    consumerProps.put("socket.receive.buffer.bytes", (2 * 1024 * 1024).toString)
    val consumerConfig = new ConsumerConfig(consumerProps)
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val topicMessageStreams = consumerConnector.createMessageStreams(Predef.Map(config.inputTopic -> config.numThreads))
    var threadList = List[ZKConsumerThread]()
    for (streamList <- topicMessageStreams.values)
      for (stream <- streamList)
        threadList ::= new ZKConsumerThread(config, stream)

    for (thread <- threadList)
      thread.start

    threadList.foreach(_.shutdown)
    consumerConnector.shutdown
  }

  class Config(args: Array[String]) {
    val parser = new OptionParser(false)
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
    val propertyOpt = parser.accepts("property", "A mechanism to pass properties in the form key=value to the producer. " +
      "This allows the user to override producer properties that are not exposed by the existing command line arguments")
      .withRequiredArg
      .describedAs("producer properties")
      .ofType(classOf[String])
    val syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.")

    val options = parser.parse(args : _*)
    
    CommandLineUtils.checkRequiredArgs(parser, options, brokerListOpt, inputTopicOpt)

    val zkConnect = options.valueOf(zkConnectOpt)
    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser,brokerList)
    val numMessages = options.valueOf(numMessagesOpt).intValue
    val numThreads = options.valueOf(numThreadsOpt).intValue
    val inputTopic = options.valueOf(inputTopicOpt)
    val outputTopic = options.valueOf(outputTopicOpt)
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    val isSync = options.has(syncOpt)
    val producerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(propertyOpt).asScala)
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
  }

  class ZKConsumerThread(config: Config, stream: KafkaStream[Array[Byte], Array[Byte]]) extends Thread with Logging {
    val shutdownLatch = new CountDownLatch(1)
    val producer = new KafkaProducer[Array[Byte],Array[Byte]](config.producerProps)

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
            val response = producer.send(new ProducerRecord[Array[Byte],Array[Byte]](config.outputTopic, null,
                                            messageAndMetadata.timestamp, messageAndMetadata.key(), messageAndMetadata.message()))
            if(config.isSync) {
              response.get()
            }
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
