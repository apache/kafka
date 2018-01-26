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
    var config: Config = null
    config = new Config(args)

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
    val zkConnectOpt = parser.accepts("zookeeper", "The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("url(s) for the zookeeper connection")
      .ofType(classOf[String])
      .defaultsTo("localhost:2181")
    val brokerListOpt = parser.accepts("broker-list", "The broker list must be specified.")
      .withRequiredArg
      .describedAs("server(s) to use for bootstrapping(e.g: hostname:port)")
      .ofType(classOf[String])
      .required
    val inputTopicOpt = parser.accepts("inputtopic", "The topic to consume from.")
      .withRequiredArg
      .describedAs("topic to consume from")
      .ofType(classOf[String])
      .required
    val outputTopicOpt = parser.accepts("outputtopic", "The topic to produce to.")
      .withRequiredArg
      .describedAs("topic to produce to")
      .ofType(classOf[String])
      .required
    val numMessagesOpt = parser.accepts("messages", "The number of messages to send.")
      .withRequiredArg
      .describedAs("number of messages to send")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(-1)
    val numThreadsOpt = parser.accepts("threads", "Number of sending threads.")
      .withRequiredArg
      .describedAs("number of sending threads")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)
    val reportingIntervalOpt = parser.accepts("reporting-interval", "Interval at which to print progress info.")
      .withRequiredArg
      .describedAs("reporting interval(in ms) to print progress info")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(5000)
    val propertyOpt = parser.accepts("property", "A mechanism to pass properties in the form key=value to the producer. " +
      "This allows the user to override producer properties that are not exposed by the existing command line arguments.")
      .withRequiredArg
      .describedAs("user defined properties to pass to producer")
      .ofType(classOf[String])
    val syncOpt = parser.accepts("sync", "If set, message requests to the brokers are send synchronously, one at a time as they arrive.")
    val helpOpt = parser.accepts("help", "Print usage information.").forHelp
  
    var commandDef: String = "Consume from one topic and replay those messages and produce to another topic."
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, commandDef)

    val options = CommandLineUtils.tryParse(parser, args)

    if (options.has("help"))
      CommandLineUtils.printUsageAndDie(parser, commandDef)
      
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
