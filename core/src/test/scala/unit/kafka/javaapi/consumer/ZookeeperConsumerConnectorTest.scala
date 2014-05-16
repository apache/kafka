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

package kafka.javaapi.consumer

import kafka.server._
import kafka.message._
import kafka.serializer._
import kafka.integration.KafkaServerTestHarness
import kafka.producer.KeyedMessage
import kafka.javaapi.producer.Producer
import kafka.utils.IntEncoder
import kafka.utils.{Logging, TestUtils}
import kafka.consumer.{KafkaStream, ConsumerConfig}
import kafka.zk.ZooKeeperTestHarness
import kafka.common.MessageStreamsExistException

import scala.collection.JavaConversions

import org.scalatest.junit.JUnit3Suite
import org.apache.log4j.{Level, Logger}
import junit.framework.Assert._


class ZookeeperConsumerConnectorTest extends JUnit3Suite with KafkaServerTestHarness with ZooKeeperTestHarness with Logging {

  val zookeeperConnect = zkConnect
  val numNodes = 2
  val numParts = 2
  val topic = "topic1"
  val configs =
    for(props <- TestUtils.createBrokerConfigs(numNodes))
    yield new KafkaConfig(props) {
      override val numPartitions = numParts
      override val zkConnect = zookeeperConnect
    }
  val group = "group1"
  val consumer1 = "consumer1"
  val nMessages = 2

  def testBasic() {
    val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])
    requestHandlerLogger.setLevel(Level.FATAL)

    // create the topic
    TestUtils.createTopic(zkClient, topic, numParts, 1, servers)

    // send some messages to each broker
    val sentMessages1 = sendMessages(nMessages, "batch1")

    // create a consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zookeeperConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(toJavaMap(Map(topic -> numNodes*numParts/2)), new StringDecoder(), new StringDecoder())

    val receivedMessages1 = getMessages(nMessages*2, topicMessageStreams1)
    assertEquals(sentMessages1.sorted, receivedMessages1.sorted)

    // call createMesssageStreams twice should throw MessageStreamsExistException
    try {
      val topicMessageStreams2 = zkConsumerConnector1.createMessageStreams(toJavaMap(Map(topic -> numNodes*numParts/2)), new StringDecoder(), new StringDecoder())
      fail("Should fail with MessageStreamsExistException")
    } catch {
      case e: MessageStreamsExistException => // expected
    }
    zkConsumerConnector1.shutdown
    info("all consumer connectors stopped")
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def sendMessages(conf: KafkaConfig,
                   messagesPerNode: Int,
                   header: String,
                   compressed: CompressionCodec): List[String] = {
    var messages: List[String] = Nil
    val producer: kafka.producer.Producer[Int, String] =
      TestUtils.createProducer(TestUtils.getBrokerListStrFromConfigs(configs),
        encoder = classOf[StringEncoder].getName,
        keyEncoder = classOf[IntEncoder].getName)
    val javaProducer: Producer[Int, String] = new kafka.javaapi.producer.Producer(producer)
    for (partition <- 0 until numParts) {
      val ms = 0.until(messagesPerNode).map(x => header + conf.brokerId + "-" + partition + "-" + x)
      messages ++= ms
      import JavaConversions._
      javaProducer.send(ms.map(new KeyedMessage[Int, String](topic, partition, _)): java.util.List[KeyedMessage[Int, String]])
    }
    javaProducer.close
    messages
  }

  def sendMessages(messagesPerNode: Int,
                   header: String,
                   compressed: CompressionCodec = NoCompressionCodec): List[String] = {
    var messages: List[String] = Nil
    for(conf <- configs)
      messages ++= sendMessages(conf, messagesPerNode, header, compressed)
    messages
  }

  def getMessages(nMessagesPerThread: Int,
                  jTopicMessageStreams: java.util.Map[String, java.util.List[KafkaStream[String, String]]]): List[String] = {
    var messages: List[String] = Nil
    import scala.collection.JavaConversions._
    val topicMessageStreams: collection.mutable.Map[String, java.util.List[KafkaStream[String, String]]] = jTopicMessageStreams
    for ((topic, messageStreams) <- topicMessageStreams) {
      for (messageStream <- messageStreams) {
        val iterator = messageStream.iterator
        for (i <- 0 until nMessagesPerThread) {
          assertTrue(iterator.hasNext)
          val message = iterator.next.message
          messages ::= message
          debug("received message: " + message)
        }
      }
    }
    messages
  }

  private def toJavaMap(scalaMap: Map[String, Int]): java.util.Map[String, java.lang.Integer] = {
    val javaMap = new java.util.HashMap[String, java.lang.Integer]()
    scalaMap.foreach(m => javaMap.put(m._1, m._2.asInstanceOf[java.lang.Integer]))
    javaMap
  }
}
