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

import java.util.Properties

import kafka.server._
import kafka.serializer._
import kafka.integration.KafkaServerTestHarness
import kafka.producer.KeyedMessage
import kafka.javaapi.producer.Producer
import kafka.utils.IntEncoder
import kafka.utils.{Logging, TestUtils}
import kafka.consumer.{KafkaStream, ConsumerConfig}
import kafka.zk.ZooKeeperTestHarness
import kafka.common.MessageStreamsExistException
import org.junit.Test

import scala.collection.JavaConversions

import org.apache.log4j.{Level, Logger}
import org.junit.Assert._

@deprecated("This test has been deprecated and it will be removed in a future release", "0.10.0.0")
class ZookeeperConsumerConnectorTest extends KafkaServerTestHarness with Logging {
  val numNodes = 2
  val numParts = 2
  val topic = "topic1"

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)

  def generateConfigs() = TestUtils.createBrokerConfigs(numNodes, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))

  val group = "group1"
  val consumer1 = "consumer1"
  val nMessages = 2

  @Test
  def testBasic() {
    val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])
    requestHandlerLogger.setLevel(Level.FATAL)

    // create the topic
    TestUtils.createTopic(zkUtils, topic, numParts, 1, servers)

    // send some messages to each broker
    val sentMessages1 = sendMessages(servers, nMessages, "batch1")

    // create a consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer1))
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

  def sendMessages(servers: Seq[KafkaServer],
                   messagesPerNode: Int,
                   header: String): List[String] = {
    var messages: List[String] = Nil
    for(server <- servers) {
      val producer: kafka.producer.Producer[Int, String] =
        TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers),
          encoder = classOf[StringEncoder].getName,
          keyEncoder = classOf[IntEncoder].getName)
      val javaProducer: Producer[Int, String] = new kafka.javaapi.producer.Producer(producer)
      for (partition <- 0 until numParts) {
        val ms = 0.until(messagesPerNode).map(x => header + server.config.brokerId + "-" + partition + "-" + x)
        messages ++= ms
        import JavaConversions._
        javaProducer.send(ms.map(new KeyedMessage[Int, String](topic, partition, _)): java.util.List[KeyedMessage[Int, String]])
      }
      javaProducer.close
    }
    messages
  }

  def getMessages(nMessagesPerThread: Int,
                  jTopicMessageStreams: java.util.Map[String, java.util.List[KafkaStream[String, String]]]): List[String] = {
    var messages: List[String] = Nil
    import scala.collection.JavaConversions._
    val topicMessageStreams = jTopicMessageStreams.mapValues(_.toList)
    messages = TestUtils.getMessages(topicMessageStreams, nMessagesPerThread)
    messages
  }

  private def toJavaMap(scalaMap: Map[String, Int]): java.util.Map[String, java.lang.Integer] = {
    val javaMap = new java.util.HashMap[String, java.lang.Integer]()
    scalaMap.foreach(m => javaMap.put(m._1, m._2.asInstanceOf[java.lang.Integer]))
    javaMap
  }
}
