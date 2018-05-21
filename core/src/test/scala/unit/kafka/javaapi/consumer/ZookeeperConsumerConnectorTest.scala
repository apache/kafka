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

import kafka.serializer._
import kafka.server._
import kafka.integration.KafkaServerTestHarness
import kafka.utils.{Logging, TestUtils}
import kafka.consumer.{ConsumerConfig, KafkaStream}
import kafka.common.MessageStreamsExistException
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}
import org.junit.Test

import scala.collection.JavaConverters._

import org.apache.log4j.{Level, Logger}
import org.junit.Assert._

@deprecated("This test has been deprecated and it will be removed in a future release", "0.10.0.0")
class ZookeeperConsumerConnectorTest extends KafkaServerTestHarness with Logging {
  val numNodes = 2
  val numParts = 2
  val topic = "topic1"

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)

  def generateConfigs =
    TestUtils.createBrokerConfigs(numNodes, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))

  val group = "group1"
  val consumer1 = "consumer1"
  val nMessages = 2

  @Test
  def testBasic() {
    val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])
    requestHandlerLogger.setLevel(Level.FATAL)

    // create the topic
    createTopic(topic, numParts, 1)

    // send some messages to each broker
    val sentMessages1 = sendMessages(servers, nMessages, "batch1")

    // create a consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map[String, Integer](topic -> numNodes*numParts/2).asJava, new StringDecoder(), new StringDecoder())

    val receivedMessages1 = getMessages(nMessages*2, topicMessageStreams1)
    assertEquals(sentMessages1.sorted, receivedMessages1.sorted)

    // call createMesssageStreams twice should throw MessageStreamsExistException
    try {
      zkConsumerConnector1.createMessageStreams(Map[String, Integer](topic -> numNodes*numParts/2).asJava, new StringDecoder(), new StringDecoder())
      fail("Should fail with MessageStreamsExistException")
    } catch {
      case _: MessageStreamsExistException => // expected
    }
    zkConsumerConnector1.shutdown
    info("all consumer connectors stopped")
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def sendMessages(servers: Seq[KafkaServer],
                   messagesPerNode: Int,
                   header: String): List[String] = {
    var messages: List[String] = Nil
    val producer = TestUtils.createNewProducer[Integer, String](TestUtils.getBrokerListStrFromServers(servers),
      keySerializer = new IntegerSerializer, valueSerializer = new StringSerializer)
    for (server <- servers) {
      for (partition <- 0 until numParts) {
        val ms = (0 until messagesPerNode).map(x => header + server.config.brokerId + "-" + partition + "-" + x)
        messages ++= ms
        ms.map(new ProducerRecord[Integer, String](topic, partition, partition, _)).map(producer.send).foreach(_.get)
      }
    }
    producer.close()
    messages
  }

  def getMessages(nMessagesPerThread: Int,
                  jTopicMessageStreams: java.util.Map[String, java.util.List[KafkaStream[String, String]]]): List[String] = {
    val topicMessageStreams = jTopicMessageStreams.asScala.mapValues(_.asScala.toList)
    TestUtils.getMessages(topicMessageStreams, nMessagesPerThread)
  }
}
