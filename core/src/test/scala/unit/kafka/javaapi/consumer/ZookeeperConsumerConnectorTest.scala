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

import junit.framework.Assert._
import kafka.zk.ZooKeeperTestHarness
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{Utils, Logging}
import kafka.utils.{TestZKUtils, TestUtils}
import org.scalatest.junit.JUnit3Suite
import scala.collection.JavaConversions._
import kafka.javaapi.message.ByteBufferMessageSet
import org.apache.log4j.{Level, Logger}
import kafka.message.{NoCompressionCodec, CompressionCodec, Message}
import kafka.consumer.{KafkaStream, ConsumerConfig}


class ZookeeperConsumerConnectorTest extends JUnit3Suite with KafkaServerTestHarness with ZooKeeperTestHarness with Logging {

  val zookeeperConnect = TestZKUtils.zookeeperConnect
  val zkConnect = zookeeperConnect
  val numNodes = 2
  val numParts = 2
  val topic = "topic1"
  val configs =
    for(props <- TestUtils.createBrokerConfigs(numNodes))
    yield new KafkaConfig(props) {
      override val enableZookeeper = true
      override val numPartitions = numParts
      override val zkConnect = zookeeperConnect
    }
  val group = "group1"
  val consumer1 = "consumer1"
  val nMessages = 2

  def testBasic() {
    val requestHandlerLogger = Logger.getLogger(classOf[kafka.server.KafkaRequestHandlers])
    requestHandlerLogger.setLevel(Level.FATAL)
    var actualMessages: List[Message] = Nil

    // send some messages to each broker
    val sentMessages1 = sendMessages(nMessages, "batch1")
    // create a consumer
    val consumerConfig1 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(toJavaMap(Predef.Map(topic -> numNodes*numParts/2)))
    val receivedMessages1 = getMessages(nMessages*2, topicMessageStreams1)
    assertEquals(sentMessages1, receivedMessages1)

    zkConsumerConnector1.shutdown
    info("all consumer connectors stopped")
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def sendMessages(conf: KafkaConfig, messagesPerNode: Int, header: String, compressed: CompressionCodec): List[Message]= {
    var messages: List[Message] = Nil
    val producer = kafka.javaapi.Implicits.toJavaSyncProducer(TestUtils.createProducer("localhost", conf.port))
    for (partition <- 0 until numParts) {
      val ms = 0.until(messagesPerNode).map(x =>
        new Message((header + conf.brokerId + "-" + partition + "-" + x).getBytes)).toArray
      val mSet = new ByteBufferMessageSet(compressionCodec = compressed, messages = getMessageList(ms: _*))
      for (message <- ms)
        messages ::= message
      producer.send(topic, partition, mSet)
    }
    producer.close()
    messages
  }

  def sendMessages(messagesPerNode: Int, header: String, compressed: CompressionCodec = NoCompressionCodec): List[Message]= {
    var messages: List[Message] = Nil
    for(conf <- configs) {
      messages ++= sendMessages(conf, messagesPerNode, header, compressed)
    }
    messages.sortWith((s,t) => s.checksum < t.checksum)
  }

  def getMessages(nMessagesPerThread: Int, jTopicMessageStreams: java.util.Map[String, java.util.List[KafkaStream[Message]]])
  : List[Message]= {
    var messages: List[Message] = Nil
    val topicMessageStreams = asMap(jTopicMessageStreams)
    for ((topic, messageStreams) <- topicMessageStreams) {
      for (messageStream <- messageStreams) {
        val iterator = messageStream.iterator
        for (i <- 0 until nMessagesPerThread) {
          assertTrue(iterator.hasNext)
          val message = iterator.next.message
          messages ::= message
          debug("received message: " + Utils.toString(message.payload, "UTF-8"))
        }
      }
    }
    messages.sortWith((s,t) => s.checksum < t.checksum)
  }

  private def getMessageList(messages: Message*): java.util.List[Message] = {
    val messageList = new java.util.ArrayList[Message]()
    messages.foreach(m => messageList.add(m))
    messageList
  }

  private def toJavaMap(scalaMap: Map[String, Int]): java.util.Map[String, java.lang.Integer] = {
    val javaMap = new java.util.HashMap[String, java.lang.Integer]()
    scalaMap.foreach(m => javaMap.put(m._1, m._2.asInstanceOf[java.lang.Integer]))
    javaMap
  }  
}
