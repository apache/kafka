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
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import org.scalatest.junit.JUnit3Suite
import scala.collection.JavaConversions._
import org.apache.log4j.{Level, Logger}
import kafka.message._
import kafka.javaapi.producer.{ProducerData, Producer}
import kafka.utils.TestUtils._
import kafka.utils.{Utils, Logging, TestUtils}
import kafka.consumer.{KafkaStream, ConsumerConfig}
import kafka.zk.ZooKeeperTestHarness

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
    var actualMessages: List[Message] = Nil

    // send some messages to each broker
    val sentMessages1 = sendMessages(nMessages, "batch1")

    waitUntilLeaderIsElected(zkClient, topic, 0, 500)
    waitUntilLeaderIsElected(zkClient, topic, 1, 500)

    // create a consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zookeeperConnect, group, consumer1))
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
    val producer: kafka.producer.Producer[Int, Message] = TestUtils.createProducer(zkConnect)
    val javaProducer: Producer[Int, Message] = new kafka.javaapi.producer.Producer(producer)
    for (partition <- 0 until numParts) {
      val ms = 0.until(messagesPerNode).map(x =>
        new Message((header + conf.brokerId + "-" + partition + "-" + x).getBytes)).toArray
      for (message <- ms)
        messages ::= message
      import scala.collection.JavaConversions._
      javaProducer.send(new ProducerData[Int, Message](topic, partition, asList(ms)))
    }
    javaProducer.close
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

  private def toJavaMap(scalaMap: Map[String, Int]): java.util.Map[String, java.lang.Integer] = {
    val javaMap = new java.util.HashMap[String, java.lang.Integer]()
    scalaMap.foreach(m => javaMap.put(m._1, m._2.asInstanceOf[java.lang.Integer]))
    javaMap
  }  
}
