
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

package kafka.consumer

import junit.framework.Assert._
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import scala.collection._
import org.scalatest.junit.JUnit3Suite
import org.apache.log4j.{Level, Logger}
import kafka.message._
import kafka.serializer.StringDecoder
import kafka.admin.CreateTopicCommand
import org.I0Itec.zkclient.ZkClient
import kafka.utils._
import kafka.producer.{ProducerConfig, ProducerData, Producer}
import java.util.{Collections, Properties}
import kafka.utils.TestUtils._

class ZookeeperConsumerConnectorTest extends JUnit3Suite with KafkaServerTestHarness with Logging {

  val RebalanceBackoffMs = 5000
  var dirs : ZKGroupTopicDirs = null
  val zookeeperConnect = TestZKUtils.zookeeperConnect
  val numNodes = 2
  val numParts = 2
  val topic = "topic1"
  val configs =
    for(props <- TestUtils.createBrokerConfigs(numNodes))
    yield new KafkaConfig(props) {
      override val zkConnect = zookeeperConnect
      override val numPartitions = numParts
    }
  val group = "group1"
  val consumer0 = "consumer0"
  val consumer1 = "consumer1"
  val consumer2 = "consumer2"
  val consumer3 = "consumer3"
  val nMessages = 2

  override def setUp() {
    super.setUp()
    dirs = new ZKGroupTopicDirs(group, topic)
  }

  override def tearDown() {
    super.tearDown()
  }

  def testBasic() {
    val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])
    requestHandlerLogger.setLevel(Level.FATAL)

    // test consumer timeout logic
    val consumerConfig0 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer0)) {
      override val consumerTimeoutMs = 200
    }
    val zkConsumerConnector0 = new ZookeeperConsumerConnector(consumerConfig0, true)
    val topicMessageStreams0 = zkConsumerConnector0.createMessageStreams(Predef.Map(topic -> 1))

    // no messages to consume, we should hit timeout;
    // also the iterator should support re-entrant, so loop it twice
    for (i <- 0 until 2) {
      try {
        getMessages(nMessages*2, topicMessageStreams0)
        fail("should get an exception")
      } catch {
        case e: ConsumerTimeoutException => // this is ok
        case e => throw e
      }
    }

    zkConsumerConnector0.shutdown

    // send some messages to each broker
    val sentMessages1_1 = sendMessagesToBrokerPartition(configs.head, topic, 0, nMessages)
    val sentMessages1_2 = sendMessagesToBrokerPartition(configs.last, topic, 1, nMessages)
    val sentMessages1 = (sentMessages1_1 ++ sentMessages1_2).sortWith((s,t) => s.checksum < t.checksum)

    // wait to make sure the topic and partition have a leader for the successful case
    waitUntilLeaderIsElected(zkClient, topic, 0, 500)
    waitUntilLeaderIsElected(zkClient, topic, 1, 500)

    // create a consumer
    val consumerConfig1 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Predef.Map(topic -> 1))

    val receivedMessages1 = getMessages(nMessages*2, topicMessageStreams1)
    assertEquals(sentMessages1.size, receivedMessages1.size)
    assertEquals(sentMessages1, receivedMessages1)

    // also check partition ownership
    val actual_1 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_1 = List( ("0", "group1_consumer1-0"),
                           ("1", "group1_consumer1-0"))
    assertEquals(expected_1, actual_1)

    // commit consumed offsets
    zkConsumerConnector1.commitOffsets

    // create a consumer
    val consumerConfig2 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer2)) {
      override val rebalanceBackoffMs = RebalanceBackoffMs
    }
    val zkConsumerConnector2 = new ZookeeperConsumerConnector(consumerConfig2, true)
    val topicMessageStreams2 = zkConsumerConnector2.createMessageStreams(Predef.Map(topic -> 1))
    // send some messages to each broker
    val sentMessages2_1 = sendMessagesToBrokerPartition(configs.head, topic, 0, nMessages)
    val sentMessages2_2 = sendMessagesToBrokerPartition(configs.last, topic, 1, nMessages)
    val sentMessages2 = (sentMessages2_1 ++ sentMessages2_2).sortWith((s,t) => s.checksum < t.checksum)

    waitUntilLeaderIsElected(zkClient, topic, 0, 500)
    waitUntilLeaderIsElected(zkClient, topic, 1, 500)

    val receivedMessages2_1 = getMessages(nMessages, topicMessageStreams1)
    val receivedMessages2_2 = getMessages(nMessages, topicMessageStreams2)
    val receivedMessages2 = (receivedMessages2_1 ::: receivedMessages2_2).sortWith((s,t) => s.checksum < t.checksum)
    assertEquals(sentMessages2, receivedMessages2)

    // also check partition ownership
    val actual_2 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_2 = List( ("0", "group1_consumer1-0"),
                           ("1", "group1_consumer2-0"))
    assertEquals(expected_2, actual_2)

    // create a consumer with empty map
    val consumerConfig3 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer3))
    val zkConsumerConnector3 = new ZookeeperConsumerConnector(consumerConfig3, true)
    val topicMessageStreams3 = zkConsumerConnector3.createMessageStreams(new mutable.HashMap[String, Int]())
    // send some messages to each broker
    Thread.sleep(200)
    val sentMessages3_1 = sendMessagesToBrokerPartition(configs.head, topic, 0, nMessages)
    val sentMessages3_2 = sendMessagesToBrokerPartition(configs.last, topic, 1, nMessages)
    val sentMessages3 = (sentMessages3_1 ++ sentMessages3_2).sortWith((s,t) => s.checksum < t.checksum)

    waitUntilLeaderIsElected(zkClient, topic, 0, 500)
    waitUntilLeaderIsElected(zkClient, topic, 1, 500)

    val receivedMessages3_1 = getMessages(nMessages, topicMessageStreams1)
    val receivedMessages3_2 = getMessages(nMessages, topicMessageStreams2)
    val receivedMessages3 = (receivedMessages3_1 ::: receivedMessages3_2).sortWith((s,t) => s.checksum < t.checksum)
    assertEquals(sentMessages3.size, receivedMessages3.size)
    assertEquals(sentMessages3, receivedMessages3)

    // also check partition ownership
    val actual_3 = getZKChildrenValues(dirs.consumerOwnerDir)
    assertEquals(expected_2, actual_3)

    zkConsumerConnector1.shutdown
    zkConsumerConnector2.shutdown
    zkConsumerConnector3.shutdown
    info("all consumer connectors stopped")
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testCompression() {
    val requestHandlerLogger = Logger.getLogger(classOf[kafka.server.KafkaRequestHandler])
    requestHandlerLogger.setLevel(Level.FATAL)

    // send some messages to each broker
    val sentMessages1_1 = sendMessagesToBrokerPartition(configs.head, topic, 0, nMessages, GZIPCompressionCodec)
    val sentMessages1_2 = sendMessagesToBrokerPartition(configs.last, topic, 1, nMessages, GZIPCompressionCodec)
    val sentMessages1 = (sentMessages1_1 ++ sentMessages1_2).sortWith((s,t) => s.checksum < t.checksum)

    waitUntilLeaderIsElected(zkClient, topic, 0, 500)
    waitUntilLeaderIsElected(zkClient, topic, 1, 500)

    // create a consumer
    val consumerConfig1 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Predef.Map(topic -> 1))
    val receivedMessages1 = getMessages(nMessages*2, topicMessageStreams1)
    assertEquals(sentMessages1.size, receivedMessages1.size)
    assertEquals(sentMessages1, receivedMessages1)

    // also check partition ownership
    val actual_1 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_1 = List( ("0", "group1_consumer1-0"),
                           ("1", "group1_consumer1-0"))
    assertEquals(expected_1, actual_1)

    // commit consumed offsets
    zkConsumerConnector1.commitOffsets

    // create a consumer
    val consumerConfig2 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer2)) {
      override val rebalanceBackoffMs = RebalanceBackoffMs
    }
    val zkConsumerConnector2 = new ZookeeperConsumerConnector(consumerConfig2, true)
    val topicMessageStreams2 = zkConsumerConnector2.createMessageStreams(Predef.Map(topic -> 1))
    // send some messages to each broker
    val sentMessages2_1 = sendMessagesToBrokerPartition(configs.head, topic, 0, nMessages, GZIPCompressionCodec)
    val sentMessages2_2 = sendMessagesToBrokerPartition(configs.last, topic, 1, nMessages, GZIPCompressionCodec)
    val sentMessages2 = (sentMessages2_1 ++ sentMessages2_2).sortWith((s,t) => s.checksum < t.checksum)

    waitUntilLeaderIsElected(zkClient, topic, 0, 500)
    waitUntilLeaderIsElected(zkClient, topic, 1, 500)

    val receivedMessages2_1 = getMessages(nMessages, topicMessageStreams1)
    val receivedMessages2_2 = getMessages(nMessages, topicMessageStreams2)
    val receivedMessages2 = (receivedMessages2_1 ::: receivedMessages2_2).sortWith((s,t) => s.checksum < t.checksum)
    assertEquals(sentMessages2, receivedMessages2)

    // also check partition ownership
    val actual_2 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_2 = List( ("0", "group1_consumer1-0"),
                           ("1", "group1_consumer2-0"))
    assertEquals(expected_2, actual_2)

    // create a consumer with empty map
    val consumerConfig3 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer3))
    val zkConsumerConnector3 = new ZookeeperConsumerConnector(consumerConfig3, true)
    val topicMessageStreams3 = zkConsumerConnector3.createMessageStreams(new mutable.HashMap[String, Int]())
    // send some messages to each broker
    Thread.sleep(200)
    val sentMessages3_1 = sendMessagesToBrokerPartition(configs.head, topic, 0, nMessages, GZIPCompressionCodec)
    val sentMessages3_2 = sendMessagesToBrokerPartition(configs.last, topic, 1, nMessages, GZIPCompressionCodec)
    val sentMessages3 = (sentMessages3_1 ++ sentMessages3_2).sortWith((s,t) => s.checksum < t.checksum)

    waitUntilLeaderIsElected(zkClient, topic, 0, 500)
    waitUntilLeaderIsElected(zkClient, topic, 1, 500)

    val receivedMessages3_1 = getMessages(nMessages, topicMessageStreams1)
    val receivedMessages3_2 = getMessages(nMessages, topicMessageStreams2)
    val receivedMessages3 = (receivedMessages3_1 ::: receivedMessages3_2).sortWith((s,t) => s.checksum < t.checksum)
    assertEquals(sentMessages3.size, receivedMessages3.size)
    assertEquals(sentMessages3, receivedMessages3)

    // also check partition ownership
    val actual_3 = getZKChildrenValues(dirs.consumerOwnerDir)
    assertEquals(expected_2, actual_3)

    zkConsumerConnector1.shutdown
    zkConsumerConnector2.shutdown
    zkConsumerConnector3.shutdown
    info("all consumer connectors stopped")
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testCompressionSetConsumption() {
    val requestHandlerLogger = Logger.getLogger(classOf[kafka.server.KafkaRequestHandler])
    requestHandlerLogger.setLevel(Level.FATAL)

    // shutdown one server
    servers.last.shutdown
    Thread.sleep(500)

    // send some messages to each broker
    val sentMessages1 = sendMessagesToBrokerPartition(configs.head, topic, 0, 200, DefaultCompressionCodec)
    val sentMessages2 = sendMessagesToBrokerPartition(configs.last, topic, 1, 200, DefaultCompressionCodec)
    val sentMessages = (sentMessages1 ++ sentMessages2).sortWith((s,t) => s.checksum < t.checksum)

    // test consumer timeout logic
    val consumerConfig0 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer0)) {
      override val consumerTimeoutMs = 5000
    }
    val zkConsumerConnector0 = new ZookeeperConsumerConnector(consumerConfig0, true)
    val topicMessageStreams0 = zkConsumerConnector0.createMessageStreams(Predef.Map(topic -> 1))
    getMessages(100, topicMessageStreams0)

    // also check partition ownership
    val actual_1 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_1 = List( ("0", "group1_consumer0-0"),
                           ("1", "group1_consumer0-0"))
    assertEquals(expected_1, actual_1)

    zkConsumerConnector0.shutdown
    // at this point, only some part of the message set was consumed. So consumed offset should still be 0
    // also fetched offset should be 0
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig0, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Predef.Map(topic -> 1))
    val receivedMessages = getMessages(400, topicMessageStreams1)
    val sortedReceivedMessages = receivedMessages.sortWith((s,t) => s.checksum < t.checksum)
    val sortedSentMessages = sentMessages.sortWith((s,t) => s.checksum < t.checksum)
    assertEquals(sortedSentMessages, sortedReceivedMessages)

    // also check partition ownership
    val actual_2 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_2 = List( ("0", "group1_consumer0-0"),
                           ("1", "group1_consumer0-0"))
    assertEquals(expected_2, actual_2)

    zkConsumerConnector1.shutdown

    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testConsumerDecoder() {
    val requestHandlerLogger = Logger.getLogger(classOf[kafka.server.KafkaRequestHandler])
    requestHandlerLogger.setLevel(Level.FATAL)

    // send some messages to each broker
    val sentMessages1 = sendMessagesToBrokerPartition(configs.head, topic, 0, nMessages, NoCompressionCodec)
    val sentMessages2 = sendMessagesToBrokerPartition(configs.last, topic, 1, nMessages, NoCompressionCodec)
    val sentMessages = (sentMessages1 ++ sentMessages2).map(m => Utils.toString(m.payload, "UTF-8")).
      sortWith((s, t) => s.compare(t) == -1)

    val consumerConfig = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer1))

    waitUntilLeaderIsElected(zkClient, topic, 0, 500)
    waitUntilLeaderIsElected(zkClient, topic, 1, 500)

    val zkConsumerConnector =
      new ZookeeperConsumerConnector(consumerConfig, true)
    val topicMessageStreams =
      zkConsumerConnector.createMessageStreams(Predef.Map(topic -> 1), new StringDecoder)


    var receivedMessages: List[String] = Nil
    for ((topic, messageStreams) <- topicMessageStreams) {
      for (messageStream <- messageStreams) {
        val iterator = messageStream.iterator
        for (i <- 0 until nMessages * 2) {
          assertTrue(iterator.hasNext())
          val message = iterator.next().message
          receivedMessages ::= message
          debug("received message: " + message)
        }
      }
    }
    receivedMessages = receivedMessages.sortWith((s, t) => s.compare(t) == -1)
    assertEquals(sentMessages, receivedMessages)

    zkConsumerConnector.shutdown()
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testLeaderSelectionForPartition() {
    val zkClient = new ZkClient(zookeeperConnect, 6000, 30000, ZKStringSerializer)

    // create topic topic1 with 1 partition on broker 0
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "0")

    // send some messages to each broker
    val sentMessages1 = sendMessages(configs.head, nMessages, "batch1", NoCompressionCodec, 1)

    // create a consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Predef.Map(topic -> 1))
    val topicRegistry = zkConsumerConnector1.getTopicRegistry
    assertEquals(1, topicRegistry.map(r => r._1).size)
    assertEquals(topic, topicRegistry.map(r => r._1).head)
    val topicsAndPartitionsInRegistry = topicRegistry.map(r => (r._1, r._2.map(p => p._2)))
    val brokerPartition = topicsAndPartitionsInRegistry.head._2.head
    assertEquals(0, brokerPartition.brokerId)
    assertEquals(0, brokerPartition.partitionId)

    // also check partition ownership
    val actual_1 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_1 = List( ("0", "group1_consumer1-0"))
    assertEquals(expected_1, actual_1)

    val receivedMessages1 = getMessages(nMessages, topicMessageStreams1)
    assertEquals(nMessages, receivedMessages1.size)
    assertEquals(sentMessages1, receivedMessages1)
  }

  def sendMessagesToBrokerPartition(config: KafkaConfig, topic: String, partition: Int, numMessages: Int,
                                    compression: CompressionCodec = NoCompressionCodec): List[Message] = {
    val header = "test-%d-%d".format(config.brokerId, partition)
    val props = new Properties()
    props.put("zk.connect", zkConnect)
    props.put("partitioner.class", "kafka.utils.FixedValuePartitioner")
    props.put("compression.codec", compression.codec.toString)
    val producer: Producer[Int, Message] = new Producer[Int, Message](new ProducerConfig(props))
    val ms = 0.until(numMessages).map(x =>
      new Message((header + config.brokerId + "-" + partition + "-" + x).getBytes)).toArray
    producer.send(new ProducerData[Int, Message](topic, partition, ms))
    debug("Sent %d messages to broker %d for topic %s and partition %d".format(ms.size, config.brokerId, topic, partition))
    producer
    ms.toList
  }

  def sendMessages(conf: KafkaConfig, messagesPerNode: Int, header: String, compression: CompressionCodec, numParts: Int): List[Message]= {
    var messages: List[Message] = Nil
    val props = new Properties()
    props.put("zk.connect", zkConnect)
    props.put("partitioner.class", "kafka.utils.FixedValuePartitioner")
    val producer: Producer[Int, Message] = new Producer[Int, Message](new ProducerConfig(props))

    for (partition <- 0 until numParts) {
      val ms = 0.until(messagesPerNode).map(x =>
        new Message((header + conf.brokerId + "-" + partition + "-" + x).getBytes)).toArray
      for (message <- ms)
        messages ::= message
      producer.send(new ProducerData[Int, Message](topic, partition, ms))
      debug("Sent %d messages to broker %d for topic %s and partition %d".format(ms.size, conf.brokerId, topic, partition))
    }
    producer.close()
    messages.reverse
  }

  def sendMessages(messagesPerNode: Int, header: String, compression: CompressionCodec = NoCompressionCodec): List[Message]= {
    var messages: List[Message] = Nil
    for(conf <- configs) {
      messages ++= sendMessages(conf, messagesPerNode, header, compression, numParts)
    }
    messages.sortWith((s,t) => s.checksum < t.checksum)
  }

  def getMessages(nMessagesPerThread: Int, topicMessageStreams: Map[String,List[KafkaStream[Message]]]): List[Message]= {
    var messages: List[Message] = Nil
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

  def getZKChildrenValues(path : String) : Seq[Tuple2[String,String]] = {
    import scala.collection.JavaConversions
    val children = zkClient.getChildren(path)
    Collections.sort(children)
    val childrenAsSeq : Seq[java.lang.String] = JavaConversions.asBuffer(children)
    childrenAsSeq.map(partition =>
      (partition, zkClient.readData(path + "/" + partition).asInstanceOf[String]))
  }

}


