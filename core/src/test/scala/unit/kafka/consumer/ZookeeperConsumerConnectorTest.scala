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
import kafka.message._
import kafka.serializer._
import org.I0Itec.zkclient.ZkClient
import kafka.utils._
import java.util.Collections
import org.apache.log4j.{Logger, Level}
import kafka.utils.TestUtils._
import kafka.common.MessageStreamsExistException

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
    val topicMessageStreams0 = zkConsumerConnector0.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())

    // no messages to consume, we should hit timeout;
    // also the iterator should support re-entrant, so loop it twice
    for (i <- 0 until 2) {
      try {
        getMessages(nMessages*2, topicMessageStreams0)
        fail("should get an exception")
      } catch {
        case e: ConsumerTimeoutException => // this is ok
        case e: Throwable => throw e
      }
    }

    zkConsumerConnector0.shutdown

    // send some messages to each broker
    val sentMessages1 = sendMessagesToPartition(configs, topic, 0, nMessages) ++
                        sendMessagesToPartition(configs, topic, 1, nMessages)

    // wait to make sure the topic and partition have a leader for the successful case
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 1)

    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 1)

    // create a consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())

    val receivedMessages1 = getMessages(nMessages*2, topicMessageStreams1)
    assertEquals(sentMessages1.sorted, receivedMessages1.sorted)

    // also check partition ownership
    val actual_1 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_1 = List( ("0", "group1_consumer1-0"),
                           ("1", "group1_consumer1-0"))
    assertEquals(expected_1, actual_1)

    // commit consumed offsets
    zkConsumerConnector1.commitOffsets(true)

    // create a consumer
    val consumerConfig2 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer2)) {
      override val rebalanceBackoffMs = RebalanceBackoffMs
    }
    val zkConsumerConnector2 = new ZookeeperConsumerConnector(consumerConfig2, true)
    val topicMessageStreams2 = zkConsumerConnector2.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())
    // send some messages to each broker
    val sentMessages2 = sendMessagesToPartition(configs, topic, 0, nMessages) ++
                        sendMessagesToPartition(configs, topic, 1, nMessages)

    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 1)

    val receivedMessages2 = getMessages(nMessages, topicMessageStreams1) ++ getMessages(nMessages, topicMessageStreams2)
    assertEquals(sentMessages2.sorted, receivedMessages2.sorted)

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
    val sentMessages3 = sendMessagesToPartition(configs, topic, 0, nMessages) ++
                        sendMessagesToPartition(configs, topic, 1, nMessages)

    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 1)

    val receivedMessages3 = getMessages(nMessages, topicMessageStreams1) ++ getMessages(nMessages, topicMessageStreams2)
    assertEquals(sentMessages3.sorted, receivedMessages3.sorted)

    // also check partition ownership
    val actual_3 = getZKChildrenValues(dirs.consumerOwnerDir)
    assertEquals(expected_2, actual_3)

    // call createMesssageStreams twice should throw MessageStreamsExistException
    try {
      val topicMessageStreams4 = zkConsumerConnector3.createMessageStreams(new mutable.HashMap[String, Int]())
      fail("Should fail with MessageStreamsExistException")
    } catch {
      case e: MessageStreamsExistException => // expected
    }

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
    val sentMessages1 = sendMessagesToPartition(configs, topic, 0, nMessages, GZIPCompressionCodec) ++
                        sendMessagesToPartition(configs, topic, 1, nMessages, GZIPCompressionCodec)

    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 1)

    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 1)

    // create a consumer
    val consumerConfig1 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())
    val receivedMessages1 = getMessages(nMessages*2, topicMessageStreams1)
    assertEquals(sentMessages1.sorted, receivedMessages1.sorted)

    // also check partition ownership
    val actual_1 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_1 = List( ("0", "group1_consumer1-0"),
                           ("1", "group1_consumer1-0"))
    assertEquals(expected_1, actual_1)

    // commit consumed offsets
    zkConsumerConnector1.commitOffsets(true)

    // create a consumer
    val consumerConfig2 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer2)) {
      override val rebalanceBackoffMs = RebalanceBackoffMs
    }
    val zkConsumerConnector2 = new ZookeeperConsumerConnector(consumerConfig2, true)
    val topicMessageStreams2 = zkConsumerConnector2.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())
    // send some messages to each broker
    val sentMessages2 = sendMessagesToPartition(configs, topic, 0, nMessages, GZIPCompressionCodec) ++
                        sendMessagesToPartition(configs, topic, 1, nMessages, GZIPCompressionCodec)

    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 1)

    val receivedMessages2 = getMessages(nMessages, topicMessageStreams1) ++ getMessages(nMessages, topicMessageStreams2)
    assertEquals(sentMessages2.sorted, receivedMessages2.sorted)

    // also check partition ownership
    val actual_2 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_2 = List( ("0", "group1_consumer1-0"),
                           ("1", "group1_consumer2-0"))
    assertEquals(expected_2, actual_2)

    // create a consumer with empty map
    val consumerConfig3 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer3))
    val zkConsumerConnector3 = new ZookeeperConsumerConnector(consumerConfig3, true)
    val topicMessageStreams3 = zkConsumerConnector3.createMessageStreams(new mutable.HashMap[String, Int](), new StringDecoder(), new StringDecoder())
    // send some messages to each broker
    val sentMessages3 = sendMessagesToPartition(configs, topic, 0, nMessages, GZIPCompressionCodec) ++
                        sendMessagesToPartition(configs, topic, 1, nMessages, GZIPCompressionCodec)

    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 1)

    val receivedMessages3 = getMessages(nMessages, topicMessageStreams1) ++ getMessages(nMessages, topicMessageStreams2)
    assertEquals(sentMessages3.sorted, receivedMessages3.sorted)

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
    // send some messages to each broker
    val sentMessages = sendMessagesToPartition(configs, topic, 0, 200, DefaultCompressionCodec) ++
                       sendMessagesToPartition(configs, topic, 1, 200, DefaultCompressionCodec)

    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 1)

    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer0))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())
    val receivedMessages = getMessages(400, topicMessageStreams1)
    assertEquals(sentMessages.sorted, receivedMessages.sorted)

    // also check partition ownership
    val actual_2 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_2 = List( ("0", "group1_consumer0-0"),
                           ("1", "group1_consumer0-0"))
    assertEquals(expected_2, actual_2)

    zkConsumerConnector1.shutdown
  }

  def testConsumerDecoder() {
    val requestHandlerLogger = Logger.getLogger(classOf[kafka.server.KafkaRequestHandler])
    requestHandlerLogger.setLevel(Level.FATAL)

    // send some messages to each broker
    val sentMessages = sendMessagesToPartition(configs, topic, 0, nMessages, NoCompressionCodec) ++
                       sendMessagesToPartition(configs, topic, 1, nMessages, NoCompressionCodec)

    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 1)

    val consumerConfig = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer1))

    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkClient, topic, 1)

    val zkConsumerConnector =
      new ZookeeperConsumerConnector(consumerConfig, true)
    val topicMessageStreams =
      zkConsumerConnector.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())

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
    assertEquals(sentMessages.sorted, receivedMessages.sorted)

    zkConsumerConnector.shutdown()
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  def testLeaderSelectionForPartition() {
    val zkClient = new ZkClient(zookeeperConnect, 6000, 30000, ZKStringSerializer)

    // create topic topic1 with 1 partition on broker 0
    createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 1, servers = servers)

    // send some messages to each broker
    val sentMessages1 = sendMessages(configs, topic, "producer1", nMessages, "batch1", NoCompressionCodec, 1)

    // create a consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())
    val topicRegistry = zkConsumerConnector1.getTopicRegistry
    assertEquals(1, topicRegistry.map(r => r._1).size)
    assertEquals(topic, topicRegistry.map(r => r._1).head)
    val topicsAndPartitionsInRegistry = topicRegistry.map(r => (r._1, r._2.map(p => p._2)))
    val brokerPartition = topicsAndPartitionsInRegistry.head._2.head
    assertEquals(0, brokerPartition.partitionId)

    // also check partition ownership
    val actual_1 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_1 = List( ("0", "group1_consumer1-0"))
    assertEquals(expected_1, actual_1)

    val receivedMessages1 = getMessages(nMessages, topicMessageStreams1)
    assertEquals(sentMessages1, receivedMessages1)
    zkConsumerConnector1.shutdown()
    zkClient.close()
  }

  def getZKChildrenValues(path : String) : Seq[Tuple2[String,String]] = {
    val children = zkClient.getChildren(path)
    Collections.sort(children)
    val childrenAsSeq : Seq[java.lang.String] = {
      import JavaConversions._
      children.toSeq
    }
    childrenAsSeq.map(partition =>
      (partition, zkClient.readData(path + "/" + partition).asInstanceOf[String]))
  }

}
