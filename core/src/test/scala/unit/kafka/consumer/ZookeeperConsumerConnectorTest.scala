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

import java.util.{Collections, Properties}

import org.junit.Assert._
import kafka.common.MessageStreamsExistException
import kafka.integration.KafkaServerTestHarness
import kafka.javaapi.consumer.ConsumerRebalanceListener
import kafka.message._
import kafka.serializer._
import kafka.server._
import kafka.utils.TestUtils._
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{Level, Logger}
import org.junit.{Test, After, Before}

import scala.collection._

@deprecated("This test has been deprecated and it will be removed in a future release", "0.10.0.0")
class ZookeeperConsumerConnectorTest extends KafkaServerTestHarness with Logging {

  val RebalanceBackoffMs = 5000
  var dirs : ZKGroupTopicDirs = null
  val numNodes = 2
  val numParts = 2
  val topic = "topic1"
  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)

  override def generateConfigs() = TestUtils.createBrokerConfigs(numNodes, zkConnect)
    .map(KafkaConfig.fromProps(_, overridingProps))

  val group = "group1"
  val consumer0 = "consumer0"
  val consumer1 = "consumer1"
  val consumer2 = "consumer2"
  val consumer3 = "consumer3"
  val nMessages = 2

  @Before
  override def setUp() {
    super.setUp()
    dirs = new ZKGroupTopicDirs(group, topic)
  }

  @After
  override def tearDown() {
    super.tearDown()
  }

  @Test
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
        getMessages(topicMessageStreams0, nMessages * 2)
        fail("should get an exception")
      } catch {
        case e: ConsumerTimeoutException => // this is ok
        case e: Throwable => throw e
      }
    }

    zkConsumerConnector0.shutdown

    // send some messages to each broker
    val sentMessages1 = sendMessages(servers, topic, nMessages, 0) ++
      sendMessages(servers, topic, nMessages, 1)

    // wait to make sure the topic and partition have a leader for the successful case
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 1)

    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 1)

    // create a consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())

    val receivedMessages1 = getMessages(topicMessageStreams1, nMessages * 2)
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
    val sentMessages2 = sendMessages(servers, topic, nMessages, 0) ++
                         sendMessages(servers, topic, nMessages, 1)

    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 1)

    val receivedMessages2 = getMessages(topicMessageStreams1, nMessages) ++ getMessages(topicMessageStreams2, nMessages)
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
    val sentMessages3 = sendMessages(servers, topic, nMessages, 0) ++
                        sendMessages(servers, topic, nMessages, 1)

    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 1)

    val receivedMessages3 = getMessages(topicMessageStreams1, nMessages) ++ getMessages(topicMessageStreams2, nMessages)
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

  @Test
  def testCompression() {
    val requestHandlerLogger = Logger.getLogger(classOf[kafka.server.KafkaRequestHandler])
    requestHandlerLogger.setLevel(Level.FATAL)

    // send some messages to each broker
    val sentMessages1 = sendMessages(servers, topic, nMessages, 0, GZIPCompressionCodec) ++
                        sendMessages(servers, topic, nMessages, 1, GZIPCompressionCodec)

    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 1)

    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 1)

    // create a consumer
    val consumerConfig1 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())
    val receivedMessages1 = getMessages(topicMessageStreams1, nMessages * 2)
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
    val sentMessages2 = sendMessages(servers, topic, nMessages, 0, GZIPCompressionCodec) ++
                        sendMessages(servers, topic, nMessages, 1, GZIPCompressionCodec)

    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 1)

    val receivedMessages2 = getMessages(topicMessageStreams1, nMessages) ++ getMessages(topicMessageStreams2, nMessages)
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
    val sentMessages3 = sendMessages(servers, topic, nMessages, 0, GZIPCompressionCodec) ++
                        sendMessages(servers, topic, nMessages, 1, GZIPCompressionCodec)

    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 1)

    val receivedMessages3 = getMessages(topicMessageStreams1, nMessages) ++ getMessages(topicMessageStreams2, nMessages)
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

  @Test
  def testCompressionSetConsumption() {
    // send some messages to each broker
    val sentMessages = sendMessages(servers, topic, 200, 0, DefaultCompressionCodec) ++
                       sendMessages(servers, topic, 200, 1, DefaultCompressionCodec)

    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 1)

    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer0))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())
    val receivedMessages = getMessages(topicMessageStreams1, 400)
    assertEquals(sentMessages.sorted, receivedMessages.sorted)

    // also check partition ownership
    val actual_2 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_2 = List( ("0", "group1_consumer0-0"),
                           ("1", "group1_consumer0-0"))
    assertEquals(expected_2, actual_2)

    zkConsumerConnector1.shutdown
  }

  @Test
  def testConsumerDecoder() {
    val requestHandlerLogger = Logger.getLogger(classOf[kafka.server.KafkaRequestHandler])
    requestHandlerLogger.setLevel(Level.FATAL)

    // send some messages to each broker
    val sentMessages = sendMessages(servers, topic, nMessages, 0, NoCompressionCodec) ++
                       sendMessages(servers, topic, nMessages, 1, NoCompressionCodec)

    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 1)

    val consumerConfig = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer1))

    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 0)
    waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 1)

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

  @Test
  def testLeaderSelectionForPartition() {
    val zkUtils = ZkUtils(zkConnect, 6000, 30000, false)

    // create topic topic1 with 1 partition on broker 0
    createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 1, servers = servers)

    // send some messages to each broker
    val sentMessages1 = sendMessages(servers, topic, nMessages)

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

    val receivedMessages1 = getMessages(topicMessageStreams1, nMessages)
    assertEquals(sentMessages1, receivedMessages1)
    zkConsumerConnector1.shutdown()
    zkUtils.close()
  }

  @Test
  def testConsumerRebalanceListener() {
    // Send messages to create topic
    sendMessages(servers, topic, nMessages, 0)
    sendMessages(servers, topic, nMessages, 1)

    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    // Register consumer rebalance listener
    val rebalanceListener1 = new TestConsumerRebalanceListener()
    zkConsumerConnector1.setConsumerRebalanceListener(rebalanceListener1)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())

    // Check if rebalance listener is fired
    assertEquals(true, rebalanceListener1.beforeReleasingPartitionsCalled)
    assertEquals(true, rebalanceListener1.beforeStartingFetchersCalled)
    assertEquals(null, rebalanceListener1.partitionOwnership.get(topic))
    // Check if partition assignment in rebalance listener is correct
    assertEquals("group1_consumer1", rebalanceListener1.globalPartitionOwnership.get(topic).get(0).consumer)
    assertEquals("group1_consumer1", rebalanceListener1.globalPartitionOwnership.get(topic).get(1).consumer)
    assertEquals(0, rebalanceListener1.globalPartitionOwnership.get(topic).get(0).threadId)
    assertEquals(0, rebalanceListener1.globalPartitionOwnership.get(topic).get(1).threadId)
    assertEquals("group1_consumer1", rebalanceListener1.consumerId)
    // reset the flag
    rebalanceListener1.beforeReleasingPartitionsCalled = false
    rebalanceListener1.beforeStartingFetchersCalled = false

    val actual_1 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_1 = List(("0", "group1_consumer1-0"),
                          ("1", "group1_consumer1-0"))
    assertEquals(expected_1, actual_1)

    val consumerConfig2 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumer2))
    val zkConsumerConnector2 = new ZookeeperConsumerConnector(consumerConfig2, true)
    // Register consumer rebalance listener
    val rebalanceListener2 = new TestConsumerRebalanceListener()
    zkConsumerConnector2.setConsumerRebalanceListener(rebalanceListener2)
    val topicMessageStreams2 = zkConsumerConnector2.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder())

    // Consume messages from consumer 1 to make sure it has finished rebalance
    getMessages(topicMessageStreams1, nMessages)

    val actual_2 = getZKChildrenValues(dirs.consumerOwnerDir)
    val expected_2 = List(("0", "group1_consumer1-0"),
                          ("1", "group1_consumer2-0"))
    assertEquals(expected_2, actual_2)

    // Check if rebalance listener is fired
    assertEquals(true, rebalanceListener1.beforeReleasingPartitionsCalled)
    assertEquals(true, rebalanceListener1.beforeStartingFetchersCalled)
    assertEquals(Set[Int](0, 1), rebalanceListener1.partitionOwnership.get(topic))
    // Check if global partition ownership in rebalance listener is correct
    assertEquals("group1_consumer1", rebalanceListener1.globalPartitionOwnership.get(topic).get(0).consumer)
    assertEquals("group1_consumer2", rebalanceListener1.globalPartitionOwnership.get(topic).get(1).consumer)
    assertEquals(0, rebalanceListener1.globalPartitionOwnership.get(topic).get(0).threadId)
    assertEquals(0, rebalanceListener1.globalPartitionOwnership.get(topic).get(1).threadId)
    assertEquals("group1_consumer1", rebalanceListener1.consumerId)
    assertEquals("group1_consumer2", rebalanceListener2.consumerId)
    assertEquals(rebalanceListener1.globalPartitionOwnership, rebalanceListener2.globalPartitionOwnership)
    zkConsumerConnector1.shutdown()
    zkConsumerConnector2.shutdown()
  }

  def getZKChildrenValues(path : String) : Seq[Tuple2[String,String]] = {
    val children = zkUtils.zkClient.getChildren(path)
    Collections.sort(children)
    val childrenAsSeq : Seq[java.lang.String] = {
      import scala.collection.JavaConversions._
      children.toSeq
    }
    childrenAsSeq.map(partition =>
      (partition, zkUtils.zkClient.readData(path + "/" + partition).asInstanceOf[String]))
  }

  private class TestConsumerRebalanceListener extends ConsumerRebalanceListener {
    var beforeReleasingPartitionsCalled: Boolean = false
    var beforeStartingFetchersCalled: Boolean = false
    var consumerId: String = ""
    var partitionOwnership: java.util.Map[String, java.util.Set[java.lang.Integer]] = null
    var globalPartitionOwnership: java.util.Map[String, java.util.Map[java.lang.Integer, ConsumerThreadId]] = null

    override def beforeReleasingPartitions(partitionOwnership: java.util.Map[String, java.util.Set[java.lang.Integer]]) {
      beforeReleasingPartitionsCalled = true
      this.partitionOwnership = partitionOwnership
    }

    override def beforeStartingFetchers(consumerId: String, globalPartitionOwnership: java.util.Map[String, java.util.Map[java.lang.Integer, ConsumerThreadId]]) {
      beforeStartingFetchersCalled = true
      this.consumerId = consumerId
      this.globalPartitionOwnership = globalPartitionOwnership
    }
  }

}
