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

package kafka.integration

import kafka.api.TopicMetadataResponse
import kafka.client.ClientUtils
import kafka.cluster.BrokerEndPoint
import kafka.server.{KafkaConfig, KafkaServer, NotRunning}
import kafka.utils.TestUtils
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.protocol.Errors
import org.junit.Assert._
import org.junit.{Test, After, Before}

class TopicMetadataTest extends ZooKeeperTestHarness {
  private var server1: KafkaServer = null
  private var adHocServers: Seq[KafkaServer] = Seq()
  var brokerEndPoints: Seq[BrokerEndPoint] = null
  var adHocConfigs: Seq[KafkaConfig] = null
  val numConfigs: Int = 4

  @Before
  override def setUp(): Unit = {
    super.setUp()
    val props = createBrokerConfigs(numConfigs, zkConnect)
    val configs: Seq[KafkaConfig] = props.map(KafkaConfig.fromProps)
    adHocConfigs = configs.takeRight(configs.size - 1) // Started and stopped by individual test cases
    server1 = TestUtils.createServer(configs.head)
    brokerEndPoints = Seq(
      // We are using the Scala clients and they don't support SSL. Once we move to the Java ones, we should use
      // `securityProtocol` instead of PLAINTEXT below
      new BrokerEndPoint(server1.config.brokerId, server1.config.hostName, TestUtils.boundPort(server1))
    )
  }

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(adHocServers :+ server1)
    super.tearDown()
  }

  @Test
  def testBasicTopicMetadata(): Unit = {
    // create topic
    val topic = "test"
    createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 1, servers = Seq(server1))

    val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), brokerEndPoints, "TopicMetadataTest-testBasicTopicMetadata",
      2000, 0).topicsMetadata
    assertEquals(Errors.NONE, topicsMetadata.head.error)
    assertEquals(Errors.NONE, topicsMetadata.head.partitionsMetadata.head.error)
    assertEquals("Expecting metadata only for 1 topic", 1, topicsMetadata.size)
    assertEquals("Expecting metadata for the test topic", "test", topicsMetadata.head.topic)
    val partitionMetadata = topicsMetadata.head.partitionsMetadata
    assertEquals("Expecting metadata for 1 partition", 1, partitionMetadata.size)
    assertEquals("Expecting partition id to be 0", 0, partitionMetadata.head.partitionId)
    assertEquals(1, partitionMetadata.head.replicas.size)
  }

  @Test
  def testGetAllTopicMetadata(): Unit = {
    // create topic
    val topic1 = "testGetAllTopicMetadata1"
    val topic2 = "testGetAllTopicMetadata2"
    createTopic(zkClient, topic1, numPartitions = 1, replicationFactor = 1, servers = Seq(server1))
    createTopic(zkClient, topic2, numPartitions = 1, replicationFactor = 1, servers = Seq(server1))

    // issue metadata request with empty list of topics
    val topicsMetadata = ClientUtils.fetchTopicMetadata(Set.empty, brokerEndPoints, "TopicMetadataTest-testGetAllTopicMetadata",
      2000, 0).topicsMetadata
    assertEquals(Errors.NONE, topicsMetadata.head.error)
    assertEquals(2, topicsMetadata.size)
    assertEquals(Errors.NONE, topicsMetadata.head.partitionsMetadata.head.error)
    assertEquals(Errors.NONE, topicsMetadata.last.partitionsMetadata.head.error)
    val partitionMetadataTopic1 = topicsMetadata.head.partitionsMetadata
    val partitionMetadataTopic2 = topicsMetadata.last.partitionsMetadata
    assertEquals("Expecting metadata for 1 partition", 1, partitionMetadataTopic1.size)
    assertEquals("Expecting partition id to be 0", 0, partitionMetadataTopic1.head.partitionId)
    assertEquals(1, partitionMetadataTopic1.head.replicas.size)
    assertEquals("Expecting metadata for 1 partition", 1, partitionMetadataTopic2.size)
    assertEquals("Expecting partition id to be 0", 0, partitionMetadataTopic2.head.partitionId)
    assertEquals(1, partitionMetadataTopic2.head.replicas.size)
  }

  @Test
  def testAutoCreateTopic(): Unit = {
    // auto create topic
    val topic = "testAutoCreateTopic"
    var topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), brokerEndPoints, "TopicMetadataTest-testAutoCreateTopic",
      2000,0).topicsMetadata
    assertEquals(Errors.LEADER_NOT_AVAILABLE, topicsMetadata.head.error)
    assertEquals("Expecting metadata only for 1 topic", 1, topicsMetadata.size)
    assertEquals("Expecting metadata for the test topic", topic, topicsMetadata.head.topic)
    assertEquals(0, topicsMetadata.head.partitionsMetadata.size)

    // wait for leader to be elected
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0)
    TestUtils.waitUntilMetadataIsPropagated(Seq(server1), topic, 0)

    // retry the metadata for the auto created topic
    topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), brokerEndPoints, "TopicMetadataTest-testBasicTopicMetadata",
      2000,0).topicsMetadata
    assertEquals(Errors.NONE, topicsMetadata.head.error)
    assertEquals(Errors.NONE, topicsMetadata.head.partitionsMetadata.head.error)
    val partitionMetadata = topicsMetadata.head.partitionsMetadata
    assertEquals("Expecting metadata for 1 partition", 1, partitionMetadata.size)
    assertEquals("Expecting partition id to be 0", 0, partitionMetadata.head.partitionId)
    assertEquals(1, partitionMetadata.head.replicas.size)
    assertTrue(partitionMetadata.head.leader.isDefined)
  }

  @Test
  def testAutoCreateTopicWithInvalidReplication(): Unit = {
    val adHocProps = createBrokerConfig(2, zkConnect)
    // Set default replication higher than the number of live brokers
    adHocProps.setProperty(KafkaConfig.DefaultReplicationFactorProp, "3")
    // start adHoc brokers with replication factor too high
    val adHocServer = createServer(new KafkaConfig(adHocProps))
    adHocServers = Seq(adHocServer)
    // We are using the Scala clients and they don't support SSL. Once we move to the Java ones, we should use
    // `securityProtocol` instead of PLAINTEXT below
    val adHocEndpoint = new BrokerEndPoint(adHocServer.config.brokerId, adHocServer.config.hostName,
      TestUtils.boundPort(adHocServer))

    // auto create topic on "bad" endpoint
    val topic = "testAutoCreateTopic"
    val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), Seq(adHocEndpoint), "TopicMetadataTest-testAutoCreateTopic",
      2000, 0).topicsMetadata
    assertEquals(Errors.INVALID_REPLICATION_FACTOR, topicsMetadata.head.error)
    assertEquals("Expecting metadata only for 1 topic", 1, topicsMetadata.size)
    assertEquals("Expecting metadata for the test topic", topic, topicsMetadata.head.topic)
    assertEquals(0, topicsMetadata.head.partitionsMetadata.size)
  }

  @Test
  def testAutoCreateTopicWithCollision(): Unit = {
    // auto create topic
    val topic1 = "testAutoCreate_Topic"
    val topic2 = "testAutoCreate.Topic"
    var topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic1, topic2), brokerEndPoints, "TopicMetadataTest-testAutoCreateTopic",
      2000, 0).topicsMetadata
    assertEquals("Expecting metadata for 2 topics", 2, topicsMetadata.size)
    assertEquals("Expecting metadata for topic1", topic1, topicsMetadata.head.topic)
    assertEquals(Errors.LEADER_NOT_AVAILABLE, topicsMetadata.head.error)
    assertEquals("Expecting metadata for topic2", topic2, topicsMetadata(1).topic)
    assertEquals("Expecting InvalidTopicCode for topic2 metadata", Errors.INVALID_TOPIC_EXCEPTION, topicsMetadata(1).error)

    // wait for leader to be elected
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic1, 0)
    TestUtils.waitUntilMetadataIsPropagated(Seq(server1), topic1, 0)

    // retry the metadata for the first auto created topic
    topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic1), brokerEndPoints, "TopicMetadataTest-testBasicTopicMetadata",
      2000, 0).topicsMetadata
    assertEquals(Errors.NONE, topicsMetadata.head.error)
    assertEquals(Errors.NONE, topicsMetadata.head.partitionsMetadata.head.error)
    val partitionMetadata = topicsMetadata.head.partitionsMetadata
    assertEquals("Expecting metadata for 1 partition", 1, partitionMetadata.size)
    assertEquals("Expecting partition id to be 0", 0, partitionMetadata.head.partitionId)
    assertEquals(1, partitionMetadata.head.replicas.size)
    assertTrue(partitionMetadata.head.leader.isDefined)
  }

  private def checkIsr(servers: Seq[KafkaServer]): Unit = {
    val activeBrokers: Seq[KafkaServer] = servers.filter(x => x.brokerState.currentState != NotRunning.state)
    val expectedIsr: Seq[BrokerEndPoint] = activeBrokers.map { x =>
      new BrokerEndPoint(x.config.brokerId,
        if (x.config.hostName.nonEmpty) x.config.hostName else "localhost",
        TestUtils.boundPort(x))
    }

    // Assert that topic metadata at new brokers is updated correctly
    activeBrokers.foreach(x => {
      var metadata: TopicMetadataResponse = new TopicMetadataResponse(Seq(), Seq(), -1)
      waitUntilTrue(() => {
        metadata = ClientUtils.fetchTopicMetadata(Set.empty,
                                Seq(new BrokerEndPoint(x.config.brokerId,
                                                       if (x.config.hostName.nonEmpty) x.config.hostName else "localhost",
                                                       TestUtils.boundPort(x))),
                                "TopicMetadataTest-testBasicTopicMetadata", 2000, 0)
        metadata.topicsMetadata.nonEmpty &&
          metadata.topicsMetadata.head.partitionsMetadata.nonEmpty &&
          expectedIsr.sortBy(_.id) == metadata.topicsMetadata.head.partitionsMetadata.head.isr.sortBy(_.id)
      },
        "Topic metadata is not correctly updated for broker " + x + ".\n" +
        "Expected ISR: " + expectedIsr + "\n" +
        "Actual ISR  : " + (if (metadata.topicsMetadata.nonEmpty &&
                                metadata.topicsMetadata.head.partitionsMetadata.nonEmpty)
                              metadata.topicsMetadata.head.partitionsMetadata.head.isr
                            else
                              ""), 8000L)
    })
  }

  @Test
  def testIsrAfterBrokerShutDownAndJoinsBack(): Unit = {
    val numBrokers = 2 //just 2 brokers are enough for the test

    // start adHoc brokers
    adHocServers = adHocConfigs.take(numBrokers - 1).map(p => createServer(p))
    val allServers: Seq[KafkaServer] = Seq(server1) ++ adHocServers

    // create topic
    val topic: String = "test"
    adminZkClient.createTopic(topic, 1, numBrokers)

    // shutdown a broker
    adHocServers.last.shutdown()
    adHocServers.last.awaitShutdown()

    // startup a broker
    adHocServers.last.startup()

    // check metadata is still correct and updated at all brokers
    checkIsr(allServers)
  }

  private def checkMetadata(servers: Seq[KafkaServer], expectedBrokersCount: Int): Unit = {
    var topicMetadata: TopicMetadataResponse = new TopicMetadataResponse(Seq(), Seq(), -1)

    // Get topic metadata from old broker
    // Wait for metadata to get updated by checking metadata from a new broker
    waitUntilTrue(() => {
    topicMetadata = ClientUtils.fetchTopicMetadata(
      Set.empty, brokerEndPoints, "TopicMetadataTest-testBasicTopicMetadata", 2000, 0)
    topicMetadata.brokers.size == expectedBrokersCount},
      "Alive brokers list is not correctly propagated by coordinator to brokers"
    )

    // Assert that topic metadata at new brokers is updated correctly
    servers.filter(x => x.brokerState.currentState != NotRunning.state).foreach(x =>
      waitUntilTrue(() => {
          val foundMetadata = ClientUtils.fetchTopicMetadata(
            Set.empty,
            Seq(new BrokerEndPoint(x.config.brokerId, x.config.hostName, TestUtils.boundPort(x))),
            "TopicMetadataTest-testBasicTopicMetadata", 2000, 0)
          topicMetadata.brokers.sortBy(_.id) == foundMetadata.brokers.sortBy(_.id) &&
            topicMetadata.topicsMetadata.sortBy(_.topic) == foundMetadata.topicsMetadata.sortBy(_.topic)
        },
        s"Topic metadata is not correctly updated"))
  }

  @Test
  def testAliveBrokerListWithNoTopics(): Unit = {
    checkMetadata(Seq(server1), 1)
  }

  @Test
  def testAliveBrokersListWithNoTopicsAfterNewBrokerStartup(): Unit = {
    adHocServers = adHocConfigs.takeRight(adHocConfigs.size - 1).map(p => createServer(p))

    checkMetadata(adHocServers, numConfigs - 1)

    // Add a broker
    adHocServers = adHocServers ++ Seq(createServer(adHocConfigs.head))

    checkMetadata(adHocServers, numConfigs)
  }


  @Test
  def testAliveBrokersListWithNoTopicsAfterABrokerShutdown(): Unit = {
    adHocServers = adHocConfigs.map(p => createServer(p))

    checkMetadata(adHocServers, numConfigs)

    // Shutdown a broker
    adHocServers.last.shutdown()
    adHocServers.last.awaitShutdown()

    checkMetadata(adHocServers, numConfigs - 1)
  }
}
