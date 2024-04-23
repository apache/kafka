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
package kafka.admin

import java.util
import java.util.{Optional, Properties}
import kafka.controller.ReplicaAssignment
import kafka.server.DynamicConfig.Broker._
import kafka.server.KafkaConfig._
import kafka.server.{KafkaConfig, KafkaServer, QuorumTestHarness}
import kafka.utils.CoreUtils._
import kafka.utils.TestUtils._
import kafka.utils.{Logging, TestUtils}
import kafka.zk.{AdminZkClient, ConfigEntityTypeZNode, KafkaZkClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.errors.{InvalidReplicaAssignmentException, InvalidTopicException, TopicExistsException}
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.server.common.AdminOperationException
import org.apache.kafka.server.config.ConfigType
import org.apache.kafka.storage.internals.log.LogConfig
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.Mockito.{mock, when}

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, immutable}

class AdminZkClientTest extends QuorumTestHarness with Logging with RackAwareTest {

  private val producerByteRate = "1024"
  private val ipConnectionRate = "10"
  var servers: Seq[KafkaServer] = Seq()

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testManualReplicaAssignment(): Unit = {
    val brokers = List(0, 1, 2, 3, 4)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val topicConfig = new Properties()

    // duplicate brokers
    assertThrows(classOf[InvalidReplicaAssignmentException], () => adminZkClient.createTopicWithAssignment("test", topicConfig, Map(0->Seq(0,0))))

    // inconsistent replication factor
    assertThrows(classOf[InvalidReplicaAssignmentException], () => adminZkClient.createTopicWithAssignment("test", topicConfig, Map(0->Seq(0,1), 1->Seq(0))))

    // partitions should be 0-based
    assertThrows(classOf[InvalidReplicaAssignmentException], () => adminZkClient.createTopicWithAssignment("test", topicConfig, Map(1->Seq(1,2), 2->Seq(1,2))))

    // partitions should be 0-based and consecutive
    assertThrows(classOf[InvalidReplicaAssignmentException], () => adminZkClient.createTopicWithAssignment("test", topicConfig, Map(0->Seq(1,2), 0->Seq(1,2), 3->Seq(1,2))))

    // partitions should be 0-based and consecutive
    assertThrows(classOf[InvalidReplicaAssignmentException], () => adminZkClient.createTopicWithAssignment("test", topicConfig, Map(-1->Seq(1,2), 1->Seq(1,2), 2->Seq(1,2), 4->Seq(1,2))))

    // good assignment
    val assignment = Map(0 -> List(0, 1, 2),
                         1 -> List(1, 2, 3))
    adminZkClient.createTopicWithAssignment("test", topicConfig, assignment)
    val found = zkClient.getPartitionAssignmentForTopics(Set("test"))
    assertEquals(assignment.map { case (k, v) => k -> ReplicaAssignment(v, List(), List()) }, found("test"))
  }

  @Test
  def testTopicCreationInZK(): Unit = {
    val expectedReplicaAssignment = Map(
      0  -> List(0, 1, 2),
      1  -> List(1, 2, 3),
      2  -> List(2, 3, 4),
      3  -> List(3, 4, 0),
      4  -> List(4, 0, 1),
      5  -> List(0, 2, 3),
      6  -> List(1, 3, 4),
      7  -> List(2, 4, 0),
      8  -> List(3, 0, 1),
      9  -> List(4, 1, 2),
      10 -> List(1, 2, 3),
      11 -> List(1, 3, 4)
    )
    val leaderForPartitionMap = immutable.Map(
      0 -> 0,
      1 -> 1,
      2 -> 2,
      3 -> 3,
      4 -> 4,
      5 -> 0,
      6 -> 1,
      7 -> 2,
      8 -> 3,
      9 -> 4,
      10 -> 1,
      11 -> 1
    )
    val topic = "test"
    val topicConfig = new Properties()
    TestUtils.createBrokersInZk(zkClient, List(0, 1, 2, 3, 4))
    // create the topic
    adminZkClient.createTopicWithAssignment(topic, topicConfig, expectedReplicaAssignment)
    // create leaders for all partitions
    TestUtils.makeLeaderForPartition(zkClient, topic, leaderForPartitionMap, 1)
    val actualReplicaMap = leaderForPartitionMap.keys.map(p => p -> zkClient.getReplicasForPartition(new TopicPartition(topic, p))).toMap
    assertEquals(expectedReplicaAssignment.size, actualReplicaMap.size)
    for (i <- 0 until actualReplicaMap.size)
      assertEquals(expectedReplicaAssignment.get(i).get, actualReplicaMap(i))

    // shouldn't be able to create a topic that already exists
    assertThrows(classOf[TopicExistsException], () => adminZkClient.createTopicWithAssignment(topic, topicConfig, expectedReplicaAssignment))
  }

  @Test
  def testTopicCreationWithCollision(): Unit = {
    val topic = "test.topic"
    val collidingTopic = "test_topic"
    TestUtils.createBrokersInZk(zkClient, List(0, 1, 2, 3, 4))
    // create the topic
    adminZkClient.createTopic(topic, 3, 1)

    // shouldn't be able to create a topic that collides
    assertThrows(classOf[InvalidTopicException], () => adminZkClient.createTopic(collidingTopic, 3, 1))
  }

  @Test
  def testMarkedDeletionTopicCreation(): Unit = {
    val zkMock: KafkaZkClient = mock(classOf[KafkaZkClient])
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    when(zkMock.isTopicMarkedForDeletion(topic)).thenReturn(true)
    val adminZkClient = new AdminZkClient(zkMock)

    assertThrows(classOf[TopicExistsException], () => adminZkClient.validateTopicCreate(topic, Map.empty, new Properties))
  }

  @Test
  def testMockedConcurrentTopicCreation(): Unit = {
    val topic = "test.topic"

    // simulate the ZK interactions that can happen when a topic is concurrently created by multiple processes
    val zkMock: KafkaZkClient = mock(classOf[KafkaZkClient])
    when(zkMock.topicExists(topic)).thenReturn(false)
    when(zkMock.getAllTopicsInCluster(false)).thenReturn(Set("some.topic", topic, "some.other.topic"))
    val adminZkClient = new AdminZkClient(zkMock)

    assertThrows(classOf[TopicExistsException], () => adminZkClient.validateTopicCreate(topic, Map.empty, new Properties))
  }

  @Test
  def testConcurrentTopicCreation(): Unit = {
    val topic = "test-concurrent-topic-creation"
    TestUtils.createBrokersInZk(zkClient, List(0, 1, 2, 3, 4))
    val props = new Properties
    props.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    def createTopic(): Unit = {
      try adminZkClient.createTopic(topic, 3, 1, props)
      catch { case _: TopicExistsException => () }
      val (_, partitionAssignment) = zkClient.getPartitionAssignmentForTopics(Set(topic)).head
      assertEquals(3, partitionAssignment.size)
      partitionAssignment.foreach { case (partition, partitionReplicaAssignment) =>
        assertEquals(1, partitionReplicaAssignment.replicas.size, s"Unexpected replication factor for $partition")
      }
      val savedProps = zkClient.getEntityConfigs(ConfigType.TOPIC, topic)
      assertEquals(props, savedProps)
    }

    TestUtils.assertConcurrent("Concurrent topic creation failed", Seq(() => createTopic(), () => createTopic()),
      JTestUtils.DEFAULT_MAX_WAIT_MS.toInt)
  }

  /**
   * This test creates a topic with a few config overrides and checks that the configs are applied to the new topic
   * then changes the config and checks that the new values take effect.
   */
  @Test
  def testTopicConfigChange(): Unit = {
    val partitions = 3
    val topic = "my-topic"
    val server = TestUtils.createServer(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))
    servers = Seq(server)

    def makeConfig(messageSize: Int, retentionMs: Long, throttledLeaders: String, throttledFollowers: String) = {
      val props = new Properties()
      props.setProperty(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, messageSize.toString)
      props.setProperty(TopicConfig.RETENTION_MS_CONFIG, retentionMs.toString)
      props.setProperty(LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, throttledLeaders)
      props.setProperty(LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, throttledFollowers)
      props
    }

    def checkConfig(messageSize: Int, retentionMs: Long, throttledLeaders: String, throttledFollowers: String, quotaManagerIsThrottled: Boolean): Unit = {
      def checkList(actual: util.List[String], expected: String): Unit = {
        assertNotNull(actual)
        if (expected == "")
          assertTrue(actual.isEmpty)
        else
          assertEquals(expected.split(",").toSeq, actual.asScala)
      }
      TestUtils.retry(10000) {
        for (part <- 0 until partitions) {
          val tp = new TopicPartition(topic, part)
          val log = server.logManager.getLog(tp)
          assertTrue(log.isDefined)
          assertEquals(retentionMs, log.get.config.retentionMs)
          assertEquals(messageSize, log.get.config.maxMessageSize)
          checkList(log.get.config.leaderReplicationThrottledReplicas, throttledLeaders)
          checkList(log.get.config.followerReplicationThrottledReplicas, throttledFollowers)
          assertEquals(quotaManagerIsThrottled, server.quotaManagers.leader.isThrottled(tp))
        }
      }
    }

    // create a topic with a few config overrides and check that they are applied
    val maxMessageSize = 1024
    val retentionMs = 1000 * 1000
    adminZkClient.createTopic(topic, partitions, 1, makeConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1"))

    //Standard topic configs will be propagated at topic creation time, but the quota manager will not have been updated.
    checkConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1", false)

    //Update dynamically and all properties should be applied
    adminZkClient.changeTopicConfig(topic, makeConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1"))

    checkConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1", true)

    // now double the config values for the topic and check that it is applied
    val newConfig = makeConfig(2 * maxMessageSize, 2 * retentionMs, "*", "*")
    adminZkClient.changeTopicConfig(topic, makeConfig(2 * maxMessageSize, 2 * retentionMs, "*", "*"))
    checkConfig(2 * maxMessageSize, 2 * retentionMs, "*", "*", quotaManagerIsThrottled = true)

    // Verify that the same config can be read from ZK
    val configInZk = adminZkClient.fetchEntityConfig(ConfigType.TOPIC, topic)
    assertEquals(newConfig, configInZk)

    //Now delete the config
    adminZkClient.changeTopicConfig(topic, new Properties)
    checkConfig(LogConfig.DEFAULT_MAX_MESSAGE_BYTES, LogConfig.DEFAULT_RETENTION_MS, "", "", quotaManagerIsThrottled = false)

    //Add config back
    adminZkClient.changeTopicConfig(topic, makeConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1"))
    checkConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1", quotaManagerIsThrottled = true)

    //Now ensure updating to "" removes the throttled replica list also
    adminZkClient.changeTopicConfig(topic, propsWith((LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, ""), (LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "")))
    checkConfig(LogConfig.DEFAULT_MAX_MESSAGE_BYTES, LogConfig.DEFAULT_RETENTION_MS, "", "",  quotaManagerIsThrottled = false)
  }

  @Test
  def shouldPropagateDynamicBrokerConfigs(): Unit = {
    val brokerIds = Seq(0, 1, 2)
    servers = createBrokerConfigs(3, zkConnect).map(fromProps).map(createServer(_))

    def checkConfig(limit: Long): Unit = {
      retry(10000) {
        for (server <- servers) {
          assertEquals(limit, server.quotaManagers.leader.upperBound, "Leader Quota Manager was not updated")
          assertEquals(limit, server.quotaManagers.follower.upperBound, "Follower Quota Manager was not updated")
        }
      }
    }

    val limit: Long = 1000000

    // Set the limit & check it is applied to the log
    adminZkClient.changeBrokerConfig(brokerIds, propsWith(
      (LeaderReplicationThrottledRateProp, limit.toString),
      (FollowerReplicationThrottledRateProp, limit.toString)))
    checkConfig(limit)

    // Now double the config values for the topic and check that it is applied
    val newLimit = 2 * limit
    adminZkClient.changeBrokerConfig(brokerIds,  propsWith(
      (LeaderReplicationThrottledRateProp, newLimit.toString),
      (FollowerReplicationThrottledRateProp, newLimit.toString)))
    checkConfig(newLimit)

    // Verify that the same config can be read from ZK
    for (brokerId <- brokerIds) {
      val configInZk = adminZkClient.fetchEntityConfig(ConfigType.BROKER, brokerId.toString)
      assertEquals(newLimit, configInZk.getProperty(LeaderReplicationThrottledRateProp).toInt)
      assertEquals(newLimit, configInZk.getProperty(FollowerReplicationThrottledRateProp).toInt)
    }

    //Now delete the config
    adminZkClient.changeBrokerConfig(brokerIds, new Properties)
    checkConfig(DefaultReplicationThrottledRate)
  }

  /**
   * This test simulates a client config change in ZK whose notification has been purged.
   * Basically, it asserts that notifications are bootstrapped from ZK
   */
  @Test
  def testBootstrapClientIdConfig(): Unit = {
    val clientId = "my-client"
    val props = new Properties()
    props.setProperty("producer_byte_rate", "1000")
    props.setProperty("consumer_byte_rate", "2000")

    // Write config without notification to ZK.
    zkClient.setOrCreateEntityConfigs(ConfigType.CLIENT, clientId, props)

    val configInZk: Map[String, Properties] = adminZkClient.fetchAllEntityConfigs(ConfigType.CLIENT)
    assertEquals(1, configInZk.size, "Must have 1 overridden client config")
    assertEquals(props, configInZk(clientId))

    // Test that the existing clientId overrides are read
    val server = TestUtils.createServer(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))
    servers = Seq(server)
    assertEquals(new Quota(1000, true), server.dataPlaneRequestProcessor.quotas.produce.quota("ANONYMOUS", clientId))
    assertEquals(new Quota(2000, true), server.dataPlaneRequestProcessor.quotas.fetch.quota("ANONYMOUS", clientId))
  }

  @Test
  def testGetBrokerMetadatas(): Unit = {
    // broker 4 has no rack information
    val brokerList = 0 to 5
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 5 -> "rack3")
    val brokerMetadatas = toBrokerMetadata(rackInfo, brokersWithoutRack = brokerList.filterNot(rackInfo.keySet))
    TestUtils.createBrokersInZk(brokerMetadatas.asScala.toSeq, zkClient)

    val processedMetadatas1 = adminZkClient.getBrokerMetadatas(RackAwareMode.Disabled)
    assertEquals(brokerList, processedMetadatas1.map(_.id))
    assertEquals(List.fill(brokerList.size)(Optional.empty()), processedMetadatas1.map(_.rack))

    val processedMetadatas2 = adminZkClient.getBrokerMetadatas(RackAwareMode.Safe)
    assertEquals(brokerList, processedMetadatas2.map(_.id))
    assertEquals(List.fill(brokerList.size)(Optional.empty()), processedMetadatas2.map(_.rack))

    assertThrows(classOf[AdminOperationException], () => adminZkClient.getBrokerMetadatas(RackAwareMode.Enforced))

    val partialList = List(0, 1, 2, 3, 5)
    val processedMetadatas3 = adminZkClient.getBrokerMetadatas(RackAwareMode.Enforced, Some(partialList))
    assertEquals(partialList, processedMetadatas3.map(_.id))
    assertEquals(partialList.map(rackInfo), processedMetadatas3.map(_.rack.get()))

    val numPartitions = 3
    adminZkClient.createTopic("foo", numPartitions, 2, rackAwareMode = RackAwareMode.Safe)
    val assignment = zkClient.getReplicaAssignmentForTopics(Set("foo"))
    assertEquals(numPartitions, assignment.size)
  }

  @Test
  def testChangeUserOrUserClientIdConfigWithUserAndClientId(): Unit = {
    val config = new Properties()
    config.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, producerByteRate)
    adminZkClient.changeUserOrUserClientIdConfig("user01/clients/client01", config, isUserClientId = true)
    var props = zkClient.getEntityConfigs(ConfigType.USER, "user01/clients/client01")
    assertEquals(producerByteRate, props.getProperty(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG))

    // Children of clients and user01 should all be empty, so user01 should be deleted
    adminZkClient.changeUserOrUserClientIdConfig("user01/clients/client01", new Properties(), isUserClientId = true)
    var users = zkClient.getChildren(ConfigEntityTypeZNode.path(ConfigType.USER))
    assert(users.isEmpty)

    adminZkClient.changeUserOrUserClientIdConfig("user01", config)
    props = zkClient.getEntityConfigs(ConfigType.USER, "user01")
    assertEquals(producerByteRate, props.getProperty(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG))
    adminZkClient.changeUserOrUserClientIdConfig("user01/clients/client01", config, isUserClientId = true)
    props = zkClient.getEntityConfigs(ConfigType.USER, "user01/clients/client01")
    assertEquals(producerByteRate, props.getProperty(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG))

    // Children of clients are empty but configs of user01 are not empty, so clients should be deleted, but user01 should not
    adminZkClient.changeUserOrUserClientIdConfig("user01/clients/client01", new Properties(), isUserClientId = true)
    users = zkClient.getChildren(ConfigEntityTypeZNode.path(ConfigType.USER))
    assert(users == Seq("user01"))
  }

  @Test
  def testChangeUserOrUserClientIdConfigWithUser(): Unit = {
    val config = new Properties()
    config.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, producerByteRate)
    adminZkClient.changeUserOrUserClientIdConfig("user01", config)
    val props = zkClient.getEntityConfigs(ConfigType.USER, "user01")
    assertEquals(producerByteRate, props.getProperty(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG))

    adminZkClient.changeUserOrUserClientIdConfig("user01", new Properties())
    val users = zkClient.getChildren(ConfigEntityTypeZNode.path(ConfigType.USER))
    assert(users.isEmpty)
  }

  @Test
  def testChangeClientIdConfig(): Unit = {
    val config = new Properties()
    config.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, producerByteRate)
    adminZkClient.changeClientIdConfig("client01", config)
    val props = zkClient.getEntityConfigs(ConfigType.CLIENT, "client01")
    assertEquals(producerByteRate, props.getProperty(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG))

    adminZkClient.changeClientIdConfig("client01", new Properties())
    val users = zkClient.getChildren(ConfigEntityTypeZNode.path(ConfigType.CLIENT))
    assert(users.isEmpty)
  }

  @Test
  def testChangeIpConfig(): Unit = {
    val config = new Properties()
    config.put(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, ipConnectionRate)
    adminZkClient.changeIpConfig("127.0.0.1", config)
    val props = zkClient.getEntityConfigs(ConfigType.IP, "127.0.0.1")
    assertEquals(ipConnectionRate, props.getProperty(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG))

    adminZkClient.changeIpConfig("127.0.0.1", new Properties())
    val users = zkClient.getChildren(ConfigEntityTypeZNode.path(ConfigType.IP))
    assert(users.isEmpty)
  }
}
