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

import kafka.server.DynamicConfig.Broker._
import kafka.server.KafkaConfig._
import org.apache.kafka.common.errors.{InvalidReplicaAssignmentException, InvalidReplicationFactorException, InvalidTopicException, TopicExistsException}
import org.apache.kafka.common.metrics.Quota
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test
import java.util.Properties

import kafka.utils._
import kafka.log._
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.{Logging, TestUtils, ZkUtils}
import kafka.common.TopicAndPartition
import kafka.server.{ConfigType, KafkaConfig, KafkaServer}
import java.io.File
import java.util

import kafka.utils.TestUtils._
import kafka.admin.AdminUtils._

import scala.collection.{Map, immutable}
import kafka.utils.CoreUtils._
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

class AdminTest extends ZooKeeperTestHarness with Logging with RackAwareTest {

  @Test
  def testReplicaAssignment() {
    val brokerMetadatas = (0 to 4).map(new BrokerMetadata(_, None))

    // test 0 replication factor
    intercept[InvalidReplicationFactorException] {
      AdminUtils.assignReplicasToBrokers(brokerMetadatas, 10, 0)
    }

    // test wrong replication factor
    intercept[InvalidReplicationFactorException] {
      AdminUtils.assignReplicasToBrokers(brokerMetadatas, 10, 6)
    }

    // correct assignment
    val expectedAssignment = Map(
        0 -> List(0, 1, 2),
        1 -> List(1, 2, 3),
        2 -> List(2, 3, 4),
        3 -> List(3, 4, 0),
        4 -> List(4, 0, 1),
        5 -> List(0, 2, 3),
        6 -> List(1, 3, 4),
        7 -> List(2, 4, 0),
        8 -> List(3, 0, 1),
        9 -> List(4, 1, 2))

    val actualAssignment = AdminUtils.assignReplicasToBrokers(brokerMetadatas, 10, 3, 0)
    assertEquals(expectedAssignment, actualAssignment)
  }

  @Test
  def testManualReplicaAssignment() {
    val brokers = List(0, 1, 2, 3, 4)
    TestUtils.createBrokersInZk(zkUtils, brokers)

    // duplicate brokers
    intercept[InvalidReplicaAssignmentException] {
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, "test", Map(0->Seq(0,0)))
    }

    // inconsistent replication factor
    intercept[InvalidReplicaAssignmentException] {
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, "test", Map(0->Seq(0,1), 1->Seq(0)))
    }

    // good assignment
    val assignment = Map(0 -> List(0, 1, 2),
                         1 -> List(1, 2, 3))
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, "test", assignment)
    val found = zkUtils.getPartitionAssignmentForTopics(Seq("test"))
    assertEquals(assignment, found("test"))
  }

  @Test
  def testTopicCreationInZK() {
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
    TestUtils.createBrokersInZk(zkUtils, List(0, 1, 2, 3, 4))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, expectedReplicaAssignment)
    // create leaders for all partitions
    TestUtils.makeLeaderForPartition(zkUtils, topic, leaderForPartitionMap, 1)
    val actualReplicaList = leaderForPartitionMap.keys.toArray.map(p => p -> zkUtils.getReplicasForPartition(topic, p)).toMap
    assertEquals(expectedReplicaAssignment.size, actualReplicaList.size)
    for(i <- 0 until actualReplicaList.size)
      assertEquals(expectedReplicaAssignment.get(i).get, actualReplicaList(i))

    intercept[TopicExistsException] {
      // shouldn't be able to create a topic that already exists
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, expectedReplicaAssignment)
    }
  }

  @Test
  def testTopicCreationWithCollision() {
    val topic = "test.topic"
    val collidingTopic = "test_topic"
    TestUtils.createBrokersInZk(zkUtils, List(0, 1, 2, 3, 4))
    // create the topic
    AdminUtils.createTopic(zkUtils, topic, 3, 1)

    intercept[InvalidTopicException] {
      // shouldn't be able to create a topic that collides
      AdminUtils.createTopic(zkUtils, collidingTopic, 3, 1)
    }
  }

  @Test
  def testConcurrentTopicCreation() {
    val topic = "test.topic"

    // simulate the ZK interactions that can happen when a topic is concurrently created by multiple processes
    val zkMock = EasyMock.createNiceMock(classOf[ZkUtils])
    EasyMock.expect(zkMock.pathExists(s"/brokers/topics/$topic")).andReturn(false)
    EasyMock.expect(zkMock.getAllTopics).andReturn(Seq("some.topic", topic, "some.other.topic"))
    EasyMock.replay(zkMock)

    intercept[TopicExistsException] {
      AdminUtils.validateCreateOrUpdateTopic(zkMock, topic, Map.empty, new Properties, update = false)
    }
  }

  private def getBrokersWithPartitionDir(servers: Iterable[KafkaServer], topic: String, partitionId: Int): Set[Int] = {
    servers.filter(server => new File(server.config.logDirs.head, topic + "-" + partitionId).exists)
           .map(_.config.brokerId)
           .toSet
  }

  @Test
  def testPartitionReassignmentWithLeaderInNewReplicas() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    // create brokers
    val servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, expectedReplicaAssignment)
    // reassign partition 0
    val newReplicas = Seq(0, 2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkUtils, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment attempt failed for [test, 0]", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
        val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned().mapValues(_.newReplicas)
        ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkUtils, topicAndPartition,
        Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentCompleted
      },
      "Partition reassignment should complete")
    val assignedReplicas = zkUtils.getReplicasForPartition(topic, partitionToBeReassigned)
    // in sync replicas should not have any replica that is not in the new assigned replicas
    checkForPhantomInSyncReplicas(zkUtils, topic, partitionToBeReassigned, assignedReplicas)
    assertEquals("Partition should have been reassigned to 0, 2, 3", newReplicas, assignedReplicas)
    ensureNoUnderReplicatedPartitions(zkUtils, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")
    servers.foreach(_.shutdown())
  }

  @Test
  def testPartitionReassignmentWithLeaderNotInNewReplicas() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    // create brokers
    val servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, expectedReplicaAssignment)
    // reassign partition 0
    val newReplicas = Seq(1, 2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkUtils, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
        val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned().mapValues(_.newReplicas)
        ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkUtils, topicAndPartition,
          Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentCompleted
      },
      "Partition reassignment should complete")
    val assignedReplicas = zkUtils.getReplicasForPartition(topic, partitionToBeReassigned)
    assertEquals("Partition should have been reassigned to 0, 2, 3", newReplicas, assignedReplicas)
    checkForPhantomInSyncReplicas(zkUtils, topic, partitionToBeReassigned, assignedReplicas)
    ensureNoUnderReplicatedPartitions(zkUtils, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")

    servers.foreach(_.shutdown())
  }

  @Test
  def testPartitionReassignmentNonOverlappingReplicas() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1))
    val topic = "test"
    // create brokers
    val servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, expectedReplicaAssignment)
    // reassign partition 0
    val newReplicas = Seq(2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkUtils, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
        val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned().mapValues(_.newReplicas)
        ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkUtils, topicAndPartition,
          Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentCompleted
      },
      "Partition reassignment should complete")
    val assignedReplicas = zkUtils.getReplicasForPartition(topic, partitionToBeReassigned)
    assertEquals("Partition should have been reassigned to 2, 3", newReplicas, assignedReplicas)
    checkForPhantomInSyncReplicas(zkUtils, topic, partitionToBeReassigned, assignedReplicas)
    ensureNoUnderReplicatedPartitions(zkUtils, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")
    servers.foreach(_.shutdown())
  }

  @Test
  def testReassigningNonExistingPartition() {
    val topic = "test"
    // create brokers
    val servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // reassign partition 0
    val newReplicas = Seq(2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkUtils, Map(topicAndPartition -> newReplicas))
    assertFalse("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    val reassignedPartitions = zkUtils.getPartitionsBeingReassigned()
    assertFalse("Partition should not be reassigned", reassignedPartitions.contains(topicAndPartition))
    servers.foreach(_.shutdown())
  }

  @Test
  def testResumePartitionReassignmentThatWasCompleted() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1))
    val topic = "test"
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, expectedReplicaAssignment)
    // put the partition in the reassigned path as well
    // reassign partition 0
    val newReplicas = Seq(0, 1)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkUtils, Map(topicAndPartition -> newReplicas))
    reassignPartitionsCommand.reassignPartitions()
    // create brokers
    val servers = TestUtils.createBrokerConfigs(2, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))

    // wait until reassignment completes
    TestUtils.waitUntilTrue(() => !checkIfReassignPartitionPathExists(zkUtils),
                            "Partition reassignment should complete")
    val assignedReplicas = zkUtils.getReplicasForPartition(topic, partitionToBeReassigned)
    assertEquals("Partition should have been reassigned to 0, 1", newReplicas, assignedReplicas)
    checkForPhantomInSyncReplicas(zkUtils, topic, partitionToBeReassigned, assignedReplicas)
    // ensure that there are no under replicated partitions
    ensureNoUnderReplicatedPartitions(zkUtils, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")
    servers.foreach(_.shutdown())
  }

  @Test
  def testPreferredReplicaJsonData() {
    // write preferred replica json data to zk path
    val partitionsForPreferredReplicaElection = Set(TopicAndPartition("test", 1), TopicAndPartition("test2", 1))
    PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkUtils, partitionsForPreferredReplicaElection)
    // try to read it back and compare with what was written
    val preferredReplicaElectionZkData = zkUtils.readData(ZkUtils.PreferredReplicaLeaderElectionPath)._1
    val partitionsUndergoingPreferredReplicaElection =
      PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(preferredReplicaElectionZkData)
    assertEquals("Preferred replica election ser-de failed", partitionsForPreferredReplicaElection,
      partitionsUndergoingPreferredReplicaElection)
  }

  @Test
  def testBasicPreferredReplicaElection() {
    val expectedReplicaAssignment = Map(1  -> List(0, 1, 2))
    val topic = "test"
    val partition = 1
    val preferredReplica = 0
    // create brokers
    val brokerRack = Map(0 -> "rack0", 1 -> "rack1", 2 -> "rack2")
    val serverConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false, rackInfo = brokerRack).map(KafkaConfig.fromProps)
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, expectedReplicaAssignment)
    val servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    // broker 2 should be the leader since it was started first
    val currentLeader = TestUtils.waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partition, oldLeaderOpt = None).get
    // trigger preferred replica election
    val preferredReplicaElection = new PreferredReplicaLeaderElectionCommand(zkUtils, Set(TopicAndPartition(topic, partition)))
    preferredReplicaElection.moveLeaderToPreferredReplica()
    val newLeader = TestUtils.waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partition, oldLeaderOpt = Some(currentLeader)).get
    assertEquals("Preferred replica election failed", preferredReplica, newLeader)
    servers.foreach(_.shutdown())
  }

  @Test
  def testShutdownBroker() {
    val expectedReplicaAssignment = Map(1  -> List(0, 1, 2))
    val topic = "test"
    val partition = 1
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false).map(KafkaConfig.fromProps)
    val servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    // create the topic
    TestUtils.createTopic(zkUtils, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers)

    val controllerId = zkUtils.getController()
    val controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
    var partitionsRemaining = controller.shutdownBroker(2)
    var activeServers = servers.filter(s => s.config.brokerId != 2)
    try {
      // wait for the update metadata request to trickle to the brokers
      TestUtils.waitUntilTrue(() =>
        activeServers.forall(_.apis.metadataCache.getPartitionInfo(topic,partition).get.leaderIsrAndControllerEpoch.leaderAndIsr.isr.size != 3),
        "Topic test not created after timeout")
      assertEquals(0, partitionsRemaining.size)
      var partitionStateInfo = activeServers.head.apis.metadataCache.getPartitionInfo(topic,partition).get
      var leaderAfterShutdown = partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader
      assertEquals(0, leaderAfterShutdown)
      assertEquals(2, partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.isr.size)
      assertEquals(List(0,1), partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.isr)

      partitionsRemaining = controller.shutdownBroker(1)
      assertEquals(0, partitionsRemaining.size)
      activeServers = servers.filter(s => s.config.brokerId == 0)
      partitionStateInfo = activeServers.head.apis.metadataCache.getPartitionInfo(topic,partition).get
      leaderAfterShutdown = partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader
      assertEquals(0, leaderAfterShutdown)

      assertTrue(servers.forall(_.apis.metadataCache.getPartitionInfo(topic,partition).get.leaderIsrAndControllerEpoch.leaderAndIsr.leader == 0))
      partitionsRemaining = controller.shutdownBroker(0)
      assertEquals(1, partitionsRemaining.size)
      // leader doesn't change since all the replicas are shut down
      assertTrue(servers.forall(_.apis.metadataCache.getPartitionInfo(topic,partition).get.leaderIsrAndControllerEpoch.leaderAndIsr.leader == 0))
    }
    finally {
      servers.foreach(_.shutdown())
    }
  }

  /**
   * This test creates a topic with a few config overrides and checks that the configs are applied to the new topic
   * then changes the config and checks that the new values take effect.
   */
  @Test
  def testTopicConfigChange() {
    val partitions = 3
    val topic = "my-topic"
    val server = TestUtils.createServer(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))

    def makeConfig(messageSize: Int, retentionMs: Long, throttledLeaders: String, throttledFollowers: String) = {
      val props = new Properties()
      props.setProperty(LogConfig.MaxMessageBytesProp, messageSize.toString)
      props.setProperty(LogConfig.RetentionMsProp, retentionMs.toString)
      props.setProperty(LogConfig.LeaderReplicationThrottledReplicasProp, throttledLeaders)
      props.setProperty(LogConfig.FollowerReplicationThrottledReplicasProp, throttledFollowers)
      props
    }

    def checkConfig(messageSize: Int, retentionMs: Long, throttledLeaders: String, throttledFollowers: String, quotaManagerIsThrottled: Boolean) {
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
          checkList(log.get.config.LeaderReplicationThrottledReplicas, throttledLeaders)
          checkList(log.get.config.FollowerReplicationThrottledReplicas, throttledFollowers)
          assertEquals(quotaManagerIsThrottled, server.quotaManagers.leader.isThrottled(tp))
        }
      }
    }

    try {
      // create a topic with a few config overrides and check that they are applied
      val maxMessageSize = 1024
      val retentionMs = 1000 * 1000
      AdminUtils.createTopic(server.zkUtils, topic, partitions, 1, makeConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1"))

      //Standard topic configs will be propagated at topic creation time, but the quota manager will not have been updated.
      checkConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1", false)

      //Update dynamically and all properties should be applied
      AdminUtils.changeTopicConfig(server.zkUtils, topic, makeConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1"))

      checkConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1", true)

      // now double the config values for the topic and check that it is applied
      val newConfig = makeConfig(2 * maxMessageSize, 2 * retentionMs, "*", "*")
      AdminUtils.changeTopicConfig(server.zkUtils, topic, makeConfig(2 * maxMessageSize, 2 * retentionMs, "*", "*"))
      checkConfig(2 * maxMessageSize, 2 * retentionMs, "*", "*", quotaManagerIsThrottled = true)

      // Verify that the same config can be read from ZK
      val configInZk = AdminUtils.fetchEntityConfig(server.zkUtils, ConfigType.Topic, topic)
      assertEquals(newConfig, configInZk)

      //Now delete the config
      AdminUtils.changeTopicConfig(server.zkUtils, topic, new Properties)
      checkConfig(Defaults.MaxMessageSize, Defaults.RetentionMs, "", "", quotaManagerIsThrottled = false)

      //Add config back
      AdminUtils.changeTopicConfig(server.zkUtils, topic, makeConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1"))
      checkConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1", quotaManagerIsThrottled = true)

      //Now ensure updating to "" removes the throttled replica list also
      AdminUtils.changeTopicConfig(server.zkUtils, topic, propsWith((LogConfig.FollowerReplicationThrottledReplicasProp, ""), (LogConfig.LeaderReplicationThrottledReplicasProp, "")))
      checkConfig(Defaults.MaxMessageSize, Defaults.RetentionMs, "", "",  quotaManagerIsThrottled = false)

    } finally {
      server.shutdown()
      CoreUtils.delete(server.config.logDirs)
    }
  }

  @Test
  def shouldPropagateDynamicBrokerConfigs() {
    val brokerIds = Seq(0, 1, 2)
    val servers = createBrokerConfigs(3, zkConnect).map(fromProps).map(createServer(_))

    def checkConfig(limit: Long) {
      retry(10000) {
        for (server <- servers) {
          assertEquals("Leader Quota Manager was not updated", limit, server.quotaManagers.leader.upperBound)
          assertEquals("Follower Quota Manager was not updated", limit, server.quotaManagers.follower.upperBound)
        }
      }
    }

    try {
      val limit: Long = 1000000

      // Set the limit & check it is applied to the log
      changeBrokerConfig(zkUtils, brokerIds, propsWith(
        (LeaderReplicationThrottledRateProp, limit.toString),
        (FollowerReplicationThrottledRateProp, limit.toString)))
      checkConfig(limit)

      // Now double the config values for the topic and check that it is applied
      val newLimit = 2 * limit
      changeBrokerConfig(zkUtils, brokerIds,  propsWith(
        (LeaderReplicationThrottledRateProp, newLimit.toString),
        (FollowerReplicationThrottledRateProp, newLimit.toString)))
      checkConfig(newLimit)

      // Verify that the same config can be read from ZK
      for (brokerId <- brokerIds) {
        val configInZk = AdminUtils.fetchEntityConfig(servers(brokerId).zkUtils, ConfigType.Broker, brokerId.toString)
        assertEquals(newLimit, configInZk.getProperty(LeaderReplicationThrottledRateProp).toInt)
        assertEquals(newLimit, configInZk.getProperty(FollowerReplicationThrottledRateProp).toInt)
      }

      //Now delete the config
      changeBrokerConfig(servers(0).zkUtils, brokerIds, new Properties)
      checkConfig(DefaultReplicationThrottledRate)

    } finally {
      servers.foreach(_.shutdown())
      servers.foreach(server => CoreUtils.delete(server.config.logDirs))
    }
  }

  /**
   * This test simulates a client config change in ZK whose notification has been purged.
   * Basically, it asserts that notifications are bootstrapped from ZK
   */
  @Test
  def testBootstrapClientIdConfig() {
    val clientId = "my-client"
    val props = new Properties()
    props.setProperty("producer_byte_rate", "1000")
    props.setProperty("consumer_byte_rate", "2000")

    // Write config without notification to ZK.
    val configMap = Map[String, String] ("producer_byte_rate" -> "1000", "consumer_byte_rate" -> "2000")
    val map = Map("version" -> 1, "config" -> configMap)
    zkUtils.updatePersistentPath(ZkUtils.getEntityConfigPath(ConfigType.Client, clientId), Json.encode(map))

    val configInZk: Map[String, Properties] = AdminUtils.fetchAllEntityConfigs(zkUtils, ConfigType.Client)
    assertEquals("Must have 1 overriden client config", 1, configInZk.size)
    assertEquals(props, configInZk(clientId))

    // Test that the existing clientId overrides are read
    val server = TestUtils.createServer(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))
    try {
      assertEquals(new Quota(1000, true), server.apis.quotas.produce.quota("ANONYMOUS", clientId))
      assertEquals(new Quota(2000, true), server.apis.quotas.fetch.quota("ANONYMOUS", clientId))
    } finally {
      server.shutdown()
      CoreUtils.delete(server.config.logDirs)
    }
  }

  @Test
  def testGetBrokerMetadatas() {
    // broker 4 has no rack information
    val brokerList = 0 to 5
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 5 -> "rack3")
    val brokerMetadatas = toBrokerMetadata(rackInfo, brokersWithoutRack = brokerList.filterNot(rackInfo.keySet))
    TestUtils.createBrokersInZk(brokerMetadatas, zkUtils)

    val processedMetadatas1 = AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Disabled)
    assertEquals(brokerList, processedMetadatas1.map(_.id))
    assertEquals(List.fill(brokerList.size)(None), processedMetadatas1.map(_.rack))

    val processedMetadatas2 = AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Safe)
    assertEquals(brokerList, processedMetadatas2.map(_.id))
    assertEquals(List.fill(brokerList.size)(None), processedMetadatas2.map(_.rack))

    intercept[AdminOperationException] {
      AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Enforced)
    }

    val partialList = List(0, 1, 2, 3, 5)
    val processedMetadatas3 = AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Enforced, Some(partialList))
    assertEquals(partialList, processedMetadatas3.map(_.id))
    assertEquals(partialList.map(rackInfo), processedMetadatas3.flatMap(_.rack))

    val numPartitions = 3
    AdminUtils.createTopic(zkUtils, "foo", numPartitions, 2, rackAwareMode = RackAwareMode.Safe)
    val assignment = zkUtils.getReplicaAssignmentForTopics(Seq("foo"))
    assertEquals(numPartitions, assignment.size)
  }
}
