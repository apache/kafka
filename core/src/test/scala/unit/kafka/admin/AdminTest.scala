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

import junit.framework.Assert._
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import java.util.Properties
import kafka.utils._
import kafka.log._
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.{Logging, ZkUtils, TestUtils}
import kafka.common.{TopicExistsException, TopicAndPartition}
import kafka.server.{KafkaServer, KafkaConfig}
import java.io.File
import TestUtils._


class AdminTest extends JUnit3Suite with ZooKeeperTestHarness with Logging {

  @Test
  def testReplicaAssignment() {
    val brokerList = List(0, 1, 2, 3, 4)

    // test 0 replication factor
    intercept[AdminOperationException] {
      AdminUtils.assignReplicasToBrokers(brokerList, 10, 0)
    }

    // test wrong replication factor
    intercept[AdminOperationException] {
      AdminUtils.assignReplicasToBrokers(brokerList, 10, 6)
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

    val actualAssignment = AdminUtils.assignReplicasToBrokers(brokerList, 10, 3, 0)
    val e = (expectedAssignment.toList == actualAssignment.toList)
    assertTrue(expectedAssignment.toList == actualAssignment.toList)
  }

  @Test
  def testManualReplicaAssignment() {
    val brokers = List(0, 1, 2, 3, 4)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // duplicate brokers
    intercept[IllegalArgumentException] {
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, "test", Map(0->Seq(0,0)))
    }

    // inconsistent replication factor
    intercept[IllegalArgumentException] {
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, "test", Map(0->Seq(0,1), 1->Seq(0)))
    }

    // good assignment
    val assignment = Map(0 -> List(0, 1, 2),
                         1 -> List(1, 2, 3))
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, "test", assignment)
    val found = ZkUtils.getPartitionAssignmentForTopics(zkClient, Seq("test"))
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
    val leaderForPartitionMap = Map(
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
    TestUtils.createBrokersInZk(zkClient, List(0, 1, 2, 3, 4))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // create leaders for all partitions
    TestUtils.makeLeaderForPartition(zkClient, topic, leaderForPartitionMap, 1)
    val actualReplicaList = leaderForPartitionMap.keys.toArray.map(p => (p -> ZkUtils.getReplicasForPartition(zkClient, topic, p))).toMap
    assertEquals(expectedReplicaAssignment.size, actualReplicaList.size)
    for(i <- 0 until actualReplicaList.size)
      assertEquals(expectedReplicaAssignment.get(i).get, actualReplicaList(i))

    intercept[TopicExistsException] {
      // shouldn't be able to create a topic that already exists
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
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
    val servers = TestUtils.createBrokerConfigs(4, false).map(b => TestUtils.createServer(new KafkaConfig(b)))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // reassign partition 0
    val newReplicas = Seq(0, 2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment attempt failed for [test, 0]", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
        val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas);
        ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkClient, topicAndPartition, newReplicas,
        Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentCompleted;
      },
      "Partition reassignment should complete")
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partitionToBeReassigned)
    // in sync replicas should not have any replica that is not in the new assigned replicas
    checkForPhantomInSyncReplicas(zkClient, topic, partitionToBeReassigned, assignedReplicas)
    assertEquals("Partition should have been reassigned to 0, 2, 3", newReplicas, assignedReplicas)
    ensureNoUnderReplicatedPartitions(zkClient, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")
    servers.foreach(_.shutdown())
  }

  @Test
  def testPartitionReassignmentWithLeaderNotInNewReplicas() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    // create brokers
    val servers = TestUtils.createBrokerConfigs(4, false).map(b => TestUtils.createServer(new KafkaConfig(b)))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // reassign partition 0
    val newReplicas = Seq(1, 2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
        val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas);
        ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkClient, topicAndPartition, newReplicas,
          Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentCompleted;
      },
      "Partition reassignment should complete")
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partitionToBeReassigned)
    assertEquals("Partition should have been reassigned to 0, 2, 3", newReplicas, assignedReplicas)
    checkForPhantomInSyncReplicas(zkClient, topic, partitionToBeReassigned, assignedReplicas)
    ensureNoUnderReplicatedPartitions(zkClient, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")

    servers.foreach(_.shutdown())
  }

  @Test
  def testPartitionReassignmentNonOverlappingReplicas() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1))
    val topic = "test"
    // create brokers
    val servers = TestUtils.createBrokerConfigs(4, false).map(b => TestUtils.createServer(new KafkaConfig(b)))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // reassign partition 0
    val newReplicas = Seq(2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
        val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas);
        ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkClient, topicAndPartition, newReplicas,
          Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentCompleted;
      },
      "Partition reassignment should complete")
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partitionToBeReassigned)
    assertEquals("Partition should have been reassigned to 2, 3", newReplicas, assignedReplicas)
    checkForPhantomInSyncReplicas(zkClient, topic, partitionToBeReassigned, assignedReplicas)
    ensureNoUnderReplicatedPartitions(zkClient, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")
    servers.foreach(_.shutdown())
  }

  @Test
  def testReassigningNonExistingPartition() {
    val topic = "test"
    // create brokers
    val servers = TestUtils.createBrokerConfigs(4, false).map(b => TestUtils.createServer(new KafkaConfig(b)))
    // reassign partition 0
    val newReplicas = Seq(2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    val reassignedPartitions = ZkUtils.getPartitionsBeingReassigned(zkClient)
    assertFalse("Partition should not be reassigned", reassignedPartitions.contains(topicAndPartition))
    servers.foreach(_.shutdown())
  }

  @Test
  def testResumePartitionReassignmentThatWasCompleted() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1))
    val topic = "test"
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // put the partition in the reassigned path as well
    // reassign partition 0
    val newReplicas = Seq(0, 1)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    reassignPartitionsCommand.reassignPartitions
    // create brokers
    val servers = TestUtils.createBrokerConfigs(2, false).map(b => TestUtils.createServer(new KafkaConfig(b)))

    // wait until reassignment completes
    TestUtils.waitUntilTrue(() => !checkIfReassignPartitionPathExists(zkClient),
                            "Partition reassignment should complete")
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partitionToBeReassigned)
    assertEquals("Partition should have been reassigned to 0, 1", newReplicas, assignedReplicas)
    checkForPhantomInSyncReplicas(zkClient, topic, partitionToBeReassigned, assignedReplicas)
    // ensure that there are no under replicated partitions
    ensureNoUnderReplicatedPartitions(zkClient, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")
    servers.foreach(_.shutdown())
  }

  @Test
  def testPreferredReplicaJsonData() {
    // write preferred replica json data to zk path
    val partitionsForPreferredReplicaElection = Set(TopicAndPartition("test", 1), TopicAndPartition("test2", 1))
    PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkClient, partitionsForPreferredReplicaElection)
    // try to read it back and compare with what was written
    val preferredReplicaElectionZkData = ZkUtils.readData(zkClient,
        ZkUtils.PreferredReplicaLeaderElectionPath)._1
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
    val serverConfigs = TestUtils.createBrokerConfigs(3, false).map(new KafkaConfig(_))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    val servers = serverConfigs.reverse.map(s => TestUtils.createServer(s))
    // broker 2 should be the leader since it was started first
    val currentLeader = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partition, oldLeaderOpt = None).get
    // trigger preferred replica election
    val preferredReplicaElection = new PreferredReplicaLeaderElectionCommand(zkClient, Set(TopicAndPartition(topic, partition)))
    preferredReplicaElection.moveLeaderToPreferredReplica()
    val newLeader = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partition, oldLeaderOpt = Some(currentLeader)).get
    assertEquals("Preferred replica election failed", preferredReplica, newLeader)
    servers.foreach(_.shutdown())
  }

  @Test
  def testShutdownBroker() {
    val expectedReplicaAssignment = Map(1  -> List(0, 1, 2))
    val topic = "test"
    val partition = 1
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(3, false).map(new KafkaConfig(_))
    val servers = serverConfigs.reverse.map(s => TestUtils.createServer(s))
    // create the topic
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers)

    val controllerId = ZkUtils.getController(zkClient)
    val controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
    var partitionsRemaining = controller.shutdownBroker(2)
    var activeServers = servers.filter(s => s.config.brokerId != 2)
    try {
      // wait for the update metadata request to trickle to the brokers
      TestUtils.waitUntilTrue(() =>
        activeServers.foldLeft(true)(_ && _.apis.metadataCache.getPartitionInfo(topic,partition).get.leaderIsrAndControllerEpoch.leaderAndIsr.isr.size != 3),
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

      assertTrue(servers.foldLeft(true)(_ && _.apis.metadataCache.getPartitionInfo(topic,partition).get.leaderIsrAndControllerEpoch.leaderAndIsr.leader == 0))
      partitionsRemaining = controller.shutdownBroker(0)
      assertEquals(1, partitionsRemaining.size)
      // leader doesn't change since all the replicas are shut down
      assertTrue(servers.foldLeft(true)(_ && _.apis.metadataCache.getPartitionInfo(topic,partition).get.leaderIsrAndControllerEpoch.leaderAndIsr.leader == 0))
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
    val server = TestUtils.createServer(new KafkaConfig(TestUtils.createBrokerConfig(0)))

    def makeConfig(messageSize: Int, retentionMs: Long) = {
      var props = new Properties()
      props.setProperty(LogConfig.MaxMessageBytesProp, messageSize.toString)
      props.setProperty(LogConfig.RententionMsProp, retentionMs.toString)
      props
    }

    def checkConfig(messageSize: Int, retentionMs: Long) {
      TestUtils.retry(10000) {
        for(part <- 0 until partitions) {
          val logOpt = server.logManager.getLog(TopicAndPartition(topic, part))
          assertTrue(logOpt.isDefined)
          assertEquals(retentionMs, logOpt.get.config.retentionMs)
          assertEquals(messageSize, logOpt.get.config.maxMessageSize)
        }
      }
    }

    try {
      // create a topic with a few config overrides and check that they are applied
      val maxMessageSize = 1024
      val retentionMs = 1000*1000
      AdminUtils.createTopic(server.zkClient, topic, partitions, 1, makeConfig(maxMessageSize, retentionMs))
      checkConfig(maxMessageSize, retentionMs)

      // now double the config values for the topic and check that it is applied
      AdminUtils.changeTopicConfig(server.zkClient, topic, makeConfig(2*maxMessageSize, 2 * retentionMs))
      checkConfig(2*maxMessageSize, 2 * retentionMs)
    } finally {
      server.shutdown()
      server.config.logDirs.map(Utils.rm(_))
    }
  }

}
