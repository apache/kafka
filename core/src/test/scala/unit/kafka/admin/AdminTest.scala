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
import kafka.zk.ZooKeeperTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{Logging, ZkUtils, TestUtils}
import kafka.common.{TopicExistsException, ErrorMapping, TopicAndPartition}


class AdminTest extends JUnit3Suite with ZooKeeperTestHarness with Logging {

  @Test
  def testReplicaAssignment() {
    val brokerList = List(0, 1, 2, 3, 4)

    // test 0 replication factor
    try {
      AdminUtils.assignReplicasToBrokers(brokerList, 10, 0)
      fail("shouldn't allow replication factor 0")
    }
    catch {
      case e: AdministrationException => // this is good
      case e2 => throw e2
    }

    // test wrong replication factor
    try {
      AdminUtils.assignReplicasToBrokers(brokerList, 10, 6)
      fail("shouldn't allow replication factor larger than # of brokers")
    }
    catch {
      case e: AdministrationException => // this is good
      case e2 => throw e2
    }

    // correct assignment
    {
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
        9 -> List(4, 1, 2)
      )

      val actualAssignment = AdminUtils.assignReplicasToBrokers(brokerList, 10, 3, 0)
      val e = (expectedAssignment.toList == actualAssignment.toList)
      assertTrue(expectedAssignment.toList == actualAssignment.toList)
    }
  }

  @Test
  def testManualReplicaAssignment() {
    val brokerList = Set(0, 1, 2, 3, 4)

    // duplicated brokers
    try {
      val replicationAssignmentStr = "0,0,1:1,2,3"
      CreateTopicCommand.getManualReplicaAssignment(replicationAssignmentStr, brokerList)
      fail("replication assginment shouldn't have duplicated brokers")
    }
    catch {
      case e: AdministrationException => // this is good
      case e2 => throw e2
    }

    // non-exist brokers
    try {
      val replicationAssignmentStr = "0,1,2:1,2,7"
      CreateTopicCommand.getManualReplicaAssignment(replicationAssignmentStr, brokerList)
      fail("replication assginment shouldn't contain non-exist brokers")
    }
    catch {
      case e: AdministrationException => // this is good
      case e2 => throw e2
    }

    // inconsistent replication factor
    try {
      val replicationAssignmentStr = "0,1,2:1,2"
      CreateTopicCommand.getManualReplicaAssignment(replicationAssignmentStr, brokerList)
      fail("all partitions should have the same replication factor")
    }
    catch {
      case e: AdministrationException => // this is good
      case e2 => throw e2
    }

    // good assignment
    {
      val replicationAssignmentStr = "0:1:2,1:2:3"
      val expectedReplicationAssignment = Map(
        0 -> List(0, 1, 2),
        1 -> List(1, 2, 3)
      )
      val actualReplicationAssignment = CreateTopicCommand.getManualReplicaAssignment(replicationAssignmentStr, brokerList)
      assertEquals(expectedReplicationAssignment.size, actualReplicationAssignment.size)
      for( (part, replicas) <- expectedReplicationAssignment ) {
        assertEquals(replicas, actualReplicationAssignment(part))
      }
    }
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
    AdminUtils.createTopicPartitionAssignmentPathInZK(topic, expectedReplicaAssignment, zkClient)
    // create leaders for all partitions
    TestUtils.makeLeaderForPartition(zkClient, topic, leaderForPartitionMap, 1)
    val actualReplicaAssignment = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient).partitionsMetadata.map(p => p.replicas)
    val actualReplicaList = actualReplicaAssignment.map(r => r.map(b => b.id).toList).toList
    assertEquals(expectedReplicaAssignment.size, actualReplicaList.size)
    for(i <- 0 until actualReplicaList.size)
      assertEquals(expectedReplicaAssignment.get(i).get, actualReplicaList(i))

    try {
      AdminUtils.createTopicPartitionAssignmentPathInZK(topic, expectedReplicaAssignment, zkClient)
      fail("shouldn't be able to create a topic already exists")
    } catch {
      case e: TopicExistsException => // this is good
      case e2 => throw e2
    }
  }

  @Test
  def testGetTopicMetadata() {
    val expectedReplicaAssignment = Map(
      0 -> List(0, 1, 2),
      1 -> List(1, 2, 3)
    )
    val leaderForPartitionMap = Map(
      0 -> 0,
      1 -> 1
    )
    val topic = "auto-topic"
    TestUtils.createBrokersInZk(zkClient, List(0, 1, 2, 3))
    AdminUtils.createTopicPartitionAssignmentPathInZK(topic, expectedReplicaAssignment, zkClient)
    // create leaders for all partitions
    TestUtils.makeLeaderForPartition(zkClient, topic, leaderForPartitionMap, 1)

    val newTopicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient)
    newTopicMetadata.errorCode match {
      case ErrorMapping.UnknownTopicOrPartitionCode =>
        fail("Topic " + topic + " should've been automatically created")
      case _ =>
        assertEquals(topic, newTopicMetadata.topic)
        assertNotNull("partition metadata list cannot be null", newTopicMetadata.partitionsMetadata)
        assertEquals("partition metadata list length should be 2", 2, newTopicMetadata.partitionsMetadata.size)
        val actualReplicaAssignment = newTopicMetadata.partitionsMetadata.map(p => p.replicas)
        val actualReplicaList = actualReplicaAssignment.map(r => r.map(b => b.id).toList).toList
        assertEquals(expectedReplicaAssignment.size, actualReplicaList.size)
        for(i <- 0 until actualReplicaList.size) {
          assertEquals(expectedReplicaAssignment(i), actualReplicaList(i))
        }
    }
  }

  @Test
  def testPartitionReassignmentWithLeaderInNewReplicas() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    // create brokers
    val servers = TestUtils.createBrokerConfigs(4).map(b => TestUtils.createServer(new KafkaConfig(b)))
    // create the topic
    AdminUtils.createTopicPartitionAssignmentPathInZK(topic, expectedReplicaAssignment, zkClient)
    // reassign partition 0
    val newReplicas = Seq(0, 2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment attempt failed for [test, 0]", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
      val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas);
      CheckReassignmentStatus.checkIfPartitionReassignmentSucceeded(zkClient, topicAndPartition, newReplicas,
      Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentCompleted;
    }, 1000)
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partitionToBeReassigned)
    assertEquals("Partition should have been reassigned to 0, 2, 3", newReplicas, assignedReplicas)
    servers.foreach(_.shutdown())
  }

  @Test
  def testPartitionReassignmentWithLeaderNotInNewReplicas() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    // create brokers
    val servers = TestUtils.createBrokerConfigs(4).map(b => TestUtils.createServer(new KafkaConfig(b)))
    // create the topic
    AdminUtils.createTopicPartitionAssignmentPathInZK(topic, expectedReplicaAssignment, zkClient)
    // reassign partition 0
    val newReplicas = Seq(1, 2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
      val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas);
      CheckReassignmentStatus.checkIfPartitionReassignmentSucceeded(zkClient, topicAndPartition, newReplicas,
        Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentCompleted;
    }, 1000)
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partitionToBeReassigned)
    assertEquals("Partition should have been reassigned to 0, 2, 3", newReplicas, assignedReplicas)
    // leader should be 2
    servers.foreach(_.shutdown())
  }

  @Test
  def testPartitionReassignmentNonOverlappingReplicas() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1))
    val topic = "test"
    // create brokers
    val servers = TestUtils.createBrokerConfigs(4).map(b => TestUtils.createServer(new KafkaConfig(b)))
    // create the topic
    AdminUtils.createTopicPartitionAssignmentPathInZK(topic, expectedReplicaAssignment, zkClient)
    // reassign partition 0
    val newReplicas = Seq(2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
      val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas);
      CheckReassignmentStatus.checkIfPartitionReassignmentSucceeded(zkClient, topicAndPartition, newReplicas,
        Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentCompleted;
    }, 1000)
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partitionToBeReassigned)
    assertEquals("Partition should have been reassigned to 2, 3", newReplicas, assignedReplicas)
    // leader should be 2
    servers.foreach(_.shutdown())
  }

  @Test
  def testReassigningNonExistingPartition() {
    val topic = "test"
    // create brokers
    val servers = TestUtils.createBrokerConfigs(4).map(b => TestUtils.createServer(new KafkaConfig(b)))
    // reassign partition 0
    val newReplicas = Seq(2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    val reassignedPartitions = ZkUtils.getPartitionsBeingReassigned(zkClient)
    assertFalse("Partition should not be reassigned", reassignedPartitions.contains(topicAndPartition))
    // leader should be 2
    servers.foreach(_.shutdown())
  }

  @Test
  def testResumePartitionReassignmentThatWasCompleted() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1))
    val topic = "test"
    // create the topic
    AdminUtils.createTopicPartitionAssignmentPathInZK(topic, expectedReplicaAssignment, zkClient)
    // put the partition in the reassigned path as well
    // reassign partition 0
    val newReplicas = Seq(0, 1)
    val partitionToBeReassigned = 0
    val topicAndPartition = TopicAndPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    reassignPartitionsCommand.reassignPartitions
    // create brokers
    val servers = TestUtils.createBrokerConfigs(2).map(b => TestUtils.createServer(new KafkaConfig(b)))
    TestUtils.waitUntilTrue(checkIfReassignPartitionPathExists, 1000)
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partitionToBeReassigned)
    assertEquals("Partition should have been reassigned to 0, 1", newReplicas, assignedReplicas)
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
      PreferredReplicaLeaderElectionCommand.parsePreferredReplicaJsonData(preferredReplicaElectionZkData)
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
    val serverConfigs = TestUtils.createBrokerConfigs(3).map(new KafkaConfig(_))
    // create the topic
    AdminUtils.createTopicPartitionAssignmentPathInZK(topic, expectedReplicaAssignment, zkClient)
    val servers = serverConfigs.reverse.map(s => TestUtils.createServer(s))
    // broker 2 should be the leader since it was started first
    val currentLeader = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partition, 1000, None).get
    // trigger preferred replica election
    val preferredReplicaElection = new PreferredReplicaLeaderElectionCommand(zkClient, Set(TopicAndPartition(topic, partition)))
    preferredReplicaElection.moveLeaderToPreferredReplica()
    val newLeader = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partition, 1000, Some(currentLeader)).get
    assertEquals("Preferred replica election failed", preferredReplica, newLeader)
    servers.foreach(_.shutdown())
  }

  @Test
  def testShutdownBroker() {
    info("inside testShutdownBroker")
    val expectedReplicaAssignment = Map(1  -> List(0, 1, 2))
    val topic = "test"
    val partition = 1
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(3).map(new KafkaConfig(_))
    // create the topic
    AdminUtils.createTopicPartitionAssignmentPathInZK(topic, expectedReplicaAssignment, zkClient)
    val servers = serverConfigs.reverse.map(s => TestUtils.createServer(s))

    // broker 2 should be the leader since it was started first
    var leaderBeforeShutdown = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partition, 1000, None).get
    var controllerId = ZkUtils.getController(zkClient)
    var controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
    var partitionsRemaining = controller.shutdownBroker(2)
    try {
      assertEquals(0, partitionsRemaining)
      var topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient)
      var leaderAfterShutdown = topicMetadata.partitionsMetadata.head.leader.get.id
      assertTrue(leaderAfterShutdown != leaderBeforeShutdown)

      leaderBeforeShutdown = leaderAfterShutdown
      controllerId = ZkUtils.getController(zkClient)
      controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
      partitionsRemaining = controller.shutdownBroker(1)
      assertEquals(0, partitionsRemaining)
      topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient)
      leaderAfterShutdown = topicMetadata.partitionsMetadata.head.leader.get.id
      assertTrue(leaderAfterShutdown != leaderBeforeShutdown)
      assertEquals(1, controller.controllerContext.allLeaders(TopicAndPartition("test", 1)).leaderAndIsr.isr.size)

      leaderBeforeShutdown = leaderAfterShutdown
      controllerId = ZkUtils.getController(zkClient)
      controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
      partitionsRemaining = controller.shutdownBroker(0)
      assertEquals(1, partitionsRemaining)
      topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkClient)
      leaderAfterShutdown = topicMetadata.partitionsMetadata.head.leader.get.id
      assertTrue(leaderAfterShutdown == leaderBeforeShutdown)
      assertEquals(1, controller.controllerContext.allLeaders(TopicAndPartition("test", 1)).leaderAndIsr.isr.size)
    } finally {
      servers.foreach(_.shutdown())
    }
  }

  private def checkIfReassignPartitionPathExists(): Boolean = {
    ZkUtils.pathExists(zkClient, ZkUtils.ReassignPartitionsPath)
  }
}
