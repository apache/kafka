/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import kafka.log.Log
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.TestUtils
import kafka.utils.ZkUtils._
import kafka.server.{KafkaServer, KafkaConfig}
import org.junit.Assert._
import org.junit.Test
import java.util.Properties
import kafka.common.{TopicAlreadyMarkedForDeletionException, TopicAndPartition}

class DeleteTopicTest extends ZooKeeperTestHarness {

  @Test
  def testDeleteTopicWithAllAliveReplicas() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkUtils, topic)
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testResumeDeleteTopicWithRecoveredFollower() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // shut down one follower replica
    val leaderIdOpt = zkUtils.getLeaderForPartition(topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()
    // start topic deletion
    AdminUtils.deleteTopic(zkUtils, topic)
    // check if all replicas but the one that is shut down has deleted the log
    TestUtils.waitUntilTrue(() =>
      servers.filter(s => s.config.brokerId != follower.config.brokerId)
        .forall(_.getLogManager().getLog(topicAndPartition).isEmpty), "Replicas 0,1 have not deleted log.")
    // ensure topic deletion is halted
    TestUtils.waitUntilTrue(() => zkUtils.pathExists(getDeleteTopicPath(topic)),
      "Admin path /admin/delete_topic/test path deleted even when a follower replica is down")
    // restart follower replica
    follower.startup()
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testResumeDeleteTopicOnControllerFailover() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    val controllerId = zkUtils.getController()
    val controller = servers.filter(s => s.config.brokerId == controllerId).head
    val leaderIdOpt = zkUtils.getLeaderForPartition(topic, 0)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get && s.config.brokerId != controllerId).last
    follower.shutdown()

    // start topic deletion
    AdminUtils.deleteTopic(zkUtils, topic)
    // shut down the controller to trigger controller failover during delete topic
    controller.shutdown()

    // ensure topic deletion is halted
    TestUtils.waitUntilTrue(() => zkUtils.pathExists(getDeleteTopicPath(topic)),
      "Admin path /admin/delete_topic/test path deleted even when a replica is down")

    controller.startup()
    follower.startup()

    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testPartitionReassignmentDuringDeleteTopic() {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    val brokerConfigs = TestUtils.createBrokerConfigs(4, zkConnect, false)
    brokerConfigs.foreach(p => p.setProperty("delete.topic.enable", "true"))
    // create brokers
    val allServers = brokerConfigs.map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    val servers = allServers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, expectedReplicaAssignment)
    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(() => servers.forall(_.getLogManager().getLog(topicAndPartition).isDefined),
      "Replicas for topic test not created.")
    val leaderIdOpt = zkUtils.getLeaderForPartition(topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()
    // start topic deletion
    AdminUtils.deleteTopic(zkUtils, topic)
    // start partition reassignment at the same time right after delete topic. In this case, reassignment will fail since
    // the topic is being deleted
    // reassign partition 0
    val oldAssignedReplicas = zkUtils.getReplicasForPartition(topic, 0)
    val newReplicas = Seq(1, 2, 3)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkUtils, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment should fail for [test,0]", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
      val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned().mapValues(_.newReplicas);
      ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkUtils, topicAndPartition, newReplicas,
        Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentFailed;
    }, "Partition reassignment shouldn't complete.")
    val controllerId = zkUtils.getController()
    val controller = servers.filter(s => s.config.brokerId == controllerId).head
    assertFalse("Partition reassignment should fail",
      controller.kafkaController.controllerContext.partitionsBeingReassigned.contains(topicAndPartition))
    val assignedReplicas = zkUtils.getReplicasForPartition(topic, 0)
    assertEquals("Partition should not be reassigned to 0, 1, 2", oldAssignedReplicas, assignedReplicas)
    follower.startup()
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    allServers.foreach(_.shutdown())
  }

  @Test
  def testDeleteTopicDuringAddPartition() {
    val topic = "test"
    val servers = createTestTopicAndCluster(topic)
    val leaderIdOpt = zkUtils.getLeaderForPartition(topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    val newPartition = TopicAndPartition(topic, 1)
    follower.shutdown()
    // add partitions to topic
    AdminUtils.addPartitions(zkUtils, topic, 2, "0:1:2,0:1:2", false)
    // start topic deletion
    AdminUtils.deleteTopic(zkUtils, topic)
    follower.startup()
    // test if topic deletion is resumed
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    // verify that new partition doesn't exist on any broker either
    TestUtils.waitUntilTrue(() =>
      servers.forall(_.getLogManager().getLog(newPartition).isEmpty),
      "Replica logs not for new partition [test,1] not deleted after delete topic is complete.")
    servers.foreach(_.shutdown())
  }

  @Test
  def testAddPartitionDuringDeleteTopic() {
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkUtils, topic)
    // add partitions to topic
    val newPartition = TopicAndPartition(topic, 1)
    AdminUtils.addPartitions(zkUtils, topic, 2, "0:1:2,0:1:2")
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    // verify that new partition doesn't exist on any broker either
    assertTrue("Replica logs not deleted after delete topic is complete",
      servers.forall(_.getLogManager().getLog(newPartition).isEmpty))
    servers.foreach(_.shutdown())
  }

  @Test
  def testRecreateTopicAfterDeletion() {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkUtils, topic)
    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    // re-create topic on same replicas
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, expectedReplicaAssignment)
    // wait until leader is elected
    val leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 0, 1000)
    assertTrue("New leader should be elected after re-creating topic test", leaderIdOpt.isDefined)
    // check if all replica logs are created
    TestUtils.waitUntilTrue(() => servers.forall(_.getLogManager().getLog(topicAndPartition).isDefined),
      "Replicas for topic test not created.")
    servers.foreach(_.shutdown())
  }

  @Test
  def testDeleteNonExistingTopic() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkUtils, "test2")
    // verify delete topic path for test2 is removed from zookeeper
    TestUtils.verifyTopicDeletion(zkUtils, "test2", 1, servers)
    // verify that topic test is untouched
    TestUtils.waitUntilTrue(() => servers.forall(_.getLogManager().getLog(topicAndPartition).isDefined),
      "Replicas for topic test not created")
    // test the topic path exists
    assertTrue("Topic test mistakenly deleted", zkUtils.pathExists(getTopicPath(topic)))
    // topic test should have a leader
    val leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkUtils, topic, 0, 1000)
    assertTrue("Leader should exist for topic test", leaderIdOpt.isDefined)
    servers.foreach(_.shutdown())
  }

  @Test
  def testDeleteTopicWithCleaner() {
    val topicName = "test"
    val topicAndPartition = TopicAndPartition(topicName, 0)
    val topic = topicAndPartition.topic

    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false)
    brokerConfigs(0).setProperty("delete.topic.enable", "true")
    brokerConfigs(0).setProperty("log.cleaner.enable","true")
    brokerConfigs(0).setProperty("log.cleanup.policy","compact")
    brokerConfigs(0).setProperty("log.segment.bytes","100")
    brokerConfigs(0).setProperty("log.segment.delete.delay.ms","1000")
    brokerConfigs(0).setProperty("log.cleaner.dedupe.buffer.size","1048577")

    val servers = createTestTopicAndCluster(topic,brokerConfigs)

    // for simplicity, we are validating cleaner offsets on a single broker
    val server = servers(0)
    val log = server.logManager.getLog(topicAndPartition).get

    // write to the topic to activate cleaner
    writeDups(numKeys = 100, numDups = 3,log)

    // wait for cleaner to clean
   server.logManager.cleaner.awaitCleaned(topicName, 0, 0)

    // delete topic
    AdminUtils.deleteTopic(zkUtils, "test")
    TestUtils.verifyTopicDeletion(zkUtils, "test", 1, servers)

    servers.foreach(_.shutdown())
  }

  @Test
  def testDeleteTopicAlreadyMarkedAsDeleted() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)

    try {
      // start topic deletion
      AdminUtils.deleteTopic(zkUtils, topic)
      // try to delete topic marked as deleted
      AdminUtils.deleteTopic(zkUtils, topic)
      fail("Expected TopicAlreadyMarkedForDeletionException")
    }
    catch {
      case e: TopicAlreadyMarkedForDeletionException => // expected exception
    }

    TestUtils.verifyTopicDeletion(zkUtils, topic, 1, servers)
    servers.foreach(_.shutdown())
  }

  private def createTestTopicAndCluster(topic: String): Seq[KafkaServer] = {

    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false)
    brokerConfigs.foreach(p => p.setProperty("delete.topic.enable", "true")
    )
    createTestTopicAndCluster(topic,brokerConfigs)
  }

  private def createTestTopicAndCluster(topic: String, brokerConfigs: Seq[Properties]): Seq[KafkaServer] = {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topicAndPartition = TopicAndPartition(topic, 0)
    // create brokers
    val servers = brokerConfigs.map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, expectedReplicaAssignment)
    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(() => servers.forall(_.getLogManager().getLog(topicAndPartition).isDefined),
      "Replicas for topic test not created")
    servers
  }

  private def writeDups(numKeys: Int, numDups: Int, log: Log): Seq[(Int, Int)] = {
    var counter = 0
    for(dup <- 0 until numDups; key <- 0 until numKeys) yield {
      val count = counter
      log.append(TestUtils.singleMessageSet(payload = counter.toString.getBytes, key = key.toString.getBytes), assignOffsets = true)
      counter += 1
      (key, count)
    }
  }
}
