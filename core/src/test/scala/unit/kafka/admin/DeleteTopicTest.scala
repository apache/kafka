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

import java.io.File

import kafka.log.Log
import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import junit.framework.Assert._
import kafka.utils.{ZkUtils, TestUtils}
import kafka.server.{OffsetCheckpoint, KafkaServer, KafkaConfig}
import org.junit.Test
import kafka.common._
import kafka.producer.{ProducerConfig, Producer}
import java.util.Properties
import kafka.api._
import kafka.consumer.SimpleConsumer
import kafka.producer.KeyedMessage
import kafka.common.TopicAndPartition
import kafka.api.PartitionOffsetRequestInfo

class DeleteTopicTest extends JUnit3Suite with ZooKeeperTestHarness {

  @Test
  def testDeleteTopicWithAllAliveReplicas() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    verifyTopicDeletion(topic, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testResumeDeleteTopicWithRecoveredFollower() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // shut down one follower replica
    val leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // check if all replicas but the one that is shut down has deleted the log
    TestUtils.waitUntilTrue(() =>
      servers.filter(s => s.config.brokerId != follower.config.brokerId)
        .foldLeft(true)((res, server) => res && server.getLogManager().getLog(topicAndPartition).isEmpty), "Replicas 0,1 have not deleted log.")
    // ensure topic deletion is halted
    TestUtils.waitUntilTrue(() => ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic)),
      "Admin path /admin/delete_topic/test path deleted even when a follower replica is down")
    // restart follower replica
    follower.startup()
    verifyTopicDeletion(topic, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testResumeDeleteTopicOnControllerFailover() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    val controllerId = ZkUtils.getController(zkClient)
    val controller = servers.filter(s => s.config.brokerId == controllerId).head
    val leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get && s.config.brokerId != controllerId).last
    follower.shutdown()

    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // shut down the controller to trigger controller failover during delete topic
    controller.shutdown()

    // ensure topic deletion is halted
    TestUtils.waitUntilTrue(() => ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic)),
      "Admin path /admin/delete_topic/test path deleted even when a replica is down")

    controller.startup()
    follower.startup()

    verifyTopicDeletion(topic, servers)
    servers.foreach(_.shutdown())
  }

  @Test
  def testPartitionReassignmentDuringDeleteTopic() {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    val brokerConfigs = TestUtils.createBrokerConfigs(4, false)
    brokerConfigs.foreach(p => p.setProperty("delete.topic.enable", "true"))
    // create brokers
    val allServers = brokerConfigs.map(b => TestUtils.createServer(new KafkaConfig(b)))
    val servers = allServers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) =>
      res && server.getLogManager().getLog(topicAndPartition).isDefined),
      "Replicas for topic test not created.")
    val leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // start partition reassignment at the same time right after delete topic. In this case, reassignment will fail since
    // the topic is being deleted
    // reassign partition 0
    val oldAssignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, 0)
    val newReplicas = Seq(1, 2, 3)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, Map(topicAndPartition -> newReplicas))
    assertTrue("Partition reassignment should fail for [test,0]", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
      val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas);
      ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkClient, topicAndPartition, newReplicas,
        Map(topicAndPartition -> newReplicas), partitionsBeingReassigned) == ReassignmentFailed;
    }, "Partition reassignment shouldn't complete.")
    val controllerId = ZkUtils.getController(zkClient)
    val controller = servers.filter(s => s.config.brokerId == controllerId).head
    assertFalse("Partition reassignment should fail",
      controller.kafkaController.controllerContext.partitionsBeingReassigned.contains(topicAndPartition))
    val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, 0)
    assertEquals("Partition should not be reassigned to 0, 1, 2", oldAssignedReplicas, assignedReplicas)
    follower.startup()
    verifyTopicDeletion(topic, servers)
    allServers.foreach(_.shutdown())
  }

  @Test
  def testDeleteTopicDuringAddPartition() {
    val topic = "test"
    val servers = createTestTopicAndCluster(topic)
    val leaderIdOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    val newPartition = TopicAndPartition(topic, 1)
    follower.shutdown()
    // add partitions to topic
    AdminUtils.addPartitions(zkClient, topic, 2, "0:1:2,0:1:2", false)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    follower.startup()
    // test if topic deletion is resumed
    verifyTopicDeletion(topic, servers)
    // verify that new partition doesn't exist on any broker either
    TestUtils.waitUntilTrue(() =>
      servers.foldLeft(true)((res, server) => res && server.getLogManager().getLog(newPartition).isEmpty),
      "Replica logs not for new partition [test,1] not deleted after delete topic is complete.")
    servers.foreach(_.shutdown())
  }

  @Test
  def testAddPartitionDuringDeleteTopic() {
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    // add partitions to topic
    val newPartition = TopicAndPartition(topic, 1)
    AdminUtils.addPartitions(zkClient, topic, 2, "0:1:2,0:1:2")
    verifyTopicDeletion(topic, servers)
    // verify that new partition doesn't exist on any broker either
    assertTrue("Replica logs not deleted after delete topic is complete",
      servers.foldLeft(true)((res, server) => res && server.getLogManager().getLog(newPartition).isEmpty))
    servers.foreach(_.shutdown())
  }

  @Test
  def testRecreateTopicAfterDeletion() {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topic = "test"
    val topicAndPartition = TopicAndPartition(topic, 0)
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, topic)
    verifyTopicDeletion(topic, servers)
    // re-create topic on same replicas
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // wait until leader is elected
    val leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 1000)
    assertTrue("New leader should be elected after re-creating topic test", leaderIdOpt.isDefined)
    // check if all replica logs are created
    TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) => res && server.getLogManager().getLog(topicAndPartition).isDefined),
      "Replicas for topic test not created.")
    servers.foreach(_.shutdown())
  }

  @Test
  def testDeleteNonExistingTopic() {
    val topicAndPartition = TopicAndPartition("test", 0)
    val topic = topicAndPartition.topic
    val servers = createTestTopicAndCluster(topic)
    // start topic deletion
    AdminUtils.deleteTopic(zkClient, "test2")
    // verify delete topic path for test2 is removed from zookeeper
    verifyTopicDeletion("test2", servers)
    // verify that topic test is untouched
    TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) =>
      res && server.getLogManager().getLog(topicAndPartition).isDefined),
      "Replicas for topic test not created")
    // test the topic path exists
    assertTrue("Topic test mistakenly deleted", ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic)))
    // topic test should have a leader
    val leaderIdOpt = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 1000)
    assertTrue("Leader should exist for topic test", leaderIdOpt.isDefined)
    servers.foreach(_.shutdown())
  }

  @Test
  def testDeleteTopicWithCleaner() {
    val topicName = "test"
    val topicAndPartition = TopicAndPartition(topicName, 0)
    val topic = topicAndPartition.topic

    val brokerConfigs = TestUtils.createBrokerConfigs(3, false)
    brokerConfigs(0).setProperty("delete.topic.enable", "true")
    brokerConfigs(0).setProperty("log.cleaner.enable","true")
    brokerConfigs(0).setProperty("log.cleanup.policy","compact")
    brokerConfigs(0).setProperty("log.segment.bytes","100")
    brokerConfigs(0).setProperty("log.segment.delete.delay.ms","1000")
    val servers = createTestTopicAndCluster(topic,brokerConfigs)

    // for simplicity, we are validating cleaner offsets on a single broker
    val server = servers(0)
    val log = server.logManager.getLog(topicAndPartition).get

    // write to the topic to activate cleaner
    writeDups(numKeys = 100, numDups = 3,log)

    // wait for cleaner to clean
   server.logManager.cleaner.awaitCleaned(topicName,0,0)

    // delete topic
    AdminUtils.deleteTopic(zkClient, "test")
    verifyTopicDeletion("test", servers)

    servers.foreach(_.shutdown())
  }

  private def createTestTopicAndCluster(topic: String): Seq[KafkaServer] = {

    val brokerConfigs = TestUtils.createBrokerConfigs(3, false)
    brokerConfigs.foreach(p => p.setProperty("delete.topic.enable", "true")
    )
    createTestTopicAndCluster(topic,brokerConfigs)
  }

  private def createTestTopicAndCluster(topic: String, brokerConfigs: Seq[Properties]): Seq[KafkaServer] = {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topicAndPartition = TopicAndPartition(topic, 0)
    // create brokers
    val servers = brokerConfigs.map(b => TestUtils.createServer(new KafkaConfig(b)))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, expectedReplicaAssignment)
    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) =>
      res && server.getLogManager().getLog(topicAndPartition).isDefined),
      "Replicas for topic test not created")
    servers
  }

  private def verifyTopicDeletion(topic: String, servers: Seq[KafkaServer]) {
    val topicAndPartition = TopicAndPartition(topic, 0)
    // wait until admin path for delete topic is deleted, signaling completion of topic deletion
    TestUtils.waitUntilTrue(() => !ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic)),
      "Admin path /admin/delete_topic/test path not deleted even after a replica is restarted")
    TestUtils.waitUntilTrue(() => !ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic)),
      "Topic path /brokers/topics/test not deleted after /admin/delete_topic/test path is deleted")
    // ensure that the topic-partition has been deleted from all brokers' replica managers
    TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res, server) => res && server.replicaManager.getPartition(topic, 0) == None),
      "Replica manager's should have deleted all of this topic's partitions")
    // ensure that logs from all replicas are deleted if delete topic is marked successful in zookeeper
    assertTrue("Replica logs not deleted after delete topic is complete",
      servers.foldLeft(true)((res, server) => res && server.getLogManager().getLog(topicAndPartition).isEmpty))
    // ensure that topic is removed from all cleaner offsets
    TestUtils.waitUntilTrue(() => servers.foldLeft(true)((res,server) => res &&
    {
      val topicAndPartition = TopicAndPartition(topic,0)
      val logdir = server.getLogManager().logDirs(0)
      val checkpoints =  new OffsetCheckpoint(new File(logdir,"cleaner-offset-checkpoint")).read()
      !checkpoints.contains(topicAndPartition)
    }),
      "Cleaner offset for deleted partition should have been removed")
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
