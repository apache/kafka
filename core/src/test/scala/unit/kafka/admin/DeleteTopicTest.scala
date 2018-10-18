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
import kafka.zk.{TopicPartitionZNode, ZooKeeperTestHarness}
import kafka.utils.TestUtils
import kafka.server.{KafkaConfig, KafkaServer}
import org.junit.Assert._
import org.junit.{After, Test}
import java.util.Properties

import kafka.common.TopicAlreadyMarkedForDeletionException
import kafka.controller.{OfflineReplica, PartitionAndReplica, ReplicaDeletionSuccessful}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException

class DeleteTopicTest extends ZooKeeperTestHarness {

  var servers: Seq[KafkaServer] = Seq()

  val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))

  @After
  override def tearDown() {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testDeleteTopicWithAllAliveReplicas() {
    val topic = "test"
    servers = createTestTopicAndCluster(topic)
    // start topic deletion
    adminZkClient.deleteTopic(topic)
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
  }

  @Test
  def testResumeDeleteTopicWithRecoveredFollower() {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    servers = createTestTopicAndCluster(topic)
    // shut down one follower replica
    val leaderIdOpt = zkClient.getLeaderForPartition(new TopicPartition(topic, 0))
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()
    // start topic deletion
    adminZkClient.deleteTopic(topic)
    // check if all replicas but the one that is shut down has deleted the log
    TestUtils.waitUntilTrue(() =>
      servers.filter(s => s.config.brokerId != follower.config.brokerId)
        .forall(_.getLogManager().getLog(topicPartition).isEmpty), "Replicas 0,1 have not deleted log.")
    // ensure topic deletion is halted
    TestUtils.waitUntilTrue(() => zkClient.isTopicMarkedForDeletion(topic),
      "Admin path /admin/delete_topic/test path deleted even when a follower replica is down")
    // restart follower replica
    follower.startup()
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
  }

  @Test
  def testResumeDeleteTopicOnControllerFailover() {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    servers = createTestTopicAndCluster(topic)
    val controllerId = zkClient.getControllerId.getOrElse(fail("Controller doesn't exist"))
    val controller = servers.filter(s => s.config.brokerId == controllerId).head
    val leaderIdOpt = zkClient.getLeaderForPartition(new TopicPartition(topic, 0))
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get && s.config.brokerId != controllerId).last
    follower.shutdown()

    // start topic deletion
    adminZkClient.deleteTopic(topic)
    // shut down the controller to trigger controller failover during delete topic
    controller.shutdown()

    // ensure topic deletion is halted
    TestUtils.waitUntilTrue(() => zkClient.isTopicMarkedForDeletion(topic),
      "Admin path /admin/delete_topic/test path deleted even when a replica is down")

    controller.startup()
    follower.startup()

    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
  }

  @Test
  def testPartitionReassignmentDuringDeleteTopic() {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topic = "test"
    val topicPartition = new TopicPartition(topic, 0)
    val brokerConfigs = TestUtils.createBrokerConfigs(4, zkConnect, false)
    brokerConfigs.foreach(p => p.setProperty("delete.topic.enable", "true"))
    // create brokers
    val allServers = brokerConfigs.map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    this.servers = allServers
    val servers = allServers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))
    // create the topic
    TestUtils.createTopic(zkClient, topic, expectedReplicaAssignment, servers)
    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(() => servers.forall(_.getLogManager().getLog(topicPartition).isDefined),
      "Replicas for topic test not created.")
    val leaderIdOpt = zkClient.getLeaderForPartition(new TopicPartition(topic, 0))
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()
    // start topic deletion
    adminZkClient.deleteTopic(topic)
    // start partition reassignment at the same time right after delete topic. In this case, reassignment will fail since
    // the topic is being deleted
    // reassign partition 0
    val oldAssignedReplicas = zkClient.getReplicasForPartition(new TopicPartition(topic, 0))
    val newReplicas = Seq(1, 2, 3)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, None,
      Map(topicPartition -> newReplicas),  adminZkClient = adminZkClient)
    assertTrue("Partition reassignment should fail for [test,0]", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
      ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkClient, Map(topicPartition -> newReplicas))
        .getOrElse(topicPartition, fail(s"Failed to get reassignment status for $topicPartition")) == ReassignmentFailed
    }, "Partition reassignment shouldn't complete.")
    val controllerId = zkClient.getControllerId.getOrElse(fail("Controller doesn't exist"))
    val controller = servers.filter(s => s.config.brokerId == controllerId).head
    assertFalse("Partition reassignment should fail",
      controller.kafkaController.controllerContext.partitionsBeingReassigned.contains(topicPartition))
    val assignedReplicas = zkClient.getReplicasForPartition(new TopicPartition(topic, 0))
    assertEquals("Partition should not be reassigned to 0, 1, 2", oldAssignedReplicas, assignedReplicas)
    follower.startup()
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
  }

  private def getController() : (KafkaServer, Int) = {
    val controllerId = zkClient.getControllerId.getOrElse(fail("Controller doesn't exist"))
    val controller = servers.find(s => s.config.brokerId == controllerId).get
    (controller, controllerId)
  }

  private def ensureControllerExists() = {
    TestUtils.waitUntilTrue(() => {
      try {
        getController()
        true
      } catch {
        case _: Throwable  => false
      }
    }, "Controller should eventually exist")
  }

  private def getAllReplicasFromAssignment(topic : String, assignment : Map[Int, Seq[Int]]) : Set[PartitionAndReplica] = {
    assignment.flatMap { case (partition, replicas) =>
      replicas.map {r => new PartitionAndReplica(new TopicPartition(topic, partition), r)}
    }.toSet
  }

  @Test
  def testIncreasePartitionCountDuringDeleteTopic() {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topic = "test"
    val topicPartition = new TopicPartition(topic, 0)
    val brokerConfigs = TestUtils.createBrokerConfigs(4, zkConnect, false)
    brokerConfigs.foreach(p => p.setProperty("delete.topic.enable", "true"))
    // create brokers
    val allServers = brokerConfigs.map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    this.servers = allServers
    val servers = allServers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))
    // create the topic
    TestUtils.createTopic(zkClient, topic, expectedReplicaAssignment, servers)
    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(() => servers.forall(_.getLogManager().getLog(topicPartition).isDefined),
      "Replicas for topic test not created.")
    // shutdown a broker to make sure the following topic deletion will be suspended
    val leaderIdOpt = zkClient.getLeaderForPartition(topicPartition)
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(s => s.config.brokerId != leaderIdOpt.get).last
    follower.shutdown()
    // start topic deletion
    adminZkClient.deleteTopic(topic)

    // make sure deletion of all of the topic's replicas have been tried
    ensureControllerExists()
    val (controller, controllerId) = getController()
    val allReplicasForTopic = getAllReplicasFromAssignment(topic, expectedReplicaAssignment)
    TestUtils.waitUntilTrue(() => {
      val replicasInDeletionSuccessful = controller.kafkaController.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
      val offlineReplicas = controller.kafkaController.replicaStateMachine.replicasInState(topic, OfflineReplica)
      allReplicasForTopic == (replicasInDeletionSuccessful union offlineReplicas)
    }, s"Not all replicas for topic $topic are in states of either ReplicaDeletionSuccessful or OfflineReplica")

    // increase the partition count for topic
    val topicCommandOptions = new TopicCommand.TopicCommandOptions(Array("--zookeeper", zkConnect, "--alter", "--topic", topic, "--partitions", "2"))
    TopicCommand.alterTopic(zkClient, topicCommandOptions)

    // trigger a controller switch now
    val previousControllerId = controllerId

    controller.shutdown()

    ensureControllerExists()
    // wait until a new controller to show up
    TestUtils.waitUntilTrue(() => {
      val (newController, newControllerId) = getController()
      newControllerId != previousControllerId
    }, "The new controller should not have the failed controller id")

    // bring back the failed brokers
    follower.startup()
    controller.startup()
    TestUtils.verifyTopicDeletion(zkClient, topic, 2, servers)
  }


  @Test
  def testDeleteTopicDuringAddPartition() {
    val topic = "test"
    servers = createTestTopicAndCluster(topic)
    val leaderIdOpt = zkClient.getLeaderForPartition(new TopicPartition(topic, 0))
    assertTrue("Leader should exist for partition [test,0]", leaderIdOpt.isDefined)
    val follower = servers.filter(_.config.brokerId != leaderIdOpt.get).last
    val newPartition = new TopicPartition(topic, 1)
    follower.shutdown()
    // wait until the broker has been removed from ZK to reduce non-determinism
    TestUtils.waitUntilTrue(() => zkClient.getBroker(follower.config.brokerId).isEmpty,
      s"Follower ${follower.config.brokerId} was not removed from ZK")
    // add partitions to topic
    adminZkClient.addPartitions(topic, expectedReplicaAssignment, 2, Some(Map(1 -> Seq(0, 1, 2), 2 -> Seq(0, 1, 2))))
    // start topic deletion
    adminZkClient.deleteTopic(topic)
    follower.startup()
    // test if topic deletion is resumed
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
    // verify that new partition doesn't exist on any broker either
    TestUtils.waitUntilTrue(() =>
      servers.forall(_.getLogManager().getLog(newPartition).isEmpty),
      "Replica logs not for new partition [test,1] not deleted after delete topic is complete.")
  }

  @Test
  def testAddPartitionDuringDeleteTopic() {
    zkClient.createTopLevelPaths()
    val topic = "test"
    servers = createTestTopicAndCluster(topic)
    // start topic deletion
    adminZkClient.deleteTopic(topic)
    // add partitions to topic
    val newPartition = new TopicPartition(topic, 1)
    adminZkClient.addPartitions(topic, expectedReplicaAssignment, 2, Some(Map(1 -> Seq(0, 1, 2), 2 -> Seq(0, 1, 2))))
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
    // verify that new partition doesn't exist on any broker either
    assertTrue("Replica logs not deleted after delete topic is complete",
      servers.forall(_.getLogManager().getLog(newPartition).isEmpty))
  }

  @Test
  def testRecreateTopicAfterDeletion() {
    val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
    val topic = "test"
    val topicPartition = new TopicPartition(topic, 0)
    servers = createTestTopicAndCluster(topic)
    // start topic deletion
    adminZkClient.deleteTopic(topic)
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
    // re-create topic on same replicas
    TestUtils.createTopic(zkClient, topic, expectedReplicaAssignment, servers)
    // check if all replica logs are created
    TestUtils.waitUntilTrue(() => servers.forall(_.getLogManager().getLog(topicPartition).isDefined),
      "Replicas for topic test not created.")
  }

  @Test
  def testDeleteNonExistingTopic() {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    servers = createTestTopicAndCluster(topic)
    // start topic deletion
    try {
      adminZkClient.deleteTopic("test2")
      fail("Expected UnknownTopicOrPartitionException")
    } catch {
      case _: UnknownTopicOrPartitionException => // expected exception
    }
    // verify delete topic path for test2 is removed from ZooKeeper
    TestUtils.verifyTopicDeletion(zkClient, "test2", 1, servers)
    // verify that topic test is untouched
    TestUtils.waitUntilTrue(() => servers.forall(_.getLogManager().getLog(topicPartition).isDefined),
      "Replicas for topic test not created")
    // test the topic path exists
    assertTrue("Topic test mistakenly deleted", zkClient.topicExists(topic))
    // topic test should have a leader
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 1000)
  }

  @Test
  def testDeleteTopicWithCleaner() {
    val topicName = "test"
    val topicPartition = new TopicPartition(topicName, 0)
    val topic = topicPartition.topic

    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false)
    brokerConfigs.head.setProperty("delete.topic.enable", "true")
    brokerConfigs.head.setProperty("log.cleaner.enable","true")
    brokerConfigs.head.setProperty("log.cleanup.policy","compact")
    brokerConfigs.head.setProperty("log.segment.bytes","100")
    brokerConfigs.head.setProperty("log.cleaner.dedupe.buffer.size","1048577")

    servers = createTestTopicAndCluster(topic, brokerConfigs, expectedReplicaAssignment)

    // for simplicity, we are validating cleaner offsets on a single broker
    val server = servers.head
    val log = server.logManager.getLog(topicPartition).get

    // write to the topic to activate cleaner
    writeDups(numKeys = 100, numDups = 3,log)

    // wait for cleaner to clean
   server.logManager.cleaner.awaitCleaned(new TopicPartition(topicName, 0), 0)

    // delete topic
    adminZkClient.deleteTopic("test")
    TestUtils.verifyTopicDeletion(zkClient, "test", 1, servers)
  }

  @Test
  def testDeleteTopicAlreadyMarkedAsDeleted() {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    servers = createTestTopicAndCluster(topic)

    try {
      // start topic deletion
      adminZkClient.deleteTopic(topic)
      // try to delete topic marked as deleted
      adminZkClient.deleteTopic(topic)
      fail("Expected TopicAlreadyMarkedForDeletionException")
    }
    catch {
      case _: TopicAlreadyMarkedForDeletionException => // expected exception
    }

    TestUtils.verifyTopicDeletion(zkClient, topic, 1, servers)
  }

  private def createTestTopicAndCluster(topic: String, deleteTopicEnabled: Boolean = true, replicaAssignment: Map[Int, List[Int]] = expectedReplicaAssignment): Seq[KafkaServer] = {
    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, enableControlledShutdown = false)
    brokerConfigs.foreach(_.setProperty("delete.topic.enable", deleteTopicEnabled.toString))
    createTestTopicAndCluster(topic, brokerConfigs, replicaAssignment)
  }

  private def createTestTopicAndCluster(topic: String, brokerConfigs: Seq[Properties], replicaAssignment: Map[Int, List[Int]]): Seq[KafkaServer] = {
    val topicPartition = new TopicPartition(topic, 0)
    // create brokers
    val servers = brokerConfigs.map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // create the topic
    TestUtils.createTopic(zkClient, topic, expectedReplicaAssignment, servers)
    // wait until replica log is created on every broker
    TestUtils.waitUntilTrue(() => servers.forall(_.getLogManager().getLog(topicPartition).isDefined),
      "Replicas for topic test not created")
    servers
  }

  private def writeDups(numKeys: Int, numDups: Int, log: Log): Seq[(Int, Int)] = {
    var counter = 0
    for (_ <- 0 until numDups; key <- 0 until numKeys) yield {
      val count = counter
      log.appendAsLeader(TestUtils.singletonRecords(value = counter.toString.getBytes, key = key.toString.getBytes), leaderEpoch = 0)
      counter += 1
      (key, count)
    }
  }

  @Test
  def testDisableDeleteTopic() {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    servers = createTestTopicAndCluster(topic, deleteTopicEnabled = false)
    // mark the topic for deletion
    adminZkClient.deleteTopic("test")
    TestUtils.waitUntilTrue(() => !zkClient.isTopicMarkedForDeletion(topic),
      "Admin path /admin/delete_topic/%s path not deleted even if deleteTopic is disabled".format(topic))
    // verify that topic test is untouched
    assertTrue(servers.forall(_.getLogManager().getLog(topicPartition).isDefined))
    // test the topic path exists
    assertTrue("Topic path disappeared", zkClient.topicExists(topic))
    // topic test should have a leader
    val leaderIdOpt = zkClient.getLeaderForPartition(new TopicPartition(topic, 0))
    assertTrue("Leader should exist for topic test", leaderIdOpt.isDefined)
  }

  @Test
  def testDeletingPartiallyDeletedTopic() {
    /**
      * A previous controller could have deleted some partitions of a topic from ZK, but not all partitions, and then crashed.
      * In that case, the new controller should be able to handle the partially deleted topic, and finish the deletion.
      */

    val replicaAssignment = Map(0 -> List(0, 1, 2), 1 -> List(0, 1, 2))
    val topic = "test"
    servers = createTestTopicAndCluster(topic, true, replicaAssignment)

    /**
      * shutdown all brokers in order to create a partially deleted topic on ZK
      */
    servers.foreach(_.shutdown())

    /**
      * delete the partition znode at /brokers/topics/test/partition/0
      * to simulate the case that a previous controller crashed right after deleting the partition znode
      */
    zkClient.deleteRecursive(TopicPartitionZNode.path(new TopicPartition(topic, 0)))
    adminZkClient.deleteTopic(topic)

    /**
      * start up all brokers and verify that topic deletion eventually finishes.
      */
    servers.foreach(_.startup())
    TestUtils.waitUntilTrue(() => servers.exists(_.kafkaController.isActive), "No controller is elected")
    TestUtils.verifyTopicDeletion(zkClient, topic, 2, servers)
  }
}
