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

import java.util
import java.util.concurrent.ExecutionException
import java.util.{Collections, Optional, Properties}

import kafka.common.TopicAlreadyMarkedForDeletionException
import kafka.controller.{OfflineReplica, PartitionAndReplica, ReplicaAssignment, ReplicaDeletionSuccessful}
import kafka.log.UnifiedLog
import kafka.server
import kafka.server.{KafkaConfig, KafkaServer, QuorumTestHarness}
import kafka.utils.{TestInfoUtils, TestUtils}
import kafka.zk.TopicPartitionZNode
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewPartitionReassignment, NewPartitions}
import org.apache.kafka.common.{TopicPartition, TopicPartitionInfo}
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.Assertions.{assertTrue, _}
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.opentest4j.AssertionFailedError

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class DeleteTopicTest extends QuorumTestHarness {
  var adminClient: Admin = null

  val expectedReplicaAssignment = Map(0 -> List(0, 1, 2))
  val expectedReplicaFullAssignment = expectedReplicaAssignment.map { case (k, v) =>
    k -> ReplicaAssignment(v, List(), List())
  }

  @AfterEach
  override def tearDown(): Unit = {
    adminClient.close()
    TestUtils.shutdownServers(brokers)
    super.tearDown()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDeleteTopicWithAllAliveReplicas(quorum: String): Unit = {
    val topic = "test"
    createTestTopicAndCluster(topic)
    TestUtils.deleteTopicWithAdmin(adminClient, topic, brokers)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testResumeDeleteTopicWithRecoveredFollower(quorum: String): Unit = {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    createTestTopicAndCluster(topic)
    // shut down one follower replica
    val partition = getTopicPartitionInfo(adminClient, topic, 0)
    assertTrue(partition.isPresent, "Partition [test,0] should exist")
    val leaderNode = partition.get().leader()
    assertFalse(leaderNode.isEmpty, "Leader should exist for partition [test,0]")
    val follower = brokers.filter(s => s.config.brokerId != leaderNode.id()).last
    follower.shutdown()
    // start topic deletion
    faultHandler.setIgnore(true)
    adminClient.deleteTopics(Collections.singleton(topic))
    // check if all replicas but the one that is shut down has deleted the log
    TestUtils.waitUntilTrue(() =>
      brokers.filter(s => s.config.brokerId != follower.config.brokerId)
        .forall(_.logManager.getLog(topicPartition).isEmpty), "Replicas 0,1 have not deleted log.")
    // ensure topic deletion is halted
    val aliveBrokers = brokers.filter(b => b != follower)
    TestUtils.waitForAllPartitionsMetadata(aliveBrokers, topic, 0)
    TestUtils.waitForAllPartitionsMetadata(Seq(follower), topic, 1)
    assertThrows(classOf[ExecutionException], () => adminClient.describeTopics(Collections.singleton(topic)).topicNameValues().get(topic).get())
    // restart follower replica
    follower.startup()
    TestUtils.waitForAllPartitionsMetadata(brokers, topic, 0)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testResumeDeleteTopicOnControllerFailover(quorum: String): Unit = {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    createTestTopicAndCluster(topic)

    val partition = getTopicPartitionInfo(adminClient, topic, 0)
    assertTrue(partition.isPresent, "Partition [test,0] should exist")
    val leaderNode = partition.get().leader()
    assertFalse(leaderNode.isEmpty, "Leader should exist for partition [test,0]")

    val verifyDeletionExecutable: Executable = () => TestUtils.verifyTopicDeletion(zkClientOrNull, topic, 1, brokers)
    val controllerId = if (isKRaftTest()) null else zkClient.getControllerId.getOrElse(throw new RuntimeException("Controller does not exist"))
    val follower = brokers.filter(s => s.config.brokerId != leaderNode.id() && s.config.brokerId != controllerId).last
    follower.shutdown()
    // start topic deletion
    faultHandler.setIgnore(true)
    adminClient.deleteTopics(Collections.singleton(topic))
    // shut down the controller to trigger controller failover during delete topic
    stopController()
    // ensure topic deletion is halted
    assertThrows(classOf[AssertionFailedError], verifyDeletionExecutable)

    restartController()
    follower.startup()
    TestUtils.verifyTopicDeletion(zkClientOrNull, topic, 1, brokers)
  }

  def getTopicPartitionInfo(adminClient: Admin, topic: String, partition: Int): Optional[TopicPartitionInfo] = {
    adminClient.describeTopics(Collections.singleton(topic)).topicNameValues().get(topic).get().
      partitions().stream().filter(_.partition == partition).findAny()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testPartitionReassignmentDuringDeleteTopic(quorum: String): Unit = {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    createTestTopicAndCluster(topic, numConfigs = 4)

    val brokersWithReplica = brokers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))

    val partition = getTopicPartitionInfo(adminClient, topic, 0)
    assertTrue(partition.isPresent, "Partition [test,0] should exist")
    val leaderNode = partition.get().leader()
    assertFalse(leaderNode.isEmpty, "Leader should exist for partition [test,0]")
    val follower = brokersWithReplica.filter(s => s.config.brokerId != leaderNode.id()).last
    follower.shutdown()
    // start topic deletion
    faultHandler.setIgnore(true)
    adminClient.deleteTopics(Collections.singleton(topic))
    // verify that a partition from the topic cannot be reassigned
    waitUntilTopicGone(adminClient, "test")
    verifyReassignmentFailsForMissing(adminClient, new TopicPartition(topic, 0),
      new NewPartitionReassignment(util.Arrays.asList(1, 2, 3)))
    follower.startup()
    TestUtils.verifyTopicDeletion(zkClientOrNull, topic, 1, brokersWithReplica)
  }

  private def waitUntilTopicGone(adminClient: Admin, topicName: String): Unit = {
    TestUtils.waitUntilTrue(() => {
      try {
        adminClient.describeTopics(util.Collections.singletonList(topicName)).allTopicNames().get()
        false
      } catch {
        case e: ExecutionException =>
          classOf[UnknownTopicOrPartitionException].equals(e.getCause.getClass)
      }
    }, s"Topic ${topicName} should be deleted.")
  }

  private def verifyReassignmentFailsForMissing(adminClient: Admin,
                                                partition: TopicPartition,
                                                reassignment: NewPartitionReassignment): Unit = {
    val e = assertThrows(classOf[ExecutionException], () => adminClient.alterPartitionReassignments(Collections.singletonMap(partition,
      Optional.of(reassignment))).all().get())
    assertEquals(classOf[UnknownTopicOrPartitionException], e.getCause.getClass)
  }

  private def getController() : (KafkaServer, Int) = {
    val controllerId = zkClient.getControllerId.getOrElse(throw new AssertionError("Controller doesn't exist"))
    val controller = brokers.find(s => s.config.brokerId == controllerId).get
    (controller.asInstanceOf[KafkaServer], controllerId)
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testIncreasePartitionCountDuringDeleteTopicZKMode(quorum: String): Unit = {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    createTestTopicAndCluster(topic, numConfigs = 4)
    val brokersWithReplicas = brokers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))
    // shutdown a broker to make sure the following topic deletion will be suspended
    val partition = getTopicPartitionInfo(adminClient, topic, 0)
    assertTrue(partition.isPresent, "Partition [test,0] should exist")
    val leaderNode = partition.get().leader()
    assertFalse(leaderNode.isEmpty, "Leader should exist for partition [test,0]")
    val follower = brokersWithReplicas.filter(s => s.config.brokerId != leaderNode.id()).last
    follower.shutdown()
    // start topic deletion
    adminClient.deleteTopics(Collections.singleton(topic))

    // make sure deletion of all of the topic's replicas have been tried
    ensureControllerExists()
    val (controller, controllerId) = getController()
    val allReplicasForTopic = getAllReplicasFromAssignment(topic, expectedReplicaAssignment)
    TestUtils.waitUntilTrue(() => {
      val replicasInDeletionSuccessful = controller.kafkaController.controllerContext.replicasInState(topic, ReplicaDeletionSuccessful)
      val offlineReplicas = controller.kafkaController.controllerContext.replicasInState(topic, OfflineReplica)
      allReplicasForTopic == (replicasInDeletionSuccessful union offlineReplicas)
    }, s"Not all replicas for topic $topic are in states of either ReplicaDeletionSuccessful or OfflineReplica")
    // increase the partition count for topic
    try {
      adminClient.createPartitions(Map(topic -> NewPartitions.increaseTo(2)).asJava).all().get()
    } catch {
      case _: ExecutionException =>
    }
    // trigger a controller switch now
    val previousControllerId = controllerId

    controller.shutdown()

    ensureControllerExists()
    // wait until a new controller to show up
    TestUtils.waitUntilTrue(() => {
      val (_, newControllerId) = getController()
      newControllerId != previousControllerId
    }, "The new controller should not have the failed controller id")

    // bring back the failed brokers
    follower.startup()
    controller.startup()
    TestUtils.verifyTopicDeletion(zkClientOrNull, topic, 2, brokersWithReplicas)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testIncreasePartitionCountDuringDeleteTopicKRaftMode(quorum: String): Unit = {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    createTestTopicAndCluster(topic, numConfigs = 4)
    val brokersWithReplicas = brokers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))
    // shutdown a broker to make sure the following topic deletion will be suspended
    val partition = getTopicPartitionInfo(adminClient, topic, 0)
    assertTrue(partition.isPresent, "Partition [test,0] should exist")
    val leaderNode = partition.get().leader()
    assertFalse(leaderNode.isEmpty, "Leader should exist for partition [test,0]")
    val follower = brokersWithReplicas.filter(s => s.config.brokerId != leaderNode.id()).last
    follower.shutdown()
    // start topic deletion
    faultHandler.setIgnore(true)
    adminClient.deleteTopics(Collections.singleton(topic))

    // make sure deletion of all of the topic's replicas have been tried
    val aliveBrokers = brokers.filter(b => b != follower)
    TestUtils.waitForAllPartitionsMetadata(aliveBrokers, topic, 0)
    TestUtils.waitForAllPartitionsMetadata(Seq(follower), topic, 1)
    // increase the partition count for topic
    try {
      adminClient.createPartitions(Map(topic -> NewPartitions.increaseTo(2)).asJava).all().get()
    } catch {
      case _: ExecutionException =>
    }
    // trigger a controller switch now
    controllerServer.shutdown()
    restartControllerServer()

    // bring back the failed brokers
    follower.startup()
    TestUtils.verifyTopicDeletion(zkClientOrNull, topic, 2, brokersWithReplicas)
  }


  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDeleteTopicDuringAddPartition(quorum: String): Unit = {
    val topic = "test"
    createTestTopicAndCluster(topic, numConfigs = 4)
    val partition = getTopicPartitionInfo(adminClient, topic, 0)
    assertTrue(partition.isPresent, "Partition [test,0] should exist")
    val leaderNode = partition.get().leader()
    assertFalse(leaderNode.isEmpty, "Leader should exist for partition [test,0]")
    val brokersWithReplica = brokers.filter(s => expectedReplicaAssignment(0).contains(s.config.brokerId))
    val follower = brokersWithReplica.filter(s => s.config.brokerId != leaderNode.id()).last
    val newPartition = new TopicPartition(topic, 1)
    follower.shutdown()
    if (!isKRaftTest()) {
      // wait until the broker has been removed from ZK to reduce non-determinism
      TestUtils.waitUntilTrue(() => zkClient.getBroker(follower.config.brokerId).isEmpty,
        s"Follower ${follower.config.brokerId} was not removed from ZK")
    }
    // add partitions to topic
    faultHandler.setIgnore(true)
    assertDoesNotThrow(() => adminClient.createPartitions(Map(topic -> NewPartitions.increaseTo(3)).asJava).all().get())
    // start topic deletion
    adminClient.deleteTopics(Collections.singleton(topic))
    follower.startup()
    // test if topic deletion is resumed
    TestUtils.verifyTopicDeletion(zkClientOrNull, topic, 1, brokers)
    // verify that new partition doesn't exist on any broker either
    TestUtils.waitUntilTrue(() =>
      brokers.forall(_.logManager.getLog(newPartition).isEmpty),
      "Replica logs not for new partition [test,1] not deleted after delete topic is complete.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testAddPartitionDuringDeleteTopic(quorum: String): Unit = {
    zkClient.createTopLevelPaths()
    val topic = "test"
    createTestTopicAndCluster(topic)
    val brokerMetadata = adminZkClient.getBrokerMetadatas()
    // start topic deletion
    adminZkClient.deleteTopic(topic)
    // add partitions to topic
    val newPartition = new TopicPartition(topic, 1)
    adminZkClient.addPartitions(topic, expectedReplicaFullAssignment, brokerMetadata, 2,
      Some(Map(1 -> Seq(0, 1, 2), 2 -> Seq(0, 1, 2))))
    TestUtils.verifyTopicDeletion(zkClient, topic, 1, brokers)
    // verify that new partition doesn't exist on any broker either
    assertTrue(brokers.forall(_.logManager.getLog(newPartition).isEmpty), "Replica logs not deleted after delete topic is complete")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testAddPartitionAfterDeleteTopic(quorum: String): Unit = {
    val topic = "test"
    createTestTopicAndCluster(topic)
    // topic deletion
    adminClient.deleteTopics(Collections.singleton(topic))
    // adding partitions to topic should fail
    assertThrows(classOf[ExecutionException], () => adminClient.createPartitions(Map(topic -> NewPartitions.increaseTo(3)).asJava).all().get())
    TestUtils.verifyTopicDeletion(zkClientOrNull, topic, 1, brokers)
    // verify that new partition doesn't exist on any broker either
    val newPartition = new TopicPartition(topic, 1)
    assertTrue(brokers.forall(_.logManager.getLog(newPartition).isEmpty), "Replica logs not deleted after delete topic is complete")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testRecreateTopicAfterDeletion(quroum: String): Unit = {
    val topic = "test"
    val topicPartition = new TopicPartition(topic, 0)
    createTestTopicAndCluster(topic)
    // start topic deletion
    adminClient.deleteTopics(Collections.singleton(topic))
    TestUtils.verifyTopicDeletion(zkClientOrNull, topic, 1, brokers)
    // re-create topic on same replicas
    TestUtils.createTopicWithAdmin(admin = adminClient, topic = topic, brokers = brokers, replicaAssignment = expectedReplicaAssignment)
    // check if all replica logs are created
    TestUtils.waitUntilTrue(() => brokers.forall(_.logManager.getLog(topicPartition).isDefined),
      "Replicas for topic test not created.")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDeleteNonExistingTopic(quorum: String): Unit = {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    createTestTopicAndCluster(topic)
    // start topic deletion
    adminClient.deleteTopics(Collections.singleton("test2"))
    if (!isKRaftTest()) {
      // verify delete topic path for test2 is removed from ZooKeeper
      TestUtils.verifyTopicDeletion(zkClientOrNull, "test2", 1, brokers)
    }
    // verify that topic test is untouched
    TestUtils.waitUntilTrue(() => brokers.forall(_.logManager.getLog(topicPartition).isDefined),
      "Replicas for topic test not created")
    // test the topic exists
    val partition = getTopicPartitionInfo(adminClient, topic, 0)
    assertTrue(partition.isPresent, "Partition [test,0] should exist")
    // topic test should have a leader
    assertFalse(partition.get().leader().isEmpty, "Leader should exist for partition [test,0]")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDeleteTopicWithCleaner(quorum: String): Unit = {
    val topicName = "test"
    val topicPartition = new TopicPartition(topicName, 0)
    val topic = topicPartition.topic

    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnectOrNull, false)
    brokerConfigs.head.setProperty("delete.topic.enable", "true")
    brokerConfigs.head.setProperty("log.cleaner.enable","true")
    brokerConfigs.head.setProperty("log.cleanup.policy","compact")
    brokerConfigs.head.setProperty("log.segment.bytes","100")
    brokerConfigs.head.setProperty("log.cleaner.dedupe.buffer.size","1048577")

    createTestTopicAndCluster(topic, brokerConfigs, expectedReplicaAssignment)

    // for simplicity, we are validating cleaner offsets on a single broker
    val server = brokers.head
    val log = server.logManager.getLog(topicPartition).get

    // write to the topic to activate cleaner
    writeDups(numKeys = 100, numDups = 3,log)

    // wait for cleaner to clean
    server.logManager.cleaner.awaitCleaned(new TopicPartition(topicName, 0), 0)

    // delete topic
    adminClient.deleteTopics(Collections.singleton("test"))
    TestUtils.verifyTopicDeletion(zkClientOrNull, "test", 1, brokers)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testDeleteTopicAlreadyMarkedAsDeleted(quorum: String): Unit = {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    createTestTopicAndCluster(topic)
    // start topic deletion
    adminZkClient.deleteTopic(topic)
    // try to delete topic marked as deleted
    assertThrows(classOf[TopicAlreadyMarkedForDeletionException], () => adminZkClient.deleteTopic(topic))

    TestUtils.verifyTopicDeletion(zkClient, topic, 1, brokers)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testDisableDeleteTopic(quorum: String): Unit = {
    val topicPartition = new TopicPartition("test", 0)
    val topic = topicPartition.topic
    createTestTopicAndCluster(topic, deleteTopicEnabled = false)
    // mark the topic for deletion
    val result = adminClient.deleteTopics(Collections.singleton("test"))
    assertThrows(classOf[ExecutionException], () => result.topicNameValues.get(topic).get)
    // verify that topic test is untouched
    assertTrue(brokers.forall(_.logManager.getLog(topicPartition).isDefined))
    // test the topic exists
    val partition = getTopicPartitionInfo(adminClient, topic, 0)
    assertTrue(partition.isPresent, "Partition [test,0] should exist")
    // topic test should have a leader
    assertFalse(partition.get().leader().isEmpty, "Leader should exist for partition [test,0]")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testDeletingPartiallyDeletedTopic(quorum: String): Unit = {
    /**
     * A previous controller could have deleted some partitions of a topic from ZK, but not all partitions, and then crashed.
     * In that case, the new controller should be able to handle the partially deleted topic, and finish the deletion.
     */

    val replicaAssignment = Map(0 -> List(0, 1, 2), 1 -> List(0, 1, 2))
    val topic = "test"
    createTestTopicAndCluster(topic, replicaAssignment = replicaAssignment)

    /**
     * shutdown all brokers in order to create a partially deleted topic on ZK
     */
    brokers.foreach(_.shutdown())

    /**
     * delete the partition znode at /brokers/topics/test/partition/0
     * to simulate the case that a previous controller crashed right after deleting the partition znode
     */
    zkClient.deleteRecursive(TopicPartitionZNode.path(new TopicPartition(topic, 0)))
    adminZkClient.deleteTopic(topic)

    /**
     * start up all brokers and verify that topic deletion eventually finishes.
     */
    brokers.foreach(_.startup())
    TestUtils.waitUntilTrue(() => brokers.asInstanceOf[Seq[server.KafkaServer]].exists(_.kafkaController.isActive), "No controller is elected")
    TestUtils.verifyTopicDeletion(zkClient, topic, 2, brokers)
  }

  protected def securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT
  protected def listenerName: ListenerName = ListenerName.forSecurityProtocol(securityProtocol)

  private def setUpAdminClient(): Admin = {
    val adminClientConfig = new Properties(TestUtils.securityConfigs(Mode.CLIENT, SecurityProtocol.PLAINTEXT, None,
      "adminClient", TestUtils.SslCertificateCn, None))
    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, TestUtils.bootstrapServers(brokers, listenerName))
    TestUtils.createAdminClient(brokers, listenerName, adminClientConfig)
  }

  private def createTestTopicAndCluster(topic: String, deleteTopicEnabled: Boolean = true, numConfigs: Int = 3, replicaAssignment: Map[Int, List[Int]] = expectedReplicaAssignment): Unit = {
    if (isKRaftTest() && !deleteTopicEnabled) {
      controllerServer.shutdown()
      restartControllerServer(deleteTopicEnabled = false)
    }
    val brokerConfigs = TestUtils.createBrokerConfigs(numConfigs, zkConnectOrNull, enableControlledShutdown = false)
    brokerConfigs.foreach(_.setProperty("delete.topic.enable", deleteTopicEnabled.toString))
    createTestTopicAndCluster(topic, brokerConfigs, replicaAssignment)
  }

  private def createTestTopicAndCluster(topic: String, brokerConfigs: Seq[Properties], replicaAssignment: Map[Int, List[Int]]): Unit = {
    val topicPartition = new TopicPartition(topic, 0)
    // create brokers
    brokerConfigs.foreach(b => createBroker(KafkaConfig.fromProps(b)))
    // spin up admin client
    adminClient = setUpAdminClient()
    // create the topic
    TestUtils.createTopicWithAdmin(
      admin = adminClient,
      topic = topic,
      brokers = brokers,
      replicaAssignment = replicaAssignment
    )
    // wait until replica log is created on every broker
    val brokersWithReplicas = brokers.filter(s => replicaAssignment.values.flatten.toList.distinct.contains(s.config.brokerId))
    TestUtils.waitUntilTrue(() => brokersWithReplicas.forall(_.logManager.getLog(topicPartition).isDefined),
      "Replicas for topic test not created.")
  }

  private def writeDups(numKeys: Int, numDups: Int, log: UnifiedLog): Seq[(Int, Int)] = {
    var counter = 0
    for (_ <- 0 until numDups; key <- 0 until numKeys) yield {
      val count = counter
      log.appendAsLeader(TestUtils.singletonRecords(value = counter.toString.getBytes, key = key.toString.getBytes), leaderEpoch = 0)
      counter += 1
      (key, count)
    }
  }
}
