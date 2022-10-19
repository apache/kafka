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

package kafka.controller

import com.yammer.metrics.core.Timer
import kafka.api.{ApiVersion, KAFKA_2_6_IV0, KAFKA_2_7_IV0, LeaderAndIsr}
import kafka.controller.KafkaController.AlterIsrCallback
import kafka.metrics.KafkaYammerMetrics
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{LogCaptureAppender, TestUtils}
import kafka.zk._
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{ControllerMovedException, NotEnoughReplicasException, StaleBrokerEpochException}
import org.apache.kafka.common.feature.Features
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.{ElectionType, TopicPartition, Uuid}
import org.apache.log4j.Level
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito.{doAnswer, spy, verify}
import org.mockito.invocation.InvocationOnMock

import java.util.Properties
import java.util.concurrent.{CompletableFuture, CountDownLatch, LinkedBlockingQueue, TimeUnit}
import scala.annotation.nowarn
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class ControllerIntegrationTest extends ZooKeeperTestHarness {
  var servers = Seq.empty[KafkaServer]
  val firstControllerEpoch = KafkaController.InitialControllerEpoch + 1
  val firstControllerEpochZkVersion = KafkaController.InitialControllerEpochZkVersion + 1

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    servers = Seq.empty[KafkaServer]
  }

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testEmptyCluster(): Unit = {
    servers = makeServers(1)
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "failed to elect a controller")
    waitUntilControllerEpoch(firstControllerEpoch, "broker failed to set controller epoch")
  }

  @Test
  def testControllerEpochPersistsWhenAllBrokersDown(): Unit = {
    servers = makeServers(1)
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "failed to elect a controller")
    waitUntilControllerEpoch(firstControllerEpoch, "broker failed to set controller epoch")
    servers.head.shutdown()
    servers.head.awaitShutdown()
    TestUtils.waitUntilTrue(() => !zkClient.getControllerId.isDefined, "failed to kill controller")
    waitUntilControllerEpoch(firstControllerEpoch, "controller epoch was not persisted after broker failure")
  }

  @Test
  def testControllerMoveIncrementsControllerEpoch(): Unit = {
    servers = makeServers(1)
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "failed to elect a controller")
    waitUntilControllerEpoch(firstControllerEpoch, "broker failed to set controller epoch")
    servers.head.shutdown()
    servers.head.awaitShutdown()
    servers.head.startup()
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "failed to elect a controller")
    waitUntilControllerEpoch(firstControllerEpoch + 1, "controller epoch was not incremented after controller move")
  }

  @Test
  def testMetadataPropagationOnControlPlane(): Unit = {
    servers = makeServers(1,
      listeners = Some("PLAINTEXT://localhost:0,CONTROLLER://localhost:0"),
      listenerSecurityProtocolMap = Some("PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"),
      controlPlaneListenerName = Some("CONTROLLER"))
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
    val controlPlaneMetricMap = mutable.Map[String, KafkaMetric]()
    val dataPlaneMetricMap = mutable.Map[String, KafkaMetric]()
    servers.head.metrics.metrics.values.forEach { kafkaMetric =>
      if (kafkaMetric.metricName.tags.values.contains("CONTROLLER")) {
        controlPlaneMetricMap.put(kafkaMetric.metricName().name(), kafkaMetric)
      }
      if (kafkaMetric.metricName.tags.values.contains("PLAINTEXT")) {
        dataPlaneMetricMap.put(kafkaMetric.metricName.name, kafkaMetric)
      }
    }
    assertEquals(1e-0, controlPlaneMetricMap("response-total").metricValue().asInstanceOf[Double], 0)
    assertEquals(0e-0, dataPlaneMetricMap("response-total").metricValue().asInstanceOf[Double], 0)
    assertEquals(1e-0, controlPlaneMetricMap("request-total").metricValue().asInstanceOf[Double], 0)
    assertEquals(0e-0, dataPlaneMetricMap("request-total").metricValue().asInstanceOf[Double], 0)
    assertTrue(controlPlaneMetricMap("incoming-byte-total").metricValue().asInstanceOf[Double] > 1.0)
    assertTrue(dataPlaneMetricMap("incoming-byte-total").metricValue().asInstanceOf[Double] == 0.0)
    assertTrue(controlPlaneMetricMap("network-io-total").metricValue().asInstanceOf[Double] == 2.0)
    assertTrue(dataPlaneMetricMap("network-io-total").metricValue().asInstanceOf[Double] == 0.0)
  }

  @Test
  def testSumOfTopicNameLength(): Unit = {
    servers = makeServers(1)
    val topic1 = "topic1"
    TestUtils.createTopic(zkClient, topic1, 1, 1, servers)
    val topic2 = "topic2"
    TestUtils.createTopic(zkClient, topic2, 1, 1, servers)

    TestUtils.waitUntilTrue(() => {
      val sumOfTopicNameLength = TestUtils.yammerMetricValue("SumOfTopicNameLength")
      topic1.size + topic2.size + 2 * KafkaController.topicNameBytesOverheadOnZk == sumOfTopicNameLength
    }, "all topics in the cluster " + zkClient.getAllTopicsInCluster())
  }

  // This test case is used to ensure that there will be no correctness issue after we avoid sending out full
  // UpdateMetadataRequest to all brokers in the cluster
  @Test
  def testMetadataPropagationOnBrokerChange(): Unit = {
    servers = makeServers(3)
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    // Need to make sure the broker we shutdown and startup are not the controller. Otherwise we will send out
    // full UpdateMetadataReuqest to all brokers during controller failover.
    val testBroker = servers.filter(e => e.config.brokerId != controllerId).head
    val remainingBrokers = servers.filter(_.config.brokerId != testBroker.config.brokerId)
    val topic = "topic1"
    // Make sure shutdown the test broker will not require any leadership change to test avoid sending out full
    // UpdateMetadataRequest on broker failure
    val assignment = Map(
      0 -> Seq(remainingBrokers(0).config.brokerId, testBroker.config.brokerId),
      1 -> remainingBrokers.map(_.config.brokerId))

    // Create topic
    TestUtils.createTopic(zkClient, topic, assignment, servers)

    // Shutdown the broker
    testBroker.shutdown()
    testBroker.awaitShutdown()
    TestUtils.waitUntilBrokerMetadataIsPropagated(remainingBrokers)
    remainingBrokers.foreach { server =>
      val offlineReplicaPartitionInfo = server.metadataCache.getPartitionInfo(topic, 0).get
      assertEquals(1, offlineReplicaPartitionInfo.offlineReplicas.size())
      assertEquals(testBroker.config.brokerId, offlineReplicaPartitionInfo.offlineReplicas.get(0))
      assertEquals(assignment(0).asJava, offlineReplicaPartitionInfo.replicas)
      assertEquals(Seq(remainingBrokers.head.config.brokerId).asJava, offlineReplicaPartitionInfo.isr)
      val onlinePartitionInfo = server.metadataCache.getPartitionInfo(topic, 1).get
      assertEquals(assignment(1).asJava, onlinePartitionInfo.replicas)
      assertTrue(onlinePartitionInfo.offlineReplicas.isEmpty)
    }

    // Startup the broker
    testBroker.startup()
    TestUtils.waitUntilTrue( () => {
      !servers.exists { server =>
        assignment.exists { case (partitionId, replicas) =>
          val partitionInfoOpt = server.metadataCache.getPartitionInfo(topic, partitionId)
          if (partitionInfoOpt.isDefined) {
            val partitionInfo = partitionInfoOpt.get
            !partitionInfo.offlineReplicas.isEmpty || !partitionInfo.replicas.asScala.equals(replicas)
          } else {
            true
          }
        }
      }
    }, "Inconsistent metadata after broker startup")
  }

  @Test
  def testMetadataPropagationForOfflineReplicas(): Unit = {
    servers = makeServers(3)
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)

    //get brokerId for topic creation with single partition and RF =1
    val replicaBroker = servers.filter(e => e.config.brokerId != controllerId).head

    val controllerBroker = servers.filter(e => e.config.brokerId == controllerId).head
    val otherBroker = servers.filter(e => e.config.brokerId != controllerId &&
      e.config.brokerId != replicaBroker.config.brokerId).head

    val topic = "topic1"
    val assignment = Map(0 -> Seq(replicaBroker.config.brokerId))

    // Create topic
    TestUtils.createTopic(zkClient, topic, assignment, servers)

    // Shutdown the other broker
    otherBroker.shutdown()
    otherBroker.awaitShutdown()

    // Shutdown the broker with replica
    replicaBroker.shutdown()
    replicaBroker.awaitShutdown()

    //Shutdown controller broker
    controllerBroker.shutdown()
    controllerBroker.awaitShutdown()

    def verifyMetadata(broker: KafkaServer): Unit = {
      broker.startup()
      TestUtils.waitUntilTrue(() => {
        val partitionInfoOpt = broker.metadataCache.getPartitionInfo(topic, 0)
        if (partitionInfoOpt.isDefined) {
          val partitionInfo = partitionInfoOpt.get
          (!partitionInfo.offlineReplicas.isEmpty && partitionInfo.leader == -1
            && !partitionInfo.replicas.isEmpty && !partitionInfo.isr.isEmpty)
        } else {
          false
        }
      }, "Inconsistent metadata after broker startup")
    }

    //Start controller broker and check metadata
    verifyMetadata(controllerBroker)

    //Start other broker and check metadata
    verifyMetadata(otherBroker)
  }

  @Test
  def testTopicCreation(): Unit = {
    servers = makeServers(1)
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(0))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
  }

  @Test
  def testTopicCreationWithOfflineReplica(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBrokerId, controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers.take(1))
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
  }

  /**
   * Tests that controller will fix insufficient RF topic by assigning sufficient replicas
   */
  @Test
  def testTopicCreationWithFixingRF(): Unit = {
    val topicRF1 = "test_topic_rf1"
    val partition = 0
    val defaultRF = 2
    val serverConfigs = TestUtils.createBrokerConfigs(3, zkConnect, enableControlledShutdown = false)
      .map(prop => {
        prop.setProperty(KafkaConfig.DefaultReplicationFactorProp, defaultRF.toString)
        prop.setProperty(KafkaConfig.CreateTopicPolicyClassNameProp, "kafka.server.LiCreateTopicPolicy")
        prop
      }).map(KafkaConfig.fromProps)
    servers = serverConfigs.map(s => TestUtils.createServer(s))

    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val assignment = Map(partition -> List(controllerId)) // topicRF1 original RF set to 1
    TestUtils.createTopic(zkClient, topicRF1, partitionReplicaAssignment = assignment, servers = servers, new Properties())

    waitForPartitionState(new TopicPartition(topicRF1, partition), firstControllerEpoch, zkClient.getAllBrokersInCluster.map(b => b.id),
      LeaderAndIsr.initialLeaderEpoch, "failed to get expected partition state upon valid RF topic creation")

    // Actual RF should be corrected to 2
    assertEquals(defaultRF, zkClient.getReplicasForPartition(new TopicPartition(topicRF1, partition)).size)
  }

  @Test
  def testTopicPartitionExpansion(): Unit = {
    servers = makeServers(1)
    val tp0 = new TopicPartition("t", 0)
    val tp1 = new TopicPartition("t", 1)
    val assignment = Map(tp0.partition -> Seq(0))
    val expandedAssignment = Map(
      tp0 -> ReplicaAssignment(Seq(0), Seq(), Seq()),
      tp1 -> ReplicaAssignment(Seq(0), Seq(), Seq()))
    TestUtils.createTopic(zkClient, tp0.topic, partitionReplicaAssignment = assignment, servers = servers)
    zkClient.setTopicAssignment(tp0.topic, Some(Uuid.randomUuid()), expandedAssignment, firstControllerEpochZkVersion)
    waitForPartitionState(tp1, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic partition expansion")
    TestUtils.waitForPartitionMetadata(servers, tp1.topic, tp1.partition)
  }

  @Test
  def testTopicPartitionExpansionWithOfflineReplica(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp0 = new TopicPartition("t", 0)
    val tp1 = new TopicPartition("t", 1)
    val assignment = Map(tp0.partition -> Seq(otherBrokerId, controllerId))
    val expandedAssignment = Map(
      tp0 -> ReplicaAssignment(Seq(otherBrokerId, controllerId), Seq(), Seq()),
      tp1 -> ReplicaAssignment(Seq(otherBrokerId, controllerId), Seq(), Seq()))
    TestUtils.createTopic(zkClient, tp0.topic, partitionReplicaAssignment = assignment, servers = servers)
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    zkClient.setTopicAssignment(tp0.topic, Some(Uuid.randomUuid()), expandedAssignment, firstControllerEpochZkVersion)
    waitForPartitionState(tp1, firstControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic partition expansion")
    TestUtils.waitForPartitionMetadata(Seq(servers(controllerId)), tp1.topic, tp1.partition)
  }

  @Test
  def testPartitionReassignment(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)

    val metricName = s"kafka.controller:type=ControllerStats,name=${ControllerState.AlterPartitionReassignment.rateAndTimeMetricName.get}"
    val timerCount = timer(metricName).count

    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    val reassignment = Map(tp -> ReplicaAssignment(Seq(otherBrokerId), List(), List()))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    zkClient.createPartitionReassignment(reassignment.map { case (k, v) => k -> v.replicas })
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch + 3,
      "failed to get expected partition state after partition reassignment")
    TestUtils.waitUntilTrue(() =>  zkClient.getFullReplicaAssignmentForTopics(Set(tp.topic)) == reassignment,
      "failed to get updated partition assignment on topic znode after partition reassignment")
    TestUtils.waitUntilTrue(() => !zkClient.reassignPartitionsInProgress,
      "failed to remove reassign partitions path after completion")

    val updatedTimerCount = timer(metricName).count
    assertTrue(updatedTimerCount > timerCount,
      s"Timer count $updatedTimerCount should be greater than $timerCount")
  }

  @Test
  def testPartitionReassignmentToBrokerWithOfflineLogDir(): Unit = {
    servers = makeServers(2, logDirCount = 2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)

    val metricName = s"kafka.controller:type=ControllerStats,name=${ControllerState.AlterPartitionReassignment.rateAndTimeMetricName.get}"
    val timerCount = timer(metricName).count

    val otherBroker = servers.filter(_.config.brokerId != controllerId).head
    val otherBrokerId = otherBroker.config.brokerId

    // To have an offline log dir, we need a topicPartition assigned to it
    val topicPartitionToPutOffline = new TopicPartition("filler", 0)
    TestUtils.createTopic(
      zkClient,
      topicPartitionToPutOffline.topic,
      partitionReplicaAssignment = Map(topicPartitionToPutOffline.partition -> Seq(otherBrokerId)),
      servers = servers
    )

    TestUtils.causeLogDirFailure(TestUtils.Checkpoint, otherBroker, topicPartitionToPutOffline)

    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    val reassignment = Map(tp -> ReplicaAssignment(Seq(otherBrokerId), List(), List()))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    zkClient.createPartitionReassignment(reassignment.map { case (k, v) => k -> v.replicas })
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch + 3,
      "with an offline log directory on the target broker, the partition reassignment stalls")
    TestUtils.waitUntilTrue(() =>  zkClient.getFullReplicaAssignmentForTopics(Set(tp.topic)) == reassignment,
      "failed to get updated partition assignment on topic znode after partition reassignment")
    TestUtils.waitUntilTrue(() => !zkClient.reassignPartitionsInProgress,
      "failed to remove reassign partitions path after completion")

    val updatedTimerCount = timer(metricName).count
    assertTrue(updatedTimerCount > timerCount,
      s"Timer count $updatedTimerCount should be greater than $timerCount")
  }

  @Test
  def testPartitionReassignmentWithOfflineReplicaHaltingProgress(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    val reassignment = Map(tp -> Seq(otherBrokerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    val controller = getController()
    zkClient.setOrCreatePartitionReassignment(reassignment, controller.kafkaController.controllerContext.epochZkVersion)
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch + 1,
      "failed to get expected partition state during partition reassignment with offline replica")
    TestUtils.waitUntilTrue(() => zkClient.reassignPartitionsInProgress,
      "partition reassignment path should remain while reassignment in progress")
  }

  @Test
  def testPartitionReassignmentResumesAfterReplicaComesOnline(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    val reassignment = Map(tp -> ReplicaAssignment(Seq(otherBrokerId), List(), List()))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    zkClient.createPartitionReassignment(reassignment.map { case (k, v) => k -> v.replicas })
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch + 1,
      "failed to get expected partition state during partition reassignment with offline replica")
    servers(otherBrokerId).startup()
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch + 4,
      "failed to get expected partition state after partition reassignment")
    TestUtils.waitUntilTrue(() => zkClient.getFullReplicaAssignmentForTopics(Set(tp.topic)) == reassignment,
      "failed to get updated partition assignment on topic znode after partition reassignment")
    TestUtils.waitUntilTrue(() => !zkClient.reassignPartitionsInProgress,
      "failed to remove reassign partitions path after completion")
  }

  @Test
  def testPreferredReplicaLeaderElection(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBroker = servers.find(_.config.brokerId != controllerId).get
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBroker.config.brokerId, controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    preferredReplicaLeaderElection(controllerId, otherBroker, tp, assignment(tp.partition).toSet, LeaderAndIsr.initialLeaderEpoch)
  }

  @Test
  def testBackToBackPreferredReplicaLeaderElections(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBroker = servers.find(_.config.brokerId != controllerId).get
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBroker.config.brokerId, controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    preferredReplicaLeaderElection(controllerId, otherBroker, tp, assignment(tp.partition).toSet, LeaderAndIsr.initialLeaderEpoch)
    preferredReplicaLeaderElection(controllerId, otherBroker, tp, assignment(tp.partition).toSet, LeaderAndIsr.initialLeaderEpoch + 2)
  }

  @Test
  def testPreferredReplicaLeaderElectionWithOfflinePreferredReplica(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBrokerId, controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    zkClient.createPreferredReplicaElection(Set(tp))
    TestUtils.waitUntilTrue(() => !zkClient.pathExists(PreferredReplicaElectionZNode.path),
      "failed to remove preferred replica leader election path after giving up")
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch + 1,
      "failed to get expected partition state upon broker shutdown")
  }

  @Test
  def testAutoPreferredReplicaLeaderElection(): Unit = {
    servers = makeServers(2, autoLeaderRebalanceEnable = true)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(1, 0))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch + 1,
      "failed to get expected partition state upon broker shutdown")
    servers(otherBrokerId).startup()
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch + 2,
      "failed to get expected partition state upon broker startup")
  }

  @Test
  def testTopicDeletionCleanUpPartitionState(): Unit = {
    servers = makeServers(3)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = servers.find(broker => broker.config.brokerId == controllerId).get.kafkaController
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(1, 0, 2))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)

    // Make sure the partition state has been populated
    assertTrue(controller.controllerContext.partitionStates.contains(tp))
    adminZkClient.deleteTopic(tp.topic())
    TestUtils.verifyTopicDeletion(zkClient, tp.topic(), 1, servers)

    // Make sure the partition state has been removed
    assertTrue(!controller.controllerContext.partitionStates.contains(tp))
  }

  @Test
  def testLeaderAndIsrWhenEntireIsrOfflineAndUncleanLeaderElectionDisabled(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBrokerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    TestUtils.waitUntilTrue(() => {
      val leaderIsrAndControllerEpochMap = zkClient.getTopicPartitionStates(Seq(tp))
      leaderIsrAndControllerEpochMap.contains(tp) &&
        isExpectedPartitionState(leaderIsrAndControllerEpochMap(tp), firstControllerEpoch, LeaderAndIsr.NoLeader, LeaderAndIsr.initialLeaderEpoch + 1) &&
        leaderIsrAndControllerEpochMap(tp).leaderAndIsr.isr == List(otherBrokerId)
    }, "failed to get expected partition state after entire isr went offline")
  }

  @Test
  def testLeaderAndIsrWhenEntireIsrOfflineAndUncleanLeaderElectionEnabled(): Unit = {
    servers = makeServers(2, uncleanLeaderElectionEnable = true)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBrokerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    TestUtils.waitUntilTrue(() => {
      val leaderIsrAndControllerEpochMap = zkClient.getTopicPartitionStates(Seq(tp))
      leaderIsrAndControllerEpochMap.contains(tp) &&
        isExpectedPartitionState(leaderIsrAndControllerEpochMap(tp), firstControllerEpoch, LeaderAndIsr.NoLeader, LeaderAndIsr.initialLeaderEpoch + 1) &&
        leaderIsrAndControllerEpochMap(tp).leaderAndIsr.isr == List(otherBrokerId)
    }, "failed to get expected partition state after entire isr went offline")
  }

  @Test
  def testControllerChangeDoesNotPutReplicasOffline(): Unit = {
    val expectedReplicaAssignment = Map(0  -> List(0, 1))
    val topic = "test"
    val partition = 0

    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(3, zkConnect, enableControlledShutdown = false)
      .map { props => {
        props.setProperty(KafkaConfig.ControlledShutdownMaxRetriesProp, "2147483640")
        KafkaConfig.fromProps(props)
      }
      }
    servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    // create the topic
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers)
    assertTrue(servers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.leader == 0))

    var controllerId = zkClient.getControllerId.get
    var controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController

    val broker1 = servers.find(_.config.brokerId == 1).get
    broker1.shutdown()

    val activeServers = servers.filter(s => s != broker1)
    // wait for the update metadata request to trickle to the brokers
    TestUtils.waitUntilTrue(() =>
      activeServers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.isr.size == 1),
      "ISR did not get reduced after controlled shutdown of broker 1")

    controllerId = zkClient.getControllerId.get
    controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController

    val resultQueue = new LinkedBlockingQueue[Try[collection.Set[TopicPartition]]]()
    val controlledShutdownCallback = (controlledShutdownResult: Try[collection.Set[TopicPartition]]) => resultQueue.put(controlledShutdownResult)

    controller.controlledShutdown(0, servers.find(_.config.brokerId == 0).get.kafkaController.brokerEpoch, controlledShutdownCallback)

    var partitionsRemaining = resultQueue.take().get
    assertEquals(1, partitionsRemaining.size)
    // leader doesn't change since all the other replicas are shut down
    assertTrue(servers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.leader == 0))

  // Now ensure that after the controller moves, shutdown is still rejected.
    zkClient.deleteController(controller.controllerContext.epochZkVersion)
    TestUtils.waitUntilTrue(() => !controller.isActive, "Controller fails to resign")
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "New controller failed to start")

    val newControllerId = zkClient.getControllerId.get
    val newController = servers.find(p => p.config.brokerId == newControllerId).get.kafkaController

    newController.controlledShutdown(0, servers.find(_.config.brokerId == 0).get.kafkaController.brokerEpoch, controlledShutdownCallback)
    partitionsRemaining = resultQueue.take().get
    assertEquals(1, partitionsRemaining.size)
    // leader should still not change
    assertTrue(servers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.leader == 0))
  }

  @Test
  def testControlledShutdown(): Unit = {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    val partition = 0
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false).map(KafkaConfig.fromProps)
    servers = serverConfigs.reverse.map(s => TestUtils.createServer(s))
    // create the topic
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers)

    val controllerId = zkClient.getControllerId.get
    val controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
    val resultQueue = new LinkedBlockingQueue[Try[collection.Set[TopicPartition]]]()
    val controlledShutdownCallback = (controlledShutdownResult: Try[collection.Set[TopicPartition]]) => resultQueue.put(controlledShutdownResult)
    controller.controlledShutdown(2, servers.find(_.config.brokerId == 2).get.kafkaController.brokerEpoch, controlledShutdownCallback)
    var partitionsRemaining = resultQueue.take().get
    var activeServers = servers.filter(s => s.config.brokerId != 2)
    // wait for the update metadata request to trickle to the brokers
    TestUtils.waitUntilTrue(() =>
      activeServers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.isr.size != 3),
      "Topic test not created after timeout")
    assertEquals(0, partitionsRemaining.size)
    var partitionStateInfo = activeServers.head.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get
    var leaderAfterShutdown = partitionStateInfo.leader
    assertEquals(0, leaderAfterShutdown)
    assertEquals(2, partitionStateInfo.isr.size)
    assertEquals(List(0,1), partitionStateInfo.isr.asScala)
    controller.controlledShutdown(1, servers.find(_.config.brokerId == 1).get.kafkaController.brokerEpoch, controlledShutdownCallback)
    partitionsRemaining = resultQueue.take() match {
      case Success(partitions) => partitions
      case Failure(exception) => throw new AssertionError("Controlled shutdown failed due to error", exception)
    }
    assertEquals(0, partitionsRemaining.size)
    activeServers = servers.filter(s => s.config.brokerId == 0)
    partitionStateInfo = activeServers.head.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get
    leaderAfterShutdown = partitionStateInfo.leader
    assertEquals(0, leaderAfterShutdown)

    assertTrue(servers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.leader == 0))
    controller.controlledShutdown(0, servers.find(_.config.brokerId == 0).get.kafkaController.brokerEpoch, controlledShutdownCallback)
    partitionsRemaining = resultQueue.take().get
    assertEquals(1, partitionsRemaining.size)
    // leader doesn't change since all the replicas are shut down
    assertTrue(servers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.leader == 0))
  }

  @Test
  def testControlledShutdownRejectRequestForAvailabilityRisk(): Unit = {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2, 3))
    val topic = "test"
    val partition = 0
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(4, zkConnect, false, enableControlledShutdownSafetyCheck = true)
      .map(KafkaConfig.fromProps)
    servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    TestUtils.waitUntilControllerElected(zkClient)

    // create the topic with min ISR of 2, which should allow one broker to shut down but should block subsequent
    // shutdowns.
    val topicConfig = new Properties()
    topicConfig.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers, topicConfig = topicConfig)

    val controllerId = zkClient.getControllerId.get
    val controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
    val resultQueue = new LinkedBlockingQueue[Try[collection.Set[TopicPartition]]]()

    // The controller should record the min.insync.replicas config for this newly created topic.
    TestUtils.waitUntilTrue(() => controller.controllerContext.topicMinIsrConfig.getOrElse(topic, -1) == 2, "Controller never saw min.insync.replicas config change for topic.")

    // Attempt to shut down one broker, which should be allowed with the ISR at 3 and the min ISR of 2
    val server2 = servers.find(s => s.config.brokerId == 2).get
    val controlledShutdownCallback = (controlledShutdownResult: Try[collection.Set[TopicPartition]]) => resultQueue.put(controlledShutdownResult)
    controller.controlledShutdown(2, server2.kafkaController.brokerEpoch, controlledShutdownCallback)
    server2.shutdown()

    var partitionsRemaining = resultQueue.take().get
    var activeServers = servers.filterNot(_ == server2)
    // wait for the update metadata request to trickle to the brokers
    TestUtils.waitUntilTrue(() =>
      activeServers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.isr.size != 4),
      "ISR did not get updated on all brokers after timeout")
    assertEquals(0, partitionsRemaining.size)
    var partitionStateInfo = activeServers.head.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get
    var leaderAfterShutdown = partitionStateInfo.leader
    assertTrue(Set(0, 1, 3).contains(leaderAfterShutdown))
    assertEquals(3, partitionStateInfo.isr.size)
    assertEquals(List(0, 1, 3), partitionStateInfo.isr.asScala)

    // Now attempt to shut down a second broker which should fail with NotEnoughReplicasException
    @volatile var notEnoughReplicasDetected = false
    controller.controlledShutdown(1, servers.find(_.config.brokerId == 1).get.kafkaController.brokerEpoch, {
      case scala.util.Failure(exception) if exception.isInstanceOf[NotEnoughReplicasException] => notEnoughReplicasDetected = true
      case _ =>
    })

    TestUtils.waitUntilTrue(() => notEnoughReplicasDetected, "Fail to detect expected NotEnoughReplicasException")
    val shutdownEpoch = zkClient.getBrokerShutdownEntries.get(2).get
    assertTrue(shutdownEpoch > 0)

    // Now ensure that after the controller moves, shutdown is still rejected.
    zkClient.deleteController(controller.controllerContext.epochZkVersion)
    TestUtils.waitUntilTrue(() => !controller.isActive, "Controller fails to resign")
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "New controller failed to start")

    @volatile var newControllerId = zkClient.getControllerId.get
    @volatile var newController = servers.find(p => p.config.brokerId == newControllerId).get.kafkaController

    // The controller should record the min.insync.replicas config for this newly created topic since the controller
    // will scan topic configurations on failover.
    TestUtils.waitUntilTrue(() => newController.controllerContext.topicMinIsrConfig.getOrElse(topic, -1) == 2, "Controller never saw min.insync.replicas config change for topic.")

    // Controller moved, try to shut down again. We should get rejected in the same way.
    notEnoughReplicasDetected = false
    newController.controlledShutdown(1, servers.find(_.config.brokerId == 1).get.kafkaController.brokerEpoch, {
      case scala.util.Failure(exception) if exception.isInstanceOf[NotEnoughReplicasException] => notEnoughReplicasDetected = true
      case _ =>
    })
    TestUtils.waitUntilTrue(() => notEnoughReplicasDetected, "Fail to detect expected NotEnoughReplicasException")
    assertEquals(shutdownEpoch, zkClient.getBrokerShutdownEntries.get(2).get)

    // Tell the controller to skip shutdown for this brokerEpoch, simulating LiControlledShutdownSkipSafetyCheckRequest.
    // After that the controller should allow the shutdown that it just rejected.
    @volatile var skipSafetyCheckSucceeded = false
    newController.skipControlledShutdownSafetyCheck(1, servers.find(_.config.brokerId == 1).get.kafkaController.brokerEpoch, {
      case scala.util.Failure(exception) =>
      case _ => skipSafetyCheckSucceeded = true
    })
    TestUtils.waitUntilTrue(() => skipSafetyCheckSucceeded, "Failed to skip shutdown safety check")

    // Now shutting down broker id 1 should succeed.
    val server1 = servers.find(s => s.config.brokerId == 1).get
    newController.controlledShutdown(1, server1.kafkaController.brokerEpoch, controlledShutdownCallback)
    server1.shutdown()
    // update the newController since it might have switched from broker 1 who has just shut down
    newControllerId = zkClient.getControllerId.get
    newController = servers.find(p => p.config.brokerId == newControllerId).get.kafkaController

    partitionsRemaining = resultQueue.take().get
    activeServers = activeServers.filterNot(server => server == server1)
    // wait for the update metadata request to trickle to the brokers
    TestUtils.waitUntilTrue(() =>
      activeServers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.isr.size != 3),
      "ISR did not get updated on all brokers after timeout")
    assertEquals(0, partitionsRemaining.size)
    partitionStateInfo = activeServers.head.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get
    leaderAfterShutdown = partitionStateInfo.leader
    assertEquals(0, leaderAfterShutdown)
    assertEquals(2, partitionStateInfo.isr.size)
    assertEquals(List(0, 3), partitionStateInfo.isr.asScala)

    // The controller should see topic configuration changes.
    val adminClient = createAdminClient();
    try{
      val updatedTopicConfig = new Properties()
      updatedTopicConfig.setProperty(KafkaConfig.MinInSyncReplicasProp, "1")
      alterTopicConfigs(adminClient, topic, updatedTopicConfig)
    } finally {
      adminClient.close()
    }
    TestUtils.waitUntilTrue(() => newController.controllerContext.topicMinIsrConfig.getOrElse(topic, -1) == 1, "Controller never saw min.insync.replicas config change for topic.")
  }

  // This test is to verify that skipping the shutdown check would allow the broker to shutdown
  // even though it may lead to offline partitions
  @Test
  def testSkipShutdownCheck(): Unit = {
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(3, zkConnect, true, enableControlledShutdownSafetyCheck = true)
      .map{props => {
        props.setProperty(KafkaConfig.ControlledShutdownMaxRetriesProp, Integer.MAX_VALUE.toString)
        props
      }}
      .map(KafkaConfig.fromProps)
    // create the servers in reverse order so that broker 2 becomes the controller
    servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    assertTrue(controllerId == 2)
    val controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController

    val topicConfig = new Properties()
    val expectedReplicaAssignment = Map(0  -> List(0, 1))
    val topic = "test"
    val tp = new TopicPartition(topic, 0)
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers, topicConfig = topicConfig)

    // skip shutdown safety check for all data brokers
    val dataBrokers = servers.filterNot(_.config.brokerId == controllerId)

    val brokerIdAndEpochs = dataBrokers.map{s => (s.config.brokerId, s.kafkaController.brokerEpoch)}
    for (brokerEntry <- brokerIdAndEpochs) {
      @volatile var skipSafetyCheckSucceeded = false
      controller.skipControlledShutdownSafetyCheck(brokerEntry._1, brokerEntry._2, {
        case scala.util.Failure(t) => throw t
        case _ => skipSafetyCheckSucceeded = true
      })
      TestUtils.waitUntilTrue(() => skipSafetyCheckSucceeded, s"Failed to skip shutdown safety check for $brokerEntry")
    }
    dataBrokers.foreach {s => s.shutdown()}

    // verify that the partition has become Offline
    TestUtils.waitUntilTrue(() => {
      controller.controllerContext.partitionState(tp) == OfflinePartition
    }, s"the partition $tp does not become offline after all replicas are shutdown")
  }

  @nowarn("cat=deprecation")
  private def alterTopicConfigs(adminClient: Admin, topic: String, topicConfigs: Properties): AlterConfigsResult = {
    val configEntries = topicConfigs.asScala.map { case (k, v) => new ConfigEntry(k, v) }.toList.asJava
    val newConfig = new Config(configEntries)
    val configs = Map(new ConfigResource(ConfigResource.Type.TOPIC, topic) -> newConfig).asJava
    adminClient.alterConfigs(configs)
  }

  /**
   * Test that rolling controlled shutdown of several brokers can be allowed by the availability safety check
   */
  @Test
  def testRollingControlledShutdown(): Unit = {
    val expectedReplicaAssignment = Map(0  -> List(0, 1))
    val topic = "test"
    val partition = 0
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(2, zkConnect, enableControlledShutdown = true, enableControlledShutdownSafetyCheck = true)
      .map{props => {
        props.setProperty(KafkaConfig.ControlledShutdownMaxRetriesProp, "2147483640")
        props.setProperty(KafkaConfig.ControlledShutdownSafetyCheckRedundancyFactorProp, "0")
        KafkaConfig.fromProps(props)
      }
      }

    servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    TestUtils.waitUntilControllerElected(zkClient)

    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers)
    TestUtils.waitUntilTrue(() =>
      servers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic, partition).exists(_.isr.size == 2)),
      "Topic test not created after timeout")

    // Attempt to shut down and then restart broker1
    val broker1 = servers.filter(s => s.config.brokerId == 1).head
    broker1.shutdown()
    var activeServers = servers.filter(s => s.config.brokerId != 1)
    TestUtils.waitUntilTrue(() =>
      activeServers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).exists(_.isr.size != 2)),
      "Broker 1 has not been removed from ISR of all other brokers")

    broker1.startup()
    TestUtils.waitUntilTrue(() =>
      servers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic, partition).exists(_.isr.size == 2)),
      "The ISR does not include the full set after the offline broker is restarted")

    // now try to shutdown broker 0
    val broker0 = servers.filter(s => s.config.brokerId == 0).head
    broker0.shutdown()
    activeServers = Seq(broker1)
    TestUtils.waitUntilTrue(() =>
      activeServers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).exists(_.isr.size != 2)),
      "Topic test not created after timeout")


    // The test has completed. In order for both brokers to be shutdown, one option is to delete the topic
    adminZkClient.deleteTopic(topic)
    val controllerId = zkClient.getControllerId.get
    val controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
    TestUtils.waitUntilTrue(() => controller.topicDeletionManager.isTopicQueuedUpForDeletion(topic),
      s"Deletion of the topic $topic never happened")
  }

  @Test
  def testTopicsBeingDeletedDoesNotBlockShutdown(): Unit = {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2, 3))
    val topic = "test"
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(4, zkConnect, true, enableControlledShutdownSafetyCheck = true)
      .map(props => {
        props.put(KafkaConfig.ControlledShutdownMaxRetriesProp, Int.MaxValue.toString)
        KafkaConfig.fromProps(props)
      })
    servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
    TestUtils.waitUntilControllerElected(zkClient)

    // create the topic with min ISR of 2, which should allow one broker to shut down but should block subsequent
    // shutdowns unless the topic is deleted.
    val topicConfig = new Properties()
    topicConfig.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = expectedReplicaAssignment, servers = servers, topicConfig = topicConfig)

    // shutdown one broker so that the topic deletion can be queued but not completed
    val broker2 = servers.find(_.config.brokerId == 2).get
    broker2.shutdown()

    // delete one topic
    adminZkClient.deleteTopic(topic)
    val controllerId = zkClient.getControllerId.get
    val controller = servers.find(p => p.config.brokerId == controllerId).get.kafkaController
    TestUtils.waitUntilTrue(() => controller.topicDeletionManager.isTopicQueuedUpForDeletion(topic),
      s"Deletion of the topic $topic never happened")

    // test that a 2nd broker can still shutdown after the topic is being queued for deletion
    val broker1 = servers.find(_.config.brokerId == 1).get
    broker1.shutdown()
  }

  @Test
  def testControllerRejectControlledShutdownRequestWithStaleBrokerEpoch(): Unit = {
    // create brokers
    val serverConfigs = TestUtils.createBrokerConfigs(2, zkConnect, false).map(KafkaConfig.fromProps)
    servers = serverConfigs.reverse.map(s => TestUtils.createServer(s))

    val controller = getController().kafkaController
    val otherBroker = servers.find(e => e.config.brokerId != controller.config.brokerId).get
    @volatile var staleBrokerEpochDetected = false
    controller.controlledShutdown(otherBroker.config.brokerId, otherBroker.kafkaController.brokerEpoch - 1, {
      case scala.util.Failure(exception) if exception.isInstanceOf[StaleBrokerEpochException] => staleBrokerEpochDetected = true
      case _ =>
    })

    TestUtils.waitUntilTrue(() => staleBrokerEpochDetected, "Fail to detect stale broker epoch")
  }

  @Test
  def testControllerMoveOnTopicCreation(): Unit = {
    servers = makeServers(1)
    TestUtils.waitUntilControllerElected(zkClient)
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(0))

    testControllerMove(() => {
      val adminZkClient = new AdminZkClient(zkClient)
      adminZkClient.createTopicWithAssignment(tp.topic, config = new Properties(), assignment)
    })
  }

  @Test
  def testControllerMoveOnTopicDeletion(): Unit = {
    servers = makeServers(1)
    TestUtils.waitUntilControllerElected(zkClient)
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(0))
    TestUtils.createTopic(zkClient, tp.topic(), assignment, servers)

    testControllerMove(() => {
      val adminZkClient = new AdminZkClient(zkClient)
      adminZkClient.deleteTopic(tp.topic())
    })
  }

  @Test
  def testControllerMoveOnPreferredReplicaElection(): Unit = {
    servers = makeServers(1)
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(0))
    TestUtils.createTopic(zkClient, tp.topic(), assignment, servers)

    testControllerMove(() => zkClient.createPreferredReplicaElection(Set(tp)))
  }

  @Test
  def testControllerMoveOnPartitionReassignment(): Unit = {
    servers = makeServers(1)
    TestUtils.waitUntilControllerElected(zkClient)
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(0))
    TestUtils.createTopic(zkClient, tp.topic(), assignment, servers)

    val reassignment = Map(tp -> Seq(0))
    testControllerMove(() => zkClient.createPartitionReassignment(reassignment))
  }

  @Test
  def testControllerFeatureZNodeSetupWhenFeatureVersioningIsEnabledWithNonExistingFeatureZNode(): Unit = {
    testControllerFeatureZNodeSetup(Option.empty, KAFKA_2_7_IV0)
  }

  @Test
  def testControllerFeatureZNodeSetupWhenFeatureVersioningIsEnabledWithDisabledExistingFeatureZNode(): Unit = {
    testControllerFeatureZNodeSetup(Some(new FeatureZNode(FeatureZNodeStatus.Disabled, Features.emptyFinalizedFeatures())), KAFKA_2_7_IV0)
  }

  @Test
  def testControllerFeatureZNodeSetupWhenFeatureVersioningIsEnabledWithEnabledExistingFeatureZNode(): Unit = {
    testControllerFeatureZNodeSetup(Some(new FeatureZNode(FeatureZNodeStatus.Enabled, Features.emptyFinalizedFeatures())), KAFKA_2_7_IV0)
  }

  @Test
  def testControllerFeatureZNodeSetupWhenFeatureVersioningIsDisabledWithNonExistingFeatureZNode(): Unit = {
    testControllerFeatureZNodeSetup(Option.empty, KAFKA_2_6_IV0)
  }

  @Test
  def testControllerFeatureZNodeSetupWhenFeatureVersioningIsDisabledWithDisabledExistingFeatureZNode(): Unit = {
    testControllerFeatureZNodeSetup(Some(new FeatureZNode(FeatureZNodeStatus.Disabled, Features.emptyFinalizedFeatures())), KAFKA_2_6_IV0)
  }

  @Test
  def testControllerFeatureZNodeSetupWhenFeatureVersioningIsDisabledWithEnabledExistingFeatureZNode(): Unit = {
    testControllerFeatureZNodeSetup(Some(new FeatureZNode(FeatureZNodeStatus.Enabled, Features.emptyFinalizedFeatures())), KAFKA_2_6_IV0)
  }

  @Test
  def testControllerDetectsBouncedBrokers(): Unit = {
    servers = makeServers(2, enableControlledShutdown = false)
    val controller = getController().kafkaController
    val otherBroker = servers.find(e => e.config.brokerId != controller.config.brokerId).get

    // Create a topic
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(0, 1))

    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")

    // Wait until the event thread is idle
    TestUtils.waitUntilTrue(() => {
      controller.eventManager.state == ControllerState.Idle
    }, "Controller event thread is still busy")

    val latch = new CountDownLatch(1)

    // Let the controller event thread await on a latch until broker bounce finishes.
    // This is used to simulate fast broker bounce

    controller.eventManager.put(new MockEvent(ControllerState.TopicChange) {
      override def process(): Unit = latch.await()
      override def preempt(): Unit = {}
    })

    otherBroker.shutdown()
    otherBroker.startup()

    assertEquals(0, otherBroker.replicaManager.partitionCount.value())

    // Release the latch so that controller can process broker change event
    latch.countDown()
    TestUtils.waitUntilTrue(() => {
      otherBroker.replicaManager.partitionCount.value() == 1 &&
      otherBroker.replicaManager.metadataCache.getAllTopics().size == 1 &&
      otherBroker.replicaManager.metadataCache.getAliveBrokers().size == 2
    }, "Broker fail to initialize after restart")
  }

  @Test
  def testPreemptionOnControllerShutdown(): Unit = {
    servers = makeServers(1, enableControlledShutdown = false)
    val controller = getController().kafkaController
    var count = 2
    val latch = new CountDownLatch(1)
    val spyThread = spy(controller.eventManager.thread)
    controller.eventManager.thread = spyThread
    val processedEvent = new MockEvent(ControllerState.TopicChange) {
      override def process(): Unit = latch.await()
      override def preempt(): Unit = {}
    }
    val preemptedEvent = new MockEvent(ControllerState.TopicChange) {
      override def process(): Unit = {}
      override def preempt(): Unit = count -= 1
    }

    controller.eventManager.put(processedEvent)
    controller.eventManager.put(preemptedEvent)
    controller.eventManager.put(preemptedEvent)

    doAnswer((_: InvocationOnMock) => {
      latch.countDown()
    }).doCallRealMethod().when(spyThread).awaitShutdown()
    controller.shutdown()
    TestUtils.waitUntilTrue(() => {
      count == 0
    }, "preemption was not fully completed before shutdown")

    verify(spyThread).awaitShutdown()
  }

  @Test
  def testPreemptionWithCallbacks(): Unit = {
    servers = makeServers(1, enableControlledShutdown = false)
    val controller = getController().kafkaController
    val latch = new CountDownLatch(1)
    val spyThread = spy(controller.eventManager.thread)
    controller.eventManager.thread = spyThread
    val processedEvent = new MockEvent(ControllerState.TopicChange) {
      override def process(): Unit = latch.await()
      override def preempt(): Unit = {}
    }
    val tp0 = new TopicPartition("t", 0)
    val tp1 = new TopicPartition("t", 1)
    val partitions = Set(tp0, tp1)
    val event1 = ReplicaLeaderElection(Some(partitions), Some(Map.empty), ElectionType.PREFERRED, ZkTriggered, topResult => {
      topResult match {
        case Right(error) =>
        case Left(partitionsMap) => // To make the rebasing with upstream kafka easier, we don't indent the following block
      for (partition <- partitionsMap) {
        partition._2 match {
          case Left(e) => assertEquals(Errors.NOT_CONTROLLER, e.error())
          case Right(_) => throw new AssertionError("replica leader election should error")
        }
      }
      }
    })
    val event2 = ControlledShutdown(0, 0, {
      case Success(_) => throw new AssertionError("controlled shutdown should error")
      case Failure(e) =>
        assertEquals(classOf[ControllerMovedException], e.getClass)
    })
    val event3  = ApiPartitionReassignment(Map(tp0 -> None, tp1 -> None), {
      case Left(_) => throw new AssertionError("api partition reassignment should error")
      case Right(e) => assertEquals(Errors.NOT_CONTROLLER, e.error())
    })
    val event4 = ListPartitionReassignments(Some(partitions), {
      case Left(_) => throw new AssertionError("api partition reassignment should error")
      case Right(e) => assertEquals(Errors.NOT_CONTROLLER, e.error())
    })

    controller.eventManager.put(processedEvent)
    controller.eventManager.put(event1)
    controller.eventManager.put(event2)
    controller.eventManager.put(event3)
    controller.eventManager.put(event4)

    doAnswer((_: InvocationOnMock) => {
      latch.countDown()
    }).doCallRealMethod().when(spyThread).awaitShutdown()
    controller.shutdown()
  }

  private def testControllerFeatureZNodeSetup(initialZNode: Option[FeatureZNode],
                                              interBrokerProtocolVersion: ApiVersion): Unit = {
    val versionBeforeOpt = initialZNode match {
      case Some(node) =>
        zkClient.createFeatureZNode(node)
        Some(zkClient.getDataAndVersion(FeatureZNode.path)._2)
      case None =>
        Option.empty
    }
    servers = makeServers(1, interBrokerProtocolVersion = Some(interBrokerProtocolVersion))
    TestUtils.waitUntilControllerElected(zkClient)
    // Below we wait on a dummy event to finish processing in the controller event thread.
    // We schedule this dummy event only after the controller is elected, which is a sign that the
    // controller has already started processing the Startup event. Waiting on the dummy event is
    // used to make sure that the event thread has completed processing Startup event, that triggers
    // the setup of FeatureZNode.
    val controller = getController().kafkaController
    val latch = new CountDownLatch(1)
    controller.eventManager.put(new MockEvent(ControllerState.TopicChange) {
      override def process(): Unit = {
        latch.countDown()
      }
      override def preempt(): Unit = {}
    })
    latch.await()

    val (mayBeFeatureZNodeBytes, versionAfter) = zkClient.getDataAndVersion(FeatureZNode.path)
    val newZNode = FeatureZNode.decode(mayBeFeatureZNodeBytes.get)
    if (interBrokerProtocolVersion >= KAFKA_2_7_IV0) {
      val emptyZNode = new FeatureZNode(FeatureZNodeStatus.Enabled, Features.emptyFinalizedFeatures)
      initialZNode match {
        case Some(node) => {
          node.status match {
            case FeatureZNodeStatus.Enabled =>
              assertEquals(versionBeforeOpt.get, versionAfter)
              assertEquals(node, newZNode)
            case FeatureZNodeStatus.Disabled =>
              assertEquals(versionBeforeOpt.get + 1, versionAfter)
              assertEquals(emptyZNode, newZNode)
          }
        }
        case None =>
          assertEquals(0, versionAfter)
          assertEquals(new FeatureZNode(FeatureZNodeStatus.Enabled, Features.emptyFinalizedFeatures), newZNode)
      }
    } else {
      val emptyZNode = new FeatureZNode(FeatureZNodeStatus.Disabled, Features.emptyFinalizedFeatures)
      initialZNode match {
        case Some(node) => {
          node.status match {
            case FeatureZNodeStatus.Enabled =>
              assertEquals(versionBeforeOpt.get + 1, versionAfter)
              assertEquals(emptyZNode, newZNode)
            case FeatureZNodeStatus.Disabled =>
              assertEquals(versionBeforeOpt.get, versionAfter)
              assertEquals(emptyZNode, newZNode)
          }
        }
        case None =>
          assertEquals(0, versionAfter)
          assertEquals(new FeatureZNode(FeatureZNodeStatus.Disabled, Features.emptyFinalizedFeatures), newZNode)
      }
    }
  }

  @Test
  def testIdempotentAlterIsr(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBroker = servers.find(_.config.brokerId != controllerId).get
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBroker.config.brokerId, controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)

    val latch = new CountDownLatch(1)
    val controller = getController().kafkaController

    val leaderIsrAndControllerEpochMap = zkClient.getTopicPartitionStates(Seq(tp))
    val newLeaderAndIsr = leaderIsrAndControllerEpochMap(tp).leaderAndIsr

    val callback = (result: Either[Map[TopicPartition, Either[Errors, LeaderAndIsr]], Errors]) => {
      result match {
        case Left(partitionResults: Map[TopicPartition, Either[Errors, LeaderAndIsr]]) =>
          partitionResults.get(tp) match {
            case Some(Left(error: Errors)) => throw new AssertionError(s"Should not have seen error for $tp")
            case Some(Right(leaderAndIsr: LeaderAndIsr)) => assertEquals(leaderAndIsr, newLeaderAndIsr, "ISR should remain unchanged")
            case None => throw new AssertionError(s"Should have seen $tp in result")
          }
        case Right(_: Errors) => throw new AssertionError("Should not have had top-level error here")
      }
      latch.countDown()
    }

    val brokerEpoch = controller.controllerContext.liveBrokerIdAndEpochs.get(otherBroker.config.brokerId).get
    // When re-sending the current ISR, we should not get and error or any ISR changes
    controller.eventManager.put(AlterIsrReceived(otherBroker.config.brokerId, brokerEpoch, Map(tp -> newLeaderAndIsr), callback))
    latch.await()
  }

  @Test
  def testAlterIsrErrors(): Unit = {
    servers = makeServers(1)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    val controller = getController().kafkaController
    var future = captureAlterIsrError(controllerId, controller.brokerEpoch - 1,
      Map(tp -> LeaderAndIsr(controllerId, List(controllerId))))
    var capturedError = future.get(5, TimeUnit.SECONDS)
    assertEquals(Errors.STALE_BROKER_EPOCH, capturedError)

    future = captureAlterIsrError(99, controller.brokerEpoch,
      Map(tp -> LeaderAndIsr(controllerId, List(controllerId))))
    capturedError = future.get(5, TimeUnit.SECONDS)
    assertEquals(Errors.STALE_BROKER_EPOCH, capturedError)

    val unknownTopicPartition = new TopicPartition("unknown", 99)
    future = captureAlterIsrPartitionError(controllerId, controller.brokerEpoch,
      Map(unknownTopicPartition -> LeaderAndIsr(controllerId, List(controllerId))), unknownTopicPartition)
    capturedError = future.get(5, TimeUnit.SECONDS)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, capturedError)

    future = captureAlterIsrPartitionError(controllerId, controller.brokerEpoch,
      Map(tp -> LeaderAndIsr(controllerId, 1, List(controllerId), 99)), tp)
    capturedError = future.get(5, TimeUnit.SECONDS)
    assertEquals(Errors.INVALID_UPDATE_VERSION, capturedError)
  }

  def captureAlterIsrError(brokerId: Int, brokerEpoch: Long, isrsToAlter: Map[TopicPartition, LeaderAndIsr]): CompletableFuture[Errors] = {
    val future = new CompletableFuture[Errors]()
    val controller = getController().kafkaController
    val callback: AlterIsrCallback = {
      case Left(_: Map[TopicPartition, Either[Errors, LeaderAndIsr]]) =>
        future.completeExceptionally(new AssertionError(s"Should have seen top-level error"))
      case Right(error: Errors) =>
        future.complete(error)
    }
    controller.eventManager.put(AlterIsrReceived(brokerId, brokerEpoch, isrsToAlter, callback))
    future
  }

  def captureAlterIsrPartitionError(brokerId: Int, brokerEpoch: Long, isrsToAlter: Map[TopicPartition, LeaderAndIsr], tp: TopicPartition): CompletableFuture[Errors] = {
    val future = new CompletableFuture[Errors]()
    val controller = getController().kafkaController
    val callback: AlterIsrCallback = {
      case Left(partitionResults: Map[TopicPartition, Either[Errors, LeaderAndIsr]]) =>
        partitionResults.get(tp) match {
          case Some(Left(error: Errors)) => future.complete(error)
          case Some(Right(_: LeaderAndIsr)) => future.completeExceptionally(new AssertionError(s"Should have seen an error for $tp in result"))
          case None => future.completeExceptionally(new AssertionError(s"Should have seen $tp in result"))
        }
      case Right(_: Errors) =>
        future.completeExceptionally(new AssertionError(s"Should not seen top-level error"))
    }
    controller.eventManager.put(AlterIsrReceived(brokerId, brokerEpoch, isrsToAlter, callback))
    future
  }

  @Test
  def testTopicIdsAreAdded(): Unit = {
    servers = makeServers(1)
    TestUtils.waitUntilControllerElected(zkClient)
    val controller = getController().kafkaController
    val tp1 = new TopicPartition("t1", 0)
    val assignment1 = Map(tp1.partition -> Seq(0))

    // Before adding the topic, an attempt to get the ID should result in None.
    assertEquals(None, controller.controllerContext.topicIds.get("t1"))

    TestUtils.createTopic(zkClient, tp1.topic(), assignment1, servers)

    // Test that the first topic has its ID added correctly
    waitForPartitionState(tp1, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    assertNotEquals(None, controller.controllerContext.topicIds.get("t1"))
    val topicId1 = controller.controllerContext.topicIds("t1")
    assertEquals("t1", controller.controllerContext.topicNames(topicId1))

    val tp2 = new TopicPartition("t2", 0)
    val assignment2 = Map(tp2.partition -> Seq(0))
    TestUtils.createTopic(zkClient, tp2.topic(), assignment2, servers)

    // Test that the second topic has its ID added correctly
    waitForPartitionState(tp2, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    assertNotEquals(None, controller.controllerContext.topicIds.get("t2"))
    val topicId2 = controller.controllerContext.topicIds("t2")
    assertEquals("t2", controller.controllerContext.topicNames(topicId2))

    // The first topic ID has not changed
    assertEquals(topicId1, controller.controllerContext.topicIds.get("t1").get)
    assertNotEquals(topicId1, topicId2)
  }

  @Test
  def testTopicIdsAreNotAdded(): Unit = {
    servers = makeServers(1, interBrokerProtocolVersion = Some(KAFKA_2_7_IV0))
    TestUtils.waitUntilControllerElected(zkClient)
    val controller = getController().kafkaController
    val tp1 = new TopicPartition("t1", 0)
    val assignment1 = Map(tp1.partition -> Seq(0))

    // Before adding the topic, an attempt to get the ID should result in None.
    assertEquals(None, controller.controllerContext.topicIds.get("t1"))

    TestUtils.createTopic(zkClient, tp1.topic(), assignment1, servers)

    // Test that the first topic has no topic ID added.
    waitForPartitionState(tp1, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    assertEquals(None, controller.controllerContext.topicIds.get("t1"))

    val tp2 = new TopicPartition("t2", 0)
    val assignment2 = Map(tp2.partition -> Seq(0))
    TestUtils.createTopic(zkClient, tp2.topic(), assignment2, servers)

    // Test that the second topic has no topic ID added.
    waitForPartitionState(tp2, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    assertEquals(None, controller.controllerContext.topicIds.get("t2"))

    // The first topic ID has not changed
    assertEquals(None, controller.controllerContext.topicIds.get("t1"))
  }


  @Test
  def testTopicIdMigrationAndHandling(): Unit = {
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> ReplicaAssignment(Seq(0), List(), List()))
    val adminZkClient = new AdminZkClient(zkClient)

    servers = makeServers(1)
    adminZkClient.createTopic(tp.topic, 1, 1)
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    val topicIdAfterCreate = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertTrue(topicIdAfterCreate.isDefined)
    assertEquals(topicIdAfterCreate, servers.head.kafkaController.controllerContext.topicIds.get(tp.topic),
      "correct topic ID cannot be found in the controller context")

    adminZkClient.addPartitions(tp.topic, assignment, adminZkClient.getBrokerMetadatas(), 2)
    val topicIdAfterAddition = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertEquals(topicIdAfterCreate, topicIdAfterAddition)
    assertEquals(topicIdAfterCreate, servers.head.kafkaController.controllerContext.topicIds.get(tp.topic),
      "topic ID changed after partition additions")

    adminZkClient.deleteTopic(tp.topic)
    TestUtils.waitUntilTrue(() => servers.head.kafkaController.controllerContext.topicIds.get(tp.topic).isEmpty,
      "topic ID for topic should have been removed from controller context after deletion")
  }

  @Test
  def testTopicIdMigrationAndHandlingWithOlderVersion(): Unit = {
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> ReplicaAssignment(Seq(0), List(), List()))
    val adminZkClient = new AdminZkClient(zkClient)

    servers = makeServers(1, interBrokerProtocolVersion = Some(KAFKA_2_7_IV0))
    adminZkClient.createTopic(tp.topic, 1, 1)
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    val topicIdAfterCreate = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertEquals(None, topicIdAfterCreate)
    assertEquals(topicIdAfterCreate, servers.head.kafkaController.controllerContext.topicIds.get(tp.topic),
      "incorrect topic ID can be found in the controller context")

    adminZkClient.addPartitions(tp.topic, assignment, adminZkClient.getBrokerMetadatas(), 2)
    val topicIdAfterAddition = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertEquals(topicIdAfterCreate, topicIdAfterAddition)
    assertEquals(topicIdAfterCreate, servers.head.kafkaController.controllerContext.topicIds.get(tp.topic),
      "topic ID changed after partition additions")

    adminZkClient.deleteTopic(tp.topic)
    TestUtils.waitUntilTrue(() => !servers.head.kafkaController.controllerContext.allTopics.contains(tp.topic),
      "topic should have been removed from controller context after deletion")
  }

  @Test
  def testTopicIdPersistsThroughControllerReelection(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = getController().kafkaController
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    val topicId = controller.controllerContext.topicIds.get("t").get

    servers(controllerId).shutdown()
    servers(controllerId).awaitShutdown()
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "failed to elect a controller")
    val controller2 = getController().kafkaController
    assertEquals(topicId, controller2.controllerContext.topicIds.get("t").get)
  }

  @Test
  def testNoTopicIdPersistsThroughControllerReelection(): Unit = {
    servers = makeServers(2, interBrokerProtocolVersion = Some(KAFKA_2_7_IV0))
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = getController().kafkaController
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    val emptyTopicId = controller.controllerContext.topicIds.get("t")
    assertEquals(None, emptyTopicId)

    servers(controllerId).shutdown()
    servers(controllerId).awaitShutdown()
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "failed to elect a controller")
    val controller2 = getController().kafkaController
    assertEquals(emptyTopicId, controller2.controllerContext.topicIds.get("t"))
  }

  @Test
  def testTopicIdPersistsThroughControllerRestart(): Unit = {
    servers = makeServers(1)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = getController().kafkaController
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    val topicId = controller.controllerContext.topicIds.get("t").get

    servers(controllerId).shutdown()
    servers(controllerId).awaitShutdown()
    servers(controllerId).startup()
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "failed to elect a controller")
    val controller2 = getController().kafkaController
    assertEquals(topicId, controller2.controllerContext.topicIds.get("t").get)
  }

  @Test
  def testTopicIdCreatedOnUpgrade(): Unit = {
    servers = makeServers(1, interBrokerProtocolVersion = Some(KAFKA_2_7_IV0))
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = getController().kafkaController
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    val topicIdAfterCreate = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertEquals(None, topicIdAfterCreate)
    val emptyTopicId = controller.controllerContext.topicIds.get("t")
    assertEquals(None, emptyTopicId)

    servers(controllerId).shutdown()
    servers(controllerId).awaitShutdown()
    servers = makeServers(1)
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "failed to elect a controller")
    val topicIdAfterUpgrade = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertNotEquals(emptyTopicId, topicIdAfterUpgrade)
    val controller2 = getController().kafkaController
    assertNotEquals(emptyTopicId, controller2.controllerContext.topicIds.get("t"))
    val topicId = controller2.controllerContext.topicIds.get("t").get
    assertEquals(topicIdAfterUpgrade.get, topicId)
    assertEquals("t", controller2.controllerContext.topicNames(topicId))

    TestUtils.waitUntilTrue(() => servers(0).logManager.getLog(tp).isDefined, "log was not created")

    val topicIdInLog = servers(0).logManager.getLog(tp).get.topicId
    assertEquals(Some(topicId), topicIdInLog)

    adminZkClient.deleteTopic(tp.topic)
    TestUtils.waitUntilTrue(() => !servers.head.kafkaController.controllerContext.allTopics.contains(tp.topic),
      "topic should have been removed from controller context after deletion")
  }

  @Test
  def testTopicIdCreatedOnUpgradeMultiBrokerScenario(): Unit = {
    // Simulate an upgrade scenario where the controller is still on a pre-topic ID IBP, but the other two brokers are upgraded.
    servers = makeServers(1, interBrokerProtocolVersion = Some(KAFKA_2_7_IV0))
    servers = servers ++ makeServers(3, startingIdNumber = 1)
    val originalControllerId = TestUtils.waitUntilControllerElected(zkClient)
    assertEquals(0, originalControllerId)
    val controller = getController().kafkaController
    assertEquals(KAFKA_2_7_IV0, servers(originalControllerId).config.interBrokerProtocolVersion)
    val remainingBrokers = servers.filter(_.config.brokerId != originalControllerId)
    val tp = new TopicPartition("t", 0)
    // Only the remaining brokers will have the replicas for the partition
    val assignment = Map(tp.partition -> remainingBrokers.map(_.config.brokerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, remainingBrokers(0).config.brokerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    val topicIdAfterCreate = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertEquals(None, topicIdAfterCreate)
    val emptyTopicId = controller.controllerContext.topicIds.get("t")
    assertEquals(None, emptyTopicId)

    // All partition logs should not have topic IDs
    remainingBrokers.foreach { server =>
      TestUtils.waitUntilTrue(() => server.logManager.getLog(tp).isDefined, "log was not created for server" + server.config.brokerId)
      val topicIdInLog = server.logManager.getLog(tp).get.topicId
      assertEquals(None, topicIdInLog)
    }

    // Shut down the controller to transfer the controller to a new IBP broker.
    servers(originalControllerId).shutdown()
    servers(originalControllerId).awaitShutdown()
    // If we were upgrading, this server would be the latest IBP, but it doesn't matter in this test scenario
    servers(originalControllerId).startup()
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "failed to elect a controller")
    val topicIdAfterUpgrade = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertNotEquals(emptyTopicId, topicIdAfterUpgrade)
    val controller2 = getController().kafkaController
    assertNotEquals(emptyTopicId, controller2.controllerContext.topicIds.get("t"))
    val topicId = controller2.controllerContext.topicIds.get("t").get
    assertEquals(topicIdAfterUpgrade.get, topicId)
    assertEquals("t", controller2.controllerContext.topicNames(topicId))

    // All partition logs should have topic IDs
    remainingBrokers.foreach { server =>
      TestUtils.waitUntilTrue(() => server.logManager.getLog(tp).isDefined, "log was not created for server" + server.config.brokerId)
      val topicIdInLog = server.logManager.getLog(tp).get.topicId
      assertEquals(Some(topicId), topicIdInLog,
        s"Server ${server.config.brokerId} had topic ID $topicIdInLog instead of ${Some(topicId)} as expected.")
    }

    adminZkClient.deleteTopic(tp.topic)
    TestUtils.waitUntilTrue(() => !servers.head.kafkaController.controllerContext.allTopics.contains(tp.topic),
      "topic should have been removed from controller context after deletion")
  }

  @Test
  def testTopicDeletionWithOfflineBrokers(): Unit = {
    val tp = new TopicPartition("t", 0)
    val adminZkClient = new AdminZkClient(zkClient)

    servers = makeServers(2)
    TestUtils.createTopic(zkClient, tp.topic, 1, 2, servers)
    val topicId1 = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())

    // shutdown one broker and then delete the topic
    val broker0 = servers.find(_.config.brokerId == 0).get
    broker0.shutdown()
    broker0.awaitShutdown()
    adminZkClient.deleteTopic(tp.topic())
    val broker1 = servers.find(_.config.brokerId == 1).get
    TestUtils.waitUntilTrue(() => {
      !broker1.replicaManager.getLog(tp).isDefined
    }, "The replica on broker1 cannot be deleted")
    TestUtils.waitUntilTrue(() => {
      !zkClient.pathExists(TopicZNode.path(tp.topic()))
    }, s"The topic ${tp.topic} is not removed from zookeeper")

    // recreate the topic and wait until broker1 get the log recreated
    val assignment = Map(tp.partition -> Seq(0, 1))
    adminZkClient.createTopicWithAssignment(tp.topic, config = new Properties(), assignment)
    TestUtils.waitUntilTrue(() => {
      broker1.replicaManager.getLog(tp).isDefined
    }, "The replica on broker1 cannot be deleted")
    val topicId2 = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertNotEquals(topicId2, topicId1, "Topic IDs across generations should not be the same")

    // restart the offline broker0, and wait until convergence of topic ID on all brokers
    broker0.startup()
    TestUtils.waitUntilTrue(() => {
      servers.forall{s => {
        val logOpt = s.replicaManager.getLog(tp)
        if (logOpt.isDefined) {
          logOpt.get.topicId == topicId2
        } else {
          false
        }
      }}
    }, s"Not every online broker has the correct topic ID for topic ${tp.topic()}")
  }

  @Test
  def testDeletionOfStrayPartitions(): Unit = {
    val tp = new TopicPartition("t1", 0)
    val adminZkClient = new AdminZkClient(zkClient)

    servers = makeServers(2)
    TestUtils.createTopic(zkClient, tp.topic, 1, 2, servers)
    TestUtils.waitUntilTrue(() => {
      servers.forall{server => server.replicaManager.getLog(tp).isDefined}
    }, "The replica on broker1 cannot be deleted")

    // shutdown one broker and then delete the topic
    val broker0 = servers.find(_.config.brokerId == 0).get
    broker0.shutdown()
    broker0.awaitShutdown()
    adminZkClient.deleteTopic(tp.topic())
    TestUtils.waitUntilTrue(() => {
      !zkClient.pathExists(TopicZNode.path(tp.topic()))
    }, "The replica on broker1 cannot be deleted")


    // restart the offline broker and make sure the stray partitions will be deleted
    broker0.startup()
    val topic2 = "t2"
    // create another topic to ensure at least one LeaderAndIsr request is being sent to the restarted broker
    TestUtils.createTopic(zkClient, topic2, 1, 2, servers)

    TestUtils.waitUntilTrue(() => {
      !broker0.replicaManager.getLog(tp).isDefined
    }, "The replica on broker0 cannot be deleted", waitTimeMs = 20000)
  }

  @Test
  def testFastTopicDeletionAndRecreation(): Unit = {
    val topic = "t1"
    val tp = new TopicPartition(topic, 0)
    // delay the 2nd broker's UpdateMetadata
    servers = makeServers(2, liUpdateMetadataDelayMap = Map(0 -> 0, 1 -> 5000))

    val assignment = Map(0 -> Seq(0))
    adminZkClient.createTopicWithAssignment(topic, new Properties(), assignment)
    // wait until broker 0 has the metadata for the topic
    // Note that broker 1 won't get the metadata since its processing of the UpdateMetadata is delayed
    var originalTopicId: Uuid = Uuid.ZERO_UUID
    TestUtils.waitUntilTrue(() => {
      val topicMetadataSeq = servers.find(_.config.brokerId == 0).get.metadataCache.getTopicMetadata(Set(topic), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
      topicMetadataSeq.find(_.name() == topic) match {
        case Some(topicMetadata) => {
          originalTopicId = topicMetadata.topicId()
          true
        }
        case _ => false
      }
    }, "broker 0 never gets the metadata for the topic $topic")

    info(s"original topic id $originalTopicId")
    // delete and then immediately recreate the topic
    info("deleting the 1st topic")
    adminZkClient.deleteTopic(topic)
    TestUtils.waitUntilTrue(() => {
      !zkClient.pathExists(TopicZNode.path(tp.topic()))
    }, "The topic cannot be deleted")

    info("creating topic for the 2nd time")
    TestUtils.createTopic(zkClient, topic, assignment, servers)
    TestUtils.waitUntilTrue(() => {
      servers.forall { server =>
        val topicMetadataOpt = server.metadataCache.getTopicMetadata(Set(topic), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)).find(_.name() == topic)
        val result = topicMetadataOpt.isDefined && !topicMetadataOpt.get.topicId().equals(originalTopicId)
        if (result) {
          info(s"server ${server.config.brokerId} got new topic id ${topicMetadataOpt.get.topicId()}")
        }
        result
      }
    }, "Now all servers end up with the new topic id in its metadata cache")
  }

  @Test
  def testTopicIdUpgradeAfterReassigningPartitions(): Unit = {
    val tp = new TopicPartition("t", 0)
    val reassignment = Map(tp -> Some(Seq(0)))
    val adminZkClient = new AdminZkClient(zkClient)

    // start server with old IBP
    servers = makeServers(1, interBrokerProtocolVersion = Some(KAFKA_2_7_IV0))
    // use create topic with ZK client directly, without topic ID
    adminZkClient.createTopic(tp.topic, 1, 1)
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    val topicIdAfterCreate = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    val id = servers.head.kafkaController.controllerContext.topicIds.get(tp.topic)
    assertTrue(topicIdAfterCreate.isEmpty)
    assertEquals(topicIdAfterCreate, id,
      "expected no topic ID, but one existed")

    // Upgrade to IBP 2.8
    servers(0).shutdown()
    servers(0).awaitShutdown()
    servers = makeServers(1)
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon controller restart")
    val topicIdAfterUpgrade = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertEquals(topicIdAfterUpgrade, servers.head.kafkaController.controllerContext.topicIds.get(tp.topic),
      "expected same topic ID but it can not be found")
    assertEquals(tp.topic(), servers.head.kafkaController.controllerContext.topicNames(topicIdAfterUpgrade.get),
      "correct topic name expected but cannot be found in the controller context")

    // Downgrade back to 2.7
    servers(0).shutdown()
    servers(0).awaitShutdown()
    servers = makeServers(1, interBrokerProtocolVersion = Some(KAFKA_2_7_IV0))
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    val topicIdAfterDowngrade = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertTrue(topicIdAfterDowngrade.isDefined)
    assertEquals(topicIdAfterUpgrade, topicIdAfterDowngrade,
      "expected same topic ID but it can not be found after downgrade")
    assertEquals(topicIdAfterDowngrade, servers.head.kafkaController.controllerContext.topicIds.get(tp.topic),
      "expected same topic ID in controller context but it is no longer found after downgrade")
    assertEquals(tp.topic(), servers.head.kafkaController.controllerContext.topicNames(topicIdAfterUpgrade.get),
      "correct topic name expected but cannot be found in the controller context")

    // Reassign partitions
    servers(0).kafkaController.eventManager.put(ApiPartitionReassignment(reassignment, _ => ()))
    waitForPartitionState(tp, 3, 0, 1,
      "failed to get expected partition state upon controller restart")
    val topicIdAfterReassignment = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertTrue(topicIdAfterReassignment.isDefined)
    assertEquals(topicIdAfterUpgrade, topicIdAfterReassignment,
      "expected same topic ID but it can not be found after reassignment")
    assertEquals(topicIdAfterUpgrade, servers.head.kafkaController.controllerContext.topicIds.get(tp.topic),
      "expected same topic ID in controller context but is no longer found after reassignment")
    assertEquals(tp.topic(), servers.head.kafkaController.controllerContext.topicNames(topicIdAfterUpgrade.get),
      "correct topic name expected but cannot be found in the controller context")

    // Upgrade back to 2.8
    servers(0).shutdown()
    servers(0).awaitShutdown()
    servers = makeServers(1)
    waitForPartitionState(tp, 3, 0, 1,
      "failed to get expected partition state upon controller restart")
    val topicIdAfterReUpgrade = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertEquals(topicIdAfterUpgrade, topicIdAfterReUpgrade,
      "expected same topic ID but it can not be found after re-upgrade")
    assertEquals(topicIdAfterReUpgrade, servers.head.kafkaController.controllerContext.topicIds.get(tp.topic),
      "topic ID can not be found in controller context after re-upgrading IBP")
    assertEquals(tp.topic(), servers.head.kafkaController.controllerContext.topicNames(topicIdAfterReUpgrade.get),
      "correct topic name expected but cannot be found in the controller context")

    adminZkClient.deleteTopic(tp.topic)
    TestUtils.waitUntilTrue(() => servers.head.kafkaController.controllerContext.topicIds.get(tp.topic).isEmpty,
      "topic ID for topic should have been removed from controller context after deletion")
    assertTrue(servers.head.kafkaController.controllerContext.topicNames.get(topicIdAfterUpgrade.get).isEmpty)
  }

  private def testControllerMove(fun: () => Unit): Unit = {
    val controller = getController().kafkaController
    val appender = LogCaptureAppender.createAndRegister()
    val previousLevel = LogCaptureAppender.setClassLoggerLevel(controller.getClass, Level.INFO)

    try {
      TestUtils.waitUntilTrue(() => {
        controller.eventManager.state == ControllerState.Idle
      }, "Controller event thread is still busy")

      val latch = new CountDownLatch(1)

      // Let the controller event thread await on a latch before the pre-defined logic is triggered.
      // This is used to make sure that when the event thread resumes and starts processing events, the controller has already moved.
      controller.eventManager.put(new MockEvent(ControllerState.TopicChange) {
        override def process(): Unit = latch.await()
        override def preempt(): Unit = {}
      })

      // Execute pre-defined logic. This can be topic creation/deletion, preferred leader election, etc.
      fun()

      // Delete the controller path, re-create /controller znode to emulate controller movement
      zkClient.deleteController(controller.controllerContext.epochZkVersion)
      zkClient.registerControllerAndIncrementControllerEpoch(servers.size)

      // Resume the controller event thread. At this point, the controller should see mismatch controller epoch zkVersion and resign
      latch.countDown()
      TestUtils.waitUntilTrue(() => !controller.isActive, "Controller fails to resign")

      // Expect to capture the ControllerMovedException in the log of ControllerEventThread
      val event = appender.getMessages.find(e => e.getLevel == Level.INFO
        && e.getThrowableInformation != null
        && e.getThrowableInformation.getThrowable.getClass.getName.equals(classOf[ControllerMovedException].getName))
      assertTrue(event.isDefined)

    } finally {
      LogCaptureAppender.unregister(appender)
      LogCaptureAppender.setClassLoggerLevel(controller.eventManager.thread.getClass, previousLevel)
    }
  }

  private def preferredReplicaLeaderElection(controllerId: Int, otherBroker: KafkaServer, tp: TopicPartition,
                                             replicas: Set[Int], leaderEpoch: Int): Unit = {
    otherBroker.shutdown()
    otherBroker.awaitShutdown()
    waitForPartitionState(tp, firstControllerEpoch, controllerId, leaderEpoch + 1,
      "failed to get expected partition state upon broker shutdown")
    otherBroker.startup()
    TestUtils.waitUntilTrue(() => zkClient.getInSyncReplicasForPartition(new TopicPartition(tp.topic, tp.partition)).get.toSet == replicas, "restarted broker failed to join in-sync replicas")
    zkClient.createPreferredReplicaElection(Set(tp))
    TestUtils.waitUntilTrue(() => !zkClient.pathExists(PreferredReplicaElectionZNode.path),
      "failed to remove preferred replica leader election path after completion")
    waitForPartitionState(tp, firstControllerEpoch, otherBroker.config.brokerId, leaderEpoch + 2,
      "failed to get expected partition state upon broker startup")
  }

  private def waitUntilControllerEpoch(epoch: Int, message: String): Unit = {
    TestUtils.waitUntilTrue(() => zkClient.getControllerEpoch.map(_._1).contains(epoch) , message)
  }

  private def waitForPartitionState(tp: TopicPartition,
                                    controllerEpoch: Int,
                                    leader: Int,
                                    leaderEpoch: Int,
                                    message: String): Unit = {
    TestUtils.waitUntilTrue(() => {
      val leaderIsrAndControllerEpochMap = zkClient.getTopicPartitionStates(Seq(tp))
      leaderIsrAndControllerEpochMap.contains(tp) &&
        isExpectedPartitionState(leaderIsrAndControllerEpochMap(tp), controllerEpoch, leader, leaderEpoch)
    }, message)
  }

  private def waitForPartitionState(tp: TopicPartition,
    controllerEpoch: Int,
    leaders: Seq[Int],
    leaderEpoch: Int,
    message: String): Unit = {
    TestUtils.waitUntilTrue(() => {
      val leaderIsrAndControllerEpochMap = zkClient.getTopicPartitionStates(Seq(tp))
      leaderIsrAndControllerEpochMap.contains(tp) && leaders.exists(
        leader=> isExpectedPartitionState(leaderIsrAndControllerEpochMap(tp), controllerEpoch, leader, leaderEpoch))
    }, message)
  }

  private def isExpectedPartitionState(leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       controllerEpoch: Int,
                                       leader: Int,
                                       leaderEpoch: Int) =
    leaderIsrAndControllerEpoch.controllerEpoch == controllerEpoch &&
      leaderIsrAndControllerEpoch.leaderAndIsr.leader == leader &&
      leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch == leaderEpoch

  private def makeServers(numConfigs: Int,
                          autoLeaderRebalanceEnable: Boolean = false,
                          uncleanLeaderElectionEnable: Boolean = false,
                          enableControlledShutdown: Boolean = true,
                          listeners : Option[String] = None,
                          listenerSecurityProtocolMap : Option[String] = None,
                          controlPlaneListenerName : Option[String] = None,
                          interBrokerProtocolVersion: Option[ApiVersion] = None,
                          logDirCount: Int = 1,
                          startingIdNumber: Int = 0,
                          liUpdateMetadataDelayMap: Map[Int, Long] = Map.empty) = {
    val configs = TestUtils.createBrokerConfigs(numConfigs, zkConnect, enableControlledShutdown = enableControlledShutdown, logDirCount = logDirCount, startingIdNumber = startingIdNumber )
    configs.foreach { config =>
      config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, autoLeaderRebalanceEnable.toString)
      config.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, uncleanLeaderElectionEnable.toString)
      config.setProperty(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp, "1")
      config.setProperty(KafkaConfig.LiCombinedControlRequestEnableProp, "true")
      val brokerId: Int = config.get(KafkaConfig.BrokerIdProp).toString.toInt
      config.setProperty(KafkaConfig.LiUpdateMetadataDelayMsProp, liUpdateMetadataDelayMap.getOrElse(brokerId, 0).toString)
      listeners.foreach(listener => config.setProperty(KafkaConfig.ListenersProp, listener))
      listenerSecurityProtocolMap.foreach(listenerMap => config.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, listenerMap))
      controlPlaneListenerName.foreach(controlPlaneListener => config.setProperty(KafkaConfig.ControlPlaneListenerNameProp, controlPlaneListener))
      interBrokerProtocolVersion.foreach(ibp => config.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, ibp.toString))
    }
    configs.map(config => TestUtils.createServer(KafkaConfig.fromProps(config)))
  }

  private def timer(metricName: String): Timer = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) =>
      k.getMBeanName == metricName
    }.values.headOption.getOrElse(throw new AssertionError(s"Unable to find metric $metricName")).asInstanceOf[Timer]
  }

  private def getController(): KafkaServer = {
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    servers.filter(s => s.config.brokerId == controllerId).head
  }

  private def createAdminClient(): Admin = {
    val config = new Properties
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName("PLAINTEXT"))
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10")
    AdminClient.create(config)
  }
}
