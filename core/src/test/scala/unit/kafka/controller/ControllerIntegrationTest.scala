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

import java.util.Properties
import java.util.concurrent.{CompletableFuture, CountDownLatch, LinkedBlockingQueue, TimeUnit}

import com.yammer.metrics.core.Timer
import kafka.api.LeaderAndIsr
import kafka.controller.KafkaController.AlterPartitionCallback
import kafka.server.{KafkaConfig, KafkaServer, QuorumTestHarness}
import kafka.utils.TestUtils
import kafka.zk.{FeatureZNodeStatus, _}
import org.apache.kafka.common.errors.{ControllerMovedException, StaleBrokerEpochException}
import org.apache.kafka.common.feature.Features
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.{ElectionType, TopicPartition, Uuid}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.{IBP_2_6_IV0, IBP_2_7_IV0}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.logging.log4j.Level
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.mockito.Mockito.{doAnswer, spy, verify}
import org.mockito.invocation.InvocationOnMock
import unit.kafka.utils.LogCaptureContext

import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class ControllerIntegrationTest extends QuorumTestHarness {
  var servers = Seq.empty[KafkaServer]
  val firstControllerEpoch = KafkaController.InitialControllerEpoch + 1
  val firstControllerEpochZkVersion = KafkaController.InitialControllerEpochZkVersion + 1

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
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
  def testMetadataPropagationOnBrokerShutdownWithNoReplicas(): Unit = {
    servers = makeServers(3)
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val replicaBroker = servers.filter(e => e.config.brokerId != controllerId).head

    val controllerBroker = servers.filter(e => e.config.brokerId == controllerId).head
    val otherBroker = servers.filter(e => e.config.brokerId != controllerId &&
      e.config.brokerId != replicaBroker.config.brokerId).head

    val topic = "topic1"
    val assignment = Map(0 -> Seq(replicaBroker.config.brokerId))

    // Create topic
    TestUtils.createTopic(zkClient, topic, assignment, servers)

    // Shutdown the broker with replica
    replicaBroker.shutdown()
    replicaBroker.awaitShutdown()

    // Shutdown the other broker
    otherBroker.shutdown()
    otherBroker.awaitShutdown()

    // The controller should be the only alive broker
    TestUtils.waitUntilBrokerMetadataIsPropagated(Seq(controllerBroker))
  }

  @Test
  def testTopicCreation(): Unit = {
    servers = makeServers(1)
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(0))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.InitialLeaderEpoch,
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
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.InitialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
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
    waitForPartitionState(tp1, firstControllerEpoch, 0, LeaderAndIsr.InitialLeaderEpoch,
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
    waitForPartitionState(tp1, firstControllerEpoch, controllerId, LeaderAndIsr.InitialLeaderEpoch,
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
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.InitialLeaderEpoch + 3,
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
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.InitialLeaderEpoch + 3,
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
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.InitialLeaderEpoch + 1,
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
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.InitialLeaderEpoch + 1,
      "failed to get expected partition state during partition reassignment with offline replica")
    servers(otherBrokerId).startup()
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.InitialLeaderEpoch + 4,
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
    preferredReplicaLeaderElection(controllerId, otherBroker, tp, assignment(tp.partition).toSet, LeaderAndIsr.InitialLeaderEpoch)
  }

  @Test
  def testBackToBackPreferredReplicaLeaderElections(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBroker = servers.find(_.config.brokerId != controllerId).get
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBroker.config.brokerId, controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    preferredReplicaLeaderElection(controllerId, otherBroker, tp, assignment(tp.partition).toSet, LeaderAndIsr.InitialLeaderEpoch)
    preferredReplicaLeaderElection(controllerId, otherBroker, tp, assignment(tp.partition).toSet, LeaderAndIsr.InitialLeaderEpoch + 2)
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
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.InitialLeaderEpoch + 1,
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
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.InitialLeaderEpoch + 1,
      "failed to get expected partition state upon broker shutdown")
    servers(otherBrokerId).startup()
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.InitialLeaderEpoch + 2,
      "failed to get expected partition state upon broker startup")
  }

  @Test
  def testLeaderAndIsrWhenEntireIsrOfflineAndUncleanLeaderElectionDisabled(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBrokerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.InitialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    TestUtils.waitUntilTrue(() => {
      val leaderIsrAndControllerEpochMap = zkClient.getTopicPartitionStates(Seq(tp))
      leaderIsrAndControllerEpochMap.contains(tp) &&
        isExpectedPartitionState(leaderIsrAndControllerEpochMap(tp), firstControllerEpoch, LeaderAndIsr.NoLeader, LeaderAndIsr.InitialLeaderEpoch + 1) &&
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
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.InitialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    TestUtils.waitUntilTrue(() => {
      val leaderIsrAndControllerEpochMap = zkClient.getTopicPartitionStates(Seq(tp))
      leaderIsrAndControllerEpochMap.contains(tp) &&
        isExpectedPartitionState(leaderIsrAndControllerEpochMap(tp), firstControllerEpoch, LeaderAndIsr.NoLeader, LeaderAndIsr.InitialLeaderEpoch + 1) &&
        leaderIsrAndControllerEpochMap(tp).leaderAndIsr.isr == List(otherBrokerId)
    }, "failed to get expected partition state after entire isr went offline")
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

    testControllerMove(
      () => zkClient.createPreferredReplicaElection(Set(tp)))
  }

  @Test
  def testControllerMoveOnPartitionReassignment(): Unit = {
    servers = makeServers(1)
    TestUtils.waitUntilControllerElected(zkClient)
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(0))
    TestUtils.createTopic(zkClient, tp.topic(), assignment, servers)

    val reassignment = Map(tp -> Seq(0))
    testControllerMove(
      () => zkClient.createPartitionReassignment(reassignment))
  }

  @Test
  def testControllerFeatureZNodeSetupWhenFeatureVersioningIsEnabledWithNonExistingFeatureZNode(): Unit = {
    testControllerFeatureZNodeSetup(Option.empty, IBP_2_7_IV0)
  }

  @Test
  def testControllerFeatureZNodeSetupWhenFeatureVersioningIsEnabledWithDisabledExistingFeatureZNode(): Unit = {
    testControllerFeatureZNodeSetup(Some(new FeatureZNode(FeatureZNodeStatus.Disabled, Features.emptyFinalizedFeatures())), IBP_2_7_IV0)
  }

  @Test
  def testControllerFeatureZNodeSetupWhenFeatureVersioningIsEnabledWithEnabledExistingFeatureZNode(): Unit = {
    testControllerFeatureZNodeSetup(Some(new FeatureZNode(FeatureZNodeStatus.Enabled, Features.emptyFinalizedFeatures())), IBP_2_7_IV0)
  }

  @Test
  def testControllerFeatureZNodeSetupWhenFeatureVersioningIsDisabledWithNonExistingFeatureZNode(): Unit = {
    testControllerFeatureZNodeSetup(Option.empty, IBP_2_6_IV0)
  }

  @Test
  def testControllerFeatureZNodeSetupWhenFeatureVersioningIsDisabledWithDisabledExistingFeatureZNode(): Unit = {
    testControllerFeatureZNodeSetup(Some(new FeatureZNode(FeatureZNodeStatus.Disabled, Features.emptyFinalizedFeatures())), IBP_2_6_IV0)
  }

  @Test
  def testControllerFeatureZNodeSetupWhenFeatureVersioningIsDisabledWithEnabledExistingFeatureZNode(): Unit = {
    testControllerFeatureZNodeSetup(Some(new FeatureZNode(FeatureZNodeStatus.Enabled, Features.emptyFinalizedFeatures())), IBP_2_6_IV0)
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
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.InitialLeaderEpoch,
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
    val event1 = ReplicaLeaderElection(Some(partitions), ElectionType.PREFERRED, ZkTriggered, partitionsMap => {
      for (partition <- partitionsMap) {
        partition._2 match {
          case Left(e) => assertEquals(Errors.NOT_CONTROLLER, e.error())
          case Right(_) => throw new AssertionError("replica leader election should error")
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
                                              interBrokerProtocolVersion: MetadataVersion): Unit = {
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
    if (interBrokerProtocolVersion.isAtLeast(IBP_2_7_IV0)) {
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
  def testIdempotentAlterPartition(): Unit = {
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
    controller.eventManager.put(AlterPartitionReceived(otherBroker.config.brokerId, brokerEpoch, Map(tp -> newLeaderAndIsr), callback))
    latch.await()
  }

  @Test
  def testAlterPartitionErrors(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val tp = new TopicPartition("t", 0)
    val replicas = controllerId :: servers.map(_.config.nodeId).filter(_ != controllerId).take(1).toList
    val assignment = Map(tp.partition -> replicas)

    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    val controller = getController().kafkaController
    val partitionState = controller.controllerContext.partitionLeadershipInfo(tp).get
    val leaderId = partitionState.leaderAndIsr.leader
    val leaderBrokerEpoch = servers(leaderId).kafkaController.brokerEpoch
    val leaderEpoch = partitionState.leaderAndIsr.leaderEpoch
    val partitionEpoch = partitionState.leaderAndIsr.partitionEpoch

    def assertAlterPartition(
      topLevelError: Errors = Errors.NONE,
      partitionError: Errors = Errors.NONE,
      topicPartition: TopicPartition = tp,
      leaderId: Int = leaderId,
      brokerEpoch: Long = leaderBrokerEpoch,
      leaderEpoch: Int = leaderEpoch,
      partitionEpoch: Int = partitionEpoch,
      isr: Set[Int] = replicas.toSet,
      leaderRecoveryState: LeaderRecoveryState = LeaderRecoveryState.RECOVERED
    ): Unit = {
      assertAlterPartitionError(
        topicPartition = topicPartition,
        leaderId = leaderId,
        brokerEpoch = brokerEpoch,
        leaderEpoch = leaderEpoch,
        partitionEpoch = partitionEpoch,
        isr = isr,
        leaderRecoveryState = leaderRecoveryState,
        topLevelError = topLevelError,
        partitionError = partitionError
      )
    }

    assertAlterPartition(
      topLevelError = Errors.STALE_BROKER_EPOCH,
      brokerEpoch = leaderBrokerEpoch - 1
    )

    assertAlterPartition(
      topLevelError = Errors.STALE_BROKER_EPOCH,
      leaderId = 99,
    )

    assertAlterPartition(
      partitionError = Errors.UNKNOWN_TOPIC_OR_PARTITION,
      topicPartition = new TopicPartition("unknown", 0)
    )

    assertAlterPartition(
      partitionError = Errors.UNKNOWN_TOPIC_OR_PARTITION,
      topicPartition = new TopicPartition(tp.topic, 1)
    )

    assertAlterPartition(
      partitionError = Errors.INVALID_UPDATE_VERSION,
      partitionEpoch = partitionEpoch - 1
    )

    assertAlterPartition(
      partitionError = Errors.FENCED_LEADER_EPOCH,
      leaderEpoch = leaderEpoch - 1
    )

    assertAlterPartition(
      partitionError = Errors.FENCED_LEADER_EPOCH,
      leaderEpoch = leaderEpoch + 1
    )

    assertAlterPartition(
      partitionError = Errors.INVALID_REQUEST,
      leaderRecoveryState = LeaderRecoveryState.RECOVERING
    )

    assertAlterPartition(
      partitionError = Errors.INVALID_REQUEST,
      leaderRecoveryState = LeaderRecoveryState.RECOVERING,
      isr = Set(controllerId)
    )

    // Version/epoch errors take precedence over other validations since
    // the leader may be working with outdated state.

    assertAlterPartition(
      partitionError = Errors.INVALID_UPDATE_VERSION,
      leaderRecoveryState = LeaderRecoveryState.RECOVERING,
      partitionEpoch = partitionEpoch - 1
    )

    assertAlterPartition(
      partitionError = Errors.FENCED_LEADER_EPOCH,
      leaderRecoveryState = LeaderRecoveryState.RECOVERING,
      leaderEpoch = leaderEpoch - 1
    )

    assertAlterPartition(
      partitionError = Errors.FENCED_LEADER_EPOCH,
      leaderRecoveryState = LeaderRecoveryState.RECOVERING,
      leaderEpoch = leaderEpoch + 1
    )
  }

  @Test
  def testAlterPartitionErrorsAfterUncleanElection(): Unit = {
    // - Start 3 brokers with unclean election enabled
    // - Create a topic with two non-controller replicas: A and B
    // - Shutdown A to bring ISR to [B]
    // - Shutdown B to make partition offline
    // - Restart A to force unclean election with ISR [A]
    // - Verify AlterPartition handling in this state

    servers = makeServers(numConfigs = 3, uncleanLeaderElectionEnable = true)
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = getController().kafkaController

    val tp = new TopicPartition("t", 0)
    val replicas = servers.map(_.config.nodeId).filter(_ != controllerId).take(2).toList
    val assignment = Map(tp.partition -> replicas)

    val replica1 :: replica2 :: Nil = replicas

    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    servers(replica1).shutdown()
    servers(replica1).awaitShutdown()

    val partitionStateAfterFirstShutdown = controller.controllerContext.partitionLeadershipInfo(tp).get
    assertEquals(replica2, partitionStateAfterFirstShutdown.leaderAndIsr.leader)
    assertEquals(Set(replica2), partitionStateAfterFirstShutdown.leaderAndIsr.isr.toSet)

    servers(replica2).shutdown()
    servers(replica2).awaitShutdown()

    val partitionStateAfterSecondShutdown = controller.controllerContext.partitionLeadershipInfo(tp).get
    assertEquals(-1, partitionStateAfterSecondShutdown.leaderAndIsr.leader)
    assertEquals(Set(replica2), partitionStateAfterSecondShutdown.leaderAndIsr.isr.toSet)

    servers(replica1).startup()
    TestUtils.waitUntilLeaderIsKnown(servers, tp)

    val partitionStateAfterRestart = controller.controllerContext.partitionLeadershipInfo(tp).get
    assertEquals(replica1, partitionStateAfterRestart.leaderAndIsr.leader)
    assertEquals(Set(replica1), partitionStateAfterRestart.leaderAndIsr.isr.toSet)
    assertEquals(LeaderRecoveryState.RECOVERING, partitionStateAfterRestart.leaderAndIsr.leaderRecoveryState)

    val leaderId = replica1
    val leaderBrokerEpoch = servers(replica1).kafkaController.brokerEpoch
    val leaderEpoch = partitionStateAfterRestart.leaderAndIsr.leaderEpoch
    val partitionEpoch = partitionStateAfterRestart.leaderAndIsr.partitionEpoch

    def assertAlterPartition(
      topLevelError: Errors = Errors.NONE,
      partitionError: Errors = Errors.NONE,
      leaderId: Int = leaderId,
      brokerEpoch: Long = leaderBrokerEpoch,
      leaderEpoch: Int = leaderEpoch,
      partitionEpoch: Int = partitionEpoch,
      leaderRecoveryState: LeaderRecoveryState = LeaderRecoveryState.RECOVERED
    ): Unit = {
      assertAlterPartitionError(
        topicPartition = tp,
        leaderId = leaderId,
        brokerEpoch = brokerEpoch,
        leaderEpoch = leaderEpoch,
        partitionEpoch = partitionEpoch,
        isr = replicas.toSet,
        leaderRecoveryState = leaderRecoveryState,
        topLevelError = topLevelError,
        partitionError = partitionError
      )
    }

    assertAlterPartition(
      topLevelError = Errors.STALE_BROKER_EPOCH,
      brokerEpoch = leaderBrokerEpoch - 1
    )

    assertAlterPartition(
      topLevelError = Errors.STALE_BROKER_EPOCH,
      leaderId = 99
    )

    assertAlterPartition(
      partitionError = Errors.INVALID_UPDATE_VERSION,
      partitionEpoch = partitionEpoch - 1
    )

    assertAlterPartition(
      partitionError = Errors.FENCED_LEADER_EPOCH,
      leaderEpoch = leaderEpoch - 1
    )

    assertAlterPartition(
      partitionError = Errors.FENCED_LEADER_EPOCH,
      leaderEpoch = leaderEpoch + 1
    )

    assertAlterPartition(
      partitionError = Errors.INVALID_REQUEST,
      leaderRecoveryState = LeaderRecoveryState.RECOVERING
    )

    // Version/epoch errors take precedence over other validations since
    // the leader may be working with outdated state.

    assertAlterPartition(
      partitionError = Errors.INVALID_UPDATE_VERSION,
      partitionEpoch = partitionEpoch - 1,
      leaderRecoveryState = LeaderRecoveryState.RECOVERING
    )

    assertAlterPartition(
      partitionError = Errors.FENCED_LEADER_EPOCH,
      leaderEpoch = leaderEpoch - 1,
      leaderRecoveryState = LeaderRecoveryState.RECOVERING
    )

    assertAlterPartition(
      partitionError = Errors.FENCED_LEADER_EPOCH,
      leaderEpoch = leaderEpoch + 1,
      leaderRecoveryState = LeaderRecoveryState.RECOVERING
    )
  }

  def assertAlterPartitionError(
    topicPartition: TopicPartition,
    leaderId: Int,
    brokerEpoch: Long,
    leaderEpoch: Int,
    partitionEpoch: Int,
    isr: Set[Int],
    leaderRecoveryState: LeaderRecoveryState,
    topLevelError: Errors,
    partitionError: Errors,
  ): Unit = {
    val leaderAndIsr = LeaderAndIsr(
      leader = leaderId,
      leaderEpoch = leaderEpoch,
      isr = isr.toList,
      partitionEpoch = partitionEpoch,
      leaderRecoveryState = leaderRecoveryState
    )

    val future = captureAlterPartitionError(
      brokerId = leaderId,
      brokerEpoch = brokerEpoch,
      topicPartition = topicPartition,
      leaderAndIsr = leaderAndIsr
    )

    val errors = future.get(10, TimeUnit.SECONDS)
    assertEquals(topLevelError, errors.topLevelError)

    if (topLevelError == Errors.NONE) {
      assertEquals(Some(partitionError), errors.partitionError)
    }
  }

  private case class AlterPartitionError(topLevelError: Errors, partitionError: Option[Errors])

  private def captureAlterPartitionError(
    brokerId: Int,
    brokerEpoch: Long,
    topicPartition: TopicPartition,
    leaderAndIsr: LeaderAndIsr
  ): CompletableFuture[AlterPartitionError] = {
    val future = new CompletableFuture[AlterPartitionError]()
    val callback: AlterPartitionCallback = {
      case Left(partitionResults: Map[TopicPartition, Either[Errors, LeaderAndIsr]]) =>
        partitionResults.get(topicPartition) match {
          case Some(Left(error: Errors)) =>
            future.complete(AlterPartitionError(topLevelError = Errors.NONE, partitionError = Some(error)))

          case Some(Right(_: LeaderAndIsr)) =>
            future.complete(AlterPartitionError(topLevelError = Errors.NONE, partitionError = Some(Errors.NONE)))

          case None =>
            future.completeExceptionally(new AssertionError(s"Should have seen $topicPartition in result"))
        }

      case Right(error: Errors) =>
        future.complete(AlterPartitionError(topLevelError = error, partitionError = None))
    }

    val partitionsToAlter = Map(topicPartition -> leaderAndIsr)
    val controller = getController().kafkaController
    controller.eventManager.put(AlterPartitionReceived(brokerId, brokerEpoch, partitionsToAlter, callback))
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
    waitForPartitionState(tp1, firstControllerEpoch, 0, LeaderAndIsr.InitialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    assertNotEquals(None, controller.controllerContext.topicIds.get("t1"))
    val topicId1 = controller.controllerContext.topicIds("t1")
    assertEquals("t1", controller.controllerContext.topicNames(topicId1))

    val tp2 = new TopicPartition("t2", 0)
    val assignment2 = Map(tp2.partition -> Seq(0))
    TestUtils.createTopic(zkClient, tp2.topic(), assignment2, servers)

    // Test that the second topic has its ID added correctly
    waitForPartitionState(tp2, firstControllerEpoch, 0, LeaderAndIsr.InitialLeaderEpoch,
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
    servers = makeServers(1, interBrokerProtocolVersion = Some(IBP_2_7_IV0))
    TestUtils.waitUntilControllerElected(zkClient)
    val controller = getController().kafkaController
    val tp1 = new TopicPartition("t1", 0)
    val assignment1 = Map(tp1.partition -> Seq(0))

    // Before adding the topic, an attempt to get the ID should result in None.
    assertEquals(None, controller.controllerContext.topicIds.get("t1"))

    TestUtils.createTopic(zkClient, tp1.topic(), assignment1, servers)

    // Test that the first topic has no topic ID added.
    waitForPartitionState(tp1, firstControllerEpoch, 0, LeaderAndIsr.InitialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    assertEquals(None, controller.controllerContext.topicIds.get("t1"))

    val tp2 = new TopicPartition("t2", 0)
    val assignment2 = Map(tp2.partition -> Seq(0))
    TestUtils.createTopic(zkClient, tp2.topic(), assignment2, servers)

    // Test that the second topic has no topic ID added.
    waitForPartitionState(tp2, firstControllerEpoch, 0, LeaderAndIsr.InitialLeaderEpoch,
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
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.InitialLeaderEpoch,
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

    servers = makeServers(1, interBrokerProtocolVersion = Some(IBP_2_7_IV0))
    adminZkClient.createTopic(tp.topic, 1, 1)
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.InitialLeaderEpoch,
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
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.InitialLeaderEpoch,
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
    servers = makeServers(2, interBrokerProtocolVersion = Some(IBP_2_7_IV0))
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = getController().kafkaController
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.InitialLeaderEpoch,
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
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.InitialLeaderEpoch,
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
    servers = makeServers(1, interBrokerProtocolVersion = Some(IBP_2_7_IV0))
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = getController().kafkaController
    val tp = new TopicPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.InitialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    assertEquals(None, zkClient.getTopicIdsForTopics(Set(tp.topic)).get(tp.topic))
    assertEquals(None, controller.controllerContext.topicIds.get(tp.topic))

    servers(controllerId).shutdown()
    servers(controllerId).awaitShutdown()
    servers = makeServers(1)
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "failed to elect a controller")

    val (topicIdAfterUpgrade, _) = TestUtils.computeUntilTrue(zkClient.getTopicIdsForTopics(Set(tp.topic)).get(tp.topic))(_.nonEmpty)
    assertNotEquals(None, topicIdAfterUpgrade, s"topic id for ${tp.topic} not found in ZK")

    val controller2 = getController().kafkaController
    val topicId = controller2.controllerContext.topicIds.get(tp.topic)
    assertEquals(topicIdAfterUpgrade, topicId)
    assertEquals(tp.topic, controller2.controllerContext.topicNames(topicId.get))

    TestUtils.waitUntilTrue(() => servers(0).logManager.getLog(tp).isDefined, "log was not created")

    val topicIdInLog = servers(0).logManager.getLog(tp).get.topicId
    assertEquals(topicId, topicIdInLog)

    adminZkClient.deleteTopic(tp.topic)
    TestUtils.waitUntilTrue(() => !servers.head.kafkaController.controllerContext.allTopics.contains(tp.topic),
      "topic should have been removed from controller context after deletion")
  }

  @Test
  def testTopicIdCreatedOnUpgradeMultiBrokerScenario(): Unit = {
    // Simulate an upgrade scenario where the controller is still on a pre-topic ID IBP, but the other two brokers are upgraded.
    servers = makeServers(1, interBrokerProtocolVersion = Some(MetadataVersion.IBP_2_7_IV0))
    servers = servers ++ makeServers(3, startingIdNumber = 1)
    val originalControllerId = TestUtils.waitUntilControllerElected(zkClient)
    assertEquals(0, originalControllerId)
    val controller = getController().kafkaController
    assertEquals(IBP_2_7_IV0, servers(originalControllerId).config.interBrokerProtocolVersion)
    val remainingBrokers = servers.filter(_.config.brokerId != originalControllerId)
    val tp = new TopicPartition("t", 0)
    // Only the remaining brokers will have the replicas for the partition
    val assignment = Map(tp.partition -> remainingBrokers.map(_.config.brokerId))
    TestUtils.createTopic(zkClient, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, firstControllerEpoch, remainingBrokers(0).config.brokerId, LeaderAndIsr.InitialLeaderEpoch,
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
  def testTopicIdUpgradeAfterReassigningPartitions(): Unit = {
    val tp = new TopicPartition("t", 0)
    val reassignment = Map(tp -> Some(Seq(0)))
    val adminZkClient = new AdminZkClient(zkClient)

    // start server with old IBP
    servers = makeServers(1, interBrokerProtocolVersion = Some(IBP_2_7_IV0))
    // use create topic with ZK client directly, without topic ID
    adminZkClient.createTopic(tp.topic, 1, 1)
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.InitialLeaderEpoch,
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
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.InitialLeaderEpoch,
      "failed to get expected partition state upon controller restart")
    val topicIdAfterUpgrade = zkClient.getTopicIdsForTopics(Set(tp.topic())).get(tp.topic())
    assertEquals(topicIdAfterUpgrade, servers.head.kafkaController.controllerContext.topicIds.get(tp.topic),
      "expected same topic ID but it can not be found")
    assertEquals(tp.topic(), servers.head.kafkaController.controllerContext.topicNames(topicIdAfterUpgrade.get),
      "correct topic name expected but cannot be found in the controller context")

    // Downgrade back to 2.7
    servers(0).shutdown()
    servers(0).awaitShutdown()
    servers = makeServers(1, interBrokerProtocolVersion = Some(IBP_2_7_IV0))
    waitForPartitionState(tp, firstControllerEpoch, 0, LeaderAndIsr.InitialLeaderEpoch,
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
    val logCaptureContext = LogCaptureContext(scala.Predef.Map(classOf[KafkaController].getName -> "INFO"))
    logCaptureContext.setLatch(1)

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
      logCaptureContext.await(30, TimeUnit.SECONDS)
      val event = logCaptureContext.getMessages.find(e => e.getLevel == Level.INFO
        && e.getThrown != null
        && e.getThrown.getClass.getName.equals(classOf[ControllerMovedException].getName))
      assertTrue(event.isDefined)
    } finally {
      logCaptureContext.close
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
                          interBrokerProtocolVersion: Option[MetadataVersion] = None,
                          logDirCount: Int = 1,
                          startingIdNumber: Int = 0): Seq[KafkaServer] = {
    val configs = TestUtils.createBrokerConfigs(numConfigs, zkConnect, enableControlledShutdown = enableControlledShutdown, logDirCount = logDirCount, startingIdNumber = startingIdNumber)
    configs.foreach { config =>
      config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, autoLeaderRebalanceEnable.toString)
      config.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, uncleanLeaderElectionEnable.toString)
      config.setProperty(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp, "1")
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

}
