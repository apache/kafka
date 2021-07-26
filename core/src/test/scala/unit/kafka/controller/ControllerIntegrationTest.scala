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
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}
import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Timer
import kafka.api.LeaderAndIsr
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.zk._
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertEquals, assertTrue}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{ControllerMovedException, NotEnoughReplicasException, StaleBrokerEpochException}
import org.apache.log4j.Level
import kafka.utils.LogCaptureAppender
import org.apache.kafka.clients.admin.{Admin, AdminClient, AdminClientConfig}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.network.ListenerName
import org.scalatest.Assertions.fail

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.Seq
import scala.util.{Failure, Success, Try}

class ControllerIntegrationTest extends ZooKeeperTestHarness {
  var servers = Seq.empty[KafkaServer]
  val firstControllerEpoch = KafkaController.InitialControllerEpoch + 1
  val firstControllerEpochZkVersion = KafkaController.InitialControllerEpochZkVersion + 1

  @Before
  override def setUp(): Unit = {
    super.setUp
    servers = Seq.empty[KafkaServer]
  }

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown
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
    servers.head.metrics.metrics().values().asScala.foreach { kafkaMetric =>
      if (kafkaMetric.metricName().tags().values().contains("CONTROLLER")) {
        controlPlaneMetricMap.put(kafkaMetric.metricName().name(), kafkaMetric)
      }
      if (kafkaMetric.metricName().tags().values().contains("PLAINTEXT")) {
        dataPlaneMetricMap.put(kafkaMetric.metricName().name(), kafkaMetric)
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

  /*
  * Tests that controller will fix insufficient RF topic by assigning sufficient replicas
  * */
  @Test
  def testTopicCreationWithFixingRF(): Unit = {
    val topicRF1 = "test_topic_rf1"
    val partition = 0
    val defaultRF = 2
    val serverConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false)
      .map(prop => {
        prop.setProperty(KafkaConfig.DefaultReplicationFactorProp,defaultRF.toString)
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
    zkClient.setTopicAssignment(tp0.topic, expandedAssignment, firstControllerEpochZkVersion)
    waitForPartitionState(tp1, firstControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic partition expansion")
    TestUtils.waitUntilMetadataIsPropagated(servers, tp1.topic, tp1.partition)
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
    zkClient.setTopicAssignment(tp0.topic, expandedAssignment, firstControllerEpochZkVersion)
    waitForPartitionState(tp1, firstControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic partition expansion")
    TestUtils.waitUntilMetadataIsPropagated(Seq(servers(controllerId)), tp1.topic, tp1.partition)
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
    zkClient.createPartitionReassignment(reassignment.mapValues(_.replicas).toMap)
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch + 3,
      "failed to get expected partition state after partition reassignment")
    TestUtils.waitUntilTrue(() =>  zkClient.getFullReplicaAssignmentForTopics(Set(tp.topic)) == reassignment,
      "failed to get updated partition assignment on topic znode after partition reassignment")
    TestUtils.waitUntilTrue(() => !zkClient.reassignPartitionsInProgress(),
      "failed to remove reassign partitions path after completion")

    val updatedTimerCount = timer(metricName).count
    assertTrue(s"Timer count $updatedTimerCount should be greater than $timerCount", updatedTimerCount > timerCount)
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
    TestUtils.waitUntilTrue(() => zkClient.reassignPartitionsInProgress(),
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
    zkClient.createPartitionReassignment(reassignment.mapValues(_.replicas).toMap)
    waitForPartitionState(tp, firstControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch + 1,
      "failed to get expected partition state during partition reassignment with offline replica")
    servers(otherBrokerId).startup()
    waitForPartitionState(tp, firstControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch + 4,
      "failed to get expected partition state after partition reassignment")
    TestUtils.waitUntilTrue(() => zkClient.getFullReplicaAssignmentForTopics(Set(tp.topic)) == reassignment,
      "failed to get updated partition assignment on topic znode after partition reassignment")
    TestUtils.waitUntilTrue(() => !zkClient.reassignPartitionsInProgress(),
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
    servers = makeServers(3, enableDeleteTopic = true)
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
    servers(1).shutdown()
    servers(1).awaitShutdown()
    TestUtils.waitUntilTrue(() => {
      val leaderIsrAndControllerEpochMap = zkClient.getTopicPartitionStates(Seq(tp))
      leaderIsrAndControllerEpochMap.contains(tp) &&
        isExpectedPartitionState(leaderIsrAndControllerEpochMap(tp), firstControllerEpoch, LeaderAndIsr.NoLeader, LeaderAndIsr.initialLeaderEpoch + 1) &&
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
    servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))
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
      case Failure(exception) => fail("Controlled shutdown failed due to error", exception)
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
    val controlledShutdownCallback = (controlledShutdownResult: Try[collection.Set[TopicPartition]]) => resultQueue.put(controlledShutdownResult)
    controller.controlledShutdown(2, servers.find(_.config.brokerId == 2).get.kafkaController.brokerEpoch, controlledShutdownCallback)
    var partitionsRemaining = resultQueue.take().get
    var activeServers = servers.filter(s => s.config.brokerId != 2)
    // wait for the update metadata request to trickle to the brokers
    TestUtils.waitUntilTrue(() =>
      activeServers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.isr.size != 4),
      "Topic test not created after timeout")
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

    val newControllerId = zkClient.getControllerId.get
    val newController = servers.find(p => p.config.brokerId == newControllerId).get.kafkaController

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
    newController.controlledShutdown(1, servers.find(_.config.brokerId == 1).get.kafkaController.brokerEpoch, controlledShutdownCallback)
    partitionsRemaining = resultQueue.take().get
    activeServers = servers.filter(s => s.config.brokerId != 1)
    // wait for the update metadata request to trickle to the brokers
    TestUtils.waitUntilTrue(() =>
      activeServers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.isr.size != 3),
      "Topic test not created after timeout")
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
      TestUtils.alterTopicConfigs(adminClient, topic, updatedTopicConfig)
    } finally {
      adminClient.close()
    }
    TestUtils.waitUntilTrue(() => newController.controllerContext.topicMinIsrConfig.getOrElse(topic, -1) == 1, "Controller never saw min.insync.replicas config change for topic.")
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

    // Attempt to shut down and then restart broker1
    val broker1 = servers.filter(s => s.config.brokerId == 1).head
    broker1.shutdown()
    var activeServers = servers.filter(s => s.config.brokerId != 1)
    TestUtils.waitUntilTrue(() =>
      activeServers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.isr.size != 2),
      "Topic test not created after timeout")

    broker1.startup()
    TestUtils.waitUntilTrue(() =>
      servers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic, partition).get.isr.size == 2),
      "The ISR does not include the full set after the offline broker is restarted")

    // now try to shutdown broker 0
    val broker0 = servers.filter(s => s.config.brokerId == 0).head
    broker0.shutdown()
    activeServers = Seq(broker1)
    TestUtils.waitUntilTrue(() =>
      activeServers.forall(_.dataPlaneRequestProcessor.metadataCache.getPartitionInfo(topic,partition).get.isr.size != 2),
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
    servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))

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
    })

    otherBroker.shutdown()
    otherBroker.startup()

    assertEquals(0, otherBroker.replicaManager.partitionCount.value())

    // Release the latch so that controller can process broker change event
    latch.countDown()
    TestUtils.waitUntilTrue(() => {
      otherBroker.replicaManager.partitionCount.value() == 1 &&
      otherBroker.replicaManager.metadataCache.getAllTopics().size == 1 &&
      otherBroker.replicaManager.metadataCache.getAliveBrokers.size == 2
    }, "Broker fail to initialize after restart")
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
                          enableDeleteTopic: Boolean = false) = {
    val configs = TestUtils.createBrokerConfigs(numConfigs, zkConnect, enableControlledShutdown = enableControlledShutdown)
    configs.foreach { config =>
      config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, autoLeaderRebalanceEnable.toString)
      config.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, uncleanLeaderElectionEnable.toString)
      config.setProperty(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp, "1")
      listeners.foreach(listener => config.setProperty(KafkaConfig.ListenersProp, listener))
      listenerSecurityProtocolMap.foreach(listenerMap => config.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, listenerMap))
      controlPlaneListenerName.foreach(controlPlaneListener => config.setProperty(KafkaConfig.ControlPlaneListenerNameProp, controlPlaneListener))
      config.setProperty(KafkaConfig.DeleteTopicEnableProp, enableDeleteTopic.toString)
    }
    configs.map(config => TestUtils.createServer(KafkaConfig.fromProps(config)))
  }

  private def timer(metricName: String): Timer = {
    Metrics.defaultRegistry.allMetrics.asScala.filterKeys(_.getMBeanName == metricName).values.headOption
      .getOrElse(fail(s"Unable to find metric $metricName")).asInstanceOf[Timer]
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
