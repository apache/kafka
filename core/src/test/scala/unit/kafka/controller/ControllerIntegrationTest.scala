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

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Timer
import kafka.api.LeaderAndIsr
import kafka.common.TopicAndPartition
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{TestUtils, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.junit.{After, Before, Test}
import org.junit.Assert.assertTrue

import scala.collection.JavaConverters._

class ControllerIntegrationTest extends ZooKeeperTestHarness {
  var servers = Seq.empty[KafkaServer]

  @Before
  override def setUp() {
    super.setUp
    servers = Seq.empty[KafkaServer]
  }

  @After
  override def tearDown() {
    TestUtils.shutdownServers(servers)
    super.tearDown
  }

  @Test
  def testEmptyCluster(): Unit = {
    servers = makeServers(1)
    TestUtils.waitUntilTrue(() => zkUtils.pathExists(ZkUtils.ControllerPath), "failed to elect a controller")
    waitUntilControllerEpoch(KafkaController.InitialControllerEpoch, "broker failed to set controller epoch")
  }

  @Test
  def testControllerEpochPersistsWhenAllBrokersDown(): Unit = {
    servers = makeServers(1)
    TestUtils.waitUntilTrue(() => zkUtils.pathExists(ZkUtils.ControllerPath), "failed to elect a controller")
    waitUntilControllerEpoch(KafkaController.InitialControllerEpoch, "broker failed to set controller epoch")
    servers.head.shutdown()
    servers.head.awaitShutdown()
    TestUtils.waitUntilTrue(() => !zkUtils.pathExists(ZkUtils.ControllerPath), "failed to kill controller")
    waitUntilControllerEpoch(KafkaController.InitialControllerEpoch, "controller epoch was not persisted after broker failure")
  }

  @Test
  def testControllerMoveIncrementsControllerEpoch(): Unit = {
    servers = makeServers(1)
    TestUtils.waitUntilTrue(() => zkUtils.pathExists(ZkUtils.ControllerPath), "failed to elect a controller")
    waitUntilControllerEpoch(KafkaController.InitialControllerEpoch, "broker failed to set controller epoch")
    servers.head.shutdown()
    servers.head.awaitShutdown()
    servers.head.startup()
    TestUtils.waitUntilTrue(() => zkUtils.pathExists(ZkUtils.ControllerPath), "failed to elect a controller")
    waitUntilControllerEpoch(KafkaController.InitialControllerEpoch + 1, "controller epoch was not incremented after controller move")
  }

  @Test
  def testTopicCreation(): Unit = {
    servers = makeServers(1)
    val tp = TopicAndPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(0))
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
  }

  @Test
  def testTopicCreationWithOfflineReplica(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    val tp = TopicAndPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBrokerId, controllerId))
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = assignment, servers = servers.take(1))
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
  }

  @Test
  def testTopicPartitionExpansion(): Unit = {
    servers = makeServers(1)
    val tp0 = TopicAndPartition("t", 0)
    val tp1 = TopicAndPartition("t", 1)
    val assignment = Map(tp0.partition -> Seq(0))
    val expandedAssignment = Map(tp0.partition -> Seq(0), tp1.partition -> Seq(0))
    TestUtils.createTopic(zkUtils, tp0.topic, partitionReplicaAssignment = assignment, servers = servers)
    zkUtils.updatePersistentPath(ZkUtils.getTopicPath(tp0.topic), zkUtils.replicaAssignmentZkData(expandedAssignment.map(kv => kv._1.toString -> kv._2)))
    waitForPartitionState(tp1, KafkaController.InitialControllerEpoch, 0, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic partition expansion")
    TestUtils.waitUntilMetadataIsPropagated(servers, tp1.topic, tp1.partition)
  }

  @Test
  def testTopicPartitionExpansionWithOfflineReplica(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp0 = TopicAndPartition("t", 0)
    val tp1 = TopicAndPartition("t", 1)
    val assignment = Map(tp0.partition -> Seq(otherBrokerId, controllerId))
    val expandedAssignment = Map(tp0.partition -> Seq(otherBrokerId, controllerId), tp1.partition -> Seq(otherBrokerId, controllerId))
    TestUtils.createTopic(zkUtils, tp0.topic, partitionReplicaAssignment = assignment, servers = servers)
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    zkUtils.updatePersistentPath(ZkUtils.getTopicPath(tp0.topic), zkUtils.replicaAssignmentZkData(expandedAssignment.map(kv => kv._1.toString -> kv._2)))
    waitForPartitionState(tp1, KafkaController.InitialControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic partition expansion")
    TestUtils.waitUntilMetadataIsPropagated(Seq(servers(controllerId)), tp1.topic, tp1.partition)
  }

  @Test
  def testPartitionReassignment(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)

    val metricName = s"kafka.controller:type=ControllerStats,name=${ControllerState.PartitionReassignment.rateAndTimeMetricName.get}"
    val timerCount = timer(metricName).count

    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = TopicAndPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    val reassignment = Map(tp -> Seq(otherBrokerId))
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    zkUtils.createPersistentPath(ZkUtils.ReassignPartitionsPath, ZkUtils.formatAsReassignmentJson(reassignment))
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch + 3,
      "failed to get expected partition state after partition reassignment")
    TestUtils.waitUntilTrue(() => zkUtils.getReplicaAssignmentForTopics(Seq(tp.topic)) == reassignment,
      "failed to get updated partition assignment on topic znode after partition reassignment")
    TestUtils.waitUntilTrue(() => !zkUtils.pathExists(ZkUtils.ReassignPartitionsPath),
      "failed to remove reassign partitions path after completion")

    val updatedTimerCount = timer(metricName).count
    assertTrue(s"Timer count $updatedTimerCount should be greater than $timerCount", updatedTimerCount > timerCount)
  }

  @Test
  def testPartitionReassignmentWithOfflineReplicaHaltingProgress(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = TopicAndPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    val reassignment = Map(tp -> Seq(otherBrokerId))
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    zkUtils.createPersistentPath(ZkUtils.ReassignPartitionsPath, ZkUtils.formatAsReassignmentJson(reassignment))
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch + 1,
      "failed to get expected partition state during partition reassignment with offline replica")
    TestUtils.waitUntilTrue(() => zkUtils.pathExists(ZkUtils.ReassignPartitionsPath),
      "partition reassignment path should remain while reassignment in progress")
  }

  @Test
  def testPartitionReassignmentResumesAfterReplicaComesOnline(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = TopicAndPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(controllerId))
    val reassignment = Map(tp -> Seq(otherBrokerId))
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    zkUtils.createPersistentPath(ZkUtils.ReassignPartitionsPath, ZkUtils.formatAsReassignmentJson(reassignment))
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch + 1,
      "failed to get expected partition state during partition reassignment with offline replica")
    servers(otherBrokerId).startup()
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch + 4,
      "failed to get expected partition state after partition reassignment")
    TestUtils.waitUntilTrue(() => zkUtils.getReplicaAssignmentForTopics(Seq(tp.topic)) == reassignment,
      "failed to get updated partition assignment on topic znode after partition reassignment")
    TestUtils.waitUntilTrue(() => !zkUtils.pathExists(ZkUtils.ReassignPartitionsPath),
      "failed to remove reassign partitions path after completion")
  }

  @Test
  def testPreferredReplicaLeaderElection(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = TopicAndPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBrokerId, controllerId))
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch + 1,
      "failed to get expected partition state upon broker shutdown")
    servers(otherBrokerId).startup()
    TestUtils.waitUntilTrue(() => zkUtils.getInSyncReplicasForPartition(tp.topic, tp.partition).toSet == assignment(tp.partition).toSet, "restarted broker failed to join in-sync replicas")
    zkUtils.createPersistentPath(ZkUtils.PreferredReplicaLeaderElectionPath, ZkUtils.preferredReplicaLeaderElectionZkData(Set(tp)))
    TestUtils.waitUntilTrue(() => !zkUtils.pathExists(ZkUtils.PreferredReplicaLeaderElectionPath),
      "failed to remove preferred replica leader election path after completion")
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch + 2,
      "failed to get expected partition state upon broker startup")
  }

  @Test
  def testPreferredReplicaLeaderElectionWithOfflinePreferredReplica(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = TopicAndPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBrokerId, controllerId))
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    zkUtils.createPersistentPath(ZkUtils.PreferredReplicaLeaderElectionPath, ZkUtils.preferredReplicaLeaderElectionZkData(Set(tp)))
    TestUtils.waitUntilTrue(() => !zkUtils.pathExists(ZkUtils.PreferredReplicaLeaderElectionPath),
      "failed to remove preferred replica leader election path after giving up")
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch + 1,
      "failed to get expected partition state upon broker shutdown")
  }

  @Test
  def testAutoPreferredReplicaLeaderElection(): Unit = {
    servers = makeServers(2, autoLeaderRebalanceEnable = true)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = TopicAndPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(1, 0))
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, controllerId, LeaderAndIsr.initialLeaderEpoch + 1,
      "failed to get expected partition state upon broker shutdown")
    servers(otherBrokerId).startup()
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch + 2,
      "failed to get expected partition state upon broker startup")
  }

  @Test
  def testLeaderAndIsrWhenEntireIsrOfflineAndUncleanLeaderElectionDisabled(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = TopicAndPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBrokerId))
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    servers(otherBrokerId).shutdown()
    servers(otherBrokerId).awaitShutdown()
    TestUtils.waitUntilTrue(() => {
      val leaderIsrAndControllerEpochMap = zkUtils.getPartitionLeaderAndIsrForTopics(Set(tp))
      leaderIsrAndControllerEpochMap.contains(tp) &&
        isExpectedPartitionState(leaderIsrAndControllerEpochMap(tp), KafkaController.InitialControllerEpoch, LeaderAndIsr.NoLeader, LeaderAndIsr.initialLeaderEpoch + 1) &&
        leaderIsrAndControllerEpochMap(tp).leaderAndIsr.isr == List(otherBrokerId)
    }, "failed to get expected partition state after entire isr went offline")
  }

  @Test
  def testLeaderAndIsrWhenEntireIsrOfflineAndUncleanLeaderElectionEnabled(): Unit = {
    servers = makeServers(2, uncleanLeaderElectionEnable = true)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)
    val otherBrokerId = servers.map(_.config.brokerId).filter(_ != controllerId).head
    val tp = TopicAndPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBrokerId))
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, otherBrokerId, LeaderAndIsr.initialLeaderEpoch,
      "failed to get expected partition state upon topic creation")
    servers(1).shutdown()
    servers(1).awaitShutdown()
    TestUtils.waitUntilTrue(() => {
      val leaderIsrAndControllerEpochMap = zkUtils.getPartitionLeaderAndIsrForTopics(Set(tp))
      leaderIsrAndControllerEpochMap.contains(tp) &&
        isExpectedPartitionState(leaderIsrAndControllerEpochMap(tp), KafkaController.InitialControllerEpoch, LeaderAndIsr.NoLeader, LeaderAndIsr.initialLeaderEpoch + 1) &&
        leaderIsrAndControllerEpochMap(tp).leaderAndIsr.isr == List.empty
    }, "failed to get expected partition state after entire isr went offline")
  }

  private def waitUntilControllerEpoch(epoch: Int, message: String): Unit = {
    TestUtils.waitUntilTrue(() => zkUtils.readDataMaybeNull(ZkUtils.ControllerEpochPath)._1.map(_.toInt) == Some(epoch), message)
  }

  private def waitForPartitionState(tp: TopicAndPartition,
                                    controllerEpoch: Int,
                                    leader: Int,
                                    leaderEpoch: Int,
                                    message: String): Unit = {
    TestUtils.waitUntilTrue(() => {
      val leaderIsrAndControllerEpochMap = zkUtils.getPartitionLeaderAndIsrForTopics(Set(tp))
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

  private def makeServers(numConfigs: Int, autoLeaderRebalanceEnable: Boolean = false, uncleanLeaderElectionEnable: Boolean = false) = {
    val configs = TestUtils.createBrokerConfigs(numConfigs, zkConnect)
    configs.foreach { config =>
      config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, autoLeaderRebalanceEnable.toString)
      config.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, uncleanLeaderElectionEnable.toString)
      config.setProperty(KafkaConfig.LeaderImbalanceCheckIntervalSecondsProp, "1")
    }
    configs.map(config => TestUtils.createServer(KafkaConfig.fromProps(config)))
  }

  private def timer(metricName: String): Timer = {
    Metrics.defaultRegistry.allMetrics.asScala.filterKeys(_.getMBeanName == metricName).values.headOption
      .getOrElse(fail(s"Unable to find metric $metricName")).asInstanceOf[Timer]
  }

}
