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
import kafka.common.{PartitionReassignment, TopicAndPartition}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{TestUtils, ZkUtils}
import kafka.zk.{PartitionReassignmentZNode, ReassignmentRequestZNode, ReassignmentsZNode, ZooKeeperTestHarness}
import org.apache.kafka.common.TopicPartition
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertTrue, assertFalse, assertEquals}

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

  private def changeReassignment(controllerId: Int,
                                 tp: TopicPartition,
                                 reassignments: Seq[Map[TopicPartition, Seq[Int]]],
                                 interReassignmentCallback: (Int, Map[TopicPartition, Seq[Int]])=>Unit = (_,_)=>()) = {
    val metricName = s"kafka.controller:type=ControllerStats,name=${ControllerState.PartitionReassignment.rateAndTimeMetricName.get}"
    val timerCount = timer(metricName).count

    val initialAssignment = Map(tp.partition -> Seq(controllerId))
    info(s"Controller is $controllerId")
    info(s"Initial assignment is to ${initialAssignment.mkString(",")}")

    val reassignmentPath = ZkUtils.ReassignmentsPath + s"/${tp.topic}/${tp.partition}"
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = initialAssignment, servers = servers)

    reassignments.zipWithIndex.map{ case (reassignment, i) =>
      info(s"Next assignment will be to ${reassignment.mkString(",")}")
      val request = new String(ReassignmentRequestZNode.encode(reassignment, false), "UTF-8")
      val requestPath = ReassignmentRequestZNode.path("request_" + "%010d".format(i))
      (i, reassignment, request, requestPath)
    }.foreach{ case (i, reassignment, toRequest, toRequestPath) =>
      info(s"Assigning to $toRequest")
      zkUtils.createPersistentPath(toRequestPath, toRequest)
      // Low pause so that we change the existing initialReassignment, not create a new one
      TestUtils.waitUntilTrue(() => !zkUtils.pathExists(toRequestPath),
        s"failed to remove path ${toRequestPath} after completion", pause = 1L)
        // TODO execute a callback here, e.g. to check that a broker does or doesn't have a replica
      interReassignmentCallback(i, reassignment)
    }

    TestUtils.waitUntilTrue(() => {
        zkUtils.getReplicaAssignmentForTopics(Seq(tp.topic)).map { case (tap, assignment) =>
          new TopicPartition(tap.topic, tap.partition) -> assignment
        }.toMap == reassignments.last
      },
      "failed to get updated partition assignment on topic znode after partition reassignment")
    TestUtils.waitUntilTrue(() => !zkUtils.pathExists(reassignmentPath),
      s"failed to remove path $reassignmentPath after completion")
    val updatedTimerCount = timer(metricName).count
    assertTrue(s"Timer count $updatedTimerCount should be greater than $timerCount", updatedTimerCount > timerCount)
  }

  @Test
  def testPartitionReassignmentCancellation(): Unit = {
    // bijective with the sequence of assignments [0] -> [1] -> [0] with 0 the controller throughout
    // "bijective" because we don't know which broker will be elected controller
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)
    val nonControllers = servers.map(_.config.brokerId).filter(_ != controllerId)
    val tp = new TopicPartition("t", 0)
    val initialReassignment = Map(tp -> Seq(nonControllers.head))
    val changedReassignment = Map(tp -> Seq(controllerId))
    changeReassignment(controllerId, tp, Seq(initialReassignment, changedReassignment))
  }

  @Test
  def testPartitionReassignmentChangeWithDrop(): Unit = {
    // bijective with the initialReassignment [0] -> [1] -> [2] -> [3] with 0 the controller throughout
    // "bijective" because we don't know which broker will be elected controller
    // on the 2nd reassignment broker 1 can (and should) drop it's replica (before the reassignment completes)
    // on the 3rd reassignment broker 2 can (and should) drop it's replica (before the reassignment completes)
    servers = makeServers(4)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)
    val nonControllers = servers.map(_.config.brokerId).filter(_ != controllerId)
    val tp = new TopicPartition("t", 0)
    val initialReassignment = Map(tp -> Seq(nonControllers(0)))
    val changedReassignment = Map(tp -> Seq(nonControllers(1)))
    val changedReassignment2 = Map(tp -> Seq(nonControllers(2)))
    changeReassignment(controllerId, tp, Seq(initialReassignment, changedReassignment, changedReassignment2),
      { case (i, r) =>
        if (i == 2) {
          // [1] should have deleted its replica by now
          assertFalse(s"After reassignment to ${r(tp)}, at step $i, broker ${nonControllers(0)} is replicating $tp",
            servers(nonControllers(0)).replicaManager.getPartition(tp).isDefined)
          assertTrue(s"After reassignment to ${r(tp)}, at step $i, broker ${nonControllers(2)} is not replicating $tp",
            servers(nonControllers(2)).replicaManager.getPartition(tp).isDefined)
        }
    })
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
    val otherBroker = servers.find(_.config.brokerId != controllerId).get
    val tp = TopicAndPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBroker.config.brokerId, controllerId))
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    preferredReplicaLeaderElection(controllerId, otherBroker, tp, assignment(tp.partition).toSet, LeaderAndIsr.initialLeaderEpoch)
  }

  @Test
  def testBackToBackPreferredReplicaLeaderElections(): Unit = {
    servers = makeServers(2)
    val controllerId = TestUtils.waitUntilControllerElected(zkUtils)
    val otherBroker = servers.find(_.config.brokerId != controllerId).get
    val tp = TopicAndPartition("t", 0)
    val assignment = Map(tp.partition -> Seq(otherBroker.config.brokerId, controllerId))
    TestUtils.createTopic(zkUtils, tp.topic, partitionReplicaAssignment = assignment, servers = servers)
    preferredReplicaLeaderElection(controllerId, otherBroker, tp, assignment(tp.partition).toSet, LeaderAndIsr.initialLeaderEpoch)
    preferredReplicaLeaderElection(controllerId, otherBroker, tp, assignment(tp.partition).toSet, LeaderAndIsr.initialLeaderEpoch + 2)
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
        leaderIsrAndControllerEpochMap(tp).leaderAndIsr.isr == List(otherBrokerId)
    }, "failed to get expected partition state after entire isr went offline")
  }

  private def preferredReplicaLeaderElection(controllerId: Int, otherBroker: KafkaServer, tp: TopicAndPartition,
                                             replicas: Set[Int], leaderEpoch: Int): Unit = {
    otherBroker.shutdown()
    otherBroker.awaitShutdown()
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, controllerId, leaderEpoch + 1,
      "failed to get expected partition state upon broker shutdown")
    otherBroker.startup()
    TestUtils.waitUntilTrue(() => zkUtils.getInSyncReplicasForPartition(tp.topic, tp.partition).toSet == replicas, "restarted broker failed to join in-sync replicas")
    zkUtils.createPersistentPath(ZkUtils.PreferredReplicaLeaderElectionPath, ZkUtils.preferredReplicaLeaderElectionZkData(Set(tp)))
    TestUtils.waitUntilTrue(() => !zkUtils.pathExists(ZkUtils.PreferredReplicaLeaderElectionPath),
      "failed to remove preferred replica leader election path after completion")
    waitForPartitionState(tp, KafkaController.InitialControllerEpoch, otherBroker.config.brokerId, leaderEpoch + 2,
      "failed to get expected partition state upon broker startup")
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
