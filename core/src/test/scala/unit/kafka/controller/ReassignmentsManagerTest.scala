/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.api.LeaderAndIsr
import kafka.cluster.{Broker, EndPoint}
import kafka.zk.{KafkaZkClient, TopicPartitionStateZNode}
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zookeeper.SetDataResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.zookeeper.KeeperException.Code
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}
import org.mockito.Mockito
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.verify

import scala.collection.{Map, Set, mutable}

class ReassignmentsManagerTest {
  private var controllerContext: ControllerContext = null
  private var mockZkClient: KafkaZkClient = null
  private var mockTopicDeletionManager: TopicDeletionManager = null
  private var mockControllerBrokerRequestBatch: ControllerBrokerRequestBatch = null
  private var mockReplicaStateMachine: ReplicaStateMachine = null
  private var mockPartitionStateMachine: PartitionStateMachine = null
  private var mockBrokerRequestBatch: ControllerBrokerRequestBatch = null

  private final val controllerEpoch = 10
  private final val zkEpoch = 105
  private final val topic = "topic"
  private final val tp = new TopicPartition(topic, 0)
  private final val mockPartitionReassignmentHandler = new PartitionReassignmentHandler(null)

  private var partitionReassignmentManager: ReassignmentsManager = null

  @Before
  def setUp(): Unit = {
    controllerContext = new ControllerContext
    controllerContext.epoch = controllerEpoch
    controllerContext.epochZkVersion = zkEpoch
    mockZkClient = Mockito.mock(classOf[KafkaZkClient])
    mockTopicDeletionManager = Mockito.mock(classOf[TopicDeletionManager])
    mockControllerBrokerRequestBatch = Mockito.mock(classOf[ControllerBrokerRequestBatch])
    mockReplicaStateMachine = Mockito.mock(classOf[ReplicaStateMachine])
    mockPartitionStateMachine = Mockito.mock(classOf[PartitionStateMachine])
    mockBrokerRequestBatch = Mockito.mock(classOf[ControllerBrokerRequestBatch])
    partitionReassignmentManager = new ReassignmentsManager(controllerContext, mockZkClient, mockTopicDeletionManager,
      mockReplicaStateMachine, mockPartitionStateMachine, null, mockBrokerRequestBatch, new StateChangeLogger(0, inControllerContext = true, None))
  }

  @Test
  def testTopicsQueuedUpForDeletionDoNotGetReassigned(): Unit = {
    val initialAssignment = ReplicaAssignment(Seq(0, 1, 2), Seq(), Seq())
    controllerContext.updatePartitionFullReplicaAssignment(tp, initialAssignment)
    setLiveBrokers(Seq(0, 1, 2, 3))
    doReturn(true, Nil: _*).when(mockTopicDeletionManager).isTopicQueuedUpForDeletion(tp.topic())
    val results = partitionReassignmentManager.triggerApiReassignment(Map(tp -> Some(Seq(1,2,3))))
    assertTrue(results(tp).is(Errors.UNKNOWN_TOPIC_OR_PARTITION))
  }

  /**
   * Phase A of a partition reassignment denotes the initial trigger of a reassignment.
   *
   * A1. Bump the leader epoch for the partition and send LeaderAndIsr updates to RS.
   * A2. Start new replicas AR by moving replicas in AR to NewReplica state.
   */
  @Test
  def testPhaseAOfPartitionReassignment(): Unit = {
    /*
     * Existing assignment is [1,2,3]
     * We issue a reassignment to [3, 4, 5]
     */
    val expectedAddingReplicas = Seq(4, 5)
    val expectedFullReplicaSet = Seq(3, 4, 5, 1, 2)
    val initialAssignment = ReplicaAssignment(Seq(1, 2, 3), Seq(), Seq())
    val initialLeaderAndIsr = new LeaderAndIsr(1, 1, List(1, 2, 3), zkEpoch)
    controllerContext.partitionAssignments.put(topic, mutable.Map(tp.partition() -> initialAssignment))
    controllerContext.updatePartitionFullReplicaAssignment(tp, initialAssignment)
    controllerContext.partitionsBeingReassigned.add(tp)
    setLiveBrokers(Seq(1,2,3,4,5))
    mockAreReplicasInIsr(tp, List(1, 2, 3), initialLeaderAndIsr)
    val expectedNewAssignment = ReplicaAssignment.fromOldAndNewReplicas(Seq(1, 2, 3), Seq(3, 4, 5))
    assertEquals(expectedFullReplicaSet, expectedNewAssignment.replicas)
    // U1. Should update ZK
    doReturn(mockSetDataResponseOK, Nil: _*).when(mockZkClient).setTopicAssignmentRaw(topic, mutable.Map(tp -> expectedNewAssignment), zkEpoch)
    // U2. Should update memory
    // A1. Should update partition leader epoch in ZK
    val expectedLeaderAndIsr = initialLeaderAndIsr.newEpochAndZkVersion
    doReturn(UpdateLeaderAndIsrResult(Map(tp -> Right(expectedLeaderAndIsr)), Seq()), Nil: _*)
      .when(mockZkClient).updateLeaderAndIsr(Map(tp -> expectedLeaderAndIsr), controllerEpoch, zkEpoch)

    // act
    val results = partitionReassignmentManager.triggerApiReassignment(Map(tp -> Some(Seq(3, 4, 5))))
    assertTrue(s"reassignment failed - $results", results(tp).isSuccess)

     // U2. Should have updated memory
    assertEquals(expectedNewAssignment, controllerContext.partitionFullReplicaAssignment(tp))
    // A1. Should send a LeaderAndIsr request to every replica in ORS + TRS (with the new RS, AR and RR).
    verify(mockBrokerRequestBatch).addLeaderAndIsrRequestForBrokers(
      expectedFullReplicaSet, tp,
      LeaderIsrAndControllerEpoch(expectedLeaderAndIsr, controllerEpoch), expectedNewAssignment, isNew = false
    )
    // A2. replicas in AR -> NewReplica
    expectedAddingReplicas.foreach { newReplica =>
      verify(mockReplicaStateMachine).handleStateChanges(
        Seq(PartitionAndReplica(tp, newReplica)), NewReplica
      )
    }
    verify(mockBrokerRequestBatch).sendRequestsToBrokers(controllerEpoch)
  }

  /**
   * Phase B of a partition reassignment is the part where all the new replicas are in ISR
   *  and the controller finishes the reassignment
   *   B1. Move all replicas in AR to OnlineReplica state.
   *   B2. Set RS = TRS, AR = [], RR = [] in memory.
   *   B3. Send a LeaderAndIsr request with RS = TRS. This will prevent the leader from adding any replica in TRS - ORS back in the isr.
   *       If the current leader is not in TRS or isn't alive, we move the leader to a new replica in TRS.
   *       We may send the LeaderAndIsr to more than the TRS replicas due to the
   *       way the partition state machine works (it reads replicas from ZK)
   *   B4. Move all replicas in RR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
   *       isr to remove RR in ZooKeeper and send a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
   *       After that, we send a StopReplica (delete = false) to the replicas in RR.
   *   B5. Move all replicas in RR to NonExistentReplica state. This will send a StopReplica (delete = true) to
   *       the replicas in RR to physically delete the replicas on disk.
   *   B6. Update ZK with RS=TRS, AR=[], RR=[].
   *   B7. Remove the ISR reassign listener and maybe update the /admin/reassign_partitions path in ZK to remove this partition from it if present.
   *   B8. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
   *
   */
  @Test
  def testPhaseBOfPartitionReassignment(): Unit = {
    /*
     * Existing assignment is [1,2,3]
     * We had issued a reassignment to [3, 4, 5] and now all replicas are in ISR
     */
    val expectedRemovingReplicas = Seq(PartitionAndReplica(tp, 1), PartitionAndReplica(tp, 2))
    val initialAssignment = ReplicaAssignment.fromOldAndNewReplicas(Seq(1, 2, 3), Seq(3, 4, 5))
    val expectedNewAssignment = ReplicaAssignment(Seq(3, 4, 5), Seq(), Seq())
    val initialLeaderAndIsr = new LeaderAndIsr(1, 1, List(1, 2, 3, 4, 5), zkEpoch)
    setLiveBrokers(Seq(1, 2, 3, 4, 5))
    controllerContext.partitionAssignments.put(topic, mutable.Map(tp.partition() -> initialAssignment))
    controllerContext.updatePartitionFullReplicaAssignment(tp, initialAssignment)
    controllerContext.partitionsBeingReassigned.add(tp)
    controllerContext.partitionLeadershipInfo.put(tp,
      LeaderIsrAndControllerEpoch(initialLeaderAndIsr, controllerEpoch))
    mockAreReplicasInIsr(tp, List(1, 2, 3, 4, 5), initialLeaderAndIsr)

    // B2. Set RS = TRS, AR = [], RR = [] in memory.
    // B3. Send a LeaderAndIsr request with RS = TRS.
    //     If the current leader is not in TRS or isn't alive, we move the leader to a new replica in TRS.
    //     We may send the LeaderAndIsr to more than the TRS replicas due to the way the partition state machine works (it reads replicas from ZK)
    doReturn(Map.empty[TopicPartition, Either[Throwable, LeaderAndIsr]], Nil: _*)
      .when(mockPartitionStateMachine).handleStateChanges(Seq(tp), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
    // B6. Update ZK with RS = TRS, AR = [], RR = [].
    doReturn(mockSetDataResponseOK, Nil: _*).when(mockZkClient)
      .setTopicAssignmentRaw(tp.topic(), mutable.Map(tp -> expectedNewAssignment), zkEpoch)
    // B7. Remove the ISR reassign listener and maybe update the /admin/reassign_partitions path in ZK to remove this partition from it if present.
    doReturn(true, Nil: _*).when(mockZkClient).reassignPartitionsInProgress()
    doReturn(Map(tp -> Seq(1, 2, 3)), Nil: _*).when(mockZkClient).getPartitionReassignment()
    doReturn(false, Nil: _*).when(mockZkClient).registerZNodeChangeHandlerAndCheckExistence(mockPartitionReassignmentHandler)

    // act
    partitionReassignmentManager.maybeResumeReassignment(tp)

    // B1. All adding replicas moved to OnlineReplica state.
    verify(mockReplicaStateMachine).handleStateChanges(
      initialAssignment.addingReplicas.map(PartitionAndReplica(tp, _)), OnlineReplica
    )
    // B2. Should have updated memory
    assertEquals(expectedNewAssignment, controllerContext.partitionFullReplicaAssignment(tp))
    // B4. replicas in RR -> Offline (force those replicas out of isr)
    // B5. replicas in RR -> NonExistentReplica (force those replicas to be deleted)
    verify(mockReplicaStateMachine).handleStateChanges(expectedRemovingReplicas, OfflineReplica)
    verify(mockReplicaStateMachine).handleStateChanges(expectedRemovingReplicas, ReplicaDeletionStarted)
    verify(mockReplicaStateMachine).handleStateChanges(expectedRemovingReplicas, ReplicaDeletionSuccessful)
    verify(mockReplicaStateMachine).handleStateChanges(expectedRemovingReplicas, NonExistentReplica)
    // B7. Should have cleared in-memory partitionsBeingReassigned and unregistered the znode handler
    assertEquals(Set(), controllerContext.partitionsBeingReassigned)
    verify(mockZkClient).unregisterZNodeChangeHandler(TopicPartitionStateZNode.path(tp))
    // B8. Resend the update metadata request to every broker
    verify(mockBrokerRequestBatch).addUpdateMetadataRequestForBrokers(controllerContext.liveBrokerIds.toSeq, Set(tp))
    verify(mockBrokerRequestBatch).sendRequestsToBrokers(controllerEpoch)
    verify(mockTopicDeletionManager).resumeDeletionForTopics(Set(tp.topic()))
  }

  def setLiveBrokers(brokerIds: Seq[Int]): Unit = {
    val endpoint1 = new EndPoint("localhost", 9997, new ListenerName("blah"),
      SecurityProtocol.PLAINTEXT)
    val brokerEpochs = brokerIds.map {
      id => (Broker(id, Seq(endpoint1), rack = None), 1L)
    }.toMap
    controllerContext.setLiveBrokerAndEpochs(brokerEpochs)
  }

  /**
   * To determine what phase of the reassignment we are in, we check whether the target replicas are in the ISR set
   * If they aren't, we enter phase A. If they are - phase B
   */
  def mockAreReplicasInIsr(tp: TopicPartition, isr: List[Int], leaderAndIsr: LeaderAndIsr): Unit = {
    val tpStateMap: Map[TopicPartition, LeaderIsrAndControllerEpoch] = Map(
      tp -> LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    )
    doReturn(tpStateMap, Nil: _*).when(mockZkClient).getTopicPartitionStates(Seq(tp))
  }

  def mockSetDataResponseOK: SetDataResponse =
    SetDataResponse(Code.OK, "", None, null, null)
}