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

import kafka.api.LeaderAndIsr
import kafka.cluster.{Broker, EndPoint}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.{KafkaZkClient, TopicPartitionStateZNode}
import kafka.zookeeper.{GetDataResponse, ResponseMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.Stat
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{mock, verify, when}

class ReplicaStateMachineTest {
  private var controllerContext: ControllerContext = _
  private var mockZkClient: KafkaZkClient = _
  private var mockControllerBrokerRequestBatch: ControllerBrokerRequestBatch = _
  private var replicaStateMachine: ReplicaStateMachine = _

  private val brokerId = 5
  private val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId, "zkConnect"))
  private val controllerEpoch = 50
  private val partition = new TopicPartition("t", 0)
  private val partitions = Seq(partition)
  private val replica = PartitionAndReplica(partition, brokerId)
  private val replicas = Seq(replica)

  @BeforeEach
  def setUp(): Unit = {
    controllerContext = new ControllerContext
    controllerContext.epoch = controllerEpoch
    mockZkClient = mock(classOf[KafkaZkClient])
    mockControllerBrokerRequestBatch = mock(classOf[ControllerBrokerRequestBatch])
    replicaStateMachine = new ZkReplicaStateMachine(config, new StateChangeLogger(brokerId, true, None),
      controllerContext, mockZkClient, mockControllerBrokerRequestBatch)
  }

  private def replicaState(replica: PartitionAndReplica): ReplicaState = {
    controllerContext.replicaState(replica)
  }

  @Test
  def testStartupOnlinePartition(): Unit = {
    val endpoint1 = new EndPoint("localhost", 9997, new ListenerName("blah"),
      SecurityProtocol.PLAINTEXT)
    val liveBrokerEpochs = Map(Broker(brokerId, Seq(endpoint1), rack = None) -> 1L)
    controllerContext.setLiveBrokers(liveBrokerEpochs)
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
    assertEquals(None, controllerContext.replicaStates.get(replica))
    replicaStateMachine.startup()
    assertEquals(OnlineReplica, replicaState(replica))
  }

  @Test
  def testStartupOfflinePartition(): Unit = {
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
    assertEquals(None, controllerContext.replicaStates.get(replica))
    replicaStateMachine.startup()
    assertEquals(OfflineReplica, replicaState(replica))
  }

  @Test
  def testStartupWithReplicaWithoutLeader(): Unit = {
    val shutdownBrokerId = 100
    val offlineReplica = PartitionAndReplica(partition, shutdownBrokerId)
    val endpoint1 = new EndPoint("localhost", 9997, new ListenerName("blah"),
      SecurityProtocol.PLAINTEXT)
    val liveBrokerEpochs = Map(Broker(brokerId, Seq(endpoint1), rack = None) -> 1L)
    controllerContext.setLiveBrokers(liveBrokerEpochs)
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(shutdownBrokerId)))
    assertEquals(None, controllerContext.replicaStates.get(offlineReplica))
    replicaStateMachine.startup()
    assertEquals(OfflineReplica, replicaState(offlineReplica))
  }

  @Test
  def testNonexistentReplicaToNewReplicaTransition(): Unit = {
    replicaStateMachine.handleStateChanges(replicas, NewReplica)
    assertEquals(NewReplica, replicaState(replica))
  }

  @Test
  def testInvalidNonexistentReplicaToOnlineReplicaTransition(): Unit = {
    replicaStateMachine.handleStateChanges(replicas, OnlineReplica)
    assertEquals(NonExistentReplica, replicaState(replica))
  }

  @Test
  def testInvalidNonexistentReplicaToOfflineReplicaTransition(): Unit = {
    replicaStateMachine.handleStateChanges(replicas, OfflineReplica)
    assertEquals(NonExistentReplica, replicaState(replica))
  }

  @Test
  def testInvalidNonexistentReplicaToReplicaDeletionStartedTransition(): Unit = {
    replicaStateMachine.handleStateChanges(replicas, ReplicaDeletionStarted)
    assertEquals(NonExistentReplica, replicaState(replica))
  }

  @Test
  def testInvalidNonexistentReplicaToReplicaDeletionIneligibleTransition(): Unit = {
    replicaStateMachine.handleStateChanges(replicas, ReplicaDeletionIneligible)
    assertEquals(NonExistentReplica, replicaState(replica))
  }

  @Test
  def testInvalidNonexistentReplicaToReplicaDeletionSuccessfulTransition(): Unit = {
    replicaStateMachine.handleStateChanges(replicas, ReplicaDeletionSuccessful)
    assertEquals(NonExistentReplica, replicaState(replica))
  }

  @Test
  def testInvalidNewReplicaToNonexistentReplicaTransition(): Unit = {
    testInvalidTransition(NewReplica, NonExistentReplica)
  }

  @Test
  def testNewReplicaToOnlineReplicaTransition(): Unit = {
    controllerContext.putReplicaState(replica, NewReplica)
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
    replicaStateMachine.handleStateChanges(replicas, OnlineReplica)
    assertEquals(OnlineReplica, replicaState(replica))
  }

  @Test
  def testNewReplicaToOfflineReplicaTransition(): Unit = {
    val endpoint1 = new EndPoint("localhost", 9997, new ListenerName("blah"),
      SecurityProtocol.PLAINTEXT)
    val liveBrokerEpochs = Map(Broker(brokerId, Seq(endpoint1), rack = None) -> 1L)
    controllerContext.setLiveBrokers(liveBrokerEpochs)
    controllerContext.putReplicaState(replica, NewReplica)

    replicaStateMachine.handleStateChanges(replicas, OfflineReplica)
    verify(mockControllerBrokerRequestBatch).newBatch()
    verify(mockControllerBrokerRequestBatch).addStopReplicaRequestForBrokers(ArgumentMatchers.eq(Seq(brokerId)), ArgumentMatchers.eq(partition), ArgumentMatchers.eq(false))
    verify(mockControllerBrokerRequestBatch).addUpdateMetadataRequestForBrokers(ArgumentMatchers.eq(Seq(brokerId)),  ArgumentMatchers.eq(Set(partition)))
    verify(mockControllerBrokerRequestBatch).sendRequestsToBrokers(ArgumentMatchers.eq(controllerEpoch))
    assertEquals(OfflineReplica, replicaState(replica))
  }

  @Test
  def testInvalidNewReplicaToReplicaDeletionStartedTransition(): Unit = {
    testInvalidTransition(NewReplica, ReplicaDeletionStarted)
  }

  @Test
  def testInvalidNewReplicaToReplicaDeletionIneligibleTransition(): Unit = {
    testInvalidTransition(NewReplica, ReplicaDeletionIneligible)
  }

  @Test
  def testInvalidNewReplicaToReplicaDeletionSuccessfulTransition(): Unit = {
    testInvalidTransition(NewReplica, ReplicaDeletionSuccessful)
  }

  @Test
  def testInvalidOnlineReplicaToNonexistentReplicaTransition(): Unit = {
    testInvalidTransition(OnlineReplica, NonExistentReplica)
  }

  @Test
  def testInvalidOnlineReplicaToNewReplicaTransition(): Unit = {
    testInvalidTransition(OnlineReplica, NewReplica)
  }

  @Test
  def testOnlineReplicaToOnlineReplicaTransition(): Unit = {
    controllerContext.putReplicaState(replica, OnlineReplica)
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)

    replicaStateMachine.handleStateChanges(replicas, OnlineReplica)
    verify(mockControllerBrokerRequestBatch).newBatch()
    verify(mockControllerBrokerRequestBatch).addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, leaderIsrAndControllerEpoch, replicaAssignment(Seq(brokerId)), isNew = false)
    verify(mockControllerBrokerRequestBatch).sendRequestsToBrokers(controllerEpoch)
    assertEquals(OnlineReplica, replicaState(replica))
  }

  @Test
  def testOnlineReplicaToOfflineReplicaTransition(): Unit = {
    val otherBrokerId = brokerId + 1
    val replicaIds = List(brokerId, otherBrokerId)
    controllerContext.putReplicaState(replica, OnlineReplica)
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(replicaIds))
    val leaderAndIsr = LeaderAndIsr(brokerId, replicaIds)
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)

    val stat = new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    val adjustedLeaderAndIsr = leaderAndIsr.newLeaderAndIsr(LeaderAndIsr.NoLeader, List(otherBrokerId))
    val updatedLeaderAndIsr = adjustedLeaderAndIsr.withPartitionEpoch(adjustedLeaderAndIsr.partitionEpoch + 1)
    val updatedLeaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(updatedLeaderAndIsr, controllerEpoch)
    when(mockZkClient.getTopicPartitionStatesRaw(partitions)).thenReturn(
      Seq(GetDataResponse(Code.OK, null, Some(partition),
        TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch), stat, ResponseMetadata(0, 0))))
    when(mockZkClient.updateLeaderAndIsr(Map(partition -> adjustedLeaderAndIsr), controllerEpoch, controllerContext.epochZkVersion))
      .thenReturn(UpdateLeaderAndIsrResult(Map(partition -> Right(updatedLeaderAndIsr)), Seq.empty))

    replicaStateMachine.handleStateChanges(replicas, OfflineReplica)
    verify(mockControllerBrokerRequestBatch).newBatch()
    verify(mockControllerBrokerRequestBatch).addStopReplicaRequestForBrokers(ArgumentMatchers.eq(Seq(brokerId)), ArgumentMatchers.eq(partition), ArgumentMatchers.eq(false))
    verify(mockControllerBrokerRequestBatch).addLeaderAndIsrRequestForBrokers(Seq(otherBrokerId),
      partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment(replicaIds), isNew = false)
    verify(mockControllerBrokerRequestBatch).sendRequestsToBrokers(controllerEpoch)
    verify(mockZkClient).getTopicPartitionStatesRaw(any())
    verify(mockZkClient).updateLeaderAndIsr(any(), anyInt(), anyInt())
    assertEquals(updatedLeaderIsrAndControllerEpoch, controllerContext.partitionLeadershipInfo(partition).get)
    assertEquals(OfflineReplica, replicaState(replica))
  }

  @Test
  def testInvalidOnlineReplicaToReplicaDeletionStartedTransition(): Unit = {
    testInvalidTransition(OnlineReplica, ReplicaDeletionStarted)
  }

  @Test
  def testInvalidOnlineReplicaToReplicaDeletionIneligibleTransition(): Unit = {
    testInvalidTransition(OnlineReplica, ReplicaDeletionIneligible)
  }

  @Test
  def testInvalidOnlineReplicaToReplicaDeletionSuccessfulTransition(): Unit = {
    testInvalidTransition(OnlineReplica, ReplicaDeletionSuccessful)
  }

  @Test
  def testInvalidOfflineReplicaToNonexistentReplicaTransition(): Unit = {
    testInvalidTransition(OfflineReplica, NonExistentReplica)
  }

  @Test
  def testInvalidOfflineReplicaToNewReplicaTransition(): Unit = {
    testInvalidTransition(OfflineReplica, NewReplica)
  }

  @Test
  def testOfflineReplicaToOnlineReplicaTransition(): Unit = {
    controllerContext.putReplicaState(replica, OfflineReplica)
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
    replicaStateMachine.handleStateChanges(replicas, OnlineReplica)
    verify(mockControllerBrokerRequestBatch).newBatch()
    verify(mockControllerBrokerRequestBatch).addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, leaderIsrAndControllerEpoch, replicaAssignment(Seq(brokerId)), isNew = false)
    verify(mockControllerBrokerRequestBatch).sendRequestsToBrokers(controllerEpoch)
    assertEquals(OnlineReplica, replicaState(replica))
  }

  @Test
  def testOfflineReplicaToReplicaDeletionStartedTransition(): Unit = {
    controllerContext.putReplicaState(replica, OfflineReplica)
    replicaStateMachine.handleStateChanges(replicas, ReplicaDeletionStarted)
    verify(mockControllerBrokerRequestBatch).newBatch()
    verify(mockControllerBrokerRequestBatch).addStopReplicaRequestForBrokers(Seq(brokerId), partition, true)
    verify(mockControllerBrokerRequestBatch).sendRequestsToBrokers(controllerEpoch)
    assertEquals(ReplicaDeletionStarted, replicaState(replica))
  }

  @Test
  def testOfflineReplicaToReplicaDeletionIneligibleTransition(): Unit = {
    controllerContext.putReplicaState(replica, OfflineReplica)
    replicaStateMachine.handleStateChanges(replicas, ReplicaDeletionIneligible)
    assertEquals(ReplicaDeletionIneligible, replicaState(replica))
  }

  @Test
  def testInvalidOfflineReplicaToReplicaDeletionSuccessfulTransition(): Unit = {
    testInvalidTransition(OfflineReplica, ReplicaDeletionSuccessful)
  }

  @Test
  def testInvalidReplicaDeletionStartedToNonexistentReplicaTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionStarted, NonExistentReplica)
  }

  @Test
  def testInvalidReplicaDeletionStartedToNewReplicaTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionStarted, NewReplica)
  }

  @Test
  def testInvalidReplicaDeletionStartedToOnlineReplicaTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionStarted, OnlineReplica)
  }

  @Test
  def testInvalidReplicaDeletionStartedToOfflineReplicaTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionStarted, OfflineReplica)
  }

  @Test
  def testReplicaDeletionStartedToReplicaDeletionIneligibleTransition(): Unit = {
    controllerContext.putReplicaState(replica, ReplicaDeletionStarted)
    replicaStateMachine.handleStateChanges(replicas, ReplicaDeletionIneligible)
    assertEquals(ReplicaDeletionIneligible, replicaState(replica))
  }

  @Test
  def testReplicaDeletionStartedToReplicaDeletionSuccessfulTransition(): Unit = {
    controllerContext.putReplicaState(replica, ReplicaDeletionStarted)
    replicaStateMachine.handleStateChanges(replicas, ReplicaDeletionSuccessful)
    assertEquals(ReplicaDeletionSuccessful, replicaState(replica))
  }

  @Test
  def testReplicaDeletionSuccessfulToNonexistentReplicaTransition(): Unit = {
    controllerContext.putReplicaState(replica, ReplicaDeletionSuccessful)
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
    replicaStateMachine.handleStateChanges(replicas, NonExistentReplica)
    assertEquals(Seq.empty, controllerContext.partitionReplicaAssignment(partition))
    assertEquals(None, controllerContext.replicaStates.get(replica))
  }

  @Test
  def testInvalidReplicaDeletionSuccessfulToNewReplicaTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionSuccessful, NewReplica)
  }

  @Test
  def testInvalidReplicaDeletionSuccessfulToOnlineReplicaTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionSuccessful, OnlineReplica)
  }

  @Test
  def testInvalidReplicaDeletionSuccessfulToOfflineReplicaTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionSuccessful, OfflineReplica)
  }

  @Test
  def testInvalidReplicaDeletionSuccessfulToReplicaDeletionStartedTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionSuccessful, ReplicaDeletionStarted)
  }

  @Test
  def testInvalidReplicaDeletionSuccessfulToReplicaDeletionIneligibleTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionSuccessful, ReplicaDeletionIneligible)
  }

  @Test
  def testInvalidReplicaDeletionIneligibleToNonexistentReplicaTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionIneligible, NonExistentReplica)
  }

  @Test
  def testInvalidReplicaDeletionIneligibleToNewReplicaTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionIneligible, NewReplica)
  }

  @Test
  def testReplicaDeletionIneligibleToOnlineReplicaTransition(): Unit = {
    controllerContext.putReplicaState(replica, ReplicaDeletionIneligible)
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
    replicaStateMachine.handleStateChanges(replicas, OnlineReplica)
    verify(mockControllerBrokerRequestBatch).newBatch()
    verify(mockControllerBrokerRequestBatch).addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, leaderIsrAndControllerEpoch, replicaAssignment(Seq(brokerId)), isNew = false)
    verify(mockControllerBrokerRequestBatch).sendRequestsToBrokers(controllerEpoch)
    assertEquals(OnlineReplica, replicaState(replica))
  }

  @Test
  def testInvalidReplicaDeletionIneligibleToReplicaDeletionStartedTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionIneligible, ReplicaDeletionStarted)
  }

  @Test
  def testInvalidReplicaDeletionIneligibleToReplicaDeletionSuccessfulTransition(): Unit = {
    testInvalidTransition(ReplicaDeletionIneligible, ReplicaDeletionSuccessful)
  }

  private def testInvalidTransition(fromState: ReplicaState, toState: ReplicaState): Unit = {
    controllerContext.putReplicaState(replica, fromState)
    replicaStateMachine.handleStateChanges(replicas, toState)
    assertEquals(fromState, replicaState(replica))
  }

  private def replicaAssignment(replicas: Seq[Int]): ReplicaAssignment = ReplicaAssignment(replicas, Seq(), Seq())

}
