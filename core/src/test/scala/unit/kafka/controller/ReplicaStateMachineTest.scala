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
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.{KafkaZkClient, TopicPartitionStateZNode}
import kafka.zookeeper.{GetDataResponse, ResponseMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.Stat
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{Before, Test}
import org.scalatest.junit.JUnitSuite

import scala.collection.mutable

class ReplicaStateMachineTest extends JUnitSuite {
  private var controllerContext: ControllerContext = null
  private var mockZkClient: KafkaZkClient = null
  private var mockControllerBrokerRequestBatch: ControllerBrokerRequestBatch = null
  private var mockTopicDeletionManager: TopicDeletionManager = null
  private var replicaState: mutable.Map[PartitionAndReplica, ReplicaState] = null
  private var replicaStateMachine: ReplicaStateMachine = null

  private val brokerId = 5
  private val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId, "zkConnect"))
  private val controllerEpoch = 50
  private val partition = new TopicPartition("t", 0)
  private val partitions = Seq(partition)
  private val replica = PartitionAndReplica(partition, brokerId)
  private val replicas = Seq(replica)

  @Before
  def setUp(): Unit = {
    controllerContext = new ControllerContext
    controllerContext.epoch = controllerEpoch
    mockZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    mockControllerBrokerRequestBatch = EasyMock.createMock(classOf[ControllerBrokerRequestBatch])
    mockTopicDeletionManager = EasyMock.createMock(classOf[TopicDeletionManager])
    replicaState = mutable.Map.empty[PartitionAndReplica, ReplicaState]
    replicaStateMachine = new ReplicaStateMachine(config, new StateChangeLogger(brokerId, true, None), controllerContext, mockTopicDeletionManager, mockZkClient,
      replicaState, mockControllerBrokerRequestBatch)
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
    replicaState.put(replica, NewReplica)
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    replicaStateMachine.handleStateChanges(replicas, OnlineReplica)
    assertEquals(OnlineReplica, replicaState(replica))
  }

  @Test
  def testNewReplicaToOfflineReplicaTransition(): Unit = {
    replicaState.put(replica, NewReplica)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockControllerBrokerRequestBatch.addStopReplicaRequestForBrokers(EasyMock.eq(Seq(brokerId)),
      EasyMock.eq(partition), EasyMock.eq(false), EasyMock.anyObject()))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockControllerBrokerRequestBatch)
    replicaStateMachine.handleStateChanges(replicas, OfflineReplica)
    EasyMock.verify(mockControllerBrokerRequestBatch)
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
    replicaState.put(replica, OnlineReplica)
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, leaderIsrAndControllerEpoch, Seq(brokerId), isNew = false))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)
    replicaStateMachine.handleStateChanges(replicas, OnlineReplica)
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlineReplica, replicaState(replica))
  }

  @Test
  def testOnlineReplicaToOfflineReplicaTransition(): Unit = {
    val otherBrokerId = brokerId + 1
    val replicaIds = List(brokerId, otherBrokerId)
    replicaState.put(replica, OnlineReplica)
    controllerContext.updatePartitionReplicaAssignment(partition, replicaIds)
    val leaderAndIsr = LeaderAndIsr(brokerId, replicaIds)
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    val stat = new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockControllerBrokerRequestBatch.addStopReplicaRequestForBrokers(EasyMock.eq(Seq(brokerId)),
      EasyMock.eq(partition), EasyMock.eq(false), EasyMock.anyObject()))
    val adjustedLeaderAndIsr = leaderAndIsr.newLeaderAndIsr(LeaderAndIsr.NoLeader, List(otherBrokerId))
    val updatedLeaderAndIsr = adjustedLeaderAndIsr.withZkVersion(adjustedLeaderAndIsr .zkVersion + 1)
    val updatedLeaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(updatedLeaderAndIsr, controllerEpoch)
    EasyMock.expect(mockZkClient.getTopicPartitionStatesRaw(partitions)).andReturn(
      Seq(GetDataResponse(Code.OK, null, Some(partition),
        TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch), stat, ResponseMetadata(0, 0))))
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> adjustedLeaderAndIsr), controllerEpoch, controllerContext.epochZkVersion))
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> updatedLeaderAndIsr), Seq.empty, Map.empty))
    EasyMock.expect(mockTopicDeletionManager.isTopicQueuedUpForDeletion(partition.topic)).andReturn(false)
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(otherBrokerId),
      partition, updatedLeaderIsrAndControllerEpoch, replicaIds, isNew = false))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))

    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch, mockTopicDeletionManager)
    replicaStateMachine.handleStateChanges(replicas, OfflineReplica)
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch, mockTopicDeletionManager)
    assertEquals(updatedLeaderIsrAndControllerEpoch, controllerContext.partitionLeadershipInfo(partition))
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
    replicaState.put(replica, OfflineReplica)
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, leaderIsrAndControllerEpoch, Seq(brokerId), isNew = false))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)
    replicaStateMachine.handleStateChanges(replicas, OnlineReplica)
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlineReplica, replicaState(replica))
  }

  @Test
  def testOfflineReplicaToReplicaDeletionStartedTransition(): Unit = {
    val callbacks = new Callbacks()
    replicaState.put(replica, OfflineReplica)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockControllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(brokerId),
      partition, true, callbacks.stopReplicaResponseCallback))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)
    replicaStateMachine.handleStateChanges(replicas, ReplicaDeletionStarted, callbacks)
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(ReplicaDeletionStarted, replicaState(replica))
  }

  @Test
  def testInvalidOfflineReplicaToReplicaDeletionIneligibleTransition(): Unit = {
    testInvalidTransition(OfflineReplica, ReplicaDeletionIneligible)
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
    replicaState.put(replica, ReplicaDeletionStarted)
    replicaStateMachine.handleStateChanges(replicas, ReplicaDeletionIneligible)
    assertEquals(ReplicaDeletionIneligible, replicaState(replica))
  }

  @Test
  def testReplicaDeletionStartedToReplicaDeletionSuccessfulTransition(): Unit = {
    replicaState.put(replica, ReplicaDeletionStarted)
    replicaStateMachine.handleStateChanges(replicas, ReplicaDeletionSuccessful)
    assertEquals(ReplicaDeletionSuccessful, replicaState(replica))
  }

  @Test
  def testReplicaDeletionSuccessfulToNonexistentReplicaTransition(): Unit = {
    replicaState.put(replica, ReplicaDeletionSuccessful)
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    replicaStateMachine.handleStateChanges(replicas, NonExistentReplica)
    assertEquals(Seq.empty, controllerContext.partitionReplicaAssignment(partition))
    assertEquals(None, replicaState.get(replica))
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
    replicaState.put(replica, ReplicaDeletionIneligible)
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, leaderIsrAndControllerEpoch, Seq(brokerId), isNew = false))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)
    replicaStateMachine.handleStateChanges(replicas, OnlineReplica)
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
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
    replicaState.put(replica, fromState)
    replicaStateMachine.handleStateChanges(replicas, toState)
    assertEquals(fromState, replicaState(replica))
  }
}
