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
import kafka.zk.KafkaZkClient
import kafka.zookeeper._
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.KeeperException.Code
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.Mockito
import scala.collection.breakOut

class PartitionStateMachineTest {
  private var controllerContext: ControllerContext = null
  private var mockZkClient: KafkaZkClient = null
  private var mockControllerBrokerRequestBatch: ControllerBrokerRequestBatch = null
  private var partitionStateMachine: PartitionStateMachine = null

  private val brokerId = 5
  private val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId, "zkConnect"))
  private val controllerEpoch = 50
  private val partition = new TopicPartition("t", 0)
  private val partitions = Seq(partition)

  @Before
  def setUp(): Unit = {
    controllerContext = new ControllerContext
    controllerContext.epoch = controllerEpoch
    mockZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    mockControllerBrokerRequestBatch = EasyMock.createMock(classOf[ControllerBrokerRequestBatch])
    partitionStateMachine = new ZkPartitionStateMachine(config, new StateChangeLogger(brokerId, true, None), controllerContext,
      mockZkClient, mockControllerBrokerRequestBatch)
  }

  private def partitionState(partition: TopicPartition): PartitionState = {
    controllerContext.partitionState(partition)
  }

  @Test
  def testNonexistentPartitionToNewPartitionTransition(): Unit = {
    partitionStateMachine.handleStateChanges(partitions, NewPartition)
    assertEquals(NewPartition, partitionState(partition))
  }

  @Test
  def testInvalidNonexistentPartitionToOnlinePartitionTransition(): Unit = {
    partitionStateMachine.handleStateChanges(
      partitions,
      OnlinePartition,
      Option(OfflinePartitionLeaderElectionStrategy(false))
    )
    assertEquals(NonExistentPartition, partitionState(partition))
  }

  @Test
  def testInvalidNonexistentPartitionToOfflinePartitionTransition(): Unit = {
    partitionStateMachine.handleStateChanges(partitions, OfflinePartition)
    assertEquals(NonExistentPartition, partitionState(partition))
  }

  @Test
  def testNewPartitionToOnlinePartitionTransition(): Unit = {
    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(brokerId, "host", 0)))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    controllerContext.putPartitionState(partition, NewPartition)
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.createTopicPartitionStatesRaw(Map(partition -> leaderIsrAndControllerEpoch), controllerContext.epochZkVersion))
      .andReturn(Seq(CreateResponse(Code.OK, null, Some(partition), null, ResponseMetadata(0, 0))))
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, leaderIsrAndControllerEpoch, Seq(brokerId), isNew = true))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)
    partitionStateMachine.handleStateChanges(
      partitions,
      OnlinePartition,
      Option(OfflinePartitionLeaderElectionStrategy(false))
    )
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testNewPartitionToOnlinePartitionTransitionZooKeeperClientExceptionFromCreateStates(): Unit = {
    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(brokerId, "host", 0)))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    controllerContext.putPartitionState(partition, NewPartition)
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.createTopicPartitionStatesRaw(Map(partition -> leaderIsrAndControllerEpoch), controllerContext.epochZkVersion))
      .andThrow(new ZooKeeperClientException("test"))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)
    partitionStateMachine.handleStateChanges(
      partitions,
      OnlinePartition,
      Option(OfflinePartitionLeaderElectionStrategy(false))
    )
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(NewPartition, partitionState(partition))
  }

  @Test
  def testNewPartitionToOnlinePartitionTransitionErrorCodeFromCreateStates(): Unit = {
    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(brokerId, "host", 0)))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    controllerContext.putPartitionState(partition, NewPartition)
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.createTopicPartitionStatesRaw(Map(partition -> leaderIsrAndControllerEpoch), controllerContext.epochZkVersion))
      .andReturn(Seq(CreateResponse(Code.NODEEXISTS, null, Some(partition), null, ResponseMetadata(0, 0))))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)
    partitionStateMachine.handleStateChanges(
      partitions,
      OnlinePartition,
      Option(OfflinePartitionLeaderElectionStrategy(false))
    )
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(NewPartition, partitionState(partition))
  }

  @Test
  def testNewPartitionToOfflinePartitionTransition(): Unit = {
    controllerContext.putPartitionState(partition, NewPartition)
    partitionStateMachine.handleStateChanges(partitions, OfflinePartition)
    assertEquals(OfflinePartition, partitionState(partition))
  }

  @Test
  def testInvalidNewPartitionToNonexistentPartitionTransition(): Unit = {
    controllerContext.putPartitionState(partition, NewPartition)
    partitionStateMachine.handleStateChanges(partitions, NonExistentPartition)
    assertEquals(NewPartition, partitionState(partition))
  }

  @Test
  def testOnlinePartitionToOnlineTransition(): Unit = {
    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(brokerId, "host", 0)))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    controllerContext.putPartitionState(partition, OnlinePartition)
    val leaderAndIsr = LeaderAndIsr(brokerId, List(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())

    val leaderAndIsrAfterElection = leaderAndIsr.newLeader(brokerId)
    val updatedLeaderAndIsr = leaderAndIsrAfterElection.withZkVersion(2)
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> leaderAndIsrAfterElection), controllerEpoch, controllerContext.epochZkVersion))
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> Right(updatedLeaderAndIsr)), Seq.empty))
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, LeaderIsrAndControllerEpoch(updatedLeaderAndIsr, controllerEpoch), Seq(brokerId), isNew = false))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, Option(PreferredReplicaPartitionLeaderElectionStrategy))
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testOnlinePartitionToOnlineTransitionForControlledShutdown(): Unit = {
    val otherBrokerId = brokerId + 1
    controllerContext.setLiveBrokerAndEpochs(Map(
      TestUtils.createBrokerAndEpoch(brokerId, "host", 0),
      TestUtils.createBrokerAndEpoch(otherBrokerId, "host", 0)))
    controllerContext.shuttingDownBrokerIds.add(brokerId)
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId, otherBrokerId))
    controllerContext.putPartitionState(partition, OnlinePartition)
    val leaderAndIsr = LeaderAndIsr(brokerId, List(brokerId, otherBrokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())

    val leaderAndIsrAfterElection = leaderAndIsr.newLeaderAndIsr(otherBrokerId, List(otherBrokerId))
    val updatedLeaderAndIsr = leaderAndIsrAfterElection.withZkVersion(2)
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> leaderAndIsrAfterElection), controllerEpoch, controllerContext.epochZkVersion))
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> Right(updatedLeaderAndIsr)), Seq.empty))

    // The leaderAndIsr request should be sent to both brokers, including the shutting down one
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId, otherBrokerId),
      partition, LeaderIsrAndControllerEpoch(updatedLeaderAndIsr, controllerEpoch), Seq(brokerId, otherBrokerId),
      isNew = false))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, Option(ControlledShutdownPartitionLeaderElectionStrategy))
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testOnlinePartitionToOfflineTransition(): Unit = {
    controllerContext.putPartitionState(partition, OnlinePartition)
    partitionStateMachine.handleStateChanges(partitions, OfflinePartition)
    assertEquals(OfflinePartition, partitionState(partition))
  }

  @Test
  def testInvalidOnlinePartitionToNonexistentPartitionTransition(): Unit = {
    controllerContext.putPartitionState(partition, OnlinePartition)
    partitionStateMachine.handleStateChanges(partitions, NonExistentPartition)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testInvalidOnlinePartitionToNewPartitionTransition(): Unit = {
    controllerContext.putPartitionState(partition, OnlinePartition)
    partitionStateMachine.handleStateChanges(partitions, NewPartition)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testOfflinePartitionToOnlinePartitionTransition(): Unit = {
    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(brokerId, "host", 0)))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    controllerContext.putPartitionState(partition, OfflinePartition)
    val leaderAndIsr = LeaderAndIsr(LeaderAndIsr.NoLeader, List(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())

    val leaderAndIsrAfterElection = leaderAndIsr.newLeader(brokerId)
    val updatedLeaderAndIsr = leaderAndIsrAfterElection.withZkVersion(2)
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> leaderAndIsrAfterElection), controllerEpoch, controllerContext.epochZkVersion))
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> Right(updatedLeaderAndIsr)), Seq.empty))
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, LeaderIsrAndControllerEpoch(updatedLeaderAndIsr, controllerEpoch), Seq(brokerId), isNew = false))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    partitionStateMachine.handleStateChanges(
      partitions,
      OnlinePartition,
      Option(OfflinePartitionLeaderElectionStrategy(false))
    )
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testOfflinePartitionToUncleanOnlinePartitionTransition(): Unit = {
    /* Starting scenario: Leader: X, Isr: [X], Replicas: [X, Y], LiveBrokers: [Y]
     * Ending scenario: Leader: Y, Isr: [Y], Replicas: [X, Y], LiverBrokers: [Y]
     *
     * For the give staring scenario verify that performing an unclean leader
     * election on the offline partition results on the first live broker getting
     * elected.
     */
    val leaderBrokerId = brokerId + 1
    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(brokerId, "host", 0)))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(leaderBrokerId, brokerId))
    controllerContext.putPartitionState(partition, OfflinePartition)

    val leaderAndIsr = LeaderAndIsr(leaderBrokerId, List(leaderBrokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())

    val leaderAndIsrAfterElection = leaderAndIsr.newLeaderAndIsr(brokerId, List(brokerId))
    val updatedLeaderAndIsr = leaderAndIsrAfterElection.withZkVersion(2)

    EasyMock
      .expect(
        mockZkClient.updateLeaderAndIsr(
          Map(partition -> leaderAndIsrAfterElection),
          controllerEpoch,
          controllerContext.epochZkVersion
        )
      )
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> Right(updatedLeaderAndIsr)), Seq.empty))
    EasyMock.expect(
      mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(
        Seq(brokerId),
        partition,
        LeaderIsrAndControllerEpoch(updatedLeaderAndIsr, controllerEpoch),
        Seq(leaderBrokerId, brokerId),
        false
      )
    )
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    partitionStateMachine.handleStateChanges(
      partitions,
      OnlinePartition,
      Option(OfflinePartitionLeaderElectionStrategy(true))
    )
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testOfflinePartitionToOnlinePartitionTransitionZooKeeperClientExceptionFromStateLookup(): Unit = {
    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(brokerId, "host", 0)))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    controllerContext.putPartitionState(partition, OfflinePartition)
    val leaderAndIsr = LeaderAndIsr(LeaderAndIsr.NoLeader, List(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    val exception = new ZooKeeperClientException("")
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> leaderAndIsr.newLeader(brokerId)),
      controllerEpoch, controllerContext.epochZkVersion))
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> Left(exception)), Seq.empty))

    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    val results = partitionStateMachine.handleStateChanges(
      partitions,
      OnlinePartition,
      Option(OfflinePartitionLeaderElectionStrategy(false))
    )
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertTrue(results.get(partition).isDefined)
    assertEquals(Left(exception), results(partition))
    assertEquals(OfflinePartition, partitionState(partition))
  }

  @Test
  def testPreferredLeaderElectionWithInFlightIsrShrink(): Unit = {
    // Tests the scenario where the controller is electing the preferred leader
    // while the current leader has removed a node from the ISR.

    val otherBrokerId = brokerId + 1
    val preferredBrokerId = brokerId + 2
    val replicas = Seq(preferredBrokerId, brokerId, otherBrokerId)

    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(brokerId, "host", 0)))
    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(otherBrokerId, "host", 0)))
    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(preferredBrokerId, "host", 0)))

    controllerContext.updatePartitionReplicaAssignment(partition, replicas)
    controllerContext.putPartitionState(partition, OnlinePartition)

    // Initially all replicas are in the ISR
    val leaderAndIsr = LeaderAndIsr(brokerId, 1, List(brokerId, otherBrokerId, preferredBrokerId), 1)
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    // The controller tries to elect the preferred leader, but it fails since the ISR has changed
    val firstTryPreferredLeaderAndIsr = leaderAndIsr.newLeader(preferredBrokerId)
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> firstTryPreferredLeaderAndIsr),
      controllerEpoch, controllerContext.epochZkVersion))
      .andReturn(UpdateLeaderAndIsrResult(Map.empty, Seq(partition)))

    // So we look up the latest partition state and see the shrunk ISR
    val shrunkLeaderAndIsr = LeaderAndIsr(brokerId, 1, List(brokerId, preferredBrokerId), 2)
    EasyMock.expect(mockZkClient.fetchPartitionStates(partitions))
      .andReturn(Seq(partition -> Some(LeaderIsrAndControllerEpoch(shrunkLeaderAndIsr, controllerEpoch))))

    // Then we can retry the preferred election
    val preferredLeaderAndIsr = shrunkLeaderAndIsr.newLeader(preferredBrokerId)
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> preferredLeaderAndIsr),
      controllerEpoch, controllerContext.epochZkVersion))
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> Right(preferredLeaderAndIsr)), Seq()))

    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(
      replicas, partition, LeaderIsrAndControllerEpoch(preferredLeaderAndIsr, controllerEpoch),
      replicas, isNew = false))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    partitionStateMachine.handleStateChanges(
      partitions,
      OnlinePartition,
      Option(PreferredReplicaPartitionLeaderElectionStrategy)
    )

    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlinePartition, partitionState(partition))
    assertEquals(preferredLeaderAndIsr, controllerContext.partitionLeadershipInfo(partition).leaderAndIsr)
  }

  @Test
  def testOfflinePartitionToOnlinePartitionTransitionErrorCodeFromStateLookup(): Unit = {
    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(brokerId, "host", 0)))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    controllerContext.putPartitionState(partition, OfflinePartition)
    val leaderAndIsr = LeaderAndIsr(LeaderAndIsr.NoLeader, List(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> leaderAndIsr.newLeader(brokerId)),
      controllerEpoch, controllerContext.epochZkVersion))
      .andReturn(UpdateLeaderAndIsrResult(Map.empty, Seq(partition)))
    EasyMock.expect(mockZkClient.fetchPartitionStates(partitions))
      .andReturn(Seq(partition -> None))

    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    partitionStateMachine.handleStateChanges(
      partitions,
      OnlinePartition,
      Option(OfflinePartitionLeaderElectionStrategy(false))
    )
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OfflinePartition, partitionState(partition))
  }

  @Test
  def testOfflinePartitionToNonexistentPartitionTransition(): Unit = {
    controllerContext.putPartitionState(partition, OfflinePartition)
    partitionStateMachine.handleStateChanges(partitions, NonExistentPartition)
    assertEquals(NonExistentPartition, partitionState(partition))
  }

  @Test
  def testInvalidOfflinePartitionToNewPartitionTransition(): Unit = {
    controllerContext.putPartitionState(partition, OfflinePartition)
    partitionStateMachine.handleStateChanges(partitions, NewPartition)
    assertEquals(OfflinePartition, partitionState(partition))
  }

  private def prepareMockToElectLeaderForPartitions(partitions: Seq[TopicPartition]): Unit = {
    val leaderAndIsr = LeaderAndIsr(brokerId, List(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    partitions.foreach { partition =>
      controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
    }

    def prepareMockToGetLogConfigs(): Unit = {
      EasyMock.expect(mockZkClient.getLogConfigs(Set.empty, config.originals()))
        .andReturn(Map.empty, Map.empty)
    }
    prepareMockToGetLogConfigs()

    def prepareMockToUpdateLeaderAndIsr(): Unit = {
      val updatedLeaderAndIsr: Map[TopicPartition, LeaderAndIsr] = partitions.map { partition =>
        partition -> leaderAndIsr.newLeaderAndIsr(brokerId, List(brokerId))
      }(breakOut)
      EasyMock.expect(mockZkClient.updateLeaderAndIsr(updatedLeaderAndIsr, controllerEpoch, controllerContext.epochZkVersion))
        .andReturn(UpdateLeaderAndIsrResult(updatedLeaderAndIsr.mapValues(Right(_)), Seq.empty))
    }
    prepareMockToUpdateLeaderAndIsr()
  }

  /**
    * This method tests changing partitions' state to OfflinePartition increments the offlinePartitionCount,
    * and changing their state back to OnlinePartition decrements the offlinePartitionCount
    */
  @Test
  def testUpdatingOfflinePartitionsCount(): Unit = {
    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(brokerId, "host", 0)))

    val partitionIds = Seq(0, 1, 2, 3)
    val topic = "test"
    val partitions = partitionIds.map(new TopicPartition(topic, _))

    partitions.foreach { partition =>
      controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    }

    prepareMockToElectLeaderForPartitions(partitions)
    EasyMock.replay(mockZkClient)

    partitionStateMachine.handleStateChanges(partitions, NewPartition)
    partitionStateMachine.handleStateChanges(partitions, OfflinePartition)
    assertEquals(s"There should be ${partitions.size} offline partition(s)", partitions.size, controllerContext.offlinePartitionCount)

    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, Some(OfflinePartitionLeaderElectionStrategy(false)))
    assertEquals(s"There should be no offline partition(s)", 0, controllerContext.offlinePartitionCount)
  }

  /**
    * This method tests if a topic is being deleted, then changing partitions' state to OfflinePartition makes no change
    * to the offlinePartitionCount
    */
  @Test
  def testNoOfflinePartitionsChangeForTopicsBeingDeleted() = {
    val partitionIds = Seq(0, 1, 2, 3)
    val topic = "test"
    val partitions = partitionIds.map(new TopicPartition(topic, _))

    controllerContext.topicsToBeDeleted.add(topic)
    controllerContext.topicsWithDeletionStarted.add(topic)

    partitionStateMachine.handleStateChanges(partitions, NewPartition)
    partitionStateMachine.handleStateChanges(partitions, OfflinePartition)
    assertEquals(s"There should be no offline partition(s)", 0, controllerContext.offlinePartitionCount)
  }

  /**
    * This method tests if some partitions are already in OfflinePartition state,
    * then deleting their topic will decrement the offlinePartitionCount.
    * For example, if partitions test-0, test-1, test-2, test-3 are in OfflinePartition state,
    * and the offlinePartitionCount is 4, trying to delete the topic "test" means these
    * partitions no longer qualify as offline-partitions, and the offlinePartitionCount
    * should be decremented to 0.
    */
  @Test
  def testUpdatingOfflinePartitionsCountDuringTopicDeletion() = {
    val partitionIds = Seq(0, 1, 2, 3)
    val topic = "test"
    val partitions = partitionIds.map(new TopicPartition("test", _))
    partitions.foreach { partition =>
      controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    }

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    val deletionClient = Mockito.mock(classOf[DeletionClient])
    val topicDeletionManager = new TopicDeletionManager(config, controllerContext,
      replicaStateMachine, partitionStateMachine, deletionClient)

    partitionStateMachine.handleStateChanges(partitions, NewPartition)
    partitionStateMachine.handleStateChanges(partitions, OfflinePartition)
    partitions.foreach { partition =>
      val replica = PartitionAndReplica(partition, brokerId)
      controllerContext.putReplicaState(replica, OfflineReplica)
    }

    assertEquals(s"There should be ${partitions.size} offline partition(s)", partitions.size, controllerContext.offlinePartitionCount)
    topicDeletionManager.enqueueTopicsForDeletion(Set(topic))
    assertEquals(s"There should be no offline partition(s)", 0, controllerContext.offlinePartitionCount)
  }
}
