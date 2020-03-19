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
import kafka.log.LogConfig
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.{KafkaZkClient, TopicPartitionStateZNode}
import kafka.zookeeper._
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.Stat
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.Mockito
import scala.collection.JavaConverters._

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
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
    controllerContext.putPartitionState(partition, NewPartition)
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.createTopicPartitionStatesRaw(Map(partition -> leaderIsrAndControllerEpoch), controllerContext.epochZkVersion))
      .andReturn(Seq(CreateResponse(Code.OK, null, Some(partition), null, ResponseMetadata(0, 0))))
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, leaderIsrAndControllerEpoch, replicaAssignment(Seq(brokerId)), isNew = true))
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
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
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
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
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
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
    controllerContext.putPartitionState(partition, OnlinePartition)
    val leaderAndIsr = LeaderAndIsr(brokerId, List(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    val stat = new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.getTopicPartitionStatesRaw(partitions))
      .andReturn(Seq(GetDataResponse(Code.OK, null, Some(partition),
        TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch), stat, ResponseMetadata(0, 0))))

    val leaderAndIsrAfterElection = leaderAndIsr.newLeader(brokerId)
    val updatedLeaderAndIsr = leaderAndIsrAfterElection.withZkVersion(2)
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> leaderAndIsrAfterElection), controllerEpoch, controllerContext.epochZkVersion))
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> Right(updatedLeaderAndIsr)), Seq.empty))
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, LeaderIsrAndControllerEpoch(updatedLeaderAndIsr, controllerEpoch), replicaAssignment(Seq(brokerId)), isNew = false))
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
    controllerContext.updatePartitionFullReplicaAssignment(
      partition,
      ReplicaAssignment(Seq(brokerId, otherBrokerId))
    )
    controllerContext.putPartitionState(partition, OnlinePartition)
    val leaderAndIsr = LeaderAndIsr(brokerId, List(brokerId, otherBrokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    val stat = new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.getTopicPartitionStatesRaw(partitions))
      .andReturn(Seq(GetDataResponse(Code.OK, null, Some(partition),
        TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch), stat, ResponseMetadata(0, 0))))

    val leaderAndIsrAfterElection = leaderAndIsr.newLeaderAndIsr(otherBrokerId, List(otherBrokerId))
    val updatedLeaderAndIsr = leaderAndIsrAfterElection.withZkVersion(2)
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> leaderAndIsrAfterElection), controllerEpoch, controllerContext.epochZkVersion))
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> Right(updatedLeaderAndIsr)), Seq.empty))

    // The leaderAndIsr request should be sent to both brokers, including the shutting down one
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId, otherBrokerId),
      partition, LeaderIsrAndControllerEpoch(updatedLeaderAndIsr, controllerEpoch), replicaAssignment(Seq(brokerId, otherBrokerId)),
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

  def testOfflinePartitionToOnlinePartitionTransitionInIsr(): Unit = {
    testOfflinePartitionToOnlinePartitionTransition(testUncleanLeaderElection = false)
  }

  @Test
  def testOfflinePartitionToOnlinePartitionTransitionUncleanLeaderElection(): Unit = {
    testOfflinePartitionToOnlinePartitionTransition(testUncleanLeaderElection = true)
  }

  private[this] def testOfflinePartitionToOnlinePartitionTransition(testUncleanLeaderElection : Boolean) : Unit = {
    val uncleanLeaderElectionCount = controllerContext.stats.uncleanLeaderElectionRate.count();

    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(brokerId, "host", 0)))
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
    controllerContext.putPartitionState(partition, OfflinePartition)
    val leaderAndIsr = LeaderAndIsr(LeaderAndIsr.NoLeader, if (testUncleanLeaderElection) List.empty else List(brokerId))

    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    val stat = new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.getTopicPartitionStatesRaw(partitions))
      .andReturn(Seq(GetDataResponse(Code.OK, null, Some(partition),
        TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch), stat, ResponseMetadata(0, 0))))

    if (testUncleanLeaderElection) {
      val uleLogConfig = LogConfig(Map(LogConfig.UncleanLeaderElectionEnableProp -> "true").asJava)
      EasyMock.expect(mockZkClient.getLogConfigs(Set(partition.topic), config.originals()))
        .andReturn((Map(partition.topic -> uleLogConfig), Map.empty))
    } else {
      EasyMock.expect(mockZkClient.getLogConfigs(Set.empty, config.originals()))
        .andReturn((Map(partition.topic -> LogConfig()), Map.empty))
    }
    val leaderAndIsrAfterElection = leaderAndIsr.newLeaderAndIsr(brokerId, List(brokerId))

    val updatedLeaderAndIsr = leaderAndIsrAfterElection.withZkVersion(2)
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> leaderAndIsrAfterElection), controllerEpoch, controllerContext.epochZkVersion))
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> Right(updatedLeaderAndIsr)), Seq.empty))
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, LeaderIsrAndControllerEpoch(updatedLeaderAndIsr, controllerEpoch), replicaAssignment(Seq(brokerId)), isNew = false))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    partitionStateMachine.handleStateChanges(
      partitions,
      OnlinePartition,
      Option(OfflinePartitionLeaderElectionStrategy(false))
    )
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlinePartition, partitionState(partition))
    val expectedUncleanLeaderElectionCount = if (testUncleanLeaderElection) uncleanLeaderElectionCount + 1 else uncleanLeaderElectionCount
    assertEquals(expectedUncleanLeaderElectionCount, controllerContext.stats.uncleanLeaderElectionRate.count())
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
    controllerContext.updatePartitionFullReplicaAssignment(
      partition,
      ReplicaAssignment(Seq(leaderBrokerId, brokerId))
    )
    controllerContext.putPartitionState(partition, OfflinePartition)

    val leaderAndIsr = LeaderAndIsr(leaderBrokerId, List(leaderBrokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock
      .expect(mockZkClient.getTopicPartitionStatesRaw(partitions))
      .andReturn(
        Seq(
          GetDataResponse(
            Code.OK,
            null,
            Option(partition),
            TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch),
            new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            ResponseMetadata(0, 0)
          )
        )
      )

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
        replicaAssignment(Seq(leaderBrokerId, brokerId)),
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
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
    controllerContext.putPartitionState(partition, OfflinePartition)
    val leaderAndIsr = LeaderAndIsr(LeaderAndIsr.NoLeader, List(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.getTopicPartitionStatesRaw(partitions))
      .andThrow(new ZooKeeperClientException(""))

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
  def testOfflinePartitionToOnlinePartitionTransitionErrorCodeFromStateLookup(): Unit = {
    controllerContext.setLiveBrokerAndEpochs(Map(TestUtils.createBrokerAndEpoch(brokerId, "host", 0)))
    controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
    controllerContext.putPartitionState(partition, OfflinePartition)
    val leaderAndIsr = LeaderAndIsr(LeaderAndIsr.NoLeader, List(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    val stat = new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.getTopicPartitionStatesRaw(partitions))
      .andReturn(Seq(GetDataResponse(Code.NONODE, null, Some(partition),
        TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch), stat, ResponseMetadata(0, 0))))

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
    def prepareMockToGetTopicPartitionsStatesRaw(): Unit = {
      val stat = new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
      val getDataResponses = partitions.map {p => GetDataResponse(Code.OK, null, Some(p),
        TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch), stat, ResponseMetadata(0, 0))}
      EasyMock.expect(mockZkClient.getTopicPartitionStatesRaw(partitions))
        .andReturn(getDataResponses)
    }
    prepareMockToGetTopicPartitionsStatesRaw()

    def prepareMockToGetLogConfigs(): Unit = {
      EasyMock.expect(mockZkClient.getLogConfigs(Set.empty, config.originals()))
        .andReturn(Map.empty, Map.empty)
    }
    prepareMockToGetLogConfigs()

    def prepareMockToUpdateLeaderAndIsr(): Unit = {
      val updatedLeaderAndIsr: Map[TopicPartition, LeaderAndIsr] = partitions.map { partition =>
        partition -> leaderAndIsr.newLeaderAndIsr(brokerId, List(brokerId))
      }.toMap
      EasyMock.expect(mockZkClient.updateLeaderAndIsr(updatedLeaderAndIsr, controllerEpoch, controllerContext.epochZkVersion))
        .andReturn(UpdateLeaderAndIsrResult(updatedLeaderAndIsr.mapValues(Right(_)).toMap, Seq.empty))
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
      controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
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
      controllerContext.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(Seq(brokerId)))
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

  private def replicaAssignment(replicas: Seq[Int]): ReplicaAssignment = ReplicaAssignment(replicas, Seq(), Seq())

}
