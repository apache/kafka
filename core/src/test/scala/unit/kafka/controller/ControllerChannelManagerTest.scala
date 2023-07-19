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

import java.util.Properties
import kafka.api.LeaderAndIsr
import kafka.cluster.{Broker, EndPoint}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicError
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.message.StopReplicaResponseData.StopReplicaPartitionError
import org.apache.kafka.common.message.{LeaderAndIsrResponseData, StopReplicaResponseData, UpdateMetadataResponseData}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractControlRequest, AbstractResponse, LeaderAndIsrRequest, LeaderAndIsrResponse, StopReplicaRequest, StopReplicaResponse, UpdateMetadataRequest, UpdateMetadataResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion.{IBP_0_10_0_IV1, IBP_0_10_2_IV0, IBP_0_9_0, IBP_1_0_IV0, IBP_2_2_IV0, IBP_2_4_IV0, IBP_2_4_IV1, IBP_2_6_IV0, IBP_2_8_IV1, IBP_3_2_IV0, IBP_3_4_IV0}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class ControllerChannelManagerTest {
  private val controllerId = 1
  private val controllerEpoch = 1
  private val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(controllerId, "zkConnect"))
  private val logger = new StateChangeLogger(controllerId, true, None)

  type ControlRequest = AbstractControlRequest.Builder[_ <: AbstractControlRequest]

  @Test
  def testLeaderAndIsrRequestSent(): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val batch = new MockControllerBrokerRequestBatch(context)

    val partitions = Map(
      new TopicPartition("foo", 0) -> LeaderAndIsr(1, List(1, 2)),
      new TopicPartition("foo", 1) -> LeaderAndIsr(2, List(2, 3)),
      new TopicPartition("bar", 1) -> LeaderAndIsr(3, List(1, 3))
    )

    batch.newBatch()
    partitions.foreach { case (partition, leaderAndIsr) =>
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
      context.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
      batch.addLeaderAndIsrRequestForBrokers(Seq(2), partition, leaderIsrAndControllerEpoch, replicaAssignment(Seq(1, 2, 3)), isNew = false)
    }
    batch.sendRequestsToBrokers(controllerEpoch)

    val leaderAndIsrRequests = batch.collectLeaderAndIsrRequestsFor(2)
    val updateMetadataRequests = batch.collectUpdateMetadataRequestsFor(2)
    assertEquals(1, leaderAndIsrRequests.size)
    assertEquals(1, updateMetadataRequests.size)

    val leaderAndIsrRequest = leaderAndIsrRequests.head
    val topicIds = leaderAndIsrRequest.topicIds();
    val topicNames = topicIds.asScala.map { case (k, v) => (v, k) }
    assertEquals(controllerId, leaderAndIsrRequest.controllerId)
    assertEquals(controllerEpoch, leaderAndIsrRequest.controllerEpoch)
    assertEquals(partitions.keySet,
      leaderAndIsrRequest.partitionStates.asScala.map(p => new TopicPartition(p.topicName, p.partitionIndex)).toSet)
    assertEquals(partitions.map { case (k, v) => (k, v.leader) },
      leaderAndIsrRequest.partitionStates.asScala.map(p => new TopicPartition(p.topicName, p.partitionIndex) -> p.leader).toMap)
    assertEquals(partitions.map { case (k, v) => (k, v.isr) },
      leaderAndIsrRequest.partitionStates.asScala.map(p => new TopicPartition(p.topicName, p.partitionIndex) -> p.isr.asScala).toMap)

    applyLeaderAndIsrResponseCallbacks(Errors.NONE, batch.sentRequests(2).toList)
    assertEquals(1, batch.sentEvents.size)

    val LeaderAndIsrResponseReceived(leaderAndIsrResponse, brokerId) = batch.sentEvents.head
    assertEquals(2, brokerId)
    assertEquals(partitions.keySet,
      leaderAndIsrResponse.topics.asScala.flatMap(t => t.partitionErrors.asScala.map(p =>
        new TopicPartition(topicNames(t.topicId), p.partitionIndex))).toSet)
    leaderAndIsrResponse.topics.forEach(topic =>
      assertEquals(topicIds.get(topicNames.get(topic.topicId).get), topic.topicId))
  }

  @Test
  def testLeaderAndIsrRequestIsNew(): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val batch = new MockControllerBrokerRequestBatch(context)

    val partition = new TopicPartition("foo", 0)
    val leaderAndIsr = LeaderAndIsr(1, List(1, 2))

    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    context.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)

    batch.newBatch()
    batch.addLeaderAndIsrRequestForBrokers(Seq(2), partition, leaderIsrAndControllerEpoch, replicaAssignment(Seq(1, 2, 3)), isNew = true)
    batch.addLeaderAndIsrRequestForBrokers(Seq(2), partition, leaderIsrAndControllerEpoch, replicaAssignment(Seq(1, 2, 3)), isNew = false)
    batch.sendRequestsToBrokers(controllerEpoch)

    val leaderAndIsrRequests = batch.collectLeaderAndIsrRequestsFor(2)
    val updateMetadataRequests = batch.collectUpdateMetadataRequestsFor(2)
    assertEquals(1, leaderAndIsrRequests.size)
    assertEquals(1, updateMetadataRequests.size)

    val leaderAndIsrRequest = leaderAndIsrRequests.head
    val partitionStates = leaderAndIsrRequest.partitionStates.asScala
    assertEquals(Seq(partition), partitionStates.map(p => new TopicPartition(p.topicName, p.partitionIndex)))
    val partitionState = partitionStates.find(p => p.topicName == partition.topic && p.partitionIndex == partition.partition)
    assertEquals(Some(true), partitionState.map(_.isNew))
  }

  @Test
  def testLeaderAndIsrRequestSentToLiveOrShuttingDownBrokers(): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val batch = new MockControllerBrokerRequestBatch(context)

    // 2 is shutting down, 3 is dead
    context.shuttingDownBrokerIds.add(2)
    context.removeLiveBrokers(Set(3))

    val partition = new TopicPartition("foo", 0)
    val leaderAndIsr = LeaderAndIsr(1, List(1, 2))

    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    context.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)

    batch.newBatch()
    batch.addLeaderAndIsrRequestForBrokers(Seq(1, 2, 3), partition, leaderIsrAndControllerEpoch, replicaAssignment(Seq(1, 2, 3)), isNew = false)
    batch.sendRequestsToBrokers(controllerEpoch)

    assertEquals(0, batch.sentEvents.size)
    assertEquals(2, batch.sentRequests.size)
    assertEquals(Set(1, 2), batch.sentRequests.keySet)

    for (brokerId <- Set(1, 2)) {
      val leaderAndIsrRequests = batch.collectLeaderAndIsrRequestsFor(brokerId)
      val updateMetadataRequests = batch.collectUpdateMetadataRequestsFor(brokerId)
      assertEquals(1, leaderAndIsrRequests.size)
      assertEquals(1, updateMetadataRequests.size)
      val leaderAndIsrRequest = leaderAndIsrRequests.head
      assertEquals(Seq(partition), leaderAndIsrRequest.partitionStates.asScala.map(p => new TopicPartition(p.topicName, p.partitionIndex)))
    }
  }

  @Test
  def testLeaderAndIsrInterBrokerProtocolVersion(): Unit = {
    testLeaderAndIsrRequestFollowsInterBrokerProtocolVersion(MetadataVersion.latest, ApiKeys.LEADER_AND_ISR.latestVersion)

    for (metadataVersion <- MetadataVersion.VERSIONS) {
      val leaderAndIsrRequestVersion: Short = {
        if (metadataVersion.isAtLeast(IBP_3_4_IV0)) 7
        else if (metadataVersion.isAtLeast(IBP_3_2_IV0)) 6
        else if (metadataVersion.isAtLeast(IBP_2_8_IV1)) 5
        else if (metadataVersion.isAtLeast(IBP_2_4_IV1)) 4
        else if (metadataVersion.isAtLeast(IBP_2_4_IV0)) 3
        else if (metadataVersion.isAtLeast(IBP_2_2_IV0)) 2
        else if (metadataVersion.isAtLeast(IBP_1_0_IV0)) 1
        else 0
      }

      testLeaderAndIsrRequestFollowsInterBrokerProtocolVersion(metadataVersion, leaderAndIsrRequestVersion)
    }
  }

  private def testLeaderAndIsrRequestFollowsInterBrokerProtocolVersion(interBrokerProtocolVersion: MetadataVersion,
                                                                       expectedLeaderAndIsrVersion: Short): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val config = createConfig(interBrokerProtocolVersion)
    val batch = new MockControllerBrokerRequestBatch(context, config)

    val partition = new TopicPartition("foo", 0)
    var leaderAndIsr = LeaderAndIsr(1, List(1, 2))
    if (interBrokerProtocolVersion.isAtLeast(IBP_3_2_IV0)) {
      leaderAndIsr = leaderAndIsr.copy(leaderRecoveryState = LeaderRecoveryState.RECOVERING)
    }

    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    context.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)

    batch.newBatch()
    batch.addLeaderAndIsrRequestForBrokers(Seq(2), partition, leaderIsrAndControllerEpoch, replicaAssignment(Seq(1, 2, 3)), isNew = false)
    batch.sendRequestsToBrokers(controllerEpoch)

    val leaderAndIsrRequests = batch.collectLeaderAndIsrRequestsFor(2)
    assertEquals(1, leaderAndIsrRequests.size)
    assertEquals(expectedLeaderAndIsrVersion, leaderAndIsrRequests.head.version,
      s"IBP $interBrokerProtocolVersion should use version $expectedLeaderAndIsrVersion")

    val request = leaderAndIsrRequests.head
    val byteBuffer = request.serialize
    val deserializedRequest = LeaderAndIsrRequest.parse(byteBuffer, expectedLeaderAndIsrVersion)

    val expectedRecovery = if (interBrokerProtocolVersion.isAtLeast(IBP_3_2_IV0)) {
      LeaderRecoveryState.RECOVERING
    } else {
      LeaderRecoveryState.RECOVERED
    }

    Seq(request, deserializedRequest).foreach { request =>
      request.partitionStates.forEach { state =>
        assertEquals(expectedRecovery , LeaderRecoveryState.of(state.leaderRecoveryState()))
      }
    }

    if (interBrokerProtocolVersion.isAtLeast(IBP_2_8_IV1)) {
      assertFalse(request.topicIds().get("foo").equals(Uuid.ZERO_UUID))
      assertFalse(deserializedRequest.topicIds().get("foo").equals(Uuid.ZERO_UUID))
    } else if (interBrokerProtocolVersion.isAtLeast(IBP_2_2_IV0)) {
      assertFalse(request.topicIds().get("foo").equals(Uuid.ZERO_UUID))
      assertTrue(deserializedRequest.topicIds().get("foo").equals(Uuid.ZERO_UUID))
    } else {
      assertTrue(request.topicIds().get("foo") == null)
      assertTrue(deserializedRequest.topicIds().get("foo") == null)
    }
  }

  @Test
  def testUpdateMetadataRequestSent(): Unit = {

    val topicIds = Map("foo" -> Uuid.randomUuid(), "bar" -> Uuid.randomUuid())
    val context = initContext(Seq(1, 2, 3), 2, 3, topicIds)
    val batch = new MockControllerBrokerRequestBatch(context)

    val partitions = Map(
      new TopicPartition("foo", 0) -> LeaderAndIsr(1, List(1, 2)),
      new TopicPartition("foo", 1) -> LeaderAndIsr(2, List(2, 3)),
      new TopicPartition("bar", 1) -> LeaderAndIsr(3, List(1, 3))
    )

    partitions.foreach { case (partition, leaderAndIsr) =>
      context.putPartitionLeadershipInfo(partition, LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))
    }

    batch.newBatch()
    batch.addUpdateMetadataRequestForBrokers(Seq(2), partitions.keySet)
    batch.sendRequestsToBrokers(controllerEpoch)

    val updateMetadataRequests = batch.collectUpdateMetadataRequestsFor(2)
    assertEquals(1, updateMetadataRequests.size)

    val updateMetadataRequest = updateMetadataRequests.head
    val partitionStates = updateMetadataRequest.partitionStates.asScala.toBuffer
    assertEquals(3, partitionStates.size)
    assertEquals(partitions.map { case (k, v) => (k, v.leader) },
      partitionStates.map(ps => (new TopicPartition(ps.topicName, ps.partitionIndex), ps.leader)).toMap)
    assertEquals(partitions.map { case (k, v) => (k, v.isr) },
      partitionStates.map(ps => (new TopicPartition(ps.topicName, ps.partitionIndex), ps.isr.asScala)).toMap)

    val topicStates = updateMetadataRequest.topicStates()
    assertEquals(2, topicStates.size)
    for (topicState <- topicStates.asScala) {
      assertEquals(topicState.topicId(), topicIds(topicState.topicName()))
    }

    assertEquals(controllerId, updateMetadataRequest.controllerId)
    assertEquals(controllerEpoch, updateMetadataRequest.controllerEpoch)
    assertEquals(3, updateMetadataRequest.liveBrokers.size)
    assertEquals(Set(1, 2, 3), updateMetadataRequest.liveBrokers.asScala.map(_.id).toSet)

    applyUpdateMetadataResponseCallbacks(Errors.STALE_BROKER_EPOCH, batch.sentRequests(2).toList)
    assertEquals(1, batch.sentEvents.size)

    val UpdateMetadataResponseReceived(updateMetadataResponse, brokerId) = batch.sentEvents.head
    assertEquals(2, brokerId)
    assertEquals(Errors.STALE_BROKER_EPOCH, updateMetadataResponse.error)
  }

  @Test
  def testUpdateMetadataDoesNotIncludePartitionsWithoutLeaderAndIsr(): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val batch = new MockControllerBrokerRequestBatch(context)

    val partitions = Set(
      new TopicPartition("foo", 0),
      new TopicPartition("foo", 1),
      new TopicPartition("bar", 1)
    )

    batch.newBatch()
    batch.addUpdateMetadataRequestForBrokers(Seq(2), partitions)
    batch.sendRequestsToBrokers(controllerEpoch)

    assertEquals(0, batch.sentEvents.size)
    assertEquals(1, batch.sentRequests.size)
    assertTrue(batch.sentRequests.contains(2))

    val updateMetadataRequests = batch.collectUpdateMetadataRequestsFor(2)
    assertEquals(1, updateMetadataRequests.size)

    val updateMetadataRequest = updateMetadataRequests.head
    assertEquals(0, updateMetadataRequest.partitionStates.asScala.size)
    assertEquals(3, updateMetadataRequest.liveBrokers.size)
    assertEquals(Set(1, 2, 3), updateMetadataRequest.liveBrokers.asScala.map(_.id).toSet)
  }

  @Test
  def testUpdateMetadataRequestDuringTopicDeletion(): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val batch = new MockControllerBrokerRequestBatch(context)

    val partitions = Map(
      new TopicPartition("foo", 0) -> LeaderAndIsr(1, List(1, 2)),
      new TopicPartition("foo", 1) -> LeaderAndIsr(2, List(2, 3)),
      new TopicPartition("bar", 1) -> LeaderAndIsr(3, List(1, 3))
    )

    partitions.foreach { case (partition, leaderAndIsr) =>
      context.putPartitionLeadershipInfo(partition, LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))
    }

    context.queueTopicDeletion(Set("foo"))

    batch.newBatch()
    batch.addUpdateMetadataRequestForBrokers(Seq(2), partitions.keySet)
    batch.sendRequestsToBrokers(controllerEpoch)

    val updateMetadataRequests = batch.collectUpdateMetadataRequestsFor(2)
    assertEquals(1, updateMetadataRequests.size)

    val updateMetadataRequest = updateMetadataRequests.head
    assertEquals(3, updateMetadataRequest.partitionStates.asScala.size)

    assertTrue(updateMetadataRequest.partitionStates.asScala
      .filter(_.topicName == "foo")
      .map(_.leader)
      .forall(leaderId => leaderId == LeaderAndIsr.LeaderDuringDelete))

    assertEquals(partitions.filter { case (k, _) => k.topic == "bar" }.map { case (k, v) => (k, v.leader) },
      updateMetadataRequest.partitionStates.asScala.filter(ps => ps.topicName == "bar").map { ps =>
        (new TopicPartition(ps.topicName, ps.partitionIndex), ps.leader) }.toMap)
    assertEquals(partitions.map { case (k, v) => (k, v.isr) },
      updateMetadataRequest.partitionStates.asScala.map(ps => (new TopicPartition(ps.topicName, ps.partitionIndex), ps.isr.asScala)).toMap)

    assertEquals(3, updateMetadataRequest.liveBrokers.size)
    assertEquals(Set(1, 2, 3), updateMetadataRequest.liveBrokers.asScala.map(_.id).toSet)
  }

  @Test
  def testUpdateMetadataIncludesLiveOrShuttingDownBrokers(): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val batch = new MockControllerBrokerRequestBatch(context)

    // 2 is shutting down, 3 is dead
    context.shuttingDownBrokerIds.add(2)
    context.removeLiveBrokers(Set(3))

    batch.newBatch()
    batch.addUpdateMetadataRequestForBrokers(Seq(1, 2, 3), Set.empty)
    batch.sendRequestsToBrokers(controllerEpoch)

    assertEquals(Set(1, 2), batch.sentRequests.keySet)

    for (brokerId <- Set(1, 2)) {
      val updateMetadataRequests = batch.collectUpdateMetadataRequestsFor(brokerId)
      assertEquals(1, updateMetadataRequests.size)

      val updateMetadataRequest = updateMetadataRequests.head
      assertEquals(0, updateMetadataRequest.partitionStates.asScala.size)
      assertEquals(2, updateMetadataRequest.liveBrokers.size)
      assertEquals(Set(1, 2), updateMetadataRequest.liveBrokers.asScala.map(_.id).toSet)
    }
  }

  @Test
  def testUpdateMetadataInterBrokerProtocolVersion(): Unit = {
    testUpdateMetadataFollowsInterBrokerProtocolVersion(MetadataVersion.latest, ApiKeys.UPDATE_METADATA.latestVersion)

    for (metadataVersion <- MetadataVersion.VERSIONS) {
      val updateMetadataRequestVersion: Short =
        if (metadataVersion.isAtLeast(IBP_3_4_IV0)) 8
        else if (metadataVersion.isAtLeast(IBP_2_8_IV1)) 7
        else if (metadataVersion.isAtLeast(IBP_2_4_IV1)) 6
        else if (metadataVersion.isAtLeast(IBP_2_2_IV0)) 5
        else if (metadataVersion.isAtLeast(IBP_1_0_IV0)) 4
        else if (metadataVersion.isAtLeast(IBP_0_10_2_IV0)) 3
        else if (metadataVersion.isAtLeast(IBP_0_10_0_IV1)) 2
        else if (metadataVersion.isAtLeast(IBP_0_9_0)) 1
        else 0

      testUpdateMetadataFollowsInterBrokerProtocolVersion(metadataVersion, updateMetadataRequestVersion)
    }
  }

  private def testUpdateMetadataFollowsInterBrokerProtocolVersion(interBrokerProtocolVersion: MetadataVersion,
                                                                  expectedUpdateMetadataVersion: Short): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val config = createConfig(interBrokerProtocolVersion)
    val batch = new MockControllerBrokerRequestBatch(context, config)

    batch.newBatch()
    batch.addUpdateMetadataRequestForBrokers(Seq(2), Set.empty)
    batch.sendRequestsToBrokers(controllerEpoch)

    assertEquals(0, batch.sentEvents.size)
    assertEquals(1, batch.sentRequests.size)
    assertTrue(batch.sentRequests.contains(2))

    val requests = batch.collectUpdateMetadataRequestsFor(2)
    val allVersions = requests.map(_.version)
    assertTrue(allVersions.forall(_ == expectedUpdateMetadataVersion),
      s"IBP $interBrokerProtocolVersion should use version $expectedUpdateMetadataVersion, but found versions $allVersions")
  }

  @Test
  def testStopReplicaRequestSent(): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val batch = new MockControllerBrokerRequestBatch(context)

    val partitions = Map(
      new TopicPartition("foo", 0) -> LeaderAndDelete(1, false),
      new TopicPartition("foo", 1) -> LeaderAndDelete(2, false),
      new TopicPartition("bar", 1) -> LeaderAndDelete(3, false)
    )

    batch.newBatch()
    partitions.foreach { case (partition, LeaderAndDelete(leaderAndIsr, deletePartition)) =>
      context.putPartitionLeadershipInfo(partition, LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))
      batch.addStopReplicaRequestForBrokers(Seq(2), partition, deletePartition)
    }
    batch.sendRequestsToBrokers(controllerEpoch)

    assertEquals(0, batch.sentEvents.size)
    assertEquals(1, batch.sentRequests.size)
    assertTrue(batch.sentRequests.contains(2))

    val sentRequests = batch.sentRequests(2)
    assertEquals(1, sentRequests.size)

    val sentStopReplicaRequests = batch.collectStopReplicaRequestsFor(2)
    assertEquals(1, sentStopReplicaRequests.size)

    val stopReplicaRequest = sentStopReplicaRequests.head
    assertEquals(partitionStates(partitions), stopReplicaRequest.partitionStates().asScala)

    applyStopReplicaResponseCallbacks(Errors.NONE, batch.sentRequests(2).toList)
    assertEquals(0, batch.sentEvents.size)
  }

  @Test
  def testStopReplicaRequestWithAlreadyDefinedDeletedPartition(): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val batch = new MockControllerBrokerRequestBatch(context)

    val partition = new TopicPartition("foo", 0)
    val leaderAndIsr = LeaderAndIsr(1, List(1, 2))
    context.putPartitionLeadershipInfo(partition, LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))

    batch.newBatch()
    batch.addStopReplicaRequestForBrokers(Seq(2), partition, deletePartition = true)
    batch.addStopReplicaRequestForBrokers(Seq(2), partition, deletePartition = false)
    batch.sendRequestsToBrokers(controllerEpoch)

    val sentStopReplicaRequests = batch.collectStopReplicaRequestsFor(2)
    assertEquals(1, sentStopReplicaRequests.size)

    val stopReplicaRequest = sentStopReplicaRequests.head
    assertEquals(partitionStates(Map(partition -> LeaderAndDelete(leaderAndIsr, true))),
      stopReplicaRequest.partitionStates().asScala)
  }

  @Test
  def testStopReplicaRequestsWhileTopicQueuedForDeletion(): Unit = {
    for (metadataVersion <- MetadataVersion.VERSIONS) {
      testStopReplicaRequestsWhileTopicQueuedForDeletion(metadataVersion)
    }
  }

  private def testStopReplicaRequestsWhileTopicQueuedForDeletion(interBrokerProtocolVersion: MetadataVersion): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val config = createConfig(interBrokerProtocolVersion)
    val batch = new MockControllerBrokerRequestBatch(context, config)

    val partitions = Map(
      new TopicPartition("foo", 0) -> LeaderAndDelete(1, true),
      new TopicPartition("foo", 1) -> LeaderAndDelete(2, true),
      new TopicPartition("bar", 1) -> LeaderAndDelete(3, true)
    )

    // Topic deletion is queued, but has not begun
    context.queueTopicDeletion(Set("foo"))

    batch.newBatch()
    partitions.foreach { case (partition, LeaderAndDelete(leaderAndIsr, deletePartition)) =>
      context.putPartitionLeadershipInfo(partition, LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))
      batch.addStopReplicaRequestForBrokers(Seq(2), partition, deletePartition)
    }
    batch.sendRequestsToBrokers(controllerEpoch)

    assertEquals(0, batch.sentEvents.size)
    assertEquals(1, batch.sentRequests.size)
    assertTrue(batch.sentRequests.contains(2))

    val sentRequests = batch.sentRequests(2)
    assertEquals(1, sentRequests.size)

    val sentStopReplicaRequests = batch.collectStopReplicaRequestsFor(2)
    assertEquals(1, sentStopReplicaRequests.size)

    val stopReplicaRequest = sentStopReplicaRequests.head
    assertEquals(partitionStates(partitions, context.topicsQueuedForDeletion, stopReplicaRequest.version),
      stopReplicaRequest.partitionStates().asScala)

    // No events will be sent after the response returns
    applyStopReplicaResponseCallbacks(Errors.NONE, batch.sentRequests(2).toList)
    assertEquals(0, batch.sentEvents.size)
  }

  @Test
  def testStopReplicaRequestsWhileTopicDeletionStarted(): Unit = {
    for (metadataVersion <- MetadataVersion.VERSIONS) {
      testStopReplicaRequestsWhileTopicDeletionStarted(metadataVersion)
    }
  }

  private def testStopReplicaRequestsWhileTopicDeletionStarted(interBrokerProtocolVersion: MetadataVersion): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val config = createConfig(interBrokerProtocolVersion)
    val batch = new MockControllerBrokerRequestBatch(context, config)

    val partitions = Map(
      new TopicPartition("foo", 0) -> LeaderAndDelete(1, true),
      new TopicPartition("foo", 1) -> LeaderAndDelete(2, true),
      new TopicPartition("bar", 1) -> LeaderAndDelete(3, true)
    )

    context.queueTopicDeletion(Set("foo"))
    context.beginTopicDeletion(Set("foo"))

    batch.newBatch()
    partitions.foreach { case (partition, LeaderAndDelete(leaderAndIsr, deletePartition)) =>
      context.putPartitionLeadershipInfo(partition, LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))
      batch.addStopReplicaRequestForBrokers(Seq(2), partition, deletePartition)
    }
    batch.sendRequestsToBrokers(controllerEpoch)

    assertEquals(0, batch.sentEvents.size)
    assertEquals(1, batch.sentRequests.size)
    assertTrue(batch.sentRequests.contains(2))

    val sentRequests = batch.sentRequests(2)
    assertEquals(1, sentRequests.size)

    val sentStopReplicaRequests = batch.collectStopReplicaRequestsFor(2)
    assertEquals(1, sentStopReplicaRequests.size)

    val stopReplicaRequest = sentStopReplicaRequests.head
    assertEquals(partitionStates(partitions, context.topicsQueuedForDeletion, stopReplicaRequest.version),
      stopReplicaRequest.partitionStates().asScala)

    // When the topic is being deleted, we should provide a callback which sends
    // the received event for the StopReplica response
    applyStopReplicaResponseCallbacks(Errors.NONE, batch.sentRequests(2).toList)
    assertEquals(1, batch.sentEvents.size)

    // We should only receive events for the topic being deleted
    val includedPartitions = batch.sentEvents.flatMap {
      case event: TopicDeletionStopReplicaResponseReceived => event.partitionErrors.keySet
      case otherEvent => throw new AssertionError(s"Unexpected sent event: $otherEvent")
    }.toSet
    assertEquals(partitions.keys.filter(_.topic == "foo"), includedPartitions)
  }

  @Test
  def testStopReplicaRequestWithoutDeletePartitionWhileTopicDeletionStarted(): Unit = {
    for (metadataVersion <- MetadataVersion.VERSIONS) {
      testStopReplicaRequestWithoutDeletePartitionWhileTopicDeletionStarted(metadataVersion)
    }
  }

  private def testStopReplicaRequestWithoutDeletePartitionWhileTopicDeletionStarted(interBrokerProtocolVersion: MetadataVersion): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val config = createConfig(interBrokerProtocolVersion)
    val batch = new MockControllerBrokerRequestBatch(context, config)

    val partitions = Map(
      new TopicPartition("foo", 0) -> LeaderAndDelete(1, false),
      new TopicPartition("foo", 1) -> LeaderAndDelete(2, false),
      new TopicPartition("bar", 1) -> LeaderAndDelete(3, false)
    )

    context.queueTopicDeletion(Set("foo"))
    context.beginTopicDeletion(Set("foo"))

    batch.newBatch()
    partitions.foreach { case (partition, LeaderAndDelete(leaderAndIsr, deletePartition)) =>
      context.putPartitionLeadershipInfo(partition, LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))
      batch.addStopReplicaRequestForBrokers(Seq(2), partition, deletePartition)
    }
    batch.sendRequestsToBrokers(controllerEpoch)

    assertEquals(0, batch.sentEvents.size)
    assertEquals(1, batch.sentRequests.size)
    assertTrue(batch.sentRequests.contains(2))

    val sentRequests = batch.sentRequests(2)
    assertEquals(1, sentRequests.size)

    val sentStopReplicaRequests = batch.collectStopReplicaRequestsFor(2)
    assertEquals(1, sentStopReplicaRequests.size)

    val stopReplicaRequest = sentStopReplicaRequests.head
    assertEquals(partitionStates(partitions, context.topicsQueuedForDeletion, stopReplicaRequest.version),
      stopReplicaRequest.partitionStates().asScala)

    // No events should be fired
    applyStopReplicaResponseCallbacks(Errors.NONE, batch.sentRequests(2).toList)
    assertEquals(0, batch.sentEvents.size)
  }

  @Test
  def testMixedDeleteAndNotDeleteStopReplicaRequests(): Unit = {
    testMixedDeleteAndNotDeleteStopReplicaRequests(MetadataVersion.latest,
      ApiKeys.STOP_REPLICA.latestVersion)

    for (metadataVersion <- MetadataVersion.VERSIONS) {
      if (metadataVersion.isLessThan(IBP_2_2_IV0))
        testMixedDeleteAndNotDeleteStopReplicaRequests(metadataVersion, 0.toShort)
      else if (metadataVersion.isLessThan(IBP_2_4_IV1))
        testMixedDeleteAndNotDeleteStopReplicaRequests(metadataVersion, 1.toShort)
      else if (metadataVersion.isLessThan(IBP_2_6_IV0))
        testMixedDeleteAndNotDeleteStopReplicaRequests(metadataVersion, 2.toShort)
      else
        testMixedDeleteAndNotDeleteStopReplicaRequests(metadataVersion, 3.toShort)
    }
  }

  private def testMixedDeleteAndNotDeleteStopReplicaRequests(interBrokerProtocolVersion: MetadataVersion,
                                                             expectedStopReplicaRequestVersion: Short): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val config = createConfig(interBrokerProtocolVersion)
    val batch = new MockControllerBrokerRequestBatch(context, config)

    val deletePartitions = Map(
      new TopicPartition("foo", 0) -> LeaderAndDelete(1, true),
      new TopicPartition("foo", 1) -> LeaderAndDelete(2, true)
    )

    val nonDeletePartitions = Map(
      new TopicPartition("bar", 0) -> LeaderAndDelete(1, false),
      new TopicPartition("bar", 1) -> LeaderAndDelete(2, false)
    )

    batch.newBatch()
    deletePartitions.foreach { case (partition, LeaderAndDelete(leaderAndIsr, deletePartition)) =>
      context.putPartitionLeadershipInfo(partition, LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))
      batch.addStopReplicaRequestForBrokers(Seq(2), partition, deletePartition)
    }
    nonDeletePartitions.foreach { case (partition, LeaderAndDelete(leaderAndIsr, deletePartition)) =>
      context.putPartitionLeadershipInfo(partition, LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))
      batch.addStopReplicaRequestForBrokers(Seq(2), partition, deletePartition)
    }
    batch.sendRequestsToBrokers(controllerEpoch)

    assertEquals(0, batch.sentEvents.size)
    assertEquals(1, batch.sentRequests.size)
    assertTrue(batch.sentRequests.contains(2))

    // Since IBP_2_6_IV0, only one StopReplicaRequest is sent out
    if (interBrokerProtocolVersion.isAtLeast(IBP_2_6_IV0)) {
      val sentRequests = batch.sentRequests(2)
      assertEquals(1, sentRequests.size)

      val sentStopReplicaRequests = batch.collectStopReplicaRequestsFor(2)
      assertEquals(1, sentStopReplicaRequests.size)

      val stopReplicaRequest = sentStopReplicaRequests.head
      assertEquals(partitionStates(deletePartitions ++ nonDeletePartitions, version = stopReplicaRequest.version),
        stopReplicaRequest.partitionStates().asScala)
    } else {
      val sentRequests = batch.sentRequests(2)
      assertEquals(2, sentRequests.size)

      val sentStopReplicaRequests = batch.collectStopReplicaRequestsFor(2)
      assertEquals(2, sentStopReplicaRequests.size)

      // StopReplicaRequest (deletePartitions = true) is sent first
      val stopReplicaRequestWithDelete = sentStopReplicaRequests(0)
      assertEquals(partitionStates(deletePartitions, version = stopReplicaRequestWithDelete.version),
        stopReplicaRequestWithDelete.partitionStates().asScala)
      val stopReplicaRequestWithoutDelete = sentStopReplicaRequests(1)
      assertEquals(partitionStates(nonDeletePartitions, version = stopReplicaRequestWithoutDelete.version),
        stopReplicaRequestWithoutDelete.partitionStates().asScala)
    }
  }

  @Test
  def testStopReplicaGroupsByBroker(): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val batch = new MockControllerBrokerRequestBatch(context)

    val partitions = Map(
      new TopicPartition("foo", 0) -> LeaderAndDelete(1, false),
      new TopicPartition("foo", 1) -> LeaderAndDelete(2, false),
      new TopicPartition("bar", 1) -> LeaderAndDelete(3, false)
    )

    batch.newBatch()
    partitions.foreach { case (partition, LeaderAndDelete(leaderAndIsr, deletePartition)) =>
      context.putPartitionLeadershipInfo(partition, LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))
      batch.addStopReplicaRequestForBrokers(Seq(2, 3), partition, deletePartition)
    }
    batch.sendRequestsToBrokers(controllerEpoch)

    assertEquals(0, batch.sentEvents.size)
    assertEquals(2, batch.sentRequests.size)
    assertTrue(batch.sentRequests.contains(2))
    assertTrue(batch.sentRequests.contains(3))

    val sentRequests = batch.sentRequests(2)
    assertEquals(1, sentRequests.size)

    for (brokerId <- Set(2, 3)) {
      val sentStopReplicaRequests = batch.collectStopReplicaRequestsFor(brokerId)
      assertEquals(1, sentStopReplicaRequests.size)

      val stopReplicaRequest = sentStopReplicaRequests.head
      assertEquals(partitionStates(partitions), stopReplicaRequest.partitionStates().asScala)

      applyStopReplicaResponseCallbacks(Errors.NONE, batch.sentRequests(2).toList)
      assertEquals(0, batch.sentEvents.size)
    }
  }

  @Test
  def testStopReplicaSentOnlyToLiveAndShuttingDownBrokers(): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo", "bar"))
    val batch = new MockControllerBrokerRequestBatch(context)

    // 2 is shutting down, 3 is dead
    context.shuttingDownBrokerIds.add(2)
    context.removeLiveBrokers(Set(3))

    val partitions = Map(
      new TopicPartition("foo", 0) -> LeaderAndDelete(1, false),
      new TopicPartition("foo", 1) -> LeaderAndDelete(2, false),
      new TopicPartition("bar", 1) -> LeaderAndDelete(3, false)
    )

    batch.newBatch()
    partitions.foreach { case (partition, LeaderAndDelete(leaderAndIsr, deletePartition)) =>
      context.putPartitionLeadershipInfo(partition, LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))
      batch.addStopReplicaRequestForBrokers(Seq(2, 3), partition, deletePartition)
    }
    batch.sendRequestsToBrokers(controllerEpoch)

    assertEquals(0, batch.sentEvents.size)
    assertEquals(1, batch.sentRequests.size)
    assertTrue(batch.sentRequests.contains(2))

    val sentRequests = batch.sentRequests(2)
    assertEquals(1, sentRequests.size)

    val sentStopReplicaRequests = batch.collectStopReplicaRequestsFor(2)
    assertEquals(1, sentStopReplicaRequests.size)

    val stopReplicaRequest = sentStopReplicaRequests.head
    assertEquals(partitionStates(partitions), stopReplicaRequest.partitionStates().asScala)
  }

  @Test
  def testStopReplicaInterBrokerProtocolVersion(): Unit = {
    testStopReplicaFollowsInterBrokerProtocolVersion(MetadataVersion.latest, ApiKeys.STOP_REPLICA.latestVersion)

    for (metadataVersion <- MetadataVersion.VERSIONS) {
      if (metadataVersion.isLessThan(IBP_2_2_IV0))
        testStopReplicaFollowsInterBrokerProtocolVersion(metadataVersion, 0.toShort)
      else if (metadataVersion.isLessThan(IBP_2_4_IV1))
        testStopReplicaFollowsInterBrokerProtocolVersion(metadataVersion, 1.toShort)
      else if (metadataVersion.isLessThan(IBP_2_6_IV0))
        testStopReplicaFollowsInterBrokerProtocolVersion(metadataVersion, 2.toShort)
      else if (metadataVersion.isLessThan(IBP_3_4_IV0))
        testStopReplicaFollowsInterBrokerProtocolVersion(metadataVersion, 3.toShort)
      else
        testStopReplicaFollowsInterBrokerProtocolVersion(metadataVersion, 4.toShort)
    }
  }

  private def testStopReplicaFollowsInterBrokerProtocolVersion(interBrokerProtocolVersion: MetadataVersion,
                                                               expectedStopReplicaRequestVersion: Short): Unit = {
    val context = initContext(Seq(1, 2, 3), 2, 3, Set("foo"))
    val config = createConfig(interBrokerProtocolVersion)
    val batch = new MockControllerBrokerRequestBatch(context, config)

    val partition = new TopicPartition("foo", 0)
    val leaderAndIsr = LeaderAndIsr(1, List(1, 2))

    context.putPartitionLeadershipInfo(partition, LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))

    batch.newBatch()
    batch.addStopReplicaRequestForBrokers(Seq(2), partition, deletePartition = false)
    batch.sendRequestsToBrokers(controllerEpoch)

    assertEquals(0, batch.sentEvents.size)
    assertEquals(1, batch.sentRequests.size)
    assertTrue(batch.sentRequests.contains(2))

    val requests = batch.collectStopReplicaRequestsFor(2)
    val allVersions = requests.map(_.version)
    assertTrue(allVersions.forall(_ == expectedStopReplicaRequestVersion),
      s"IBP $interBrokerProtocolVersion should use version $expectedStopReplicaRequestVersion, but found versions $allVersions")
  }

  private case class LeaderAndDelete(leaderAndIsr: LeaderAndIsr,
                                     deletePartition: Boolean)

  private object LeaderAndDelete {
    def apply(leader: Int, deletePartition: Boolean): LeaderAndDelete =
      new LeaderAndDelete(LeaderAndIsr(leader, List()), deletePartition)
  }

  private def partitionStates(partitions: Map[TopicPartition, LeaderAndDelete],
                              topicsQueuedForDeletion: collection.Set[String] = Set.empty[String],
                              version: Short = ApiKeys.STOP_REPLICA.latestVersion): Map[TopicPartition, StopReplicaPartitionState] = {
    partitions.map { case (topicPartition, LeaderAndDelete(leaderAndIsr, deletePartition)) =>
      topicPartition -> {
        val partitionState = new StopReplicaPartitionState()
          .setPartitionIndex(topicPartition.partition)
          .setDeletePartition(deletePartition)

        if (version >= 3) {
          partitionState.setLeaderEpoch(if (topicsQueuedForDeletion.contains(topicPartition.topic))
            LeaderAndIsr.EpochDuringDelete
          else
            leaderAndIsr.leaderEpoch)
        }

        partitionState
      }
    }
  }

  private def applyStopReplicaResponseCallbacks(error: Errors, sentRequests: List[SentRequest]): Unit = {
    sentRequests.filter(_.responseCallback != null).foreach { sentRequest =>
      val stopReplicaRequest = sentRequest.request.build().asInstanceOf[StopReplicaRequest]
      val stopReplicaResponse =
        if (error == Errors.NONE) {
          val partitionErrors = stopReplicaRequest.topicStates.asScala.flatMap { topic =>
            topic.partitionStates.asScala.map { partition =>
              new StopReplicaPartitionError()
                .setTopicName(topic.topicName)
                .setPartitionIndex(partition.partitionIndex)
                .setErrorCode(error.code)
            }
          }.toBuffer.asJava
          new StopReplicaResponse(new StopReplicaResponseData().setPartitionErrors(partitionErrors))
        } else {
          stopReplicaRequest.getErrorResponse(error.exception)
        }
      sentRequest.responseCallback.apply(stopReplicaResponse)
    }
  }

  private def applyLeaderAndIsrResponseCallbacks(error: Errors, sentRequests: List[SentRequest]): Unit = {
    sentRequests.filter(_.request.apiKey == ApiKeys.LEADER_AND_ISR).filter(_.responseCallback != null).foreach { sentRequest =>
      val leaderAndIsrRequest = sentRequest.request.build().asInstanceOf[LeaderAndIsrRequest]
      val topicIds = leaderAndIsrRequest.topicIds
      val data = new LeaderAndIsrResponseData()
        .setErrorCode(error.code)
      leaderAndIsrRequest.data.topicStates.asScala.foreach { t =>
        data.topics.add(new LeaderAndIsrTopicError()
          .setTopicId(topicIds.get(t.topicName))
          .setPartitionErrors(t.partitionStates.asScala.map(p =>
            new LeaderAndIsrPartitionError()
              .setPartitionIndex(p.partitionIndex)
              .setErrorCode(error.code)).asJava))
      }
      val leaderAndIsrResponse = new LeaderAndIsrResponse(data, leaderAndIsrRequest.version)
      sentRequest.responseCallback(leaderAndIsrResponse)
    }
  }

  private def applyUpdateMetadataResponseCallbacks(error: Errors, sentRequests: List[SentRequest]): Unit = {
    sentRequests.filter(_.request.apiKey == ApiKeys.UPDATE_METADATA).filter(_.responseCallback != null).foreach { sentRequest =>
      val response = new UpdateMetadataResponse(new UpdateMetadataResponseData().setErrorCode(error.code))
      sentRequest.responseCallback(response)
    }
  }

  private def createConfig(interBrokerVersion: MetadataVersion): KafkaConfig = {
    val props = new Properties()
    props.put(KafkaConfig.BrokerIdProp, controllerId.toString)
    props.put(KafkaConfig.ZkConnectProp, "zkConnect")
    TestUtils.setIbpAndMessageFormatVersions(props, interBrokerVersion)
    KafkaConfig.fromProps(props)
  }

  private def replicaAssignment(replicas: Seq[Int]): ReplicaAssignment = ReplicaAssignment(replicas, Seq(), Seq())

  private def initContext(brokers: Seq[Int],
                          numPartitions: Int,
                          replicationFactor: Int,
                          topics: Set[String]): ControllerContext = initContext(brokers, numPartitions,
    replicationFactor, topics.map(_ -> Uuid.randomUuid()).toMap)

  private def initContext(brokers: Seq[Int],
                          numPartitions: Int,
                          replicationFactor: Int,
                          topicIds: Map[String, Uuid]): ControllerContext = {
    val context = new ControllerContext
    val brokerEpochs = brokers.map { brokerId =>
      val endpoint = new EndPoint("localhost", 9900 + brokerId, new ListenerName("PLAINTEXT"),
        SecurityProtocol.PLAINTEXT)
      Broker(brokerId, Seq(endpoint), rack = None) -> 1L
    }.toMap

    context.setLiveBrokers(brokerEpochs)
    context.setAllTopics(topicIds.keySet)
    topicIds.foreach { case (name, id) => context.addTopicId(name, id) }

    // Simple round-robin replica assignment
    var leaderIndex = 0
    for (topic <- topicIds.keys; partitionId <- 0 until numPartitions) {
      val partition = new TopicPartition(topic, partitionId)
      val replicas = (0 until replicationFactor).map { i =>
        val replica = brokers((i + leaderIndex) % brokers.size)
        replica
      }
      context.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(replicas))
      leaderIndex += 1
    }

    context
  }

  private case class SentRequest(request: ControlRequest, responseCallback: AbstractResponse => Unit)

  private class MockControllerBrokerRequestBatch(context: ControllerContext, config: KafkaConfig = config)
    extends AbstractControllerBrokerRequestBatch(config, () => context, () => config.interBrokerProtocolVersion, logger) {

    val sentEvents = ListBuffer.empty[ControllerEvent]
    val sentRequests = mutable.Map.empty[Int, ListBuffer[SentRequest]]

    override def sendRequest(brokerId: Int, request: ControlRequest, callback: AbstractResponse => Unit): Unit = {
      sentRequests.getOrElseUpdate(brokerId, ListBuffer.empty)
      sentRequests(brokerId).append(SentRequest(request, callback))
    }

    def collectStopReplicaRequestsFor(brokerId: Int): List[StopReplicaRequest] = {
      sentRequests.get(brokerId) match {
        case Some(requests) => requests
          .filter(_.request.apiKey == ApiKeys.STOP_REPLICA)
          .map(_.request.build().asInstanceOf[StopReplicaRequest]).toList
        case None => List.empty[StopReplicaRequest]
      }
    }

    def collectUpdateMetadataRequestsFor(brokerId: Int): List[UpdateMetadataRequest] = {
      sentRequests.get(brokerId) match {
        case Some(requests) => requests
          .filter(_.request.apiKey == ApiKeys.UPDATE_METADATA)
          .map(_.request.build().asInstanceOf[UpdateMetadataRequest]).toList
        case None => List.empty[UpdateMetadataRequest]
      }
    }

    def collectLeaderAndIsrRequestsFor(brokerId: Int): List[LeaderAndIsrRequest] = {
      sentRequests.get(brokerId) match {
        case Some(requests) => requests
          .filter(_.request.apiKey == ApiKeys.LEADER_AND_ISR)
          .map(_.request.build().asInstanceOf[LeaderAndIsrRequest]).toList
        case None => List.empty[LeaderAndIsrRequest]
      }
    }

    override def handleLeaderAndIsrResponse(response: LeaderAndIsrResponse, broker: Int): Unit = {
      sentEvents.append(LeaderAndIsrResponseReceived(response, broker))
    }

    override def handleUpdateMetadataResponse(response: UpdateMetadataResponse, broker: Int)
    : Unit = {
      sentEvents.append(UpdateMetadataResponseReceived(response, broker))
    }

    override def handleStopReplicaResponse(stopReplicaResponse: StopReplicaResponse,
                                           brokerId: Int, partitionErrorsForDeletingTopics: Map[TopicPartition, Errors]): Unit = {
      if (partitionErrorsForDeletingTopics.nonEmpty)
        sentEvents.append(TopicDeletionStopReplicaResponseReceived(brokerId, stopReplicaResponse.error, partitionErrorsForDeletingTopics))
    }
  }

}
