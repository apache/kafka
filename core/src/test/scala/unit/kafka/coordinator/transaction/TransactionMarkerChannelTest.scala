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

package kafka.coordinator.transaction

import kafka.api.{LeaderAndIsr, PartitionStateInfo}
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.server.{DelayedOperationPurgatory, MetadataCache}
import kafka.utils.MockTime
import kafka.utils.timer.MockTimer
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkersRequest}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Node, TopicPartition}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TransactionMarkerChannelTest {

  private val metadataCache = EasyMock.createNiceMock(classOf[MetadataCache])
  private val networkClient = EasyMock.createNiceMock(classOf[NetworkClient])
  private val purgatory = new DelayedOperationPurgatory[DelayedTxnMarker]("name", new MockTimer, reaperEnabled = false)
  private val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
  private val channel = new TransactionMarkerChannel(listenerName, metadataCache, networkClient, new MockTime())
  private val partition1 = new TopicPartition("topic1", 0)


  @Test
  def shouldAddEmptyBrokerQueueWhenAddingNewBroker(): Unit = {
    channel.addOrUpdateBroker(new Node(1, "host", 10))
    channel.addOrUpdateBroker(new Node(2, "host", 10))
    assertEquals(0, channel.queueForBroker(1).get.eachMetadataPartition{case(partition:Int, _) => partition}.size)
    assertEquals(0, channel.queueForBroker(2).get.eachMetadataPartition{case(partition:Int, _) => partition}.size)
  }

  @Test
  def shouldUpdateDestinationBrokerNodeWhenUpdatingBroker(): Unit = {
    val newDestination = new Node(1, "otherhost", 100)

    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))

    // getAliveEndpoint returns an updated node
    EasyMock.expect(metadataCache.getAliveEndpoint(1, listenerName)).andReturn(Some(newDestination))
    EasyMock.replay(metadataCache)

    channel.addOrUpdateBroker(new Node(1, "host", 10))
    channel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](partition1))

    val brokerRequestQueue = channel.queueForBroker(1).get
    assertEquals(newDestination, brokerRequestQueue.node)
    assertEquals(1, brokerRequestQueue.totalQueuedRequests())
  }


  @Test
  def shouldQueueRequestsByBrokerId(): Unit = {
    channel.addOrUpdateBroker(new Node(1, "host", 10))
    channel.addOrUpdateBroker(new Node(2, "otherhost", 10))
    channel.addRequestForBroker(1, 0, new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, 0, TransactionResult.COMMIT, Utils.mkList()))
    channel.addRequestForBroker(1, 0, new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, 0, TransactionResult.COMMIT, Utils.mkList()))
    channel.addRequestForBroker(2, 0, new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, 0, TransactionResult.COMMIT, Utils.mkList()))

    assertEquals(2, channel.queueForBroker(1).get.totalQueuedRequests())
    assertEquals(1, channel.queueForBroker(2).get.totalQueuedRequests())
  }

  @Test
  def shouldNotAddPendingTxnIfOneAlreadyExistsForPid(): Unit = {
    channel.maybeAddPendingRequest(0, new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set.empty, 0, 0))
    assertFalse(channel.maybeAddPendingRequest(0, new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set.empty, 0, 0)))
  }

  @Test
  def shouldAddRequestsToCorrectBrokerQueues(): Unit = {
    val partition2 = new TopicPartition("topic1", 1)

    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))

    EasyMock.expect(metadataCache.getPartitionInfo(partition2.topic(), partition2.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(2, 0, List.empty, 0), 0), Set.empty)))

    EasyMock.expect(metadataCache.getAliveEndpoint(1, listenerName)).andReturn(Some(new Node(1, "host", 10)))
    EasyMock.expect(metadataCache.getAliveEndpoint(2, listenerName)).andReturn(Some(new Node(2, "otherhost", 10)))

    EasyMock.replay(metadataCache)
    channel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](partition1, partition2))

    assertEquals(1, channel.queueForBroker(1).get.totalQueuedRequests())
    assertEquals(1, channel.queueForBroker(2).get.totalQueuedRequests())
  }
  @Test
  def shouldWakeupNetworkClientWhenRequestsQueued(): Unit = {
    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))
    EasyMock.expect(metadataCache.getAliveEndpoint(1, listenerName)).andReturn(Some(new Node(1, "host", 10)))

    EasyMock.expect(networkClient.wakeup())

    EasyMock.replay(metadataCache, networkClient)
    channel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](partition1))

    EasyMock.verify(networkClient)
  }

  @Test
  def shouldAddNewBrokerQueueIfDoesntAlreadyExistWhenAddingRequest(): Unit = {
    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))
    EasyMock.expect(metadataCache.getAliveEndpoint(1, listenerName)).andReturn(Some(new Node(1, "host", 10)))

    EasyMock.replay(metadataCache)
    channel.addRequestToSend(0, 0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](partition1))

    assertEquals(1, channel.queueForBroker(1).get.totalQueuedRequests())
    EasyMock.verify(metadataCache)
  }

  @Test
  def shouldGetPendingTxnMetadataByPid(): Unit = {
    val metadataPartition = 0
    val transaction = new TransactionMetadata(1, 0, 0, PrepareCommit, mutable.Set.empty, 0, 0)
    channel.maybeAddPendingRequest(metadataPartition, transaction)
    channel.maybeAddPendingRequest(metadataPartition, new TransactionMetadata(2, 0, 0, PrepareCommit, mutable.Set.empty, 0, 0))
    assertEquals(Some(transaction), channel.pendingTxnMetadata(metadataPartition, 1))
  }

  @Test
  def shouldRemovePendingRequestsForPartitionWhenPartitionEmigrated(): Unit = {
    channel.maybeAddPendingRequest(0, new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set.empty, 0, 0))
    channel.maybeAddPendingRequest(0, new TransactionMetadata(1, 0, 0, PrepareCommit, mutable.Set.empty, 0, 0))
    val metadata = new TransactionMetadata(2, 0, 0, PrepareCommit, mutable.Set.empty, 0, 0)
    channel.maybeAddPendingRequest(1, metadata)

    channel.removeStateForPartition(0)

    assertEquals(None, channel.pendingTxnMetadata(0, 0))
    assertEquals(None, channel.pendingTxnMetadata(0, 1))
    assertEquals(Some(metadata), channel.pendingTxnMetadata(1, 2))
  }

  @Test
  def shouldRemoveBrokerRequestsForPartitionWhenPartitionEmigrated(): Unit = {
    channel.addOrUpdateBroker(new Node(1, "host", 10))
    channel.addRequestForBroker(1, 0, new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, 0, TransactionResult.COMMIT, Utils.mkList()))
    channel.addRequestForBroker(1, 1, new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, 0, TransactionResult.COMMIT, Utils.mkList()))
    channel.addRequestForBroker(1, 1, new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, 0, TransactionResult.COMMIT, Utils.mkList()))

    channel.removeStateForPartition(1)


    val result = channel.queueForBroker(1).get.eachMetadataPartition{case (partition:Int, _) => partition}.toList
    assertEquals(List(0), result)
  }



  def errorCallback(error: Errors): Unit = {}
}
