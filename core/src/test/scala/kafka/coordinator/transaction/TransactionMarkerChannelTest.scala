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
import kafka.server.MetadataCache
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkersRequest}
import org.apache.kafka.common.utils.Utils
import org.easymock.EasyMock
import org.junit.Test
import org.junit.Assert._

import scala.collection.mutable

class TransactionMarkerChannelTest {

  private val metadataCache = EasyMock.createNiceMock(classOf[MetadataCache])
  private val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
  private val channel = new TransactionMarkerChannel(listenerName, metadataCache)


  @Test
  def shouldAddEmptyBrokerQueueWhenAddNewBroker(): Unit = {
    channel.addNewBroker(new Node(1, "host", 10))
    channel.addNewBroker(new Node(2, "host", 10))
    assertEquals(0, channel.brokerStateMap(1).markersQueue.size())
    assertEquals(0, channel.brokerStateMap(2).markersQueue.size())
  }

  @Test
  def shouldQueueRequestsByBrokerId(): Unit = {
    channel.addNewBroker(new Node(1, "host", 10))
    channel.addNewBroker(new Node(2, "otherhost", 10))
    channel.addRequestForBroker(1, CoordinatorEpochAndMarkers(0, Utils.mkList(new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, TransactionResult.COMMIT, Utils.mkList()))))
    channel.addRequestForBroker(1, CoordinatorEpochAndMarkers(0, Utils.mkList(new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, TransactionResult.COMMIT, Utils.mkList()))))
    channel.addRequestForBroker(2, CoordinatorEpochAndMarkers(0, Utils.mkList(new WriteTxnMarkersRequest.TxnMarkerEntry(0, 0, TransactionResult.COMMIT, Utils.mkList()))))

    assertEquals(2, channel.brokerStateMap(1).markersQueue.size())
    assertEquals(1, channel.brokerStateMap(2).markersQueue.size())
  }

  @Test
  def shouldNotAddPendingTxnIfOneAlreadyExistsForPid(): Unit = {
    channel.maybeAddPendingRequest(new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set.empty, 0), errorCallback)
    try {
      channel.maybeAddPendingRequest(new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set.empty, 0), errorCallback)
      fail("Should have throw IllegalStateException")
    } catch {
      case e: IllegalStateException => // ok
    }
  }

  @Test
  def shouldAddRequestsToCorrectBrokerQueues(): Unit = {
    val partition1 = new TopicPartition("topic1", 0)
    val partition2 = new TopicPartition("topic1", 1)

    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))

    EasyMock.expect(metadataCache.getPartitionInfo(partition2.topic(), partition2.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(2, 0, List.empty, 0), 0), Set.empty)))

    EasyMock.replay(metadataCache)
    channel.addNewBroker(new Node(1, "host", 10))
    channel.addNewBroker(new Node(2, "otherhost", 10))
    channel.addRequestToSend(0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](partition1, partition2))

    assertEquals(1, channel.brokerStateMap(1).markersQueue.size())
    assertEquals(1, channel.brokerStateMap(2).markersQueue.size())
  }

  @Test
  def shouldAddNewBrokerQueueIfDoesntAlreadyExistWhenAddingRequest(): Unit = {
    val partition1 = new TopicPartition("topic1", 0)

    EasyMock.expect(metadataCache.getPartitionInfo(partition1.topic(), partition1.partition()))
      .andReturn(Some(PartitionStateInfo(LeaderIsrAndControllerEpoch(LeaderAndIsr(1, 0, List.empty, 0), 0), Set.empty)))
    EasyMock.expect(metadataCache.getAliveEndpoint(1, listenerName)).andReturn(Some(new Node(1, "host", 10)))

    EasyMock.replay(metadataCache)
    channel.addRequestToSend(0, 0, TransactionResult.COMMIT, 0, Set[TopicPartition](partition1))

    assertEquals(1, channel.brokerStateMap(1).markersQueue.size())
    EasyMock.verify(metadataCache)
  }

  @Test
  def shouldReturnCompletedTransactions(): Unit = {
    val completedTransaction = new TransactionMetadata(0, 0, 0, PrepareCommit, mutable.Set.empty, 0)
    channel.maybeAddPendingRequest(completedTransaction, errorCallback)
    channel.maybeAddPendingRequest(new TransactionMetadata(1, 0, 0, PrepareCommit, mutable.Set[TopicPartition](new TopicPartition("topic", 1)), 0), errorCallback)
    channel.maybeAddPendingRequest(new TransactionMetadata(2, 0, 0, PrepareCommit, mutable.Set[TopicPartition](new TopicPartition("topic", 2)), 0), errorCallback)

    val completed = channel.completedTransactions()
    assertEquals(1, completed.size)
    val pendingTxn = completed(0L)
    assertEquals(completedTransaction, pendingTxn.transactionMetadata)
  }

  def shouldGetPendingTxnMetadataByPid(): Unit = {
    val transaction = new TransactionMetadata(1, 0, 0, PrepareCommit, mutable.Set.empty, 0)
    channel.maybeAddPendingRequest(transaction, errorCallback)
    channel.maybeAddPendingRequest(new TransactionMetadata(2, 0, 0, PrepareCommit, mutable.Set.empty, 0), errorCallback)
    assertEquals(transaction, channel.pendingTxnMetadata(1))
  }

  def errorCallback(error: Errors): Unit = {}
}
