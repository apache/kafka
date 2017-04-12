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

import java.util.concurrent.LinkedBlockingQueue

import kafka.server.MetadataCache
import kafka.utils.Logging
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkersRequest}
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.{concurrent, immutable, mutable}
import collection.JavaConverters._

class TransactionMarkerChannel(interBrokerListenerName: ListenerName, metadataCache: MetadataCache) extends Logging {

  private[transaction] val brokerStateMap: concurrent.Map[Int, DestinationBrokerAndQueuedMarkers] = concurrent.TrieMap.empty[Int, DestinationBrokerAndQueuedMarkers]
  private val pendingTxnMap: concurrent.Map[Long, PendingTxn] = concurrent.TrieMap.empty[Long, PendingTxn]

  def addNewBroker(broker: Node) {
    val markersQueue = new LinkedBlockingQueue[CoordinatorEpochAndMarkers]()
    brokerStateMap.put(broker.id, DestinationBrokerAndQueuedMarkers(broker, markersQueue))
    trace(s"Added destination broker ${broker.id}: $broker")
  }

  private[transaction] def addRequestForBroker(brokerId: Int, txnMarkerRequest: CoordinatorEpochAndMarkers) {
    val markersQueue = brokerStateMap(brokerId).markersQueue
    markersQueue.add(txnMarkerRequest)
    trace(s"Added markers $txnMarkerRequest for broker $brokerId")
  }

  def addRequestToSend(pid: Long, epoch: Short, result: TransactionResult, coordinatorEpoch: Int, topicPartitions: immutable.Set[TopicPartition]): Unit = {
    val partitionsByDestination: immutable.Map[Int, immutable.Set[TopicPartition]] = topicPartitions.groupBy { topicPartition: TopicPartition =>
      val leaderForPartition = metadataCache.getPartitionInfo(topicPartition.topic, topicPartition.partition)
      leaderForPartition match {
        case Some(partitionInfo) =>
          val brokerId = partitionInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader
          if (!brokerStateMap.contains(brokerId)) {
            val broker = metadataCache.getAliveEndpoint(brokerId, interBrokerListenerName).get
            addNewBroker(broker)
          }
          brokerId
        case None =>
          // TODO: there is a rare case that the producer gets the partition info from another broker who has the newer information of the
          // partition, while the TC itself has not received the propagated metadata update; do we need to block when this happens?
          throw new IllegalStateException("Cannot find the leader broker for the txn marker to write for topic partition " + topicPartition)
      }
    }

    for ((brokerId: Int, topicPartitions: immutable.Set[TopicPartition]) <- partitionsByDestination) {
      val txnMarker = new TxnMarkerEntry(pid, epoch, result, topicPartitions.toList.asJava)
      addRequestForBroker(brokerId, CoordinatorEpochAndMarkers(coordinatorEpoch, Utils.mkList(txnMarker)))
    }
  }

  def maybeAddPendingRequest(metadata: TransactionMetadata, completionCallback: Errors => Unit): Unit = {
    val existingMetadataToWrite = pendingTxnMap.putIfAbsent(metadata.pid, PendingTxn(metadata, completionCallback))
    if (existingMetadataToWrite.isDefined)
      throw new IllegalStateException(s"There is already a pending txn to write its markers ${existingMetadataToWrite.get}; this should not happen")
  }

  def completedTransactions(): mutable.Map[Long, PendingTxn] = pendingTxnMap.filter { entry =>
    entry._2.transactionMetadata.topicPartitions.isEmpty
  }

  def removeCompletedTxn(pid: Long): Unit = {
    pendingTxnMap.remove(pid)
  }

  def pendingTxnMetadata(pid: Long): TransactionMetadata = {
    pendingTxnMap.getOrElse (pid, throw new IllegalStateException ("Cannot find the pending txn marker in cache; it should not happen") ).transactionMetadata
  }

  def clear(): Unit = {
    brokerStateMap.clear()
    pendingTxnMap.clear()
  }

}
