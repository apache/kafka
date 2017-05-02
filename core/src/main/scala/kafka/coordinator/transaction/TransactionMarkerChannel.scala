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

import java.util
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import kafka.common.{BrokerEndPointNotAvailableException, RequestAndCompletionHandler}
import kafka.server.{DelayedOperationPurgatory, MetadataCache}
import kafka.utils.Logging
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkersRequest}
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.{concurrent, immutable, mutable}
import collection.JavaConverters._

class TransactionMarkerChannel(interBrokerListenerName: ListenerName,
                               metadataCache: MetadataCache,
                               networkClient: NetworkClient,
                               time: Time) extends Logging {

  // we need the txnTopicPartition so we can clean up when Transaction Log partitions emigrate
  case class PendingTxnKey(txnTopicPartition: Int, producerId: Long)

  class BrokerRequestQueue(private var destination: Node) {

    // keep track of the requests per txn topic partition so we can easily clear the queue
    // during partition emigration
    private val requestsPerTxnTopicPartition: concurrent.Map[Int, BlockingQueue[TxnMarkerEntry]]
      = concurrent.TrieMap.empty[Int, BlockingQueue[TxnMarkerEntry]]

    def removeRequestsForPartition(partition: Int): Unit = {
      requestsPerTxnTopicPartition.remove(partition)
    }

    def maybeUpdateNode(node: Node): Unit = {
      destination = node
    }

    def addRequests(txnTopicPartition: Int, txnMarkerEntry: TxnMarkerEntry): Unit = {
      val queue = requestsPerTxnTopicPartition.getOrElseUpdate(txnTopicPartition, new LinkedBlockingQueue[TxnMarkerEntry]())
      queue.add(txnMarkerEntry)
    }

    def eachMetadataPartition[B](f:(Int, BlockingQueue[TxnMarkerEntry]) => B): mutable.Iterable[B] =
      requestsPerTxnTopicPartition.filter{ case(_, queue) => !queue.isEmpty}
        .map{case(partition:Int, queue:BlockingQueue[TxnMarkerEntry]) => f(partition, queue)}


    def node: Node = destination

    def totalQueuedRequests(): Int =
      requestsPerTxnTopicPartition.map { case(_, queue) => queue.size()}
        .sum

  }

  private val brokerStateMap: concurrent.Map[Int, BrokerRequestQueue] = concurrent.TrieMap.empty[Int, BrokerRequestQueue]
  private val pendingTxnMap: concurrent.Map[PendingTxnKey, TransactionMetadata] = concurrent.TrieMap.empty[PendingTxnKey, TransactionMetadata]

  // TODO: What is reasonable for this
  private val brokerNotAliveBackoffMs = 10

  // visible for testing
  private[transaction] def queueForBroker(brokerId: Int) = {
    brokerStateMap.get(brokerId)
  }

  private[transaction]
  def drainQueuedTransactionMarkers(txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker]): Iterable[RequestAndCompletionHandler] = {
    brokerStateMap.flatMap {case (brokerId: Int, brokerRequestQueue: BrokerRequestQueue) =>
      brokerRequestQueue.eachMetadataPartition{ case(partitionId, queue) =>
        val markersToSend: java.util.List[TxnMarkerEntry] = new util.ArrayList[TxnMarkerEntry]()
        queue.drainTo(markersToSend)
        val requestCompletionHandler = new TransactionMarkerRequestCompletionHandler(this, txnMarkerPurgatory, partitionId, markersToSend, brokerId)
        RequestAndCompletionHandler(brokerRequestQueue.node, new WriteTxnMarkersRequest.Builder(markersToSend), requestCompletionHandler)
      }
    }
  }


  def addOrUpdateBroker(broker: Node) {
    brokerStateMap.putIfAbsent(broker.id(), new BrokerRequestQueue(broker)) match {
      case Some(brokerQueue) => brokerQueue.maybeUpdateNode(broker)
      case None => // nothing to do
    }
  }

  private[transaction] def addRequestForBroker(brokerId: Int, metadataPartition: Int, txnMarkerEntry: TxnMarkerEntry) {
    val brokerQueue = brokerStateMap(brokerId)
    brokerQueue.addRequests(metadataPartition, txnMarkerEntry)
    trace(s"Added marker $txnMarkerEntry for broker $brokerId")
  }

  def addRequestToSend(metadataPartition: Int, pid: Long, epoch: Short, result: TransactionResult, coordinatorEpoch: Int, topicPartitions: immutable.Set[TopicPartition]): Unit = {
    val partitionsByDestination: immutable.Map[Int, immutable.Set[TopicPartition]] = topicPartitions.groupBy { topicPartition: TopicPartition =>
      val currentBrokers = mutable.Set.empty[Int]
      var brokerId:Option[Int] = None

      while(brokerId.isEmpty) {
        val leaderForPartition = metadataCache.getPartitionInfo(topicPartition.topic, topicPartition.partition)
        leaderForPartition match {
          case Some(partitionInfo) =>
            val leaderId = partitionInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader
            if (currentBrokers.add(leaderId)) {
              try {
                metadataCache.getAliveEndpoint(leaderId, interBrokerListenerName) match {
                  case Some(broker) =>
                    addOrUpdateBroker(broker)
                    brokerId = Some(leaderId)
                  case None =>
                    currentBrokers.remove(leaderId)
                    trace(s"alive endpoint for broker with id: $leaderId not available. retrying")

                }
              } catch {
                case _:BrokerEndPointNotAvailableException =>
                  currentBrokers.remove(leaderId)
                  trace(s"alive endpoint for broker with id: $leaderId not available. retrying")
              }
            }
          case None =>
            trace(s"couldn't find leader for partition: $topicPartition")
        }
        if (brokerId.isEmpty)
          time.sleep(brokerNotAliveBackoffMs)
      }
      brokerId.get
    }

    for ((brokerId: Int, topicPartitions: immutable.Set[TopicPartition]) <- partitionsByDestination) {
      val txnMarker = new TxnMarkerEntry(pid, epoch, coordinatorEpoch, result, topicPartitions.toList.asJava)
      addRequestForBroker(brokerId, metadataPartition, txnMarker)
    }
    networkClient.wakeup()
  }

  def maybeAddPendingRequest(metadataPartition: Int, metadata: TransactionMetadata): Boolean = {
    val existingMetadataToWrite = pendingTxnMap.putIfAbsent(PendingTxnKey(metadataPartition, metadata.pid), metadata)
    existingMetadataToWrite.isEmpty
  }

  def removeCompletedTxn(metadataPartition: Int, pid: Long): Unit = {
    pendingTxnMap.remove(PendingTxnKey(metadataPartition, pid))
  }

  def pendingTxnMetadata(metadataPartition: Int, pid: Long): Option[TransactionMetadata] = {
    pendingTxnMap.get(PendingTxnKey(metadataPartition, pid))
  }

  def clear(): Unit = {
    brokerStateMap.clear()
    pendingTxnMap.clear()
  }

  def removeStateForPartition(partition: Int): mutable.Iterable[Long] = {
    brokerStateMap.foreach { case(_, brokerQueue) =>
      brokerQueue.removeRequestsForPartition(partition)
    }
    pendingTxnMap.filter { case (key: PendingTxnKey, _) => key.txnTopicPartition == partition }
      .map { case (key: PendingTxnKey, _) =>
        pendingTxnMap.remove(key)
        key.producerId
      }
  }

}
