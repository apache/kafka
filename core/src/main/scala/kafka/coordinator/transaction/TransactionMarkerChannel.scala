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

import kafka.common.RequestAndCompletionHandler
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
                               txnStateManager: TransactionStateManager,
                               metadataCache: MetadataCache,
                               networkClient: NetworkClient,
                               time: Time) extends Logging {

  class BrokerRequestQueue(private var destination: Node) {

    // keep track of the requests per txn topic partition so we can easily clear the queue
    // during partition emigration
    private val markersPerTxnTopicPartition: concurrent.Map[Int, BlockingQueue[TxnIdAndMarkerEntry]]
      = concurrent.TrieMap.empty[Int, BlockingQueue[TxnIdAndMarkerEntry]]

    def removeMarkersForTxnTopicPartition(partition: Int): Option[BlockingQueue[TxnIdAndMarkerEntry]] = {
      markersPerTxnTopicPartition.remove(partition)
    }

    def maybeUpdateNode(node: Node): Unit = {
      destination = node
    }

    def addMarkers(txnTopicPartition: Int, txnIdAndMarker: TxnIdAndMarkerEntry): Unit = {
      val queue = markersPerTxnTopicPartition.getOrElseUpdate(txnTopicPartition, new LinkedBlockingQueue[TxnIdAndMarkerEntry]())
      queue.add(txnIdAndMarker)
    }

    def forEachTxnTopicPartition[B](f:(Int, BlockingQueue[TxnIdAndMarkerEntry]) => B): mutable.Iterable[B] =
      markersPerTxnTopicPartition.filter { case(_, queue) => !queue.isEmpty }
                                 .map { case(partition:Int, queue:BlockingQueue[TxnIdAndMarkerEntry]) => f(partition, queue) }

    def node: Node = destination

    def totalQueuedRequests(): Int = markersPerTxnTopicPartition.map { case(_, queue) => queue.size()}.sum
  }

  private val brokerStateMap: concurrent.Map[Int, BrokerRequestQueue] = concurrent.TrieMap.empty[Int, BrokerRequestQueue]

  // TODO: What is reasonable for this
  private val brokerNotAliveBackoffMs = 10

  // visible for testing
  private[transaction] def queueForBroker(brokerId: Int) = {
    brokerStateMap.get(brokerId)
  }

  private[transaction] def addMarkersForBroker(broker: Node, txnTopicPartition: Int, txnIdAndMarker: TxnIdAndMarkerEntry) {
    val brokerId = broker.id

    // we do not synchronize on the update of the broker node with the enqueuing,
    // since even if there is a race condition we will just retry
    val brokerRequestQueue = brokerStateMap.getOrElseUpdate(brokerId, new BrokerRequestQueue(broker))
    brokerRequestQueue.maybeUpdateNode(broker)
    brokerRequestQueue.addMarkers(txnTopicPartition, txnIdAndMarker)

    trace(s"Added marker ${txnIdAndMarker.txnMarkerEntry} for transactional id ${txnIdAndMarker.txnId} to destination broker $brokerId")
  }

  private[transaction] def drainQueuedTransactionMarkers(txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker]): Iterable[RequestAndCompletionHandler] = {
    brokerStateMap.flatMap { case (brokerId: Int, brokerRequestQueue: BrokerRequestQueue) =>
      brokerRequestQueue.forEachTxnTopicPartition { case(partitionId, queue) =>
        val txnIdAndMarkerEntries: java.util.List[TxnIdAndMarkerEntry] = new util.ArrayList[TxnIdAndMarkerEntry]()
        queue.drainTo(txnIdAndMarkerEntries)
        val markersToSend: java.util.List[TxnMarkerEntry] = txnIdAndMarkerEntries.asScala.map(_.txnMarkerEntry).asJava
        val requestCompletionHandler = new TransactionMarkerRequestCompletionHandler(brokerId, partitionId, txnStateManager, this, txnIdAndMarkerEntries, txnMarkerPurgatory)
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

  def addTxnMarkersToSend(transactionalId: String, pid: Long, epoch: Short, result: TransactionResult, coordinatorEpoch: Int, topicPartitions: immutable.Set[TopicPartition]): Unit = {
    val txnTopicPartition = txnStateManager.partitionFor(transactionalId)
    val partitionsByDestination: immutable.Map[Node, immutable.Set[TopicPartition]] = topicPartitions.groupBy { topicPartition: TopicPartition =>
      var brokerNode: Option[Node] = None

      while (brokerNode.isEmpty) {
        brokerNode = metadataCache.getPartitionLeaderEndpoint(topicPartition.topic, topicPartition.partition, interBrokerListenerName)

        if (brokerNode.isEmpty) {
          trace(s"Couldn't find leader endpoint for partition: $topicPartition, retrying.")
          time.sleep(brokerNotAliveBackoffMs)
        }
      }
      brokerNode.get
    }

    for ((broker: Node, topicPartitions: immutable.Set[TopicPartition]) <- partitionsByDestination) {
      val txnIdAndMarker = TxnIdAndMarkerEntry(transactionalId, new TxnMarkerEntry(pid, epoch, coordinatorEpoch, result, topicPartitions.toList.asJava))
      addMarkersForBroker(broker, txnTopicPartition, txnIdAndMarker)
    }

    networkClient.wakeup()
  }

  def clear(): Unit = {
    brokerStateMap.clear()
  }

  def removeMarkersForTxnTopicPartition(partition: Int): Unit = {
    brokerStateMap.foreach { case(_, brokerQueue) =>
      brokerQueue.removeMarkersForTxnTopicPartition(partition)
    }
  }
}

case class TxnIdAndMarkerEntry(txnId: String, txnMarkerEntry: TxnMarkerEntry)
