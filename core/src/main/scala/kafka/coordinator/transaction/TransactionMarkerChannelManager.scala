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

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkersRequest}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry

import collection.JavaConverters._
import scala.collection.{concurrent, immutable, mutable}

object TransactionMarkerChannelManager {
  def apply(config: KafkaConfig,
            metrics: Metrics,
            metadataCache: MetadataCache,
            txnStateManager: TransactionStateManager,
            txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker],
            time: Time): TransactionMarkerChannelManager = {

    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      config.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      config,
      config.interBrokerListenerName,
      config.saslMechanismInterBrokerProtocol,
      config.saslInterBrokerHandshakeRequestEnable
    )
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      config.connectionsMaxIdleMs,
      metrics,
      time,
      "txn-marker-channel",
      Map.empty[String, String].asJava,
      false,
      channelBuilder
    )
    val networkClient = new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      s"broker-${config.brokerId}-txn-marker-sender",
      1,
      50,
      50,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.socketReceiveBufferBytes,
      config.requestTimeoutMs,
      time,
      false,
      new ApiVersions
    )

    new TransactionMarkerChannelManager(config,
      metadataCache,
      networkClient,
      txnStateManager,
      txnMarkerPurgatory,
      time)
  }

  private[transaction] def requestGenerator(transactionMarkerChannelManager: TransactionMarkerChannelManager): () => Iterable[RequestAndCompletionHandler] = {
    () => transactionMarkerChannelManager.drainQueuedTransactionMarkers()
  }
}

class TxnMarkerQueue(@volatile private var destination: Node) {

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

  // TODO: this function is only for metrics recording, not yet added
  def totalNumMarkers(): Int = markersPerTxnTopicPartition.map { case(_, queue) => queue.size()}.sum

  // visible for testing
  def totalNumMarkers(txnTopicPartition: Int): Int = markersPerTxnTopicPartition.get(txnTopicPartition).fold(0)(_.size())
}

class TransactionMarkerChannelManager(config: KafkaConfig,
                                      metadataCache: MetadataCache,
                                      networkClient: NetworkClient,
                                      txnStateManager: TransactionStateManager,
                                      txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker],
                                      time: Time) extends Logging {

  private val markersQueuePerBroker: concurrent.Map[Int, TxnMarkerQueue] = concurrent.TrieMap.empty[Int, TxnMarkerQueue]

  private val interBrokerListenerName: ListenerName = config.interBrokerListenerName

  // TODO: What is reasonable for this
  private val brokerNotAliveBackoffMs = 10

  private val txnMarkerSendThread: InterBrokerSendThread = {
    new InterBrokerSendThread("TxnMarkerSenderThread-" + config.brokerId, networkClient, drainQueuedTransactionMarkers, time)
  }

  def start(): Unit = {
    txnMarkerSendThread.start()
    networkClient.wakeup()    // FIXME: is this really required?
  }

  def shutdown(): Unit = {
    txnMarkerSendThread.shutdown()
    markersQueuePerBroker.clear()
  }

  // visible for testing
  private[transaction] def queueForBroker(brokerId: Int) = {
    markersQueuePerBroker.get(brokerId)
  }

  // visible for testing
  private[transaction] def senderThread = txnMarkerSendThread

  private[transaction] def addMarkersForBroker(broker: Node, txnTopicPartition: Int, txnIdAndMarker: TxnIdAndMarkerEntry) {
    val brokerId = broker.id

    // we do not synchronize on the update of the broker node with the enqueuing,
    // since even if there is a race condition we will just retry
    val brokerRequestQueue = markersQueuePerBroker.getOrElseUpdate(brokerId, new TxnMarkerQueue(broker))
    brokerRequestQueue.maybeUpdateNode(broker)
    brokerRequestQueue.addMarkers(txnTopicPartition, txnIdAndMarker)

    trace(s"Added marker ${txnIdAndMarker.txnMarkerEntry} for transactional id ${txnIdAndMarker.txnId} to destination broker $brokerId")
  }

  private[transaction] def drainQueuedTransactionMarkers(): Iterable[RequestAndCompletionHandler] = {
    markersQueuePerBroker.map { case (brokerId: Int, brokerRequestQueue: TxnMarkerQueue) =>
      val txnIdAndMarkerEntries: java.util.List[TxnIdAndMarkerEntry] = new util.ArrayList[TxnIdAndMarkerEntry]()
      brokerRequestQueue.forEachTxnTopicPartition { case (_, queue) =>
        queue.drainTo(txnIdAndMarkerEntries)
      }
      (brokerRequestQueue.node, txnIdAndMarkerEntries)
    }
      .filter { case (_, entries) => !entries.isEmpty}
      .map { case (node, entries) =>
        val markersToSend: java.util.List[TxnMarkerEntry] = entries.asScala.map(_.txnMarkerEntry).asJava
        val requestCompletionHandler = new TransactionMarkerRequestCompletionHandler(node.id, txnStateManager, this, entries)
        RequestAndCompletionHandler(node, new WriteTxnMarkersRequest.Builder(markersToSend), requestCompletionHandler)
      }
  }

  def addTxnMarkersToSend(transactionalId: String,
                          coordinatorEpoch: Int,
                          txnResult: TransactionResult,
                          txnMetadata: TransactionMetadata,
                          newMetadata: TransactionMetadataTransition): Unit = {

    def appendToLogCallback(error: Errors): Unit = {
      error match {
        case Errors.NONE =>
          trace(s"Completed sending transaction markers for $transactionalId as $txnResult")

          txnStateManager.getTransactionState(transactionalId) match {
            case Some(epochAndMetadata) =>
              if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                debug(s"Updating $transactionalId's transaction state to $txnMetadata with coordinator epoch $coordinatorEpoch for $transactionalId succeeded")

                // try to append to the transaction log
                def retryAppendCallback(error: Errors): Unit =
                  error match {
                    case Errors.NONE =>
                      trace(s"Completed transaction for $transactionalId with coordinator epoch $coordinatorEpoch, final state: state after commit: ${txnMetadata.state}")

                    case Errors.NOT_COORDINATOR =>
                      info(s"No longer the coordinator for transactionalId: $transactionalId while trying to append to transaction log, skip writing to transaction log")

                    case Errors.COORDINATOR_NOT_AVAILABLE =>
                      warn(s"Failed updating transaction state for $transactionalId when appending to transaction log due to ${error.exceptionName}. retrying")

                      // retry appending
                      txnStateManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, retryAppendCallback)

                    case errors: Errors =>
                      throw new IllegalStateException(s"Unexpected error ${errors.exceptionName} while appending to transaction log for $transactionalId")
                  }

                txnStateManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, retryAppendCallback)
              } else {
                info(s"Updating $transactionalId's transaction state to $txnMetadata with coordinator epoch $coordinatorEpoch for $transactionalId failed after the transaction markers " +
                  s"has been sent to brokers. The cached metadata have been changed to $epochAndMetadata since preparing to send markers")
              }

            case None =>
              // this transactional id no longer exists, maybe the corresponding partition has already been migrated out.
              // we will stop appending the completed log entry to transaction topic as the new leader should be doing it.
              info(s"Updating $transactionalId's transaction state (txn topic partition ${txnStateManager.partitionFor(transactionalId)}) to $newMetadata with coordinator epoch $coordinatorEpoch for $transactionalId " +
                s"failed after the transaction message has been appended to the log since the corresponding metadata does not exist in the cache anymore")
          }

        case other =>
          throw new IllegalStateException(s"Unexpected error ${other.exceptionName} before appending to txn log for $transactionalId")
      }
    }

    // watch for both the transactional id and the transaction topic partition id,
    // so we can cancel all the delayed operations for the same partition id;
    // NOTE this is only possible because the hashcode of Int / String never overlaps

    // TODO: if the delayed txn marker will always have infinite timeout, we can replace it with a map
    val delayedTxnMarker = new DelayedTxnMarker(txnMetadata, appendToLogCallback)
    val txnTopicPartition = txnStateManager.partitionFor(transactionalId)
    txnMarkerPurgatory.tryCompleteElseWatch(delayedTxnMarker, Seq(transactionalId, txnTopicPartition))

    addTxnMarkersToBrokerQueue(transactionalId, txnMetadata.producerId, txnMetadata.producerEpoch, txnResult, coordinatorEpoch, txnMetadata.topicPartitions.toSet)
  }

  def addTxnMarkersToBrokerQueue(transactionalId: String, producerId: Long, producerEpoch: Short,
                                 result: TransactionResult, coordinatorEpoch: Int,
                                 topicPartitions: immutable.Set[TopicPartition]): Unit = {
    val txnTopicPartition = txnStateManager.partitionFor(transactionalId)
    val partitionsByDestination: immutable.Map[Node, immutable.Set[TopicPartition]] = topicPartitions.groupBy { topicPartition: TopicPartition =>
      var brokerNode: Option[Node] = None

      // TODO: instead of retry until succeed, we can first put it into an unknown broker queue and let the sender thread to look for its broker and migrate them
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
      val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, result, topicPartitions.toList.asJava)
      val txnIdAndMarker = TxnIdAndMarkerEntry(transactionalId, marker)
      addMarkersForBroker(broker, txnTopicPartition, txnIdAndMarker)
    }

    networkClient.wakeup()
  }

  def removeMarkersForTxnTopicPartition(txnTopicPartitionId: Int): Unit = {
    txnMarkerPurgatory.cancelForKey(txnTopicPartitionId)
    markersQueuePerBroker.foreach { case(_, brokerQueue) =>
      brokerQueue.removeMarkersForTxnTopicPartition(txnTopicPartitionId).foreach { queue =>
        for (entry: TxnIdAndMarkerEntry <- queue.asScala)
          removeMarkersForTxnId(entry.txnId)
      }
    }
  }

  def removeMarkersForTxnId(transactionalId: String): Unit = {
    // we do not need to clear the queue since it should have
    // already been drained by the sender thread
    txnMarkerPurgatory.cancelForKey(transactionalId)
  }

  // FIXME: Currently, operations registered under partition in txnMarkerPurgatory
  // are only cleaned during coordinator immigration, which happens rarely. This means potential memory leak
  def completeSendMarkersForTxnId(transactionalId: String): Unit = {
    txnMarkerPurgatory.checkAndComplete(transactionalId)
  }
}

case class TxnIdAndMarkerEntry(txnId: String, txnMarkerEntry: TxnMarkerEntry)
