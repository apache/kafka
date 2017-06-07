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
import java.util.Collections
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.metrics.{Metrics, Sensor}
import org.apache.kafka.common.metrics.stats.Total
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
      time,
      metrics
    )
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

  def totalNumMarkers(): Int = markersPerTxnTopicPartition.map { case(_, queue) => queue.size()}.sum

  // visible for testing
  def totalNumMarkers(txnTopicPartition: Int): Int = markersPerTxnTopicPartition.get(txnTopicPartition).fold(0)(_.size())
}

class TransactionMarkerChannelManager(config: KafkaConfig,
                                      metadataCache: MetadataCache,
                                      networkClient: NetworkClient,
                                      txnStateManager: TransactionStateManager,
                                      txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker],
                                      time: Time,
                                      metrics:Metrics) extends Logging {

  private def addQueueLengthMetric(sensor: Sensor, namePrefix: String, description: String): Unit = {
    sensor.add(metrics.metricName(s"$namePrefix-queue-length",
      "transaction-marker-channel-metrics",
      description,
      Collections.singletonMap("broker-id",config.brokerId.toString)),
      new Total())
  }

  private val markersQueuePerBroker: concurrent.Map[Int, TxnMarkerQueue] = concurrent.TrieMap.empty[Int, TxnMarkerQueue]

  private val markersQueueForUnknownBroker = new TxnMarkerQueue(Node.noNode)
  private val unknownBrokerQueueSensor = metrics.sensor("unknown-broker-queue")
  addQueueLengthMetric(unknownBrokerQueueSensor, "unknown-broker", "the number of WriteTxnMarker requests with unknown brokers")

  private val interBrokerListenerName: ListenerName = config.interBrokerListenerName

  private val txnMarkerSendThread: InterBrokerSendThread = {
    new InterBrokerSendThread("TxnMarkerSenderThread-" + config.brokerId, networkClient, drainQueuedTransactionMarkers, time)
  }

  private val txnLogAppendRetryQueue = new LinkedBlockingQueue[TxnLogAppend]()

  private val txnLogAppendRetryQueueSensor = metrics.sensor("txn-log-append-retry-queue")
  addQueueLengthMetric(txnLogAppendRetryQueueSensor, "txn-log-append-retry", "the number of txn log appends that need to be retried")

  def start(): Unit = {
    txnMarkerSendThread.start()
  }

  def shutdown(): Unit = {
    txnMarkerSendThread.initiateShutdown()
    // wake up the thread in case it is blocked inside poll
    networkClient.wakeup()
    txnMarkerSendThread.awaitShutdown()
    txnMarkerPurgatory.shutdown()
    markersQueuePerBroker.clear()
  }

  // visible for testing
  private[transaction] def queueForBroker(brokerId: Int) = {
    markersQueuePerBroker.get(brokerId)
  }

  // visible for testing
  private[transaction] def queueForUnknownBroker = markersQueueForUnknownBroker

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

  def retryLogAppends(): Unit = {
    val txnLogAppendRetries: java.util.List[TxnLogAppend] = new util.ArrayList[TxnLogAppend]()
    txnLogAppendRetryQueue.drainTo(txnLogAppendRetries)
    txnLogAppendRetryQueueSensor.record(-txnLogAppendRetries.size())
    debug(s"retrying: ${txnLogAppendRetries.size} transaction log appends")
    txnLogAppendRetries.asScala.foreach { txnLogAppend =>
      tryAppendToLog(txnLogAppend)
    }
  }


  private[transaction] def drainQueuedTransactionMarkers(): Iterable[RequestAndCompletionHandler] = {
    retryLogAppends()
    val txnIdAndMarkerEntries: java.util.List[TxnIdAndMarkerEntry] = new util.ArrayList[TxnIdAndMarkerEntry]()
    markersQueueForUnknownBroker.forEachTxnTopicPartition { case (_, queue) =>
      queue.drainTo(txnIdAndMarkerEntries)
    }

    unknownBrokerQueueSensor.record(-txnIdAndMarkerEntries.size())
    for (txnIdAndMarker: TxnIdAndMarkerEntry <- txnIdAndMarkerEntries.asScala) {
      val transactionalId = txnIdAndMarker.txnId
      val producerId = txnIdAndMarker.txnMarkerEntry.producerId
      val producerEpoch = txnIdAndMarker.txnMarkerEntry.producerEpoch
      val txnResult = txnIdAndMarker.txnMarkerEntry.transactionResult
      val coordinatorEpoch = txnIdAndMarker.txnMarkerEntry.coordinatorEpoch
      val topicPartitions = txnIdAndMarker.txnMarkerEntry.partitions.asScala.toSet

      addTxnMarkersToBrokerQueue(transactionalId, producerId, producerEpoch, txnResult, coordinatorEpoch, topicPartitions)
    }

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
                          newMetadata: TxnTransitMetadata): Unit = {

    def appendToLogCallback(error: Errors): Unit = {
      error match {
        case Errors.NONE =>
          trace(s"Completed sending transaction markers for $transactionalId as $txnResult")

          txnStateManager.getTransactionState(transactionalId) match {
            case Left(Errors.NOT_COORDINATOR) =>
              info(s"I am no longer the coordinator for $transactionalId with coordinator epoch $coordinatorEpoch; cancel appending $newMetadata to transaction log")

            case Left(Errors.COORDINATOR_LOAD_IN_PROGRESS) =>
              info(s"I am loading the transaction partition that contains $transactionalId while my current coordinator epoch is $coordinatorEpoch; " +
                s"so appending $newMetadata to transaction log since the loading process will continue the left work")

            case Right(Some(epochAndMetadata)) =>
              if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                debug(s"Updating $transactionalId's transaction state to $txnMetadata with coordinator epoch $coordinatorEpoch for $transactionalId succeeded")

                tryAppendToLog(TxnLogAppend(transactionalId, coordinatorEpoch, txnMetadata, newMetadata))
              } else {
                info(s"Updating $transactionalId's transaction state to $txnMetadata with coordinator epoch $coordinatorEpoch for $transactionalId failed after the transaction markers " +
                  s"has been sent to brokers. The cached metadata have been changed to $epochAndMetadata since preparing to send markers")
              }

            case Right(None) =>
              throw new IllegalStateException(s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                s"no metadata in the cache; this is not expected")
          }

        case other =>
          throw new IllegalStateException(s"Unexpected error ${other.exceptionName} before appending to txn log for $transactionalId")
      }
    }

    val delayedTxnMarker = new DelayedTxnMarker(txnMetadata, appendToLogCallback)
    txnMarkerPurgatory.tryCompleteElseWatch(delayedTxnMarker, Seq(transactionalId))

    addTxnMarkersToBrokerQueue(transactionalId, txnMetadata.producerId, txnMetadata.producerEpoch, txnResult, coordinatorEpoch, txnMetadata.topicPartitions.toSet)
  }

  private def tryAppendToLog(txnLogAppend: TxnLogAppend) = {
    // try to append to the transaction log
    def retryAppendCallback(error: Errors): Unit =
      error match {
        case Errors.NONE =>
          trace(s"Completed transaction for ${txnLogAppend.transactionalId} with coordinator epoch ${txnLogAppend.coordinatorEpoch}, final state: state after commit: ${txnLogAppend.txnMetadata.state}")

        case Errors.NOT_COORDINATOR =>
          info(s"No longer the coordinator for transactionalId: ${txnLogAppend.transactionalId} while trying to append to transaction log, skip writing to transaction log")

        case Errors.COORDINATOR_NOT_AVAILABLE =>
          warn(s"Failed updating transaction state for ${txnLogAppend.transactionalId} when appending to transaction log due to ${error.exceptionName}. retrying")
          txnLogAppendRetryQueueSensor.record(1)
          // enqueue for retry
          txnLogAppendRetryQueue.add(txnLogAppend)

        case errors: Errors =>
          throw new IllegalStateException(s"Unexpected error ${errors.exceptionName} while appending to transaction log for ${txnLogAppend.transactionalId}")
      }

    txnStateManager.appendTransactionToLog(txnLogAppend.transactionalId, txnLogAppend.coordinatorEpoch, txnLogAppend.newMetadata, retryAppendCallback)
  }

  def addTxnMarkersToBrokerQueue(transactionalId: String, producerId: Long, producerEpoch: Short,
                                 result: TransactionResult, coordinatorEpoch: Int,
                                 topicPartitions: immutable.Set[TopicPartition]): Unit = {
    val txnTopicPartition = txnStateManager.partitionFor(transactionalId)
    val partitionsByDestination: immutable.Map[Option[Node], immutable.Set[TopicPartition]] = topicPartitions.groupBy { topicPartition: TopicPartition =>
      metadataCache.getPartitionLeaderEndpoint(topicPartition.topic, topicPartition.partition, interBrokerListenerName)
    }

    for ((broker: Option[Node], topicPartitions: immutable.Set[TopicPartition]) <- partitionsByDestination) {
      broker match {
        case Some(brokerNode) =>
          val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, result, topicPartitions.toList.asJava)
          val txnIdAndMarker = TxnIdAndMarkerEntry(transactionalId, marker)

          if (brokerNode == Node.noNode) {
            unknownBrokerQueueSensor.record(1)
            // if the leader of the partition is known but node not available, put it into an unknown broker queue
            // and let the sender thread to look for its broker and migrate them later
            markersQueueForUnknownBroker.addMarkers(txnTopicPartition, txnIdAndMarker)
          } else {
            addMarkersForBroker(brokerNode, txnTopicPartition, txnIdAndMarker)
          }

        case None =>
          txnStateManager.getTransactionState(transactionalId) match {
            case Left(error) =>
              info(s"Encountered $error trying to fetch transaction metadata for $transactionalId with coordinator epoch $coordinatorEpoch; cancel sending markers to its partition leaders")
              txnMarkerPurgatory.cancelForKey(transactionalId)

            case Right(Some(epochAndMetadata)) =>
              if (epochAndMetadata.coordinatorEpoch != coordinatorEpoch) {
                info(s"The cached metadata has changed to $epochAndMetadata (old coordinator epoch is $coordinatorEpoch) since preparing to send markers; cancel sending markers to its partition leaders")
                txnMarkerPurgatory.cancelForKey(transactionalId)
              } else {
                // if the leader of the partition is unknown, skip sending the txn marker since
                // the partition is likely to be deleted already
                info(s"Couldn't find leader endpoint for partitions $topicPartitions while trying to send transaction markers for " +
                  s"$transactionalId, these partitions are likely deleted already and hence can be skipped")

                val txnMetadata = epochAndMetadata.transactionMetadata

                txnMetadata synchronized {
                  topicPartitions.foreach(txnMetadata.removePartition)
                }

                txnMarkerPurgatory.checkAndComplete(transactionalId)
              }

            case Right(None) =>
              throw new IllegalStateException(s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                s"no metadata in the cache; this is not expected")
          }
      }
    }

    networkClient.wakeup()
  }

  def removeMarkersForTxnTopicPartition(txnTopicPartitionId: Int): Unit = {
    markersQueueForUnknownBroker.removeMarkersForTxnTopicPartition(txnTopicPartitionId).foreach { queue =>
      for (entry: TxnIdAndMarkerEntry <- queue.asScala)
        removeMarkersForTxnId(entry.txnId)
    }

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

  def completeSendMarkersForTxnId(transactionalId: String): Unit = {
    txnMarkerPurgatory.checkAndComplete(transactionalId)
  }
}

case class TxnIdAndMarkerEntry(txnId: String, txnMarkerEntry: TxnMarkerEntry)

case class TxnLogAppend(transactionalId: String, coordinatorEpoch: Int, txnMetadata: TransactionMetadata, newMetadata: TxnTransitMetadata)
