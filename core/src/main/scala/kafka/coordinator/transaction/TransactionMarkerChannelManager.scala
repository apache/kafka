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


import kafka.coordinator.transaction.TransactionMarkerChannelManager.{LogAppendRetryQueueSizeMetricName, MetricNames, UnknownDestinationQueueSizeMetricName}

import java.util
import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, LinkedBlockingQueue}
import kafka.server.{KafkaConfig, MetadataCache, RequestLocal}
import kafka.utils.Implicits._
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.clients._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkersRequest}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{Node, Reconfigurable, TopicPartition}
import org.apache.kafka.server.common.MetadataVersion.IBP_2_8_IV0
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.{InterBrokerSendThread, RequestAndCompletionHandler}

import scala.collection.{concurrent, immutable}
import scala.jdk.CollectionConverters._

object TransactionMarkerChannelManager {
  private val UnknownDestinationQueueSizeMetricName = "UnknownDestinationQueueSize"
  private val LogAppendRetryQueueSizeMetricName = "LogAppendRetryQueueSize"

  // Visible for testing
  private[transaction] val MetricNames = Set(
    UnknownDestinationQueueSizeMetricName,
    LogAppendRetryQueueSizeMetricName
  )

  def apply(config: KafkaConfig,
            metrics: Metrics,
            metadataCache: MetadataCache,
            txnStateManager: TransactionStateManager,
            time: Time,
            logContext: LogContext): TransactionMarkerChannelManager = {
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      config.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      config,
      config.interBrokerListenerName,
      config.saslMechanismInterBrokerProtocol,
      time,
      config.saslInterBrokerHandshakeRequestEnable,
      logContext
    )
    channelBuilder match {
      case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
      case _ =>
    }
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      config.connectionsMaxIdleMs,
      metrics,
      time,
      "txn-marker-channel",
      Map.empty[String, String].asJava,
      false,
      channelBuilder,
      logContext
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
      config.connectionSetupTimeoutMs,
      config.connectionSetupTimeoutMaxMs,
      time,
      false,
      new ApiVersions,
      logContext
    )

    new TransactionMarkerChannelManager(config,
      metadataCache,
      networkClient,
      txnStateManager,
      time
    )
  }

}

class TxnMarkerQueue(@volatile var destination: Node) {

  // keep track of the requests per txn topic partition so we can easily clear the queue
  // during partition emigration
  private val markersPerTxnTopicPartition = new ConcurrentHashMap[Int, BlockingQueue[TxnIdAndMarkerEntry]]().asScala

  def removeMarkersForTxnTopicPartition(partition: Int): Option[BlockingQueue[TxnIdAndMarkerEntry]] = {
    markersPerTxnTopicPartition.remove(partition)
  }

  def addMarkers(txnTopicPartition: Int, txnIdAndMarker: TxnIdAndMarkerEntry): Unit = {
    val queue = CoreUtils.atomicGetOrUpdate(markersPerTxnTopicPartition, txnTopicPartition,
        new LinkedBlockingQueue[TxnIdAndMarkerEntry]())
    queue.add(txnIdAndMarker)
  }

  def forEachTxnTopicPartition[B](f:(Int, BlockingQueue[TxnIdAndMarkerEntry]) => B): Unit =
    markersPerTxnTopicPartition.forKeyValue { (partition, queue) =>
      if (!queue.isEmpty) f(partition, queue)
    }

  def totalNumMarkers: Int = markersPerTxnTopicPartition.values.foldLeft(0) { _ + _.size }

  // visible for testing
  def totalNumMarkers(txnTopicPartition: Int): Int = markersPerTxnTopicPartition.get(txnTopicPartition).fold(0)(_.size)
}

class TransactionMarkerChannelManager(
  config: KafkaConfig,
  metadataCache: MetadataCache,
  networkClient: NetworkClient,
  txnStateManager: TransactionStateManager,
  time: Time
) extends InterBrokerSendThread("TxnMarkerSenderThread-" + config.brokerId, networkClient, config.requestTimeoutMs, time)
  with Logging {

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  this.logIdent = "[Transaction Marker Channel Manager " + config.brokerId + "]: "

  private val interBrokerListenerName: ListenerName = config.interBrokerListenerName

  private val markersQueuePerBroker: concurrent.Map[Int, TxnMarkerQueue] = new ConcurrentHashMap[Int, TxnMarkerQueue]().asScala

  private val markersQueueForUnknownBroker = new TxnMarkerQueue(Node.noNode)

  private val txnLogAppendRetryQueue = new LinkedBlockingQueue[PendingCompleteTxn]()

  private val transactionsWithPendingMarkers = new ConcurrentHashMap[String, PendingCompleteTxn]

  val writeTxnMarkersRequestVersion: Short =
    if (config.interBrokerProtocolVersion.isAtLeast(IBP_2_8_IV0)) 1
    else 0

  metricsGroup.newGauge(UnknownDestinationQueueSizeMetricName, () => markersQueueForUnknownBroker.totalNumMarkers)
  metricsGroup.newGauge(LogAppendRetryQueueSizeMetricName, () => txnLogAppendRetryQueue.size)

  override def shutdown(): Unit = {
    try {
      super.shutdown()
      markersQueuePerBroker.clear()
    } finally {
      removeMetrics()
    }
  }

  private def removeMetrics(): Unit = {
    MetricNames.foreach(metricsGroup.removeMetric(_))
  }

  // visible for testing
  private[transaction] def queueForBroker(brokerId: Int) = {
    markersQueuePerBroker.get(brokerId)
  }

  // visible for testing
  private[transaction] def queueForUnknownBroker = markersQueueForUnknownBroker

  private[transaction] def addMarkersForBroker(broker: Node, txnTopicPartition: Int, txnIdAndMarker: TxnIdAndMarkerEntry): Unit = {
    val brokerId = broker.id

    // we do not synchronize on the update of the broker node with the enqueuing,
    // since even if there is a race condition we will just retry
    val brokerRequestQueue = CoreUtils.atomicGetOrUpdate(markersQueuePerBroker, brokerId,
        new TxnMarkerQueue(broker))
    brokerRequestQueue.destination = broker
    brokerRequestQueue.addMarkers(txnTopicPartition, txnIdAndMarker)

    trace(s"Added marker ${txnIdAndMarker.txnMarkerEntry} for transactional id ${txnIdAndMarker.txnId} to destination broker $brokerId")
  }

  def retryLogAppends(): Unit = {
    val txnLogAppendRetries: util.List[PendingCompleteTxn] = new util.ArrayList[PendingCompleteTxn]()
    txnLogAppendRetryQueue.drainTo(txnLogAppendRetries)
    txnLogAppendRetries.forEach { txnLogAppend =>
      debug(s"Retry appending $txnLogAppend transaction log")
      tryAppendToLog(txnLogAppend)
    }
  }

  override def generateRequests(): util.Collection[RequestAndCompletionHandler] = {
    retryLogAppends()
    val txnIdAndMarkerEntries: util.List[TxnIdAndMarkerEntry] = new util.ArrayList[TxnIdAndMarkerEntry]()
    markersQueueForUnknownBroker.forEachTxnTopicPartition { case (_, queue) =>
      queue.drainTo(txnIdAndMarkerEntries)
    }

    for (txnIdAndMarker: TxnIdAndMarkerEntry <- txnIdAndMarkerEntries.asScala) {
      val transactionalId = txnIdAndMarker.txnId
      val producerId = txnIdAndMarker.txnMarkerEntry.producerId
      val producerEpoch = txnIdAndMarker.txnMarkerEntry.producerEpoch
      val txnResult = txnIdAndMarker.txnMarkerEntry.transactionResult
      val coordinatorEpoch = txnIdAndMarker.txnMarkerEntry.coordinatorEpoch
      val topicPartitions = txnIdAndMarker.txnMarkerEntry.partitions.asScala.toSet

      addTxnMarkersToBrokerQueue(transactionalId, producerId, producerEpoch, txnResult, coordinatorEpoch, topicPartitions)
    }

    val currentTimeMs = time.milliseconds()
    markersQueuePerBroker.values.map { brokerRequestQueue =>
      val txnIdAndMarkerEntries = new util.ArrayList[TxnIdAndMarkerEntry]()
      brokerRequestQueue.forEachTxnTopicPartition { case (_, queue) =>
        queue.drainTo(txnIdAndMarkerEntries)
      }
      (brokerRequestQueue.destination, txnIdAndMarkerEntries)
    }.filter { case (_, entries) => !entries.isEmpty }.map { case (node, entries) =>
      val markersToSend = entries.asScala.map(_.txnMarkerEntry).asJava
      val requestCompletionHandler = new TransactionMarkerRequestCompletionHandler(node.id, txnStateManager, this, entries)
      val request = new WriteTxnMarkersRequest.Builder(writeTxnMarkersRequestVersion, markersToSend)

      new RequestAndCompletionHandler(
        currentTimeMs,
        node,
        request,
        requestCompletionHandler
      )
    }.asJavaCollection
  }

  private def writeTxnCompletion(pendingCompleteTxn: PendingCompleteTxn): Unit = {
    val transactionalId = pendingCompleteTxn.transactionalId
    val txnMetadata = pendingCompleteTxn.txnMetadata
    val newMetadata = pendingCompleteTxn.newMetadata
    val coordinatorEpoch = pendingCompleteTxn.coordinatorEpoch

    trace(s"Completed sending transaction markers for $transactionalId; begin transition " +
      s"to ${newMetadata.txnState}")

    txnStateManager.getTransactionState(transactionalId) match {
      case Left(Errors.NOT_COORDINATOR) =>
        info(s"No longer the coordinator for $transactionalId with coordinator epoch " +
          s"$coordinatorEpoch; cancel appending $newMetadata to transaction log")

      case Left(Errors.COORDINATOR_LOAD_IN_PROGRESS) =>
        info(s"Loading the transaction partition that contains $transactionalId while my " +
          s"current coordinator epoch is $coordinatorEpoch; so cancel appending $newMetadata to " +
          s"transaction log since the loading process will continue the remaining work")

      case Left(unexpectedError) =>
        throw new IllegalStateException(s"Unhandled error $unexpectedError when fetching current transaction state")

      case Right(Some(epochAndMetadata)) =>
        if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
          debug(s"Sending $transactionalId's transaction markers for $txnMetadata with " +
            s"coordinator epoch $coordinatorEpoch succeeded, trying to append complete transaction log now")
          tryAppendToLog(PendingCompleteTxn(transactionalId, coordinatorEpoch, txnMetadata, newMetadata))
        } else {
          info(s"The cached metadata $txnMetadata has changed to $epochAndMetadata after " +
            s"completed sending the markers with coordinator epoch $coordinatorEpoch; abort " +
            s"transiting the metadata to $newMetadata as it may have been updated by another process")
        }

      case Right(None) =>
        val errorMsg = s"The coordinator still owns the transaction partition for $transactionalId, " +
          s"but there is no metadata in the cache; this is not expected"
        fatal(errorMsg)
        throw new IllegalStateException(errorMsg)
    }
  }

  def addTxnMarkersToSend(coordinatorEpoch: Int,
                          txnResult: TransactionResult,
                          txnMetadata: TransactionMetadata,
                          newMetadata: TxnTransitMetadata): Unit = {
    val transactionalId = txnMetadata.transactionalId
    val pendingCompleteTxn = PendingCompleteTxn(
      transactionalId,
      coordinatorEpoch,
      txnMetadata,
      newMetadata)

    transactionsWithPendingMarkers.put(transactionalId, pendingCompleteTxn)
    addTxnMarkersToBrokerQueue(transactionalId, txnMetadata.producerId,
      txnMetadata.producerEpoch, txnResult, coordinatorEpoch, txnMetadata.topicPartitions.toSet)
    maybeWriteTxnCompletion(transactionalId)
  }

  def numTxnsWithPendingMarkers: Int = transactionsWithPendingMarkers.size

  private def hasPendingMarkersToWrite(txnMetadata: TransactionMetadata): Boolean = {
    txnMetadata.inLock {
      txnMetadata.topicPartitions.nonEmpty
    }
  }

  def maybeWriteTxnCompletion(transactionalId: String): Unit = {
    Option(transactionsWithPendingMarkers.get(transactionalId)).foreach { pendingCompleteTxn =>
      if (!hasPendingMarkersToWrite(pendingCompleteTxn.txnMetadata) &&
          transactionsWithPendingMarkers.remove(transactionalId, pendingCompleteTxn)) {
        writeTxnCompletion(pendingCompleteTxn)
      }
    }
  }

  private def tryAppendToLog(txnLogAppend: PendingCompleteTxn): Unit = {
    // try to append to the transaction log
    def appendCallback(error: Errors): Unit =
      error match {
        case Errors.NONE =>
          trace(s"Completed transaction for ${txnLogAppend.transactionalId} with coordinator epoch ${txnLogAppend.coordinatorEpoch}, final state after commit: ${txnLogAppend.txnMetadata.state}")

        case Errors.NOT_COORDINATOR =>
          info(s"No longer the coordinator for transactionalId: ${txnLogAppend.transactionalId} while trying to append to transaction log, skip writing to transaction log")

        case Errors.COORDINATOR_NOT_AVAILABLE =>
          info(s"Not available to append $txnLogAppend: possible causes include ${Errors.UNKNOWN_TOPIC_OR_PARTITION}, ${Errors.NOT_ENOUGH_REPLICAS}, " +
            s"${Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND} and ${Errors.REQUEST_TIMED_OUT}; retry appending")

          // enqueue for retry
          txnLogAppendRetryQueue.add(txnLogAppend)

        case Errors.COORDINATOR_LOAD_IN_PROGRESS =>
          info(s"Coordinator is loading the partition ${txnStateManager.partitionFor(txnLogAppend.transactionalId)} and hence cannot complete append of $txnLogAppend; " +
            s"skip writing to transaction log as the loading process should complete it")

        case other: Errors =>
          val errorMsg = s"Unexpected error ${other.exceptionName} while appending to transaction log for ${txnLogAppend.transactionalId}"
          fatal(errorMsg)
          throw new IllegalStateException(errorMsg)
      }

    txnStateManager.appendTransactionToLog(txnLogAppend.transactionalId, txnLogAppend.coordinatorEpoch,
      txnLogAppend.newMetadata, appendCallback, _ == Errors.COORDINATOR_NOT_AVAILABLE, RequestLocal.NoCaching)
  }

  def addTxnMarkersToBrokerQueue(transactionalId: String,
                                 producerId: Long,
                                 producerEpoch: Short,
                                 result: TransactionResult,
                                 coordinatorEpoch: Int,
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
              transactionsWithPendingMarkers.remove(transactionalId)

            case Right(Some(epochAndMetadata)) =>
              if (epochAndMetadata.coordinatorEpoch != coordinatorEpoch) {
                info(s"The cached metadata has changed to $epochAndMetadata (old coordinator epoch is $coordinatorEpoch) since preparing to send markers; cancel sending markers to its partition leaders")
                transactionsWithPendingMarkers.remove(transactionalId)
              } else {
                // if the leader of the partition is unknown, skip sending the txn marker since
                // the partition is likely to be deleted already
                info(s"Couldn't find leader endpoint for partitions $topicPartitions while trying to send transaction markers for " +
                  s"$transactionalId, these partitions are likely deleted already and hence can be skipped")

                val txnMetadata = epochAndMetadata.transactionMetadata

                txnMetadata.inLock {
                  topicPartitions.foreach(txnMetadata.removePartition)
                }

                maybeWriteTxnCompletion(transactionalId)
              }

            case Right(None) =>
              val errorMsg = s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                s"no metadata in the cache; this is not expected"
              fatal(errorMsg)
              throw new IllegalStateException(errorMsg)

          }
      }
    }

    wakeup()
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
    transactionsWithPendingMarkers.remove(transactionalId)
  }
}

case class TxnIdAndMarkerEntry(txnId: String, txnMarkerEntry: TxnMarkerEntry)

case class PendingCompleteTxn(transactionalId: String,
                              coordinatorEpoch: Int,
                              txnMetadata: TransactionMetadata,
                              newMetadata: TxnTransitMetadata) {

  override def toString: String = {
    "PendingCompleteTxn(" +
      s"transactionalId=$transactionalId, " +
      s"coordinatorEpoch=$coordinatorEpoch, " +
      s"txnMetadata=$txnMetadata, " +
      s"newMetadata=$newMetadata)"
  }
}
