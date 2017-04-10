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

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache}
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.clients._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkerRequest, WriteTxnMarkerResponse}
import org.apache.kafka.common.requests.WriteTxnMarkerRequest.TxnMarkerEntry
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.{concurrent, immutable, mutable}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import collection.JavaConverters._
import collection.JavaConversions._

class TransactionMarkerChannelManager(config: KafkaConfig,
                                      metrics: Metrics,
                                      metadataCache: MetadataCache,
                                      scheduler: Scheduler,
                                      time: Time) extends Logging {

  this.logIdent = "[Transaction Maker Channel Manager " + config.brokerId + "]: "
  type WriteTxnMarkerCallback = Errors => Unit

  private val sendThread: InterBrokerSendThread = {
    val threadName = "TxnMarkerSenderThread-" + config.brokerId
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
      "replica-fetcher",
      Map("broker-id" -> config.brokerId.toString).asJava,
      false,
      channelBuilder
    )
    val networkClient = new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      threadName,
      1,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.socketReceiveBufferBytes,
      config.requestTimeoutMs,
      time,
      false,
      new ApiVersions
    )

    new InterBrokerSendThread(threadName, networkClient, generateRequests, time)
  }

  private case class PendingTxn(transactionMetadata: TransactionMetadata, callback: WriteTxnMarkerCallback)

  // use finer synchronization (concurrent map + blocking queue) so that we do not need a global lock
  private val brokerStateMap: concurrent.Map[Int, DestinationBrokerAndQueuedMarkers] = concurrent.TrieMap.empty[Int, DestinationBrokerAndQueuedMarkers]

  private val pendingTxnMap: concurrent.Map[Long, PendingTxn] = concurrent.TrieMap.empty[Long, PendingTxn]

  // TODO: Config for how often this runs?
  private val CommitCompleteScheduleMs = 10

  def start(): Unit = {
    sendThread.start()
    def completedTransactions(): Unit = {
      pendingTxnMap.filter { entry =>
        entry._2.transactionMetadata.topicPartitions.isEmpty
      }.foreach { completed =>
        completed._2.callback(Errors.NONE)
        pendingTxnMap.remove(completed._1)
      }

    }

    scheduler.schedule("transaction-channel-manager",
      completedTransactions,
      delay = CommitCompleteScheduleMs,
      period = CommitCompleteScheduleMs)
  }

  def shutdown(): Unit = {
    sendThread.shutdown()
    brokerStateMap.clear()
    pendingTxnMap.clear()
  }

  private def addNewBroker(broker: Node) {
    val markersQueue = new LinkedBlockingQueue[TxnMarkerEntry]()

    brokerStateMap.put(broker.id, DestinationBrokerAndQueuedMarkers(broker, markersQueue))

    trace(s"Added destination broker ${broker.id}: $broker")
  }

  private def addRequestForBroker(brokerId: Int, txnMarker: TxnMarkerEntry) {
    val markersQueue = brokerStateMap(brokerId).markersQueue
    markersQueue.add(txnMarker)
    trace(s"Added markers $txnMarker for broker $brokerId")
  }

  private def addRequestToSend(pid: Long, epoch: Short, result: TransactionResult, coordinatorEpoch: Int, topicPartitions: immutable.Set[TopicPartition], watchInPurgatory: Boolean = true): Unit = {

    val partitionsByDestination: immutable.Map[Int, immutable.Set[TopicPartition]] = topicPartitions.groupBy { topicPartition: TopicPartition =>
      val leaderForPartition = metadataCache.getPartitionInfo(topicPartition.topic, topicPartition.partition)
      leaderForPartition match {
        case Some(partitionInfo) =>
          val brokerId = partitionInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader
          if (!brokerStateMap.contains(brokerId)) {
            val broker = metadataCache.getAliveEndpoint(brokerId, config.interBrokerListenerName).get
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
      val txnMarker = new TxnMarkerEntry(pid, epoch, coordinatorEpoch, result, topicPartitions.toList.asJava)
      addRequestForBroker(brokerId, txnMarker)
    }
  }

  def sendTxnMarkerRequest(metadata: TransactionMetadata, coordinatorEpoch: Int, completionCallback: WriteTxnMarkerCallback): Unit = {
    val metadataToWrite = metadata synchronized metadata.copy()
    val existingMetadataToWrite = pendingTxnMap.putIfAbsent(metadata.pid, PendingTxn(metadataToWrite, completionCallback))
    if (existingMetadataToWrite.isDefined)
      throw new IllegalStateException(s"There is already a pending txn to write its markers ${existingMetadataToWrite.get}; this should not happen")

    val result = metadataToWrite.state match {
      case PrepareCommit => TransactionResult.COMMIT
      case PrepareAbort => TransactionResult.ABORT
      case s => throw new IllegalStateException("Unexpected txn metadata state while writing markers: " + s)
    }
    addRequestToSend(metadataToWrite.pid, metadataToWrite.epoch, result, coordinatorEpoch, metadataToWrite.topicPartitions.toSet)
  }

  private def generateRequests(): immutable.Map[Node, RequestAndCompletionHandler] =
    brokerStateMap.flatMap { case (brokerId: Int, destAndMarkerQueue: DestinationBrokerAndQueuedMarkers) =>
      val markersToSend: java.util.List[TxnMarkerEntry] = new util.ArrayList[TxnMarkerEntry]()
      destAndMarkerQueue.markersQueue.drainTo(markersToSend)

      val requestCompletionHandler = new RequestCompletionHandler {
        override def onComplete(response: ClientResponse): Unit = {
          val correlationId = response.requestHeader.correlationId
          if (response.wasDisconnected) {
            trace(s"Cancelled request $response due to node ${response.destination} being disconnected")

            // re-enqueue the markers
            for (txnMarker: TxnMarkerEntry <- markersToSend)
              addRequestForBroker(brokerId, txnMarker)
          } else {
            trace(s"Received response $response from node ${response.destination} with correlation id $correlationId")

            val writeTxnMarkerResponse = response.responseBody.asInstanceOf[WriteTxnMarkerResponse]

            for (txnMarker: TxnMarkerEntry <- markersToSend) {
              val errors = writeTxnMarkerResponse.errors(txnMarker.pid)

              if (errors == null) // TODO: could this ever happen?
                throw new IllegalStateException("WriteTxnMarkerResponse does not contain expected error map for pid " + txnMarker.pid)

              val retryPartitions: mutable.Set[TopicPartition] = mutable.Set.empty[TopicPartition]
              for ((topicPartition: TopicPartition, error: Errors) <- errors) {
                error match {
                  case Errors.NONE =>
                    val metadata = pendingTxnMap.getOrElse(txnMarker.pid, throw new IllegalStateException("Cannot find the pending txn marker in cache; it should not happen")).transactionMetadata
                    // do not synchronize on this metadata since it will only be accessed by the sender thread
                    metadata.topicPartitions -= topicPartition
                  case Errors.UNKNOWN_TOPIC_OR_PARTITION | Errors.NOT_LEADER_FOR_PARTITION |
                       Errors.NOT_ENOUGH_REPLICAS | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                    retryPartitions += topicPartition
                  case _ =>
                    throw new IllegalStateException("Writing txn marker request failed permanently for pid " + txnMarker.pid)
                }

                if (retryPartitions.nonEmpty) {
                  // re-enqueue with possible new leaders of the partitions
                  addRequestToSend(txnMarker.pid, txnMarker.epoch, txnMarker.transactionResult, txnMarker.coordinatorEpoch, retryPartitions.toSet, watchInPurgatory = false)
                }
              }
            }
          }
        }
      }
      if (markersToSend.isEmpty)
        None
      else
        Some((destAndMarkerQueue.destBrokerNode, RequestAndCompletionHandler(new WriteTxnMarkerRequest.Builder(markersToSend), requestCompletionHandler)))
    }.toMap
}

case class DestinationBrokerAndQueuedMarkers(destBrokerNode: Node, markersQueue: BlockingQueue[WriteTxnMarkerRequest.TxnMarkerEntry])