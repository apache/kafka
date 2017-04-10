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
import kafka.server.{KafkaConfig, MetadataCache}
import kafka.utils.{KafkaScheduler, Logging, Scheduler}
import org.apache.kafka.clients._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkerRequest, WriteTxnMarkerResponse}
import org.apache.kafka.common.requests.WriteTxnMarkerRequest.TxnMarkerEntry
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.{immutable, mutable}
import java.util.concurrent.BlockingQueue


import collection.JavaConverters._
import collection.JavaConversions._

case class PendingTxn(transactionMetadata: TransactionMetadata, callback: Errors => Unit)
case class DestinationBrokerAndQueuedMarkers(destBrokerNode: Node, markersQueue: BlockingQueue[WriteTxnMarkerRequest.TxnMarkerEntry])

object TransactionMarkerChannelManager {
  def apply(config: KafkaConfig,
            metrics: Metrics,
            metadataCache: MetadataCache,
            time: Time): TransactionMarkerChannelManager = {

    val channel = new TransactionMarkerChannel(config.interBrokerListenerName, metadataCache)
    val sendThread: InterBrokerSendThread = {
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


      new InterBrokerSendThread(threadName, networkClient, requestGenerator(channel), time)
    }

    new TransactionMarkerChannelManager(config,
      metrics,
      metadataCache,
      new KafkaScheduler(1, "transaction-marker-channel-manager"),
      sendThread,
      channel,
      time)
  }

  def requestGenerator(transactionMarkerChannel: TransactionMarkerChannel): () => immutable.Map[Node, RequestAndCompletionHandler] = {
    def generateRequests(): immutable.Map[Node, RequestAndCompletionHandler] = {
      transactionMarkerChannel.brokerStateMap.flatMap {case (brokerId: Int, destAndMarkerQueue: DestinationBrokerAndQueuedMarkers) =>
        val markersToSend: java.util.List[TxnMarkerEntry] = new util.ArrayList[TxnMarkerEntry] ()
        destAndMarkerQueue.markersQueue.drainTo (markersToSend)

        val requestCompletionHandler = new RequestCompletionHandler {
          override def onComplete (response: ClientResponse): Unit = {
            val correlationId = response.requestHeader.correlationId
            if (response.wasDisconnected) {
              transactionMarkerChannel.trace (s"Cancelled request $response due to node ${response.destination} being disconnected")

              // re-enqueue the markers
              for (txnMarker: TxnMarkerEntry <- markersToSend)
                transactionMarkerChannel.addRequestForBroker (brokerId, txnMarker)
            } else {
              transactionMarkerChannel.trace (s"Received response $response from node ${response.destination} with correlation id $correlationId")

              val writeTxnMarkerResponse = response.responseBody.asInstanceOf[WriteTxnMarkerResponse]

              for (txnMarker: TxnMarkerEntry <- markersToSend) {
                val errors = writeTxnMarkerResponse.errors (txnMarker.pid)

                if (errors == null) // TODO: could this ever happen?
                  throw new IllegalStateException ("WriteTxnMarkerResponse does not contain expected error map for pid " + txnMarker.pid)

                val retryPartitions: mutable.Set[TopicPartition] = mutable.Set.empty[TopicPartition]
                for ((topicPartition: TopicPartition, error: Errors) <- errors) {
                  error match {
                    case Errors.NONE =>
                      val metadata = transactionMarkerChannel.pendingTxnMetadata(txnMarker.pid)
                      // do not synchronize on this metadata since it will only be accessed by the sender thread
                      metadata.topicPartitions -= topicPartition
                    case Errors.UNKNOWN_TOPIC_OR_PARTITION | Errors.NOT_LEADER_FOR_PARTITION |
                         Errors.NOT_ENOUGH_REPLICAS | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                      retryPartitions += topicPartition
                    case _ =>
                      throw new IllegalStateException ("Writing txn marker request failed permanently for pid " + txnMarker.pid)
                  }

                  if (retryPartitions.nonEmpty) {
                    // re-enqueue with possible new leaders of the partitions
                    transactionMarkerChannel.addRequestToSend (txnMarker.pid, txnMarker.epoch, txnMarker.transactionResult, txnMarker.coordinatorEpoch, retryPartitions.toSet)
                  }
                }
              }
            }
          }
        }
        if (markersToSend.isEmpty)
          None
        else
          Some ((destAndMarkerQueue.destBrokerNode, RequestAndCompletionHandler (new WriteTxnMarkerRequest.Builder (markersToSend), requestCompletionHandler) ) )
      }.toMap
    }
    generateRequests
  }
}

class TransactionMarkerChannelManager(config: KafkaConfig,
                                      metrics: Metrics,
                                      metadataCache: MetadataCache,
                                      scheduler: Scheduler,
                                      interBrokerSendThread: InterBrokerSendThread,
                                      transactionMarkerChannel: TransactionMarkerChannel,
                                      time: Time) extends Logging {

  type WriteTxnMarkerCallback = Errors => Unit

  // TODO: Config for how often this runs?
  private val CommitCompleteScheduleMs = 10

  def start(): Unit = {
    scheduler.startup()
    interBrokerSendThread.start()

    def completedTransactions(): Unit = {
      transactionMarkerChannel.completedTransactions().foreach {
        completed =>
          completed._2.callback(Errors.NONE)
          transactionMarkerChannel.removeCompletedTxn(completed._1)
      }

    }

    scheduler.schedule("transaction-channel-manager",
      completedTransactions,
      delay = CommitCompleteScheduleMs,
      period = CommitCompleteScheduleMs)
  }

  def shutdown(): Unit = {
    interBrokerSendThread.shutdown()
    scheduler.shutdown()
    transactionMarkerChannel.clear()
  }


  def sendTxnMarkerRequest(metadata: TransactionMetadata, coordinatorEpoch: Int, completionCallback: WriteTxnMarkerCallback): Unit = {
    val metadataToWrite = metadata synchronized metadata.copy()
    transactionMarkerChannel.maybeAddPendingRequest(metadataToWrite, completionCallback)
    val result = metadataToWrite.state match {
      case PrepareCommit => TransactionResult.COMMIT
      case PrepareAbort => TransactionResult.ABORT
      case s => throw new IllegalStateException("Unexpected txn metadata state while writing markers: " + s)
    }
    transactionMarkerChannel.addRequestToSend(metadataToWrite.pid, metadataToWrite.epoch, result, coordinatorEpoch, metadataToWrite.topicPartitions.toSet)
  }

}
