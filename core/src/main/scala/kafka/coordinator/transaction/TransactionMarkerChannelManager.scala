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
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkersRequest}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.Node

import scala.collection.{immutable, mutable}
import java.util.concurrent.BlockingQueue

import collection.JavaConverters._
import collection.JavaConversions._

case class PendingTxn(transactionMetadata: TransactionMetadata, callback: Errors => Unit)
case class CoordinatorEpochAndMarkers(coordinatorEpoch: Int, txnMarkerEntry: util.List[WriteTxnMarkersRequest.TxnMarkerEntry])
case class DestinationBrokerAndQueuedMarkers(destBrokerNode: Node, markersQueue: BlockingQueue[CoordinatorEpochAndMarkers])

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
      metadataCache,
      new KafkaScheduler(1, "transaction-marker-channel-manager"),
      sendThread,
      channel)
  }


  private[transaction] def requestGenerator(transactionMarkerChannel: TransactionMarkerChannel): () => Iterable[RequestAndCompletionHandler] = {
    def generateRequests(): Iterable[RequestAndCompletionHandler] = {
      transactionMarkerChannel.brokerStateMap.flatMap {case (brokerId: Int, destAndMarkerQueue: DestinationBrokerAndQueuedMarkers) =>
        val markersToSend: java.util.List[CoordinatorEpochAndMarkers] = new util.ArrayList[CoordinatorEpochAndMarkers] ()
        destAndMarkerQueue.markersQueue.drainTo (markersToSend)
        markersToSend.groupBy{ epochAndMarker => epochAndMarker.coordinatorEpoch }
          .map { case(coordinatorEpoch:Int, buffer: mutable.Buffer[CoordinatorEpochAndMarkers]) =>
            val txnMarkerEntries = buffer.flatMap { x => x.txnMarkerEntry }.asJava
            val requestCompletionHandler = new TransactionMarkerRequestCompletionHandler(
              transactionMarkerChannel,
              CoordinatorEpochAndMarkers(coordinatorEpoch, txnMarkerEntries),
              brokerId)
            RequestAndCompletionHandler(destAndMarkerQueue.destBrokerNode, new WriteTxnMarkersRequest.Builder(coordinatorEpoch, txnMarkerEntries), requestCompletionHandler)
          }
      }
    }
    generateRequests
  }
}



class TransactionMarkerChannelManager(config: KafkaConfig,
                                      metadataCache: MetadataCache,
                                      scheduler: Scheduler,
                                      interBrokerSendThread: InterBrokerSendThread,
                                      transactionMarkerChannel: TransactionMarkerChannel) extends Logging {

  type WriteTxnMarkerCallback = Errors => Unit

  // TODO: Config for how often this runs.
  private val CommitCompleteScheduleMs = 10


  def start(): Unit = {
    scheduler.startup()
    interBrokerSendThread.start()

    scheduler.schedule("transaction-channel-manager",
      completeCompletedRequests,
      delay = CommitCompleteScheduleMs,
      period = CommitCompleteScheduleMs)
  }

  def shutdown(): Unit = {
    interBrokerSendThread.shutdown()
    scheduler.shutdown()
    transactionMarkerChannel.clear()
  }


  def addTxnMarkerRequest(metadata: TransactionMetadata, coordinatorEpoch: Int, completionCallback: WriteTxnMarkerCallback): Unit = {
    val metadataToWrite = metadata synchronized metadata.copy()
    transactionMarkerChannel.maybeAddPendingRequest(metadataToWrite, completionCallback)
    val result = metadataToWrite.state match {
      case PrepareCommit => TransactionResult.COMMIT
      case PrepareAbort => TransactionResult.ABORT
      case s => throw new IllegalStateException("Unexpected txn metadata state while writing markers: " + s)
    }
    transactionMarkerChannel.addRequestToSend(metadataToWrite.pid, metadataToWrite.epoch, result, coordinatorEpoch, metadataToWrite.topicPartitions.toSet)
  }


  private[transaction] def completeCompletedRequests(): Unit = {
    transactionMarkerChannel.completedTransactions().foreach {
      completed =>
        completed._2.callback(Errors.NONE)
        transactionMarkerChannel.removeCompletedTxn(completed._1)
    }
  }

}
