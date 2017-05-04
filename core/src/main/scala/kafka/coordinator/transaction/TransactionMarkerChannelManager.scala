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


import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.Time

import org.apache.kafka.common.protocol.Errors

import collection.JavaConverters._

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
    val threadName = "TxnMarkerSenderThread-" + config.brokerId
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
      threadName,
      1,
      50,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.socketReceiveBufferBytes,
      config.requestTimeoutMs,
      time,
      false,
      new ApiVersions
    )
    val txnMarkerChannel = new TransactionMarkerChannel(config.interBrokerListenerName, txnStateManager, metadataCache, networkClient, time)

    val txnMarkerSendThread: InterBrokerSendThread = {
      networkClient.wakeup()
      new InterBrokerSendThread(threadName, networkClient, requestGenerator(txnMarkerChannel, txnMarkerPurgatory), time)
    }

    new TransactionMarkerChannelManager(config,
      metadataCache,
      txnStateManager,
      txnMarkerSendThread,
      txnMarkerChannel,
      txnMarkerPurgatory)
  }


  private[transaction] def requestGenerator(transactionMarkerChannel: TransactionMarkerChannel,
                                            txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker]): () => Iterable[RequestAndCompletionHandler] = {
    def generateRequests(): Iterable[RequestAndCompletionHandler] = transactionMarkerChannel.drainQueuedTransactionMarkers(txnMarkerPurgatory)

    generateRequests
  }
}



class TransactionMarkerChannelManager(config: KafkaConfig,
                                      metadataCache: MetadataCache,
                                      txnStateManager: TransactionStateManager,
                                      txnMarkerSendThread: InterBrokerSendThread,
                                      txnMarkerChannel: TransactionMarkerChannel,
                                      txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker]) extends Logging {

  def start(): Unit = {
    txnMarkerSendThread.start()
  }

  def shutdown(): Unit = {
    txnMarkerSendThread.shutdown()
    txnMarkerChannel.clear()
  }

  def addTxnMarkersToSend(transactionalId: String,
                          coordinatorEpoch: Int,
                          txnResult: TransactionResult,
                          txnMetadata: TransactionMetadata): Unit = {

    def appendToLogCallback(error: Errors): Unit = {
      error match {
        case Errors.NONE =>
          trace(s"Competed sending transaction markers for $transactionalId as $txnResult")

          txnStateManager.getTransactionState(transactionalId) match {
            case Some(epochAndMetadata) =>
              epochAndMetadata synchronized {
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
                        txnStateManager.appendTransactionToLog(transactionalId, coordinatorEpoch, txnMetadata, retryAppendCallback)

                      case errors: Errors =>
                        throw new IllegalStateException(s"Unexpected error ${errors.exceptionName} while appending to transaction log for $transactionalId")
                    }

                  txnStateManager.appendTransactionToLog(transactionalId, coordinatorEpoch, txnMetadata, retryAppendCallback)
                } else {
                  info(s"Updating $transactionalId's transaction state to $txnMetadata with coordinator epoch $coordinatorEpoch for $transactionalId failed after the transaction markers " +
                    s"has been sent to brokers. The cached metadata have been changed to $epochAndMetadata since preparing to send markers")
                }
              }

            case None =>
              // this transactional id no longer exists, maybe the corresponding partition has already been migrated out.
              // we will stop appending the completed log entry to transaction topic as the new leader should be doing it.
              info(s"Updating $transactionalId's transaction state to $txnMetadata with coordinator epoch $coordinatorEpoch for $transactionalId failed after the transaction message " +
                s"has been appended to the log. The partition ${txnStateManager.partitionFor(transactionalId)} may have migrated as the metadata is no longer in the cache")
          }

        case other =>
          throw new IllegalStateException(s"Unexpected error ${other.exceptionName} before appending to txn log for $transactionalId")
      }
    }

    // watch for both the transactional id and the transaction topic partition id,
    // so we can cancel all the delayed operations for the same partition id;
    // NOTE this is only possible because the hashcode of Int / String never overlaps
    val delayedTxnMarker = new DelayedTxnMarker(txnMetadata, appendToLogCallback)
    val txnTopicPartition = txnStateManager.partitionFor(transactionalId)
    txnMarkerPurgatory.tryCompleteElseWatch(delayedTxnMarker, Seq(transactionalId, txnTopicPartition))

    txnMarkerChannel.addTxnMarkersToSend(transactionalId,
      txnMetadata.producerId,
      txnMetadata.producerEpoch,
      txnResult,
      coordinatorEpoch,
      txnMetadata.topicPartitions.toSet)
  }

  def removeMarkersForTxnTopicPartition(txnTopicPartitionId: Int): Unit = {
    txnMarkerPurgatory.cancelForKey(txnTopicPartitionId)
    txnMarkerChannel.removeMarkersForTxnTopicPartition(txnTopicPartitionId)
  }
}
