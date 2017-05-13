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

import kafka.server.DelayedOperationPurgatory
import kafka.utils.Logging
import org.apache.kafka.clients.{ClientResponse, RequestCompletionHandler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.WriteTxnMarkersResponse

import scala.collection.mutable
import collection.JavaConversions._

class TransactionMarkerRequestCompletionHandler(brokerId: Int,
                                                txnStateManager: TransactionStateManager,
                                                txnMarkerChannelManager: TransactionMarkerChannelManager,
                                                txnIdAndMarkerEntries: java.util.List[TxnIdAndMarkerEntry]) extends RequestCompletionHandler with Logging {
  override def onComplete(response: ClientResponse): Unit = {
    val correlationId = response.requestHeader.correlationId
    if (response.wasDisconnected) {
      trace(s"Cancelled request $response due to node ${response.destination} being disconnected")
      // re-enqueue the markers
      for (txnIdAndMarker: TxnIdAndMarkerEntry <- txnIdAndMarkerEntries) {
        val txnMarker = txnIdAndMarker.txnMarkerEntry
        txnMarkerChannelManager.addTxnMarkersToBrokerQueue(txnIdAndMarker.txnId,
          txnMarker.producerId(),
          txnMarker.producerEpoch(),
          txnMarker.transactionResult(),
          txnMarker.coordinatorEpoch(),
          txnMarker.partitions().toSet)
      }
    } else {
      trace(s"Received response $response from node ${response.destination} with correlation id $correlationId")

      val writeTxnMarkerResponse = response.responseBody.asInstanceOf[WriteTxnMarkersResponse]

      for (txnIdAndMarker: TxnIdAndMarkerEntry <- txnIdAndMarkerEntries) {
        val transactionalId = txnIdAndMarker.txnId
        val txnMarker = txnIdAndMarker.txnMarkerEntry
        val errors = writeTxnMarkerResponse.errors(txnMarker.producerId)

        if (errors == null)
          throw new IllegalStateException(s"WriteTxnMarkerResponse does not contain expected error map for producer id ${txnMarker.producerId}")

        txnStateManager.getTransactionState(transactionalId) match {
          case None =>
            info(s"Transaction topic partition for $transactionalId may likely has emigrated, as the corresponding metadata do not exist in the cache" +
              s"any more; cancel sending transaction markers $txnMarker to the brokers")

            // txn topic partition has likely emigrated, just cancel it from the purgatory
            txnMarkerChannelManager.removeMarkersForTxnId(transactionalId)

          case Some(epochAndMetadata) =>
            val txnMetadata = epochAndMetadata.transactionMetadata
            val retryPartitions: mutable.Set[TopicPartition] = mutable.Set.empty[TopicPartition]
            var abortSending: Boolean = false

            if (epochAndMetadata.coordinatorEpoch != txnMarker.coordinatorEpoch) {
              // coordinator epoch has changed, just cancel it from the purgatory
              info(s"Transaction coordinator epoch for $transactionalId has changed from ${txnMarker.coordinatorEpoch} to " +
                s"${epochAndMetadata.coordinatorEpoch}; cancel sending transaction markers $txnMarker to the brokers")

              txnMarkerChannelManager.removeMarkersForTxnId(transactionalId)
              abortSending = true
            } else {
              txnMetadata synchronized {
                for ((topicPartition: TopicPartition, error: Errors) <- errors) {
                  error match {
                    case Errors.NONE =>

                      txnMetadata.removePartition(topicPartition)

                    case Errors.CORRUPT_MESSAGE |
                         Errors.MESSAGE_TOO_LARGE |
                         Errors.RECORD_LIST_TOO_LARGE |
                         Errors.INVALID_REQUIRED_ACKS => // these are all unexpected and fatal errors

                      throw new IllegalStateException(s"Received fatal error ${error.exceptionName} while sending txn marker for $transactionalId")

                    case Errors.UNKNOWN_TOPIC_OR_PARTITION |
                         Errors.NOT_LEADER_FOR_PARTITION |
                         Errors.NOT_ENOUGH_REPLICAS |
                         Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND => // these are retriable errors

                      info(s"Sending $transactionalId's transaction marker for partition $topicPartition has failed with error ${error.exceptionName}, retrying " +
                        s"with current coordinator epoch ${epochAndMetadata.coordinatorEpoch}")

                      retryPartitions += topicPartition

                    case Errors.INVALID_PRODUCER_EPOCH |
                         Errors.TRANSACTION_COORDINATOR_FENCED => // producer or coordinator epoch has changed, this txn can now be ignored

                      info(s"Sending $transactionalId's transaction marker for partition $topicPartition has permanently failed with error ${error.exceptionName} " +
                        s"with the current coordinator epoch ${epochAndMetadata.coordinatorEpoch}; cancel sending any more transaction markers $txnMarker to the brokers")

                      txnMarkerChannelManager.removeMarkersForTxnId(transactionalId)
                      abortSending = true

                    case other =>
                      throw new IllegalStateException(s"Unexpected error ${other.exceptionName} while sending txn marker for $transactionalId")
                  }
                }
              }
            }

            if (!abortSending) {
              if (retryPartitions.nonEmpty) {
                // re-enqueue with possible new leaders of the partitions
                txnMarkerChannelManager.addTxnMarkersToBrokerQueue(
                  transactionalId,
                  txnMarker.producerId(),
                  txnMarker.producerEpoch(),
                  txnMarker.transactionResult,
                  txnMarker.coordinatorEpoch(),
                  retryPartitions.toSet)
              } else {
                txnMarkerChannelManager.completeSendMarkersForTxnId(transactionalId)
              }
            }
        }
      }
    }
  }
}
