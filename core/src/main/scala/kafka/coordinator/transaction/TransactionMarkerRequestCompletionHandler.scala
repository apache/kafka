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
                                                txnTopicPartition: Int,
                                                txnStateManager: TransactionStateManager,
                                                txnMarkerChannel: TransactionMarkerChannel,
                                                txnIdAndMarkerEntries: java.util.List[TxnIdAndMarkerEntry],
                                                txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker]) extends RequestCompletionHandler with Logging {
  override def onComplete(response: ClientResponse): Unit = {
    val correlationId = response.requestHeader.correlationId
    if (response.wasDisconnected) {
      trace(s"Cancelled request $response due to node ${response.destination} being disconnected")
      // re-enqueue the markers
      for (txnIdAndMarker: TxnIdAndMarkerEntry <- txnIdAndMarkerEntries) {
        val txnMarker = txnIdAndMarker.txnMarkerEntry
        txnMarkerChannel.addTxnMarkersToSend(txnIdAndMarker.txnId,
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

        txnStateManager.getTransactionState(transactionalId) match {
          case None =>
            // txn topic partition has likely emigrated, just cancel it from the purgatory
            txnMarkerPurgatory.cancelForKey(transactionalId)

          case Some(epochAndMetadata) =>
            val retryPartitions: mutable.Set[TopicPartition] = mutable.Set.empty[TopicPartition]
            val errors = writeTxnMarkerResponse.errors(txnMarker.producerId)

            if (errors == null)
              throw new IllegalStateException(s"WriteTxnMarkerResponse does not contain expected error map for pid ${txnMarker.producerId}")

            epochAndMetadata synchronized {
              for ((topicPartition: TopicPartition, error: Errors) <- errors) {
                error match {
                  case Errors.NONE =>
                    epochAndMetadata.transactionMetadata.topicPartitions -= topicPartition

                  case Errors.UNKNOWN_TOPIC_OR_PARTITION | Errors.NOT_LEADER_FOR_PARTITION |
                       Errors.NOT_ENOUGH_REPLICAS | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                    retryPartitions += topicPartition

                  case other =>
                    throw new IllegalStateException(s"Unexpected error ${other.exceptionName} while sending txn marker for $transactionalId")
                }
              }
            }

            if (retryPartitions.nonEmpty) {
              // re-enqueue with possible new leaders of the partitions
              txnMarkerChannel.addTxnMarkersToSend(
                transactionalId,
                txnMarker.producerId(),
                txnMarker.producerEpoch(),
                txnMarker.transactionResult,
                txnMarker.coordinatorEpoch(),
                retryPartitions.toSet)
            } else {
              txnMarkerPurgatory.checkAndComplete(txnMarker.producerId)
            }
        }
      }
    }
  }
}
