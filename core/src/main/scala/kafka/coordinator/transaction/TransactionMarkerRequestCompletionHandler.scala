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
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests.WriteTxnMarkersResponse

import scala.collection.mutable
import collection.JavaConversions._

class TransactionMarkerRequestCompletionHandler(transactionMarkerChannel: TransactionMarkerChannel,
                                                txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker],
                                                epochAndMarkers: CoordinatorEpochAndMarkers,
                                                brokerId: Int) extends RequestCompletionHandler with Logging {
  override def onComplete(response: ClientResponse): Unit = {
    val correlationId = response.requestHeader.correlationId
    if (response.wasDisconnected) {
      trace(s"Cancelled request $response due to node ${response.destination} being disconnected")
      // re-enqueue the markers
      for (txnMarker: TxnMarkerEntry <- epochAndMarkers.txnMarkerEntries) {
        transactionMarkerChannel.addRequestToSend(
          epochAndMarkers.metadataPartition,
          txnMarker.producerId(),
          txnMarker.producerEpoch(),
          txnMarker.transactionResult(),
          epochAndMarkers.coordinatorEpoch,
          txnMarker.partitions().toSet)
      }
    } else {
      trace(s"Received response $response from node ${response.destination} with correlation id $correlationId")

      val writeTxnMarkerResponse = response.responseBody.asInstanceOf[WriteTxnMarkersResponse]

      for (txnMarker: TxnMarkerEntry <- epochAndMarkers.txnMarkerEntries) {
        val errors = writeTxnMarkerResponse.errors(txnMarker.producerId())

        if (errors == null)
          throw new IllegalStateException("WriteTxnMarkerResponse does not contain expected error map for pid " + txnMarker.producerId())

        val retryPartitions: mutable.Set[TopicPartition] = mutable.Set.empty[TopicPartition]
        for ((topicPartition: TopicPartition, error: Errors) <- errors) {
          error match {
            case Errors.NONE =>
              transactionMarkerChannel.pendingTxnMetadata(epochAndMarkers.metadataPartition, txnMarker.producerId()) match {
                case None =>
                  // TODO: probably need to respond with Errors.NOT_COORDINATOR
                  throw new IllegalArgumentException(s"transaction metadata not found during write txn marker request. partition ${epochAndMarkers.metadataPartition} has likely emigrated")
                case Some(metadata) =>
                  // do not synchronize on this metadata since it will only be accessed by the sender thread
                  metadata.topicPartitions -= topicPartition
              }
            case Errors.UNKNOWN_TOPIC_OR_PARTITION | Errors.NOT_LEADER_FOR_PARTITION |
                 Errors.NOT_ENOUGH_REPLICAS | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
              retryPartitions += topicPartition
            case _ =>
              throw new IllegalStateException("Writing txn marker request failed permanently for pid " + txnMarker.producerId())
          }

          if (retryPartitions.nonEmpty) {
            // re-enqueue with possible new leaders of the partitions
            transactionMarkerChannel.addRequestToSend(
              epochAndMarkers.metadataPartition,
              txnMarker.producerId(),
              txnMarker.producerEpoch(),
              txnMarker.transactionResult,
              epochAndMarkers.coordinatorEpoch,
              retryPartitions.toSet)
          }
          val completed = txnMarkerPurgatory.checkAndComplete(txnMarker.producerId())
          trace(s"Competed $completed transactions for producerId ${txnMarker.producerId()}")
        }
      }
    }
  }
}
