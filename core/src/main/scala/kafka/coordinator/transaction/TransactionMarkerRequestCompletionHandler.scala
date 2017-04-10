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

import kafka.utils.Logging
import org.apache.kafka.clients.{ClientResponse, RequestCompletionHandler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.WriteTxnMarkerRequest.TxnMarkerEntry
import org.apache.kafka.common.requests.WriteTxnMarkerResponse

import scala.collection.mutable
import collection.JavaConversions._

class TransactionMarkerRequestCompletionHandler(transactionMarkerChannel: TransactionMarkerChannel,
                                                markers: java.util.List[TxnMarkerEntry],
                                                brokerId: Int) extends RequestCompletionHandler with Logging {
  override def onComplete(response: ClientResponse): Unit = {
    val correlationId = response.requestHeader.correlationId
    if (response.wasDisconnected) {
      trace(s"Cancelled request $response due to node ${response.destination} being disconnected")

      // re-enqueue the markers
      for (txnMarker: TxnMarkerEntry <- markers)
        transactionMarkerChannel.addRequestForBroker(brokerId, txnMarker)
    } else {
      trace(s"Received response $response from node ${response.destination} with correlation id $correlationId")

      val writeTxnMarkerResponse = response.responseBody.asInstanceOf[WriteTxnMarkerResponse]

      for (txnMarker: TxnMarkerEntry <- markers) {
        val errors = writeTxnMarkerResponse.errors(txnMarker.pid)

        if (errors == null) // TODO: could this ever happen?
          throw new IllegalStateException("WriteTxnMarkerResponse does not contain expected error map for pid " + txnMarker.pid)

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
              throw new IllegalStateException("Writing txn marker request failed permanently for pid " + txnMarker.pid)
          }

          if (retryPartitions.nonEmpty) {
            // re-enqueue with possible new leaders of the partitions
            transactionMarkerChannel.addRequestToSend(txnMarker.pid, txnMarker.epoch, txnMarker.transactionResult, txnMarker.coordinatorEpoch, retryPartitions.toSet)
          }
        }
      }
    }
  }
}
