/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.api._
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition
import kafka.utils.Logging
import kafka.network.RequestChannel

import scala.Some
import scala.collection.immutable.Map
import scala.collection.Seq

/** A delayed produce request, which is satisfied (or more
  * accurately, unblocked) -- if for every partition it produce to:
  * Case A: This broker is not the leader: unblock - should return error.
  * Case B: This broker is the leader:
  *   B.1 - If there was a localError (when writing to the local log): unblock - should return error
  *   B.2 - else, at least requiredAcks replicas should be caught up to this request.
  */

class DelayedProduce(override val keys: Seq[TopicAndPartition],
                     override val request: RequestChannel.Request,
                     override val delayMs: Long,
                     val produce: ProducerRequest,
                     val partitionStatus: Map[TopicAndPartition, DelayedProduceResponseStatus],
                     val offsetCommitRequestOpt: Option[OffsetCommitRequest] = None)
  extends DelayedRequest(keys, request, delayMs) with Logging {

  // first update the acks pending variable according to the error code
  partitionStatus foreach { case (topicAndPartition, delayedStatus) =>
    if (delayedStatus.responseStatus.error == ErrorMapping.NoError) {
      // Timeout error state will be cleared when required acks are received
      delayedStatus.acksPending = true
      delayedStatus.responseStatus.error = ErrorMapping.RequestTimedOutCode
    } else {
      delayedStatus.acksPending = false
    }

    trace("Initial partition status for %s is %s".format(topicAndPartition, delayedStatus))
  }

  def respond(offsetManager: OffsetManager): RequestOrResponse = {
    val responseStatus = partitionStatus.mapValues(status => status.responseStatus)

    val errorCode = responseStatus.find { case (_, status) =>
      status.error != ErrorMapping.NoError
    }.map(_._2.error).getOrElse(ErrorMapping.NoError)

    if (errorCode == ErrorMapping.NoError) {
      offsetCommitRequestOpt.foreach(ocr => offsetManager.putOffsets(ocr.groupId, ocr.requestInfo) )
    }

    val response = offsetCommitRequestOpt.map(_.responseFor(errorCode, offsetManager.config.maxMetadataSize))
      .getOrElse(ProducerResponse(produce.correlationId, responseStatus))

    response
  }

  def isSatisfied(replicaManager: ReplicaManager) = {
    // check for each partition if it still has pending acks
    partitionStatus.foreach { case (topicAndPartition, fetchPartitionStatus) =>
      trace("Checking producer request satisfaction for %s, acksPending = %b"
        .format(topicAndPartition, fetchPartitionStatus.acksPending))
      // skip those partitions that have already been satisfied
      if (fetchPartitionStatus.acksPending) {
        val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition)
        val (hasEnough, errorCode) = partitionOpt match {
          case Some(partition) =>
            partition.checkEnoughReplicasReachOffset(
              fetchPartitionStatus.requiredOffset,
              produce.requiredAcks)
          case None =>
            (false, ErrorMapping.UnknownTopicOrPartitionCode)
        }
        if (errorCode != ErrorMapping.NoError) {
          fetchPartitionStatus.acksPending = false
          fetchPartitionStatus.responseStatus.error = errorCode
        } else if (hasEnough) {
          fetchPartitionStatus.acksPending = false
          fetchPartitionStatus.responseStatus.error = ErrorMapping.NoError
        }
      }
    }

    // unblocked if there are no partitions with pending acks
    val satisfied = ! partitionStatus.exists(p => p._2.acksPending)
    satisfied
  }
}

case class DelayedProduceResponseStatus(val requiredOffset: Long,
                                        val responseStatus: ProducerResponseStatus) {
  @volatile var acksPending = false

  override def toString =
    "acksPending:%b, error: %d, startOffset: %d, requiredOffset: %d".format(
      acksPending, responseStatus.error, responseStatus.offset, requiredOffset)
}
