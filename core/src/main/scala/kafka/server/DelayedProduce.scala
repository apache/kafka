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


import kafka.api.ProducerResponseStatus
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition

import scala.Some
import scala.collection._

case class ProducePartitionStatus(requiredOffset: Long, responseStatus: ProducerResponseStatus) {
  @volatile var acksPending = false

  override def toString = "[acksPending: %b, error: %d, startOffset: %d, requiredOffset: %d]"
    .format(acksPending, responseStatus.error, responseStatus.offset, requiredOffset)
}

/**
 * The produce metadata maintained by the delayed produce request
 */
case class ProduceMetadata(produceRequiredAcks: Short,
                           produceStatus: Map[TopicAndPartition, ProducePartitionStatus]) {

  override def toString = "[requiredAcks: %d, partitionStatus: %s]"
    .format(produceRequiredAcks, produceStatus)
}

/**
 * A delayed produce request that can be created by the replica manager and watched
 * in the produce request purgatory
 */
class DelayedProduce(delayMs: Long,
                     produceMetadata: ProduceMetadata,
                     replicaManager: ReplicaManager,
                     responseCallback: Map[TopicAndPartition, ProducerResponseStatus] => Unit)
  extends DelayedRequest(delayMs) {

  // first update the acks pending variable according to the error code
  produceMetadata.produceStatus.foreach { case (topicAndPartition, status) =>
    if (status.responseStatus.error == ErrorMapping.NoError) {
      // Timeout error state will be cleared when required acks are received
      status.acksPending = true
      status.responseStatus.error = ErrorMapping.RequestTimedOutCode
    } else {
      status.acksPending = false
    }

    trace("Initial partition status for %s is %s".format(topicAndPartition, status))
  }

  /**
   * The delayed produce request can be completed if every partition
   * it produces to is satisfied by one of the following:
   *
   * Case A: This broker is no longer the leader: set an error in response
   * Case B: This broker is the leader:
   *   B.1 - If there was a local error thrown while checking if at least requiredAcks
   *         replicas have caught up to this request: set an error in response
   *   B.2 - Otherwise, set the response with no error.
   */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    produceMetadata.produceStatus.foreach { case (topicAndPartition, status) =>
      trace("Checking produce satisfaction for %s, current status %s"
        .format(topicAndPartition, status))
      // skip those partitions that have already been satisfied
      if (status.acksPending) {
        val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition)
        val (hasEnough, errorCode) = partitionOpt match {
          case Some(partition) =>
            partition.checkEnoughReplicasReachOffset(
              status.requiredOffset,
              produceMetadata.produceRequiredAcks)
          case None =>
            // Case A
            (false, ErrorMapping.UnknownTopicOrPartitionCode)
        }
        if (errorCode != ErrorMapping.NoError) {
          // Case B.1
          status.acksPending = false
          status.responseStatus.error = errorCode
        } else if (hasEnough) {
          // Case B.2
          status.acksPending = false
          status.responseStatus.error = ErrorMapping.NoError
        }
      }
    }

    // check if each partition has satisfied at lease one of case A and case B
    if (! produceMetadata.produceStatus.values.exists(p => p.acksPending))
      forceComplete()
    else
      false
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   */
  override def onComplete() {
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    responseCallback(responseStatus)
  }
}

