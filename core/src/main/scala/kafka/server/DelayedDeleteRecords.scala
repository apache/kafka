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


import java.util.concurrent.TimeUnit

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Implicits._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.DeleteRecordsResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DeleteRecordsResponse

import scala.collection._


case class DeleteRecordsPartitionStatus(requiredOffset: Long,
                                        responseStatus: DeleteRecordsResponseData.DeleteRecordsPartitionResult) {
  @volatile var acksPending = false

  override def toString = "[acksPending: %b, error: %s, lowWatermark: %d, requiredOffset: %d]"
    .format(acksPending, Errors.forCode(responseStatus.errorCode).toString, responseStatus.lowWatermark, requiredOffset)
}

/**
 * A delayed delete records operation that can be created by the replica manager and watched
 * in the delete records operation purgatory
 */
class DelayedDeleteRecords(delayMs: Long,
                           deleteRecordsStatus:  Map[TopicPartition, DeleteRecordsPartitionStatus],
                           replicaManager: ReplicaManager,
                           responseCallback: Map[TopicPartition, DeleteRecordsResponseData.DeleteRecordsPartitionResult] => Unit)
  extends DelayedOperation(delayMs) {

  // first update the acks pending variable according to the error code
  deleteRecordsStatus.forKeyValue { (topicPartition, status) =>
    if (status.responseStatus.errorCode == Errors.NONE.code) {
      // Timeout error state will be cleared when required acks are received
      status.acksPending = true
      status.responseStatus.setErrorCode(Errors.REQUEST_TIMED_OUT.code)
    } else {
      status.acksPending = false
    }

    trace("Initial partition status for %s is %s".format(topicPartition, status))
  }

  /**
   * The delayed delete records operation can be completed if every partition specified in the request satisfied one of the following:
   *
   * 1) There was an error while checking if all replicas have caught up to the deleteRecordsOffset: set an error in response
   * 2) The low watermark of the partition has caught up to the deleteRecordsOffset. set the low watermark in response
   *
   */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    deleteRecordsStatus.forKeyValue { (topicPartition, status) =>
      trace(s"Checking delete records satisfaction for $topicPartition, current status $status")
      // skip those partitions that have already been satisfied
      if (status.acksPending) {
        val (lowWatermarkReached, error, lw) = replicaManager.getPartition(topicPartition) match {
          case HostedPartition.Online(partition) =>
            partition.leaderLogIfLocal match {
              case Some(_) =>
                val leaderLW = partition.lowWatermarkIfLeader
                (leaderLW >= status.requiredOffset, Errors.NONE, leaderLW)
              case None =>
                (false, Errors.NOT_LEADER_OR_FOLLOWER, DeleteRecordsResponse.INVALID_LOW_WATERMARK)
            }

          case _: HostedPartition.Deferred =>
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION, DeleteRecordsResponse.INVALID_LOW_WATERMARK)

          case HostedPartition.Offline =>
            (false, Errors.KAFKA_STORAGE_ERROR, DeleteRecordsResponse.INVALID_LOW_WATERMARK)

          case HostedPartition.None =>
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION, DeleteRecordsResponse.INVALID_LOW_WATERMARK)
        }
        if (error != Errors.NONE || lowWatermarkReached) {
          status.acksPending = false
          status.responseStatus.setErrorCode(error.code)
          status.responseStatus.setLowWatermark(lw)
        }
      }
    }

    // check if every partition has satisfied at least one of case A or B
    if (!deleteRecordsStatus.values.exists(_.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration(): Unit = {
    deleteRecordsStatus.forKeyValue { (topicPartition, status) =>
      if (status.acksPending) {
        DelayedDeleteRecordsMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   */
  override def onComplete(): Unit = {
    val responseStatus = deleteRecordsStatus.map { case (k, status) => k -> status.responseStatus }
    responseCallback(responseStatus)
  }
}

object DelayedDeleteRecordsMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  def recordExpiration(partition: TopicPartition): Unit = {
    aggregateExpirationMeter.mark()
  }
}

