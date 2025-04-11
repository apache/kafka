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
import java.util.concurrent.locks.Lock
import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.Meter
import kafka.utils.{Logging, Pool}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.purgatory.DelayedOperation

import scala.collection._
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption

case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  @volatile var acksPending = false

  override def toString: String = s"[acksPending: $acksPending, error: ${responseStatus.error.code}, " +
    s"startOffset: ${responseStatus.baseOffset}, requiredOffset: $requiredOffset]"
}

/**
 * The produce metadata maintained by the delayed produce operation
 */
case class ProduceMetadata(produceRequiredAcks: Short,
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString = s"[requiredAcks: $produceRequiredAcks, partitionStatus: $produceStatus]"
}

object DelayedProduce {
  private final val logger = Logger(classOf[DelayedProduce])
}

/**
 * A delayed produce operation that can be created by the replica manager and watched
 * in the produce operation purgatory
 */
class DelayedProduce(delayMs: Long,
                     produceMetadata: ProduceMetadata,
                     replicaManager: ReplicaManager,
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                     lockOpt: Option[Lock])
  extends DelayedOperation(delayMs, lockOpt.toJava) with Logging {

  override lazy val logger: Logger = DelayedProduce.logger

  // first update the acks pending variable according to the error code
  produceMetadata.produceStatus.foreachEntry { (topicPartition, status) =>
    if (status.responseStatus.error == Errors.NONE) {
      // Timeout error state will be cleared when required acks are received
      status.acksPending = true
      status.responseStatus.error = Errors.REQUEST_TIMED_OUT
    } else {
      status.acksPending = false
    }

    trace(s"Initial partition status for $topicPartition is $status")
  }

  /**
   * The delayed produce operation can be completed if every partition
   * it produces to is satisfied by one of the following:
   *
   * Case A: Replica not assigned to partition
   * Case B: Replica is no longer the leader of this partition
   * Case C: This broker is the leader:
   *   C.1 - If there was a local error thrown while checking if at least requiredAcks
   *         replicas have caught up to this operation: set an error in response
   *   C.2 - Otherwise, set the response with no error.
   */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    produceMetadata.produceStatus.foreachEntry { (topicPartition, status) =>
      trace(s"Checking produce satisfaction for $topicPartition, current status $status")
      // skip those partitions that have already been satisfied
      if (status.acksPending) {
        val (hasEnough, error) = replicaManager.getPartitionOrError(topicPartition) match {
          case Left(err) =>
            // Case A
            (false, err)

          case Right(partition) =>
            partition.checkEnoughReplicasReachOffset(status.requiredOffset)
        }

        // Case B || C.1 || C.2
        if (error != Errors.NONE || hasEnough) {
          status.acksPending = false
          status.responseStatus.error = error
        }
      }
    }

    // check if every partition has satisfied at least one of case A, B or C
    if (!produceMetadata.produceStatus.values.exists(_.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration(): Unit = {
    produceMetadata.produceStatus.foreachEntry { (topicPartition, status) =>
      if (status.acksPending) {
        debug(s"Expiring produce request for partition $topicPartition with status $status")
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   */
  override def onComplete(): Unit = {
    val responseStatus = produceMetadata.produceStatus.map { case (k, status) => k -> status.responseStatus }
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics {
  private val metricsGroup = new KafkaMetricsGroup(DelayedProduceMetrics.getClass)

  private val aggregateExpirationMeter = metricsGroup.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    metricsGroup.newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             Map("topic" -> key.topic, "partition" -> key.partition.toString).asJava)
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition): Unit = {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}
