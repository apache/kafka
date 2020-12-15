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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}

import scala.collection._

case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionData) {

  override def toString: String = {
    "[startOffsetMetadata: " + startOffsetMetadata +
      ", fetchInfo: " + fetchInfo +
      "]"
  }
}

/**
 * The fetch metadata maintained by the delayed fetch operation
 */
case class FetchMetadata(fetchMinBytes: Int,
                         fetchMaxBytes: Int,
                         hardMaxBytesLimit: Boolean,
                         fetchOnlyLeader: Boolean,
                         fetchIsolation: FetchIsolation,
                         isFromFollower: Boolean,
                         replicaId: Int,
                         fetchPartitionStatus: Seq[(TopicPartition, FetchPartitionStatus)]) {

  override def toString = "FetchMetadata(minBytes=" + fetchMinBytes + ", " +
    "maxBytes=" + fetchMaxBytes + ", " +
    "onlyLeader=" + fetchOnlyLeader + ", " +
    "fetchIsolation=" + fetchIsolation + ", " +
    "replicaId=" + replicaId + ", " +
    "partitionStatus=" + fetchPartitionStatus + ")"
}
/**
 * A delayed fetch operation that can be created by the replica manager and watched
 * in the fetch operation purgatory
 */
class DelayedFetch(delayMs: Long,
                   fetchMetadata: FetchMetadata,
                   replicaManager: ReplicaManager,
                   quota: ReplicaQuota,
                   clientMetadata: Option[ClientMetadata],
                   responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit)
  extends DelayedOperation(delayMs) {

  /**
   * The operation can be completed if:
   *
   * Case A: This broker is no longer the leader for some partitions it tries to fetch
   * Case B: The replica is no longer available on this broker
   * Case C: This broker does not know of some partitions it tries to fetch
   * Case D: The partition is in an offline log directory on this broker
   * Case E: This broker is the leader, but the requested epoch is now fenced
   * Case F: The fetch offset locates not on the last segment of the log
   * Case G: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
   * Case H: A diverging epoch was found, return response to trigger truncation
   * Upon completion, should return whatever data is available for each valid partition
   */
  override def tryComplete(): Boolean = {
    var accumulatedSize = 0
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicPartition, fetchStatus) =>
        val fetchOffset = fetchStatus.startOffsetMetadata
        val fetchLeaderEpoch = fetchStatus.fetchInfo.currentLeaderEpoch
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            val partition = replicaManager.getPartitionOrException(topicPartition)
            val offsetSnapshot = partition.fetchOffsetSnapshot(fetchLeaderEpoch, fetchMetadata.fetchOnlyLeader)

            val endOffset = fetchMetadata.fetchIsolation match {
              case FetchLogEnd => offsetSnapshot.logEndOffset
              case FetchHighWatermark => offsetSnapshot.highWatermark
              case FetchTxnCommitted => offsetSnapshot.lastStableOffset
            }

            // Go directly to the check for Case G if the message offsets are the same. If the log segment
            // has just rolled, then the high watermark offset will remain the same but be on the old segment,
            // which would incorrectly be seen as an instance of Case F.
            if (endOffset.messageOffset != fetchOffset.messageOffset) {
              if (endOffset.onOlderSegment(fetchOffset)) {
                // Case F, this can happen when the new fetch operation is on a truncated leader
                debug(s"Satisfying fetch $fetchMetadata since it is fetching later segments of partition $topicPartition.")
                return forceComplete()
              } else if (fetchOffset.onOlderSegment(endOffset)) {
                // Case F, this can happen when the fetch operation is falling behind the current segment
                // or the partition has just rolled a new segment
                debug(s"Satisfying fetch $fetchMetadata immediately since it is fetching older segments.")
                // We will not force complete the fetch request if a replica should be throttled.
                if (!replicaManager.shouldLeaderThrottle(quota, partition, fetchMetadata.replicaId))
                  return forceComplete()
              } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                // we take the partition fetch size as upper bound when accumulating the bytes (skip if a throttled partition)
                val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.maxBytes)
                if (!replicaManager.shouldLeaderThrottle(quota, partition, fetchMetadata.replicaId))
                  accumulatedSize += bytesAvailable
              }
            }

            // Case H: If truncation has caused diverging epoch while this request was in purgatory, return to trigger truncation
            fetchStatus.fetchInfo.lastFetchedEpoch.ifPresent { fetchEpoch =>
              val epochEndOffset = partition.lastOffsetForLeaderEpoch(fetchLeaderEpoch, fetchEpoch, fetchOnlyFromLeader = false)
              if (epochEndOffset.errorCode != Errors.NONE.code()
                  || epochEndOffset.endOffset == UNDEFINED_EPOCH_OFFSET
                  || epochEndOffset.leaderEpoch == UNDEFINED_EPOCH) {
                debug(s"Could not obtain last offset for leader epoch for partition $topicPartition, epochEndOffset=$epochEndOffset.")
                return forceComplete()
              } else if (epochEndOffset.leaderEpoch < fetchEpoch || epochEndOffset.endOffset < fetchStatus.fetchInfo.fetchOffset) {
                debug(s"Satisfying fetch $fetchMetadata since it has diverging epoch requiring truncation for partition " +
                  s"$topicPartition epochEndOffset=$epochEndOffset fetchEpoch=$fetchEpoch fetchOffset=${fetchStatus.fetchInfo.fetchOffset}.")
                return forceComplete()
              }
            }
          }
        } catch {
          case _: NotLeaderOrFollowerException =>  // Case A or Case B
            debug(s"Broker is no longer the leader or follower of $topicPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: UnknownTopicOrPartitionException => // Case C
            debug(s"Broker no longer knows of partition $topicPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: KafkaStorageException => // Case D
            debug(s"Partition $topicPartition is in an offline log directory, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: FencedLeaderEpochException => // Case E
            debug(s"Broker is the leader of partition $topicPartition, but the requested epoch " +
              s"$fetchLeaderEpoch is fenced by the latest leader epoch, satisfy $fetchMetadata immediately")
            return forceComplete()
        }
    }

    // Case G
    if (accumulatedSize >= fetchMetadata.fetchMinBytes)
       forceComplete()
    else
      false
  }

  override def onExpiration(): Unit = {
    if (fetchMetadata.isFromFollower)
      DelayedFetchMetrics.followerExpiredRequestMeter.mark()
    else
      DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete(): Unit = {
    val logReadResults = replicaManager.readFromLocalLog(
      replicaId = fetchMetadata.replicaId,
      fetchOnlyFromLeader = fetchMetadata.fetchOnlyLeader,
      fetchIsolation = fetchMetadata.fetchIsolation,
      fetchMaxBytes = fetchMetadata.fetchMaxBytes,
      hardMaxBytesLimit = fetchMetadata.hardMaxBytesLimit,
      readPartitionInfo = fetchMetadata.fetchPartitionStatus.map { case (tp, status) => tp -> status.fetchInfo },
      clientMetadata = clientMetadata,
      quota = quota)

    val fetchPartitionData = logReadResults.map { case (tp, result) =>
      val isReassignmentFetch = fetchMetadata.isFromFollower &&
        replicaManager.isAddingReplica(tp, fetchMetadata.replicaId)

      tp -> FetchPartitionData(
        result.error,
        result.highWatermark,
        result.leaderLogStartOffset,
        result.info.records,
        result.divergingEpoch,
        result.lastStableOffset,
        result.info.abortedTransactions,
        result.preferredReadReplica,
        isReassignmentFetch)
    }

    responseCallback(fetchPartitionData)
  }
}

object DelayedFetchMetrics extends KafkaMetricsGroup {
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "follower"))
  val consumerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "consumer"))
}

