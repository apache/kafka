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

import java.util.concurrent.{CompletableFuture, TimeUnit}

import kafka.log.remote.{RemoteLogManager, RemoteLogReadResult}
import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.record.Records

import scala.collection._

/**
 * A remote fetch operation that can be created by the replica manager and watched
 * in the remote fetch operation purgatory
 */
class DelayedRemoteFetch(remoteFetchTask: RemoteLogManager#AsyncReadTask,
                         remoteFetchResult: CompletableFuture[RemoteLogReadResult],
                         remoteFetchInfo: RemoteStorageFetchInfo,
                         delayMs: Long,
                         fetchMetadata: FetchMetadata,
                         localReadResults: Seq[(TopicPartition, LogReadResult)],
                         replicaManager: ReplicaManager,
                         quota: ReplicaQuota,
                         responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit)
  extends DelayedOperation(delayMs) {

  /**
   * The operation can be completed if:
   *
   * Case a: This broker is no longer the leader of the partition it tries to fetch
   * Case b: This broker does not know the partition it tries to fetch
   * Case c: The remote storage read request completed (succeeded or failed)
   * Case d: The partition is in an offline log directory on this broker
   * Case e: This broker is the leader, but the requested epoch is now fenced
   *
   * Upon completion, should return whatever data is available for each valid partition
   */
  override def tryComplete(): Boolean = {
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicPartition, fetchStatus) =>
        val fetchOffset = fetchStatus.startOffsetMetadata
        val fetchLeaderEpoch = fetchStatus.fetchInfo.currentLeaderEpoch
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            replicaManager.getPartitionOrException(topicPartition, expectLeader = fetchMetadata.fetchOnlyLeader)
          }
        } catch {
          case _: KafkaStorageException => // Case d
            debug(s"Partition $topicPartition is in an offline log directory, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: UnknownTopicOrPartitionException => // Case b
            debug(s"Broker no longer knows of partition $topicPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: FencedLeaderEpochException => // Case e
            debug(s"Broker is the leader of partition $topicPartition, but the requested epoch " +
              s"$fetchLeaderEpoch is fenced by the latest leader epoch, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: NotLeaderForPartitionException =>  // Case a
            debug("Broker is no longer the leader of %s, satisfy %s immediately".format(topicPartition, fetchMetadata))
            return forceComplete()
        }
    }
    if (remoteFetchResult.isDone)
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
    DelayedRemoteFetchMetrics.expiredRequestMeter.mark()

    // cancel the remote storage read task, if it has not been executed yet
    remoteFetchTask.cancel(false)
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete() {
    val fetchPartitionData = localReadResults.map { case (tp, result) =>
      if (tp.equals(remoteFetchInfo.topicPartition) && remoteFetchResult.isDone
        && result.exception.isEmpty && result.info.delayedRemoteStorageFetch.isDefined) {
        if (remoteFetchResult.get.error.isDefined) {
          val r = replicaManager.createLogReadResult(remoteFetchResult.get.error.get)
          tp -> FetchPartitionData(r.error, r.highWatermark, r.leaderLogStartOffset, r.info.records,
            r.lastStableOffset, r.info.abortedTransactions, r.preferredReadReplica, false)
        } else {
          val info = remoteFetchResult.get.info.get
          tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, info.records,
            result.lastStableOffset, info.abortedTransactions, result.preferredReadReplica, false)
        }
      } else {
        tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
          result.lastStableOffset, result.info.abortedTransactions, result.preferredReadReplica, false)
      }
    }

    responseCallback(fetchPartitionData)
  }
}

object DelayedRemoteFetchMetrics extends KafkaMetricsGroup {
  val expiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)
}
