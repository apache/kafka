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

import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.storage.internals.log.{FetchParams, FetchPartitionData, LogOffsetMetadata, RemoteLogReadResult, RemoteStorageFetchInfo}

import java.util.concurrent.{CompletableFuture, Future, TimeUnit}
import java.util.{Optional, OptionalInt, OptionalLong}
import scala.collection._

/**
 * A remote fetch operation that can be created by the replica manager and watched
 * in the remote fetch operation purgatory
 */
class DelayedRemoteFetch(remoteFetchTask: Future[Void],
                         remoteFetchResult: CompletableFuture[RemoteLogReadResult],
                         remoteFetchInfo: RemoteStorageFetchInfo,
                         fetchPartitionStatus: Seq[(TopicIdPartition, FetchPartitionStatus)],
                         fetchParams: FetchParams,
                         localReadResults: Seq[(TopicIdPartition, LogReadResult)],
                         replicaManager: ReplicaManager,
                         responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit)
  extends DelayedOperation(fetchParams.maxWaitMs) {

  if (fetchParams.isFromFollower) {
    throw new IllegalStateException(s"The follower should not invoke remote fetch. Fetch params are: $fetchParams")
  }

  /**
   * The operation can be completed if:
   *
   * Case a: This broker is no longer the leader of the partition it tries to fetch
   * Case b: This broker does not know the partition it tries to fetch
   * Case c: The remote storage read request completed (succeeded or failed)
   * Case d: The partition is in an offline log directory on this broker
   *
   * Upon completion, should return whatever data is available for each valid partition
   */
  override def tryComplete(): Boolean = {
    fetchPartitionStatus.foreach {
      case (topicPartition, fetchStatus) =>
        val fetchOffset = fetchStatus.startOffsetMetadata
        try {
          if (fetchOffset != LogOffsetMetadata.UNKNOWN_OFFSET_METADATA) {
            replicaManager.getPartitionOrException(topicPartition.topicPartition())
          }
        } catch {
          case _: KafkaStorageException => // Case d
            debug(s"Partition $topicPartition is in an offline log directory, satisfy $fetchParams immediately")
            return forceComplete()
          case _: UnknownTopicOrPartitionException => // Case b
            debug(s"Broker no longer knows of partition $topicPartition, satisfy $fetchParams immediately")
            return forceComplete()
          case _: NotLeaderOrFollowerException =>  // Case a
            debug("Broker is no longer the leader or follower of %s, satisfy %s immediately".format(topicPartition, fetchParams))
            return forceComplete()
        }
    }
    if (remoteFetchResult.isDone) // Case c
      forceComplete()
    else
      false
  }

  override def onExpiration(): Unit = {
    // cancel the remote storage read task, if it has not been executed yet
    val cancelled = remoteFetchTask.cancel(true)
    if (!cancelled) debug(s"Remote fetch task for RemoteStorageFetchInfo: $remoteFetchInfo could not be cancelled and its isDone value is ${remoteFetchTask.isDone}")

    DelayedRemoteFetchMetrics.expiredRequestMeter.mark()
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete(): Unit = {
    val fetchPartitionData = localReadResults.map { case (tp, result) =>
      if (tp.topicPartition().equals(remoteFetchInfo.topicPartition)
        && remoteFetchResult.isDone
        && result.error == Errors.NONE
        && result.info.delayedRemoteStorageFetch.isPresent) {
        if (remoteFetchResult.get.error.isPresent) {
          tp -> ReplicaManager.createLogReadResult(remoteFetchResult.get.error.get).toFetchPartitionData(false)
        } else {
          val info = remoteFetchResult.get.fetchDataInfo.get
          tp -> new FetchPartitionData(
            result.error,
            result.highWatermark,
            result.leaderLogStartOffset,
            info.records,
            Optional.empty(),
            if (result.lastStableOffset.isDefined) OptionalLong.of(result.lastStableOffset.get) else OptionalLong.empty(),
            info.abortedTransactions,
            if (result.preferredReadReplica.isDefined) OptionalInt.of(result.preferredReadReplica.get) else OptionalInt.empty(),
            false)
        }
      } else {
        tp -> result.toFetchPartitionData(false)
      }
    }

    responseCallback(fetchPartitionData)
  }
}

object DelayedRemoteFetchMetrics {
  private val metricsGroup = new KafkaMetricsGroup(DelayedRemoteFetchMetrics.getClass)
  val expiredRequestMeter = metricsGroup.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)
}
