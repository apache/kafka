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

import com.yammer.metrics.core.Meter
import kafka.utils.Logging
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.server.LogReadResult
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.purgatory.DelayedOperation
import org.apache.kafka.server.storage.log.{FetchParams, FetchPartitionData}
import org.apache.kafka.storage.internals.log.{LogOffsetMetadata, RemoteLogReadResult, RemoteStorageFetchInfo}

import java.util
import java.util.concurrent.{CompletableFuture, Future, TimeUnit}
import java.util.{Optional, OptionalInt, OptionalLong}
import scala.collection._

/**
 * A remote fetch operation that can be created by the replica manager and watched
 * in the remote fetch operation purgatory
 */
class DelayedRemoteFetch(remoteFetchTasks: util.Map[TopicIdPartition, Future[Void]],
                         remoteFetchResults: util.Map[TopicIdPartition, CompletableFuture[RemoteLogReadResult]],
                         remoteFetchInfos: util.Map[TopicIdPartition, RemoteStorageFetchInfo],
                         remoteFetchMaxWaitMs: Long,
                         fetchPartitionStatus: Seq[(TopicIdPartition, FetchPartitionStatus)],
                         fetchParams: FetchParams,
                         localReadResults: Seq[(TopicIdPartition, LogReadResult)],
                         replicaManager: ReplicaManager,
                         responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit)
  extends DelayedOperation(remoteFetchMaxWaitMs) with Logging {

  if (fetchParams.isFromFollower) {
    throw new IllegalStateException(s"The follower should not invoke remote fetch. Fetch params are: $fetchParams")
  }

  /**
   * The operation can be completed if:
   *
   * Case a: This broker is no longer the leader of the partition it tries to fetch
   * Case b: This broker does not know the partition it tries to fetch
   * Case c: All the remote storage read request completed (succeeded or failed)
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
    // Case c
    if (remoteFetchResults.values().stream().allMatch(taskResult => taskResult.isDone))
      forceComplete()
    else
      false
  }

  override def onExpiration(): Unit = {
    // cancel the remote storage read task, if it has not been executed yet and
    // avoid interrupting the task if it is already running as it may force closing opened/cached resources as transaction index.
    remoteFetchTasks.forEach { (topicIdPartition, task) =>
      if (task != null && !task.isDone) {
         if (!task.cancel(false)) {
           debug(s"Remote fetch task for remoteFetchInfo: ${remoteFetchInfos.get(topicIdPartition)} could not be cancelled.")
         }
      }
    }

    DelayedRemoteFetchMetrics.expiredRequestMeter.mark()
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete(): Unit = {
    val fetchPartitionData = localReadResults.map { case (tp, result) =>
      val remoteFetchResult = remoteFetchResults.get(tp)
      if (remoteFetchResults.containsKey(tp)
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
            if (result.lastStableOffset.isPresent) OptionalLong.of(result.lastStableOffset.getAsLong) else OptionalLong.empty(),
            info.abortedTransactions,
            if (result.preferredReadReplica.isPresent) OptionalInt.of(result.preferredReadReplica.getAsInt) else OptionalInt.empty(),
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
  // Changing the package or class name may cause incompatibility with existing code and metrics configuration
  private val metricsPackage = "kafka.server"
  private val metricsClassName = "DelayedRemoteFetchMetrics"
  private val metricsGroup = new KafkaMetricsGroup(metricsPackage, metricsClassName)
  val expiredRequestMeter: Meter = metricsGroup.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)
}
