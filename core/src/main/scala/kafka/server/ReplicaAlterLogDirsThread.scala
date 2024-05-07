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

import kafka.cluster.Partition
import kafka.server.ReplicaAlterLogDirsThread.{PromotionState, ReassignmentState}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.server.common.{DirectoryEventHandler, OffsetAndEpoch, TopicIdPartition}
import org.apache.kafka.storage.internals.log.{LogAppendInfo, LogStartOffsetIncrementReason}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.{Map, Set}

class ReplicaAlterLogDirsThread(name: String,
                                leader: LeaderEndPoint,
                                failedPartitions: FailedPartitions,
                                replicaMgr: ReplicaManager,
                                quota: ReplicationQuotaManager,
                                brokerTopicStats: BrokerTopicStats,
                                fetchBackOffMs: Int,
                                directoryEventHandler: DirectoryEventHandler = DirectoryEventHandler.NOOP,
                               )
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                leader = leader,
                                failedPartitions,
                                fetchTierStateMachine = new ReplicaAlterLogDirsTierStateMachine(),
                                fetchBackOffMs = fetchBackOffMs,
                                isInterruptible = false,
                                brokerTopicStats) {

  // Visible for testing
  private[server] val promotionStates: ConcurrentHashMap[TopicPartition, PromotionState] = new ConcurrentHashMap()

  override protected def latestEpoch(topicPartition: TopicPartition): Option[Int] = {
    replicaMgr.futureLocalLogOrException(topicPartition).latestEpoch
  }

  override protected def logStartOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.futureLocalLogOrException(topicPartition).logStartOffset
  }

  override protected def logEndOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.futureLocalLogOrException(topicPartition).logEndOffset
  }

  override protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Option[OffsetAndEpoch] = {
    replicaMgr.futureLocalLogOrException(topicPartition).endOffsetForEpoch(epoch)
  }

  // process fetched data
  override def processPartitionData(topicPartition: TopicPartition,
                                    fetchOffset: Long,
                                    partitionData: FetchData): Option[LogAppendInfo] = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    val futureLog = partition.futureLocalLogOrException
    val records = toMemoryRecords(FetchResponse.recordsOrFail(partitionData))

    if (fetchOffset != futureLog.logEndOffset)
      throw new IllegalStateException("Offset mismatch for the future replica %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, futureLog.logEndOffset))

    val logAppendInfo = if (records.sizeInBytes() > 0)
      partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = true)
    else
      None

    futureLog.updateHighWatermark(partitionData.highWatermark)
    futureLog.maybeIncrementLogStartOffset(partitionData.logStartOffset, LogStartOffsetIncrementReason.LeaderOffsetIncremented)

    directoryEventHandler match {
      case DirectoryEventHandler.NOOP =>
        if (partition.maybeReplaceCurrentWithFutureReplica())
          removePartitions(Set(topicPartition))
      case _ =>
        maybePromoteFutureReplica(topicPartition, partition)
    }

    quota.record(records.sizeInBytes)
    logAppendInfo
  }

  override def removePartitions(topicPartitions: Set[TopicPartition]): Map[TopicPartition, PartitionFetchState] = {
    for (topicPartition <- topicPartitions) {
      if (this.promotionStates.containsKey(topicPartition)) {
        val PromotionState(reassignmentState, topicId, originalDir) = this.promotionStates.get(topicPartition)
        // Revert any reassignments for partitions that did not complete the future replica promotion
        if (originalDir.isDefined && topicId.isDefined && reassignmentState.maybeInconsistentMetadata) {
          directoryEventHandler.handleAssignment(new TopicIdPartition(topicId.get, topicPartition.partition()), originalDir.get,
            "Reverting reassignment for canceled future replica", () => ())
        }
        this.promotionStates.remove(topicPartition)
      }
    }

    super.removePartitions(topicPartitions)
  }

  private def reassignmentState(topicPartition: TopicPartition): ReassignmentState = promotionStates.get(topicPartition).reassignmentState

  // Visible for testing
  private[server] def updateReassignmentState(topicPartition: TopicPartition, state: ReassignmentState): Unit = {
    log.debug(s"Updating future replica $topicPartition reassignment state to $state")
    promotionStates.put(topicPartition, promotionStates.get(topicPartition).withAssignment(state))
  }

  private def maybePromoteFutureReplica(topicPartition: TopicPartition, partition: Partition) = {
    val topicId = partition.topicId
    if (topicId.isEmpty)
      throw new IllegalStateException(s"Topic ${topicPartition.topic()} does not have an ID.")

    reassignmentState(topicPartition) match {
      case ReassignmentState.None =>
        // Schedule assignment request and don't promote the future replica yet until the controller has accepted the request.
        partition.runCallbackIfFutureReplicaCaughtUp(_ => {
          val targetDir = partition.futureReplicaDirectoryId().get
          val topicIdPartition = new TopicIdPartition(topicId.get, topicPartition.partition())
          directoryEventHandler.handleAssignment(topicIdPartition, targetDir, "Future replica promotion", () => updateReassignmentState(topicPartition, ReassignmentState.Accepted))
          updateReassignmentState(topicPartition, ReassignmentState.Queued)
        })
      case ReassignmentState.Accepted =>
        // Promote future replica if controller accepted the request and the replica caught-up with the original log.
        if (partition.maybeReplaceCurrentWithFutureReplica()) {
          updateReassignmentState(topicPartition, ReassignmentState.Effective)
          removePartitions(Set(topicPartition))
        }
      case ReassignmentState.Queued =>
        log.trace("Waiting for AssignReplicasToDirsRequest to succeed before promoting the future replica.")
      case ReassignmentState.Effective =>
        throw new IllegalStateException("BUG: trying to promote a future replica twice")
    }
  }

  override def addPartitions(initialFetchStates: Map[TopicPartition, InitialFetchState]): Set[TopicPartition] = {
    partitionMapLock.lockInterruptibly()
    try {
      // It is possible that the log dir fetcher completed just before this call, so we
      // filter only the partitions which still have a future log dir.
      val filteredFetchStates = initialFetchStates.filter { case (tp, _) =>
        replicaMgr.futureLogExists(tp)
      }
      filteredFetchStates.foreach {
        case (topicPartition, state) =>
          val topicId = state.topicId
          val currentDirectoryId = replicaMgr.getPartitionOrException(topicPartition).logDirectoryId()
          val promotionState = PromotionState(ReassignmentState.None, topicId, currentDirectoryId)
          promotionStates.put(topicPartition, promotionState)
      }
      super.addPartitions(filteredFetchStates)
    } finally {
      partitionMapLock.unlock()
    }
  }

  override protected val isOffsetForLeaderEpochSupported: Boolean = true

  /**
   * Truncate the log for each partition based on current replica's returned epoch and offset.
   *
   * The logic for finding the truncation offset is the same as in ReplicaFetcherThread
   * and mainly implemented in AbstractFetcherThread.getOffsetTruncationState. One difference is
   * that the initial fetch offset for topic partition could be set to the truncation offset of
   * the current replica if that replica truncates. Otherwise, it is high watermark as in ReplicaFetcherThread.
   *
   * The reason we have to follow the leader epoch approach for truncating a future replica is to
   * cover the case where a future replica is offline when the current replica truncates and
   * re-replicates offsets that may have already been copied to the future replica. In that case,
   * the future replica may miss "mark for truncation" event and must use the offset for leader epoch
   * exchange with the current replica to truncate to the largest common log prefix for the topic partition
   */
  override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.truncateTo(truncationState.offset, isFuture = true)
  }

  override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.truncateFullyAndStartAt(offset, isFuture = true)
  }
}
object ReplicaAlterLogDirsThread {
  /**
   * @param reassignmentState Tracks the state of the replica-to-directory assignment update in the metadata
   * @param topicId           The ID of the topic, which is useful if a reverting the assignment is required
   * @param currentDir        The original directory ID from which the future replica fetches from
   */
  case class PromotionState(reassignmentState: ReassignmentState, topicId: Option[Uuid], currentDir: Option[Uuid]) {
    def withAssignment(newDirReassignmentState: ReassignmentState): PromotionState =
      PromotionState(newDirReassignmentState, topicId, currentDir)
  }

  /**
   * Represents the state of the request to update the directory assignment from the current replica directory
   * to the future replica directory.
   */
  sealed trait ReassignmentState {
    /**
     * @return true if the directory assignment in the cluster metadata may be inconsistent with the actual
     *         directory where the main replica is hosted.
     */
    def maybeInconsistentMetadata: Boolean = false
  }

  object ReassignmentState {

    /**
     * The request has not been created.
     */
    case object None extends ReassignmentState

    /**
     * The request has been queued, it may or may not yet have been sent to the Controller.
     */
    case object Queued extends ReassignmentState {
      override def maybeInconsistentMetadata: Boolean = true
    }

    /**
     * The controller has acknowledged the new directory assignment and persisted the change in metadata.
     */
    case object Accepted extends ReassignmentState {
      override def maybeInconsistentMetadata: Boolean = true
    }

    /**
     * The future replica has been promoted and replaced the current replica.
     */
    case object Effective extends ReassignmentState
  }
}
