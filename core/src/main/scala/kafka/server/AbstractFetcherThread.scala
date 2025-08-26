/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import com.yammer.metrics.core.Meter
import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.PartitionStates
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.message.FetchResponseData.PartitionData
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, Records}
import org.apache.kafka.common.requests._

import org.apache.kafka.common.{ClientIdAndBroker, InvalidRecordException, TopicPartition, Uuid}
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.server.ReplicaState
import org.apache.kafka.server.PartitionFetchState
import org.apache.kafka.server.log.remote.storage.RetriableRemoteStorageException
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.ShutdownableThread
import org.apache.kafka.storage.internals.log.LogAppendInfo
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.nio.ByteBuffer
import java.util
import java.util.Optional
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import scala.collection.{Map, Set, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.{RichOption, RichOptional}

/**
 * Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class AbstractFetcherThread(name: String,
                                     clientId: String,
                                     val leader: LeaderEndPoint,
                                     failedPartitions: FailedPartitions,
                                     val fetchTierStateMachine: TierStateMachine,
                                     fetchBackOffMs: Int = 0,
                                     isInterruptible: Boolean = true,
                                     val brokerTopicStats: BrokerTopicStats) //BrokerTopicStats's lifecycle managed by ReplicaManager
  extends ShutdownableThread(name, isInterruptible) with Logging {

  this.logIdent = this.logPrefix

  type FetchData = FetchResponseData.PartitionData

  private val partitionStates = new PartitionStates[PartitionFetchState]
  protected val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()

  private val metricId = new ClientIdAndBroker(clientId, leader.brokerEndPoint().host, leader.brokerEndPoint().port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)

  /* callbacks to be defined in subclass */

  // process fetched data
  protected def processPartitionData(
    topicPartition: TopicPartition,
    fetchOffset: Long,
    partitionLeaderEpoch: Int,
    partitionData: FetchData
  ): Option[LogAppendInfo]

  protected def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit

  protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit

  protected def latestEpoch(topicPartition: TopicPartition): Optional[Integer]

  protected def logStartOffset(topicPartition: TopicPartition): Long

  protected def logEndOffset(topicPartition: TopicPartition): Long

  protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Optional[OffsetAndEpoch]

  override def shutdown(): Unit = {
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()

    // we don't need the lock since the thread has finished shutdown and metric removal is safe
    fetcherStats.unregister()
    fetcherLagStats.unregister()
  }

  override def doWork(): Unit = {
    maybeTruncate()
    maybeFetch()
  }

  private def maybeFetch(): Unit = {
    val fetchRequestOpt = inLock(partitionMapLock) {
      val result = leader.buildFetch(partitionStates.partitionStateMap)
      val fetchRequestOpt = result.result
      val partitionsWithError = result.partitionsWithError

      handlePartitionsWithErrors(partitionsWithError.asScala, "maybeFetch")

      if (fetchRequestOpt.isEmpty) {
        trace(s"There are no active partitions. Back off for $fetchBackOffMs ms before sending a fetch request")
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }

      fetchRequestOpt
    }

    fetchRequestOpt.ifPresent(replicaFetch =>
      processFetchRequest(replicaFetch.partitionData, replicaFetch.fetchRequest)
    )
  }

  // deal with partitions with errors, potentially due to leadership changes
  private def handlePartitionsWithErrors(partitions: Iterable[TopicPartition], methodName: String): Unit = {
    if (partitions.nonEmpty) {
      debug(s"Handling errors in $methodName for partitions $partitions")
      delayPartitions(partitions, fetchBackOffMs)
    }
  }

  private def maybeTruncate(): Unit = {
    val partitionsWithoutEpochs = getTruncatingPartitions()
    if (partitionsWithoutEpochs.nonEmpty) {
      truncateToHighWatermark(partitionsWithoutEpochs)
    }
  }

  private def getTruncatingPartitions(): Set[TopicPartition] = inLock(partitionMapLock) {
    val truncatingPartitions = mutable.Set.empty[TopicPartition]
    partitionStates.partitionStateMap.forEach { (tp, state) =>
      if (state.isTruncating) {
        truncatingPartitions += tp
      }
    }
    truncatingPartitions.toSet
  }

  private def doTruncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Boolean = {
    try {
      truncate(topicPartition, truncationState)
      true
    }
    catch {
      case e: KafkaStorageException =>
        error(s"Failed to truncate $topicPartition at offset ${truncationState.offset}", e)
        markPartitionFailed(topicPartition)
        false
      case t: Throwable =>
        error(s"Unexpected error occurred during truncation for $topicPartition "
          + s"at offset ${truncationState.offset}", t)
        markPartitionFailed(topicPartition)
        false
    }
  }

  // Visibility for unit tests
  protected[server] def truncateOnFetchResponse(epochEndOffsets: Map[TopicPartition, EpochEndOffset]): Unit = {
    inLock(partitionMapLock) {
      val fetchOffsets = mutable.HashMap.empty[TopicPartition, OffsetTruncationState]

      epochEndOffsets.foreachEntry { (tp, epochEndOffset) =>
        if (partitionStates.contains(tp)) {
          val truncationState = OffsetTruncationState(epochEndOffset.endOffset, truncationCompleted = true)
          info(s"Truncating partition $tp to offset ${epochEndOffset.endOffset} due to diverging epoch ${epochEndOffset.leaderEpoch}")

          if (doTruncate(tp, truncationState)) {
            fetchOffsets.put(tp, truncationState)
          }
        } else {
          trace(s"Ignoring epoch offsets for partition $tp since it has been removed from this fetcher thread.")
        }
      }

      updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets.toMap)
    }
  }

  // Visible for testing
  private[server] def truncateToHighWatermark(partitions: Set[TopicPartition]): Unit = inLock(partitionMapLock) {
    val fetchOffsets = mutable.HashMap.empty[TopicPartition, OffsetTruncationState]

    for (tp <- partitions) {
      val partitionState = partitionStates.stateValue(tp)
      if (partitionState != null) {
        val highWatermark = partitionState.fetchOffset
        val truncationState = OffsetTruncationState(highWatermark, truncationCompleted = true)

        info(s"Truncating partition $tp with $truncationState due to local high watermark $highWatermark")
        if (doTruncate(tp, truncationState))
          fetchOffsets.put(tp, truncationState)
      }
    }

    updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets)
  }

  /**
   * remove the partition if the partition state is NOT updated. Otherwise, keep the partition active.
   *
   * @return true if the epoch in this thread is updated. otherwise, false
   */
  private def onPartitionFenced(tp: TopicPartition, requestEpoch: Optional[Integer]): Boolean = inLock(partitionMapLock) {
    Option(partitionStates.stateValue(tp)).exists { currentFetchState =>
      val currentLeaderEpoch = currentFetchState.currentLeaderEpoch
      if (requestEpoch.isPresent && requestEpoch.get == currentLeaderEpoch) {
        info(s"Partition $tp has an older epoch ($currentLeaderEpoch) than the current leader. Will await " +
          s"the new LeaderAndIsr state before resuming fetching.")
        markPartitionFailed(tp)
        false
      } else {
        info(s"Partition $tp has a newer epoch ($currentLeaderEpoch) than the current leader. Retry the partition later.")
        true
      }
    }
  }

  // visible for testing
  private[server] def processFetchRequest(sessionPartitions: util.Map[TopicPartition, FetchRequest.PartitionData],
                                  fetchRequest: FetchRequest.Builder): Unit = {
    val partitionsWithError = mutable.Set[TopicPartition]()
    val divergingEndOffsets = mutable.Map.empty[TopicPartition, EpochEndOffset]
    var responseData: Map[TopicPartition, FetchData] = Map.empty

    try {
      trace(s"Sending fetch request $fetchRequest")
      responseData = leader.fetch(fetchRequest).asScala
    } catch {
      case t: Throwable =>
        if (isRunning) {
          warn(s"Error in response for fetch request $fetchRequest", t)
          inLock(partitionMapLock) {
            partitionsWithError ++= partitionStates.partitionSet.asScala
          }
        }
    }
    fetcherStats.requestRate.mark()

    if (responseData.nonEmpty) {
      // process fetched data
      inLock(partitionMapLock) {
        responseData.foreachEntry { (topicPartition, partitionData) =>
          Option(partitionStates.stateValue(topicPartition)).foreach { currentFetchState =>
            // It's possible that a partition is removed and re-added or truncated when there is a pending fetch request.
            // In this case, we only want to process the fetch response if:
            // - the partition state is ready for fetch
            // - the current offset is the same as the offset requested
            // - the current leader epoch is the same as the leader epoch requested
            val fetchPartitionData = sessionPartitions.get(topicPartition)
            if (fetchPartitionData != null &&
                fetchPartitionData.fetchOffset == currentFetchState.fetchOffset &&
                fetchPartitionData.currentLeaderEpoch.map[Boolean](_ == currentFetchState.currentLeaderEpoch).orElse(true) &&
                currentFetchState.isReadyForFetch) {
              Errors.forCode(partitionData.errorCode) match {
                case Errors.NONE =>
                  try {
                    if (FetchResponse.isDivergingEpoch(partitionData)) {
                      // If a diverging epoch is present, we truncate the log of the replica
                      // but we don't process the partition data in order to not update the
                      // low/high watermarks until the truncation is actually done. Those will
                      // be updated by the next fetch.
                      divergingEndOffsets += topicPartition -> new EpochEndOffset()
                        .setPartition(topicPartition.partition)
                        .setErrorCode(Errors.NONE.code)
                        .setLeaderEpoch(partitionData.divergingEpoch.epoch)
                        .setEndOffset(partitionData.divergingEpoch.endOffset)
                    } else {
                      /* Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                       *
                       * When appending batches to the log only append record batches up to the leader epoch when the FETCH
                       * request was handled. This is done to make sure that logs are not inconsistent because of log
                       * truncation and append after the FETCH request was handled. See KAFKA-18723 for more details.
                       */
                      val logAppendInfoOpt = processPartitionData(
                        topicPartition,
                        currentFetchState.fetchOffset,
                        currentFetchState.currentLeaderEpoch,
                        partitionData
                      )

                      logAppendInfoOpt.foreach { logAppendInfo =>
                        val validBytes = logAppendInfo.validBytes
                        val nextOffset = if (validBytes > 0) logAppendInfo.lastOffset + 1 else currentFetchState.fetchOffset
                        val lag = Math.max(0L, partitionData.highWatermark - nextOffset)
                        fetcherLagStats.getAndMaybePut(topicPartition).lag = lag

                        // ReplicaDirAlterThread may have removed topicPartition from the partitionStates after processing the partition data
                        if ((validBytes > 0 || currentFetchState.lag.isEmpty) && partitionStates.contains(topicPartition)) {
                          val lastFetchedEpoch =
                            if (logAppendInfo.lastLeaderEpoch.isPresent) logAppendInfo.lastLeaderEpoch else currentFetchState.lastFetchedEpoch
                          // Update partitionStates only if there is no exception during processPartitionData
                          val newFetchState = new PartitionFetchState(currentFetchState.topicId, nextOffset, Optional.of(lag),
                            currentFetchState.currentLeaderEpoch, ReplicaState.FETCHING, lastFetchedEpoch)
                          partitionStates.updateAndMoveToEnd(topicPartition, newFetchState)
                          if (validBytes > 0) fetcherStats.byteRate.mark(validBytes)
                        }
                      }
                    }
                  } catch {
                    case ime@(_: CorruptRecordException | _: InvalidRecordException) =>
                      // we log the error and continue. This ensures two things
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread
                      //    down and cause other topic partition to also lag
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes
                      //    can cause this), we simply continue and should get fixed in the subsequent fetches
                      error(s"Found invalid messages during fetch for partition $topicPartition " +
                        s"offset ${currentFetchState.fetchOffset}", ime)
                      partitionsWithError += topicPartition
                    case e: KafkaStorageException =>
                      error(s"Error while processing data for partition $topicPartition " +
                        s"at offset ${currentFetchState.fetchOffset}", e)
                      markPartitionFailed(topicPartition)
                    case t: Throwable =>
                      // stop monitoring this partition and add it to the set of failed partitions
                      error(s"Unexpected error occurred while processing data for partition $topicPartition " +
                        s"at offset ${currentFetchState.fetchOffset}", t)
                      markPartitionFailed(topicPartition)
                  }
                case Errors.OFFSET_OUT_OF_RANGE =>
                  if (!handleOutOfRangeError(topicPartition, currentFetchState, fetchPartitionData.currentLeaderEpoch))
                    partitionsWithError += topicPartition

                case Errors.UNKNOWN_LEADER_EPOCH =>
                  debug(s"Remote broker has a smaller leader epoch for partition $topicPartition than " +
                    s"this replica's current leader epoch of ${currentFetchState.currentLeaderEpoch}.")
                  partitionsWithError += topicPartition

                case Errors.FENCED_LEADER_EPOCH =>
                  if (onPartitionFenced(topicPartition, fetchPartitionData.currentLeaderEpoch))
                    partitionsWithError += topicPartition

                case Errors.OFFSET_MOVED_TO_TIERED_STORAGE =>
                  debug(s"Received error ${Errors.OFFSET_MOVED_TO_TIERED_STORAGE}, " +
                    s"at fetch offset: ${currentFetchState.fetchOffset}, " + s"topic-partition: $topicPartition")
                  if (!handleOffsetsMovedToTieredStorage(topicPartition, currentFetchState, fetchPartitionData.currentLeaderEpoch, partitionData))
                    partitionsWithError += topicPartition

                case Errors.NOT_LEADER_OR_FOLLOWER =>
                  debug(s"Remote broker is not the leader for partition $topicPartition, which could indicate " +
                    "that the partition is being moved")
                  partitionsWithError += topicPartition

                case Errors.UNKNOWN_TOPIC_OR_PARTITION =>
                  warn(s"Received ${Errors.UNKNOWN_TOPIC_OR_PARTITION} from the leader for partition $topicPartition. " +
                    "This error may be returned transiently when the partition is being created or deleted, but it is not " +
                    "expected to persist.")
                  partitionsWithError += topicPartition

                case Errors.UNKNOWN_TOPIC_ID =>
                  warn(s"Received ${Errors.UNKNOWN_TOPIC_ID} from the leader for partition $topicPartition. " +
                    "This error may be returned transiently when the partition is being created or deleted, but it is not " +
                    "expected to persist.")
                  partitionsWithError += topicPartition

                case Errors.INCONSISTENT_TOPIC_ID =>
                  warn(s"Received ${Errors.INCONSISTENT_TOPIC_ID} from the leader for partition $topicPartition. " +
                    "This error may be returned transiently when the partition is being created or deleted, but it is not " +
                    "expected to persist.")
                  partitionsWithError += topicPartition

                case partitionError =>
                  error(s"Error for partition $topicPartition at offset ${currentFetchState.fetchOffset}", partitionError.exception)
                  partitionsWithError += topicPartition
              }
            }
          }
        }
      }
    }

    if (divergingEndOffsets.nonEmpty)
      truncateOnFetchResponse(divergingEndOffsets)
    if (partitionsWithError.nonEmpty) {
      handlePartitionsWithErrors(partitionsWithError, "processFetchRequest")
    }
  }

  /**
   * This is used to mark partitions for truncation in ReplicaAlterLogDirsThread after leader
   * offsets are known.
   */
  def markPartitionsForTruncation(topicPartition: TopicPartition, truncationOffset: Long): Unit = {
    partitionMapLock.lockInterruptibly()
    try {
      Option(partitionStates.stateValue(topicPartition)).foreach { state =>
        val newState = new PartitionFetchState(state.topicId, math.min(truncationOffset, state.fetchOffset),
          state.lag, state.currentLeaderEpoch, state.delay, ReplicaState.TRUNCATING,
          Optional.empty())
        partitionStates.updateAndMoveToEnd(topicPartition, newState)
        partitionMapCond.signalAll()
      }
    } finally partitionMapLock.unlock()
  }

  private def markPartitionFailed(topicPartition: TopicPartition): Unit = {
    partitionMapLock.lock()
    try {
      failedPartitions.add(topicPartition)
      removePartitions(Set(topicPartition))
    } finally partitionMapLock.unlock()
    warn(s"Partition $topicPartition marked as failed")
  }

  /**
   * Returns initial partition fetch state based on current state and the provided `initialFetchState`.
   * From IBP 2.7 onwards, we can rely on truncation based on diverging data returned in fetch responses.
   * For older versions, we can skip the truncation step iff the leader epoch matches the existing epoch.
   */
  private def partitionFetchState(tp: TopicPartition, initialFetchState: InitialFetchState, currentState: PartitionFetchState): PartitionFetchState = {
    if (currentState != null && currentState.currentLeaderEpoch == initialFetchState.currentLeaderEpoch) {
      currentState
    } else if (initialFetchState.initOffset < 0) {
      fetchOffsetAndTruncate(tp, initialFetchState.topicId, initialFetchState.currentLeaderEpoch)
    } else {
      // With old message format, `latestEpoch` will be empty and we use Truncating state
      // to truncate to high watermark.
      val lastFetchedEpoch = latestEpoch(tp)
      val state = if (lastFetchedEpoch.isPresent) ReplicaState.FETCHING else ReplicaState.TRUNCATING
      new PartitionFetchState(initialFetchState.topicId.toJava, initialFetchState.initOffset, Optional.empty(), initialFetchState.currentLeaderEpoch,
        state, lastFetchedEpoch)
    }
  }

  def addPartitions(initialFetchStates: Map[TopicPartition, InitialFetchState]): Set[TopicPartition] = {
    partitionMapLock.lockInterruptibly()
    try {
      failedPartitions.removeAll(initialFetchStates.keySet)

      initialFetchStates.foreachEntry { (tp, initialFetchState) =>
        val currentState = partitionStates.stateValue(tp)
        val updatedState = partitionFetchState(tp, initialFetchState, currentState)
        partitionStates.updateAndMoveToEnd(tp, updatedState)
      }

      partitionMapCond.signalAll()
      initialFetchStates.keySet
    } finally partitionMapLock.unlock()
  }

  def maybeUpdateTopicIds(partitions: Set[TopicPartition], topicIds: String => Option[Uuid]): Unit = {
    partitionMapLock.lockInterruptibly()
    try {
      partitions.foreach { tp =>
        val currentState = partitionStates.stateValue(tp)
        if (currentState != null) {
          val updatedState = currentState.updateTopicId(topicIds(tp.topic).toJava)
          partitionStates.update(tp, updatedState)
        }
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  /**
   * Loop through all partitions, updating their fetch offset and maybe marking them as
   * truncation completed if their offsetTruncationState indicates truncation completed
   *
   * @param fetchOffsets the partitions to update fetch offset and maybe mark truncation complete
   */
  private def updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets: Map[TopicPartition, OffsetTruncationState]): Unit = {
    val newStates: Map[TopicPartition, PartitionFetchState] = partitionStates.partitionStateMap.asScala
      .map { case (topicPartition, currentFetchState) =>
        val maybeTruncationComplete = fetchOffsets.get(topicPartition) match {
          case Some(offsetTruncationState) =>
            val lastFetchedEpoch = latestEpoch(topicPartition)
            val state = if (offsetTruncationState.truncationCompleted)
              ReplicaState.FETCHING
            else
              ReplicaState.TRUNCATING
            new PartitionFetchState(currentFetchState.topicId, offsetTruncationState.offset, currentFetchState.lag,
              currentFetchState.currentLeaderEpoch, currentFetchState.delay, state, lastFetchedEpoch)
          case None => currentFetchState
        }
        (topicPartition, maybeTruncationComplete)
      }
    partitionStates.set(newStates.asJava)
  }

  /**
   * Handle a partition whose offset is out of range and return a new fetch offset.
   */
  private def fetchOffsetAndTruncate(topicPartition: TopicPartition, topicId: Option[Uuid], currentLeaderEpoch: Int): PartitionFetchState = {
    val replicaEndOffset = logEndOffset(topicPartition)

    /**
     * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
     * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
     * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
     * and it may discover that the current leader's end offset is behind its own end offset.
     *
     * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
     *
     * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
     */
    val offsetAndEpoch = leader.fetchLatestOffset(topicPartition, currentLeaderEpoch)
    val leaderEndOffset = offsetAndEpoch.offset
    if (leaderEndOffset < replicaEndOffset) {
      warn(s"Reset fetch offset for partition $topicPartition from $replicaEndOffset to current " +
        s"leader's latest offset $leaderEndOffset")
      truncate(topicPartition, OffsetTruncationState(leaderEndOffset, truncationCompleted = true))

      fetcherLagStats.getAndMaybePut(topicPartition).lag = 0
      new PartitionFetchState(topicId.toJava, leaderEndOffset, Optional.of(0L), currentLeaderEpoch,
        ReplicaState.FETCHING, latestEpoch(topicPartition))
    } else {
      /**
       * If the leader's log end offset is greater than the follower's log end offset, there are two possibilities:
       * 1. The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
       * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
       * 2. When unclean leader election occurs, it is possible that the old leader's high watermark is greater than
       * the new leader's log end offset. So when the old leader truncates its offset to its high watermark and starts
       * to fetch from the new leader, an OffsetOutOfRangeException will be thrown. After that some more messages are
       * produced to the new leader. While the old leader is trying to handle the OffsetOutOfRangeException and query
       * the log end offset of the new leader, the new leader's log end offset becomes higher than the follower's log end offset.
       *
       * In the first case, if the follower's current log end offset is smaller than the leader's log start offset, the
       * follower should truncate all its logs, roll out a new segment and start to fetch from the current leader's log
       * start offset since the data are all stale.
       * In the second case, the follower should just keep the current log segments and retry the fetch. In the second
       * case, there will be some inconsistency of data between old and new leader. We are not solving it here.
       * If users want to have strong consistency guarantees, appropriate configurations needs to be set for both
       * brokers and producers.
       *
       * Putting the two cases together, the follower should fetch from the higher one of its replica log end offset
       * and the current leader's log start offset.
       */
      val offsetAndEpoch = leader.fetchEarliestOffset(topicPartition, currentLeaderEpoch)
      val leaderStartOffset = offsetAndEpoch.offset
      val offsetToFetch = Math.max(leaderStartOffset, replicaEndOffset)
      // Only truncate log when current leader's log start offset is greater than follower's log end offset.
      if (leaderStartOffset > replicaEndOffset) {
        warn(s"Truncate fully and reset fetch offset for partition $topicPartition from $replicaEndOffset to the " +
          s"current leader's start offset $leaderStartOffset because the local replica's end offset is smaller than the " +
          s"current leader's start offsets.")
        truncateFullyAndStartAt(topicPartition, leaderStartOffset)
      } else {
        info(s"Reset fetch offset for partition $topicPartition from $replicaEndOffset to " +
          s"the current local replica's end offset $offsetToFetch")
      }

      val initialLag = leaderEndOffset - offsetToFetch
      fetcherLagStats.getAndMaybePut(topicPartition).lag = initialLag
      new PartitionFetchState(topicId.toJava, offsetToFetch, Optional.of(initialLag), currentLeaderEpoch,
        ReplicaState.FETCHING, latestEpoch(topicPartition))
    }
  }

  /**
   * Handles the out of range error for the given topic partition.
   *
   * Returns true if
   *    - the request succeeded or
   *    - it was fenced and this thread hasn't received new epoch, which means we need not backoff and retry as the
   *    partition is moved to failed state.
   *
   * Returns false if there was a retriable error.
   *
   * @param topicPartition topic partition
   * @param fetchState current fetch state
   * @param leaderEpochInRequest current leader epoch sent in the fetch request.
   */
  private def handleOutOfRangeError(topicPartition: TopicPartition,
                                    fetchState: PartitionFetchState,
                                    leaderEpochInRequest: Optional[Integer]): Boolean = {
    try {
      val newFetchState = fetchOffsetAndTruncate(topicPartition, fetchState.topicId.toScala, fetchState.currentLeaderEpoch)
      partitionStates.updateAndMoveToEnd(topicPartition, newFetchState)
      info(s"Current offset ${fetchState.fetchOffset} for partition $topicPartition is " +
        s"out of range, which typically implies a leader change. Reset fetch offset to ${newFetchState.fetchOffset}")
      true
    } catch {
      case _: FencedLeaderEpochException =>
        onPartitionFenced(topicPartition, leaderEpochInRequest)

      case e@(_: UnknownTopicOrPartitionException |
              _: UnknownLeaderEpochException |
              _: NotLeaderOrFollowerException) =>
        info(s"Could not fetch offset for $topicPartition due to error: ${e.getMessage}")
        false

      case e: Throwable =>
        error(s"Error getting offset for partition $topicPartition", e)
        false
    }
  }

  /**
   * Handles the offset moved to tiered storage error for the given topic partition.
   *
   * Returns true if
   *    - the request succeeded or
   *    - it was fenced and this thread haven't received new epoch, which means we need not backoff and retry as the
   *    partition is moved to failed state.
   *
   * Returns false if there was a retriable error.
   *
   * @param topicPartition topic partition
   * @param fetchState current partition fetch state
   * @param leaderEpochInRequest current leader epoch sent in the fetch request
   * @param fetchPartitionData the fetch response data for this topic partition
   */
  private def handleOffsetsMovedToTieredStorage(topicPartition: TopicPartition,
                                                fetchState: PartitionFetchState,
                                                leaderEpochInRequest: Optional[Integer],
                                                fetchPartitionData: PartitionData): Boolean = {
    try {
      val newFetchState = fetchTierStateMachine.start(topicPartition, fetchState, fetchPartitionData)

      // TODO: use fetchTierStateMachine.maybeAdvanceState when implementing async tiering logic in KAFKA-13560

      fetcherLagStats.getAndMaybePut(topicPartition).lag = newFetchState.lag.orElse(0L)
      partitionStates.updateAndMoveToEnd(topicPartition, newFetchState)
      debug(s"Current offset ${fetchState.fetchOffset} for partition $topicPartition is " +
        s"out of range or moved to remote tier. Reset fetch offset to ${newFetchState.fetchOffset}")
      true
    } catch {
      case _: FencedLeaderEpochException =>
        onPartitionFenced(topicPartition, leaderEpochInRequest)
      case e@(_: UnknownTopicOrPartitionException |
              _: UnknownLeaderEpochException |
              _: NotLeaderOrFollowerException |
              _: RetriableRemoteStorageException) =>
        info(s"Could not build remote log auxiliary state for $topicPartition due to error: ${e.getMessage}")
        false
      case e: Throwable =>
        error(s"Error building remote log auxiliary state for $topicPartition", e)
        false
    }
  }

  private def delayPartitions(partitions: Iterable[TopicPartition], delay: Long): Unit = {
    partitionMapLock.lockInterruptibly()
    try {
      for (partition <- partitions) {
        Option(partitionStates.stateValue(partition)).foreach { currentFetchState =>
          if (!currentFetchState.isDelayed) {
            partitionStates.updateAndMoveToEnd(partition,
              new PartitionFetchState(
                currentFetchState.topicId,
                currentFetchState.fetchOffset,
                currentFetchState.lag,
                currentFetchState.currentLeaderEpoch,
                Optional.of(delay),
                currentFetchState.state,
                currentFetchState.lastFetchedEpoch))
          }
        }
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def removePartitions(topicPartitions: Set[TopicPartition]): Map[TopicPartition, PartitionFetchState] = {
    partitionMapLock.lockInterruptibly()
    try {
      topicPartitions.map { topicPartition =>
        val state = partitionStates.stateValue(topicPartition)
        partitionStates.remove(topicPartition)
        fetcherLagStats.unregister(topicPartition)
        topicPartition -> state
      }.filter(_._2 != null).toMap
    } finally partitionMapLock.unlock()
  }

  def removeAllPartitions(): Map[TopicPartition, PartitionFetchState] = {
    partitionMapLock.lockInterruptibly()
    try {
      val allPartitionState = partitionStates.partitionStateMap.asScala.toMap
      allPartitionState.keys.foreach { tp =>
        partitionStates.remove(tp)
        fetcherLagStats.unregister(tp)
      }
      allPartitionState
    } finally partitionMapLock.unlock()
  }

  def partitionCount: Int = {
    partitionMapLock.lockInterruptibly()
    try partitionStates.size
    finally partitionMapLock.unlock()
  }

  def partitions: Set[TopicPartition] = {
    partitionMapLock.lockInterruptibly()
    try partitionStates.partitionSet.asScala.toSet
    finally partitionMapLock.unlock()
  }

  // Visible for testing
  private[server] def fetchState(topicPartition: TopicPartition): Option[PartitionFetchState] = inLock(partitionMapLock) {
    Option(partitionStates.stateValue(topicPartition))
  }

  protected def toMemoryRecords(records: Records): MemoryRecords = {
    (records: @unchecked) match {
      case r: MemoryRecords => r
      case r: FileRecords =>
        val buffer = ByteBuffer.allocate(r.sizeInBytes)
        r.readInto(buffer, 0)
        MemoryRecords.readableRecords(buffer)
    }
  }
}

object FetcherMetrics {
  val ConsumerLag = "ConsumerLag"
  val RequestsPerSec = "RequestsPerSec"
  val BytesPerSec = "BytesPerSec"
}

class FetcherLagMetrics(metricId: ClientIdTopicPartition) {
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  private[this] val lagVal = new AtomicLong(-1L)
  private[this] val tags = Map(
    "clientId" -> metricId.clientId,
    "topic" -> metricId.topicPartition.topic,
    "partition" -> metricId.topicPartition.partition.toString).asJava

  metricsGroup.newGauge(FetcherMetrics.ConsumerLag, () => lagVal.get, tags)

  def lag_=(newLag: Long): Unit = {
    lagVal.set(newLag)
  }

  def lag: Long = lagVal.get

  def unregister(): Unit = {
    metricsGroup.removeMetric(FetcherMetrics.ConsumerLag, tags)
  }
}

class FetcherLagStats(metricId: ClientIdAndBroker) {
  val stats = new ConcurrentHashMap[TopicPartition, FetcherLagMetrics]

  def getAndMaybePut(topicPartition: TopicPartition): FetcherLagMetrics = {
    stats.computeIfAbsent(topicPartition, k => new FetcherLagMetrics(ClientIdTopicPartition(metricId.clientId, k)))
  }

  def unregister(topicPartition: TopicPartition): Unit = {
    val lagMetrics = stats.remove(topicPartition)
    if (lagMetrics != null) lagMetrics.unregister()
  }

  def unregister(): Unit = {
    stats.forEach((key, _) => unregister(key))
  }
}

class FetcherStats(metricId: ClientIdAndBroker) {
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val tags: util.Map[String, String] = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString).asJava

  val requestRate: Meter = metricsGroup.newMeter(FetcherMetrics.RequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  val byteRate: Meter = metricsGroup.newMeter(FetcherMetrics.BytesPerSec, "bytes", TimeUnit.SECONDS, tags)

  def unregister(): Unit = {
    metricsGroup.removeMetric(FetcherMetrics.RequestsPerSec, tags)
    metricsGroup.removeMetric(FetcherMetrics.BytesPerSec, tags)
  }

}

case class ClientIdTopicPartition(clientId: String, topicPartition: TopicPartition) {
  override def toString: String = s"$clientId-$topicPartition"
}

case class OffsetTruncationState(offset: Long, truncationCompleted: Boolean) {

  def this(offset: Long) = this(offset, true)

  override def toString: String = s"TruncationState(offset=$offset, completed=$truncationCompleted)"
}
