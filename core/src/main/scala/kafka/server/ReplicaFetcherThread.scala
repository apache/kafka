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

import kafka.log.{LeaderOffsetIncremented, LogAppendInfo, UnifiedLog}
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.server.epoch.EpochEntry
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests._
<<<<<<< HEAD
=======
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.common.{KafkaException, TopicPartition, Uuid}
>>>>>>> a5ddef6535 (Addressed review comments.)
import org.apache.kafka.server.common.CheckpointFile.CheckpointReadBuffer
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.log.remote.storage.{RemoteStorageException, RemoteStorageManager}

import java.io.{BufferedReader, File, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardCopyOption}
import scala.jdk.CollectionConverters.CollectionHasAsScala

import scala.collection.mutable

class ReplicaFetcherThread(name: String,
                           leader: LeaderEndPoint,
                           brokerConfig: KafkaConfig,
                           failedPartitions: FailedPartitions,
                           replicaMgr: ReplicaManager,
                           quota: ReplicaQuota,
                           logPrefix: String,
                           metadataVersionSupplier: () => MetadataVersion)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                leader = leader,
                                failedPartitions,
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false,
                                replicaMgr.brokerTopicStats) {

  this.logIdent = logPrefix

  // Visible for testing
  private[server] val partitionsWithNewHighWatermark = mutable.Buffer[TopicPartition]()

  override protected val isOffsetForLeaderEpochSupported: Boolean = metadataVersionSupplier().isOffsetForLeaderEpochSupported

  override protected def latestEpoch(topicPartition: TopicPartition): Option[Int] = {
    replicaMgr.localLogOrException(topicPartition).latestEpoch
  }

  override protected def logStartOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.localLogOrException(topicPartition).logStartOffset
  }

  override protected def logEndOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.localLogOrException(topicPartition).logEndOffset
  }

  override protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Option[OffsetAndEpoch] = {
    replicaMgr.localLogOrException(topicPartition).endOffsetForEpoch(epoch)
  }

  override def initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown()
    if (justShutdown) {
      // This is thread-safe, so we don't expect any exceptions, but catch and log any errors
      // to avoid failing the caller, especially during shutdown. We will attempt to close
      // leaderEndpoint after the thread terminates.
      try {
        leader.initiateClose()
      } catch {
        case t: Throwable =>
          error(s"Failed to initiate shutdown of $leader after initiating replica fetcher thread shutdown", t)
      }
    }
    justShutdown
  }

  override def awaitShutdown(): Unit = {
    super.awaitShutdown()
    // We don't expect any exceptions here, but catch and log any errors to avoid failing the caller,
    // especially during shutdown. It is safe to catch the exception here without causing correctness
    // issue because we are going to shutdown the thread and will not re-use the leaderEndpoint anyway.
    try {
      leader.close()
    } catch {
      case t: Throwable =>
        error(s"Failed to close $leader after shutting down replica fetcher thread", t)
    }
  }

  override def doWork(): Unit = {
    super.doWork()
    completeDelayedFetchRequests()
  }

  // process fetched data
  override def processPartitionData(topicPartition: TopicPartition,
                                    fetchOffset: Long,
                                    partitionData: FetchData): Option[LogAppendInfo] = {
    val logTrace = isTraceEnabled
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    val log = partition.localLogOrException
    val records = toMemoryRecords(FetchResponse.recordsOrFail(partitionData))

    maybeWarnIfOversizedRecords(records, topicPartition)

    if (fetchOffset != log.logEndOffset)
      throw new IllegalStateException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, log.logEndOffset))

    if (logTrace)
      trace("Follower has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
        .format(log.logEndOffset, topicPartition, records.sizeInBytes, partitionData.highWatermark))

    // Append the leader's messages to the log
    val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)

    if (logTrace)
      trace("Follower has replica log end offset %d after appending %d bytes of messages for partition %s"
        .format(log.logEndOffset, records.sizeInBytes, topicPartition))
    val leaderLogStartOffset = partitionData.logStartOffset

    // For the follower replica, we do not need to keep its segment base offset and physical position.
    // These values will be computed upon becoming leader or handling a preferred read replica fetch.
    var maybeUpdateHighWatermarkMessage = s"but did not update replica high watermark"
    log.maybeUpdateHighWatermark(partitionData.highWatermark).foreach { newHighWatermark =>
      maybeUpdateHighWatermarkMessage = s"and updated replica high watermark to $newHighWatermark"
      partitionsWithNewHighWatermark += topicPartition
    }

    log.maybeIncrementLogStartOffset(leaderLogStartOffset, LeaderOffsetIncremented)
    if (logTrace)
      trace(s"Follower received high watermark ${partitionData.highWatermark} from the leader " +
        s"$maybeUpdateHighWatermarkMessage for partition $topicPartition")

    // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
    // traffic doesn't exceed quota.
    if (quota.isThrottled(topicPartition))
      quota.record(records.sizeInBytes)

    if (partition.isReassigning && partition.isAddingLocalReplica)
      brokerTopicStats.updateReassignmentBytesIn(records.sizeInBytes)

    brokerTopicStats.updateReplicationBytesIn(records.sizeInBytes)

    logAppendInfo
  }

  private def completeDelayedFetchRequests(): Unit = {
    if (partitionsWithNewHighWatermark.nonEmpty) {
      replicaMgr.completeDelayedFetchRequests(partitionsWithNewHighWatermark.toSeq)
      partitionsWithNewHighWatermark.clear()
    }
  }

  def maybeWarnIfOversizedRecords(records: MemoryRecords, topicPartition: TopicPartition): Unit = {
    // oversized messages don't cause replication to fail from fetch request version 3 (KIP-74)
    if (metadataVersionSupplier().fetchRequestVersion <= 2 && records.sizeInBytes > 0 && records.validBytes <= 0)
      error(s"Replication is failing due to a message that is greater than replica.fetch.max.bytes for partition $topicPartition. " +
        "This generally occurs when the max.message.bytes has been overridden to exceed this value and a suitably large " +
        "message has also been sent. To fix this problem increase replica.fetch.max.bytes in your broker config to be " +
        "equal or larger than your settings for max.message.bytes, both at a broker and topic level.")
  }

  /**
   * Truncate the log for each partition's epoch based on leader's returned epoch and offset.
   * The logic for finding the truncation offset is implemented in AbstractFetcherThread.getOffsetTruncationState
   */
  override def truncate(tp: TopicPartition, offsetTruncationState: OffsetTruncationState): Unit = {
    val partition = replicaMgr.getPartitionOrException(tp)
    val log = partition.localLogOrException

    partition.truncateTo(offsetTruncationState.offset, isFuture = false)

    if (offsetTruncationState.offset < log.highWatermark)
      warn(s"Truncating $tp to offset ${offsetTruncationState.offset} below high watermark " +
        s"${log.highWatermark}")

    // mark the future replica for truncation only when we do last truncation
    if (offsetTruncationState.truncationCompleted)
      replicaMgr.replicaAlterLogDirsManager.markPartitionsForTruncation(brokerConfig.brokerId, tp,
        offsetTruncationState.offset)
  }

  override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.truncateFullyAndStartAt(offset, isFuture = false)
  }

  /**
   * It tries to build the required state for this partition from leader and remote storage so that it can start
   * fetching records from the leader.
   */
  override protected def buildRemoteLogAuxState(partition: TopicPartition,
                                                currentLeaderEpoch: Int,
                                                leaderLocalLogStartOffset: Long,
                                                epochForLeaderLocalLogStartOffset: Int,
                                                leaderLogStartOffset: Long): Unit = {

    def fetchEarlierEpochEndOffset(epoch:Int): EpochEndOffset = {
        val previousEpoch = epoch - 1
        // Find the end-offset for the epoch earlier to the given epoch from the leader
        val partitionsWithEpochs = Map(partition -> new EpochData().setPartition(partition.partition())
          .setCurrentLeaderEpoch(currentLeaderEpoch)
          .setLeaderEpoch(previousEpoch))
        val maybeEpochEndOffset = leader.fetchEpochEndOffsets(partitionsWithEpochs).get(partition)
        if (maybeEpochEndOffset.isEmpty) {
          throw new KafkaException("No response received for partition: " + partition);
        }

        val epochEndOffset = maybeEpochEndOffset.get
        if (epochEndOffset.errorCode() != Errors.NONE.code()) {
          throw Errors.forCode(epochEndOffset.errorCode()).exception()
        }

        epochEndOffset
    }

    replicaMgr.localLog(partition).foreach { log =>
      if (log.remoteStorageSystemEnable && log.config.remoteLogConfig.remoteStorageEnable) {
        replicaMgr.remoteLogManager.foreach { rlm =>

          // Find the respective leader epoch for (leaderLocalLogStartOffset - 1). We need to build the leader epoch cache
          // until that offset
          val highestOffsetInRemoteFromLeader = leaderLocalLogStartOffset - 1
          val targetEpoch: Int = {
            // If the existing epoch is 0, no need to fetch from earlier epoch as the desired offset(leaderLogStartOffset - 1)
            // will have the same epoch.
            if (epochForLeaderLocalLogStartOffset == 0) {
              epochForLeaderLocalLogStartOffset
            } else {
              // Fetch the earlier epoch/end-offset(exclusive) from the leader.
              val earlierEpochEndOffset = fetchEarlierEpochEndOffset(epochForLeaderLocalLogStartOffset)
              // Check if the target offset lies with in the range of earlier epoch. Here, epoch's end-offset is exclusive.
              if (earlierEpochEndOffset.endOffset > highestOffsetInRemoteFromLeader) {
                // Always use the leader epoch from returned earlierEpochEndOffset. This gives the respective leader
                // epoch, that will handle any gaps in epochs.
                // For ex:
                //  0 		20
                //  1 		85
                //  <2> - gap no messages were appended in this leader epoch.
                //  3 		90
                //  4 		98
                // There is a gap in leader epoch. For leaderLocalLogStartOffset as 85, leader-epoch is 3.
                // fetchEarlierEpochEndOffset(3) will return leader-epoch as 1, end-offset as 85.
                // So, we should use leader-epoch as 1, for offset 84.
                earlierEpochEndOffset.leaderEpoch()
              } else epochForLeaderLocalLogStartOffset
            }
          }

          val rlsMetadata = rlm.fetchRemoteLogSegmentMetadata(partition, targetEpoch, highestOffsetInRemoteFromLeader)

          if (rlsMetadata.isPresent) {
            val epochStream = rlm.storageManager().fetchIndex(rlsMetadata.get(), RemoteStorageManager.IndexType.LEADER_EPOCH)
            val epochs = readLeaderEpochCheckpoint(epochStream)

            // Truncate the existing local log before restoring the leader epoch cache and producer snapshots.
            truncateFullyAndStartAt(partition, leaderLocalLogStartOffset)

            log.maybeIncrementLogStartOffset(leaderLogStartOffset, LeaderOffsetIncremented)
            epochs.foreach { epochEntry =>
              log.leaderEpochCache.foreach(cache => cache.assign(epochEntry.epoch, epochEntry.startOffset))
            }
            info(s"Updated the epoch cache from remote tier till offset: $leaderLocalLogStartOffset " +
              s"with size: ${epochs.size} for $partition")

            // Restore producer snapshot
            val snapshotFile = UnifiedLog.producerSnapshotFile(log.dir, leaderLocalLogStartOffset)
            val tmpSnapshotFile = new File(snapshotFile.getAbsolutePath + ".tmp");
            // Copy it to snapshot file in atomic manner.
            Files.copy(rlm.storageManager().fetchIndex(rlsMetadata.get(), RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT),
              tmpSnapshotFile.toPath, StandardCopyOption.REPLACE_EXISTING)
            Utils.atomicMoveWithFallback(tmpSnapshotFile.toPath, snapshotFile.toPath, false)

            // Reload producer snapshots.
            log.producerStateManager.reloadSnapshots()
            log.loadProducerState(leaderLocalLogStartOffset, reloadFromCleanShutdown = false)
            info(s"Built the leader epoch cache and producer snapshots from remote tier for $partition. " +
              s"Active producers: ${log.producerStateManager.activeProducers.size}, LeaderLogStartOffset: $leaderLogStartOffset")
          } else {
            throw new RemoteStorageException(s"Couldn't build the state from remote store for partition: $partition, " +
              s"currentLeaderEpoch: $currentLeaderEpoch, leaderLocalLogStartOffset: $leaderLocalLogStartOffset, " +
              s"leaderLogStartOffset: $leaderLogStartOffset, epoch: $targetEpoch as the previous remote log segment " +
              s"metadata was not found")
          }
        }
      } else {
        // Truncate the existing local log  and start from leader's localLogStartOffset.
        truncateFullyAndStartAt(partition, leaderLocalLogStartOffset)
      }
    }
  }

  private def readLeaderEpochCheckpoint(stream: InputStream): collection.Seq[EpochEntry] = {
    val bufferedReader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
    try {
      val readBuffer = new CheckpointReadBuffer[EpochEntry]("", bufferedReader,  0, LeaderEpochCheckpointFile.Formatter)
      readBuffer.read().asScala.toSeq
    } finally {
      bufferedReader.close()
    }
  }

}
