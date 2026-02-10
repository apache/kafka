/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.common.record.internal._
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.server.ReplicaState
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.storage.internals.log.LogAppendInfo
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._

import java.util.Optional
import scala.collection.{Map, Set, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption

class MockFetcherThread(val mockLeader: MockLeaderEndPoint,
                        val mockTierStateMachine: MockTierStateMachine,
                        val replicaId: Int = 0,
                        val leaderId: Int = 1,
                        fetchBackOffMs: Int = 0,
                        failedPartitions: FailedPartitions = new FailedPartitions,
                        fetchFromLastTieredOffset: Boolean = false)
  extends AbstractFetcherThread("mock-fetcher",
    clientId = "mock-fetcher",
    leader = mockLeader,
    failedPartitions = failedPartitions,
    fetchTierStateMachine = mockTierStateMachine,
    fetchBackOffMs = fetchBackOffMs,
    brokerTopicStats = new BrokerTopicStats) {

  private val replicaPartitionStates = mutable.Map[TopicPartition, PartitionState]()
  private var latestEpochDefault: Optional[Integer] = Optional.of(0)

  mockTierStateMachine.setFetcher(this)

  def setReplicaState(topicPartition: TopicPartition, state: PartitionState): Unit = {
    replicaPartitionStates.put(topicPartition, state)
  }

  def replicaPartitionState(topicPartition: TopicPartition): PartitionState = {
    replicaPartitionStates.getOrElse(topicPartition,
      throw new IllegalArgumentException(s"Unknown partition $topicPartition"))
  }

  def addPartitions(initialFetchStates: Map[TopicPartition, InitialFetchState], forceTruncation: Boolean): Set[TopicPartition] = {
    latestEpochDefault = if (forceTruncation) Optional.empty else Optional.of(0)
    val partitions = super.addPartitions(initialFetchStates)
    latestEpochDefault = Optional.of(0)
    partitions
  }

  override def processPartitionData(
    topicPartition: TopicPartition,
    fetchOffset: Long,
    leaderEpochForReplica: Int,
    partitionData: FetchData
  ): Option[LogAppendInfo] = {
    val state = replicaPartitionState(topicPartition)

    if (leader.isTruncationOnFetchSupported && FetchResponse.isDivergingEpoch(partitionData)) {
      throw new IllegalStateException("processPartitionData should not be called for a partition with " +
        "a diverging epoch.")
    }

    // Throw exception if the fetchOffset does not match the fetcherThread partition state
    if (fetchOffset != state.logEndOffset)
      throw new RuntimeException(s"Offset mismatch for partition $topicPartition: " +
        s"fetched offset = $fetchOffset, log end offset = ${state.logEndOffset}.")

    // Now check message's crc
    val batches = FetchResponse.recordsOrFail(partitionData).batches.asScala
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var shallowOffsetOfMaxTimestamp = -1L
    var lastOffset = state.logEndOffset
    var lastEpoch: Optional[Integer] = Optional.empty()
    var skipRemainingBatches = false

    for (batch <- batches) {
      batch.ensureValid()

      skipRemainingBatches = skipRemainingBatches || hasHigherPartitionLeaderEpoch(batch, leaderEpochForReplica)
      if (skipRemainingBatches) {
        info(s"Skipping batch $batch because leader epoch is $leaderEpochForReplica")
      } else {
        if (batch.maxTimestamp > maxTimestamp) {
          maxTimestamp = batch.maxTimestamp
          shallowOffsetOfMaxTimestamp = batch.baseOffset
        }
        state.log.append(batch)
        state.logEndOffset = batch.nextOffset
        lastOffset = batch.lastOffset
        lastEpoch = Optional.of(batch.partitionLeaderEpoch)
      }
    }

    state.logStartOffset = partitionData.logStartOffset
    state.highWatermark = partitionData.highWatermark

    Some(new LogAppendInfo(fetchOffset,
      lastOffset,
      lastEpoch,
      maxTimestamp,
      Time.SYSTEM.milliseconds(),
      state.logStartOffset,
      RecordValidationStats.EMPTY,
      CompressionType.NONE,
      FetchResponse.recordsSize(partitionData),
      batches.headOption.map(_.lastOffset).getOrElse(-1)))
  }

  private def hasHigherPartitionLeaderEpoch(batch: RecordBatch, leaderEpoch: Int): Boolean = {
    batch.partitionLeaderEpoch() != RecordBatch.NO_PARTITION_LEADER_EPOCH &&
    batch.partitionLeaderEpoch() > leaderEpoch
  }

  override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
    val state = replicaPartitionState(topicPartition)
    state.log = state.log.takeWhile { batch =>
      batch.lastOffset < truncationState.offset
    }
    state.logEndOffset = state.log.lastOption.map(_.lastOffset + 1).getOrElse(state.logStartOffset)
    state.highWatermark = math.min(state.highWatermark, state.logEndOffset)
  }

  override def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    val state = replicaPartitionState(topicPartition)
    state.log.clear()
    if (state.rlmEnabled) {
      state.localLogStartOffset = offset
    } else {
      state.logStartOffset = offset
    }
    state.logEndOffset = offset
    state.highWatermark = offset
  }

  override def latestEpoch(topicPartition: TopicPartition): Optional[Integer] = {
    val state = replicaPartitionState(topicPartition)
    val partitionLeaderEpoch: Optional[Integer] = state.log.lastOption.toJava.map(_.partitionLeaderEpoch)
    if (partitionLeaderEpoch.isPresent)
      partitionLeaderEpoch
    else
      latestEpochDefault
  }

  override def logStartOffset(topicPartition: TopicPartition): Long = replicaPartitionState(topicPartition).logStartOffset

  override def logEndOffset(topicPartition: TopicPartition): Long = replicaPartitionState(topicPartition).logEndOffset

  override def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Optional[OffsetAndEpoch] = {
    val epochData = new EpochData()
      .setPartition(topicPartition.partition)
      .setLeaderEpoch(epoch)
    val result = mockLeader.lookupEndOffsetForEpoch(topicPartition, epochData, replicaPartitionState(topicPartition))
    if (result.endOffset == UNDEFINED_EPOCH_OFFSET)
      Optional.empty
    else
      Optional.of(new OffsetAndEpoch(result.endOffset, result.leaderEpoch))
  }

  def verifyLastFetchedEpoch(partition: TopicPartition, expectedEpoch: Option[Int]): Unit = {
    if (leader.isTruncationOnFetchSupported) {
      assertEquals(Some(ReplicaState.FETCHING), fetchState(partition).map(_.state))
      assertEquals(expectedEpoch, fetchState(partition).map(_.lastFetchedEpoch.get()))
    }
  }

  override def shouldFetchFromLastTieredOffset(topicPartition: TopicPartition, leaderEndOffset: Long, replicaEndOffset: Long): Boolean = fetchFromLastTieredOffset
}
