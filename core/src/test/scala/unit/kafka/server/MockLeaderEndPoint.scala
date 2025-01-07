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

import kafka.server.AbstractFetcherThread.ReplicaFetch
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.server.network.BrokerEndPoint

import java.nio.ByteBuffer
import java.util.Optional
import scala.collection.{Map, Set, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.{RichOption, RichOptional}
import scala.util.Random

class MockLeaderEndPoint(sourceBroker: BrokerEndPoint = new BrokerEndPoint(1, "localhost", Random.nextInt()),
                         truncateOnFetch: Boolean = true,
                         version: Short = ApiKeys.FETCH.latestVersion())
  extends LeaderEndPoint {

  private val leaderPartitionStates = mutable.Map[TopicPartition, PartitionState]()
  var responseCallback: () => Unit = () => {}

  var replicaPartitionStateCallback: TopicPartition => Option[PartitionState] = { _ => Option.empty }
  var replicaId: Int = 0

  override val isTruncationOnFetchSupported: Boolean = truncateOnFetch

  def leaderPartitionState(topicPartition: TopicPartition): PartitionState = {
    leaderPartitionStates.getOrElse(topicPartition,
      throw new IllegalArgumentException(s"Unknown partition $topicPartition"))
  }

  def setLeaderState(topicPartition: TopicPartition, state: PartitionState): Unit = {
    leaderPartitionStates.put(topicPartition, state)
  }

  def setResponseCallback(callback: () => Unit): Unit = {
    responseCallback = callback
  }

  def setReplicaPartitionStateCallback(callback: TopicPartition => PartitionState): Unit = {
    replicaPartitionStateCallback = topicPartition => Some(callback(topicPartition))
  }

  def setReplicaId(replicaId: Int): Unit = {
    this.replicaId = replicaId
  }

  override def initiateClose(): Unit = {}

  override def close(): Unit = {}

  override def brokerEndPoint(): BrokerEndPoint = sourceBroker

  override def fetch(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData] = {
    fetchRequest.fetchData.asScala.map { case (partition, fetchData) =>
      val leaderState = leaderPartitionState(partition)
      val epochCheckError = checkExpectedLeaderEpoch(fetchData.currentLeaderEpoch, leaderState)
      val divergingEpoch = divergingEpochAndOffset(partition, fetchData.lastFetchedEpoch, fetchData.fetchOffset, leaderState)

      val (error, records) = if (epochCheckError.isDefined) {
        (epochCheckError.get, MemoryRecords.EMPTY)
      } else if (fetchData.fetchOffset > leaderState.logEndOffset || fetchData.fetchOffset < leaderState.logStartOffset) {
        (Errors.OFFSET_OUT_OF_RANGE, MemoryRecords.EMPTY)
      } else if (divergingEpoch.nonEmpty) {
        (Errors.NONE, MemoryRecords.EMPTY)
      } else if (leaderState.rlmEnabled && fetchData.fetchOffset < leaderState.localLogStartOffset) {
        (Errors.OFFSET_MOVED_TO_TIERED_STORAGE, MemoryRecords.EMPTY)
      } else {
        // for simplicity, we fetch only one batch at a time
        val records = leaderState.log.find(_.baseOffset >= fetchData.fetchOffset) match {
          case Some(batch) =>
            val buffer = ByteBuffer.allocate(batch.sizeInBytes)
            batch.writeTo(buffer)
            buffer.flip()
            MemoryRecords.readableRecords(buffer)

          case None =>
            MemoryRecords.EMPTY
        }

        (Errors.NONE, records)
      }
      val partitionData = new FetchData()
        .setPartitionIndex(partition.partition)
        .setErrorCode(error.code)
        .setHighWatermark(leaderState.highWatermark)
        .setLastStableOffset(leaderState.highWatermark)
        .setLogStartOffset(leaderState.logStartOffset)
        .setRecords(records)
      divergingEpoch.foreach(partitionData.setDivergingEpoch)

      (partition, partitionData)
    }.toMap
  }

  override def fetchEarliestOffset(topicPartition: TopicPartition, leaderEpoch: Int): OffsetAndEpoch = {
    val leaderState = leaderPartitionState(topicPartition)
    checkLeaderEpochAndThrow(leaderEpoch, leaderState)
    new OffsetAndEpoch(leaderState.logStartOffset, leaderState.leaderEpoch)
  }

  override def fetchLatestOffset(topicPartition: TopicPartition, leaderEpoch: Int): OffsetAndEpoch = {
    val leaderState = leaderPartitionState(topicPartition)
    checkLeaderEpochAndThrow(leaderEpoch, leaderState)
    new OffsetAndEpoch(leaderState.logEndOffset, leaderState.leaderEpoch)
  }

  override def fetchEarliestLocalOffset(topicPartition: TopicPartition, leaderEpoch: Int): OffsetAndEpoch = {
    val leaderState = leaderPartitionState(topicPartition)
    checkLeaderEpochAndThrow(leaderEpoch, leaderState)
    new OffsetAndEpoch(leaderState.localLogStartOffset, leaderState.leaderEpoch)
  }

  override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
    val endOffsets = mutable.Map[TopicPartition, EpochEndOffset]()
    partitions.foreachEntry { (partition, epochData) =>
      assert(partition.partition == epochData.partition,
        "Partition must be consistent between TopicPartition and EpochData")
      val leaderState = leaderPartitionState(partition)
      val epochEndOffset = lookupEndOffsetForEpoch(partition, epochData, leaderState)
      endOffsets.put(partition, epochEndOffset)
    }
    endOffsets
  }

  override def buildFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[ReplicaFetch]] = {
    val fetchData = mutable.Map.empty[TopicPartition, FetchRequest.PartitionData]
    partitionMap.foreach { case (partition, state) =>
      if (state.isReadyForFetch) {
        val replicaState = replicaPartitionStateCallback(partition).getOrElse(throw new IllegalArgumentException(s"Unknown partition $partition"))
        val lastFetchedEpoch = if (isTruncationOnFetchSupported)
          state.lastFetchedEpoch.map(_.asInstanceOf[Integer]).toJava
        else
          Optional.empty[Integer]
        fetchData.put(partition,
          new FetchRequest.PartitionData(state.topicId.getOrElse(Uuid.ZERO_UUID), state.fetchOffset, replicaState.logStartOffset,
            1024 * 1024, Optional.of[Integer](state.currentLeaderEpoch), lastFetchedEpoch))
      }
    }
    val fetchRequest = FetchRequest.Builder.forReplica(version, replicaId, 1, 0, 1, fetchData.asJava)
    val fetchRequestOpt =
      if (fetchData.isEmpty)
        None
      else
        Some(ReplicaFetch(fetchData.asJava, fetchRequest))
    ResultWithPartitions(fetchRequestOpt, Set.empty)
  }

  private def checkLeaderEpochAndThrow(expectedEpoch: Int, partitionState: PartitionState): Unit = {
    checkExpectedLeaderEpoch(expectedEpoch, partitionState).foreach { error =>
      throw error.exception()
    }
  }

  private def checkExpectedLeaderEpoch(expectedEpochOpt: Optional[Integer],
                                       partitionState: PartitionState): Option[Errors] = {
    if (expectedEpochOpt.isPresent) {
      checkExpectedLeaderEpoch(expectedEpochOpt.get, partitionState)
    } else {
      None
    }
  }

  private def checkExpectedLeaderEpoch(expectedEpoch: Int,
                                       partitionState: PartitionState): Option[Errors] = {
    if (expectedEpoch != RecordBatch.NO_PARTITION_LEADER_EPOCH) {
      if (expectedEpoch < partitionState.leaderEpoch)
        Some(Errors.FENCED_LEADER_EPOCH)
      else if (expectedEpoch > partitionState.leaderEpoch)
        Some(Errors.UNKNOWN_LEADER_EPOCH)
      else
        None
    } else {
      None
    }
  }

  private def divergingEpochAndOffset(topicPartition: TopicPartition,
                                      lastFetchedEpoch: Optional[Integer],
                                      fetchOffset: Long,
                                      partitionState: PartitionState): Option[FetchResponseData.EpochEndOffset] = {
    lastFetchedEpoch.toScala.flatMap { fetchEpoch =>
      val epochEndOffset = fetchEpochEndOffsets(
        Map(topicPartition -> new EpochData()
          .setPartition(topicPartition.partition)
          .setLeaderEpoch(fetchEpoch)))(topicPartition)

      if (partitionState.log.isEmpty
        || epochEndOffset.endOffset == UNDEFINED_EPOCH_OFFSET
        || epochEndOffset.leaderEpoch == UNDEFINED_EPOCH)
        None
      else if (epochEndOffset.leaderEpoch < fetchEpoch || epochEndOffset.endOffset < fetchOffset) {
        Some(new FetchResponseData.EpochEndOffset()
          .setEpoch(epochEndOffset.leaderEpoch)
          .setEndOffset(epochEndOffset.endOffset))
      } else
        None
    }
  }

  def lookupEndOffsetForEpoch(topicPartition: TopicPartition,
                              epochData: EpochData,
                              partitionState: PartitionState): EpochEndOffset = {
    checkExpectedLeaderEpoch(epochData.currentLeaderEpoch, partitionState).foreach { error =>
      return new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(error.code)
    }

    var epochLowerBound = UNDEFINED_EPOCH
    for (batch <- partitionState.log) {
      if (batch.partitionLeaderEpoch > epochData.leaderEpoch) {
        // If we don't have the requested epoch, return the next higher entry
        if (epochLowerBound == UNDEFINED_EPOCH)
          return new EpochEndOffset()
            .setPartition(topicPartition.partition)
            .setErrorCode(Errors.NONE.code)
            .setLeaderEpoch(batch.partitionLeaderEpoch)
            .setEndOffset(batch.baseOffset)
        else
          return new EpochEndOffset()
            .setPartition(topicPartition.partition)
            .setErrorCode(Errors.NONE.code)
            .setLeaderEpoch(epochLowerBound)
            .setEndOffset(batch.baseOffset)
      }
      epochLowerBound = batch.partitionLeaderEpoch
    }
    new EpochEndOffset()
      .setPartition(topicPartition.partition)
      .setErrorCode(Errors.NONE.code)
  }
}

class PartitionState(var log: mutable.Buffer[RecordBatch],
                     var leaderEpoch: Int,
                     var logStartOffset: Long,
                     var logEndOffset: Long,
                     var highWatermark: Long,
                     var rlmEnabled: Boolean = false,
                     var localLogStartOffset: Long)

object PartitionState {
  def apply(log: Seq[RecordBatch], leaderEpoch: Int, highWatermark: Long, rlmEnabled: Boolean = false): PartitionState = {
    val logStartOffset = log.headOption.map(_.baseOffset).getOrElse(0L)
    val logEndOffset = log.lastOption.map(_.nextOffset).getOrElse(0L)
    new PartitionState(log.toBuffer, leaderEpoch, logStartOffset, logEndOffset, highWatermark, rlmEnabled, logStartOffset)
  }

  def apply(leaderEpoch: Int): PartitionState = {
    apply(Seq(), leaderEpoch = leaderEpoch, highWatermark = 0L)
  }
}
