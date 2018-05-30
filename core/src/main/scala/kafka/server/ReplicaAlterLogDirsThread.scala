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

import java.nio.ByteBuffer
import java.util

import AbstractFetcherThread.ResultWithPartitions
import kafka.cluster.BrokerEndPoint
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.{EpochEndOffset, FetchResponse, FetchRequest => JFetchRequest}
import ReplicaAlterLogDirsThread.FetchRequest
import ReplicaAlterLogDirsThread.PartitionData
import kafka.api.Request
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server.epoch.LeaderEpochCache
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{FileRecords, MemoryRecords}

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq, Set, mutable}

class ReplicaAlterLogDirsThread(name: String,
                                sourceBroker: BrokerEndPoint,
                                brokerConfig: KafkaConfig,
                                replicaMgr: ReplicaManager,
                                quota: ReplicationQuotaManager,
                                brokerTopicStats: BrokerTopicStats)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false,
                                includeLogTruncation = true) {

  type REQ = FetchRequest
  type PD = PartitionData

  private val replicaId = brokerConfig.brokerId
  private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
  private val fetchSize = brokerConfig.replicaFetchMaxBytes

  def fetch(fetchRequest: FetchRequest): Seq[(TopicPartition, PartitionData)] = {
    var partitionData: Seq[(TopicPartition, FetchResponse.PartitionData)] = null
    val request = fetchRequest.underlying.build()

    def processResponseCallback(responsePartitionData: Seq[(TopicPartition, FetchPartitionData)]) {
      partitionData = responsePartitionData.map { case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.map(_.asJava).orNull
        val lastStableOffset = data.lastStableOffset.getOrElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        tp -> new FetchResponse.PartitionData(data.error, data.highWatermark, lastStableOffset,
          data.logStartOffset, abortedTransactions, data.records)
      }
    }

    replicaMgr.fetchMessages(
      0L, // timeout is 0 so that the callback will be executed immediately
      Request.FutureLocalReplicaId,
      request.minBytes,
      request.maxBytes,
      request.version <= 2,
      request.fetchData.asScala.toSeq,
      UnboundedQuota,
      processResponseCallback,
      request.isolationLevel)

    if (partitionData == null)
      throw new IllegalStateException(s"Failed to fetch data for partitions ${request.fetchData.keySet().toArray.mkString(",")}")

    partitionData.map { case (key, value) =>
      key -> new PartitionData(value)
    }
  }

  // process fetched data
  def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PartitionData) {
    val futureReplica = replicaMgr.getReplicaOrException(topicPartition, Request.FutureLocalReplicaId)
    val partition = replicaMgr.getPartition(topicPartition).get
    val records = partitionData.toRecords

    if (fetchOffset != futureReplica.logEndOffset.messageOffset)
      throw new IllegalStateException("Offset mismatch for the future replica %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, futureReplica.logEndOffset.messageOffset))

    // Append the leader's messages to the log
    partition.appendRecordsToFutureReplica(records)
    val futureReplicaHighWatermark = futureReplica.logEndOffset.messageOffset.min(partitionData.highWatermark)
    futureReplica.highWatermark = new LogOffsetMetadata(futureReplicaHighWatermark)
    futureReplica.maybeIncrementLogStartOffset(partitionData.logStartOffset)

    if (partition.maybeReplaceCurrentWithFutureReplica())
      removePartitions(Set(topicPartition))

    quota.record(records.sizeInBytes)
  }

  def handleOffsetOutOfRange(topicPartition: TopicPartition): Long = {
    val futureReplica = replicaMgr.getReplicaOrException(topicPartition, Request.FutureLocalReplicaId)
    val currentReplica = replicaMgr.getReplicaOrException(topicPartition)
    val partition = replicaMgr.getPartition(topicPartition).get
    val logEndOffset: Long = currentReplica.logEndOffset.messageOffset

    if (logEndOffset < futureReplica.logEndOffset.messageOffset) {
      warn("Future replica for partition %s reset its fetch offset from %d to current replica's latest offset %d"
        .format(topicPartition, futureReplica.logEndOffset.messageOffset, logEndOffset))
      partition.truncateTo(logEndOffset, isFuture = true)
      logEndOffset
    } else {
      val currentReplicaStartOffset: Long = currentReplica.logStartOffset
      warn("Future replica for partition %s reset its fetch offset from %d to current replica's start offset %d"
        .format(topicPartition, futureReplica.logEndOffset.messageOffset, currentReplicaStartOffset))
      val offsetToFetch = Math.max(currentReplicaStartOffset, futureReplica.logEndOffset.messageOffset)
      // Only truncate the log when current replica's log start offset is greater than future replica's log end offset.
      if (currentReplicaStartOffset > futureReplica.logEndOffset.messageOffset)
        partition.truncateFullyAndStartAt(currentReplicaStartOffset, isFuture = true)
      offsetToFetch
    }
  }

  def handlePartitionsWithErrors(partitions: Iterable[TopicPartition]) {
    if (partitions.nonEmpty)
      delayPartitions(partitions, brokerConfig.replicaFetchBackoffMs.toLong)
  }

  /**
   * Builds offset for leader epoch requests for partitions that are in the truncating phase based
   * on latest epochs of the future replicas (the one that is fetching)
   */
  def buildLeaderEpochRequest(allPartitions: Seq[(TopicPartition, PartitionFetchState)]): ResultWithPartitions[Map[TopicPartition, Int]] = {
    def epochCacheOpt(tp: TopicPartition): Option[LeaderEpochCache] = replicaMgr.getReplica(tp, Request.FutureLocalReplicaId).map(_.epochs.get)

    val partitionEpochOpts = allPartitions
      .filter { case (_, state) => state.isTruncatingLog }
      .map { case (tp, _) => tp -> epochCacheOpt(tp) }.toMap

    val (partitionsWithEpoch, partitionsWithoutEpoch) = partitionEpochOpts.partition { case (_, epochCacheOpt) => epochCacheOpt.nonEmpty }

    val result = partitionsWithEpoch.map { case (tp, epochCacheOpt) => tp -> epochCacheOpt.get.latestEpoch() }
    ResultWithPartitions(result, partitionsWithoutEpoch.keys.toSet)
  }

  /**
   * Fetches offset for leader epoch from local replica for each given topic partitions
   * @param partitions map of topic partition -> leader epoch of the future replica
   * @return map of topic partition -> end offset for a requested leader epoch
   */
  def fetchEpochsFromLeader(partitions: Map[TopicPartition, Int]): Map[TopicPartition, EpochEndOffset] = {
    partitions.map { case (tp, epoch) =>
      try {
        val (leaderEpoch, leaderOffset) = replicaMgr.getReplicaOrException(tp).epochs.get.endOffsetFor(epoch)
        tp -> new EpochEndOffset(Errors.NONE, leaderEpoch, leaderOffset)
      } catch {
        case t: Throwable =>
          warn(s"Error when getting EpochEndOffset for $tp", t)
          tp -> new EpochEndOffset(Errors.forException(t), UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
    }
  }

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
  def maybeTruncate(fetchedEpochs: Map[TopicPartition, EpochEndOffset]): ResultWithPartitions[Map[TopicPartition, OffsetTruncationState]] = {
    val fetchOffsets = scala.collection.mutable.HashMap.empty[TopicPartition, OffsetTruncationState]
    val partitionsWithError = mutable.Set[TopicPartition]()

    fetchedEpochs.foreach { case (topicPartition, epochOffset) =>
      try {
        val futureReplica = replicaMgr.getReplicaOrException(topicPartition, Request.FutureLocalReplicaId)
        val partition = replicaMgr.getPartition(topicPartition).get

        if (epochOffset.hasError) {
          info(s"Retrying leaderEpoch request for partition $topicPartition as the current replica reported an error: ${epochOffset.error}")
          partitionsWithError += topicPartition
        } else {
          val offsetTruncationState = getOffsetTruncationState(topicPartition, epochOffset, futureReplica, isFutureReplica = true)

          partition.truncateTo(offsetTruncationState.offset, isFuture = true)
          fetchOffsets.put(topicPartition, offsetTruncationState)
        }
      } catch {
        case e: KafkaStorageException =>
          info(s"Failed to truncate $topicPartition", e)
          partitionsWithError += topicPartition
      }
    }
    ResultWithPartitions(fetchOffsets, partitionsWithError)
  }

  def buildFetchRequest(partitionMap: Seq[(TopicPartition, PartitionFetchState)]): ResultWithPartitions[FetchRequest] = {
    // Only include replica in the fetch request if it is not throttled.
    val maxPartitionOpt = partitionMap.filter { case (_, partitionFetchState) =>
      partitionFetchState.isReadyForFetch && !quota.isQuotaExceeded
    }.reduceLeftOption { (left, right) =>
      if ((left._1.topic > right._1.topic()) || (left._1.topic == right._1.topic() && left._1.partition() >= right._1.partition()))
        left
      else
        right
    }

    // Only move one replica at a time to increase its catch-up rate and thus reduce the time spent on moving any given replica
    // Replicas are ordered by their TopicPartition
    val requestMap = new util.LinkedHashMap[TopicPartition, JFetchRequest.PartitionData]
    val partitionsWithError = mutable.Set[TopicPartition]()

    if (maxPartitionOpt.nonEmpty) {
      val (topicPartition, partitionFetchState) = maxPartitionOpt.get
      try {
        val logStartOffset = replicaMgr.getReplicaOrException(topicPartition, Request.FutureLocalReplicaId).logStartOffset
        requestMap.put(topicPartition, new JFetchRequest.PartitionData(partitionFetchState.fetchOffset, logStartOffset, fetchSize))
      } catch {
        case _: KafkaStorageException =>
          partitionsWithError += topicPartition
      }
    }
    // Set maxWait and minBytes to 0 because the response should return immediately if
    // the future log has caught up with the current log of the partition
    val requestBuilder = JFetchRequest.Builder.forReplica(ApiKeys.FETCH.latestVersion, replicaId, 0, 0, requestMap).setMaxBytes(maxBytes)
    ResultWithPartitions(new FetchRequest(requestBuilder), partitionsWithError)
  }
}

object ReplicaAlterLogDirsThread {

  private[server] class FetchRequest(val underlying: JFetchRequest.Builder) extends AbstractFetcherThread.FetchRequest {
    def isEmpty: Boolean = underlying.fetchData.isEmpty
    def offset(topicPartition: TopicPartition): Long = underlying.fetchData.asScala(topicPartition).fetchOffset
    override def toString = underlying.toString
  }

  private[server] class PartitionData(val underlying: FetchResponse.PartitionData) extends AbstractFetcherThread.PartitionData {

    def error = underlying.error

    def toRecords: MemoryRecords = {
      if (underlying.records == MemoryRecords.EMPTY)
        underlying.records.asInstanceOf[MemoryRecords]
      else {
        val buffer = ByteBuffer.allocate(underlying.records.sizeInBytes())
        underlying.records.asInstanceOf[FileRecords].readInto(buffer, 0)
        MemoryRecords.readableRecords(buffer)
      }
    }

    def highWatermark: Long = underlying.highWatermark

    def logStartOffset: Long = underlying.logStartOffset

    def exception: Option[Throwable] = error match {
      case Errors.NONE => None
      case e => Some(e.exception)
    }

    override def toString = underlying.toString
  }
}
