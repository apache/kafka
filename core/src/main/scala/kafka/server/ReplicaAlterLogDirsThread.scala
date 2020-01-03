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

import java.util
import java.util.Optional

import kafka.api.Request
import kafka.cluster.BrokerEndPoint
import kafka.log.LogAppendInfo
import kafka.server.AbstractFetcherThread.ReplicaFetch
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.server.QuotaFactory.UnboundedQuota
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.FetchResponse.PartitionData
import org.apache.kafka.common.requests.{EpochEndOffset, FetchRequest, FetchResponse}

import scala.collection.JavaConverters._
import scala.collection.{mutable, Map, Seq, Set}

class ReplicaAlterLogDirsThread(name: String,
                                sourceBroker: BrokerEndPoint,
                                brokerConfig: KafkaConfig,
                                failedPartitions: FailedPartitions,
                                replicaMgr: ReplicaManager,
                                quota: ReplicationQuotaManager,
                                brokerTopicStats: BrokerTopicStats)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                failedPartitions,
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false,
                                brokerTopicStats) {

  private val replicaId = brokerConfig.brokerId
  private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
  private val fetchSize = brokerConfig.replicaFetchMaxBytes
  private var inProgressPartition: Option[TopicPartition] = None

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

  def fetchFromLeader(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData] = {
    var partitionData: Seq[(TopicPartition, FetchResponse.PartitionData[Records])] = null
    val request = fetchRequest.build()

    def processResponseCallback(responsePartitionData: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
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
      request.isolationLevel,
      None)

    if (partitionData == null)
      throw new IllegalStateException(s"Failed to fetch data for partitions ${request.fetchData.keySet().toArray.mkString(",")}")

    partitionData.toMap
  }

  // process fetched data
  override def processPartitionData(topicPartition: TopicPartition,
                                    fetchOffset: Long,
                                    partitionData: PartitionData[Records]): Option[LogAppendInfo] = {
    val partition = replicaMgr.nonOfflinePartition(topicPartition).get
    val futureLog = partition.futureLocalLogOrException
    val records = toMemoryRecords(partitionData.records)

    if (fetchOffset != futureLog.logEndOffset)
      throw new IllegalStateException("Offset mismatch for the future replica %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, futureLog.logEndOffset))

    val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = true)
    futureLog.updateHighWatermark(partitionData.highWatermark)
    futureLog.maybeIncrementLogStartOffset(partitionData.logStartOffset)

    if (partition.maybeReplaceCurrentWithFutureReplica())
      removePartitions(Set(topicPartition))

    quota.record(records.sizeInBytes)
    logAppendInfo
  }

  override protected def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition, leaderEpoch: Int): Long = {
    val partition = replicaMgr.getPartitionOrException(topicPartition, expectLeader = false)
    partition.localLogOrException.logStartOffset
  }

  override protected def fetchLatestOffsetFromLeader(topicPartition: TopicPartition, leaderEpoch: Int): Long = {
    val partition = replicaMgr.getPartitionOrException(topicPartition, expectLeader = false)
    partition.localLogOrException.logEndOffset
  }

  /**
   * Fetches offset for leader epoch from local replica for each given topic partitions
   * @param partitions map of topic partition -> leader epoch of the future replica
   * @return map of topic partition -> end offset for a requested leader epoch
   */
  override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
    partitions.map { case (tp, epochData) =>
      try {
        val endOffset = if (epochData.leaderEpoch == UNDEFINED_EPOCH) {
          new EpochEndOffset(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
        } else {
          val partition = replicaMgr.getPartitionOrException(tp, expectLeader = false)
          partition.lastOffsetForLeaderEpoch(
            currentLeaderEpoch = epochData.currentLeaderEpoch,
            leaderEpoch = epochData.leaderEpoch,
            fetchOnlyFromLeader = false)
        }
        tp -> endOffset
      } catch {
        case t: Throwable =>
          warn(s"Error when getting EpochEndOffset for $tp", t)
          tp -> new EpochEndOffset(Errors.forException(t), UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
    }
  }

  override protected def isOffsetForLeaderEpochSupported: Boolean = true

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
    val partition = replicaMgr.getPartitionOrException(topicPartition, expectLeader = false)
    partition.truncateTo(truncationState.offset, isFuture = true)
  }

  override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    val partition = replicaMgr.getPartitionOrException(topicPartition, expectLeader = false)
    partition.truncateFullyAndStartAt(offset, isFuture = true)
  }

  private def nextReadyPartition(partitionMap: Map[TopicPartition, PartitionFetchState]): Option[(TopicPartition, PartitionFetchState)] = {
    partitionMap.filter { case (_, partitionFetchState) =>
      partitionFetchState.isReadyForFetch
    }.reduceLeftOption { (left, right) =>
      if ((left._1.topic < right._1.topic) || (left._1.topic == right._1.topic && left._1.partition < right._1.partition))
        left
      else
        right
    }
  }

  private def selectPartitionToFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): Option[(TopicPartition, PartitionFetchState)] = {
    // Only move one partition at a time to increase its catch-up rate and thus reduce the time spent on
    // moving any given replica. Replicas are selected in ascending order (lexicographically by topic) from the
    // partitions that are ready to fetch. Once selected, we will continue fetching the same partition until it
    // becomes unavailable or is removed.

    inProgressPartition.foreach { tp =>
      val fetchStateOpt = partitionMap.get(tp)
      fetchStateOpt.filter(_.isReadyForFetch).foreach { fetchState =>
        return Some((tp, fetchState))
      }
    }

    inProgressPartition = None

    val nextPartitionOpt = nextReadyPartition(partitionMap)
    nextPartitionOpt.foreach { case (tp, fetchState) =>
      inProgressPartition = Some(tp)
      info(s"Beginning/resuming copy of partition $tp from offset ${fetchState.fetchOffset}. " +
        s"Including this partition, there are ${partitionMap.size} remaining partitions to copy by this thread.")
    }
    nextPartitionOpt
  }

  private def buildFetchForPartition(tp: TopicPartition, fetchState: PartitionFetchState): ResultWithPartitions[Option[ReplicaFetch]] = {
    val requestMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val partitionsWithError = mutable.Set[TopicPartition]()

    try {
      val logStartOffset = replicaMgr.futureLocalLogOrException(tp).logStartOffset
      requestMap.put(tp, new FetchRequest.PartitionData(fetchState.fetchOffset, logStartOffset,
        fetchSize, Optional.of(fetchState.currentLeaderEpoch)))
    } catch {
      case e: KafkaStorageException =>
        debug(s"Failed to build fetch for $tp", e)
        partitionsWithError += tp
    }

    val fetchRequestOpt = if (requestMap.isEmpty) {
      None
    } else {
      // Set maxWait and minBytes to 0 because the response should return immediately if
      // the future log has caught up with the current log of the partition
      val requestBuilder = FetchRequest.Builder.forReplica(replicaId, 0, 0, requestMap).setMaxBytes(maxBytes)
      Some(ReplicaFetch(requestMap, requestBuilder))
    }

    ResultWithPartitions(fetchRequestOpt, partitionsWithError)
  }

  def buildFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[ReplicaFetch]] = {
    // Only include replica in the fetch request if it is not throttled.
    if (quota.isQuotaExceeded) {
      ResultWithPartitions(None, Set.empty)
    } else {
      selectPartitionToFetch(partitionMap) match {
        case Some((tp, fetchState)) =>
          buildFetchForPartition(tp, fetchState)
        case None =>
          ResultWithPartitions(None, Set.empty)
      }
    }
  }

}
