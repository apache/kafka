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

import kafka.server.QuotaFactory.UNBOUNDED_QUOTA
import kafka.utils.Logging
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.message.{FetchResponseData, OffsetForLeaderEpochRequestData}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, RequestUtils}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.server.{PartitionFetchState, ReplicaFetch, ResultWithPartitions}
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}

import java.util
import java.util.Optional
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

/**
 * Facilitates fetches from a local replica leader.
 *
 * @param sourceBroker The broker (host:port) that we want to connect to
 * @param brokerConfig A config file with broker related configurations
 * @param replicaManager A ReplicaManager
 * @param quota The quota, used when building a fetch request
 */
class LocalLeaderEndPoint(sourceBroker: BrokerEndPoint,
                          brokerConfig: KafkaConfig,
                          replicaManager: ReplicaManager,
                          quota: ReplicaQuota) extends LeaderEndPoint with Logging {

  private val replicaId = brokerConfig.brokerId
  private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
  private val fetchSize = brokerConfig.replicaFetchMaxBytes
  private var inProgressPartition: Option[TopicPartition] = None

  override val isTruncationOnFetchSupported: Boolean = false

  override def initiateClose(): Unit = {} // do nothing

  override def close(): Unit = {} // do nothing

  override def brokerEndPoint(): BrokerEndPoint = sourceBroker

  override def fetch(fetchRequest: FetchRequest.Builder): java.util.Map[TopicPartition, FetchResponseData.PartitionData] = {
    var partitionData: Seq[(TopicPartition, FetchResponseData.PartitionData)] = null
    val request = fetchRequest.build()

    // We can build the map from the request since it contains topic IDs and names.
    // Only one ID can be associated with a name and vice versa.
    val topicNames = new mutable.HashMap[Uuid, String]()
    request.data.topics.forEach { topic =>
      topicNames.put(topic.topicId, topic.topic)
    }

    def processResponseCallback(responsePartitionData: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      partitionData = responsePartitionData.map { case (tp, data) =>
        val abortedTransactions =  data.abortedTransactions.orElse(null)
        val lastStableOffset: Long = data.lastStableOffset.orElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        tp.topicPartition -> new FetchResponseData.PartitionData()
          .setPartitionIndex(tp.topicPartition.partition)
          .setErrorCode(data.error.code)
          .setHighWatermark(data.highWatermark)
          .setLastStableOffset(lastStableOffset)
          .setLogStartOffset(data.logStartOffset)
          .setAbortedTransactions(abortedTransactions)
          .setRecords(data.records)
      }
    }

    val fetchData = request.fetchData(topicNames.asJava)

    val fetchParams = new FetchParams(
      FetchRequest.FUTURE_LOCAL_REPLICA_ID,
      -1,
      0L, // timeout is 0 so that the callback will be executed immediately
      request.minBytes,
      request.maxBytes,
      FetchIsolation.LOG_END,
      Optional.empty()
    )

    replicaManager.fetchMessages(
      params = fetchParams,
      fetchInfos = fetchData.asScala.toSeq,
      quota = UNBOUNDED_QUOTA,
      responseCallback = processResponseCallback
    )

    if (partitionData == null)
      throw new IllegalStateException(s"Failed to fetch data for partitions ${fetchData.keySet().toArray.mkString(",")}")

    partitionData.toMap.asJava
  }

  override def fetchEarliestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = {
    val partition = replicaManager.getPartitionOrException(topicPartition)
    val logStartOffset = partition.localLogOrException.logStartOffset
    val epoch = partition.localLogOrException.leaderEpochCache.epochForOffset(logStartOffset)
    new OffsetAndEpoch(logStartOffset, epoch.orElse(0))
  }

  override def fetchLatestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = {
    val partition = replicaManager.getPartitionOrException(topicPartition)
    val logEndOffset = partition.localLogOrException.logEndOffset
    val epoch = partition.localLogOrException.leaderEpochCache.epochForOffset(logEndOffset)
    new OffsetAndEpoch(logEndOffset, epoch.orElse(0))
  }

  override def fetchEarliestLocalOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = {
    val partition = replicaManager.getPartitionOrException(topicPartition)
    val localLogStartOffset = partition.localLogOrException.localLogStartOffset()
    val epoch = partition.localLogOrException.leaderEpochCache.epochForOffset(localLogStartOffset)
    new OffsetAndEpoch(localLogStartOffset, epoch.orElse(0))
  }

  override def fetchEarliestPendingUploadOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = {
    val partition = replicaManager.getPartitionOrException(topicPartition)
    val log = partition.localLogOrException

    if (!log.remoteLogEnabled()) {
      new OffsetAndEpoch(-1L, -1)
    } else {
      val highestRemoteOffset = log.highestOffsetInRemoteStorage()
      val logStartOffset = fetchEarliestOffset(topicPartition, currentLeaderEpoch)
      val localLogStartOffset = fetchEarliestLocalOffset(topicPartition, currentLeaderEpoch)

      highestRemoteOffset match {
        case -1L =>
          if (localLogStartOffset.offset() == logStartOffset.offset()) {
            // No segments have been uploaded yet
            logStartOffset
          } else {
            // Leader currently does not know about the already uploaded segments
            new OffsetAndEpoch(-1L, -1)
          }
        case _ =>
          val earliestPendingUploadOffset = Math.max(highestRemoteOffset + 1, Math.max(logStartOffset.offset(), localLogStartOffset.offset()))
          val epoch = log.leaderEpochCache.epochForOffset(earliestPendingUploadOffset)
          new OffsetAndEpoch(earliestPendingUploadOffset, epoch.orElse(0))
      }
    }
  }

  override def fetchEpochEndOffsets(partitions: util.Map[TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition]): util.Map[TopicPartition, EpochEndOffset] = {
    partitions.asScala.map { case (tp, epochData) =>
      try {
        val endOffset = if (epochData.leaderEpoch == UNDEFINED_EPOCH) {
          new EpochEndOffset()
            .setPartition(tp.partition)
            .setErrorCode(Errors.NONE.code)
        } else {
          val partition = replicaManager.getPartitionOrException(tp)
          partition.lastOffsetForLeaderEpoch(
            currentLeaderEpoch = RequestUtils.getLeaderEpoch(epochData.currentLeaderEpoch),
            leaderEpoch = epochData.leaderEpoch,
            fetchOnlyFromLeader = false)
        }
        tp -> endOffset
      } catch {
        case t: Throwable =>
          warn(s"Error when getting EpochEndOffset for $tp", t)
          tp -> new EpochEndOffset()
            .setPartition(tp.partition)
            .setErrorCode(Errors.forException(t).code)
      }
    }.asJava
  }

  override def buildFetch(partitions: util.Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[util.Optional[ReplicaFetch]] = {
    // Only include replica in the fetch request if it is not throttled.
    if (quota.isQuotaExceeded) {
      new ResultWithPartitions(util.Optional.empty(), util.Set.of())
    } else {
      val selectPartition = selectPartitionToFetch(partitions)
      if (selectPartition.isPresent) {
        val (tp, fetchState) = selectPartition.get()
        buildFetchForPartition(tp, fetchState)
      } else {
        new ResultWithPartitions(util.Optional.empty(), util.Set.of())
      }
    }
  }

  private def selectPartitionToFetch(partitions: util.Map[TopicPartition, PartitionFetchState]): Optional[(TopicPartition, PartitionFetchState)] = {
    // Only move one partition at a time to increase its catch-up rate and thus reduce the time spent on
    // moving any given replica. Replicas are selected in ascending order (lexicographically by topic) from the
    // partitions that are ready to fetch. Once selected, we will continue fetching the same partition until it
    // becomes unavailable or is removed.

    inProgressPartition.foreach { tp =>
      val fetchStateOpt = Option(partitions.get(tp))
      fetchStateOpt.filter(_.isReadyForFetch).foreach { fetchState =>
        return Optional.of((tp, fetchState))
      }
    }

    inProgressPartition = None

    val nextPartitionOpt = nextReadyPartition(partitions.asScala.toMap)
    nextPartitionOpt.foreach { case (tp, fetchState) =>
      inProgressPartition = Some(tp)
      info(s"Beginning/resuming copy of partition $tp from offset ${fetchState.fetchOffset}. " +
        s"Including this partition, there are ${partitions.size} remaining partitions to copy by this thread.")
    }
    nextPartitionOpt match {
      case Some((tp, fetchState)) => Optional.of((tp, fetchState))
      case None => Optional.empty()
    }
  }

  private def buildFetchForPartition(topicPartition: TopicPartition, fetchState: PartitionFetchState): ResultWithPartitions[Optional[ReplicaFetch]] = {
    val requestMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val partitionsWithError = mutable.Set[TopicPartition]()

    try {
      val logStartOffset = replicaManager.futureLocalLogOrException(topicPartition).logStartOffset
      val lastFetchedEpoch = if (isTruncationOnFetchSupported)
        fetchState.lastFetchedEpoch
      else
        Optional.empty[Integer]
      val topicId = fetchState.topicId.orElse(Uuid.ZERO_UUID)
      requestMap.put(topicPartition, new FetchRequest.PartitionData(topicId, fetchState.fetchOffset, logStartOffset,
        fetchSize, Optional.of(fetchState.currentLeaderEpoch), lastFetchedEpoch))
    } catch {
      case e: KafkaStorageException =>
        debug(s"Failed to build fetch for $topicPartition", e)
        partitionsWithError += topicPartition
    }

    val fetchRequestOpt = if (requestMap.isEmpty) {
      Optional.empty[ReplicaFetch]()
    } else {
      val version: Short = if (fetchState.topicId.isEmpty)
        12
      else
        ApiKeys.FETCH.latestVersion
      // Set maxWait and minBytes to 0 because the response should return immediately if
      // the future log has caught up with the current log of the partition
      val requestBuilder = FetchRequest.Builder.forReplica(version, replicaId, -1, 0, 0, requestMap).setMaxBytes(maxBytes)
      Optional.of(new ReplicaFetch(requestMap, requestBuilder))
    }

    new ResultWithPartitions(fetchRequestOpt, partitionsWithError.asJava)
  }

  private def nextReadyPartition(partitions: Map[TopicPartition, PartitionFetchState]): Option[(TopicPartition, PartitionFetchState)] = {
    partitions.filter { case (_, partitionFetchState) =>
      partitionFetchState.isReadyForFetch
    }.reduceLeftOption { (left, right) =>
      if ((left._1.topic < right._1.topic) || (left._1.topic == right._1.topic && left._1.partition < right._1.partition))
        left
      else
        right
    }
  }

  override def toString: String = s"LocalLeaderEndPoint"
}
