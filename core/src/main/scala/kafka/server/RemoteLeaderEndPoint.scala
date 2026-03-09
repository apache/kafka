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

import java.util.{Collections, Optional}
import kafka.utils.Logging
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.message.{FetchResponseData, OffsetForLeaderEpochRequestData}
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.{OffsetForLeaderTopic, OffsetForLeaderTopicCollection}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, OffsetsForLeaderEpochRequest, OffsetsForLeaderEpochResponse}
import org.apache.kafka.server.common.{MetadataVersion, OffsetAndEpoch}
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.server.{PartitionFetchState, ReplicaFetch, ResultWithPartitions}

import scala.jdk.CollectionConverters._
import scala.collection.mutable

/**
 * Facilitates fetches from a remote replica leader.
 *
 * @param logPrefix The log prefix
 * @param blockingSender The raw leader endpoint used to communicate with the leader
 * @param fetchSessionHandler A FetchSessionHandler to track the partitions in the session
 * @param brokerConfig Broker configuration
 * @param replicaManager A ReplicaManager
 * @param quota The quota, used when building a fetch request
 * @param metadataVersionSupplier A supplier that returns the current MetadataVersion. This can change during
 *                                runtime in KRaft mode.
 */
class RemoteLeaderEndPoint(logPrefix: String,
                           blockingSender: BlockingSend,
                           private[server] val fetchSessionHandler: FetchSessionHandler, // visible for testing
                           brokerConfig: KafkaConfig,
                           replicaManager: ReplicaManager,
                           quota: ReplicaQuota,
                           metadataVersionSupplier: () => MetadataVersion,
                           brokerEpochSupplier: () => Long) extends LeaderEndPoint with Logging {

  this.logIdent = logPrefix

  private val maxWait = brokerConfig.replicaFetchWaitMaxMs
  private val minBytes = brokerConfig.replicaFetchMinBytes
  private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
  private val fetchSize = brokerConfig.replicaFetchMaxBytes

  override def isTruncationOnFetchSupported: Boolean = true

  override def initiateClose(): Unit = blockingSender.initiateClose()

  override def close(): Unit = blockingSender.close()

  override def brokerEndPoint(): BrokerEndPoint = blockingSender.brokerEndPoint()

  override def fetch(fetchRequest: FetchRequest.Builder): java.util.Map[TopicPartition, FetchResponseData.PartitionData] = {
    val clientResponse = try {
      blockingSender.sendRequest(fetchRequest)
    } catch {
      case t: Throwable =>
        fetchSessionHandler.handleError(t)
        throw t
    }
    val fetchResponse = clientResponse.responseBody.asInstanceOf[FetchResponse]
    if (!fetchSessionHandler.handleResponse(fetchResponse, clientResponse.requestHeader().apiVersion())) {
      // If we had a session topic ID related error, throw it, otherwise return an empty fetch data map.
      if (fetchResponse.error == Errors.FETCH_SESSION_TOPIC_ID_ERROR) {
        throw Errors.forCode(fetchResponse.error().code()).exception()
      } else {
        java.util.Map.of()
      }
    } else {
      fetchResponse.responseData(fetchSessionHandler.sessionTopicNames, clientResponse.requestHeader().apiVersion())
    }
  }

  override def fetchEarliestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = {
    fetchOffset(topicPartition, currentLeaderEpoch, ListOffsetsRequest.EARLIEST_TIMESTAMP)
  }

  override def fetchLatestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = {
    fetchOffset(topicPartition, currentLeaderEpoch, ListOffsetsRequest.LATEST_TIMESTAMP)
  }

  override def fetchEarliestLocalOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = {
    fetchOffset(topicPartition, currentLeaderEpoch, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)
  }

  override def fetchEarliestPendingUploadOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = {
    fetchOffset(topicPartition, currentLeaderEpoch, ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP)
  }

  private def fetchOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int, timestamp: Long): OffsetAndEpoch = {
    val topic = new ListOffsetsTopic()
      .setName(topicPartition.topic)
      .setPartitions(Collections.singletonList(
        new ListOffsetsPartition()
          .setPartitionIndex(topicPartition.partition)
          .setCurrentLeaderEpoch(currentLeaderEpoch)
          .setTimestamp(timestamp)))
    val metadataVersion = metadataVersionSupplier()
    val requestBuilder = ListOffsetsRequest.Builder.forReplica(metadataVersion.listOffsetRequestVersion, brokerConfig.brokerId)
      .setTargetTimes(Collections.singletonList(topic))

    val clientResponse = blockingSender.sendRequest(requestBuilder)
    val response = clientResponse.responseBody.asInstanceOf[ListOffsetsResponse]
    val responsePartition = response.topics.asScala.find(_.name == topicPartition.topic).get
      .partitions.asScala.find(_.partitionIndex == topicPartition.partition).get

    Errors.forCode(responsePartition.errorCode) match {
      case Errors.NONE => new OffsetAndEpoch(responsePartition.offset, responsePartition.leaderEpoch)
      case error => throw error.exception
    }
  }

  override def fetchEpochEndOffsets(partitions: java.util.Map[TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition]): java.util.Map[TopicPartition, EpochEndOffset] = {
    if (partitions.isEmpty) {
      debug("Skipping leaderEpoch request since all partitions do not have an epoch")
      return java.util.Map.of()
    }

    val topics = new OffsetForLeaderTopicCollection(partitions.size)
    partitions.forEach { (topicPartition, epochData) =>
      var topic = topics.find(topicPartition.topic)
      if (topic == null) {
        topic = new OffsetForLeaderTopic().setTopic(topicPartition.topic)
        topics.add(topic)
      }
      topic.partitions.add(epochData)
    }

    val epochRequest = OffsetsForLeaderEpochRequest.Builder.forFollower(topics, brokerConfig.brokerId)
    debug(s"Sending offset for leader epoch request $epochRequest")

    try {
      val response = blockingSender.sendRequest(epochRequest)
      val responseBody = response.responseBody.asInstanceOf[OffsetsForLeaderEpochResponse]
      debug(s"Received leaderEpoch response $response")
      responseBody.data.topics.asScala.flatMap { offsetForLeaderTopicResult =>
        offsetForLeaderTopicResult.partitions.asScala.map { offsetForLeaderPartitionResult =>
          val tp = new TopicPartition(offsetForLeaderTopicResult.topic, offsetForLeaderPartitionResult.partition)
          tp -> offsetForLeaderPartitionResult
        }
      }.toMap.asJava
    } catch {
      case t: Throwable =>
        warn(s"Error when sending leader epoch request for $partitions", t)

        // if we get any unexpected exception, mark all partitions with an error
        val error = Errors.forException(t)
        partitions.asScala.map { case (tp, _) =>
          tp -> new EpochEndOffset()
            .setPartition(tp.partition)
            .setErrorCode(error.code)
        }.asJava
    }
  }

  override def buildFetch(partitions: java.util.Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[java.util.Optional[ReplicaFetch]] = {
    val partitionsWithError = mutable.Set[TopicPartition]()
    val builder = fetchSessionHandler.newBuilder(partitions.size, false)
    partitions.forEach { (topicPartition, fetchState) =>
      // We will not include a replica in the fetch request if it should be throttled.
      if (fetchState.isReadyForFetch && !shouldFollowerThrottle(quota, fetchState, topicPartition)) {
        try {
          val logStartOffset = replicaManager.localLogOrException(topicPartition).logStartOffset
          val lastFetchedEpoch = if (isTruncationOnFetchSupported)
            fetchState.lastFetchedEpoch()
          else
            Optional.empty[Integer]
          builder.add(topicPartition, new FetchRequest.PartitionData(
            fetchState.topicId().orElse(Uuid.ZERO_UUID),
            fetchState.fetchOffset(),
            logStartOffset,
            fetchSize,
            Optional.of(fetchState.currentLeaderEpoch()),
            lastFetchedEpoch))
        } catch {
          case _: KafkaStorageException =>
            // The replica has already been marked offline due to log directory failure and the original failure should have already been logged.
            // This partition should be removed from ReplicaFetcherThread soon by ReplicaManager.handleLogDirFailure()
            partitionsWithError += topicPartition
        }
      }
    }

    val fetchData = builder.build()
    val fetchRequestOpt = if (fetchData.sessionPartitions.isEmpty && fetchData.toForget.isEmpty) {
      Optional.empty[ReplicaFetch]
    } else {
      val metadataVersion = metadataVersionSupplier()
      val version: Short = if (!fetchData.canUseTopicIds) {
        12
      } else {
        metadataVersion.fetchRequestVersion
      }
      val requestBuilder = FetchRequest.Builder
        .forReplica(version, brokerConfig.brokerId, brokerEpochSupplier(), maxWait, minBytes, fetchData.toSend)
        .setMaxBytes(maxBytes)
        .removed(fetchData.toForget)
        .replaced(fetchData.toReplace)
        .metadata(fetchData.metadata)
      Optional.of(new ReplicaFetch(fetchData.sessionPartitions(), requestBuilder))
    }

    new ResultWithPartitions(fetchRequestOpt, partitionsWithError.asJava)
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the follower if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  private def shouldFollowerThrottle(quota: ReplicaQuota, fetchState: PartitionFetchState, topicPartition: TopicPartition): Boolean = {
    !fetchState.isReplicaInSync && quota.isThrottled(topicPartition) && quota.isQuotaExceeded
  }

  override def toString: String = s"RemoteLeaderEndPoint(blockingSender=$blockingSender)"
}
