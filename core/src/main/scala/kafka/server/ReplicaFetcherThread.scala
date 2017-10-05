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

import AbstractFetcherThread.ResultWithPartitions
import kafka.admin.AdminUtils
import kafka.api.{FetchRequest => _, _}
import kafka.cluster.{BrokerEndPoint, Replica}
import kafka.log.LogConfig
import kafka.server.ReplicaFetcherThread._
import kafka.server.epoch.LeaderEpochCache
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{EpochEndOffset, FetchResponse, ListOffsetRequest, ListOffsetResponse, OffsetsForLeaderEpochRequest, OffsetsForLeaderEpochResponse, FetchRequest => JFetchRequest}
import org.apache.kafka.common.utils.{LogContext, Time}
import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}

class ReplicaFetcherThread(name: String,
                           fetcherId: Int,
                           sourceBroker: BrokerEndPoint,
                           brokerConfig: KafkaConfig,
                           replicaMgr: ReplicaManager,
                           metrics: Metrics,
                           time: Time,
                           quota: ReplicationQuotaManager,
                           leaderEndpointBlockingSend: Option[BlockingSend] = None)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false,
                                includeLogTruncation = true) {

  type REQ = FetchRequest
  type PD = PartitionData

  private val replicaId = brokerConfig.brokerId
  private val logContext = new LogContext(s"[ReplicaFetcher replicaId=$replicaId, leaderId=${sourceBroker.id}, " +
    s"fetcherId=$fetcherId] ")
  this.logIdent = logContext.logPrefix
  private val leaderEndpoint = leaderEndpointBlockingSend.getOrElse(
    new ReplicaFetcherBlockingSend(sourceBroker, brokerConfig, metrics, time, fetcherId,
      s"broker-$replicaId-fetcher-$fetcherId", logContext))
  private val fetchRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV1) 5
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV0) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
    else 0
  private val maxWait = brokerConfig.replicaFetchWaitMaxMs
  private val minBytes = brokerConfig.replicaFetchMinBytes
  private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
  private val fetchSize = brokerConfig.replicaFetchMaxBytes
  private val shouldSendLeaderEpochRequest: Boolean = brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV2

  private def epochCacheOpt(tp: TopicPartition): Option[LeaderEpochCache] =  replicaMgr.getReplica(tp).map(_.epochs.get)

  override def shutdown(): Unit = {
    super.shutdown()
    leaderEndpoint.close()
  }

  // process fetched data
  def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PartitionData) {
    val replica = replicaMgr.getReplicaOrException(topicPartition)
    val records = partitionData.toRecords

    maybeWarnIfOversizedRecords(records, topicPartition)

    if (fetchOffset != replica.logEndOffset.messageOffset)
      throw new RuntimeException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, replica.logEndOffset.messageOffset))
    if (logger.isTraceEnabled)
      trace("Follower has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
        .format(replica.logEndOffset.messageOffset, topicPartition, records.sizeInBytes, partitionData.highWatermark))

    // Append the leader's messages to the log
    replica.log.get.appendAsFollower(records)

    if (logger.isTraceEnabled)
      trace("Follower has replica log end offset %d after appending %d bytes of messages for partition %s"
        .format(replica.logEndOffset.messageOffset, records.sizeInBytes, topicPartition))
    val followerHighWatermark = replica.logEndOffset.messageOffset.min(partitionData.highWatermark)
    val leaderLogStartOffset = partitionData.logStartOffset
    // for the follower replica, we do not need to keep
    // its segment base offset the physical position,
    // these values will be computed upon making the leader
    replica.highWatermark = new LogOffsetMetadata(followerHighWatermark)
    replica.maybeIncrementLogStartOffset(leaderLogStartOffset)
    if (logger.isTraceEnabled)
      trace(s"Follower set replica high watermark for partition $topicPartition to $followerHighWatermark")
    if (quota.isThrottled(topicPartition))
      quota.record(records.sizeInBytes)
    replicaMgr.brokerTopicStats.updateReplicationBytesIn(records.sizeInBytes)
  }

  def maybeWarnIfOversizedRecords(records: MemoryRecords, topicPartition: TopicPartition): Unit = {
    // oversized messages don't cause replication to fail from fetch request version 3 (KIP-74)
    if (fetchRequestVersion <= 2 && records.sizeInBytes > 0 && records.validBytes <= 0)
      error(s"Replication is failing due to a message that is greater than replica.fetch.max.bytes for partition $topicPartition. " +
        "This generally occurs when the max.message.bytes has been overridden to exceed this value and a suitably large " +
        "message has also been sent. To fix this problem increase replica.fetch.max.bytes in your broker config to be " +
        "equal or larger than your settings for max.message.bytes, both at a broker and topic level.")
  }

  /**
   * Handle a partition whose offset is out of range and return a new fetch offset.
   */
  def handleOffsetOutOfRange(topicPartition: TopicPartition): Long = {
    val replica = replicaMgr.getReplicaOrException(topicPartition)

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
    val leaderEndOffset: Long = earliestOrLatestOffset(topicPartition, ListOffsetRequest.LATEST_TIMESTAMP)

    if (leaderEndOffset < replica.logEndOffset.messageOffset) {
      // Prior to truncating the follower's log, ensure that doing so is not disallowed by the configuration for unclean leader election.
      // This situation could only happen if the unclean election configuration for a topic changes while a replica is down. Otherwise,
      // we should never encounter this situation since a non-ISR leader cannot be elected if disallowed by the broker configuration.
      if (!LogConfig.fromProps(brokerConfig.originals, AdminUtils.fetchEntityConfig(replicaMgr.zkUtils,
        ConfigType.Topic, topicPartition.topic)).uncleanLeaderElectionEnable) {
        // Log a fatal error and shutdown the broker to ensure that data loss does not occur unexpectedly.
        fatal(s"Exiting because log truncation is not allowed for partition $topicPartition, current leader's " +
          s"latest offset $leaderEndOffset is less than replica's latest offset ${replica.logEndOffset.messageOffset}")
        throw new FatalExitError
      }

      warn(s"Reset fetch offset for partition $topicPartition from ${replica.logEndOffset.messageOffset} to current " +
        s"leader's latest offset $leaderEndOffset")
      replicaMgr.logManager.truncateTo(Map(topicPartition -> leaderEndOffset))
      leaderEndOffset
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
       * In the first case, the follower's current log end offset is smaller than the leader's log start offset. So the
       * follower should truncate all its logs, roll out a new segment and start to fetch from the current leader's log
       * start offset.
       * In the second case, the follower should just keep the current log segments and retry the fetch. In the second
       * case, there will be some inconsistency of data between old and new leader. We are not solving it here.
       * If users want to have strong consistency guarantees, appropriate configurations needs to be set for both
       * brokers and producers.
       *
       * Putting the two cases together, the follower should fetch from the higher one of its replica log end offset
       * and the current leader's log start offset.
       *
       */
      val leaderStartOffset: Long = earliestOrLatestOffset(topicPartition, ListOffsetRequest.EARLIEST_TIMESTAMP)
      warn(s"Reset fetch offset for partition $topicPartition from ${replica.logEndOffset.messageOffset} to current " +
        s"leader's start offset $leaderStartOffset")
      val offsetToFetch = Math.max(leaderStartOffset, replica.logEndOffset.messageOffset)
      // Only truncate log when current leader's log start offset is greater than follower's log end offset.
      if (leaderStartOffset > replica.logEndOffset.messageOffset)
        replicaMgr.logManager.truncateFullyAndStartAt(topicPartition, leaderStartOffset)
      offsetToFetch
    }
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[TopicPartition]) {
    if (partitions.nonEmpty)
      delayPartitions(partitions, brokerConfig.replicaFetchBackoffMs.toLong)
  }

  protected def fetch(fetchRequest: FetchRequest): Seq[(TopicPartition, PartitionData)] = {
    val clientResponse = leaderEndpoint.sendRequest(fetchRequest.underlying)
    val fetchResponse = clientResponse.responseBody.asInstanceOf[FetchResponse]
    fetchResponse.responseData.asScala.toSeq.map { case (key, value) =>
      key -> new PartitionData(value)
    }
  }

  private def earliestOrLatestOffset(topicPartition: TopicPartition, earliestOrLatest: Long): Long = {
    val requestBuilder = if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2) {
        val partitions = Map(topicPartition -> (earliestOrLatest: java.lang.Long))
        ListOffsetRequest.Builder.forReplica(1, replicaId).setTargetTimes(partitions.asJava)
      } else {
        val partitions = Map(topicPartition -> new ListOffsetRequest.PartitionData(earliestOrLatest, 1))
        ListOffsetRequest.Builder.forReplica(0, replicaId).setOffsetData(partitions.asJava)
      }
    val clientResponse = leaderEndpoint.sendRequest(requestBuilder)
    val response = clientResponse.responseBody.asInstanceOf[ListOffsetResponse]
    val partitionData = response.responseData.get(topicPartition)
    partitionData.error match {
      case Errors.NONE =>
        if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2)
          partitionData.offset
        else
          partitionData.offsets.get(0)
      case error => throw error.exception
    }
  }

  override def buildFetchRequest(partitionMap: Seq[(TopicPartition, PartitionFetchState)]): ResultWithPartitions[FetchRequest] = {
    val requestMap = new util.LinkedHashMap[TopicPartition, JFetchRequest.PartitionData]
    val partitionsWithError = mutable.Set[TopicPartition]()

    partitionMap.foreach { case (topicPartition, partitionFetchState) =>
      // We will not include a replica in the fetch request if it should be throttled.
      if (partitionFetchState.isReadyForFetch && !shouldFollowerThrottle(quota, topicPartition)) {
        try {
          val logStartOffset = replicaMgr.getReplicaOrException(topicPartition).logStartOffset
          requestMap.put(topicPartition, new JFetchRequest.PartitionData(partitionFetchState.fetchOffset, logStartOffset, fetchSize))
        } catch {
          case _: KafkaStorageException =>
            // The replica has already been marked offline due to log directory failure and the original failure should have already been logged.
            // This partition should be removed from ReplicaFetcherThread soon by ReplicaManager.handleLogDirFailure()
            partitionsWithError += topicPartition
        }
      }
    }

    val requestBuilder = JFetchRequest.Builder.forReplica(fetchRequestVersion, replicaId, maxWait, minBytes, requestMap)
      .setMaxBytes(maxBytes)
    ResultWithPartitions(new FetchRequest(requestBuilder), partitionsWithError)
  }

  /**
    * - Truncate the log to the leader's offset for each partition's epoch.
    * - If the leader's offset is greater, we stick with the Log End Offset
    *   otherwise we truncate to the leaders offset.
    * - If the leader replied with undefined epoch offset we must use the high watermark
    */
  override def maybeTruncate(fetchedEpochs: Map[TopicPartition, EpochEndOffset]): ResultWithPartitions[Map[TopicPartition, Long]] = {
    val truncationPoints = scala.collection.mutable.HashMap.empty[TopicPartition, Long]
    val partitionsWithError = mutable.Set[TopicPartition]()

    fetchedEpochs.foreach { case (tp, epochOffset) =>
      try {
        val replica = replicaMgr.getReplicaOrException(tp)

        if (epochOffset.hasError) {
          info(s"Retrying leaderEpoch request for partition ${replica.topicPartition} as the leader reported an error: ${epochOffset.error}")
          partitionsWithError += tp
        } else {
          val truncationOffset =
            if (epochOffset.endOffset == UNDEFINED_EPOCH_OFFSET)
              highWatermark(replica, epochOffset)
            else if (epochOffset.endOffset >= replica.logEndOffset.messageOffset)
              logEndOffset(replica, epochOffset)
            else
              epochOffset.endOffset

          replicaMgr.logManager.truncateTo(Map(tp -> truncationOffset))
          truncationPoints.put(tp, truncationOffset)
        }
      } catch {
        case e: KafkaStorageException =>
          info(s"Failed to truncate $tp", e)
          partitionsWithError += tp
      }
    }

    ResultWithPartitions(truncationPoints, partitionsWithError)
  }

  override def buildLeaderEpochRequest(allPartitions: Seq[(TopicPartition, PartitionFetchState)]): ResultWithPartitions[Map[TopicPartition, Int]] = {
    val partitionEpochOpts = allPartitions
      .filter { case (_, state) => state.isTruncatingLog }
      .map { case (tp, _) => tp -> epochCacheOpt(tp) }.toMap

    val (partitionsWithEpoch, partitionsWithoutEpoch) = partitionEpochOpts.partition { case (tp, epochCacheOpt) => epochCacheOpt.nonEmpty }

    debug(s"Build leaderEpoch request $partitionsWithEpoch")
    val result = partitionsWithEpoch.map { case (tp, epochCacheOpt) => tp -> epochCacheOpt.get.latestEpoch() }
    ResultWithPartitions(result, partitionsWithoutEpoch.keys.toSet)
  }

  override def fetchEpochsFromLeader(partitions: Map[TopicPartition, Int]): Map[TopicPartition, EpochEndOffset] = {
    var result: Map[TopicPartition, EpochEndOffset] = null
    if (shouldSendLeaderEpochRequest) {
      val partitionsAsJava = partitions.map { case (tp, epoch) => tp -> epoch.asInstanceOf[Integer] }.toMap.asJava
      val epochRequest = new OffsetsForLeaderEpochRequest.Builder(partitionsAsJava)
      try {
        val response = leaderEndpoint.sendRequest(epochRequest)
        result = response.responseBody.asInstanceOf[OffsetsForLeaderEpochResponse].responses.asScala
        debug(s"Receive leaderEpoch response $result")
      } catch {
        case t: Throwable =>
          warn(s"Error when sending leader epoch request for $partitions", t)

          // if we get any unexpected exception, mark all partitions with an error
          result = partitions.map { case (tp, _) =>
            tp -> new EpochEndOffset(Errors.forException(t), UNDEFINED_EPOCH_OFFSET)
          }
      }
    } else {
      // just generate a response with no error but UNDEFINED_OFFSET so that we can fall back to truncating using
      // high watermark in maybeTruncate()
      result = partitions.map { case (tp, _) =>
        tp -> new EpochEndOffset(Errors.NONE, UNDEFINED_EPOCH_OFFSET)
      }
    }
    result
  }

  private def logEndOffset(replica: Replica, epochOffset: EpochEndOffset): Long = {
    val logEndOffset = replica.logEndOffset.messageOffset
    info(s"Based on follower's leader epoch, leader replied with an offset ${epochOffset.endOffset} >= the " +
      s"follower's log end offset $logEndOffset in ${replica.topicPartition}. No truncation needed.")
    logEndOffset
  }

  private def highWatermark(replica: Replica, epochOffset: EpochEndOffset): Long = {
    warn(s"Based on follower's leader epoch, leader replied with an unknown offset in ${replica.topicPartition}. " +
      s"High watermark ${replica.highWatermark.messageOffset} will be used for truncation.")
    replica.highWatermark.messageOffset
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the follower if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  private def shouldFollowerThrottle(quota: ReplicaQuota, topicPartition: TopicPartition): Boolean = {
    val isReplicaInSync = fetcherLagStats.isReplicaInSync(topicPartition.topic, topicPartition.partition)
    quota.isThrottled(topicPartition) && quota.isQuotaExceeded && !isReplicaInSync
  }
}

object ReplicaFetcherThread {

  private[server] class FetchRequest(val underlying: JFetchRequest.Builder) extends AbstractFetcherThread.FetchRequest {
    def isEmpty: Boolean = underlying.fetchData.isEmpty
    def offset(topicPartition: TopicPartition): Long =
      underlying.fetchData.asScala(topicPartition).fetchOffset
    override def toString = underlying.toString
  }

  private[server] class PartitionData(val underlying: FetchResponse.PartitionData) extends AbstractFetcherThread.PartitionData {

    def error = underlying.error

    def toRecords: MemoryRecords = {
      underlying.records.asInstanceOf[MemoryRecords]
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
