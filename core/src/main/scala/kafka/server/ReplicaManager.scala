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

import com.yammer.metrics.core.Meter
import kafka.cluster.Partition
import kafka.log.LogManager
import kafka.server.HostedPartition.Online
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.ReplicaManager.{AtMinIsrPartitionCountMetricName, FailedIsrUpdatesPerSecMetricName, IsrExpandsPerSecMetricName, IsrShrinksPerSecMetricName, LeaderCountMetricName, OfflineReplicaCountMetricName, PartitionCountMetricName, PartitionsWithLateTransactionsCountMetricName, ProducerIdCountMetricName, ReassigningPartitionsMetricName, UnderMinIsrPartitionCountMetricName, UnderReplicatedPartitionsMetricName, createLogReadResult, isListOffsetsTimestampUnsupported}
import kafka.server.share.DelayedShareFetch
import kafka.utils._
import org.apache.kafka.common.{IsolationLevel, Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.{Plugin, Topic}
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsTopic
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult}
import org.apache.kafka.common.message.{DescribeLogDirsResponseData, DescribeProducersResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.PartitionView.DefaultPartitionView
import org.apache.kafka.common.replica.ReplicaView.DefaultReplicaView
import org.apache.kafka.common.replica._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{Exit, Time, Utils}
import org.apache.kafka.coordinator.transaction.{AddPartitionsToTxnConfig, TransactionLogConfig}
import org.apache.kafka.image.{LocalReplicaChanges, MetadataImage, TopicsDelta}
import org.apache.kafka.logger.StateChangeLogger
import org.apache.kafka.metadata.LeaderConstants.NO_LEADER
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.common.{DirectoryEventHandler, RequestLocal, StopPartition}
import org.apache.kafka.server.log.remote.TopicPartitionLog
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.server.log.remote.storage.RemoteLogManager
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.partition.PartitionListener
import org.apache.kafka.server.purgatory.{DelayedDeleteRecords, DelayedOperationPurgatory, DelayedRemoteFetch, DelayedRemoteListOffsets, DeleteRecordsPartitionStatus, ListOffsetsPartitionStatus, TopicPartitionOperationKey}
import org.apache.kafka.server.share.fetch.{DelayedShareFetchKey, DelayedShareFetchPartitionKey}
import org.apache.kafka.server.storage.log.{FetchParams, FetchPartitionData}
import org.apache.kafka.server.transaction.AddPartitionsToTxnManager
import org.apache.kafka.server.transaction.AddPartitionsToTxnManager.TransactionSupportedOperation
import org.apache.kafka.server.util.timer.{SystemTimer, TimerTask}
import org.apache.kafka.server.util.{Scheduler, ShutdownableThread}
import org.apache.kafka.server.{ActionQueue, DelayedActionQueue, common}
import org.apache.kafka.storage.internals.checkpoint.{LazyOffsetCheckpoints, OffsetCheckpointFile, OffsetCheckpoints}
import org.apache.kafka.storage.internals.log.{AppendOrigin, FetchDataInfo, FetchPartitionStatus, LeaderHwChange, LogAppendInfo, LogConfig, LogDirFailureChannel, LogOffsetMetadata, LogReadInfo, LogReadResult, OffsetResultHolder, RecordValidationException, RemoteLogReadResult, RemoteStorageFetchInfo, UnifiedLog, VerificationGuard}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.io.File
import java.lang.{Long => JLong}
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, Future, RejectedExecutionException, TimeUnit}
import java.util.{Collections, Optional, OptionalInt, OptionalLong}
import java.util.function.Consumer
import scala.collection.{Map, Seq, Set, immutable, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOptional

/*
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo,
                           exception: Option[Throwable],
                           hasCustomErrorMessage: Boolean) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }

  def errorMessage: String = {
    exception match {
      case Some(e) if hasCustomErrorMessage => e.getMessage
      case _ => null
    }
  }
}

case class LogDeleteRecordsResult(requestedOffset: Long, lowWatermark: Long, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

/**
 * Trait to represent the state of hosted partitions. We create a concrete (active) Partition
 * instance when the broker receives a LeaderAndIsr request from the controller or a metadata
 * log record from the Quorum controller indicating that the broker should be either a leader
 * or follower of a partition.
 */
sealed trait HostedPartition

object HostedPartition {
  /**
   * This broker does not have any state for this partition locally.
   */
  final object None extends HostedPartition

  /**
   * This broker hosts the partition and it is online.
   */
  final case class Online(partition: Partition) extends HostedPartition

  /**
   * This broker hosts the partition, but it is in an offline log directory.
   */
  final case class Offline(partition: Option[Partition]) extends HostedPartition
}

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"

  private val LeaderCountMetricName = "LeaderCount"
  private val PartitionCountMetricName = "PartitionCount"
  private val OfflineReplicaCountMetricName = "OfflineReplicaCount"
  private val UnderReplicatedPartitionsMetricName = "UnderReplicatedPartitions"
  private val UnderMinIsrPartitionCountMetricName = "UnderMinIsrPartitionCount"
  private val AtMinIsrPartitionCountMetricName = "AtMinIsrPartitionCount"
  private val ReassigningPartitionsMetricName = "ReassigningPartitions"
  private val PartitionsWithLateTransactionsCountMetricName = "PartitionsWithLateTransactionsCount"
  private val ProducerIdCountMetricName = "ProducerIdCount"
  private val IsrExpandsPerSecMetricName = "IsrExpandsPerSec"
  private val IsrShrinksPerSecMetricName = "IsrShrinksPerSec"
  private val FailedIsrUpdatesPerSecMetricName = "FailedIsrUpdatesPerSec"

  private[server] val GaugeMetricNames = Set(
    LeaderCountMetricName,
    PartitionCountMetricName,
    OfflineReplicaCountMetricName,
    UnderReplicatedPartitionsMetricName,
    UnderMinIsrPartitionCountMetricName,
    AtMinIsrPartitionCountMetricName,
    ReassigningPartitionsMetricName,
    PartitionsWithLateTransactionsCountMetricName,
    ProducerIdCountMetricName
  )

  private[server] val MeterMetricNames = Set(
    IsrExpandsPerSecMetricName,
    IsrShrinksPerSecMetricName,
    FailedIsrUpdatesPerSecMetricName
  )

  private[server] val MetricNames = GaugeMetricNames.union(MeterMetricNames)

  private val timestampMinSupportedVersion: immutable.Map[Long, Short] = immutable.Map[Long, Short](
    ListOffsetsRequest.EARLIEST_TIMESTAMP -> 1.toShort,
    ListOffsetsRequest.LATEST_TIMESTAMP -> 1.toShort,
    ListOffsetsRequest.MAX_TIMESTAMP -> 7.toShort,
    ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP -> 8.toShort,
    ListOffsetsRequest.LATEST_TIERED_TIMESTAMP -> 9.toShort,
    ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP -> 11.toShort
  )

  def createLogReadResult(highWatermark: Long,
                          leaderLogStartOffset: Long,
                          leaderLogEndOffset: Long,
                          e: Throwable): LogReadResult = {
    new LogReadResult(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
      Optional.empty(),
      highWatermark,
      leaderLogStartOffset,
      leaderLogEndOffset,
      -1L,
      -1L,
      OptionalLong.empty(),
      Errors.forException(e));
  }

  private[server] def isListOffsetsTimestampUnsupported(timestamp: JLong, version: Short): Boolean = {
    timestamp < 0 &&
      (!timestampMinSupportedVersion.contains(timestamp) || version < timestampMinSupportedVersion(timestamp))
  }
}

class ReplicaManager(val config: KafkaConfig,
                     metrics: Metrics,
                     time: Time,
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val remoteLogManager: Option[RemoteLogManager] = None,
                     quotaManagers: QuotaManagers,
                     val metadataCache: MetadataCache,
                     logDirFailureChannel: LogDirFailureChannel,
                     val alterPartitionManager: AlterPartitionManager,
                     val brokerTopicStats: BrokerTopicStats = new BrokerTopicStats(),
                     delayedProducePurgatoryParam: Option[DelayedOperationPurgatory[DelayedProduce]] = None,
                     delayedFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedFetch]] = None,
                     delayedDeleteRecordsPurgatoryParam: Option[DelayedOperationPurgatory[DelayedDeleteRecords]] = None,
                     delayedRemoteFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedRemoteFetch]] = None,
                     delayedRemoteListOffsetsPurgatoryParam: Option[DelayedOperationPurgatory[DelayedRemoteListOffsets]] = None,
                     delayedShareFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedShareFetch]] = None,
                     val brokerEpochSupplier: () => Long = () => -1,
                     addPartitionsToTxnManager: Option[AddPartitionsToTxnManager] = None,
                     val directoryEventHandler: DirectoryEventHandler = DirectoryEventHandler.NOOP,
                     val defaultActionQueue: ActionQueue = new DelayedActionQueue
                     ) extends Logging {
  // Changing the package or class name may cause incompatibility with existing code and metrics configuration
  private val metricsPackage = "kafka.server"
  private val metricsClassName = "ReplicaManager"
  private val metricsGroup = new KafkaMetricsGroup(metricsPackage, metricsClassName)
  private val addPartitionsToTxnConfig = new AddPartitionsToTxnConfig(config)
  private val shareFetchPurgatoryName = "ShareFetch"
  private val delayedShareFetchTimer = new SystemTimer(shareFetchPurgatoryName)

  val delayedProducePurgatory = delayedProducePurgatoryParam.getOrElse(
    new DelayedOperationPurgatory[DelayedProduce](
      "Produce", config.brokerId,
      config.producerPurgatoryPurgeIntervalRequests))
  val delayedFetchPurgatory = delayedFetchPurgatoryParam.getOrElse(
    new DelayedOperationPurgatory[DelayedFetch](
      "Fetch", config.brokerId,
      config.fetchPurgatoryPurgeIntervalRequests))
  val delayedDeleteRecordsPurgatory = delayedDeleteRecordsPurgatoryParam.getOrElse(
    new DelayedOperationPurgatory[DelayedDeleteRecords](
      "DeleteRecords", config.brokerId,
      config.deleteRecordsPurgatoryPurgeIntervalRequests))
  // delayedRemoteFetchPurgatory purgeInterval is set to 0 to release the references of completed DelayedRemoteFetch
  // instances immediately for GC. The DelayedRemoteFetch instance internally holds the RemoteLogReadResult that can be
  // up to the size of `fetch.max.bytes` which defaults to 50 MB.
  val delayedRemoteFetchPurgatory = delayedRemoteFetchPurgatoryParam.getOrElse(
    new DelayedOperationPurgatory[DelayedRemoteFetch](
      "RemoteFetch", config.brokerId, 0))
  val delayedRemoteListOffsetsPurgatory = delayedRemoteListOffsetsPurgatoryParam.getOrElse(
    new DelayedOperationPurgatory[DelayedRemoteListOffsets](
      "RemoteListOffsets", config.brokerId))
  val delayedShareFetchPurgatory = delayedShareFetchPurgatoryParam.getOrElse(
    new DelayedOperationPurgatory[DelayedShareFetch](
      shareFetchPurgatoryName, delayedShareFetchTimer, config.brokerId,
      config.shareGroupConfig.shareFetchPurgatoryPurgeIntervalRequests))

  /* epoch of the controller that last changed the leader */
  protected val localBrokerId = config.brokerId
  protected val allPartitions = new ConcurrentHashMap[TopicPartition, HostedPartition]
  private val replicaStateChangeLock = new Object
  val replicaFetcherManager = createReplicaFetcherManager(metrics, time, quotaManagers.follower)
  private[server] val replicaAlterLogDirsManager = createReplicaAlterLogDirsManager(quotaManagers.alterLogDirs, brokerTopicStats)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  @volatile private[server] var highWatermarkCheckpoints: Map[String, OffsetCheckpointFile] = logManager.liveLogDirs.map(dir =>
    (dir.getAbsolutePath, new OffsetCheckpointFile(new File(dir, ReplicaManager.HighWatermarkFilename), logDirFailureChannel))).toMap

  @volatile private var isInControlledShutdown = false

  this.logIdent = s"[ReplicaManager broker=$localBrokerId] "
  protected val stateChangeLogger = new StateChangeLogger(localBrokerId)

  private var logDirFailureHandler: LogDirFailureHandler = _

  private class LogDirFailureHandler(name: String) extends ShutdownableThread(name) {
    override def doWork(): Unit = {
      val newOfflineLogDir = logDirFailureChannel.takeNextOfflineLogDir()
      handleLogDirFailure(newOfflineLogDir)
    }
  }

  // Visible for testing
  private[server] val replicaSelectorPlugin: Option[Plugin[ReplicaSelector]] = createReplicaSelector(metrics)

  metricsGroup.newGauge(LeaderCountMetricName, () => leaderPartitionsIterator.size)
  // Visible for testing
  private[kafka] val partitionCount = metricsGroup.newGauge(PartitionCountMetricName, () => allPartitions.size)
  metricsGroup.newGauge(OfflineReplicaCountMetricName, () => offlinePartitionCount)
  metricsGroup.newGauge(UnderReplicatedPartitionsMetricName, () => underReplicatedPartitionCount)
  metricsGroup.newGauge(UnderMinIsrPartitionCountMetricName, () => leaderPartitionsIterator.count(_.isUnderMinIsr))
  metricsGroup.newGauge(AtMinIsrPartitionCountMetricName, () => leaderPartitionsIterator.count(_.isAtMinIsr))
  metricsGroup.newGauge(ReassigningPartitionsMetricName, () => reassigningPartitionsCount)
  metricsGroup.newGauge(PartitionsWithLateTransactionsCountMetricName, () => lateTransactionsCount)
  metricsGroup.newGauge(ProducerIdCountMetricName, () => producerIdCount)

  private def reassigningPartitionsCount: Int = leaderPartitionsIterator.count(_.isReassigning)

  private def lateTransactionsCount: Int = {
    val currentTimeMs = time.milliseconds()
    leaderPartitionsIterator.count(_.hasLateTransaction(currentTimeMs))
  }

  def producerIdCount: Int = onlinePartitionsIterator.map(_.producerIdCount).sum

  val isrExpandRate: Meter = metricsGroup.newMeter(IsrExpandsPerSecMetricName, "expands", TimeUnit.SECONDS)
  val isrShrinkRate: Meter = metricsGroup.newMeter(IsrShrinksPerSecMetricName, "shrinks", TimeUnit.SECONDS)
  val failedIsrUpdatesRate: Meter = metricsGroup.newMeter(FailedIsrUpdatesPerSecMetricName, "failedUpdates", TimeUnit.SECONDS)

  def underReplicatedPartitionCount: Int = leaderPartitionsIterator.count(_.isUnderReplicated)

  def startHighWatermarkCheckPointThread(): Unit = {
    if (highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", () => checkpointHighWatermarks(), 0L, config.replicaHighWatermarkCheckpointIntervalMs)
  }

  // When ReplicaAlterDirThread finishes replacing a current replica with a future replica, it will
  // remove the partition from the partition state map. But it will not close itself even if the
  // partition state map is empty. Thus we need to call shutdownIdleReplicaAlterDirThread() periodically
  // to shutdown idle ReplicaAlterDirThread
  private def shutdownIdleReplicaAlterLogDirsThread(): Unit = {
    replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
  }

  def resizeFetcherThreadPool(newSize: Int): Unit = {
    replicaFetcherManager.resizeThreadPool(newSize)
  }

  def getLog(topicPartition: TopicPartition): Option[UnifiedLog] = logManager.getLog(topicPartition)

  def startup(): Unit = {
    // start ISR expiration thread
    // A follower can lag behind leader for up to config.replicaLagTimeMaxMs x 1.5 before it is removed from ISR
    scheduler.schedule("isr-expiration", () => maybeShrinkIsr(), 0L, config.replicaLagTimeMaxMs / 2)
    scheduler.schedule("shutdown-idle-replica-alter-log-dirs-thread", () => shutdownIdleReplicaAlterLogDirsThread(), 0L, 10000L)

    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler")
    logDirFailureHandler.start()
    addPartitionsToTxnManager.foreach(_.start())
    remoteLogManager.foreach(rlm => rlm.setDelayedOperationPurgatory(delayedRemoteListOffsetsPurgatory))
  }

  private def maybeRemoveTopicMetrics(topic: String): Unit = {
    val topicHasNonOfflinePartition = allPartitions.values.asScala.exists {
      case online: HostedPartition.Online => topic == online.partition.topic
      case HostedPartition.None | HostedPartition.Offline(_) => false
    }
    if (!topicHasNonOfflinePartition) // nothing online or deferred
      brokerTopicStats.removeMetrics(topic)
  }

  private def completeDelayedOperationsWhenNotPartitionLeader(topicPartition: TopicPartition, topicId: Option[Uuid]): Unit = {
    val topicPartitionOperationKey = new TopicPartitionOperationKey(topicPartition)
    delayedProducePurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedFetchPurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedRemoteFetchPurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedRemoteListOffsetsPurgatory.checkAndComplete(topicPartitionOperationKey)
    if (topicId.isDefined) delayedShareFetchPurgatory.checkAndComplete(
      new DelayedShareFetchPartitionKey(topicId.get, topicPartition.partition()))
  }

  /**
   * Complete any local follower fetches that have been unblocked since new data is available
   * from the leader for one or more partitions. Should only be called by ReplicaFetcherThread
   * after successfully replicating from the leader.
   */
  private[server] def completeDelayedFetchRequests(topicPartitions: Seq[TopicPartition]): Unit = {
    topicPartitions.foreach(tp => delayedFetchPurgatory.checkAndComplete(new TopicPartitionOperationKey(tp)))
  }

  /**
   * Complete any delayed share fetch requests that have been unblocked since new data is available from the leader
   * for one of the partitions. This could happen due to acknowledgements, acquisition lock timeout of records, partition
   * locks getting freed and release of acquired records due to share session close.
   * @param delayedShareFetchKey The key corresponding to which the share fetch request has been stored in the purgatory
   */
  private[server] def completeDelayedShareFetchRequest(delayedShareFetchKey: DelayedShareFetchKey): Unit = {
    delayedShareFetchPurgatory.checkAndComplete(delayedShareFetchKey)
  }

  /**
   * Add and watch a share fetch request in the delayed share fetch purgatory corresponding to a set of keys in case it cannot be
   * completed instantaneously, otherwise complete it.
   * @param delayedShareFetch Refers to the DelayedOperation over share fetch request
   * @param delayedShareFetchKeys The keys corresponding to which the delayed share fetch request will be stored in the purgatory
   */
  private[server] def addDelayedShareFetchRequest(delayedShareFetch: DelayedShareFetch,
                                                  delayedShareFetchKeys : util.List[DelayedShareFetchKey]): Unit = {
    delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch, delayedShareFetchKeys)
  }

  /**
   * Add a timer task to the delayedShareFetchTimer.
   * @param timerTask The timer task to be added to the delayedShareFetchTimer
   */
  private[server] def addShareFetchTimerRequest(timerTask: TimerTask): Unit = {
    delayedShareFetchTimer.add(timerTask)
  }

  /**
   * Registers the provided listener to the partition iff the partition is online.
   */
  def maybeAddListener(partition: TopicPartition, listener: PartitionListener): Boolean = {
    getPartition(partition) match {
      case HostedPartition.Online(partition) =>
        partition.maybeAddListener(listener)
      case _ =>
        false
    }
  }

  /**
   * Removes the provided listener from the partition.
   */
  def removeListener(partition: TopicPartition, listener: PartitionListener): Unit = {
    getPartition(partition) match {
      case HostedPartition.Online(partition) =>
        partition.removeListener(listener)
      case _ => // Ignore
    }
  }

  /**
   * Stop the given partitions.
   *
   * @param partitionsToStop set of topic-partitions to be stopped which also indicates whether to remove the
   *                         partition data from the local and remote log storage.
   *
   * @return                 A map from partitions to exceptions which occurred.
   *                         If no errors occurred, the map will be empty.
   */
  private def stopPartitions(partitionsToStop: Set[StopPartition]): Map[TopicPartition, Throwable] = {
    // First stop fetchers for all partitions.
    val partitions = partitionsToStop.map(_.topicPartition)
    replicaFetcherManager.removeFetcherForPartitions(partitions)
    replicaAlterLogDirsManager.removeFetcherForPartitions(partitions)

    // Second remove deleted partitions from the partition map. Fetchers rely on the
    // ReplicaManager to get Partition's information so they must be stopped first.
    val partitionsToDelete = mutable.Set.empty[TopicPartition]
    partitionsToStop.foreach { stopPartition =>
      val topicPartition = stopPartition.topicPartition
      var topicId: Option[Uuid] = None
      if (stopPartition.deleteLocalLog) {
        getPartition(topicPartition) match {
          case hostedPartition: HostedPartition.Online =>
            if (allPartitions.remove(topicPartition, hostedPartition)) {
              maybeRemoveTopicMetrics(topicPartition.topic)
              // Logs are not deleted here. They are deleted in a single batch later on.
              // This is done to avoid having to checkpoint for every deletions.
              hostedPartition.partition.delete()
              topicId = hostedPartition.partition.topicId
            }

          case _ =>
        }
        partitionsToDelete += topicPartition
      }
      // If we were the leader, we may have some operations still waiting for completion.
      // We force completion to prevent them from timing out.
      completeDelayedOperationsWhenNotPartitionLeader(topicPartition, topicId)
    }

    // Third delete the logs and checkpoint.
    val errorMap = new mutable.HashMap[TopicPartition, Throwable]()
    val remotePartitionsToStop = partitionsToStop.filter {
      sp => logManager.getLog(sp.topicPartition).exists(unifiedLog => unifiedLog.remoteLogEnabled())
    }
    if (partitionsToDelete.nonEmpty) {
      // Delete the logs and checkpoint.
      logManager.asyncDelete(partitionsToDelete, isStray = false, (tp, e) => errorMap.put(tp, e))
    }
    remoteLogManager.foreach { rlm =>
      // exclude the partitions with offline/error state
      val partitions = remotePartitionsToStop.filterNot(sp => errorMap.contains(sp.topicPartition)).toSet.asJava
      if (!partitions.isEmpty) {
        rlm.stopPartitions(partitions, (tp, e) => errorMap.put(tp, e))
      }
    }
    errorMap
  }

  def topicIdPartition(topicPartition: TopicPartition): TopicIdPartition = {
    val topicId = metadataCache.getTopicId(topicPartition.topic())
    new TopicIdPartition(topicId, topicPartition)
  }

  def getPartition(topicPartition: TopicPartition): HostedPartition = {
    Option(allPartitions.get(topicPartition)).getOrElse(HostedPartition.None)
  }

  def isAddingReplica(topicPartition: TopicPartition, replicaId: Int): Boolean = {
    getPartition(topicPartition) match {
      case Online(partition) => partition.isAddingReplica(replicaId)
      case _ => false
    }
  }

  // Visible for testing
  def createPartition(topicPartition: TopicPartition): Partition = {
    val partition = Partition(topicPartition, time, this)
    addOnlinePartition(topicPartition, partition)
    partition
  }

  // Visible for testing
  private[server] def addOnlinePartition(topicPartition: TopicPartition, partition: Partition): Unit = {
    allPartitions.put(topicPartition, HostedPartition.Online(partition))
  }

  def onlinePartition(topicPartition: TopicPartition): Option[Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) => Some(partition)
      case _ => None
    }
  }

  // An iterator over all non offline partitions. This is a weakly consistent iterator; a partition made offline after
  // the iterator has been constructed could still be returned by this iterator.
  private def onlinePartitionsIterator: Iterator[Partition] = {
    allPartitions.values.asScala.iterator.flatMap {
      case HostedPartition.Online(partition) => Some(partition)
      case _ => None
    }
  }

  private def offlinePartitionCount: Int = {
    allPartitions.values.asScala.iterator.count(_.getClass == HostedPartition.Offline.getClass)
  }

  def getPartitionOrException(topicPartition: TopicPartition): Partition = {
    getPartitionOrError(topicPartition) match {
      case Left(Errors.KAFKA_STORAGE_ERROR) =>
        throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory")

      case Left(error) =>
        throw error.exception(s"Error while fetching partition state for $topicPartition")

      case Right(partition) => partition
    }
  }

  def getPartitionOrException(topicIdPartition: TopicIdPartition): Partition = {
    getPartitionOrError(topicIdPartition.topicPartition()) match {
      case Left(Errors.KAFKA_STORAGE_ERROR) =>
        throw new KafkaStorageException(s"Partition ${topicIdPartition.topicPartition()} is in an offline log directory")

      case Left(error) =>
        throw error.exception(s"Error while fetching partition state for ${topicIdPartition.topicPartition()}")

      case Right(partition) =>
        // Get topic id for an existing partition from disk if topicId is none get it from the metadata cache
        val topicId = partition.topicId.getOrElse(metadataCache.getTopicId(topicIdPartition.topic()))
        // If topic id is set to zero_uuid fall back to non topic id aware behaviour
        val topicIdNotProvided = topicIdPartition.topicId() == Uuid.ZERO_UUID
        if (topicIdNotProvided || topicId == topicIdPartition.topicId()) {
          partition
        } else {
          throw new UnknownTopicIdException(s"Partition $topicIdPartition's topic id doesn't match the one on disk $topicId.'")
        }
    }
  }

  def getPartitionOrError(topicPartition: TopicPartition): Either[Errors, Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) =>
        Right(partition)

      case HostedPartition.Offline(_) =>
        Left(Errors.KAFKA_STORAGE_ERROR)

      case HostedPartition.None if metadataCache.contains(topicPartition) =>
        // The topic exists, but this broker is no longer a replica of it, so we return NOT_LEADER_OR_FOLLOWER which
        // forces clients to refresh metadata to find the new location. This can happen, for example,
        // during a partition reassignment if a produce request from the client is sent to a broker after
        // the local replica has been deleted.
        Left(Errors.NOT_LEADER_OR_FOLLOWER)

      case HostedPartition.None =>
        Left(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    }
  }

  def localLogOrException(topicPartition: TopicPartition): UnifiedLog = {
    getPartitionOrException(topicPartition).localLogOrException
  }

  def futureLocalLogOrException(topicPartition: TopicPartition): UnifiedLog = {
    getPartitionOrException(topicPartition).futureLocalLogOrException
  }

  def futureLogExists(topicPartition: TopicPartition): Boolean = {
    getPartitionOrException(topicPartition).futureLog.isDefined
  }

  def futureLogOrException(topicPartition: TopicPartition): UnifiedLog = {
    getPartitionOrException(topicPartition).futureLocalLogOrException
  }

  def localLog(topicPartition: TopicPartition): Option[UnifiedLog] = {
    onlinePartition(topicPartition).flatMap(_.log)
  }

  def tryCompleteActions(): Unit = defaultActionQueue.tryCompleteActions()

  def addToActionQueue(action: Runnable): Unit = defaultActionQueue.add(action)

  /**
   * Append messages to leader replicas of the partition, without waiting on replication.
   *
   * Noted that all pending delayed check operations are stored in a queue. All callers to ReplicaManager.appendRecordsToLeader()
   * are expected to call ActionQueue.tryCompleteActions for all affected partitions, without holding any conflicting
   * locks.
   *
   * @param requiredAcks                  the required acks -- it is only used to ensure that the append meets the
   *                                      required acks.
   * @param internalTopicsAllowed         boolean indicating whether internal topics can be appended to
   * @param origin                        source of the append request (ie, client, replication, coordinator)
   * @param entriesPerPartition           the records per topic partition to be appended.
   *                                      If topic partition contains Uuid.ZERO_UUID as topicId the method
   *                                      will fall back to the old behaviour and rely on topic name.
   * @param requestLocal                  container for the stateful instances scoped to this request -- this must correspond to the
   *                                      thread calling this method
   * @param actionQueue                   the action queue to use. ReplicaManager#defaultActionQueue is used by default.
   * @param verificationGuards            the mapping from topic partition to verification guards if transaction verification is used
   */
  def appendRecordsToLeader(
    requiredAcks: Short,
    internalTopicsAllowed: Boolean,
    origin: AppendOrigin,
    entriesPerPartition: Map[TopicIdPartition, MemoryRecords],
    requestLocal: RequestLocal = RequestLocal.noCaching,
    actionQueue: ActionQueue = this.defaultActionQueue,
    verificationGuards: Map[TopicPartition, VerificationGuard] = Map.empty
  ): Map[TopicIdPartition, LogAppendResult] = {
    val startTimeMs = time.milliseconds
    val localProduceResultsWithTopicId = appendToLocalLog(
      internalTopicsAllowed = internalTopicsAllowed,
      origin,
      entriesPerPartition,
      requiredAcks,
      requestLocal,
      verificationGuards.toMap
    )
    debug("Produce to local log in %d ms".format(time.milliseconds - startTimeMs))

    addCompletePurgatoryAction(actionQueue, localProduceResultsWithTopicId)

    localProduceResultsWithTopicId
  }

  /**
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied;
   * if the callback function itself is already synchronized on some object then pass this object to avoid deadlock.
   *
   * Noted that all pending delayed check operations are stored in a queue. All callers to ReplicaManager.appendRecords()
   * are expected to call ActionQueue.tryCompleteActions for all affected partitions, without holding any conflicting
   * locks.
   *
   * @param timeout                       maximum time we will wait to append before returning
   * @param requiredAcks                  number of replicas who must acknowledge the append before sending the response
   * @param internalTopicsAllowed         boolean indicating whether internal topics can be appended to
   * @param origin                        source of the append request (ie, client, replication, coordinator)
   * @param entriesPerPartition           the records per topic partition to be appended.
   *                                      If topic partition contains Uuid.ZERO_UUID as topicId the method
   *                                      will fall back to the old behaviour and rely on topic name.
   * @param responseCallback              callback for sending the response
   * @param recordValidationStatsCallback callback for updating stats on record conversions
   * @param requestLocal                  container for the stateful instances scoped to this request -- this must correspond to the
   *                                      thread calling this method
   * @param verificationGuards            the mapping from topic partition to verification guards if transaction verification is used
   */
  def appendRecords(timeout: Long,
                    requiredAcks: Short,
                    internalTopicsAllowed: Boolean,
                    origin: AppendOrigin,
                    entriesPerPartition: Map[TopicIdPartition, MemoryRecords],
                    responseCallback: Map[TopicIdPartition, PartitionResponse] => Unit,
                    recordValidationStatsCallback: Map[TopicIdPartition, RecordValidationStats] => Unit = _ => (),
                    requestLocal: RequestLocal = RequestLocal.noCaching,
                    verificationGuards: Map[TopicPartition, VerificationGuard] = Map.empty): Unit = {
    if (!isValidRequiredAcks(requiredAcks)) {
      sendInvalidRequiredAcksResponse(entriesPerPartition, responseCallback)
      return
    }

    val localProduceResults = appendRecordsToLeader(
      requiredAcks,
      internalTopicsAllowed,
      origin,
      entriesPerPartition,
      requestLocal,
      defaultActionQueue,
      verificationGuards
    )

    val produceStatus = buildProducePartitionStatus(localProduceResults)

    recordValidationStatsCallback(localProduceResults.map { case (k, v) =>
      k -> v.info.recordValidationStats
    })

    maybeAddDelayedProduce(
      requiredAcks,
      timeout,
      entriesPerPartition,
      localProduceResults,
      produceStatus,
      responseCallback
    )
  }

  /**
   * Handles the produce request by starting any transactional verification before appending.
   *
   * @param timeout                       maximum time we will wait to append before returning
   * @param requiredAcks                  number of replicas who must acknowledge the append before sending the response
   * @param internalTopicsAllowed         boolean indicating whether internal topics can be appended to
   * @param transactionalId               the transactional ID for the produce request or null if there is none.
   * @param entriesPerPartition           the records per partition to be appended
   * @param responseCallback              callback for sending the response
   * @param recordValidationStatsCallback callback for updating stats on record conversions
   * @param requestLocal                  container for the stateful instances scoped to this request -- this must correspond to the
   *                                      thread calling this method
   * @param transactionSupportedOperation determines the supported Operation based on the client's Request api version
   *
   * The responseCallback is wrapped so that it is scheduled on a request handler thread. There, it should be called with
   * that request handler thread's thread local and not the one supplied to this method.
   */
  def handleProduceAppend(timeout: Long,
                          requiredAcks: Short,
                          internalTopicsAllowed: Boolean,
                          transactionalId: String,
                          entriesPerPartition: Map[TopicIdPartition, MemoryRecords],
                          responseCallback: Map[TopicIdPartition, PartitionResponse] => Unit,
                          recordValidationStatsCallback: Map[TopicIdPartition, RecordValidationStats] => Unit = _ => (),
                          requestLocal: RequestLocal = RequestLocal.noCaching,
                          transactionSupportedOperation: TransactionSupportedOperation): Unit = {

    val transactionalProducerInfo = mutable.HashSet[(Long, Short)]()
    val topicPartitionBatchInfo = mutable.Map[TopicPartition, Int]()
    val topicIds = entriesPerPartition.keys.map(tp => tp.topic() -> tp.topicId()).toMap
    entriesPerPartition.foreachEntry { (topicIdPartition, records) =>
      // Produce requests (only requests that require verification) should only have one batch per partition in "batches" but check all just to be safe.
      val transactionalBatches = records.batches.asScala.filter(batch => batch.hasProducerId && batch.isTransactional)
      transactionalBatches.foreach(batch => transactionalProducerInfo.add(batch.producerId, batch.producerEpoch))
      if (transactionalBatches.nonEmpty) topicPartitionBatchInfo.put(topicIdPartition.topicPartition(), records.firstBatch.baseSequence)
    }
    if (transactionalProducerInfo.size > 1) {
      throw new InvalidPidMappingException("Transactional records contained more than one producer ID")
    }

    def postVerificationCallback(newRequestLocal: RequestLocal,
                                 results: (Map[TopicPartition, Errors], Map[TopicPartition, VerificationGuard])): Unit = {
      val (preAppendErrors, verificationGuards) = results
      val errorResults: Map[TopicIdPartition, LogAppendResult] = preAppendErrors.map {
        case (topicPartition, error) =>
          // translate transaction coordinator errors to known producer response errors
          val customException =
            error match {
              case Errors.INVALID_TXN_STATE => Some(error.exception("Partition was not added to the transaction"))
              // Transaction verification can fail with a retriable error that older clients may not
              // retry correctly. Translate these to an error which will cause such clients to retry
              // the produce request. We pick `NOT_ENOUGH_REPLICAS` because it does not trigger a
              // metadata refresh.
              case Errors.NETWORK_EXCEPTION |
                   Errors.COORDINATOR_LOAD_IN_PROGRESS |
                   Errors.COORDINATOR_NOT_AVAILABLE |
                   Errors.NOT_COORDINATOR => Some(new NotEnoughReplicasException(
                s"Unable to verify the partition has been added to the transaction. Underlying error: ${error.toString}"))
              case Errors.CONCURRENT_TRANSACTIONS =>
                if (!transactionSupportedOperation.supportsEpochBump) {
                  Some(new NotEnoughReplicasException(
                    s"Unable to verify the partition has been added to the transaction. Underlying error: ${error.toString}"))
                } else {
                  // Don't convert the Concurrent Transaction exception for TV2. Because the error is very common during
                  // the transaction commit phase. Returning Concurrent Transaction is less confusing to the client.
                  None
                }
              case _ => None
            }
          new TopicIdPartition(topicIds.getOrElse(topicPartition.topic(), Uuid.ZERO_UUID), topicPartition) -> LogAppendResult(
            LogAppendInfo.UNKNOWN_LOG_APPEND_INFO,
            Some(customException.getOrElse(error.exception)),
            hasCustomErrorMessage = customException.isDefined
          )
      }
      // In non-transaction paths, errorResults is typically empty, so we can
      // directly use entriesPerPartition instead of creating a new filtered collection
      val entriesWithoutErrorsPerPartition =
        if (errorResults.nonEmpty) entriesPerPartition.filter { case (key, _) => !errorResults.contains(key) }
        else entriesPerPartition

      val preAppendPartitionResponses = buildProducePartitionStatus(errorResults).map { case (k, status) => k -> status.responseStatus }

      def newResponseCallback(responses: Map[TopicIdPartition, PartitionResponse]): Unit = {
        responseCallback(preAppendPartitionResponses ++ responses)
      }

      appendRecords(
        timeout = timeout,
        requiredAcks = requiredAcks,
        internalTopicsAllowed = internalTopicsAllowed,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = entriesWithoutErrorsPerPartition,
        responseCallback = newResponseCallback,
        recordValidationStatsCallback = recordValidationStatsCallback,
        requestLocal = newRequestLocal,
        verificationGuards = verificationGuards
      )
    }

    if (transactionalProducerInfo.size < 1) {
      postVerificationCallback(
        requestLocal,
        (Map.empty[TopicPartition, Errors], Map.empty[TopicPartition, VerificationGuard])
      )
      return
    }

    // Wrap the callback to be handled on an arbitrary request handler thread
    // when transaction verification is complete. The request local passed in
    // is only used when the callback is executed immediately.
    val wrappedPostVerificationCallback = KafkaRequestHandler.wrapAsyncCallback(
      postVerificationCallback,
      requestLocal
    )

    val retryTimeoutMs = Math.min(addPartitionsToTxnConfig.addPartitionsToTxnRetryBackoffMaxMs(), config.requestTimeoutMs)
    val addPartitionsRetryBackoffMs = addPartitionsToTxnConfig.addPartitionsToTxnRetryBackoffMs()
    val startVerificationTimeMs = time.milliseconds
    def maybeRetryOnConcurrentTransactions(results: (Map[TopicPartition, Errors], Map[TopicPartition, VerificationGuard])): Unit = {
      if (time.milliseconds() - startVerificationTimeMs >= retryTimeoutMs) {
        // We've exceeded the retry timeout, so just call the callback with whatever results we have
        wrappedPostVerificationCallback(results)
      } else if (results._1.values.exists(_ == Errors.CONCURRENT_TRANSACTIONS)) {
        // Retry the verification with backoff
        scheduler.scheduleOnce("retry-add-partitions-to-txn", () => {
          maybeSendPartitionsToTransactionCoordinator(
            topicPartitionBatchInfo,
            transactionalId,
            transactionalProducerInfo.head._1,
            transactionalProducerInfo.head._2,
            maybeRetryOnConcurrentTransactions,
            transactionSupportedOperation
          )
        }, addPartitionsRetryBackoffMs * 1L)
      } else {
        // We don't have concurrent transaction errors, so just call the callback with the results
        wrappedPostVerificationCallback(results)
      }
    }

    maybeSendPartitionsToTransactionCoordinator(
      topicPartitionBatchInfo,
      transactionalId,
      transactionalProducerInfo.head._1,
      transactionalProducerInfo.head._2,
      // If we add partition directly from produce request,
      // we should retry on concurrent transaction error here because:
      //  - the produce backoff adds too much delay
      //  - the produce request is expensive to retry
      if (transactionSupportedOperation.supportsEpochBump) maybeRetryOnConcurrentTransactions else wrappedPostVerificationCallback,
      transactionSupportedOperation
    )
  }

  private def buildProducePartitionStatus(
    results: Map[TopicIdPartition, LogAppendResult]
  ): Map[TopicIdPartition, ProducePartitionStatus] = {
    results.map { case (topicIdPartition, result) =>
      topicIdPartition -> ProducePartitionStatus(
        result.info.lastOffset + 1, // required offset
        new PartitionResponse(
          result.error,
          result.info.firstOffset,
          result.info.logAppendTime,
          result.info.logStartOffset,
          result.info.recordErrors,
          result.errorMessage
        )
      )
    }
  }

  private def addCompletePurgatoryAction(
    actionQueue: ActionQueue,
    appendResults: Map[TopicIdPartition, LogAppendResult]
  ): Unit = {
    actionQueue.add {
      () => appendResults.foreach { case (topicIdPartition, result) =>
        val requestKey = new TopicPartitionOperationKey(topicIdPartition.topicPartition)
        result.info.leaderHwChange match {
          case LeaderHwChange.INCREASED =>
            // some delayed operations may be unblocked after HW changed
            delayedProducePurgatory.checkAndComplete(requestKey)
            delayedFetchPurgatory.checkAndComplete(requestKey)
            delayedDeleteRecordsPurgatory.checkAndComplete(requestKey)
            if (topicIdPartition.topicId != Uuid.ZERO_UUID) delayedShareFetchPurgatory.checkAndComplete(new DelayedShareFetchPartitionKey(
              topicIdPartition.topicId, topicIdPartition.partition))
          case LeaderHwChange.SAME =>
            // probably unblock some follower fetch requests since log end offset has been updated
            delayedFetchPurgatory.checkAndComplete(requestKey)
          case LeaderHwChange.NONE =>
          // nothing
        }
      }
    }
  }

  private def maybeAddDelayedProduce(
    requiredAcks: Short,
    timeoutMs: Long,
    entriesPerPartition: Map[TopicIdPartition, MemoryRecords],
    initialAppendResults: Map[TopicIdPartition, LogAppendResult],
    initialProduceStatus: Map[TopicIdPartition, ProducePartitionStatus],
    responseCallback: Map[TopicIdPartition, PartitionResponse] => Unit,
  ): Unit = {
    if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, initialAppendResults)) {
      // create delayed produce operation
      val produceMetadata = ProduceMetadata(requiredAcks, initialProduceStatus)
      val delayedProduce = new DelayedProduce(timeoutMs, produceMetadata, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
      val producerRequestKeys = entriesPerPartition.keys.map(new TopicPartitionOperationKey(_)).toList

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed produce operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys.asJava)
    } else {
      // we can respond immediately
      val produceResponseStatus = initialProduceStatus.map { case (k, status) => k -> status.responseStatus }
      responseCallback(produceResponseStatus)
    }
  }

  private def sendInvalidRequiredAcksResponse(
    entries: Map[TopicIdPartition, MemoryRecords],
    responseCallback: Map[TopicIdPartition, PartitionResponse] => Unit): Unit = {
    // If required.acks is outside accepted range, something is wrong with the client
    // Just return an error and don't handle the request at all
    val responseStatus = entries.map { case (topicIdPartition, _) =>
      topicIdPartition -> new PartitionResponse(
        Errors.INVALID_REQUIRED_ACKS,
        LogAppendInfo.UNKNOWN_LOG_APPEND_INFO.firstOffset,
        RecordBatch.NO_TIMESTAMP,
        LogAppendInfo.UNKNOWN_LOG_APPEND_INFO.logStartOffset
      )
    }
    responseCallback(responseStatus)
  }

  /**
   *
   * @param topicPartition                the topic partition to maybe verify or add
   * @param transactionalId               the transactional id for the transaction
   * @param producerId                    the producer id for the producer writing to the transaction
   * @param producerEpoch                 the epoch of the producer writing to the transaction
   * @param baseSequence                  the base sequence of the first record in the batch we are trying to append
   * @param callback                      the method to execute once the verification is either completed or returns an error
   * @param transactionSupportedOperation determines the supported operation based on the client's Request API version
   *
   * If this is the first time a partition appears in a transaction, it must be verified or added to the partition depending on the
   * transactionSupported operation.
   * If verifying, when the verification returns, the callback will be supplied the error if it exists or Errors.NONE.
   * If the verification guard exists, it will also be supplied. Otherwise the SENTINEL verification guard will be returned.
   * This guard can not be used for verification and any appends that attempt to use it will fail.
   *
   * If adding, the callback will be supplied the error if it exists or Errors.NONE.
   */
  def maybeSendPartitionToTransactionCoordinator(
    topicPartition: TopicPartition,
    transactionalId: String,
    producerId: Long,
    producerEpoch: Short,
    baseSequence: Int,
    callback: ((Errors, VerificationGuard)) => Unit,
    transactionSupportedOperation: TransactionSupportedOperation
  ): Unit = {
    def generalizedCallback(results: (Map[TopicPartition, Errors], Map[TopicPartition, VerificationGuard])): Unit = {
      val (preAppendErrors, verificationGuards) = results
      callback((
        preAppendErrors.getOrElse(topicPartition, Errors.NONE),
        verificationGuards.getOrElse(topicPartition, VerificationGuard.SENTINEL)
      ))
    }

    maybeSendPartitionsToTransactionCoordinator(
      Map(topicPartition -> baseSequence),
      transactionalId,
      producerId,
      producerEpoch,
      generalizedCallback,
      transactionSupportedOperation
    )
  }

  /**
   *
   * @param topicPartitionBatchInfo         the topic partitions to maybe verify or add mapped to the base sequence of their first record batch
   * @param transactionalId                 the transactional id for the transaction
   * @param producerId                      the producer id for the producer writing to the transaction
   * @param producerEpoch                   the epoch of the producer writing to the transaction
   * @param callback                        the method to execute once the verification is either completed or returns an error
   * @param transactionSupportedOperation   determines the supported operation based on the client's Request API version
   *
   * If this is the first time the partitions appear in a transaction, they must be verified or added to the partition depending on the
   * transactionSupported operation.
   * If verifying, when the verification returns, the callback will be supplied the errors per topic partition if there were errors.
   * The callback will also be supplied the verification guards per partition if they exist. It is possible to have an
   * error and a verification guard for a topic partition if the topic partition was unable to be verified by the transaction
   * coordinator. Transaction coordinator errors are mapped to append-friendly errors.
   *
   * If adding, the callback will be e supplied the errors per topic partition if there were errors.
   */
  private def maybeSendPartitionsToTransactionCoordinator(
    topicPartitionBatchInfo: Map[TopicPartition, Int],
    transactionalId: String,
    producerId: Long,
    producerEpoch: Short,
    callback: ((Map[TopicPartition, Errors], Map[TopicPartition, VerificationGuard])) => Unit,
    transactionSupportedOperation: TransactionSupportedOperation
  ): Unit = {
    def transactionPartitionVerificationEnable = {
      new TransactionLogConfig(config).transactionPartitionVerificationEnable
    }
    // Skip verification if the request is not transactional or transaction verification is disabled.
    if (transactionalId == null
      || addPartitionsToTxnManager.isEmpty
      || (!transactionSupportedOperation.supportsEpochBump && !transactionPartitionVerificationEnable)
    ) {
      callback((Map.empty[TopicPartition, Errors], Map.empty[TopicPartition, VerificationGuard]))
      return
    }

    val verificationGuards = mutable.Map[TopicPartition, VerificationGuard]()
    val errors = mutable.Map[TopicPartition, Errors]()

    topicPartitionBatchInfo.map { case (topicPartition, baseSequence) =>
      val errorOrGuard = maybeStartTransactionVerificationForPartition(
        topicPartition,
        producerId,
        producerEpoch,
        baseSequence,
        transactionSupportedOperation.supportsEpochBump
      )

      errorOrGuard match {
        case Left(error) => errors.put(topicPartition, error)
        case Right(verificationGuard) => if (verificationGuard != VerificationGuard.SENTINEL)
          verificationGuards.put(topicPartition, verificationGuard)
      }
    }

    if (verificationGuards.isEmpty) {
      callback((errors.toMap, Map.empty[TopicPartition, VerificationGuard]))
      return
    }

    def invokeCallback(
      verificationErrors: java.util.Map[TopicPartition, Errors]
    ): Unit = {
      callback((errors ++ verificationErrors.asScala, verificationGuards.toMap))
    }

    addPartitionsToTxnManager.foreach(_.addOrVerifyTransaction(
      transactionalId,
      producerId,
      producerEpoch,
      verificationGuards.keys.toSeq.asJava,
      invokeCallback,
      transactionSupportedOperation
    ))

  }

  private def maybeStartTransactionVerificationForPartition(
    topicPartition: TopicPartition,
    producerId: Long,
    producerEpoch: Short,
    baseSequence: Int,
    supportsEpochBump: Boolean
  ): Either[Errors, VerificationGuard] = {
    try {
      val verificationGuard = getPartitionOrException(topicPartition)
        .maybeStartTransactionVerification(producerId, baseSequence, producerEpoch, supportsEpochBump)
      Right(verificationGuard)
    } catch {
      case e: Exception =>
        Left(Errors.forException(e))
    }
  }

  /**
   * Delete records on leader replicas of the partition, and wait for delete records operation be propagated to other replicas;
   * the callback function will be triggered either when timeout or logStartOffset of all live replicas have reached the specified offset
   */
  private def deleteRecordsOnLocalLog(offsetPerPartition: Map[TopicPartition, Long], allowInternalTopicDeletion: Boolean): Map[TopicPartition, LogDeleteRecordsResult] = {
    trace("Delete records on local logs to offsets [%s]".format(offsetPerPartition))
    offsetPerPartition.map { case (topicPartition, requestedOffset) =>
      // reject delete records operation for internal topics unless allowInternalTopicDeletion is true
      if (Topic.isInternal(topicPartition.topic) && !allowInternalTopicDeletion) {
        (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(new InvalidTopicException(s"Cannot delete records of internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val partition = getPartitionOrException(topicPartition)
          val logDeleteResult = partition.deleteRecordsOnLeader(requestedOffset)
          (topicPartition, logDeleteResult)
        } catch {
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderOrFollowerException |
                   _: OffsetOutOfRangeException |
                   _: PolicyViolationException |
                   _: KafkaStorageException) =>
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(e)))
          case t: Throwable =>
            error("Error processing delete records operation on partition %s".format(topicPartition), t)
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(t)))
        }
      }
    }
  }

  // If there exists a topic partition that meets the following requirement,
  // we need to put a delayed DeleteRecordsRequest and wait for the delete records operation to complete
  //
  // 1. the delete records operation on this partition is successful
  // 2. low watermark of this partition is smaller than the specified offset
  private def delayedDeleteRecordsRequired(localDeleteRecordsResults: Map[TopicPartition, LogDeleteRecordsResult]): Boolean = {
    localDeleteRecordsResults.exists{ case (_, deleteRecordsResult) =>
      deleteRecordsResult.exception.isEmpty && deleteRecordsResult.lowWatermark < deleteRecordsResult.requestedOffset
    }
  }

  /**
   * For each pair of partition and log directory specified in the map, if the partition has already been created on
   * this broker, move its log files to the specified log directory. Otherwise, record the pair in the memory so that
   * the partition will be created in the specified log directory when broker receives LeaderAndIsrRequest for the partition later.
   */
  def alterReplicaLogDirs(partitionDirs: Map[TopicPartition, String]): Map[TopicPartition, Errors] = {
    replicaStateChangeLock synchronized {
      partitionDirs.map { case (topicPartition, destinationDir) =>
        try {
          /* If the topic name is exceptionally long, we can't support altering the log directory.
           * See KAFKA-4893 for details.
           * TODO: fix this by implementing topic IDs. */
          if (UnifiedLog.logFutureDirName(topicPartition).length > 255)
            throw new InvalidTopicException("The topic name is too long.")
          if (!logManager.isLogDirOnline(destinationDir))
            throw new KafkaStorageException(s"Log directory $destinationDir is offline")

          getPartition(topicPartition) match {
            case HostedPartition.Online(partition) =>
              // Stop current replica movement if the destinationDir is different from the existing destination log directory
              if (partition.futureReplicaDirChanged(destinationDir)) {
                replicaAlterLogDirsManager.removeFetcherForPartitions(Set(topicPartition))
                // There's a chance that the future replica can be promoted between the check for futureReplicaDirChanged
                // and call to removeFetcherForPartitions. We want to avoid resuming cleaning again in that case to avoid
                // an IllegalStateException. The presence of a future log after the call to removeFetcherForPartitions
                // implies that it has not been promoted as both synchronize on partitionMapLock.
                val futureReplicaPromoted = partition.futureLog.isEmpty
                partition.removeFutureLocalReplica()
                if (!futureReplicaPromoted) {
                  logManager.resumeCleaning(topicPartition)
                }
              }
            case HostedPartition.Offline(_) =>
              throw new KafkaStorageException(s"Partition $topicPartition is offline")

            case HostedPartition.None => // Do nothing
          }

          // If the log for this partition has not been created yet:
          // 1) Record the destination log directory in the memory so that the partition will be created in this log directory
          //    when broker receives LeaderAndIsrRequest for this partition later.
          // 2) Respond with NotLeaderOrFollowerException for this partition in the AlterReplicaLogDirsResponse
          logManager.maybeUpdatePreferredLogDir(topicPartition, destinationDir)

          // throw NotLeaderOrFollowerException if replica does not exist for the given partition
          val partition = getPartitionOrException(topicPartition)
          val log = partition.localLogOrException
          val topicId = log.topicId

          // If the destinationLDir is different from the current log directory of the replica:
          // - If there is no offline log directory, create the future log in the destinationDir (if it does not exist) and
          //   start ReplicaAlterDirThread to move data of this partition from the current log to the future log
          // - Otherwise, return KafkaStorageException. We do not create the future log while there is offline log directory
          //   so that we can avoid creating future log for the same partition in multiple log directories.
          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints.asJava)
          if (partition.maybeCreateFutureReplica(destinationDir, highWatermarkCheckpoints)) {
            val futureLog = futureLocalLogOrException(topicPartition)
            logManager.abortAndPauseCleaning(topicPartition)

            val initialFetchState = InitialFetchState(topicId.toScala, new BrokerEndPoint(config.brokerId, "localhost", -1),
              partition.getLeaderEpoch, futureLog.highWatermark)
            replicaAlterLogDirsManager.addFetcherForPartitions(Map(topicPartition -> initialFetchState))
          }

          (topicPartition, Errors.NONE)
        } catch {
          case e@(_: InvalidTopicException |
                  _: LogDirNotFoundException |
                  _: ReplicaNotAvailableException |
                  _: KafkaStorageException) =>
            warn(s"Unable to alter log dirs for $topicPartition", e)
            (topicPartition, Errors.forException(e))
          case e: NotLeaderOrFollowerException =>
            // Retaining REPLICA_NOT_AVAILABLE exception for ALTER_REPLICA_LOG_DIRS for compatibility
            warn(s"Unable to alter log dirs for $topicPartition", e)
            (topicPartition, Errors.REPLICA_NOT_AVAILABLE)
          case t: Throwable =>
            error("Error while changing replica dir for partition %s".format(topicPartition), t)
            (topicPartition, Errors.forException(t))
        }
      }
    }
  }

  /*
   * Get the LogDirInfo for the specified list of partitions.
   *
   * Each LogDirInfo specifies the following information for a given log directory:
   * 1) Error of the log directory, e.g. whether the log is online or offline
   * 2) size and lag of current and future logs for each partition in the given log directory. Only logs of the queried partitions
   *    are included. There may be future logs (which will replace the current logs of the partition in the future) on the broker after KIP-113 is implemented.
   */
  def describeLogDirs(partitions: Set[TopicPartition]): util.List[DescribeLogDirsResponseData.DescribeLogDirsResult] = {
    val logsByDir = logManager.allLogs.groupBy(log => log.parentDir)

    config.logDirs.stream().distinct().map(logDir => {
      val file = Paths.get(logDir)
      val absolutePath = file.toAbsolutePath.toString
      try {
        if (!logManager.isLogDirOnline(absolutePath))
          throw new KafkaStorageException(s"Log directory $absolutePath is offline")

        val fileStore = Files.getFileStore(file)
        val totalBytes = adjustForLargeFileSystems(fileStore.getTotalSpace)
        val usableBytes = adjustForLargeFileSystems(fileStore.getUsableSpace)
        val topicInfos = logsByDir.get(absolutePath) match {
          case Some(logs) =>
            logs.groupBy(_.topicPartition.topic).map { case (topic, logs) =>
              new DescribeLogDirsResponseData.DescribeLogDirsTopic().setName(topic).setPartitions(
                logs.filter { log =>
                  partitions.contains(log.topicPartition)
                }.map { log =>
                  new DescribeLogDirsResponseData.DescribeLogDirsPartition()
                    .setPartitionSize(log.size)
                    .setPartitionIndex(log.topicPartition.partition)
                    .setOffsetLag(getLogEndOffsetLag(log.topicPartition, log.logEndOffset, log.isFuture))
                    .setIsFutureKey(log.isFuture)
                }.toList.asJava)
            }.filterNot(_.partitions().isEmpty).toList.asJava
          case None =>
            Collections.emptyList[DescribeLogDirsTopic]()
        }

        val describeLogDirsResult = new DescribeLogDirsResponseData.DescribeLogDirsResult()
          .setLogDir(absolutePath)
          .setTopics(topicInfos)
          .setErrorCode(Errors.NONE.code)
          .setTotalBytes(totalBytes)
          .setUsableBytes(usableBytes)
        describeLogDirsResult

      } catch {
        case e: KafkaStorageException =>
          warn("Unable to describe replica dirs for %s".format(absolutePath), e)
          new DescribeLogDirsResponseData.DescribeLogDirsResult()
            .setLogDir(absolutePath)
            .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)
        case t: Throwable =>
          error(s"Error while describing replica in dir $absolutePath", t)
          new DescribeLogDirsResponseData.DescribeLogDirsResult()
            .setLogDir(absolutePath)
            .setErrorCode(Errors.forException(t).code)
      }
    }).toList()
  }

  // See: https://bugs.openjdk.java.net/browse/JDK-8162520
  private def adjustForLargeFileSystems(space: Long): Long = {
    if (space < 0)
      return Long.MaxValue
    space
  }

  def getLogEndOffsetLag(topicPartition: TopicPartition, logEndOffset: Long, isFuture: Boolean): Long = {
    localLog(topicPartition) match {
      case Some(log) =>
        if (isFuture)
          log.logEndOffset - logEndOffset
        else
          math.max(log.highWatermark - logEndOffset, 0)
      case None =>
        // return -1L to indicate that the LEO lag is not available if the replica is not created or is offline
        DescribeLogDirsResponse.INVALID_OFFSET_LAG
    }
  }

  def deleteRecords(timeout: Long,
                    offsetPerPartition: Map[TopicPartition, Long],
                    responseCallback: Map[TopicPartition, DeleteRecordsPartitionResult] => Unit,
                    allowInternalTopicDeletion: Boolean = false): Unit = {
    val timeBeforeLocalDeleteRecords = time.milliseconds
    val localDeleteRecordsResults = deleteRecordsOnLocalLog(offsetPerPartition, allowInternalTopicDeletion)
    debug("Delete records on local log in %d ms".format(time.milliseconds - timeBeforeLocalDeleteRecords))

    val deleteRecordsStatus = localDeleteRecordsResults.map { case (topicPartition, result) =>
      topicPartition ->
        new DeleteRecordsPartitionStatus(
          result.requestedOffset, // requested offset
          new DeleteRecordsPartitionResult()
            .setLowWatermark(result.lowWatermark)
            .setErrorCode(result.error.code)
            .setPartitionIndex(topicPartition.partition)) // response status
    }

    if (delayedDeleteRecordsRequired(localDeleteRecordsResults)) {
      def onAcks(topicPartition: TopicPartition, status: DeleteRecordsPartitionStatus): Unit = {
        val (lowWatermarkReached, error, lw) = getPartition(topicPartition) match {
          case HostedPartition.Online(partition) =>
            partition.leaderLogIfLocal match {
              case Some(_) =>
                val leaderLW = partition.lowWatermarkIfLeader
                (leaderLW >= status.requiredOffset, Errors.NONE, leaderLW)
              case None =>
                (false, Errors.NOT_LEADER_OR_FOLLOWER, DeleteRecordsResponse.INVALID_LOW_WATERMARK)
            }

          case HostedPartition.Offline(_) =>
            (false, Errors.KAFKA_STORAGE_ERROR, DeleteRecordsResponse.INVALID_LOW_WATERMARK)

          case HostedPartition.None =>
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION, DeleteRecordsResponse.INVALID_LOW_WATERMARK)
        }
        if (error != Errors.NONE || lowWatermarkReached) {
          status.setAcksPending(false)
          status.responseStatus.setErrorCode(error.code)
          status.responseStatus.setLowWatermark(lw)
        }
      }
      // create delayed delete records operation
      val delayedDeleteRecords = new DelayedDeleteRecords(timeout, deleteRecordsStatus.asJava, onAcks,  response => responseCallback(response.asScala))

      // create a list of (topic, partition) pairs to use as keys for this delayed delete records operation
      val deleteRecordsRequestKeys = offsetPerPartition.keys.map(new TopicPartitionOperationKey(_)).toList

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed delete records operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedDeleteRecordsPurgatory.tryCompleteElseWatch(delayedDeleteRecords, deleteRecordsRequestKeys.asJava)
    } else {
      // we can respond immediately
      val deleteRecordsResponseStatus = deleteRecordsStatus.map { case (k, status) => k -> status.responseStatus }
      responseCallback(deleteRecordsResponseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  //
  // 1. required acks = -1
  // 2. there is data to append
  // 3. at least one partition append was successful (fewer errors than partitions)
  private def delayedProduceRequestRequired(requiredAcks: Short,
                                            entriesPerPartition: Map[TopicIdPartition, MemoryRecords],
                                            localProduceResults: Map[TopicIdPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 &&
    entriesPerPartition.nonEmpty &&
    localProduceResults.values.count(_.exception.isDefined) < entriesPerPartition.size
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
   */
  private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               origin: AppendOrigin,
                               entriesPerPartition: Map[TopicIdPartition, MemoryRecords],
                               requiredAcks: Short,
                               requestLocal: RequestLocal,
                               verificationGuards: Map[TopicPartition, VerificationGuard]):
  Map[TopicIdPartition, LogAppendResult] = {
    val traceEnabled = isTraceEnabled
    def processFailedRecord(topicIdPartition: TopicIdPartition, t: Throwable) = {
      val logStartOffset = onlinePartition(topicIdPartition.topicPartition()).map(_.logStartOffset).getOrElse(-1L)
      brokerTopicStats.topicStats(topicIdPartition.topic).failedProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
      t match {
        case _: InvalidProducerEpochException =>
          info(s"Error processing append operation on partition $topicIdPartition", t)
        case _ =>
          error(s"Error processing append operation on partition $topicIdPartition", t)
      }

      logStartOffset
    }

    if (traceEnabled)
      trace(s"Append [$entriesPerPartition] to local log")

    entriesPerPartition.map { case (topicIdPartition, records) =>
      brokerTopicStats.topicStats(topicIdPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      if (Topic.isInternal(topicIdPartition.topic) && !internalTopicsAllowed) {
        (topicIdPartition, LogAppendResult(
          LogAppendInfo.UNKNOWN_LOG_APPEND_INFO,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicIdPartition.topic}")),
          hasCustomErrorMessage = false))
      } else {
        try {
          val partition = getPartitionOrException(topicIdPartition)
          val info = partition.appendRecordsToLeader(records, origin, requiredAcks, requestLocal,
            verificationGuards.getOrElse(topicIdPartition.topicPartition(), VerificationGuard.SENTINEL))
          val numAppendedMessages = info.numMessages

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          brokerTopicStats.topicStats(topicIdPartition.topic).bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.topicStats(topicIdPartition.topic).messagesInRate.mark(numAppendedMessages)
          brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

          if (traceEnabled)
            trace(s"${records.sizeInBytes} written to log $topicIdPartition beginning at offset " +
              s"${info.firstOffset} and ending at offset ${info.lastOffset}")

          (topicIdPartition, LogAppendResult(info, exception = None, hasCustomErrorMessage = false))

        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderOrFollowerException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: KafkaStorageException |
                   _: UnknownTopicIdException) =>
            (topicIdPartition, LogAppendResult(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO, Some(e), hasCustomErrorMessage = false))
          case rve: RecordValidationException =>
            val logStartOffset = processFailedRecord(topicIdPartition, rve.invalidException)
            val recordErrors = rve.recordErrors
            (topicIdPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithAdditionalInfo(logStartOffset, recordErrors),
              Some(rve.invalidException), hasCustomErrorMessage = true))
          case t: Throwable =>
            val logStartOffset = processFailedRecord(topicIdPartition, t)
            (topicIdPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset),
              Some(t), hasCustomErrorMessage = false))
        }
      }
    }
  }

  def fetchOffset(topics: Seq[ListOffsetsTopic],
                  duplicatePartitions: Set[TopicPartition],
                  isolationLevel: IsolationLevel,
                  replicaId: Int,
                  clientId: String,
                  correlationId: Int,
                  version: Short,
                  buildErrorResponse: (Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse,
                  responseCallback: Consumer[util.Collection[ListOffsetsTopicResponse]],
                  timeoutMs: Int = 0): Unit = {
    val statusByPartition = mutable.Map[TopicPartition, ListOffsetsPartitionStatus]()
    topics.foreach { topic =>
      topic.partitions.asScala.foreach { partition =>
        val topicPartition = new TopicPartition(topic.name, partition.partitionIndex)
        if (duplicatePartitions.contains(topicPartition)) {
          debug(s"OffsetRequest with correlation id $correlationId from client $clientId on partition $topicPartition " +
            s"failed because the partition is duplicated in the request.")
          statusByPartition += topicPartition ->
            ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.INVALID_REQUEST, partition))).build()
        } else if (isListOffsetsTimestampUnsupported(partition.timestamp(), version)) {
          statusByPartition += topicPartition ->
            ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.UNSUPPORTED_VERSION, partition))).build()
        } else {
          try {
            val fetchOnlyFromLeader = replicaId != ListOffsetsRequest.DEBUGGING_REPLICA_ID
            val isClientRequest = replicaId == ListOffsetsRequest.CONSUMER_REPLICA_ID
            val isolationLevelOpt = if (isClientRequest)
              Some(isolationLevel)
            else
              None

            val resultHolder = fetchOffsetForTimestamp(topicPartition,
              partition.timestamp,
              isolationLevelOpt,
              if (partition.currentLeaderEpoch == ListOffsetsResponse.UNKNOWN_EPOCH) Optional.empty() else Optional.of(partition.currentLeaderEpoch),
              fetchOnlyFromLeader)

            val status = {
              if (resultHolder.timestampAndOffsetOpt().isPresent) {
                // This case is for normal topic that does not have remote storage.
                val timestampAndOffsetOpt = resultHolder.timestampAndOffsetOpt.get
                var partitionResponse = buildErrorResponse(Errors.NONE, partition)
                if (resultHolder.lastFetchableOffset.isPresent &&
                  timestampAndOffsetOpt.offset >= resultHolder.lastFetchableOffset.get) {
                  resultHolder.maybeOffsetsError.map(e => throw e)
                } else {
                  partitionResponse = new ListOffsetsPartitionResponse()
                    .setPartitionIndex(partition.partitionIndex)
                    .setErrorCode(Errors.NONE.code)
                    .setTimestamp(timestampAndOffsetOpt.timestamp)
                    .setOffset(timestampAndOffsetOpt.offset)
                  if (timestampAndOffsetOpt.leaderEpoch.isPresent && version >= 4)
                    partitionResponse.setLeaderEpoch(timestampAndOffsetOpt.leaderEpoch.get)
                }
                ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(partitionResponse)).build()
              } else if (resultHolder.timestampAndOffsetOpt.isEmpty && resultHolder.futureHolderOpt.isEmpty) {
                // This is an empty offset response scenario
                resultHolder.maybeOffsetsError.map(e => throw e)
                ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.NONE, partition))).build()
              } else if (resultHolder.timestampAndOffsetOpt.isEmpty && resultHolder.futureHolderOpt.isPresent) {
                // This case is for topic enabled with remote storage and we want to search the timestamp in
                // remote storage using async fashion.
                ListOffsetsPartitionStatus.builder()
                  .futureHolderOpt(resultHolder.futureHolderOpt())
                  .lastFetchableOffset(resultHolder.lastFetchableOffset)
                  .maybeOffsetsError(resultHolder.maybeOffsetsError)
                  .build()
              } else {
                throw new IllegalStateException(s"Unexpected result holder state $resultHolder")
              }
            }
            statusByPartition += topicPartition -> status
          } catch {
            // NOTE: These exceptions are special cases since these error messages are typically transient or the client
            // would have received a clear exception and there is no value in logging the entire stack trace for the same
            case e @ (_ : UnknownTopicOrPartitionException |
                      _ : NotLeaderOrFollowerException |
                      _ : UnknownLeaderEpochException |
                      _ : FencedLeaderEpochException |
                      _ : KafkaStorageException |
                      _ : UnsupportedForMessageFormatException) =>
              debug(s"Offset request with correlation id $correlationId from client $clientId on " +
                s"partition $topicPartition failed due to ${e.getMessage}")
              statusByPartition += topicPartition ->
                ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.forException(e), partition))).build()
            // Only V5 and newer ListOffset calls should get OFFSET_NOT_AVAILABLE
            case e: OffsetNotAvailableException =>
              if (version >= 5) {
                statusByPartition += topicPartition ->
                  ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.forException(e), partition))).build()
              } else {
                statusByPartition += topicPartition ->
                  ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.LEADER_NOT_AVAILABLE, partition))).build()
              }

            case e: Throwable =>
              error("Error while responding to offset request", e)
              statusByPartition += topicPartition ->
                ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.forException(e), partition))).build()
          }
        }
      }
    }

    if (delayedRemoteListOffsetsRequired(statusByPartition)) {
      val delayMs: Long = if (timeoutMs > 0) timeoutMs else config.remoteLogManagerConfig.remoteListOffsetsRequestTimeoutMs()
      // create delayed remote list offsets operation
      val delayedRemoteListOffsets = new DelayedRemoteListOffsets(delayMs, version, statusByPartition.asJava, tp => getPartitionOrException(tp), responseCallback)
      // create a list of (topic, partition) pairs to use as keys for this delayed remote list offsets operation
      val listOffsetsRequestKeys = statusByPartition.keys.map(new TopicPartitionOperationKey(_)).toList
      // try to complete the request immediately, otherwise put it into the purgatory
      delayedRemoteListOffsetsPurgatory.tryCompleteElseWatch(delayedRemoteListOffsets, listOffsetsRequestKeys.asJava)
    } else {
      // we can respond immediately
      val responseTopics = statusByPartition.groupBy(e => e._1.topic()).map {
        case (topic, status) =>
          new ListOffsetsTopicResponse().setName(topic).setPartitions(status.values.flatMap(s => Some(s.responseOpt.get())).toList.asJava)
      }.toList
      responseCallback.accept(responseTopics.asJava)
    }
  }

  private def delayedRemoteListOffsetsRequired(responseByPartition: Map[TopicPartition, ListOffsetsPartitionStatus]): Boolean = {
    responseByPartition.values.exists(status => status.futureHolderOpt.isPresent)
  }

  def fetchOffsetForTimestamp(topicPartition: TopicPartition,
                              timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): OffsetResultHolder = {
    val partition = getPartitionOrException(topicPartition)
    partition.fetchOffsetForTimestamp(timestamp, isolationLevel, currentLeaderEpoch, fetchOnlyFromLeader, remoteLogManager)
  }

  /**
   * Initiates an asynchronous remote storage fetch operation for the given remote fetch information.
   *
   * This method schedules a remote fetch task with the remote log manager and sets up the necessary
   * completion handling for the operation. The remote fetch result will be used to populate the
   * delayed remote fetch purgatory when completed.
   *
   * @param remoteFetchInfo The remote storage fetch information
   *
   * @return A tuple containing the remote fetch task and the remote fetch result
   */
  private def processRemoteFetch(remoteFetchInfo: RemoteStorageFetchInfo): (Future[Void], CompletableFuture[RemoteLogReadResult]) = {
    val key = new TopicPartitionOperationKey(remoteFetchInfo.topicIdPartition)
    val remoteFetchResult = new CompletableFuture[RemoteLogReadResult]
    var remoteFetchTask: Future[Void] = null
    try {
      remoteFetchTask = remoteLogManager.get.asyncRead(remoteFetchInfo, (result: RemoteLogReadResult) => {
        remoteFetchResult.complete(result)
        delayedRemoteFetchPurgatory.checkAndComplete(key)
      })
    } catch {
      case e: RejectedExecutionException =>
        warn(s"Unable to fetch data from remote storage for remoteFetchInfo: $remoteFetchInfo", e)
        // Store the error in RemoteLogReadResult if any in scheduling the remote fetch task.
        // It will be sent back to the client in DelayedRemoteFetch along with other successful remote fetch results.
        remoteFetchResult.complete(new RemoteLogReadResult(Optional.empty, Optional.of(e)))
    }

    (remoteFetchTask, remoteFetchResult)
  }

  /**
   * Process all remote fetches by creating async read tasks and handling them in DelayedRemoteFetch collectively.
   */
  private def processRemoteFetches(remoteFetchInfos: util.LinkedHashMap[TopicIdPartition, RemoteStorageFetchInfo],
                                   params: FetchParams,
                                   responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit,
                                   logReadResults: util.LinkedHashMap[TopicIdPartition, LogReadResult],
                                   fetchPartitionStatus: util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]): Unit = {
    val remoteFetchTasks = new util.HashMap[TopicIdPartition, Future[Void]]
    val remoteFetchResults = new util.HashMap[TopicIdPartition, CompletableFuture[RemoteLogReadResult]]

    remoteFetchInfos.forEach { (topicIdPartition, remoteFetchInfo) =>
      val (task, result) = processRemoteFetch(remoteFetchInfo)
      remoteFetchTasks.put(topicIdPartition, task)
      remoteFetchResults.put(topicIdPartition, result)
    }

    val remoteFetchMaxWaitMs = config.remoteLogManagerConfig.remoteFetchMaxWaitMs().toLong
    val remoteFetch = new DelayedRemoteFetch(remoteFetchTasks,
                                             remoteFetchResults,
                                             remoteFetchInfos,
                                             remoteFetchMaxWaitMs,
                                             fetchPartitionStatus,
                                             params,
                                             logReadResults,
                                             tp => getPartitionOrException(tp),
                                             response => responseCallback(response.asScala.toSeq))

    // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
    val delayedFetchKeys = remoteFetchTasks.asScala.map { case (tp, _) => new TopicPartitionOperationKey(tp) }.toList
    // We only guarantee eventual cleanup via the next FETCH request for the same set of partitions or
    // using reaper-thread.
    delayedRemoteFetchPurgatory.tryCompleteElseWatch(remoteFetch, delayedFetchKeys.asJava)
  }

  /**
   * Fetch messages from a replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied.
   * Consumers may fetch from any replica, but followers can only fetch from the leader.
   */
  def fetchMessages(params: FetchParams,
                    fetchInfos: Seq[(TopicIdPartition, PartitionData)],
                    quota: ReplicaQuota,
                    responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit): Unit = {

    // check if this fetch request can be satisfied right away
    val logReadResults = readFromLog(params, fetchInfos, quota, readFromPurgatory = false)
    var bytesReadable: Long = 0
    var errorReadingData = false

    // topic-partitions that have to be read from remote storage
    val remoteFetchInfos = new util.LinkedHashMap[TopicIdPartition, RemoteStorageFetchInfo]()

    var hasDivergingEpoch = false
    var hasPreferredReadReplica = false
    val logReadResultMap = new util.LinkedHashMap[TopicIdPartition, LogReadResult]

    logReadResults.foreach { case (topicIdPartition, logReadResult) =>
      brokerTopicStats.topicStats(topicIdPartition.topicPartition.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()
      if (logReadResult.error != Errors.NONE)
        errorReadingData = true
      if (logReadResult.info.delayedRemoteStorageFetch.isPresent) {
        remoteFetchInfos.put(topicIdPartition, logReadResult.info.delayedRemoteStorageFetch.get())
      }
      if (logReadResult.divergingEpoch.isPresent)
        hasDivergingEpoch = true
      if (logReadResult.preferredReadReplica.isPresent)
        hasPreferredReadReplica = true
      bytesReadable = bytesReadable + logReadResult.info.records.sizeInBytes
      logReadResultMap.put(topicIdPartition, logReadResult)
    }

    // Respond immediately if no remote fetches are required and any of the below conditions is true
    //                        1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) has enough data to respond
    //                        4) some error happens while reading data
    //                        5) we found a diverging epoch
    //                        6) has a preferred read replica
    if (remoteFetchInfos.isEmpty && (params.maxWaitMs <= 0 || fetchInfos.isEmpty || bytesReadable >= params.minBytes || errorReadingData ||
      hasDivergingEpoch || hasPreferredReadReplica)) {
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        val isReassignmentFetch = params.isFromFollower && isAddingReplica(tp.topicPartition, params.replicaId)
        tp -> result.toFetchPartitionData(isReassignmentFetch)
      }
      responseCallback(fetchPartitionData)
    } else {
      // construct the fetch results from the read results
      val fetchPartitionStatus = new util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]
      fetchInfos.foreach { case (topicIdPartition, partitionData) =>
        val logReadResult = logReadResultMap.get(topicIdPartition)
        if (logReadResult != null) {
          val logOffsetMetadata = logReadResult.info.fetchOffsetMetadata
          fetchPartitionStatus.put(topicIdPartition, new FetchPartitionStatus(logOffsetMetadata, partitionData))
        }
      }

      if (!remoteFetchInfos.isEmpty) {
        processRemoteFetches(remoteFetchInfos, params, responseCallback, logReadResultMap, fetchPartitionStatus)
      } else {
        // If there is not enough data to respond and there is no remote data, we will let the fetch request
        // wait for new data.
        val delayedFetch = new DelayedFetch(
          params = params,
          fetchPartitionStatus = fetchPartitionStatus,
          replicaManager = this,
          quota = quota,
          responseCallback = responseCallback
        )

        // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
        val delayedFetchKeys = fetchPartitionStatus.keySet()
          .stream()
          .map(new TopicPartitionOperationKey(_))
          .toList()

        // try to complete the request immediately, otherwise put it into the purgatory;
        // this is because while the delayed fetch operation is being created, new requests
        // may arrive and hence make this operation completable.
        delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
      }
    }
  }

  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
   */
  def readFromLog(
    params: FetchParams,
    readPartitionInfo: Seq[(TopicIdPartition, PartitionData)],
    quota: ReplicaQuota,
    readFromPurgatory: Boolean): Seq[(TopicIdPartition, LogReadResult)] = {
    val traceEnabled = isTraceEnabled

    def checkFetchDataInfo(partition: Partition, givenFetchedDataInfo: FetchDataInfo) = {
      if (params.isFromFollower && shouldLeaderThrottle(quota, partition, params.replicaId)) {
        // If the partition is being throttled, simply return an empty set.
        new FetchDataInfo(givenFetchedDataInfo.fetchOffsetMetadata, MemoryRecords.EMPTY)
      } else if (givenFetchedDataInfo.firstEntryIncomplete) {
        // Replace incomplete message sets with an empty one as consumers can make progress in such
        // cases and don't need to report a `RecordTooLargeException`
        new FetchDataInfo(givenFetchedDataInfo.fetchOffsetMetadata, MemoryRecords.EMPTY)
      } else {
        givenFetchedDataInfo
      }
    }

    def read(tp: TopicIdPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      val adjustedMaxBytes = math.min(fetchInfo.maxBytes, limitBytes)
      var log: UnifiedLog = null
      var partition : Partition = null
      val fetchTimeMs = time.milliseconds
      try {
        if (traceEnabled)
          trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
            s"remaining response limit $limitBytes" +
            (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        partition = getPartitionOrException(tp.topicPartition)

        // Check if topic ID from the fetch request/session matches the ID in the log
        val topicId = if (tp.topicId == Uuid.ZERO_UUID) None else Some(tp.topicId)
        if (!hasConsistentTopicId(topicId, partition.topicId))
          throw new InconsistentTopicIdException("Topic ID in the fetch session did not match the topic ID in the log.")

        // If we are the leader, determine the preferred read-replica
        val preferredReadReplica = params.clientMetadata.toScala.flatMap(
          metadata => findPreferredReadReplica(partition, metadata, params.replicaId, fetchInfo.fetchOffset, fetchTimeMs))

        if (preferredReadReplica.isDefined) {
          replicaSelectorPlugin.foreach { selector =>
            debug(s"Replica selector ${selector.get.getClass.getSimpleName} returned preferred replica " +
              s"${preferredReadReplica.get} for ${params.clientMetadata}")
          }
          // If a preferred read-replica is set, skip the read
          val offsetSnapshot = partition.fetchOffsetSnapshot(fetchInfo.currentLeaderEpoch, fetchOnlyFromLeader = false)
          new LogReadResult(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
            Optional.empty(),
            offsetSnapshot.highWatermark.messageOffset,
            offsetSnapshot.logStartOffset,
            offsetSnapshot.logEndOffset.messageOffset,
            followerLogStartOffset,
            -1L,
            OptionalLong.of(offsetSnapshot.lastStableOffset.messageOffset),
            if (preferredReadReplica.isDefined) OptionalInt.of(preferredReadReplica.get) else OptionalInt.empty(),
            Errors.NONE)
        } else {
          log = partition.localLogWithEpochOrThrow(fetchInfo.currentLeaderEpoch, params.fetchOnlyLeader())

          // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
          val readInfo: LogReadInfo = partition.fetchRecords(
            fetchParams = params,
            fetchPartitionData = fetchInfo,
            fetchTimeMs = fetchTimeMs,
            maxBytes = adjustedMaxBytes,
            minOneMessage = minOneMessage,
            updateFetchState = !readFromPurgatory)

          val fetchDataInfo = checkFetchDataInfo(partition, readInfo.fetchedData)

          new LogReadResult(fetchDataInfo,
            readInfo.divergingEpoch,
            readInfo.highWatermark,
            readInfo.logStartOffset,
            readInfo.logEndOffset,
            followerLogStartOffset,
            fetchTimeMs,
            OptionalLong.of(readInfo.lastStableOffset),
            if (preferredReadReplica.isDefined) OptionalInt.of(preferredReadReplica.get) else OptionalInt.empty(),
            Errors.NONE
          )
        }
      } catch {
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        case e@ (_: UnknownTopicOrPartitionException |
                 _: NotLeaderOrFollowerException |
                 _: UnknownLeaderEpochException |
                 _: FencedLeaderEpochException |
                 _: ReplicaNotAvailableException |
                 _: KafkaStorageException |
                 _: InconsistentTopicIdException) =>
          new LogReadResult(Errors.forException(e))
        case e: OffsetOutOfRangeException =>
          handleOffsetOutOfRangeError(tp, params, fetchInfo, adjustedMaxBytes, minOneMessage, log, fetchTimeMs, e)
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()

          val fetchSource = FetchRequest.describeReplicaId(params.replicaId)
          error(s"Error processing fetch with max size $adjustedMaxBytes from $fetchSource " +
            s"on partition $tp: $fetchInfo", e)

          new LogReadResult(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
            Optional.empty(),
            UnifiedLog.UNKNOWN_OFFSET,
            UnifiedLog.UNKNOWN_OFFSET,
            UnifiedLog.UNKNOWN_OFFSET,
            UnifiedLog.UNKNOWN_OFFSET,
            -1L,
            OptionalLong.empty(),
            Errors.forException(e)
          )
      }
    }

    var limitBytes = params.maxBytes
    val result = new mutable.ArrayBuffer[(TopicIdPartition, LogReadResult)]
    var minOneMessage = true
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      val recordBatchSize = readResult.info.records.sizeInBytes
      // Because we don't know how much data will be retrieved in remote fetch yet, and we don't want to block the API call
      // to query remoteLogMetadata, assume it will fetch the max bytes size of data to avoid to exceed the "fetch.max.bytes" setting.
      val estimatedRecordBatchSize = if (recordBatchSize == 0 && readResult.info.delayedRemoteStorageFetch.isPresent)
        readResult.info.delayedRemoteStorageFetch.get.fetchMaxBytes else recordBatchSize
      // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
      if (estimatedRecordBatchSize > 0)
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - estimatedRecordBatchSize)
      result += (tp -> readResult)
    }
    result
  }

  private def handleOffsetOutOfRangeError(tp: TopicIdPartition, params: FetchParams, fetchInfo: PartitionData,
                                          adjustedMaxBytes: Int, minOneMessage:
                                          Boolean, log: UnifiedLog, fetchTimeMs: Long,
                                          exception: OffsetOutOfRangeException): LogReadResult = {
    val offset = fetchInfo.fetchOffset
    // In case of offset out of range errors, handle it for tiered storage only if all the below conditions are true.
    //   1) remote log manager is enabled and it is available
    //   2) `log` instance should not be null here as that would have been caught earlier with NotLeaderOrFollowerException or ReplicaNotAvailableException.
    //   3) fetch offset is within the offset range of the remote storage layer
    if (remoteLogManager.isDefined && log != null && log.remoteLogEnabled() &&
      log.logStartOffset <= offset && offset < log.localLogStartOffset())
    {
      val highWatermark = log.highWatermark
      val leaderLogStartOffset = log.logStartOffset
      val leaderLogEndOffset = log.logEndOffset

      if (params.isFromFollower || params.isFromFuture) {
        // If it is from a follower or from a future replica, then send the offset metadata only as the data is already available in remote
        // storage and throw an error saying that this offset is moved to tiered storage.
        createLogReadResult(highWatermark, leaderLogStartOffset, leaderLogEndOffset,
          new OffsetMovedToTieredStorageException("Given offset" + offset + " is moved to tiered storage"))
      } else {
        val throttleTimeMs = remoteLogManager.get.getFetchThrottleTimeMs
        val fetchDataInfo = if (throttleTimeMs > 0) {
          // Record the throttle time for the remote log fetches
          remoteLogManager.get.fetchThrottleTimeSensor().record(throttleTimeMs, time.milliseconds())

          // We do not want to send an exception in a LogReadResult response (like we do in other cases when we send
          // UnknownOffsetMetadata), because it is classified as an error in reading the data, and a response is
          // immediately sent back to the client. Instead, we want to serve data for the other topic partitions of the
          // fetch request via delayed fetch if required (when sending immediate response, we skip delayed fetch).
          new FetchDataInfo(
            LogOffsetMetadata.UNKNOWN_OFFSET_METADATA,
            MemoryRecords.EMPTY,
            false,
            Optional.empty(),
            Optional.empty()
          )
        } else {
          val remoteStorageFetchInfoOpt = if (adjustedMaxBytes > 0) {
            // For consume fetch requests, create a dummy FetchDataInfo with the remote storage fetch information.
            // For the topic-partitions that need remote data, we will use this information to read the data in another thread.
            Optional.of(new RemoteStorageFetchInfo(adjustedMaxBytes, minOneMessage, tp, fetchInfo, params.isolation))
          } else {
            Optional.empty[RemoteStorageFetchInfo]()
          }
          new FetchDataInfo(new LogOffsetMetadata(offset), MemoryRecords.EMPTY, false, Optional.empty(), remoteStorageFetchInfoOpt)
        }

        new LogReadResult(fetchDataInfo,
          Optional.empty(),
          highWatermark,
          leaderLogStartOffset,
          leaderLogEndOffset,
          fetchInfo.logStartOffset,
          fetchTimeMs,
          OptionalLong.of(log.lastStableOffset),
          Errors.NONE)
      }
    } else {
      new LogReadResult(Errors.forException(exception))
    }
  }

  /**
    * Using the configured [[ReplicaSelector]], determine the preferred read replica for a partition given the
    * client metadata, the requested offset, and the current set of replicas. If the preferred read replica is the
    * leader, return None
    */
  def findPreferredReadReplica(partition: Partition,
                               clientMetadata: ClientMetadata,
                               replicaId: Int,
                               fetchOffset: Long,
                               currentTimeMs: Long): Option[Int] = {
    partition.leaderIdIfLocal.flatMap { leaderReplicaId =>
      // Don't look up preferred for follower fetches via normal replication
      if (FetchRequest.isValidBrokerId(replicaId))
        None
      else {
        replicaSelectorPlugin.flatMap { replicaSelector =>
          val replicaEndpoints = metadataCache.getPartitionReplicaEndpoints(partition.topicPartition,
            new ListenerName(clientMetadata.listenerName)).asScala
          val replicaInfoSet = mutable.Set[ReplicaView]()

          partition.remoteReplicas.foreach { replica =>
            val replicaState = replica.stateSnapshot
            // Exclude replicas that are not in the ISR as the follower may lag behind. Worst case, the follower
            // will continue to lag and the consumer will fall behind the produce. The leader will
            // continuously pick the lagging follower when the consumer refreshes its preferred read replica.
            // This can go on indefinitely.
            if (partition.inSyncReplicaIds.contains(replica.brokerId) &&
                replicaState.logEndOffset >= fetchOffset &&
                replicaState.logStartOffset <= fetchOffset) {

              replicaInfoSet.add(new DefaultReplicaView(
                replicaEndpoints.getOrElse(replica.brokerId, Node.noNode()),
                replicaState.logEndOffset,
                currentTimeMs - replicaState.lastCaughtUpTimeMs
              ))
            }
          }

          val leaderReplica = new DefaultReplicaView(
            replicaEndpoints.getOrElse(leaderReplicaId, Node.noNode()),
            partition.localLogOrException.logEndOffset,
            0L
          )
          replicaInfoSet.add(leaderReplica)

          val partitionInfo = new DefaultPartitionView(replicaInfoSet.asJava, leaderReplica)
          replicaSelector.get.select(partition.topicPartition, clientMetadata, partitionInfo).toScala.collect {
            // Even though the replica selector can return the leader, we don't want to send it out with the
            // FetchResponse, so we exclude it here
            case selected if !selected.endpoint.isEmpty && selected != leaderReplica => selected.endpoint.id
          }
        }
      }
    }
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, partition: Partition, replicaId: Int): Boolean = {
    val isReplicaInSync = partition.inSyncReplicaIds.contains(replicaId)
    !isReplicaInSync && quota.isThrottled(partition.topicPartition) && quota.isQuotaExceeded
  }

  def getLogConfig(topicPartition: TopicPartition): Option[LogConfig] = localLog(topicPartition).map(_.config)

  /**
   * Checks if the topic ID provided in the request is consistent with the topic ID in the log.
   * When using this method to handle a Fetch request, the topic ID may have been provided by an earlier request.
   *
   * If the request had an invalid topic ID (null or zero), then we assume that topic IDs are not supported.
   * The topic ID was not inconsistent, so return true.
   * If the log does not exist or the topic ID is not yet set, logTopicIdOpt will be None.
   * In both cases, the ID is not inconsistent so return true.
   *
   * @param requestTopicIdOpt the topic ID from the request if it exists
   * @param logTopicIdOpt the topic ID in the log if the log and the topic ID exist
   * @return true if the request topic id is consistent, false otherwise
   */
  private def hasConsistentTopicId(requestTopicIdOpt: Option[Uuid], logTopicIdOpt: Option[Uuid]): Boolean = {
    requestTopicIdOpt match {
      case None => true
      case Some(requestTopicId) => logTopicIdOpt.isEmpty || logTopicIdOpt.contains(requestTopicId)
    }
  }

  /**
   * KAFKA-8392
   * For topic partitions of which the broker is no longer a leader, delete metrics related to
   * those topics. Note that this means the broker stops being either a replica or a leader of
   * partitions of said topics
   */
  private def updateLeaderAndFollowerMetrics(newFollowerTopics: Set[String]): Unit = {
    val leaderTopicSet = leaderPartitionsIterator.map(_.topic).toSet
    newFollowerTopics.diff(leaderTopicSet).foreach(brokerTopicStats.removeOldLeaderMetrics)
    // Currently, there are no follower metrics that need to be updated.
  }

  protected[server] def maybeAddLogDirFetchers(partitions: Set[Partition],
                                               offsetCheckpoints: OffsetCheckpoints,
                                               topicIds: String => Option[Uuid]): Unit = {
    val futureReplicasAndInitialOffset = new mutable.HashMap[TopicPartition, InitialFetchState]
    for (partition <- partitions) {
      val topicPartition = partition.topicPartition
      logManager.getLog(topicPartition, isFuture = true).foreach { futureLog =>
        partition.log.foreach { _ =>
          val leader = new BrokerEndPoint(config.brokerId, "localhost", -1)

          // Add future replica log to partition's map if it's not existed
          if (partition.maybeCreateFutureReplica(futureLog.parentDir, offsetCheckpoints, topicIds(partition.topic))) {
            // pause cleaning for partitions that are being moved and start ReplicaAlterDirThread to move
            // replica from source dir to destination dir
            logManager.abortAndPauseCleaning(topicPartition)
          }

          futureReplicasAndInitialOffset.put(topicPartition, InitialFetchState(topicIds(topicPartition.topic), leader,
            partition.getLeaderEpoch, futureLog.highWatermark))
        }
      }
    }

    if (futureReplicasAndInitialOffset.nonEmpty) {
      // Even though it's possible that there is another thread adding fetcher for this future log partition,
      // but it's fine because `BrokerIdAndFetcherId` will be identical and the operation will be no-op.
      replicaAlterLogDirsManager.addFetcherForPartitions(futureReplicasAndInitialOffset)
    }
  }

  /**
   * From IBP 2.7 onwards, we send latest fetch epoch in the request and truncate if a
   * diverging epoch is returned in the response, avoiding the need for a separate
   * OffsetForLeaderEpoch request.
   */
  protected def initialFetchOffset(log: UnifiedLog): Long = {
    if (log.latestEpoch.isPresent)
      log.logEndOffset
    else
      log.highWatermark
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")

    // Shrink ISRs for non offline partitions
    allPartitions.forEach { (topicPartition, _) =>
      onlinePartition(topicPartition).foreach(_.maybeShrinkIsr())
    }
  }

  private def leaderPartitionsIterator: Iterator[Partition] =
    onlinePartitionsIterator.filter(_.leaderLogIfLocal.isDefined)

  def getLogEndOffset(topicPartition: TopicPartition): Option[Long] =
    onlinePartition(topicPartition).flatMap(_.leaderLogIfLocal.map(_.logEndOffset))

  // Flushes the highwatermark value for all partitions to the highwatermark file
  def checkpointHighWatermarks(): Unit = {
    def putHw(logDirToCheckpoints: mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, JLong]],
              log: UnifiedLog): Unit = {
      val checkpoints = logDirToCheckpoints.getOrElseUpdate(log.parentDir,
        new mutable.AnyRefMap[TopicPartition, JLong]())
      checkpoints.put(log.topicPartition, log.highWatermark)
    }

    val logDirToHws = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, JLong]](
      allPartitions.size)
    onlinePartitionsIterator.foreach { partition =>
      partition.log.foreach(putHw(logDirToHws, _))
      partition.futureLog.foreach(putHw(logDirToHws, _))
    }

    for ((logDir, hws) <- logDirToHws) {
      try highWatermarkCheckpoints.get(logDir).foreach(_.write(hws.asJava))
      catch {
        case e: KafkaStorageException =>
          error(s"Error while writing to highwatermark file in directory $logDir", e)
      }
    }
  }

  def markPartitionOffline(tp: TopicPartition): Unit = replicaStateChangeLock synchronized {
    allPartitions.get(tp) match {
      case HostedPartition.Online(partition) =>
        allPartitions.put(tp, HostedPartition.Offline(Some(partition)))
        partition.markOffline()
      case _ =>
        allPartitions.put(tp, HostedPartition.Offline(None))
    }
  }

  /**
   * The log directory failure handler for the replica
   *
   * @param dir                     the absolute path of the log directory
   * @param notifyController        check if we need to send notification to the Controller (needed for unit test)
   */
  def handleLogDirFailure(dir: String, notifyController: Boolean = true): Unit = {
    if (!logManager.isLogDirOnline(dir))
      return
    // retrieve the UUID here because logManager.handleLogDirFailure handler removes it
    val uuid = logManager.directoryId(dir)
    warn(s"Stopping serving replicas in dir $dir with uuid $uuid because the log directory has failed.")
    replicaStateChangeLock synchronized {
      val newOfflinePartitions = onlinePartitionsIterator.filter { partition =>
        partition.log.exists { _.parentDir == dir }
      }.map(_.topicPartition).toSet

      val partitionsWithOfflineFutureReplica = onlinePartitionsIterator.filter { partition =>
        partition.futureLog.exists { _.parentDir == dir }
      }.toSet

      replicaFetcherManager.removeFetcherForPartitions(newOfflinePartitions)
      replicaAlterLogDirsManager.removeFetcherForPartitions(newOfflinePartitions ++ partitionsWithOfflineFutureReplica.map(_.topicPartition))

      partitionsWithOfflineFutureReplica.foreach(partition => partition.removeFutureLocalReplica(deleteFromLogDir = false))
      newOfflinePartitions.foreach { topicPartition =>
        markPartitionOffline(topicPartition)
      }
      newOfflinePartitions.map(_.topic).foreach { topic: String =>
        maybeRemoveTopicMetrics(topic)
      }
      highWatermarkCheckpoints = highWatermarkCheckpoints.filter { case (checkpointDir, _) => checkpointDir != dir }

      warn(s"Broker $localBrokerId stopped fetcher for partitions ${newOfflinePartitions.mkString(",")} and stopped moving logs " +
           s"for partitions ${partitionsWithOfflineFutureReplica.mkString(",")} because they are in the failed log directory $dir.")
    }
    logManager.handleLogDirFailure(dir)
    if (dir == new File(config.metadataLogDir).getAbsolutePath && config.processRoles.nonEmpty) {
      fatal(s"Shutdown broker because the metadata log dir $dir has failed")
      Exit.halt(1)
    }

    if (notifyController) {
      if (uuid.isDefined) {
        directoryEventHandler.handleFailure(uuid.get)
      } else {
        fatal(s"Unable to propagate directory failure disabled because directory $dir has no UUID")
        Exit.halt(1)
      }
    }
    warn(s"Stopped serving replicas in dir $dir")
  }

  def removeMetrics(): Unit = {
    ReplicaManager.MetricNames.foreach(metricsGroup.removeMetric)
  }

  def beginControlledShutdown(): Unit = {
    isInControlledShutdown = true
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true): Unit = {
    info("Shutting down")
    removeMetrics()
    if (logDirFailureHandler != null)
      logDirFailureHandler.shutdown()
    replicaFetcherManager.shutdown()
    replicaAlterLogDirsManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedRemoteFetchPurgatory.shutdown()
    delayedRemoteListOffsetsPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    delayedDeleteRecordsPurgatory.shutdown()
    delayedShareFetchPurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    replicaSelectorPlugin.foreach(_.close)
    removeAllTopicMetrics()
    addPartitionsToTxnManager.foreach(_.shutdown())
    info("Shut down completely")
  }

  private def removeAllTopicMetrics(): Unit = {
    val allTopics = new util.HashSet[String]
    allPartitions.forEach((partition, _) =>
      if (allTopics.add(partition.topic())) {
        brokerTopicStats.removeMetrics(partition.topic())
      })
  }

  protected def createReplicaFetcherManager(metrics: Metrics, time: Time, quotaManager: ReplicationQuotaManager) = {
    new ReplicaFetcherManager(config, this, metrics, time, quotaManager, () => metadataCache.metadataVersion(), brokerEpochSupplier)
  }

  protected def createReplicaAlterLogDirsManager(quotaManager: ReplicationQuotaManager, brokerTopicStats: BrokerTopicStats) = {
    new ReplicaAlterLogDirsManager(config, this, quotaManager, brokerTopicStats, directoryEventHandler)
  }

  private def createReplicaSelector(metrics: Metrics): Option[Plugin[ReplicaSelector]] = {
    config.replicaSelectorClassName.map { className =>
      val tmpReplicaSelector: ReplicaSelector = Utils.newInstance(className, classOf[ReplicaSelector])
      tmpReplicaSelector.configure(config.originals())
      Plugin.wrapInstance(tmpReplicaSelector, metrics, ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG)
    }
  }

  def lastOffsetForLeaderEpoch(
    requestedEpochInfo: Seq[OffsetForLeaderTopic]
  ): Seq[OffsetForLeaderTopicResult] = {
    requestedEpochInfo.map { offsetForLeaderTopic =>
      val partitions = offsetForLeaderTopic.partitions.asScala.map { offsetForLeaderPartition =>
        val tp = new TopicPartition(offsetForLeaderTopic.topic, offsetForLeaderPartition.partition)
        getPartition(tp) match {
          case HostedPartition.Online(partition) =>
            val currentLeaderEpochOpt =
              if (offsetForLeaderPartition.currentLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH)
                Optional.empty[Integer]
              else
                Optional.of[Integer](offsetForLeaderPartition.currentLeaderEpoch)

            partition.lastOffsetForLeaderEpoch(
              currentLeaderEpochOpt,
              offsetForLeaderPartition.leaderEpoch,
              fetchOnlyFromLeader = true)

          case HostedPartition.Offline(_) =>
            new EpochEndOffset()
              .setPartition(offsetForLeaderPartition.partition)
              .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)

          case HostedPartition.None if metadataCache.contains(tp) =>
            new EpochEndOffset()
              .setPartition(offsetForLeaderPartition.partition)
              .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)

          case HostedPartition.None =>
            new EpochEndOffset()
              .setPartition(offsetForLeaderPartition.partition)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        }
      }

      new OffsetForLeaderTopicResult()
        .setTopic(offsetForLeaderTopic.topic)
        .setPartitions(partitions.toList.asJava)
    }
  }

  def activeProducerState(requestPartition: TopicPartition): DescribeProducersResponseData.PartitionResponse = {
    getPartitionOrError(requestPartition) match {
      case Left(error) => new DescribeProducersResponseData.PartitionResponse()
        .setPartitionIndex(requestPartition.partition)
        .setErrorCode(error.code)
      case Right(partition) => partition.activeProducerState
    }
  }

  private[kafka] def getOrCreatePartition(tp: TopicPartition,
                                          delta: TopicsDelta,
                                          topicId: Uuid): Option[(Partition, Boolean)] = {
    getPartition(tp) match {
      case HostedPartition.Offline(offlinePartition) =>
        if (offlinePartition.flatMap(p => p.topicId).contains(topicId)) {
          stateChangeLogger.warn(s"Unable to bring up new local leader $tp " +
            s"with topic id $topicId because it resides in an offline log " +
            "directory.")
          None
        } else {
          stateChangeLogger.info(s"Creating new partition $tp with topic id " + s"$topicId." +
            s"A topic with the same name but different id exists but it resides in an offline log " +
            s"directory.")
          val partition = Partition(new TopicIdPartition(topicId, tp), time, this)
          allPartitions.put(tp, HostedPartition.Online(partition))
          Some(partition, true)
        }

      case HostedPartition.Online(partition) =>
        if (partition.topicId.exists(_ != topicId)) {
          // Note: Partition#topicId will be None here if the Log object for this partition
          // has not been created.
          throw new IllegalStateException(s"Topic $tp exists, but its ID is " +
            s"${partition.topicId.get}, not $topicId as expected")
        }
        Some(partition, false)

      case HostedPartition.None =>
        if (delta.image().topicsById().containsKey(topicId)) {
          stateChangeLogger.error(s"Expected partition $tp with topic id " +
            s"$topicId to exist, but it was missing. Creating...")
        } else {
          stateChangeLogger.info(s"Creating new partition $tp with topic id " +
            s"$topicId.")
        }
        // it's a partition that we don't know about yet, so create it and mark it online
        val partition = Partition(new TopicIdPartition(topicId, tp), time, this)
        allPartitions.put(tp, HostedPartition.Online(partition))
        Some(partition, true)
    }
  }

  /**
   * Apply a KRaft topic change delta.
   *
   * @param delta           The delta to apply.
   * @param newImage        The new metadata image.
   */
  def applyDelta(delta: TopicsDelta, newImage: MetadataImage): Unit = {
    // Before taking the lock, compute the local changes
    val localChanges = delta.localChanges(config.nodeId)
    val metadataVersion = newImage.features().metadataVersionOrThrow()

    replicaStateChangeLock.synchronized {
      // Handle deleted partitions. We need to do this first because we might subsequently
      // create new partitions with the same names as the ones we are deleting here.
      if (!localChanges.deletes.isEmpty) {
        val deletes = localChanges.deletes.asScala
          .map { tp =>
            val isCurrentLeader = Option(delta.image().getTopic(tp.topic()))
              .map(image => image.partitions().get(tp.partition()))
              .exists(partition => partition.leader == config.nodeId)
            val deleteRemoteLog = delta.topicWasDeleted(tp.topic()) && isCurrentLeader
            new StopPartition(tp, true, deleteRemoteLog, false)
          }
          .toSet
        stateChangeLogger.info(s"Deleting ${deletes.size} partition(s).")
        stopPartitions(deletes).foreachEntry { (topicPartition, e) =>
          if (e.isInstanceOf[KafkaStorageException]) {
            stateChangeLogger.error(s"Unable to delete replica $topicPartition because " +
              "the local replica for the partition is in an offline log directory")
          } else {
            stateChangeLogger.error(s"Unable to delete replica $topicPartition because " +
              s"we got an unexpected ${e.getClass.getName} exception: ${e.getMessage}")
          }
        }
      }

      // Handle partitions which we are now the leader or follower for.
      if (!localChanges.leaders.isEmpty || !localChanges.followers.isEmpty) {
        val lazyOffsetCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints.asJava)
        val leaderChangedPartitions = new mutable.HashSet[Partition]
        val followerChangedPartitions = new mutable.HashSet[Partition]
        if (!localChanges.leaders.isEmpty) {
          applyLocalLeadersDelta(leaderChangedPartitions, delta, lazyOffsetCheckpoints, localChanges.leaders.asScala, localChanges.directoryIds.asScala)
        }
        if (!localChanges.followers.isEmpty) {
          applyLocalFollowersDelta(followerChangedPartitions, newImage, delta, lazyOffsetCheckpoints, localChanges.followers.asScala, localChanges.directoryIds.asScala)
        }

        maybeAddLogDirFetchers(leaderChangedPartitions ++ followerChangedPartitions, lazyOffsetCheckpoints,
          name => Option(newImage.topics().getTopic(name)).map(_.id()))

        replicaFetcherManager.shutdownIdleFetcherThreads()
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()

        remoteLogManager.foreach(rlm => rlm.onLeadershipChange((leaderChangedPartitions.toSet: Set[TopicPartitionLog]).asJava, (followerChangedPartitions.toSet: Set[TopicPartitionLog]).asJava, localChanges.topicIds()))
      }

      if (metadataVersion.isDirectoryAssignmentSupported) {
        // We only want to update the directoryIds if DirectoryAssignment is supported!
        localChanges.directoryIds.forEach(maybeUpdateTopicAssignment)
      }
    }
  }

  private def applyLocalLeadersDelta(
    changedPartitions: mutable.Set[Partition],
    delta: TopicsDelta,
    offsetCheckpoints: OffsetCheckpoints,
    localLeaders: mutable.Map[TopicPartition, LocalReplicaChanges.PartitionInfo],
    directoryIds: mutable.Map[TopicIdPartition, Uuid]
  ): Unit = {
    stateChangeLogger.info(s"Transitioning ${localLeaders.size} partition(s) to " +
      "local leaders.")
    replicaFetcherManager.removeFetcherForPartitions(localLeaders.keySet)
    localLeaders.foreachEntry { (tp, info) =>
      getOrCreatePartition(tp, delta, info.topicId).foreach { case (partition, isNew) =>
        try {
          val partitionAssignedDirectoryId = directoryIds.find(_._1.topicPartition() == tp).map(_._2)
          partition.makeLeader(info.partition, isNew, offsetCheckpoints, Some(info.topicId), partitionAssignedDirectoryId)

          changedPartitions.add(partition)
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.info(s"Skipped the become-leader state change for $tp " +
              s"with topic id ${info.topicId} due to a storage error ${e.getMessage}")
            // If there is an offline log directory, a Partition object may have been created by
            // `getOrCreatePartition()` before `createLogIfNotExists()` failed to create local replica due
            // to KafkaStorageException. In this case `ReplicaManager.allPartitions` will map this topic-partition
            // to an empty Partition object. We need to map this topic-partition to OfflinePartition instead.
            markPartitionOffline(tp)
        }
      }
    }
  }

  private def applyLocalFollowersDelta(
    changedPartitions: mutable.Set[Partition],
    newImage: MetadataImage,
    delta: TopicsDelta,
    offsetCheckpoints: OffsetCheckpoints,
    localFollowers: mutable.Map[TopicPartition, LocalReplicaChanges.PartitionInfo],
    directoryIds: mutable.Map[TopicIdPartition, Uuid]
  ): Unit = {
    stateChangeLogger.info(s"Transitioning ${localFollowers.size} partition(s) to " +
      "local followers.")
    val partitionsToStartFetching = new mutable.HashMap[TopicPartition, Partition]
    val partitionsToStopFetching = new mutable.HashMap[TopicPartition, Boolean]
    val followerTopicSet = new mutable.HashSet[String]
    localFollowers.foreachEntry { (tp, info) =>
      getOrCreatePartition(tp, delta, info.topicId).foreach { case (partition, isNew) =>
        try {
          followerTopicSet.add(tp.topic)

          // We always update the follower state.
          // - This ensure that a replica with no leader can step down;
          // - This also ensures that the local replica is created even if the leader
          //   is unavailable. This is required to ensure that we include the partition's
          //   high watermark in the checkpoint file (see KAFKA-1647).
          val partitionAssignedDirectoryId = directoryIds.find(_._1.topicPartition() == tp).map(_._2)
          val isNewLeaderEpoch = partition.makeFollower(info.partition, isNew, offsetCheckpoints, Some(info.topicId), partitionAssignedDirectoryId)

          if (isInControlledShutdown && (info.partition.leader == NO_LEADER ||
              !info.partition.isr.contains(config.brokerId))) {
            // During controlled shutdown, replica with no leaders and replica
            // where this broker is not in the ISR are stopped.
            partitionsToStopFetching.put(tp, false)
          } else if (isNewLeaderEpoch) {
            // Invoke the follower transition listeners for the partition.
            partition.invokeOnBecomingFollowerListeners()
            // Otherwise, fetcher is restarted if the leader epoch has changed.
            partitionsToStartFetching.put(tp, partition)
          }

          changedPartitions.add(partition)
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Unable to start fetching $tp " +
              s"with topic ID ${info.topicId} due to a storage error ${e.getMessage}", e)
            replicaFetcherManager.addFailedPartition(tp)
            // If there is an offline log directory, a Partition object may have been created by
            // `getOrCreatePartition()` before `createLogIfNotExists()` failed to create local replica due
            // to KafkaStorageException. In this case `ReplicaManager.allPartitions` will map this topic-partition
            // to an empty Partition object. We need to map this topic-partition to OfflinePartition instead.
            markPartitionOffline(tp)

          case e: Throwable =>
            stateChangeLogger.error(s"Unable to start fetching $tp " +
              s"with topic ID ${info.topicId} due to ${e.getClass.getSimpleName}", e)
            replicaFetcherManager.addFailedPartition(tp)
        }
      }
    }

    if (partitionsToStartFetching.nonEmpty) {
      // Stopping the fetchers must be done first in order to initialize the fetch
      // position correctly.
      replicaFetcherManager.removeFetcherForPartitions(partitionsToStartFetching.keySet)
      stateChangeLogger.info(s"Stopped fetchers as part of become-follower for ${partitionsToStartFetching.size} partitions")

      val listenerName = config.interBrokerListenerName.value
      val partitionAndOffsets = new mutable.HashMap[TopicPartition, InitialFetchState]

      partitionsToStartFetching.foreachEntry { (topicPartition, partition) =>
        val nodeOpt = partition.leaderReplicaIdOpt
          .flatMap(leaderId => Option(newImage.cluster.broker(leaderId)))
          .flatMap(_.node(listenerName).toScala)

        nodeOpt match {
          case Some(node) =>
            val log = partition.localLogOrException
            partitionAndOffsets.put(topicPartition, InitialFetchState(
              log.topicId.toScala,
              new BrokerEndPoint(node.id, node.host, node.port),
              partition.getLeaderEpoch,
              initialFetchOffset(log)
            ))
          case None =>
            stateChangeLogger.trace(s"Unable to start fetching $topicPartition with topic ID ${partition.topicId} " +
              s"from leader ${partition.leaderReplicaIdOpt} because it is not alive.")
        }
      }

      replicaFetcherManager.addFetcherForPartitions(partitionAndOffsets)
      stateChangeLogger.info(s"Started fetchers as part of become-follower for ${partitionsToStartFetching.size} partitions")

      partitionsToStartFetching.foreach{ case (topicPartition, partition) =>
        completeDelayedOperationsWhenNotPartitionLeader(topicPartition, partition.topicId)}

      updateLeaderAndFollowerMetrics(followerTopicSet)
    }

    if (partitionsToStopFetching.nonEmpty) {
      val partitionsToStop = partitionsToStopFetching.map { case (tp, deleteLocalLog) => new StopPartition(tp, deleteLocalLog, false, false) }.toSet
      stopPartitions(partitionsToStop)
      stateChangeLogger.info(s"Stopped fetchers as part of controlled shutdown for ${partitionsToStop.size} partitions")
    }
  }

  private def maybeUpdateTopicAssignment(partition: TopicIdPartition, partitionDirectoryId: Uuid): Unit = {
    for {
      topicPartitionActualLog <- logManager.getLog(partition.topicPartition())
      topicPartitionActualDirectoryId <- logManager.directoryId(topicPartitionActualLog.dir.getParent)
      if partitionDirectoryId != topicPartitionActualDirectoryId
    } directoryEventHandler.handleAssignment(
      new common.TopicIdPartition(partition.topicId, partition.partition()),
      topicPartitionActualDirectoryId,
      "Applying metadata delta",
      () => ()
    )
  }
}
