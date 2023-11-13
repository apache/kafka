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
import kafka.api._
import kafka.cluster.{BrokerEndPoint, Partition, PartitionListener}
import kafka.controller.{KafkaController, StateChangeLogger}
import kafka.log.remote.RemoteLogManager
import kafka.log.{LogManager, UnifiedLog}
import kafka.server.HostedPartition.Online
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.ReplicaManager.{AtMinIsrPartitionCountMetricName, FailedIsrUpdatesPerSecMetricName, IsrExpandsPerSecMetricName, IsrShrinksPerSecMetricName, LeaderCountMetricName, OfflineReplicaCountMetricName, PartitionCountMetricName, PartitionsWithLateTransactionsCountMetricName, ProducerIdCountMetricName, ReassigningPartitionsMetricName, UnderMinIsrPartitionCountMetricName, UnderReplicatedPartitionsMetricName}
import kafka.server.ReplicaManager.createLogReadResult
import kafka.server.checkpoints.{LazyOffsetCheckpoints, OffsetCheckpointFile, OffsetCheckpoints}
import kafka.server.metadata.ZkMetadataCache
import kafka.utils.Implicits._
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LeaderAndIsrResponseData.{LeaderAndIsrPartitionError, LeaderAndIsrTopicError}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult}
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.message.{DescribeLogDirsResponseData, DescribeProducersResponseData, FetchResponseData, LeaderAndIsrResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.PartitionView.DefaultPartitionView
import org.apache.kafka.common.replica.ReplicaView.DefaultReplicaView
import org.apache.kafka.common.replica._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{ElectionType, IsolationLevel, Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.image.{LocalReplicaChanges, MetadataImage, TopicsDelta}
import org.apache.kafka.metadata.LeaderConstants.NO_LEADER
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.{Scheduler, ShutdownableThread}
import org.apache.kafka.storage.internals.log.{AppendOrigin, FetchDataInfo, FetchParams, FetchPartitionData, LeaderHwChange, LogAppendInfo, LogConfig, LogDirFailureChannel, LogOffsetMetadata, LogReadInfo, RecordValidationException, RemoteLogReadResult, RemoteStorageFetchInfo, VerificationGuard}

import java.io.File
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.Lock
import java.util.concurrent.{CompletableFuture, Future, RejectedExecutionException, TimeUnit}
import java.util.{Optional, OptionalInt, OptionalLong}
import scala.collection.{Map, Seq, Set, mutable}
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

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

case class StopPartition(topicPartition: TopicPartition, deleteLocalLog: Boolean, deleteRemoteLog: Boolean = false)

/**
 * Result metadata of a log read operation on the log
 * @param info @FetchDataInfo returned by the @Log read
 * @param divergingEpoch Optional epoch and end offset which indicates the largest epoch such
 *                       that subsequent records are known to diverge on the follower/consumer
 * @param highWatermark high watermark of the local replica
 * @param leaderLogStartOffset The log start offset of the leader at the time of the read
 * @param leaderLogEndOffset The log end offset of the leader at the time of the read
 * @param followerLogStartOffset The log start offset of the follower taken from the Fetch request
 * @param fetchTimeMs The time the fetch was received
 * @param lastStableOffset Current LSO or None if the result has an exception
 * @param preferredReadReplica the preferred read replica to be used for future fetches
 * @param exception Exception if error encountered while reading from the log
 */
case class LogReadResult(info: FetchDataInfo,
                         divergingEpoch: Option[FetchResponseData.EpochEndOffset],
                         highWatermark: Long,
                         leaderLogStartOffset: Long,
                         leaderLogEndOffset: Long,
                         followerLogStartOffset: Long,
                         fetchTimeMs: Long,
                         lastStableOffset: Option[Long],
                         preferredReadReplica: Option[Int] = None,
                         exception: Option[Throwable] = None) {

  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }

  def toFetchPartitionData(isReassignmentFetch: Boolean): FetchPartitionData = new FetchPartitionData(
    this.error,
    this.highWatermark,
    this.leaderLogStartOffset,
    this.info.records,
    this.divergingEpoch.asJava,
    if (this.lastStableOffset.isDefined) OptionalLong.of(this.lastStableOffset.get) else OptionalLong.empty(),
    this.info.abortedTransactions,
    if (this.preferredReadReplica.isDefined) OptionalInt.of(this.preferredReadReplica.get) else OptionalInt.empty(),
    isReassignmentFetch)

  def withEmptyFetchInfo: LogReadResult =
    copy(info = new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY))

  override def toString = {
    "LogReadResult(" +
      s"info=$info, " +
      s"divergingEpoch=$divergingEpoch, " +
      s"highWatermark=$highWatermark, " +
      s"leaderLogStartOffset=$leaderLogStartOffset, " +
      s"leaderLogEndOffset=$leaderLogEndOffset, " +
      s"followerLogStartOffset=$followerLogStartOffset, " +
      s"fetchTimeMs=$fetchTimeMs, " +
      s"preferredReadReplica=$preferredReadReplica, " +
      s"lastStableOffset=$lastStableOffset, " +
      s"error=$error" +
      ")"
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
  final object Offline extends HostedPartition
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

  def createLogReadResult(highWatermark: Long,
                          leaderLogStartOffset: Long,
                          leaderLogEndOffset: Long,
                          e: Throwable) = {
    LogReadResult(info = new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
      divergingEpoch = None,
      highWatermark,
      leaderLogStartOffset,
      leaderLogEndOffset,
      followerLogStartOffset = -1L,
      fetchTimeMs = -1L,
      lastStableOffset = None,
      exception = Some(e))
  }

  def createLogReadResult(e: Throwable): LogReadResult = {
    LogReadResult(info = new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
      divergingEpoch = None,
      highWatermark = UnifiedLog.UnknownOffset,
      leaderLogStartOffset = UnifiedLog.UnknownOffset,
      leaderLogEndOffset = UnifiedLog.UnknownOffset,
      followerLogStartOffset = UnifiedLog.UnknownOffset,
      fetchTimeMs = -1L,
      lastStableOffset = None,
      exception = Some(e))
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
                     val isShuttingDown: AtomicBoolean = new AtomicBoolean(false),
                     val zkClient: Option[KafkaZkClient] = None,
                     delayedProducePurgatoryParam: Option[DelayedOperationPurgatory[DelayedProduce]] = None,
                     delayedFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedFetch]] = None,
                     delayedDeleteRecordsPurgatoryParam: Option[DelayedOperationPurgatory[DelayedDeleteRecords]] = None,
                     delayedElectLeaderPurgatoryParam: Option[DelayedOperationPurgatory[DelayedElectLeader]] = None,
                     delayedRemoteFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedRemoteFetch]] = None,
                     threadNamePrefix: Option[String] = None,
                     val brokerEpochSupplier: () => Long = () => -1,
                     addPartitionsToTxnManager: Option[AddPartitionsToTxnManager] = None
                     ) extends Logging {
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val delayedProducePurgatory = delayedProducePurgatoryParam.getOrElse(
    DelayedOperationPurgatory[DelayedProduce](
      purgatoryName = "Produce", brokerId = config.brokerId,
      purgeInterval = config.producerPurgatoryPurgeIntervalRequests))
  val delayedFetchPurgatory = delayedFetchPurgatoryParam.getOrElse(
    DelayedOperationPurgatory[DelayedFetch](
      purgatoryName = "Fetch", brokerId = config.brokerId,
      purgeInterval = config.fetchPurgatoryPurgeIntervalRequests))
  val delayedDeleteRecordsPurgatory = delayedDeleteRecordsPurgatoryParam.getOrElse(
    DelayedOperationPurgatory[DelayedDeleteRecords](
      purgatoryName = "DeleteRecords", brokerId = config.brokerId,
      purgeInterval = config.deleteRecordsPurgatoryPurgeIntervalRequests))
  val delayedElectLeaderPurgatory = delayedElectLeaderPurgatoryParam.getOrElse(
    DelayedOperationPurgatory[DelayedElectLeader](
      purgatoryName = "ElectLeader", brokerId = config.brokerId))
  val delayedRemoteFetchPurgatory = delayedRemoteFetchPurgatoryParam.getOrElse(
    DelayedOperationPurgatory[DelayedRemoteFetch](
      purgatoryName = "RemoteFetch", brokerId = config.brokerId))

  /* epoch of the controller that last changed the leader */
  @volatile private[server] var controllerEpoch: Int = KafkaController.InitialControllerEpoch
  protected val localBrokerId = config.brokerId
  protected val allPartitions = new Pool[TopicPartition, HostedPartition](
    valueFactory = Some(tp => HostedPartition.Online(Partition(tp, time, this)))
  )
  protected val replicaStateChangeLock = new Object
  val replicaFetcherManager = createReplicaFetcherManager(metrics, time, threadNamePrefix, quotaManagers.follower)
  private[server] val replicaAlterLogDirsManager = createReplicaAlterLogDirsManager(quotaManagers.alterLogDirs, brokerTopicStats)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  @volatile private[server] var highWatermarkCheckpoints: Map[String, OffsetCheckpointFile] = logManager.liveLogDirs.map(dir =>
    (dir.getAbsolutePath, new OffsetCheckpointFile(new File(dir, ReplicaManager.HighWatermarkFilename), logDirFailureChannel))).toMap

  @volatile private var isInControlledShutdown = false

  this.logIdent = s"[ReplicaManager broker=$localBrokerId] "
  protected val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)

  private var logDirFailureHandler: LogDirFailureHandler = _

  private class LogDirFailureHandler(name: String, haltBrokerOnDirFailure: Boolean) extends ShutdownableThread(name) {
    override def doWork(): Unit = {
      val newOfflineLogDir = logDirFailureChannel.takeNextOfflineLogDir()
      if (haltBrokerOnDirFailure) {
        fatal(s"Halting broker because dir $newOfflineLogDir is offline")
        Exit.halt(1)
      }
      handleLogDirFailure(newOfflineLogDir)
    }
  }

  // Visible for testing
  private[server] val replicaSelectorOpt: Option[ReplicaSelector] = createReplicaSelector()

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

  def reassigningPartitionsCount: Int = leaderPartitionsIterator.count(_.isReassigning)

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
  def shutdownIdleReplicaAlterLogDirsThread(): Unit = {
    replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
  }

  def resizeFetcherThreadPool(newSize: Int): Unit = {
    replicaFetcherManager.resizeThreadPool(newSize)
  }

  def getLog(topicPartition: TopicPartition): Option[UnifiedLog] = logManager.getLog(topicPartition)

  def hasDelayedElectionOperations: Boolean = delayedElectLeaderPurgatory.numDelayed != 0

  def tryCompleteElection(key: DelayedOperationKey): Unit = {
    val completed = delayedElectLeaderPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d ElectLeader.".format(key.keyLabel, completed))
  }

  def startup(): Unit = {
    // start ISR expiration thread
    // A follower can lag behind leader for up to config.replicaLagTimeMaxMs x 1.5 before it is removed from ISR
    scheduler.schedule("isr-expiration", () => maybeShrinkIsr(), 0L, config.replicaLagTimeMaxMs / 2)
    scheduler.schedule("shutdown-idle-replica-alter-log-dirs-thread", () => shutdownIdleReplicaAlterLogDirsThread(), 0L, 10000L)

    // If inter-broker protocol (IBP) < 1.0, the controller will send LeaderAndIsrRequest V0 which does not include isNew field.
    // In this case, the broker receiving the request cannot determine whether it is safe to create a partition if a log directory has failed.
    // Thus, we choose to halt the broker on any log directory failure if IBP < 1.0
    val haltBrokerOnFailure = metadataCache.metadataVersion().isLessThan(IBP_1_0_IV0)
    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
    addPartitionsToTxnManager.foreach(_.start())
  }

  private def maybeRemoveTopicMetrics(topic: String): Unit = {
    val topicHasNonOfflinePartition = allPartitions.values.exists {
      case online: HostedPartition.Online => topic == online.partition.topic
      case HostedPartition.None | HostedPartition.Offline => false
    }
    if (!topicHasNonOfflinePartition) // nothing online or deferred
      brokerTopicStats.removeMetrics(topic)
  }

  private[server] def updateStrayLogs(strayPartitions: Set[TopicPartition]): Unit = {
    if (strayPartitions.isEmpty) {
      return
    }
    warn(s"Found stray partitions ${strayPartitions.mkString(",")}")

    // First, stop the partitions. This will shutdown the fetchers and other managers
    val partitionsToStop = strayPartitions.map { tp => tp -> false }.toMap
    stopPartitions(partitionsToStop).forKeyValue { (topicPartition, exception) =>
      error(s"Unable to stop stray partition $topicPartition", exception)
    }

    // Next, delete the in-memory partition state. Normally, stopPartitions would do this, but since we're not
    // actually deleting the log, so we can't rely on the "deleteLocalLog" behavior in stopPartitions.
    strayPartitions.foreach { topicPartition =>
      getPartition(topicPartition) match {
        case hostedPartition: HostedPartition.Online =>
          if (allPartitions.remove(topicPartition, hostedPartition)) {
            maybeRemoveTopicMetrics(topicPartition.topic)
            hostedPartition.partition.delete()
          }
        case _ =>
      }
    }

    // Mark the log as stray in-memory and rename the directory
    strayPartitions.foreach { tp =>
      logManager.getLog(tp).foreach(logManager.addStrayLog(tp, _))
      logManager.getLog(tp, isFuture = true).foreach(logManager.addStrayLog(tp, _))
    }
    logManager.asyncDelete(strayPartitions, isStray = true, (topicPartition, e) => {
      error(s"Failed to delete stray partition $topicPartition due to " +
        s"${e.getClass.getName} exception: ${e.getMessage}")
    })
  }

  // Find logs which exist on the broker, but aren't present in the full LISR
  private[server] def findStrayPartitionsFromLeaderAndIsr(partitionsFromRequest: Set[TopicPartition]): Set[TopicPartition] = {
    logManager.allLogs.map(_.topicPartition).filterNot(partitionsFromRequest.contains).toSet
  }

  protected def completeDelayedFetchOrProduceRequests(topicPartition: TopicPartition): Unit = {
    val topicPartitionOperationKey = TopicPartitionOperationKey(topicPartition)
    delayedProducePurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedFetchPurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedRemoteFetchPurgatory.checkAndComplete(topicPartitionOperationKey)
  }

  /**
   * Complete any local follower fetches that have been unblocked since new data is available
   * from the leader for one or more partitions. Should only be called by ReplicaFetcherThread
   * after successfully replicating from the leader.
   */
  private[server] def completeDelayedFetchRequests(topicPartitions: Seq[TopicPartition]): Unit = {
    topicPartitions.foreach(tp => delayedFetchPurgatory.checkAndComplete(TopicPartitionOperationKey(tp)))
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

  def stopReplicas(correlationId: Int,
                   controllerId: Int,
                   controllerEpoch: Int,
                   partitionStates: Map[TopicPartition, StopReplicaPartitionState]
                  ): (mutable.Map[TopicPartition, Errors], Errors) = {
    replicaStateChangeLock synchronized {
      stateChangeLogger.info(s"Handling StopReplica request correlationId $correlationId from controller " +
        s"$controllerId for ${partitionStates.size} partitions")
      if (stateChangeLogger.isTraceEnabled)
        partitionStates.forKeyValue { (topicPartition, partitionState) =>
          stateChangeLogger.trace(s"Received StopReplica request $partitionState " +
            s"correlation id $correlationId from controller $controllerId " +
            s"epoch $controllerEpoch for partition $topicPartition")
        }

      val responseMap = new collection.mutable.HashMap[TopicPartition, Errors]
      if (controllerEpoch < this.controllerEpoch) {
        stateChangeLogger.warn(s"Ignoring StopReplica request from " +
          s"controller $controllerId with correlation id $correlationId " +
          s"since its controller epoch $controllerEpoch is old. " +
          s"Latest known controller epoch is ${this.controllerEpoch}")
        (responseMap, Errors.STALE_CONTROLLER_EPOCH)
      } else {
        this.controllerEpoch = controllerEpoch

        val stoppedPartitions = mutable.Buffer.empty[StopPartition]
        partitionStates.forKeyValue { (topicPartition, partitionState) =>
          val deletePartition = partitionState.deletePartition()

          getPartition(topicPartition) match {
            case HostedPartition.Offline =>
              stateChangeLogger.warn(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                "partition is in an offline log directory")
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)

            case HostedPartition.Online(partition) =>
              val currentLeaderEpoch = partition.getLeaderEpoch
              val requestLeaderEpoch = partitionState.leaderEpoch
              // When a topic is deleted, the leader epoch is not incremented. To circumvent this,
              // a sentinel value (EpochDuringDelete) overwriting any previous epoch is used.
              // When an older version of the StopReplica request which does not contain the leader
              // epoch, a sentinel value (NoEpoch) is used and bypass the epoch validation.
              if (requestLeaderEpoch == LeaderAndIsr.EpochDuringDelete ||
                  requestLeaderEpoch == LeaderAndIsr.NoEpoch ||
                  requestLeaderEpoch >= currentLeaderEpoch) {
                stoppedPartitions += StopPartition(topicPartition, deletePartition,
                  deletePartition && partition.isLeader && requestLeaderEpoch == LeaderAndIsr.EpochDuringDelete)
                // Assume that everything will go right. It is overwritten in case of an error.
                responseMap.put(topicPartition, Errors.NONE)
              } else if (requestLeaderEpoch < currentLeaderEpoch) {
                stateChangeLogger.warn(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch is smaller than the current " +
                  s"leader epoch $currentLeaderEpoch")
                responseMap.put(topicPartition, Errors.FENCED_LEADER_EPOCH)
              } else {
                stateChangeLogger.info(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch matches the current leader epoch")
                responseMap.put(topicPartition, Errors.FENCED_LEADER_EPOCH)
              }

            case HostedPartition.None =>
              // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
              // This could happen when topic is being deleted while broker is down and recovers.
              stoppedPartitions += StopPartition(topicPartition, deletePartition)
              responseMap.put(topicPartition, Errors.NONE)
          }
        }

        stopPartitions(stoppedPartitions.toSet).foreach { case (topicPartition, e) =>
          if (e.isInstanceOf[KafkaStorageException]) {
              stateChangeLogger.error(s"Ignoring StopReplica request (delete=true) from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                "partition is in an offline log directory")
          } else {
            stateChangeLogger.error(s"Ignoring StopReplica request (delete=true) from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition due to an unexpected " +
                s"${e.getClass.getName} exception: ${e.getMessage}")
          }
          responseMap.put(topicPartition, Errors.forException(e))
        }
        (responseMap, Errors.NONE)
      }
    }
  }

  /**
   * Stop the given partitions.
   *
   * @param partitionsToStop A map from a topic partition to a boolean indicating
   *                         whether the partition should be deleted.
   * @return A map from partitions to exceptions which occurred.
   *         If no errors occurred, the map will be empty.
   */
  private def stopPartitions(partitionsToStop: Map[TopicPartition, Boolean]): Map[TopicPartition, Throwable] = {
    stopPartitions(partitionsToStop.map {
      case (topicPartition, deleteLocalLog) => StopPartition(topicPartition, deleteLocalLog)
    }.toSet)
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
      if (stopPartition.deleteLocalLog) {
        getPartition(topicPartition) match {
          case hostedPartition: HostedPartition.Online =>
            if (allPartitions.remove(topicPartition, hostedPartition)) {
              maybeRemoveTopicMetrics(topicPartition.topic)
              // Logs are not deleted here. They are deleted in a single batch later on.
              // This is done to avoid having to checkpoint for every deletions.
              hostedPartition.partition.delete()
            }

          case _ =>
        }
        partitionsToDelete += topicPartition
      }
      // If we were the leader, we may have some operations still waiting for completion.
      // We force completion to prevent them from timing out.
      completeDelayedFetchOrProduceRequests(topicPartition)
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
    allPartitions.put(topicPartition, HostedPartition.Online(partition))
    partition
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
    allPartitions.values.iterator.flatMap {
      case HostedPartition.Online(partition) => Some(partition)
      case _ => None
    }
  }

  private def offlinePartitionCount: Int = {
    allPartitions.values.iterator.count(_ == HostedPartition.Offline)
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

  def getPartitionOrError(topicPartition: TopicPartition): Either[Errors, Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) =>
        Right(partition)

      case HostedPartition.Offline =>
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

  def localLog(topicPartition: TopicPartition): Option[UnifiedLog] = {
    onlinePartition(topicPartition).flatMap(_.log)
  }

  def getLogDir(topicPartition: TopicPartition): Option[String] = {
    localLog(topicPartition).map(_.parentDir)
  }

  /**
   * TODO: move this action queue to handle thread so we can simplify concurrency handling
   */
  private val actionQueue = new DelayedActionQueue

  def tryCompleteActions(): Unit = actionQueue.tryCompleteActions()

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
   * @param entriesPerPartition           the records per partition to be appended
   * @param responseCallback              callback for sending the response
   * @param delayedProduceLock            lock for the delayed actions
   * @param recordConversionStatsCallback callback for updating stats on record conversions
   * @param requestLocal                  container for the stateful instances scoped to this request
   * @param transactionalId               transactional ID if the request is from a producer and the producer is transactional
   * @param actionQueue                   the action queue to use. ReplicaManager#actionQueue is used by default.
   */
  def appendRecords(timeout: Long,
                    requiredAcks: Short,
                    internalTopicsAllowed: Boolean,
                    origin: AppendOrigin,
                    entriesPerPartition: Map[TopicPartition, MemoryRecords],
                    responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                    delayedProduceLock: Option[Lock] = None,
                    recordConversionStatsCallback: Map[TopicPartition, RecordConversionStats] => Unit = _ => (),
                    requestLocal: RequestLocal = RequestLocal.NoCaching,
                    transactionalId: String = null,
                    actionQueue: ActionQueue = this.actionQueue): Unit = {
    if (isValidRequiredAcks(requiredAcks)) {

      val verificationGuards: mutable.Map[TopicPartition, VerificationGuard] = mutable.Map[TopicPartition, VerificationGuard]()
      val (verifiedEntriesPerPartition, notYetVerifiedEntriesPerPartition, errorsPerPartition) =
        if (transactionalId == null || !config.transactionPartitionVerificationEnable)
          (entriesPerPartition, Map.empty[TopicPartition, MemoryRecords], Map.empty[TopicPartition, Errors])
        else {
          val verifiedEntries = mutable.Map[TopicPartition, MemoryRecords]()
          val unverifiedEntries = mutable.Map[TopicPartition, MemoryRecords]()
          val errorEntries = mutable.Map[TopicPartition, Errors]()
          partitionEntriesForVerification(verificationGuards, entriesPerPartition, verifiedEntries, unverifiedEntries, errorEntries)
          (verifiedEntries.toMap, unverifiedEntries.toMap, errorEntries.toMap)
        }

      if (notYetVerifiedEntriesPerPartition.isEmpty || addPartitionsToTxnManager.isEmpty) {
        appendEntries(verifiedEntriesPerPartition, internalTopicsAllowed, origin, requiredAcks, verificationGuards.toMap,
          errorsPerPartition, recordConversionStatsCallback, timeout, responseCallback, delayedProduceLock)(requestLocal, Map.empty)
      } else {
        // For unverified entries, send a request to verify. When verified, the append process will proceed via the callback.
        // We verify above that all partitions use the same producer ID.
        val batchInfo = notYetVerifiedEntriesPerPartition.head._2.firstBatch()
        addPartitionsToTxnManager.foreach(_.verifyTransaction(
          transactionalId = transactionalId,
          producerId = batchInfo.producerId,
          producerEpoch = batchInfo.producerEpoch,
          topicPartitions = notYetVerifiedEntriesPerPartition.keySet.toSeq,
          callback = KafkaRequestHandler.wrapAsyncCallback(
            appendEntries(
              entriesPerPartition,
              internalTopicsAllowed,
              origin,
              requiredAcks,
              verificationGuards.toMap,
              errorsPerPartition,
              recordConversionStatsCallback,
              timeout,
              responseCallback,
              delayedProduceLock
            ),
            requestLocal)
        ))
      }
    } else {
      // If required.acks is outside accepted range, something is wrong with the client
      // Just return an error and don't handle the request at all
      val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
        topicPartition -> new PartitionResponse(
          Errors.INVALID_REQUIRED_ACKS,
          LogAppendInfo.UNKNOWN_LOG_APPEND_INFO.firstOffset,
          RecordBatch.NO_TIMESTAMP,
          LogAppendInfo.UNKNOWN_LOG_APPEND_INFO.logStartOffset
        )
      }
      responseCallback(responseStatus)
    }
  }

  /*
   * Note: This method can be used as a callback in a different request thread. Ensure that correct RequestLocal
   * is passed when executing this method. Accessing non-thread-safe data structures should be avoided if possible.
   */
  private def appendEntries(allEntries: Map[TopicPartition, MemoryRecords],
                            internalTopicsAllowed: Boolean,
                            origin: AppendOrigin,
                            requiredAcks: Short,
                            verificationGuards: Map[TopicPartition, VerificationGuard],
                            errorsPerPartition: Map[TopicPartition, Errors],
                            recordConversionStatsCallback: Map[TopicPartition, RecordConversionStats] => Unit,
                            timeout: Long,
                            responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                            delayedProduceLock: Option[Lock])
                           (requestLocal: RequestLocal, unverifiedEntries: Map[TopicPartition, Errors]): Unit = {
    val sTime = time.milliseconds
    val verifiedEntries =
      if (unverifiedEntries.isEmpty)
        allEntries
      else
        allEntries.filter { case (tp, _) =>
          !unverifiedEntries.contains(tp)
        }

    val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed,
      origin, verifiedEntries, requiredAcks, requestLocal, verificationGuards.toMap)
    debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

    val errorResults = (unverifiedEntries ++ errorsPerPartition).map {
      case (topicPartition, error) =>
        // translate transaction coordinator errors to known producer response errors
        val customException =
          error match {
            case Errors.INVALID_TXN_STATE => Some(error.exception("Partition was not added to the transaction"))
            case Errors.CONCURRENT_TRANSACTIONS |
                 Errors.COORDINATOR_LOAD_IN_PROGRESS |
                 Errors.COORDINATOR_NOT_AVAILABLE |
                 Errors.NOT_COORDINATOR => Some(new NotEnoughReplicasException(
              s"Unable to verify the partition has been added to the transaction. Underlying error: ${error.toString}"))
            case _ => None
          }
        topicPartition -> LogAppendResult(
          LogAppendInfo.UNKNOWN_LOG_APPEND_INFO,
          Some(customException.getOrElse(error.exception)),
          hasCustomErrorMessage = customException.isDefined
        )
    }

    val allResults = localProduceResults ++ errorResults
    val produceStatus = allResults.map { case (topicPartition, result) =>
      topicPartition -> ProducePartitionStatus(
        result.info.lastOffset + 1, // required offset
        new PartitionResponse(
          result.error,
          result.info.firstOffset,
          result.info.lastOffset,
          result.info.logAppendTime,
          result.info.logStartOffset,
          result.info.recordErrors,
          result.errorMessage
        )
      ) // response status
    }

    actionQueue.add {
      () =>
        allResults.foreach { case (topicPartition, result) =>
          val requestKey = TopicPartitionOperationKey(topicPartition)
          result.info.leaderHwChange match {
            case LeaderHwChange.INCREASED =>
              // some delayed operations may be unblocked after HW changed
              delayedProducePurgatory.checkAndComplete(requestKey)
              delayedFetchPurgatory.checkAndComplete(requestKey)
              delayedDeleteRecordsPurgatory.checkAndComplete(requestKey)
            case LeaderHwChange.SAME =>
              // probably unblock some follower fetch requests since log end offset has been updated
              delayedFetchPurgatory.checkAndComplete(requestKey)
            case LeaderHwChange.NONE =>
            // nothing
          }
        }
    }

    recordConversionStatsCallback(localProduceResults.map { case (k, v) => k -> v.info.recordConversionStats })

    if (delayedProduceRequestRequired(requiredAcks, allEntries, allResults)) {
      // create delayed produce operation
      val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
      val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)

      // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
      val producerRequestKeys = allEntries.keys.map(TopicPartitionOperationKey(_)).toSeq

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed produce operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)
    } else {
      // we can respond immediately
      val produceResponseStatus = produceStatus.map { case (k, status) => k -> status.responseStatus }
      responseCallback(produceResponseStatus)
    }
  }

  private def partitionEntriesForVerification(verificationGuards: mutable.Map[TopicPartition, VerificationGuard],
                                              entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                              verifiedEntries: mutable.Map[TopicPartition, MemoryRecords],
                                              unverifiedEntries: mutable.Map[TopicPartition, MemoryRecords],
                                              errorEntries: mutable.Map[TopicPartition, Errors]): Unit= {
    val transactionalProducerIds = mutable.HashSet[Long]()
    entriesPerPartition.foreach { case (topicPartition, records) =>
      try {
        // Produce requests (only requests that require verification) should only have one batch per partition in "batches" but check all just to be safe.
        val transactionalBatches = records.batches.asScala.filter(batch => batch.hasProducerId && batch.isTransactional)
        transactionalBatches.foreach(batch => transactionalProducerIds.add(batch.producerId))

        if (transactionalBatches.nonEmpty) {
          // We return VerificationGuard if the partition needs to be verified. If no state is present, no need to verify.
          val firstBatch = records.firstBatch
          val verificationGuard = getPartitionOrException(topicPartition).maybeStartTransactionVerification(firstBatch.producerId, firstBatch.baseSequence, firstBatch.producerEpoch)
          if (verificationGuard != VerificationGuard.SENTINEL) {
            verificationGuards.put(topicPartition, verificationGuard)
            unverifiedEntries.put(topicPartition, records)
          } else
            verifiedEntries.put(topicPartition, records)
        } else {
          // If there is no producer ID or transactional records in the batches, no need to verify.
          verifiedEntries.put(topicPartition, records)
        }
      } catch {
        case e: Exception => errorEntries.put(topicPartition, Errors.forException(e))
      }
    }
    // We should have exactly one producer ID for transactional records
    if (transactionalProducerIds.size > 1) {
      throw new InvalidPidMappingException("Transactional records contained more than one producer ID")
    }
  }

  /**
   * Delete records on leader replicas of the partition, and wait for delete records operation be propagated to other replicas;
   * the callback function will be triggered either when timeout or logStartOffset of all live replicas have reached the specified offset
   */
  private def deleteRecordsOnLocalLog(offsetPerPartition: Map[TopicPartition, Long]): Map[TopicPartition, LogDeleteRecordsResult] = {
    trace("Delete records on local logs to offsets [%s]".format(offsetPerPartition))
    offsetPerPartition.map { case (topicPartition, requestedOffset) =>
      // reject delete records operation on internal topics
      if (Topic.isInternal(topicPartition.topic)) {
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
                partition.removeFutureLocalReplica()
              }
            case HostedPartition.Offline =>
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
          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
          if (partition.maybeCreateFutureReplica(destinationDir, highWatermarkCheckpoints)) {
            val futureLog = futureLocalLogOrException(topicPartition)
            logManager.abortAndPauseCleaning(topicPartition)

            val initialFetchState = InitialFetchState(topicId, BrokerEndPoint(config.brokerId, "localhost", -1),
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
  def describeLogDirs(partitions: Set[TopicPartition]): List[DescribeLogDirsResponseData.DescribeLogDirsResult] = {
    val logsByDir = logManager.allLogs.groupBy(log => log.parentDir)

    config.logDirs.toSet.map { logDir: String =>
      val file = Paths.get(logDir)
      val absolutePath = file.toAbsolutePath.toString
      try {
        if (!logManager.isLogDirOnline(absolutePath))
          throw new KafkaStorageException(s"Log directory $absolutePath is offline")

        val fileStore = Files.getFileStore(file)
        val totalBytes = adjustForLargeFileSystems(fileStore.getTotalSpace)
        val usableBytes = adjustForLargeFileSystems(fileStore.getUsableSpace)
        logsByDir.get(absolutePath) match {
          case Some(logs) =>
            val topicInfos = logs.groupBy(_.topicPartition.topic).map{case (topic, logs) =>
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
            }.toList.asJava

            new DescribeLogDirsResponseData.DescribeLogDirsResult().setLogDir(absolutePath)
              .setErrorCode(Errors.NONE.code).setTopics(topicInfos)
              .setTotalBytes(totalBytes).setUsableBytes(usableBytes)
          case None =>
            new DescribeLogDirsResponseData.DescribeLogDirsResult().setLogDir(absolutePath)
              .setErrorCode(Errors.NONE.code)
              .setTotalBytes(totalBytes).setUsableBytes(usableBytes)
        }

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
    }.toList
  }

  // See: https://bugs.openjdk.java.net/browse/JDK-8162520
  def adjustForLargeFileSystems(space: Long): Long = {
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
                    responseCallback: Map[TopicPartition, DeleteRecordsPartitionResult] => Unit): Unit = {
    val timeBeforeLocalDeleteRecords = time.milliseconds
    val localDeleteRecordsResults = deleteRecordsOnLocalLog(offsetPerPartition)
    debug("Delete records on local log in %d ms".format(time.milliseconds - timeBeforeLocalDeleteRecords))

    val deleteRecordsStatus = localDeleteRecordsResults.map { case (topicPartition, result) =>
      topicPartition ->
        DeleteRecordsPartitionStatus(
          result.requestedOffset, // requested offset
          new DeleteRecordsPartitionResult()
            .setLowWatermark(result.lowWatermark)
            .setErrorCode(result.error.code)
            .setPartitionIndex(topicPartition.partition)) // response status
    }

    if (delayedDeleteRecordsRequired(localDeleteRecordsResults)) {
      // create delayed delete records operation
      val delayedDeleteRecords = new DelayedDeleteRecords(timeout, deleteRecordsStatus, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed delete records operation
      val deleteRecordsRequestKeys = offsetPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed delete records operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedDeleteRecordsPurgatory.tryCompleteElseWatch(delayedDeleteRecords, deleteRecordsRequestKeys)
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
                                            entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                            localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
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
                               entriesPerPartition: Map[TopicPartition, MemoryRecords],
                               requiredAcks: Short,
                               requestLocal: RequestLocal,
                               verificationGuards: Map[TopicPartition, VerificationGuard]): Map[TopicPartition, LogAppendResult] = {
    val traceEnabled = isTraceEnabled
    def processFailedRecord(topicPartition: TopicPartition, t: Throwable) = {
      val logStartOffset = onlinePartition(topicPartition).map(_.logStartOffset).getOrElse(-1L)
      brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
      error(s"Error processing append operation on partition $topicPartition", t)

      logStartOffset
    }

    if (traceEnabled)
      trace(s"Append [$entriesPerPartition] to local log")

    entriesPerPartition.map { case (topicPartition, records) =>
      brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UNKNOWN_LOG_APPEND_INFO,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}")),
          hasCustomErrorMessage = false))
      } else {
        try {
          val partition = getPartitionOrException(topicPartition)
          val info = partition.appendRecordsToLeader(records, origin, requiredAcks, requestLocal,
            verificationGuards.getOrElse(topicPartition, VerificationGuard.SENTINEL))
          val numAppendedMessages = info.numMessages

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

          if (traceEnabled)
            trace(s"${records.sizeInBytes} written to log $topicPartition beginning at offset " +
              s"${info.firstOffset} and ending at offset ${info.lastOffset}")

          (topicPartition, LogAppendResult(info, exception = None, hasCustomErrorMessage = false))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderOrFollowerException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: KafkaStorageException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO, Some(e), hasCustomErrorMessage = false))
          case rve: RecordValidationException =>
            val logStartOffset = processFailedRecord(topicPartition, rve.invalidException)
            val recordErrors = rve.recordErrors
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithAdditionalInfo(logStartOffset, recordErrors),
              Some(rve.invalidException), hasCustomErrorMessage = true))
          case t: Throwable =>
            val logStartOffset = processFailedRecord(topicPartition, t)
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset),
              Some(t), hasCustomErrorMessage = false))
        }
      }
    }
  }

  def fetchOffsetForTimestamp(topicPartition: TopicPartition,
                              timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset] = {
    val partition = getPartitionOrException(topicPartition)
    partition.fetchOffsetForTimestamp(timestamp, isolationLevel, currentLeaderEpoch, fetchOnlyFromLeader, remoteLogManager)
  }

  def legacyFetchOffsetsForTimestamp(topicPartition: TopicPartition,
                                     timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long] = {
    val partition = getPartitionOrException(topicPartition)
    partition.legacyFetchOffsetsForTimestamp(timestamp, maxNumOffsets, isFromConsumer, fetchOnlyFromLeader)
  }

  /**
   * Returns [[LogReadResult]] with error if a task for RemoteStorageFetchInfo could not be scheduled successfully
   * else returns [[None]].
   */
  private def processRemoteFetch(remoteFetchInfo: RemoteStorageFetchInfo,
                                 params: FetchParams,
                                 responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit,
                                 logReadResults: Seq[(TopicIdPartition, LogReadResult)],
                                 fetchPartitionStatus: Seq[(TopicIdPartition, FetchPartitionStatus)]): Option[LogReadResult] = {
    val key = new TopicPartitionOperationKey(remoteFetchInfo.topicPartition.topic(), remoteFetchInfo.topicPartition.partition())
    val remoteFetchResult = new CompletableFuture[RemoteLogReadResult]
    var remoteFetchTask: Future[Void] = null
    try {
      remoteFetchTask = remoteLogManager.get.asyncRead(remoteFetchInfo, (result: RemoteLogReadResult) => {
        remoteFetchResult.complete(result)
        delayedRemoteFetchPurgatory.checkAndComplete(key)
      })
    } catch {
      case e: RejectedExecutionException =>
        // Return the error if any in scheduling the remote fetch task
        warn("Unable to fetch data from remote storage", e)
        return Some(createLogReadResult(e))
    }

    val remoteFetch = new DelayedRemoteFetch(remoteFetchTask, remoteFetchResult, remoteFetchInfo,
      fetchPartitionStatus, params, logReadResults, this, responseCallback)

    delayedRemoteFetchPurgatory.tryCompleteElseWatch(remoteFetch, Seq(key))
    None
  }

  private def buildPartitionToFetchPartitionData(logReadResults: Seq[(TopicIdPartition, LogReadResult)],
                                                 remoteFetchTopicPartition: TopicPartition,
                                                 error: LogReadResult): Seq[(TopicIdPartition, FetchPartitionData)] = {
    logReadResults.map { case (tp, result) =>
      val fetchPartitionData = {
        if (tp.topicPartition().equals(remoteFetchTopicPartition))
          error
        else
          result
      }.toFetchPartitionData(false)

      tp -> fetchPartitionData
    }
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

    // The 1st topic-partition that has to be read from remote storage
    var remoteFetchInfo: Optional[RemoteStorageFetchInfo] = Optional.empty()

    var hasDivergingEpoch = false
    var hasPreferredReadReplica = false
    val logReadResultMap = new mutable.HashMap[TopicIdPartition, LogReadResult]

    logReadResults.foreach { case (topicIdPartition, logReadResult) =>
      brokerTopicStats.topicStats(topicIdPartition.topicPartition.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()
      if (logReadResult.error != Errors.NONE)
        errorReadingData = true
      if (!remoteFetchInfo.isPresent && logReadResult.info.delayedRemoteStorageFetch.isPresent) {
        remoteFetchInfo = logReadResult.info.delayedRemoteStorageFetch
      }
      if (logReadResult.divergingEpoch.nonEmpty)
        hasDivergingEpoch = true
      if (logReadResult.preferredReadReplica.nonEmpty)
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
    if (!remoteFetchInfo.isPresent && (params.maxWaitMs <= 0 || fetchInfos.isEmpty || bytesReadable >= params.minBytes || errorReadingData ||
      hasDivergingEpoch || hasPreferredReadReplica)) {
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        val isReassignmentFetch = params.isFromFollower && isAddingReplica(tp.topicPartition, params.replicaId)
        tp -> result.toFetchPartitionData(isReassignmentFetch)
      }
      responseCallback(fetchPartitionData)
    } else {
      // construct the fetch results from the read results
      val fetchPartitionStatus = new mutable.ArrayBuffer[(TopicIdPartition, FetchPartitionStatus)]
      fetchInfos.foreach { case (topicIdPartition, partitionData) =>
        logReadResultMap.get(topicIdPartition).foreach(logReadResult => {
          val logOffsetMetadata = logReadResult.info.fetchOffsetMetadata
          fetchPartitionStatus += (topicIdPartition -> FetchPartitionStatus(logOffsetMetadata, partitionData))
        })
      }

      if (remoteFetchInfo.isPresent) {
        val maybeLogReadResultWithError = processRemoteFetch(remoteFetchInfo.get(), params, responseCallback, logReadResults, fetchPartitionStatus)
        if (maybeLogReadResultWithError.isDefined) {
          // If there is an error in scheduling the remote fetch task, return what we currently have
          // (the data read from local log segment for the other topic-partitions) and an error for the topic-partition
          // that we couldn't read from remote storage
          val partitionToFetchPartitionData = buildPartitionToFetchPartitionData(logReadResults, remoteFetchInfo.get().topicPartition, maybeLogReadResultWithError.get)
          responseCallback(partitionToFetchPartitionData)
        }
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
        val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => TopicPartitionOperationKey(tp) }

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
      } else if (!params.hardMaxBytesLimit && givenFetchedDataInfo.firstEntryIncomplete) {
        // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
        // progress in such cases and don't need to report a `RecordTooLargeException`
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
        val preferredReadReplica = params.clientMetadata.asScala.flatMap(
          metadata => findPreferredReadReplica(partition, metadata, params.replicaId, fetchInfo.fetchOffset, fetchTimeMs))

        if (preferredReadReplica.isDefined) {
          replicaSelectorOpt.foreach { selector =>
            debug(s"Replica selector ${selector.getClass.getSimpleName} returned preferred replica " +
              s"${preferredReadReplica.get} for ${params.clientMetadata}")
          }
          // If a preferred read-replica is set, skip the read
          val offsetSnapshot = partition.fetchOffsetSnapshot(fetchInfo.currentLeaderEpoch, fetchOnlyFromLeader = false)
          LogReadResult(info = new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = offsetSnapshot.highWatermark.messageOffset,
            leaderLogStartOffset = offsetSnapshot.logStartOffset,
            leaderLogEndOffset = offsetSnapshot.logEndOffset.messageOffset,
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = -1L,
            lastStableOffset = Some(offsetSnapshot.lastStableOffset.messageOffset),
            preferredReadReplica = preferredReadReplica,
            exception = None)
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

          LogReadResult(info = fetchDataInfo,
            divergingEpoch = readInfo.divergingEpoch.asScala,
            highWatermark = readInfo.highWatermark,
            leaderLogStartOffset = readInfo.logStartOffset,
            leaderLogEndOffset = readInfo.logEndOffset,
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = fetchTimeMs,
            lastStableOffset = Some(readInfo.lastStableOffset),
            preferredReadReplica = preferredReadReplica,
            exception = None
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
          createLogReadResult(e)
        case e: OffsetOutOfRangeException =>
          handleOffsetOutOfRangeError(tp, params, fetchInfo, adjustedMaxBytes, minOneMessage, log, fetchTimeMs, e)
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()

          val fetchSource = FetchRequest.describeReplicaId(params.replicaId)
          error(s"Error processing fetch with max size $adjustedMaxBytes from $fetchSource " +
            s"on partition $tp: $fetchInfo", e)

          LogReadResult(info = new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = UnifiedLog.UnknownOffset,
            leaderLogStartOffset = UnifiedLog.UnknownOffset,
            leaderLogEndOffset = UnifiedLog.UnknownOffset,
            followerLogStartOffset = UnifiedLog.UnknownOffset,
            fetchTimeMs = -1L,
            lastStableOffset = None,
            exception = Some(e)
          )
      }
    }

    var limitBytes = params.maxBytes
    val result = new mutable.ArrayBuffer[(TopicIdPartition, LogReadResult)]
    var minOneMessage = !params.hardMaxBytesLimit
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      val recordBatchSize = readResult.info.records.sizeInBytes
      // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
      if (recordBatchSize > 0)
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - recordBatchSize)
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
    //   2) `log` instance should not be null here as that would have been caught earlier with NotLeaderForPartitionException or ReplicaNotAvailableException.
    //   3) fetch offset is within the offset range of the remote storage layer
    if (remoteLogManager.isDefined && log != null && log.remoteLogEnabled() &&
      log.logStartOffset <= offset && offset < log.localLogStartOffset())
    {
      val highWatermark = log.highWatermark
      val leaderLogStartOffset = log.logStartOffset
      val leaderLogEndOffset = log.logEndOffset

      if (params.isFromFollower) {
        // If it is from a follower then send the offset metadata only as the data is already available in remote
        // storage and throw an error saying that this offset is moved to tiered storage.
        createLogReadResult(highWatermark, leaderLogStartOffset, leaderLogEndOffset,
          new OffsetMovedToTieredStorageException("Given offset" + offset + " is moved to tiered storage"))
      } else {
        // For consume fetch requests, create a dummy FetchDataInfo with the remote storage fetch information.
        // For the first topic-partition that needs remote data, we will use this information to read the data in another thread.
        val fetchDataInfo =
        new FetchDataInfo(new LogOffsetMetadata(offset), MemoryRecords.EMPTY, false, Optional.empty(),
          Optional.of(new RemoteStorageFetchInfo(adjustedMaxBytes, minOneMessage, tp.topicPartition(),
            fetchInfo, params.isolation, params.hardMaxBytesLimit())))

        LogReadResult(fetchDataInfo,
          divergingEpoch = None,
          highWatermark,
          leaderLogStartOffset,
          leaderLogEndOffset,
          fetchInfo.logStartOffset,
          fetchTimeMs,
          Some(log.lastStableOffset),
          exception = None)
      }
    } else {
      createLogReadResult(exception)
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
        replicaSelectorOpt.flatMap { replicaSelector =>
          val replicaEndpoints = metadataCache.getPartitionReplicaEndpoints(partition.topicPartition,
            new ListenerName(clientMetadata.listenerName))
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
          replicaSelector.select(partition.topicPartition, clientMetadata, partitionInfo).asScala.collect {
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

  def getMagic(topicPartition: TopicPartition): Option[Byte] = getLogConfig(topicPartition).map(_.recordVersion.value)

  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest) : Seq[TopicPartition] =  {
    replicaStateChangeLock synchronized {
      if (updateMetadataRequest.controllerEpoch < controllerEpoch) {
        val stateControllerEpochErrorMessage = s"Received update metadata request with correlation id $correlationId " +
          s"from an old controller ${updateMetadataRequest.controllerId} with epoch ${updateMetadataRequest.controllerEpoch}. " +
          s"Latest known controller epoch is $controllerEpoch"
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        throw new ControllerMovedException(stateChangeLogger.messageWithPrefix(stateControllerEpochErrorMessage))
      } else {
        val zkMetadataCache = metadataCache.asInstanceOf[ZkMetadataCache]
        val deletedPartitions = zkMetadataCache.updateMetadata(correlationId, updateMetadataRequest)
        controllerEpoch = updateMetadataRequest.controllerEpoch
        deletedPartitions
      }
    }
  }

  def becomeLeaderOrFollower(correlationId: Int,
                             leaderAndIsrRequest: LeaderAndIsrRequest,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): LeaderAndIsrResponse = {
    val startMs = time.milliseconds()
    replicaStateChangeLock synchronized {
      val controllerId = leaderAndIsrRequest.controllerId
      val requestPartitionStates = leaderAndIsrRequest.partitionStates.asScala
      stateChangeLogger.info(s"Handling LeaderAndIsr request correlationId $correlationId from controller " +
        s"$controllerId for ${requestPartitionStates.size} partitions")
      if (stateChangeLogger.isTraceEnabled)
        requestPartitionStates.foreach { partitionState =>
          stateChangeLogger.trace(s"Received LeaderAndIsr request $partitionState " +
            s"correlation id $correlationId from controller $controllerId " +
            s"epoch ${leaderAndIsrRequest.controllerEpoch}")
        }
      val topicIds = leaderAndIsrRequest.topicIds()
      def topicIdFromRequest(topicName: String): Option[Uuid] = {
        val topicId = topicIds.get(topicName)
        // if invalid topic ID return None
        if (topicId == null || topicId == Uuid.ZERO_UUID)
          None
        else
          Some(topicId)
      }

      val response = {
        if (leaderAndIsrRequest.controllerEpoch < controllerEpoch) {
          stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
            s"correlation id $correlationId since its controller epoch ${leaderAndIsrRequest.controllerEpoch} is old. " +
            s"Latest known controller epoch is $controllerEpoch")
          leaderAndIsrRequest.getErrorResponse(0, Errors.STALE_CONTROLLER_EPOCH.exception)
        } else {
          val responseMap = new mutable.HashMap[TopicPartition, Errors]
          controllerEpoch = leaderAndIsrRequest.controllerEpoch

          val partitions = new mutable.HashSet[Partition]()
          val partitionsToBeLeader = new mutable.HashMap[Partition, LeaderAndIsrPartitionState]()
          val partitionsToBeFollower = new mutable.HashMap[Partition, LeaderAndIsrPartitionState]()
          val topicIdUpdateFollowerPartitions = new mutable.HashSet[Partition]()
          val allTopicPartitionsInRequest = new mutable.HashSet[TopicPartition]()

          // First create the partition if it doesn't exist already
          requestPartitionStates.foreach { partitionState =>
            val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
            allTopicPartitionsInRequest += topicPartition
            val partitionOpt = getPartition(topicPartition) match {
              case HostedPartition.Offline =>
                stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                  "partition is in an offline log directory")
                responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
                None

              case HostedPartition.Online(partition) =>
                Some(partition)

              case HostedPartition.None =>
                val partition = Partition(topicPartition, time, this)
                allPartitions.putIfNotExists(topicPartition, HostedPartition.Online(partition))
                Some(partition)
            }

            // Next check the topic ID and the partition's leader epoch
            partitionOpt.foreach { partition =>
              val currentLeaderEpoch = partition.getLeaderEpoch
              val requestLeaderEpoch = partitionState.leaderEpoch
              val requestTopicId = topicIdFromRequest(topicPartition.topic)
              val logTopicId = partition.topicId

              if (!hasConsistentTopicId(requestTopicId, logTopicId)) {
                stateChangeLogger.error(s"Topic ID in memory: ${logTopicId.get} does not" +
                  s" match the topic ID for partition $topicPartition received: " +
                  s"${requestTopicId.get}.")
                responseMap.put(topicPartition, Errors.INCONSISTENT_TOPIC_ID)
              } else if (requestLeaderEpoch > currentLeaderEpoch) {
                // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
                // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
                if (partitionState.replicas.contains(localBrokerId)) {
                  partitions += partition
                  if (partitionState.leader == localBrokerId) {
                    partitionsToBeLeader.put(partition, partitionState)
                  } else {
                    partitionsToBeFollower.put(partition, partitionState)
                  }
                } else {
                  stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
                    s"correlation id $correlationId epoch $controllerEpoch for partition $topicPartition as itself is not " +
                    s"in assigned replica list ${partitionState.replicas.asScala.mkString(",")}")
                  responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
                }
              } else if (requestLeaderEpoch < currentLeaderEpoch) {
                stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch is smaller than the current " +
                  s"leader epoch $currentLeaderEpoch")
                responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
              } else {
                val error = requestTopicId match {
                  case Some(topicId) if logTopicId.isEmpty =>
                    // The controller may send LeaderAndIsr to upgrade to using topic IDs without bumping the epoch.
                    // If we have a matching epoch, we expect the log to be defined.
                    val log = localLogOrException(partition.topicPartition)
                    log.assignTopicId(topicId)
                    stateChangeLogger.info(s"Updating log for $topicPartition to assign topic ID " +
                      s"$topicId from LeaderAndIsr request from controller $controllerId with correlation " +
                      s"id $correlationId epoch $controllerEpoch")
                    if (partitionState.leader != localBrokerId)
                      topicIdUpdateFollowerPartitions.add(partition)
                    Errors.NONE
                  case None if logTopicId.isDefined && partitionState.leader != localBrokerId =>
                    // If we have a topic ID in the log but not in the request, we must have previously had topic IDs but
                    // are now downgrading. If we are a follower, remove the topic ID from the PartitionFetchState.
                    stateChangeLogger.info(s"Updating PartitionFetchState for $topicPartition to remove log topic ID " +
                      s"${logTopicId.get} since LeaderAndIsr request from controller $controllerId with correlation " +
                      s"id $correlationId epoch $controllerEpoch did not contain a topic ID")
                    topicIdUpdateFollowerPartitions.add(partition)
                    Errors.NONE
                  case _ =>
                    stateChangeLogger.info(s"Ignoring LeaderAndIsr request from " +
                      s"controller $controllerId with correlation id $correlationId " +
                      s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                      s"leader epoch $requestLeaderEpoch matches the current leader epoch")
                    Errors.STALE_CONTROLLER_EPOCH
                }
                responseMap.put(topicPartition, error)
              }
            }
          }

          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
          val partitionsBecomeLeader = if (partitionsToBeLeader.nonEmpty)
            makeLeaders(controllerId, controllerEpoch, partitionsToBeLeader, correlationId, responseMap,
              highWatermarkCheckpoints, topicIdFromRequest)
          else
            Set.empty[Partition]
          val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
            makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap,
              highWatermarkCheckpoints, topicIdFromRequest)
          else
            Set.empty[Partition]

          val followerTopicSet = partitionsBecomeFollower.map(_.topic).toSet
          updateLeaderAndFollowerMetrics(followerTopicSet)

          if (topicIdUpdateFollowerPartitions.nonEmpty)
            updateTopicIdForFollowers(controllerId, controllerEpoch, topicIdUpdateFollowerPartitions, correlationId, topicIdFromRequest)

          // We initialize highwatermark thread after the first LeaderAndIsr request. This ensures that all the partitions
          // have been completely populated before starting the checkpointing there by avoiding weird race conditions
          startHighWatermarkCheckPointThread()

          // In migration mode, reconcile missed topic deletions when handling full LISR from KRaft controller.
          // LISR "type" field was previously unspecified (0), so if we see it set to Full (2), then we know the
          // request came from a KRaft controller.
          if (
            config.migrationEnabled &&
            leaderAndIsrRequest.isKRaftController &&
            leaderAndIsrRequest.requestType() == LeaderAndIsrRequest.Type.FULL
          ) {
            updateStrayLogs(findStrayPartitionsFromLeaderAndIsr(allTopicPartitionsInRequest))
          }

          maybeAddLogDirFetchers(partitions, highWatermarkCheckpoints, topicIdFromRequest)

          replicaFetcherManager.shutdownIdleFetcherThreads()
          replicaAlterLogDirsManager.shutdownIdleFetcherThreads()

          remoteLogManager.foreach(rlm => rlm.onLeadershipChange(partitionsBecomeLeader.asJava, partitionsBecomeFollower.asJava, topicIds))

          onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)

          val data = new LeaderAndIsrResponseData().setErrorCode(Errors.NONE.code)
          if (leaderAndIsrRequest.version < 5) {
            responseMap.forKeyValue { (tp, error) =>
              data.partitionErrors.add(new LeaderAndIsrPartitionError()
                .setTopicName(tp.topic)
                .setPartitionIndex(tp.partition)
                .setErrorCode(error.code))
            }
          } else {
            responseMap.forKeyValue { (tp, error) =>
              val topicId = topicIds.get(tp.topic)
              var topic = data.topics.find(topicId)
              if (topic == null) {
                topic = new LeaderAndIsrTopicError().setTopicId(topicId)
                data.topics.add(topic)
              }
              topic.partitionErrors.add(new LeaderAndIsrPartitionError()
                .setPartitionIndex(tp.partition)
                .setErrorCode(error.code))
            }
          }
          new LeaderAndIsrResponse(data, leaderAndIsrRequest.version)
        }
      }
      val endMs = time.milliseconds()
      val elapsedMs = endMs - startMs
      stateChangeLogger.info(s"Finished LeaderAndIsr request in ${elapsedMs}ms correlationId $correlationId from controller " +
        s"$controllerId for ${requestPartitionStates.size} partitions")
      response
    }
  }

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
  protected def updateLeaderAndFollowerMetrics(newFollowerTopics: Set[String]): Unit = {
    val leaderTopicSet = leaderPartitionsIterator.map(_.topic).toSet
    newFollowerTopics.diff(leaderTopicSet).foreach(brokerTopicStats.removeOldLeaderMetrics)

    // remove metrics for brokers which are not followers of a topic
    leaderTopicSet.diff(newFollowerTopics).foreach(brokerTopicStats.removeOldFollowerMetrics)
  }

  protected[server] def maybeAddLogDirFetchers(partitions: Set[Partition],
                                       offsetCheckpoints: OffsetCheckpoints,
                                       topicIds: String => Option[Uuid]): Unit = {
    val futureReplicasAndInitialOffset = new mutable.HashMap[TopicPartition, InitialFetchState]
    for (partition <- partitions) {
      val topicPartition = partition.topicPartition
      logManager.getLog(topicPartition, isFuture = true).foreach { futureLog =>
        partition.log.foreach { _ =>
          val leader = BrokerEndPoint(config.brokerId, "localhost", -1)

          // Add future replica log to partition's map
          partition.createLogIfNotExists(
            isNew = false,
            isFutureReplica = true,
            offsetCheckpoints,
            topicIds(partition.topic))

          // pause cleaning for partitions that are being moved and start ReplicaAlterDirThread to move
          // replica from source dir to destination dir
          logManager.abortAndPauseCleaning(topicPartition)

          futureReplicasAndInitialOffset.put(topicPartition, InitialFetchState(topicIds(topicPartition.topic), leader,
            partition.getLeaderEpoch, futureLog.highWatermark))
        }
      }
    }

    if (futureReplicasAndInitialOffset.nonEmpty)
      replicaAlterLogDirsManager.addFetcherForPartitions(futureReplicasAndInitialOffset)
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made leader due to this method
   *
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int,
                          controllerEpoch: Int,
                          partitionStates: Map[Partition, LeaderAndIsrPartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Errors],
                          highWatermarkCheckpoints: OffsetCheckpoints,
                          topicIds: String => Option[Uuid]): Set[Partition] = {
    val traceEnabled = stateChangeLogger.isTraceEnabled
    partitionStates.keys.foreach { partition =>
      if (traceEnabled)
        stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from " +
          s"controller $controllerId epoch $controllerEpoch starting the become-leader transition for " +
          s"partition ${partition.topicPartition}")
      responseMap.put(partition.topicPartition, Errors.NONE)
    }

    val partitionsToMakeLeaders = mutable.Set[Partition]()

    try {
      // First stop fetchers for all the partitions
      replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))
      stateChangeLogger.info(s"Stopped fetchers as part of LeaderAndIsr request correlationId $correlationId from " +
        s"controller $controllerId epoch $controllerEpoch as part of the become-leader transition for " +
        s"${partitionStates.size} partitions")
      // Update the partition information to be the leader
      partitionStates.forKeyValue { (partition, partitionState) =>
        try {
          if (partition.makeLeader(partitionState, highWatermarkCheckpoints, topicIds(partitionState.topicName))) {
            partitionsToMakeLeaders += partition
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-leader state change with " +
              s"correlation id $correlationId from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) since " +
              s"the replica for the partition is offline due to storage error $e")
            // If there is an offline log directory, a Partition object may have been created and have been added
            // to `ReplicaManager.allPartitions` before `createLogIfNotExists()` failed to create local replica due
            // to KafkaStorageException. In this case `ReplicaManager.allPartitions` will map this topic-partition
            // to an empty Partition object. We need to map this topic-partition to OfflinePartition instead.
            markPartitionOffline(partition.topicPartition)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

    } catch {
      case e: Throwable =>
        partitionStates.keys.foreach { partition =>
          stateChangeLogger.error(s"Error while processing LeaderAndIsr request correlationId $correlationId received " +
            s"from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition}", e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    if (traceEnabled)
      partitionStates.keys.foreach { partition =>
        stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch for the become-leader transition for partition ${partition.topicPartition}")
      }

    partitionsToMakeLeaders
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Remove these partitions from the leader partitions set.
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   * 4. Truncate the log and checkpoint offsets for these partitions.
   * 5. Clear the produce and fetch requests in the purgatory
   * 6. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made follower due to this method
   */
  private def makeFollowers(controllerId: Int,
                            controllerEpoch: Int,
                            partitionStates: Map[Partition, LeaderAndIsrPartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Errors],
                            highWatermarkCheckpoints: OffsetCheckpoints,
                            topicIds: String => Option[Uuid]) : Set[Partition] = {
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled
    partitionStates.forKeyValue { (partition, partitionState) =>
      if (traceLoggingEnabled)
        stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch starting the become-follower transition for partition ${partition.topicPartition} with leader " +
          s"${partitionState.leader}")
      responseMap.put(partition.topicPartition, Errors.NONE)
    }

    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()
    try {
      partitionStates.forKeyValue { (partition, partitionState) =>
        val newLeaderBrokerId = partitionState.leader
        try {
          if (metadataCache.hasAliveBroker(newLeaderBrokerId)) {
            // Only change partition state when the leader is available
            if (partition.makeFollower(partitionState, highWatermarkCheckpoints, topicIds(partitionState.topicName))) {
              partitionsToMakeFollower += partition
            }
          } else {
            // The leader broker should always be present in the metadata cache.
            // If not, we should record the error message and abort the transition process for this partition
            stateChangeLogger.error(s"Received LeaderAndIsrRequest with correlation id $correlationId from " +
              s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) " +
              s"but cannot become follower since the new leader $newLeaderBrokerId is unavailable.")
            // Create the local replica even if the leader is unavailable. This is required to ensure that we include
            // the partition's high watermark in the checkpoint file (see KAFKA-1647)
            partition.createLogIfNotExists(isNew = partitionState.isNew, isFutureReplica = false,
              highWatermarkCheckpoints, topicIds(partitionState.topicName))
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-follower state change with correlation id $correlationId from " +
              s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) with leader " +
              s"$newLeaderBrokerId since the replica for the partition is offline due to storage error $e")
            // If there is an offline log directory, a Partition object may have been created and have been added
            // to `ReplicaManager.allPartitions` before `createLogIfNotExists()` failed to create local replica due
            // to KafkaStorageException. In this case `ReplicaManager.allPartitions` will map this topic-partition
            // to an empty Partition object. We need to map this topic-partition to OfflinePartition instead.
            markPartitionOffline(partition.topicPartition)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

      // Stopping the fetchers must be done first in order to initialize the fetch
      // position correctly.
      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
      stateChangeLogger.info(s"Stopped fetchers as part of become-follower request from controller $controllerId " +
        s"epoch $controllerEpoch with correlation id $correlationId for ${partitionsToMakeFollower.size} partitions")

      partitionsToMakeFollower.foreach { partition =>
        completeDelayedFetchOrProduceRequests(partition.topicPartition)
      }

      if (isShuttingDown.get()) {
        if (traceLoggingEnabled) {
          partitionsToMakeFollower.foreach { partition =>
            stateChangeLogger.trace(s"Skipped the adding-fetcher step of the become-follower state " +
              s"change with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
              s"partition ${partition.topicPartition} with leader ${partitionStates(partition).leader} " +
              "since it is shutting down")
          }
        }
      } else {
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map { partition =>
          val leaderNode = partition.leaderReplicaIdOpt.flatMap(leaderId => metadataCache.
            getAliveBrokerNode(leaderId, config.interBrokerListenerName)).getOrElse(Node.noNode())
          val leader = new BrokerEndPoint(leaderNode.id(), leaderNode.host(), leaderNode.port())
          val log = partition.localLogOrException
          val fetchOffset = initialFetchOffset(log)
          partition.topicPartition -> InitialFetchState(topicIds(partition.topic), leader, partition.getLeaderEpoch, fetchOffset)
        }.toMap

        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"Error while processing LeaderAndIsr request with correlationId $correlationId " +
          s"received from controller $controllerId epoch $controllerEpoch", e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    if (traceLoggingEnabled)
      partitionStates.keys.foreach { partition =>
        stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch for the become-follower transition for partition ${partition.topicPartition} with leader " +
          s"${partitionStates(partition).leader}")
      }

    partitionsToMakeFollower
  }

  private def updateTopicIdForFollowers(controllerId: Int,
                                        controllerEpoch: Int,
                                        partitions: Set[Partition],
                                        correlationId: Int,
                                        topicIds: String => Option[Uuid]): Unit = {
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled

    try {
      if (isShuttingDown.get()) {
        if (traceLoggingEnabled) {
          partitions.foreach { partition =>
            stateChangeLogger.trace(s"Skipped the update topic ID step of the become-follower state " +
              s"change with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
              s"partition ${partition.topicPartition} since it is shutting down")
          }
        }
      } else {
        val partitionsToUpdateFollowerWithLeader = mutable.Map.empty[TopicPartition, Int]
        partitions.foreach { partition =>
          partition.leaderReplicaIdOpt.foreach { leader =>
            if (metadataCache.hasAliveBroker(leader)) {
              partitionsToUpdateFollowerWithLeader += partition.topicPartition -> leader
            }
          }
        }
        replicaFetcherManager.maybeUpdateTopicIds(partitionsToUpdateFollowerWithLeader, topicIds)
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"Error while processing LeaderAndIsr request with correlationId $correlationId " +
          s"received from controller $controllerId epoch $controllerEpoch when trying to update topic IDs in the fetchers", e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }
  }

  /**
   * From IBP 2.7 onwards, we send latest fetch epoch in the request and truncate if a
   * diverging epoch is returned in the response, avoiding the need for a separate
   * OffsetForLeaderEpoch request.
   */
  protected def initialFetchOffset(log: UnifiedLog): Long = {
    if (metadataCache.metadataVersion().isTruncationOnFetchSupported && log.latestEpoch.nonEmpty)
      log.logEndOffset
    else
      log.highWatermark
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")

    // Shrink ISRs for non offline partitions
    allPartitions.keys.foreach { topicPartition =>
      onlinePartition(topicPartition).foreach(_.maybeShrinkIsr())
    }
  }

  private def leaderPartitionsIterator: Iterator[Partition] =
    onlinePartitionsIterator.filter(_.leaderLogIfLocal.isDefined)

  def getLogEndOffset(topicPartition: TopicPartition): Option[Long] =
    onlinePartition(topicPartition).flatMap(_.leaderLogIfLocal.map(_.logEndOffset))

  // Flushes the highwatermark value for all partitions to the highwatermark file
  def checkpointHighWatermarks(): Unit = {
    def putHw(logDirToCheckpoints: mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Long]],
              log: UnifiedLog): Unit = {
      val checkpoints = logDirToCheckpoints.getOrElseUpdate(log.parentDir,
        new mutable.AnyRefMap[TopicPartition, Long]())
      checkpoints.put(log.topicPartition, log.highWatermark)
    }

    val logDirToHws = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Long]](
      allPartitions.size)
    onlinePartitionsIterator.foreach { partition =>
      partition.log.foreach(putHw(logDirToHws, _))
      partition.futureLog.foreach(putHw(logDirToHws, _))
    }

    for ((logDir, hws) <- logDirToHws) {
      try highWatermarkCheckpoints.get(logDir).foreach(_.write(hws))
      catch {
        case e: KafkaStorageException =>
          error(s"Error while writing to highwatermark file in directory $logDir", e)
      }
    }
  }

  def markPartitionOffline(tp: TopicPartition): Unit = replicaStateChangeLock synchronized {
    allPartitions.put(tp, HostedPartition.Offline) match {
      case HostedPartition.Online(partition) =>
        partition.markOffline()
      case _ => // Nothing
    }
  }

  /**
   * The log directory failure handler for the replica
   *
   * @param dir                     the absolute path of the log directory
   * @param sendZkNotification      check if we need to send notification to zookeeper node (needed for unit test)
   */
  def handleLogDirFailure(dir: String, sendZkNotification: Boolean = true): Unit = {
    if (!logManager.isLogDirOnline(dir))
      return
    warn(s"Stopping serving replicas in dir $dir")
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

    if (sendZkNotification)
      if (zkClient.isEmpty) {
        warn("Unable to propagate log dir failure via Zookeeper in KRaft mode")
      } else {
        zkClient.get.propagateLogDirEvent(localBrokerId)
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
    delayedProducePurgatory.shutdown()
    delayedDeleteRecordsPurgatory.shutdown()
    delayedElectLeaderPurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    replicaSelectorOpt.foreach(_.close)
    removeAllTopicMetrics()
    addPartitionsToTxnManager.foreach(_.shutdown())
    info("Shut down completely")
  }

  private def removeAllTopicMetrics(): Unit = {
    val allTopics = new util.HashSet[String]
    allPartitions.keys.foreach(partition =>
      if (allTopics.add(partition.topic())) {
        brokerTopicStats.removeMetrics(partition.topic())
      })
  }

  protected def createReplicaFetcherManager(metrics: Metrics, time: Time, threadNamePrefix: Option[String], quotaManager: ReplicationQuotaManager) = {
    new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager, () => metadataCache.metadataVersion(), brokerEpochSupplier)
  }

  protected def createReplicaAlterLogDirsManager(quotaManager: ReplicationQuotaManager, brokerTopicStats: BrokerTopicStats) = {
    new ReplicaAlterLogDirsManager(config, this, quotaManager, brokerTopicStats)
  }

  protected def createReplicaSelector(): Option[ReplicaSelector] = {
    config.replicaSelectorClassName.map { className =>
      val tmpReplicaSelector: ReplicaSelector = CoreUtils.createObject[ReplicaSelector](className)
      tmpReplicaSelector.configure(config.originals())
      tmpReplicaSelector
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

          case HostedPartition.Offline =>
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

  def electLeaders(
    controller: KafkaController,
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    responseCallback: Map[TopicPartition, ApiError] => Unit,
    requestTimeout: Int
  ): Unit = {

    val deadline = time.milliseconds() + requestTimeout

    def electionCallback(results: Map[TopicPartition, Either[ApiError, Int]]): Unit = {
      val expectedLeaders = mutable.Map.empty[TopicPartition, Int]
      val failures = mutable.Map.empty[TopicPartition, ApiError]
      results.foreach {
        case (partition, Right(leader)) => expectedLeaders += partition -> leader
        case (partition, Left(error)) => failures += partition -> error
      }
      if (expectedLeaders.nonEmpty) {
        val watchKeys = expectedLeaders.iterator.map {
          case (tp, _) => TopicPartitionOperationKey(tp)
        }.toBuffer

        delayedElectLeaderPurgatory.tryCompleteElseWatch(
          new DelayedElectLeader(
            math.max(0, deadline - time.milliseconds()),
            expectedLeaders,
            failures,
            this,
            responseCallback
          ),
          watchKeys
        )
      } else {
          // There are no partitions actually being elected, so return immediately
          responseCallback(failures)
      }
    }

    controller.electLeaders(partitions, electionType, electionCallback)
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
      case HostedPartition.Offline =>
        stateChangeLogger.warn(s"Unable to bring up new local leader $tp " +
          s"with topic id $topicId because it resides in an offline log " +
          "directory.")
        None

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
        val partition = Partition(tp, time, this)
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
            StopPartition(tp, deleteLocalLog = true, deleteRemoteLog = deleteRemoteLog)
          }
          .toSet
        stateChangeLogger.info(s"Deleting ${deletes.size} partition(s).")
        stopPartitions(deletes).forKeyValue { (topicPartition, e) =>
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
        val lazyOffsetCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
        val leaderChangedPartitions = new mutable.HashSet[Partition]
        val followerChangedPartitions = new mutable.HashSet[Partition]
        if (!localChanges.leaders.isEmpty) {
          applyLocalLeadersDelta(leaderChangedPartitions, delta, lazyOffsetCheckpoints, localChanges.leaders.asScala)
        }
        if (!localChanges.followers.isEmpty) {
          applyLocalFollowersDelta(followerChangedPartitions, newImage, delta, lazyOffsetCheckpoints, localChanges.followers.asScala)
        }

        maybeAddLogDirFetchers(leaderChangedPartitions ++ followerChangedPartitions, lazyOffsetCheckpoints,
          name => Option(newImage.topics().getTopic(name)).map(_.id()))

        replicaFetcherManager.shutdownIdleFetcherThreads()
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()

        remoteLogManager.foreach(rlm => rlm.onLeadershipChange(leaderChangedPartitions.asJava, followerChangedPartitions.asJava, localChanges.topicIds()))
      }
    }
  }

  private def applyLocalLeadersDelta(
    changedPartitions: mutable.Set[Partition],
    delta: TopicsDelta,
    offsetCheckpoints: OffsetCheckpoints,
    localLeaders: mutable.Map[TopicPartition, LocalReplicaChanges.PartitionInfo]
  ): Unit = {
    stateChangeLogger.info(s"Transitioning ${localLeaders.size} partition(s) to " +
      "local leaders.")
    replicaFetcherManager.removeFetcherForPartitions(localLeaders.keySet)
    localLeaders.forKeyValue { (tp, info) =>
      getOrCreatePartition(tp, delta, info.topicId).foreach { case (partition, isNew) =>
        try {
          val state = info.partition.toLeaderAndIsrPartitionState(tp, isNew)
          partition.makeLeader(state, offsetCheckpoints, Some(info.topicId))
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
    localFollowers: mutable.Map[TopicPartition, LocalReplicaChanges.PartitionInfo]
  ): Unit = {
    stateChangeLogger.info(s"Transitioning ${localFollowers.size} partition(s) to " +
      "local followers.")
    val partitionsToStartFetching = new mutable.HashMap[TopicPartition, Partition]
    val partitionsToStopFetching = new mutable.HashMap[TopicPartition, Boolean]
    val followerTopicSet = new mutable.HashSet[String]
    localFollowers.forKeyValue { (tp, info) =>
      getOrCreatePartition(tp, delta, info.topicId).foreach { case (partition, isNew) =>
        try {
          followerTopicSet.add(tp.topic)

          // We always update the follower state.
          // - This ensure that a replica with no leader can step down;
          // - This also ensures that the local replica is created even if the leader
          //   is unavailable. This is required to ensure that we include the partition's
          //   high watermark in the checkpoint file (see KAFKA-1647).
          val state = info.partition.toLeaderAndIsrPartitionState(tp, isNew)
          val isNewLeaderEpoch = partition.makeFollower(state, offsetCheckpoints, Some(info.topicId))

          if (isInControlledShutdown && (info.partition.leader == NO_LEADER ||
              !info.partition.isr.contains(config.brokerId))) {
            // During controlled shutdown, replica with no leaders and replica
            // where this broker is not in the ISR are stopped.
            partitionsToStopFetching.put(tp, false)
          } else if (isNewLeaderEpoch) {
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

      partitionsToStartFetching.forKeyValue { (topicPartition, partition) =>
        val nodeOpt = partition.leaderReplicaIdOpt
          .flatMap(leaderId => Option(newImage.cluster.broker(leaderId)))
          .flatMap(_.node(listenerName).asScala)

        nodeOpt match {
          case Some(node) =>
            val log = partition.localLogOrException
            partitionAndOffsets.put(topicPartition, InitialFetchState(
              log.topicId,
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

      partitionsToStartFetching.keySet.foreach(completeDelayedFetchOrProduceRequests)

      updateLeaderAndFollowerMetrics(followerTopicSet)
    }

    if (partitionsToStopFetching.nonEmpty) {
      stopPartitions(partitionsToStopFetching)
      stateChangeLogger.info(s"Stopped fetchers as part of controlled shutdown for ${partitionsToStopFetching.size} partitions")
    }
  }

  def deleteStrayReplicas(topicPartitions: Iterable[TopicPartition]): Unit = {
    stopPartitions(topicPartitions.map(tp => tp -> true).toMap).forKeyValue { (topicPartition, exception) =>
      exception match {
        case e: KafkaStorageException =>
          stateChangeLogger.error(s"Unable to delete stray replica $topicPartition because " +
            s"the local replica for the partition is in an offline log directory: ${e.getMessage}.")
        case e: Throwable =>
          stateChangeLogger.error(s"Unable to delete stray replica $topicPartition because " +
            s"we got an unexpected ${e.getClass.getName} exception: ${e.getMessage}", e)
      }
    }
  }
}
