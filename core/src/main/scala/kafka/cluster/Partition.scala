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
package kafka.cluster

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.Optional
import java.util.concurrent.{CompletableFuture, CopyOnWriteArrayList}
import kafka.controller.{KafkaController, StateChangeLogger}
import kafka.log._
import kafka.log.remote.RemoteLogManager
import kafka.server._
import kafka.server.metadata.{KRaftMetadataCache, ZkMetadataCache}
import kafka.server.share.DelayedShareFetch
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.zookeeper.ZooKeeperClientException
import org.apache.kafka.common.{DirectoryId, IsolationLevel, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState
import org.apache.kafka.common.message.{DescribeProducersResponseData, FetchResponseData}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.{LeaderAndIsr, LeaderRecoveryState}
import org.apache.kafka.server.common.{MetadataVersion, RequestLocal}
import org.apache.kafka.storage.internals.log.{AppendOrigin, FetchDataInfo, LeaderHwChange, LogAppendInfo, LogOffsetMetadata, LogOffsetSnapshot, LogOffsetsListener, LogReadInfo, LogStartOffsetIncrementReason, VerificationGuard}
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.purgatory.{DelayedOperationPurgatory, TopicPartitionOperationKey}
import org.apache.kafka.server.share.fetch.DelayedShareFetchPartitionKey
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, UnexpectedAppendOffsetException}
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpoints
import org.slf4j.event.Level

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

/**
 * Listener receives notification from an Online Partition.
 *
 * A listener can be (re-)registered to an Online partition only. The listener
 * is notified as long as the partition remains Online. When the partition fails
 * or is deleted, respectively `onFailed` or `onDeleted` are called once. No further
 * notifications are sent after this point on.
 *
 * Note that the callbacks are executed in the thread that triggers the change
 * AND that locks may be held during their execution. They are meant to be used
 * as notification mechanism only.
 */
trait PartitionListener {
  /**
   * Called when the Log increments its high watermark.
   */
  def onHighWatermarkUpdated(partition: TopicPartition, offset: Long): Unit = {}

  /**
   * Called when the Partition (or replica) on this broker has a failure (e.g. goes offline).
   */
  def onFailed(partition: TopicPartition): Unit = {}

  /**
   * Called when the Partition (or replica) on this broker is deleted. Note that it does not mean
   * that the partition was deleted but only that this broker does not host a replica of it any more.
   */
  def onDeleted(partition: TopicPartition): Unit = {}

  /**
   * Called when the Partition on this broker is transitioned to follower.
   */
  def onBecomingFollower(partition: TopicPartition): Unit = {}
}

trait AlterPartitionListener {
  def markIsrExpand(): Unit
  def markIsrShrink(): Unit
  def markFailed(): Unit
}

class DelayedOperations(topicId: Option[Uuid],
                        topicPartition: TopicPartition,
                        produce: DelayedOperationPurgatory[DelayedProduce],
                        fetch: DelayedOperationPurgatory[DelayedFetch],
                        deleteRecords: DelayedOperationPurgatory[DelayedDeleteRecords],
                        shareFetch: DelayedOperationPurgatory[DelayedShareFetch]) extends Logging {

  def checkAndCompleteAll(): Unit = {
    val requestKey = new TopicPartitionOperationKey(topicPartition)
    CoreUtils.swallow(() -> fetch.checkAndComplete(requestKey), this, Level.ERROR)
    CoreUtils.swallow(() -> produce.checkAndComplete(requestKey), this, Level.ERROR)
    CoreUtils.swallow(() -> deleteRecords.checkAndComplete(requestKey), this, Level.ERROR)
    if (topicId.isDefined) CoreUtils.swallow(() -> shareFetch.checkAndComplete(new DelayedShareFetchPartitionKey(
      topicId.get, topicPartition.partition())), this, Level.ERROR)
  }

  def numDelayedDelete: Int = deleteRecords.numDelayed()
}

object Partition {
  private val metricsGroup = new KafkaMetricsGroup(classOf[Partition])

  def apply(topicIdPartition: TopicIdPartition,
            time: Time,
            replicaManager: ReplicaManager): Partition = {
    Partition(
      topicPartition = topicIdPartition.topicPartition(),
      topicId = Option(topicIdPartition.topicId()),
      time = time,
      replicaManager = replicaManager)
  }
  def apply(topicPartition: TopicPartition,
            time: Time,
            replicaManager: ReplicaManager,
            topicId: Option[Uuid] = None): Partition = {

    val isrChangeListener = new AlterPartitionListener {
      override def markIsrExpand(): Unit = {
        replicaManager.isrExpandRate.mark()
      }

      override def markIsrShrink(): Unit = {
        replicaManager.isrShrinkRate.mark()
      }

      override def markFailed(): Unit = replicaManager.failedIsrUpdatesRate.mark()
    }

    val delayedOperations = new DelayedOperations(
      topicId,
      topicPartition,
      replicaManager.delayedProducePurgatory,
      replicaManager.delayedFetchPurgatory,
      replicaManager.delayedDeleteRecordsPurgatory,
      replicaManager.delayedShareFetchPurgatory)

    new Partition(topicPartition,
      _topicId = topicId,
      replicaLagTimeMaxMs = replicaManager.config.replicaLagTimeMaxMs,
      interBrokerProtocolVersion = replicaManager.metadataCache.metadataVersion(),
      localBrokerId = replicaManager.config.brokerId,
      localBrokerEpochSupplier = replicaManager.brokerEpochSupplier,
      time = time,
      alterPartitionListener = isrChangeListener,
      delayedOperations = delayedOperations,
      metadataCache = replicaManager.metadataCache,
      logManager = replicaManager.logManager,
      alterIsrManager = replicaManager.alterPartitionManager)
  }

  def removeMetrics(topicPartition: TopicPartition): Unit = {
    val tags = Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString).asJava
    metricsGroup.removeMetric("UnderReplicated", tags)
    metricsGroup.removeMetric("UnderMinIsr", tags)
    metricsGroup.removeMetric("InSyncReplicasCount", tags)
    metricsGroup.removeMetric("ReplicasCount", tags)
    metricsGroup.removeMetric("LastStableOffsetLag", tags)
    metricsGroup.removeMetric("AtMinIsr", tags)
  }
}


sealed trait AssignmentState {
  def replicas: Seq[Int]
  def replicationFactor: Int = replicas.size
  def isAddingReplica(brokerId: Int): Boolean = false
}

case class OngoingReassignmentState(addingReplicas: Seq[Int],
                                    removingReplicas: Seq[Int],
                                    replicas: Seq[Int]) extends AssignmentState {

  override def replicationFactor: Int = replicas.diff(addingReplicas).size // keep the size of the original replicas
  override def isAddingReplica(replicaId: Int): Boolean = addingReplicas.contains(replicaId)
}

case class SimpleAssignmentState(replicas: Seq[Int]) extends AssignmentState


sealed trait PartitionState {
  /**
   * Includes only the in-sync replicas which have been committed to ZK.
   */
  def isr: Set[Int]

  /**
   * This set may include un-committed ISR members following an expansion. This "effective" ISR is used for advancing
   * the high watermark as well as determining which replicas are required for acks=all produce requests.
   *
   * Only applicable as of IBP 2.7-IV2, for older versions this will return the committed ISR
   */
  def maximalIsr: Set[Int]

  /**
   * The leader recovery state. See the description for LeaderRecoveryState for details on the different values.
   */
  def leaderRecoveryState: LeaderRecoveryState

  /**
   * Indicates if we have an AlterPartition request inflight.
   */
  def isInflight: Boolean
}

sealed trait PendingPartitionChange extends PartitionState {
  def lastCommittedState: CommittedPartitionState
  def sentLeaderAndIsr: LeaderAndIsr

  override val leaderRecoveryState: LeaderRecoveryState = LeaderRecoveryState.RECOVERED

  def notifyListener(alterPartitionListener: AlterPartitionListener): Unit
}

case class PendingExpandIsr(
  newInSyncReplicaId: Int,
  sentLeaderAndIsr: LeaderAndIsr,
  lastCommittedState: CommittedPartitionState
) extends PendingPartitionChange {
  val isr: Set[Int] = lastCommittedState.isr
  val maximalIsr: Set[Int] = isr + newInSyncReplicaId
  val isInflight = true

  def notifyListener(alterPartitionListener: AlterPartitionListener): Unit = {
    alterPartitionListener.markIsrExpand()
  }

  override def toString: String = {
    s"PendingExpandIsr(newInSyncReplicaId=$newInSyncReplicaId" +
    s", sentLeaderAndIsr=$sentLeaderAndIsr" +
    s", leaderRecoveryState=$leaderRecoveryState" +
    s", lastCommittedState=$lastCommittedState" +
    ")"
  }
}

case class PendingShrinkIsr(
  outOfSyncReplicaIds: Set[Int],
  sentLeaderAndIsr: LeaderAndIsr,
  lastCommittedState: CommittedPartitionState
) extends PendingPartitionChange  {
  val isr: Set[Int] = lastCommittedState.isr
  val maximalIsr: Set[Int] = isr
  val isInflight = true

  def notifyListener(alterPartitionListener: AlterPartitionListener): Unit = {
    alterPartitionListener.markIsrShrink()
  }

  override def toString: String = {
    s"PendingShrinkIsr(outOfSyncReplicaIds=$outOfSyncReplicaIds" +
    s", sentLeaderAndIsr=$sentLeaderAndIsr" +
    s", leaderRecoveryState=$leaderRecoveryState" +
    s", lastCommittedState=$lastCommittedState" +
    ")"
  }
}

case class CommittedPartitionState(
  isr: Set[Int],
  leaderRecoveryState: LeaderRecoveryState
) extends PartitionState {
  val maximalIsr: Set[Int] = isr
  val isInflight = false

  override def toString: String = {
    s"CommittedPartitionState(isr=$isr" +
    s", leaderRecoveryState=$leaderRecoveryState" +
    ")"
  }
}


/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 *
 * Concurrency notes:
 * 1) Partition is thread-safe. Operations on partitions may be invoked concurrently from different
 *    request handler threads
 * 2) ISR updates are synchronized using a read-write lock. Read lock is used to check if an update
 *    is required to avoid acquiring write lock in the common case of replica fetch when no update
 *    is performed. ISR update condition is checked a second time under write lock before performing
 *    the update
 * 3) Various other operations like leader changes are processed while holding the ISR write lock.
 *    This can introduce delays in produce and replica fetch requests, but these operations are typically
 *    infrequent.
 * 4) HW updates are synchronized using ISR read lock. @Log lock is acquired during the update with
 *    locking order Partition lock -> Log lock.
 * 5) lock is used to prevent the follower replica from being updated while ReplicaAlterDirThread is
 *    executing maybeReplaceCurrentWithFutureReplica() to replace follower replica with the future replica.
 */
class Partition(val topicPartition: TopicPartition,
                val replicaLagTimeMaxMs: Long,
                interBrokerProtocolVersion: MetadataVersion,
                localBrokerId: Int,
                localBrokerEpochSupplier: () => Long,
                time: Time,
                alterPartitionListener: AlterPartitionListener,
                delayedOperations: DelayedOperations,
                metadataCache: MetadataCache,
                logManager: LogManager,
                alterIsrManager: AlterPartitionManager,
                @volatile private var _topicId: Option[Uuid] = None // TODO: merge topicPartition and _topicId into TopicIdPartition once TopicId persist in most of the code by KAFKA-16212
               ) extends Logging {

  import Partition.metricsGroup
  def topic: String = topicPartition.topic
  def partitionId: Int = topicPartition.partition

  private val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)
  private val remoteReplicasMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock

  // lock to prevent the follower replica log update while checking if the log dir could be replaced with future log.
  private val futureLogLock = new Object()
  // The current epoch for the partition for KRaft controllers. The current ZK version for the legacy controllers.
  @volatile private var partitionEpoch: Int = LeaderAndIsr.INITIAL_PARTITION_EPOCH
  @volatile private var leaderEpoch: Int = LeaderAndIsr.INITIAL_LEADER_EPOCH - 1
  // start offset for 'leaderEpoch' above (leader epoch of the current leader for this partition),
  // defined when this broker is leader for partition
  @volatile private[cluster] var leaderEpochStartOffsetOpt: Option[Long] = None
  // Replica ID of the leader, defined when this broker is leader or follower for the partition.
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  @volatile private[cluster] var partitionState: PartitionState = CommittedPartitionState(Set.empty, LeaderRecoveryState.RECOVERED)
  @volatile var assignmentState: AssignmentState = SimpleAssignmentState(Seq.empty)

  // Logs belonging to this partition. Majority of time it will be only one log, but if log directory
  // is getting changed (as a result of ReplicaAlterLogDirs command), we may have two logs until copy
  // completes and a switch to new location is performed.
  // log and futureLog variables defined below are used to capture this
  @volatile var log: Option[UnifiedLog] = None
  // If ReplicaAlterLogDir command is in progress, this is future location of the log
  @volatile var futureLog: Option[UnifiedLog] = None

  // Partition listeners
  private val listeners = new CopyOnWriteArrayList[PartitionListener]()

  private val logOffsetsListener = new LogOffsetsListener {
    override def onHighWatermarkUpdated(offset: Long): Unit = {
      listeners.forEach { listener =>
        listener.onHighWatermarkUpdated(topicPartition, offset)
      }
    }
  }

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch
  this.logIdent = s"[Partition $topicPartition broker=$localBrokerId] "

  private val tags = Map("topic" -> topic, "partition" -> partitionId.toString).asJava

  metricsGroup.newGauge("UnderReplicated", () => if (isUnderReplicated) 1 else 0, tags)
  metricsGroup.newGauge("InSyncReplicasCount", () => if (isLeader) partitionState.isr.size else 0, tags)
  metricsGroup.newGauge("UnderMinIsr", () => if (isUnderMinIsr) 1 else 0, tags)
  metricsGroup.newGauge("AtMinIsr", () => if (isAtMinIsr) 1 else 0, tags)
  metricsGroup.newGauge("ReplicasCount", () => if (isLeader) assignmentState.replicationFactor else 0, tags)
  metricsGroup.newGauge("LastStableOffsetLag", () => log.map(_.lastStableOffsetLag).getOrElse(0), tags)

  def hasLateTransaction(currentTimeMs: Long): Boolean = leaderLogIfLocal.exists(_.hasLateTransaction(currentTimeMs))

  def isUnderReplicated: Boolean = isLeader && (assignmentState.replicationFactor - partitionState.isr.size) > 0

  // In the makeFollower process, it will first change the leaderReplicaIdOpt then clear the ISR. In order not to get
  // a false positive under min isr check, it has to check the leaderReplicaIdOpt again. Though it can still be affected
  // by ABA problems when leader->follower->leader, but it should be good enough for a metric.
  def isUnderMinIsr: Boolean = {
    leaderLogIfLocal.exists { partitionState.isr.size < effectiveMinIsr(_) } && isLeader
  }

/**
 * When setting the min ISR, there is no restriction on it. Even if the value does not make sense to be larger than
 * the replication factor. In case there are such setting, the effective min ISR of min(replication factor, min ISR)
 * is returned here.
 */
  private def effectiveMinIsr(leaderLog: UnifiedLog): Int = {
      leaderLog.config.minInSyncReplicas.min(remoteReplicasMap.size + 1)
  }

  /**
   * Returns whether the partition ISR size is equals to the effective min ISR. For more info about this min ISR, refer to
   * the effectiveMinIsr().
   */
  def isAtMinIsr: Boolean = leaderLogIfLocal.exists { partitionState.isr.size == effectiveMinIsr(_) }

  def isReassigning: Boolean = assignmentState.isInstanceOf[OngoingReassignmentState]

  def isAddingLocalReplica: Boolean = assignmentState.isAddingReplica(localBrokerId)

  def isAddingReplica(replicaId: Int): Boolean = assignmentState.isAddingReplica(replicaId)

  def producerIdCount: Int = log.map(_.producerIdCount).getOrElse(0)

  // Visible for testing
  def removeExpiredProducers(currentTimeMs: Long): Unit = log.foreach(_.removeExpiredProducers(currentTimeMs))

  def inSyncReplicaIds: Set[Int] = partitionState.isr

  def maybeAddListener(listener: PartitionListener): Boolean = {
    inReadLock(leaderIsrUpdateLock) {
      // `log` is set to `None` when the partition is failed or deleted.
      log match {
        case Some(_) =>
          listeners.add(listener)
          true

        case None =>
          false
      }
    }
  }

  def removeListener(listener: PartitionListener): Unit = {
    listeners.remove(listener)
  }

  /**
    * Create the future replica if 1) the current replica is not in the given log directory and 2) the future replica
    * does not exist. This method assumes that the current replica has already been created.
    *
    * @param logDir log directory
    * @param highWatermarkCheckpoints Checkpoint to load initial high watermark from
    * @return true iff the future replica is created
    */
  def maybeCreateFutureReplica(logDir: String, highWatermarkCheckpoints: OffsetCheckpoints, topicId: Option[Uuid] = topicId): Boolean = {
    // The writeLock is needed to make sure that while the caller checks the log directory of the
    // current replica and the existence of the future replica, no other thread can update the log directory of the
    // current replica or remove the future replica.
    inWriteLock(leaderIsrUpdateLock) {
      val currentLogDir = localLogOrException.parentDir
      if (currentLogDir == logDir) {
        info(s"Current log directory $currentLogDir is same as requested log dir $logDir. " +
          s"Skipping future replica creation.")
        false
      } else {
        futureLog match {
          case Some(partitionFutureLog) =>
            val futureLogDir = partitionFutureLog.parentDir
            if (futureLogDir != logDir)
              throw new IllegalStateException(s"The future log dir $futureLogDir of $topicPartition is " +
                s"different from the requested log dir $logDir")
            false
          case None =>
            createLogIfNotExists(isNew = false, isFutureReplica = true, highWatermarkCheckpoints, topicId)
            true
        }
      }
    }
  }

  def createLogIfNotExists(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints, topicId: Option[Uuid],
                           targetLogDirectoryId: Option[Uuid] = None): Unit = {
    def maybeCreate(logOpt: Option[UnifiedLog]): UnifiedLog = {
      logOpt match {
        case Some(log) =>
          trace(s"${if (isFutureReplica) "Future UnifiedLog" else "UnifiedLog"} already exists.")
          if (log.topicId.isEmpty)
            topicId.foreach(log.assignTopicId)
          log
        case None =>
          createLog(isNew, isFutureReplica, offsetCheckpoints, topicId, targetLogDirectoryId)
      }
    }

    if (isFutureReplica) {
      this.futureLog = Some(maybeCreate(this.futureLog))
    } else {
      this.log = Some(maybeCreate(this.log))
    }
  }

  // Visible for testing
  private[cluster] def createLog(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints,
                                 topicId: Option[Uuid], targetLogDirectoryId: Option[Uuid]): UnifiedLog = {
    def updateHighWatermark(log: UnifiedLog): Unit = {
      val checkpointHighWatermark = offsetCheckpoints.fetch(log.parentDir, topicPartition).orElseGet(() => {
        info(s"No checkpointed highwatermark is found for partition $topicPartition")
        0L
      })
      val initialHighWatermark = log.updateHighWatermark(checkpointHighWatermark)
      info(s"Log loaded for partition $topicPartition with initial high watermark $initialHighWatermark")
    }

    logManager.initializingLog(topicPartition)
    var maybeLog: Option[UnifiedLog] = None
    try {
      val log = logManager.getOrCreateLog(topicPartition, isNew, isFutureReplica, topicId, targetLogDirectoryId)
      if (!isFutureReplica) log.setLogOffsetsListener(logOffsetsListener)
      maybeLog = Some(log)
      updateHighWatermark(log)
      log
    } finally {
      logManager.finishedInitializingLog(topicPartition, maybeLog)
    }
  }

  def getReplica(replicaId: Int): Option[Replica] = Option(remoteReplicasMap.get(replicaId))

  private def checkCurrentLeaderEpoch(remoteLeaderEpochOpt: Optional[Integer]): Errors = {
    if (!remoteLeaderEpochOpt.isPresent) {
      Errors.NONE
    } else {
      val remoteLeaderEpoch = remoteLeaderEpochOpt.get
      val localLeaderEpoch = leaderEpoch
      if (localLeaderEpoch > remoteLeaderEpoch)
        Errors.FENCED_LEADER_EPOCH
      else if (localLeaderEpoch < remoteLeaderEpoch)
        Errors.UNKNOWN_LEADER_EPOCH
      else
        Errors.NONE
    }
  }

  private def getLocalLog(currentLeaderEpoch: Optional[Integer],
                          requireLeader: Boolean): Either[UnifiedLog, Errors] = {
    checkCurrentLeaderEpoch(currentLeaderEpoch) match {
      case Errors.NONE =>
        if (requireLeader && !isLeader) {
          Right(Errors.NOT_LEADER_OR_FOLLOWER)
        } else {
          log match {
            case Some(partitionLog) =>
              Left(partitionLog)
            case _ =>
              Right(Errors.NOT_LEADER_OR_FOLLOWER)
          }
        }
      case error =>
        Right(error)
    }
  }

  def localLogOrException: UnifiedLog = log.getOrElse {
    throw new NotLeaderOrFollowerException(s"Log for partition $topicPartition is not available " +
      s"on broker $localBrokerId")
  }

  def futureLocalLogOrException: UnifiedLog = futureLog.getOrElse {
    throw new NotLeaderOrFollowerException(s"Future log for partition $topicPartition is not available " +
      s"on broker $localBrokerId")
  }

  def leaderLogIfLocal: Option[UnifiedLog] = {
    log.filter(_ => isLeader)
  }

  /**
   * Returns true if this node is currently leader for the Partition.
   */
  def isLeader: Boolean = leaderReplicaIdOpt.contains(localBrokerId)

  def leaderIdIfLocal: Option[Int] = {
    leaderReplicaIdOpt.filter(_ == localBrokerId)
  }

  def localLogWithEpochOrThrow(
    currentLeaderEpoch: Optional[Integer],
    requireLeader: Boolean
  ): UnifiedLog = {
    getLocalLog(currentLeaderEpoch, requireLeader) match {
      case Left(localLog) => localLog
      case Right(error) =>
        throw error.exception(s"Failed to find ${if (requireLeader) "leader" else ""} log for " +
          s"partition $topicPartition with leader epoch $currentLeaderEpoch. The current leader " +
          s"is $leaderReplicaIdOpt and the current epoch $leaderEpoch")
    }
  }

  // Visible for testing -- Used by unit tests to set log for this partition
  def setLog(log: UnifiedLog, isFutureLog: Boolean): Unit = {
    if (isFutureLog) {
      futureLog = Some(log)
    } else {
      log.setLogOffsetsListener(logOffsetsListener)
      this.log = Some(log)
    }
  }

  /**
   * Return either the value of _topicId if it is provided or return the topic id attached to the log itself.
   * If _topicId is empty then the method will fetch topicId from the log and update _topicId.
   * @return the topic ID for the log or None if the log or the topic ID does not exist.
   */
  def topicId: Option[Uuid] = {
    if (_topicId.isEmpty || _topicId.contains(Uuid.ZERO_UUID)) {
      _topicId = this.log.orElse(logManager.getLog(topicPartition)).flatMap(_.topicId)
    }
    _topicId
  }

  // remoteReplicas will be called in the hot path, and must be inexpensive
  def remoteReplicas: Iterable[Replica] =
    remoteReplicasMap.values

  def futureReplicaDirChanged(newDestinationDir: String): Boolean = {
    inReadLock(leaderIsrUpdateLock) {
      futureLog.exists(_.parentDir != newDestinationDir)
    }
  }

  def removeFutureLocalReplica(deleteFromLogDir: Boolean = true): Unit = {
    inWriteLock(leaderIsrUpdateLock) {
      futureLog = None
      if (deleteFromLogDir)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }
  }

  // Returns a VerificationGuard if we need to verify. This starts or continues the verification process. Otherwise return the
  // sentinel VerificationGuard.
  def maybeStartTransactionVerification(producerId: Long, sequence: Int, epoch: Short): VerificationGuard = {
    leaderLogIfLocal match {
      case Some(log) => log.maybeStartTransactionVerification(producerId, sequence, epoch)
      case None => throw new NotLeaderOrFollowerException()
    }
  }

  // Return true if the future replica exists and it has caught up with the current replica for this partition
  // Only ReplicaAlterDirThread will call this method and ReplicaAlterDirThread should remove the partition
  // from its partitionStates if this method returns true
  def maybeReplaceCurrentWithFutureReplica(): Boolean = {
    runCallbackIfFutureReplicaCaughtUp((futurePartitionLog: UnifiedLog) => {
      logManager.replaceCurrentWithFutureLog(topicPartition)
      futurePartitionLog.setLogOffsetsListener(logOffsetsListener)
      log = futureLog
      removeFutureLocalReplica(false)
    })
  }

  def runCallbackIfFutureReplicaCaughtUp(callback: UnifiedLog => Unit): Boolean = {
    // lock to prevent the log append by followers while checking if future log caught-up.
    futureLogLock.synchronized {
      val localReplicaLEO = localLogOrException.logEndOffset
      val futureReplicaLEO = futureLog.map(_.logEndOffset)
      if (futureReplicaLEO.contains(localReplicaLEO)) {
        // The write lock is needed to make sure that while ReplicaAlterDirThread checks the LEO of the
        // current replica, no other thread can update LEO of the current replica via log truncation or log append operation.
        inWriteLock(leaderIsrUpdateLock) {
          futureLog match {
            case Some(futurePartitionLog) =>
              if (log.exists(_.logEndOffset == futurePartitionLog.logEndOffset)) {
                callback(futurePartitionLog)
                true
              } else false
            case None =>
              // Future replica is removed by a non-ReplicaAlterLogDirsThread before this method is called
              // In this case the partition should have been removed from state of the ReplicaAlterLogDirsThread
              // Return false so that ReplicaAlterLogDirsThread does not have to remove this partition from the
              // state again to avoid race condition
              false
          }
        }
      } else false
    }
  }

  def futureReplicaDirectoryId(): Option[Uuid] = futureLog.flatMap(log => logManager.directoryId(log.dir.getParent))
  def logDirectoryId(): Option[Uuid] = log.flatMap(log => logManager.directoryId(log.dir.getParent))
  /**
   * Delete the partition. Note that deleting the partition does not delete the underlying logs.
   * The logs are deleted by the ReplicaManager after having deleted the partition.
   */
  def delete(): Unit = {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      clear()
      listeners.forEach { listener =>
        listener.onDeleted(topicPartition)
      }
      listeners.clear()
    }
  }

  /**
   * Fail the partition. This is called by the ReplicaManager when the partition
   * transitions to Offline.
   */
  def markOffline(): Unit = {
    inWriteLock(leaderIsrUpdateLock) {
      clear()

      listeners.forEach { listener =>
        listener.onFailed(topicPartition)
      }
      listeners.clear()
    }
  }

  /**
   * Invoke the partition listeners when the partition has been transitioned to follower.
   */
  def invokeOnBecomingFollowerListeners(): Unit = {
    listeners.forEach { listener =>
      listener.onBecomingFollower(topicPartition)
    }
  }

  private def clear(): Unit = {
    remoteReplicasMap.clear()
    assignmentState = SimpleAssignmentState(Seq.empty)
    log = None
    futureLog = None
    partitionState = CommittedPartitionState(Set.empty, LeaderRecoveryState.RECOVERED)
    leaderReplicaIdOpt = None
    leaderEpochStartOffsetOpt = None
    Partition.removeMetrics(topicPartition)
  }

  def getLeaderEpoch: Int = this.leaderEpoch

  def getPartitionEpoch: Int = this.partitionEpoch

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  def makeLeader(partitionState: LeaderAndIsrPartitionState,
                 highWatermarkCheckpoints: OffsetCheckpoints,
                 topicId: Option[Uuid],
                 targetDirectoryId: Option[Uuid] = None): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      // Partition state changes are expected to have an partition epoch larger or equal
      // to the current partition epoch. The latter is allowed because the partition epoch
      // is also updated by the AlterPartition response so the new epoch might be known
      // before a LeaderAndIsr request is received or before an update is received via
      // the metadata log.
      if (partitionState.partitionEpoch < partitionEpoch) {
        stateChangeLogger.info(s"Skipped the become-leader state change for $topicPartition with topic id $topicId " +
          s"and partition state $partitionState since the leader is already at a newer partition epoch $partitionEpoch.")
        return false
      }

      // Record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path.
      controllerEpoch = partitionState.controllerEpoch

      val currentTimeMs = time.milliseconds
      val isNewLeader = !isLeader
      val isNewLeaderEpoch = partitionState.leaderEpoch > leaderEpoch
      val replicas = partitionState.replicas.asScala.map(_.toInt)
      val isr = partitionState.isr.asScala.map(_.toInt).toSet
      val addingReplicas = partitionState.addingReplicas.asScala.map(_.toInt)
      val removingReplicas = partitionState.removingReplicas.asScala.map(_.toInt)

      if (partitionState.leaderRecoveryState == LeaderRecoveryState.RECOVERING.value) {
        stateChangeLogger.info(s"The topic partition $topicPartition was marked as RECOVERING. " +
          "Marking the topic partition as RECOVERED.")
      }

      // Updating the assignment and ISR state is safe if the partition epoch is
      // larger or equal to the current partition epoch.
      updateAssignmentAndIsr(
        replicas = replicas,
        isLeader = true,
        isr = isr,
        addingReplicas = addingReplicas,
        removingReplicas = removingReplicas,
        LeaderRecoveryState.RECOVERED
      )

      try {
        createLogInAssignedDirectoryId(partitionState, highWatermarkCheckpoints, topicId, targetDirectoryId)
      } catch {
        case e: ZooKeeperClientException =>
          stateChangeLogger.error(s"A ZooKeeper client exception has occurred and makeLeader will be skipping the " +
            s"state change for the partition $topicPartition with leader epoch: $leaderEpoch.", e)
          return false
      }

      val leaderLog = localLogOrException

      // We update the epoch start offset and the replicas' state only if the leader epoch
      // has changed.
      if (isNewLeaderEpoch) {
        val leaderEpochStartOffset = leaderLog.logEndOffset
        stateChangeLogger.info(s"Leader $topicPartition with topic id $topicId starts at " +
          s"leader epoch ${partitionState.leaderEpoch} from offset $leaderEpochStartOffset " +
          s"with partition epoch ${partitionState.partitionEpoch}, high watermark ${leaderLog.highWatermark}, " +
          s"ISR ${isr.mkString("[", ",", "]")}, adding replicas ${addingReplicas.mkString("[", ",", "]")} and " +
          s"removing replicas ${removingReplicas.mkString("[", ",", "]")} ${if (isUnderMinIsr) "(under-min-isr)" else ""}. " +
          s"Previous leader $leaderReplicaIdOpt and previous leader epoch was $leaderEpoch.")

        // In the case of successive leader elections in a short time period, a follower may have
        // entries in its log from a later epoch than any entry in the new leader's log. In order
        // to ensure that these followers can truncate to the right offset, we must cache the new
        // leader epoch and the start offset since it should be larger than any epoch that a follower
        // would try to query.
        leaderLog.maybeAssignEpochStartOffset(partitionState.leaderEpoch, leaderEpochStartOffset)

        // Initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and
        // lastFetchLeaderLogEndOffset.
        remoteReplicas.foreach { replica =>
          replica.resetReplicaState(
            currentTimeMs = currentTimeMs,
            leaderEndOffset = leaderEpochStartOffset,
            isNewLeader = isNewLeader,
            isFollowerInSync = partitionState.isr.contains(replica.brokerId)
          )
        }

        // We update the leader epoch and the leader epoch start offset iff the
        // leader epoch changed.
        leaderEpoch = partitionState.leaderEpoch
        leaderEpochStartOffsetOpt = Some(leaderEpochStartOffset)
      } else {
        stateChangeLogger.info(s"Skipped the become-leader state change for $topicPartition with topic id $topicId " +
          s"and partition state $partitionState since it is already the leader with leader epoch $leaderEpoch. " +
          s"Current high watermark ${leaderLog.highWatermark}, ISR ${isr.mkString("[", ",", "]")}, " +
          s"adding replicas ${addingReplicas.mkString("[", ",", "]")} and " +
          s"removing replicas ${removingReplicas.mkString("[", ",", "]")}.")
      }

      partitionEpoch = partitionState.partitionEpoch
      leaderReplicaIdOpt = Some(localBrokerId)

      // We may need to increment high watermark since ISR could be down to 1.
      (maybeIncrementLeaderHW(leaderLog, currentTimeMs = currentTimeMs), isNewLeader)
    }

    // Some delayed operations may be unblocked after HW changed.
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()

    isNewLeader
  }

  /**
   * Make the local replica the follower by setting the new leader and ISR to empty
   * If the leader replica id does not change and the new epoch is equal or one
   * greater (that is, no updates have been missed), return false to indicate to the
   * replica manager that state is already correct and the become-follower steps can
   * be skipped.
   */
  def makeFollower(partitionState: LeaderAndIsrPartitionState,
                   highWatermarkCheckpoints: OffsetCheckpoints,
                   topicId: Option[Uuid],
                   targetLogDirectoryId: Option[Uuid] = None): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      if (partitionState.partitionEpoch < partitionEpoch) {
        stateChangeLogger.info(s"Skipped the become-follower state change for $topicPartition with topic id $topicId " +
          s"and partition state $partitionState since the follower is already at a newer partition epoch $partitionEpoch.")
        return false
      }

      // Record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionState.controllerEpoch

      val isNewLeaderEpoch = partitionState.leaderEpoch > leaderEpoch
      // The leader should be updated before updateAssignmentAndIsr where we clear the ISR. Or it is possible to meet
      // the under min isr condition during the makeFollower process and emits the wrong metric.
      leaderReplicaIdOpt = Option(partitionState.leader)
      leaderEpoch = partitionState.leaderEpoch
      leaderEpochStartOffsetOpt = None
      partitionEpoch = partitionState.partitionEpoch

      updateAssignmentAndIsr(
        replicas = partitionState.replicas.asScala.iterator.map(_.toInt).toSeq,
        isLeader = false,
        isr = Set.empty,
        addingReplicas = partitionState.addingReplicas.asScala.map(_.toInt),
        removingReplicas = partitionState.removingReplicas.asScala.map(_.toInt),
        LeaderRecoveryState.of(partitionState.leaderRecoveryState)
      )

      try {
        createLogInAssignedDirectoryId(partitionState, highWatermarkCheckpoints, topicId, targetLogDirectoryId)
      } catch {
        case e: ZooKeeperClientException =>
          stateChangeLogger.error(s"A ZooKeeper client exception has occurred. makeFollower will be skipping the " +
            s"state change for the partition $topicPartition with leader epoch: $leaderEpoch.", e)
          return false
      }

      val followerLog = localLogOrException
      if (isNewLeaderEpoch) {
        val leaderEpochEndOffset = followerLog.logEndOffset
        stateChangeLogger.info(s"Follower $topicPartition starts at leader epoch ${partitionState.leaderEpoch} from " +
          s"offset $leaderEpochEndOffset with partition epoch ${partitionState.partitionEpoch} and " +
          s"high watermark ${followerLog.highWatermark}. Current leader is ${partitionState.leader}. " +
          s"Previous leader $leaderReplicaIdOpt and previous leader epoch was $leaderEpoch.")
      } else {
        stateChangeLogger.info(s"Skipped the become-follower state change for $topicPartition with topic id $topicId " +
          s"and partition state $partitionState since it is already a follower with leader epoch $leaderEpoch.")
      }

      // We must restart the fetchers when the leader epoch changed regardless of
      // whether the leader changed as well.
      isNewLeaderEpoch
    }
  }

  private def createLogInAssignedDirectoryId(partitionState: LeaderAndIsrPartitionState, highWatermarkCheckpoints: OffsetCheckpoints, topicId: Option[Uuid], targetLogDirectoryId: Option[Uuid]): Unit = {
    targetLogDirectoryId match {
      case Some(directoryId) =>
        if (logManager.onlineLogDirId(directoryId) || !logManager.hasOfflineLogDirs() || directoryId == DirectoryId.UNASSIGNED) {
          createLogIfNotExists(partitionState.isNew, isFutureReplica = false, highWatermarkCheckpoints, topicId, targetLogDirectoryId)
        } else {
          warn(s"Skipping creation of log because there are potentially offline log " +
            s"directories and log may already exist there. directoryId=$directoryId, " +
            s"topicId=$topicId, targetLogDirectoryId=$targetLogDirectoryId")
        }

      case None =>
        createLogIfNotExists(partitionState.isNew, isFutureReplica = false, highWatermarkCheckpoints, topicId)
    }
  }

  /**
   * Update the follower's state in the leader based on the last fetch request. See
   * [[Replica.updateFetchStateOrThrow()]] for details.
   *
   * This method is visible for performance testing (see `UpdateFollowerFetchStateBenchmark`)
   */
  def updateFollowerFetchState(
    replica: Replica,
    followerFetchOffsetMetadata: LogOffsetMetadata,
    followerStartOffset: Long,
    followerFetchTimeMs: Long,
    leaderEndOffset: Long,
    brokerEpoch: Long
  ): Unit = {
    // No need to calculate low watermark if there is no delayed DeleteRecordsRequest
    val oldLeaderLW = if (delayedOperations.numDelayedDelete > 0) lowWatermarkIfLeader else -1L
    val prevFollowerEndOffset = replica.stateSnapshot.logEndOffset

    // Apply read lock here to avoid the race between ISR updates and the fetch requests from rebooted follower. It
    // could break the broker epoch checks in the ISR expansion.
    inReadLock(leaderIsrUpdateLock) {
      replica.updateFetchStateOrThrow(
        followerFetchOffsetMetadata,
        followerStartOffset,
        followerFetchTimeMs,
        leaderEndOffset,
        brokerEpoch
      )
    }

    val newLeaderLW = if (delayedOperations.numDelayedDelete > 0) lowWatermarkIfLeader else -1L
    // check if the LW of the partition has incremented
    // since the replica's logStartOffset may have incremented
    val leaderLWIncremented = newLeaderLW > oldLeaderLW

    // Check if this in-sync replica needs to be added to the ISR.
    maybeExpandIsr(replica)

    // check if the HW of the partition can now be incremented
    // since the replica may already be in the ISR and its LEO has just incremented
    val leaderHWIncremented = if (prevFollowerEndOffset != replica.stateSnapshot.logEndOffset) {
      // the leader log may be updated by ReplicaAlterLogDirsThread so the following method must be in lock of
      // leaderIsrUpdateLock to prevent adding new hw to invalid log.
      inReadLock(leaderIsrUpdateLock) {
        leaderLogIfLocal.exists(leaderLog => maybeIncrementLeaderHW(leaderLog, followerFetchTimeMs))
      }
    } else {
      false
    }

    // some delayed operations may be unblocked after HW or LW changed
    if (leaderLWIncremented || leaderHWIncremented)
      tryCompleteDelayedRequests()

    debug(s"Recorded replica ${replica.brokerId} log end offset (LEO) position " +
      s"${followerFetchOffsetMetadata.messageOffset} and log start offset $followerStartOffset.")
  }

  /**
   * Stores the topic partition assignment and ISR.
   * It creates a new Replica object for any new remote broker. The isr parameter is
   * expected to be a subset of the assignment parameter.
   *
   * Note: public visibility for tests.
   *
   * @param replicas An ordered sequence of all the broker ids that were assigned to this
   *                   topic partition
   * @param isLeader True if this replica is the leader.
   * @param isr The set of broker ids that are known to be insync with the leader
   * @param addingReplicas An ordered sequence of all broker ids that will be added to the
    *                       assignment
   * @param removingReplicas An ordered sequence of all broker ids that will be removed from
    *                         the assignment
   */
  def updateAssignmentAndIsr(
    replicas: Seq[Int],
    isLeader: Boolean,
    isr: Set[Int],
    addingReplicas: Seq[Int],
    removingReplicas: Seq[Int],
    leaderRecoveryState: LeaderRecoveryState
  ): Unit = {
    if (isLeader) {
      val followers = replicas.filter(_ != localBrokerId)
      val removedReplicas = remoteReplicasMap.keys.filterNot(followers.contains(_))

      // Due to code paths accessing remoteReplicasMap without a lock,
      // first add the new replicas and then remove the old ones.
      followers.foreach(id => remoteReplicasMap.getAndMaybePut(id, new Replica(id, topicPartition, metadataCache)))
      remoteReplicasMap.removeAll(removedReplicas)
    } else {
      remoteReplicasMap.clear()
    }

    assignmentState = if (addingReplicas.nonEmpty || removingReplicas.nonEmpty)
      OngoingReassignmentState(addingReplicas, removingReplicas, replicas)
    else
      SimpleAssignmentState(replicas)

    partitionState = CommittedPartitionState(isr, leaderRecoveryState)
  }

  /**
   * Check and maybe expand the ISR of the partition.
   * A replica will be added to ISR if its LEO >= current hw of the partition and it is caught up to
   * an offset within the current leader epoch. A replica must be caught up to the current leader
   * epoch before it can join ISR, because otherwise, if there is committed data between current
   * leader's HW and LEO, the replica may become the leader before it fetches the committed data
   * and the data will be lost.
   *
   * Technically, a replica shouldn't be in ISR if it hasn't caught up for longer than replicaLagTimeMaxMs,
   * even if its log end offset is >= HW. However, to be consistent with how the follower determines
   * whether a replica is in-sync, we only check HW.
   *
   * This function can be triggered when a replica's LEO has incremented.
   */
  private def maybeExpandIsr(followerReplica: Replica): Unit = {
    val needsIsrUpdate = !partitionState.isInflight && canAddReplicaToIsr(followerReplica.brokerId) && inReadLock(leaderIsrUpdateLock) {
      needsExpandIsr(followerReplica)
    }
    if (needsIsrUpdate) {
      val alterIsrUpdateOpt = inWriteLock(leaderIsrUpdateLock) {
        // check if this replica needs to be added to the ISR
        partitionState match {
          case currentState: CommittedPartitionState if needsExpandIsr(followerReplica) =>
            Some(prepareIsrExpand(currentState, followerReplica.brokerId))
          case _ =>
            None
        }
      }
      // Send the AlterPartition request outside of the LeaderAndIsr lock since the completion logic
      // may increment the high watermark (and consequently complete delayed operations).
      alterIsrUpdateOpt.foreach(submitAlterPartition)
    }
  }

  private def needsExpandIsr(followerReplica: Replica): Boolean = {
    canAddReplicaToIsr(followerReplica.brokerId) && isFollowerInSync(followerReplica)
  }

  private def canAddReplicaToIsr(followerReplicaId: Int): Boolean = {
    val current = partitionState
    !current.isInflight &&
      !current.isr.contains(followerReplicaId) &&
      isReplicaIsrEligible(followerReplicaId)
  }

  private def isFollowerInSync(followerReplica: Replica): Boolean = {
    leaderLogIfLocal.exists { leaderLog =>
      val followerEndOffset = followerReplica.stateSnapshot.logEndOffset
      followerEndOffset >= leaderLog.highWatermark && leaderEpochStartOffsetOpt.exists(followerEndOffset >= _)
    }
  }

  private def isReplicaIsrEligible(followerReplicaId: Int): Boolean = {
    metadataCache match {
      // In KRaft mode, only a replica which meets all of the following requirements is allowed to join the ISR.
      // 1. It is not fenced.
      // 2. It is not in controlled shutdown.
      // 3. Its metadata cached broker epoch matches its Fetch request broker epoch. Or the Fetch
      //    request broker epoch is -1 which bypasses the epoch verification.
      case kRaftMetadataCache: KRaftMetadataCache =>
        val mayBeReplica = getReplica(followerReplicaId)
        // The topic is already deleted and we don't have any replica information. In this case, we can return false
        // so as to avoid NPE
        if (mayBeReplica.isEmpty) {
          warn(s"The replica state of replica ID:[$followerReplicaId] doesn't exist in the leader node. It might because the topic is already deleted.")
          return false
        }
        val storedBrokerEpoch = mayBeReplica.get.stateSnapshot.brokerEpoch
        val cachedBrokerEpoch = kRaftMetadataCache.getAliveBrokerEpoch(followerReplicaId)
        !kRaftMetadataCache.isBrokerFenced(followerReplicaId) &&
          !kRaftMetadataCache.isBrokerShuttingDown(followerReplicaId) &&
          isBrokerEpochIsrEligible(storedBrokerEpoch, cachedBrokerEpoch)

      // In ZK mode, we just ensure the broker is alive. Although we do not check for shutting down brokers here,
      // the controller will block them from being added to ISR.
      case zkMetadataCache: ZkMetadataCache =>
        zkMetadataCache.hasAliveBroker(followerReplicaId)

      case _ => true
    }
  }

  private def isBrokerEpochIsrEligible(storedBrokerEpoch: Option[Long], cachedBrokerEpoch: Option[Long]): Boolean = {
    storedBrokerEpoch.isDefined && cachedBrokerEpoch.isDefined &&
      (storedBrokerEpoch.get == -1 || storedBrokerEpoch == cachedBrokerEpoch)
  }

  /*
   * Returns a tuple where the first element is a boolean indicating whether enough replicas reached `requiredOffset`
   * and the second element is an error (which would be `Errors.NONE` for no error).
   *
   * Note that this method will only be called if requiredAcks = -1 and we are waiting for all replicas in ISR to be
   * fully caught up to the (local) leader's offset corresponding to this produce request before we acknowledge the
   * produce request.
   */
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Errors) = {
    leaderLogIfLocal match {
      case Some(leaderLog) =>
        // keep the current immutable replica list reference
        val curMaximalIsr = partitionState.maximalIsr

        if (isTraceEnabled) {
          def logEndOffsetString: ((Int, Long)) => String = {
            case (brokerId, logEndOffset) => s"broker $brokerId: $logEndOffset"
          }

          val curInSyncReplicaObjects = (curMaximalIsr - localBrokerId).flatMap(getReplica)
          val replicaInfo = curInSyncReplicaObjects.map(replica => (replica.brokerId, replica.stateSnapshot.logEndOffset))
          val localLogInfo = (localBrokerId, localLogOrException.logEndOffset)
          val (ackedReplicas, awaitingReplicas) = (replicaInfo + localLogInfo).partition { _._2 >= requiredOffset}

          trace(s"Progress awaiting ISR acks for offset $requiredOffset: " +
            s"acked: ${ackedReplicas.map(logEndOffsetString)}, " +
            s"awaiting ${awaitingReplicas.map(logEndOffsetString)}")
        }

        val minIsr = effectiveMinIsr(leaderLog)
        if (leaderLog.highWatermark >= requiredOffset) {
          /*
           * The topic may be configured not to accept messages if there are not enough replicas in ISR
           * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
           */
          if (minIsr <= curMaximalIsr.size)
            (true, Errors.NONE)
          else
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
        } else
          (false, Errors.NONE)
      case None =>
        (false, Errors.NOT_LEADER_OR_FOLLOWER)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   *
   * 1. Partition ISR changed
   * 2. Any replica's LEO changed
   *
   * The HW is determined by the smallest log end offset among all replicas that are in sync; or are considered caught-up
   * and are allowed to join the ISR. This way, if a replica is considered caught-up, but its log end offset is smaller
   * than HW, we will wait for this replica to catch up to the HW before advancing the HW. This helps the situation when
   * the ISR only includes the leader replica and a follower tries to catch up. If we don't wait for the follower when
   * advancing the HW, the follower's log end offset may keep falling behind the HW (determined by the leader's log end
   * offset) and therefore will never be added to ISR.
   *
   * The HW can only advance if the ISR size is equal or large than the min ISR(min.insync.replicas).
   *
   * With the addition of AlterPartition, we also consider newly added replicas as part of the ISR when advancing
   * the HW. These replicas have not yet been committed to the ISR by the controller, so we could revert to the previously
   * committed ISR. However, adding additional replicas to the ISR makes it more restrictive and therefore safe. We call
   * this set the "maximal" ISR. See KIP-497 for more details
   *
   * Note There is no need to acquire the leaderIsrUpdate lock here since all callers of this private API acquire that lock
   *
   * @return true if the HW was incremented, and false otherwise.
   */
  private def maybeIncrementLeaderHW(leaderLog: UnifiedLog, currentTimeMs: Long = time.milliseconds): Boolean = {
    if (isUnderMinIsr) {
      trace(s"Not increasing HWM because partition is under min ISR(ISR=${partitionState.isr}")
      return false
    }
    // maybeIncrementLeaderHW is in the hot path, the following code is written to
    // avoid unnecessary collection generation
    val leaderLogEndOffset = leaderLog.logEndOffsetMetadata
    var newHighWatermark = leaderLogEndOffset
    remoteReplicasMap.values.foreach { replica =>
      val replicaState = replica.stateSnapshot

      def shouldWaitForReplicaToJoinIsr: Boolean = {
        replicaState.isCaughtUp(leaderLogEndOffset.messageOffset, currentTimeMs, replicaLagTimeMaxMs) &&
        isReplicaIsrEligible(replica.brokerId)
      }

      // Note here we are using the "maximal", see explanation above
      if (replicaState.logEndOffsetMetadata.messageOffset < newHighWatermark.messageOffset &&
          (partitionState.maximalIsr.contains(replica.brokerId) || shouldWaitForReplicaToJoinIsr)
      ) {
        newHighWatermark = replicaState.logEndOffsetMetadata
      }
    }

    leaderLog.maybeIncrementHighWatermark(newHighWatermark) match {
      case Some(oldHighWatermark) =>
        debug(s"High watermark updated from $oldHighWatermark to $newHighWatermark")
        true

      case None =>
        def logEndOffsetString: ((Int, LogOffsetMetadata)) => String = {
          case (brokerId, logEndOffsetMetadata) => s"replica $brokerId: $logEndOffsetMetadata"
        }

        if (isTraceEnabled) {
          val replicaInfo = remoteReplicas.map(replica => (replica.brokerId, replica.stateSnapshot.logEndOffsetMetadata)).toSet
          val localLogInfo = (localBrokerId, localLogOrException.logEndOffsetMetadata)
          trace(s"Skipping update high watermark since new hw $newHighWatermark is not larger than old value. " +
            s"All current LEOs are ${(replicaInfo + localLogInfo).map(logEndOffsetString)}")
        }
        false
    }
  }

  /**
   * The low watermark offset value, calculated only if the local replica is the partition leader
   * It is only used by leader broker to decide when DeleteRecordsRequest is satisfied. Its value is minimum logStartOffset of all live replicas
   * Low watermark will increase when the leader broker receives either FetchRequest or DeleteRecordsRequest.
   */
  def lowWatermarkIfLeader: Long = {
    if (!isLeader)
      throw new NotLeaderOrFollowerException(s"Leader not local for partition $topicPartition on broker $localBrokerId")

    // lowWatermarkIfLeader may be called many times when a DeleteRecordsRequest is outstanding,
    // care has been taken to avoid generating unnecessary collections in this code
    var lowWaterMark = localLogOrException.logStartOffset
    remoteReplicas.foreach { replica =>
      val logStartOffset = replica.stateSnapshot.logStartOffset
      if (metadataCache.hasAliveBroker(replica.brokerId) && logStartOffset < lowWaterMark) {
        lowWaterMark = logStartOffset
      }
    }

    futureLog match {
      case Some(partitionFutureLog) =>
        Math.min(lowWaterMark, partitionFutureLog.logStartOffset)
      case None =>
        lowWaterMark
    }
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  def tryCompleteDelayedRequests(): Unit = {
    delayedOperations.checkAndCompleteAll()
  }

  def maybeShrinkIsr(): Unit = {
    def needsIsrUpdate: Boolean = {
      !partitionState.isInflight && inReadLock(leaderIsrUpdateLock) {
        needsShrinkIsr()
      }
    }

    if (needsIsrUpdate) {
      val alterIsrUpdateOpt = inWriteLock(leaderIsrUpdateLock) {
        leaderLogIfLocal.flatMap { leaderLog =>
          val outOfSyncReplicaIds = getOutOfSyncReplicas(replicaLagTimeMaxMs)
          partitionState match {
            case currentState: CommittedPartitionState if outOfSyncReplicaIds.nonEmpty =>
              val outOfSyncReplicaLog = outOfSyncReplicaIds.map { replicaId =>
                val replicaStateSnapshot = getReplica(replicaId).map(_.stateSnapshot)
                val logEndOffsetMessage = replicaStateSnapshot
                  .map(_.logEndOffset.toString)
                  .getOrElse("unknown")
                val lastCaughtUpTimeMessage = replicaStateSnapshot
                  .map(_.lastCaughtUpTimeMs.toString)
                  .getOrElse("unknown")
                s"(brokerId: $replicaId, endOffset: $logEndOffsetMessage, lastCaughtUpTimeMs: $lastCaughtUpTimeMessage)"
              }.mkString(" ")
              val newIsrLog = (partitionState.isr -- outOfSyncReplicaIds).mkString(",")
              info(s"Shrinking ISR from ${partitionState.isr.mkString(",")} to $newIsrLog. " +
                s"Leader: (highWatermark: ${leaderLog.highWatermark}, " +
                s"endOffset: ${leaderLog.logEndOffset}). " +
                s"Out of sync replicas: $outOfSyncReplicaLog.")
              Some(prepareIsrShrink(currentState, outOfSyncReplicaIds))
            case _ =>
              None
          }
        }
      }
      // Send the AlterPartition request outside of the LeaderAndIsr lock since the completion logic
      // may increment the high watermark (and consequently complete delayed operations).
      alterIsrUpdateOpt.foreach(submitAlterPartition)
    }
  }

  private def needsShrinkIsr(): Boolean = {
    leaderLogIfLocal.exists { _ => getOutOfSyncReplicas(replicaLagTimeMaxMs).nonEmpty }
  }

  private def isFollowerOutOfSync(replicaId: Int,
                                  leaderEndOffset: Long,
                                  currentTimeMs: Long,
                                  maxLagMs: Long): Boolean = {
    getReplica(replicaId).fold(true) { followerReplica =>
      !followerReplica.stateSnapshot.isCaughtUp(leaderEndOffset, currentTimeMs, maxLagMs)
    }
  }

  /**
   * If the follower already has the same leo as the leader, it will not be considered as out-of-sync,
   * otherwise there are two cases that will be handled here -
   * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms,
   *                     the follower is stuck and should be removed from the ISR
   * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
   *                    then the follower is lagging and should be removed from the ISR
   * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
   * the last time when the replica was fully caught up. If either of the above conditions
   * is violated, that replica is considered to be out of sync
   *
   * If an ISR update is in-flight, we will return an empty set here
   **/
  def getOutOfSyncReplicas(maxLagMs: Long): Set[Int] = {
    val current = partitionState
    if (!current.isInflight) {
      val candidateReplicaIds = current.isr - localBrokerId
      val currentTimeMs = time.milliseconds()
      val leaderEndOffset = localLogOrException.logEndOffset
      candidateReplicaIds.filter(replicaId => isFollowerOutOfSync(replicaId, leaderEndOffset, currentTimeMs, maxLagMs))
    } else {
      Set.empty
    }
  }

  private def doAppendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Option[LogAppendInfo] = {
    if (isFuture) {
      // The read lock is needed to handle race condition if request handler thread tries to
      // remove future replica after receiving AlterReplicaLogDirsRequest.
      inReadLock(leaderIsrUpdateLock) {
        // Note the replica may be undefined if it is removed by a non-ReplicaAlterLogDirsThread before
        // this method is called
        futureLog.map { _.appendAsFollower(records) }
      }
    } else {
      // The lock is needed to prevent the follower replica from being updated while ReplicaAlterDirThread
      // is executing maybeReplaceCurrentWithFutureReplica() to replace follower replica with the future replica.
      futureLogLock.synchronized {
        Some(localLogOrException.appendAsFollower(records))
      }
    }
  }

  def appendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Option[LogAppendInfo] = {
    try {
      doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
    } catch {
      case e: UnexpectedAppendOffsetException =>
        val log = if (isFuture) futureLocalLogOrException else localLogOrException
        val logEndOffset = log.logEndOffset
        if (logEndOffset == log.logStartOffset &&
            e.firstOffset < logEndOffset && e.lastOffset >= logEndOffset) {
          // This may happen if the log start offset on the leader (or current replica) falls in
          // the middle of the batch due to delete records request and the follower tries to
          // fetch its first offset from the leader.
          // We handle this case here instead of Log#append() because we will need to remove the
          // segment that start with log start offset and create a new one with earlier offset
          // (base offset of the batch), which will move recoveryPoint backwards, so we will need
          // to checkpoint the new recovery point before we append
          val replicaName = if (isFuture) "future replica" else "follower"
          info(s"Unexpected offset in append to $topicPartition. First offset ${e.firstOffset} is less than log start offset ${log.logStartOffset}." +
               s" Since this is the first record to be appended to the $replicaName's log, will start the log from offset ${e.firstOffset}.")
          truncateFullyAndStartAt(e.firstOffset, isFuture)
          doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
        } else
          throw e
    }
  }

  def appendRecordsToLeader(records: MemoryRecords, origin: AppendOrigin, requiredAcks: Int,
                            requestLocal: RequestLocal, verificationGuard: VerificationGuard = VerificationGuard.SENTINEL): LogAppendInfo = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      leaderLogIfLocal match {
        case Some(leaderLog) =>
          val minIsr = effectiveMinIsr(leaderLog)
          val inSyncSize = partitionState.isr.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException(s"The size of the current ISR ${partitionState.isr} " +
              s"is insufficient to satisfy the min.isr requirement of $minIsr for partition $topicPartition")
          }

          val info = leaderLog.appendAsLeader(records, leaderEpoch = this.leaderEpoch, origin,
            interBrokerProtocolVersion, requestLocal, verificationGuard)

          // we may need to increment high watermark since ISR could be down to 1
          (info, maybeIncrementLeaderHW(leaderLog))

        case None =>
          throw new NotLeaderOrFollowerException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }

    info.copy(if (leaderHWIncremented) LeaderHwChange.INCREASED else LeaderHwChange.SAME)
  }

  /**
   * Fetch records from the partition.
   *
   * @param fetchParams parameters of the corresponding `Fetch` request
   * @param fetchPartitionData partition-level parameters of the `Fetch` (e.g. the fetch offset)
   * @param fetchTimeMs current time in milliseconds on the broker of this fetch request
   * @param maxBytes the maximum bytes to return
   * @param minOneMessage whether to ensure that at least one complete message is returned
   * @param updateFetchState true if the Fetch should update replica state (only applies to follower fetches)
   * @return [[LogReadInfo]] containing the fetched records or the diverging epoch if present
   * @throws NotLeaderOrFollowerException if this node is not the current leader and `FetchParams.fetchOnlyLeader`
   *                                      is enabled, or if this is a follower fetch with an older request version
   *                                      and the replicaId is not recognized among the current valid replicas
   * @throws FencedLeaderEpochException if the leader epoch in the `Fetch` request is lower than the current
   *                                    leader epoch
   * @throws UnknownLeaderEpochException if the leader epoch in the `Fetch` request is higher than the current
   *                                     leader epoch, or if this is a follower fetch and the replicaId is not
   *                                     recognized among the current valid replicas
   * @throws OffsetOutOfRangeException if the fetch offset is smaller than the log start offset or larger than
   *                                   the log end offset (or high watermark depending on `FetchParams.isolation`),
   *                                   or if the end offset for the last fetched epoch in [[FetchRequest.PartitionData]]
   *                                   cannot be determined from the local epoch cache (e.g. if it is larger than
   *                                   any cached epoch value)
   */
  def fetchRecords(
    fetchParams: FetchParams,
    fetchPartitionData: FetchRequest.PartitionData,
    fetchTimeMs: Long,
    maxBytes: Int,
    minOneMessage: Boolean,
    updateFetchState: Boolean
  ): LogReadInfo = {
    def readFromLocalLog(log: UnifiedLog): LogReadInfo = {
      readRecords(
        log,
        fetchPartitionData.lastFetchedEpoch,
        fetchPartitionData.fetchOffset,
        fetchPartitionData.currentLeaderEpoch,
        maxBytes,
        fetchParams.isolation,
        minOneMessage
      )
    }

    if (fetchParams.isFromFollower) {
      // Check that the request is from a valid replica before doing the read
      val (replica, logReadInfo) = inReadLock(leaderIsrUpdateLock) {
        val localLog = localLogWithEpochOrThrow(
          fetchPartitionData.currentLeaderEpoch,
          fetchParams.fetchOnlyLeader
        )
        val replica = followerReplicaOrThrow(
          fetchParams.replicaId,
          fetchPartitionData
        )
        val logReadInfo = readFromLocalLog(localLog)
        (replica, logReadInfo)
      }

      if (updateFetchState && !logReadInfo.divergingEpoch.isPresent) {
        updateFollowerFetchState(
          replica,
          followerFetchOffsetMetadata = logReadInfo.fetchedData.fetchOffsetMetadata,
          followerStartOffset = fetchPartitionData.logStartOffset,
          followerFetchTimeMs = fetchTimeMs,
          leaderEndOffset = logReadInfo.logEndOffset,
          fetchParams.replicaEpoch
        )
      }

      logReadInfo
    } else {
      inReadLock(leaderIsrUpdateLock) {
        val localLog = localLogWithEpochOrThrow(
          fetchPartitionData.currentLeaderEpoch,
          fetchParams.fetchOnlyLeader
        )
        readFromLocalLog(localLog)
      }
    }
  }

  private def followerReplicaOrThrow(
    replicaId: Int,
    fetchPartitionData: FetchRequest.PartitionData
  ): Replica = {
    getReplica(replicaId).getOrElse {
      debug(s"Leader $localBrokerId failed to record follower $replicaId's position " +
        s"${fetchPartitionData.fetchOffset}, and last sent high watermark since the replica is " +
        s"not recognized to be one of the assigned replicas ${assignmentState.replicas.mkString(",")} " +
        s"for leader epoch $leaderEpoch with partition epoch $partitionEpoch")

      val error = if (fetchPartitionData.currentLeaderEpoch.isPresent) {
        // The leader epoch is present in the request and matches the local epoch, but
        // the replica is not in the replica set. This case is possible in KRaft,
        // for example, when new replicas are added as part of a reassignment.
        // We return UNKNOWN_LEADER_EPOCH to signify that the tuple (replicaId, leaderEpoch)
        // is not yet recognized as valid, which causes the follower to retry.
        Errors.UNKNOWN_LEADER_EPOCH
      } else {
        // The request has no leader epoch, which means it is an older version. We cannot
        // say if the follower's state is stale or the local state is. In this case, we
        // return `NOT_LEADER_OR_FOLLOWER` for lack of a better error so that the follower
        // will retry.
        Errors.NOT_LEADER_OR_FOLLOWER
      }

      throw error.exception(s"Replica $replicaId is not recognized as a " +
        s"valid replica of $topicPartition in leader epoch $leaderEpoch with " +
        s"partition epoch $partitionEpoch")
    }
  }

  private def readRecords(
    localLog: UnifiedLog,
    lastFetchedEpoch: Optional[Integer],
    fetchOffset: Long,
    currentLeaderEpoch: Optional[Integer],
    maxBytes: Int,
    fetchIsolation: FetchIsolation,
    minOneMessage: Boolean
  ): LogReadInfo = {
    // Note we use the log end offset prior to the read. This ensures that any appends following
    // the fetch do not prevent a follower from coming into sync.
    val initialHighWatermark = localLog.highWatermark
    val initialLogStartOffset = localLog.logStartOffset
    val initialLogEndOffset = localLog.logEndOffset
    val initialLastStableOffset = localLog.lastStableOffset

    lastFetchedEpoch.ifPresent { fetchEpoch =>
      val epochEndOffset = lastOffsetForLeaderEpoch(currentLeaderEpoch, fetchEpoch, fetchOnlyFromLeader = false)
      val error = Errors.forCode(epochEndOffset.errorCode)
      if (error != Errors.NONE) {
        throw error.exception()
      }

      if (epochEndOffset.endOffset == UNDEFINED_EPOCH_OFFSET || epochEndOffset.leaderEpoch == UNDEFINED_EPOCH) {
        throw new OffsetOutOfRangeException("Could not determine the end offset of the last fetched epoch " +
          s"$lastFetchedEpoch from the request")
      }

      // If fetch offset is less than log start, fail with OffsetOutOfRangeException, regardless of whether epochs are diverging
      if (fetchOffset < initialLogStartOffset) {
        throw new OffsetOutOfRangeException(s"Received request for offset $fetchOffset for partition $topicPartition, " +
          s"but we only have log segments in the range $initialLogStartOffset to $initialLogEndOffset.")
      }

      if (epochEndOffset.leaderEpoch < fetchEpoch || epochEndOffset.endOffset < fetchOffset) {
        val divergingEpoch = new FetchResponseData.EpochEndOffset()
          .setEpoch(epochEndOffset.leaderEpoch)
          .setEndOffset(epochEndOffset.endOffset)

        return new LogReadInfo(
          FetchDataInfo.empty(fetchOffset),
          Optional.of(divergingEpoch),
          initialHighWatermark,
          initialLogStartOffset,
          initialLogEndOffset,
          initialLastStableOffset)
      }
    }

    val fetchedData = localLog.read(
      fetchOffset,
      maxBytes,
      fetchIsolation,
      minOneMessage
    )

    new LogReadInfo(
      fetchedData,
      Optional.empty(),
      initialHighWatermark,
      initialLogStartOffset,
      initialLogEndOffset,
      initialLastStableOffset
    )
  }

  def fetchOffsetForTimestamp(timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean,
                              remoteLogManager: Option[RemoteLogManager] = None): OffsetResultHolder = inReadLock(leaderIsrUpdateLock) {
    // decide whether to only fetch from leader
    val localLog = localLogWithEpochOrThrow(currentLeaderEpoch, fetchOnlyFromLeader)

    val lastFetchableOffset = isolationLevel match {
      case Some(IsolationLevel.READ_COMMITTED) => localLog.lastStableOffset
      case Some(IsolationLevel.READ_UNCOMMITTED) => localLog.highWatermark
      case None => localLog.logEndOffset
    }

    val epochLogString = if (currentLeaderEpoch.isPresent) {
      s"epoch ${currentLeaderEpoch.get}"
    } else {
      "unknown epoch"
    }

    // Only consider throwing an error if we get a client request (isolationLevel is defined) and the high watermark
    // is lagging behind the start offset
    val maybeOffsetsError: Option[ApiException] = leaderEpochStartOffsetOpt
      .filter(epochStart => isolationLevel.isDefined && epochStart > localLog.highWatermark)
      .map(epochStart => Errors.OFFSET_NOT_AVAILABLE.exception(s"Failed to fetch offsets for " +
        s"partition $topicPartition with leader $epochLogString as this partition's " +
        s"high watermark (${localLog.highWatermark}) is lagging behind the " +
        s"start offset from the beginning of this epoch ($epochStart)."))

    def getOffsetByTimestamp: OffsetResultHolder = {
      logManager.getLog(topicPartition)
        .map(log => log.fetchOffsetByTimestamp(timestamp, remoteLogManager))
        .getOrElse(OffsetResultHolder(timestampAndOffsetOpt = None))
    }

    // If we're in the lagging HW state after a leader election, throw OffsetNotAvailable for "latest" offset
    // or for a timestamp lookup that is beyond the last fetchable offset.
    timestamp match {
      case ListOffsetsRequest.LATEST_TIMESTAMP =>
        maybeOffsetsError.map(e => throw e)
          .getOrElse(OffsetResultHolder(Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, lastFetchableOffset, Optional.of(leaderEpoch)))))
      case ListOffsetsRequest.EARLIEST_TIMESTAMP | ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP =>
        getOffsetByTimestamp
      case _ =>
        val offsetResultHolder = getOffsetByTimestamp
        offsetResultHolder.maybeOffsetsError = maybeOffsetsError
        offsetResultHolder.lastFetchableOffset = Some(lastFetchableOffset)
        offsetResultHolder
    }
  }

  def activeProducerState: DescribeProducersResponseData.PartitionResponse = {
    val producerState = new DescribeProducersResponseData.PartitionResponse()
      .setPartitionIndex(topicPartition.partition())

    log.map(_.activeProducers) match {
      case Some(producers) =>
        producerState
          .setErrorCode(Errors.NONE.code)
          .setActiveProducers(producers.asJava)
      case None =>
        producerState
          .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)
    }

    producerState
  }

  def fetchOffsetSnapshot(currentLeaderEpoch: Optional[Integer],
                          fetchOnlyFromLeader: Boolean): LogOffsetSnapshot = inReadLock(leaderIsrUpdateLock) {
    // decide whether to only fetch from leader
    val localLog = localLogWithEpochOrThrow(currentLeaderEpoch, fetchOnlyFromLeader)
    localLog.fetchOffsetSnapshot
  }

  def legacyFetchOffsetsForTimestamp(timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long] = inReadLock(leaderIsrUpdateLock) {
    val localLog = localLogWithEpochOrThrow(Optional.empty(), fetchOnlyFromLeader)
    val allOffsets = localLog.legacyFetchOffsetsBefore(timestamp, maxNumOffsets)

    if (!isFromConsumer) {
      allOffsets
    } else {
      val hw = localLog.highWatermark
      if (allOffsets.exists(_ > hw))
        hw +: allOffsets.dropWhile(_ > hw)
      else
        allOffsets
    }
  }

  def logStartOffset: Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderLogIfLocal.map(_.logStartOffset).getOrElse(-1)
    }
  }

  /**
   * Update logStartOffset and low watermark if 1) offset <= highWatermark and 2) it is the leader replica.
   * This function can trigger log segment deletion and log rolling.
   *
   * Return low watermark of the partition.
   */
  def deleteRecordsOnLeader(offset: Long): LogDeleteRecordsResult = inReadLock(leaderIsrUpdateLock) {
    leaderLogIfLocal match {
      case Some(leaderLog) =>
        if (!leaderLog.config.delete)
          throw new PolicyViolationException(s"Records of partition $topicPartition can not be deleted due to the configured policy")

        val convertedOffset = if (offset == DeleteRecordsRequest.HIGH_WATERMARK)
          leaderLog.highWatermark
        else
          offset

        if (convertedOffset < 0)
          throw new OffsetOutOfRangeException(s"The offset $convertedOffset for partition $topicPartition is not valid")

        leaderLog.maybeIncrementLogStartOffset(convertedOffset, LogStartOffsetIncrementReason.ClientRecordDeletion)
        LogDeleteRecordsResult(
          requestedOffset = convertedOffset,
          lowWatermark = lowWatermarkIfLeader)
      case None =>
        throw new NotLeaderOrFollowerException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
    }
  }

  /**
    * Truncate the local log of this partition to the specified offset and checkpoint the recovery point to this offset
    *
    * @param offset offset to be used for truncation
    * @param isFuture True iff the truncation should be performed on the future log of this partition
    */
  def truncateTo(offset: Long, isFuture: Boolean): Unit = {
    // The read lock is needed to prevent the follower replica from being truncated while ReplicaAlterDirThread
    // is executing maybeReplaceCurrentWithFutureReplica() to replace follower replica with the future replica.
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateTo(Map(topicPartition -> offset), isFuture = isFuture)
    }
  }

  /**
    * Delete all data in the local log of this partition and start the log at the new offset
    *
    * @param newOffset The new offset to start the log with
    * @param isFuture True iff the truncation should be performed on the future log of this partition
    * @param logStartOffsetOpt The log start offset to set for the log. If None, the new offset will be used.
    */
  def truncateFullyAndStartAt(newOffset: Long,
                              isFuture: Boolean,
                              logStartOffsetOpt: Option[Long] = None): Unit = {
    // The read lock is needed to prevent the follower replica from being truncated while ReplicaAlterDirThread
    // is executing maybeReplaceCurrentWithFutureReplica() to replace follower replica with the future replica.
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateFullyAndStartAt(topicPartition, newOffset, isFuture = isFuture, logStartOffsetOpt)
    }
  }

  /**
   * Find the (exclusive) last offset of the largest epoch less than or equal to the requested epoch.
   *
   * @param currentLeaderEpoch The expected epoch of the current leader (if known)
   * @param leaderEpoch Requested leader epoch
   * @param fetchOnlyFromLeader Whether or not to require servicing only from the leader
   *
   * @return The requested leader epoch and the end offset of this leader epoch, or if the requested
   *         leader epoch is unknown, the leader epoch less than the requested leader epoch and the end offset
   *         of this leader epoch. The end offset of a leader epoch is defined as the start
   *         offset of the first leader epoch larger than the leader epoch, or else the log end
   *         offset if the leader epoch is the latest leader epoch.
   */
  def lastOffsetForLeaderEpoch(currentLeaderEpoch: Optional[Integer],
                               leaderEpoch: Int,
                               fetchOnlyFromLeader: Boolean): EpochEndOffset = {
    inReadLock(leaderIsrUpdateLock) {
      val localLogOrError = getLocalLog(currentLeaderEpoch, fetchOnlyFromLeader)
      localLogOrError match {
        case Left(localLog) =>
          localLog.endOffsetForEpoch(leaderEpoch) match {
            case Some(epochAndOffset) => new EpochEndOffset()
              .setPartition(partitionId)
              .setErrorCode(Errors.NONE.code)
              .setLeaderEpoch(epochAndOffset.leaderEpoch)
              .setEndOffset(epochAndOffset.offset)
            case None => new EpochEndOffset()
              .setPartition(partitionId)
              .setErrorCode(Errors.NONE.code)
          }
        case Right(error) => new EpochEndOffset()
          .setPartition(partitionId)
          .setErrorCode(error.code)
      }
    }
  }

  private def prepareIsrExpand(
    currentState: CommittedPartitionState,
    newInSyncReplicaId: Int
  ): PendingExpandIsr = {
    // When expanding the ISR, we assume that the new replica will make it into the ISR
    // before we receive confirmation that it has. This ensures that the HW will already
    // reflect the updated ISR even if there is a delay before we receive the confirmation.
    // Alternatively, if the update fails, no harm is done since the expanded ISR puts
    // a stricter requirement for advancement of the HW.
    val isrToSend = partitionState.isr + newInSyncReplicaId
    val isrWithBrokerEpoch = addBrokerEpochToIsr(isrToSend.toList).asJava
    val newLeaderAndIsr = new LeaderAndIsr(
      localBrokerId,
      leaderEpoch,
      partitionState.leaderRecoveryState,
      isrWithBrokerEpoch,
      partitionEpoch
    )
    val updatedState = PendingExpandIsr(
      newInSyncReplicaId,
      newLeaderAndIsr,
      currentState
    )
    partitionState = updatedState
    updatedState
  }

  private[cluster] def prepareIsrShrink(
    currentState: CommittedPartitionState,
    outOfSyncReplicaIds: Set[Int]
  ): PendingShrinkIsr = {
    // When shrinking the ISR, we cannot assume that the update will succeed as this could
    // erroneously advance the HW if the `AlterPartition` were to fail. Hence the "maximal ISR"
    // for `PendingShrinkIsr` is the the current ISR.
    val isrToSend = partitionState.isr -- outOfSyncReplicaIds
    val isrWithBrokerEpoch = addBrokerEpochToIsr(isrToSend.toList).asJava
    val newLeaderAndIsr = new LeaderAndIsr(
      localBrokerId,
      leaderEpoch,
      partitionState.leaderRecoveryState,
      isrWithBrokerEpoch,
      partitionEpoch
    )
    val updatedState = PendingShrinkIsr(
      outOfSyncReplicaIds,
      newLeaderAndIsr,
      currentState
    )
    partitionState = updatedState
    updatedState
  }

  private def addBrokerEpochToIsr(isr: List[Int]): List[BrokerState] = {
    isr.map { brokerId =>
      val brokerState = new BrokerState().setBrokerId(brokerId)
      if (!metadataCache.isInstanceOf[KRaftMetadataCache]) {
        brokerState.setBrokerEpoch(-1)
      } else if (brokerId == localBrokerId) {
        brokerState.setBrokerEpoch(localBrokerEpochSupplier())
      } else {
        val replica = remoteReplicasMap.get(brokerId)
        val brokerEpoch = if (replica == null) Option.empty else replica.stateSnapshot.brokerEpoch
        if (brokerEpoch.isEmpty) {
          // There are two cases where the broker epoch can be missing:
          // 1. During ISR expansion, we already held lock for the partition and did the broker epoch check, so the new
          //   ISR replica should have a valid broker epoch. Then, the missing broker epoch can only happen to the
          //   existing ISR replicas whose fetch request has not been received by this leader. It is safe to set the epoch
          //   -1 for this replica because even if it crashes at the moment, the controller will remove it from ISR
          //   and bump the partition epoch to reject this AlterPartition request.
          // 2. In ISR shrinking. Similarly, if the existing ISR replicas does not have broker epoch, it is safe to
          //   set the broker epoch to -1 here.
          brokerState.setBrokerEpoch(-1)
        } else {
          brokerState.setBrokerEpoch(brokerEpoch.get)
        }
      }
      brokerState
    }
  }

  private def submitAlterPartition(proposedIsrState: PendingPartitionChange): CompletableFuture[LeaderAndIsr] = {
    debug(s"Submitting ISR state change $proposedIsrState")
    val future = alterIsrManager.submit(
      new TopicIdPartition(topicId.getOrElse(Uuid.ZERO_UUID), topicPartition),
      proposedIsrState.sentLeaderAndIsr,
      controllerEpoch
    )
    future.whenComplete { (leaderAndIsr, e) =>
      var hwIncremented = false
      var shouldRetry = false

      inWriteLock(leaderIsrUpdateLock) {
        if (partitionState != proposedIsrState) {
          // This means partitionState was updated through leader election or some other mechanism
          // before we got the AlterPartition response. We don't know what happened on the controller
          // exactly, but we do know this response is out of date so we ignore it.
          debug(s"Ignoring failed ISR update to $proposedIsrState since we have already " +
            s"updated state to $partitionState")
        } else if (leaderAndIsr != null) {
          hwIncremented = handleAlterPartitionUpdate(proposedIsrState, leaderAndIsr)
        } else {
          shouldRetry = handleAlterPartitionError(proposedIsrState, Errors.forException(e))
        }
      }

      if (hwIncremented) {
        tryCompleteDelayedRequests()
      }

      // Send the AlterPartition request outside of the LeaderAndIsr lock since the completion logic
      // may increment the high watermark (and consequently complete delayed operations).
      if (shouldRetry) {
        submitAlterPartition(proposedIsrState)
      }
    }
  }

  /**
   * Handle a failed `AlterPartition` request. For errors which are non-retriable, we simply give up.
   * This leaves [[Partition.partitionState]] in a pending state. Since the error was non-retriable,
   * we are okay staying in this state until we see new metadata from LeaderAndIsr (or an update
   * to the KRaft metadata log).
   *
   * @param proposedIsrState The ISR state change that was requested
   * @param error The error returned from [[AlterPartitionManager]]
   * @return true if the `AlterPartition` request should be retried, false otherwise
   */
  private def handleAlterPartitionError(
    proposedIsrState: PendingPartitionChange,
    error: Errors
  ): Boolean = {
    alterPartitionListener.markFailed()
    error match {
      case Errors.OPERATION_NOT_ATTEMPTED | Errors.INELIGIBLE_REPLICA =>
        // Care must be taken when resetting to the last committed state since we may not
        // know in general whether the request was applied or not taking into account retries
        // and controller changes which might have occurred before we received the response.
        // However, when the controller returns INELIGIBLE_REPLICA (or OPERATION_NOT_ATTEMPTED),
        // the controller is explicitly telling us 1) that the current partition epoch is correct,
        // and 2) that the request was not applied. Even if the controller that sent the response
        // is stale, we are guaranteed from the monotonicity of the controller epoch that the
        // request could not have been applied by any past or future controller.
        partitionState = proposedIsrState.lastCommittedState
        info(s"Failed to alter partition to $proposedIsrState since the controller rejected the request with $error. " +
          s"Partition state has been reset to the latest committed state $partitionState.")
        false
      case Errors.UNKNOWN_TOPIC_OR_PARTITION =>
        debug(s"Failed to alter partition to $proposedIsrState since the controller doesn't know about " +
          "this topic or partition. Partition state may be out of sync, awaiting new the latest metadata.")
        false
      case Errors.UNKNOWN_TOPIC_ID =>
        debug(s"Failed to alter partition to $proposedIsrState since the controller doesn't know about " +
          "this topic. Partition state may be out of sync, awaiting new the latest metadata.")
        false
      case Errors.FENCED_LEADER_EPOCH =>
        debug(s"Failed to alter partition to $proposedIsrState since the leader epoch is old. " +
          "Partition state may be out of sync, awaiting new the latest metadata.")
        false
      case Errors.INVALID_UPDATE_VERSION =>
        debug(s"Failed to alter partition to $proposedIsrState because the partition epoch is invalid. " +
          "Partition state may be out of sync, awaiting new the latest metadata.")
        false
      case Errors.INVALID_REQUEST =>
        debug(s"Failed to alter partition to $proposedIsrState because the request is invalid. " +
          "Partition state may be out of sync, awaiting new the latest metadata.")
        false
      case Errors.NEW_LEADER_ELECTED =>
        // The operation completed successfully but this replica got removed from the replica set by the controller
        // while completing a ongoing reassignment. This replica is no longer the leader but it does not know it
        // yet. It should remain in the current pending state until the metadata overrides it.
        // This is only raised in KRaft mode.
        debug(s"The alter partition request successfully updated the partition state to $proposedIsrState but " +
          "this replica got removed from the replica set while completing a reassignment. " +
          "Waiting on new metadata to clean up this replica.")
        false
      case _ =>
        warn(s"Failed to update ISR to $proposedIsrState due to unexpected $error. Retrying.")
        true
    }
  }

  /**
   * Handle a successful `AlterPartition` response.
   *
   * @param proposedIsrState The ISR state change that was requested
   * @param leaderAndIsr The updated LeaderAndIsr state
   * @return true if the high watermark was successfully incremented following, false otherwise
   */
  private def handleAlterPartitionUpdate(
    proposedIsrState: PendingPartitionChange,
    leaderAndIsr: LeaderAndIsr
  ): Boolean = {
    // Success from controller, still need to check a few things
    if (leaderAndIsr.leaderEpoch != leaderEpoch) {
      debug(s"Ignoring new ISR $leaderAndIsr since we have a stale leader epoch $leaderEpoch.")
      alterPartitionListener.markFailed()
      false
    } else if (leaderAndIsr.partitionEpoch < partitionEpoch) {
      debug(s"Ignoring new ISR $leaderAndIsr since we have a newer version $partitionEpoch.")
      alterPartitionListener.markFailed()
      false
    } else {
      // This is one of two states:
      //   1) leaderAndIsr.partitionEpoch > partitionEpoch: Controller updated to new version with proposedIsrState.
      //   2) leaderAndIsr.partitionEpoch == partitionEpoch: No update was performed since proposed and actual state are the same.
      // In both cases, we want to move from Pending to Committed state to ensure new updates are processed.

      partitionState = CommittedPartitionState(leaderAndIsr.isr.asScala.map(_.toInt).toSet, leaderAndIsr.leaderRecoveryState)
      partitionEpoch = leaderAndIsr.partitionEpoch
      info(s"ISR updated to ${partitionState.isr.mkString(",")} ${if (isUnderMinIsr) "(under-min-isr)" else ""} " +
        s"and version updated to $partitionEpoch")

      proposedIsrState.notifyListener(alterPartitionListener)

      // we may need to increment high watermark since ISR could be down to 1
      leaderLogIfLocal.exists(log => maybeIncrementLeaderHW(log))
    }
  }

  override def equals(that: Any): Boolean = that match {
    case other: Partition => partitionId == other.partitionId && topic == other.topic
    case _ => false
  }

  override def hashCode: Int =
    31 + topic.hashCode + 17 * partitionId

  override def toString: String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; Replicas: " + assignmentState.replicas.mkString(","))
    partitionString.append("; ISR: " + partitionState.isr.mkString(","))
    assignmentState match {
      case OngoingReassignmentState(adding, removing, _) =>
        partitionString.append("; AddingReplicas: " + adding.mkString(","))
        partitionString.append("; RemovingReplicas: " + removing.mkString(","))
      case _ =>
    }
    partitionString.append("; LeaderRecoveryState: " + partitionState.leaderRecoveryState)
    partitionString.toString
  }
}
