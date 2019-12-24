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

import com.yammer.metrics.core.Gauge
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.{Optional, Properties}

import kafka.api.{ApiVersion, LeaderAndIsr, Request}
import kafka.common.UnexpectedAppendOffsetException
import kafka.controller.KafkaController
import kafka.log._
import kafka.log.remote.RemoteLogManager
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq}

trait PartitionStateStore {
  def fetchTopicConfig(): Properties
  def shrinkIsr(controllerEpoch: Int, leaderAndIsr: LeaderAndIsr): Option[Int]
  def expandIsr(controllerEpoch: Int, leaderAndIsr: LeaderAndIsr): Option[Int]
}

class ZkPartitionStateStore(topicPartition: TopicPartition,
                            zkClient: KafkaZkClient,
                            replicaManager: ReplicaManager) extends PartitionStateStore {

  override def fetchTopicConfig(): Properties = {
    val adminZkClient = new AdminZkClient(zkClient)
    adminZkClient.fetchEntityConfig(ConfigType.Topic, topicPartition.topic)
  }

  override def shrinkIsr(controllerEpoch: Int, leaderAndIsr: LeaderAndIsr): Option[Int] = {
    val newVersionOpt = updateIsr(controllerEpoch, leaderAndIsr)
    if (newVersionOpt.isDefined)
      replicaManager.isrShrinkRate.mark()
    newVersionOpt
  }

  override def expandIsr(controllerEpoch: Int, leaderAndIsr: LeaderAndIsr): Option[Int] = {
    val newVersionOpt = updateIsr(controllerEpoch, leaderAndIsr)
    if (newVersionOpt.isDefined)
      replicaManager.isrExpandRate.mark()
    newVersionOpt
  }

  private def updateIsr(controllerEpoch: Int, leaderAndIsr: LeaderAndIsr): Option[Int] = {
    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topicPartition,
      leaderAndIsr, controllerEpoch)

    if (updateSucceeded) {
      replicaManager.recordIsrChange(topicPartition)
      Some(newVersion)
    } else {
      replicaManager.failedIsrUpdatesRate.mark()
      None
    }
  }
}

class DelayedOperations(topicPartition: TopicPartition,
                        produce: DelayedOperationPurgatory[DelayedProduce],
                        fetch: DelayedOperationPurgatory[DelayedFetch],
                        deleteRecords: DelayedOperationPurgatory[DelayedDeleteRecords]) {

  def checkAndCompleteAll(): Unit = {
    val requestKey = TopicPartitionOperationKey(topicPartition)
    fetch.checkAndComplete(requestKey)
    produce.checkAndComplete(requestKey)
    deleteRecords.checkAndComplete(requestKey)
  }

  def checkAndCompleteFetch(): Unit = {
    fetch.checkAndComplete(TopicPartitionOperationKey(topicPartition))
  }

  def checkAndCompleteProduce(): Unit = {
    produce.checkAndComplete(TopicPartitionOperationKey(topicPartition))
  }

  def checkAndCompleteDeleteRecords(): Unit = {
    deleteRecords.checkAndComplete(TopicPartitionOperationKey(topicPartition))
  }

  def numDelayedDelete: Int = deleteRecords.numDelayed

  def numDelayedFetch: Int = fetch.numDelayed

  def numDelayedProduce: Int = produce.numDelayed
}

object Partition extends KafkaMetricsGroup {
  def apply(topicPartition: TopicPartition,
            time: Time,
            replicaManager: ReplicaManager): Partition = {
    val zkIsrBackingStore = new ZkPartitionStateStore(
      topicPartition,
      replicaManager.zkClient,
      replicaManager)

    val delayedOperations = new DelayedOperations(
      topicPartition,
      replicaManager.delayedProducePurgatory,
      replicaManager.delayedFetchPurgatory,
      replicaManager.delayedDeleteRecordsPurgatory)

    new Partition(topicPartition,
      replicaLagTimeMaxMs = replicaManager.config.replicaLagTimeMaxMs,
      interBrokerProtocolVersion = replicaManager.config.interBrokerProtocolVersion,
      localBrokerId = replicaManager.config.brokerId,
      time = time,
      stateStore = zkIsrBackingStore,
      delayedOperations = delayedOperations,
      metadataCache = replicaManager.metadataCache,
      logManager = replicaManager.logManager)
  }

  def removeMetrics(topicPartition: TopicPartition): Unit = {
    val tags = Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString)
    removeMetric("UnderReplicated", tags)
    removeMetric("UnderMinIsr", tags)
    removeMetric("InSyncReplicasCount", tags)
    removeMetric("ReplicasCount", tags)
    removeMetric("LastStableOffsetLag", tags)
    removeMetric("AtMinIsr", tags)
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

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
class Partition(val topicPartition: TopicPartition,
                replicaLagTimeMaxMs: Long,
                interBrokerProtocolVersion: ApiVersion,
                localBrokerId: Int,
                time: Time,
                stateStore: PartitionStateStore,
                delayedOperations: DelayedOperations,
                metadataCache: MetadataCache,
                logManager: LogManager) extends Logging with KafkaMetricsGroup {

  def topic: String = topicPartition.topic
  def partitionId: Int = topicPartition.partition

  private val remoteReplicasMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  // start offset for 'leaderEpoch' above (leader epoch of the current leader for this partition),
  // defined when this broker is leader for partition
  @volatile private var leaderEpochStartOffsetOpt: Option[Long] = None
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  @volatile var inSyncReplicaIds = Set.empty[Int]
  @volatile var assignmentState: AssignmentState = SimpleAssignmentState(Seq.empty)

  // Logs belonging to this partition. Majority of time it will be only one log, but if log directory
  // is getting changed (as a result of ReplicaAlterLogDirs command), we may have two logs until copy
  // completes and a switch to new location is performed.
  // log and futureLog variables defined below are used to capture this
  @volatile var log: Option[Log] = None
  // If ReplicaAlterLogDir command is in progress, this is future location of the log
  @volatile var futureLog: Option[Log] = None

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch
  this.logIdent = s"[Partition $topicPartition broker=$localBrokerId] "

  private val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  newGauge("UnderReplicated",
    new Gauge[Int] {
      def value: Int = {
        if (isUnderReplicated) 1 else 0
      }
    },
    tags
  )

  newGauge("InSyncReplicasCount",
    new Gauge[Int] {
      def value: Int = {
        if (isLeader) inSyncReplicaIds.size else 0
      }
    },
    tags
  )

  newGauge("UnderMinIsr",
    new Gauge[Int] {
      def value: Int = {
        if (isUnderMinIsr) 1 else 0
      }
    },
    tags
  )

  newGauge("AtMinIsr",
    new Gauge[Int] {
      def value: Int = {
        if (isAtMinIsr) 1 else 0
      }
    },
    tags
  )

  newGauge("ReplicasCount",
    new Gauge[Int] {
      def value: Int = {
        if (isLeader) assignmentState.replicationFactor else 0
      }
    },
    tags
  )

  newGauge("LastStableOffsetLag",
    new Gauge[Long] {
      def value: Long = {
        log.map(_.lastStableOffsetLag).getOrElse(0)
      }
    },
    tags
  )

  def isUnderReplicated: Boolean = isLeader && (assignmentState.replicationFactor - inSyncReplicaIds.size) > 0

  def isUnderMinIsr: Boolean = {
    leaderLogIfLocal.exists { inSyncReplicaIds.size < _.config.minInSyncReplicas }
  }

  def isAtMinIsr: Boolean = {
    leaderLogIfLocal.exists { inSyncReplicaIds.size == _.config.minInSyncReplicas }
  }

  def isReassigning: Boolean = assignmentState.isInstanceOf[OngoingReassignmentState]

  def isAddingLocalReplica: Boolean = assignmentState.isAddingReplica(localBrokerId)

  def isAddingReplica(replicaId: Int): Boolean = assignmentState.isAddingReplica(replicaId)

  /**
    * Create the future replica if 1) the current replica is not in the given log directory and 2) the future replica
    * does not exist. This method assumes that the current replica has already been created.
    *
    * @param logDir log directory
    * @param highWatermarkCheckpoints Checkpoint to load initial high watermark from
    * @return true iff the future replica is created
    */
  def maybeCreateFutureReplica(logDir: String, highWatermarkCheckpoints: OffsetCheckpoints): Boolean = {
    // The writeLock is needed to make sure that while the caller checks the log directory of the
    // current replica and the existence of the future replica, no other thread can update the log directory of the
    // current replica or remove the future replica.
    inWriteLock(leaderIsrUpdateLock) {
      val currentLogDir = localLogOrException.dir.getParent
      if (currentLogDir == logDir) {
        info(s"Current log directory $currentLogDir is same as requested log dir $logDir. " +
          s"Skipping future replica creation.")
        false
      } else {
        futureLog match {
          case Some(partitionFutureLog) =>
            val futureLogDir = partitionFutureLog.dir.getParent
            if (futureLogDir != logDir)
              throw new IllegalStateException(s"The future log dir $futureLogDir of $topicPartition is " +
                s"different from the requested log dir $logDir")
            false
          case None =>
            createLogIfNotExists(Request.FutureLocalReplicaId, isNew = false, isFutureReplica = true, highWatermarkCheckpoints)
            true
        }
      }
    }
  }

  def createLogIfNotExists(replicaId: Int, isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints): Unit = {
    isFutureReplica match {
      case true if futureLog.isEmpty =>
        val log = createLog(replicaId, isNew, isFutureReplica, offsetCheckpoints)
        this.futureLog = Option(log)
      case false if log.isEmpty =>
        val log = createLog(replicaId, isNew, isFutureReplica, offsetCheckpoints)
        this.log = Option(log)
      case _ => trace(s"${if (isFutureReplica) "Future Log" else "Log"} already exists.")
    }
  }

  // Visible for testing
  private[cluster] def createLog(replicaId: Int, isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints): Log = {
    val fetchLogConfig = () => {
      val props = stateStore.fetchTopicConfig()
      LogConfig.fromProps(logManager.currentDefaultConfig.originals, props)
    }

    logManager.initializingLog(topicPartition)
    var maybeLog: Option[Log] = None
    try {
      val log = logManager.getOrCreateLog(topicPartition, fetchLogConfig(), isNew, isFutureReplica)
      val checkpointHighWatermark = offsetCheckpoints.fetch(log.dir.getParent, topicPartition).getOrElse {
        info(s"No checkpointed highwatermark is found for partition $topicPartition")
        0L
      }
      val initialHighWatermark = log.updateHighWatermark(checkpointHighWatermark)
      info(s"Log loaded for partition $topicPartition with initial high watermark $initialHighWatermark")
      maybeLog = Some(log)
      log
    } finally {
      logManager.finishedInitializingLog(topicPartition, maybeLog, fetchLogConfig)
    }
  }

  def getReplica(replicaId: Int): Option[Replica] = Option(remoteReplicasMap.get(replicaId))

  private def getReplicaOrException(replicaId: Int): Replica = getReplica(replicaId).getOrElse{
    throw new ReplicaNotAvailableException(s"Replica with id $replicaId is not available on broker $localBrokerId")
  }

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
                          requireLeader: Boolean): Either[Log, Errors] = {
    checkCurrentLeaderEpoch(currentLeaderEpoch) match {
      case Errors.NONE =>
        if (requireLeader && !isLeader) {
          Right(Errors.NOT_LEADER_FOR_PARTITION)
        } else {
          log match {
            case Some(partitionLog) =>
              Left(partitionLog)
            case _ =>
              if (requireLeader)
                Right(Errors.NOT_LEADER_FOR_PARTITION)
              else
                Right(Errors.REPLICA_NOT_AVAILABLE)
          }
        }
      case error =>
        Right(error)
    }
  }

  def localLogOrException: Log = log.getOrElse {
    throw new ReplicaNotAvailableException(s"Log for partition $topicPartition is not available " +
      s"on broker $localBrokerId")
  }

  def futureLocalLogOrException: Log = futureLog.getOrElse {
    throw new ReplicaNotAvailableException(s"Future log for partition $topicPartition is not available " +
      s"on broker $localBrokerId")
  }

  def leaderLogIfLocal: Option[Log] = {
    log.filter(_ => isLeader)
  }

  /**
   * Returns true if this node is currently leader for the Partition.
   */
  def isLeader: Boolean = leaderReplicaIdOpt.contains(localBrokerId)

  def localLogWithEpochOrException(currentLeaderEpoch: Optional[Integer],
                                           requireLeader: Boolean): Log = {
    getLocalLog(currentLeaderEpoch, requireLeader) match {
      case Left(localLog) => localLog
      case Right(error) =>
        throw error.exception(s"Failed to find ${if (requireLeader) "leader " else ""} log for " +
          s"partition $topicPartition with leader epoch $currentLeaderEpoch. The current leader " +
          s"is $leaderReplicaIdOpt and the current epoch $leaderEpoch")
    }
  }

  // Visible for testing -- Used by unit tests to set log for this partition
  def setLog(log: Log, isFutureLog: Boolean): Unit = {
    if (isFutureLog)
      futureLog = Some(log)
    else
      this.log = Some(log)
  }

  // remoteReplicas will be called in the hot path, and must be inexpensive
  def remoteReplicas: Iterable[Replica] =
    remoteReplicasMap.values

  def futureReplicaDirChanged(newDestinationDir: String): Boolean = {
    inReadLock(leaderIsrUpdateLock) {
      futureLog.exists(_.dir.getParent != newDestinationDir)
    }
  }

  def removeFutureLocalReplica(deleteFromLogDir: Boolean = true): Unit = {
    inWriteLock(leaderIsrUpdateLock) {
      futureLog = None
      if (deleteFromLogDir)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }
  }

  // Return true iff the future replica exists and it has caught up with the current replica for this partition
  // Only ReplicaAlterDirThread will call this method and ReplicaAlterDirThread should remove the partition
  // from its partitionStates if this method returns true
  def maybeReplaceCurrentWithFutureReplica(): Boolean = {
    val localReplicaLEO = localLogOrException.logEndOffset
    val futureReplicaLEO = futureLog.map(_.logEndOffset)
    if (futureReplicaLEO.contains(localReplicaLEO)) {
      // The write lock is needed to make sure that while ReplicaAlterDirThread checks the LEO of the
      // current replica, no other thread can update LEO of the current replica via log truncation or log append operation.
      inWriteLock(leaderIsrUpdateLock) {
        futureLog match {
          case Some(futurePartitionLog) =>
            if (log.exists(_.logEndOffset == futurePartitionLog.logEndOffset)) {
              logManager.replaceCurrentWithFutureLog(topicPartition)
              log = futureLog
              removeFutureLocalReplica(false)
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

  def delete(): Unit = {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      remoteReplicasMap.clear()
      assignmentState = SimpleAssignmentState(Seq.empty)
      log = None
      futureLog = None
      inSyncReplicaIds = Set.empty
      leaderReplicaIdOpt = None
      leaderEpochStartOffsetOpt = None
      Partition.removeMetrics(topicPartition)
      logManager.asyncDelete(topicPartition)
      if (logManager.getLog(topicPartition, isFuture = true).isDefined)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }
  }

  def getLeaderEpoch: Int = this.leaderEpoch

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  def makeLeader(controllerId: Int,
                 partitionState: LeaderAndIsrPartitionState,
                 correlationId: Int,
                 highWatermarkCheckpoints: OffsetCheckpoints): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionState.controllerEpoch

      updateAssignmentAndIsr(
        assignment = partitionState.replicas.asScala.map(_.toInt),
        isr = partitionState.isr.asScala.map(_.toInt).toSet,
        addingReplicas = partitionState.addingReplicas.asScala.map(_.toInt),
        removingReplicas = partitionState.removingReplicas.asScala.map(_.toInt)
      )
      createLogIfNotExists(localBrokerId, partitionState.isNew, isFutureReplica = false, highWatermarkCheckpoints)

      val leaderLog = localLogOrException
      val leaderEpochStartOffset = leaderLog.logEndOffset
      info(s"$topicPartition starts at Leader Epoch ${partitionState.leaderEpoch} from " +
        s"offset $leaderEpochStartOffset. Previous Leader Epoch was: $leaderEpoch")

      //We cache the leader epoch here, persisting it only if it's local (hence having a log dir)
      leaderEpoch = partitionState.leaderEpoch
      leaderEpochStartOffsetOpt = Some(leaderEpochStartOffset)
      zkVersion = partitionState.zkVersion

      // In the case of successive leader elections in a short time period, a follower may have
      // entries in its log from a later epoch than any entry in the new leader's log. In order
      // to ensure that these followers can truncate to the right offset, we must cache the new
      // leader epoch and the start offset since it should be larger than any epoch that a follower
      // would try to query.
      leaderLog.maybeAssignEpochStartOffset(leaderEpoch, leaderEpochStartOffset)

      val isNewLeader = !isLeader
      val curLeaderLogEndOffset = leaderLog.logEndOffset
      val curTimeMs = time.milliseconds
      // initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and lastFetchLeaderLogEndOffset.
      remoteReplicas.foreach { replica =>
        val lastCaughtUpTimeMs = if (inSyncReplicaIds.contains(replica.brokerId)) curTimeMs else 0L
        replica.resetLastCaughtUpTime(curLeaderLogEndOffset, curTimeMs, lastCaughtUpTimeMs)
      }

      if (isNewLeader) {
        // mark local replica as the leader after converting hw
        leaderReplicaIdOpt = Some(localBrokerId)
        // reset log end offset for remote replicas
        remoteReplicas.foreach { replica =>
          replica.updateFetchState(
            followerFetchOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata,
            followerStartOffset = Log.UnknownOffset,
            followerFetchTimeMs = 0L,
            leaderEndOffset = Log.UnknownOffset
          )
          replica.updateLastSentHighWatermark(0L)
        }
      }
      // we may need to increment high watermark since ISR could be down to 1
      (maybeIncrementLeaderHW(leaderLog), isNewLeader)
    }
    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change and the new epoch is equal or one
   *  greater (that is, no updates have been missed), return false to indicate to the
    * replica manager that state is already correct and the become-follower steps can be skipped
   */
  def makeFollower(controllerId: Int,
                   partitionState: LeaderAndIsrPartitionState,
                   correlationId: Int,
                   highWatermarkCheckpoints: OffsetCheckpoints): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val newLeaderBrokerId = partitionState.leader
      val oldLeaderEpoch = leaderEpoch
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionState.controllerEpoch

      updateAssignmentAndIsr(
        assignment = partitionState.replicas.asScala.iterator.map(_.toInt).toSeq,
        isr = Set.empty[Int],
        addingReplicas = partitionState.addingReplicas.asScala.map(_.toInt),
        removingReplicas = partitionState.removingReplicas.asScala.map(_.toInt)
      )
      createLogIfNotExists(localBrokerId, partitionState.isNew, isFutureReplica = false, highWatermarkCheckpoints)

      leaderEpoch = partitionState.leaderEpoch
      leaderEpochStartOffsetOpt = None
      zkVersion = partitionState.zkVersion

      if (leaderReplicaIdOpt.contains(newLeaderBrokerId) && leaderEpoch == oldLeaderEpoch) {
        false
      } else {
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the follower's state in the leader based on the last fetch request. See
   * [[kafka.cluster.Replica#updateLogReadResult]] for details.
   *
   * @return true if the follower's fetch state was updated, false if the followerId is not recognized
   */
  def updateFollowerFetchState(followerId: Int,
                               followerFetchOffsetMetadata: LogOffsetMetadata,
                               followerStartOffset: Long,
                               followerFetchTimeMs: Long,
                               leaderEndOffset: Long): Boolean = {
    getReplica(followerId) match {
      case Some(followerReplica) =>
        // No need to calculate low watermark if there is no delayed DeleteRecordsRequest
        val oldLeaderLW = if (delayedOperations.numDelayedDelete > 0) lowWatermarkIfLeader else -1L
        val prevFollowerEndOffset = followerReplica.logEndOffset
        followerReplica.updateFetchState(
          followerFetchOffsetMetadata,
          followerStartOffset,
          followerFetchTimeMs,
          leaderEndOffset)

        val newLeaderLW = if (delayedOperations.numDelayedDelete > 0) lowWatermarkIfLeader else -1L
        // check if the LW of the partition has incremented
        // since the replica's logStartOffset may have incremented
        val leaderLWIncremented = newLeaderLW > oldLeaderLW

        // check if we need to expand ISR to include this replica
        // if it is not in the ISR yet
        if (!inSyncReplicaIds.contains(followerId))
          maybeExpandIsr(followerReplica, followerFetchTimeMs)

        // check if the HW of the partition can now be incremented
        // since the replica may already be in the ISR and its LEO has just incremented
        val leaderHWIncremented = if (prevFollowerEndOffset != followerReplica.logEndOffset) {
          leaderLogIfLocal.exists(leaderLog => maybeIncrementLeaderHW(leaderLog, followerFetchTimeMs))
        } else {
          false
        }

        // some delayed operations may be unblocked after HW or LW changed
        if (leaderLWIncremented || leaderHWIncremented)
          tryCompleteDelayedRequests()

        debug(s"Recorded replica $followerId log end offset (LEO) position " +
          s"${followerFetchOffsetMetadata.messageOffset} and log start offset $followerStartOffset.")
        true

      case None =>
        false
    }
  }

  /**
   * Stores the topic partition assignment and ISR.
   * It creates a new Replica object for any new remote broker. The isr parameter is
   * expected to be a subset of the assignment parameter.
   *
   * Note: public visibility for tests.
   *
   * @param assignment An ordered sequence of all the broker ids that were assigned to this
   *                   topic partition
   * @param isr The set of broker ids that are known to be insync with the leader
   * @param addingReplicas An ordered sequence of all broker ids that will be added to the
    *                       assignment
   * @param removingReplicas An ordered sequence of all broker ids that will be removed from
    *                         the assignment
   */
  def updateAssignmentAndIsr(assignment: Seq[Int],
                             isr: Set[Int],
                             addingReplicas: Seq[Int],
                             removingReplicas: Seq[Int]): Unit = {
    val replicaSet = assignment.toSet
    val removedReplicas = remoteReplicasMap.keys -- replicaSet

    assignment
      .filter(_ != localBrokerId)
      .foreach(id => remoteReplicasMap.getAndMaybePut(id, new Replica(id, topicPartition)))
    removedReplicas.foreach(remoteReplicasMap.remove)
    if (addingReplicas.nonEmpty || removingReplicas.nonEmpty)
      assignmentState = OngoingReassignmentState(addingReplicas, removingReplicas, assignment)
    else
      assignmentState = SimpleAssignmentState(assignment)
    inSyncReplicaIds = isr
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
  private def maybeExpandIsr(followerReplica: Replica, followerFetchTimeMs: Long): Unit = {
    inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR
      leaderLogIfLocal.foreach { leaderLog =>
        val leaderHighwatermark = leaderLog.highWatermark
        if (!inSyncReplicaIds.contains(followerReplica.brokerId) && isFollowerInSync(followerReplica, leaderHighwatermark)) {
          val newInSyncReplicaIds = inSyncReplicaIds + followerReplica.brokerId
          info(s"Expanding ISR from ${inSyncReplicaIds.mkString(",")} " +
            s"to ${newInSyncReplicaIds.mkString(",")}")

          // update ISR in ZK and cache
          expandIsr(newInSyncReplicaIds)
        }
      }
    }
  }

  private def isFollowerInSync(followerReplica: Replica, highWatermark: Long): Boolean = {
    val followerEndOffset = followerReplica.logEndOffset
    followerEndOffset >= highWatermark && leaderEpochStartOffsetOpt.exists(followerEndOffset >= _)
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
        val curInSyncReplicaIds = inSyncReplicaIds

        if (isTraceEnabled) {
          def logEndOffsetString: ((Int, Long)) => String = {
            case (brokerId, logEndOffset) => s"broker $brokerId: $logEndOffset"
          }

          val curInSyncReplicaObjects = (curInSyncReplicaIds - localBrokerId).map(getReplicaOrException)
          val replicaInfo = curInSyncReplicaObjects.map(replica => (replica.brokerId, replica.logEndOffset))
          val localLogInfo = (localBrokerId, localLogOrException.logEndOffset)
          val (ackedReplicas, awaitingReplicas) = (replicaInfo + localLogInfo).partition { _._2 >= requiredOffset}

          trace(s"Progress awaiting ISR acks for offset $requiredOffset: " +
            s"acked: ${ackedReplicas.map(logEndOffsetString)}, " +
            s"awaiting ${awaitingReplicas.map(logEndOffsetString)}")
        }

        val minIsr = leaderLog.config.minInSyncReplicas
        if (leaderLog.highWatermark >= requiredOffset) {
          /*
           * The topic may be configured not to accept messages if there are not enough replicas in ISR
           * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
           */
          if (minIsr <= curInSyncReplicaIds.size)
            (true, Errors.NONE)
          else
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
        } else
          (false, Errors.NONE)
      case None =>
        (false, Errors.NOT_LEADER_FOR_PARTITION)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   *
   * 1. Partition ISR changed
   * 2. Any replica's LEO changed
   *
   * The HW is determined by the smallest log end offset among all replicas that are in sync or are considered caught-up.
   * This way, if a replica is considered caught-up, but its log end offset is smaller than HW, we will wait for this
   * replica to catch up to the HW before advancing the HW. This helps the situation when the ISR only includes the
   * leader replica and a follower tries to catch up. If we don't wait for the follower when advancing the HW, the
   * follower's log end offset may keep falling behind the HW (determined by the leader's log end offset) and therefore
   * will never be added to ISR.
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  private def maybeIncrementLeaderHW(leaderLog: Log, curTime: Long = time.milliseconds): Boolean = {
    inReadLock(leaderIsrUpdateLock) {
      // maybeIncrementLeaderHW is in the hot path, the following code is written to
      // avoid unnecessary collection generation
      var newHighWatermark = leaderLog.logEndOffsetMetadata
      remoteReplicasMap.values.foreach { replica =>
        if (replica.logEndOffsetMetadata.messageOffset < newHighWatermark.messageOffset &&
          (curTime - replica.lastCaughtUpTimeMs <= replicaLagTimeMaxMs || inSyncReplicaIds.contains(replica.brokerId))) {
          newHighWatermark = replica.logEndOffsetMetadata
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
            val replicaInfo = remoteReplicas.map(replica => (replica.brokerId, replica.logEndOffsetMetadata)).toSet
            val localLogInfo = (localBrokerId, localLogOrException.logEndOffsetMetadata)
            trace(s"Skipping update high watermark since new hw $newHighWatermark is not larger than old value. " +
              s"All current LEOs are ${(replicaInfo + localLogInfo).map(logEndOffsetString)}")
          }
          false
      }
    }
  }

  /**
   * The low watermark offset value, calculated only if the local replica is the partition leader
   * It is only used by leader broker to decide when DeleteRecordsRequest is satisfied. Its value is minimum logStartOffset of all live replicas
   * Low watermark will increase when the leader broker receives either FetchRequest or DeleteRecordsRequest.
   */
  def lowWatermarkIfLeader: Long = {
    if (!isLeader)
      throw new NotLeaderForPartitionException(s"Leader not local for partition $topicPartition on broker $localBrokerId")

    // lowWatermarkIfLeader may be called many times when a DeleteRecordsRequest is outstanding,
    // care has been taken to avoid generating unnecessary collections in this code
    var lowWaterMark = localLogOrException.logStartOffset
    remoteReplicas.foreach { replica =>
      if (metadataCache.getAliveBroker(replica.brokerId).nonEmpty && replica.logStartOffset < lowWaterMark) {
        lowWaterMark = replica.logStartOffset
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
  private def tryCompleteDelayedRequests(): Unit = delayedOperations.checkAndCompleteAll()

  def maybeShrinkIsr(replicaMaxLagTimeMs: Long): Unit = {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      leaderLogIfLocal match {
        case Some(leaderLog) =>
          val outOfSyncReplicaIds = getOutOfSyncReplicas(replicaMaxLagTimeMs)
          if (outOfSyncReplicaIds.nonEmpty) {
            val newInSyncReplicaIds = inSyncReplicaIds -- outOfSyncReplicaIds
            assert(newInSyncReplicaIds.nonEmpty)
            info("Shrinking ISR from %s to %s. Leader: (highWatermark: %d, endOffset: %d). Out of sync replicas: %s."
              .format(inSyncReplicaIds.mkString(","),
                newInSyncReplicaIds.mkString(","),
                leaderLog.highWatermark,
                leaderLog.logEndOffset,
                outOfSyncReplicaIds.map { replicaId =>
                  s"(brokerId: $replicaId, endOffset: ${getReplicaOrException(replicaId).logEndOffset})"
                }.mkString(" ")
              )
            )

            // update ISR in zk and in cache
            shrinkIsr(newInSyncReplicaIds)

            // we may need to increment high watermark since ISR could be down to 1
            maybeIncrementLeaderHW(leaderLog)
          } else {
            false
          }

        case None => false // do nothing if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  private def isFollowerOutOfSync(replicaId: Int,
                                  leaderEndOffset: Long,
                                  currentTimeMs: Long,
                                  maxLagMs: Long): Boolean = {
    val followerReplica = getReplicaOrException(replicaId)
    followerReplica.logEndOffset != leaderEndOffset &&
      (currentTimeMs - followerReplica.lastCaughtUpTimeMs) > maxLagMs
  }

  def getOutOfSyncReplicas(maxLagMs: Long): Set[Int] = {
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
     **/
    val candidateReplicaIds = inSyncReplicaIds - localBrokerId
    val currentTimeMs = time.milliseconds()
    val leaderEndOffset = localLogOrException.logEndOffset
    candidateReplicaIds.filter(replicaId => isFollowerOutOfSync(replicaId, leaderEndOffset, currentTimeMs, maxLagMs))
  }

  private def doAppendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Option[LogAppendInfo] = {
    // The read lock is needed to handle race condition if request handler thread tries to
    // remove future replica after receiving AlterReplicaLogDirsRequest.
    inReadLock(leaderIsrUpdateLock) {
      if (isFuture) {
        // Note the replica may be undefined if it is removed by a non-ReplicaAlterLogDirsThread before
        // this method is called
        futureLog.map { _.appendAsFollower(records) }
      } else {
        // The read lock is needed to prevent the follower replica from being updated while ReplicaAlterDirThread
        // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
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

  def appendRecordsToLeader(records: MemoryRecords, isFromClient: Boolean, requiredAcks: Int = 0): LogAppendInfo = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      leaderLogIfLocal match {
        case Some(leaderLog) =>
          val minIsr = leaderLog.config.minInSyncReplicas
          val inSyncSize = inSyncReplicaIds.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException(s"The size of the current ISR $inSyncReplicaIds " +
              s"is insufficient to satisfy the min.isr requirement of $minIsr for partition $topicPartition")
          }

          val info = leaderLog.appendAsLeader(records, leaderEpoch = this.leaderEpoch, isFromClient,
            interBrokerProtocolVersion)

          // we may need to increment high watermark since ISR could be down to 1
          (info, maybeIncrementLeaderHW(leaderLog))

        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
    else {
      // probably unblock some follower fetch requests since log end offset has been updated
      delayedOperations.checkAndCompleteFetch()
    }

    info
  }

  def readRecords(fetchOffset: Long,
                  currentLeaderEpoch: Optional[Integer],
                  maxBytes: Int,
                  fetchIsolation: FetchIsolation,
                  fetchOnlyFromLeader: Boolean,
                  minOneMessage: Boolean): LogReadInfo = inReadLock(leaderIsrUpdateLock) {
    // decide whether to only fetch from leader
    val localLog = localLogWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)
    readRecords(fetchOffset, localLog, maxBytes, fetchIsolation, minOneMessage)
  }

  def readRecords(fetchOffset: Long,
                  localLog: Log,
                  maxBytes: Int,
                  fetchIsolation: FetchIsolation,
                  minOneMessage: Boolean): LogReadInfo = inReadLock(leaderIsrUpdateLock) {
    // Note we use the log end offset prior to the read. This ensures that any appends following
    // the fetch do not prevent a follower from coming into sync.
    val initialHighWatermark = localLog.highWatermark
    val initialLogStartOffset = localLog.logStartOffset
    val initialLogEndOffset = localLog.logEndOffset
    val initialLastStableOffset = localLog.lastStableOffset
    val fetchedData = localLog.read(fetchOffset, maxBytes, fetchIsolation, minOneMessage)

    LogReadInfo(
      fetchedData = fetchedData,
      highWatermark = initialHighWatermark,
      logStartOffset = initialLogStartOffset,
      logEndOffset = initialLogEndOffset,
      lastStableOffset = initialLastStableOffset)
  }

  def fetchOffsetForTimestamp(timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean,
                              remoteLogManager: Option[RemoteLogManager] = None): Option[TimestampAndOffset] = inReadLock(leaderIsrUpdateLock) {
    // decide whether to only fetch from leader
    val localLog = localLogWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)

    val lastFetchableOffset = isolationLevel match {
      case Some(IsolationLevel.READ_COMMITTED) => localLog.lastStableOffset
      case Some(IsolationLevel.READ_UNCOMMITTED) => localLog.highWatermark
      case None => localLog.logEndOffset
    }

    val epochLogString = if(currentLeaderEpoch.isPresent) {
      s"epoch ${currentLeaderEpoch.get}"
    } else {
      "unknown epoch"
    }

    // Only consider throwing an error if we get a client request (isolationLevel is defined) and the start offset
    // is lagging behind the high watermark
    val maybeOffsetsError: Option[ApiException] = leaderEpochStartOffsetOpt
      .filter(epochStart => isolationLevel.isDefined && epochStart > localLog.highWatermark)
      .map(epochStart => Errors.OFFSET_NOT_AVAILABLE.exception(s"Failed to fetch offsets for " +
        s"partition $topicPartition with leader $epochLogString as this partition's " +
        s"high watermark (${localLog.highWatermark}) is lagging behind the " +
        s"start offset from the beginning of this epoch ($epochStart)."))

    def getOffsetByTimestamp: Option[TimestampAndOffset] = {
      logManager.getLog(topicPartition).flatMap(log => log.fetchOffsetByTimestamp(timestamp, remoteLogManager))
    }

    // If we're in the lagging HW state after a leader election, throw OffsetNotAvailable for "latest" offset
    // or for a timestamp lookup that is beyond the last fetchable offset.
    timestamp match {
      case ListOffsetRequest.LATEST_TIMESTAMP =>
        maybeOffsetsError.map(e => throw e)
          .orElse(Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, lastFetchableOffset, Optional.of(leaderEpoch))))
      case ListOffsetRequest.EARLIEST_TIMESTAMP =>
        getOffsetByTimestamp
      case ListOffsetRequest.NEXT_LOCAL_TIMESTAMP =>
        getOffsetByTimestamp
      case _ =>
        getOffsetByTimestamp.filter(timestampAndOffset => timestampAndOffset.offset < lastFetchableOffset)
          .orElse(maybeOffsetsError.map(e => throw e))
    }
  }

  def fetchOffsetSnapshot(currentLeaderEpoch: Optional[Integer],
                          fetchOnlyFromLeader: Boolean): LogOffsetSnapshot = inReadLock(leaderIsrUpdateLock) {
    // decide whether to only fetch from leader
    val localLog = localLogWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)
    localLog.fetchOffsetSnapshot
  }

  def legacyFetchOffsetsForTimestamp(timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long] = inReadLock(leaderIsrUpdateLock) {
    val localLog = localLogWithEpochOrException(Optional.empty(), fetchOnlyFromLeader)
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

        leaderLog.maybeIncrementLogStartOffset(convertedOffset)
        LogDeleteRecordsResult(
          requestedOffset = convertedOffset,
          lowWatermark = lowWatermarkIfLeader)
      case None =>
        throw new NotLeaderForPartitionException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
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
    // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateTo(Map(topicPartition -> offset), isFuture = isFuture)
    }
  }

  /**
    * Delete all data in the local log of this partition and start the log at the new offset
    *
    * @param newOffset The new offset to start the log with
    * @param isFuture True iff the truncation should be performed on the future log of this partition
    */
  def truncateFullyAndStartAt(newOffset: Long, isFuture: Boolean): Unit = {
    // The read lock is needed to prevent the follower replica from being truncated while ReplicaAlterDirThread
    // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateFullyAndStartAt(topicPartition, newOffset, isFuture = isFuture)
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
            case Some(epochAndOffset) => new EpochEndOffset(NONE, epochAndOffset.leaderEpoch, epochAndOffset.offset)
            case None => new EpochEndOffset(NONE, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
          }
        case Right(error) =>
          new EpochEndOffset(error, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
    }
  }

  private def expandIsr(newIsr: Set[Int]): Unit = {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.toList, zkVersion)
    val zkVersionOpt = stateStore.expandIsr(controllerEpoch, newLeaderAndIsr)
    maybeUpdateIsrAndVersion(newIsr, zkVersionOpt)
  }

  private def shrinkIsr(newIsr: Set[Int]): Unit = {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.toList, zkVersion)
    val zkVersionOpt = stateStore.shrinkIsr(controllerEpoch, newLeaderAndIsr)
    maybeUpdateIsrAndVersion(newIsr, zkVersionOpt)
  }

  private def maybeUpdateIsrAndVersion(isr: Set[Int], zkVersionOpt: Option[Int]): Unit = {
    zkVersionOpt match {
      case Some(newVersion) =>
        inSyncReplicaIds = isr
        zkVersion = newVersion
        info("ISR updated to [%s] and zkVersion updated to [%d]".format(isr.mkString(","), zkVersion))

      case None =>
        info(s"Cached zkVersion $zkVersion not equal to that in zookeeper, skip updating ISR")
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
    partitionString.append("; ISR: " + inSyncReplicaIds.mkString(","))
    assignmentState match {
      case OngoingReassignmentState(adding, removing, _) =>
        partitionString.append("; AddingReplicas: " + adding.mkString(","))
        partitionString.append("; RemovingReplicas: " + removing.mkString(","))
      case _ =>
    }
    partitionString.toString
  }
}
