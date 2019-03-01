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

import java.util.Optional
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.yammer.metrics.core.Gauge
import kafka.api.{ApiVersion, LeaderAndIsr, Request}
import kafka.common.UnexpectedAppendOffsetException
import kafka.controller.KafkaController
import kafka.log._
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.Map

object Partition {
  def apply(topicPartition: TopicPartition,
            time: Time,
            replicaManager: ReplicaManager): Partition = {
    new Partition(topicPartition,
      isOffline = false,
      replicaLagTimeMaxMs = replicaManager.config.replicaLagTimeMaxMs,
      interBrokerProtocolVersion = replicaManager.config.interBrokerProtocolVersion,
      localBrokerId = replicaManager.config.brokerId,
      time = time,
      replicaManager = replicaManager,
      logManager = replicaManager.logManager,
      zkClient = replicaManager.zkClient)
  }
}

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
class Partition(val topicPartition: TopicPartition,
                val isOffline: Boolean,
                private val replicaLagTimeMaxMs: Long,
                private val interBrokerProtocolVersion: ApiVersion,
                private val localBrokerId: Int,
                private val time: Time,
                private val replicaManager: ReplicaManager,
                private val logManager: LogManager,
                private val zkClient: KafkaZkClient) extends Logging with KafkaMetricsGroup {

  def topic: String = topicPartition.topic
  def partitionId: Int = topicPartition.partition

  // allReplicasMap includes both assigned replicas and the future replica if there is ongoing replica movement
  private val allReplicasMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  // start offset for 'leaderEpoch' above (leader epoch of the current leader for this partition),
  // defined when this broker is leader for partition
  @volatile private var leaderEpochStartOffsetOpt: Option[Long] = None
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch
  this.logIdent = s"[Partition $topicPartition broker=$localBrokerId] "

  private def isReplicaLocal(replicaId: Int) : Boolean = replicaId == localBrokerId || replicaId == Request.FutureLocalReplicaId

  private val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  // Do not create metrics if this partition is ReplicaManager.OfflinePartition
  if (!isOffline) {
    newGauge("UnderReplicated",
      new Gauge[Int] {
        def value = {
          if (isUnderReplicated) 1 else 0
        }
      },
      tags
    )

    newGauge("InSyncReplicasCount",
      new Gauge[Int] {
        def value = {
          if (isLeaderReplicaLocal) inSyncReplicas.size else 0
        }
      },
      tags
    )

    newGauge("UnderMinIsr",
      new Gauge[Int] {
        def value = {
          if (isUnderMinIsr) 1 else 0
        }
      },
      tags
    )

    newGauge("ReplicasCount",
      new Gauge[Int] {
        def value = {
          if (isLeaderReplicaLocal) assignedReplicas.size else 0
        }
      },
      tags
    )

    newGauge("LastStableOffsetLag",
      new Gauge[Long] {
        def value = {
          leaderReplicaIfLocal.map { replica =>
            replica.highWatermark.messageOffset - replica.lastStableOffset.messageOffset
          }.getOrElse(0)
        }
      },
      tags
    )
  }

  private def isLeaderReplicaLocal: Boolean = leaderReplicaIfLocal.isDefined

  def isUnderReplicated: Boolean =
    isLeaderReplicaLocal && inSyncReplicas.size < assignedReplicas.size

  def isUnderMinIsr: Boolean = {
    leaderReplicaIfLocal match {
      case Some(leaderReplica) =>
        inSyncReplicas.size < leaderReplica.log.get.config.minInSyncReplicas
      case None =>
        false
    }
  }

  /**
    * Create the future replica if 1) the current replica is not in the given log directory and 2) the future replica
    * does not exist. This method assumes that the current replica has already been created.
    *
    * @param logDir log directory
    * @return true iff the future replica is created
    */
  def maybeCreateFutureReplica(logDir: String): Boolean = {
    // The writeLock is needed to make sure that while the caller checks the log directory of the
    // current replica and the existence of the future replica, no other thread can update the log directory of the
    // current replica or remove the future replica.
    inWriteLock(leaderIsrUpdateLock) {
      val currentReplica = localReplicaOrException
      if (currentReplica.log.get.dir.getParent == logDir)
        false
      else {
        futureLocalReplica match {
          case Some(replica) =>
            val futureReplicaLogDir = replica.log.get.dir.getParent
            if (futureReplicaLogDir != logDir)
              throw new IllegalStateException(s"The future log dir $futureReplicaLogDir of $topicPartition is " +
                s"different from the requested log dir $logDir")
            false
          case None =>
            getOrCreateReplica(Request.FutureLocalReplicaId, isNew = false)
            true
        }
      }
    }
  }

  def getOrCreateReplica(replicaId: Int, isNew: Boolean = false): Replica = {
    allReplicasMap.getAndMaybePut(replicaId, {
      if (isReplicaLocal(replicaId)) {
        val adminZkClient = new AdminZkClient(zkClient)
        val props = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
        val config = LogConfig.fromProps(logManager.currentDefaultConfig.originals, props)
        val log = logManager.getOrCreateLog(topicPartition, config, isNew, replicaId == Request.FutureLocalReplicaId)
        val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParent)
        val offsetMap = checkpoint.read()
        if (!offsetMap.contains(topicPartition))
          info(s"No checkpointed highwatermark is found for partition $topicPartition")
        val offset = math.min(offsetMap.getOrElse(topicPartition, 0L), log.logEndOffset)
        new Replica(replicaId, topicPartition, time, offset, Some(log))
      } else new Replica(replicaId, topicPartition, time)
    })
  }

  def getReplica(replicaId: Int): Option[Replica] = Option(allReplicasMap.get(replicaId))

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

  private def getLocalReplica(replicaId: Int,
                              currentLeaderEpoch: Optional[Integer],
                              requireLeader: Boolean): Either[Replica, Errors] = {
    checkCurrentLeaderEpoch(currentLeaderEpoch) match {
      case Errors.NONE =>
        if (requireLeader && !leaderReplicaIdOpt.contains(localBrokerId)) {
          Right(Errors.NOT_LEADER_FOR_PARTITION)
        } else {
          val replica = allReplicasMap.get(replicaId)
          if (replica == null) {
            if (requireLeader)
              Right(Errors.NOT_LEADER_FOR_PARTITION)
            else
              Right(Errors.REPLICA_NOT_AVAILABLE)
          } else {
            Left(replica)
          }
        }
      case error =>
        Right(error)
    }
  }

  def localReplica: Option[Replica] = getReplica(localBrokerId)

  def localReplicaOrException: Replica = localReplica.getOrElse {
    throw new ReplicaNotAvailableException(s"Replica for partition $topicPartition is not available " +
      s"on broker $localBrokerId")
  }

  def futureLocalReplica: Option[Replica] = getReplica(Request.FutureLocalReplicaId)

  def futureLocalReplicaOrException: Replica = futureLocalReplica.getOrElse {
    throw new ReplicaNotAvailableException(s"Future replica for partition $topicPartition is not available " +
      s"on broker $localBrokerId")
  }

  def leaderReplicaIfLocal: Option[Replica] = {
    if (leaderReplicaIdOpt.contains(localBrokerId))
      localReplica
    else
      None
  }

  private def localReplicaWithEpochOrException(currentLeaderEpoch: Optional[Integer],
                                               requireLeader: Boolean): Replica = {
    getLocalReplica(localBrokerId, currentLeaderEpoch, requireLeader) match {
      case Left(replica) => replica
      case Right(error) =>
        throw error.exception(s"Failed to find ${if (requireLeader) "leader " else ""} for " +
          s"partition $topicPartition with leader epoch $currentLeaderEpoch. The current leader " +
          s"is $leaderReplicaIdOpt and the current epoch $leaderEpoch")
    }
  }

  def addReplicaIfNotExists(replica: Replica): Replica =
    allReplicasMap.putIfNotExists(replica.brokerId, replica)

  def assignedReplicas: Set[Replica] =
    allReplicasMap.values.filter(replica => Request.isValidBrokerId(replica.brokerId)).toSet

  def allReplicas: Set[Replica] =
    allReplicasMap.values.toSet

  private def removeReplica(replicaId: Int) {
    allReplicasMap.remove(replicaId)
  }

  def futureReplicaDirChanged(newDestinationDir: String): Boolean = {
    inReadLock(leaderIsrUpdateLock) {
      futureLocalReplica match {
        case Some(replica) =>
          if (replica.log.get.dir.getParent != newDestinationDir)
            true
          else
            false
        case None => false
      }
    }
  }

  def removeFutureLocalReplica(deleteFromLogDir: Boolean = true) {
    inWriteLock(leaderIsrUpdateLock) {
      allReplicasMap.remove(Request.FutureLocalReplicaId)
      if (deleteFromLogDir)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }
  }

  // Return true iff the future replica exists and it has caught up with the current replica for this partition
  // Only ReplicaAlterDirThread will call this method and ReplicaAlterDirThread should remove the partition
  // from its partitionStates if this method returns true
  def maybeReplaceCurrentWithFutureReplica(): Boolean = {
    val replica = localReplicaOrException
    val futureReplicaLEO = futureLocalReplica.map(_.logEndOffset)
    if (futureReplicaLEO.contains(replica.logEndOffset)) {
      // The write lock is needed to make sure that while ReplicaAlterDirThread checks the LEO of the
      // current replica, no other thread can update LEO of the current replica via log truncation or log append operation.
      inWriteLock(leaderIsrUpdateLock) {
        futureLocalReplica match {
          case Some(futureReplica) =>
            if (replica.logEndOffset == futureReplica.logEndOffset) {
              logManager.replaceCurrentWithFutureLog(topicPartition)
              replica.log = futureReplica.log
              futureReplica.log = None
              allReplicasMap.remove(Request.FutureLocalReplicaId)
              true
            } else false
          case None =>
            // Future replica is removed by a non-ReplicaAlterLogDirsThread before this method is called
            // In this case the partition should have been removed from state of the ReplicaAlterLogDirsThread
            // Return false so that ReplicaAlterLogDirsThread does not have to remove this partition from the state again to avoid race condition
            false
        }
      }
    } else false
  }

  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      allReplicasMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      leaderEpochStartOffsetOpt = None
      removePartitionMetrics()
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
  def makeLeader(controllerId: Int, partitionStateInfo: LeaderAndIsrRequest.PartitionState, correlationId: Int): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      val newAssignedReplicas = partitionStateInfo.basePartitionState.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.basePartitionState.controllerEpoch
      // add replicas that are new
      val newInSyncReplicas = partitionStateInfo.basePartitionState.isr.asScala.map(r => getOrCreateReplica(r, partitionStateInfo.isNew)).toSet
      // remove assigned replicas that have been removed by the controller
      (assignedReplicas.map(_.brokerId) -- newAssignedReplicas).foreach(removeReplica)
      inSyncReplicas = newInSyncReplicas
      newAssignedReplicas.foreach(id => getOrCreateReplica(id, partitionStateInfo.isNew))

      val leaderReplica = localReplicaOrException
      val leaderEpochStartOffset = leaderReplica.logEndOffset
      info(s"$topicPartition starts at Leader Epoch ${partitionStateInfo.basePartitionState.leaderEpoch} from " +
        s"offset $leaderEpochStartOffset. Previous Leader Epoch was: $leaderEpoch")

      //We cache the leader epoch here, persisting it only if it's local (hence having a log dir)
      leaderEpoch = partitionStateInfo.basePartitionState.leaderEpoch
      leaderEpochStartOffsetOpt = Some(leaderEpochStartOffset)
      zkVersion = partitionStateInfo.basePartitionState.zkVersion

      // In the case of successive leader elections in a short time period, a follower may have
      // entries in its log from a later epoch than any entry in the new leader's log. In order
      // to ensure that these followers can truncate to the right offset, we must cache the new
      // leader epoch and the start offset since it should be larger than any epoch that a follower
      // would try to query.
      leaderReplica.log.foreach { log =>
        log.maybeAssignEpochStartOffset(leaderEpoch, leaderEpochStartOffset)
      }

      val isNewLeader = !leaderReplicaIdOpt.contains(localBrokerId)
      val curLeaderLogEndOffset = leaderReplica.logEndOffset
      val curTimeMs = time.milliseconds
      // initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and lastFetchLeaderLogEndOffset.
      (assignedReplicas - leaderReplica).foreach { replica =>
        val lastCaughtUpTimeMs = if (inSyncReplicas.contains(replica)) curTimeMs else 0L
        replica.resetLastCaughtUpTime(curLeaderLogEndOffset, curTimeMs, lastCaughtUpTimeMs)
      }

      if (isNewLeader) {
        // construct the high watermark metadata for the new leader replica
        leaderReplica.convertHWToLocalOffsetMetadata()
        // mark local replica as the leader after converting hw
        leaderReplicaIdOpt = Some(localBrokerId)
        // reset log end offset for remote replicas
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      // we may need to increment high watermark since ISR could be down to 1
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
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
  def makeFollower(controllerId: Int, partitionStateInfo: LeaderAndIsrRequest.PartitionState, correlationId: Int): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val newAssignedReplicas = partitionStateInfo.basePartitionState.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId = partitionStateInfo.basePartitionState.leader
      val oldLeaderEpoch = leaderEpoch
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.basePartitionState.controllerEpoch
      // add replicas that are new
      newAssignedReplicas.foreach(r => getOrCreateReplica(r, partitionStateInfo.isNew))
      // remove assigned replicas that have been removed by the controller
      (assignedReplicas.map(_.brokerId) -- newAssignedReplicas).foreach(removeReplica)
      inSyncReplicas = Set.empty[Replica]
      leaderEpoch = partitionStateInfo.basePartitionState.leaderEpoch
      leaderEpochStartOffsetOpt = None
      zkVersion = partitionStateInfo.basePartitionState.zkVersion

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
   * @return true if the leader's log start offset or high watermark have been updated
   */
  def updateReplicaLogReadResult(replica: Replica, logReadResult: LogReadResult): Boolean = {
    val replicaId = replica.brokerId
    // No need to calculate low watermark if there is no delayed DeleteRecordsRequest
    val oldLeaderLW = if (replicaManager.delayedDeleteRecordsPurgatory.delayed > 0) lowWatermarkIfLeader else -1L
    replica.updateLogReadResult(logReadResult)
    val newLeaderLW = if (replicaManager.delayedDeleteRecordsPurgatory.delayed > 0) lowWatermarkIfLeader else -1L
    // check if the LW of the partition has incremented
    // since the replica's logStartOffset may have incremented
    val leaderLWIncremented = newLeaderLW > oldLeaderLW
    // check if we need to expand ISR to include this replica
    // if it is not in the ISR yet
    val leaderHWIncremented = maybeExpandIsr(replicaId, logReadResult)

    val result = leaderLWIncremented || leaderHWIncremented
    // some delayed operations may be unblocked after HW or LW changed
    if (result)
      tryCompleteDelayedRequests()

    debug(s"Recorded replica $replicaId log end offset (LEO) position ${logReadResult.info.fetchOffsetMetadata.messageOffset}.")
    result
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
   *
   * @return true if the high watermark has been updated
   */
  def maybeExpandIsr(replicaId: Int, logReadResult: LogReadResult): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val replica = getReplica(replicaId).get
          val leaderHW = leaderReplica.highWatermark
          val fetchOffset = logReadResult.info.fetchOffsetMetadata.messageOffset
          if (!inSyncReplicas.contains(replica) &&
             assignedReplicas.map(_.brokerId).contains(replicaId) &&
             replica.logEndOffsetMetadata.offsetDiff(leaderHW) >= 0 &&
             leaderEpochStartOffsetOpt.exists(fetchOffset >= _)) {
            val newInSyncReplicas = inSyncReplicas + replica
            info(s"Expanding ISR from ${inSyncReplicas.map(_.brokerId).mkString(",")} " +
              s"to ${newInSyncReplicas.map(_.brokerId).mkString(",")}")
            // update ISR in ZK and cache
            updateIsr(newInSyncReplicas)
            replicaManager.isrExpandRate.mark()
          }
          // check if the HW of the partition can now be incremented
          // since the replica may already be in the ISR and its LEO has just incremented
          maybeIncrementLeaderHW(leaderReplica, logReadResult.fetchTimeMs)
        case None => false // nothing to do if no longer leader
      }
    }
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
    leaderReplicaIfLocal match {
      case Some(leaderReplica) =>
        // keep the current immutable replica list reference
        val curInSyncReplicas = inSyncReplicas

        if (isTraceEnabled) {
          def logEndOffsetString(r: Replica) = s"broker ${r.brokerId}: ${r.logEndOffset}"
          val (ackedReplicas, awaitingReplicas) = curInSyncReplicas.partition { replica =>
            replica.logEndOffset >= requiredOffset
          }
          trace(s"Progress awaiting ISR acks for offset $requiredOffset: acked: ${ackedReplicas.map(logEndOffsetString)}, " +
            s"awaiting ${awaitingReplicas.map(logEndOffsetString)}")
        }

        val minIsr = leaderReplica.log.get.config.minInSyncReplicas
        if (leaderReplica.highWatermark.messageOffset >= requiredOffset) {
          /*
           * The topic may be configured not to accept messages if there are not enough replicas in ISR
           * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
           */
          if (minIsr <= curInSyncReplicas.size)
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
  private def maybeIncrementLeaderHW(leaderReplica: Replica, curTime: Long = time.milliseconds): Boolean = {
    val allLogEndOffsets = assignedReplicas.filter { replica =>
      curTime - replica.lastCaughtUpTimeMs <= replicaLagTimeMaxMs || inSyncReplicas.contains(replica)
    }.map(_.logEndOffsetMetadata)
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)
    val oldHighWatermark = leaderReplica.highWatermark

    // Ensure that the high watermark increases monotonically. We also update the high watermark when the new
    // offset metadata is on a newer segment, which occurs whenever the log is rolled to a new segment.
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset ||
      (oldHighWatermark.messageOffset == newHighWatermark.messageOffset && oldHighWatermark.onOlderSegment(newHighWatermark))) {
      leaderReplica.highWatermark = newHighWatermark
      debug(s"High watermark updated to $newHighWatermark")
      true
    } else {
      def logEndOffsetString(r: Replica) = s"replica ${r.brokerId}: ${r.logEndOffsetMetadata}"
      debug(s"Skipping update high watermark since new hw $newHighWatermark is not larger than old hw $oldHighWatermark. " +
        s"All current LEOs are ${assignedReplicas.map(logEndOffsetString)}")
      false
    }
  }

  /**
   * The low watermark offset value, calculated only if the local replica is the partition leader
   * It is only used by leader broker to decide when DeleteRecordsRequest is satisfied. Its value is minimum logStartOffset of all live replicas
   * Low watermark will increase when the leader broker receives either FetchRequest or DeleteRecordsRequest.
   */
  def lowWatermarkIfLeader: Long = {
    if (!isLeaderReplicaLocal)
      throw new NotLeaderForPartitionException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
    val logStartOffsets = allReplicas.collect {
      case replica if replicaManager.metadataCache.isBrokerAlive(replica.brokerId) || replica.brokerId == Request.FutureLocalReplicaId => replica.logStartOffset
    }
    CoreUtils.min(logStartOffsets, 0L)
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(topicPartition)
    replicaManager.tryCompleteDelayedFetch(requestKey)
    replicaManager.tryCompleteDelayedProduce(requestKey)
    replicaManager.tryCompleteDelayedDeleteRecords(requestKey)
  }

  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if (outOfSyncReplicas.nonEmpty) {
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.nonEmpty)
            info("Shrinking ISR from %s to %s. Leader: (highWatermark: %d, endOffset: %d). Out of sync replicas: %s."
              .format(inSyncReplicas.map(_.brokerId).mkString(","),
                newInSyncReplicas.map(_.brokerId).mkString(","),
                leaderReplica.highWatermark.messageOffset,
                leaderReplica.logEndOffset,
                outOfSyncReplicas.map { replica =>
                  s"(brokerId: ${replica.brokerId}, endOffset: ${replica.logEndOffset})"
                }.mkString(" ")
              )
            )

            // update ISR in zk and in cache
            updateIsr(newInSyncReplicas)
            replicaManager.isrShrinkRate.mark()

            // we may need to increment high watermark since ISR could be down to 1
            maybeIncrementLeaderHW(leaderReplica)
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

  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
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
    val candidateReplicas = inSyncReplicas - leaderReplica

    val laggingReplicas = candidateReplicas.filter(r =>
      r.logEndOffset != leaderReplica.logEndOffset && (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if (laggingReplicas.nonEmpty)
      debug("Lagging replicas are %s".format(laggingReplicas.map(_.brokerId).mkString(",")))

    laggingReplicas
  }

  private def doAppendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Option[LogAppendInfo] = {
    // The read lock is needed to handle race condition if request handler thread tries to
    // remove future replica after receiving AlterReplicaLogDirsRequest.
    inReadLock(leaderIsrUpdateLock) {
      if (isFuture) {
        // Note the replica may be undefined if it is removed by a non-ReplicaAlterLogDirsThread before
        // this method is called
        futureLocalReplica.map { replica =>
          replica.log.get.appendAsFollower(records)
        }
      } else {
        // The read lock is needed to prevent the follower replica from being updated while ReplicaAlterDirThread
        // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
        Some(localReplicaOrException.log.get.appendAsFollower(records))
      }
    }
  }

  def appendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Option[LogAppendInfo] = {
    try {
      doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
    } catch {
      case e: UnexpectedAppendOffsetException =>
        val replica = if (isFuture) futureLocalReplicaOrException else localReplicaOrException
        val logEndOffset = replica.logEndOffset
        if (logEndOffset == replica.logStartOffset &&
            e.firstOffset < logEndOffset && e.lastOffset >= logEndOffset) {
          // This may happen if the log start offset on the leader (or current replica) falls in
          // the middle of the batch due to delete records request and the follower tries to
          // fetch its first offset from the leader.
          // We handle this case here instead of Log#append() because we will need to remove the
          // segment that start with log start offset and create a new one with earlier offset
          // (base offset of the batch), which will move recoveryPoint backwards, so we will need
          // to checkpoint the new recovery point before we append
          val replicaName = if (isFuture) "future replica" else "follower"
          info(s"Unexpected offset in append to $topicPartition. First offset ${e.firstOffset} is less than log start offset ${replica.logStartOffset}." +
               s" Since this is the first record to be appended to the $replicaName's log, will start the log from offset ${e.firstOffset}.")
          truncateFullyAndStartAt(e.firstOffset, isFuture)
          doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
        } else
          throw e
    }
  }

  def appendRecordsToLeader(records: MemoryRecords, isFromClient: Boolean, requiredAcks: Int = 0): LogAppendInfo = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val log = leaderReplica.log.get
          val minIsr = log.config.minInSyncReplicas
          val inSyncSize = inSyncReplicas.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException(s"The size of the current ISR ${inSyncReplicas.map(_.brokerId)} " +
              s"is insufficient to satisfy the min.isr requirement of $minIsr for partition $topicPartition")
          }

          val info = log.appendAsLeader(records, leaderEpoch = this.leaderEpoch, isFromClient,
            interBrokerProtocolVersion)

          // we may need to increment high watermark since ISR could be down to 1
          (info, maybeIncrementLeaderHW(leaderReplica))

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
      replicaManager.tryCompleteDelayedFetch(new TopicPartitionOperationKey(topicPartition))
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
    val localReplica = localReplicaWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)

    /* Read the LogOffsetMetadata prior to performing the read from the log.
     * We use the LogOffsetMetadata to determine if a particular replica is in-sync or not.
     * Using the log end offset after performing the read can lead to a race condition
     * where data gets appended to the log immediately after the replica has consumed from it
     * This can cause a replica to always be out of sync.
     */
    val initialHighWatermark = localReplica.highWatermark.messageOffset
    val initialLogStartOffset = localReplica.logStartOffset
    val initialLogEndOffset = localReplica.logEndOffset
    val initialLastStableOffset = localReplica.lastStableOffset.messageOffset

    val maxOffsetOpt = fetchIsolation match {
      case FetchLogEnd => None
      case FetchHighWatermark => Some(initialHighWatermark)
      case FetchTxnCommitted => Some(initialLastStableOffset)
    }

    val fetchedData = localReplica.log match {
      case Some(log) =>
        log.read(fetchOffset, maxBytes, maxOffsetOpt, minOneMessage,
          includeAbortedTxns = fetchIsolation == FetchTxnCommitted)

      case None =>
        error(s"Leader does not have a local log")
        FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY)
    }

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
                              fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset] = inReadLock(leaderIsrUpdateLock) {
    // decide whether to only fetch from leader
    val localReplica = localReplicaWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)

    val lastFetchableOffset = isolationLevel match {
      case Some(IsolationLevel.READ_COMMITTED) => localReplica.lastStableOffset.messageOffset
      case Some(IsolationLevel.READ_UNCOMMITTED) => localReplica.highWatermark.messageOffset
      case None => localReplica.logEndOffset
    }

    val epochLogString = if(currentLeaderEpoch.isPresent) {
      s"epoch ${currentLeaderEpoch.get}"
    } else {
      "unknown epoch"
    }

    // Only consider throwing an error if we get a client request (isolationLevel is defined) and the start offset
    // is lagging behind the high watermark
    val maybeOffsetsError: Option[ApiException] = leaderEpochStartOffsetOpt
      .filter(epochStart => isolationLevel.isDefined && epochStart > localReplica.highWatermark.messageOffset)
      .map(epochStart => Errors.OFFSET_NOT_AVAILABLE.exception(s"Failed to fetch offsets for " +
        s"partition $topicPartition with leader $epochLogString as this partition's " +
        s"high watermark (${localReplica.highWatermark.messageOffset}) is lagging behind the " +
        s"start offset from the beginning of this epoch ($epochStart)."))

    def getOffsetByTimestamp: Option[TimestampAndOffset] = {
      logManager.getLog(topicPartition).flatMap(log => log.fetchOffsetByTimestamp(timestamp))
    }

    // If we're in the lagging HW state after a leader election, throw OffsetNotAvailable for "latest" offset
    // or for a timestamp lookup that is beyond the last fetchable offset.
    timestamp match {
      case ListOffsetRequest.LATEST_TIMESTAMP =>
        maybeOffsetsError.map(e => throw e)
          .orElse(Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, lastFetchableOffset, Optional.of(leaderEpoch))))
      case ListOffsetRequest.EARLIEST_TIMESTAMP =>
        getOffsetByTimestamp
      case _ =>
        getOffsetByTimestamp.filter(timestampAndOffset => timestampAndOffset.offset < lastFetchableOffset)
          .orElse(maybeOffsetsError.map(e => throw e))
    }
  }

  def fetchOffsetSnapshot(currentLeaderEpoch: Optional[Integer],
                          fetchOnlyFromLeader: Boolean): LogOffsetSnapshot = inReadLock(leaderIsrUpdateLock) {
    // decide whether to only fetch from leader
    val localReplica = localReplicaWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)
    localReplica.offsetSnapshot
  }

  def fetchOffsetSnapshotOrError(currentLeaderEpoch: Optional[Integer],
                                 fetchOnlyFromLeader: Boolean): Either[LogOffsetSnapshot, Errors] = {
    inReadLock(leaderIsrUpdateLock) {
      getLocalReplica(localBrokerId, currentLeaderEpoch, fetchOnlyFromLeader)
        .left.map(_.offsetSnapshot)
    }
  }

  def legacyFetchOffsetsForTimestamp(timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long] = inReadLock(leaderIsrUpdateLock) {
    val localReplica = localReplicaWithEpochOrException(Optional.empty(), fetchOnlyFromLeader)
    val allOffsets = logManager.getLog(topicPartition) match {
      case Some(log) =>
        log.legacyFetchOffsetsBefore(timestamp, maxNumOffsets)
      case None =>
        if (timestamp == ListOffsetRequest.LATEST_TIMESTAMP || timestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
          Seq(0L)
        else
          Nil
    }

    if (!isFromConsumer) {
      allOffsets
    } else {
      val hw = localReplica.highWatermark.messageOffset
      if (allOffsets.exists(_ > hw))
        hw +: allOffsets.dropWhile(_ > hw)
      else
        allOffsets
    }
  }

  def logStartOffset: Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal.map(_.log.get.logStartOffset).getOrElse(-1)
    }
  }

  /**
   * Update logStartOffset and low watermark if 1) offset <= highWatermark and 2) it is the leader replica.
   * This function can trigger log segment deletion and log rolling.
   *
   * Return low watermark of the partition.
   */
  def deleteRecordsOnLeader(offset: Long): LogDeleteRecordsResult = inReadLock(leaderIsrUpdateLock) {
    leaderReplicaIfLocal match {
      case Some(leaderReplica) =>
        if (!leaderReplica.log.get.config.delete)
          throw new PolicyViolationException(s"Records of partition $topicPartition can not be deleted due to the configured policy")

        val convertedOffset = if (offset == DeleteRecordsRequest.HIGH_WATERMARK)
          leaderReplica.highWatermark.messageOffset
        else
          offset

        if (convertedOffset < 0)
          throw new OffsetOutOfRangeException(s"The offset $convertedOffset for partition $topicPartition is not valid")

        leaderReplica.maybeIncrementLogStartOffset(convertedOffset)
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
  def truncateTo(offset: Long, isFuture: Boolean) {
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
  def truncateFullyAndStartAt(newOffset: Long, isFuture: Boolean) {
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
      val localReplicaOrError = getLocalReplica(localBrokerId, currentLeaderEpoch, fetchOnlyFromLeader)
      localReplicaOrError match {
        case Left(replica) =>
          replica.endOffsetForEpoch(leaderEpoch) match {
            case Some(epochAndOffset) => new EpochEndOffset(NONE, epochAndOffset.leaderEpoch, epochAndOffset.offset)
            case None => new EpochEndOffset(NONE, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
          }
        case Right(error) =>
          new EpochEndOffset(error, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
    }
  }

  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(_.brokerId).toList, zkVersion)
    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topicPartition, newLeaderAndIsr,
      controllerEpoch)

    if (updateSucceeded) {
      replicaManager.recordIsrChange(topicPartition)
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      replicaManager.failedIsrUpdatesRate.mark()
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
    removeMetric("UnderMinIsr", tags)
    removeMetric("InSyncReplicasCount", tags)
    removeMetric("ReplicasCount", tags)
    removeMetric("LastStableOffsetLag", tags)
  }

  override def equals(that: Any): Boolean = that match {
    case other: Partition => partitionId == other.partitionId && topic == other.topic && isOffline == other.isOffline
    case _ => false
  }

  override def hashCode: Int =
    31 + topic.hashCode + 17 * partitionId + (if (isOffline) 1 else 0)

  override def toString(): String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AllReplicas: " + allReplicasMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString
  }
}
