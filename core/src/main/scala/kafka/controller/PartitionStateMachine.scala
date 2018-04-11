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
package kafka.controller

import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import kafka.server.KafkaConfig
import kafka.utils.Logging
import kafka.zk.{KafkaZkClient, TopicPartitionStateZNode}
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.mutable


/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 */
class PartitionStateMachine(config: KafkaConfig,
                            stateChangeLogger: StateChangeLogger,
                            controllerContext: ControllerContext,
                            topicDeletionManager: TopicDeletionManager,
                            zkClient: KafkaZkClient,
                            partitionState: mutable.Map[TopicPartition, PartitionState],
                            controllerBrokerRequestBatch: ControllerBrokerRequestBatch) extends Logging {
  private val controllerId = config.brokerId

  this.logIdent = s"[PartitionStateMachine controllerId=$controllerId] "

  /**
   * Invoked on successful controller election.
   */
  def startup() {
    info("Initializing partition state")
    initializePartitionState()
    info("Triggering online partition state changes")
    triggerOnlinePartitionStateChange()
    info(s"Started partition state machine with initial state -> $partitionState")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    partitionState.clear()
    info("Stopped partition state machine")
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   */
  private def initializePartitionState() {
    for (topicPartition <- controllerContext.allPartitions) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      controllerContext.partitionLeadershipInfo.get(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          if (controllerContext.isReplicaOnline(currentLeaderIsrAndEpoch.leaderAndIsr.leader, topicPartition))
          // leader is alive
            partitionState.put(topicPartition, OnlinePartition)
          else
            partitionState.put(topicPartition, OfflinePartition)
        case None =>
          partitionState.put(topicPartition, NewPartition)
      }
    }
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   */
  def triggerOnlinePartitionStateChange() {
    // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions
    // that belong to topics to be deleted
    val partitionsToTrigger = partitionState.filter { case (partition, partitionState) =>
      !topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic) &&
        (partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
    }.keys.toSeq
    handleStateChanges(partitionsToTrigger, OnlinePartition, Option(OfflinePartitionLeaderElectionStrategy))
    // TODO: If handleStateChanges catches an exception, it is not enough to bail out and log an error.
    // It is important to trigger leader election for those partitions.
  }

  def handleStateChanges(partitions: Seq[TopicPartition], targetState: PartitionState,
                         partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy] = None): Unit = {
    if (partitions.nonEmpty) {
      try {
        controllerBrokerRequestBatch.newBatch()
        doHandleStateChanges(partitions, targetState, partitionLeaderElectionStrategyOpt)
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
      } catch {
        case e: Throwable => error(s"Error while moving some partitions to $targetState state", e)
      }
    }
  }

  def partitionsInState(state: PartitionState): Set[TopicPartition] = {
    partitionState.filter { case (_, s) => s == state }.keySet.toSet
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition
   * @param partitions  The partitions for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
   */
  private def doHandleStateChanges(partitions: Seq[TopicPartition], targetState: PartitionState,
                           partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]): Unit = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    partitions.foreach(partition => partitionState.getOrElseUpdate(partition, NonExistentPartition))
    val (validPartitions, invalidPartitions) = partitions.partition(partition => isValidTransition(partition, targetState))
    invalidPartitions.foreach(partition => logInvalidTransition(partition, targetState))
    targetState match {
      case NewPartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState with " +
            s"assigned replicas ${controllerContext.partitionReplicaAssignment(partition).mkString(",")}")
          partitionState.put(partition, NewPartition)
        }
      case OnlinePartition =>
        val uninitializedPartitions = validPartitions.filter(partition => partitionState(partition) == NewPartition)
        val partitionsToElectLeader = validPartitions.filter(partition => partitionState(partition) == OfflinePartition || partitionState(partition) == OnlinePartition)
        if (uninitializedPartitions.nonEmpty) {
          val successfulInitializations = initializeLeaderAndIsrForPartitions(uninitializedPartitions)
          successfulInitializations.foreach { partition =>
            stateChangeLog.trace(s"Changed partition $partition from ${partitionState(partition)} to $targetState with state " +
              s"${controllerContext.partitionLeadershipInfo(partition).leaderAndIsr}")
            partitionState.put(partition, OnlinePartition)
          }
        }
        if (partitionsToElectLeader.nonEmpty) {
          val successfulElections = electLeaderForPartitions(partitionsToElectLeader, partitionLeaderElectionStrategyOpt.get)
          successfulElections.foreach { partition =>
            stateChangeLog.trace(s"Changed partition $partition from ${partitionState(partition)} to $targetState with state " +
              s"${controllerContext.partitionLeadershipInfo(partition).leaderAndIsr}")
            partitionState.put(partition, OnlinePartition)
          }
        }
      case OfflinePartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          partitionState.put(partition, OfflinePartition)
        }
      case NonExistentPartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          partitionState.put(partition, NonExistentPartition)
        }
    }
  }

  /**
   * Initialize leader and isr partition state in zookeeper.
   * @param partitions The partitions  that we're trying to initialize.
   * @return The partitions that have been successfully initialized.
   */
  private def initializeLeaderAndIsrForPartitions(partitions: Seq[TopicPartition]): Seq[TopicPartition] = {
    val successfulInitializations = mutable.Buffer.empty[TopicPartition]
    val replicasPerPartition = partitions.map(partition => partition -> controllerContext.partitionReplicaAssignment(partition))
    val liveReplicasPerPartition = replicasPerPartition.map { case (partition, replicas) =>
        val liveReplicasForPartition = replicas.filter(replica => controllerContext.isReplicaOnline(replica, partition))
        partition -> liveReplicasForPartition
    }
    val (partitionsWithoutLiveReplicas, partitionsWithLiveReplicas) = liveReplicasPerPartition.partition { case (_, liveReplicas) => liveReplicas.isEmpty }

    partitionsWithoutLiveReplicas.foreach { case (partition, replicas) =>
      val failMsg = s"Controller $controllerId epoch ${controllerContext.epoch} encountered error during state change of " +
        s"partition $partition from New to Online, assigned replicas are " +
        s"[${replicas.mkString(",")}], live brokers are [${controllerContext.liveBrokerIds}]. No assigned " +
        "replica is alive."
      logFailedStateChange(partition, NewPartition, OnlinePartition, new StateChangeFailedException(failMsg))
    }
    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
      partition -> leaderIsrAndControllerEpoch
    }.toMap
    val createResponses = try {
      zkClient.createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs)
    } catch {
      case e: Exception =>
        partitionsWithLiveReplicas.foreach { case (partition,_) => logFailedStateChange(partition, partitionState(partition), NewPartition, e) }
        Seq.empty
    }
    createResponses.foreach { createResponse =>
      val code = createResponse.resultCode
      val partition = createResponse.ctx.get.asInstanceOf[TopicPartition]
      val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochs(partition)
      if (code == Code.OK) {
        controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(leaderIsrAndControllerEpoch.leaderAndIsr.isr,
          partition, leaderIsrAndControllerEpoch, controllerContext.partitionReplicaAssignment(partition), isNew = true)
        successfulInitializations += partition
      } else {
        logFailedStateChange(partition, NewPartition, OnlinePartition, code)
      }
    }
    successfulInitializations
  }

  /**
   * Repeatedly attempt to elect leaders for multiple partitions until there are no more remaining partitions to retry.
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return The partitions that successfully had a leader elected.
   */
  private def electLeaderForPartitions(partitions: Seq[TopicPartition], partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy): Seq[TopicPartition] = {
    val successfulElections = mutable.Buffer.empty[TopicPartition]
    var remaining = partitions
    while (remaining.nonEmpty) {
      val (success, updatesToRetry, failedElections) = doElectLeaderForPartitions(partitions, partitionLeaderElectionStrategy)
      remaining = updatesToRetry
      successfulElections ++= success
      failedElections.foreach { case (partition, e) =>
        logFailedStateChange(partition, partitionState(partition), OnlinePartition, e)
      }
    }
    successfulElections
  }

  /**
   * Try to elect leaders for multiple partitions.
   * Electing a leader for a partition updates partition state in zookeeper.
   *
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A tuple of three values:
   *         1. The partitions that successfully had a leader elected.
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   *         3. Exceptions corresponding to failed elections that should not be retried.
   */
  private def doElectLeaderForPartitions(partitions: Seq[TopicPartition], partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy):
  (Seq[TopicPartition], Seq[TopicPartition], Map[TopicPartition, Exception]) = {
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (Seq.empty, Seq.empty, partitions.map(_ -> e).toMap)
    }
    val failedElections = mutable.Map.empty[TopicPartition, Exception]
    val leaderIsrAndControllerEpochPerPartition = mutable.Buffer.empty[(TopicPartition, LeaderIsrAndControllerEpoch)]
    getDataResponses.foreach { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      val currState = partitionState(partition)
      if (getDataResponse.resultCode == Code.OK) {
        val leaderIsrAndControllerEpochOpt = TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat)
        if (leaderIsrAndControllerEpochOpt.isEmpty) {
          val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
          failedElections.put(partition, exception)
        }
        leaderIsrAndControllerEpochPerPartition += partition -> leaderIsrAndControllerEpochOpt.get
      } else if (getDataResponse.resultCode == Code.NONODE) {
        val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
        failedElections.put(partition, exception)
      } else {
        failedElections.put(partition, getDataResponse.resultException.get)
      }
    }
    val (invalidPartitionsForElection, validPartitionsForElection) = leaderIsrAndControllerEpochPerPartition.partition { case (partition, leaderIsrAndControllerEpoch) =>
      leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch
    }
    invalidPartitionsForElection.foreach { case (partition, leaderIsrAndControllerEpoch) =>
      val failMsg = s"aborted leader election for partition $partition since the LeaderAndIsr path was " +
        s"already written by another controller. This probably means that the current controller $controllerId went through " +
        s"a soft failure and another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}."
      failedElections.put(partition, new StateChangeFailedException(failMsg))
    }
    if (validPartitionsForElection.isEmpty) {
      return (Seq.empty, Seq.empty, failedElections.toMap)
    }
    val shuttingDownBrokers  = controllerContext.shuttingDownBrokerIds.toSet
    val (partitionsWithoutLeaders, partitionsWithLeaders) = partitionLeaderElectionStrategy match {
      case OfflinePartitionLeaderElectionStrategy =>
        leaderForOffline(validPartitionsForElection).partition { case (_, newLeaderAndIsrOpt, _) => newLeaderAndIsrOpt.isEmpty }
      case ReassignPartitionLeaderElectionStrategy =>
        leaderForReassign(validPartitionsForElection).partition { case (_, newLeaderAndIsrOpt, _) => newLeaderAndIsrOpt.isEmpty }
      case PreferredReplicaPartitionLeaderElectionStrategy =>
        leaderForPreferredReplica(validPartitionsForElection).partition { case (_, newLeaderAndIsrOpt, _) => newLeaderAndIsrOpt.isEmpty }
      case ControlledShutdownPartitionLeaderElectionStrategy =>
        leaderForControlledShutdown(validPartitionsForElection, shuttingDownBrokers).partition { case (_, newLeaderAndIsrOpt, _) => newLeaderAndIsrOpt.isEmpty }
    }
    partitionsWithoutLeaders.foreach { case (partition, leaderAndIsrOpt, recipients) =>
      val failMsg = s"Failed to elect leader for partition $partition under strategy $partitionLeaderElectionStrategy"
      failedElections.put(partition, new StateChangeFailedException(failMsg))
    }
    val recipientsPerPartition = partitionsWithLeaders.map { case (partition, leaderAndIsrOpt, recipients) => partition -> recipients }.toMap
    val adjustedLeaderAndIsrs = partitionsWithLeaders.map { case (partition, leaderAndIsrOpt, recipients) => partition -> leaderAndIsrOpt.get }.toMap
    val UpdateLeaderAndIsrResult(successfulUpdates, updatesToRetry, failedUpdates) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch)
    successfulUpdates.foreach { case (partition, leaderAndIsr) =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
      controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
      controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipientsPerPartition(partition), partition,
        leaderIsrAndControllerEpoch, replicas, isNew = false)
    }
    (successfulUpdates.keys.toSeq, updatesToRetry, failedElections.toMap ++ failedUpdates)
  }

  private def leaderForOffline(leaderIsrAndControllerEpochs: Seq[(TopicPartition, LeaderIsrAndControllerEpoch)]):
  Seq[(TopicPartition, Option[LeaderAndIsr], Seq[Int])] = {
    val (partitionsWithNoLiveInSyncReplicas, partitionsWithLiveInSyncReplicas) = leaderIsrAndControllerEpochs.partition { case (partition, leaderIsrAndControllerEpoch) =>
      val liveInSyncReplicas = leaderIsrAndControllerEpoch.leaderAndIsr.isr.filter(replica => controllerContext.isReplicaOnline(replica, partition))
      liveInSyncReplicas.isEmpty
    }
    val (logConfigs, failed) = zkClient.getLogConfigs(partitionsWithNoLiveInSyncReplicas.map { case (partition, _) => partition.topic }, config.originals())
    val partitionsWithUncleanLeaderElectionState = partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderIsrAndControllerEpoch) =>
      if (failed.contains(partition.topic)) {
        logFailedStateChange(partition, partitionState(partition), OnlinePartition, failed(partition.topic))
        (partition, None, false)
      } else {
        (partition, Option(leaderIsrAndControllerEpoch), logConfigs(partition.topic).uncleanLeaderElectionEnable.booleanValue())
      }
    } ++ partitionsWithLiveInSyncReplicas.map { case (partition, leaderIsrAndControllerEpoch) => (partition, Option(leaderIsrAndControllerEpoch), false) }
    partitionsWithUncleanLeaderElectionState.map { case (partition, leaderIsrAndControllerEpochOpt, uncleanLeaderElectionEnabled) =>
      val assignment = controllerContext.partitionReplicaAssignment(partition)
      val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
      if (leaderIsrAndControllerEpochOpt.nonEmpty) {
        val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochOpt.get
        val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr
        val leaderOpt = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(assignment, isr, liveReplicas.toSet, uncleanLeaderElectionEnabled, controllerContext)
        val newLeaderAndIsrOpt = leaderOpt.map { leader =>
          val newIsr = if (isr.contains(leader)) isr.filter(replica => controllerContext.isReplicaOnline(replica, partition))
          else List(leader)
          leaderIsrAndControllerEpoch.leaderAndIsr.newLeaderAndIsr(leader, newIsr)
        }
        (partition, newLeaderAndIsrOpt, liveReplicas)
      } else {
        (partition, None, liveReplicas)
      }
    }
  }

  private def leaderForReassign(leaderIsrAndControllerEpochs: Seq[(TopicPartition, LeaderIsrAndControllerEpoch)]):
  Seq[(TopicPartition, Option[LeaderAndIsr], Seq[Int])] = {
    leaderIsrAndControllerEpochs.map { case (partition, leaderIsrAndControllerEpoch) =>
      val reassignment = controllerContext.partitionsBeingReassigned(partition).newReplicas
      val liveReplicas = reassignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
      val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr
      val leaderOpt = PartitionLeaderElectionAlgorithms.reassignPartitionLeaderElection(reassignment, isr, liveReplicas.toSet)
      val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderIsrAndControllerEpoch.leaderAndIsr.newLeader(leader))
      (partition, newLeaderAndIsrOpt, reassignment)
    }
  }

  private def leaderForPreferredReplica(leaderIsrAndControllerEpochs: Seq[(TopicPartition, LeaderIsrAndControllerEpoch)]):
  Seq[(TopicPartition, Option[LeaderAndIsr], Seq[Int])] = {
    leaderIsrAndControllerEpochs.map { case (partition, leaderIsrAndControllerEpoch) =>
      val assignment = controllerContext.partitionReplicaAssignment(partition)
      val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
      val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr
      val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment, isr, liveReplicas.toSet)
      val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderIsrAndControllerEpoch.leaderAndIsr.newLeader(leader))
      (partition, newLeaderAndIsrOpt, assignment)
    }
  }

  private def leaderForControlledShutdown(leaderIsrAndControllerEpochs: Seq[(TopicPartition, LeaderIsrAndControllerEpoch)], shuttingDownBrokers: Set[Int]):
  Seq[(TopicPartition, Option[LeaderAndIsr], Seq[Int])] = {
    leaderIsrAndControllerEpochs.map { case (partition, leaderIsrAndControllerEpoch) =>
      val assignment = controllerContext.partitionReplicaAssignment(partition)
      val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
      val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr
      val leaderOpt = PartitionLeaderElectionAlgorithms.controlledShutdownPartitionLeaderElection(assignment, isr, liveReplicas.toSet, shuttingDownBrokers)
      val newIsr = isr.filter(replica => !controllerContext.shuttingDownBrokerIds.contains(replica))
      val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderIsrAndControllerEpoch.leaderAndIsr.newLeaderAndIsr(leader, newIsr))
      (partition, newLeaderAndIsrOpt, liveReplicas)
    }
  }

  private def isValidTransition(partition: TopicPartition, targetState: PartitionState) =
    targetState.validPreviousStates.contains(partitionState(partition))

  private def logInvalidTransition(partition: TopicPartition, targetState: PartitionState): Unit = {
    val currState = partitionState(partition)
    val e = new IllegalStateException(s"Partition $partition should be in one of " +
      s"${targetState.validPreviousStates.mkString(",")} states before moving to $targetState state. Instead it is in " +
      s"$currState state")
    logFailedStateChange(partition, currState, targetState, e)
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, code: Code): Unit = {
    logFailedStateChange(partition, currState, targetState, KeeperException.create(code))
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} failed to change state for partition $partition " +
        s"from $currState to $targetState", t)
  }
}

object PartitionLeaderElectionAlgorithms {
  def offlinePartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], uncleanLeaderElectionEnabled: Boolean, controllerContext: ControllerContext): Option[Int] = {
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id)).orElse {
      if (uncleanLeaderElectionEnabled) {
        val leaderOpt = assignment.find(liveReplicas.contains)
        if (!leaderOpt.isEmpty)
          controllerContext.stats.uncleanLeaderElectionRate.mark()
        leaderOpt
      } else {
        None
      }
    }
  }

  def reassignPartitionLeaderElection(reassignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    reassignment.find(id => liveReplicas.contains(id) && isr.contains(id))
  }

  def preferredReplicaPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    assignment.headOption.filter(id => liveReplicas.contains(id) && isr.contains(id))
  }

  def controlledShutdownPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], shuttingDownBrokers: Set[Int]): Option[Int] = {
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id) && !shuttingDownBrokers.contains(id))
  }
}

sealed trait PartitionLeaderElectionStrategy
case object OfflinePartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
case object ReassignPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
case object PreferredReplicaPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
case object ControlledShutdownPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

sealed trait PartitionState {
  def state: Byte
  def validPreviousStates: Set[PartitionState]
}

case object NewPartition extends PartitionState {
  val state: Byte = 0
  val validPreviousStates: Set[PartitionState] = Set(NonExistentPartition)
}

case object OnlinePartition extends PartitionState {
  val state: Byte = 1
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object OfflinePartition extends PartitionState {
  val state: Byte = 2
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object NonExistentPartition extends PartitionState {
  val state: Byte = 3
  val validPreviousStates: Set[PartitionState] = Set(OfflinePartition)
}
