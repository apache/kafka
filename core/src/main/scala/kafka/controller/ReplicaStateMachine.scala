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
import org.apache.zookeeper.KeeperException.Code

import scala.collection.mutable

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous state is ReplicaDeletionStarted
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful
 */
class ReplicaStateMachine(config: KafkaConfig,
                          stateChangeLogger: StateChangeLogger,
                          controllerContext: ControllerContext,
                          topicDeletionManager: TopicDeletionManager,
                          zkClient: KafkaZkClient,
                          replicaState: mutable.Map[PartitionAndReplica, ReplicaState],
                          controllerBrokerRequestBatch: ControllerBrokerRequestBatch) extends Logging {
  private val controllerId = config.brokerId

  this.logIdent = s"[ReplicaStateMachine controllerId=$controllerId] "

  /**
   * Invoked on successful controller election.
   */
  def startup() {
    info("Initializing replica state")
    initializeReplicaState()
    info("Triggering online replica state changes")
    handleStateChanges(controllerContext.allLiveReplicas().toSeq, OnlineReplica)
    info(s"Started replica state machine with initial state -> $replicaState")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    replicaState.clear()
    info("Stopped replica state machine")
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   */
  private def initializeReplicaState() {
    controllerContext.allPartitions.foreach { partition =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      replicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(partition, replicaId)
        if (controllerContext.isReplicaOnline(replicaId, partition))
          replicaState.put(partitionAndReplica, OnlineReplica)
        else
        // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
        // This is required during controller failover since during controller failover a broker can go down,
        // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
          replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
      }
    }
  }

  def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState,
                         callbacks: Callbacks = new Callbacks()): Unit = {
    if (replicas.nonEmpty) {
      try {
        controllerBrokerRequestBatch.newBatch()
        replicas.groupBy(_.replica).map { case (replicaId, replicas) =>
          val partitions = replicas.map(_.topicPartition)
          doHandleStateChanges(replicaId, partitions, targetState, callbacks)
        }
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
      } catch {
        case e: Throwable => error(s"Error while moving some replicas to $targetState state", e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache
   *
   * @param replicaId The replica for which the state transition is invoked
   * @param partitions The partitions on this replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
  private def doHandleStateChanges(replicaId: Int, partitions: Seq[TopicPartition], targetState: ReplicaState,
                                   callbacks: Callbacks): Unit = {
    val replicas = partitions.map(partition => PartitionAndReplica(partition, replicaId))
    replicas.foreach(replica => replicaState.getOrElseUpdate(replica, NonExistentReplica))
    val (validReplicas, invalidReplicas) = replicas.partition(replica => isValidTransition(replica, targetState))
    invalidReplicas.foreach(replica => logInvalidTransition(replica, targetState))
    targetState match {
      case NewReplica =>
        validReplicas.foreach { replica =>
          val partition = replica.topicPartition
          controllerContext.partitionLeadershipInfo.get(partition) match {
            case Some(leaderIsrAndControllerEpoch) =>
              if (leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId) {
                val exception = new StateChangeFailedException(s"Replica $replicaId for partition $partition cannot be moved to NewReplica state as it is being requested to become leader")
                logFailedStateChange(replica, replicaState(replica), OfflineReplica, exception)
              } else {
                controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                  replica.topicPartition,
                  leaderIsrAndControllerEpoch,
                  controllerContext.partitionReplicaAssignment(replica.topicPartition),
                  isNew = true)
                logSuccessfulTransition(replicaId, partition, replicaState(replica), NewReplica)
                replicaState.put(replica, NewReplica)
              }
            case None =>
              logSuccessfulTransition(replicaId, partition, replicaState(replica), NewReplica)
              replicaState.put(replica, NewReplica)
          }
        }
      case OnlineReplica =>
        validReplicas.foreach { replica =>
          val partition = replica.topicPartition
          replicaState(replica) match {
            case NewReplica =>
              val assignment = controllerContext.partitionReplicaAssignment(partition)
              if (!assignment.contains(replicaId)) {
                controllerContext.updatePartitionReplicaAssignment(partition, assignment :+ replicaId)
              }
            case _ =>
              controllerContext.partitionLeadershipInfo.get(partition) match {
                case Some(leaderIsrAndControllerEpoch) =>
                  controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                    replica.topicPartition,
                    leaderIsrAndControllerEpoch,
                    controllerContext.partitionReplicaAssignment(partition), isNew = false)
                case None =>
              }
          }
          logSuccessfulTransition(replicaId, partition, replicaState(replica), OnlineReplica)
          replicaState.put(replica, OnlineReplica)
        }
      case OfflineReplica =>
        validReplicas.foreach { replica =>
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition,
            deletePartition = false, (_, _) => ())
        }
        val (replicasWithLeadershipInfo, replicasWithoutLeadershipInfo) = validReplicas.partition { replica =>
          controllerContext.partitionLeadershipInfo.contains(replica.topicPartition)
        }
        val updatedLeaderIsrAndControllerEpochs = removeReplicasFromIsr(replicaId, replicasWithLeadershipInfo.map(_.topicPartition))
        updatedLeaderIsrAndControllerEpochs.foreach { case (partition, leaderIsrAndControllerEpoch) =>
          if (!topicDeletionManager.isPartitionToBeDeleted(partition)) {
            val recipients = controllerContext.partitionReplicaAssignment(partition).filterNot(_ == replicaId)
            controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipients,
              partition,
              leaderIsrAndControllerEpoch,
              controllerContext.partitionReplicaAssignment(partition), isNew = false)
          }
          val replica = PartitionAndReplica(partition, replicaId)
          logSuccessfulTransition(replicaId, partition, replicaState(replica), OfflineReplica)
          replicaState.put(replica, OfflineReplica)
        }

        replicasWithoutLeadershipInfo.foreach { replica =>
          logSuccessfulTransition(replicaId, replica.topicPartition, replicaState(replica), OfflineReplica)
          replicaState.put(replica, OfflineReplica)
        }
      case ReplicaDeletionStarted =>
        validReplicas.foreach { replica =>
          logSuccessfulTransition(replicaId, replica.topicPartition, replicaState(replica), ReplicaDeletionStarted)
          replicaState.put(replica, ReplicaDeletionStarted)
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId),
            replica.topicPartition,
            deletePartition = true,
            callbacks.stopReplicaResponseCallback)
        }
      case ReplicaDeletionIneligible =>
        validReplicas.foreach { replica =>
          logSuccessfulTransition(replicaId, replica.topicPartition, replicaState(replica), ReplicaDeletionIneligible)
          replicaState.put(replica, ReplicaDeletionIneligible)
        }
      case ReplicaDeletionSuccessful =>
        validReplicas.foreach { replica =>
          logSuccessfulTransition(replicaId, replica.topicPartition, replicaState(replica), ReplicaDeletionSuccessful)
          replicaState.put(replica, ReplicaDeletionSuccessful)
        }
      case NonExistentReplica =>
        validReplicas.foreach { replica =>
          val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(replica.topicPartition)
          controllerContext.updatePartitionReplicaAssignment(replica.topicPartition, currentAssignedReplicas.filterNot(_ == replica.replica))
          logSuccessfulTransition(replicaId, replica.topicPartition, replicaState(replica), NonExistentReplica)
          replicaState.remove(replica)
        }
    }
  }

  /**
   * Repeatedly attempt to remove a replica from the isr of multiple partitions until there are no more remaining partitions
   * to retry.
   * @param replicaId The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return The updated LeaderIsrAndControllerEpochs of all partitions for which we successfully removed the replica from isr.
   */
  private def removeReplicasFromIsr(replicaId: Int, partitions: Seq[TopicPartition]):
  Map[TopicPartition, LeaderIsrAndControllerEpoch] = {
    var results = Map.empty[TopicPartition, LeaderIsrAndControllerEpoch]
    var remaining = partitions
    while (remaining.nonEmpty) {
      val (successfulRemovals, removalsToRetry, failedRemovals) = doRemoveReplicasFromIsr(replicaId, remaining)
      results ++= successfulRemovals
      remaining = removalsToRetry
      failedRemovals.foreach { case (partition, e) =>
        val replica = PartitionAndReplica(partition, replicaId)
        logFailedStateChange(replica, replicaState(replica), OfflineReplica, e)
      }
    }
    results
  }

  /**
   * Try to remove a replica from the isr of multiple partitions.
   * Removing a replica from isr updates partition state in zookeeper.
   *
   * @param replicaId The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return A tuple of three values:
   *         1. The updated LeaderIsrAndControllerEpochs of all partitions for which we successfully removed the replica from isr.
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   *         3. Exceptions corresponding to failed removals that should not be retried.
   */
  private def doRemoveReplicasFromIsr(replicaId: Int, partitions: Seq[TopicPartition]):
  (Map[TopicPartition, LeaderIsrAndControllerEpoch],
    Seq[TopicPartition],
    Map[TopicPartition, Exception]) = {
    val (leaderAndIsrs, partitionsWithNoLeaderAndIsrInZk, failedStateReads) = getTopicPartitionStatesFromZk(partitions)
    val (leaderAndIsrsWithReplica, leaderAndIsrsWithoutReplica) = leaderAndIsrs.partition { case (_, leaderAndIsr) => leaderAndIsr.isr.contains(replicaId) }
    val adjustedLeaderAndIsrs = leaderAndIsrsWithReplica.mapValues { leaderAndIsr =>
      val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
      val adjustedIsr = if (leaderAndIsr.isr.size == 1) leaderAndIsr.isr else leaderAndIsr.isr.filter(_ != replicaId)
      leaderAndIsr.newLeaderAndIsr(newLeader, adjustedIsr)
    }
    val UpdateLeaderAndIsrResult(successfulUpdates, updatesToRetry, failedUpdates) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch)
    val exceptionsForPartitionsWithNoLeaderAndIsrInZk = partitionsWithNoLeaderAndIsrInZk.flatMap { partition =>
      if (!topicDeletionManager.isPartitionToBeDeleted(partition)) {
        val exception = new StateChangeFailedException(s"Failed to change state of replica $replicaId for partition $partition since the leader and isr path in zookeeper is empty")
        Option(partition -> exception)
      } else None
    }.toMap
    val leaderIsrAndControllerEpochs = (leaderAndIsrsWithoutReplica ++ successfulUpdates).map { case (partition, leaderAndIsr) =>
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
      controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
      partition -> leaderIsrAndControllerEpoch
    }
    (leaderIsrAndControllerEpochs, updatesToRetry, failedStateReads ++ exceptionsForPartitionsWithNoLeaderAndIsrInZk ++ failedUpdates)
  }

  /**
   * Gets the partition state from zookeeper
   * @param partitions the partitions whose state we want from zookeeper
   * @return A tuple of three values:
   *         1. The LeaderAndIsrs of partitions whose state we successfully read from zookeeper
   *         2. The partitions that had no leader and isr state in zookeeper. This happens if the controller
   *         didn't finish partition initialization.
   *         3. Exceptions corresponding to failed zookeeper lookups or states whose controller epoch exceeds our current epoch.
   */
  private def getTopicPartitionStatesFromZk(partitions: Seq[TopicPartition]):
  (Map[TopicPartition, LeaderAndIsr],
    Seq[TopicPartition],
    Map[TopicPartition, Exception]) = {
    val leaderAndIsrs = mutable.Map.empty[TopicPartition, LeaderAndIsr]
    val partitionsWithNoLeaderAndIsrInZk = mutable.Buffer.empty[TopicPartition]
    val failed = mutable.Map.empty[TopicPartition, Exception]
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        partitions.foreach(partition => failed.put(partition, e))
        return (leaderAndIsrs.toMap, partitionsWithNoLeaderAndIsrInZk, failed.toMap)
    }
    getDataResponses.foreach { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      if (getDataResponse.resultCode == Code.OK) {
        val leaderIsrAndControllerEpochOpt = TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat)
        if (leaderIsrAndControllerEpochOpt.isEmpty) {
          partitionsWithNoLeaderAndIsrInZk += partition
        } else {
          val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochOpt.get
          if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
            val exception = new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              s"means the current controller with epoch ${controllerContext.epoch} went through a soft failure and another " +
              s"controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}. Aborting state change by this controller")
            failed.put(partition, exception)
          } else {
            leaderAndIsrs.put(partition, leaderIsrAndControllerEpoch.leaderAndIsr)
          }
        }
      } else if (getDataResponse.resultCode == Code.NONODE) {
        partitionsWithNoLeaderAndIsrInZk += partition
      } else {
        failed.put(partition, getDataResponse.resultException.get)
      }
    }
    (leaderAndIsrs.toMap, partitionsWithNoLeaderAndIsrInZk, failed.toMap)
  }

  def isAtLeastOneReplicaInDeletionStartedState(topic: String): Boolean = {
    controllerContext.replicasForTopic(topic).exists(replica => replicaState(replica) == ReplicaDeletionStarted)
  }

  def replicasInState(topic: String, state: ReplicaState): Set[PartitionAndReplica] = {
    replicaState.filter { case (replica, s) => replica.topic.equals(topic) && s == state }.keySet.toSet
  }

  def areAllReplicasForTopicDeleted(topic: String): Boolean = {
    controllerContext.replicasForTopic(topic).forall(replica => replicaState(replica) == ReplicaDeletionSuccessful)
  }

  def isAnyReplicaInState(topic: String, state: ReplicaState): Boolean = {
    replicaState.exists { case (replica, s) => replica.topic.equals(topic) && s == state}
  }

  private def isValidTransition(replica: PartitionAndReplica, targetState: ReplicaState) =
    targetState.validPreviousStates.contains(replicaState(replica))

  private def logSuccessfulTransition(replicaId: Int, partition: TopicPartition, currState: ReplicaState, targetState: ReplicaState): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .trace(s"Changed state of replica $replicaId for partition $partition from $currState to $targetState")
  }

  private def logInvalidTransition(replica: PartitionAndReplica, targetState: ReplicaState): Unit = {
    val currState = replicaState(replica)
    val e = new IllegalStateException(s"Replica $replica should be in the ${targetState.validPreviousStates.mkString(",")} " +
      s"states before moving to $targetState state. Instead it is in $currState state")
    logFailedStateChange(replica, currState, targetState, e)
  }

  private def logFailedStateChange(replica: PartitionAndReplica, currState: ReplicaState, targetState: ReplicaState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} initiated state change of replica ${replica.replica} " +
        s"for partition ${replica.topicPartition} from $currState to $targetState failed", t)
  }
}

sealed trait ReplicaState {
  def state: Byte
  def validPreviousStates: Set[ReplicaState]
}

case object NewReplica extends ReplicaState {
  val state: Byte = 1
  val validPreviousStates: Set[ReplicaState] = Set(NonExistentReplica)
}

case object OnlineReplica extends ReplicaState {
  val state: Byte = 2
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object OfflineReplica extends ReplicaState {
  val state: Byte = 3
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object ReplicaDeletionStarted extends ReplicaState {
  val state: Byte = 4
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica)
}

case object ReplicaDeletionSuccessful extends ReplicaState {
  val state: Byte = 5
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionStarted)
}

case object ReplicaDeletionIneligible extends ReplicaState {
  val state: Byte = 6
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionStarted)
}

case object NonExistentReplica extends ReplicaState {
  val state: Byte = 7
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionSuccessful)
}
