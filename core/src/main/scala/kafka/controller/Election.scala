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
package kafka.controller

import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import org.apache.kafka.common.TopicPartition

import scala.collection.Seq

sealed trait LeaderAndIsrUpdateResult {
  def topicPartition: TopicPartition
}

object LeaderAndIsrUpdateResult {
  final case class Successful(
    topicPartition: TopicPartition,
    newLeaderAndIsr: LeaderAndIsr,
    liveReplicas: Seq[Int],
    replicasToStop: Seq[Int] = Seq.empty,
    replicasToDelete: Seq[Int] = Seq.empty
  ) extends LeaderAndIsrUpdateResult

  final case class NotNeeded(
    topicPartition: TopicPartition,
    currentLeaderAndIsr: LeaderAndIsr
  ) extends LeaderAndIsrUpdateResult

  final case class Failed(
    topicPartition: TopicPartition,
    error: StateChangeFailedException
  ) extends LeaderAndIsrUpdateResult
}

object Election {

  private def leaderForOffline(partition: TopicPartition,
                               leaderAndIsrOpt: Option[LeaderAndIsr],
                               uncleanLeaderElectionEnabled: Boolean,
                               isLeaderRecoverySupported: Boolean,
                               controllerContext: ControllerContext): LeaderAndIsrUpdateResult = {

    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        val isr = leaderAndIsr.isr
        val leaderOpt = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(
          assignment, isr, liveReplicas.toSet, uncleanLeaderElectionEnabled, controllerContext)
        val newLeaderAndIsrOpt = leaderOpt.map { leader =>
          val newIsr = if (isr.contains(leader)) isr.filter(replica => controllerContext.isReplicaOnline(replica, partition))
          else List(leader)

          if (!isr.contains(leader) && isLeaderRecoverySupported) {
            // The new leader is not in the old ISR so mark the partition a RECOVERING
            leaderAndIsr.newRecoveringLeaderAndIsr(leader, newIsr)
          } else {
            // Elect a new leader but keep the previous leader recovery state
            leaderAndIsr.newLeaderAndIsr(leader, newIsr)
          }
        }
        newLeaderAndIsrOpt match {
          case Some(newLeaderandIsr) =>
            LeaderAndIsrUpdateResult.Successful(partition, newLeaderandIsr, liveReplicas, Seq.empty)
          case None =>
            LeaderAndIsrUpdateResult.Failed(partition, new StateChangeFailedException(
              s"Failed to elect leader for offline partition $partition"
            ))
        }

      case None =>
        LeaderAndIsrUpdateResult.Failed(partition, new StateChangeFailedException(
          s"Failed to elect leader for offline partition $partition"
        ))
    }
  }

  /**
   * Elect leaders for new or offline partitions.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param isLeaderRecoverySupported true leader recovery is support and should be set if election is unclean
   * @param partitionsWithUncleanLeaderRecoveryState A sequence of tuples representing the partitions
   *                                                 that need election, their leader/ISR state, and whether
   *                                                 or not unclean leader election is enabled
   *
   * @return The election results
   */
  def leaderForOffline(
    controllerContext: ControllerContext,
    isLeaderRecoverySupported: Boolean,
    partitionsWithUncleanLeaderRecoveryState: Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)]
  ): Seq[LeaderAndIsrUpdateResult] = {
    partitionsWithUncleanLeaderRecoveryState.map {
      case (partition, leaderAndIsrOpt, uncleanLeaderElectionEnabled) =>
        leaderForOffline(partition, leaderAndIsrOpt, uncleanLeaderElectionEnabled, isLeaderRecoverySupported, controllerContext)
    }
  }

  private def leaderForPreferredReplica(partition: TopicPartition,
                                        leaderAndIsr: LeaderAndIsr,
                                        controllerContext: ControllerContext): LeaderAndIsrUpdateResult = {
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    val isr = leaderAndIsr.isr
    if (leaderAndIsr.leader == assignment.head) {
      LeaderAndIsrUpdateResult.NotNeeded(partition, leaderAndIsr)
    } else {
      val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment, isr, liveReplicas.toSet)
      leaderOpt match {
        case Some(newLeader) =>
          val newLeaderAndIsr = leaderAndIsr.newLeader(newLeader)
          LeaderAndIsrUpdateResult.Successful(partition, newLeaderAndIsr, liveReplicas = liveReplicas)
        case None =>
          val reason = if (!leaderAndIsr.isr.contains(assignment.head)) "offline" else "not in the ISR"
          LeaderAndIsrUpdateResult.Failed(partition, new StateChangeFailedException(
            s"Failed to elect preferred leader ${assignment.head} for partition $partition since " +
              s"it is $reason"
          ))
      }
    }
  }

  /**
   * Elect preferred leaders.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   *
   * @return The election results
   */
  def leaderForPreferredReplica(controllerContext: ControllerContext,
                                leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[LeaderAndIsrUpdateResult] = {
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForPreferredReplica(partition, leaderAndIsr, controllerContext)
    }
  }

  /**
   * Controlled shutdown attempts to gracefully remove and fence active replicas
   * that are shutting down:
   *
   *   1) If there is only one replica, do nothing
   *   2) If the replica is the only member of the ISR, leave it as the current leader
   *   3) If the replica is in the ISR, elect the next preferred leader
   *   4) If the replica is not in the ISR, bump the leader epoch to fence the replica
   *
   * @param partition The topic partition to update
   * @param leaderAndIsr The current LeaderAndIsr
   * @param shuttingDownBrokerIds The brokers that are shutting down
   * @param controllerContext Current controller context
   * @return [[LeaderAndIsrUpdateResult.Failed]] if there is no replica available from the remaining
   *         replicas in the ISR after removing the shutting down replicas; or
   *         [[LeaderAndIsrUpdateResult.Successful]] if a new leader could be elected from the
   *         remaining replicas in the ISR.
   */
  private def updatePartitionForControlledShutdown(
    partition: TopicPartition,
    leaderAndIsr: LeaderAndIsr,
    shuttingDownBrokerIds: Set[Int],
    controllerContext: ControllerContext
  ): LeaderAndIsrUpdateResult = {

    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveReplicaIds = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    val isr = leaderAndIsr.isr

    // Try to find a live replica in the ISR which is not shutting down.
    val newLeaderIdOpt = PartitionLeaderElectionAlgorithms.controlledShutdownPartitionLeaderElection(
      assignment,
      isr,
      liveReplicaIds.toSet,
      shuttingDownBrokerIds
    )

    newLeaderIdOpt match {
      case Some(newLeaderId) =>
        // We found a new leader from the current ISR that is not shutting down,
        // so we can remove all shutting down replicas from the ISR.
        val newIsr = isr.filter(replica => !shuttingDownBrokerIds.contains(replica))
        val newLeaderAndIsr = leaderAndIsr.newLeaderAndIsr(newLeaderId, newIsr)
        LeaderAndIsrUpdateResult.Successful(partition, newLeaderAndIsr, liveReplicaIds, shuttingDownBrokerIds.toSeq)

      case None =>
        if (leaderAndIsr.leader == LeaderAndIsr.NoLeader) {
          // If there is no leader already, then we do not need to change the ISR. We
          // can fence and shutdown the replicas immediately.
          val newLeaderAndIsr = leaderAndIsr.newEpoch
          LeaderAndIsrUpdateResult.Successful(partition, newLeaderAndIsr, liveReplicaIds, shuttingDownBrokerIds.toSeq)
        } else {
          // One of the shutting down replicas is the current leader. We will leave it
          // as the current leader and shutdown any other replicas.
          val currentLeaderId = leaderAndIsr.leader
          val nonLeaderShuttingDownReplicas = shuttingDownBrokerIds.filterNot(_ == currentLeaderId)
          if (nonLeaderShuttingDownReplicas.isEmpty) {
            // There are no shutting down replicas besides the current leader, so make no changes.
            LeaderAndIsrUpdateResult.Failed(partition, new StateChangeFailedException(
              s"Failed to process controlled shutdown of broker $currentLeaderId for partition $partition " +
                "since it is the leader and there are no replicas in the ISR available to elect"
            ))
          } else {
            // We can shutdown all replicas except the current leader. We must still send
            // LeaderAndIsr to the current leader that is shutting down.
            val newIsr = isr.filter(replica => !nonLeaderShuttingDownReplicas.contains(replica))
            val newLeaderAndIsr = leaderAndIsr.newLeaderAndIsr(currentLeaderId, newIsr)
            LeaderAndIsrUpdateResult.Successful(
              partition,
              newLeaderAndIsr,
              liveReplicas = liveReplicaIds :+ currentLeaderId,
              replicasToStop = nonLeaderShuttingDownReplicas.toSeq
            )
          }
        }
    }
  }

  /**
   * Elect leaders for partitions whose current leaders are shutting down.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   *
   * @return The election results
   */
  def processControlledShutdown(
    controllerContext: ControllerContext,
    leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]
  ): Seq[LeaderAndIsrUpdateResult] = {
    val shuttingDownBrokerIds = controllerContext.shuttingDownBrokerIds.toSet
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      updatePartitionForControlledShutdown(partition, leaderAndIsr, shuttingDownBrokerIds, controllerContext)
    }
  }

  def processReassignmentCancellation(
    controllerContext: ControllerContext,
    leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]
  ): Seq[LeaderAndIsrUpdateResult] = {
    leaderAndIsrs.map { case (topicPartition, leaderAndIsr) =>
      val assignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
      if (!assignment.isBeingReassigned) {
        throw new IllegalStateException(s"Cancellation of reassignment failed for $topicPartition " +
          "since there is not reassignment in progress")
      }

      updateLeaderAndIsrForCancelledReassignment(
        topicPartition,
        controllerContext,
        assignment,
        leaderAndIsr
      )
    }
  }

  /**
   * Build a new LeaderAndIsr state in order to cancel an active reassignment. This is done
   * by removing the `AddingReplicas` set from the ISR and electing a new leader from the
   * original replicas (if necessary).
   *
   * Note that if the `AddingReplicas` set is empty (i.e. if the reassignment is removing replicas
   * only), then no changes to the LeaderAndIsr state will be made. On the other hand, if this
   * set is not empty, then this method guarantees a bump to the leader epoch. This ensures that
   * a `StopReplica` request can be sent to any replicas that need to be deleted with a larger
   * epoch than previously seen.
   *
   * An error will be returned if a leader cannot be elected from the original replica set.
   *
   * @param topicPartition The topic partition to update
   * @param controllerContext Current controller context
   * @param assignment The current reassignment which is being cancelled
   * @param leaderAndIsr The current LeaderAndIsr state
   * @return [[LeaderAndIsrUpdateResult.NotNeeded]] if `AddingReplicas` is empty;
   *         [[LeaderAndIsrUpdateResult.Failed]] if no live leader from the original replicas could be elected; or
   *         [[LeaderAndIsrUpdateResult.Successful]] if all `AddingReplicas` were successfully removed from the
   *         ISR and a new leader elected
   */
  private def updateLeaderAndIsrForCancelledReassignment(
    topicPartition: TopicPartition,
    controllerContext: ControllerContext,
    assignment: ReplicaAssignment,
    leaderAndIsr: LeaderAndIsr
  ): LeaderAndIsrUpdateResult = {
    val addingReplicas = assignment.addingReplicas
    if (addingReplicas.isEmpty) {
      // If there are no adding replicas, then there are no partitions to stop and delete
      LeaderAndIsrUpdateResult.NotNeeded(topicPartition, leaderAndIsr)
    } else {
      // Otherwise, we need to remove the adding replicas and maybe elect a new leader
      val originReplicas = assignment.originReplicas
      val liveOriginReplicas = originReplicas.filter(replica => controllerContext.isReplicaOnline(replica, topicPartition))
      val newIsr = leaderAndIsr.isr.filter(originReplicas.contains)
      val currentLeader = leaderAndIsr.leader

      if (newIsr.isEmpty) {
        // If there are no replicas from the original set in the ISR, then it is not possible
        // to cancel the assignment without having an unclean election.
        LeaderAndIsrUpdateResult.Failed(topicPartition, new StateChangeFailedException(
          s"Failed to cancel reassignment $assignment for partition $topicPartition since " +
            s"there are no replicas in the original replica set $originReplicas remaining in the ISR"
        ))
      } else {
        val newLeaderOpt = if (originReplicas.contains(currentLeader)) {
          // The leader is already among the origin replicas, so we can keep it.
          Some(currentLeader)
        } else {
          // Try to use the preferred origin leader if it is live and in the ISR
          val preferredOriginLeader = originReplicas.head
          if (liveOriginReplicas.contains(preferredOriginLeader) && newIsr.contains(preferredOriginLeader)) {
            Some(preferredOriginLeader)
          } else {
            // Otherwise, use a random live replica from the original set if available
            newIsr.find(liveOriginReplicas.contains)
          }
        }

        newLeaderOpt match {
          case Some(newLeader) =>
            val newLeaderAndIsr = leaderAndIsr.newLeaderAndIsr(newLeader, newIsr)
            LeaderAndIsrUpdateResult.Successful(
              topicPartition,
              newLeaderAndIsr,
              liveReplicas = liveOriginReplicas,
              replicasToDelete = addingReplicas
            )
          case None =>
            LeaderAndIsrUpdateResult.Failed(topicPartition, new StateChangeFailedException(
              s"Failed to elect new leader for $topicPartition from the live original replicas: $liveOriginReplicas"
            ))
        }
      }
    }
  }

  def processReassignmentCompletion(
    controllerContext: ControllerContext,
    leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]
  ): Seq[LeaderAndIsrUpdateResult] = {
    leaderAndIsrs.map { case (topicPartition, leaderAndIsr) =>
      val assignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
      if (!assignment.isBeingReassigned) {
        throw new IllegalStateException(s"Completion of reassignment failed for $topicPartition " +
          "since there is not reassignment in progress")
      }

      updateLeaderAndIsrForCompletedReassignment(
        topicPartition,
        controllerContext,
        assignment,
        leaderAndIsr
      )
    }
  }

  /**
   * Build a new LeaderAndIsr state in order to complete an active reassignment. This is done
   * by removing the `RemovingReplicas` set from the ISR and electing a new leader from the
   * target replicas (if necessary).
   *
   * Note that if the `RemovingReplicas` set is empty (i.e. if the reassignment is adding replicas
   * only), then no changes to the LeaderAndIsr state will be made. On the other hand, if this
   * set is not empty, then this method guarantees a bump to the leader epoch. This ensures that
   * a `StopReplica` request can be sent to any replicas that need to be deleted with a larger
   * epoch than previously seen.
   *
   * An error will be returned if a leader cannot be elected from the original replica set.
   *
   * @param topicPartition The topic partition to update
   * @param controllerContext Current controller context
   * @param assignment The current reassignment which is being completed
   * @param leaderAndIsr The current LeaderAndIsr state
   * @return [[LeaderAndIsrUpdateResult.NotNeeded]] if `RemovingReplicas` is empty;
   *         [[LeaderAndIsrUpdateResult.Failed]] if there are any target replicas NOT in the ISR or if
   *         no live leader from the target replicas could be elected; or
   *         [[LeaderAndIsrUpdateResult.Successful]] if all `RemovingReplicas` were successfully removed from the
   *         ISR and a new leader elected
   */
  private def updateLeaderAndIsrForCompletedReassignment(
    topicPartition: TopicPartition,
    controllerContext: ControllerContext,
    assignment: ReplicaAssignment,
    leaderAndIsr: LeaderAndIsr
  ): LeaderAndIsrUpdateResult = {
    val removingReplicas = assignment.removingReplicas
    val targetReplicas = assignment.targetReplicas
    val newIsr = leaderAndIsr.isr.filter(targetReplicas.contains)

    // For a reassignment to be completed, all of the target replicas must be in the ISR.
    if (targetReplicas.size != newIsr.size) {
      LeaderAndIsrUpdateResult.Failed(topicPartition, new StateChangeFailedException(
        s"Failed to complete reassignment for partition $topicPartition since the current ISR " +
          s"${leaderAndIsr.isr} does not contain all of the target replicas $targetReplicas"
      ))
    } else if (removingReplicas.isEmpty) {
      // There are no replicas to remove, so there is no need to update the Leader and ISR
      LeaderAndIsrUpdateResult.NotNeeded(topicPartition, leaderAndIsr)
    } else {
      val liveTargetReplicas = targetReplicas.filter(replica => controllerContext.isReplicaOnline(replica, topicPartition))
      val currentLeader = leaderAndIsr.leader

      val newLeaderOpt = if (targetReplicas.contains(currentLeader)) {
        // The current leader is already among target replicas, so we can keep it
        Some(currentLeader)
      } else {
        // Try to use the preferred target leader if it is live and in the ISR
        val preferredTargetLeader = targetReplicas.head
        if (liveTargetReplicas.contains(preferredTargetLeader) && newIsr.contains(preferredTargetLeader)) {
          Some(preferredTargetLeader)
        } else {
          // Otherwise choose a random live replica from the ISR
          newIsr.find(liveTargetReplicas.contains)
        }
      }

      newLeaderOpt match {
        case Some(newLeader) =>
          val newLeaderAndIsr = leaderAndIsr.newLeaderAndIsr(newLeader, newIsr)
          LeaderAndIsrUpdateResult.Successful(
            topicPartition,
            newLeaderAndIsr,
            liveReplicas = liveTargetReplicas,
            replicasToDelete = removingReplicas
          )
        case None =>
          LeaderAndIsrUpdateResult.Failed(topicPartition, new StateChangeFailedException(
            s"Failed to elect new leader for $topicPartition from the live target replicas: $liveTargetReplicas"
          ))
      }
    }
  }

}
