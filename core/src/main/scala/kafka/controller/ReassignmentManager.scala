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

import kafka.admin.AdminOperationException
import kafka.common.StateChangeFailedException
import kafka.utils.Logging
import kafka.zk.{KafkaZkClient, ReassignPartitionsZNode, TopicPartitionStateZNode}
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.{Map, Seq, Set, immutable, mutable}

trait ReassignmentListener {
  /**
   * Called right before the reassignment is starting
   */
  def preReassignmentStartOrResume(tp: TopicPartition)

  /**
   * Called right after the reassignment is persisted in ZK
   */
  def postReassignmentStartedOrResumed(tp: TopicPartition)

  /**
   * Called right before we clear a reassignment from memory
   * @param deletedZNode - boolean indicating whether the reassign_partitions znode was
   *                       deleted as part of the completion of the reassignment
   *                       (e.g if it was the last reassignment in that znode)
   */
  def preReassignmentFinish(tp: TopicPartition, deletedZNode: Boolean)

  /**
   * Called at the very end of the reassignment process
   */
  def postReassignmentFinished(tp: TopicPartition)
}

/**
 * A helper class which contains logic for driving partition reassignments.
 * This class is not thread-safe.
 */
class ReassignmentManager(controllerContext: ControllerContext,
                          zkClient: KafkaZkClient,
                          reassignmentListener: ReassignmentListener,
                          replicaStateMachine: ReplicaStateMachine,
                          partitionStateMachine: PartitionStateMachine,
                          brokerRequestBatch: ControllerBrokerRequestBatch,
                          stateChangeLogger: StateChangeLogger,
                          shouldSkipReassignment: TopicPartition => Option[ApiError]) extends Logging {

  /**
   * If there is an existing reassignment through zookeeper for any of the requested partitions, they will be
   * cancelled prior to beginning the new reassignment. Any zk-based reassignment for partitions
   * which are NOT included in this call will not be affected.
   *
   * @param reassignments Map of reassignments passed through the AlterReassignments API. A null value
   *                      means that we should cancel an in-progress reassignment.
   */
  def triggerApiReassignment(reassignments: Map[TopicPartition, Option[Seq[Int]]]): mutable.Map[TopicPartition, ApiError] = {
    val reassignmentResults = mutable.Map.empty[TopicPartition, ApiError]
    val partitionsToReassign = mutable.Map.empty[TopicPartition, ReplicaAssignment]

    reassignments.foreach { case (tp, targetReplicas) =>
      if (replicasAreValid(tp, targetReplicas)) {
        maybeBuildReassignment(tp, targetReplicas) match {
          case Some(context) => partitionsToReassign.put(tp, context)
          case None => reassignmentResults.put(tp, new ApiError(Errors.NO_REASSIGNMENT_IN_PROGRESS))
        }
      } else {
        reassignmentResults.put(tp, new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT))
      }
    }

    // The latest reassignment (whether by API or through zk) always takes precedence,
    // so remove from active zk reassignment (if one exists)
    maybeRemoveFromZkReassignment((tp, _) => partitionsToReassign.contains(tp))

    reassignmentResults ++= maybeTriggerPartitionReassignment(partitionsToReassign)
  }

  /**
   * @throws IllegalStateException
   */
  def triggerZkReassignment(): Set[TopicPartition] = {
    val reassignmentResults = mutable.Map.empty[TopicPartition, ApiError]
    val partitionsToReassign = mutable.Map.empty[TopicPartition, ReplicaAssignment]

    zkClient.getPartitionReassignment().foreach { case (tp, targetReplicas) =>
      maybeBuildReassignment(tp, Some(targetReplicas)) match {
        case Some(context) => partitionsToReassign.put(tp, context)
        case None => reassignmentResults.put(tp, new ApiError(Errors.NO_REASSIGNMENT_IN_PROGRESS))
      }
    }

    reassignmentResults ++= maybeTriggerPartitionReassignment(partitionsToReassign)
    val (partitionsReassigned, partitionsFailed) = reassignmentResults.partition(_._2.error == Errors.NONE)
    if (partitionsFailed.nonEmpty) {
      warn(s"Failed reassignment through zk with the following errors: $partitionsFailed")
      maybeRemoveFromZkReassignment((tp, _) => partitionsFailed.contains(tp))
    }
    partitionsReassigned.keySet
  }

  /**
   * @throws IllegalStateException
   */
  def maybeResumeReassignments(shouldResume: (TopicPartition, ReplicaAssignment) => Boolean): Unit = {
    controllerContext.partitionsBeingReassigned.foreach { tp =>
      val currentAssignment = controllerContext.partitionFullReplicaAssignment(tp)
      if (shouldResume(tp, currentAssignment))
        onPartitionReassignment(tp, currentAssignment)
    }
  }

  /**
   * @throws IllegalStateException
   */
  def maybeResumeReassignment(topicPartition: TopicPartition): Unit = {
    if (controllerContext.partitionsBeingReassigned.contains(topicPartition)) {
      val reassignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
      if (isReassignmentComplete(topicPartition, reassignment)) {
        // resume the partition reassignment process
        info(s"Target replicas ${reassignment.targetReplicas} have all caught up with the leader for " +
          s"reassigning partition $topicPartition")
        onPartitionReassignment(topicPartition, reassignment)
      }
    }
  }

  def listPartitionsBeingReassigned(partitionsOpt: Option[Set[TopicPartition]]): Map[TopicPartition, ReplicaAssignment] = {
    val results: mutable.Map[TopicPartition, ReplicaAssignment] = mutable.Map.empty
    val partitionsToList = partitionsOpt match {
      case Some(partitions) => partitions
      case None => controllerContext.partitionsBeingReassigned
    }

    partitionsToList.foreach { tp =>
      val assignment = controllerContext.partitionFullReplicaAssignment(tp)
      if (assignment.isBeingReassigned) {
        results += tp -> assignment
      }
    }
    results
  }

  /**
   * Trigger a partition reassignment provided that the topic exists and is not being deleted.
   *
   * This is called when a reassignment is initially received either through Zookeeper or through the
   * AlterPartitionReassignments API
   *
   * The `partitionsBeingReassigned` field in the controller context will be updated by this
   * call after the reassignment completes validation and is successfully stored in the topic
   * assignment zNode.
   *
   * @param reassignments The reassignments to begin processing
   * @return A map of any errors in the reassignment. If the error is NONE for a given partition,
   *         then the reassignment was submitted successfully.
   */
  private def maybeTriggerPartitionReassignment(reassignments: Map[TopicPartition, ReplicaAssignment]): Map[TopicPartition, ApiError] = {
    reassignments.map { case (tp, reassignment) =>
      val apiError = shouldSkipReassignment(tp).getOrElse {
        val assignedReplicas = controllerContext.partitionReplicaAssignment(tp)
        if (assignedReplicas.nonEmpty) {
          try {
            onPartitionReassignment(tp, reassignment)
            ApiError.NONE
          } catch {
            case e: ControllerMovedException =>
              info(s"Failed completing reassignment of partition $tp because controller has moved to another broker")
              throw e
            case e: Throwable =>
              error(s"Error completing reassignment of partition $tp", e)
              new ApiError(Errors.UNKNOWN_SERVER_ERROR)
          }
        } else {
          new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist.")
        }
      }

      tp -> apiError
    }
  }

  private def replicasAreValid(topicPartition: TopicPartition, replicasOpt: Option[Seq[Int]]): Boolean = {
    replicasOpt match {
      case Some(replicas) =>
        val replicaSet = replicas.toSet
        if (replicas.isEmpty || replicas.size != replicaSet.size)
          false
        else if (replicas.exists(_ < 0))
          false
        else {
          // Ensure that any new replicas are among the live brokers
          val currentAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
          val newAssignment = currentAssignment.reassignTo(replicas)
          newAssignment.addingReplicas.toSet.subsetOf(controllerContext.liveBrokerIds)
        }

      case None => true
    }
  }

  private def maybeBuildReassignment(topicPartition: TopicPartition,
                                     targetReplicasOpt: Option[Seq[Int]]): Option[ReplicaAssignment] = {
    val replicaAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
    if (replicaAssignment.isBeingReassigned) {
      val targetReplicas = targetReplicasOpt.getOrElse(replicaAssignment.originReplicas)
      Some(replicaAssignment.reassignTo(targetReplicas))
    } else {
      targetReplicasOpt.map { targetReplicas =>
        replicaAssignment.reassignTo(targetReplicas)
      }
    }
  }

  /**
   * This callback is invoked:
   * 1. By the AlterPartitionReassignments API
   * 2. By the reassigned partitions listener which is triggered when the /admin/reassign_partitions znode is created
   * 3. When an ongoing reassignment finishes - this is detected by a change in the partition's ISR znode
   * 4. Whenever a new broker comes up which is part of an ongoing reassignment
   * 5. On controller startup/failover
   *
   * Reassigning replicas for a partition goes through a few steps listed in the code.
   * RS = current assigned replica set
   * ORS = Original replica set for partition
   * TRS = Reassigned (target) replica set
   * AR = The replicas we are adding as part of this reassignment
   * RR = The replicas we are removing as part of this reassignment
   *
   * A reassignment may have up to three phases, each with its own steps:
   *
   * Phase U (Assignment update): Regardless of the trigger, the first step is in the reassignment process
   * is to update the existing assignment state. We always update the state in Zookeeper before
   * we update memory so that it can be resumed upon controller fail-over.
   *
   *   U1. Update ZK with RS = ORS + TRS, AR = TRS - ORS, RR = ORS - TRS.
   *   U2. Update memory with RS = ORS + TRS, AR = TRS - ORS and RR = ORS - TRS
   *   U3. If we are cancelling or replacing an existing reassignment, send StopReplica to all members
   *       of AR in the original reassignment if they are not in TRS from the new assignment
   *
   * To complete the reassignment, we need to bring the new replicas into sync, so depending on the state
   * of the ISR, we will execute one of the following steps.
   *
   * Phase A (when TRS != ISR): The reassignment is not yet complete
   *
   *   A1. Bump the leader epoch for the partition and send LeaderAndIsr updates to RS.
   *   A2. Start new replicas AR by moving replicas in AR to NewReplica state.
   *
   * Phase B (when TRS = ISR): The reassignment is complete
   *
   *   B1. Move all replicas in AR to OnlineReplica state.
   *   B2. Set RS = TRS, AR = [], RR = [] in memory.
   *   B3. Send a LeaderAndIsr request with RS = TRS. This will prevent the leader from adding any replica in TRS - ORS back in the isr.
   *       If the current leader is not in TRS or isn't alive, we move the leader to a new replica in TRS.
   *       We may send the LeaderAndIsr to more than the TRS replicas due to the
   *       way the partition state machine works (it reads replicas from ZK)
   *   B4. Move all replicas in RR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
   *       isr to remove RR in ZooKeeper and send a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
   *       After that, we send a StopReplica (delete = false) to the replicas in RR.
   *   B5. Move all replicas in RR to NonExistentReplica state. This will send a StopReplica (delete = true) to
   *       the replicas in RR to physically delete the replicas on disk.
   *   B6. Update ZK with RS=TRS, AR=[], RR=[].
   *   B7. Remove the ISR reassign listener and maybe update the /admin/reassign_partitions path in ZK to remove this partition from it if present.
   *   B8. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
   *
   * In general, there are two goals we want to aim for:
   * 1. Every replica present in the replica set of a LeaderAndIsrRequest gets the request sent to it
   * 2. Replicas that are removed from a partition's assignment get StopReplica sent to them
   *
   * For example, if ORS = {1,2,3} and TRS = {4,5,6}, the values in the topic and leader/isr paths in ZK
   * may go through the following transitions.
   * RS                AR          RR          leader     isr
   * {1,2,3}           {}          {}          1          {1,2,3}           (initial state)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     1          {1,2,3}           (step A2)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     1          {1,2,3,4,5,6}     (phase B)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     4          {1,2,3,4,5,6}     (step B3)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     4          {4,5,6}           (step B4)
   * {4,5,6}           {}          {}          4          {4,5,6}           (step B6)
   *
   * Note that we have to update RS in ZK with TRS last since it's the only place where we store ORS persistently.
   * This way, if the controller crashes before that step, we can still recover.
   *
   * @throws IllegalStateException
   */
  private def onPartitionReassignment(topicPartition: TopicPartition, reassignment: ReplicaAssignment): Unit = {
    reassignmentListener.preReassignmentStartOrResume(topicPartition)

    updateCurrentReassignment(topicPartition, reassignment)

    val addingReplicas = reassignment.addingReplicas
    val removingReplicas = reassignment.removingReplicas

    if (!isReassignmentComplete(topicPartition, reassignment)) {
      // A1. Send LeaderAndIsr request to every replica in ORS + TRS (with the new RS, AR and RR).
      updateLeaderEpochAndSendRequest(topicPartition, reassignment)
      // A2. replicas in AR -> NewReplica
      startNewReplicasForReassignedPartition(topicPartition, addingReplicas)
    } else {
      // B1. replicas in AR -> OnlineReplica
      replicaStateMachine.handleStateChanges(addingReplicas.map(PartitionAndReplica(topicPartition, _)), OnlineReplica)
      // B2. Set RS = TRS, AR = [], RR = [] in memory.
      val completedReassignment = ReplicaAssignment(reassignment.targetReplicas)
      controllerContext.updatePartitionFullReplicaAssignment(topicPartition, completedReassignment)
      // B3. Send LeaderAndIsr request with a potential new leader (if current leader not in TRS) and
      //   a new RS (using TRS) and same isr to every broker in ORS + TRS or TRS
      moveReassignedPartitionLeaderIfRequired(topicPartition, completedReassignment)
      // B4. replicas in RR -> Offline (force those replicas out of isr)
      // B5. replicas in RR -> NonExistentReplica (force those replicas to be deleted)
      stopRemovedReplicasOfReassignedPartition(topicPartition, removingReplicas)
      // B6. Update ZK with RS = TRS, AR = [], RR = [].
      updateReplicaAssignmentForPartition(topicPartition, completedReassignment)
      // B7. Remove the ISR reassign listener and maybe update the /admin/reassign_partitions path in ZK to remove this partition from it.
      removePartitionFromReassigningPartitions(topicPartition, completedReassignment)
      // B8. After electing a leader in B3, the replicas and isr information changes, so resend the update metadata request to every broker
      brokerRequestBatch.newBatch()
      brokerRequestBatch.addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
      brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
      reassignmentListener.postReassignmentFinished(topicPartition)
    }
  }

  /**
   * Update the current assignment state in Zookeeper and in memory. If a reassignment is already in
   * progress, then the new reassignment will supplant it and some replicas will be shutdown.
   *
   * Note that due to the way we compute the original replica set, we cannot guarantee that a
   * cancellation will restore the original replica order. Target replicas are always listed
   * first in the replica set in the desired order, which means we have no way to get to the
   * original order if the reassignment overlaps with the current assignment. For example,
   * with an initial assignment of [1, 2, 3] and a reassignment of [3, 4, 2], then the replicas
   * will be encoded as [3, 4, 2, 1] while the reassignment is in progress. If the reassignment
   * is cancelled, there is no way to restore the original order.
   *
   * @param topicPartition The reassigning partition
   * @param reassignment The new reassignment
   */
  private def updateCurrentReassignment(topicPartition: TopicPartition, reassignment: ReplicaAssignment): Unit = {
    val currentAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)

    if (currentAssignment != reassignment) {
      debug(s"Updating assignment of partition $topicPartition from $currentAssignment to $reassignment")

      // U1. Update assignment state in zookeeper
      updateReplicaAssignmentForPartition(topicPartition, reassignment)
      // U2. Update assignment state in memory
      controllerContext.updatePartitionFullReplicaAssignment(topicPartition, reassignment)

      // If there is a reassignment already in progress, then some of the currently adding replicas
      // may be eligible for immediate removal, in which case we need to stop the replicas.
      val unneededReplicas = currentAssignment.replicas.diff(reassignment.replicas)
      if (unneededReplicas.nonEmpty)
        stopRemovedReplicasOfReassignedPartition(topicPartition, unneededReplicas)
    }

    reassignmentListener.postReassignmentStartedOrResumed(topicPartition)

    controllerContext.partitionsBeingReassigned.add(topicPartition)
  }

  private def isReassignmentComplete(partition: TopicPartition, assignment: ReplicaAssignment): Boolean = {
    if (!assignment.isBeingReassigned) {
      true
    } else {
      zkClient.getTopicPartitionStates(Seq(partition)).get(partition).exists { leaderIsrAndControllerEpoch =>
        val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr.toSet
        val targetReplicas = assignment.targetReplicas.toSet
        targetReplicas.subsetOf(isr)
      }
    }
  }

  /**
   * @throws IllegalStateException
   */
  private def updateLeaderEpochAndSendRequest(topicPartition: TopicPartition,
                                              assignment: ReplicaAssignment): Unit = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    updateLeaderEpoch(topicPartition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        brokerRequestBatch.newBatch()
        brokerRequestBatch.addLeaderAndIsrRequestForBrokers(assignment.replicas, topicPartition,
          updatedLeaderIsrAndControllerEpoch, assignment, isNew = false)
        brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        stateChangeLog.trace(s"Sent LeaderAndIsr request $updatedLeaderIsrAndControllerEpoch with " +
          s"new replica assignment $assignment to leader ${updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader} " +
          s"for partition being reassigned $topicPartition")

      case None => // fail the reassignment
        stateChangeLog.error(s"Failed to send LeaderAndIsr request with new replica assignment " +
          s"$assignment to leader for partition being reassigned $topicPartition")
    }
  }

  /**
   * Does not change leader or isr, but just increments the leader epoch
   *
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   */
  private def updateLeaderEpoch(partition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    debug(s"Updating leader epoch for partition $partition")
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      zkWriteCompleteOrUnnecessary = zkClient.getTopicPartitionStates(Seq(partition)).get(partition) match {
        case Some(leaderIsrAndControllerEpoch) =>
          val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
          if (controllerEpoch > controllerContext.epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably " +
              s"means the current controller with epoch ${controllerContext.epoch} went through a soft failure and another " +
              s"controller was elected with epoch $controllerEpoch. Aborting state change by this controller")
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = leaderAndIsr.newEpochAndZkVersion
          // update the new leadership decision in zookeeper or retry
          val UpdateLeaderAndIsrResult(finishedUpdates, _) =
            zkClient.updateLeaderAndIsr(immutable.Map(partition -> newLeaderAndIsr), controllerContext.epoch, controllerContext.epochZkVersion)

          finishedUpdates.headOption.exists {
            case (partition, Right(leaderAndIsr)) =>
              finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch))
              info(s"Updated leader epoch for partition $partition to ${leaderAndIsr.leaderEpoch}")
              true
            case (_, Left(e)) =>
              throw e
          }
        case None =>
          throw new IllegalStateException(s"Cannot update leader epoch for partition $partition as " +
            "leaderAndIsr path is empty. This could mean we somehow tried to reassign a partition that doesn't exist")
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  private def updateReplicaAssignmentForPartition(topicPartition: TopicPartition, assignment: ReplicaAssignment): Unit = {
    var topicAssignment = controllerContext.partitionFullReplicaAssignmentForTopic(topicPartition.topic)
    topicAssignment += topicPartition -> assignment

    val setDataResponse = zkClient.setTopicAssignmentRaw(topicPartition.topic, topicAssignment, controllerContext.epochZkVersion)
    setDataResponse.resultCode match {
      case Code.OK =>
        info(s"Successfully updated assignment of partition $topicPartition to $assignment")
      case Code.NONODE =>
        throw new IllegalStateException(s"Failed to update assignment for $topicPartition since the topic " +
          "has no current assignment")
      case _ => throw new KafkaException(setDataResponse.resultException.get)
    }
  }

  private def startNewReplicasForReassignedPartition(topicPartition: TopicPartition, newReplicas: Seq[Int]): Unit = {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Seq(PartitionAndReplica(topicPartition, replica)), NewReplica)
    }
  }

  private def moveReassignedPartitionLeaderIfRequired(topicPartition: TopicPartition,
                                                      newAssignment: ReplicaAssignment): Unit = {
    val reassignedReplicas = newAssignment.replicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader

    if (!reassignedReplicas.contains(currentLeader)) {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is not in the new list of replicas ${reassignedReplicas.mkString(",")}. Re-electing leader")
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
    } else if (controllerContext.isReplicaOnline(currentLeader, topicPartition)) {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} and is alive")
      // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
      updateLeaderEpochAndSendRequest(topicPartition, newAssignment)
    } else {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} but is dead")
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
    }
  }

  private def stopRemovedReplicasOfReassignedPartition(topicPartition: TopicPartition,
                                                       removedReplicas: Seq[Int]): Unit = {
    // first move the replica to offline state (the controller removes it from the ISR)
    val replicasToBeDeleted = removedReplicas.map(PartitionAndReplica(topicPartition, _))
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica)
    // send stop replica command to the old replicas
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted)
    // TODO: Eventually partition reassignment could use a callback that does retries if deletion failed
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful)
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica)
  }

  private def removePartitionFromReassigningPartitions(topicPartition: TopicPartition,
                                                       assignment: ReplicaAssignment): Unit = {
    if (controllerContext.partitionsBeingReassigned.contains(topicPartition)) {
      val deletedZNode = maybeRemoveFromZkReassignment(
        (tp, replicas) => tp == topicPartition && replicas == assignment.replicas
      )

      reassignmentListener.preReassignmentFinish(topicPartition, deletedZNode)
      controllerContext.partitionsBeingReassigned.remove(topicPartition)
    } else {
      throw new IllegalStateException("Cannot remove a reassigning partition because it is not present in memory")
    }
  }

  /**
   * Remove partitions from an active zk-based reassignment (if one exists).
   *
   * @param shouldRemoveReassignment Predicate indicating which partition reassignments should be removed
   * @return a boolean indicating whether the /admin/reassign_partitions znode was deleted
   */
  private def maybeRemoveFromZkReassignment(shouldRemoveReassignment: (TopicPartition, Seq[Int]) => Boolean): Boolean = {
    if (!zkClient.reassignPartitionsInProgress())
      return false

    val reassigningPartitions = zkClient.getPartitionReassignment()
    val (removingPartitions, updatedPartitionsBeingReassigned) = reassigningPartitions.partition { case (tp, replicas) =>
      shouldRemoveReassignment(tp, replicas)
    }
    info(s"Removing partitions $removingPartitions from the list of reassigned partitions in zookeeper")

    // write the new list to zookeeper
    if (updatedPartitionsBeingReassigned.isEmpty) {
      info(s"No more partitions need to be reassigned. Deleting zk path ${ReassignPartitionsZNode.path}")
      zkClient.deletePartitionReassignment(controllerContext.epochZkVersion)
      true
    } else {
      try {
        zkClient.setOrCreatePartitionReassignment(updatedPartitionsBeingReassigned, controllerContext.epochZkVersion)
        false
      } catch {
        case e: KeeperException => throw new AdminOperationException(e)
      }
    }
  }
}
