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
import kafka.controller.Election._
import org.apache.kafka.common.TopicPartition

import scala.collection.{Seq, mutable}

class MockPartitionStateMachine(
  controllerContext: ControllerContext,
  uncleanLeaderElectionEnabled: Boolean,
  isLeaderRecoverySupported: Boolean
) extends PartitionStateMachine(controllerContext) {

  var stateChangesByTargetState = mutable.Map.empty[PartitionState, Int].withDefaultValue(0)

  def stateChangesCalls(targetState: PartitionState): Int = {
    stateChangesByTargetState(targetState)
  }

  def clear(): Unit = {
    stateChangesByTargetState.clear()
  }

  override def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    leaderElectionStrategy: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    stateChangesByTargetState(targetState) = stateChangesByTargetState(targetState) + 1

    partitions.foreach(partition => controllerContext.putPartitionStateIfNotExists(partition, NonExistentPartition))
    val (validPartitions, invalidPartitions) = controllerContext.checkValidPartitionStateChange(partitions, targetState)
    if (invalidPartitions.nonEmpty) {
      val currentStates = invalidPartitions.map(p => controllerContext.partitionStates.get(p))
      throw new IllegalStateException(s"Invalid state transition to $targetState for partitions $currentStates")
    }

    if (targetState == OnlinePartition) {
      val uninitializedPartitions = validPartitions.filter(partition => controllerContext.partitionState(partition) == NewPartition)
      val partitionsToElectLeader = partitions.filter { partition =>
        val currentState = controllerContext.partitionState(partition)
        currentState == OfflinePartition || currentState == OnlinePartition
      }

      uninitializedPartitions.foreach { partition =>
        controllerContext.putPartitionState(partition, targetState)
      }

      val electionResults = doLeaderElections(partitionsToElectLeader, leaderElectionStrategy.get)
      electionResults.foreach {
        case (partition, Right(_)) => controllerContext.putPartitionState(partition, targetState)
        case (_, Left(_)) => // Ignore; No need to update the context if the election failed
      }

      electionResults
    } else {
      validPartitions.foreach { partition =>
        controllerContext.putPartitionState(partition, targetState)
      }
      Map.empty
    }
  }

  private def doLeaderElections(
    partitions: Seq[TopicPartition],
    leaderElectionStrategy: PartitionLeaderElectionStrategy
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    val failedElections = mutable.Map.empty[TopicPartition, Either[Throwable, LeaderAndIsr]]
    val validLeaderAndIsrs = mutable.Buffer.empty[(TopicPartition, LeaderAndIsr)]

    for (partition <- partitions) {
      val leaderIsrAndControllerEpoch = controllerContext.partitionLeadershipInfo(partition).get
      if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
        val failMsg = s"Aborted leader election for partition $partition since the LeaderAndIsr path was " +
          s"already written by another controller. This probably means that the current controller went through " +
          s"a soft failure and another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}."
        failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
      } else {
        validLeaderAndIsrs.append((partition, leaderIsrAndControllerEpoch.leaderAndIsr))
      }
    }

    val electionResults = leaderElectionStrategy match {
      case OfflinePartitionLeaderElectionStrategy(isUnclean) =>
        val partitionsWithUncleanLeaderElectionState = validLeaderAndIsrs.map { case (partition, leaderAndIsr) =>
          (partition, Some(leaderAndIsr), isUnclean || uncleanLeaderElectionEnabled)
        }
        leaderForOffline(
          controllerContext,
          isLeaderRecoverySupported,
          partitionsWithUncleanLeaderElectionState
        )
      case ReassignPartitionLeaderElectionStrategy =>
        leaderForReassign(controllerContext, validLeaderAndIsrs)
      case PreferredReplicaPartitionLeaderElectionStrategy =>
        leaderForPreferredReplica(controllerContext, validLeaderAndIsrs)
      case ControlledShutdownPartitionLeaderElectionStrategy =>
        leaderForControlledShutdown(controllerContext, validLeaderAndIsrs)
    }

    val results: Map[TopicPartition, Either[Exception, LeaderAndIsr]] = electionResults.map { electionResult =>
      val partition = electionResult.topicPartition
      val value = electionResult.leaderAndIsr match {
        case None =>
          val failMsg = s"Failed to elect leader for partition $partition under strategy $leaderElectionStrategy"
          Left(new StateChangeFailedException(failMsg))
        case Some(leaderAndIsr) =>
          val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
          controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
          Right(leaderAndIsr)
      }

      partition -> value
    }.toMap

    results ++ failedElections
  }

}
