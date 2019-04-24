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

import kafka.common.StateChangeFailedException
import kafka.controller.Election._
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

class MockPartitionStateMachine(controllerContext: ControllerContext,
                                uncleanLeaderElectionEnabled: Boolean)
  extends PartitionStateMachine(controllerContext) {

  override def handleStateChanges(partitions: Seq[TopicPartition],
                                  targetState: PartitionState,
                                  leaderElectionStrategy: Option[PartitionLeaderElectionStrategy]): Map[TopicPartition, Throwable] = {
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

      val failedElections = doLeaderElections(partitionsToElectLeader, leaderElectionStrategy.get)
      val successfulElections = partitionsToElectLeader.filterNot(failedElections.keySet.contains)
      successfulElections.foreach { partition =>
        controllerContext.putPartitionState(partition, targetState)
      }

      failedElections
    } else {
      validPartitions.foreach { partition =>
        controllerContext.putPartitionState(partition, targetState)
      }
      Map.empty
    }
  }

  private def doLeaderElections(partitions: Seq[TopicPartition],
                                leaderElectionStrategy: PartitionLeaderElectionStrategy): Map[TopicPartition, Throwable] = {
    val failedElections = mutable.Map.empty[TopicPartition, Exception]
    val leaderIsrAndControllerEpochPerPartition = partitions.map { partition =>
      partition -> controllerContext.partitionLeadershipInfo(partition)
    }

    val (invalidPartitionsForElection, validPartitionsForElection) = leaderIsrAndControllerEpochPerPartition.partition { case (_, leaderIsrAndControllerEpoch) =>
      leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch
    }
    invalidPartitionsForElection.foreach { case (partition, leaderIsrAndControllerEpoch) =>
      val failMsg = s"aborted leader election for partition $partition since the LeaderAndIsr path was " +
        s"already written by another controller. This probably means that the current controller went through " +
        s"a soft failure and another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}."
      failedElections.put(partition, new StateChangeFailedException(failMsg))
    }

    val electionResults = leaderElectionStrategy match {
      case OfflinePartitionLeaderElectionStrategy =>
        val partitionsWithUncleanLeaderElectionState = validPartitionsForElection.map { case (partition, leaderIsrAndControllerEpoch) =>
          (partition, Some(leaderIsrAndControllerEpoch), uncleanLeaderElectionEnabled)
        }
        leaderForOffline(controllerContext, partitionsWithUncleanLeaderElectionState)
      case ReassignPartitionLeaderElectionStrategy =>
        leaderForReassign(controllerContext, validPartitionsForElection)
      case PreferredReplicaPartitionLeaderElectionStrategy =>
        leaderForPreferredReplica(controllerContext, validPartitionsForElection)
      case ControlledShutdownPartitionLeaderElectionStrategy =>
        leaderForControlledShutdown(controllerContext, validPartitionsForElection)
    }

    for (electionResult <- electionResults) {
      val partition = electionResult.topicPartition
      electionResult.leaderAndIsr match {
        case None =>
          val failMsg = s"Failed to elect leader for partition $partition under strategy $leaderElectionStrategy"
          failedElections.put(partition, new StateChangeFailedException(failMsg))
        case Some(leaderAndIsr) =>
          val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
          controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
      }
    }
    failedElections.toMap
  }

}
