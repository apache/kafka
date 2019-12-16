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
import org.apache.kafka.common.TopicPartition

import scala.collection.Seq

case class ElectionResult(topicPartition: TopicPartition, leaderAndIsr: Option[LeaderAndIsr], liveReplicas: Seq[Int])

object Election {

  private def leaderForOffline(partition: TopicPartition,
                               leaderAndIsrOpt: Option[LeaderAndIsr],
                               uncleanLeaderElectionEnabled: Boolean,
                               controllerContext: ControllerContext): ElectionResult = {

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
          leaderAndIsr.newLeaderAndIsr(leader, newIsr)
        }
        ElectionResult(partition, newLeaderAndIsrOpt, liveReplicas)

      case None =>
        ElectionResult(partition, None, liveReplicas)
    }
  }

  /**
   * Elect leaders for new or offline partitions.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param partitionsWithUncleanLeaderElectionState A sequence of tuples representing the partitions
   *                                                 that need election, their leader/ISR state, and whether
   *                                                 or not unclean leader election is enabled
   *
   * @return The election results
   */
  def leaderForOffline(
    controllerContext: ControllerContext,
    partitionsWithUncleanLeaderElectionState: Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)]
  ): Seq[ElectionResult] = {
    partitionsWithUncleanLeaderElectionState.map {
      case (partition, leaderAndIsrOpt, uncleanLeaderElectionEnabled) =>
        leaderForOffline(partition, leaderAndIsrOpt, uncleanLeaderElectionEnabled, controllerContext)
    }
  }

  private def leaderForReassign(partition: TopicPartition,
                                leaderAndIsr: LeaderAndIsr,
                                controllerContext: ControllerContext): ElectionResult = {
    val targetReplicas = controllerContext.partitionFullReplicaAssignment(partition).targetReplicas
    val liveReplicas = targetReplicas.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    val isr = leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.reassignPartitionLeaderElection(targetReplicas, isr, liveReplicas.toSet)
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, targetReplicas)
  }

  /**
   * Elect leaders for partitions that are undergoing reassignment.
   *
   * @param controllerContext Context with the current state of the cluster
   * @param leaderAndIsrs A sequence of tuples representing the partitions that need election
   *                                     and their respective leader/ISR states
   *
   * @return The election results
   */
  def leaderForReassign(controllerContext: ControllerContext,
                        leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForReassign(partition, leaderAndIsr, controllerContext)
    }
  }

  private def leaderForPreferredReplica(partition: TopicPartition,
                                        leaderAndIsr: LeaderAndIsr,
                                        controllerContext: ControllerContext): ElectionResult = {
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    val isr = leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment, isr, liveReplicas.toSet)
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, assignment)
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
                                leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForPreferredReplica(partition, leaderAndIsr, controllerContext)
    }
  }

  private def leaderForControlledShutdown(partition: TopicPartition,
                                          leaderAndIsr: LeaderAndIsr,
                                          shuttingDownBrokerIds: Set[Int],
                                          controllerContext: ControllerContext): ElectionResult = {
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveOrShuttingDownReplicas = assignment.filter(replica =>
      controllerContext.isReplicaOnline(replica, partition, includeShuttingDownBrokers = true))
    val isr = leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.controlledShutdownPartitionLeaderElection(assignment, isr,
      liveOrShuttingDownReplicas.toSet, shuttingDownBrokerIds)
    val newIsr = isr.filter(replica => !shuttingDownBrokerIds.contains(replica))
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderAndIsr.newLeaderAndIsr(leader, newIsr))
    ElectionResult(partition, newLeaderAndIsrOpt, liveOrShuttingDownReplicas)
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
  def leaderForControlledShutdown(controllerContext: ControllerContext,
                                  leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)]): Seq[ElectionResult] = {
    val shuttingDownBrokerIds = controllerContext.shuttingDownBrokerIds.toSet
    leaderAndIsrs.map { case (partition, leaderAndIsr) =>
      leaderForControlledShutdown(partition, leaderAndIsr, shuttingDownBrokerIds, controllerContext)
    }
  }
}
