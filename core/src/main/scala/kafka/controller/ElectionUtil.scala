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

case class ElectionResult(topicPartition: TopicPartition, leaderAndIsr: Option[LeaderAndIsr], liveReplicas: Seq[Int])

object ElectionUtil {

  def leaderForOffline(partition: TopicPartition,
                       leaderIsrAndControllerEpochOpt: Option[LeaderIsrAndControllerEpoch],
                       uncleanLeaderElectionEnabled: Boolean,
                       controllerContext: ControllerContext): ElectionResult = {
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
      ElectionResult(partition, newLeaderAndIsrOpt, liveReplicas)
    } else {
      ElectionResult(partition, None, liveReplicas)
    }
  }

  def leaderForOffline(controllerContext: ControllerContext,
                       partitionsWithUncleanLeaderElectionState: Seq[(TopicPartition, Option[LeaderIsrAndControllerEpoch], Boolean)]): Seq[ElectionResult] = {
    partitionsWithUncleanLeaderElectionState.map { case (partition, leaderIsrAndControllerEpochOpt, uncleanLeaderElectionEnabled) =>
      leaderForOffline(partition, leaderIsrAndControllerEpochOpt, uncleanLeaderElectionEnabled, controllerContext)
    }
  }

  def leaderForReassign(partition: TopicPartition,
                        leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                        controllerContext: ControllerContext): ElectionResult = {
    val reassignment = controllerContext.partitionsBeingReassigned(partition).newReplicas
    val liveReplicas = reassignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.reassignPartitionLeaderElection(reassignment, isr, liveReplicas.toSet)
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderIsrAndControllerEpoch.leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, reassignment)
  }

  def leaderForReassign(controllerContext: ControllerContext,
                        leaderIsrAndControllerEpochs: Seq[(TopicPartition, LeaderIsrAndControllerEpoch)]): Seq[ElectionResult] = {
    leaderIsrAndControllerEpochs.map { case (partition, leaderIsrAndControllerEpoch) =>
      leaderForReassign(partition, leaderIsrAndControllerEpoch, controllerContext)
    }
  }

  def leaderForPreferredReplica(partition: TopicPartition,
                                leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                controllerContext: ControllerContext): ElectionResult = {
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
    val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment, isr, liveReplicas.toSet)
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderIsrAndControllerEpoch.leaderAndIsr.newLeader(leader))
    ElectionResult(partition, newLeaderAndIsrOpt, assignment)
  }

  def leaderForPreferredReplica(controllerContext: ControllerContext,
                                leaderIsrAndControllerEpochs: Seq[(TopicPartition, LeaderIsrAndControllerEpoch)]): Seq[ElectionResult] = {
    leaderIsrAndControllerEpochs.map { case (partition, leaderIsrAndControllerEpoch) =>
      leaderForPreferredReplica(partition, leaderIsrAndControllerEpoch, controllerContext)
    }
  }

  def leaderForControlledShutdown(partition: TopicPartition,
                                  leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                  shuttingDownBrokerIds: Set[Int],
                                  controllerContext: ControllerContext): ElectionResult = {
    val assignment = controllerContext.partitionReplicaAssignment(partition)
    val liveOrShuttingDownReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition, includeShuttingDownBrokers = true))
    val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr
    val leaderOpt = PartitionLeaderElectionAlgorithms.controlledShutdownPartitionLeaderElection(assignment, isr,
      liveOrShuttingDownReplicas.toSet, shuttingDownBrokerIds)
    val newIsr = isr.filter(replica => !shuttingDownBrokerIds.contains(replica))
    val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderIsrAndControllerEpoch.leaderAndIsr.newLeaderAndIsr(leader, newIsr))
    ElectionResult(partition, newLeaderAndIsrOpt, liveOrShuttingDownReplicas)
  }

  def leaderForControlledShutdown(controllerContext: ControllerContext,
                                  leaderIsrAndControllerEpochs: Seq[(TopicPartition, LeaderIsrAndControllerEpoch)]): Seq[ElectionResult] = {
    val shuttingDownBrokerIds = controllerContext.shuttingDownBrokerIds.toSet
    leaderIsrAndControllerEpochs.map { case (partition, leaderIsrAndControllerEpoch) =>
      leaderForControlledShutdown(partition, leaderIsrAndControllerEpoch, shuttingDownBrokerIds, controllerContext)
    }
  }
}
