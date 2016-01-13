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

import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.utils.{ZkUtils, ReplicationUtils, Logging}
import kafka.common.{LeaderElectionNotNeededException, TopicAndPartition, StateChangeFailedException, NoReplicaOnlineException}
import kafka.server.{ConfigType, KafkaConfig}

trait PartitionLeaderSelector {

  /**
   * @param topicAndPartition          The topic and partition whose leader needs to be elected
   * @param currentLeaderAndIsr        The current leader and isr of input partition read from zookeeper
   * @throws NoReplicaOnlineException If no replica in the assigned replicas list is alive
   * @return The leader and isr request, with the newly selected leader and isr, and the set of replicas to receive
   * the LeaderAndIsrRequest.
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int])

}

/**
 * Select the new leader, new isr and receiving replicas (for the LeaderAndIsrRequest):
 * 1. If at least one broker from the isr is alive, it picks a broker from the live isr as the new leader and the live
 *    isr as the new isr.
 * 2. Else, if unclean leader election for the topic is disabled, it throws a NoReplicaOnlineException.
 * 3. Else, it picks some alive broker from the assigned replica list as the new leader and the new isr.
 * 4. If no broker in the assigned replica list is alive, it throws a NoReplicaOnlineException
 * Replicas to receive LeaderAndIsr request = live assigned replicas
 */
class OfflinePartitionLeaderSelector(zkUtils: ZkUtils, controllerContext: ControllerContext, config: KafkaConfig)
  extends PartitionLeaderSelector with Logging {
  this.logIdent = "[OfflinePartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    controllerContext.partitionReplicaAssignment.get(topicAndPartition) match {
      case Some(assignedReplicas) =>
        val liveBrokerIds = controllerContext.liveBrokerIds
        val liveAssignedReplicas = assignedReplicas.filter(liveBrokerIds.contains)
        val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(liveBrokerIds.contains)
        val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
        val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
        val newLeaderAndIsr =
          if (liveBrokersInIsr.isEmpty) {
            // Prior to electing an unclean (i.e. non-ISR) leader, ensure that doing so is not disallowed by the configuration
            // for unclean leader election.
            if (!LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(controllerContext.zkUtils,
              ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
              var zkUpdateSucceeded = false
              while (!zkUpdateSucceeded) {
                val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topicAndPartition.topic,
                  topicAndPartition.partition, currentLeaderAndIsr.copy(leader = -1), controllerContext.epoch,
                  currentLeaderAndIsr.zkVersion)
                zkUpdateSucceeded = updateSucceeded
              }
              throw new NoReplicaOnlineException(s"No broker in ISR for partition $topicAndPartition is alive." +
                s" Live brokers are: [${liveBrokerIds.mkString(",")}], ISR brokers are: [${currentLeaderAndIsr.isr.mkString(",")}]")
            }

            debug(s"No broker in ISR is alive for $topicAndPartition. Pick the leader from the live assigned replicas:" +
              s" [${liveAssignedReplicas.mkString(",")}]")
            liveAssignedReplicas.headOption.map { newLeader =>
              ControllerStats.uncleanLeaderElectionRate.mark()
              warn(s"No broker in ISR is alive for $topicAndPartition. Elect leader $newLeader from live brokers" +
                s" [${liveAssignedReplicas.mkString(",")}]. There's potential data loss.")
              new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, List(newLeader), currentLeaderIsrZkPathVersion + 1)
            }.getOrElse {
              throw new NoReplicaOnlineException(s"No replica for partition $topicAndPartition is alive. Live brokers" +
                s" are: [${liveBrokerIds.mkString(",")}], Assigned replicas are: [${assignedReplicas.mkString(",")}]")
            }
          }
          else {
            val newLeader = liveAssignedReplicas.find(liveBrokersInIsr.contains).getOrElse {
              throw new IllegalStateException(s"Could not find any broker in both live assigned replicas" +
                s" [${liveAssignedReplicas.mkString}] and ISR [${liveBrokersInIsr.mkString(",")}]")
            }
            debug(s"Some broker in ISR is alive for $topicAndPartition. Select $newLeader from ISR" +
              s" [${liveBrokersInIsr.mkString(",")}] to be the leader.")
            new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr.toList, currentLeaderIsrZkPathVersion + 1)
          }
        info(s"Selected new leader and ISR $newLeaderAndIsr for offline partition $topicAndPartition")
        (newLeaderAndIsr, liveAssignedReplicas)
      case None =>
        throw new NoReplicaOnlineException(s"Partition $topicAndPartition doesn't have replicas assigned to it")
    }
  }
}

/**
 * New leader = a live in-sync reassigned replica
 * New isr = current isr
 * Replicas to receive LeaderAndIsr request = reassigned replicas
 */
class ReassignedPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {
  this.logIdent = "[ReassignedPartitionLeaderSelector]: "

  /**
   * The reassigned replicas are already in the ISR when selectLeader is called.
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned(topicAndPartition).newReplicas
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
    val aliveReassignedInSyncReplicas = reassignedInSyncReplicas.filter(r => controllerContext.liveBrokerIds.contains(r) &&
                                                                             currentLeaderAndIsr.isr.contains(r))
    aliveReassignedInSyncReplicas.headOption match {
      case Some(newLeader) => (new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, currentLeaderAndIsr.isr,
        currentLeaderIsrZkPathVersion + 1), reassignedInSyncReplicas)
      case None =>
        reassignedInSyncReplicas.size match {
          case 0 =>
            throw new NoReplicaOnlineException("List of reassigned replicas for partition" +
              s" $topicAndPartition is empty. Current leader and ISR: [$currentLeaderAndIsr]")
          case _ =>
            throw new NoReplicaOnlineException("None of the reassigned replicas for partition" +
              s" $topicAndPartition are in-sync with the leader. Current leader and ISR: [$currentLeaderAndIsr]")
        }
    }
  }
}

/**
 * New leader = preferred (first assigned) replica (if in isr and alive);
 * New isr = current isr;
 * Replicas to receive LeaderAndIsr request = assigned replicas
 */
class PreferredReplicaPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector
with Logging {
  this.logIdent = "[PreferredReplicaPartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    val preferredReplica = assignedReplicas.head
    // check if preferred replica is the current leader
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    if (currentLeader == preferredReplica) {
      throw new LeaderElectionNotNeededException(s"Preferred replica $preferredReplica is already the current leader" +
        s" for partition $topicAndPartition")
    } else {
      info(s"Current leader $currentLeader for partition $topicAndPartition is not the preferred replica." +
        " Triggering preferred replica leader election")
      // check if preferred replica is not the current leader and is alive and in the isr
      if (controllerContext.liveBrokerIds.contains(preferredReplica) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
        (new LeaderAndIsr(preferredReplica, currentLeaderAndIsr.leaderEpoch + 1, currentLeaderAndIsr.isr,
          currentLeaderAndIsr.zkVersion + 1), assignedReplicas)
      } else {
        throw new StateChangeFailedException(s"Preferred replica $preferredReplica for partition $topicAndPartition" +
          s" is either not alive or not in the ISR. Current leader and ISR: [$currentLeaderAndIsr]")
      }
    }
  }
}

/**
 * New leader = replica in isr that's not being shutdown;
 * New isr = current isr - shutdown replica;
 * Replicas to receive LeaderAndIsr request = live assigned replicas
 */
class ControlledShutdownLeaderSelector(controllerContext: ControllerContext)
        extends PartitionLeaderSelector
        with Logging {

  this.logIdent = "[ControlledShutdownLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion

    val currentLeader = currentLeaderAndIsr.leader

    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
    val liveAssignedReplicas = assignedReplicas.filter(liveOrShuttingDownBrokerIds.contains)

    val newIsr = currentLeaderAndIsr.isr.filter(!controllerContext.shuttingDownBrokerIds.contains(_))
    newIsr.headOption match {
      case Some(newLeader) =>
        debug(s"Partition $topicAndPartition : current leader = $currentLeader, new leader = $newLeader")
        (LeaderAndIsr(newLeader, currentLeaderEpoch + 1, newIsr, currentLeaderIsrZkPathVersion + 1), liveAssignedReplicas)
      case None =>
        throw new StateChangeFailedException(s"No other replicas in ISR [${currentLeaderAndIsr.isr.mkString(",")}] for" +
          s" $topicAndPartition besides shutting down brokers [${controllerContext.shuttingDownBrokerIds.mkString(",")}]")
    }
  }
}

/**
 * Essentially does nothing. Returns the current leader and ISR, and the current
 * set of replicas assigned to a given topic/partition.
 */
class NoOpLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  this.logIdent = "[NoOpLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    warn("I should never have been asked to perform leader election, returning the current LeaderAndIsr and replica assignment.")
    (currentLeaderAndIsr, controllerContext.partitionReplicaAssignment(topicAndPartition))
  }
}
