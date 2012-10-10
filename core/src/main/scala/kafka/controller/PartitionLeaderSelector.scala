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
import kafka.utils.Logging
import kafka.common.{StateChangeFailedException, PartitionOfflineException}

trait PartitionLeaderSelector {

  /**
   * @param topic                      The topic of the partition whose leader needs to be elected
   * @param partition                  The partition whose leader needs to be elected
   * @param assignedReplicas           The list of replicas assigned to the input partition
   * @param currentLeaderAndIsr        The current leader and isr of input partition read from zookeeper
   * @throws PartitionOfflineException If no replica in the assigned replicas list is alive
   * @returns The leader and isr request, with the newly selected leader info, to send to the brokers
   * Also, returns the list of replicas the returned leader and isr request should be sent to
   * This API selects a new leader for the input partition
   */
  def selectLeader(topic: String, partition: Int, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int])

}

/**
 * This API selects a new leader for the input partition -
 * 1. If at least one broker from the isr is alive, it picks a broker from the isr as the new leader
 * 2. Else, it picks some alive broker from the assigned replica list as the new leader
 * 3. If no broker in the assigned replica list is alive, it throws PartitionOfflineException
 * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
 */
class OfflinePartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  def selectLeader(topic: String, partition: Int, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    controllerContext.partitionReplicaAssignment.get((topic, partition)) match {
      case Some(assignedReplicas) =>
        val liveAssignedReplicasToThisPartition = assignedReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
        val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(r => controllerContext.liveBrokerIds.contains(r))
        val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
        val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
        debug("Leader, epoch, ISR and zkPathVersion for partition (%s, %d) are: [%d], [%d], [%s], [%d]"
          .format(topic, partition, currentLeaderAndIsr.leader, currentLeaderEpoch, currentLeaderAndIsr.isr,
          currentLeaderIsrZkPathVersion))
        val newLeaderAndIsr = liveBrokersInIsr.isEmpty match {
          case true =>
            debug("No broker is ISR is alive, picking the leader from the alive assigned replicas: %s"
              .format(liveAssignedReplicasToThisPartition.mkString(",")))
            liveAssignedReplicasToThisPartition.isEmpty match {
              case true =>
                ControllerStat.offlinePartitionRate.mark()
                throw new PartitionOfflineException(("No replica for partition " +
                  "([%s, %d]) is alive. Live brokers are: [%s],".format(topic, partition, controllerContext.liveBrokerIds)) +
                  " Assigned replicas are: [%s]".format(assignedReplicas))
              case false =>
                ControllerStat.uncleanLeaderElectionRate.mark()
                val newLeader = liveAssignedReplicasToThisPartition.head
                warn("No broker in ISR is alive, elected leader from the alive replicas is [%s], ".format(newLeader) +
                  "There's potential data loss")
                new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, List(newLeader), currentLeaderIsrZkPathVersion + 1)
            }
          case false =>
            val newLeader = liveBrokersInIsr.head
            debug("Some broker in ISR is alive, selecting the leader from the ISR: " + newLeader)
            new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr.toList, currentLeaderIsrZkPathVersion + 1)
        }
        info("Selected new leader and ISR %s for offline partition [%s, %d]".format(newLeaderAndIsr.toString(), topic,
          partition))
        (newLeaderAndIsr, liveAssignedReplicasToThisPartition)
      case None =>
        ControllerStat.offlinePartitionRate.mark()
        throw new PartitionOfflineException("Partition [%s, %d] doesn't have".format(topic, partition) +
                                            "replicas assigned to it")
    }
  }
}

/**
 * Picks one of the alive in-sync reassigned replicas as the new leader
 */
class ReassignedPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  def selectLeader(topic: String, partition: Int, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val reassignedReplicas = controllerContext.partitionsBeingReassigned((topic, partition)).newReplicas
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
    debug("Leader, epoch, ISR and zkPathVersion for partition (%s, %d) are: [%d], [%d], [%s], [%d]"
      .format(topic, partition, currentLeaderAndIsr.leader, currentLeaderEpoch, currentLeaderAndIsr.isr,
      currentLeaderIsrZkPathVersion))
    // pick any replica from the newly assigned replicas list that is in the ISR
    val aliveReassignedReplicas = reassignedReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
    val newLeaderOpt = aliveReassignedReplicas.headOption
    newLeaderOpt match {
      case Some(newLeader) => (new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, currentLeaderAndIsr.isr,
                              currentLeaderIsrZkPathVersion + 1), reassignedReplicas)
      case None =>
        reassignedReplicas.size match {
          case 0 =>
            throw new StateChangeFailedException("List of reassigned replicas for partition " +
              "([%s, %d]) is empty. Current leader and ISR: [%s]".format(topic, partition, currentLeaderAndIsr))
          case _ =>
            throw new StateChangeFailedException("None of the reassigned replicas for partition " +
              "([%s, %d]) are alive. Current leader and ISR: [%s]".format(topic, partition, currentLeaderAndIsr))
        }
    }
  }
}