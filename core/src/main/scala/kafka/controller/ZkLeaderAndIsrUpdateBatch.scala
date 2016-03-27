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
import kafka.common.TopicAndPartition
import kafka.utils.{ReplicationUtils, ZkUtils}

/**
 * The class that batches the LeaderAndisrUpdate together.
 */
class ZkLeaderAndIsrUpdateBatch(zkUtils: ZkUtils) {
  val leaderAndIsrUpdates = new collection.mutable.HashMap[TopicAndPartition, ZkLeaderAndIsrUpdate]

  def newBatch() = leaderAndIsrUpdates.clear()

  def addLeaderAndIsrUpdate(topicAndPartition: TopicAndPartition,
                            newLeaderAndIsr: LeaderAndIsr,
                            expectZkVersion: Int,
                            onSuccess: (TopicAndPartition, ZkLeaderAndIsrUpdateResult) => Unit) = {
    // We need to chain up the callbacks if there are multiple callbacks to be fired for the same partition.
    // This is used by the ReplicaStateMachine where multiple replicas of the same partition can change in the same
    // batch.
    val onSuccessCallbacks = {
      if (leaderAndIsrUpdates.contains(topicAndPartition))
        leaderAndIsrUpdates(topicAndPartition).onSuccessCallbacks :+ onSuccess
      else
        List(onSuccess)
    }
    leaderAndIsrUpdates += (topicAndPartition -> new ZkLeaderAndIsrUpdate(newLeaderAndIsr, expectZkVersion, onSuccessCallbacks))
  }

  def completeLeaderAndIsrUpdate(topicAndPartition: TopicAndPartition) = {
    leaderAndIsrUpdates -= topicAndPartition
  }

  def size = this synchronized(leaderAndIsrUpdates.size)

  def containsPartition(topicAndPartition: TopicAndPartition): Boolean =
    leaderAndIsrUpdates.contains(topicAndPartition)

  def pendingLeaderAndIsrUpdate(topicAndPartition: TopicAndPartition) = {
    leaderAndIsrUpdates(topicAndPartition)
  }

  def writeLeaderAndIsrUpdateToZk(controllerEpoch: Int) =
    ReplicationUtils.asyncUpdateLeaderAndIsr(zkUtils, this, controllerEpoch)
}

/**
 * This is a container class to host the LeaderAndIsr updates in zookeeper.
 * @param newLeaderAndIsr The new LeaderAndIsr information.
 * @param expectZkVersion The expected zkVersion of the path
 * @param onSuccessCallbacks The callbacks to fire when the LeaderAndIsr update succeeded. We need a list because there
 *                           might be multiple replica state changes for the same partition in one batch.
 */
class ZkLeaderAndIsrUpdate(val newLeaderAndIsr: LeaderAndIsr,
                           val expectZkVersion: Int,
                           val onSuccessCallbacks: List[(TopicAndPartition, ZkLeaderAndIsrUpdateResult) => Unit])

case class ZkLeaderAndIsrUpdateResult(val leaderAndIsr: LeaderAndIsr,
                                      val newZkVersion: Int)

case class ZkLeaderAndIsrReadResult(val leaderIsrAndControllerEpochOpt: Option[LeaderIsrAndControllerEpoch],
                                    val exceptionOpt: Option[Exception]) {
  override def toString: String = "[" + leaderIsrAndControllerEpochOpt + "," + exceptionOpt + "]"
}
