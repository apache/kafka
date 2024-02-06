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

package kafka.api

import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState
import org.apache.kafka.metadata.LeaderRecoveryState

object LeaderAndIsr {
  val InitialLeaderEpoch: Int = 0
  val InitialPartitionEpoch: Int = 0
  val NoLeader: Int = -1
  val NoEpoch: Int = -1
  val LeaderDuringDelete: Int = -2
  val EpochDuringDelete: Int = -2

  def apply(leader: Int, isr: List[Int]): LeaderAndIsr = {
    LeaderAndIsr(leader, InitialLeaderEpoch, isr, LeaderRecoveryState.RECOVERED, InitialPartitionEpoch)
  }

  def apply(leader: Int,
    leaderEpoch: Int,
    isr: List[Int],
    leaderRecoveryState: LeaderRecoveryState,
    partitionEpoch: Int): LeaderAndIsr = {
    LeaderAndIsr(
      leader,
      leaderEpoch,
      leaderRecoveryState,
      isr.map(brokerId => new BrokerState().setBrokerId(brokerId)),
      partitionEpoch)
  }

  def duringDelete(isr: List[Int]): LeaderAndIsr = LeaderAndIsr(LeaderDuringDelete, isr)
}

case class LeaderAndIsr(
  leader: Int,
  leaderEpoch: Int,
  leaderRecoveryState: LeaderRecoveryState,
  isrWithBrokerEpoch: List[BrokerState],
  // The current epoch for the partition for KRaft controllers. The current ZK version for the
  // legacy controllers. The epoch is a monotonically increasing value which is incremented
  // after every partition change.
  partitionEpoch: Int
) {
  def withPartitionEpoch(partitionEpoch: Int): LeaderAndIsr = copy(partitionEpoch = partitionEpoch)

  def newLeader(leader: Int): LeaderAndIsr = newLeaderAndIsrWithBrokerEpoch(leader, isrWithBrokerEpoch)

  def newLeaderAndIsr(leader: Int, isr: List[Int]): LeaderAndIsr = {
    LeaderAndIsr(leader, leaderEpoch + 1, isr, leaderRecoveryState, partitionEpoch)
  }

  private def newLeaderAndIsrWithBrokerEpoch(leader: Int, isrWithBrokerEpoch: List[BrokerState]): LeaderAndIsr = {
    LeaderAndIsr(leader, leaderEpoch + 1, leaderRecoveryState, isrWithBrokerEpoch, partitionEpoch)
  }

  def newRecoveringLeaderAndIsr(leader: Int, isr: List[Int]): LeaderAndIsr = {
    LeaderAndIsr(leader, leaderEpoch + 1, isr, LeaderRecoveryState.RECOVERING, partitionEpoch)
  }

  def newEpoch: LeaderAndIsr = newLeaderAndIsrWithBrokerEpoch(leader, isrWithBrokerEpoch)

  def leaderOpt: Option[Int] = {
    if (leader == LeaderAndIsr.NoLeader) None else Some(leader)
  }

  def isr: List[Int] = {
    isrWithBrokerEpoch.map(_.brokerId())
  }

  def equalsAllowStalePartitionEpoch(other: LeaderAndIsr): Boolean = {
    if (this == other) {
      true
    } else if (other == null) {
      false
    } else {
      leader == other.leader &&
        leaderEpoch == other.leaderEpoch &&
        isrWithBrokerEpoch.equals(other.isrWithBrokerEpoch) &&
        leaderRecoveryState == other.leaderRecoveryState &&
        partitionEpoch <= other.partitionEpoch
    }
  }

  override def toString: String = {
    s"LeaderAndIsr(leader=$leader, leaderEpoch=$leaderEpoch, isrWithBrokerEpoch=$isrWithBrokerEpoch, leaderRecoveryState=$leaderRecoveryState, partitionEpoch=$partitionEpoch)"
  }
}
