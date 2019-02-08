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

object LeaderAndIsr {
  val initialLeaderEpoch: Int = 0
  val initialZKVersion: Int = 0
  val NoLeader: Int = -1
  val LeaderDuringDelete: Int = -2

  def apply(leader: Int, isr: List[Int]): LeaderAndIsr = LeaderAndIsr(leader, initialLeaderEpoch, isr, initialZKVersion)

  def duringDelete(isr: List[Int]): LeaderAndIsr = LeaderAndIsr(LeaderDuringDelete, isr)
}

case class LeaderAndIsr(leader: Int,
                        leaderEpoch: Int,
                        isr: List[Int],
                        zkVersion: Int) {
  def withZkVersion(zkVersion: Int) = copy(zkVersion = zkVersion)

  def newLeader(leader: Int) = newLeaderAndIsr(leader, isr)

  def newLeaderAndIsr(leader: Int, isr: List[Int]) = LeaderAndIsr(leader, leaderEpoch + 1, isr, zkVersion)

  def newEpochAndZkVersion = newLeaderAndIsr(leader, isr)

  override def toString: String = {
    s"LeaderAndIsr(leader=$leader, leaderEpoch=$leaderEpoch, isr=$isr, zkVersion=$zkVersion)"
  }
}
