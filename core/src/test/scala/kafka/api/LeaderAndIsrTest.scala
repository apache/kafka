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

import org.apache.kafka.metadata.LeaderRecoveryState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

final class LeaderAndIsrTest {
  @Test
  def testRecoveringLeaderAndIsr(): Unit = {
    val leaderAndIsr = LeaderAndIsr(1, List(1, 2))
    val recoveringLeaderAndIsr = leaderAndIsr.newRecoveringLeaderAndIsr(3, List(3))

    assertEquals(3, recoveringLeaderAndIsr.leader)
    assertEquals(List(3), recoveringLeaderAndIsr.isr)
    assertEquals(LeaderRecoveryState.RECOVERING, recoveringLeaderAndIsr.leaderRecoveryState)
  }

  @Test
  def testNewLeaderAndIsr(): Unit = {
    val leaderAndIsr = LeaderAndIsr(1, List(1, 2))
    val newLeaderAndIsr = leaderAndIsr.newLeaderAndIsr(2, List(1, 2))

    assertEquals(2, newLeaderAndIsr.leader)
    assertEquals(List(1, 2), newLeaderAndIsr.isr)
    assertEquals(LeaderRecoveryState.RECOVERED, newLeaderAndIsr.leaderRecoveryState)
  }

  @Test
  def testNewLeader(): Unit = {
    val leaderAndIsr = LeaderAndIsr(2, List(1, 2, 3))

    assertEquals(2, leaderAndIsr.leader)
    assertEquals(List(1, 2, 3), leaderAndIsr.isr)

    val newLeaderAndIsr = leaderAndIsr.newLeader(3)

    assertEquals(3, newLeaderAndIsr.leader)
    assertEquals(List(1, 2, 3), newLeaderAndIsr.isr)
  }

  @Test
  def testNewEpoch() : Unit = {
    val leaderAndIsr = LeaderAndIsr(3, List(1, 2, 3))

    assertEquals(0, leaderAndIsr.leaderEpoch)

    val leaderWithNewEpoch = leaderAndIsr.newEpoch

    assertEquals(1, leaderWithNewEpoch.leaderEpoch)
  }

  @Test
  def testLeaderOpt() : Unit = {
    val leaderAndIsr = LeaderAndIsr(2, List(1, 2, 3))

    assertEquals(2, leaderAndIsr.leaderOpt.get)
  }
}
