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

import kafka.controller.PartitionLeaderElectionAlgorithms.OfflineElectionResult
import kafka.server.OffsetAndEpoch
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class PartitionLeaderElectionAlgorithmsTest {

  @Test
  def testOfflinePartitionLeaderElection(): Unit = {
    val assignment = Seq(2, 4)
    val isr = Seq(2, 4)
    val liveReplicas = Set(4)
    val corruptedReplicas = Set.empty[Int]
    val result = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(assignment,
      isr,
      liveReplicas,
      corruptedReplicas,
      uncleanLeaderElectionEnabled = false)
    assertEquals(OfflineElectionResult.CleanLeader(4), result)
  }

  @Test
  def testOfflinePartitionLeaderElectionLastIsrOfflineUncleanLeaderElectionDisabled(): Unit = {
    val assignment = Seq(2, 4)
    val isr = Seq(2)
    val liveReplicas = Set(4)
    val corruptedReplicas = Set.empty[Int]
    val result = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(assignment,
      isr,
      liveReplicas,
      corruptedReplicas,
      uncleanLeaderElectionEnabled = false)
    assertEquals(OfflineElectionResult.NoLeader, result)
  }

  @Test
  def testOfflinePartitionLeaderElectionLastIsrOfflineUncleanLeaderElectionEnabled(): Unit = {
    val assignment = Seq(2, 4, 6)
    val isr = Seq(2)
    val liveReplicas = Set(4, 6)
    val corruptedReplicas = Set.empty[Int]
    val result = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(assignment,
      isr,
      liveReplicas,
      corruptedReplicas,
      uncleanLeaderElectionEnabled = true)
    assertEquals(OfflineElectionResult.UncleanLeader(4), result)
  }

  @Test
  def testOfflinePartitionLeaderElectionCorruptedBrokerNotSelected(): Unit = {
    val assignment = Seq(2, 4, 6)
    val isr = Seq(2)
    val liveReplicas = Set(4, 6)
    val corruptedReplicas = Set(4)
    val result = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(assignment,
      isr,
      liveReplicas,
      corruptedReplicas,
      uncleanLeaderElectionEnabled = true)
    assertEquals(OfflineElectionResult.UncleanLeader(6), result)
  }

  @Test
  def testOfflinePartitionLeaderElectionCorruptedBrokerSelected(): Unit = {
    val assignment = Seq(2, 4, 6)
    val isr = Seq(2)
    val liveReplicas = Set(4)
    val corruptedReplicas = Set(4)
    val result = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(assignment,
      isr,
      liveReplicas,
      corruptedReplicas,
      uncleanLeaderElectionEnabled = true)
    assertEquals(OfflineElectionResult.CorruptedUncleanLeader(4), result)
  }

  @Test
  def testOfflinePartitionLeaderElectionNoLeaderElected(): Unit = {
    val assignment = Seq(2, 4, 6)
    val isr = Seq(2)
    val liveReplicas = Set.empty[Int]
    val corruptedReplicas = Set.empty[Int]
    val result = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(assignment,
      isr,
      liveReplicas,
      corruptedReplicas,
      uncleanLeaderElectionEnabled = true)
    assertEquals(OfflineElectionResult.NoLeader, result)
  }

  @Test
  def testDelayedElectionReplicaWithHigherEpochElected(): Unit = {
    val assignment = Seq(2, 4, 6)
    val liveReplicas = Set(2, 4, 6)
    val brokerIdToOffsetAndEpoch = Map(
      4 -> OffsetAndEpoch(offset = 1001, leaderEpoch = 200),
      6 -> OffsetAndEpoch(offset = 1000, leaderEpoch = 201),
    )
    val leaderOpt = PartitionLeaderElectionAlgorithms.delayedPartitionLeaderElection(
      brokerIdToOffsetAndEpoch, assignment, liveReplicas)
    assertEquals(Some(6), leaderOpt)
  }

  @Test
  def testDelayedElectionReplicaWithHigherOffsetElectedWhenEpochEqual(): Unit = {
    val assignment = Seq(2, 4, 6)
    val liveReplicas = Set(2, 4, 6)
    val brokerIdToOffsetAndEpoch = Map(
      2 -> OffsetAndEpoch(offset = 1001, leaderEpoch = 200),
      4 -> OffsetAndEpoch(offset = 1002, leaderEpoch = 200),
    )
    val leaderOpt = PartitionLeaderElectionAlgorithms.delayedPartitionLeaderElection(
      brokerIdToOffsetAndEpoch, assignment, liveReplicas)
    assertEquals(Some(4), leaderOpt)
  }

  @Test
  def testDelayedElectionLiveReplicaElected(): Unit = {
    val assignment = Seq(2, 4, 6)
    val liveReplicas = Set(4)
    val brokerIdToOffsetAndEpoch = Map(
      2 -> OffsetAndEpoch(offset = 1001, leaderEpoch = 200),
      4 -> OffsetAndEpoch(offset = 1000, leaderEpoch = 200),
    )
    val leaderOpt = PartitionLeaderElectionAlgorithms.delayedPartitionLeaderElection(
      brokerIdToOffsetAndEpoch, assignment, liveReplicas)
    assertEquals(Some(4), leaderOpt)
  }

  @Test
  def testDelayedElectionNoLiveReplicas(): Unit = {
    val assignment = Seq(2, 4, 6)
    val liveReplicas = Set.empty[Int]
    val brokerIdToOffsetAndEpoch = Map(
      2 -> OffsetAndEpoch(offset = 1001, leaderEpoch = 200),
      4 -> OffsetAndEpoch(offset = 1002, leaderEpoch = 200),
    )
    val leaderOpt = PartitionLeaderElectionAlgorithms.delayedPartitionLeaderElection(
      brokerIdToOffsetAndEpoch, assignment, liveReplicas)
    assertEquals(None, leaderOpt)
  }

  @Test
  def testDelayedElectionNoOffsetsReceived(): Unit = {
    val assignment = Seq(2, 4, 6)
    val liveReplicas = Set(4, 6)
    val brokerIdToOffsetAndEpoch = Map.empty[Int, OffsetAndEpoch]
    val leaderOpt = PartitionLeaderElectionAlgorithms.delayedPartitionLeaderElection(
      brokerIdToOffsetAndEpoch, assignment, liveReplicas)
    assertEquals(Some(4), leaderOpt)
  }

  @Test
  def testReassignPartitionLeaderElection(): Unit = {
    val reassignment = Seq(2, 4)
    val isr = Seq(2, 4)
    val liveReplicas = Set(4)
    val leaderOpt = PartitionLeaderElectionAlgorithms.reassignPartitionLeaderElection(reassignment,
      isr,
      liveReplicas)
    assertEquals(Option(4), leaderOpt)
  }

  @Test
  def testReassignPartitionLeaderElectionWithNoLiveIsr(): Unit = {
    val reassignment = Seq(2, 4)
    val isr = Seq(2)
    val liveReplicas = Set.empty[Int]
    val leaderOpt = PartitionLeaderElectionAlgorithms.reassignPartitionLeaderElection(reassignment,
      isr,
      liveReplicas)
    assertEquals(None, leaderOpt)
  }

  @Test
  def testReassignPartitionLeaderElectionWithEmptyIsr(): Unit = {
    val reassignment = Seq(2, 4)
    val isr = Seq.empty[Int]
    val liveReplicas = Set(2)
    val leaderOpt = PartitionLeaderElectionAlgorithms.reassignPartitionLeaderElection(reassignment,
      isr,
      liveReplicas)
    assertEquals(None, leaderOpt)
  }

  @Test
  def testPreferredReplicaPartitionLeaderElection(): Unit = {
    val assignment = Seq(2, 4)
    val isr = Seq(2, 4)
    val liveReplicas = Set(2, 4)
    val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment,
      isr,
      liveReplicas)
    assertEquals(Option(2), leaderOpt)
  }

  @Test
  def testPreferredReplicaPartitionLeaderElectionPreferredReplicaInIsrNotLive(): Unit = {
    val assignment = Seq(2, 4)
    val isr = Seq(2)
    val liveReplicas = Set.empty[Int]
    val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment,
      isr,
      liveReplicas)
    assertEquals(None, leaderOpt)
  }

  @Test
  def testPreferredReplicaPartitionLeaderElectionPreferredReplicaNotInIsrLive(): Unit = {
    val assignment = Seq(2, 4)
    val isr = Seq(4)
    val liveReplicas = Set(2, 4)
    val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment,
      isr,
      liveReplicas)
    assertEquals(None, leaderOpt)
  }

  @Test
  def testPreferredReplicaPartitionLeaderElectionPreferredReplicaNotInIsrNotLive(): Unit = {
    val assignment = Seq(2, 4)
    val isr = Seq.empty[Int]
    val liveReplicas = Set.empty[Int]
    val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment,
      isr,
      liveReplicas)
    assertEquals(None, leaderOpt)
  }

  @Test
  def testControlledShutdownPartitionLeaderElection(): Unit = {
    val assignment = Seq(2, 4)
    val isr = Seq(2, 4)
    val liveReplicas = Set(2, 4)
    val shuttingDownBrokers = Set(2)
    val leaderOpt = PartitionLeaderElectionAlgorithms.controlledShutdownPartitionLeaderElection(assignment,
      isr,
      liveReplicas,
      shuttingDownBrokers)
    assertEquals(Option(4), leaderOpt)
  }

  @Test
  def testControlledShutdownPartitionLeaderElectionLastIsrShuttingDown(): Unit = {
    val assignment = Seq(2, 4)
    val isr = Seq(2)
    val liveReplicas = Set(2, 4)
    val shuttingDownBrokers = Set(2)
    val leaderOpt = PartitionLeaderElectionAlgorithms.controlledShutdownPartitionLeaderElection(assignment,
      isr,
      liveReplicas,
      shuttingDownBrokers)
    assertEquals(None, leaderOpt)
  }

  @Test
  def testControlledShutdownPartitionLeaderElectionAllIsrSimultaneouslyShutdown(): Unit = {
    val assignment = Seq(2, 4)
    val isr = Seq(2, 4)
    val liveReplicas = Set(2, 4)
    val shuttingDownBrokers = Set(2, 4)
    val leaderOpt = PartitionLeaderElectionAlgorithms.controlledShutdownPartitionLeaderElection(assignment,
      isr,
      liveReplicas,
      shuttingDownBrokers)
    assertEquals(None, leaderOpt)
  }
}
