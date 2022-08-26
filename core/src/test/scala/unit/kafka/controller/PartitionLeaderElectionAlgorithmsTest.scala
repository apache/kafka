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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

class PartitionLeaderElectionAlgorithmsTest {
  private var controllerContext: ControllerContext = _

  @BeforeEach
  def setUp(): Unit = {
    controllerContext = new ControllerContext
    controllerContext.stats.removeMetric("UncleanLeaderElectionsPerSec")
  }

  @Test
  def testOfflinePartitionLeaderElection(): Unit = {
    val assignment = Seq(2, 4)
    val isr = Seq(2, 4)
    val liveReplicas = Set(4)
    val leaderOpt = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(assignment,
      isr,
      liveReplicas,
      uncleanLeaderElectionEnabled = false,
      controllerContext)
    assertEquals(Option(4), leaderOpt)
  }

  @Test
  def testOfflinePartitionLeaderElectionLastIsrOfflineUncleanLeaderElectionDisabled(): Unit = {
    val assignment = Seq(2, 4)
    val isr = Seq(2)
    val liveReplicas = Set(4)
    val leaderOpt = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(assignment,
      isr,
      liveReplicas,
      uncleanLeaderElectionEnabled = false,
      controllerContext)
    assertEquals(None, leaderOpt)
    assertEquals(0, controllerContext.stats.uncleanLeaderElectionRate.count())
  }

  @Test
  def testOfflinePartitionLeaderElectionLastIsrOfflineUncleanLeaderElectionEnabled(): Unit = {
    val assignment = Seq(2, 4)
    val isr = Seq(2)
    val liveReplicas = Set(4)
    val leaderOpt = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(assignment,
      isr,
      liveReplicas,
      uncleanLeaderElectionEnabled = true,
      controllerContext)
    assertEquals(Option(4), leaderOpt)
    assertEquals(1, controllerContext.stats.uncleanLeaderElectionRate.count())
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
