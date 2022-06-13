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
import org.apache.kafka.metadata.LeaderRecoveryState
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, fail}
import org.junit.jupiter.api.Test
import org.mockito.Mockito

import scala.collection.mutable

class ElectionTest {
  private val topicPartition = new TopicPartition("foo", 15)

  @Test
  def testControlledShutdownIfAnotherLiveReplicaIsInIsr(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3))
    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 1,
      leaderEpoch = 5,
      isr = List(1, 2),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = controlledShutdown(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(2, 3),
      shuttingDownBrokerIds = Seq(1)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = 2,
      leaderEpoch = 6,
      isr = List(2),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )
    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(2, 3),
      replicasToStop = Seq(1)
    ), result)
  }

  @Test
  def testControlledShutdownIfShuttingDownBrokerIsOnlyMemberOfIsr(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3))
    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 1,
      leaderEpoch = 5,
      isr = List(1),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertFailedUpdate(controlledShutdown(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(2, 3),
      shuttingDownBrokerIds = Seq(1)
    ))
  }

  @Test
  def testControlledShutdownIfNoActiveLeader(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3))
    val initialLeaderAndIsr = LeaderAndIsr(
      leader = LeaderAndIsr.NoLeader,
      leaderEpoch = 5,
      isr = List(2),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = controlledShutdown(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(3),
      shuttingDownBrokerIds = Seq(1)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = LeaderAndIsr.NoLeader,
      leaderEpoch = 6,
      isr = List(2),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )
    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(3),
      replicasToStop = Seq(1)
    ), result)
  }

  @Test
  def testControlledShutdownIfShuttingDownBrokerNotInIsr(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3))
    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 2,
      leaderEpoch = 5,
      isr = List(2, 3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = controlledShutdown(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(2, 3),
      shuttingDownBrokerIds = Seq(1)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = 2,
      leaderEpoch = 6,
      isr = List(2, 3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )
    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(2, 3),
      replicasToStop = Seq(1)
    ), result)
  }

  @Test
  def testControlledShutdownMultipleBrokersInIsr(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3))
    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 2,
      leaderEpoch = 5,
      isr = List(1, 2, 3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = controlledShutdown(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(3),
      shuttingDownBrokerIds = Seq(1, 2)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = 3,
      leaderEpoch = 6,
      isr = List(3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )
    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(3),
      replicasToStop = Seq(1, 2)
    ), result)
  }

  @Test
  def testControlledShutdownMultipleBrokersOnlyMembersInIsr(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3))
    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 2,
      leaderEpoch = 5,
      isr = List(1, 2),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = controlledShutdown(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(3),
      shuttingDownBrokerIds = Seq(1, 2)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = 2,
      leaderEpoch = 6,
      isr = List(2),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )
    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(3, 2),
      replicasToStop = Seq(1)
    ), result)
  }

  @Test
  def testControlledShutdownMultipleBrokersAndNoCurrentLeader(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3))
    val initialLeaderAndIsr = LeaderAndIsr(
      leader = LeaderAndIsr.NoLeader,
      leaderEpoch = 5,
      isr = List(3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = controlledShutdown(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(),
      shuttingDownBrokerIds = Seq(1, 2)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = LeaderAndIsr.NoLeader,
      leaderEpoch = 6,
      isr = List(3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )
    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(),
      replicasToStop = Seq(1, 2)
    ), result)
  }

  @Test
  def testControlledShutdownMultipleBrokersNotInIsr(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3))
    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 3,
      leaderEpoch = 5,
      isr = List(3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = controlledShutdown(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(3),
      shuttingDownBrokerIds = Seq(1, 2)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = 3,
      leaderEpoch = 6,
      isr = List(3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )
    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(3),
      replicasToStop = Seq(1, 2)
    ), result)
  }

  @Test
  def testControlledShutdownMultipleBrokersOneInIsrAndOneNot(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3))
    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 3,
      leaderEpoch = 5,
      isr = List(2, 3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = controlledShutdown(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(3),
      shuttingDownBrokerIds = Seq(1, 2)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = 3,
      leaderEpoch = 6,
      isr = List(3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )
    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(3),
      replicasToStop = Seq(1, 2)
    ), result)
  }

  @Test
  def testControlledShutdownSingleReplica(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3))
    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 1,
      leaderEpoch = 5,
      isr = List(1),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertFailedUpdate(controlledShutdown(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(),
      shuttingDownBrokerIds = Seq(1)
    ))
  }

  @Test
  def testCancelReassignmentNoAddingReplicasInIsr(): Unit = {
    val assignment = ReplicaAssignment(
      replicas = Seq(1, 2, 3, 4, 5),
      addingReplicas = Seq(4, 5),
      removingReplicas = Seq(2, 3)
    )

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 1,
      leaderEpoch = 5,
      isr = List(1, 2, 3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = cancelReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(1, 2, 3, 4, 5)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = 1,
      leaderEpoch = 6,
      isr = List(1, 2, 3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(1, 2, 3),
      replicasToDelete = Seq(4, 5)
    ), result)
  }

  @Test
  def testCancelReassignmentWithAddingReplicasInIsr(): Unit = {
    val assignment = ReplicaAssignment(
      replicas = Seq(1, 2, 3, 4, 5),
      addingReplicas = Seq(4, 5),
      removingReplicas = Seq(2, 3)
    )

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 1,
      leaderEpoch = 5,
      isr = List(1, 2, 3, 5),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = cancelReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(1, 2, 3, 4, 5)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = 1,
      leaderEpoch = 6,
      isr = List(1, 2, 3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(1, 2, 3),
      replicasToDelete = Seq(4, 5)
    ), result)
  }

  @Test
  def testCancelReassignmentWithLeaderInTargetReplicas(): Unit = {
    val assignment = ReplicaAssignment(
      replicas = Seq(1, 2, 3, 4, 5),
      addingReplicas = Seq(4, 5),
      removingReplicas = Seq(2, 3)
    )

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 4,
      leaderEpoch = 5,
      isr = List(1, 2, 3, 4),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = cancelReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(1, 2, 3, 4, 5)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = 1,
      leaderEpoch = 6,
      isr = List(1, 2, 3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(1, 2, 3),
      replicasToDelete = Seq(4, 5)
    ), result)
  }

  @Test
  def testCancelReassignmentWithPreferredLeaderOffline(): Unit = {
    val assignment = ReplicaAssignment(
      replicas = Seq(1, 2, 3, 4, 5),
      addingReplicas = Seq(4, 5),
      removingReplicas = Seq(2, 3)
    )

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 4,
      leaderEpoch = 5,
      isr = List(3, 4),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = cancelReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(3, 4, 5)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = 3,
      leaderEpoch = 6,
      isr = List(3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(3),
      replicasToDelete = Seq(4, 5)
    ), result)
  }

  @Test
  def testCancelReassignmentWithNoOriginalReplicasInIsr(): Unit = {
    val assignment = ReplicaAssignment(
      replicas = Seq(1, 2, 3, 4, 5),
      addingReplicas = Seq(4, 5),
      removingReplicas = Seq(2, 3)
    )

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 4,
      leaderEpoch = 5,
      isr = List(4),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertFailedUpdate(cancelReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(3, 4, 5)
    ))
  }

  @Test
  def testCancelReassignmentWithNoLiveOriginalReplicasInIsr(): Unit = {
    val assignment = ReplicaAssignment(
      replicas = Seq(1, 2),
      addingReplicas = Seq(2),
      removingReplicas = Seq(1)
    )

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = LeaderAndIsr.NoLeader,
      leaderEpoch = 5,
      isr = List(1),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertFailedUpdate(cancelReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(2)
    ))
  }

  @Test
  def testCancelReassignmentWithNoReassignmentInProgress(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3))

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 1,
      leaderEpoch = 5,
      isr = List(1, 2, 3),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertThrows(classOf[IllegalStateException], () => cancelReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(3, 4, 5)
    ))
  }

  @Test
  def testCancelReassignmentWithNoAddingReplicas(): Unit = {
    val assignment = ReplicaAssignment(
      replicas = Seq(1, 2, 3, 4, 5),
      addingReplicas = Seq(),
      removingReplicas = Seq(4, 5)
    )

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 4,
      leaderEpoch = 5,
      isr = List(3, 4),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = cancelReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(1, 2, 3, 4, 5)
    )

    assertEquals(LeaderAndIsrUpdateResult.NotNeeded(
      topicPartition,
      initialLeaderAndIsr
    ), result)
  }

  @Test
  def testCompleteReassignmentCurrentLeaderNotInTargetReplicas(): Unit = {
    val assignment = ReplicaAssignment(
      replicas = Seq(1, 2, 3, 4, 5),
      addingReplicas = Seq(4, 5),
      removingReplicas = Seq(2, 3)
    )

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 2,
      leaderEpoch = 5,
      isr = List(1, 2, 3, 4, 5),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = completeReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(1, 2, 3, 4, 5)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = 1,
      leaderEpoch = 6,
      isr = List(1, 4, 5),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(1, 4, 5),
      replicasToDelete = Seq(2, 3)
    ), result)
  }

  @Test
  def testCompleteReassignmentCurrentLeaderInTargetReplicas(): Unit = {
    val assignment = ReplicaAssignment(
      replicas = Seq(1, 2, 3, 4, 5),
      addingReplicas = Seq(4, 5),
      removingReplicas = Seq(2, 3)
    )

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 4,
      leaderEpoch = 5,
      isr = List(1, 2, 3, 4, 5),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = completeReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(1, 2, 3, 4, 5)
    )

    val expectedLeaderAndIsr = LeaderAndIsr(
      leader = 4,
      leaderEpoch = 6,
      isr = List(1, 4, 5),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertEquals(LeaderAndIsrUpdateResult.Successful(
      topicPartition,
      expectedLeaderAndIsr,
      liveReplicas = Seq(1, 4, 5),
      replicasToDelete = Seq(2, 3)
    ), result)
  }

  @Test
  def testCompleteReassignmentWithNoReplicasToRemove(): Unit = {
    val assignment = ReplicaAssignment(
      replicas = Seq(1, 2, 3, 4, 5),
      addingReplicas = Seq(4, 5),
      removingReplicas = Seq()
    )

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = 1,
      leaderEpoch = 5,
      isr = List(1, 2, 3, 4, 5),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    val result = completeReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(1, 2, 3, 4, 5)
    )

    assertEquals(LeaderAndIsrUpdateResult.NotNeeded(
      topicPartition,
      initialLeaderAndIsr
    ), result)
  }

  @Test
  def testCompleteReassignmentWithSomeTargetReplicasOutOfIsr(): Unit = {
    val assignment = ReplicaAssignment(
      replicas = Seq(1, 2, 3, 4, 5),
      addingReplicas = Seq(4, 5),
      removingReplicas = Seq(2, 3)
    )

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = LeaderAndIsr.NoLeader,
      leaderEpoch = 1,
      isr = List(1, 2, 3, 4),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertFailedUpdate(completeReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(1, 2, 3, 4, 5)
    ))
  }

  @Test
  def testCompleteReassignmentWithNoLiveTargetReplicasInIsr(): Unit = {
    val assignment = ReplicaAssignment(
      replicas = Seq(1, 2),
      addingReplicas = Seq(2),
      removingReplicas = Seq(1)
    )

    val initialLeaderAndIsr = LeaderAndIsr(
      leader = LeaderAndIsr.NoLeader,
      leaderEpoch = 5,
      isr = List(2),
      LeaderRecoveryState.RECOVERED,
      partitionEpoch = 79
    )

    assertFailedUpdate(completeReassignment(
      initialLeaderAndIsr,
      assignment,
      liveBrokerIds = Seq(1)
    ))
  }

  private def assertFailedUpdate(
    result: LeaderAndIsrUpdateResult
  ): Unit = {
    result match {
      case LeaderAndIsrUpdateResult.Failed(topicPartition, _) =>
        assertEquals(this.topicPartition, topicPartition)
      case _ =>
        fail(s"Unexpected result: $result")
    }
  }

  private def cancelReassignment(
    initialLeaderAndIsr: LeaderAndIsr,
    assignment: ReplicaAssignment,
    liveBrokerIds: Seq[Int],
    shuttingDownBrokerIds: Seq[Int] = Seq()
  ): LeaderAndIsrUpdateResult = {
    val controllerContext = mockedControllerContext(
      topicPartition,
      assignment,
      liveBrokerIds,
      shuttingDownBrokerIds
    )

    val results = Election.processReassignmentCancellation(
      controllerContext = controllerContext,
      Seq(topicPartition -> initialLeaderAndIsr)
    )
    assertEquals(1, results.size)
    assertEquals(topicPartition, results.head.topicPartition)
    results.head
  }

  private def completeReassignment(
    initialLeaderAndIsr: LeaderAndIsr,
    assignment: ReplicaAssignment,
    liveBrokerIds: Seq[Int],
    shuttingDownBrokerIds: Seq[Int] = Seq()
  ): LeaderAndIsrUpdateResult = {
    val controllerContext = mockedControllerContext(
      topicPartition,
      assignment,
      liveBrokerIds,
      shuttingDownBrokerIds
    )

    val results = Election.processReassignmentCompletion(
      controllerContext = controllerContext,
      Seq(topicPartition -> initialLeaderAndIsr)
    )
    assertEquals(1, results.size)
    assertEquals(topicPartition, results.head.topicPartition)
    results.head
  }
  private def controlledShutdown(
    initialLeaderAndIsr: LeaderAndIsr,
    assignment: ReplicaAssignment,
    liveBrokerIds: Seq[Int],
    shuttingDownBrokerIds: Seq[Int]

  ): LeaderAndIsrUpdateResult = {
    val controllerContext = mockedControllerContext(
      topicPartition,
      assignment,
      liveBrokerIds,
      shuttingDownBrokerIds
    )

    val results = Election.processControlledShutdown(
      controllerContext = controllerContext,
      Seq(topicPartition -> initialLeaderAndIsr)
    )
    assertEquals(1, results.size)
    assertEquals(topicPartition, results.head.topicPartition)
    results.head
  }

  private def mockedControllerContext(
    topicPartition: TopicPartition,
    assignment: ReplicaAssignment,
    liveBrokerIds: Seq[Int],
    shuttingDownBrokerIds: Seq[Int]
  ): ControllerContext = {
    val controllerContext = Mockito.mock(classOf[ControllerContext])
    Mockito.when(controllerContext.partitionReplicaAssignment(topicPartition))
      .thenReturn(assignment.replicas)
    Mockito.when(controllerContext.partitionFullReplicaAssignment(topicPartition))
      .thenReturn(assignment)

    Mockito.when(controllerContext.shuttingDownBrokerIds)
      .thenReturn(mutable.Set(shuttingDownBrokerIds: _*))

    assignment.replicas.foreach { replicaId =>
      Mockito.when(controllerContext.isReplicaOnline(replicaId, topicPartition, includeShuttingDownBrokers = false))
        .thenReturn(liveBrokerIds.contains(replicaId))
    }

    controllerContext
  }

}
