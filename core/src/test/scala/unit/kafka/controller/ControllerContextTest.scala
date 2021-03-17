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

package unit.kafka.controller

import kafka.api.LeaderAndIsr
import kafka.cluster.{Broker, EndPoint}
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.controller.{ControllerContext, ReplicaAssignment}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertFalse


class ControllerContextTest {

  var context: ControllerContext = null
  val brokers: Seq[Int] = Seq(1, 2, 3)
  val tp1 = new TopicPartition("A", 0)
  val tp2 = new TopicPartition("A", 1)
  val tp3 = new TopicPartition("B", 0)

  @BeforeEach
  def setUp(): Unit = {
    context = new ControllerContext

    val brokerEpochs = Seq(1,2,3).map { brokerId =>
      val endpoint = new EndPoint("localhost", 9900 + brokerId, new ListenerName("PLAINTEXT"),
        SecurityProtocol.PLAINTEXT)
      Broker(brokerId, Seq(endpoint), rack = None) -> 1L
    }.toMap

    context.setLiveBrokers(brokerEpochs)

    // Simple round-robin replica assignment
    var leaderIndex = 0
    Seq(tp1, tp2, tp3).foreach { partition =>
      val replicas = brokers.indices.map { i =>
        brokers((i + leaderIndex) % brokers.size)
      }
      context.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(replicas))
      leaderIndex += 1
    }
  }

  @Test
  def testUpdatePartitionFullReplicaAssignmentUpdatesReplicaAssignment(): Unit = {
    val initialReplicas = Seq(4)
    context.updatePartitionFullReplicaAssignment(tp1, ReplicaAssignment(initialReplicas))
    val fullAssignment = context.partitionFullReplicaAssignment(tp1)
    assertEquals(initialReplicas, fullAssignment.replicas)
    assertEquals(Seq(), fullAssignment.addingReplicas)
    assertEquals(Seq(), fullAssignment.removingReplicas)

    val expectedFullAssignment = ReplicaAssignment(Seq(3), Seq(1), Seq(2))
    context.updatePartitionFullReplicaAssignment(tp1, expectedFullAssignment)
    val updatedFullAssignment = context.partitionFullReplicaAssignment(tp1)
    assertEquals(expectedFullAssignment.replicas, updatedFullAssignment.replicas)
    assertEquals(expectedFullAssignment.addingReplicas, updatedFullAssignment.addingReplicas)
    assertEquals(expectedFullAssignment.removingReplicas, updatedFullAssignment.removingReplicas)
  }

  @Test
  def testPartitionReplicaAssignmentReturnsEmptySeqIfTopicOrPartitionDoesNotExist(): Unit = {
    val noTopicReplicas = context.partitionReplicaAssignment(new TopicPartition("NONEXISTENT", 0))
    assertEquals(Seq.empty, noTopicReplicas)
    val noPartitionReplicas = context.partitionReplicaAssignment(new TopicPartition("A", 100))
    assertEquals(Seq.empty, noPartitionReplicas)
  }

  @Test
  def testPartitionFullReplicaAssignmentReturnsEmptyAssignmentIfTopicOrPartitionDoesNotExist(): Unit = {
    val expectedEmptyAssignment = ReplicaAssignment(Seq.empty, Seq.empty, Seq.empty)

    val noTopicAssignment = context.partitionFullReplicaAssignment(new TopicPartition("NONEXISTENT", 0))
    assertEquals(expectedEmptyAssignment, noTopicAssignment)
    val noPartitionAssignment = context.partitionFullReplicaAssignment(new TopicPartition("A", 100))
    assertEquals(expectedEmptyAssignment, noPartitionAssignment)
  }

  @Test
  def testPartitionReplicaAssignmentForTopicReturnsEmptyMapIfTopicDoesNotExist(): Unit = {
    assertEquals(Map.empty, context.partitionReplicaAssignmentForTopic("NONEXISTENT"))
  }

  @Test
  def testPartitionReplicaAssignmentForTopicReturnsExpectedReplicaAssignments(): Unit = {
    val expectedAssignments = Map(
      tp1 -> context.partitionReplicaAssignment(tp1),
      tp2 -> context.partitionReplicaAssignment(tp2)
    )
    val receivedAssignments = context.partitionReplicaAssignmentForTopic("A")
    assertEquals(expectedAssignments, receivedAssignments)
  }

  @Test
  def testPartitionReplicaAssignment(): Unit = {
    val reassigningPartition = ReplicaAssignment(List(1, 2, 3, 4, 5, 6), List(2, 3, 4), List(1, 5, 6))
    assertTrue(reassigningPartition.isBeingReassigned)
    assertEquals(List(2, 3, 4), reassigningPartition.targetReplicas)

    val reassigningPartition2 = ReplicaAssignment(List(1, 2, 3, 4), List(), List(1, 4))
    assertTrue(reassigningPartition2.isBeingReassigned)
    assertEquals(List(2, 3), reassigningPartition2.targetReplicas)

    val reassigningPartition3 = ReplicaAssignment(List(1, 2, 3, 4), List(4), List(2))
    assertTrue(reassigningPartition3.isBeingReassigned)
    assertEquals(List(1, 3, 4), reassigningPartition3.targetReplicas)

    val partition = ReplicaAssignment(List(1, 2, 3, 4, 5, 6), List(), List())
    assertFalse(partition.isBeingReassigned)
    assertEquals(List(1, 2, 3, 4, 5, 6), partition.targetReplicas)

    val reassigningPartition4 = ReplicaAssignment(Seq(1, 2, 3, 4)).reassignTo(Seq(4, 2, 5, 3))
    assertEquals(List(4, 2, 5, 3, 1), reassigningPartition4.replicas)
    assertEquals(List(4, 2, 5, 3), reassigningPartition4.targetReplicas)
    assertEquals(List(5), reassigningPartition4.addingReplicas)
    assertEquals(List(1), reassigningPartition4.removingReplicas)
    assertTrue(reassigningPartition4.isBeingReassigned)

    val reassigningPartition5 = ReplicaAssignment(Seq(1, 2, 3)).reassignTo(Seq(4, 5, 6))
    assertEquals(List(4, 5, 6, 1, 2, 3), reassigningPartition5.replicas)
    assertEquals(List(4, 5, 6), reassigningPartition5.targetReplicas)
    assertEquals(List(4, 5, 6), reassigningPartition5.addingReplicas)
    assertEquals(List(1, 2, 3), reassigningPartition5.removingReplicas)
    assertTrue(reassigningPartition5.isBeingReassigned)

    val nonReassigningPartition = ReplicaAssignment(Seq(1, 2, 3)).reassignTo(Seq(3, 1, 2))
    assertEquals(List(3, 1, 2), nonReassigningPartition.replicas)
    assertEquals(List(3, 1, 2), nonReassigningPartition.targetReplicas)
    assertEquals(List(), nonReassigningPartition.addingReplicas)
    assertEquals(List(), nonReassigningPartition.removingReplicas)
    assertFalse(nonReassigningPartition.isBeingReassigned)
  }

  @Test
  def testReassignToIdempotence(): Unit = {
    val assignment1 = ReplicaAssignment(Seq(1, 2, 3))
    assertEquals(assignment1, assignment1.reassignTo(assignment1.targetReplicas))

    val assignment2 = ReplicaAssignment(Seq(4, 5, 6, 1, 2, 3),
      addingReplicas = Seq(4, 5, 6), removingReplicas = Seq(1, 2, 3))
    assertEquals(assignment2, assignment2.reassignTo(assignment2.targetReplicas))

    val assignment3 = ReplicaAssignment(Seq(4, 2, 3, 1),
      addingReplicas = Seq(4), removingReplicas = Seq(1))
    assertEquals(assignment3, assignment3.reassignTo(assignment3.targetReplicas))
  }

  @Test
  def testReassignTo(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3))
    val firstReassign = assignment.reassignTo(Seq(4, 5, 6))

    assertEquals(ReplicaAssignment(Seq(4, 5, 6, 1, 2, 3), Seq(4, 5, 6), Seq(1, 2, 3)), firstReassign)
    assertEquals(ReplicaAssignment(Seq(7, 8, 9, 1, 2, 3), Seq(7, 8, 9), Seq(1, 2, 3)), firstReassign.reassignTo(Seq(7, 8, 9)))
    assertEquals(ReplicaAssignment(Seq(7, 8, 9, 1, 2, 3), Seq(7, 8, 9), Seq(1, 2, 3)), assignment.reassignTo(Seq(7, 8, 9)))
    assertEquals(assignment, firstReassign.reassignTo(Seq(1,2,3)))
  }

  @Test
  def testPreferredReplicaImbalanceMetric(): Unit = {
    context.updatePartitionFullReplicaAssignment(tp1, ReplicaAssignment(Seq(1, 2, 3)))
    context.updatePartitionFullReplicaAssignment(tp2, ReplicaAssignment(Seq(1, 2, 3)))
    context.updatePartitionFullReplicaAssignment(tp3, ReplicaAssignment(Seq(1, 2, 3)))
    assertEquals(0, context.preferredReplicaImbalanceCount)

    context.putPartitionLeadershipInfo(tp1, LeaderIsrAndControllerEpoch(LeaderAndIsr(1, List(1, 2, 3)), 0))
    assertEquals(0, context.preferredReplicaImbalanceCount)

    context.putPartitionLeadershipInfo(tp2, LeaderIsrAndControllerEpoch(LeaderAndIsr(2, List(2, 3, 1)), 0))
    assertEquals(1, context.preferredReplicaImbalanceCount)

    context.putPartitionLeadershipInfo(tp3, LeaderIsrAndControllerEpoch(LeaderAndIsr(3, List(3, 1, 2)), 0))
    assertEquals(2, context.preferredReplicaImbalanceCount)

    context.updatePartitionFullReplicaAssignment(tp1, ReplicaAssignment(Seq(2, 3, 1)))
    context.updatePartitionFullReplicaAssignment(tp2, ReplicaAssignment(Seq(2, 3, 1)))
    assertEquals(2, context.preferredReplicaImbalanceCount)

    context.queueTopicDeletion(Set(tp3.topic))
    assertEquals(1, context.preferredReplicaImbalanceCount)

    context.putPartitionLeadershipInfo(tp3, LeaderIsrAndControllerEpoch(LeaderAndIsr(1, List(3, 1, 2)), 0))
    assertEquals(1, context.preferredReplicaImbalanceCount)

    context.removeTopic(tp1.topic)
    context.removeTopic(tp2.topic)
    context.removeTopic(tp3.topic)
    assertEquals(0, context.preferredReplicaImbalanceCount)
  }
}
