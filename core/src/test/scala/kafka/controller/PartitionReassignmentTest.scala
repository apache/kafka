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

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.scalatest.junit.JUnitSuite


// TODO: Test cases:
// [0, 1, 2] -> [1, 0, 2]
// [0, 1, 2] -> [3, 4, 5]
// [0, 1, 2] -> [0, 1, 3]
// [0, 1, 2] -> [3, 1, 2]
// [0, 1, 2] -> [0, 1]
// [0, 1, 2] -> [0]
// [0, 1, 2] -> [3]
// [0, 1, 2] -> [1, 2] ?
// [0, 1, 2] -> [3, 4]
// [0, 1] -> [0, 1, 2]
// [0, 1] -> [3, 4, 5]
// [0, 1, 2] -> []
//
// controller failover in the middle
// old broker fails / gets out of ISR
// old broker not in ISR from beginning
// more topics, partitions
// write test for "reassignment with offline brokers with DR"
class PartitionReassignmentTest  extends JUnitSuite {

  @Test
  def testCalculateReassignmentStepReplaceAllNodes(): Unit = {
    assertEquals(Seq(
      ReassignmentStep(List(0, 1),List(),List(2),List(0, 1, 2)),
      ReassignmentStep(List(0, 1, 2),List(1),List(3),List(0, 2, 3)),
      ReassignmentStep(List(0, 2, 3),List(0),Seq.empty,List(2, 3))
    ),
      testSteps(Seq(0, 1), 0, Seq(2, 3))
    )
  }

  @Test
  def testCalculateReassignmentReplaceOneReplica(): Unit = {
    testSteps(Seq(0, 1), 0, Seq(0, 2))
  }

  @Test
  def testCalculateReassignmentWithOfflineBrokersAndNonDefaultIsr(): Unit = {
    implicit val numberOfSteps: Int = 2
    assertEquals(Seq(
      ReassignmentStep(List(0, 1), List(), List(2, 3, 4), List(0, 1, 2, 3, 4)),
      ReassignmentStep(List(0, 1, 2, 3, 4), List(1), List(), List(0, 2, 3, 4))
    ), testSteps(Seq(0, 1), 0, Seq(0, 2, 3 ,4), Set(0, 2, 3, 4), 4))
  }

  @Test
  def testDecrementAssignmentWithOfflineBrokersAndNonDefaultIsr(): Unit = {
    implicit val numberOfSteps: Int = 1
    assertEquals(Seq(
      ReassignmentStep(List(0, 1, 2, 3, 4), List(1, 4, 0), List(), List(2, 3))
    ), testSteps(Seq(0, 1, 2, 3, 4), 0, Seq(2, 3), Set(0, 2, 3, 4), 2))
  }

  @Test
  def tesReassignmentWithOfflineBrokersWontFinish(): Unit = {
    assertEquals(Seq(
      ReassignmentStep(List(0, 1, 2, 3, 4), List(), List(6), List(0, 1, 2, 3, 4, 6)),
      ReassignmentStep(List(0, 1, 2, 3, 4, 6), List(1), List(7), List(0, 2, 3, 4, 6, 7)),
      ReassignmentStep(List(0, 2, 3, 4, 6, 7), List(2), List(), List(0, 3, 4, 6, 7)),
      ReassignmentStep(List(0, 3, 4, 6, 7), List(), List(), List(0, 3, 4, 6, 7)),
      ReassignmentStep(List(0, 3, 4, 6, 7), List(), List(), List(0, 3, 4, 6, 7)),
      ReassignmentStep(List(0, 3, 4, 6, 7), List(), List(), List(0, 3, 4, 6, 7))
    ), calculateSteps(Seq(0, 1, 2, 3, 4), 0, Seq(5, 6, 7, 8, 9), Set(0, 2, 3, 4, 6, 7)))
  }

  @Test
  def testCalculateReassignmentX(): Unit = {
    testSteps(Seq(0, 1, 2), 0, Seq(3, 1, 2))
  }

  @Test
  def testFullCalculateReassignment(): Unit = {
    testSteps(Seq(0, 1, 2), 0, Seq(3, 4, 5))
  }

  @Test
  def testCalculateReassignmentWithMultiDrop(): Unit = {
    testSteps(Seq(0, 1, 2, 3, 4), 0, Seq(1, 2))
  }

  @Test
  def testCalculateReassignmentWithBiggerTargetSet(): Unit = {
    testSteps(Seq(0, 1), 0, Seq(1, 2, 3, 4, 5))
  }

  @Test
  def testCalculateReassignmentNoOP(): Unit = {
    testSteps(Seq(0, 1), 0, Seq(0, 1))
  }

  def calculateSteps(startingReplicas: Seq[Int], startingLeader: Int, targetReplicas:Seq[Int], liveBrokerIds: Set[Int], minIsrSize: Int = 1)(implicit numberOfSteps: Int = -1) = {
    var currentReplicas = startingReplicas
    var steps = Seq.empty[ReassignmentStep]

    // we can remove any number of replicas, but we can add one in each step
    val numOfSteps = if (numberOfSteps == -1) (targetReplicas.toSet -- startingReplicas.toSet).size + 1 else numberOfSteps

    (0 until numOfSteps).foreach { _ =>
      val step = ReassignmentHelper.calculateReassignmentStep(targetReplicas, currentReplicas, startingLeader, liveBrokerIds, minIsrSize)
      println(step)
      steps = steps :+ step
      currentReplicas = step.targetReplicas
    }
    steps
  }

  def firstStepStartsWithStartingReplicas(startingReplicas: Seq[Int], steps: Seq[ReassignmentStep]) =
    assertEquals(startingReplicas, steps.head.currentReplicas)

  def stepsAreChainedProperly(startingReplicas: Seq[Int], steps: Seq[ReassignmentStep], targetReplicas: Seq[Int]) = {
    assertEquals(steps.map(_.targetReplicas).init, steps.map(_.currentReplicas).tail)
  }

  def lastStepEndsWithTargetReplicas(steps: Seq[ReassignmentStep], targetReplicas: Seq[Int]) =
    assertEquals(targetReplicas, steps.last.targetReplicas)

  def addPlusDropResultsInTarget(startingReplicas: Seq[Int], steps: Seq[ReassignmentStep]) =
    steps.foreach { step =>
      assertEquals(step.currentReplicas.toSet ++ step.add.toSet -- step.drop, step.targetReplicas.toSet)
    }

  def leaderIsKeptUntilLastStep(startingLeader: Int, steps: Seq[ReassignmentStep]) =
    steps.init.foreach { step =>
      assertTrue(step.targetReplicas.contains(startingLeader))
    }
  private def checkInvariants(startingReplicas: Seq[Int], startingLeader: Int, targetReplicas:Seq[Int], steps: Seq[ReassignmentStep]) = {
    firstStepStartsWithStartingReplicas(startingReplicas, steps)
    stepsAreChainedProperly(startingReplicas: Seq[Int], steps, targetReplicas)
    lastStepEndsWithTargetReplicas(steps, targetReplicas)
    addPlusDropResultsInTarget(startingReplicas, steps)
    leaderIsKeptUntilLastStep(startingLeader, steps)
  }

  private def testSteps(startingReplicas: Seq[Int], startingLeader: Int, targetReplicas:Seq[Int], liveBrokerIds: Set[Int] = null, minIsrSize: Int = 1)(implicit numberOfSteps: Int = -1) = {
    val liveBrokers = if (liveBrokerIds == null) Set(targetReplicas ++ startingReplicas).flatten else liveBrokerIds
    val steps = calculateSteps(startingReplicas, startingLeader, targetReplicas, liveBrokers, minIsrSize)
    checkInvariants(startingReplicas, startingLeader, targetReplicas, steps)
    steps
  }
}
