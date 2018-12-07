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
      ReassignmentStep(List(0, 1),List(),Some(2),List(0, 1, 2)),
      ReassignmentStep(List(0, 1, 2),List(1),Some(3),List(0, 2, 3)),
      ReassignmentStep(List(0, 2, 3),List(0),None,List(2, 3))
    ),
      testSteps(Seq(0, 1), 0, Seq(2, 3))
    )
  }

  @Test
  def testCalculateReassignmentReplaceOneReplica(): Unit = {
    testSteps(Seq(0, 1), 0, Seq(0,2))
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

  def calculateSteps(startingReplicas: Seq[Int], startingLeader: Int, targetReplicas:Seq[Int]) = {
    var currentReplicas = startingReplicas
    var steps = Seq.empty[ReassignmentStep]

    // we can remove any number of replicas, but we can add one in each step
    // TODO: when we need +1 ?
    val numberOfSteps = (targetReplicas.toSet -- startingReplicas.toSet).size + 1

    (0 until numberOfSteps).foreach { _ =>
      val step = ReassignmentHelper.calculateReassignmentStep(targetReplicas, currentReplicas, startingLeader)
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

  private def testSteps(startingReplicas: Seq[Int], startingLeader: Int, targetReplicas:Seq[Int]) = {
    val steps = calculateSteps(startingReplicas, startingLeader, targetReplicas)
    checkInvariants(startingReplicas, startingLeader, targetReplicas, steps)
    steps
  }
}
