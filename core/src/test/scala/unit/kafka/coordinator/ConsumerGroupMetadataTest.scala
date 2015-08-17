/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.coordinator

import org.junit.Assert._
import org.junit.{Before, Test}
import org.scalatest.junit.JUnitSuite

/**
 * Test group state transitions
 */
class ConsumerGroupMetadataTest extends JUnitSuite {
  var group: ConsumerGroupMetadata = null

  @Before
  def setUp() {
    group = new ConsumerGroupMetadata("test", "range")
  }

  @Test
  def testCanRebalanceWhenStable() {
    assertTrue(group.canRebalance)
  }

  @Test
  def testCannotRebalanceWhenPreparingRebalance() {
    group.transitionTo(PreparingRebalance)
    assertFalse(group.canRebalance)
  }

  @Test
  def testCannotRebalanceWhenRebalancing() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Rebalancing)
    assertFalse(group.canRebalance)
  }

  @Test
  def testCannotRebalanceWhenDead() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    assertFalse(group.canRebalance)
  }

  @Test
  def testStableToPreparingRebalanceTransition() {
    group.transitionTo(PreparingRebalance)
    assertState(group, PreparingRebalance)
  }

  @Test
  def testPreparingRebalanceToRebalancingTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Rebalancing)
    assertState(group, Rebalancing)
  }

  @Test
  def testPreparingRebalanceToDeadTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    assertState(group, Dead)
  }

  @Test
  def testRebalancingToStableTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Rebalancing)
    group.transitionTo(Stable)
    assertState(group, Stable)
  }

  @Test(expected = classOf[IllegalStateException])
  def testStableToStableIllegalTransition() {
    group.transitionTo(Stable)
  }

  @Test(expected = classOf[IllegalStateException])
  def testStableToRebalancingIllegalTransition() {
    group.transitionTo(Rebalancing)
  }

  @Test(expected = classOf[IllegalStateException])
  def testStableToDeadIllegalTransition() {
    group.transitionTo(Dead)
  }

  @Test(expected = classOf[IllegalStateException])
  def testPreparingRebalanceToPreparingRebalanceIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(PreparingRebalance)
  }

  @Test(expected = classOf[IllegalStateException])
  def testPreparingRebalanceToStableIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Stable)
  }

  @Test(expected = classOf[IllegalStateException])
  def testRebalancingToRebalancingIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Rebalancing)
    group.transitionTo(Rebalancing)
  }

  @Test(expected = classOf[IllegalStateException])
  def testRebalancingToPreparingRebalanceTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Rebalancing)
    group.transitionTo(PreparingRebalance)
  }

  @Test(expected = classOf[IllegalStateException])
  def testRebalancingToDeadIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Rebalancing)
    group.transitionTo(Dead)
  }

  @Test(expected = classOf[IllegalStateException])
  def testDeadToDeadIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    group.transitionTo(Dead)
  }

  @Test(expected = classOf[IllegalStateException])
  def testDeadToStableIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    group.transitionTo(Stable)
  }

  @Test(expected = classOf[IllegalStateException])
  def testDeadToPreparingRebalanceIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    group.transitionTo(PreparingRebalance)
  }

  @Test(expected = classOf[IllegalStateException])
  def testDeadToRebalancingIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    group.transitionTo(Rebalancing)
  }

  private def assertState(group: ConsumerGroupMetadata, targetState: GroupState) {
    val states: Set[GroupState] = Set(Stable, PreparingRebalance, Rebalancing, Dead)
    val otherStates = states - targetState
    otherStates.foreach { otherState =>
      assertFalse(group.is(otherState))
    }
    assertTrue(group.is(targetState))
  }
}
