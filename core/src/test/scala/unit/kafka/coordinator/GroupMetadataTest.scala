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
class GroupMetadataTest extends JUnitSuite {
  var group: GroupMetadata = null

  @Before
  def setUp() {
    group = new GroupMetadata("groupId", "consumer")
  }

  @Test
  def testCanRebalanceWhenStable() {
    assertTrue(group.canRebalance)
  }

  @Test
  def testCanRebalanceWhenAwaitingSync(){
    group.transitionTo(PreparingRebalance)
    group.transitionTo(AwaitingSync)
    assertTrue(group.canRebalance)
  }

  @Test
  def testCannotRebalanceWhenPreparingRebalance() {
    group.transitionTo(PreparingRebalance)
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
  def testAwaitingSyncToPreparingRebalanceTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(AwaitingSync)
    group.transitionTo(PreparingRebalance)
    assertState(group, PreparingRebalance)
  }

  @Test
  def testPreparingRebalanceToDeadTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    assertState(group, Dead)
  }

  @Test
  def testAwaitingSyncToStableTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(AwaitingSync)
    group.transitionTo(Stable)
    assertState(group, Stable)
  }

  @Test(expected = classOf[IllegalStateException])
  def testStableToStableIllegalTransition() {
    group.transitionTo(Stable)
  }

  @Test(expected = classOf[IllegalStateException])
  def testStableToAwaitingSyncIllegalTransition() {
    group.transitionTo(AwaitingSync)
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
  def testAwaitingSyncToAwaitingSyncIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(AwaitingSync)
    group.transitionTo(AwaitingSync)
  }

  @Test(expected = classOf[IllegalStateException])
  def testAwaitingSyncToDeadIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(AwaitingSync)
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
  def testDeadToAwaitingSyncIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    group.transitionTo(AwaitingSync)
  }

  private def assertState(group: GroupMetadata, targetState: GroupState) {
    val states: Set[GroupState] = Set(Stable, PreparingRebalance, AwaitingSync, Dead)
    val otherStates = states - targetState
    otherStates.foreach { otherState =>
      assertFalse(group.is(otherState))
    }
    assertTrue(group.is(targetState))
  }
}
