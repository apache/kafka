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
 * Test group state transitions and other GroupMetadata functionality
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

  @Test
  def testSelectProtocol() {
    val groupId = "groupId"
    val clientId = "clientId"
    val clientHost = "clientHost"
    val sessionTimeoutMs = 10000

    val memberId = "memberId"
    val member = new MemberMetadata(memberId, groupId, clientId, clientHost, sessionTimeoutMs,
      List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    group.add(memberId, member)
    assertEquals("range", group.selectProtocol)

    val otherMemberId = "otherMemberId"
    val otherMember = new MemberMetadata(otherMemberId, groupId, clientId, clientHost, sessionTimeoutMs,
      List(("roundrobin", Array.empty[Byte]), ("range", Array.empty[Byte])))

    group.add(otherMemberId, otherMember)
    // now could be either range or robin since there is no majority preference
    assertTrue(Set("range", "roundrobin")(group.selectProtocol))

    val lastMemberId = "lastMemberId"
    val lastMember = new MemberMetadata(lastMemberId, groupId, clientId, clientHost, sessionTimeoutMs,
      List(("roundrobin", Array.empty[Byte]), ("range", Array.empty[Byte])))

    group.add(lastMemberId, lastMember)
    // now we should prefer 'roundrobin'
    assertEquals("roundrobin", group.selectProtocol)
  }

  @Test(expected = classOf[IllegalStateException])
  def testSelectProtocolRaisesIfNoMembers() {
    group.selectProtocol
    fail()
  }

  @Test
  def testSelectProtocolChoosesCompatibleProtocol() {
    val groupId = "groupId"
    val clientId = "clientId"
    val clientHost = "clientHost"
    val sessionTimeoutMs = 10000

    val memberId = "memberId"
    val member = new MemberMetadata(memberId, groupId, clientId, clientHost, sessionTimeoutMs,
      List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    val otherMemberId = "otherMemberId"
    val otherMember = new MemberMetadata(otherMemberId, groupId, clientId, clientHost, sessionTimeoutMs,
      List(("roundrobin", Array.empty[Byte]), ("blah", Array.empty[Byte])))

    group.add(memberId, member)
    group.add(otherMemberId, otherMember)
    assertEquals("roundrobin", group.selectProtocol)
  }

  @Test
  def testSupportsProtocols() {
    val groupId = "groupId"
    val clientId = "clientId"
    val clientHost = "clientHost"
    val sessionTimeoutMs = 10000

    // by default, the group supports everything
    assertTrue(group.supportsProtocols(Set("roundrobin", "range")))

    val memberId = "memberId"
    val member = new MemberMetadata(memberId, groupId, clientId, clientHost, sessionTimeoutMs,
      List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    group.add(memberId, member)
    assertTrue(group.supportsProtocols(Set("roundrobin", "foo")))
    assertTrue(group.supportsProtocols(Set("range", "foo")))
    assertFalse(group.supportsProtocols(Set("foo", "bar")))

    val otherMemberId = "otherMemberId"
    val otherMember = new MemberMetadata(otherMemberId, groupId, clientId, clientHost, sessionTimeoutMs,
      List(("roundrobin", Array.empty[Byte]), ("blah", Array.empty[Byte])))

    group.add(otherMemberId, otherMember)

    assertTrue(group.supportsProtocols(Set("roundrobin", "foo")))
    assertFalse(group.supportsProtocols(Set("range", "foo")))
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
