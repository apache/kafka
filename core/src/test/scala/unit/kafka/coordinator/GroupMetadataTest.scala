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

import kafka.common.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{Assert, Before, Test}
import org.scalatest.junit.JUnitSuite

/**
 * Test group state transitions and other GroupMetadata functionality
 */
class GroupMetadataTest extends JUnitSuite {
  private val protocolType = "consumer"
  private val groupId = "groupId"
  private val clientId = "clientId"
  private val clientHost = "clientHost"
  private val rebalanceTimeoutMs = 60000
  private val sessionTimeoutMs = 10000

  private var group: GroupMetadata = null

  @Before
  def setUp() {
    group = new GroupMetadata("groupId")
  }

  @Test
  def testCanRebalanceWhenStable() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(AwaitingSync)
    group.transitionTo(Stable)
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
  def testCanRebalanceWhenInitialRebalance(): Unit = {
    assertTrue(group.canRebalance)
  }

  @Test
  def testCannotRebalanceWhenDead() {
    group.transitionTo(Dead)
    assertFalse(group.canRebalance)
  }

  @Test
  def testStableToPreparingRebalanceTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(AwaitingSync)
    group.transitionTo(Stable)
    group.transitionTo(PreparingRebalance)
    assertState(group, PreparingRebalance)
  }

  @Test
  def testStableToDeadTransition() {
    group.transitionTo(Dead)
    assertState(group, Dead)
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
  def testPreparingRebalanceToEmptyTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Empty)
    assertState(group, Empty)
  }

  @Test
  def testEmptyToDeadTransition() {
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
  def testEmptyToStableIllegalTransition() {
    group.transitionTo(Stable)
  }

  @Test
  def testStableToStableIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(AwaitingSync)
    group.transitionTo(Stable)
    validateIllegalTransition(Stable)
  }

  @Test(expected = classOf[IllegalStateException])
  def testEmptyToAwaitingSyncIllegalTransition() {
    group.transitionTo(AwaitingSync)
  }

  @Test
  def testStableToAwaitingSyncIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(AwaitingSync)
    group.transitionTo(Stable)
    validateIllegalTransition(AwaitingSync)
  }

  @Test
  def testPreparingRebalanceToPreparingRebalanceIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    validateIllegalTransition(PreparingRebalance)
  }

  @Test
  def testPreparingRebalanceToStableIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    validateIllegalTransition(Stable)
  }

  @Test
  def testAwaitingSyncToAwaitingSyncIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(AwaitingSync)
    validateIllegalTransition(AwaitingSync)
  }

  def testDeadToDeadIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    group.transitionTo(Dead)
    assertState(group, Dead)
  }

  @Test
  def testDeadToStableIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    validateIllegalTransition(Stable)
  }

  @Test
  def testDeadToPreparingRebalanceIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    validateIllegalTransition(PreparingRebalance)
  }

  private def validateIllegalTransition(state: GroupState) = {
    try {
      group.transitionTo(state)
      Assert.fail()
    } catch {
      case _: IllegalStateException => // success
    }
  }

  @Test
  def testDeadToAwaitingSyncIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    validateIllegalTransition(AwaitingSync)
  }

  @Test
  def testSelectProtocol() {
    val memberId = "memberId"
    val member = new MemberMetadata(memberId, groupId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    group.add(member)
    assertEquals("range", group.selectProtocol)

    val otherMemberId = "otherMemberId"
    val otherMember = new MemberMetadata(otherMemberId, groupId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("roundrobin", Array.empty[Byte]), ("range", Array.empty[Byte])))

    group.add(otherMember)
    // now could be either range or robin since there is no majority preference
    assertTrue(Set("range", "roundrobin")(group.selectProtocol))

    val lastMemberId = "lastMemberId"
    val lastMember = new MemberMetadata(lastMemberId, groupId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("roundrobin", Array.empty[Byte]), ("range", Array.empty[Byte])))

    group.add(lastMember)
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
    val memberId = "memberId"
    val member = new MemberMetadata(memberId, groupId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    val otherMemberId = "otherMemberId"
    val otherMember = new MemberMetadata(otherMemberId, groupId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("roundrobin", Array.empty[Byte]), ("blah", Array.empty[Byte])))

    group.add(member)
    group.add(otherMember)
    assertEquals("roundrobin", group.selectProtocol)
  }

  @Test
  def testSupportsProtocols() {
    // by default, the group supports everything
    assertTrue(group.supportsProtocols(Set("roundrobin", "range")))

    val memberId = "memberId"
    val member = new MemberMetadata(memberId, groupId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    group.add(member)
    assertTrue(group.supportsProtocols(Set("roundrobin", "foo")))
    assertTrue(group.supportsProtocols(Set("range", "foo")))
    assertFalse(group.supportsProtocols(Set("foo", "bar")))

    val otherMemberId = "otherMemberId"
    val otherMember = new MemberMetadata(otherMemberId, groupId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("roundrobin", Array.empty[Byte]), ("blah", Array.empty[Byte])))

    group.add(otherMember)

    assertTrue(group.supportsProtocols(Set("roundrobin", "foo")))
    assertFalse(group.supportsProtocols(Set("range", "foo")))
  }

  @Test
  def testInitNextGeneration() {
    val memberId = "memberId"
    val member = new MemberMetadata(memberId, groupId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("roundrobin", Array.empty[Byte])))

    group.transitionTo(PreparingRebalance)
    member.awaitingJoinCallback = _ => ()
    group.add(member)

    assertEquals(0, group.generationId)
    assertNull(group.protocol)

    group.initNextGeneration()

    assertEquals(1, group.generationId)
    assertEquals("roundrobin", group.protocol)
  }

  @Test
  def testInitNextGenerationEmptyGroup() {
    assertEquals(Empty, group.currentState)
    assertEquals(0, group.generationId)
    assertNull(group.protocol)

    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    assertEquals(1, group.generationId)
    assertNull(group.protocol)
  }

  @Test
  def testOffsetCommit(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val offset = OffsetAndMetadata(37)

    group.prepareOffsetCommit(Map(partition -> offset))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.completePendingOffsetWrite(partition, offset)
    assertTrue(group.hasOffsets)
    assertEquals(Some(offset), group.offset(partition))
  }

  @Test
  def testOffsetCommitFailure(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val offset = OffsetAndMetadata(37)

    group.prepareOffsetCommit(Map(partition -> offset))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.failPendingOffsetWrite(partition, offset)
    assertFalse(group.hasOffsets)
    assertEquals(None, group.offset(partition))
  }

  @Test
  def testOffsetCommitFailureWithAnotherPending(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val firstOffset = OffsetAndMetadata(37)
    val secondOffset = OffsetAndMetadata(57)

    group.prepareOffsetCommit(Map(partition -> firstOffset))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.prepareOffsetCommit(Map(partition -> secondOffset))
    assertTrue(group.hasOffsets)

    group.failPendingOffsetWrite(partition, firstOffset)
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.completePendingOffsetWrite(partition, secondOffset)
    assertTrue(group.hasOffsets)
    assertEquals(Some(secondOffset), group.offset(partition))
  }

  @Test
  def testOffsetCommitWithAnotherPending(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val firstOffset = OffsetAndMetadata(37)
    val secondOffset = OffsetAndMetadata(57)

    group.prepareOffsetCommit(Map(partition -> firstOffset))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.prepareOffsetCommit(Map(partition -> secondOffset))
    assertTrue(group.hasOffsets)

    group.completePendingOffsetWrite(partition, firstOffset)
    assertTrue(group.hasOffsets)
    assertEquals(Some(firstOffset), group.offset(partition))

    group.completePendingOffsetWrite(partition, secondOffset)
    assertTrue(group.hasOffsets)
    assertEquals(Some(secondOffset), group.offset(partition))
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
