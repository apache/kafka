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

package kafka.coordinator.group

import kafka.common.OffsetAndMetadata
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.{Time, MockTime}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.jdk.CollectionConverters._

/**
 * Test group state transitions and other GroupMetadata functionality
 */
class GroupMetadataTest {
  private val protocolType = "consumer"
  private val groupInstanceId = "groupInstanceId"
  private val memberId = "memberId"
  private val clientId = "clientId"
  private val clientHost = "clientHost"
  private val rebalanceTimeoutMs = 60000
  private val sessionTimeoutMs = 10000

  private var group: GroupMetadata = null

  @BeforeEach
  def setUp(): Unit = {
    group = new GroupMetadata("groupId", Empty, Time.SYSTEM)
  }

  @Test
  def testCanRebalanceWhenStable(): Unit = {
    assertTrue(group.canRebalance)
  }

  @Test
  def testCanRebalanceWhenCompletingRebalance(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(CompletingRebalance)
    assertTrue(group.canRebalance)
  }

  @Test
  def testCannotRebalanceWhenPreparingRebalance(): Unit = {
    group.transitionTo(PreparingRebalance)
    assertFalse(group.canRebalance)
  }

  @Test
  def testCannotRebalanceWhenDead(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Empty)
    group.transitionTo(Dead)
    assertFalse(group.canRebalance)
  }

  @Test
  def testStableToPreparingRebalanceTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    assertState(group, PreparingRebalance)
  }

  @Test
  def testStableToDeadTransition(): Unit = {
    group.transitionTo(Dead)
    assertState(group, Dead)
  }

  @Test
  def testAwaitingRebalanceToPreparingRebalanceTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(CompletingRebalance)
    group.transitionTo(PreparingRebalance)
    assertState(group, PreparingRebalance)
  }

  @Test
  def testPreparingRebalanceToDeadTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    assertState(group, Dead)
  }

  @Test
  def testPreparingRebalanceToEmptyTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Empty)
    assertState(group, Empty)
  }

  @Test
  def testEmptyToDeadTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Empty)
    group.transitionTo(Dead)
    assertState(group, Dead)
  }

  @Test
  def testAwaitingRebalanceToStableTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(CompletingRebalance)
    group.transitionTo(Stable)
    assertState(group, Stable)
  }

  @Test
  def testEmptyToStableIllegalTransition(): Unit = {
    assertThrows(classOf[IllegalStateException], () => group.transitionTo(Stable))
  }

  @Test
  def testStableToStableIllegalTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(CompletingRebalance)
    group.transitionTo(Stable)
    assertThrows(classOf[IllegalStateException], () => group.transitionTo(Stable))
  }

  @Test
  def testEmptyToAwaitingRebalanceIllegalTransition(): Unit = {
    assertThrows(classOf[IllegalStateException], () => group.transitionTo(CompletingRebalance))
  }

  @Test
  def testPreparingRebalanceToPreparingRebalanceIllegalTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    assertThrows(classOf[IllegalStateException], () => group.transitionTo(PreparingRebalance))
  }

  @Test
  def testPreparingRebalanceToStableIllegalTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    assertThrows(classOf[IllegalStateException], () => group.transitionTo(Stable))
  }

  @Test
  def testAwaitingRebalanceToAwaitingRebalanceIllegalTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(CompletingRebalance)
    assertThrows(classOf[IllegalStateException], () => group.transitionTo(CompletingRebalance))
  }

  def testDeadToDeadIllegalTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    group.transitionTo(Dead)
    assertState(group, Dead)
  }

  @Test
  def testDeadToStableIllegalTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    assertThrows(classOf[IllegalStateException], () => group.transitionTo(Stable))
  }

  @Test
  def testDeadToPreparingRebalanceIllegalTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    assertThrows(classOf[IllegalStateException], () => group.transitionTo(PreparingRebalance))
  }

  @Test
  def testDeadToAwaitingRebalanceIllegalTransition(): Unit = {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    assertThrows(classOf[IllegalStateException], () => group.transitionTo(CompletingRebalance))
  }

  @Test
  def testSelectProtocol(): Unit = {
    val memberId = "memberId"
    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    group.add(member)
    assertEquals("range", group.selectProtocol)

    val otherMemberId = "otherMemberId"
    val otherMember = new MemberMetadata(otherMemberId, None, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("roundrobin", Array.empty[Byte]), ("range", Array.empty[Byte])))

    group.add(otherMember)
    // now could be either range or robin since there is no majority preference
    assertTrue(Set("range", "roundrobin")(group.selectProtocol))

    val lastMemberId = "lastMemberId"
    val lastMember = new MemberMetadata(lastMemberId, None, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("roundrobin", Array.empty[Byte]), ("range", Array.empty[Byte])))

    group.add(lastMember)
    // now we should prefer 'roundrobin'
    assertEquals("roundrobin", group.selectProtocol)
  }

  @Test
  def testSelectProtocolRaisesIfNoMembers(): Unit = {
    assertThrows(classOf[IllegalStateException], () => group.selectProtocol)
  }

  @Test
  def testSelectProtocolChoosesCompatibleProtocol(): Unit = {
    val memberId = "memberId"
    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    val otherMemberId = "otherMemberId"
    val otherMember = new MemberMetadata(otherMemberId, None, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("roundrobin", Array.empty[Byte]), ("blah", Array.empty[Byte])))

    group.add(member)
    group.add(otherMember)
    assertEquals("roundrobin", group.selectProtocol)
  }

  @Test
  def testSupportsProtocols(): Unit = {
    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    // by default, the group supports everything
    assertTrue(group.supportsProtocols(protocolType, Set("roundrobin", "range")))

    group.add(member)
    group.transitionTo(PreparingRebalance)
    assertTrue(group.supportsProtocols(protocolType, Set("roundrobin", "foo")))
    assertTrue(group.supportsProtocols(protocolType, Set("range", "foo")))
    assertFalse(group.supportsProtocols(protocolType, Set("foo", "bar")))

    val otherMemberId = "otherMemberId"
    val otherMember = new MemberMetadata(otherMemberId, None, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("roundrobin", Array.empty[Byte]), ("blah", Array.empty[Byte])))

    group.add(otherMember)

    assertTrue(group.supportsProtocols(protocolType, Set("roundrobin", "foo")))
    assertFalse(group.supportsProtocols("invalid_type", Set("roundrobin", "foo")))
    assertFalse(group.supportsProtocols(protocolType, Set("range", "foo")))
  }

  @Test
  def testOffsetRemovalDuringTransitionFromEmptyToNonEmpty(): Unit = {
    val topic = "foo"
    val partition = new TopicPartition(topic, 0)
    val time = new MockTime()
    group = new GroupMetadata("groupId", Empty, time)

    // Rebalance once in order to commit offsets
    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("range", ConsumerProtocol.serializeSubscription(new Subscription(List("foo").asJava)).array())))
    group.transitionTo(PreparingRebalance)
    group.add(member)
    group.initNextGeneration()
    assertEquals(Some(Set("foo")), group.getSubscribedTopics)

    val offset = offsetAndMetadata(offset = 37, timestamp = time.milliseconds())
    val commitRecordOffset = 3

    group.prepareOffsetCommit(Map(partition -> offset))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))
    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(commitRecordOffset), offset))

    val offsetRetentionMs = 50000L
    time.sleep(offsetRetentionMs + 1)

    // Rebalance again so that the group becomes empty
    group.transitionTo(PreparingRebalance)
    group.remove(memberId)
    group.initNextGeneration()

    // The group is empty, but we should not expire the offset because the state was just changed
    assertEquals(Empty, group.currentState)
    assertEquals(Map.empty, group.removeExpiredOffsets(time.milliseconds(), offsetRetentionMs))

    // Start a new rebalance to add the member back. The offset should not be expired
    // while the rebalance is in progress.
    group.transitionTo(PreparingRebalance)
    group.add(member)
    assertEquals(Map.empty, group.removeExpiredOffsets(time.milliseconds(), offsetRetentionMs))
  }

  @Test
  def testSubscribedTopics(): Unit = {
    // not able to compute it for a newly created group
    assertEquals(None, group.getSubscribedTopics)

    val memberId = "memberId"
    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("range", ConsumerProtocol.serializeSubscription(new Subscription(List("foo").asJava)).array())))

    group.transitionTo(PreparingRebalance)
    group.add(member)

    group.initNextGeneration()

    assertEquals(Some(Set("foo")), group.getSubscribedTopics)

    group.transitionTo(PreparingRebalance)
    group.remove(memberId)

    group.initNextGeneration()

    assertEquals(Some(Set.empty), group.getSubscribedTopics)

    val memberWithFaultyProtocol  = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("range", Array.empty[Byte])))

    group.transitionTo(PreparingRebalance)
    group.add(memberWithFaultyProtocol)

    group.initNextGeneration()

    assertEquals(None, group.getSubscribedTopics)
  }

  @Test
  def testSubscribedTopicsNonConsumerGroup(): Unit = {
    // not able to compute it for a newly created group
    assertEquals(None, group.getSubscribedTopics)

    val memberId = "memberId"
    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, "My Protocol", List(("range", Array.empty[Byte])))

    group.transitionTo(PreparingRebalance)
    group.add(member)

    group.initNextGeneration()

    assertEquals(None, group.getSubscribedTopics)
  }

  @Test
  def testInitNextGeneration(): Unit = {
    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    member.supportedProtocols = List(("roundrobin", Array.empty[Byte]))

    group.transitionTo(PreparingRebalance)
    group.add(member, _ => ())

    assertEquals(0, group.generationId)
    assertNull(group.protocolName.orNull)

    group.initNextGeneration()

    assertEquals(1, group.generationId)
    assertEquals("roundrobin", group.protocolName.orNull)
  }

  @Test
  def testInitNextGenerationEmptyGroup(): Unit = {
    assertEquals(Empty, group.currentState)
    assertEquals(0, group.generationId)
    assertNull(group.protocolName.orNull)

    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    assertEquals(1, group.generationId)
    assertNull(group.protocolName.orNull)
  }

  @Test
  def testOffsetCommit(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val offset = offsetAndMetadata(37)
    val commitRecordOffset = 3

    group.prepareOffsetCommit(Map(partition -> offset))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(commitRecordOffset), offset))
    assertTrue(group.hasOffsets)
    assertEquals(Some(offset), group.offset(partition))
  }

  @Test
  def testOffsetCommitFailure(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val offset = offsetAndMetadata(37)

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
    val firstOffset = offsetAndMetadata(37)
    val secondOffset = offsetAndMetadata(57)

    group.prepareOffsetCommit(Map(partition -> firstOffset))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.prepareOffsetCommit(Map(partition -> secondOffset))
    assertTrue(group.hasOffsets)

    group.failPendingOffsetWrite(partition, firstOffset)
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(3L), secondOffset))
    assertTrue(group.hasOffsets)
    assertEquals(Some(secondOffset), group.offset(partition))
  }

  @Test
  def testOffsetCommitWithAnotherPending(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val firstOffset = offsetAndMetadata(37)
    val secondOffset = offsetAndMetadata(57)

    group.prepareOffsetCommit(Map(partition -> firstOffset))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.prepareOffsetCommit(Map(partition -> secondOffset))
    assertTrue(group.hasOffsets)

    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(4L), firstOffset))
    assertTrue(group.hasOffsets)
    assertEquals(Some(firstOffset), group.offset(partition))

    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(5L), secondOffset))
    assertTrue(group.hasOffsets)
    assertEquals(Some(secondOffset), group.offset(partition))
  }

  @Test
  def testConsumerBeatsTransactionalOffsetCommit(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val producerId = 13232L
    val txnOffsetCommit = offsetAndMetadata(37)
    val consumerOffsetCommit = offsetAndMetadata(57)

    group.prepareTxnOffsetCommit(producerId, Map(partition -> txnOffsetCommit))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.prepareOffsetCommit(Map(partition -> consumerOffsetCommit))
    assertTrue(group.hasOffsets)

    group.onTxnOffsetCommitAppend(producerId, partition, CommitRecordMetadataAndOffset(Some(3L), txnOffsetCommit))
    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(4L), consumerOffsetCommit))
    assertTrue(group.hasOffsets)
    assertEquals(Some(consumerOffsetCommit), group.offset(partition))

    group.completePendingTxnOffsetCommit(producerId, isCommit = true)
    assertTrue(group.hasOffsets)
    // This is the crucial assertion which validates that we materialize offsets in offset order, not transactional order.
    assertEquals(Some(consumerOffsetCommit), group.offset(partition))
  }

  @Test
  def testTransactionBeatsConsumerOffsetCommit(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val producerId = 13232L
    val txnOffsetCommit = offsetAndMetadata(37)
    val consumerOffsetCommit = offsetAndMetadata(57)

    group.prepareTxnOffsetCommit(producerId, Map(partition -> txnOffsetCommit))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.prepareOffsetCommit(Map(partition -> consumerOffsetCommit))
    assertTrue(group.hasOffsets)

    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(3L), consumerOffsetCommit))
    group.onTxnOffsetCommitAppend(producerId, partition, CommitRecordMetadataAndOffset(Some(4L), txnOffsetCommit))
    assertTrue(group.hasOffsets)
    // The transactional offset commit hasn't been committed yet, so we should materialize the consumer offset commit.
    assertEquals(Some(consumerOffsetCommit), group.offset(partition))

    group.completePendingTxnOffsetCommit(producerId, isCommit = true)
    assertTrue(group.hasOffsets)
    // The transactional offset commit has been materialized and the transactional commit record is later in the log,
    // so it should be materialized.
    assertEquals(Some(txnOffsetCommit), group.offset(partition))
  }

  @Test
  def testTransactionalCommitIsAbortedAndConsumerCommitWins(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val producerId = 13232L
    val txnOffsetCommit = offsetAndMetadata(37)
    val consumerOffsetCommit = offsetAndMetadata(57)

    group.prepareTxnOffsetCommit(producerId, Map(partition -> txnOffsetCommit))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.prepareOffsetCommit(Map(partition -> consumerOffsetCommit))
    assertTrue(group.hasOffsets)

    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(3L), consumerOffsetCommit))
    group.onTxnOffsetCommitAppend(producerId, partition, CommitRecordMetadataAndOffset(Some(4L), txnOffsetCommit))
    assertTrue(group.hasOffsets)
    // The transactional offset commit hasn't been committed yet, so we should materialize the consumer offset commit.
    assertEquals(Some(consumerOffsetCommit), group.offset(partition))

    group.completePendingTxnOffsetCommit(producerId, isCommit = false)
    assertTrue(group.hasOffsets)
    // The transactional offset commit should be discarded and the consumer offset commit should continue to be
    // materialized.
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertEquals(Some(consumerOffsetCommit), group.offset(partition))
  }

  @Test
  def testFailedTxnOffsetCommitLeavesNoPendingState(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val producerId = 13232L
    val txnOffsetCommit = offsetAndMetadata(37)

    group.prepareTxnOffsetCommit(producerId, Map(partition -> txnOffsetCommit))
    assertTrue(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))
    group.failPendingTxnOffsetCommit(producerId, partition)
    assertFalse(group.hasOffsets)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))

    // The commit marker should now have no effect.
    group.completePendingTxnOffsetCommit(producerId, isCommit = true)
    assertFalse(group.hasOffsets)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
  }

  @Test
  def testReplaceGroupInstanceWithNonExistingMember(): Unit = {
    val newMemberId = "newMemberId"
    assertThrows(classOf[IllegalArgumentException], () => group.replaceStaticMember(groupInstanceId, memberId, newMemberId))
  }

  @Test
  def testReplaceGroupInstance(): Unit = {
    val member = new MemberMetadata(memberId, Some(groupInstanceId), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    var joinAwaitingMemberFenced = false
    group.add(member, joinGroupResult => {
      joinAwaitingMemberFenced = joinGroupResult.error == Errors.FENCED_INSTANCE_ID
    })
    var syncAwaitingMemberFenced = false
    member.awaitingSyncCallback = syncGroupResult => {
      syncAwaitingMemberFenced = syncGroupResult.error == Errors.FENCED_INSTANCE_ID
    }
    assertTrue(group.isLeader(memberId))
    assertEquals(Some(memberId), group.currentStaticMemberId(groupInstanceId))

    val newMemberId = "newMemberId"
    group.replaceStaticMember(groupInstanceId, memberId, newMemberId)
    assertTrue(group.isLeader(newMemberId))
    assertEquals(Some(newMemberId), group.currentStaticMemberId(groupInstanceId))
    assertTrue(joinAwaitingMemberFenced)
    assertTrue(syncAwaitingMemberFenced)
    assertFalse(member.isAwaitingJoin)
    assertFalse(member.isAwaitingSync)
  }

  @Test
  def testInvokeJoinCallback(): Unit = {
    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    var invoked = false
    group.add(member, _ => {
      invoked = true
    })

    assertTrue(group.hasAllMembersJoined)
    group.maybeInvokeJoinCallback(member, JoinGroupResult(member.memberId, Errors.NONE))
    assertTrue(invoked)
    assertFalse(member.isAwaitingJoin)
  }

  @Test
  def testNotInvokeJoinCallback(): Unit = {
    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))
    group.add(member)

    assertFalse(member.isAwaitingJoin)
    group.maybeInvokeJoinCallback(member, JoinGroupResult(member.memberId, Errors.NONE))
    assertFalse(member.isAwaitingJoin)
  }

  @Test
  def testInvokeSyncCallback(): Unit = {
    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    group.add(member)
    member.awaitingSyncCallback = _ => {}

    val invoked = group.maybeInvokeSyncCallback(member, SyncGroupResult(Errors.NONE))
    assertTrue(invoked)
    assertFalse(member.isAwaitingSync)
  }

  @Test
  def testNotInvokeSyncCallback(): Unit = {
    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))
    group.add(member)

    val invoked = group.maybeInvokeSyncCallback(member, SyncGroupResult(Errors.NONE))
    assertFalse(invoked)
    assertFalse(member.isAwaitingSync)
  }

  @Test
  def testHasPendingNonTxnOffsets(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val offset = offsetAndMetadata(37)

    group.prepareOffsetCommit(Map(partition -> offset))
    assertTrue(group.hasPendingOffsetCommitsForTopicPartition(partition))
  }

  @Test
  def testHasPendingTxnOffsets(): Unit = {
    val txnPartition = new TopicPartition("foo", 1)
    val offset = offsetAndMetadata(37)
    val producerId = 5

    group.prepareTxnOffsetCommit(producerId, Map(txnPartition -> offset))
    assertTrue(group.hasPendingOffsetCommitsForTopicPartition(txnPartition))

    assertFalse(group.hasPendingOffsetCommitsForTopicPartition(new TopicPartition("non-exist", 0)))
  }

  @Test
  def testCannotAddPendingMemberIfStable(): Unit = {
    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))
    group.add(member)
    assertThrows(classOf[IllegalStateException], () => group.addPendingMember(memberId))
  }

  @Test
  def testRemovalFromPendingAfterMemberIsStable(): Unit = {
    group.addPendingMember(memberId)
    assertFalse(group.has(memberId))
    assertTrue(group.isPendingMember(memberId))

    val member = new MemberMetadata(memberId, None, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))
    group.add(member)
    assertTrue(group.has(memberId))
    assertFalse(group.isPendingMember(memberId))
  }

  @Test
  def testRemovalFromPendingWhenMemberIsRemoved(): Unit = {
    group.addPendingMember(memberId)
    assertFalse(group.has(memberId))
    assertTrue(group.isPendingMember(memberId))

    group.remove(memberId)
    assertFalse(group.has(memberId))
    assertFalse(group.isPendingMember(memberId))
  }

  @Test
  def testCannotAddStaticMemberIfAlreadyPresent(): Unit = {
    val member = new MemberMetadata(memberId, Some(groupInstanceId), clientId, clientHost,
      rebalanceTimeoutMs, sessionTimeoutMs, protocolType, List(("range", Array.empty[Byte])))
    group.add(member)
    assertTrue(group.has(memberId))
    assertTrue(group.hasStaticMember(groupInstanceId))

    // We aren ot permitted to add the member again if it is already present
    assertThrows(classOf[IllegalStateException], () => group.add(member))
  }

  @Test
  def testCannotAddPendingSyncOfUnknownMember(): Unit = {
    assertThrows(classOf[IllegalStateException],
      () => group.addPendingSyncMember(memberId))
  }

  @Test
  def testCannotRemovePendingSyncOfUnknownMember(): Unit = {
    assertThrows(classOf[IllegalStateException],
      () => group.removePendingSyncMember(memberId))
  }

  @Test
  def testCanAddAndRemovePendingSyncMember(): Unit = {
    val member = new MemberMetadata(memberId, Some(groupInstanceId), clientId, clientHost,
      rebalanceTimeoutMs, sessionTimeoutMs, protocolType, List(("range", Array.empty[Byte])))
    group.add(member)
    group.addPendingSyncMember(memberId)
    assertEquals(Set(memberId), group.allPendingSyncMembers)
    group.removePendingSyncMember(memberId)
    assertEquals(Set(), group.allPendingSyncMembers)
  }

  @Test
  def testRemovalFromPendingSyncWhenMemberIsRemoved(): Unit = {
    val member = new MemberMetadata(memberId, Some(groupInstanceId), clientId, clientHost,
      rebalanceTimeoutMs, sessionTimeoutMs, protocolType, List(("range", Array.empty[Byte])))
    group.add(member)
    group.addPendingSyncMember(memberId)
    assertEquals(Set(memberId), group.allPendingSyncMembers)
    group.remove(memberId)
    assertEquals(Set(), group.allPendingSyncMembers)
  }

  @Test
  def testNewGenerationClearsPendingSyncMembers(): Unit = {
    val member = new MemberMetadata(memberId, Some(groupInstanceId), clientId, clientHost,
      rebalanceTimeoutMs, sessionTimeoutMs, protocolType, List(("range", Array.empty[Byte])))
    group.add(member)
    group.transitionTo(PreparingRebalance)
    group.addPendingSyncMember(memberId)
    assertEquals(Set(memberId), group.allPendingSyncMembers)
    group.initNextGeneration()
    assertEquals(Set(), group.allPendingSyncMembers)
  }

  private def assertState(group: GroupMetadata, targetState: GroupState): Unit = {
    val states: Set[GroupState] = Set(Stable, PreparingRebalance, CompletingRebalance, Dead)
    val otherStates = states - targetState
    otherStates.foreach { otherState =>
      assertFalse(group.is(otherState))
    }
    assertTrue(group.is(targetState))
  }

  private def offsetAndMetadata(offset: Long, timestamp: Long = Time.SYSTEM.milliseconds()): OffsetAndMetadata = {
    OffsetAndMetadata(offset, "", timestamp)
  }

}
