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
package kafka.server

import kafka.utils.TestUtils
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{EndTxnRequest, JoinGroupRequest}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.common.utils.ProducerIdAndEpoch
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.junit.jupiter.api.Assertions.{assertNotEquals, assertThrows}

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
  )
)
class TxnOffsetCommitRequestTest(cluster:ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest
  def testTxnOffsetCommitWithNewConsumerGroupProtocol(): Unit = {
    testTxnOffsetCommit(true)
  }

  @ClusterTest
  def testTxnOffsetCommitWithOldConsumerGroupProtocol(): Unit = {
    testTxnOffsetCommit(false)
  }

  @ClusterTest
  def testDelayedTxnOffsetCommitWithBumpedEpochIsRejectedWithNewConsumerGroupProtocol(): Unit = {
    testDelayedTxnOffsetCommitWithBumpedEpochIsRejected(true)
  }

  @ClusterTest
  def testDelayedTxnOffsetCommitWithBumpedEpochIsRejectedWithOldConsumerGroupProtocol(): Unit = {
    testDelayedTxnOffsetCommitWithBumpedEpochIsRejected(false)
  }

  private def testTxnOffsetCommit(useNewProtocol: Boolean): Unit = {
    val topic = "topic"
    val partition = 0
    val transactionalId = "txn"
    val groupId = "group"

    // Creates the __consumer_offsets and __transaction_state topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()
    createTransactionStateTopic()

    // Join the consumer group. Note that we don't heartbeat here so we must use
    // a session long enough for the duration of the test.
    val (memberId: String, memberEpoch: Int) = joinConsumerGroup(groupId, useNewProtocol)
    assertNotEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, memberId)
    assertNotEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, memberEpoch)

    createTopic(topic, 1)

    for (version <- 0 to ApiKeys.TXN_OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)) {
      // Verify that the TXN_OFFSET_COMMIT request is processed correctly when member id is UNKNOWN_MEMBER_ID
      // and generation id is UNKNOWN_GENERATION_ID under all api versions.
      verifyTxnCommitAndFetch(
        topic = topic,
        partition = partition,
        transactionalId = transactionalId,
        groupId = groupId,
        memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
        generationId = JoinGroupRequest.UNKNOWN_GENERATION_ID,
        offset = 100 + version,
        version = version.toShort,
        expectedTxnCommitError = Errors.NONE
      )

      if (version >= 3) {
        // Verify that the TXN_OFFSET_COMMIT request is processed correctly when the member ID
        // and generation ID are known. This validation starts from version 3, as the member ID
        // must not be empty from version 3 onwards.
        verifyTxnCommitAndFetch(
          topic = topic,
          partition = partition,
          transactionalId = transactionalId,
          groupId = groupId,
          memberId = memberId,
          generationId = memberEpoch,
          offset = 200 + version,
          version = version.toShort,
          expectedTxnCommitError = Errors.NONE
        )

        // Verify TXN_OFFSET_COMMIT request failed with incorrect memberId.
        verifyTxnCommitAndFetch(
          topic = topic,
          partition = partition,
          transactionalId = transactionalId,
          groupId = groupId,
          memberId = "non-exist",
          generationId = memberEpoch,
          offset = 200 + version,
          version = version.toShort,
          expectedTxnCommitError = Errors.UNKNOWN_MEMBER_ID
        )

        // Verify TXN_OFFSET_COMMIT request failed with incorrect generationId.
        verifyTxnCommitAndFetch(
          topic = topic,
          partition = partition,
          transactionalId = transactionalId,
          groupId = groupId,
          memberId = memberId,
          generationId = 100,
          offset = 200 + version,
          version = version.toShort,
          expectedTxnCommitError = Errors.ILLEGAL_GENERATION
        )
      } else {
        // Verify that the TXN_OFFSET_COMMIT request failed when group metadata is set under version 3.
        assertThrows(classOf[UnsupportedVersionException], () =>
          verifyTxnCommitAndFetch(
            topic = topic,
            partition = partition,
            transactionalId = transactionalId,
            groupId = groupId,
            memberId = memberId,
            generationId = memberEpoch,
            offset = 200 + version,
            version = version.toShort,
            expectedTxnCommitError = Errors.NONE
          )
        )
      }
    }
  }

  private def verifyTxnCommitAndFetch(
    topic: String,
    partition: Int,
    transactionalId: String,
    groupId: String,
    memberId: String,
    generationId: Int,
    offset: Long,
    version: Short,
    expectedTxnCommitError: Errors
  ): Unit = {
    var producerIdAndEpoch: ProducerIdAndEpoch = null
    // Wait until the coordinator finishes loading.
    TestUtils.waitUntilTrue(() =>
      try {
        producerIdAndEpoch = initProducerId(
          transactionalId = transactionalId,
          producerIdAndEpoch = ProducerIdAndEpoch.NONE,
          expectedError = Errors.NONE
        )
        true
      } catch {
        case _: Throwable => false
      }, "initProducerId request failed"
    )

    addOffsetsToTxn(
      groupId = groupId,
      producerId = producerIdAndEpoch.producerId,
      producerEpoch = producerIdAndEpoch.epoch,
      transactionalId = transactionalId
    )

    val originalOffset = fetchOffset(groupId, topic, partition)

    commitTxnOffset(
      groupId = groupId,
      memberId = memberId,
      generationId = generationId,
      producerId = producerIdAndEpoch.producerId,
      producerEpoch = producerIdAndEpoch.epoch,
      transactionalId = transactionalId,
      topic = topic,
      partition = partition,
      offset = offset,
      expectedError = expectedTxnCommitError,
      version = version
    )

    endTxn(
      producerId = producerIdAndEpoch.producerId,
      producerEpoch = producerIdAndEpoch.epoch,
      transactionalId = transactionalId,
      isTransactionV2Enabled = false,
      committed = true,
      expectedError = Errors.NONE
    )

    val expectedOffset = if (expectedTxnCommitError == Errors.NONE) offset else originalOffset

    TestUtils.waitUntilTrue(() =>
      try {
        fetchOffset(groupId, topic, partition) == expectedOffset
      } catch {
        case _: Throwable => false
      }, "txn commit offset validation failed"
    )
  }

  private def testDelayedTxnOffsetCommitWithBumpedEpochIsRejected(useNewProtocol: Boolean): Unit = {
    val topic = "topic"
    val partition = 0
    val transactionalId = "txn"
    val groupId = "group"
    val offset = 100L

    // Creates the __consumer_offsets and __transaction_state topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()
    createTransactionStateTopic()

    // Join the consumer group. Note that we don't heartbeat here so we must use
    // a session long enough for the duration of the test.
    val (memberId: String, memberEpoch: Int) = joinConsumerGroup(groupId, useNewProtocol)
    assertNotEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, memberId)
    assertNotEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, memberEpoch)

    createTopic(topic, 1)

    for (version <- ApiKeys.TXN_OFFSET_COMMIT.oldestVersion to ApiKeys.TXN_OFFSET_COMMIT.latestVersion(isUnstableApiEnabled)) {
      val useTV2 = version > EndTxnRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2

      // Initialize producer. Wait until the coordinator finishes loading.
      var producerIdAndEpoch: ProducerIdAndEpoch = null
      TestUtils.waitUntilTrue(() =>
        try {
          producerIdAndEpoch = initProducerId(
            transactionalId = transactionalId,
            producerIdAndEpoch = ProducerIdAndEpoch.NONE,
            expectedError = Errors.NONE
          )
          true
        } catch {
          case _: Throwable => false
        }, "initProducerId request failed"
      )

      addOffsetsToTxn(
        groupId = groupId,
        producerId = producerIdAndEpoch.producerId,
        producerEpoch = producerIdAndEpoch.epoch,
        transactionalId = transactionalId
      )

      // Complete the transaction.
      endTxn(
        producerId = producerIdAndEpoch.producerId,
        producerEpoch = producerIdAndEpoch.epoch,
        transactionalId = transactionalId,
        isTransactionV2Enabled = useTV2,
        committed = true,
        expectedError = Errors.NONE
      )

      // Start a new transaction. Wait for the previous transaction to complete.
      TestUtils.waitUntilTrue(() =>
        try {
          addOffsetsToTxn(
            groupId = groupId,
            producerId = producerIdAndEpoch.producerId,
            producerEpoch = if (useTV2) (producerIdAndEpoch.epoch + 1).toShort else producerIdAndEpoch.epoch,
            transactionalId = transactionalId
          )
          true
        } catch {
          case _: Throwable => false
        }, "addOffsetsToTxn request failed"
      )

      // Committing offset with old epoch succeeds for TV1 and fails for TV2.
      commitTxnOffset(
        groupId = groupId,
        memberId = if (version >= 3) memberId else JoinGroupRequest.UNKNOWN_MEMBER_ID,
        generationId = if (version >= 3) 1 else JoinGroupRequest.UNKNOWN_GENERATION_ID,
        producerId = producerIdAndEpoch.producerId,
        producerEpoch = producerIdAndEpoch.epoch,
        transactionalId = transactionalId,
        topic = topic,
        partition = partition,
        offset = offset,
        expectedError = if (useTV2) Errors.INVALID_PRODUCER_EPOCH else Errors.NONE,
        version = version.toShort
      )

      // Complete the transaction.
      endTxn(
        producerId = producerIdAndEpoch.producerId,
        producerEpoch = if (useTV2) (producerIdAndEpoch.epoch + 1).toShort else producerIdAndEpoch.epoch,
        transactionalId = transactionalId,
        isTransactionV2Enabled = useTV2,
        committed = true,
        expectedError = Errors.NONE
      )
    }
  }
}
