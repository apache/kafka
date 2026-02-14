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
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{EndTxnRequest, JoinGroupRequest}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.common.utils.ProducerIdAndEpoch
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.junit.jupiter.api.Assertions.assertNotEquals

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
  )
)
class WriteTxnMarkersRequestTest(cluster:ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest
  def testDelayedWriteTxnMarkersShouldNotCommitTxnOffsetWithNewConsumerGroupProtocol(): Unit = {
    testDelayedWriteTxnMarkersShouldNotCommitTxnOffset(true)
  }

  @ClusterTest
  def testDelayedWriteTxnMarkersShouldNotCommitTxnOffsetWithOldConsumerGroupProtocol(): Unit = {
    testDelayedWriteTxnMarkersShouldNotCommitTxnOffset(false)
  }

  private def testDelayedWriteTxnMarkersShouldNotCommitTxnOffset(useNewProtocol: Boolean): Unit = {
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

      commitTxnOffset(
        groupId = groupId,
        memberId = if (version >= 3) memberId else JoinGroupRequest.UNKNOWN_MEMBER_ID,
        generationId = if (version >= 3) 1 else JoinGroupRequest.UNKNOWN_GENERATION_ID,
        producerId = producerIdAndEpoch.producerId,
        producerEpoch = if (useTV2) (producerIdAndEpoch.epoch + 1).toShort else producerIdAndEpoch.epoch,
        transactionalId = transactionalId,
        topic = topic,
        partition = partition,
        offset = offset + version,
        expectedError = Errors.NONE,
        version = version.toShort
      )

      // Delayed txn marker should be accepted for TV1 and rejected for TV2.
      // Note that for the ideal case, producer epoch + 1 should also be rejected for TV2,
      // which is still under fixing.
      writeTxnMarkers(
        producerId = producerIdAndEpoch.producerId,
        producerEpoch = producerIdAndEpoch.epoch,
        committed = true,
        expectedError = if (useTV2) Errors.INVALID_PRODUCER_EPOCH else Errors.NONE
      )

      // The offset is committed for TV1 and not committed for TV2.
      TestUtils.waitUntilTrue(() =>
        try {
          fetchOffset(groupId, topic, partition) == (if (useTV2) -1L else offset + version)
        } catch {
          case _: Throwable => false
        }, "unexpected txn commit offset"
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

      // The offset is committed for TV2.
      TestUtils.waitUntilTrue(() =>
        try {
          fetchOffset(groupId, topic, partition) == offset + version
        } catch {
          case _: Throwable => false
        }, "txn commit offset validation failed"
      )
    }
  }
}
