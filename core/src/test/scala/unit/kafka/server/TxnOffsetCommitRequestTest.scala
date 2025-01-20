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

import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterInstance, ClusterTest, ClusterTestDefaults, ClusterTestExtensions, Type}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.utils.ProducerIdAndEpoch
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue, fail}
import org.junit.jupiter.api.extension.ExtendWith

import scala.jdk.CollectionConverters.IterableHasAsScala

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(types = Array(Type.KRAFT), serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
  )
)
class TxnOffsetCommitRequestTest(cluster:ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest
  def testTxnOffsetCommitWithNewConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testTxnOffsetCommit(true)
  }

  @ClusterTest
  def testTxnOffsetCommitWithOldConsumerGroupProtocolAndNewGroupCoordinator(): Unit = {
    testTxnOffsetCommit(false)
  }

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, value = "false"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, value = "classic"),
    )
  )
  def testTxnOffsetCommitWithOldConsumerGroupProtocolAndOldGroupCoordinator(): Unit = {
    testTxnOffsetCommit(false)
  }

  private def testTxnOffsetCommit(useNewProtocol: Boolean): Unit = {
    if (useNewProtocol && !isNewGroupCoordinatorEnabled) {
      fail("Cannot use the new protocol with the old group coordinator.")
    }

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
    assertTrue(memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID)
    assertTrue(memberEpoch != JoinGroupRequest.UNKNOWN_GENERATION_ID)

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

    val originalOffset = fetchOffset(topic, partition, groupId)

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
        fetchOffset(topic, partition, groupId) == expectedOffset
      } catch {
        case _: Throwable => false
      }, "txn commit offset validation failed"
    )
  }

  private def fetchOffset(
     topic: String,
     partition: Int,
     groupId: String
  ): Long = {
    val fetchOffsetsResp = fetchOffsets(
      groups = Map(groupId -> List(new TopicPartition(topic, partition))),
      requireStable = true,
      version = ApiKeys.OFFSET_FETCH.latestVersion
    )
    val groupIdRecord = fetchOffsetsResp.find(_.groupId == groupId).head
    val topicRecord = groupIdRecord.topics.asScala.find(_.name == topic).head
    val partitionRecord = topicRecord.partitions.asScala.find(_.partitionIndex == partition).head
    partitionRecord.committedOffset
  }
}
