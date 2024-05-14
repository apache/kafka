/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.common.errors.FencedLeaderEpochException
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record._
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import kafka.server.FetcherThreadTestUtils.{initialFetchState, mkBatch}

import scala.collection.Map

class ReplicaFetcherTierStateMachineTest {

  val truncateOnFetch = true
  val topicIds = Map("topic1" -> Uuid.randomUuid(), "topic2" -> Uuid.randomUuid())
  val version = ApiKeys.FETCH.latestVersion()
  private val failedPartitions = new FailedPartitions

  @Test
  def testFollowerFetchMovedToTieredStore(): Unit = {
    val partition = new TopicPartition("topic", 0)

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = 5, highWatermark = 0L, rlmEnabled = true)

    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = 5)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 5, leaderEpoch = 5, new SimpleRecord("f".getBytes)),
      mkBatch(baseOffset = 6, leaderEpoch = 5, new SimpleRecord("g".getBytes)),
      mkBatch(baseOffset = 7, leaderEpoch = 5, new SimpleRecord("h".getBytes)),
      mkBatch(baseOffset = 8, leaderEpoch = 5, new SimpleRecord("i".getBytes)))

    val leaderState = PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 8L, rlmEnabled = true)
    // Overriding the log start offset to zero for mocking the scenario of segment 0-4 moved to remote store.
    leaderState.logStartOffset = 0
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    assertEquals(3L, replicaState.logEndOffset)
    val expectedState = if (truncateOnFetch) Option(Fetching) else Option(Truncating)
    assertEquals(expectedState, fetcher.fetchState(partition).map(_.state))

    fetcher.doWork()
    // verify that the offset moved to tiered store error triggered and respective states are truncated to expected.
    assertEquals(0L, replicaState.logStartOffset)
    assertEquals(5L, replicaState.localLogStartOffset)
    assertEquals(5L, replicaState.highWatermark)
    assertEquals(5L, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 5) fetcher.doWork()
    assertEquals(4, replicaState.log.size)
    assertEquals(0L, replicaState.logStartOffset)
    assertEquals(5L, replicaState.localLogStartOffset)
    assertEquals(8L, replicaState.highWatermark)
    assertEquals(9L, replicaState.logEndOffset)
  }

  /**
   * This test verifies the following scenario:
   * 1. Leader is archiving to tiered storage and has a follower.
   * 2. Follower has caught up to offset X (exclusive).
   * 3. While follower is offline, leader moves X to tiered storage and expires data locally till Y, such that,
   *    `Y = leaderLocalLogStartOffset` and `leaderLocalLogStartOffset > X`. Meanwhile, X has been expired from
   *    tiered storage as well. Hence, `X < globalLogStartOffset`.
   * 4. Follower comes online and tries to fetch X from leader.
   */
  @Test
  def testFollowerFetchOffsetOutOfRangeWithTieredStore(): Unit = {
    val partition = new TopicPartition("topic", 0)

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = 7, highWatermark = 0L, rlmEnabled = true)

    val mockLeaderEndpoint = new MockLeaderEndPoint
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = 7)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 7, leaderEpoch = 7, new SimpleRecord("h".getBytes)),
      mkBatch(baseOffset = 8, leaderEpoch = 7, new SimpleRecord("i".getBytes)),
      mkBatch(baseOffset = 9, leaderEpoch = 7, new SimpleRecord("j".getBytes)),
      mkBatch(baseOffset = 10, leaderEpoch = 7, new SimpleRecord("k".getBytes)))

    val leaderState = PartitionState(leaderLog, leaderEpoch = 7, highWatermark = 10L, rlmEnabled = true)
    // Overriding the log start offset to 5 for mocking the scenario of segments 5-6 moved to remote store and
    // segments 0-4 expired.
    leaderState.logStartOffset = 5
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    assertEquals(3L, replicaState.logEndOffset)
    val expectedState = if (truncateOnFetch) Option(Fetching) else Option(Truncating)
    assertEquals(expectedState, fetcher.fetchState(partition).map(_.state))

    fetcher.doWork()
    // Verify that the out of range error is triggered and the fetch offset is reset to the global log start offset.
    assertEquals(0L, replicaState.logStartOffset)
    assertEquals(5L, replicaState.localLogStartOffset)
    assertEquals(5L, replicaState.highWatermark)
    assertEquals(5L, replicaState.logEndOffset)

    fetcher.doWork()
    // Verify that the offset moved to tiered store error is triggered and respective states are truncated to expected
    // positions.
    assertEquals(0L, replicaState.logStartOffset)
    assertEquals(7L, replicaState.localLogStartOffset)
    assertEquals(7L, replicaState.highWatermark)
    assertEquals(7L, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so call 'n' number of times to get the desired result.
    for (_ <- 1 to 5) fetcher.doWork()
    assertEquals(4, replicaState.log.size)
    assertEquals(5L, replicaState.logStartOffset)
    assertEquals(7L, replicaState.localLogStartOffset)
    assertEquals(10L, replicaState.highWatermark)
    assertEquals(11L, replicaState.logEndOffset)
  }

  @Test
  def testFencedOffsetResetAfterMovedToRemoteTier(): Unit = {
    val partition = new TopicPartition("topic", 0)
    var isErrorHandled = false
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint) {
      override def start(topicPartition: TopicPartition, currentFetchState: PartitionFetchState, fetchPartitionData: FetchResponseData.PartitionData): PartitionFetchState = {
        isErrorHandled = true
        throw new FencedLeaderEpochException(s"Epoch ${currentFetchState.currentLeaderEpoch} is fenced")
      }
    }
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, failedPartitions = failedPartitions)

    val replicaLog = Seq(
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))
    val replicaState = PartitionState(replicaLog, leaderEpoch = 5, highWatermark = 2L, rlmEnabled = true)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), fetchOffset = 0L, leaderEpoch = 5)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 5, leaderEpoch = 5, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 6, leaderEpoch = 5, new SimpleRecord("c".getBytes)))
    val leaderState = PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 6L, rlmEnabled = true)
    // Overriding the log start offset to zero for mocking the scenario of segment 0-4 moved to remote store.
    leaderState.logStartOffset = 0
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // After the offset moved to tiered storage error, we get a fenced error and remove the partition and mark as failed
    fetcher.doWork()
    assertEquals(3, replicaState.logEndOffset)
    assertTrue(isErrorHandled)
    assertTrue(fetcher.fetchState(partition).isEmpty)
    assertTrue(failedPartitions.contains(partition))
  }
}
