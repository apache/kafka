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
import org.apache.kafka.common.errors.{FencedLeaderEpochException, UnknownLeaderEpochException, UnknownTopicIdException}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.internal._
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.common.{KafkaException, TopicPartition, Uuid}
import org.apache.kafka.storage.internals.log.LogAppendInfo
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import kafka.server.FetcherThreadTestUtils.{initialFetchState, mkBatch}
import org.apache.kafka.common.message.{FetchResponseData, OffsetForLeaderEpochRequestData}
import org.apache.kafka.server.log.remote.storage.RetriableRemoteStorageException
import org.apache.kafka.server.{PartitionFetchState, ReplicaState}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Set}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

class AbstractFetcherThreadTest {

  val topicIds = Map("topic1" -> Uuid.randomUuid(), "topic2" -> Uuid.randomUuid())
  val version = ApiKeys.FETCH.latestVersion()
  private val partition1 = new TopicPartition("topic1", 0)
  private val partition2 = new TopicPartition("topic2", 0)
  private val failedPartitions = new FailedPartitions

  @BeforeEach
  def cleanMetricRegistry(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  private def allMetricsNames: Set[String] = KafkaYammerMetrics.defaultRegistry().allMetrics().asScala.keySet.map(_.getName)

  @Test
  def testMetricsRemovedOnShutdown(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    // add one partition to create the consumer lag metric
    fetcher.setReplicaState(partition, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = 0)))
    fetcher.mockLeader.setLeaderState(partition, PartitionState(leaderEpoch = 0))
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.start()

    val brokerTopicStatsMetrics = fetcher.brokerTopicStats.allTopicsStats.metricMapKeySet().asScala
    val fetcherMetrics = Set(FetcherMetrics.BytesPerSec, FetcherMetrics.RequestsPerSec, FetcherMetrics.ConsumerLag)

    // wait until all fetcher metrics are present
    TestUtils.waitUntilTrue(() => allMetricsNames == brokerTopicStatsMetrics ++ fetcherMetrics,
      "Failed waiting for all fetcher metrics to be registered")

    fetcher.shutdown()

    // verify that all the fetcher metrics are removed and only brokerTopicStats left
    val metricNames = KafkaYammerMetrics.defaultRegistry().allMetrics().asScala.keySet.map(_.getName).toSet
    assertTrue(metricNames.intersect(fetcherMetrics).isEmpty)
    assertEquals(brokerTopicStatsMetrics, metricNames.intersect(brokerTopicStatsMetrics))
  }

  @Test
  def testConsumerLagRemovedWithPartition(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    // add one partition to create the consumer lag metric
    fetcher.setReplicaState(partition, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = 0)))
    fetcher.mockLeader.setLeaderState(partition, PartitionState(leaderEpoch = 0))
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()

    assertTrue(allMetricsNames(FetcherMetrics.ConsumerLag),
      "Failed waiting for consumer lag metric")

    // remove the partition to simulate leader migration
    fetcher.removePartitions(Set(partition))

    // the lag metric should now be gone
    assertFalse(allMetricsNames(FetcherMetrics.ConsumerLag))
  }

  @Test
  def testSimpleFetch(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    fetcher.setReplicaState(partition, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()

    val replicaState = fetcher.replicaPartitionState(partition)
    assertEquals(2L, replicaState.logEndOffset)
    assertEquals(2L, replicaState.highWatermark)
  }

  @Test
  def testDelay(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetchBackOffMs = 250

    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version) {
      override def fetch(fetchRequest: FetchRequest.Builder): java.util.Map[TopicPartition, FetchResponseData.PartitionData] = {
        throw new UnknownTopicIdException("Topic ID was unknown as expected for this test")
      }
    }
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, fetchBackOffMs = fetchBackOffMs)

    fetcher.setReplicaState(partition, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(Some(Uuid.randomUuid()), 0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // Do work for the first time. This should result in all partitions in error.
    val timeBeforeFirst = System.currentTimeMillis()
    fetcher.doWork()
    val timeAfterFirst = System.currentTimeMillis()
    val firstWorkDuration = timeAfterFirst - timeBeforeFirst

    // The second doWork will pause for fetchBackOffMs since all partitions will be delayed
    val timeBeforeSecond = System.currentTimeMillis()
    fetcher.doWork()
    val timeAfterSecond = System.currentTimeMillis()
    val secondWorkDuration = timeAfterSecond - timeBeforeSecond

    assertTrue(firstWorkDuration < secondWorkDuration)
    // The second call should have taken more than fetchBackOffMs
    assertTrue(fetchBackOffMs <= secondWorkDuration,
      "secondWorkDuration: " + secondWorkDuration + " was not greater than or equal to fetchBackOffMs: " + fetchBackOffMs)
  }

  @Test
  def testPartitionsInError(): Unit = {
    val partition1 = new TopicPartition("topic1", 0)
    val partition2 = new TopicPartition("topic2", 0)
    val partition3 = new TopicPartition("topic3", 0)
    val fetchBackOffMs = 250

    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version) {
      override def fetch(fetchRequest: FetchRequest.Builder): java.util.Map[TopicPartition, FetchResponseData.PartitionData] = {
        Map(partition1 -> new FetchResponseData.PartitionData().setErrorCode(Errors.UNKNOWN_TOPIC_ID.code),
          partition2 -> new FetchResponseData.PartitionData().setErrorCode(Errors.INCONSISTENT_TOPIC_ID.code),
          partition3 -> new FetchResponseData.PartitionData().setErrorCode(Errors.NONE.code)).asJava
      }
    }
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchBackOffMs = fetchBackOffMs)

    fetcher.setReplicaState(partition1, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition1 -> initialFetchState(Some(Uuid.randomUuid()), 0L, leaderEpoch = 0)))
    fetcher.setReplicaState(partition2, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition2 -> initialFetchState(Some(Uuid.randomUuid()), 0L, leaderEpoch = 0)))
    fetcher.setReplicaState(partition3, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition3 -> initialFetchState(Some(Uuid.randomUuid()), 0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition1, leaderState)
    fetcher.mockLeader.setLeaderState(partition2, leaderState)
    fetcher.mockLeader.setLeaderState(partition3, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()

    val partition1FetchState = fetcher.fetchState(partition1)
    val partition2FetchState = fetcher.fetchState(partition2)
    val partition3FetchState = fetcher.fetchState(partition3)
    assertTrue(partition1FetchState.isDefined)
    assertTrue(partition2FetchState.isDefined)
    assertTrue(partition3FetchState.isDefined)

    // Only the partitions with errors should be delayed.
    assertTrue(partition1FetchState.get.isDelayed)
    assertTrue(partition2FetchState.get.isDelayed)
    assertFalse(partition3FetchState.get.isDelayed)
  }

  @Test
  def testFencedTruncation(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, failedPartitions = failedPartitions)

    fetcher.setReplicaState(partition, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 1,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = PartitionState(Seq(batch), leaderEpoch = 1, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()

    // No progress should be made
    val replicaState = fetcher.replicaPartitionState(partition)
    assertEquals(0L, replicaState.logEndOffset)
    assertEquals(0L, replicaState.highWatermark)

    // After fencing, the fetcher should remove the partition from tracking and mark as failed
    assertTrue(fetcher.fetchState(partition).isEmpty)
    assertTrue(failedPartitions.contains(partition))
  }

  @Test
  def testFencedFetch(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, failedPartitions = failedPartitions)

    val replicaState = PartitionState(leaderEpoch = 0)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes),
      new SimpleRecord("b".getBytes))
    val leaderState = PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()

    // Verify we have caught up
    assertEquals(2, replicaState.logEndOffset)

    // Bump the epoch on the leader
    fetcher.mockLeader.leaderPartitionState(partition).leaderEpoch += 1

    fetcher.doWork()

    // After fencing, the fetcher should remove the partition from tracking and mark as failed
    assertTrue(fetcher.fetchState(partition).isEmpty)
    assertTrue(failedPartitions.contains(partition))
  }

  @Test
  def testUnknownLeaderEpochInTruncation(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, failedPartitions = failedPartitions)

    // The replica's leader epoch is ahead of the leader
    val replicaState = PartitionState(leaderEpoch = 1)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = 1)), forceTruncation = true)

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0, new SimpleRecord("a".getBytes))
    val leaderState = PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()

    // Not data has been fetched and the follower is still truncating
    assertEquals(0, replicaState.logEndOffset)
    assertEquals(Some(ReplicaState.TRUNCATING), fetcher.fetchState(partition).map(_.state))

    // Bump the epoch on the leader
    fetcher.mockLeader.leaderPartitionState(partition).leaderEpoch += 1

    // Now we can make progress
    fetcher.doWork()

    assertEquals(1, replicaState.logEndOffset)
    assertEquals(Some(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
  }

  @Test
  def testUnknownLeaderEpochWhileFetching(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    // This test is contrived because it shouldn't be possible to see unknown leader epoch
    // in the Fetching state as the leader must validate the follower's epoch when it checks
    // the truncation offset.

    val replicaState = PartitionState(leaderEpoch = 1)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = 1)))

    val leaderState = PartitionState(Seq(
      mkBatch(baseOffset = 0L, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1L, leaderEpoch = 0, new SimpleRecord("b".getBytes))
    ), leaderEpoch = 1, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()

    // We have fetched one batch and gotten out of the truncation phase
    assertEquals(1, replicaState.logEndOffset)
    assertEquals(Some(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))

    // Somehow the leader epoch rewinds
    fetcher.mockLeader.leaderPartitionState(partition).leaderEpoch = 0

    // We are stuck at the current offset
    fetcher.doWork()
    assertEquals(1, replicaState.logEndOffset)
    assertEquals(Some(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))

    // After returning to the right epoch, we can continue fetching
    fetcher.mockLeader.leaderPartitionState(partition).leaderEpoch = 1
    fetcher.doWork()
    assertEquals(2, replicaState.logEndOffset)
    assertEquals(Some(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
  }

  @Test
  def testTruncation(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = 5, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = 5)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 1, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 3, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 5, new SimpleRecord("c".getBytes)))

    val leaderState = PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    TestUtils.waitUntilTrue(() => {
      fetcher.doWork()
      fetcher.replicaPartitionState(partition).log == fetcher.mockLeader.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")

    assertEquals(leaderState.logStartOffset, replicaState.logStartOffset)
    assertEquals(leaderState.logEndOffset, replicaState.logEndOffset)
    assertEquals(leaderState.highWatermark, replicaState.highWatermark)
  }

  @Test
  def testTruncateToHighWatermarkIfLeaderEpochInfoNotAvailable(): Unit = {
    val highWatermark = 2L
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version) {
      override def fetchEpochEndOffsets(partitions: java.util.Map[TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition]): java.util.Map[TopicPartition, EpochEndOffset]  =
        throw new UnsupportedOperationException
    }
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine) {
        override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
          assertEquals(highWatermark, truncationState.offset)
          assertTrue(truncationState.truncationCompleted)
          super.truncate(topicPartition, truncationState)
        }

        override def latestEpoch(topicPartition: TopicPartition): Optional[Integer] = Optional.empty
      }

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = 5, highWatermark)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), highWatermark, leaderEpoch = 5)))
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()

    assertEquals(highWatermark, replicaState.logEndOffset)
    assertEquals(highWatermark, fetcher.fetchState(partition).get.fetchOffset)
    assertTrue(fetcher.fetchState(partition).get.isReadyForFetch)
  }

  @Test
  def testTruncateToHighWatermarkDuringRemovePartitions(): Unit = {
    val highWatermark = 2L
    val partition = new TopicPartition("topic", 0)

    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine) {
      override def truncateToHighWatermark(partitions: Set[TopicPartition]): Unit = {
        removePartitions(Set(partition))
        super.truncateToHighWatermark(partitions)
      }

      override def latestEpoch(topicPartition: TopicPartition): Optional[Integer] = Optional.empty
    }

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = 5, highWatermark)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), highWatermark, leaderEpoch = 5)))
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()

    assertEquals(replicaLog.last.nextOffset(), replicaState.logEndOffset)
    assertTrue(fetcher.fetchState(partition).isEmpty)
  }

  @Test
  def testTruncationSkippedIfNoEpochChange(): Unit = {
    val partition = new TopicPartition("topic", 0)

    var truncations = 0
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine) {
      override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
        truncations += 1
        super.truncate(topicPartition, truncationState)
      }
    }

    val replicaState = PartitionState(leaderEpoch = 5)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = 5)), forceTruncation = true)

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 1, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 3, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 5, new SimpleRecord("c".getBytes)))

    val leaderState = PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // Do one round of truncation
    fetcher.doWork()

    // We only fetch one record at a time with mock fetcher
    assertEquals(1, replicaState.logEndOffset)
    assertEquals(1, truncations)

    // Add partitions again with the same epoch
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = 5)))

    // Verify we did not truncate
    fetcher.doWork()

    // No truncations occurred and we have fetched another record
    assertEquals(1, truncations)
    assertEquals(2, replicaState.logEndOffset)
  }

  @Test
  def testTruncationOnFetchSkippedIfPartitionRemoved(): Unit = {
    val partition = new TopicPartition("topic", 0)
    var truncations = 0
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine) {
      override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
        truncations += 1
        super.truncate(topicPartition, truncationState)
      }
    }
    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = 5, highWatermark = 2L)
    fetcher.setReplicaState(partition, replicaState)

    // Verify that truncation based on fetch response is performed if partition is owned by fetcher thread
    fetcher.addPartitions(Map(partition -> initialFetchState(Some(Uuid.randomUuid()), 6L, leaderEpoch = 4)))
    val endOffset = new EpochEndOffset()
      .setPartition(partition.partition)
      .setErrorCode(Errors.NONE.code)
      .setLeaderEpoch(4)
      .setEndOffset(3L)
    fetcher.truncateOnFetchResponse(Map(partition -> endOffset))
    assertEquals(1, truncations)

    // Verify that truncation based on fetch response is not performed if partition is removed from fetcher thread
    val offsets = fetcher.removePartitions(Set(partition))
    assertEquals(Set(partition), offsets.keySet)
    assertEquals(3L, offsets(partition).fetchOffset)
    val newEndOffset = new EpochEndOffset()
      .setPartition(partition.partition)
      .setErrorCode(Errors.NONE.code)
      .setLeaderEpoch(4)
      .setEndOffset(2L)
    fetcher.truncateOnFetchResponse(Map(partition -> newEndOffset))
    assertEquals(1, truncations)
  }

  @Test
  def testFollowerFetchOutOfRangeHigh(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = 4, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = 4)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val leaderState = PartitionState(leaderLog, leaderEpoch = 4, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // initial truncation and verify that the log end offset is updated
    fetcher.doWork()
    assertEquals(3L, replicaState.logEndOffset)
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))

    // To hit this case, we have to change the leader log without going through the truncation phase
    leaderState.log.clear()
    leaderState.logEndOffset = 0L
    leaderState.logStartOffset = 0L
    leaderState.highWatermark = 0L

    fetcher.doWork()

    assertEquals(0L, replicaState.logEndOffset)
    assertEquals(0L, replicaState.logStartOffset)
    assertEquals(0L, replicaState.highWatermark)
  }

  @Test
  def testFencedOffsetResetAfterOutOfRange(): Unit = {
    val partition = new TopicPartition("topic", 0)
    var fetchedEarliestOffset = false

    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version) {
      override def fetchEarliestOffset(topicPartition: TopicPartition, leaderEpoch: Int): OffsetAndEpoch = {
        fetchedEarliestOffset = true
        throw new FencedLeaderEpochException(s"Epoch $leaderEpoch is fenced")
      }

      override def fetchEarliestLocalOffset(topicPartition: TopicPartition, leaderEpoch: Int): OffsetAndEpoch = {
        fetchedEarliestOffset = true
        throw new FencedLeaderEpochException(s"Epoch $leaderEpoch is fenced")
      }
    }
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, failedPartitions = failedPartitions)

    val replicaLog = Seq()
    val replicaState = PartitionState(replicaLog, leaderEpoch = 4, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = 4)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))
    val leaderState = PartitionState(leaderLog, leaderEpoch = 4, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // After the out of range error, we get a fenced error and remove the partition and mark as failed
    fetcher.doWork()
    assertEquals(0, replicaState.logEndOffset)
    assertTrue(fetchedEarliestOffset)
    assertTrue(fetcher.fetchState(partition).isEmpty)
    assertTrue(failedPartitions.contains(partition))
  }

  @Test
  def testFollowerFetchOutOfRangeLow(): Unit = {
    val leaderEpoch = 4
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, failedPartitions = failedPartitions)

    // The follower begins from an offset which is behind the leader's log start offset
    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = leaderEpoch, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(
      Map(
        partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = leaderEpoch)
      )
    )

    val leaderLog = Seq(
      mkBatch(baseOffset = 2, leaderEpoch = leaderEpoch, new SimpleRecord("c".getBytes))
    )

    val leaderState = PartitionState(leaderLog, leaderEpoch = leaderEpoch, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // initial truncation and verify that the log start offset is updated
    fetcher.doWork()
    // Second iteration required here since first iteration is required to
    // perform initial truncation based on diverging epoch.
    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(2, replicaState.logStartOffset)
    assertEquals(List(), replicaState.log.toList)

    TestUtils.waitUntilTrue(() => {
      fetcher.doWork()
      fetcher.replicaPartitionState(partition).log == fetcher.mockLeader.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")

    assertEquals(leaderState.logStartOffset, replicaState.logStartOffset)
    assertEquals(leaderState.logEndOffset, replicaState.logEndOffset)
    assertEquals(leaderState.highWatermark, replicaState.highWatermark)
  }

  @Test
  def testRetryAfterUnknownLeaderEpochInLatestOffsetFetch(): Unit = {
    val leaderEpoch = 4
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version) {
      val tries = new AtomicInteger(0)
      override def fetchLatestOffset(topicPartition: TopicPartition, leaderEpoch: Int): OffsetAndEpoch = {
        if (tries.getAndIncrement() == 0)
          throw new UnknownLeaderEpochException("Unexpected leader epoch")
        super.fetchLatestOffset(topicPartition, leaderEpoch)
      }
    }
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher: MockFetcherThread = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine)

    // The follower begins from an offset which is behind the leader's log start offset
    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes))
    )

    val replicaState = PartitionState(replicaLog, leaderEpoch = leaderEpoch, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = leaderEpoch)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes))
    )

    val leaderState = PartitionState(leaderLog, leaderEpoch = leaderEpoch, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // initial truncation and initial error response handling
    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))

    TestUtils.waitUntilTrue(() => {
      fetcher.doWork()
      fetcher.replicaPartitionState(partition).log == fetcher.mockLeader.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")

    assertEquals(leaderState.logStartOffset, replicaState.logStartOffset)
    assertEquals(leaderState.logEndOffset, replicaState.logEndOffset)
    assertEquals(leaderState.highWatermark, replicaState.highWatermark)
  }

  @Test
  def testReplicateBatchesUpToLeaderEpoch(): Unit = {
    val leaderEpoch = 4
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, failedPartitions = failedPartitions)

    val replicaState = PartitionState(Seq(), leaderEpoch = leaderEpoch, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(
      Map(
        partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = leaderEpoch)
      )
    )

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = leaderEpoch - 1, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = leaderEpoch, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = leaderEpoch + 1, new SimpleRecord("e".getBytes))
    )

    val leaderState = PartitionState(leaderLog, leaderEpoch = leaderEpoch, highWatermark = 0L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(List(), replicaState.log.toList)

    TestUtils.waitUntilTrue(() => {
      fetcher.doWork()
      fetcher.replicaPartitionState(partition).log == fetcher.mockLeader.leaderPartitionState(partition).log.dropRight(1)
    }, "Failed to reconcile leader and follower logs up to the leader epoch")

    assertEquals(leaderState.logStartOffset, replicaState.logStartOffset)
    assertEquals(leaderState.logEndOffset - 1, replicaState.logEndOffset)
    assertEquals(leaderState.highWatermark, replicaState.highWatermark)
  }

  @Test
  def testCorruptMessage(): Unit = {
    val partition = new TopicPartition("topic", 0)

    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version) {
      var fetchedOnce = false
      override def fetch(fetchRequest: FetchRequest.Builder): java.util.Map[TopicPartition, FetchResponseData.PartitionData] = {
        val fetchedData = super.fetch(fetchRequest).asScala
        if (!fetchedOnce) {
          val records = fetchedData.head._2.records.asInstanceOf[MemoryRecords]
          val buffer = records.buffer()
          buffer.putInt(15, buffer.getInt(15) ^ 23422)
          buffer.putInt(30, buffer.getInt(30) ^ 93242)
          fetchedOnce = true
        }
        fetchedData
      }.asJava
    }
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine)

    fetcher.setReplicaState(partition, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = 0)))
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)

    fetcher.doWork() // fails with corrupt record
    fetcher.doWork() // should succeed

    val replicaState = fetcher.replicaPartitionState(partition)
    assertEquals(2L, replicaState.logEndOffset)
  }

  @ParameterizedTest
  @ValueSource(ints = Array(0, 1))
  def testParameterizedLeaderEpochChangeDuringFetchEpochsFromLeader(leaderEpochOnLeader: Int): Unit = {
    // When leaderEpochOnLeader = 1:
    // The leader is on the new epoch when the OffsetsForLeaderEpoch with old epoch is sent, so it
    // returns the fence error. Validate that response is ignored if the leader epoch changes on
    // the follower while OffsetsForLeaderEpoch request is in flight, but able to truncate and fetch
    // in the next of round of "doWork"

    // When leaderEpochOnLeader = 0:
    // The leader is on the old epoch when the OffsetsForLeaderEpoch with old epoch is sent
    // and returns the valid response. Validate that response is ignored if the leader epoch changes
    // on the follower while OffsetsForLeaderEpoch request is in flight, but able to truncate and
    // fetch once the leader is on the newer epoch (same as follower)

    val partition = new TopicPartition("topic", 1)
    val initialLeaderEpochOnFollower = 0
    val nextLeaderEpochOnFollower = initialLeaderEpochOnFollower + 1

    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version) {
      var fetchEpochsFromLeaderOnce = false

      override def fetchEpochEndOffsets(partitions: java.util.Map[TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition]): java.util.Map[TopicPartition, EpochEndOffset] = {
        val fetchedEpochs = super.fetchEpochEndOffsets(partitions)
        if (!fetchEpochsFromLeaderOnce) {
          responseCallback.apply()
          fetchEpochsFromLeaderOnce = true
        }
        fetchedEpochs
      }
    }
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    def changeLeaderEpochWhileFetchEpoch(): Unit = {
      fetcher.removePartitions(Set(partition))
      fetcher.setReplicaState(partition, PartitionState(leaderEpoch = nextLeaderEpochOnFollower))
      fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = nextLeaderEpochOnFollower)), forceTruncation = true)
    }

    fetcher.setReplicaState(partition, PartitionState(leaderEpoch = initialLeaderEpochOnFollower))
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = initialLeaderEpochOnFollower)), forceTruncation = true)

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = initialLeaderEpochOnFollower, new SimpleRecord("c".getBytes)))
    val leaderState = PartitionState(leaderLog, leaderEpochOnLeader, highWatermark = 0L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setResponseCallback(changeLeaderEpochWhileFetchEpoch)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // first round of truncation
    fetcher.doWork()

    // Since leader epoch changed, fetch epochs response is ignored due to partition being in
    // truncating state with the updated leader epoch
    assertEquals(Option(ReplicaState.TRUNCATING), fetcher.fetchState(partition).map(_.state))
    assertEquals(Option(nextLeaderEpochOnFollower), fetcher.fetchState(partition).map(_.currentLeaderEpoch))

    if (leaderEpochOnLeader < nextLeaderEpochOnFollower) {
      fetcher.mockLeader.setLeaderState(
        partition, PartitionState(leaderLog, nextLeaderEpochOnFollower, highWatermark = 0L))
    }

    // make sure the fetcher is now able to truncate and fetch
    fetcher.doWork()
    assertEquals(fetcher.mockLeader.leaderPartitionState(partition).log, fetcher.replicaPartitionState(partition).log)
  }

  @Test
  def testTruncateToEpochEndOffsetsDuringRemovePartitions(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val leaderEpochOnLeader = 0
    val initialLeaderEpochOnFollower = 0
    val nextLeaderEpochOnFollower = initialLeaderEpochOnFollower + 1

    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version) {
      override def fetchEpochEndOffsets(partitions: java.util.Map[TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition]): java.util.Map[TopicPartition, EpochEndOffset]= {
        val fetchedEpochs = super.fetchEpochEndOffsets(partitions)
        responseCallback.apply()
        fetchedEpochs
      }
    }
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    def changeLeaderEpochDuringFetchEpoch(): Unit = {
      // leader epoch changes while fetching epochs from leader
      // at the same time, the replica fetcher manager removes the partition
      fetcher.removePartitions(Set(partition))
      fetcher.setReplicaState(partition, PartitionState(leaderEpoch = nextLeaderEpochOnFollower))
    }

    fetcher.setReplicaState(partition, PartitionState(leaderEpoch = initialLeaderEpochOnFollower))
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = initialLeaderEpochOnFollower)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = initialLeaderEpochOnFollower, new SimpleRecord("c".getBytes)))
    val leaderState = PartitionState(leaderLog, leaderEpochOnLeader, highWatermark = 0L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setResponseCallback(changeLeaderEpochDuringFetchEpoch)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // first round of work
    fetcher.doWork()

    // since the partition was removed before the fetched endOffsets were filtered against the leader epoch,
    // we do not expect the partition to be in Truncating state
    assertEquals(None, fetcher.fetchState(partition).map(_.state))
    assertEquals(None, fetcher.fetchState(partition).map(_.currentLeaderEpoch))

    fetcher.mockLeader.setLeaderState(
      partition, PartitionState(leaderLog, nextLeaderEpochOnFollower, highWatermark = 0L))

    // make sure the fetcher is able to continue work
    fetcher.doWork()
    assertEquals(ArrayBuffer.empty, fetcher.replicaPartitionState(partition).log)
  }

  @Test
  def testTruncationThrowsExceptionIfLeaderReturnsPartitionsNotRequestedInFetchEpochs(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version) {
      override def fetchEpochEndOffsets(partitions: java.util.Map[TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition]): java.util.Map[TopicPartition, EpochEndOffset] = {
        val unrequestedTp = new TopicPartition("topic2", 0)
        super.fetchEpochEndOffsets(partitions).asScala + (unrequestedTp -> new EpochEndOffset()
          .setPartition(unrequestedTp.partition)
          .setErrorCode(Errors.NONE.code)
          .setLeaderEpoch(0)
          .setEndOffset(0))
      }.asJava
    }
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine)

    fetcher.setReplicaState(partition, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = 0)), forceTruncation = true)
    fetcher.mockLeader.setLeaderState(partition, PartitionState(leaderEpoch = 0))
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // first round of truncation should throw an exception
    assertThrows(classOf[IllegalStateException], () => fetcher.doWork())
  }

  @Test
  def testFetcherThreadHandlingPartitionFailureDuringAppending(): Unit = {
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcherForAppend = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, failedPartitions = failedPartitions) {
      override def processPartitionData(
        topicPartition: TopicPartition,
        fetchOffset: Long,
        partitionLeaderEpoch: Int,
        partitionData: FetchData
      ): Option[LogAppendInfo] = {
        if (topicPartition == partition1) {
          throw new KafkaException()
        } else {
          super.processPartitionData(topicPartition, fetchOffset, partitionLeaderEpoch, partitionData)
        }
      }
    }
    verifyFetcherThreadHandlingPartitionFailure(fetcherForAppend)
  }

  @Test
  def testFetcherThreadHandlingPartitionFailureDuringTruncation(): Unit = {
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcherForTruncation = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, failedPartitions = failedPartitions) {
      override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
        if (topicPartition == partition1)
          throw new Exception()
        else {
          super.truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState)
        }
      }
    }
    verifyFetcherThreadHandlingPartitionFailure(fetcherForTruncation)
  }

  private def verifyFetcherThreadHandlingPartitionFailure(fetcher: MockFetcherThread): Unit = {

    fetcher.setReplicaState(partition1, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition1 -> initialFetchState(topicIds.get(partition1.topic), 0L, leaderEpoch = 0)), forceTruncation = true)
    fetcher.mockLeader.setLeaderState(partition1, PartitionState(leaderEpoch = 0))

    fetcher.setReplicaState(partition2, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition2 -> initialFetchState(topicIds.get(partition2.topic), 0L, leaderEpoch = 0)), forceTruncation = true)
    fetcher.mockLeader.setLeaderState(partition2, PartitionState(leaderEpoch = 0))
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // processing data fails for partition1
    fetcher.doWork()

    // partition1 marked as failed
    assertTrue(failedPartitions.contains(partition1))
    assertEquals(None, fetcher.fetchState(partition1))

    // make sure the fetcher continues to work with rest of the partitions
    fetcher.doWork()
    assertEquals(Some(ReplicaState.FETCHING), fetcher.fetchState(partition2).map(_.state))
    assertFalse(failedPartitions.contains(partition2))

    // simulate a leader change
    fetcher.removePartitions(Set(partition1))
    failedPartitions.removeAll(Set(partition1))
    fetcher.addPartitions(Map(partition1 -> initialFetchState(topicIds.get(partition1.topic), 0L, leaderEpoch = 1)), forceTruncation = true)

    // partition1 added back
    assertEquals(Some(ReplicaState.TRUNCATING), fetcher.fetchState(partition1).map(_.state))
    assertFalse(failedPartitions.contains(partition1))

  }

  @Test
  def testDivergingEpochs(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = 5, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = 5)))
    assertEquals(3L, replicaState.logEndOffset)
    fetcher.verifyLastFetchedEpoch(partition, expectedEpoch = Some(4))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 5, new SimpleRecord("d".getBytes)))

    val leaderState = PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    fetcher.verifyLastFetchedEpoch(partition, Some(2))

    TestUtils.waitUntilTrue(() => {
      fetcher.doWork()
      fetcher.replicaPartitionState(partition).log == fetcher.mockLeader.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")
    fetcher.verifyLastFetchedEpoch(partition, Some(5))
  }

  @Test
  def testTruncateOnFetchDoesNotProcessPartitionData(): Unit = {
    val partition = new TopicPartition("topic", 0)
    var truncateCalls = 0
    var processPartitionDataCalls = 0
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine) {
      override def processPartitionData(
        topicPartition: TopicPartition,
        fetchOffset: Long,
        partitionLeaderEpoch: Int,
        partitionData: FetchData
      ): Option[LogAppendInfo] = {
        processPartitionDataCalls += 1
        super.processPartitionData(topicPartition, fetchOffset, partitionLeaderEpoch, partitionData)
      }

      override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
        truncateCalls += 1
        super.truncate(topicPartition, truncationState)
      }
    }

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 0, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 2, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 3, leaderEpoch = 4, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 4, leaderEpoch = 4, new SimpleRecord("e".getBytes)),
      mkBatch(baseOffset = 5, leaderEpoch = 4, new SimpleRecord("f".getBytes)),
    )

    val replicaState = PartitionState(replicaLog, leaderEpoch = 5, highWatermark = 1L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = 5)))
    assertEquals(6L, replicaState.logEndOffset)
    fetcher.verifyLastFetchedEpoch(partition, expectedEpoch = Some(4))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 0, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 2, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 3, leaderEpoch = 5, new SimpleRecord("g".getBytes)),
      mkBatch(baseOffset = 4, leaderEpoch = 5, new SimpleRecord("h".getBytes)),
    )

    val leaderState = PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 4L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // The first fetch should result in truncating the follower's log and
    // it should not process the data hence not update the high watermarks.
    fetcher.doWork()

    assertEquals(1, truncateCalls)
    assertEquals(0, processPartitionDataCalls)
    assertEquals(3L, replicaState.logEndOffset)
    assertEquals(1L, replicaState.highWatermark)

    // Truncate should have been called only once and process partition data
    // should have been called at least once. The log end offset and the high
    // watermark are updated.
    TestUtils.waitUntilTrue(() => {
      fetcher.doWork()
      fetcher.replicaPartitionState(partition).log == fetcher.mockLeader.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")
    fetcher.verifyLastFetchedEpoch(partition, Some(5))

    assertEquals(1, truncateCalls)
    assertTrue(processPartitionDataCalls >= 1)
    assertEquals(5L, replicaState.logEndOffset)
    assertEquals(4L, replicaState.highWatermark)
  }

  @Test
  def testMaybeUpdateTopicIds(): Unit = {
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    // Start with no topic IDs
    fetcher.setReplicaState(partition, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(None, 0L, leaderEpoch = 0)))

    def verifyFetchState(fetchState: Option[PartitionFetchState], expectedTopicId: Option[Uuid]): Unit = {
      assertTrue(fetchState.isDefined)
      assertEquals(expectedTopicId, fetchState.get.topicId.toScala)
    }

    verifyFetchState(fetcher.fetchState(partition), None)

    // Add topic ID
    fetcher.maybeUpdateTopicIds(Set(partition), topicName => topicIds.get(topicName))
    verifyFetchState(fetcher.fetchState(partition), topicIds.get(partition.topic))

    // Try to update topic ID for non-existent topic partition
    val unknownPartition = new TopicPartition("unknown", 0)
    fetcher.maybeUpdateTopicIds(Set(unknownPartition), topicName => topicIds.get(topicName))
    assertTrue(fetcher.fetchState(unknownPartition).isEmpty)
  }

  @Test
  def testIgnoreFetchResponseWhenLeaderEpochChanged(): Unit = {
    val newEpoch = 1
    val initEpoch = 0

    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)
    val replicaState = PartitionState(leaderEpoch = newEpoch)
    fetcher.setReplicaState(partition, replicaState)
    val initFetchState = initialFetchState(topicIds.get(partition.topic), 0L, leaderEpoch = newEpoch)
    fetcher.addPartitions(Map(partition -> initFetchState))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = initEpoch, new SimpleRecord("a".getBytes))
    val leaderState = PartitionState(Seq(batch), leaderEpoch = initEpoch, highWatermark = 1L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)

    val partitionData = Map(partition -> new FetchRequest.PartitionData(Uuid.randomUuid(), 0, 0, 1048576, Optional.of(initEpoch), Optional.of(initEpoch))).asJava
    val fetchRequestOpt = FetchRequest.Builder.forReplica(0, 0, initEpoch, 0, Int.MaxValue, partitionData)

    fetcher.processFetchRequest(partitionData, fetchRequestOpt)
    assertEquals(0, replicaState.logEndOffset, "FetchResponse should be ignored when leader epoch does not match")
  }

  private def emptyReplicaState(rlmEnabled: Boolean, partition: TopicPartition, fetcher: MockFetcherThread): PartitionState = {
    // Follower begins with an empty log
    val replicaState = PartitionState(Seq(), leaderEpoch = 0, highWatermark = 0L, rlmEnabled = rlmEnabled)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), fetchOffset = 0, leaderEpoch = 0)))
    replicaState
  }

  /**
   * Test: Empty Follower Fetch with TieredStorage Disabled and Leader LogStartOffset = 0
   *
   * Purpose:
   * - Simulate a leader with logs starting at offset 0 and validate how the follower
   *   behaves when TieredStorage is disabled.
   *
   * Conditions:
   * - TieredStorage: **Disabled**
   * - Leader LogStartOffset: **0**
   *
   * Scenario:
   * - The leader starts with a log at offset 0, containing three record batches offset at 0, 150, and 199.
   * - The follower begins fetching, and we validate the correctness of its replica state as it fetches.
   *
   * Expected Outcomes:
   * 1. The follower fetch state should transition to `FETCHING` initially.
   * 2. After the first poll, one record batch is fetched.
   * 3. After subsequent polls, the entire leader log is fetched:
   *    - Replica log size: 3
   *    - Replica LogStartOffset: 0
   *    - Replica LogEndOffset: 200
   *    - Replica HighWatermark: 199
   */
  @Test
  def testEmptyFollowerFetchTieredStorageDisabledLeaderLogStartOffsetZero(): Unit = {
    val rlmEnabled = false
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)

    val leaderLog = Seq(
      // LogStartOffset = LocalLogStartOffset = 0
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )

    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(1, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(1, replicaState.logEndOffset)
    assertEquals(Some(1), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with TieredStorage Disabled and Leader LogStartOffset != 0
   *
   * Purpose:
   * - Validate follower behavior when the leader's log starts at a non-zero offset (10).
   *
   * Conditions:
   * - TieredStorage: **Disabled**
   * - Leader LogStartOffset: **10**
   *
   * Scenario:
   * - The leader log starts at offset 10 with batches at 10, 150, and 199.
   * - The follower starts fetching from offset 10.
   *
   * Expected Outcomes:
   * 1. The follower's initial log is empty.
   * 2. Replica offsets after polls:
   *    - LogStartOffset = 10
   *    - LogEndOffset = 200
   *    - HighWatermark = 199
   */
  @Test
  def testEmptyFollowerFetchTieredStorageDisabledLeaderLogStartOffsetNonZero(): Unit = {
    val rlmEnabled = false
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)

    val leaderLog = Seq(
      // LogStartOffset = LocalLogStartOffset = 10
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )

    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    // Follower gets out-of-range error (no messages received), fetch offset is updated from 0 to 10
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(Some(10), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with TieredStorage Enabled, Leader LogStartOffset = 0, and No Local Deletions
   *
   * Purpose:
   * - Simulate TieredStorage enabled and validate follower fetching behavior when the leader
   *   log starts at 0 and no segments have been uploaded or deleted locally.
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - Leader LogStartOffset: **0**
   * - Leader LocalLogStartOffset: **0** (No local segments deleted).
   *
   * Scenario:
   * - The leader log contains three record batches at offsets 0, 150, and 199.
   * - The follower starts fetching from offset 0.
   *
   * Expected Outcomes:
   * 1. The replica log accurately reflects the leader's log:
   *    - LogStartOffset = 0
   *    - LocalLogStartOffset = 0
   *    - LogEndOffset = 200
   *    - HighWatermark = 199
   */
  @Test
  def testEmptyFollowerFetchTieredStorageEnabledLeaderLogStartOffsetZeroNoLocalDeletions(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)

    val leaderLog = Seq(
      // LogStartOffset = LocalLogStartOffset = 0
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )

    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(1, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(0, replicaState.localLogStartOffset)
    assertEquals(1, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
    assertEquals(Some(1), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(0, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with TieredStorage Enabled, Leader LogStartOffset = 0, and Local Deletions
   *
   * Purpose:
   * - Simulate TieredStorage enabled with some segments uploaded and deleted locally, causing
   *   a difference between the leader's LogStartOffset (0) and LocalLogStartOffset (> 0).
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - Leader LogStartOffset: **0**
   * - Leader LocalLogStartOffset: **100** (Some segments deleted locally).
   *
   * Scenario:
   * - The leader log starts at offset 0 but the local leader log starts at offset 100.
   * - The follower fetch operation begins from offset 0.
   *
   * Expected Outcomes:
   * 1. After offset adjustments for local deletions:
   *    - LogStartOffset = 0
   *    - LocalLogStartOffset = 100
   *    - LogEndOffset = 200
   *    - HighWatermark = 199
   */
  @Test
  def testEmptyFollowerFetchTieredStorageEnabledLeaderLogStartOffsetZeroWithLocalDeletions(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)

    val leaderLog = Seq(
      // LocalLogStartOffset = 100
      mkBatch(baseOffset = 100, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )

    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled
    )
    leaderState.logStartOffset = 0
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(100, replicaState.logEndOffset)
    assertEquals(Some(100), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with TieredStorage Enabled, Leader LogStartOffset != 0, and No Local Deletions
   *
   * Purpose:
   * - Simulate TieredStorage enabled and validate follower fetch behavior when the leader's log
   *   starts at a non-zero offset and no local deletions have occurred.
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - Leader LogStartOffset: **10**
   * - Leader LocalLogStartOffset: **10** (No deletions).
   *
   * Scenario:
   * - The leader log starts at offset 10 with batches at 10, 150, and 199.
   * - The follower starts fetching from offset 10.
   *
   * Expected Outcomes:
   * 1. After fetching, the replica log matches the leader:
   *    - LogStartOffset = 10
   *    - LocalLogStartOffset = 10
   *    - LogEndOffset = 200
   *    - HighWatermark = 199
   */
  @Test
  def testEmptyFollowerFetchTieredStorageEnabledLeaderLogStartOffsetNonZeroNoLocalDeletions(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)

    val leaderLog = Seq(
      // LogStartOffset = LocalLogStartOffset = 10
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )

    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(Some(10), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with TieredStorage Enabled, Leader LogStartOffset != 0, and Local Deletions
   *
   * Purpose:
   * - Validate follower adjustments when the leader has log deletions causing
   *   LocalLogStartOffset > LogStartOffset.
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - Leader LogStartOffset: **10**
   * - Leader LocalLogStartOffset: **100** (All older segments deleted locally).
   *
   * Scenario:
   * - The leader log starts at offset 10 but the local log starts at offset 100.
   * - The follower fetch starts at offset 10 but adjusts for local deletions.
   *
   * Expected Outcomes:
   * 1. Initial fetch offset adjustments:
   *    - First adjustment: LogEndOffset = 10 (after offset-out-of-range error)
   *    - Second adjustment: LogEndOffset = 100 (after offset-moved-to-tiered-storage error)
   * 2. After successful fetches:
   *    - 3 record batches fetched
   *    - LogStartOffset = 10
   *    - LocalLogStartOffset = 100
   *    - LogEndOffset = 200
   *    - HighWatermark = 199
   */
  @Test
  def testEmptyFollowerFetchTieredStorageEnabledLeaderLogStartOffsetNonZeroWithLocalDeletions(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)

    val leaderLog = Seq(
      // LocalLogStartOffset = 100
      mkBatch(baseOffset = 100, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )

    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
    )
    leaderState.logStartOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    // On offset-out-of-range error, fetch offset is updated
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(Some(10), fetcher.fetchState(partition).map(_.fetchOffset()))

    fetcher.doWork()
    // On offset-moved-to-tiered-storage error, fetch offset is updated
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(100, replicaState.logEndOffset)
    assertEquals(Some(100), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with TieredStorage Enabled, All Local Segments Deleted
   *
   * Purpose:
   * - Handle scenarios where all local segments have been deleted:
   *   - LocalLogStartOffset > LogStartOffset.
   *   - LocalLogStartOffset = LogEndOffset.
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - Leader LogStartOffset: **0 or > 0**
   * - Leader LocalLogStartOffset: Leader LogEndOffset (all segments deleted locally).
   *
   * Expected Outcomes:
   * 1. Follower state is adjusted to reflect local deletions:
   *    - LocalLogStartOffset = LogEndOffset.
   *    - No new data remains to fetch.
   */
  @Test
  def testEmptyFollowerFetchTieredStorageEnabledLeaderLogStartOffsetZeroAllLocalSegmentsDeleted(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)

    val leaderLog = Seq(
      // LocalLogStartOffset = 100
      mkBatch(baseOffset = 100, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
    )

    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 151L,
      rlmEnabled = rlmEnabled
    )
    leaderState.logStartOffset = 0
    // Set Local Log Start Offset to Log End Offset
    leaderState.localLogStartOffset = 151
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()

    // On offset-moved-to-tiered-storage error, fetch offset is updated
    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(151, replicaState.localLogStartOffset)
    assertEquals(151, replicaState.logEndOffset)
    assertEquals(151, replicaState.highWatermark)
    assertEquals(Some(151), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Call once again to see if new data is received
    fetcher.doWork()
    // No metadata update expected
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(151, replicaState.localLogStartOffset)
    assertEquals(151, replicaState.logEndOffset)
    assertEquals(151, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with TieredStorage Enabled, Leader LogStartOffset != 0, and All Local Segments Deleted
   *
   * Purpose:
   * - Validate follower behavior when TieredStorage is enabled, the leader's log starts at a non-zero offset,
   *   and all local log segments have been deleted.
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - Leader LogStartOffset: **10**
   * - Leader LocalLogStartOffset: **151** (all older segments deleted locally).
   *
   * Scenario:
   * - The leader log contains record batches from offset 100, but all local segments up to offset 151 are deleted.
   * - The follower starts at LogStartOffset = 10 and adjusts for local segment deletions.
   *
   * Expected Outcomes:
   * 1. Follower detects offset adjustments due to local deletions:
   *    - LogStartOffset remains 10.
   *    - LocalLogStartOffset updates to 151.
   *    - LogEndOffset updates to 151.
   * 2. HighWatermark aligns with the leader (151).
   * 3. No new data is fetched since all relevant segments are deleted.
   */
  @Test
  def testEmptyFollowerFetchTieredStorageEnabledLeaderLogStartOffsetNonZeroAllLocalSegmentsDeleted(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)

    val leaderLog = Seq(
      // LocalLogStartOffset = 100
      mkBatch(baseOffset = 100, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
    )

    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 151L,
      rlmEnabled = rlmEnabled
    )
    leaderState.logStartOffset = 10
    // Set Local Log Start Offset to Log End Offset
    leaderState.localLogStartOffset = 151
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()

    // On offset-out-of-range error, fetch offset is updated
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(Some(10), fetcher.fetchState(partition).map(_.fetchOffset()))

    // On offset-moved-to-tiered-storage error, fetch offset is updated
    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(151, replicaState.localLogStartOffset)
    assertEquals(151, replicaState.logEndOffset)
    assertEquals(151, replicaState.highWatermark)
    assertEquals(Some(151), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Call once again to see if new data is received
    fetcher.doWork()
    // No metadata update expected
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(151, replicaState.localLogStartOffset)
    assertEquals(151, replicaState.logEndOffset)
    assertEquals(151, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Empty Leader Log
   *
   * Purpose:
   * - Validate follower fetch behavior when fetching from last tiered offset with an empty leader log
   * - Test scenario: Leader has no data and no segments uploaded
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader log: **Empty**
   * - earliestPendingUploadOffset: **-1** (no upload information available)
   * - Leader LogStartOffset: **0**
   * - Leader LocalLogStartOffset: **0**
   *
   * Scenario:
   * - The leader log is completely empty with no records
   * - No segments have been uploaded to remote storage
   * - The follower attempts to fetch from the last tiered offset
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. All offsets remain at 0:
   *    - logStartOffset = 0
   *    - localLogStartOffset = 0
   *    - logEndOffset = 0
   *    - highWatermark = 0
   * 3. No data is fetched (log size = 0)
   */
  @Test
  def testLastTieredOffsetWithEmptyLeaderLog(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    // Empty Logs
    val leaderLog = Seq()
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 0L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = -1
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(0, replicaState.localLogStartOffset)
    assertEquals(0, replicaState.logEndOffset)
    assertEquals(0, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with No Segments Uploaded
   *
   * Purpose:
   * - Validate follower fetch behavior when leader has local data but no segments uploaded to remote storage
   * - Test scenario: Leader has 3 record batches locally, but upload to tiered storage hasn't started yet
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader log: **3 batches** at offsets 0, 150, 199
   * - Leader LogStartOffset: **0**
   * - Leader LocalLogStartOffset: **0**
   * - earliestPendingUploadOffset: **-1** (no segments uploaded yet)
   *
   * Scenario:
   * - The leader has local data (3 record batches)
   * - No segments have been uploaded to remote storage yet (earliestPendingUploadOffset = -1)
   * - The follower starts from offset 0 and fetches the local data
   * - Since there's no gap between logStartOffset and where local log starts, fetching proceeds normally
   *
   * Expected Outcomes:
   * 1. Follower successfully fetches from offset 0
   * 2. After 3 doWork() calls, all 3 batches are fetched:
   *    - log size = 3
   *    - logStartOffset = 0
   *    - localLogStartOffset = 0
   *    - logEndOffset = 200
   *    - highWatermark = 199
   */
  @Test
  def testLastTieredOffsetWithNoSegmentsUploaded(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      // LogStartOffset = LocalLogStartOffset = 0
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = -1
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(1, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(0, replicaState.localLogStartOffset)
    assertEquals(1, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll, so calling 'n' number of times to get the desired result
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(0, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Partial Upload on New Leader (Gap Scenario)
   *
   * Purpose:
   * - Validate follower fetch behavior when new leader doesn't know upload state and there's a gap
   * - Test scenario: Gap between logStartOffset (0) and where local log actually starts (10)
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **0**
   * - Leader LocalLogStartOffset: **10** (local log starts at offset 10, creating a gap)
   * - earliestPendingUploadOffset: **-1** (new leader doesn't know about previous uploads)
   * - Leader has 3 batches at offsets 10, 150, 199
   *
   * Scenario:
   * - A new leader takes over that has local log starting at offset 10
   * - The logStartOffset is 0, but local log doesn't have data before offset 10
   * - This creates a gap: logStartOffset (0) != where local log starts (10)
   * - New leader doesn't have upload information (earliestPendingUploadOffset = -1)
   * - Follower cannot safely determine where to fetch from
   *
   * Expected Outcomes:
   * 1. Follower does NOT fetch immediately (gap scenario with unknown upload state)
   * 2. Fetch state shows delayed retry:
   *    - fetchOffset = 0 (unchanged)
   *    - lag is empty
   *    - delay is present (will retry after delay)
   * 3. LogEndOffset remains 0
   */
  @Test
  def testLastTieredOffsetWithPartialUploadOnNewLeader(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      // LocalLogStartOffset = 10
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = -1
    )
    leaderState.logStartOffset = 0
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    val fetchStateOpt = fetcher.fetchState(partition)
    assertTrue(fetchStateOpt.nonEmpty)
    assertEquals(ReplicaState.FETCHING, fetchStateOpt.get.state)
    // Fetch offset remains unchanged
    assertEquals(0, fetchStateOpt.get.fetchOffset)
    // Lag remains unchanged
    assertTrue(fetchStateOpt.get.lag.isEmpty)
    assertEquals(0, fetchStateOpt.get.currentLeaderEpoch)
    // Fetch will be retried after some delay
    assertTrue(fetchStateOpt.get.delay.isPresent)
    assertEquals(Optional.of(0), fetchStateOpt.get.lastFetchedEpoch)

    // LogEndOffset is unchanged
    assertEquals(0, replicaState.logEndOffset)
  }

  /**
   * Test: Last Tiered Offset Fetch with Slow Partial Upload
   *
   * Purpose:
   * - Validate follower fetch behavior when upload to tiered storage is in progress but slow
   * - Test scenario: Upload has just started, with earliestPendingUploadOffset matching local log start
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **0**
   * - Leader LocalLogStartOffset: **10** (local log starts at offset 10)
   * - earliestPendingUploadOffset: **10** (upload just started at offset 10)
   * - Leader has 3 batches at offsets 10, 150, 199
   *
   * Scenario:
   * - Leader has uploaded segments before offset 10 to remote storage
   * - Local log starts at offset 10 (earlier segments are in remote storage)
   * - Upload is in progress with earliestPendingUploadOffset = 10 (slow upload - no progress yet)
   * - Follower uses lastTieredOffset logic to determine fetch start point
   * - Since earliestPendingUploadOffset is provided, follower can safely fetch from offset 10
   *
   * Expected Outcomes:
   * 1. Follower starts fetching from offset 10 (lastTieredOffset logic)
   * 2. First doWork() call:
   *    - localLogStartOffset = 10
   *    - logEndOffset = 10
   *    - fetchOffset = 10
   * 3. After 3 additional doWork() calls, all data from offset 10 onwards is fetched:
   *    - log size = 3
   *    - logStartOffset = 0
   *    - localLogStartOffset = 10
   *    - logEndOffset = 200
   *    - highWatermark = 199
   */
  @Test
  def testLastTieredOffsetWithSlowPartialUpload(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      // LocalLogStartOffset = 10
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 10
    )
    leaderState.logStartOffset = 0
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(Some(10), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Fast Partial Upload
   *
   * Purpose:
   * - Validate follower fetch behavior when upload to tiered storage has made significant progress
   * - Test scenario: Upload has progressed to offset 150, follower fetches only remaining data
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **0**
   * - Leader LocalLogStartOffset: **10** (local log starts at offset 10)
   * - earliestPendingUploadOffset: **150** (upload has progressed to offset 150 - fast upload)
   * - Leader has 3 batches at offsets 10, 150, 199
   *
   * Scenario:
   * - Leader has uploaded segments up to offset 150 to remote storage
   * - Local log starts at offset 10, but segments from 10-149 are uploaded
   * - earliestPendingUploadOffset = 150 indicates upload has made significant progress
   * - Follower uses lastTieredOffset logic to start from offset 150
   * - Only remaining batches (150, 199) need to be fetched locally
   *
   * Expected Outcomes:
   * 1. Follower starts fetching from offset 150 (lastTieredOffset logic)
   * 2. First doWork() call:
   *    - localLogStartOffset = 150
   *    - logEndOffset = 150
   *    - fetchOffset = 150
   * 3. After 2 additional doWork() calls, only remaining batches are fetched:
   *    - log size = 2 (only batches at 150 and 199)
   *    - logStartOffset = 0
   *    - localLogStartOffset = 150
   *    - logEndOffset = 200
   *    - highWatermark = 199
   */
  @Test
  def testLastTieredOffsetWithFastPartialUpload(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      // LocalLogStartOffset = 10
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 150
    )
    leaderState.logStartOffset = 0
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(150, replicaState.logEndOffset)
    assertEquals(Some(150), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(2, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Full Upload Completed
   *
   * Purpose:
   * - Validate follower fetch behavior when all segments have been uploaded to remote storage
   * - Test scenario: Leader has completed uploading all data, no local data remains to be fetched
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **0**
   * - Leader LocalLogStartOffset: **10** (local log starts at offset 10)
   * - earliestPendingUploadOffset: **200** (all segments uploaded - matches logEndOffset)
   * - Leader has 3 batches at offsets 10, 150, 199
   *
   * Scenario:
   * - Leader has uploaded all segments to remote storage
   * - earliestPendingUploadOffset = 200 indicates upload has completed up to logEndOffset
   * - Local log starts at offset 10, but all data from 10-199 is already uploaded
   * - Follower uses lastTieredOffset logic to determine it should start from offset 200
   * - Since logEndOffset = earliestPendingUploadOffset = 200, there's no local data to fetch
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. Follower starts at offset 200 (end of uploaded data):
   *    - localLogStartOffset = 200
   *    - logEndOffset = 200
   *    - fetchOffset = 200
   * 3. No batches are fetched (all data already uploaded):
   *    - log size = 0
   *    - logStartOffset = 0
   *    - highWatermark = 200
   */
  @Test
  def testLastTieredOffsetWithFullUploadCompleted(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      // LocalLogStartOffset = 10
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 200
    )
    leaderState.logStartOffset = 0
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(200, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(200, replicaState.highWatermark)
    assertEquals(Some(200), fetcher.fetchState(partition).map(_.fetchOffset()))
  }

  /**
   * Test: Last Tiered Offset Fetch with Inactive Leader and Full Upload on New Leader
   *
   * Purpose:
   * - Validate follower fetch behavior when new leader has empty log with all segments uploaded
   * - Test scenario: New leader doesn't know upload state, creating a gap scenario
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader log: **Empty** (inactive leader - no local data)
   * - Leader LogStartOffset: **0**
   * - Leader LocalLogStartOffset: **200** (all local segments uploaded/deleted)
   * - Leader LogEndOffset: **200**
   * - earliestPendingUploadOffset: **-1** (new leader doesn't know about previous uploads)
   *
   * Scenario:
   * - New leader takes over with empty local log
   * - All data (0-199) has been uploaded to remote storage and deleted locally
   * - logStartOffset = 0, but localLogStartOffset = 200 (creating a gap)
   * - New leader doesn't have upload information (earliestPendingUploadOffset = -1)
   * - Follower cannot safely determine where to fetch from due to gap and unknown upload state
   *
   * Expected Outcomes:
   * 1. Follower does NOT fetch immediately (gap scenario with unknown upload state)
   * 2. Fetch state shows delayed retry:
   *    - fetchOffset = 0 (unchanged)
   *    - lag is empty
   *    - delay is present (will retry after delay)
   *    - state = FETCHING
   *    - currentLeaderEpoch = 0
   *    - lastFetchedEpoch = 0
   * 3. LogEndOffset remains 0 (no progress until upload state known)
   */
  @Test
  def testLastTieredOffsetWithInactiveLeaderFullUploadOnNewLeader(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    // Empty logs
    val leaderLog = Seq()
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = -1
    )
    leaderState.logStartOffset = 0
    leaderState.localLogStartOffset = 200
    leaderState.logEndOffset = 200
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    val fetchStateOpt = fetcher.fetchState(partition)
    assertTrue(fetchStateOpt.nonEmpty)
    assertEquals(ReplicaState.FETCHING, fetchStateOpt.get.state)
    // Fetch offset remains unchanged
    assertEquals(0, fetchStateOpt.get.fetchOffset)
    // Lag remains unchanged
    assertTrue(fetchStateOpt.get.lag.isEmpty)
    assertEquals(0, fetchStateOpt.get.currentLeaderEpoch)
    // Fetch will be retried after some delay
    assertTrue(fetchStateOpt.get.delay.isPresent)
    assertEquals(Optional.of(0), fetchStateOpt.get.lastFetchedEpoch)

    // LogEndOffset is unchanged
    assertEquals(0, replicaState.logEndOffset)
  }

  /**
   * Test: Last Tiered Offset Fetch with Inactive Leader and Full Upload Completed
   *
   * Purpose:
   * - Validate follower fetch behavior when leader has empty log with known full upload
   * - Test scenario: Leader has empty log but knows all data is uploaded (earliestPendingUploadOffset = 200)
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader log: **Empty** (inactive leader - no local data)
   * - Leader LogStartOffset: **0**
   * - Leader LocalLogStartOffset: **200** (all local segments uploaded/deleted)
   * - Leader LogEndOffset: **200**
   * - earliestPendingUploadOffset: **200** (full upload completed - leader knows upload state)
   *
   * Scenario:
   * - Leader has empty local log (all data uploaded and deleted locally)
   * - All data (0-199) has been uploaded to remote storage
   * - earliestPendingUploadOffset = 200 indicates leader knows about the completed upload
   * - Follower uses lastTieredOffset logic to start from offset 200
   * - Since all data is uploaded, no local fetching is needed
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. Follower adjusts to upload endpoint:
   *    - localLogStartOffset = 200
   *    - logEndOffset = 200
   *    - fetchOffset = 200
   *    - highWatermark = 200
   * 3. No batches are fetched (all data already uploaded):
   *    - log size = 0
   *    - logStartOffset = 0
   */
  @Test
  def testLastTieredOffsetWithInactiveLeaderFullUpload(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    // Empty logs
    val leaderLog = Seq()
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 200
    )
    leaderState.logStartOffset = 0
    leaderState.localLogStartOffset = 200
    leaderState.logEndOffset = 200
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(200, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(200, replicaState.highWatermark)
    assertEquals(Some(200), fetcher.fetchState(partition).map(_.fetchOffset()))
  }

  /**
   * Test: Last Tiered Offset Fetch with Empty Leader Log, Non-Zero LSO on New Leader
   *
   * Purpose:
   * - Validate follower fetch behavior when leader has empty log with non-zero logStartOffset
   * - Test scenario: Leader has no data with non-zero start offset, new leader scenario
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader log: **Empty** (no data)
   * - Leader LogStartOffset: **10** (non-zero - segments 0-9 previously deleted)
   * - Leader LocalLogStartOffset: **10** (same as logStartOffset - no gap)
   * - Leader LogEndOffset: **10**
   * - earliestPendingUploadOffset: **-1** (new leader doesn't know upload state)
   *
   * Scenario:
   * - Leader has empty log with logStartOffset = 10 (segments 0-9 were previously deleted/uploaded)
   * - No gap exists (logStartOffset = localLogStartOffset = 10)
   * - New leader doesn't have upload information (earliestPendingUploadOffset = -1)
   * - Since there's no gap, follower can safely fetch from logStartOffset
   * - Leader log is empty (logEndOffset = logStartOffset = 10), so no data to fetch
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts to logStartOffset:
   *    - localLogStartOffset = 10
   *    - logEndOffset = 10
   *    - highWatermark = 10
   *    - fetchOffset = 10
   * 3. Second doWork() call confirms stable state:
   *    - logStartOffset = 10
   *    - localLogStartOffset = 10
   *    - logEndOffset = 10
   *    - highWatermark = 10
   * 4. No batches are fetched (log is empty):
   *    - log size = 0
   */
  @Test
  def testLastTieredOffsetWithEmptyLeaderLogNonZeroLSOOnNewLeader(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    // Empty logs
    val leaderLog = Seq()
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 10L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = -1
    )
    leaderState.logStartOffset = 10
    leaderState.localLogStartOffset = 10
    leaderState.logEndOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(10, replicaState.highWatermark)
    assertEquals(Some(10), fetcher.fetchState(partition).map(_.fetchOffset()))

    fetcher.doWork()
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(10, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Empty Leader Log and Stale Upload Offset
   *
   * Purpose:
   * - Validate follower fetch behavior when leader has empty log with stale upload offset
   * - Test scenario: Stale upload offset is ignored in favor of logStartOffset
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader log: **Empty** (no data)
   * - Leader LogStartOffset: **10** (non-zero - segments 0-9 previously deleted)
   * - Leader LocalLogStartOffset: **10** (same as logStartOffset - no gap)
   * - Leader LogEndOffset: **10**
   * - earliestPendingUploadOffset: **5** (stale - before logStartOffset of 10)
   *
   * Scenario:
   * - Leader has empty log with logStartOffset = 10 (segments 0-9 previously deleted/uploaded)
   * - earliestPendingUploadOffset = 5 is stale (before logStartOffset of 10)
   * - Despite stale upload offset, presence of upload offset indicates leader knows about tiered storage
   * - Follower ignores stale value and uses logStartOffset (10) to determine fetch point
   * - Since logEndOffset = logStartOffset = 10, log is empty and no data to fetch
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts to logStartOffset:
   *    - localLogStartOffset = 10
   *    - logEndOffset = 10
   *    - highWatermark = 10
   *    - fetchOffset = 10
   * 3. Second doWork() call confirms stable state:
   *    - logStartOffset = 10
   *    - localLogStartOffset = 10
   *    - logEndOffset = 10
   *    - highWatermark = 10
   * 4. No batches are fetched (log is empty):
   *    - log size = 0
   */
  @Test
  def testLastTieredOffsetWithEmptyLeaderLogStaleUploadOffset(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    // Empty logs
    val leaderLog = Seq()
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 10L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 5
    )
    leaderState.logStartOffset = 10
    leaderState.localLogStartOffset = 10
    leaderState.logEndOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(10, replicaState.highWatermark)
    assertEquals(Some(10), fetcher.fetchState(partition).map(_.fetchOffset()))

    fetcher.doWork()
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(10, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Non-Zero LSO on New Leader
   *
   * Purpose:
   * - Validate follower fetch behavior when leader has non-zero logStartOffset without upload info
   * - Test scenario: New leader with data but no upload information
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **10** (same as logStartOffset - no deletions)
   * - earliestPendingUploadOffset: **-1** (new leader doesn't know upload state)
   * - Leader has 3 batches at offsets 10, 150, 199
   *
   * Scenario:
   * - Leader log starts at offset 10 with local data available
   * - No gap exists (logStartOffset = localLogStartOffset = 10)
   * - New leader doesn't have upload information (earliestPendingUploadOffset = -1)
   * - Since there's no gap, follower can safely fetch from logStartOffset
   * - Follower calculates lag correctly as difference between logEndOffset and fetchOffset
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. Follower starts at logStartOffset:
   *    - fetchOffset = 10
   *    - lag is calculated correctly (190 = 200 - 10)
   *    - state = FETCHING
   *    - currentLeaderEpoch = 0
   *    - lastFetchedEpoch = 0
   * 3. LogEndOffset updates to logStartOffset:
   *    - logEndOffset = 10
   */
  @Test
  def testLastTieredOffsetWithNonZeroLSOOnNewLeader(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      // LogStartOffset = LocalLogStartOffset = 10
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = -1
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    val fetchStateOpt = fetcher.fetchState(partition)
    assertTrue(fetchStateOpt.nonEmpty)
    assertEquals(ReplicaState.FETCHING, fetchStateOpt.get.state)
    assertEquals(10, fetchStateOpt.get.fetchOffset)
    assertFalse(fetchStateOpt.get.lag.isEmpty)
    assertEquals(190, fetchStateOpt.get.lag().get())
    assertEquals(0, fetchStateOpt.get.currentLeaderEpoch)
    assertEquals(Optional.of(0), fetchStateOpt.get.lastFetchedEpoch)
    assertEquals(10, replicaState.logEndOffset)
  }

  /**
   * Test: Last Tiered Offset Fetch with Non-Zero LSO and Stale Upload Offset
   *
   * Purpose:
   * - Validate follower fetch behavior when leader has stale upload offset with non-zero LSO
   * - Test scenario: Stale upload offset is ignored, follower fetches from logStartOffset
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **10** (same as logStartOffset - no deletions)
   * - earliestPendingUploadOffset: **5** (stale - before logStartOffset of 10)
   * - Leader has 3 batches at offsets 10, 150, 199
   *
   * Scenario:
   * - Leader log starts at offset 10 with local data available
   * - earliestPendingUploadOffset = 5 is stale (before logStartOffset of 10)
   * - Despite stale upload offset, presence of upload offset indicates leader knows about tiered storage
   * - Follower ignores stale value and uses logStartOffset (10) to determine fetch point
   * - Since logStartOffset = localLogStartOffset = 10 (no gap), follower can fetch from offset 10
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts to logStartOffset:
   *    - localLogStartOffset = 10
   *    - logEndOffset = 10
   *    - highWatermark = 10
   *    - fetchOffset = 10
   * 3. After 3 additional doWork() calls, all 3 batches are fetched:
   *    - log size = 3
   *    - logStartOffset = 10
   *    - localLogStartOffset = 10
   *    - logEndOffset = 200
   *    - highWatermark = 199
   */
  @Test
  def testLastTieredOffsetWithNonZeroLSOStaleUploadOffset(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      // LogStartOffset = LocalLogStartOffset = 10
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 5
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(10, replicaState.highWatermark)
    assertEquals(Some(10), fetcher.fetchState(partition).map(_.fetchOffset()))

    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Slow Upload and No Local Deletion
   *
   * Purpose:
   * - Validate follower fetch behavior when upload is in progress (slow) with no local deletions
   * - Test scenario: Upload has just started, follower fetches from upload point
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **10** (same as logStartOffset - no deletions)
   * - earliestPendingUploadOffset: **10** (slow upload - just started at offset 10)
   * - Leader has 3 batches at offsets 10, 150, 199
   *
   * Scenario:
   * - Leader log starts at offset 10 with local data available
   * - Upload has just started with earliestPendingUploadOffset = 10 (slow upload - no progress yet)
   * - Follower uses lastTieredOffset logic to determine fetch point
   * - Since earliestPendingUploadOffset is provided, follower can safely fetch from offset 10
   * - No segments have been uploaded yet (upload just starting)
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts to upload point:
   *    - localLogStartOffset = 10
   *    - logEndOffset = 10
   *    - fetchOffset = 10
   * 3. After 3 additional doWork() calls, all 3 batches are fetched:
   *    - log size = 3
   *    - logStartOffset = 10
   *    - localLogStartOffset = 10
   *    - logEndOffset = 200
   *    - highWatermark = 199
   */
  @Test
  def testLastTieredOffsetWithSlowUploadNoLocalDeletion(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      // LogStartOffset = LocalLogStartOffset = 10
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 10
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(Some(10), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Fast Upload and No Local Deletion
   *
   * Purpose:
   * - Validate follower fetch behavior when upload has made significant progress (fast)
   * - Test scenario: Upload has progressed significantly, follower fetches only remaining data
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **10** (same as logStartOffset - no deletions)
   * - earliestPendingUploadOffset: **150** (fast upload - significant progress to offset 150)
   * - Leader has 3 batches at offsets 10, 150, 199
   *
   * Scenario:
   * - Leader log starts at offset 10 with local data available
   * - Upload has progressed to offset 150 (fast upload - batch at offset 10 already uploaded)
   * - Follower uses lastTieredOffset logic to start from earliestPendingUploadOffset
   * - Only remaining batches (150, 199) need to be fetched locally
   * - Batch at offset 10 is already uploaded to remote storage
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts to upload point:
   *    - localLogStartOffset = 150
   *    - logEndOffset = 150
   *    - fetchOffset = 150
   * 3. After 2 additional doWork() calls, only remaining batches are fetched:
   *    - log size = 2 (only batches at 150 and 199)
   *    - logStartOffset = 10
   *    - localLogStartOffset = 150
   *    - logEndOffset = 200
   *    - highWatermark = 199
   */
  @Test
  def testLastTieredOffsetWithFastUploadNoLocalDeletion(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      // LogStartOffset = LocalLogStartOffset = 10
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 150
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(150, replicaState.logEndOffset)
    assertEquals(Some(150), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(2, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Full Upload and No Local Deletions
   *
   * Purpose:
   * - Validate follower fetch behavior when all segments are uploaded and no local deletions occurred
   * - Test scenario: Leader log starts at non-zero offset with full upload completed
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **10** (no local deletions - same as logStartOffset)
   * - earliestPendingUploadOffset: **200** (full upload completed - all data uploaded)
   * - Leader has 3 batches at offsets 10, 150, 199
   *
   * Scenario:
   * - Leader log starts at offset 10 (non-zero)
   * - No local deletions have occurred (logStartOffset = localLogStartOffset = 10)
   * - All segments have been uploaded to remote storage (earliestPendingUploadOffset = 200)
   * - Follower uses lastTieredOffset logic to start from offset 200
   * - Since all data is uploaded, no local fetching is needed
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts offsets to upload endpoint:
   *    - localLogStartOffset = 200
   *    - logEndOffset = 200
   *    - fetchOffset = 200
   * 3. Second doWork() call updates logStartOffset:
   *    - logStartOffset = 10 (leader's logStartOffset)
   *    - localLogStartOffset = 200
   *    - logEndOffset = 200
   *    - highWatermark = 199
   * 4. No batches are fetched (all data already uploaded):
   *    - log size = 0
   */
  @Test
  def testLastTieredOffsetWithFullUploadNoLocalDeletion(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      // LogStartOffset = LocalLogStartOffset = 10
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 200
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(200, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(Some(200), fetcher.fetchState(partition).map(_.fetchOffset()))

    fetcher.doWork()
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(200, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Partial Upload on New Leader (Non-Zero LSO, Gap Scenario)
   *
   * Purpose:
   * - Validate follower fetch behavior when new leader doesn't know upload state with non-zero log start
   * - Test scenario: Gap between logStartOffset (10) and where local log actually starts (100)
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10**
   * - Leader LocalLogStartOffset: **100** (local log starts at offset 100, creating a gap)
   * - earliestPendingUploadOffset: **-1** (new leader doesn't know about previous uploads)
   * - Leader has 3 batches at offsets 100, 150, 199
   *
   * Scenario:
   * - A new leader takes over that has local log starting at offset 100
   * - The logStartOffset is 10, but local log doesn't have data before offset 100
   * - This creates a gap: logStartOffset (10) != where local log starts (100)
   * - New leader doesn't have upload information (earliestPendingUploadOffset = -1)
   * - Follower cannot safely determine where to fetch from
   *
   * Expected Outcomes:
   * 1. Follower does NOT fetch immediately (gap scenario with unknown upload state)
   * 2. Fetch state shows delayed retry:
   *    - fetchOffset = 0 (unchanged)
   *    - lag is empty
   *    - delay is present (will retry after delay)
   * 3. LogEndOffset remains 0
   */
  @Test
  def testLastTieredOffsetWithPartialUploadOnNewLeaderNonZeroLSO(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      mkBatch(baseOffset = 100, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = -1
    )
    leaderState.logStartOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    val fetchStateOpt = fetcher.fetchState(partition)
    assertTrue(fetchStateOpt.nonEmpty)
    assertEquals(ReplicaState.FETCHING, fetchStateOpt.get.state)
    assertEquals(0, fetchStateOpt.get.fetchOffset)
    assertTrue(fetchStateOpt.get.lag.isEmpty)
    assertEquals(0, fetchStateOpt.get.currentLeaderEpoch)
    assertEquals(0, fetchStateOpt.get.currentLeaderEpoch)
    // Fetch will be retried after some delay
    assertTrue(fetchStateOpt.get.delay.isPresent)
    assertEquals(Optional.of(0), fetchStateOpt.get.lastFetchedEpoch)

    // LogEndOffset is unchanged
    assertEquals(0, replicaState.logEndOffset)
  }

  /**
   * Test: Last Tiered Offset Fetch with Partial Upload and Stale Upload Offset
   *
   * Purpose:
   * - Validate follower fetch behavior when earliestPendingUploadOffset is stale (before logStartOffset)
   * - Test scenario: Upload offset is stale but local log starts at offset 100, follower can still fetch
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10**
   * - Leader LocalLogStartOffset: **100** (local log starts at offset 100)
   * - earliestPendingUploadOffset: **5** (stale - before logStartOffset of 10)
   * - Leader has 3 batches at offsets 100, 150, 199
   *
   * Scenario:
   * - Leader has earliestPendingUploadOffset = 5, which is stale (before logStartOffset of 10)
   * - Despite stale upload offset, local log is available starting at offset 100
   * - Follower ignores the stale upload offset and uses localLogStartOffset (100) to start fetching
   * - The presence of a (even stale) upload offset indicates the leader knows about tiered storage
   *
   * Expected Outcomes:
   * 1. Follower successfully fetches from offset 100 (ignores stale upload offset)
   * 2. First doWork() call:
   *    - localLogStartOffset = 100
   *    - logEndOffset = 100
   *    - fetchOffset = 100
   * 3. After 3 additional doWork() calls, all 3 batches from offset 100 onwards are fetched:
   *    - log size = 3
   *    - logStartOffset = 10
   *    - localLogStartOffset = 100
   *    - logEndOffset = 200
   *    - highWatermark = 199
   */
  @Test
  def testLastTieredOffsetWithPartialUploadAndStaleUploadOffset(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      mkBatch(baseOffset = 100, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 5
    )
    leaderState.logStartOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(100, replicaState.logEndOffset)
    assertEquals(100, replicaState.highWatermark)
    assertEquals(Some(100), fetcher.fetchState(partition).map(_.fetchOffset()))

    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Slow Upload and Non-Zero LSO
   *
   * Purpose:
   * - Validate follower fetch behavior when upload is slow with non-zero logStartOffset and local deletions
   * - Test scenario: Upload just started at local log start point
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **100** (local deletions - local log starts at offset 100)
   * - earliestPendingUploadOffset: **100** (slow upload - just started at offset 100)
   * - Leader has 3 batches at offsets 100, 150, 199
   *
   * Scenario:
   * - Leader log starts at offset 10 (segments 0-9 previously deleted)
   * - Local log starts at offset 100 due to local deletions (segments 10-99 deleted locally)
   * - Upload has just started with earliestPendingUploadOffset = 100 (slow upload - no progress yet)
   * - Follower uses lastTieredOffset logic to start from earliestPendingUploadOffset
   * - All local data (100-199) needs to be fetched
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts to upload point:
   *    - localLogStartOffset = 100
   *    - logEndOffset = 100
   *    - fetchOffset = 100
   * 3. After 3 additional doWork() calls, all 3 batches are fetched:
   *    - log size = 3
   *    - logStartOffset = 10
   *    - localLogStartOffset = 100
   *    - logEndOffset = 200
   *    - highWatermark = 199
   */
  @Test
  def testLastTieredOffsetWithSlowUploadNonZeroLSO(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      mkBatch(baseOffset = 100, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 100
    )
    leaderState.logStartOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(100, replicaState.logEndOffset)
    assertEquals(Some(100), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Fast Upload and Non-Zero LSO
   *
   * Purpose:
   * - Validate follower fetch behavior when upload has made significant progress with non-zero LSO
   * - Test scenario: Fast upload progress with non-zero logStartOffset
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **100** (local deletions - local log starts at offset 100)
   * - earliestPendingUploadOffset: **150** (fast upload - progressed to offset 150)
   * - Leader has 3 batches at offsets 100, 150, 199
   *
   * Scenario:
   * - Leader log starts at offset 10 (segments 0-9 previously deleted)
   * - Local log starts at offset 100 due to local deletions (segments 10-99 deleted locally)
   * - Upload has progressed to offset 150 (fast upload - batch at offset 100 already uploaded)
   * - Follower uses lastTieredOffset logic to start from earliestPendingUploadOffset
   * - Only remaining batches (150, 199) need to be fetched locally
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts to upload point:
   *    - localLogStartOffset = 150
   *    - logEndOffset = 150
   *    - fetchOffset = 150
   * 3. After 2 additional doWork() calls, only remaining batches are fetched:
   *    - log size = 2 (only batches at 150 and 199)
   *    - logStartOffset = 10
   *    - localLogStartOffset = 150
   *    - logEndOffset = 200
   *    - highWatermark = 199
   */
  @Test
  def testLastTieredOffsetWithFastUploadNonZeroLSO(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      mkBatch(baseOffset = 100, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 150
    )
    leaderState.logStartOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(150, replicaState.logEndOffset)
    assertEquals(Some(150), fetcher.fetchState(partition).map(_.fetchOffset()))

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(2, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Full Upload and Non-Zero Local Start Offset
   *
   * Purpose:
   * - Validate follower fetch behavior when full upload is completed with local deletions
   * - Test scenario: Leader has non-zero logStartOffset with local log starting at higher offset
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **100** (local deletions - local log starts at offset 100)
   * - earliestPendingUploadOffset: **200** (full upload completed - all data uploaded)
   * - Leader has 3 batches at offsets 100, 150, 199
   *
   * Scenario:
   * - Leader log starts at offset 10 (non-zero)
   * - Local log starts at offset 100 due to local deletions (segments 10-99 deleted locally but in remote)
   * - All segments have been uploaded to remote storage (earliestPendingUploadOffset = 200)
   * - Follower uses lastTieredOffset logic to start from offset 200
   * - Since all data is uploaded (even the locally deleted segments 10-99), no local fetching needed
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts offsets to upload endpoint:
   *    - localLogStartOffset = 200
   *    - logEndOffset = 200
   *    - fetchOffset = 200
   * 3. Second doWork() call updates logStartOffset:
   *    - logStartOffset = 10 (leader's logStartOffset)
   *    - localLogStartOffset = 200
   *    - logEndOffset = 200
   *    - highWatermark = 199
   * 4. No batches are fetched (all data already uploaded):
   *    - log size = 0
   */
  @Test
  def testLastTieredOffsetWithFullUploadNonZeroLSO(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      mkBatch(baseOffset = 100, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 200
    )
    leaderState.logStartOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(200, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(Some(200), fetcher.fetchState(partition).map(_.fetchOffset()))

    fetcher.doWork()
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(200, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Inactive Leader, Full Upload, Non-Zero LSO on New Leader
   *
   * Purpose:
   * - Validate follower fetch behavior when new leader has empty log with non-zero logStartOffset
   * - Test scenario: New leader doesn't know upload state with gap scenario
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader log: **Empty** (inactive leader - no local data)
   * - Leader LogStartOffset: **10** (non-zero - segments 0-9 previously deleted)
   * - Leader LocalLogStartOffset: **200** (all local segments uploaded/deleted)
   * - Leader LogEndOffset: **200**
   * - earliestPendingUploadOffset: **-1** (new leader doesn't know about previous uploads)
   *
   * Scenario:
   * - New leader takes over with empty local log
   * - logStartOffset = 10 (segments 0-9 were previously deleted)
   * - All remaining data (10-199) has been uploaded to remote storage and deleted locally
   * - localLogStartOffset = 200 (creating a gap: 10 != 200)
   * - New leader doesn't have upload information (earliestPendingUploadOffset = -1)
   * - Follower cannot safely determine where to fetch from due to gap and unknown upload state
   *
   * Expected Outcomes:
   * 1. Follower does NOT fetch immediately (gap scenario with unknown upload state)
   * 2. Fetch state shows delayed retry:
   *    - fetchOffset = 0 (unchanged)
   *    - lag is empty
   *    - delay is present (will retry after delay)
   *    - state = FETCHING
   *    - currentLeaderEpoch = 0
   *    - lastFetchedEpoch = 0
   * 3. LogEndOffset remains 0 (no progress until upload state known)
   */
  @Test
  def testLastTieredOffsetWithInactiveLeaderFullUploadNonZeroLSOOnNewLeader(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    // Empty logs
    val leaderLog = Seq()
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = -1
    )
    leaderState.logStartOffset = 10
    leaderState.localLogStartOffset = 200
    leaderState.logEndOffset = 200
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    val fetchStateOpt = fetcher.fetchState(partition)
    assertTrue(fetchStateOpt.nonEmpty)
    assertEquals(ReplicaState.FETCHING, fetchStateOpt.get.state)
    assertEquals(0, fetchStateOpt.get.fetchOffset)
    assertTrue(fetchStateOpt.get.lag.isEmpty)
    assertEquals(0, fetchStateOpt.get.currentLeaderEpoch)
    // Fetch will be retried after some delay
    assertTrue(fetchStateOpt.get.delay.isPresent)
    assertEquals(Optional.of(0), fetchStateOpt.get.lastFetchedEpoch)

    // LogEndOffset is unchanged
    assertEquals(0, replicaState.logEndOffset)
  }

  /**
   * Test: Last Tiered Offset Fetch with Inactive Leader, Stale Upload Offset, Non-Zero LSO
   *
   * Purpose:
   * - Validate follower fetch behavior when leader has empty log with stale upload offset
   * - Test scenario: Leader has stale upload offset but follower can still determine fetch point
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader log: **Empty** (inactive leader - no local data)
   * - Leader LogStartOffset: **10** (non-zero - segments 0-9 previously deleted)
   * - Leader LocalLogStartOffset: **200** (all local segments uploaded/deleted)
   * - Leader LogEndOffset: **200**
   * - earliestPendingUploadOffset: **5** (stale - before logStartOffset of 10)
   *
   * Scenario:
   * - Leader has empty local log (all data uploaded and deleted locally)
   * - earliestPendingUploadOffset = 5 is stale (before logStartOffset of 10)
   * - Despite stale upload offset, presence of upload offset indicates leader knows about tiered storage
   * - Follower ignores stale value and uses localLogStartOffset (200) to determine fetch point
   * - Since localLogStartOffset = logEndOffset = 200, all data is uploaded
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts to localLogStartOffset:
   *    - localLogStartOffset = 200
   *    - logEndOffset = 200
   *    - highWatermark = 200
   * 3. Second doWork() call updates logStartOffset:
   *    - logStartOffset = 10 (leader's logStartOffset)
   *    - localLogStartOffset = 200
   *    - logEndOffset = 200
   *    - highWatermark = 199
   * 4. No batches are fetched (all data already uploaded):
   *    - log size = 0
   */
  @Test
  def testLastTieredOffsetWithInactiveLeaderStaleUploadNonZeroLSO(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    // Empty logs
    val leaderLog = Seq()
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 5
    )
    leaderState.logStartOffset = 10
    leaderState.localLogStartOffset = 200
    leaderState.logEndOffset = 200
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(200, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(200, replicaState.highWatermark)

    fetcher.doWork()
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(200, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Inactive Leader, Full Upload, Non-Zero LSO
   *
   * Purpose:
   * - Validate follower fetch behavior when leader has empty log with known full upload and non-zero LSO
   * - Test scenario: Leader has empty log but knows all data is uploaded with non-zero logStartOffset
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader log: **Empty** (inactive leader - no local data)
   * - Leader LogStartOffset: **10** (non-zero - segments 0-9 previously deleted)
   * - Leader LocalLogStartOffset: **200** (all local segments uploaded/deleted)
   * - Leader LogEndOffset: **200**
   * - earliestPendingUploadOffset: **200** (full upload completed - leader knows upload state)
   *
   * Scenario:
   * - Leader has empty local log (all data uploaded and deleted locally)
   * - logStartOffset = 10 (segments 0-9 were previously deleted)
   * - All remaining data (10-199) has been uploaded to remote storage
   * - earliestPendingUploadOffset = 200 indicates leader knows about the completed upload
   * - Follower uses lastTieredOffset logic to start from offset 200
   * - Since all data is uploaded, no local fetching is needed
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts to upload endpoint:
   *    - localLogStartOffset = 200
   *    - logEndOffset = 200
   *    - fetchOffset = 200
   * 3. Second doWork() call updates logStartOffset:
   *    - logStartOffset = 10 (leader's logStartOffset)
   *    - localLogStartOffset = 200
   *    - logEndOffset = 200
   *    - highWatermark = 199
   * 4. No batches are fetched (all data already uploaded):
   *    - log size = 0
   */
  @Test
  def testLastTieredOffsetWithInactiveLeaderFullUploadNonZeroLSO(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    // Empty logs
    val leaderLog = Seq()
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 200
    )
    leaderState.logStartOffset = 10
    leaderState.localLogStartOffset = 200
    leaderState.logEndOffset = 200
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(200, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(Some(200), fetcher.fetchState(partition).map(_.fetchOffset()))

    fetcher.doWork()
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(200, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Stale Local Log Offset on New Leader
   *
   * Purpose:
   * - Validate follower fetch behavior when local log offset is stale (before logStartOffset)
   * - Test scenario: New leader with stale local log offset creates gap scenario
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **5** (stale - before logStartOffset, creating inconsistency)
   * - earliestPendingUploadOffset: **-1** (new leader doesn't know upload state)
   * - Leader has 3 batches at offsets 5, 10, 199
   *
   * Scenario:
   * - Leader has local log starting at offset 5 (before logStartOffset of 10)
   * - This creates a stale/inconsistent state where localLogStartOffset < logStartOffset
   * - New leader doesn't have upload information (earliestPendingUploadOffset = -1)
   * - Follower cannot safely determine where to fetch from due to gap and unknown upload state
   * - This is an unusual scenario where local log offset hasn't been updated properly
   *
   * Expected Outcomes:
   * 1. Follower does NOT fetch immediately (gap scenario with unknown upload state)
   * 2. Fetch state shows delayed retry:
   *    - fetchOffset = 0 (unchanged)
   *    - lag is empty
   *    - delay is present (will retry after delay)
   *    - state = FETCHING
   *    - currentLeaderEpoch = 0
   *    - lastFetchedEpoch = 0
   * 3. LogEndOffset remains 0 (no progress until upload state known)
   */
  @Test
  def testLastTieredOffsetStaleLocalLogOffsetNewLeader(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      mkBatch(baseOffset = 5, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = -1
    )
    leaderState.logStartOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    val fetchStateOpt = fetcher.fetchState(partition)
    assertTrue(fetchStateOpt.nonEmpty)
    assertEquals(ReplicaState.FETCHING, fetchStateOpt.get.state)
    assertEquals(0, fetchStateOpt.get.fetchOffset)
    assertTrue(fetchStateOpt.get.lag.isEmpty)
    assertEquals(0, fetchStateOpt.get.currentLeaderEpoch)
    assertEquals(0, fetchStateOpt.get.currentLeaderEpoch)
    // Fetch will be retried after some delay
    assertTrue(fetchStateOpt.get.delay.isPresent)
    assertEquals(Optional.of(0), fetchStateOpt.get.lastFetchedEpoch)

    // LogEndOffset is unchanged
    assertEquals(0, replicaState.logEndOffset)
  }

  /**
   * Test: Last Tiered Offset Fetch with Stale Local Log Offset and Slow Upload
   *
   * Purpose:
   * - Validate follower fetch behavior when local log offset is stale but upload info is available
   * - Test scenario: Stale local log offset with known upload state allows fetching
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **5** (stale - before logStartOffset)
   * - earliestPendingUploadOffset: **10** (slow upload - just started at offset 10)
   * - Leader has 3 batches at offsets 5, 10, 199
   *
   * Scenario:
   * - Leader has stale local log offset (5 < 10)
   * - Upload has just started with earliestPendingUploadOffset = 10 (slow upload)
   * - Despite stale local log offset, presence of upload offset indicates leader knows about tiered storage
   * - Follower uses earliestPendingUploadOffset (10) to determine fetch point
   * - Follower can safely fetch from offset 10 onwards
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts to upload point:
   *    - localLogStartOffset = 10
   *    - logEndOffset = 10
   *    - fetchOffset = 10
   * 3. After 2 additional doWork() calls, 2 batches are fetched:
   *    - log size = 2 (batches at 10 and 199)
   *    - logStartOffset = 10
   *    - localLogStartOffset = 10
   *    - logEndOffset = 200
   *    - highWatermark = 199
   */
  @Test
  def testLastTieredOffsetStaleLocalLogOffsetSlowUpload():Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      mkBatch(baseOffset = 5, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 10
    )
    leaderState.logStartOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(Some(10), fetcher.fetchState(partition).map(_.fetchOffset()))

    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(2, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Stale Local Log Offset and Fast Upload
   *
   * Purpose:
   * - Validate follower fetch behavior when local log offset is stale with significant upload progress
   * - Test scenario: Stale local log offset with fast upload allows fetching from upload point
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **5** (stale - before logStartOffset)
   * - earliestPendingUploadOffset: **150** (fast upload - progressed to offset 150)
   * - Leader has 4 batches at offsets 5, 10, 150, 199
   *
   * Scenario:
   * - Leader has stale local log offset (5 < 10)
   * - Upload has progressed to offset 150 (fast upload - batches at 5 and 10 already uploaded)
   * - Despite stale local log offset, presence of upload offset indicates leader knows about tiered storage
   * - Follower uses earliestPendingUploadOffset (150) to determine fetch point
   * - Only remaining batches (150, 199) need to be fetched locally
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts to upload point:
   *    - localLogStartOffset = 150
   *    - logEndOffset = 150
   *    - fetchOffset = 150
   * 3. After 2 additional doWork() calls, only remaining batches are fetched:
   *    - log size = 2 (only batches at 150 and 199)
   *    - logStartOffset = 10
   *    - localLogStartOffset = 150
   *    - logEndOffset = 200
   *    - highWatermark = 199
   */
  @Test
  def testLastTieredOffsetStaleLocalLogOffsetFastUpload(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      mkBatch(baseOffset = 5, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("e".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("f".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 150
    )
    leaderState.logStartOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(150, replicaState.logEndOffset)
    assertEquals(Some(150), fetcher.fetchState(partition).map(_.fetchOffset()))

    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(2, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Stale Local Log Offset and Full Upload
   *
   * Purpose:
   * - Validate follower fetch behavior when local log offset is stale with full upload completed
   * - Test scenario: Stale local log offset with all data uploaded to remote storage
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **5** (stale - before logStartOffset)
   * - earliestPendingUploadOffset: **200** (full upload completed - all data uploaded to remote)
   * - Leader has 3 batches at offsets 5, 10, 199
   *
   * Scenario:
   * - Leader has stale local log offset (5 < 10)
   * - All data has been uploaded to remote storage (earliestPendingUploadOffset = 200)
   * - Despite stale local log offset, presence of upload offset indicates leader knows about tiered storage
   * - Follower uses earliestPendingUploadOffset (200) to determine fetch point
   * - Since all data is uploaded (logEndOffset = earliestPendingUploadOffset = 200), no local fetching needed
   *
   * Expected Outcomes:
   * 1. Follower fetch state transitions to FETCHING
   * 2. First doWork() call adjusts to upload endpoint (200, not logStartOffset):
   *    - localLogStartOffset = 200
   *    - logEndOffset = 200
   *    - fetchOffset = 200
   * 3. Second doWork() call updates logStartOffset:
   *    - logStartOffset = 10 (leader's logStartOffset)
   *    - localLogStartOffset = 200
   *    - logEndOffset = 200
   *    - highWatermark = 199
   * 4. No batches are fetched (all data already uploaded):
   *    - log size = 0
   */
  @Test
  def testLastTieredOffsetStaleLocalLogOffsetFullUpload(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0);
    val mockLeaderEndPoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      mkBatch(baseOffset = 5, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )
    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 200
    )
    leaderState.logStartOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(200, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(Some(200), fetcher.fetchState(partition).map(_.fetchOffset()))

    fetcher.doWork()
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(200, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Last Tiered Offset Fetch with Retriable Remote Storage Exception
   *
   * Purpose:
   * - Validate follower error handling when tier state machine throws retriable remote storage exception
   * - Test scenario: Retriable exception during tier state machine start should trigger retry, not failure
   *
   * Conditions:
   * - TieredStorage: **Enabled**
   * - fetchFromLastTieredOffset: **true**
   * - Leader LogStartOffset: **10** (leader log starts at offset 10)
   * - Leader LocalLogStartOffset: **10** (same as logStartOffset - no deletions)
   * - earliestPendingUploadOffset: **150** (fast upload - progressed to offset 150)
   * - Leader has 3 batches at offsets 10, 150, 199
   * - Mock tier state machine throws RetriableRemoteStorageException during start()
   *
   * Scenario:
   * - Follower attempts to fetch from last tiered offset
   * - Tier state machine start() throws RetriableRemoteStorageException (simulating temporary remote storage issue)
   * - Exception is retriable, so follower should enter delayed retry mode, NOT mark partition as failed
   * - Follower will retry the operation after a delay
   * - This tests proper error handling for transient remote storage failures
   *
   * Expected Outcomes:
   * 1. Partition is NOT marked as failed:
   *    - assertFalse(failedPartitions.contains(partition))
   * 2. Follower enters delayed retry mode:
   *    - fetchOffset = 0 (unchanged - no progress made)
   *    - lag is empty (not calculated due to error)
   *    - delay is present (will retry after delay)
   *    - state = FETCHING (stays in FETCHING state)
   *    - currentLeaderEpoch = 0
   *    - lastFetchedEpoch = 0
   * 3. LogEndOffset remains unchanged:
   *    - logEndOffset = 0 (no progress until retry succeeds)
   */
  @Test
  def testLastTieredOffsetRetryableRemoteStorageException(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint) {
      override def start(topicPartition: TopicPartition,
                         topicId: Optional[Uuid],
                         currentLeaderEpoch: Int,
                         fetchStartOffsetAndEpoch: OffsetAndEpoch,
                         leaderLogStartOffset: Long): PartitionFetchState = {
        throw new RetriableRemoteStorageException("Retryable exception")
      }
    }
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)
    val leaderLog = Seq(
      // LogStartOffset = LocalLogStartOffset = 10
      mkBatch(baseOffset = 10, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )

    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 150
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    // Should not be marked as failed
    assertFalse(failedPartitions.contains(partition))

    val fetchStateOpt = fetcher.fetchState(partition)
    assertTrue(fetchStateOpt.nonEmpty)
    assertEquals(ReplicaState.FETCHING, fetchStateOpt.get.state)
    // Fetch offset remains unchanged
    assertEquals(0, fetchStateOpt.get.fetchOffset)
    // Lag remains unchanged
    assertTrue(fetchStateOpt.get.lag.isEmpty)
    assertEquals(0, fetchStateOpt.get.currentLeaderEpoch)
    // Fetch will be retried after some delay
    assertTrue(fetchStateOpt.get.delay.isPresent)
    assertEquals(Optional.of(0), fetchStateOpt.get.lastFetchedEpoch)

    // LogEndOffset is unchanged
    assertEquals(0, replicaState.logEndOffset)
  }
}