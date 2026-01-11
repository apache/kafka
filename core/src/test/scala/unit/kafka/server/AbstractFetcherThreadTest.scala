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
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.common.{KafkaException, TopicPartition, Uuid}
import org.apache.kafka.storage.internals.log.{LogAppendInfo, UnifiedLog}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Disabled, Test}
import kafka.server.FetcherThreadTestUtils.{initialFetchState, mkBatch}
import org.apache.kafka.common.message.{FetchResponseData, OffsetForLeaderEpochRequestData}
import org.apache.kafka.server.log.remote.storage.RetriableRemoteStorageException
import org.apache.kafka.server.{PartitionFetchState, ReplicaState}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.lang
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
   * Test: Empty Follower Fetch with data replication starting from Last Tiered Offset, Leader LogStartOffset = 0, and
   * All Local Segments Retained
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset, the leader's log starts at offset zero,
   *   and all local log segments are retained.
   *
   * Conditions:
   *  - TieredStorage: **Enabled**
   *  - Leader LogStartOffset: **0**
   *  - Leader LocalLogStartOffset: **0** (all segments retained locally)
   *  - Should replica from the last tiered offset: **True**
   *
   * Scenario:
   *  - The leader log contains record batches starting from offset 0
   *  - Some segments are uploaded to tiered storage (with earliestPendingUploadOffset = 150)
   *  - The follower starts with an empty log and starts data replication from the last tiered offset
   *
   * Expected Outcomes:
   *  - Follower adapts to tiered storage configuration:
   *    - LogStartOffset remains 0
   *    - LocalLogStartOffset updates to 150 (matching the earliest pending upload offset)
   *    - LogEndOffset advances to 200 after fetching all records
   *  - HighWatermark aligns with the leader (199)
   *  - Only segments after LocalLogStartOffset (150) are fetched, resulting in 2 record batches
   */
  @Test
  @Disabled
  // KIP extension required for LSO = LLSO
  def testEmptyFollowerFetchLastTieredOffsetLeaderLogStartOffsetZeroAllLocalSegmentsRetained(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

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
      earliestPendingUploadOffset = 150
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(150, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(2, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with data replication starting from the Last Tiered Offset, Leader LogStartOffset = 0,
   * and All Local Segments Deleted
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset, the leader's log starts at offset zero,
   *   but local segments below a certain offset have been deleted.
   *
   * Conditions:
   *  - TieredStorage: **Enabled**
   *  - Should replica from the last tiered offset: **True**
   *  - Leader LogStartOffset: **0**
   *  - Leader LocalLogStartOffset: **100** (segments below offset 100 deleted locally)
   *  - EarliestPendingUploadOffset: **150**
   *
   * Scenario:
   *  - The leader log contains record batches starting from offset 100, with local segments below 100 deleted
   *  - Some segments are pending upload to tiered storage (from offset 150)
   *  - The follower starts with an empty log and starts data replication from the last tiered offset
   *
   * Expected Outcomes:
   *  - Follower adapts to tiered storage configuration:
   *    - LogStartOffset initializes to 0 (matching leader's logical start)
   *    - LocalLogStartOffset updates to 150 (matching the earliest pending upload offset)
   *    - LogEndOffset advances to 200 after fetching all records
   *  - HighWatermark aligns with the leader (199)
   *  - Only segments after LocalLogStartOffset (150) are fetched, resulting in 2 record batches
   *  - The follower correctly handles the gap between LogStartOffset (0) and LocalLogStartOffset (150)
   */
  @Test
  def testEmptyFollowerFetchLastTieredOffsetLeaderLogStartOffsetZeroAllLocalSegmentsDeleted(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

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
      earliestPendingUploadOffset = 150
    )
    leaderState.logStartOffset = 0
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(150, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(2, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with data replication starting from Last Tiered Offset, Leader LogStartOffset Non-Zero,
   * and All Local Segments Retained
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset, the leader's log starts at a non-zero offset,
   *   and all local log segments from the start offset are retained.
   *
   * Conditions:
   *  - TieredStorage: **Enabled**
   *  - Should replica from the last tiered offset: **True**
   *  - Leader LogStartOffset: **10** (non-zero)
   *  - Leader LocalLogStartOffset: **10** (equal to LogStartOffset, all segments retained)
   *  - EarliestPendingUploadOffset: **150**
   *
   * Scenario:
   *  - The leader log contains record batches starting from offset 10 (non-zero start)
   *  - Some segments are pending upload to tiered storage (from offset 150)
   *  - The follower starts with an empty log and starts data replication from the last tiered offset
   *
   * Expected Outcomes:
   *  - Follower adapts to tiered storage configuration:
   *    - LogStartOffset initializes to 10 (matching leader's logical start)
   *    - LocalLogStartOffset updates to 150 (matching the earliest pending upload offset)
   *    - LogEndOffset advances to 200 after fetching all records
   *  - HighWatermark aligns with the leader (199)
   *  - Only segments after LocalLogStartOffset (150) are fetched, resulting in 2 record batches
   *  - The follower correctly handles the gap between LogStartOffset (10) and LocalLogStartOffset (150)
   *    when segments in that range exist in tiered storage
   */
  @Test
  def testEmptyFollowerFetchLastTieredOffsetLeaderLogStartOffsetNonZeroAllLocalSegmentsRetained(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
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
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(150, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(2, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with data replication starting from Last Tiered Offset, Leader LogStartOffset Non-Zero,
   * and All Local Segments Deleted
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset, the leader's log starts at a non-zero offset,
   *   and local segments between log start offset and a higher offset have been deleted.
   *
   * Conditions:
   *  - TieredStorage: **Enabled**
   *  - Should replica from the last tiered offset: **True**
   *  - Leader LogStartOffset: **10** (non-zero)
   *  - Leader LocalLogStartOffset: **100** (segments between 10 and 100 deleted locally)
   *  - EarliestPendingUploadOffset: **150**
   *
   * Scenario:
   *  - The leader log contains record batches starting from offset 100, with local segments below 100 deleted
   *  - Some segments are pending upload to tiered storage (from offset 150)
   *  - The follower starts with an empty log and starts data replication from the last tiered offset
   *
   * Expected Outcomes:
   *  - Follower adapts to tiered storage configuration:
   *    - LogStartOffset initializes to 10 (matching leader's logical start)
   *    - LocalLogStartOffset updates to 150 (matching the earliest pending upload offset)
   *    - LogEndOffset advances to 200 after fetching all records
   *  - HighWatermark aligns with the leader (199)
   *  - Only segments after LocalLogStartOffset (150) are fetched, resulting in 2 record batches
   *  - The follower correctly handles two gaps:
   *    - Between LogStartOffset (10) and leader's LocalLogStartOffset (100)
   *    - Between leader's LocalLogStartOffset (100) and EarliestPendingUploadOffset (150)
   */
  @Test
  def testEmptyFollowerFetchLastTieredOffsetLeaderLogStartOffsetNonZeroAllLocalSegmentsDeleted(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

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
      earliestPendingUploadOffset = 150
    )
    leaderState.logStartOffset = 10
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(150, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(2, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with data replication starting from Last Tiered Offset - All Leader Segments Deleted Locally
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset and all leader's segments
   *   have been uploaded to tiered storage and deleted locally (complete local emptiness).
   *
   * Conditions:
   *  - TieredStorage: **Enabled**
   *  - Should replica from the last tiered offset: **True**
   *  - Leader LogStartOffset: **Parameterized (0 or 10)**
   *  - Leader LocalLogStartOffset: **151** (equals LogEndOffset)
   *  - EarliestPendingUploadOffset: **151** (all segments uploaded)
   *
   * Scenario:
   *  - The leader has historical record batches (at offsets 100 and 150)
   *  - All segments have been uploaded to tiered storage and deleted locally
   *  - LocalLogStartOffset equals LogEndOffset (151), indicating empty local storage
   *  - The follower starts with an empty log and starts data replication from the last tiered offset
   *  - The test is parameterized to run with LogStartOffset values of 0 and 10
   *
   * Expected Outcomes:
   *  - Follower properly adapts to leader's empty local state:
   *    - LogStartOffset initializes to match leader's (0 or 10)
   *    - LocalLogStartOffset and LogEndOffset both set to 151 (matching leader)
   *    - HighWatermark sets to 151 (matching leader)
   *  - No record batches are fetched (log size remains 0)
   *  - Follower remains in FETCHING state but doesn't receive any data
   *  - Subsequent fetch operations don't change the follower's state
   *  - Follower correctly handles the gap between LogStartOffset and LocalLogStartOffset
   *    when all segments are in tiered storage only
   */
  @ParameterizedTest
  @ValueSource(longs = Array(0, 10))
  def testEmptyFollowerFetchLastTieredOffsetAllLeaderSegmentsDeletedLocally(offsetToStartLog : Long): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

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
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 151
    )
    leaderState.logStartOffset = offsetToStartLog
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    // Replica log is truncated and fetch offset updated
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(151, replicaState.localLogStartOffset)
    assertEquals(151, replicaState.logEndOffset)
    assertEquals(151, replicaState.highWatermark)

    // Call once again to see if new data is received
    fetcher.doWork()
    // No metadata update expected
    assertEquals(0, replicaState.log.size)
    assertEquals(offsetToStartLog, replicaState.logStartOffset)
    assertEquals(151, replicaState.localLogStartOffset)
    assertEquals(151, replicaState.logEndOffset)
    assertEquals(151, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with Tiered Storage Disabled, fetch from Last Tiered Offset enabled, and Leader LogStartOffset Zero
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset but tiered storage is disabled,
   *   and the leader's log starts at offset zero.
   *
   * Conditions:
   *  - TieredStorage: **Disabled**
   *  - Should replica from the last tiered offset: **True**
   *  - Leader LogStartOffset: **0**
   *  - Leader LocalLogStartOffset: **0** (equals LogStartOffset, all segments retained)
   *  - EarliestPendingUploadOffset: **N/A** (tiered storage disabled)
   *
   * Scenario:
   *  - The leader log contains record batches starting from offset 0
   *  - Tiered storage is disabled, so all segments are local
   *  - The follower starts with an empty log but enabled with data replication from the last tiered offset
   *  - Even though fetch from last tiered offset is enabled, with tiered storage disabled it should follow like
   *   standard fetch
   *
   * Expected Outcomes:
   *  - Follower adapts to non-tiered environment despite fetch from last tiered offset enabled:
   *    - LogStartOffset initializes to 0 (matching leader's start)
   *    - LocalLogStartOffset equals LogStartOffset (0)
   *    - LogEndOffset advances to 200 after fetching all records
   *  - HighWatermark aligns with the leader (199)
   *  - All segments are fetched sequentially from the beginning:
   *    - First fetch: 1 record batch, logEndOffset = 1
   *    - After additional fetches: 3 record batches, logEndOffset = 200
   *  - Even with fetch from last tiered offset enabled, follower behavior matches traditional fetch
   *    pattern when tiered storage is disabled
   */
  @Test
  def testEmptyFollowerFetchTieredStorageDisabledTieredOffsetStrategyLeaderLogStartOffsetZero(): Unit = {
    val rlmEnabled = false
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

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

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(0, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with Tiered Storage Disabled, fetch from Last Tiered Offset enabled, and Leader LogStartOffset Non-Zero
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset but tiered storage is disabled
   *   and the leader's log starts at a non-zero offset.
   *
   * Conditions:
   *  - TieredStorage: **Disabled**
   *  - Should replica from the last tiered offset: **True**
   *  - Leader LogStartOffset: **10** (non-zero)
   *  - Leader LocalLogStartOffset: **10** (equals LogStartOffset, all segments retained)
   *  - EarliestPendingUploadOffset: **N/A** (tiered storage disabled)
   *
   * Scenario:
   *  - The leader log contains record batches starting from offset 10 (non-zero)
   *  - Tiered storage is disabled, so all segments are local
   *  - The follower starts with an empty log but enabled with data replication from the last tiered offset
   *  - Even though fetch from last tiered offset is enabled, with tiered storage disabled it should follow like
   *   standard fetch
   *
   * Expected Outcomes:
   *  - Follower adapts to non-tiered environment despite tiered strategy:
   *    - LogStartOffset initializes to 10 (matching leader's start)
   *    - LogEndOffset initially sets to 10, then advances as records are fetched
   *    - After all fetches, LogEndOffset reaches 200
   *  - HighWatermark aligns with the leader (199)
   *  - All segments are fetched sequentially from the leader's start offset:
   *    - First fetch: logEndOffset = 10, but no records fetched yet
   *    - After additional fetches: 3 record batches, logEndOffset = 200
   *  - Even with fetch from last tiered offset enabled, follower behavior matches traditional fetch
   *    pattern when tiered storage is disabled, properly handling non-zero start offsets
   */
  @Test
  def testEmptyFollowerFetchTieredStorageDisabledTieredOffsetStrategyLeaderLogStartOffsetNonZero(): Unit = {
    val rlmEnabled = false
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
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
      rlmEnabled = rlmEnabled
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(10, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with data replication starting from Last Tiered Offset, Leader LogStartOffset Non-Zero,
   * No Segments Uploaded
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset with tiered storage enabled,
   *   when the leader's log starts at a non-zero offset but no segments have been uploaded to tiered storage.
   *
   * Conditions:
   *  - TieredStorage: **Enabled**
   *  - Should replica from the last tiered offset: **True**
   *  - Leader LogStartOffset: **10** (non-zero)
   *  - Leader LocalLogStartOffset: **10** (equals LogStartOffset, all segments retained)
   *  - EarliestPendingUploadOffset: **-1** (no segments uploaded or pending upload)
   *
   * Scenario:
   *  - The leader log contains record batches starting from offset 10 (non-zero)
   *  - Tiered storage is enabled, but no segments have been uploaded or are pending upload
   *  - The follower starts with an empty log and starts data replication from the last tiered offset
   *  - With no segments in tiered storage, follower should replicate from leader's start offset
   *
   * Expected Outcomes:
   *  - Follower properly initializes with leader's state:
   *    - LogStartOffset initializes to 10 (matching leader's start)
   *    - LocalLogStartOffset equals LogStartOffset (10)
   *    - LogEndOffset initially sets to 10, then advances as records are fetched
   *  - HighWatermark initially set to 10, then aligned with leader (199) after fetching
   *  - All segments are fetched sequentially from the leader's start offset:
   *    - First fetch: logEndOffset = 10, but no records fetched yet
   *    - After additional fetches: 3 record batches, logEndOffset = 200
   *  - When tiered storage is enabled but no segments are uploaded, and fetch from tiered offset enabled, it
   *    correctly falls back to fetching all segments from the logical start offset
   */
  @Test
  def testEmptyFollowerFetchLastTieredOffsetLeaderLogStartOffsetNonZeroNoSegmentsUploaded(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
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
      // Leader has not uploaded any log segments, hence the offset is -1
      earliestPendingUploadOffset = -1
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)
    assertEquals(10, replicaState.highWatermark)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with data replication starting from Last Tiered Offset, Leader LogStartOffset Zero,
   * No Segments Uploaded
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset with tiered storage enabled,
   *   when the leader's log starts at offset zero and no segments have been uploaded to tiered storage.
   *
   * Conditions:
   *  - TieredStorage: **Enabled**
   *  - Should replica from the last tiered offset: **True**
   *  - Leader LogStartOffset: **0**
   *  - Leader LocalLogStartOffset: **0** (equals LogStartOffset, all segments retained)
   *  - EarliestPendingUploadOffset: **-1** (no segments uploaded or pending upload)
   *
   * Scenario:
   *  - The leader log contains record batches starting from offset 0
   *  - Tiered storage is enabled, but no segments have been uploaded or are pending upload
   *  - The follower starts with an empty log and starts data replication from the last tiered offset
   *  - With no segments in tiered storage, follower should replicate from leader's start offset (0)
   *
   * Expected Outcomes:
   *  - Follower properly initializes with leader's state:
   *    - LogStartOffset initializes to 0 (matching leader's start)
   *    - LocalLogStartOffset equals LogStartOffset (0)
   *    - First fetch returns 1 record batch, setting logEndOffset to 1
   *  - HighWatermark immediately aligned with leader (199) after first fetch
   *  - All segments are fetched sequentially from the leader's start offset:
   *    - First fetch: 1 record batch, logEndOffset = 1
   *    - After additional fetches: 3 record batches, logEndOffset = 200
   *  - When tiered storage is enabled but no segments are uploaded, and fetch from tiered offset enabled, it
   *    correctly falls back to traditional fetch behavior from offset 0
   */
  @Test
  def testEmptyFollowerFetchLastTieredOffsetLeaderLogStartOffsetZeroNoSegmentsUploaded(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

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
      // Leader has not uploaded any log segments, hence the offset is -1
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
    assertEquals(199, replicaState.highWatermark)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(0, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with data replication starting from Last Tiered Offset, Leader LogStartOffset Non-Zero,
   * Segments Uploaded and a newly elected Leader
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset with tiered storage enabled,
   *    when the leader is newly elected and has local segments that start after the logical log start offset,
   *    but doesn't yet have information about tiered segments.
   *
   * Conditions:
   *  - TieredStorage: **Enabled**
   *  - Should replica from the last tiered offset: **True**
   *  - Leader LogStartOffset: **10** (non-zero)
   *  - Leader LocalLogStartOffset: **100** (greater than LogStartOffset, indicating tiered segments should exist)
   *  - EarliestPendingUploadOffset: **-1** (leader doesn't know about tiered segments yet)
   *
   * Scenario:
   *  - The leader's local log contains record batches starting from offset 100
   *  - The leader's logical log starts at offset 10 (implying offsets 10-99 should be in tiered storage)
   *  - However, leader reports earliestPendingUploadOffset as -1, indicating it's not aware of tiered segments
   *  - This represents a newly elected leader that hasn't completed initialization of tiered storage state
   *  - The follower starts with an empty log and starts data replication from the last tiered offset
   *
   * Expected Outcomes:
   *  - Follower detects the inconsistent state and responds properly:
   *    - Remains in FETCHING state but doesn't fetch any records
   *    - Fetch offset remains at 0 (unchanged)
   *    - Sets a delay before retry to allow leader to complete initialization
   *  - Follower's state remains unchanged:
   *    - LogEndOffset remains at 0
   *    - No records are fetched until leader initializes properly
   *  - The fetch will be retried after some delay, allowing leader time to initialize
   *  - This graceful handling prevents replication issues during leader transition
   */
  @Test
  def testEmptyFollowerFetchLastTieredOffsetLeaderLogStartOffsetNonZeroSegmentsUploadedNewlyElectedLeader(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

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
      // Leader has not uploaded any log segments, hence the offset is -1
      earliestPendingUploadOffset = -1
    )
    leaderState.logStartOffset = 10
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
   * Test: Empty Follower Fetch with data replication starting from Last Tiered Offset, Leader LogStartOffset Non-Zero,
   * Slow Local Segment Deletion
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset with tiered storage enabled,
   *   when the leader's logical log start offset is higher than its local log start offset due to
   *   a lag in local segment deletion after tiering.
   *
   * Conditions:
   *  - TieredStorage: **Enabled**
   *  - Should replica from the last tiered offset: **True**
   *  - Leader LogStartOffset: **110** (non-zero)
   *  - Leader LocalLogStartOffset: **100** (less than LogStartOffset, indicating slow local segment deletion)
   *  - EarliestPendingUploadOffset: **150** (segments up to this offset are already tiered)
   *
   * Scenario:
   *  - The leader's local log contains record batches starting from offset 100
   *  - The leader's logical log starts at offset 110 (implying offsets 100-109 are deleted logically but not physically)
   *  - Segments up to offset 150 have been uploaded to tiered storage
   *  - This represents a leader where segment deletion is lagging behind the logical truncation point
   *  - The follower starts with an empty log and starts data replication from the last tiered offset
   *
   * Expected Outcomes:
   *  - Follower initializes based on tiered offsets:
   *    - LocalLogStartOffset initializes to 150 (earliestPendingUploadOffset)
   *    - LogEndOffset initially set to 150 as well
   *    - No records fetched in first call
   *  - After additional fetch operations:
   *    - LogStartOffset is properly set to 110 (matching leader's logical start)
   *    - LocalLogStartOffset remains at 150 (based on tiered storage boundary)
   *    - LogEndOffset advances to 200 after fetching all records
   *  - Follower correctly fetches only the non-tiered segments (150-199)
   *  - Follower properly ignores leader's locally retained but logically deleted segments (100-109)
   *  - HighWatermark aligns with leader (199)
   */
  @Test
  def testEmptyFollowerFetchLastTieredOffsetLeaderLogStartOffsetNonZeroSlowLocalSegmentDeletion(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

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
      earliestPendingUploadOffset = 150
    )
    // LogStartOffset > LocalLogStartOffset
    leaderState.logStartOffset = 110
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(150, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(2, replicaState.log.size)
    assertEquals(110, replicaState.logStartOffset)
    assertEquals(150, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with data replication starting from Last Tiered Offset, Earliest Offset To Upload Less
   * Than Leader LogStartOffset
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset with tiered storage enabled,
   *   when the leader's earliest pending upload offset is less than its log start offset
   *   (a scenario that can occur after log truncation).
   *
   * Conditions:
   *  - TieredStorage: **Enabled**
   *  - Should replica from the last tiered offset: **True**
   *  - Leader LogStartOffset: **100** (non-zero)
   *  - Leader LocalLogStartOffset: **100** (equals LogStartOffset)
   *  - EarliestPendingUploadOffset: **50** (less than LogStartOffset, indicating truncation)
   *
   * Scenario:
   *  - The leader's local log contains record batches starting from offset 100
   *  - The leader reports earliestPendingUploadOffset as 50, which is less than its LogStartOffset of 100
   *  - This represents a case where segments that were pending upload were truncated
   *    or where offsets were adjusted after a leader change
   *  - The follower starts with an empty log and starts data replication from the last tiered offset
   *
   * Expected Outcomes:
   *  - Follower initialization prioritizes leader's log start offset:
   *    - LocalLogStartOffset initializes to 100 (leader's LogStartOffset)
   *    - LogEndOffset initially set to 100 as well
   *    - No records fetched in first call
   *  - After additional fetch operations:
   *    - LogStartOffset remains at 100 (matching leader's logical start)
   *    - LocalLogStartOffset remains at 100
   *    - LogEndOffset advances to 200 after fetching all records
   *  - Follower correctly ignores the anomalous earliestPendingUploadOffset (50)
   *    and uses the leader's LogStartOffset (100) as the fetch starting point
   *  - All 3 record batches are fetched from the leader (100, 150, 199)
   *  - HighWatermark aligns with leader (199)
   */
  @Test
  def testEmptyFollowerFetchLastTieredOffsetEarliestOffsetToUploadLessThanLeaderLogStartOffset(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

    val replicaState = emptyReplicaState(rlmEnabled, partition, fetcher)

    val leaderLog = Seq(
      // LogStartOffset = LocalLogStartOffset = 100
      mkBatch(baseOffset = 100, leaderEpoch = 0, new SimpleRecord("c".getBytes)),
      mkBatch(baseOffset = 150, leaderEpoch = 0, new SimpleRecord("d".getBytes)),
      mkBatch(baseOffset = 199, leaderEpoch = 0, new SimpleRecord("e".getBytes))
    )

    val leaderState = PartitionState(
      leaderLog,
      leaderEpoch = 0,
      highWatermark = 199L,
      rlmEnabled = rlmEnabled,
      earliestPendingUploadOffset = 50
    )
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    fetcher.doWork()
    assertEquals(Option(ReplicaState.FETCHING), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(100, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(100, replicaState.logStartOffset)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   * Test: Empty Follower Fetch with data replication starting from Last Tiered Offset, Retryable Remote Storage Exception
   *
   * Purpose:
   *  - Validate follower behavior when starting from the last tiered offset with tiered storage enabled,
   *   when there's a temporary failure accessing the remote tiered storage.
   *
   * Conditions:
   *  - TieredStorage: **Enabled**
   *  - Should replica from the last tiered offset: **True**
   *  - Leader LogStartOffset: **0**
   *  - Leader LocalLogStartOffset: **0** (equals LogStartOffset)
   *  - EarliestPendingUploadOffset: **150**
   *  - Remote Storage: **Temporarily unavailable** (throws RetriableRemoteStorageException)
   *
   * Scenario:
   *  - The leader's local log contains record batches starting from offset 0
   *  - The leader reports earliestPendingUploadOffset as 150, indicating tiered storage is being used
   *  - When attempting to build the remote log state, a RetriableRemoteStorageException is thrown
   *  - This represents a temporary failure in accessing the remote storage service
   *  - The follower starts with an empty log and starts data replication from the last tiered offset
   *
   * Expected Outcomes:
   *  - Follower correctly handles the temporary remote storage failure:
   *    - The partition is NOT marked as failed (since the error is retryable)
   *    - Remains in FETCHING state
   *    - Fetch offset remains unchanged at 0
   *  - Follower schedules a retry:
   *    - Sets a delay before the next fetch attempt
   *    - Preserves the fetchOffset and other state for retry
   *  - Follower's log state remains unchanged during the error:
   *    - LogEndOffset remains at 0
   *    - No records are fetched until remote storage is accessible
   *  - The leader epoch tracking is maintained (lastFetchedEpoch = 0)
   */
  @Test
  @Disabled
  // KIP extension required for LSO = LLSO
  def testEmptyFollowerFetchLastTieredOffsetRetryableRemoteStorageException(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint) {
      override def buildRemoteLogAuxState(topicPartition: TopicPartition,
                                          currentLeaderEpoch: Integer,
                                          leaderLocalLogStartOffset: lang.Long,
                                          epochForLeaderLocalLogStartOffset: Integer,
                                          leaderLogStartOffset: lang.Long,
                                          unifiedLog: UnifiedLog): lang.Long = {
        throw new RetriableRemoteStorageException("Retryable exception")
      }
    }
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, fetchFromLastTieredOffset = true)

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