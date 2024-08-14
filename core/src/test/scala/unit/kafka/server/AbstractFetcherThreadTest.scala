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
import org.apache.kafka.storage.internals.log.LogAppendInfo
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.{BeforeEach, Test}
import kafka.server.FetcherThreadTestUtils.{initialFetchState, mkBatch}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Set}
import scala.jdk.CollectionConverters._

class AbstractFetcherThreadTest {

  val truncateOnFetch = true
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
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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

    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version) {
      override def fetch(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData] = {
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

    val mockLeaderEndPoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version) {
      override def fetch(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData] = {
        Map(partition1 -> new FetchData().setErrorCode(Errors.UNKNOWN_TOPIC_ID.code),
          partition2 -> new FetchData().setErrorCode(Errors.INCONSISTENT_TOPIC_ID.code),
          partition3 -> new FetchData().setErrorCode(Errors.NONE.code))
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
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assertEquals(Some(Truncating), fetcher.fetchState(partition).map(_.state))

    // Bump the epoch on the leader
    fetcher.mockLeader.leaderPartitionState(partition).leaderEpoch += 1

    // Now we can make progress
    fetcher.doWork()

    assertEquals(1, replicaState.logEndOffset)
    assertEquals(Some(Fetching), fetcher.fetchState(partition).map(_.state))
  }

  @Test
  def testUnknownLeaderEpochWhileFetching(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    // This test is contrived because it shouldn't be possible to to see unknown leader epoch
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
    assertEquals(Some(Fetching), fetcher.fetchState(partition).map(_.state))

    // Somehow the leader epoch rewinds
    fetcher.mockLeader.leaderPartitionState(partition).leaderEpoch = 0

    // We are stuck at the current offset
    fetcher.doWork()
    assertEquals(1, replicaState.logEndOffset)
    assertEquals(Some(Fetching), fetcher.fetchState(partition).map(_.state))

    // After returning to the right epoch, we can continue fetching
    fetcher.mockLeader.leaderPartitionState(partition).leaderEpoch = 1
    fetcher.doWork()
    assertEquals(2, replicaState.logEndOffset)
    assertEquals(Some(Fetching), fetcher.fetchState(partition).map(_.state))
  }

  @Test
  def testTruncation(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
  def testTruncateToHighWatermarkIfLeaderEpochRequestNotSupported(): Unit = {
    val highWatermark = 2L
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndPoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version) {
      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] =
        throw new UnsupportedOperationException

      override val isTruncationOnFetchSupported: Boolean = false
    }
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine) {
        override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
          assertEquals(highWatermark, truncationState.offset)
          assertTrue(truncationState.truncationCompleted)
          super.truncate(topicPartition, truncationState)
        }
        override protected val isOffsetForLeaderEpochSupported: Boolean = false
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
  def testTruncateToHighWatermarkIfLeaderEpochInfoNotAvailable(): Unit = {
    val highWatermark = 2L
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndPoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version) {
      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] =
        throw new UnsupportedOperationException
    }
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndPoint)
    val fetcher = new MockFetcherThread(mockLeaderEndPoint, mockTierStateMachine) {
        override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
          assertEquals(highWatermark, truncationState.offset)
          assertTrue(truncationState.truncationCompleted)
          super.truncate(topicPartition, truncationState)
        }

        override def latestEpoch(topicPartition: TopicPartition): Option[Int] = None
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

    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine) {
      override def truncateToHighWatermark(partitions: Set[TopicPartition]): Unit = {
        removePartitions(Set(partition))
        super.truncateToHighWatermark(partitions)
      }

      override def latestEpoch(topicPartition: TopicPartition): Option[Int] = None
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
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assumeTrue(truncateOnFetch)
    val partition = new TopicPartition("topic", 0)
    var truncations = 0
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))

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

    val mockLeaderEndPoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version) {
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
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, failedPartitions = failedPartitions)

    // The follower begins from an offset which is behind the leader's log start offset
    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = 0, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = 0)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val leaderState = PartitionState(leaderLog, leaderEpoch = 0, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // initial truncation and verify that the log start offset is updated
    fetcher.doWork()
    if (truncateOnFetch) {
      // Second iteration required here since first iteration is required to
      // perform initial truncation based on diverging epoch.
      fetcher.doWork()
    }
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))
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
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndPoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version) {
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
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)))

    val replicaState = PartitionState(replicaLog, leaderEpoch = 0, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), 3L, leaderEpoch = 0)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val leaderState = PartitionState(leaderLog, leaderEpoch = 0, highWatermark = 2L)
    fetcher.mockLeader.setLeaderState(partition, leaderState)
    fetcher.mockLeader.setReplicaPartitionStateCallback(fetcher.replicaPartitionState)

    // initial truncation and initial error response handling
    fetcher.doWork()
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))

    TestUtils.waitUntilTrue(() => {
      fetcher.doWork()
      fetcher.replicaPartitionState(partition).log == fetcher.mockLeader.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")

    assertEquals(leaderState.logStartOffset, replicaState.logStartOffset)
    assertEquals(leaderState.logEndOffset, replicaState.logEndOffset)
    assertEquals(leaderState.highWatermark, replicaState.highWatermark)
  }

  @Test
  def testCorruptMessage(): Unit = {
    val partition = new TopicPartition("topic", 0)

    val mockLeaderEndPoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version) {
      var fetchedOnce = false
      override def fetch(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData] = {
        val fetchedData = super.fetch(fetchRequest)
        if (!fetchedOnce) {
          val records = fetchedData.head._2.records.asInstanceOf[MemoryRecords]
          val buffer = records.buffer()
          buffer.putInt(15, buffer.getInt(15) ^ 23422)
          buffer.putInt(30, buffer.getInt(30) ^ 93242)
          fetchedOnce = true
        }
        fetchedData
      }
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

  @Test
  def testLeaderEpochChangeDuringFencedFetchEpochsFromLeader(): Unit = {
    // The leader is on the new epoch when the OffsetsForLeaderEpoch with old epoch is sent, so it
    // returns the fence error. Validate that response is ignored if the leader epoch changes on
    // the follower while OffsetsForLeaderEpoch request is in flight, but able to truncate and fetch
    // in the next of round of "doWork"
    testLeaderEpochChangeDuringFetchEpochsFromLeader(leaderEpochOnLeader = 1)
  }

  @Test
  def testLeaderEpochChangeDuringSuccessfulFetchEpochsFromLeader(): Unit = {
    // The leader is on the old epoch when the OffsetsForLeaderEpoch with old epoch is sent
    // and returns the valid response. Validate that response is ignored if the leader epoch changes
    // on the follower while OffsetsForLeaderEpoch request is in flight, but able to truncate and
    // fetch once the leader is on the newer epoch (same as follower)
    testLeaderEpochChangeDuringFetchEpochsFromLeader(leaderEpochOnLeader = 0)
  }

  private def testLeaderEpochChangeDuringFetchEpochsFromLeader(leaderEpochOnLeader: Int): Unit = {
    val partition = new TopicPartition("topic", 1)
    val initialLeaderEpochOnFollower = 0
    val nextLeaderEpochOnFollower = initialLeaderEpochOnFollower + 1

    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version) {
      var fetchEpochsFromLeaderOnce = false

      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
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
    assertEquals(Option(Truncating), fetcher.fetchState(partition).map(_.state))
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

    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version) {
      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
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
    val mockLeaderEndPoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version) {
      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
        val unrequestedTp = new TopicPartition("topic2", 0)
        super.fetchEpochEndOffsets(partitions).toMap + (unrequestedTp -> new EpochEndOffset()
          .setPartition(unrequestedTp.partition)
          .setErrorCode(Errors.NONE.code)
          .setLeaderEpoch(0)
          .setEndOffset(0))
      }
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
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcherForAppend = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine, failedPartitions = failedPartitions) {
      override def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: FetchData): Option[LogAppendInfo] = {
        if (topicPartition == partition1) {
          throw new KafkaException()
        } else {
          super.processPartitionData(topicPartition, fetchOffset, partitionData)
        }
      }
    }
    verifyFetcherThreadHandlingPartitionFailure(fetcherForAppend)
  }

  @Test
  def testFetcherThreadHandlingPartitionFailureDuringTruncation(): Unit = {
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assertEquals(Some(Fetching), fetcher.fetchState(partition2).map(_.state))
    assertFalse(failedPartitions.contains(partition2))

    // simulate a leader change
    fetcher.removePartitions(Set(partition1))
    failedPartitions.removeAll(Set(partition1))
    fetcher.addPartitions(Map(partition1 -> initialFetchState(topicIds.get(partition1.topic), 0L, leaderEpoch = 1)), forceTruncation = true)

    // partition1 added back
    assertEquals(Some(Truncating), fetcher.fetchState(partition1).map(_.state))
    assertFalse(failedPartitions.contains(partition1))

  }

  @Test
  def testDivergingEpochs(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assumeTrue(truncateOnFetch)

    val partition = new TopicPartition("topic", 0)

    var truncateCalls = 0
    var processPartitionDataCalls = 0
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine) {
      override def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: FetchData): Option[LogAppendInfo] = {
        processPartitionDataCalls += 1
        super.processPartitionData(topicPartition, fetchOffset, partitionData)
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
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
    val mockTierStateMachine = new MockTierStateMachine(mockLeaderEndpoint)
    val fetcher = new MockFetcherThread(mockLeaderEndpoint, mockTierStateMachine)

    // Start with no topic IDs
    fetcher.setReplicaState(partition, PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(None, 0L, leaderEpoch = 0)))

    def verifyFetchState(fetchState: Option[PartitionFetchState], expectedTopicId: Option[Uuid]): Unit = {
      assertTrue(fetchState.isDefined)
      assertEquals(expectedTopicId, fetchState.get.topicId)
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

  private def emptyReplicaState(rlmEnabled: Boolean, partition: TopicPartition, fetcher: MockFetcherThread) = {
    // Follower begins with an empty log
    val replicaState = PartitionState(Seq(), leaderEpoch = 0, highWatermark = 0L, rlmEnabled = rlmEnabled)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(topicIds.get(partition.topic), fetchOffset = 0, leaderEpoch = 0)))
    replicaState
  }

  /**
   *  - TieredStorage Disabled
   *  - Leader LogStartOffset = 0
   */
  @Test
  def testTSDisabledEmptyFollowerFetchZeroLeader(): Unit = {
    val rlmEnabled = false
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))
    assertEquals(1, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(1, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 2) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   *  - TieredStorage Disabled
   *  - Leader LogStartOffset != 0
   */
  @Test
  def testTSDisabledEmptyFollowerFetchNonZeroLeader(): Unit = {
    val rlmEnabled = false
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))
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
   *  - TieredStorage Enabled
   *  - Leader LogStartOffset = 0
   *  - No Segments deleted locally -> Leader LocalLogStartOffset = 0
   */
  @Test
  def testTSEnabledEmptyFollowerFetchZeroLeaderNoSegmentsDeletedLocally(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))
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
   *  - TieredStorage Enabled
   *  - Leader LogStartOffset = 0
   *  - Some segments are uploaded and deleted locally -> Leader LocalLogStartOffset > LogStartOffset
   */
  @Test
  def testTSEnabledEmptyFollowerFetchZeroLeaderSegmentsDeletedLocally(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(100, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(0, replicaState.logStartOffset)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   *  - TieredStorage Enabled
   *  - Leader LogStartOffset != 0
   *  - No Segments deleted locally -> Leader LocalLogStartOffset = LogStartOffset
   */
  @Test
  def testTSEnabledEmptyFollowerFetchNonZeroLeaderNoSegmentsDeletedLocally(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   *  - TieredStorage Enabled
   *  - Leader LogStartOffset != 0
   *  - Some segments are uploaded and deleted locally -> Leader LocalLogStartOffset > LogStartOffset
   */
  @Test
  def testTSEnabledEmptyFollowerFetchNonZeroLeaderSegmentsDeletedLocally(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)

    fetcher.doWork()
    // On offset-moved-to-tiered-storage error, fetch offset is updated
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(100, replicaState.logEndOffset)

    // Only 1 record batch is returned after a poll so calling 'n' number of times to get the desired result.
    for (_ <- 1 to 3) fetcher.doWork()
    assertEquals(3, replicaState.log.size)
    assertEquals(10, replicaState.logStartOffset)
    assertEquals(100, replicaState.localLogStartOffset)
    assertEquals(200, replicaState.logEndOffset)
    assertEquals(199, replicaState.highWatermark)
  }

  /**
   *  - TieredStorage Enabled
   *  - Leader LogStartOffset = 0
   *  - All segments are uploaded and deleted locally, hence:
   *    - Leader LocalLogStartOffset > LogStartOffset
   *    - Leader LocalLogStartOffset = LogEndOffset
   */
  @Test
  def testTSEnabledEmptyFollowerFetchZeroLeaderAllSegmentsDeletedLocally(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(151, replicaState.localLogStartOffset)
    assertEquals(151, replicaState.logEndOffset)
    assertEquals(151, replicaState.highWatermark)

    // Call once again to see if new data is received
    fetcher.doWork()
    // No metadata update expected
    assertEquals(0, replicaState.log.size)
    assertEquals(151, replicaState.localLogStartOffset)
    assertEquals(151, replicaState.logEndOffset)
    assertEquals(151, replicaState.highWatermark)
  }

  /**
   *  - TieredStorage Enabled
   *  - Leader LogStartOffset != 0
   *  - All segments are uploaded and deleted locally, hence:
   *    - Leader LocalLogStartOffset > LogStartOffset
   *    - Leader LocalLogStartOffset = LogEndOffset
   */
  @Test
  def testTSEnabledEmptyFollowerFetchNonZeroLeaderAllSegmentsDeletedLocally(): Unit = {
    val rlmEnabled = true
    val partition = new TopicPartition("topic1", 0)
    val mockLeaderEndpoint = new MockLeaderEndPoint(truncateOnFetch = truncateOnFetch, version = version)
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
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(10, replicaState.localLogStartOffset)
    assertEquals(10, replicaState.logEndOffset)

    // On offset-moved-to-tiered-storage error, fetch offset is updated
    fetcher.doWork()
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))
    assertEquals(0, replicaState.log.size)
    assertEquals(151, replicaState.localLogStartOffset)
    assertEquals(151, replicaState.logEndOffset)
    assertEquals(151, replicaState.highWatermark)

    // Call once again to see if new data is received
    fetcher.doWork()
    // No metadata update expected
    assertEquals(0, replicaState.log.size)
    assertEquals(151, replicaState.localLogStartOffset)
    assertEquals(151, replicaState.logEndOffset)
    assertEquals(151, replicaState.highWatermark)
  }
}
