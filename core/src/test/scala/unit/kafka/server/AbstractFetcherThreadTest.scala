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

import java.nio.ByteBuffer
import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger

import kafka.cluster.BrokerEndPoint
import kafka.log.LogAppendInfo
import kafka.message.NoCompressionCodec
import kafka.metrics.KafkaYammerMetrics
import kafka.server.AbstractFetcherThread.ReplicaFetch
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.utils.Implicits.MapExtensionMethods
import kafka.utils.TestUtils
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{FencedLeaderEpochException, UnknownLeaderEpochException}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Set, mutable}
import scala.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.compat.java8.OptionConverters._

class AbstractFetcherThreadTest {

  val truncateOnFetch = true
  private val partition1 = new TopicPartition("topic1", 0)
  private val partition2 = new TopicPartition("topic2", 0)
  private val failedPartitions = new FailedPartitions

  @BeforeEach
  def cleanMetricRegistry(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  private def allMetricsNames: Set[String] = KafkaYammerMetrics.defaultRegistry().allMetrics().asScala.keySet.map(_.getName)

  private def mkBatch(baseOffset: Long, leaderEpoch: Int, records: SimpleRecord*): RecordBatch = {
    MemoryRecords.withRecords(baseOffset, CompressionType.NONE, leaderEpoch, records: _*)
      .batches.asScala.head
  }

  private def initialFetchState(fetchOffset: Long, leaderEpoch: Int): InitialFetchState = {
    InitialFetchState(leader = new BrokerEndPoint(0, "localhost", 9092),
      initOffset = fetchOffset, currentLeaderEpoch = leaderEpoch)
  }

  @Test
  def testMetricsRemovedOnShutdown(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread

    // add one partition to create the consumer lag metric
    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = 0)))
    fetcher.setLeaderState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))

    fetcher.start()

    val brokerTopicStatsMetrics = fetcher.brokerTopicStats.allTopicsStats.metricMap.keySet
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
    val fetcher = new MockFetcherThread

    // add one partition to create the consumer lag metric
    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = 0)))
    fetcher.setLeaderState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))

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
    val fetcher = new MockFetcherThread

    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = MockFetcherThread.PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    fetcher.doWork()

    val replicaState = fetcher.replicaPartitionState(partition)
    assertEquals(2L, replicaState.logEndOffset)
    assertEquals(2L, replicaState.highWatermark)
  }

  @Test
  def testFencedTruncation(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread

    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 1,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = MockFetcherThread.PartitionState(Seq(batch), leaderEpoch = 1, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

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
    val fetcher = new MockFetcherThread

    val replicaState = MockFetcherThread.PartitionState(leaderEpoch = 0)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes),
      new SimpleRecord("b".getBytes))
    val leaderState = MockFetcherThread.PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    fetcher.doWork()

    // Verify we have caught up
    assertEquals(2, replicaState.logEndOffset)

    // Bump the epoch on the leader
    fetcher.leaderPartitionState(partition).leaderEpoch += 1

    fetcher.doWork()

    // After fencing, the fetcher should remove the partition from tracking and mark as failed
    assertTrue(fetcher.fetchState(partition).isEmpty)
    assertTrue(failedPartitions.contains(partition))
  }

  @Test
  def testUnknownLeaderEpochInTruncation(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread

    // The replica's leader epoch is ahead of the leader
    val replicaState = MockFetcherThread.PartitionState(leaderEpoch = 1)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = 1)), forceTruncation = true)

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0, new SimpleRecord("a".getBytes))
    val leaderState = MockFetcherThread.PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    fetcher.doWork()

    // Not data has been fetched and the follower is still truncating
    assertEquals(0, replicaState.logEndOffset)
    assertEquals(Some(Truncating), fetcher.fetchState(partition).map(_.state))

    // Bump the epoch on the leader
    fetcher.leaderPartitionState(partition).leaderEpoch += 1

    // Now we can make progress
    fetcher.doWork()

    assertEquals(1, replicaState.logEndOffset)
    assertEquals(Some(Fetching), fetcher.fetchState(partition).map(_.state))
  }

  @Test
  def testUnknownLeaderEpochWhileFetching(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread

    // This test is contrived because it shouldn't be possible to to see unknown leader epoch
    // in the Fetching state as the leader must validate the follower's epoch when it checks
    // the truncation offset.

    val replicaState = MockFetcherThread.PartitionState(leaderEpoch = 1)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = 1)))

    val leaderState = MockFetcherThread.PartitionState(Seq(
      mkBatch(baseOffset = 0L, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1L, leaderEpoch = 0, new SimpleRecord("b".getBytes))
    ), leaderEpoch = 1, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    fetcher.doWork()

    // We have fetched one batch and gotten out of the truncation phase
    assertEquals(1, replicaState.logEndOffset)
    assertEquals(Some(Fetching), fetcher.fetchState(partition).map(_.state))

    // Somehow the leader epoch rewinds
    fetcher.leaderPartitionState(partition).leaderEpoch = 0

    // We are stuck at the current offset
    fetcher.doWork()
    assertEquals(1, replicaState.logEndOffset)
    assertEquals(Some(Fetching), fetcher.fetchState(partition).map(_.state))

    // After returning to the right epoch, we can continue fetching
    fetcher.leaderPartitionState(partition).leaderEpoch = 1
    fetcher.doWork()
    assertEquals(2, replicaState.logEndOffset)
    assertEquals(Some(Fetching), fetcher.fetchState(partition).map(_.state))
  }

  @Test
  def testTruncation(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = MockFetcherThread.PartitionState(replicaLog, leaderEpoch = 5, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(3L, leaderEpoch = 5)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 1, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 3, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 5, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcherThread.PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    TestUtils.waitUntilTrue(() => {
      fetcher.doWork()
      fetcher.replicaPartitionState(partition).log == fetcher.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")

    assertEquals(leaderState.logStartOffset, replicaState.logStartOffset)
    assertEquals(leaderState.logEndOffset, replicaState.logEndOffset)
    assertEquals(leaderState.highWatermark, replicaState.highWatermark)
  }

  @Test
  def testTruncateToHighWatermarkIfLeaderEpochRequestNotSupported(): Unit = {
    val highWatermark = 2L
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread {
      override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
        assertEquals(highWatermark, truncationState.offset)
        assertTrue(truncationState.truncationCompleted)
        super.truncate(topicPartition, truncationState)
      }

      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] =
        throw new UnsupportedOperationException

      override protected val isOffsetForLeaderEpochSupported: Boolean = false

      override protected val isTruncationOnFetchSupported: Boolean = false
    }

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = MockFetcherThread.PartitionState(replicaLog, leaderEpoch = 5, highWatermark)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(highWatermark, leaderEpoch = 5)))

    fetcher.doWork()

    assertEquals(highWatermark, replicaState.logEndOffset)
    assertEquals(highWatermark, fetcher.fetchState(partition).get.fetchOffset)
    assertTrue(fetcher.fetchState(partition).get.isReadyForFetch)
  }

  @Test
  def testTruncateToHighWatermarkIfLeaderEpochInfoNotAvailable(): Unit = {
    val highWatermark = 2L
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread {
      override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
        assertEquals(highWatermark, truncationState.offset)
        assertTrue(truncationState.truncationCompleted)
        super.truncate(topicPartition, truncationState)
      }

      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] =
        throw new UnsupportedOperationException

      override def latestEpoch(topicPartition: TopicPartition): Option[Int] = None
    }

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = MockFetcherThread.PartitionState(replicaLog, leaderEpoch = 5, highWatermark)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(highWatermark, leaderEpoch = 5)))

    fetcher.doWork()

    assertEquals(highWatermark, replicaState.logEndOffset)
    assertEquals(highWatermark, fetcher.fetchState(partition).get.fetchOffset)
    assertTrue(fetcher.fetchState(partition).get.isReadyForFetch)
  }

  @Test
  def testTruncateToHighWatermarkDuringRemovePartitions(): Unit = {
    val highWatermark = 2L
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread {
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

    val replicaState = MockFetcherThread.PartitionState(replicaLog, leaderEpoch = 5, highWatermark)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(highWatermark, leaderEpoch = 5)))

    fetcher.doWork()

    assertEquals(replicaLog.last.nextOffset(), replicaState.logEndOffset)
    assertTrue(fetcher.fetchState(partition).isEmpty)
  }

  @Test
  def testTruncationSkippedIfNoEpochChange(): Unit = {
    val partition = new TopicPartition("topic", 0)

    var truncations = 0
    val fetcher = new MockFetcherThread {
      override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
        truncations += 1
        super.truncate(topicPartition, truncationState)
      }
    }

    val replicaState = MockFetcherThread.PartitionState(leaderEpoch = 5)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = 5)), forceTruncation = true)

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 1, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 3, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 5, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcherThread.PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    // Do one round of truncation
    fetcher.doWork()

    // We only fetch one record at a time with mock fetcher
    assertEquals(1, replicaState.logEndOffset)
    assertEquals(1, truncations)

    // Add partitions again with the same epoch
    fetcher.addPartitions(Map(partition -> initialFetchState(3L, leaderEpoch = 5)))

    // Verify we did not truncate
    fetcher.doWork()

    // No truncations occurred and we have fetched another record
    assertEquals(1, truncations)
    assertEquals(2, replicaState.logEndOffset)
  }

  @Test
  def testFollowerFetchOutOfRangeHigh(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread()

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = MockFetcherThread.PartitionState(replicaLog, leaderEpoch = 4, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(3L, leaderEpoch = 4)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcherThread.PartitionState(leaderLog, leaderEpoch = 4, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

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
    val fetcher = new MockFetcherThread() {
      override protected def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition, leaderEpoch: Int): Long = {
        fetchedEarliestOffset = true
        throw new FencedLeaderEpochException(s"Epoch $leaderEpoch is fenced")
      }
    }

    val replicaLog = Seq()
    val replicaState = MockFetcherThread.PartitionState(replicaLog, leaderEpoch = 4, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = 4)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))
    val leaderState = MockFetcherThread.PartitionState(leaderLog, leaderEpoch = 4, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

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
    val fetcher = new MockFetcherThread

    // The follower begins from an offset which is behind the leader's log start offset
    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)))

    val replicaState = MockFetcherThread.PartitionState(replicaLog, leaderEpoch = 0, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(3L, leaderEpoch = 0)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcherThread.PartitionState(leaderLog, leaderEpoch = 0, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    // initial truncation and verify that the log start offset is updated
    fetcher.doWork()
    if (truncateOnFetch) {
      // Second iteration required here since first iteration is required to
      // perform initial truncaton based on diverging epoch.
      fetcher.doWork()
    }
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))
    assertEquals(2, replicaState.logStartOffset)
    assertEquals(List(), replicaState.log.toList)

    TestUtils.waitUntilTrue(() => {
      fetcher.doWork()
      fetcher.replicaPartitionState(partition).log == fetcher.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")

    assertEquals(leaderState.logStartOffset, replicaState.logStartOffset)
    assertEquals(leaderState.logEndOffset, replicaState.logEndOffset)
    assertEquals(leaderState.highWatermark, replicaState.highWatermark)
  }

  @Test
  def testRetryAfterUnknownLeaderEpochInLatestOffsetFetch(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher: MockFetcherThread = new MockFetcherThread {
      val tries = new AtomicInteger(0)
      override protected def fetchLatestOffsetFromLeader(topicPartition: TopicPartition, leaderEpoch: Int): Long = {
        if (tries.getAndIncrement() == 0)
          throw new UnknownLeaderEpochException("Unexpected leader epoch")
        super.fetchLatestOffsetFromLeader(topicPartition, leaderEpoch)
      }
    }

    // The follower begins from an offset which is behind the leader's log start offset
    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)))

    val replicaState = MockFetcherThread.PartitionState(replicaLog, leaderEpoch = 0, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(3L, leaderEpoch = 0)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcherThread.PartitionState(leaderLog, leaderEpoch = 0, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    // initial truncation and initial error response handling
    fetcher.doWork()
    assertEquals(Option(Fetching), fetcher.fetchState(partition).map(_.state))

    TestUtils.waitUntilTrue(() => {
      fetcher.doWork()
      fetcher.replicaPartitionState(partition).log == fetcher.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")

    assertEquals(leaderState.logStartOffset, replicaState.logStartOffset)
    assertEquals(leaderState.logEndOffset, replicaState.logEndOffset)
    assertEquals(leaderState.highWatermark, replicaState.highWatermark)
  }

  @Test
  def testCorruptMessage(): Unit = {
    val partition = new TopicPartition("topic", 0)

    val fetcher = new MockFetcherThread {
      var fetchedOnce = false
      override def fetchFromLeader(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData] = {
        val fetchedData = super.fetchFromLeader(fetchRequest)
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

    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = MockFetcherThread.PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

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

    val fetcher = new MockFetcherThread {
      var fetchEpochsFromLeaderOnce = false
      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
        val fetchedEpochs = super.fetchEpochEndOffsets(partitions)
        if (!fetchEpochsFromLeaderOnce) {
          // leader epoch changes while fetching epochs from leader
          removePartitions(Set(partition))
          setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = nextLeaderEpochOnFollower))
          addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = nextLeaderEpochOnFollower)), forceTruncation = true)
          fetchEpochsFromLeaderOnce = true
        }
        fetchedEpochs
      }
    }

    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = initialLeaderEpochOnFollower))
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = initialLeaderEpochOnFollower)), forceTruncation = true)

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = initialLeaderEpochOnFollower, new SimpleRecord("c".getBytes)))
    val leaderState = MockFetcherThread.PartitionState(leaderLog, leaderEpochOnLeader, highWatermark = 0L)
    fetcher.setLeaderState(partition, leaderState)

    // first round of truncation
    fetcher.doWork()

    // Since leader epoch changed, fetch epochs response is ignored due to partition being in
    // truncating state with the updated leader epoch
    assertEquals(Option(Truncating), fetcher.fetchState(partition).map(_.state))
    assertEquals(Option(nextLeaderEpochOnFollower), fetcher.fetchState(partition).map(_.currentLeaderEpoch))

    if (leaderEpochOnLeader < nextLeaderEpochOnFollower) {
      fetcher.setLeaderState(
        partition, MockFetcherThread.PartitionState(leaderLog, nextLeaderEpochOnFollower, highWatermark = 0L))
    }

    // make sure the fetcher is now able to truncate and fetch
    fetcher.doWork()
    assertEquals(fetcher.leaderPartitionState(partition).log, fetcher.replicaPartitionState(partition).log)
  }

  @Test
  def testTruncateToEpochEndOffsetsDuringRemovePartitions(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val leaderEpochOnLeader = 0
    val initialLeaderEpochOnFollower = 0
    val nextLeaderEpochOnFollower = initialLeaderEpochOnFollower + 1

    val fetcher = new MockFetcherThread {
      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
        val fetchedEpochs = super.fetchEpochEndOffsets(partitions)
        // leader epoch changes while fetching epochs from leader
        // at the same time, the replica fetcher manager removes the partition
        removePartitions(Set(partition))
        setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = nextLeaderEpochOnFollower))
        fetchedEpochs
      }
    }

    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = initialLeaderEpochOnFollower))
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = initialLeaderEpochOnFollower)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = initialLeaderEpochOnFollower, new SimpleRecord("c".getBytes)))
    val leaderState = MockFetcherThread.PartitionState(leaderLog, leaderEpochOnLeader, highWatermark = 0L)
    fetcher.setLeaderState(partition, leaderState)

    // first round of work
    fetcher.doWork()

    // since the partition was removed before the fetched endOffsets were filtered against the leader epoch,
    // we do not expect the partition to be in Truncating state
    assertEquals(None, fetcher.fetchState(partition).map(_.state))
    assertEquals(None, fetcher.fetchState(partition).map(_.currentLeaderEpoch))

    fetcher.setLeaderState(
      partition, MockFetcherThread.PartitionState(leaderLog, nextLeaderEpochOnFollower, highWatermark = 0L))

    // make sure the fetcher is able to continue work
    fetcher.doWork()
    assertEquals(ArrayBuffer.empty, fetcher.replicaPartitionState(partition).log)
  }

  @Test
  def testTruncationThrowsExceptionIfLeaderReturnsPartitionsNotRequestedInFetchEpochs(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread {
      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
        val unrequestedTp = new TopicPartition("topic2", 0)
        super.fetchEpochEndOffsets(partitions).toMap + (unrequestedTp -> new EpochEndOffset()
          .setPartition(unrequestedTp.partition)
          .setErrorCode(Errors.NONE.code)
          .setLeaderEpoch(0)
          .setEndOffset(0))
      }
    }

    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> initialFetchState(0L, leaderEpoch = 0)), forceTruncation = true)
    fetcher.setLeaderState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))

    // first round of truncation should throw an exception
    assertThrows(classOf[IllegalStateException], () => fetcher.doWork())
  }

  @Test
  def testFetcherThreadHandlingPartitionFailureDuringAppending(): Unit = {
    val fetcherForAppend = new MockFetcherThread {
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
    val fetcherForTruncation = new MockFetcherThread {
      override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
        if(topicPartition == partition1)
          throw new Exception()
        else {
          super.truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState)
        }
      }
    }
    verifyFetcherThreadHandlingPartitionFailure(fetcherForTruncation)
  }

  private def verifyFetcherThreadHandlingPartitionFailure(fetcher: MockFetcherThread): Unit = {

    fetcher.setReplicaState(partition1, MockFetcherThread.PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition1 -> initialFetchState(0L, leaderEpoch = 0)), forceTruncation = true)
    fetcher.setLeaderState(partition1, MockFetcherThread.PartitionState(leaderEpoch = 0))

    fetcher.setReplicaState(partition2, MockFetcherThread.PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition2 -> initialFetchState(0L, leaderEpoch = 0)), forceTruncation = true)
    fetcher.setLeaderState(partition2, MockFetcherThread.PartitionState(leaderEpoch = 0))

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
    fetcher.addPartitions(Map(partition1 -> initialFetchState(0L, leaderEpoch = 1)), forceTruncation = true)

    // partition1 added back
    assertEquals(Some(Truncating), fetcher.fetchState(partition1).map(_.state))
    assertFalse(failedPartitions.contains(partition1))

  }

  @Test
  def testDivergingEpochs(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = MockFetcherThread.PartitionState(replicaLog, leaderEpoch = 5, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> initialFetchState(3L, leaderEpoch = 5)))
    assertEquals(3L, replicaState.logEndOffset)
    fetcher.verifyLastFetchedEpoch(partition, expectedEpoch = Some(4))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 5, new SimpleRecord("d".getBytes)))

    val leaderState = MockFetcherThread.PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    fetcher.doWork()
    fetcher.verifyLastFetchedEpoch(partition, Some(2))

    TestUtils.waitUntilTrue(() => {
      fetcher.doWork()
      fetcher.replicaPartitionState(partition).log == fetcher.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")
    fetcher.verifyLastFetchedEpoch(partition, Some(5))
  }

  object MockFetcherThread {
    class PartitionState(var log: mutable.Buffer[RecordBatch],
                         var leaderEpoch: Int,
                         var logStartOffset: Long,
                         var logEndOffset: Long,
                         var highWatermark: Long)

    object PartitionState {
      def apply(log: Seq[RecordBatch], leaderEpoch: Int, highWatermark: Long): PartitionState = {
        val logStartOffset = log.headOption.map(_.baseOffset).getOrElse(0L)
        val logEndOffset = log.lastOption.map(_.nextOffset).getOrElse(0L)
        new PartitionState(log.toBuffer, leaderEpoch, logStartOffset, logEndOffset, highWatermark)
      }

      def apply(leaderEpoch: Int): PartitionState = {
        apply(Seq(), leaderEpoch = leaderEpoch, highWatermark = 0L)
      }
    }
  }

  class MockFetcherThread(val replicaId: Int = 0, val leaderId: Int = 1)
    extends AbstractFetcherThread("mock-fetcher",
      clientId = "mock-fetcher",
      sourceBroker = new BrokerEndPoint(leaderId, host = "localhost", port = Random.nextInt()),
      failedPartitions,
      brokerTopicStats = new BrokerTopicStats) {

    import MockFetcherThread.PartitionState

    private val replicaPartitionStates = mutable.Map[TopicPartition, PartitionState]()
    private val leaderPartitionStates = mutable.Map[TopicPartition, PartitionState]()
    private var latestEpochDefault: Option[Int] = Some(0)

    def setLeaderState(topicPartition: TopicPartition, state: PartitionState): Unit = {
      leaderPartitionStates.put(topicPartition, state)
    }

    def setReplicaState(topicPartition: TopicPartition, state: PartitionState): Unit = {
      replicaPartitionStates.put(topicPartition, state)
    }

    def replicaPartitionState(topicPartition: TopicPartition): PartitionState = {
      replicaPartitionStates.getOrElse(topicPartition,
        throw new IllegalArgumentException(s"Unknown partition $topicPartition"))
    }

    def leaderPartitionState(topicPartition: TopicPartition): PartitionState = {
      leaderPartitionStates.getOrElse(topicPartition,
        throw new IllegalArgumentException(s"Unknown partition $topicPartition"))
    }

    def addPartitions(initialFetchStates: Map[TopicPartition, InitialFetchState], forceTruncation: Boolean): Set[TopicPartition] = {
      latestEpochDefault = if (forceTruncation) None else Some(0)
      val partitions = super.addPartitions(initialFetchStates)
      latestEpochDefault = Some(0)
      partitions
    }

    override def processPartitionData(topicPartition: TopicPartition,
                                      fetchOffset: Long,
                                      partitionData: FetchData): Option[LogAppendInfo] = {
      val state = replicaPartitionState(topicPartition)

      if (isTruncationOnFetchSupported && partitionData.divergingEpoch.isPresent) {
        val divergingEpoch = partitionData.divergingEpoch.get
        truncateOnFetchResponse(Map(topicPartition -> new EpochEndOffset()
          .setPartition(topicPartition.partition)
          .setErrorCode(Errors.NONE.code)
          .setLeaderEpoch(divergingEpoch.epoch)
          .setEndOffset(divergingEpoch.endOffset)))
        return None
      }

      // Throw exception if the fetchOffset does not match the fetcherThread partition state
      if (fetchOffset != state.logEndOffset)
        throw new RuntimeException(s"Offset mismatch for partition $topicPartition: " +
          s"fetched offset = $fetchOffset, log end offset = ${state.logEndOffset}.")

      // Now check message's crc
      val batches = partitionData.records.batches.asScala
      var maxTimestamp = RecordBatch.NO_TIMESTAMP
      var offsetOfMaxTimestamp = -1L
      var lastOffset = state.logEndOffset
      var lastEpoch: Option[Int] = None

      for (batch <- batches) {
        batch.ensureValid()
        if (batch.maxTimestamp > maxTimestamp) {
          maxTimestamp = batch.maxTimestamp
          offsetOfMaxTimestamp = batch.baseOffset
        }
        state.log.append(batch)
        state.logEndOffset = batch.nextOffset
        lastOffset = batch.lastOffset
        lastEpoch = Some(batch.partitionLeaderEpoch)
      }

      state.logStartOffset = partitionData.logStartOffset
      state.highWatermark = partitionData.highWatermark

      Some(LogAppendInfo(firstOffset = Some(fetchOffset),
        lastOffset = lastOffset,
        lastLeaderEpoch = lastEpoch,
        maxTimestamp = maxTimestamp,
        offsetOfMaxTimestamp = offsetOfMaxTimestamp,
        logAppendTime = Time.SYSTEM.milliseconds(),
        logStartOffset = state.logStartOffset,
        recordConversionStats = RecordConversionStats.EMPTY,
        sourceCodec = NoCompressionCodec,
        targetCodec = NoCompressionCodec,
        shallowCount = batches.size,
        validBytes = partitionData.records.sizeInBytes,
        offsetsMonotonic = true,
        lastOffsetOfFirstBatch = batches.headOption.map(_.lastOffset).getOrElse(-1)))
    }

    override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
      val state = replicaPartitionState(topicPartition)
      state.log = state.log.takeWhile { batch =>
        batch.lastOffset < truncationState.offset
      }
      state.logEndOffset = state.log.lastOption.map(_.lastOffset + 1).getOrElse(state.logStartOffset)
      state.highWatermark = math.min(state.highWatermark, state.logEndOffset)
    }

    override def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
      val state = replicaPartitionState(topicPartition)
      state.log.clear()
      state.logStartOffset = offset
      state.logEndOffset = offset
      state.highWatermark = offset
    }

    override def buildFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[ReplicaFetch]] = {
      val fetchData = mutable.Map.empty[TopicPartition, FetchRequest.PartitionData]
      partitionMap.foreach { case (partition, state) =>
        if (state.isReadyForFetch) {
          val replicaState = replicaPartitionState(partition)
          val lastFetchedEpoch = if (isTruncationOnFetchSupported)
            state.lastFetchedEpoch.map(_.asInstanceOf[Integer]).asJava
          else
            Optional.empty[Integer]
          fetchData.put(partition, new FetchRequest.PartitionData(state.fetchOffset, replicaState.logStartOffset,
            1024 * 1024, Optional.of[Integer](state.currentLeaderEpoch), lastFetchedEpoch))
        }
      }
      val fetchRequest = FetchRequest.Builder.forReplica(ApiKeys.FETCH.latestVersion, replicaId, 0, 1, fetchData.asJava)
      ResultWithPartitions(Some(ReplicaFetch(fetchData.asJava, fetchRequest)), Set.empty)
    }

    override def latestEpoch(topicPartition: TopicPartition): Option[Int] = {
      val state = replicaPartitionState(topicPartition)
      state.log.lastOption.map(_.partitionLeaderEpoch).orElse(latestEpochDefault)
    }

    override def logStartOffset(topicPartition: TopicPartition): Long = replicaPartitionState(topicPartition).logStartOffset

    override def logEndOffset(topicPartition: TopicPartition): Long = replicaPartitionState(topicPartition).logEndOffset

    override def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Option[OffsetAndEpoch] = {
      val epochData = new EpochData()
        .setPartition(topicPartition.partition)
        .setLeaderEpoch(epoch)
      val result = lookupEndOffsetForEpoch(topicPartition, epochData, replicaPartitionState(topicPartition))
      if (result.endOffset == UNDEFINED_EPOCH_OFFSET)
        None
      else
        Some(OffsetAndEpoch(result.endOffset, result.leaderEpoch))
    }

    private def checkExpectedLeaderEpoch(expectedEpochOpt: Optional[Integer],
                                         partitionState: PartitionState): Option[Errors] = {
      if (expectedEpochOpt.isPresent) {
        checkExpectedLeaderEpoch(expectedEpochOpt.get, partitionState)
      } else {
        None
      }
    }

    private def checkExpectedLeaderEpoch(expectedEpoch: Int,
                                         partitionState: PartitionState): Option[Errors] = {
      if (expectedEpoch != RecordBatch.NO_PARTITION_LEADER_EPOCH) {
        if (expectedEpoch < partitionState.leaderEpoch)
          Some(Errors.FENCED_LEADER_EPOCH)
        else if (expectedEpoch > partitionState.leaderEpoch)
          Some(Errors.UNKNOWN_LEADER_EPOCH)
        else
          None
      } else {
        None
      }
    }

    def verifyLastFetchedEpoch(partition: TopicPartition, expectedEpoch: Option[Int]): Unit = {
      if (isTruncationOnFetchSupported) {
        assertEquals(Some(Fetching), fetchState(partition).map(_.state))
        assertEquals(expectedEpoch, fetchState(partition).flatMap(_.lastFetchedEpoch))
      }
    }

    private def divergingEpochAndOffset(topicPartition: TopicPartition,
                                        lastFetchedEpoch: Optional[Integer],
                                        fetchOffset: Long,
                                        partitionState: PartitionState): Option[FetchResponseData.EpochEndOffset] = {
      lastFetchedEpoch.asScala.flatMap { fetchEpoch =>
        val epochEndOffset = fetchEpochEndOffsets(
          Map(topicPartition -> new EpochData()
            .setPartition(topicPartition.partition)
            .setLeaderEpoch(fetchEpoch)))(topicPartition)

        if (partitionState.log.isEmpty
            || epochEndOffset.endOffset == UNDEFINED_EPOCH_OFFSET
            || epochEndOffset.leaderEpoch == UNDEFINED_EPOCH)
          None
        else if (epochEndOffset.leaderEpoch < fetchEpoch || epochEndOffset.endOffset < fetchOffset) {
          Some(new FetchResponseData.EpochEndOffset()
            .setEpoch(epochEndOffset.leaderEpoch)
            .setEndOffset(epochEndOffset.endOffset))
        } else
          None
      }
    }

    private def lookupEndOffsetForEpoch(topicPartition: TopicPartition,
                                        epochData: EpochData,
                                        partitionState: PartitionState): EpochEndOffset = {
      checkExpectedLeaderEpoch(epochData.currentLeaderEpoch, partitionState).foreach { error =>
        return new EpochEndOffset()
          .setPartition(topicPartition.partition)
          .setErrorCode(error.code)
      }

      var epochLowerBound = UNDEFINED_EPOCH
      for (batch <- partitionState.log) {
        if (batch.partitionLeaderEpoch > epochData.leaderEpoch) {
          // If we don't have the requested epoch, return the next higher entry
          if (epochLowerBound == UNDEFINED_EPOCH)
            return new EpochEndOffset()
              .setPartition(topicPartition.partition)
              .setErrorCode(Errors.NONE.code)
              .setLeaderEpoch(batch.partitionLeaderEpoch)
              .setEndOffset(batch.baseOffset)
          else
            return new EpochEndOffset()
              .setPartition(topicPartition.partition)
              .setErrorCode(Errors.NONE.code)
              .setLeaderEpoch(epochLowerBound)
              .setEndOffset(batch.baseOffset)
        }
        epochLowerBound = batch.partitionLeaderEpoch
      }
      new EpochEndOffset()
        .setPartition(topicPartition.partition)
        .setErrorCode(Errors.NONE.code)
    }

    override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
      val endOffsets = mutable.Map[TopicPartition, EpochEndOffset]()
      partitions.forKeyValue { (partition, epochData) =>
        assert(partition.partition == epochData.partition,
          "Partition must be consistent between TopicPartition and EpochData")
        val leaderState = leaderPartitionState(partition)
        val epochEndOffset = lookupEndOffsetForEpoch(partition, epochData, leaderState)
        endOffsets.put(partition, epochEndOffset)
      }
      endOffsets
    }

    override protected val isOffsetForLeaderEpochSupported: Boolean = true

    override protected val isTruncationOnFetchSupported: Boolean = truncateOnFetch

    override def fetchFromLeader(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData] = {
      fetchRequest.fetchData.asScala.map { case (partition, fetchData) =>
        val leaderState = leaderPartitionState(partition)
        val epochCheckError = checkExpectedLeaderEpoch(fetchData.currentLeaderEpoch, leaderState)
        val divergingEpoch = divergingEpochAndOffset(partition, fetchData.lastFetchedEpoch, fetchData.fetchOffset, leaderState)

        val (error, records) = if (epochCheckError.isDefined) {
          (epochCheckError.get, MemoryRecords.EMPTY)
        } else if (fetchData.fetchOffset > leaderState.logEndOffset || fetchData.fetchOffset < leaderState.logStartOffset) {
          (Errors.OFFSET_OUT_OF_RANGE, MemoryRecords.EMPTY)
        } else if (divergingEpoch.nonEmpty) {
          (Errors.NONE, MemoryRecords.EMPTY)
        } else {
          // for simplicity, we fetch only one batch at a time
          val records = leaderState.log.find(_.baseOffset >= fetchData.fetchOffset) match {
            case Some(batch) =>
              val buffer = ByteBuffer.allocate(batch.sizeInBytes)
              batch.writeTo(buffer)
              buffer.flip()
              MemoryRecords.readableRecords(buffer)

            case None =>
              MemoryRecords.EMPTY
          }

          (Errors.NONE, records)
        }

        (partition, new FetchData(error, leaderState.highWatermark, leaderState.highWatermark, leaderState.logStartOffset,
          Optional.empty[Integer], List.empty.asJava, divergingEpoch.asJava, records))
      }.toMap
    }

    private def checkLeaderEpochAndThrow(expectedEpoch: Int, partitionState: PartitionState): Unit = {
      checkExpectedLeaderEpoch(expectedEpoch, partitionState).foreach { error =>
        throw error.exception()
      }
    }

    override protected def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition, leaderEpoch: Int): Long = {
      val leaderState = leaderPartitionState(topicPartition)
      checkLeaderEpochAndThrow(leaderEpoch, leaderState)
      leaderState.logStartOffset
    }

    override protected def fetchLatestOffsetFromLeader(topicPartition: TopicPartition, leaderEpoch: Int): Long = {
      val leaderState = leaderPartitionState(topicPartition)
      checkLeaderEpochAndThrow(leaderEpoch, leaderState)
      leaderState.logEndOffset
    }

  }

}
