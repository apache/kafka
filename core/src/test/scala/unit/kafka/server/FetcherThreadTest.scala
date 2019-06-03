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

import com.yammer.metrics.Metrics
import kafka.cluster.BrokerEndPoint
import kafka.log.LogAppendInfo
import kafka.message.NoCompressionCodec
import kafka.server.FetcherThread.ResultWithPartitions
import kafka.utils.TestUtils
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{FencedLeaderEpochException, UnknownLeaderEpochException}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{EpochEndOffset, FetchRequest}
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._
import scala.collection.{Map, Set, mutable}
import scala.util.Random
import org.scalatest.Assertions.assertThrows

import scala.collection.mutable.ArrayBuffer

class FetcherThreadTest {

  private val replicaId: Int = 0
  private val leaderId: Int = 1
  private val partition1 = new TopicPartition("topic1", 0)
  private val partition2 = new TopicPartition("topic2", 0)
  private val failedPartitions = new FailedPartitions

  @Before
  def cleanMetricRegistry(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  private def allMetricsNames: Set[String] = Metrics.defaultRegistry().allMetrics().asScala.keySet.map(_.getName)

  private def mkBatch(baseOffset: Long, leaderEpoch: Int, records: SimpleRecord*): RecordBatch = {
    MemoryRecords.withRecords(baseOffset, CompressionType.NONE, leaderEpoch, records: _*)
      .batches.asScala.head
  }

  private def offsetAndEpoch(fetchOffset: Long, leaderEpoch: Int): OffsetAndEpoch = {
    OffsetAndEpoch(offset = fetchOffset, leaderEpoch = leaderEpoch)
  }

  @Test
  def testMetricsRemovedOnShutdown(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher(replicaId)
    val fetcherThread = new TestFetcherThread(mockFetcher)

    // add one partition to create the consumer lag metric
    mockFetcher.setReplicaState(partition, MockFetcher.PartitionState(leaderEpoch = 0))
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))
    mockFetcher.setLeaderState(partition, MockFetcher.PartitionState(leaderEpoch = 0))

    fetcherThread.start()

    // wait until all fetcher metrics are present
    TestUtils.waitUntilTrue(() =>
      allMetricsNames == Set(FetcherMetrics.BytesPerSec, FetcherMetrics.RequestsPerSec, FetcherMetrics.ConsumerLag),
      "Failed waiting for all fetcher metrics to be registered")

    fetcherThread.shutdown()

    // after shutdown, they should be gone
    assertTrue(Metrics.defaultRegistry().allMetrics().isEmpty)
  }

  @Test
  def testConsumerLagRemovedWithPartition(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher(replicaId)
    val fetcherThread = new TestFetcherThread(mockFetcher)

    // add one partition to create the consumer lag metric
    mockFetcher.setReplicaState(partition, MockFetcher.PartitionState(leaderEpoch = 0))
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))
    mockFetcher.setLeaderState(partition, MockFetcher.PartitionState(leaderEpoch = 0))

    fetcherThread.doWork()

    assertTrue("Failed waiting for consumer lag metric",
      allMetricsNames(FetcherMetrics.ConsumerLag))

    // remove the partition to simulate leader migration
    fetcherThread.removePartitions(Set(partition))

    // the lag metric should now be gone
    assertFalse(allMetricsNames(FetcherMetrics.ConsumerLag))
  }

  @Test
  def testSimpleFetch(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher(replicaId)
    val fetcherThread = new TestFetcherThread(mockFetcher)

    mockFetcher.setReplicaState(partition, MockFetcher.PartitionState(leaderEpoch = 0))
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = MockFetcher.PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    mockFetcher.setLeaderState(partition, leaderState)

    fetcherThread.doWork()

    val replicaState = mockFetcher.replicaPartitionState(partition)
    assertEquals(2L, replicaState.logEndOffset)
    assertEquals(2L, replicaState.highWatermark)
  }

  @Test
  def testFencedTruncation(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher(replicaId)
    val fetcherThread = new TestFetcherThread(mockFetcher)

    mockFetcher.setReplicaState(partition, MockFetcher.PartitionState(leaderEpoch = 0))
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 1,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = MockFetcher.PartitionState(Seq(batch), leaderEpoch = 1, highWatermark = 2L)
    mockFetcher.setLeaderState(partition, leaderState)

    fetcherThread.doWork()

    // No progress should be made
    val replicaState = mockFetcher.replicaPartitionState(partition)
    assertEquals(0L, replicaState.logEndOffset)
    assertEquals(0L, replicaState.highWatermark)

    // After fencing, the fetcher should remove the partition from tracking and mark as failed
    assertTrue(fetcherThread.fetchState(partition).isEmpty)
    assertTrue(failedPartitions.contains(partition))
  }

  @Test
  def testFencedFetch(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher(replicaId)
    val fetcherThread = new TestFetcherThread(mockFetcher)

    val replicaState = MockFetcher.PartitionState(leaderEpoch = 0)
    mockFetcher.setReplicaState(partition, replicaState)
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes),
      new SimpleRecord("b".getBytes))
    val leaderState = MockFetcher.PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    mockFetcher.setLeaderState(partition, leaderState)

    fetcherThread.doWork()

    // Verify we have caught up
    assertEquals(2, replicaState.logEndOffset)

    // Bump the epoch on the leader
    mockFetcher.leaderPartitionState(partition).leaderEpoch += 1

    fetcherThread.doWork()

    // After fencing, the fetcher should remove the partition from tracking and mark as failed
    assertTrue(fetcherThread.fetchState(partition).isEmpty)
    assertTrue(failedPartitions.contains(partition))
  }

  @Test
  def testUnknownLeaderEpochInTruncation(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher(replicaId)
    val fetcherThread = new TestFetcherThread(mockFetcher)

    // The replica's leader epoch is ahead of the leader
    val replicaState = MockFetcher.PartitionState(leaderEpoch = 1)
    mockFetcher.setReplicaState(partition, replicaState)
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 1)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0, new SimpleRecord("a".getBytes))
    val leaderState = MockFetcher.PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    mockFetcher.setLeaderState(partition, leaderState)

    fetcherThread.doWork()

    // Not data has been fetched and the follower is still truncating
    assertEquals(0, replicaState.logEndOffset)
    assertEquals(Some(Truncating), fetcherThread.fetchState(partition).map(_.state))

    // Bump the epoch on the leader
    mockFetcher.leaderPartitionState(partition).leaderEpoch += 1

    // Now we can make progress
    fetcherThread.doWork()

    assertEquals(1, replicaState.logEndOffset)
    assertEquals(Some(Fetching), fetcherThread.fetchState(partition).map(_.state))
  }

  @Test
  def testUnknownLeaderEpochWhileFetching(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher(replicaId)
    val fetcherThread = new TestFetcherThread(mockFetcher)

    // This test is contrived because it shouldn't be possible to to see unknown leader epoch
    // in the Fetching state as the leader must validate the follower's epoch when it checks
    // the truncation offset.

    val replicaState = MockFetcher.PartitionState(leaderEpoch = 1)
    mockFetcher.setReplicaState(partition, replicaState)
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 1)))

    val leaderState = MockFetcher.PartitionState(Seq(
      mkBatch(baseOffset = 0L, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1L, leaderEpoch = 0, new SimpleRecord("b".getBytes))
    ), leaderEpoch = 1, highWatermark = 2L)
    mockFetcher.setLeaderState(partition, leaderState)

    fetcherThread.doWork()

    // We have fetched one batch and gotten out of the truncation phase
    assertEquals(1, replicaState.logEndOffset)
    assertEquals(Some(Fetching), fetcherThread.fetchState(partition).map(_.state))

    // Somehow the leader epoch rewinds
    mockFetcher.leaderPartitionState(partition).leaderEpoch = 0

    // We are stuck at the current offset
    fetcherThread.doWork()
    assertEquals(1, replicaState.logEndOffset)
    assertEquals(Some(Fetching), fetcherThread.fetchState(partition).map(_.state))

    // After returning to the right epoch, we can continue fetching
    mockFetcher.leaderPartitionState(partition).leaderEpoch = 1
    fetcherThread.doWork()
    assertEquals(2, replicaState.logEndOffset)
    assertEquals(Some(Fetching), fetcherThread.fetchState(partition).map(_.state))
  }

  @Test
  def testTruncation(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher(replicaId)
    val fetcherThread = new TestFetcherThread(mockFetcher)

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = MockFetcher.PartitionState(replicaLog, leaderEpoch = 5, highWatermark = 0L)
    mockFetcher.setReplicaState(partition, replicaState)
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(3L, leaderEpoch = 5)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 1, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 3, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 5, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcher.PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 2L)
    mockFetcher.setLeaderState(partition, leaderState)

    TestUtils.waitUntilTrue(() => {
      fetcherThread.doWork()
      mockFetcher.replicaPartitionState(partition).log == mockFetcher.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")

    assertEquals(leaderState.logStartOffset, replicaState.logStartOffset)
    assertEquals(leaderState.logEndOffset, replicaState.logEndOffset)
    assertEquals(leaderState.highWatermark, replicaState.highWatermark)
  }

  @Test
  def testTruncateToHighWatermarkIfLeaderEpochRequestNotSupported(): Unit = {
    val highWatermark = 2L
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher {
      override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
        assertEquals(highWatermark, truncationState.offset)
        assertTrue(truncationState.truncationCompleted)
        super.truncate(topicPartition, truncationState)
      }

      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] =
        throw new UnsupportedOperationException

      override def isOffsetForLeaderEpochSupported: Boolean = false
    }
    val fetcherThread = new TestFetcherThread(mockFetcher)

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = MockFetcher.PartitionState(replicaLog, leaderEpoch = 5, highWatermark)
    mockFetcher.setReplicaState(partition, replicaState)
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(highWatermark, leaderEpoch = 5)))

    fetcherThread.doWork()

    assertEquals(highWatermark, replicaState.logEndOffset)
    assertEquals(highWatermark, fetcherThread.fetchState(partition).get.fetchOffset)
    assertTrue(fetcherThread.fetchState(partition).get.isReadyForFetch)
  }

  @Test
  def testTruncateToHighWatermarkIfLeaderEpochInfoNotAvailable(): Unit = {
    val highWatermark = 2L
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher {
      override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
        assertEquals(highWatermark, truncationState.offset)
        assertTrue(truncationState.truncationCompleted)
        super.truncate(topicPartition, truncationState)
      }

      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] =
        throw new UnsupportedOperationException

      override def latestEpoch(topicPartition: TopicPartition): Option[Int] = None
    }
    val fetcher = new TestFetcherThread(mockFetcher)

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = MockFetcher.PartitionState(replicaLog, leaderEpoch = 5, highWatermark)
    mockFetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(highWatermark, leaderEpoch = 5)))

    fetcher.doWork()

    assertEquals(highWatermark, replicaState.logEndOffset)
    assertEquals(highWatermark, fetcher.fetchState(partition).get.fetchOffset)
    assertTrue(fetcher.fetchState(partition).get.isReadyForFetch)
  }

  @Test
  def testTruncateToHighWatermarkDuringRemovePartitions(): Unit = {
    val highWatermark = 2L
    val partition = new TopicPartition("topic", 0)

    val mockFetcher = new MockFetcher {
      override def latestEpoch(topicPartition: TopicPartition): Option[Int] = None
    }
    val fetcherThread = new TestFetcherThread(mockFetcher) {
      override def truncateToHighWatermark(partitions: Set[TopicPartition]): Unit = {
        removePartitions(Set(partition))
        super.truncateToHighWatermark(partitions)
      }
    }

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = MockFetcher.PartitionState(replicaLog, leaderEpoch = 5, highWatermark)
    mockFetcher.setReplicaState(partition, replicaState)
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(highWatermark, leaderEpoch = 5)))

    fetcherThread.doWork()

    assertEquals(replicaLog.last.nextOffset(), replicaState.logEndOffset)
    assertTrue(fetcherThread.fetchState(partition).isEmpty)
  }

  @Test
  def testTruncationSkippedIfNoEpochChange(): Unit = {
    val partition = new TopicPartition("topic", 0)

    var truncations = 0
    val mockFetcher = new MockFetcher {
      override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
        truncations += 1
        super.truncate(topicPartition, truncationState)
      }
    }
    val fetcherThread = new TestFetcherThread(mockFetcher)

    val replicaState = MockFetcher.PartitionState(leaderEpoch = 5)
    mockFetcher.setReplicaState(partition, replicaState)
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 5)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 1, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 3, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 5, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcher.PartitionState(leaderLog, leaderEpoch = 5, highWatermark = 2L)
    mockFetcher.setLeaderState(partition, leaderState)

    // Do one round of truncation
    fetcherThread.doWork()

    // We only fetch one record at a time with mock fetcher
    assertEquals(1, replicaState.logEndOffset)
    assertEquals(1, truncations)

    // Add partitions again with the same epoch
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(3L, leaderEpoch = 5)))

    // Verify we did not truncate
    fetcherThread.doWork()

    // No truncations occurred and we have fetched another record
    assertEquals(1, truncations)
    assertEquals(2, replicaState.logEndOffset)
  }

  @Test
  def testFollowerFetchOutOfRangeHigh(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher(replicaId)
    val fetcherThread = new TestFetcherThread(mockFetcher)

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = MockFetcher.PartitionState(replicaLog, leaderEpoch = 4, highWatermark = 0L)
    mockFetcher.setReplicaState(partition, replicaState)
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(3L, leaderEpoch = 4)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcher.PartitionState(leaderLog, leaderEpoch = 4, highWatermark = 2L)
    mockFetcher.setLeaderState(partition, leaderState)

    // initial truncation and verify that the log end offset is updated
    fetcherThread.doWork()
    assertEquals(3L, replicaState.logEndOffset)
    assertEquals(Option(Fetching), fetcherThread.fetchState(partition).map(_.state))

    // To hit this case, we have to change the leader log without going through the truncation phase
    leaderState.log.clear()
    leaderState.logEndOffset = 0L
    leaderState.logStartOffset = 0L
    leaderState.highWatermark = 0L

    fetcherThread.doWork()

    assertEquals(0L, replicaState.logEndOffset)
    assertEquals(0L, replicaState.logStartOffset)
    assertEquals(0L, replicaState.highWatermark)
  }

  @Test
  def testFencedOffsetResetAfterOutOfRange(): Unit = {
    val partition = new TopicPartition("topic", 0)
    var fetchedEarliestOffset = false
    val mockFetcher = new MockFetcher {
      override def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition, leaderEpoch: Int): Long = {
        fetchedEarliestOffset = true
        throw new FencedLeaderEpochException(s"Epoch $leaderEpoch is fenced")
      }
    }
    val fetcherThread = new TestFetcherThread(mockFetcher)

    val replicaLog = Seq()
    val replicaState = MockFetcher.PartitionState(replicaLog, leaderEpoch = 4, highWatermark = 0L)
    mockFetcher.setReplicaState(partition, replicaState)
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 4)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))
    val leaderState = MockFetcher.PartitionState(leaderLog, leaderEpoch = 4, highWatermark = 2L)
    mockFetcher.setLeaderState(partition, leaderState)

    // After the out of range error, we get a fenced error and remove the partition and mark as failed
    fetcherThread.doWork()
    assertEquals(0, replicaState.logEndOffset)
    assertTrue(fetchedEarliestOffset)
    assertTrue(fetcherThread.fetchState(partition).isEmpty)
    assertTrue(failedPartitions.contains(partition))
  }

  @Test
  def testFollowerFetchOutOfRangeLow(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher(replicaId)
    val fetcherThread = new TestFetcherThread(mockFetcher)

    // The follower begins from an offset which is behind the leader's log start offset
    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)))

    val replicaState = MockFetcher.PartitionState(replicaLog, leaderEpoch = 0, highWatermark = 0L)
    mockFetcher.setReplicaState(partition, replicaState)
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(3L, leaderEpoch = 0)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcher.PartitionState(leaderLog, leaderEpoch = 0, highWatermark = 2L)
    mockFetcher.setLeaderState(partition, leaderState)

    // initial truncation and verify that the log start offset is updated
    fetcherThread.doWork()
    assertEquals(Option(Fetching), fetcherThread.fetchState(partition).map(_.state))
    assertEquals(2, replicaState.logStartOffset)
    assertEquals(List(), replicaState.log.toList)

    TestUtils.waitUntilTrue(() => {
      fetcherThread.doWork()
      mockFetcher.replicaPartitionState(partition).log == mockFetcher.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")

    assertEquals(leaderState.logStartOffset, replicaState.logStartOffset)
    assertEquals(leaderState.logEndOffset, replicaState.logEndOffset)
    assertEquals(leaderState.highWatermark, replicaState.highWatermark)
  }

  @Test
  def testRetryAfterUnknownLeaderEpochInLatestOffsetFetch(): Unit = {
    val partition = new TopicPartition("topic", 0)

    val mockFetcher: MockFetcher = new MockFetcher {
      val tries = new AtomicInteger(0)

      override def fetchLatestOffsetFromLeader(topicPartition: TopicPartition, leaderEpoch: Int): Long = {
        if (tries.getAndIncrement() == 0)
          throw new UnknownLeaderEpochException("Unexpected leader epoch")
        super.fetchLatestOffsetFromLeader(topicPartition, leaderEpoch)
      }
    }
    val fetcherThread = new TestFetcherThread(mockFetcher)

    // The follower begins from an offset which is behind the leader's log start offset
    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)))

    val replicaState = MockFetcher.PartitionState(replicaLog, leaderEpoch = 0, highWatermark = 0L)
    mockFetcher.setReplicaState(partition, replicaState)
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(3L, leaderEpoch = 0)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcher.PartitionState(leaderLog, leaderEpoch = 0, highWatermark = 2L)
    mockFetcher.setLeaderState(partition, leaderState)

    // initial truncation and initial error response handling
    fetcherThread.doWork()
    assertEquals(Option(Fetching), fetcherThread.fetchState(partition).map(_.state))

    TestUtils.waitUntilTrue(() => {
      fetcherThread.doWork()
      mockFetcher.replicaPartitionState(partition).log == mockFetcher.leaderPartitionState(partition).log
    }, "Failed to reconcile leader and follower logs")

    assertEquals(leaderState.logStartOffset, replicaState.logStartOffset)
    assertEquals(leaderState.logEndOffset, replicaState.logEndOffset)
    assertEquals(leaderState.highWatermark, replicaState.highWatermark)
  }

  @Test
  def testCorruptMessage(): Unit = {
    val partition = new TopicPartition("topic", 0)

    val mockFetcher: MockFetcher = new MockFetcher() {
      var fetchedOnce = false

      override def fetchFromLeader(fetchRequest: FetchRequest.Builder): Seq[(TopicPartition, FetchData)] = {
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
    val fetcherThread = new TestFetcherThread(mockFetcher)

    mockFetcher.setReplicaState(partition, MockFetcher.PartitionState(leaderEpoch = 0))
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = MockFetcher.PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    mockFetcher.setLeaderState(partition, leaderState)

    fetcherThread.doWork() // fails with corrupt record
    fetcherThread.doWork() // should succeed

    val replicaState = mockFetcher.replicaPartitionState(partition)
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
    val partition = new TopicPartition("topic", 0)
    val initialLeaderEpochOnFollower = 0
    val nextLeaderEpochOnFollower = initialLeaderEpochOnFollower + 1

    val mockFetcher: MockFetcher = new MockFetcher {
      var fetchEpochsFromLeaderOnce = false

      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
        val fetchedEpochs = super.fetchEpochEndOffsets(partitions)
        if (!fetchEpochsFromLeaderOnce) {
          // leader epoch changes while fetching epochs from leader
          fetcherThread.removePartitions(Set(partition))
          setReplicaState(partition, MockFetcher.PartitionState(leaderEpoch = nextLeaderEpochOnFollower))
          fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = nextLeaderEpochOnFollower)))
          fetchEpochsFromLeaderOnce = true
        }
        fetchedEpochs
      }
    }
    val fetcherThread = new TestFetcherThread(mockFetcher)

    mockFetcher.setReplicaState(partition, MockFetcher.PartitionState(leaderEpoch = initialLeaderEpochOnFollower))
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = initialLeaderEpochOnFollower)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = initialLeaderEpochOnFollower, new SimpleRecord("c".getBytes)))
    val leaderState = MockFetcher.PartitionState(leaderLog, leaderEpochOnLeader, highWatermark = 0L)
    mockFetcher.setLeaderState(partition, leaderState)

    // first round of truncation
    fetcherThread.doWork()

    // Since leader epoch changed, fetch epochs response is ignored due to partition being in
    // truncating state with the updated leader epoch
    assertEquals(Option(Truncating), fetcherThread.fetchState(partition).map(_.state))
    assertEquals(Option(nextLeaderEpochOnFollower), fetcherThread.fetchState(partition).map(_.currentLeaderEpoch))

    if (leaderEpochOnLeader < nextLeaderEpochOnFollower) {
      mockFetcher.setLeaderState(
        partition, MockFetcher.PartitionState(leaderLog, nextLeaderEpochOnFollower, highWatermark = 0L))
    }

    // make sure the fetcher is now able to truncate and fetch
    fetcherThread.doWork()
    assertEquals(mockFetcher.leaderPartitionState(partition).log, mockFetcher.replicaPartitionState(partition).log)
  }

  @Test
  def testTruncateToEpochEndOffsetsDuringRemovePartitions(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val leaderEpochOnLeader = 0
    val initialLeaderEpochOnFollower = 0
    val nextLeaderEpochOnFollower = initialLeaderEpochOnFollower + 1

    val mockFetcher = new MockFetcher {
      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
        val fetchedEpochs = super.fetchEpochEndOffsets(partitions)
        // leader epoch changes while fetching epochs from leader
        // at the same time, the replica fetcher manager removes the partition
        fetcherThread.removePartitions(Set(partition))
        setReplicaState(partition, MockFetcher.PartitionState(leaderEpoch = nextLeaderEpochOnFollower))
        fetchedEpochs
      }
    }
    val fetcherThread = new TestFetcherThread(mockFetcher)

    mockFetcher.setReplicaState(partition, MockFetcher.PartitionState(leaderEpoch = initialLeaderEpochOnFollower))
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = initialLeaderEpochOnFollower)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = initialLeaderEpochOnFollower, new SimpleRecord("c".getBytes)))
    val leaderState = MockFetcher.PartitionState(leaderLog, leaderEpochOnLeader, highWatermark = 0L)
    mockFetcher.setLeaderState(partition, leaderState)

    // first round of work
    fetcherThread.doWork()

    // since the partition was removed before the fetched endOffsets were filtered against the leader epoch,
    // we do not expect the partition to be in Truncating state
    assertEquals(None, fetcherThread.fetchState(partition).map(_.state))
    assertEquals(None, fetcherThread.fetchState(partition).map(_.currentLeaderEpoch))

    mockFetcher.setLeaderState(
      partition, MockFetcher.PartitionState(leaderLog, nextLeaderEpochOnFollower, highWatermark = 0L))

    // make sure the fetcher is able to continue work
    fetcherThread.doWork()
    assertEquals(ArrayBuffer.empty, mockFetcher.replicaPartitionState(partition).log)
  }

  @Test
  def testTruncationThrowsExceptionIfLeaderReturnsPartitionsNotRequestedInFetchEpochs(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val mockFetcher = new MockFetcher {
      override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
        val unrequestedTp = new TopicPartition("topic2", 0)
        super.fetchEpochEndOffsets(partitions) + (unrequestedTp -> new EpochEndOffset(0, 0))
      }
    }
    val fetcherThread = new TestFetcherThread(mockFetcher)

    mockFetcher.setReplicaState(partition, MockFetcher.PartitionState(leaderEpoch = 0))
    fetcherThread.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))
    mockFetcher.setLeaderState(partition, MockFetcher.PartitionState(leaderEpoch = 0))

    // first round of truncation should throw an exception
    assertThrows[IllegalStateException] {
      fetcherThread.doWork()
    }
  }

  @Test
  def testFetcherThreadHandlingPartitionFailureDuringAppending(): Unit = {
    val mockFetcher = new MockFetcher {
      override def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: FetchData): Option[LogAppendInfo] = {
        if (topicPartition == partition1) {
          throw new KafkaException()
        } else {
          super.processPartitionData(topicPartition, fetchOffset, partitionData)
        }
      }
    }
    val fetcherThreadForAppend = new TestFetcherThread(mockFetcher)
    verifyFetcherThreadHandlingPartitionFailure(fetcherThreadForAppend, mockFetcher)
  }

  @Test
  def testFetcherThreadHandlingPartitionFailureDuringTruncation(): Unit = {
    val mockFetcher = new MockFetcher {
      override def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
        if (topicPartition == partition1)
          throw new Exception()
        else {
          super.truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState)
        }
      }
    }
    val fetcherThreadForTruncation = new TestFetcherThread(mockFetcher)
    verifyFetcherThreadHandlingPartitionFailure(fetcherThreadForTruncation, mockFetcher)
  }

  private def verifyFetcherThreadHandlingPartitionFailure(fetcherThread: TestFetcherThread,
                                                          mockFetcher: MockFetcher): Unit = {
    mockFetcher.setReplicaState(partition1, MockFetcher.PartitionState(leaderEpoch = 0))
    fetcherThread.addPartitions(Map(partition1 -> offsetAndEpoch(0L, leaderEpoch = 0)))
    mockFetcher.setLeaderState(partition1, MockFetcher.PartitionState(leaderEpoch = 0))

    mockFetcher.setReplicaState(partition2, MockFetcher.PartitionState(leaderEpoch = 0))
    fetcherThread.addPartitions(Map(partition2 -> offsetAndEpoch(0L, leaderEpoch = 0)))
    mockFetcher.setLeaderState(partition2, MockFetcher.PartitionState(leaderEpoch = 0))

    // processing data fails for partition1
    fetcherThread.doWork()

    // partition1 marked as failed
    assertTrue(failedPartitions.contains(partition1))
    assertEquals(None, fetcherThread.fetchState(partition1))

    // make sure the fetcher continues to work with rest of the partitions
    fetcherThread.doWork()
    assertEquals(Some(Fetching), fetcherThread.fetchState(partition2).map(_.state))
    assertFalse(failedPartitions.contains(partition2))

    // simulate a leader change
    fetcherThread.removePartitions(Set(partition1))
    failedPartitions.removeAll(Set(partition1))
    fetcherThread.addPartitions(Map(partition1 -> offsetAndEpoch(0L, leaderEpoch = 1)))

    // partition1 added back
    assertEquals(Some(Truncating), fetcherThread.fetchState(partition1).map(_.state))
    assertFalse(failedPartitions.contains(partition1))
  }


  class TestFetcherThread(val fetcher: MockFetcher)
    extends FetcherThread("mock-fetcher",
      clientId = "mock-fetcher",
      sourceBroker = new BrokerEndPoint(leaderId, host = "localhost", port = Random.nextInt()),
      failedPartitions,
      fetcher = fetcher) {

    // The circular dependence is annoying, but useful for testing race conditions
    // with partition addition/removal while the fetcher is handling some operation
    fetcher.fetcherThread = this
  }

}

object MockFetcher {

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

class MockFetcher(val replicaId: Int = 0) extends Fetcher {

  var fetcherThread: FetcherThread = _

  import MockFetcher.PartitionState

  private val replicaPartitionStates = mutable.Map[TopicPartition, PartitionState]()
  private val leaderPartitionStates = mutable.Map[TopicPartition, PartitionState]()

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

  override def processPartitionData(topicPartition: TopicPartition,
                                    fetchOffset: Long,
                                    partitionData: FetchData): Option[LogAppendInfo] = {
    val state = replicaPartitionState(topicPartition)

    // Throw exception if the fetchOffset does not match the fetcherThread partition state
    if (fetchOffset != state.logEndOffset)
      throw new RuntimeException(s"Offset mismatch for partition $topicPartition: " +
        s"fetched offset = $fetchOffset, log end offset = ${state.logEndOffset}.")

    // Now check message's crc
    val batches = partitionData.records.batches.asScala
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    var lastOffset = state.logEndOffset

    for (batch <- batches) {
      batch.ensureValid()
      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = batch.baseOffset
      }
      state.log.append(batch)
      state.logEndOffset = batch.nextOffset
      lastOffset = batch.lastOffset
    }

    state.logStartOffset = partitionData.logStartOffset
    state.highWatermark = partitionData.highWatermark

    Some(LogAppendInfo(firstOffset = Some(fetchOffset),
      lastOffset = lastOffset,
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

  override def buildFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[FetchRequest.Builder]] = {
    val fetchData = mutable.Map.empty[TopicPartition, FetchRequest.PartitionData]
    partitionMap.foreach { case (partition, state) =>
      if (state.isReadyForFetch) {
        val replicaState = replicaPartitionState(partition)
        fetchData.put(partition, new FetchRequest.PartitionData(state.fetchOffset, replicaState.logStartOffset,
          1024 * 1024, Optional.of[Integer](state.currentLeaderEpoch)))
      }
    }
    val fetchRequest = FetchRequest.Builder.forReplica(ApiKeys.FETCH.latestVersion, replicaId, 0, 1, fetchData.asJava)
    ResultWithPartitions(Some(fetchRequest), Set.empty)
  }

  override def latestEpoch(topicPartition: TopicPartition): Option[Int] = {
    val state = replicaPartitionState(topicPartition)
    state.log.lastOption.map(_.partitionLeaderEpoch).orElse(Some(EpochEndOffset.UNDEFINED_EPOCH))
  }

  override def logEndOffset(topicPartition: TopicPartition): Long = replicaPartitionState(topicPartition).logEndOffset

  override def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Option[OffsetAndEpoch] = {
    val epochData = new EpochData(Optional.empty[Integer](), epoch)
    val result = lookupEndOffsetForEpoch(epochData, replicaPartitionState(topicPartition))
    if (result.endOffset == EpochEndOffset.UNDEFINED_EPOCH_OFFSET)
      None
    else
      Some(OffsetAndEpoch(result.endOffset, result.leaderEpoch))
  }

  private def checkExpectedLeaderEpoch(expectedEpochOpt: Optional[Integer],
                                       partitionState: PartitionState): Option[Errors] = {
    if (expectedEpochOpt.isPresent) {
      val expectedEpoch = expectedEpochOpt.get
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

  private def lookupEndOffsetForEpoch(epochData: EpochData,
                                      partitionState: PartitionState): EpochEndOffset = {
    checkExpectedLeaderEpoch(epochData.currentLeaderEpoch, partitionState).foreach { error =>
      return new EpochEndOffset(error, EpochEndOffset.UNDEFINED_EPOCH, EpochEndOffset.UNDEFINED_EPOCH_OFFSET)
    }

    var epochLowerBound = EpochEndOffset.UNDEFINED_EPOCH
    for (batch <- partitionState.log) {
      if (batch.partitionLeaderEpoch > epochData.leaderEpoch) {
        return new EpochEndOffset(Errors.NONE, epochLowerBound, batch.baseOffset)
      }
      epochLowerBound = batch.partitionLeaderEpoch
    }
    new EpochEndOffset(Errors.NONE, EpochEndOffset.UNDEFINED_EPOCH, EpochEndOffset.UNDEFINED_EPOCH_OFFSET)
  }

  override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
    val endOffsets = mutable.Map[TopicPartition, EpochEndOffset]()
    partitions.foreach { case (partition, epochData) =>
      val leaderState = leaderPartitionState(partition)
      val epochEndOffset = lookupEndOffsetForEpoch(epochData, leaderState)
      endOffsets.put(partition, epochEndOffset)
    }
    endOffsets
  }

  override def isOffsetForLeaderEpochSupported: Boolean = true

  override def fetchFromLeader(fetchRequest: FetchRequest.Builder): Seq[(TopicPartition, FetchData)] = {
    fetchRequest.fetchData.asScala.map { case (partition, fetchData) =>
      val leaderState = leaderPartitionState(partition)
      val epochCheckError = checkExpectedLeaderEpoch(fetchData.currentLeaderEpoch, leaderState)

      val (error, records) = if (epochCheckError.isDefined) {
        (epochCheckError.get, MemoryRecords.EMPTY)
      } else if (fetchData.fetchOffset > leaderState.logEndOffset || fetchData.fetchOffset < leaderState.logStartOffset) {
        (Errors.OFFSET_OUT_OF_RANGE, MemoryRecords.EMPTY)
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
        List.empty.asJava, records))
    }.toSeq
  }

  private def checkLeaderEpochAndThrow(expectedEpoch: Int, partitionState: PartitionState): Unit = {
    checkExpectedLeaderEpoch(Optional.of[Integer](expectedEpoch), partitionState).foreach { error =>
      throw error.exception()
    }
  }

  override def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition, leaderEpoch: Int): Long = {
    val leaderState = leaderPartitionState(topicPartition)
    checkLeaderEpochAndThrow(leaderEpoch, leaderState)
    leaderState.logStartOffset
  }

  override def fetchLatestOffsetFromLeader(topicPartition: TopicPartition, leaderEpoch: Int): Long = {
    val leaderState = leaderPartitionState(topicPartition)
    checkLeaderEpochAndThrow(leaderEpoch, leaderState)
    leaderState.logEndOffset
  }

}
