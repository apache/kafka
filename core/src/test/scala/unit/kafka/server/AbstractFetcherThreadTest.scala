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
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.utils.TestUtils
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

class AbstractFetcherThreadTest {

  @Before
  def cleanMetricRegistry(): Unit = {
    for (metricName <- Metrics.defaultRegistry().allMetrics().keySet().asScala)
      Metrics.defaultRegistry().removeMetric(metricName)
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
    val fetcher = new MockFetcherThread

    // add one partition to create the consumer lag metric
    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))
    fetcher.setLeaderState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))

    fetcher.start()

    // wait until all fetcher metrics are present
    TestUtils.waitUntilTrue(() =>
      allMetricsNames == Set(FetcherMetrics.BytesPerSec, FetcherMetrics.RequestsPerSec, FetcherMetrics.ConsumerLag),
      "Failed waiting for all fetcher metrics to be registered")

    fetcher.shutdown()

    // after shutdown, they should be gone
    assertTrue(Metrics.defaultRegistry().allMetrics().isEmpty)
  }

  @Test
  def testConsumerLagRemovedWithPartition(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread

    // add one partition to create the consumer lag metric
    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))
    fetcher.setLeaderState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))

    fetcher.doWork()

    assertTrue("Failed waiting for consumer lag metric",
      allMetricsNames(FetcherMetrics.ConsumerLag))

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
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))

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
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 1,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = MockFetcherThread.PartitionState(Seq(batch), leaderEpoch = 1, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    fetcher.doWork()

    // No progress should be made
    val replicaState = fetcher.replicaPartitionState(partition)
    assertEquals(0L, replicaState.logEndOffset)
    assertEquals(0L, replicaState.highWatermark)

    // After fencing, the fetcher should remove the partition from tracking
    assertTrue(fetcher.fetchState(partition).isEmpty)
  }

  @Test
  def testFencedFetch(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread

    val replicaState = MockFetcherThread.PartitionState(leaderEpoch = 0)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))

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

    // After fencing, the fetcher should remove the partition from tracking
    assertTrue(fetcher.fetchState(partition).isEmpty)
  }

  @Test
  def testUnknownLeaderEpochInTruncation(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread

    // The replica's leader epoch is ahead of the leader
    val replicaState = MockFetcherThread.PartitionState(leaderEpoch = 1)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 1)))

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
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 1)))

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
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(3L, leaderEpoch = 5)))

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
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 5)))

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
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(3L, leaderEpoch = 5)))

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
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(3L, leaderEpoch = 4)))

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
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 4)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))
    val leaderState = MockFetcherThread.PartitionState(leaderLog, leaderEpoch = 4, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    // After the out of range error, we get a fenced error and remove the partition
    fetcher.doWork()
    assertEquals(0, replicaState.logEndOffset)
    assertTrue(fetchedEarliestOffset)
    assertTrue(fetcher.fetchState(partition).isEmpty)
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
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(3L, leaderEpoch = 0)))

    val leaderLog = Seq(
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcherThread.PartitionState(leaderLog, leaderEpoch = 0, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    // initial truncation and verify that the log start offset is updated
    fetcher.doWork()
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
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(3L, leaderEpoch = 0)))

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

    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState(leaderEpoch = 0))
    fetcher.addPartitions(Map(partition -> offsetAndEpoch(0L, leaderEpoch = 0)))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = MockFetcherThread.PartitionState(Seq(batch), leaderEpoch = 0, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    fetcher.doWork() // fails with corrupt record
    fetcher.doWork() // should succeed

    val replicaState = fetcher.replicaPartitionState(partition)
    assertEquals(2L, replicaState.logEndOffset)
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
      sourceBroker = new BrokerEndPoint(leaderId, host = "localhost", port = Random.nextInt())) {

    import MockFetcherThread.PartitionState

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
        logAppendTime = Time.SYSTEM.absoluteMilliseconds(),
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

    override def fetchEpochsFromLeader(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
      val endOffsets = mutable.Map[TopicPartition, EpochEndOffset]()
      partitions.foreach { case (partition, epochData) =>
        val leaderState = leaderPartitionState(partition)
        val epochEndOffset = lookupEndOffsetForEpoch(epochData, leaderState)
        endOffsets.put(partition, epochEndOffset)
      }
      endOffsets
    }

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
