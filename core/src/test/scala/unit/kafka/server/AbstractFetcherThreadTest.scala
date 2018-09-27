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

import AbstractFetcherThread._
import com.yammer.metrics.Metrics
import kafka.cluster.BrokerEndPoint
import kafka.log.LogAppendInfo
import kafka.message.NoCompressionCodec
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.FatalExitError
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

  @Test
  def testMetricsRemovedOnShutdown(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread

    // add one partition to create the consumer lag metric
    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState())
    fetcher.addPartitions(Map(partition -> 0L))
    fetcher.setLeaderState(partition, MockFetcherThread.PartitionState())

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
    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState())
    fetcher.addPartitions(Map(partition -> 0L))
    fetcher.setLeaderState(partition, MockFetcherThread.PartitionState())

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

    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState())
    fetcher.addPartitions(Map(partition -> 0L))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = MockFetcherThread.PartitionState(Seq(batch), highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    fetcher.doWork()

    val replicaState = fetcher.replicaPartitionState(partition)
    assertEquals(2L, replicaState.logEndOffset)
    assertEquals(2L, replicaState.highWatermark)
  }

  @Test
  def testTruncation(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = MockFetcherThread.PartitionState(replicaLog, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> 3L))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 1, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 3, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 5, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcherThread.PartitionState(leaderLog, highWatermark = 2L)
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
  def testFollowerFetchOutOfRangeHigh(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread()

    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val replicaState = MockFetcherThread.PartitionState(replicaLog, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> 3L))

    val leaderLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)),
      mkBatch(baseOffset = 1, leaderEpoch = 2, new SimpleRecord("b".getBytes)),
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcherThread.PartitionState(leaderLog, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    // initial truncation and verify that the log end offset is updated
    fetcher.doWork()
    assertEquals(3L, replicaState.logEndOffset)
    assertFalse(fetcher.partitionStates.stateValue(partition).isTruncatingLog)

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
  def testFollowerFetchOutOfRangeLow(): Unit = {
    val partition = new TopicPartition("topic", 0)
    val fetcher = new MockFetcherThread

    // The follower begins from an offset which is behind the leader's log start offset
    val replicaLog = Seq(
      mkBatch(baseOffset = 0, leaderEpoch = 0, new SimpleRecord("a".getBytes)))

    val replicaState = MockFetcherThread.PartitionState(replicaLog, highWatermark = 0L)
    fetcher.setReplicaState(partition, replicaState)
    fetcher.addPartitions(Map(partition -> 3L))

    val leaderLog = Seq(
      mkBatch(baseOffset = 2, leaderEpoch = 4, new SimpleRecord("c".getBytes)))

    val leaderState = MockFetcherThread.PartitionState(leaderLog, highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    // initial truncation and verify that the log start offset is updated
    fetcher.doWork()
    assertFalse(fetcher.partitionStates.stateValue(partition).isTruncatingLog)
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
  def testCorruptMessage(): Unit = {
    val partition = new TopicPartition("topic", 0)

    val fetcher = new MockFetcherThread {
      var fetchedOnce = false
      override def fetchFromLeader(fetchRequest: FetchRequest.Builder): Seq[(TopicPartition, PD)] = {
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

    fetcher.setReplicaState(partition, MockFetcherThread.PartitionState())
    fetcher.addPartitions(Map(partition -> 0L))

    val batch = mkBatch(baseOffset = 0L, leaderEpoch = 0,
      new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes))
    val leaderState = MockFetcherThread.PartitionState(Seq(batch), highWatermark = 2L)
    fetcher.setLeaderState(partition, leaderState)

    fetcher.doWork() // fails with corrupt record
    fetcher.doWork() // should succeed

    val replicaState = fetcher.replicaPartitionState(partition)
    assertEquals(2L, replicaState.logEndOffset)
  }

  object MockFetcherThread {
    class PartitionState(var log: mutable.Buffer[RecordBatch],
                         var logStartOffset: Long,
                         var logEndOffset: Long,
                         var highWatermark: Long)

    object PartitionState {
      def apply(log: Seq[RecordBatch], highWatermark: Long): PartitionState = {
        val logStartOffset = log.headOption.map(_.baseOffset).getOrElse(0L)
        val logEndOffset = log.lastOption.map(_.nextOffset).getOrElse(0L)
        new PartitionState(log.toBuffer, logStartOffset, logEndOffset, highWatermark)
      }

      def apply(): PartitionState = {
        apply(Seq(), 0L)
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
                                      partitionData: PD): Option[LogAppendInfo] = {
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
            1024 * 1024, Optional.empty()))
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
      lookupEndOffsetForEpoch(epoch, replicaPartitionState(topicPartition))
    }

    private def lookupEndOffsetForEpoch(epoch: Int, partitionState: PartitionState): Option[OffsetAndEpoch] = {
      var epochLowerBound = EpochEndOffset.UNDEFINED_EPOCH
      for (batch <- partitionState.log) {
        if (batch.partitionLeaderEpoch > epoch) {
          return Some(OffsetAndEpoch(batch.baseOffset, epochLowerBound))
        }
        epochLowerBound = batch.partitionLeaderEpoch
      }
      None
    }

    override def fetchEpochsFromLeader(partitions: Map[TopicPartition, Int]): Map[TopicPartition, EpochEndOffset] = {
      val endOffsets = mutable.Map[TopicPartition, EpochEndOffset]()
      partitions.foreach { case (partition, epoch) =>
        val state = leaderPartitionState(partition)
        val epochEndOffset = lookupEndOffsetForEpoch(epoch, state) match {
          case Some(OffsetAndEpoch(offset, epoch)) =>
            new EpochEndOffset(Errors.NONE, epoch, offset)
          case None =>
            new EpochEndOffset(Errors.NONE, EpochEndOffset.UNDEFINED_EPOCH, EpochEndOffset.UNDEFINED_EPOCH_OFFSET)
        }
        endOffsets.put(partition, epochEndOffset)
      }
      endOffsets
    }

    override def fetchFromLeader(fetchRequest: FetchRequest.Builder): Seq[(TopicPartition, PD)] = {
      fetchRequest.fetchData.asScala.map { case (partition, fetchData) =>
        val state = leaderPartitionState(partition)
        val (error, records) = if (fetchData.fetchOffset > state.logEndOffset || fetchData.fetchOffset < state.logStartOffset) {
          (Errors.OFFSET_OUT_OF_RANGE, MemoryRecords.EMPTY)
        } else {
          // for simplicity, we fetch only one batch at a time
          val records = state.log.find(_.baseOffset >= fetchData.fetchOffset) match {
            case Some(batch) =>
              val buffer = ByteBuffer.allocate(batch.sizeInBytes())
              batch.writeTo(buffer)
              buffer.flip()
              MemoryRecords.readableRecords(buffer)

            case None =>
              MemoryRecords.EMPTY
          }

          (Errors.NONE, records)
        }

        (partition, new PD(error, state.highWatermark, state.highWatermark, state.logStartOffset,
          List.empty.asJava, records))
      }.toSeq
    }

    override protected def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition): Long = {
      leaderPartitionState(topicPartition).logStartOffset
    }

    override protected def fetchLatestOffsetFromLeader(topicPartition: TopicPartition): Long = {
      leaderPartitionState(topicPartition).logEndOffset
    }
  }

}
