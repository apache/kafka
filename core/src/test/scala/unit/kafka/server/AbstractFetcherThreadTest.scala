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

import AbstractFetcherThread._
import com.yammer.metrics.Metrics
import kafka.cluster.BrokerEndPoint
import kafka.server.AbstractFetcherThread.{FetchRequest, PartitionData}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.EpochEndOffset
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.{Before, Test}

import scala.collection.JavaConverters._
import scala.collection.{Map, Set, mutable}

class AbstractFetcherThreadTest {

  @Before
  def cleanMetricRegistry(): Unit = {
    for (metricName <- Metrics.defaultRegistry().allMetrics().keySet().asScala)
      Metrics.defaultRegistry().removeMetric(metricName)
  }

  @Test
  def testMetricsRemovedOnShutdown() {
    val partition = new TopicPartition("topic", 0)
    val fetcherThread = new DummyFetcherThread("dummy", "client", new BrokerEndPoint(0, "localhost", 9092))

    fetcherThread.start()

    // add one partition to create the consumer lag metric
    fetcherThread.addPartitions(Map(partition -> 0L))

    // wait until all fetcher metrics are present
    TestUtils.waitUntilTrue(() =>
      allMetricsNames == Set(FetcherMetrics.BytesPerSec, FetcherMetrics.RequestsPerSec, FetcherMetrics.ConsumerLag),
      "Failed waiting for all fetcher metrics to be registered")

    fetcherThread.shutdown()

    // after shutdown, they should be gone
    assertTrue(Metrics.defaultRegistry().allMetrics().isEmpty)
  }

  @Test
  def testConsumerLagRemovedWithPartition() {
    val partition = new TopicPartition("topic", 0)
    val fetcherThread = new DummyFetcherThread("dummy", "client", new BrokerEndPoint(0, "localhost", 9092))

    fetcherThread.start()

    // add one partition to create the consumer lag metric
    fetcherThread.addPartitions(Map(partition -> 0L))

    // wait until lag metric is present
    TestUtils.waitUntilTrue(() => allMetricsNames(FetcherMetrics.ConsumerLag),
      "Failed waiting for consumer lag metric")

    // remove the partition to simulate leader migration
    fetcherThread.removePartitions(Set(partition))

    // the lag metric should now be gone
    assertFalse(allMetricsNames(FetcherMetrics.ConsumerLag))

    fetcherThread.shutdown()
  }

  private def allMetricsNames = Metrics.defaultRegistry().allMetrics().asScala.keySet.map(_.getName)

  class DummyFetchRequest(val offsets: collection.Map[TopicPartition, Long]) extends FetchRequest {
    override def isEmpty: Boolean = offsets.isEmpty

    override def offset(topicPartition: TopicPartition): Long = offsets(topicPartition)
  }

  class TestPartitionData(records: MemoryRecords = MemoryRecords.EMPTY) extends PartitionData {
    override def error: Errors = Errors.NONE

    override def toRecords: MemoryRecords = records

    override def highWatermark: Long = 0L

    override def exception: Option[Throwable] = None
  }

  class DummyFetcherThread(name: String,
                           clientId: String,
                           sourceBroker: BrokerEndPoint,
                           fetchBackOffMs: Int = 0)
    extends AbstractFetcherThread(name, clientId, sourceBroker, fetchBackOffMs, isInterruptible = true, includeLogTruncation = false) {

    type REQ = DummyFetchRequest
    type PD = PartitionData

    override def processPartitionData(topicPartition: TopicPartition,
                                      fetchOffset: Long,
                                      partitionData: PartitionData): Unit = {}

    override def handleOffsetOutOfRange(topicPartition: TopicPartition): Long = 0L

    override def handlePartitionsWithErrors(partitions: Iterable[TopicPartition]): Unit = {}

    override protected def fetch(fetchRequest: DummyFetchRequest): Seq[(TopicPartition, TestPartitionData)] =
      fetchRequest.offsets.mapValues(_ => new TestPartitionData()).toSeq

    override protected def buildFetchRequest(partitionMap: collection.Seq[(TopicPartition, PartitionFetchState)]): ResultWithPartitions[DummyFetchRequest] =
      ResultWithPartitions(new DummyFetchRequest(partitionMap.map { case (k, v) => (k, v.fetchOffset) }.toMap), Set())

    override def buildLeaderEpochRequest(allPartitions: Seq[(TopicPartition, PartitionFetchState)]): ResultWithPartitions[Map[TopicPartition, Int]] = {
      ResultWithPartitions(Map(), Set())
    }

    override def fetchEpochsFromLeader(partitions: Map[TopicPartition, Int]): Map[TopicPartition, EpochEndOffset] = { Map() }

    override def maybeTruncate(fetchedEpochs: Map[TopicPartition, EpochEndOffset]): ResultWithPartitions[Map[TopicPartition, OffsetTruncationState]] = {
      ResultWithPartitions(Map(), Set())
    }
  }


  @Test
  def testFetchRequestCorruptedMessageException() {
    val partition = new TopicPartition("topic", 0)
    val fetcherThread = new CorruptingFetcherThread("test", "client", new BrokerEndPoint(0, "localhost", 9092),
      fetchBackOffMs = 1)

    fetcherThread.start()

    // Add one partition for fetching
    fetcherThread.addPartitions(Map(partition -> 0L))

    // Wait until fetcherThread finishes the work
    TestUtils.waitUntilTrue(() => fetcherThread.fetchCount > 3, "Failed waiting for fetcherThread to finish the work")

    fetcherThread.shutdown()

    // The fetcherThread should have fetched two normal messages
    assertTrue(fetcherThread.logEndOffset == 2)
  }

  class CorruptingFetcherThread(name: String,
                                clientId: String,
                                sourceBroker: BrokerEndPoint,
                                fetchBackOffMs: Int = 0)
    extends DummyFetcherThread(name, clientId, sourceBroker, fetchBackOffMs) {

    @volatile var logEndOffset = 0L
    @volatile var fetchCount = 0

    private val normalPartitionDataSet = List(
      new TestPartitionData(MemoryRecords.withRecords(0L, CompressionType.NONE, new SimpleRecord("hello".getBytes()))),
      new TestPartitionData(MemoryRecords.withRecords(1L, CompressionType.NONE, new SimpleRecord("hello".getBytes())))
    )

    override def processPartitionData(topicPartition: TopicPartition,
                                      fetchOffset: Long,
                                      partitionData: PartitionData): Unit = {
      // Throw exception if the fetchOffset does not match the fetcherThread partition state
      if (fetchOffset != logEndOffset)
        throw new RuntimeException(
          "Offset mismatch for partition %s: fetched offset = %d, log end offset = %d."
            .format(topicPartition, fetchOffset, logEndOffset))

      // Now check message's crc
      val records = partitionData.toRecords
      for (batch <- records.batches.asScala) {
        batch.ensureValid()
        logEndOffset = batch.nextOffset
      }
    }

    override protected def fetch(fetchRequest: DummyFetchRequest): Seq[(TopicPartition, TestPartitionData)] = {
      fetchCount += 1
      // Set the first fetch to get a corrupted message
      if (fetchCount == 1) {
        val record = new SimpleRecord("hello".getBytes())
        val records = MemoryRecords.withRecords(CompressionType.NONE, record)
        val buffer = records.buffer

        // flip some bits in the message to ensure the crc fails
        buffer.putInt(15, buffer.getInt(15) ^ 23422)
        buffer.putInt(30, buffer.getInt(30) ^ 93242)
        fetchRequest.offsets.mapValues(_ => new TestPartitionData(records)).toSeq
      } else {
        // Then, the following fetches get the normal data
        fetchRequest.offsets.mapValues(v => normalPartitionDataSet(v.toInt)).toSeq
      }
    }

    override protected def buildFetchRequest(partitionMap: collection.Seq[(TopicPartition, PartitionFetchState)]): ResultWithPartitions[DummyFetchRequest] = {
      val requestMap = new mutable.HashMap[TopicPartition, Long]
      partitionMap.foreach { case (topicPartition, partitionFetchState) =>
        // Add backoff delay check
        if (partitionFetchState.isReadyForFetch)
          requestMap.put(topicPartition, partitionFetchState.fetchOffset)
      }
      ResultWithPartitions(new DummyFetchRequest(requestMap), Set())
    }

    override def handlePartitionsWithErrors(partitions: Iterable[TopicPartition]) = delayPartitions(partitions, fetchBackOffMs.toLong)

  }

}
