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

import com.yammer.metrics.core.Gauge
import kafka.utils.TestUtils
import org.apache.kafka.common.message.FetchResponseData.PartitionData
import org.apache.kafka.common.message.{FetchResponseData, OffsetForLeaderEpochRequestData}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.utils.{MockTime, Utils}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.server.common.{DirectoryEventHandler, MetadataVersion, OffsetAndEpoch}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.ReplicaFetch
import org.apache.kafka.server.ReplicaState
import org.apache.kafka.server.ResultWithPartitions
import org.apache.kafka.server.PartitionFetchState
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.storage.internals.log.LogAppendInfo
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.Mockito.{mock, verify, when}

import java.util.Optional
import scala.collection.{Map, Set, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

class AbstractFetcherManagerTest {

  @BeforeEach
  def cleanMetricRegistry(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  private def getMetricValue(name: String): Any = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) => k.getName == name }.values.headOption.get.
      asInstanceOf[Gauge[Int]].value()
  }

  @Test
  def testAddAndRemovePartition(): Unit = {
    val fetcher: AbstractFetcherThread = mock(classOf[AbstractFetcherThread])
    val fetcherManager = new AbstractFetcherManager[AbstractFetcherThread]("fetcher-manager", "fetcher-manager", 2) {
      override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
        fetcher
      }
    }

    val fetchOffset = 10L
    val leaderEpoch = 15
    val tp = new TopicPartition("topic", 0)
    val topicId = Uuid.randomUuid()
    val initialFetchState = InitialFetchState(
      topicId = Some(topicId),
      leader = new BrokerEndPoint(0, "localhost", 9092),
      currentLeaderEpoch = leaderEpoch,
      initOffset = fetchOffset)

    when(fetcher.leader)
      .thenReturn(new MockLeaderEndPoint(new BrokerEndPoint(0, "localhost", 9092)))
    when(fetcher.addPartitions(Map(tp -> initialFetchState)))
      .thenReturn(Set(tp))
    when(fetcher.fetchState(tp))
      .thenReturn(Some(new PartitionFetchState(Optional.of(topicId), fetchOffset, Optional.empty, leaderEpoch, Optional.empty, ReplicaState.TRUNCATING, Optional.empty)))
      .thenReturn(None)
    when(fetcher.removePartitions(Set(tp))).thenReturn(Map.empty[TopicPartition, PartitionFetchState])

    fetcherManager.addFetcherForPartitions(Map(tp -> initialFetchState))
    assertEquals(Some(fetcher), fetcherManager.getFetcher(tp))

    fetcherManager.removeFetcherForPartitions(Set(tp))
    assertEquals(None, fetcherManager.getFetcher(tp))

    verify(fetcher).start()
  }

  @Test
  def testMetricFailedPartitionCount(): Unit = {
    val fetcher: AbstractFetcherThread = mock(classOf[AbstractFetcherThread])
    val fetcherManager = new AbstractFetcherManager[AbstractFetcherThread]("fetcher-manager", "fetcher-manager", 2) {
      override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
        fetcher
      }
    }

    val tp = new TopicPartition("topic", 0)
    val metricName = "FailedPartitionsCount"

    // initial value for failed partition count
    assertEquals(0, getMetricValue(metricName))

    // partition marked as failed increments the count for failed partitions
    fetcherManager.failedPartitions.add(tp)
    assertEquals(1, getMetricValue(metricName))

    // removing fetcher for the partition would remove the partition from set of failed partitions and decrement the
    // count for failed partitions
    fetcherManager.removeFetcherForPartitions(Set(tp))
    assertEquals(0, getMetricValue(metricName))
  }

  @Test
  def testDeadThreadCountMetric(): Unit = {
    val fetcher: AbstractFetcherThread = mock(classOf[AbstractFetcherThread])
    val fetcherManager = new AbstractFetcherManager[AbstractFetcherThread]("fetcher-manager", "fetcher-manager", 2) {
      override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
        fetcher
      }
    }

    val fetchOffset = 10L
    val leaderEpoch = 15
    val tp = new TopicPartition("topic", 0)
    val topicId = Uuid.randomUuid()
    val initialFetchState = InitialFetchState(
      topicId = Some(topicId),
      leader = new BrokerEndPoint(0, "localhost", 9092),
      currentLeaderEpoch = leaderEpoch,
      initOffset = fetchOffset)

    when(fetcher.leader)
      .thenReturn(new MockLeaderEndPoint(new BrokerEndPoint(0, "localhost", 9092)))
    when(fetcher.addPartitions(Map(tp -> initialFetchState)))
      .thenReturn(Set(tp))
    when(fetcher.isThreadFailed).thenReturn(true)

    fetcherManager.addFetcherForPartitions(Map(tp -> initialFetchState))

    assertEquals(1, fetcherManager.deadThreadCount)
    verify(fetcher).start()

    when(fetcher.isThreadFailed).thenReturn(false)
    assertEquals(0, fetcherManager.deadThreadCount)
  }

  @Test
  def testMaybeUpdateTopicIds(): Unit = {
    val fetcher: AbstractFetcherThread = mock(classOf[AbstractFetcherThread])
    val fetcherManager = new AbstractFetcherManager[AbstractFetcherThread]("fetcher-manager", "fetcher-manager", 2) {
      override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
        fetcher
      }
    }

    val fetchOffset = 10L
    val leaderEpoch = 15
    val tp1 = new TopicPartition("topic1", 0)
    val tp2 = new TopicPartition("topic2", 0)
    val unknownTp = new TopicPartition("topic2", 1)
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    // Start out with no topic ID.
    val initialFetchState1 = InitialFetchState(
      topicId = None,
      leader = new BrokerEndPoint(0, "localhost", 9092),
      currentLeaderEpoch = leaderEpoch,
      initOffset = fetchOffset)

    // Include a partition on a different leader
    val initialFetchState2 = InitialFetchState(
      topicId = None,
      leader = new BrokerEndPoint(1, "localhost", 9092),
      currentLeaderEpoch = leaderEpoch,
      initOffset = fetchOffset)

    // Simulate calls to different fetchers due to different leaders
    when(fetcher.leader)
      .thenReturn(new MockLeaderEndPoint(new BrokerEndPoint(0, "localhost", 9092)))
    when(fetcher.addPartitions(Map(tp1 -> initialFetchState1)))
      .thenReturn(Set(tp1))
    when(fetcher.addPartitions(Map(tp2 -> initialFetchState2)))
      .thenReturn(Set(tp2))

    when(fetcher.fetchState(tp1))
      .thenReturn(Some(new PartitionFetchState(Optional.empty, fetchOffset, Optional.empty, leaderEpoch, Optional.empty, ReplicaState.TRUNCATING, Optional.empty)))
      .thenReturn(Some(new PartitionFetchState(Optional.of(topicId1), fetchOffset, Optional.empty, leaderEpoch, Optional.empty, ReplicaState.TRUNCATING, Optional.empty)))
    when(fetcher.fetchState(tp2))
      .thenReturn(Some(new PartitionFetchState(Optional.empty, fetchOffset, Optional.empty, leaderEpoch, Optional.empty, ReplicaState.TRUNCATING,  Optional.empty)))
      .thenReturn(Some(new PartitionFetchState(Optional.of(topicId2), fetchOffset, Optional.empty, leaderEpoch, Optional.empty, ReplicaState.TRUNCATING, Optional.empty)))

    val topicIds = Map(tp1.topic -> Some(topicId1), tp2.topic -> Some(topicId2))

    // When targeting a fetcher that doesn't exist, we will not see fetcher.maybeUpdateTopicIds called.
    // We will see it for a topic partition that does not exist.
    when(fetcher.fetchState(unknownTp))
      .thenReturn(None)

    def verifyFetchState(fetchState: Option[PartitionFetchState], expectedTopicId: Option[Uuid]): Unit = {
      assertTrue(fetchState.isDefined)
      assertEquals(expectedTopicId, fetchState.get.topicId.toScala)
    }

    fetcherManager.addFetcherForPartitions(Map(tp1 -> initialFetchState1, tp2 -> initialFetchState2))
    verifyFetchState(fetcher.fetchState(tp1), None)
    verifyFetchState(fetcher.fetchState(tp2), None)

    val partitionsToUpdate = Map(tp1 -> initialFetchState1.leader.id, tp2 -> initialFetchState2.leader.id)
    fetcherManager.maybeUpdateTopicIds(partitionsToUpdate, topicIds)
    verifyFetchState(fetcher.fetchState(tp1), Some(topicId1))
    verifyFetchState(fetcher.fetchState(tp2), Some(topicId2))

    // Try an invalid fetcher and an invalid topic partition
    val invalidPartitionsToUpdate = Map(tp1 -> 2, unknownTp -> initialFetchState1.leader.id)
    fetcherManager.maybeUpdateTopicIds(invalidPartitionsToUpdate, topicIds)
    assertTrue(fetcher.fetchState(unknownTp).isEmpty)

    verify(fetcher).maybeUpdateTopicIds(Set(unknownTp), topicIds)
    verify(fetcher).maybeUpdateTopicIds(Set(tp1), topicIds)
    verify(fetcher).maybeUpdateTopicIds(Set(tp2), topicIds)
  }

  @Test
  def testExpandThreadPool(): Unit = {
    testResizeThreadPool(10, 50)
  }

  @Test
  def testShrinkThreadPool(): Unit = {
    testResizeThreadPool(50, 10)
  }

  private def testResizeThreadPool(currentFetcherSize: Int, newFetcherSize: Int, brokerNum: Int = 6): Unit = {
    val fetchingTopicPartitions = makeTopicPartition(10, 100)
    val failedTopicPartitions = makeTopicPartition(2, 5, "topic_failed")
    val fetcherManager = new AbstractFetcherManager[AbstractFetcherThread]("fetcher-manager", "fetcher-manager", currentFetcherSize) {
      override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
        new TestResizeFetcherThread(sourceBroker, failedPartitions, new MockResizeFetcherTierStateMachine)
      }
    }
    try {
      fetcherManager.addFetcherForPartitions(fetchingTopicPartitions.map { tp =>
        val brokerId = getBrokerId(tp, brokerNum)
        val brokerEndPoint = new BrokerEndPoint(brokerId, s"kafka-host-$brokerId", 9092)
        tp -> InitialFetchState(None, brokerEndPoint, 0, 0)
      }.toMap)

      // Mark some of these partitions failed within resizing scope
      fetchingTopicPartitions.take(20).foreach(fetcherManager.addFailedPartition)
      // Mark failed partitions out of resizing scope
      failedTopicPartitions.foreach(fetcherManager.addFailedPartition)

      fetcherManager.resizeThreadPool(newFetcherSize)

      val ownedPartitions = mutable.Set.empty[TopicPartition]
      fetcherManager.fetcherThreadMap.foreachEntry { (brokerIdAndFetcherId, fetcherThread) =>
        val fetcherId = brokerIdAndFetcherId.fetcherId
        val brokerId = brokerIdAndFetcherId.brokerId

        fetcherThread.partitions.foreach { tp =>
          ownedPartitions += tp
          assertEquals(fetcherManager.getFetcherId(tp), fetcherId)
          assertEquals(getBrokerId(tp, brokerNum), brokerId)
        }
      }
      // Verify that all partitions are owned by the fetcher threads.
      assertEquals(fetchingTopicPartitions, ownedPartitions)

      // Only failed partitions should still be kept after resizing
      assertEquals(failedTopicPartitions, fetcherManager.failedPartitions.partitions())
    } finally {
      fetcherManager.closeAllFetchers()
    }
  }


  private def makeTopicPartition(topicNum: Int, partitionNum: Int, topicPrefix: String = "topic_"): Set[TopicPartition] = {
    val res = mutable.Set[TopicPartition]()
    for (i <- 0 to topicNum - 1) {
      val topic = topicPrefix + i
      for (j <- 0 to partitionNum - 1) {
        res += new TopicPartition(topic, j)
      }
    }
    res.toSet
  }

  private def getBrokerId(tp: TopicPartition, brokerNum: Int): Int = {
    Utils.abs(tp.hashCode) % brokerNum
  }

  private class MockLeaderEndPoint(sourceBroker: BrokerEndPoint) extends LeaderEndPoint {
    override def initiateClose(): Unit = {}

    override def close(): Unit = {}

    override def brokerEndPoint(): BrokerEndPoint = sourceBroker

    override def fetch(fetchRequest: FetchRequest.Builder): java.util.Map[TopicPartition, FetchResponseData.PartitionData] = java.util.Map.of()

    override def fetchEarliestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = new OffsetAndEpoch(1L, 0)

    override def fetchLatestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = new OffsetAndEpoch(1L, 0)

    override def fetchEpochEndOffsets(partitions: java.util.Map[TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition]): java.util.Map[TopicPartition, EpochEndOffset] = java.util.Map.of()

    override def buildFetch(partitions: java.util.Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[java.util.Optional[ReplicaFetch]] = new ResultWithPartitions(java.util.Optional.empty[ReplicaFetch](), java.util.Set.of())

    override val isTruncationOnFetchSupported: Boolean = false

    override def fetchEarliestLocalOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = new OffsetAndEpoch(1L, 0)

    override def fetchEarliestPendingUploadOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = new OffsetAndEpoch(-1L, -1)
  }

  private class MockResizeFetcherTierStateMachine extends TierStateMachine(null, null, false) {
    override def start(topicPartition: TopicPartition, currentFetchState: PartitionFetchState, fetchPartitionData: PartitionData): PartitionFetchState = {
      throw new UnsupportedOperationException("Materializing tier state is not supported in this test.")
    }
  }

  private class TestResizeFetcherThread(sourceBroker: BrokerEndPoint, failedPartitions: FailedPartitions, fetchTierStateMachine: TierStateMachine)
    extends AbstractFetcherThread(
      name = "test-resize-fetcher",
      clientId = "mock-fetcher",
      leader = new MockLeaderEndPoint(sourceBroker),
      failedPartitions,
      fetchTierStateMachine,
      fetchBackOffMs = 0,
      brokerTopicStats = new BrokerTopicStats) {

    override protected def processPartitionData(
      topicPartition: TopicPartition,
      fetchOffset: Long,
      partitionLeaderEpoch: Int,
      partitionData: FetchData
    ): Option[LogAppendInfo] = None

    override protected def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {}

    override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {}

    override protected def latestEpoch(topicPartition: TopicPartition): Optional[Integer] = Optional.of(0)

    override protected def logStartOffset(topicPartition: TopicPartition): Long = 1

    override protected def logEndOffset(topicPartition: TopicPartition): Long = 1

    override protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Optional[OffsetAndEpoch] = Optional.of(new OffsetAndEpoch(1, 0))

    override protected def shouldFetchFromLastTieredOffset(topicPartition: TopicPartition, leaderEndOffset: Long, replicaEndOffset: Long): Boolean = false
  }

  @Test
  def testMetricsClassName(): Unit = {
    val registry = KafkaYammerMetrics.defaultRegistry()
    val config = mock(classOf[KafkaConfig])
    val replicaManager = mock(classOf[ReplicaManager])
    val quotaManager = mock(classOf[ReplicationQuotaManager])
    val brokerTopicStats   = new BrokerTopicStats()
    val directoryEventHandler = DirectoryEventHandler.NOOP
    val metrics = new Metrics()
    val time = new MockTime()
    val metadataVersionSupplier = () => MetadataVersion.LATEST_PRODUCTION
    val brokerEpochSupplier = () => 1L

    val _ = new ReplicaAlterLogDirsManager(config, replicaManager, quotaManager, brokerTopicStats, directoryEventHandler)
    val _ = new ReplicaFetcherManager(config, replicaManager, metrics, time, quotaManager, metadataVersionSupplier, brokerEpochSupplier)
    val existReplicaAlterLogDirsManager = registry.allMetrics.entrySet().stream().filter(metric => metric.getKey.getType == "ReplicaAlterLogDirsManager").findFirst()
    val existReplicaFetcherManager = registry.allMetrics.entrySet().stream().filter(metric => metric.getKey.getType == "ReplicaFetcherManager").findFirst()
    assertTrue(existReplicaAlterLogDirsManager.isPresent)
    assertTrue(existReplicaFetcherManager.isPresent)
  }
}
