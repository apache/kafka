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
import kafka.cluster.BrokerEndPoint
import kafka.metrics.KafkaYammerMetrics
import kafka.utils.TestUtils
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.easymock.EasyMock
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._

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
    val fetcher: AbstractFetcherThread = EasyMock.mock(classOf[AbstractFetcherThread])
    val fetcherManager = new AbstractFetcherManager[AbstractFetcherThread]("fetcher-manager", "fetcher-manager", 2) {
      override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
        fetcher
      }
    }

    val fetchOffset = 10L
    val leaderEpoch = 15
    val tp = new TopicPartition("topic", 0)
    val topicId = Some(Uuid.randomUuid())
    val initialFetchState = InitialFetchState(
      topicId = topicId,
      leader = new BrokerEndPoint(0, "localhost", 9092),
      currentLeaderEpoch = leaderEpoch,
      initOffset = fetchOffset)

    EasyMock.expect(fetcher.start())
    EasyMock.expect(fetcher.sourceBroker)
      .andReturn(new BrokerEndPoint(0, "localhost", 9092)).anyTimes()
    EasyMock.expect(fetcher.addPartitions(Map(tp -> initialFetchState)))
        .andReturn(Set(tp))
    EasyMock.expect(fetcher.fetchState(tp))
      .andReturn(Some(PartitionFetchState(topicId, fetchOffset, None, leaderEpoch, Truncating, lastFetchedEpoch = None)))
    EasyMock.expect(fetcher.removePartitions(Set(tp))).andReturn(Map.empty)
    EasyMock.expect(fetcher.fetchState(tp)).andReturn(None)
    EasyMock.replay(fetcher)

    fetcherManager.addFetcherForPartitions(Map(tp -> initialFetchState))
    assertEquals(Some(fetcher), fetcherManager.getFetcher(tp))

    fetcherManager.removeFetcherForPartitions(Set(tp))
    assertEquals(None, fetcherManager.getFetcher(tp))

    EasyMock.verify(fetcher)
  }

  @Test
  def testMetricFailedPartitionCount(): Unit = {
    val fetcher: AbstractFetcherThread = EasyMock.mock(classOf[AbstractFetcherThread])
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
    val fetcher: AbstractFetcherThread = EasyMock.mock(classOf[AbstractFetcherThread])
    val fetcherManager = new AbstractFetcherManager[AbstractFetcherThread]("fetcher-manager", "fetcher-manager", 2) {
      override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
        fetcher
      }
    }

    val fetchOffset = 10L
    val leaderEpoch = 15
    val tp = new TopicPartition("topic", 0)
    val topicId = Some(Uuid.randomUuid())
    val initialFetchState = InitialFetchState(
      topicId = topicId,
      leader = new BrokerEndPoint(0, "localhost", 9092),
      currentLeaderEpoch = leaderEpoch,
      initOffset = fetchOffset)

    EasyMock.expect(fetcher.start())
    EasyMock.expect(fetcher.sourceBroker)
      .andReturn(new BrokerEndPoint(0, "localhost", 9092)).anyTimes()
    EasyMock.expect(fetcher.addPartitions(Map(tp -> initialFetchState)))
        .andReturn(Set(tp))
    EasyMock.expect(fetcher.isThreadFailed).andReturn(true)
    EasyMock.replay(fetcher)

    fetcherManager.addFetcherForPartitions(Map(tp -> initialFetchState))

    assertEquals(1, fetcherManager.deadThreadCount)
    EasyMock.verify(fetcher)

    EasyMock.reset(fetcher)
    EasyMock.expect(fetcher.isThreadFailed).andReturn(false)
    EasyMock.replay(fetcher)

    assertEquals(0, fetcherManager.deadThreadCount)
    EasyMock.verify(fetcher)
  }

  @Test
  def testMaybeUpdateTopicIds(): Unit = {
    val fetcher: AbstractFetcherThread = EasyMock.mock(classOf[AbstractFetcherThread])
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
    val topicId1 = Some(Uuid.randomUuid())
    val topicId2 = Some(Uuid.randomUuid())

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
    EasyMock.expect(fetcher.start())
    EasyMock.expect(fetcher.start())
    EasyMock.expect(fetcher.sourceBroker)
      .andReturn(new BrokerEndPoint(0, "localhost", 9092)).anyTimes()

    EasyMock.expect(fetcher.addPartitions(Map(tp1 -> initialFetchState1)))
      .andReturn(Set(tp1))
    EasyMock.expect(fetcher.addPartitions(Map(tp2 -> initialFetchState2)))
      .andReturn(Set(tp2))

    EasyMock.expect(fetcher.fetchState(tp1))
      .andReturn(Some(PartitionFetchState(None, fetchOffset, None, leaderEpoch, Truncating, lastFetchedEpoch = None)))
    EasyMock.expect(fetcher.fetchState(tp2))
      .andReturn(Some(PartitionFetchState(None, fetchOffset, None, leaderEpoch, Truncating, lastFetchedEpoch = None)))

    val topicIds = Map(tp1.topic -> topicId1, tp2.topic -> topicId2)
    EasyMock.expect(fetcher.maybeUpdateTopicIds(Set(tp1), topicIds))
    EasyMock.expect(fetcher.maybeUpdateTopicIds(Set(tp2), topicIds))

    EasyMock.expect(fetcher.fetchState(tp1))
      .andReturn(Some(PartitionFetchState(topicId1, fetchOffset, None, leaderEpoch, Truncating, lastFetchedEpoch = None)))
    EasyMock.expect(fetcher.fetchState(tp2))
      .andReturn(Some(PartitionFetchState(topicId2, fetchOffset, None, leaderEpoch, Truncating, lastFetchedEpoch = None)))

    // When targeting a fetcher that doesn't exist, we will not see fetcher.maybeUpdateTopicIds called.
    // We will see it for a topic partition that does not exist.
    EasyMock.expect(fetcher.maybeUpdateTopicIds(Set(unknownTp), topicIds))
    EasyMock.expect(fetcher.fetchState(unknownTp))
      .andReturn(None)
    EasyMock.replay(fetcher)

    def verifyFetchState(fetchState: Option[PartitionFetchState], expectedTopicId: Option[Uuid]): Unit = {
      assertTrue(fetchState.isDefined)
      assertEquals(expectedTopicId, fetchState.get.topicId)
    }

    fetcherManager.addFetcherForPartitions(Map(tp1 -> initialFetchState1, tp2 -> initialFetchState2))
    verifyFetchState(fetcher.fetchState(tp1), None)
    verifyFetchState(fetcher.fetchState(tp2), None)

    val partitionsToUpdate = Map(tp1 -> initialFetchState1.leader.id, tp2 -> initialFetchState2.leader.id)
    fetcherManager.maybeUpdateTopicIds(partitionsToUpdate, topicIds)
    verifyFetchState(fetcher.fetchState(tp1), topicId1)
    verifyFetchState(fetcher.fetchState(tp2), topicId2)

    // Try an invalid fetcher and an invalid topic partition
    val invalidPartitionsToUpdate = Map(tp1 -> 2, unknownTp -> initialFetchState1.leader.id)
    fetcherManager.maybeUpdateTopicIds(invalidPartitionsToUpdate, topicIds)
    assertTrue(fetcher.fetchState(unknownTp).isEmpty)

    EasyMock.verify(fetcher)
  }
}
