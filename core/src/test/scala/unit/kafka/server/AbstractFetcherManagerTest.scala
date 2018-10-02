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

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.common.TopicPartition
import org.easymock.EasyMock
import org.junit.Test
import org.junit.Assert._

class AbstractFetcherManagerTest {

  @Test
  def testAddAndRemovePartition(): Unit = {
    val fetcher = EasyMock.mock(classOf[AbstractFetcherThread])
    val fetcherManager = new AbstractFetcherManager[AbstractFetcherThread]("fetcher-manager", "fetcher-manager", 2) {
      override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
        fetcher
      }
    }

    val fetchOffset = 10L
    val leaderEpoch = 15
    val tp = new TopicPartition("topic", 0)
    val initialFetchState = InitialFetchState(
      leader = new BrokerEndPoint(0, "localhost", 9092),
      currentLeaderEpoch = leaderEpoch,
      initOffset = fetchOffset)

    EasyMock.expect(fetcher.start())
    EasyMock.expect(fetcher.addPartitions(Map(tp -> OffsetAndEpoch(fetchOffset, leaderEpoch))))
    EasyMock.expect(fetcher.removePartitions(Set(tp)))
    EasyMock.replay(fetcher)

    fetcherManager.addOrUpdateFetcherForPartitions(Map(tp -> initialFetchState))
    assertEquals(Some(fetcher), fetcherManager.getFetcher(tp))

    fetcherManager.removeFetcherForPartitions(Set(tp))
    assertEquals(None, fetcherManager.getFetcher(tp))

    EasyMock.verify(fetcher)
  }

  @Test
  def testAddOrUpdatePartition(): Unit = {
    val firstLeaderEndpoint = new BrokerEndPoint(0, "localhost", 9092)
    val secondLeaderEndpoint = new BrokerEndPoint(1, "localhost", 9093)

    val firstLeaderFetcher = EasyMock.mock(classOf[AbstractFetcherThread])
    val secondLeaderFetcher = EasyMock.mock(classOf[AbstractFetcherThread])

    val fetcherManager = new AbstractFetcherManager[AbstractFetcherThread]("fetcher-manager", "fetcher-manager", 2) {
      override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
        if (sourceBroker == firstLeaderEndpoint)
          firstLeaderFetcher
        else
          secondLeaderFetcher
      }
    }

    val tp = new TopicPartition("topic", 0)
    val initialFetchState = InitialFetchState(
      leader = firstLeaderEndpoint,
      currentLeaderEpoch = 15,
      initOffset = 0L)
    val updatedFetchState = InitialFetchState(
      leader = secondLeaderEndpoint,
      currentLeaderEpoch = 16,
      initOffset = 0L)

    EasyMock.expect(firstLeaderFetcher.start())
    EasyMock.expect(firstLeaderFetcher.addPartitions(Map(tp -> OffsetAndEpoch(0L, 15))))
    EasyMock.expect(firstLeaderFetcher.removePartitions(Set(tp)))
    EasyMock.replay(firstLeaderFetcher)

    EasyMock.expect(secondLeaderFetcher.start())
    EasyMock.expect(secondLeaderFetcher.addPartitions(Map(tp -> OffsetAndEpoch(0L, 16))))
    EasyMock.replay(secondLeaderFetcher)

    fetcherManager.addOrUpdateFetcherForPartitions(Map(tp -> initialFetchState))
    assertEquals(Some(firstLeaderFetcher), fetcherManager.getFetcher(tp))

    fetcherManager.addOrUpdateFetcherForPartitions(Map(tp -> updatedFetchState))
    assertEquals(Some(secondLeaderFetcher), fetcherManager.getFetcher(tp))
  }

}
