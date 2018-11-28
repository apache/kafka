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
    val fetcher: AbstractFetcherThread = EasyMock.mock(classOf[AbstractFetcherThread])
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
    EasyMock.expect(fetcher.fetchState(tp))
      .andReturn(Some(PartitionFetchState(fetchOffset, leaderEpoch, Truncating)))
    EasyMock.expect(fetcher.removePartitions(Set(tp)))
    EasyMock.expect(fetcher.fetchState(tp)).andReturn(None)
    EasyMock.replay(fetcher)

    fetcherManager.addFetcherForPartitions(Map(tp -> initialFetchState))
    assertEquals(Some(fetcher), fetcherManager.getFetcher(tp))

    fetcherManager.removeFetcherForPartitions(Set(tp))
    assertEquals(None, fetcherManager.getFetcher(tp))

    EasyMock.verify(fetcher)
  }

}
