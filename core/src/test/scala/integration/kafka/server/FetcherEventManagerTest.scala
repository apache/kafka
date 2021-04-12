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

package integration.kafka.server

import kafka.cluster.BrokerEndPoint
import kafka.server._
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.utils.Time
import org.easymock.EasyMock.{createMock, expect, replay, verify}
import org.junit.Assert.assertEquals
import org.junit.Test

class FetcherEventManagerTest {

  @Test
  def testInitialState(): Unit = {
    val time = Time.SYSTEM
    val fetcherEventBus: FetcherEventBus = createMock(classOf[FetcherEventBus])
    expect(fetcherEventBus.put(TruncateAndFetch)).andVoid()
    expect(fetcherEventBus.close()).andVoid()
    replay(fetcherEventBus)

    val processor : FetcherEventProcessor = createMock(classOf[FetcherEventProcessor])
    val fetcherEventManager = new FetcherEventManager("thread-1", fetcherEventBus, processor, time)

    fetcherEventManager.start()
    fetcherEventManager.close()

    verify(fetcherEventBus)
  }

  @Test
  def testEventExecution(): Unit = {
    val time = Time.SYSTEM
    val fetcherEventBus = new FetcherEventBus(time)

    @volatile var addPartitionsProcessed = 0
    @volatile var removePartitionsProcessed = 0
    @volatile var getPartitionsProcessed = 0
    @volatile var truncateAndFetchProcessed = 0
    val processor : FetcherEventProcessor = new FetcherEventProcessor {
      override def process(event: FetcherEvent): Unit = {
        event match {
          case AddPartitions(initialFetchStates, future) =>
            addPartitionsProcessed += 1
            future.complete(null)
          case RemovePartitions(topicPartitions, future) =>
            removePartitionsProcessed += 1
            future.complete(null)
          case GetPartitionCount(future) =>
            getPartitionsProcessed += 1
            future.complete(1)
          case TruncateAndFetch =>
            truncateAndFetchProcessed += 1
        }

      }

      override def fetcherStats: AsyncFetcherStats = ???

      override def fetcherLagStats: AsyncFetcherLagStats = ???

      override def sourceBroker: BrokerEndPoint = ???

      override def close(): Unit = {}
    }

    val fetcherEventManager = new FetcherEventManager("thread-1", fetcherEventBus, processor, time)
    val addPartitionsFuture = fetcherEventManager.addPartitions(Map.empty)
    val removePartitionsFuture = fetcherEventManager.removePartitions(Set.empty)
    val getPartitionCountFuture = fetcherEventManager.getPartitionsCount()

    fetcherEventManager.start()
    addPartitionsFuture.get()
    removePartitionsFuture.get()
    getPartitionCountFuture.get()

    assertEquals(1, addPartitionsProcessed)
    assertEquals(1, removePartitionsProcessed)
    assertEquals(1, getPartitionsProcessed)
    fetcherEventManager.close()
  }

}

