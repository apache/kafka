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

package unit.kafka.server

import java.util.Date
import java.util.concurrent.locks.{Condition, Lock}
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import kafka.server._
import kafka.utils.MockTime
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer


class FetcherEventBusTest {
  @Test
  def testGetWhileEmpty(): Unit = {
    // test the getNextEvent method while the fetcherEventBus is empty
    val fetcherEventBus = new FetcherEventBus(new MockTime())

    val service = Executors.newSingleThreadExecutor()
    @volatile var counter = 0
    val runnableFinished = new CountDownLatch(1)
    service.submit(new Runnable {
      override def run(): Unit = {
        // trying to call get will block indefinitely
        fetcherEventBus.getNextEvent()
        counter = 1
        runnableFinished.countDown()
      }
    })

    // the runnable should still be blocked
    assertTrue(counter == 0)

    // put a event to unblock the runnable
    fetcherEventBus.put(AddPartitions(Map.empty, null))
    service.shutdown()
    runnableFinished.await()
    assertTrue(counter == 1)
  }

  @Test
  def testQueuedEvent(): Unit = {
    val fetcherEventBus = new FetcherEventBus(new MockTime())
    val addPartitions = AddPartitions(Map.empty, null)
    fetcherEventBus.put(addPartitions)
    assertTrue(fetcherEventBus.getNextEvent().event == addPartitions)
  }


  @Test
  def testQueuedEventsWithDifferentPriorities(): Unit = {
    val fetcherEventBus = new FetcherEventBus(new MockTime())

    val lowPriorityTask = TruncateAndFetch
    fetcherEventBus.put(lowPriorityTask)

    val highPriorityTask = AddPartitions(Map.empty, null)
    fetcherEventBus.put(highPriorityTask)

    val expectedSequence = Seq(highPriorityTask, lowPriorityTask)

    val actualSequence = ArrayBuffer[FetcherEvent]()

    for (_ <- 0 until 2) {
      actualSequence += fetcherEventBus.getNextEvent().event
    }

    assertEquals(expectedSequence, actualSequence)
  }

  @Test
  def testQueuedEventsWithSamePriority(): Unit = {
    // Two queued events with the same priority should be polled
    // according to their sequence numbers in a FIFO manner
    val fetcherEventBus = new FetcherEventBus(new MockTime())

    val task1 = AddPartitions(Map.empty, null)
    fetcherEventBus.put(task1)

    val task2 = AddPartitions(Map.empty, null)
    fetcherEventBus.put(task2)

    val expectedSequence = Seq(task1, task2)

    val actualSequence = ArrayBuffer[FetcherEvent]()

    for (_ <- 0 until 2) {
      actualSequence += fetcherEventBus.getNextEvent().event
    }

    assertEquals(expectedSequence, actualSequence)
  }

  class MockCondition extends Condition {
    override def await(): Unit = ???

    override def awaitUninterruptibly(): Unit = ???

    override def awaitNanos(nanosTimeout: Long): Long = ???

    override def await(time: Long, unit: TimeUnit): Boolean = {
      awaitCalled = true
      false // false indicates that no further waiting is needed
    }

    override def awaitUntil(deadline: Date): Boolean = ???

    override def signal(): Unit = ???

    override def signalAll(): Unit = {}

    var awaitCalled = false
  }

  class MockConditionFactory(condition: MockCondition) extends ConditionFactory {
    override def createCondition(lock: Lock): Condition = condition
  }

  @Test
  def testDelayedEvent(): Unit = {
    val time = new MockTime()
    val condition = new MockCondition
    val fetcherEventBus = new FetcherEventBus(time, new MockConditionFactory(condition))
    val addPartitions = AddPartitions(Map.empty, null)
    val delay = 1000
    fetcherEventBus.schedule(new DelayedFetcherEvent(delay, addPartitions))
    assertEquals(1, fetcherEventBus.scheduledEventQueueSize())
    val service = Executors.newSingleThreadExecutor()

    val future = service.submit(new Runnable {
      override def run(): Unit = {
        assertTrue(fetcherEventBus.getNextEvent().event == addPartitions)
        assertTrue(condition.awaitCalled)
        assertEquals(0, fetcherEventBus.scheduledEventQueueSize())
      }
    })

    future.get()
    service.shutdown()
  }

  @Test
  def testDelayedEventsWithDifferentDueTimes(): Unit = {
    val time = new MockTime()
    val condition = new MockCondition
    val fetcherEventBus = new FetcherEventBus(time, new MockConditionFactory(condition))
    val secondTask = AddPartitions(Map.empty, null)
    fetcherEventBus.schedule(new DelayedFetcherEvent(200, secondTask))

    val firstTask = RemovePartitions(Set.empty, null)
    fetcherEventBus.schedule(new DelayedFetcherEvent(100, firstTask))

    val service = Executors.newSingleThreadExecutor()

    val expectedSequence = Seq(firstTask, secondTask)

    val actualSequence = ArrayBuffer[FetcherEvent]()
    val future = service.submit(new Runnable {
      override def run(): Unit = {
        for (_ <- 0 until 2) {
          actualSequence += fetcherEventBus.getNextEvent().event
        }
      }
    })

    future.get()
    assertEquals(expectedSequence, actualSequence)
    service.shutdown()
  }

  @Test
  def testBothDelayedAndQueuedEvent(): Unit = {
    val time = new MockTime()
    val condition = new MockCondition
    val fetcherEventBus = new FetcherEventBus(time, new MockConditionFactory(condition))

    val queuedEvent = RemovePartitions(Set.empty, null)
    fetcherEventBus.put(queuedEvent)

    val delay = 10
    val scheduledEvent = AddPartitions(Map.empty, null)
    fetcherEventBus.schedule(new DelayedFetcherEvent(delay, scheduledEvent))

    val service = Executors.newSingleThreadExecutor()

    @volatile var receivedEvents = 0
    val expectedEvents = 2
    val future = service.submit(new Runnable {
      override def run(): Unit = {
        for (_ <- 0 until expectedEvents) {
          fetcherEventBus.getNextEvent()
          receivedEvents += 1
        }
      }
    })

    future.get()
    assertTrue(receivedEvents == expectedEvents)
    service.shutdown()
  }
}
