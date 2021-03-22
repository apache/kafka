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

import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.utils.Time
import java.util.{Comparator, PriorityQueue}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}

import kafka.utils.DelayedItem

/**
 * A QueuedFetcherEvent can be put into the PriorityQueue of the FetcherEventBus
 * and polled according to the priority of the FetcherEvent.
 * If two events have the same priority, the one with the smaller sequence number
 * will be polled first.
 * @param event
 * @param enqueueTimeMs
 * @param sequenceNumber
 */
class QueuedFetcherEvent(val event: FetcherEvent,
  val enqueueTimeMs: Long,
  val sequenceNumber: Long) extends Comparable[QueuedFetcherEvent] {
  override def compareTo(other: QueuedFetcherEvent): Int = {
    val priorityDiff = event.compareTo(other.event)
    if (priorityDiff != 0) {
      priorityDiff
    } else {
      // the event with the smaller sequenceNumber will be polled first
      this.sequenceNumber.compareTo(other.sequenceNumber)
    }
  }
}

/**
 * The SimpleScheduler is not thread safe
 */
class SimpleScheduler[T <: DelayedItem] {
  private val delayedQueue = new PriorityQueue[T](Comparator.naturalOrder[T]())

  def schedule(item: T) : Unit = {
    delayedQueue.add(item)
  }

  /**
   * peek can be used to get the earliest item that has become current.
   * There are 3 cases when peek() is called
   * 1. There are no items whatsoever.  peek would return (None, Long.MaxValue) to indicate that the caller needs to wait
   *    indefinitely until an item is inserted.
   * 2. There are items, and yet none has become current. peek would return (None, delay) where delay represents
   *    the time to wait before the earliest item becomes current.
   * 3. Some item has become current. peek would return (Some(item), 0L)
   */
  def peek(): (Option[T], Long) = {
    if (delayedQueue.isEmpty) {
      (None, Long.MaxValue)
    } else {
      val delayedEvent = delayedQueue.peek()
      val delayMs = delayedEvent.getDelay(TimeUnit.MILLISECONDS)
      if (delayMs == 0) {
        (Some(delayedQueue.peek()), 0L)
      } else {
        (None, delayMs)
      }
    }
  }

  /**
   * poll() unconditionally removes the earliest item
   * If there are no items, poll() has no effect.
   */
  def poll(): Unit = {
    delayedQueue.poll()
  }

  def size = delayedQueue.size
}

/**
 * the ConditionFactory trait is defined such that a MockCondition can be
 * created for the purpose of testing
 */
trait ConditionFactory {
  def createCondition(lock: Lock): Condition
}

object DefaultConditionFactory extends ConditionFactory {
  override def createCondition(lock: Lock): Condition = lock.newCondition()
}

/**
 * The FetcherEventBus supports queued events and delayed events.
 * Queued events are inserted via the {@link #put} method, and delayed events
 * are inserted via the {@link #schedule} method.
 * Events are polled via the {@link #getNextEvent} method, which returns
 * either a queued event or a scheduled event.
 * @param time
 */
class FetcherEventBus(time: Time, conditionFactory: ConditionFactory = DefaultConditionFactory) {
  private val eventLock = new ReentrantLock()
  private val newEventCondition = conditionFactory.createCondition(eventLock)

  private val queue = new PriorityQueue[QueuedFetcherEvent]
  private val nextSequenceNumber = new AtomicLong()
  private val scheduler = new SimpleScheduler[DelayedFetcherEvent]
  @volatile private var shutdownInitialized = false

  def eventQueueSize() = queue.size

  def scheduledEventQueueSize() = scheduler.size

  /**
   * close should be called in a thread different from the one calling getNextEvent()
   */
  def close(): Unit = {
    shutdownInitialized = true
    inLock(eventLock) {
      newEventCondition.signalAll()
    }
  }

  def put(event: FetcherEvent): Unit = {
    inLock(eventLock) {
      queue.add(new QueuedFetcherEvent(event, time.milliseconds(), nextSequenceNumber.getAndIncrement()))
      newEventCondition.signalAll()
    }
  }

  def schedule(delayedEvent: DelayedFetcherEvent): Unit = {
    inLock(eventLock) {
      scheduler.schedule(delayedEvent)
      newEventCondition.signalAll()
    }
  }

  /**
   * There are 2 cases when the getNextEvent() method is called
   * 1. There is at least one delayed event that has become current or at least one queued event. We return either
   * the delayed event with the earliest due time or the queued event, depending on their priority.
   * 2. There are neither delayed events that have become current, nor queued events. We block until the earliest delayed
   * event becomes current. A special case is that there are no delayed events at all, under which the call would block
   * indefinitely until it is waken up by a new delayed or queued event.
   *
   * @return Either a QueuedFetcherEvent or a DelayedFetcherEvent that has become current. A special case is that the
   *         FetcherEventBus is shutdown before an event can be polled, under which null will be returned.
   */
  def getNextEvent(): QueuedFetcherEvent = {
    inLock(eventLock) {
      var result : QueuedFetcherEvent = null

      while (!shutdownInitialized && result == null) {
        // check if any delayed event has become current. If so, move it to the queue
        val (delayedFetcherEvent, delayMs) = scheduler.peek()
        if (delayedFetcherEvent.nonEmpty) {
          scheduler.poll()
          queue.add(new QueuedFetcherEvent(delayedFetcherEvent.get.fetcherEvent, time.milliseconds(), nextSequenceNumber.getAndIncrement()))
        }

        if (!queue.isEmpty) {
          result = queue.poll()
        } else {
          newEventCondition.await(delayMs, TimeUnit.MILLISECONDS)
        }
      }

      result
    }
  }
}
