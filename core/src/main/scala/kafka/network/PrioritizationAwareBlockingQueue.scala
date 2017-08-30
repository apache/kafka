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

package kafka.network

import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{Condition, ReentrantLock}

/**
 * A blocking queue that is prioritization-aware.
 *
 * Prioritized elements in the queue get polled before regular elements in the queue.
 */
private[network] class PrioritizationAwareBlockingQueue[E](private val capacity: Int) {
  private var count: Int = 0
  private var pendingPrioritizedPuts: Int = 0
  private val prioritizedElements: ArrayDeque[E] = new ArrayDeque[E]()
  private val regularElements: ArrayDeque[E] = new ArrayDeque[E]()
  private val lock: ReentrantLock = new ReentrantLock()
  private val notFull: Condition = lock.newCondition()
  private val notEmpty: Condition = lock.newCondition()

  /**
   * Inserts the potentially prioritized element into the queue, blocking if space is unavailable.
   *
   * @param e - non-null element to be inserted
   * @param prioritized - flag indicating whether the element should be prioritized
   * @throws InterruptedException
   */
  def put(e: E, prioritized: Boolean): Unit = {
    if (e == null) {
      throw new NullPointerException()
    }
    lock.lockInterruptibly()
    try {
      if (prioritized) {
        pendingPrioritizedPuts += 1
      }
      while (count == capacity || (!prioritized && pendingPrioritizedPuts > 0)) {
        notFull.await()
      }
      if (prioritized) {
        pendingPrioritizedPuts -= 1
        notFull.signal()
      }
      enqueue(e, prioritized)
    } finally {
      lock.unlock()
    }
  }

  /**
   * Return and remove elements from the head of the queue in a priority-aware manner, blocking up to the provided
   * timeout for elements.
   *
   * That is, prioritized elements in the queue get polled before regular elements in the queue.
   *
   * @param timeout - the maximum amount of time to wait for an item to be polled from the queue
   * @param timeUnit - the unit of time
   * @return potentially prioritized element from the head of the queue
   * @throws InterruptedException
   */
  def poll(timeout: Long, timeUnit: TimeUnit): E = {
    var nanos = timeUnit.toNanos(timeout)
    lock.lockInterruptibly()
    try {
      while (count == 0) {
        if (nanos <= 0) {
          return null.asInstanceOf[E]
        }
        nanos = notEmpty.awaitNanos(nanos)
      }
      dequeue()
    } finally {
      lock.unlock()
    }
  }

  /**
   * Inserts the potentially prioritized element into the queue.
   *
   * Call only within the lock when count < capacity.
   *
   * @param e - non-null element to be inserted
   * @param prioritized - flag indicating whether the element should be prioritized
   */
  private def enqueue(e: E, prioritized: Boolean): Unit = {
    if (prioritized) {
      prioritizedElements.add(e)
    } else {
      regularElements.add(e)
    }
    count += 1
    notEmpty.signal()
  }

  /**
   * Return and remove elements from the head of the queue in a priority-aware manner.
   *
   * Call only within the lock when count > 0.
   * Since count > 0, it's guaranteed that one of the queues is nonempty.
   *
   * @return potentially prioritized element from the head of the queue
   */
  private def dequeue(): E = {
    val e = if (prioritizedElements.isEmpty) regularElements.poll() else prioritizedElements.poll()
    count -= 1
    notFull.signal()
    e
  }

  def size: Int = {
    lock.lock()
    try {
      count
    } finally {
      lock.unlock()
    }
  }

  def clear(): Unit = {
    lock.lock()
    try {
      prioritizedElements.clear()
      regularElements.clear()
      count = 0
      notFull.signalAll()
    } finally {
      lock.unlock()
    }
  }
}
