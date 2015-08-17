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

package kafka.utils

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue}

/**
 * A blocking queue that have size limits on both number of elements and number of bytes.
 */
class ByteBoundedBlockingQueue[E] (val queueNumMessageCapacity: Int, val queueByteCapacity: Int, sizeFunction: Option[(E) => Int])
    extends Iterable[E] {
  private val queue = new LinkedBlockingQueue[E] (queueNumMessageCapacity)
  private var currentByteSize = new AtomicInteger()
  private val putLock = new Object

  /**
   * Please refer to [[java.util.concurrent.BlockingQueue#offer]]
   * An element can be enqueued provided the current size (in number of elements) is within the configured
   * capacity and the current size in bytes of the queue is within the configured byte capacity. i.e., the
   * element may be enqueued even if adding it causes the queue's size in bytes to exceed the byte capacity.
   * @param e the element to put into the queue
   * @param timeout the amount of time to wait before the expire the operation
   * @param unit the time unit of timeout parameter, default to millisecond
   * @return true if the element is put into queue, false if it is not
   * @throws NullPointerException if element is null
   * @throws InterruptedException if interrupted during waiting
   */
  def offer(e: E, timeout: Long, unit: TimeUnit = TimeUnit.MICROSECONDS): Boolean = {
    if (e == null) throw new NullPointerException("Putting null element into queue.")
    val startTime = SystemTime.nanoseconds
    val expireTime = startTime + unit.toNanos(timeout)
    putLock synchronized {
      var timeoutNanos = expireTime - SystemTime.nanoseconds
      while (currentByteSize.get() >= queueByteCapacity && timeoutNanos > 0) {
        // ensure that timeoutNanos > 0, otherwise (per javadoc) we have to wait until the next notify
        putLock.wait(timeoutNanos / 1000000, (timeoutNanos % 1000000).toInt)
        timeoutNanos = expireTime - SystemTime.nanoseconds
      }
      // only proceed if queue has capacity and not timeout
      timeoutNanos = expireTime - SystemTime.nanoseconds
      if (currentByteSize.get() < queueByteCapacity && timeoutNanos > 0) {
        val success = queue.offer(e, timeoutNanos, TimeUnit.NANOSECONDS)
        // only increase queue byte size if put succeeds
        if (success)
          currentByteSize.addAndGet(sizeFunction.get(e))
        // wake up another thread in case multiple threads are waiting
        if (currentByteSize.get() < queueByteCapacity)
          putLock.notify()
        success
      } else {
        false
      }
    }
  }

  /**
   * Please refer to [[java.util.concurrent.BlockingQueue#offer]].
   * Put an element to the tail of the queue, return false immediately if queue is full
   * @param e The element to put into queue
   * @return true on succeed, false on failure
   * @throws NullPointerException if element is null
   * @throws InterruptedException if interrupted during waiting
   */
  def offer(e: E): Boolean = {
    if (e == null) throw new NullPointerException("Putting null element into queue.")
    putLock synchronized {
      if (currentByteSize.get() >= queueByteCapacity) {
        false
      } else {
        val success = queue.offer(e)
        if (success)
          currentByteSize.addAndGet(sizeFunction.get(e))
        // wake up another thread in case multiple threads are waiting
        if (currentByteSize.get() < queueByteCapacity)
          putLock.notify()
        success
      }
    }
  }

  /**
   * Please refer to [[java.util.concurrent.BlockingQueue#put]].
   * Put an element to the tail of the queue, block if queue is full
   * @param e The element to put into queue
   * @return true on succeed, false on failure
   * @throws NullPointerException if element is null
   * @throws InterruptedException if interrupted during waiting
   */
  def put(e: E): Boolean = {
    if (e == null) throw new NullPointerException("Putting null element into queue.")
    putLock synchronized {
      if (currentByteSize.get() >= queueByteCapacity)
        putLock.wait()
      val success = queue.offer(e)
      if (success)
        currentByteSize.addAndGet(sizeFunction.get(e))
      // wake up another thread in case multiple threads are waiting
      if (currentByteSize.get() < queueByteCapacity)
        putLock.notify()
      success
    }
  }

  /**
   * Please refer to [[java.util.concurrent.BlockingQueue#poll]]
   * Get an element from the head of queue. Wait for some time if the queue is empty.
   * @param timeout the amount of time to wait if the queue is empty
   * @param unit the unit type
   * @return the first element in the queue, null if queue is empty
   */
  def poll(timeout: Long, unit: TimeUnit): E = {
    val e = queue.poll(timeout, unit)
    // only wake up waiting threads if the queue size drop under queueByteCapacity
    if (e != null &&
        currentByteSize.getAndAdd(-sizeFunction.get(e)) > queueByteCapacity &&
        currentByteSize.get() < queueByteCapacity)
      putLock.synchronized(putLock.notify())
    e
  }

  /**
   * Please refer to [[java.util.concurrent.BlockingQueue#poll]]
   * Get an element from the head of queue.
   * @return the first element in the queue, null if queue is empty
   */
  def poll(): E = {
    val e = queue.poll()
    // only wake up waiting threads if the queue size drop under queueByteCapacity
    if (e != null &&
      currentByteSize.getAndAdd(-sizeFunction.get(e)) > queueByteCapacity &&
      currentByteSize.get() < queueByteCapacity)
      putLock.synchronized(putLock.notify())
    e
  }

  /**
   * Please refer to [[java.util.concurrent.BlockingQueue#take]]
   * Get an element from the head of the queue, block if the queue is empty
   * @return the first element in the queue, null if queue is empty
   */
  def take(): E = {
    val e = queue.take()
    // only wake up waiting threads if the queue size drop under queueByteCapacity
    if (currentByteSize.getAndAdd(-sizeFunction.get(e)) > queueByteCapacity &&
        currentByteSize.get() < queueByteCapacity)
      putLock.synchronized(putLock.notify())
    e
  }

  /**
   * Iterator for the queue
   * @return Iterator for the queue
   */
  override def iterator = new Iterator[E] () {
    private val iter = queue.iterator()
    private var curr: E = null.asInstanceOf[E]

    def hasNext: Boolean = iter.hasNext

    def next(): E = {
      curr = iter.next()
      curr
    }

    def remove() {
      if (curr == null)
        throw new IllegalStateException("Iterator does not have a current element.")
      iter.remove()
      if (currentByteSize.addAndGet(-sizeFunction.get(curr)) < queueByteCapacity)
        putLock.synchronized(putLock.notify())
    }
  }

  /**
   * get the number of elements in the queue
   * @return number of elements in the queue
   */
  override def size() = queue.size()

  /**
   * get the current byte size in the queue
   * @return current queue size in bytes
   */
  def byteSize() = {
    val currSize = currentByteSize.get()
    // There is a potential race where after an element is put into the queue and before the size is added to
    // currentByteSize, it was taken out of the queue and the size was deducted from the currentByteSize,
    // in that case, currentByteSize would become negative, in that case, just put the queue size to be 0.
    if (currSize > 0) currSize else 0
  }

  /**
   * get the number of unused slots in the queue
   * @return the number of unused slots in the queue
   */
  def remainingSize = queue.remainingCapacity()

  /**
   * get the remaining bytes capacity of the queue
   * @return the remaining bytes capacity of the queue
   */
  def remainingByteSize = math.max(0, queueByteCapacity - currentByteSize.get())

  /**
   * remove all the items in the queue
   */
  def clear() {
    putLock synchronized {
      queue.clear()
      currentByteSize.set(0)
      putLock.notify()
    }
  }
}
