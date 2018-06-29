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

import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import kafka.server.PurgatoryTimeoutHandler.MaximumWaitNs
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time

import math.{max, min}
import scala.collection.JavaConverters._
import scala.collection.mutable


trait Delayable {
  /**
    * This function is invoked exactly once from a purgatory service thread when
    * the delayable is completed.  It must be thread-safe.
    *
    * @param timedOut   True if the delayable timed out.
    */
  def complete(timedOut: Boolean): Unit

  /**
    * This function is invoked from a purgatory service thread.  It must be thread-safe.
    * It should return true if the delayable can be completed immediately.
    */
  def check(): Boolean
}

/**
  * Data about a delayable
  */
final class DelayableData(val expirationTimeNs: Long,
                          val watchKeys: Seq[Any],
                          var expired: Boolean = false,
                          var shouldCheck: Boolean = false,
                          var checking: Boolean = false)
    extends Comparable[DelayableData] {
  override def compareTo(other: DelayableData) = {
    if (expirationTimeNs < other.expirationTimeNs) -1
    else if (expirationTimeNs > other.expirationTimeNs) 1
    else 0
  }
}

final class WatchIndex(val watchKey: Any, val expirationTimeNs: Long) extends Comparable[WatchIndex] {
  override def compareTo(other: WatchIndex) = {
    val hashCode = watchKey.hashCode()
    val otherHashCode = other.watchKey.hashCode()
    if (hashCode < otherHashCode) -1
    else if (hashCode > otherHashCode) 1
    else if (expirationTimeNs < other.expirationTimeNs) -1
    else if (expirationTimeNs > other.expirationTimeNs) 1
    else 0
  }
}

object WatchMultiMap {
  type InternalIterator = util.Iterator[util.Map.Entry[WatchIndex, Delayable]]
}

/**
  * Maps watch keys to sets of delayables.
  */
final class WatchMultiMap {
  val map = new util.TreeMap[WatchIndex, Delayable]

  def iterator(watchKey: Any) = new WatchMapIterator(watchKey,
    map.tailMap(new WatchIndex(watchKey, 0)).entrySet().iterator())

  def put(data: DelayableData, delayable: Delayable): Unit = {
    data.watchKeys.foreach(watchKey =>
      put(new WatchIndex(watchKey, data.expirationTimeNs), delayable)
    )
  }

  def put(watchIndex: WatchIndex, delayable: Delayable): Unit = map.put(watchIndex, delayable)

  def remove(watchIndex: WatchIndex, delayable: Delayable): Boolean = map.remove(watchIndex, delayable)

  def remove(data: DelayableData, delayable: Delayable): Unit = {
    data.watchKeys.foreach(watchKey =>
      remove(new WatchIndex(watchKey, data.expirationTimeNs), delayable)
    )
  }
}

final class WatchMapIterator(val watchKey: Any, var iter: WatchMultiMap.InternalIterator)
      extends util.Iterator[Delayable] {

  private[this] var nextVal: Delayable = null

  override def hasNext(): Boolean = {
    while (nextVal.eq(null)) {
      if (iter.eq(null) || !iter.hasNext()) {
        return false
      }
      val nextEntry = iter.next()
      val entryKey = nextEntry.getKey.watchKey
      if (entryKey.equals(watchKey)) {
        nextVal = nextEntry.getValue
      } else if (entryKey.hashCode() != watchKey.hashCode()) {
        iter = null
      }
    }
    true
  }

  override def next(): Delayable = {
    if (!hasNext())
      throw new NoSuchElementException()
    val temp = nextVal
    nextVal = null
    temp
  }
}

final class ThreadedPurgatory(val time: Time,
                              val purgatoryName: String = "ThreadedPurgatory",
                              numThreads: Int = 5) extends AutoCloseable with Logging {
  /**
    * The lock which protects purgatory data structures.
    */
  private [server] val lock = new ReentrantLock()

  /**
    * The condition variable used by the timeout thread.
    */
  private [server] val timeoutCond = lock.newCondition()

  /**
    * The condition variable used by the purgatory service threads.
    */
  private [server] val serviceCond = lock.newCondition()

  /**
    * True if the purgatory is shutting down.  Protected by the lock.
    */
  private [server] var shutdown = false

  /**
    * A list of delayables that we should check.  Protected by the lock.
    */
  private [server] val shouldCheck = new util.LinkedList[Delayable]

  /**
    * Maps delayables to delayable data.  Protected by the lock.
    */
  private [server] val delayables = new util.IdentityHashMap[Delayable, DelayableData]()

  /**
    * A sorted map containing expiration times.  Protected by the lock.
    */
  private [server] val next = new util.TreeMap[DelayableData, Delayable]()

  /**
    * Maps watch keys to sets of deployables.  Protected by the lock.
    */
  private [this] val watchMap = new WatchMultiMap()

  /**
    * The timeout thread which keeps track of expiration times.
    */
  private [this] val timeoutThread = {
    val thread = new Thread(new PurgatoryTimeoutHandler(this),
      s"${purgatoryName}TimeoutHandler")
    thread.start
    thread
  }

  /**
    * The service threads which check delayables.
    */
  private [this] val serviceThreads = {
    val threads = mutable.ArrayBuffer[Thread]()
    for (threadIndex <- 0 to numThreads) {
      val thread = new Thread(new PurgatoryServiceHandler(this),
        s"${purgatoryName}ServiceHandler${threadIndex}")
      threads += thread
      thread.start
    }
    threads
  }

  def tryCompleteElseRegister(delayable: Delayable, watchKeys: Seq[_ <: Any],
                              timeout: Long, timeUnit: TimeUnit) = {
    if (delayable.check()) {
      delayable.complete(false)
    } else {
      register(delayable, watchKeys, timeout, timeUnit)
    }
  }

  def register(delayable: Delayable, watchKeys: Seq[_ <: Any], timeout: Long,
               timeUnit: TimeUnit = TimeUnit.NANOSECONDS) = {
    // Get the time in nanoseconds without holding the object lock.
    val timeoutNs = TimeUnit.NANOSECONDS.convert(timeout, timeUnit)
    var expirationTimeNs = time.nanoseconds() + timeoutNs
    lock.lock()
    try {
      if (!delayables.containsKey(delayable)) {
        val prevNextWakeNs = if (next.isEmpty) Long.MaxValue
          else next.firstKey().expirationTimeNs
        val data = insertDelayableData(expirationTimeNs, delayable, watchKeys)
        delayables.put(delayable, data)
        watchMap.put(data, delayable)
        logger.trace("{} registering delayable {}", purgatoryName, delayable.hashCode())
        if (data.expirationTimeNs < prevNextWakeNs) {
          timeoutCond.signal() // Wake up the timeout thread if necessary.
        }
      }
    } finally {
      lock.unlock()
    }
  }

  def unregister(delayable: Delayable, data: DelayableData) = {
    lock.lock()
    try {
      unregisterInternal(delayable, data)
    } finally {
      lock.unlock()
    }
  }

  private [server] def unregisterInternal(delayable: Delayable, data: DelayableData) = {
    logger.trace("{} unregistering delayable {}", purgatoryName, delayable.hashCode())
    watchMap.remove(data, delayable)
    next.remove(data)
    delayables.remove(delayable)
  }

  def scheduleCheck(delayable: Delayable): Boolean = {
    lock.lock()
    try {
      scheduleCheckInternal(delayable)
    } finally {
      lock.unlock()
    }
  }

  private [server] def scheduleCheckInternal(delayable: Delayable): Boolean = {
    var startedCheck = false
    val data = delayables.get(delayable)
    if (data != null) {
      if (!data.shouldCheck) {
        data.shouldCheck = true
        if (!data.checking) {
          shouldCheck.add(delayable)
          serviceCond.signal()
          startedCheck = true
        }
      }
    }
    logger.trace("{} scheduling a check of delayable {}", purgatoryName, delayable.hashCode())
    startedCheck
  }

  def scheduleWatchKeysCheck(watchKeys: Seq[Any]): Int = {
    lock.lock()
    try {
      scheduleWatchKeysCheckInternal(watchKeys)
    } finally {
      lock.unlock()
    }
  }

  private [server] def scheduleWatchKeysCheckInternal(watchKeys: Seq[Any]): Int = {
    var checkCount = 0
    watchKeys.foreach(watchKey => {
      var iter = watchMap.iterator(watchKey)
      while (iter.hasNext()) {
        if (scheduleCheckInternal(iter.next())) {
          checkCount = checkCount + 1
        }
      }
    })
    checkCount
  }

  /**
    * Create a DelayableData object corresponding to the delayable.
    * Insert this data object into the 'next' map.
    * This method must be called while holding the lock.
    */
  private [this] def insertDelayableData(expirationTimeNs: Long,
                          delayable: Delayable,
                          watchKeys: Seq[Any]): DelayableData = {
    var data: DelayableData = null
    var effectiveExpirationTimeNs = expirationTimeNs
    do {
      data = new DelayableData(effectiveExpirationTimeNs, watchKeys)
      effectiveExpirationTimeNs = effectiveExpirationTimeNs + 1
      // Each nanosecond key can only map to one DelayableData object.
      // If someone is already using the nanonsecond key that we wanted to use,
      // increment by one and try again.
    } while (next.putIfAbsent(data, delayable) != null)
    data
  }

  def contains(delayable: Delayable): Boolean = {
    lock.lock()
    try {
      delayables.containsKey(delayable)
    } finally {
      lock.unlock()
    }
  }

  /**
    * Shut down all threads and call onExpiration for all pending delayables.
    */
  override def close(): Unit = {
    try {
      lock.lock()
      try {
        if (shutdown) {
          return
        }
        logger.debug("Closing {}", purgatoryName)
        shutdown = true
        timeoutCond.signal()
        serviceCond.signalAll()
      } finally {
        lock.unlock()
      }
      timeoutThread.join()
      serviceThreads.foreach(thread => thread.join())
      lock.lock()
      try {
        delayables.keySet().asScala.foreach(delayable => delayable.complete(true))
        delayables.clear()
      } finally {
        lock.unlock()
      }
    } catch {
      case t: Throwable =>
        logger.error("Error closing {}", purgatoryName, t)
    }
  }
}

object PurgatoryTimeoutHandler {
  private [server] val MaximumWaitNs = TimeUnit.NANOSECONDS.convert(1, TimeUnit.MINUTES)
}

/**
  * The handler for purgatory timeouts.
  *
  * This runnable wakes the service threads when a timeout occurs.
  */
final class PurgatoryTimeoutHandler(val purgatory: ThreadedPurgatory)
    extends Runnable with Logging {

  override def run(): Unit = {
    try {
      while (true) {
        val currentTimeNs = purgatory.time.nanoseconds()
        purgatory.lock.lock()
        try {
          if (purgatory.shutdown) {
            return
          }
          var entry = purgatory.next.firstEntry()
          while ((entry != null) && (entry.getKey.expirationTimeNs <= currentTimeNs)) {
            val data = entry.getKey
            if (!data.shouldCheck) {
              data.shouldCheck = true
              if (!data.checking) {
                purgatory.shouldCheck.add(entry.getValue)
              }
            }
            data.expired = true
            purgatory.next.remove(entry.getKey)
            entry = purgatory.next.firstEntry()
            purgatory.serviceCond.signal() // wake up a service thread
          }
          if (entry.eq(null))
            purgatory.timeoutCond.await(MaximumWaitNs, TimeUnit.NANOSECONDS)
          else
            purgatory.timeoutCond.await(
              min(max(1, entry.getKey.expirationTimeNs - currentTimeNs), MaximumWaitNs),
              TimeUnit.NANOSECONDS)
        } finally {
          purgatory.lock.unlock()
        }
      }
    } catch {
      case t: Throwable =>
        logger.error("{} exiting with exception",  Thread.currentThread().getName(), t)
    }
  }
}

final class PurgatoryServiceHandler(val purgatory: ThreadedPurgatory)
  extends Runnable with Logging {

  override def run(): Unit = {
    try {
      var delayable: Delayable = null
      var purge = false
      while (true) {
        var expirationTimeNs = 0L
        purgatory.lock.lock()
        try {
          if (delayable != null) {
            // Change the "checking" flag on the delayable we just processed to
            // false, to indicate that we're done checking it.
            val data = purgatory.delayables.get(delayable)
            data.checking = false
            if (purge) {
              // If the delayable completed or timed out, remove it from our data structures.
              purgatory.unregisterInternal(delayable, data)
            } else if (data.shouldCheck) {
              // If a new check on the delayable was requested while we were in the
              // process of checking it, add it to the end of the checking queue.
              // Some thread will check it after the currently queued work is done.
              purgatory.shouldCheck.add(delayable)
            }
            delayable = null
          }
          do {
            if (purgatory.shutdown) {
              return
            }
            delayable = purgatory.shouldCheck.pollFirst()
            if (delayable.eq(null)) {
              purgatory.serviceCond.await(MaximumWaitNs, TimeUnit.NANOSECONDS)
            }
          } while (delayable.eq(null))
          val data = purgatory.delayables.get(delayable)
          // Mark this delayable as checking.  If another request to check this delayable
          // comes in while we are in the process of checking, we will let the current check
          // complete first, then queue a new one.
          data.shouldCheck = false
          data.checking = true
          expirationTimeNs = if (data.expired) Long.MinValue else data.expirationTimeNs
        } finally {
          purgatory.lock.unlock()
        }
        val currentTimeNs = purgatory.time.nanoseconds()
        purge = if (expirationTimeNs <= currentTimeNs) {
          // Expire the delayable.  It will be purged as soon as we can grab the lock.
          delayable.complete(true)
          true
        } else if (delayable.check()) {
          delayable.complete(false)
          true
        } else {
          false
        }
      }
    } catch {
      case t: Throwable =>
        logger.error("{} exiting with exception", Thread.currentThread().getName, t)
    }
  }
}
