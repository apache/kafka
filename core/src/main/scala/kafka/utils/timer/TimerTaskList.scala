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
package kafka.utils.timer

import java.util.concurrent.{TimeUnit, Delayed}
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

import kafka.utils.{SystemTime, threadsafe}

import scala.math._

@threadsafe
private[timer] class TimerTaskList(taskCounter: AtomicInteger) extends Delayed {

  // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
  // root.next points to the head
  // root.prev points to the tail
  private[this] val root = new TimerTaskEntry(null)
  root.next = root
  root.prev = root

  private[this] val expiration = new AtomicLong(-1L)

  // Set the bucket's expiration time
  // Returns true if the expiration time is changed
  def setExpiration(expirationMs: Long): Boolean = {
    expiration.getAndSet(expirationMs) != expirationMs
  }

  // Get the bucket's expiration time
  def getExpiration(): Long = {
    expiration.get()
  }

  // Apply the supplied function to each of tasks in this list
  def foreach(f: (TimerTask)=>Unit): Unit = {
    synchronized {
      var entry = root.next
      while (entry ne root) {
        val nextEntry = entry.next
        f(entry.timerTask)
        entry = nextEntry
      }
    }
  }

  // Add a timer task entry to this list
  def add(timerTaskEntry: TimerTaskEntry): Unit = {
    synchronized {
      // put the timer task entry to the end of the list. (root.prev points to the tail entry)
      val tail = root.prev
      timerTaskEntry.next = root
      timerTaskEntry.prev = tail
      timerTaskEntry.list = this
      tail.next = timerTaskEntry
      root.prev = timerTaskEntry
      taskCounter.incrementAndGet()
    }
  }

  // Remove the specified timer task entry from this list
  def remove(timerTaskEntry: TimerTaskEntry): Unit = {
    synchronized {
      if (timerTaskEntry.list != null) {
        timerTaskEntry.next.prev = timerTaskEntry.prev
        timerTaskEntry.prev.next = timerTaskEntry.next
        timerTaskEntry.next = null
        timerTaskEntry.prev = null
        timerTaskEntry.list = null
        taskCounter.decrementAndGet()
      }
    }
  }

  // Remove all task entries and apply the supplied function to each of them
  def flush(f: (TimerTaskEntry)=>Unit): Unit = {
    synchronized {
      var head = root.next
      while (head ne root) {
        remove(head)
        f(head)
        head = root.next
      }
      expiration.set(-1L)
    }
  }

  def getDelay(unit: TimeUnit): Long = {
    unit.convert(max(getExpiration - SystemTime.milliseconds, 0), TimeUnit.MILLISECONDS)
  }

  def compareTo(d: Delayed): Int = {

    val other = d.asInstanceOf[TimerTaskList]

    if(getExpiration < other.getExpiration) -1
    else if(getExpiration > other.getExpiration) 1
    else 0
  }

}

private[timer] class TimerTaskEntry(val timerTask: TimerTask) {

  var list: TimerTaskList = null
  var next: TimerTaskEntry = null
  var prev: TimerTaskEntry = null

  // if this timerTask is already held by an existing timer task entry,
  // setTimerTaskEntry will remove it.
  if (timerTask != null) timerTask.setTimerTaskEntry(this)

  def remove(): Unit = {
    if (list != null) list.remove(this)
  }

}

