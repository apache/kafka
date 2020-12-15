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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{DelayQueue, Executors, TimeUnit}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{KafkaThread, Time}

trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
    */
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  def shutdown(): Unit
}

@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  // timeout timer
  private[this] val taskExecutor = Executors.newFixedThreadPool(1,
    (runnable: Runnable) => KafkaThread.nonDaemon("executor-" + executorName, runnable))

  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  private[this] val taskCounter = new AtomicInteger(0)
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      writeLock.lock()
      try {
        while (bucket != null) {
          timingWheel.advanceClock(bucket.getExpiration)
          bucket.flush(addTimerTaskEntry)
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown(): Unit = {
    taskExecutor.shutdown()
  }

}
