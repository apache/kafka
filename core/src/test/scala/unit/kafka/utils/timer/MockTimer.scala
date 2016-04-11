/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.utils.timer

import org.apache.kafka.common.utils.Time

import scala.collection.mutable

class MockTimer(time: Time) extends Timer {

  private val taskQueue = mutable.PriorityQueue[TimerTaskEntry]()

  def add(timerTask: TimerTask) {
    if (timerTask.delayMs <= 0)
      timerTask.run()
    else
      taskQueue.enqueue(new TimerTaskEntry(timerTask, timerTask.delayMs + time.milliseconds))
  }

  def advanceClock(timeoutMs: Long): Boolean = {
    var remaining = timeoutMs
    var executed = false

    do {
      val now = time.milliseconds
      while (taskQueue.nonEmpty && now > taskQueue.head.expirationMs) {
        val taskEntry = taskQueue.dequeue()
        if (!taskEntry.cancelled) {
          val task = taskEntry.timerTask
          task.run()
          executed = true
        }
      }

      if (taskQueue.nonEmpty) {
        val sleepDuration = Math.min(remaining, taskQueue.head.expirationMs - now)
        time.sleep(sleepDuration)
        remaining -= sleepDuration
      } else {
        time.sleep(remaining)
        remaining = 0
      }
    } while (remaining > 0)

    executed
  }

  def size(): Int = taskQueue.size

  override def shutdown(): Unit = {}

}
