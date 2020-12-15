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

import scala.collection.mutable.PriorityQueue
import java.util.concurrent.{Delayed, ScheduledFuture, TimeUnit}

import org.apache.kafka.common.utils.Time

/**
 * A mock scheduler that executes tasks synchronously using a mock time instance. Tasks are executed synchronously when
 * the time is advanced. This class is meant to be used in conjunction with MockTime.
 * 
 * Example usage
 * <code>
 *   val time = new MockTime
 *   time.scheduler.schedule("a task", println("hello world: " + time.milliseconds), delay = 1000)
 *   time.sleep(1001) // this should cause our scheduled task to fire
 * </code>
 *   
 * Incrementing the time to the exact next execution time of a task will result in that task executing (it as if execution itself takes no time).
 */
class MockScheduler(val time: Time) extends Scheduler {

  /* a priority queue of tasks ordered by next execution time */
  private val tasks = new PriorityQueue[MockTask]()
  
  def isStarted = true

  def startup(): Unit = {}
  
  def shutdown(): Unit = {
    var currTask: Option[MockTask] = None
    do {
      currTask = poll(_ => true)
      currTask.foreach(_.fun())
    } while (currTask.nonEmpty)
  }
  
  /**
   * Check for any tasks that need to execute. Since this is a mock scheduler this check only occurs
   * when this method is called and the execution happens synchronously in the calling thread.
   * If you are using the scheduler associated with a MockTime instance this call be triggered automatically.
   */
  def tick(): Unit = {
    val now = time.milliseconds
    var currTask: Option[MockTask] = None
    /* pop and execute the task with the lowest next execution time if ready */
    do {
      currTask = poll(_.nextExecution <= now)
      currTask.foreach { curr =>
        curr.fun()
        /* if the task is periodic, reschedule it and re-enqueue */
        if(curr.periodic) {
          curr.nextExecution += curr.period
          add(curr)
        }
      }
    } while (currTask.nonEmpty)
  }

  def schedule(name: String, fun: () => Unit, delay: Long = 0, period: Long = -1, unit: TimeUnit = TimeUnit.MILLISECONDS): ScheduledFuture[Unit] = {
    val task = MockTask(name, fun, time.milliseconds + delay, period = period, time=time)
    add(task)
    tick()
    task
  }

  def clear(): Unit = {
    this synchronized {
      tasks.clear()
    }
  }

  private def poll(predicate: MockTask => Boolean): Option[MockTask] = {
    this synchronized {
      if (tasks.nonEmpty && predicate.apply(tasks.head))
        Some(tasks.dequeue())
      else
        None
    }
  }

  private def add(task: MockTask): Unit = {
    this synchronized {
      tasks += task
    }
  }
}

case class MockTask(name: String, fun: () => Unit, var nextExecution: Long, period: Long, time: Time) extends ScheduledFuture[Unit] {
  def periodic = period >= 0
  def compare(t: MockTask): Int = {
    if(t.nextExecution == nextExecution)
      0
    else if (t.nextExecution < nextExecution)
      -1
    else
      1
  }

  /**
    * Not used, so not not fully implemented
    */
  def cancel(mayInterruptIfRunning: Boolean) : Boolean = {
    false
  }

  def get(): Unit = {
  }

  def get(timeout: Long, unit: TimeUnit): Unit = {
  }

  def isCancelled: Boolean = {
    false
  }

  def isDone: Boolean = {
    false
  }

  def getDelay(unit: TimeUnit): Long = {
    this synchronized {
      time.milliseconds - nextExecution
    }
  }

  def compareTo(o : Delayed) : Int = {
    this.getDelay(TimeUnit.MILLISECONDS).compareTo(o.getDelay(TimeUnit.MILLISECONDS))
  }
}
object MockTask {
  implicit def MockTaskOrdering: Ordering[MockTask] = (x, y) => x.compare(y)
}
