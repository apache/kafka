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

import scala.collection._
import java.util.concurrent.TimeUnit

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
 * Two gotchas:
 * <ol>
 * <li> Incrementing the time by more than one task period will result in the correct number of executions of each scheduled task
 * but the order of these executions is not specified.
 * <li> Incrementing the time to the exact next execution time of a task will result in that task executing (it as if execution itself takes no time)
 * </ol>
 */
class MockScheduler(val time: Time) extends Scheduler {
  
  var tasks = mutable.ArrayBuffer[MockScheduled]()

  def startup() {}
  
  def shutdown() {
    tasks.clear()
  }
  
  /**
   * Check for any tasks that need to execute. Since this is a mock scheduler this check only occurs
   * when this method is called and the execution happens synchronously in the calling thread.
   * If you are using the scheduler associated with a MockTime instance this call will happen automatically.
   */
  def tick() {
    var tasks = mutable.ArrayBuffer[MockScheduled]()
    val now = time.milliseconds
    for(task <- this.tasks) {
      if(task.nextExecution <= now) {
        if(task.period >= 0) {
          val executions = (now - task.nextExecution) / task.period
          for(i <- 0 to executions.toInt)
            task.fun()
          task.nextExecution += (executions + 1) * task.period
          tasks += task
        } else {
          task.fun()
        }
      } else {
        tasks += task
      }
    }
    this.tasks = tasks
  }
  
  def schedule(name: String, fun: ()=>Unit, delay: Long = 0, period: Long = -1, unit: TimeUnit = TimeUnit.MILLISECONDS) {
    tasks += MockScheduled(name, fun, time.milliseconds + delay, period = period)
    tick()
  }
  
}

case class MockScheduled(val name: String, val fun: () => Unit, var nextExecution: Long, val period: Long)