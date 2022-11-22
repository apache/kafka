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

import java.util.concurrent._
import atomic._
import org.apache.kafka.common.utils.KafkaThread

import java.util.concurrent.TimeUnit.NANOSECONDS

/**
 * A scheduler for running jobs
 * 
 * This interface controls a job scheduler that allows scheduling either repeating background jobs 
 * that execute periodically or delayed one-time actions that are scheduled in the future.
 */
trait Scheduler {
  
  /**
   * Initialize this scheduler so it is ready to accept scheduling of tasks
   */
  def startup(): Unit
  
  /**
   * Shutdown this scheduler. When this method is complete no more executions of background tasks will occur. 
   * This includes tasks scheduled with a delayed execution.
   */
  def shutdown(): Unit
  
  /**
   * Check if the scheduler has been started
   */
  def isStarted: Boolean
  
  /**
   * Schedule a task
   * @param name The name of this task
   * @param delay The amount of time to wait before the first execution
   * @param period The period with which to execute the task. If < 0 the task will execute only once.
   * @param unit The unit for the preceding times.
   * @return A Future object to manage the task scheduled.
   */
  def schedule(name: String, fun: ()=>Unit, delay: Long = 0, period: Long = -1, unit: TimeUnit = TimeUnit.MILLISECONDS) : ScheduledFuture[_]
}

/**
 * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
 * 
 * It has a pool of kafka-scheduler- threads that do the actual work.
 * 
 * @param threads The number of threads in the thread pool
 * @param threadNamePrefix The name to use for scheduler threads. This prefix will have a number appended to it.
 * @param daemon If true the scheduler threads will be "daemon" threads and will not block jvm shutdown.
 */
@threadsafe
class KafkaScheduler(val threads: Int, 
                     val threadNamePrefix: String = "kafka-scheduler-", 
                     daemon: Boolean = true) extends Scheduler with Logging {
  private var executor: ScheduledThreadPoolExecutor = _
  private val schedulerThreadId = new AtomicInteger(0)

  override def startup(): Unit = {
    debug("Initializing task scheduler.")
    this synchronized {
      if(isStarted)
        throw new IllegalStateException("This scheduler has already been started!")
      executor = new ScheduledThreadPoolExecutor(threads)
      executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false)
      executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
      executor.setRemoveOnCancelPolicy(true)
      executor.setThreadFactory(runnable =>
        new KafkaThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), runnable, daemon))
    }
  }
  
  override def shutdown(): Unit = {
    debug("Shutting down task scheduler.")
    // We use the local variable to avoid NullPointerException if another thread shuts down scheduler at same time.
    val cachedExecutor = this.executor
    if (cachedExecutor != null) {
      this synchronized {
        cachedExecutor.shutdown()
        this.executor = null
      }
      cachedExecutor.awaitTermination(1, TimeUnit.DAYS)
    }
  }

  def scheduleOnce(name: String, fun: () => Unit): Unit = {
    schedule(name, fun, delay = 0L, period = -1L, unit = TimeUnit.MILLISECONDS)
  }

  def schedule(name: String, fun: () => Unit, delay: Long, period: Long, unit: TimeUnit): ScheduledFuture[_] = {
    debug("Scheduling task %s with initial delay %d ms and period %d ms."
        .format(name, TimeUnit.MILLISECONDS.convert(delay, unit), TimeUnit.MILLISECONDS.convert(period, unit)))
    this synchronized {
      if (isStarted) {
        val runnable: Runnable = () => {
          try {
            trace("Beginning execution of scheduled task '%s'.".format(name))
            fun()
          } catch {
            case t: Throwable => error(s"Uncaught exception in scheduled task '$name'", t)
          } finally {
            trace("Completed execution of scheduled task '%s'.".format(name))
          }
        }
        if (period > 0)
          executor.scheduleAtFixedRate(runnable, delay, period, unit)
        else
          executor.schedule(runnable, delay, unit)
      } else {
        info("Kafka scheduler is not running at the time task '%s' is scheduled. The task is ignored.".format(name))
        new NoOpScheduledFutureTask
      }
    }
  }

  /**
   * Package private for testing.
   */
  private[kafka] def taskRunning(task: ScheduledFuture[_]): Boolean = {
    executor.getQueue.contains(task)
  }

  def resizeThreadPool(newSize: Int): Unit = {
    executor.setCorePoolSize(newSize)
  }
  
  def isStarted: Boolean = {
    this synchronized {
      executor != null
    }
  }
}

private class NoOpScheduledFutureTask() extends ScheduledFuture[Unit] {
  override def cancel(mayInterruptIfRunning: Boolean): Boolean = true
  override def isCancelled: Boolean = true
  override def isDone: Boolean = true
  override def get(): Unit = {}
  override def get(timeout: Long, unit: TimeUnit): Unit = {}
  override def getDelay(unit: TimeUnit): Long = 0
  override def compareTo(o: Delayed): Int = {
    val diff = getDelay(NANOSECONDS) - o.getDelay(NANOSECONDS)
    if (diff < 0) -1
    else if (diff > 0) 1
    else 0
  }
}
