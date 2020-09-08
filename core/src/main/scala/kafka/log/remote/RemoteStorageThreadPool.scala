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

package kafka.log.remote

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{Exit, Logging}
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.Time

abstract class RemoteStorageTask[T] extends Callable[T] with Logging {
  override final def call(): T = {
    this.logIdent = s"[${Thread.currentThread.getName}]: "
    execute()
  }

  def execute(): T
}

/**
 * A thread pool with a fixed number of threads, used by RemoteLogManager
 *
 * @param name Name of the thread pool
 * @param numThreads Number of threads
 * @param maxPendingTasks The task queue capacity. If the task queue is full, the submit() / execute() method will throw RejectedExecutionException
 * @param metricNamePrefix The name of average idle percentage metric
 */
abstract class RemoteStorageThreadPool(name: String, threadNamePrefix: String, numThreads: Int, maxPendingTasks: Int, time: Time, metricNamePrefix: String)
  extends ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable](maxPendingTasks), new RemoteStorageThreadFactory(threadNamePrefix + "-"))
    with Logging
    with KafkaMetricsGroup {
  newGauge(metricNamePrefix.concat("TaskQueueSize"), new Gauge[Int] {
    def value() = {
      getQueue().size()
    }
  })

  newGauge(metricNamePrefix.concat("AvgIdlePercent"), new Gauge[Double] {
    def value() = {
      1 - getActiveCount.asInstanceOf[Double] / getCorePoolSize.asInstanceOf[Double]
    }
  })

  this.logIdent = s"[${name}] "

  override def afterExecute(r: Runnable, e: Throwable): Unit = {
    if(e != null)
      e match {
      case e: FatalExitError => {
        info("Stopped")
        Exit.exit(e.statusCode())
      }
      case e: Throwable => {
        if (!isShutdown)
          error("Error due to", e)
      }
    }
  }

  def resizeThreadPool(newSize: Int): Unit = synchronized {
    val currentSize = getCorePoolSize
    if (newSize > currentSize) {
      setMaximumPoolSize(newSize)
      setCorePoolSize(newSize)
    } else if (newSize < currentSize) {
      setCorePoolSize(newSize)
      setMaximumPoolSize(newSize)
    }
  }

  override def shutdown(): Unit = synchronized {
    info("shutting down")
    shutdownNow()
    while (!awaitTermination(5, TimeUnit.SECONDS)) {
      info("shutting down")
    }
    info("shut down completely")
  }
}

class RemoteStorageThreadFactory(namePrefix : String) extends ThreadFactory {
  private val threadNumber = new AtomicInteger(0)

  override def newThread(r: Runnable): Thread = {
    new Thread(r, namePrefix + threadNumber.getAndIncrement)
  }
}
