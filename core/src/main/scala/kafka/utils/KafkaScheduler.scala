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
import collection.mutable.HashMap

/**
 * A scheduler for running jobs in the background
 */
class KafkaScheduler(val numThreads: Int) extends Logging {
  private var executor:ScheduledThreadPoolExecutor = null
  private val daemonThreadFactory = new ThreadFactory() {
      def newThread(runnable: Runnable): Thread = Utils.newThread(runnable, true)
    }
  private val nonDaemonThreadFactory = new ThreadFactory() {
      def newThread(runnable: Runnable): Thread = Utils.newThread(runnable, false)
    }
  private val threadNamesAndIds = new HashMap[String, AtomicInteger]()

  def startUp = {
    executor = new ScheduledThreadPoolExecutor(numThreads)
    executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false)
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
  }

  def hasShutdown: Boolean = executor.isShutdown

  private def ensureExecutorHasStarted = {
    if(executor == null)
      throw new IllegalStateException("Kafka scheduler has not been started")
  }

  def scheduleWithRate(fun: () => Unit, name: String, delayMs: Long, periodMs: Long, isDaemon: Boolean = true) = {
    ensureExecutorHasStarted
    if(isDaemon)
      executor.setThreadFactory(daemonThreadFactory)
    else
      executor.setThreadFactory(nonDaemonThreadFactory)
    val threadId = threadNamesAndIds.getOrElseUpdate(name, new AtomicInteger(0))
    executor.scheduleAtFixedRate(Utils.loggedRunnable(fun, name + threadId.incrementAndGet()), delayMs, periodMs,
      TimeUnit.MILLISECONDS)
  }

  def shutdownNow() {
    ensureExecutorHasStarted
    executor.shutdownNow()
    info("Forcing shutdown of Kafka scheduler")
  }

  def shutdown() {
    ensureExecutorHasStarted
    executor.shutdown()
    info("Shutdown Kafka scheduler")
  }
}
