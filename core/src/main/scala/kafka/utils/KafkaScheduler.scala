/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.concurrent.atomic._
import kafka.utils._
import org.apache.log4j.Logger

/**
 * A scheduler for running jobs in the background
 * TODO: ScheduledThreadPoolExecutor notriously swallows exceptions
 */
class KafkaScheduler(val numThreads: Int, val baseThreadName: String, isDaemon: Boolean) {
  private val logger = Logger.getLogger(getClass())
  private val threadId = new AtomicLong(0)
  private val executor = new ScheduledThreadPoolExecutor(numThreads, new ThreadFactory() {
    def newThread(runnable: Runnable): Thread = {
      val t = new Thread(runnable, baseThreadName + threadId.getAndIncrement)
      t.setDaemon(isDaemon)
      t
    }
  })
  
  def scheduleWithRate(fun: () => Unit, delayMs: Long, periodMs: Long) =
    executor.scheduleAtFixedRate(Utils.loggedRunnable(fun), delayMs, periodMs, TimeUnit.MILLISECONDS)

  def shutdown() = {
    executor.shutdownNow
    logger.info("shutdown scheduler " + baseThreadName)
  }
}
