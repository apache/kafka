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
package kafka.controller

import java.util.concurrent.{Future, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import kafka.utils._

/**
 * A timeout which will run at some time in the future-- if it is not cancelled first.
 */
case class Timeout(val timeoutCallback: () => Unit) {
  val future = new AtomicReference[Future[_]]

  /**
   * Cancel the scheduled timeout.
   *
   * @returns true if the timeout has not run yet.
   *          false if the timeout has already run or been cancelled.
   */
  def cancel(): Boolean = {
    val installedFuture = future.getAndSet(null)
    if (installedFuture == null) {
      false
    } else {
      installedFuture.cancel(false)
      true
    }
  }

  def attemptTimeout(): Unit = {
    if (cancel()) {
      timeoutCallback()
    }
  }
}

/**
 * A manager for callbacks made from the controller back into other systems.
 * The callback manager has its own worker thread which will not block the main controller thread.
 */
class CallbackManager(val threadNamePrefix: String) extends Logging {
  private[controller] val scheduler = new KafkaScheduler(1, threadNamePrefix)

  def startup() = {
    debug(s"$threadNamePrefix: Starting")
    scheduler.startup()
  }

  /**
   * Register a failure callback which will happen after a certain delay.
   * The callback will be run in the CallbackManager's scheduler thread.
   *
   * @param delayMs          The delay before the failure callback should run.
   * @param timeoutCallback  The timeout callback itself.
   * @return                 A timeout object that can be cancelled exactly once.
   */
  def registerTimeout(prefix: String, delayMs: Int, timeoutCallback: () => Unit): Timeout = {
    val timeout = Timeout(timeoutCallback)
    try {
      val future = scheduler.schedule(prefix + "Cancellation",
        timeout.attemptTimeout, delayMs, -1, TimeUnit.MILLISECONDS)
      timeout.future.set(future)
      if (future.isDone()) {
        // It is possible that the scheduler already ran our cancellation callback,
        // even before we had a chance to set the atomic reference to a non-null value.
        // In this case, we re-schedule the cancellation callback to run immediately,
        // to make sure the failure callback is run.
        scheduler.schedule(prefix + "ImmediateCancellation",
          timeout.attemptTimeout, 0, -1, TimeUnit.MILLISECONDS)
      }
    } catch {
      case e: Throwable => {
        error(s"$threadNamePrefix: Unexpected registerTimeout error for ${prefix}", e)
        timeoutCallback()
      }
    }
    timeout
  }

  def schedule(prefix: String, call: () => Unit) =
    scheduler.scheduleOnce(prefix + "Callback", call)

  def shutdown() = {
    debug(s"$threadNamePrefix: Shutting down")
    scheduler.shutdown()
  }
}
