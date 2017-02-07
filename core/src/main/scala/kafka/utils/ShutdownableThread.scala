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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CountDownLatch

import org.apache.kafka.common.internals.FatalExitError

abstract class ShutdownableThread(val name: String, val isInterruptible: Boolean = true)
        extends Thread(name) with Logging {
  this.setDaemon(false)
  this.logIdent = "[" + name + "], "
  val isRunning: AtomicBoolean = new AtomicBoolean(true)
  private val shutdownLatch = new CountDownLatch(1)

  def shutdown() = {
    initiateShutdown()
    awaitShutdown()
  }

  def initiateShutdown(): Boolean = {
    if (isRunning.compareAndSet(true, false)) {
      info("Shutting down")
      if (isInterruptible)
        interrupt()
      true
    } else
      false
  }

    /**
   * After calling initiateShutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = {
    shutdownLatch.await()
    info("Shutdown completed")
  }

  /**
   * This method is repeatedly invoked until the thread shuts down or this method throws an exception
   */
  def doWork(): Unit

  override def run(): Unit = {
    info("Starting")
    try {
      while (isRunning.get)
        doWork()
    } catch {
      case e: FatalExitError =>
        isRunning.set(false)
        shutdownLatch.countDown()
        info("Stopped")
        Exit.exit(e.statusCode())
      case e: Throwable =>
        if (isRunning.get())
          error("Error due to", e)
    }
    shutdownLatch.countDown()
    info("Stopped")
  }
}
