/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.raft

import java.util.concurrent.CompletableFuture

import kafka.utils.ShutdownableThread
import kafka.utils.timer.{Timer, TimerTask}
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.raft.ExpirationService

object TimingWheelExpirationService {
  private val WorkTimeoutMs: Long = 200L

  class TimerTaskCompletableFuture[T](override val delayMs: Long) extends CompletableFuture[T] with TimerTask {
    override def run(): Unit = {
      completeExceptionally(new TimeoutException(
        s"Future failed to be completed before timeout of $delayMs ms was reached"))
    }
  }
}

class TimingWheelExpirationService(timer: Timer) extends ExpirationService {
  import TimingWheelExpirationService._

  private val expirationReaper = new ExpiredOperationReaper()

  expirationReaper.start()

  override def failAfter[T](timeoutMs: Long): CompletableFuture[T] = {
    val future = new TimerTaskCompletableFuture[T](timeoutMs)
    future.whenComplete { (_, _) =>
      future.cancel()
    }
    timer.add(future)
    future
  }

  private class ExpiredOperationReaper extends ShutdownableThread(
    name = "raft-expiration-reaper", isInterruptible = false) {

    override def doWork(): Unit = {
      timer.advanceClock(WorkTimeoutMs)
    }
  }

  def shutdown(): Unit = {
    expirationReaper.shutdown()
  }
}
