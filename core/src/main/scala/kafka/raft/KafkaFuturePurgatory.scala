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
import java.util.concurrent.atomic.AtomicBoolean

import kafka.server.{DelayedOperation, DelayedOperationPurgatory}
import kafka.utils.Logging
import kafka.utils.timer.Timer
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.raft.FuturePurgatory

/**
 * Simple purgatory shim for integration with the Raft library. We assume that
 * both [[await()]] and [[completeAll()]] are called in the same thread.
 */
class KafkaFuturePurgatory(brokerId: Int, timer: Timer, reaperEnabled: Boolean = true)
  extends FuturePurgatory[Void] with Logging {

  private val key = new Object()
  private val purgatory = new DelayedOperationPurgatory[DelayedRaftRequest](
    "raft-request-purgatory", timer, brokerId, reaperEnabled = reaperEnabled)

  override def await(future: CompletableFuture[Void], maxWaitTimeMs: Long): Unit = {
    val op = new DelayedRaftRequest(future, maxWaitTimeMs)
    purgatory.tryCompleteElseWatch(op, Seq(key))
    op.isCompletable.set(true)
  }

  override def completeAll(value: Void): Unit = {
    purgatory.checkAndComplete(key)
  }

  override def numWaiting(): Int = {
    purgatory.numDelayed
  }
}

class DelayedRaftRequest(future: CompletableFuture[Void], delayMs: Long)
  extends DelayedOperation(delayMs) {

  val isCompletable = new AtomicBoolean(false)
  val isExpired = new AtomicBoolean(false)

  override def onExpiration(): Unit = {}

  override def onComplete(): Unit = {
    if (isExpired.get())
      future.completeExceptionally(new TimeoutException("Request timed out in purgatory"))
    else
      future.complete(null)
  }

  override def tryComplete(): Boolean = {
    isCompletable.get() && forceComplete()
  }

  override def run(): Unit = {
    isExpired.set(true)
    super.run()
  }

}
