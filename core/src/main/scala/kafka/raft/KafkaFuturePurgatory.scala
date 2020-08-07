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

import java.lang
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import kafka.server.{DelayedOperation, DelayedOperationPurgatory}
import kafka.utils.Logging
import kafka.utils.timer.Timer
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.raft.FuturePurgatory

/**
 * Simple purgatory shim for integration with the Raft library. We assume that
 * both [[await()]] and [[complete()]] are called in the same thread.
 */
class KafkaFuturePurgatory[T <: Comparable[T]](brokerId: Int,
                                               timer: Timer,
                                               reaperEnabled: Boolean = true)
  extends FuturePurgatory[T] with Logging {

  private val key = new Object()
  private val purgatory = new DelayedOperationPurgatory[DelayedRaftRequest](
    "raft-request-purgatory", timer, brokerId, reaperEnabled = reaperEnabled)

  private val thresholdValue: AtomicReference[T] = new AtomicReference[T]()
  private val completionTime: AtomicLong = new AtomicLong(-1)
  private val completionException: AtomicReference[Throwable] = new AtomicReference[Throwable]()

  override def await(value: T, maxWaitTimeMs: Long): CompletableFuture[lang.Long] = {
    val future: CompletableFuture[lang.Long] = new CompletableFuture[lang.Long]()
    val op = new DelayedRaftRequest(future, value, maxWaitTimeMs)
    completionException.set(null)
    purgatory.tryCompleteElseWatch(op, Seq(key))
    future
  }

  override def complete(value: T, currentTime: Long): Unit = {
    // all delayed request equal or smaller than the complete value can be completed
    // we assume the futures are added to the watcher list in order of the value so
    // we can stop early if the completion check failed
    thresholdValue.set(value)
    completionTime.set(currentTime)
    completionException.set(null)
    purgatory.checkAndComplete(key)
  }

  override def completeAllExceptionally(exception: Throwable): Unit = {
    // all delayed request equal or smaller than the complete value can be completed
    // we assume the futures are added to the watcher list in order of the value so
    // we can stop early if the completion check failed
    completionTime.set(-1)
    completionException.set(exception)
    purgatory.checkAndComplete(key)
  }


  override def numWaiting(): Int = {
    purgatory.numDelayed
  }

  private class DelayedRaftRequest(future: CompletableFuture[lang.Long], value: T, delayMs: Long)
    extends DelayedOperation(delayMs) {

    val isExpired = new AtomicBoolean(false)

    override def onExpiration(): Unit = {}

    override def onComplete(): Unit = {
      // the future may be completed by the caller thread already, in which case we can just skip here
      if (future.isDone)
        return

      if (isExpired.get() || completionTime.get() < 0)
        future.completeExceptionally(new TimeoutException("Request timed out in purgatory"))
      else if (completionException.get() != null)
        future.completeExceptionally(completionException.get())
      else
        future.complete(completionTime.get())
    }

    override def tryComplete(): Boolean = {
      if (completionException.get() != null) {
        forceComplete()
      } else if (thresholdValue.get() == null) {
        false
      } else {
        // the request is completable if its future result
        // is smaller than the complete value
        thresholdValue.get().compareTo(value) > 0 && forceComplete()
      }
    }

    override def run(): Unit = {
      isExpired.set(true)
      super.run()
    }
  }
}
