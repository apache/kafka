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

package kafka.server

import java.util.concurrent._
import java.util.function.BiConsumer

import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.utils.KafkaThread

import scala.collection.Seq

/**
  * A delayed operation using CompletionFutures that can be created by KafkaApis and watched
  * in a DelayedFuturePurgatory purgatory. This is used for ACL updates using async Authorizers.
  */
class DelayedFuture[T](timeoutMs: Long,
                       futures: Seq[CompletableFuture[T]],
                       responseCallback: () => Unit)
  extends DelayedOperation(timeoutMs) {

  /**
   * The operation can be completed if all the futures have completed successfully
   * or failed with exceptions.
   */
  override def tryComplete() : Boolean = {
    trace(s"Trying to complete operation for ${futures.size} futures")

    val pending = futures.count(future => !future.isDone)
    if (pending == 0) {
      trace("All futures have been completed or have errors, completing the delayed operation")
      forceComplete()
    } else {
      trace(s"$pending future still pending, not completing the delayed operation")
      false
    }
  }

  /**
   * Timeout any pending futures and invoke responseCallback. This is invoked when all
   * futures have completed or the operation has timed out.
   */
  override def onComplete(): Unit = {
    val pendingFutures = futures.filterNot(_.isDone)
    trace(s"Completing operation for ${futures.size} futures, expired ${pendingFutures.size}")
    pendingFutures.foreach(_.completeExceptionally(new TimeoutException(s"Request has been timed out after $timeoutMs ms")))
    responseCallback.apply()
  }

  /**
   * This is invoked after onComplete(), so no actions required.
   */
  override def onExpiration(): Unit = {
  }
}

class DelayedFuturePurgatory(purgatoryName: String, brokerId: Int) {
  private val purgatory = DelayedOperationPurgatory[DelayedFuture[_]](purgatoryName, brokerId)
  private val executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable](),
    new ThreadFactory {
      override def newThread(r: Runnable): Thread = new KafkaThread(s"DelayedExecutor-$purgatoryName", r, true)
    })
  val purgatoryKey = new Object

  def tryCompleteElseWatch[T](timeoutMs: Long,
                              futures: Seq[CompletableFuture[T]],
                              responseCallback: () => Unit): DelayedFuture[T] = {
    val delayedFuture = new DelayedFuture[T](timeoutMs, futures, responseCallback)
    val done = purgatory.tryCompleteElseWatch(delayedFuture, Seq(purgatoryKey))
    if (!done) {
      val callbackAction = new BiConsumer[Void, Throwable]() {
        override def accept(result: Void, exception: Throwable): Unit = delayedFuture.forceComplete()
      }
      CompletableFuture.allOf(futures.toArray: _*).whenCompleteAsync(callbackAction, executor)
    }
    delayedFuture
  }

  def shutdown(): Unit = {
    executor.shutdownNow()
    executor.awaitTermination(60, TimeUnit.SECONDS)
    purgatory.shutdown()
  }
}
