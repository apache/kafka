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
package integration.kafka.server

import kafka.server.DelayedFuturePurgatory
import kafka.utils.TestUtils
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.concurrent.{CompletableFuture, ExecutionException}
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.CollectionHasAsScala

class DelayedFutureTest {

  @Test
  def testDelayedFuture(): Unit = {
    val purgatoryName = "testDelayedFuture"
    val purgatory = new DelayedFuturePurgatory(purgatoryName, brokerId = 0)
    try {
      val result = new AtomicInteger()

      def hasExecutorThread: Boolean = Thread.getAllStackTraces.keySet.asScala.map(_.getName)
        .exists(_.contains(s"DelayedExecutor-$purgatoryName"))

      def updateResult(futures: List[CompletableFuture[Integer]]): Unit =
        result.set(futures.filterNot(_.isCompletedExceptionally).map(_.get.intValue).sum)

      assertFalse(hasExecutorThread, "Unnecessary thread created")

      // Two completed futures: callback should be executed immediately on the same thread
      val futures1 = List(CompletableFuture.completedFuture(10.asInstanceOf[Integer]),
        CompletableFuture.completedFuture(11.asInstanceOf[Integer]))
      val r1 = purgatory.tryCompleteElseWatch[Integer](100000L, futures1, () => updateResult(futures1))
      assertTrue(r1.isCompleted, "r1 not completed")
      assertEquals(21, result.get())
      assertFalse(hasExecutorThread, "Unnecessary thread created")

      // Two delayed futures: callback should wait for both to complete
      result.set(-1)
      val futures2 = List(new CompletableFuture[Integer], new CompletableFuture[Integer])
      val r2 = purgatory.tryCompleteElseWatch[Integer](100000L, futures2, () => updateResult(futures2))
      assertFalse(r2.isCompleted, "r2 should be incomplete")
      futures2.head.complete(20)
      assertFalse(r2.isCompleted)
      assertEquals(-1, result.get())
      futures2(1).complete(21)
      TestUtils.waitUntilTrue(() => r2.isCompleted, "r2 not completed")
      TestUtils.waitUntilTrue(() => result.get == 41, "callback not invoked")
      assertTrue(hasExecutorThread, "Thread not created for executing delayed task")

      // One immediate and one delayed future: callback should wait for delayed task to complete
      result.set(-1)
      val futures3 = List(new CompletableFuture[Integer], CompletableFuture.completedFuture(31.asInstanceOf[Integer]))
      val r3 = purgatory.tryCompleteElseWatch[Integer](100000L, futures3, () => updateResult(futures3))
      assertFalse(r3.isCompleted, "r3 should be incomplete")
      assertEquals(-1, result.get())
      futures3.head.complete(30)
      TestUtils.waitUntilTrue(() => r3.isCompleted, "r3 not completed")
      TestUtils.waitUntilTrue(() => result.get == 61, "callback not invoked")

      // One future doesn't complete within timeout. Should expire and invoke callback after timeout.
      result.set(-1)
      val start = Time.SYSTEM.hiResClockMs
      val expirationMs = 2000L
      val futures4 = List(new CompletableFuture[Integer], new CompletableFuture[Integer])
      val r4 = purgatory.tryCompleteElseWatch[Integer](expirationMs, futures4, () => updateResult(futures4))
      futures4.head.complete(40)
      TestUtils.waitUntilTrue(() => futures4(1).isDone, "r4 futures not expired")
      assertTrue(r4.isCompleted, "r4 not completed after timeout")
      val elapsed = Time.SYSTEM.hiResClockMs - start
      assertTrue(elapsed >= expirationMs, s"Time for expiration $elapsed should at least $expirationMs")
      assertEquals(40, futures4.head.get)
      assertEquals(classOf[org.apache.kafka.common.errors.TimeoutException],
        assertThrows(classOf[ExecutionException], () => futures4(1).get).getCause.getClass)
      assertEquals(40, result.get())
    } finally {
      purgatory.shutdown()
    }
  }
}
