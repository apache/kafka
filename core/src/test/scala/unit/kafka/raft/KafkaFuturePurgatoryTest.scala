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
package unit.kafka.raft

import java.util.concurrent.CompletableFuture

import kafka.raft.KafkaFuturePurgatory
import kafka.utils.timer.MockTimer
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.test.TestUtils
import org.junit.Assert._
import org.junit.Test

class KafkaFuturePurgatoryTest {

  @Test
  def testExpiration(): Unit = {
    val brokerId = 0
    val timer = new MockTimer()
    val purgatory = new KafkaFuturePurgatory(brokerId, timer, reaperEnabled = false)
    assertEquals(0, purgatory.numWaiting())

    val future1 = new CompletableFuture[Void]()
    purgatory.await(future1, 500)
    assertEquals(1, purgatory.numWaiting())

    val future2 = new CompletableFuture[Void]()
    purgatory.await(future2, 500)
    assertEquals(2, purgatory.numWaiting())

    val future3 = new CompletableFuture[Void]()
    purgatory.await(future3, 1000)
    assertEquals(3, purgatory.numWaiting())

    timer.advanceClock(501)
    assertEquals(1, purgatory.numWaiting())
    TestUtils.assertFutureThrows(future1, classOf[TimeoutException])
    TestUtils.assertFutureThrows(future2, classOf[TimeoutException])

    timer.advanceClock(500)
    assertEquals(0, purgatory.numWaiting())
    TestUtils.assertFutureThrows(future3, classOf[TimeoutException])
  }

  @Test
  def testCompletion(): Unit = {
    val brokerId = 0
    val timer = new MockTimer()
    val purgatory = new KafkaFuturePurgatory(brokerId, timer, reaperEnabled = false)
    assertEquals(0, purgatory.numWaiting())

    val future1 = new CompletableFuture[Void]()
    purgatory.await(future1, 500)
    assertEquals(1, purgatory.numWaiting())

    val future2 = new CompletableFuture[Void]()
    purgatory.await(future2, 500)
    assertEquals(2, purgatory.numWaiting())

    val future3 = new CompletableFuture[Void]()
    purgatory.await(future3, 1000)
    assertEquals(3, purgatory.numWaiting())

    purgatory.completeAll(null)
    assertTrue(future1.isDone)
    assertEquals(null, future1.get())

    assertTrue(future2.isDone)
    assertEquals(null, future2.get())

    assertTrue(future3.isDone)
    assertEquals(null, future3.get())
  }
}
