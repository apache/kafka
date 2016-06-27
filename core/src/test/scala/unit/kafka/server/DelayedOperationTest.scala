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

import org.junit.{After, Before, Test}
import org.junit.Assert._

class DelayedOperationTest {

  var purgatory: DelayedOperationPurgatory[MockDelayedOperation] = null

  @Before
  def setUp() {
    purgatory = DelayedOperationPurgatory[MockDelayedOperation](purgatoryName = "mock")
  }

  @After
  def tearDown() {
    purgatory.shutdown()
  }

  @Test
  def testRequestSatisfaction() {
    val r1 = new MockDelayedOperation(100000L)
    val r2 = new MockDelayedOperation(100000L)
    assertEquals("With no waiting requests, nothing should be satisfied", 0, purgatory.checkAndComplete("test1"))
    assertFalse("r1 not satisfied and hence watched", purgatory.tryCompleteElseWatch(r1, Array("test1")))
    assertEquals("Still nothing satisfied", 0, purgatory.checkAndComplete("test1"))
    assertFalse("r2 not satisfied and hence watched", purgatory.tryCompleteElseWatch(r2, Array("test2")))
    assertEquals("Still nothing satisfied", 0, purgatory.checkAndComplete("test2"))
    r1.completable = true
    assertEquals("r1 satisfied", 1, purgatory.checkAndComplete("test1"))
    assertEquals("Nothing satisfied", 0, purgatory.checkAndComplete("test1"))
    r2.completable = true
    assertEquals("r2 satisfied", 1, purgatory.checkAndComplete("test2"))
    assertEquals("Nothing satisfied", 0, purgatory.checkAndComplete("test2"))
  }

  @Test
  def testRequestExpiry() {
    val expiration = 20L
    val start = System.currentTimeMillis
    val r1 = new MockDelayedOperation(expiration)
    val r2 = new MockDelayedOperation(200000L)
    assertFalse("r1 not satisfied and hence watched", purgatory.tryCompleteElseWatch(r1, Array("test1")))
    assertFalse("r2 not satisfied and hence watched", purgatory.tryCompleteElseWatch(r2, Array("test2")))
    r1.awaitExpiration()
    val elapsed = System.currentTimeMillis - start
    assertTrue("r1 completed due to expiration", r1.isCompleted())
    assertFalse("r2 hasn't completed", r2.isCompleted())
    assertTrue("Time for expiration %d should at least %d".format(elapsed, expiration), elapsed >= expiration)
  }

  @Test
  def testRequestPurge() {
    val r1 = new MockDelayedOperation(100000L)
    val r2 = new MockDelayedOperation(100000L)
    val r3 = new MockDelayedOperation(100000L)
    purgatory.tryCompleteElseWatch(r1, Array("test1"))
    purgatory.tryCompleteElseWatch(r2, Array("test1", "test2"))
    purgatory.tryCompleteElseWatch(r3, Array("test1", "test2", "test3"))

    assertEquals("Purgatory should have 3 total delayed operations", 3, purgatory.delayed())
    assertEquals("Purgatory should have 6 watched elements", 6, purgatory.watched())

    // complete the operations, it should immediately be purged from the delayed operation
    r2.completable = true
    r2.tryComplete()
    assertEquals("Purgatory should have 2 total delayed operations instead of " + purgatory.delayed(), 2, purgatory.delayed())

    r3.completable = true
    r3.tryComplete()
    assertEquals("Purgatory should have 1 total delayed operations instead of " + purgatory.delayed(), 1, purgatory.delayed())

    // checking a watch should purge the watch list
    purgatory.checkAndComplete("test1")
    assertEquals("Purgatory should have 4 watched elements instead of " + purgatory.watched(), 4, purgatory.watched())

    purgatory.checkAndComplete("test2")
    assertEquals("Purgatory should have 2 watched elements instead of " + purgatory.watched(), 2, purgatory.watched())

    purgatory.checkAndComplete("test3")
    assertEquals("Purgatory should have 1 watched elements instead of " + purgatory.watched(), 1, purgatory.watched())
  }

  class MockDelayedOperation(delayMs: Long) extends DelayedOperation(delayMs) {
    var completable = false

    def awaitExpiration() {
      synchronized {
        wait()
      }
    }

    override def tryComplete() = {
      if (completable)
        forceComplete()
      else
        false
    }

    override def onExpiration() {

    }

    override def onComplete() {
      synchronized {
        notify()
      }
    }
  }

}
