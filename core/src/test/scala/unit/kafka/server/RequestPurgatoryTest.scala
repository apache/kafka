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

import scala.collection._
import org.junit.Test
import junit.framework.Assert._
import kafka.message._
import kafka.api._
import kafka.utils.TestUtils
import org.scalatest.junit.JUnit3Suite


class RequestPurgatoryTest extends JUnit3Suite {

  val producerRequest1 = TestUtils.produceRequest("test", 0, new ByteBufferMessageSet(new Message("hello1".getBytes)))
  val producerRequest2 = TestUtils.produceRequest("test", 0, new ByteBufferMessageSet(new Message("hello2".getBytes)))
  var purgatory: MockRequestPurgatory = null
  
  override def setUp() {
    super.setUp()
    purgatory = new MockRequestPurgatory(5)
  }
  
  override def tearDown() {
    purgatory.shutdown()
    super.tearDown()
  }

  @Test
  def testRequestSatisfaction() {
    val r1 = new DelayedRequest(Array("test1"), null, 100000L)
    val r2 = new DelayedRequest(Array("test2"), null, 100000L)
    assertEquals("With no waiting requests, nothing should be satisfied", 0, purgatory.update("test1").size)
    assertFalse("r1 not satisfied and hence watched", purgatory.checkAndMaybeWatch(r1))
    assertEquals("Still nothing satisfied", 0, purgatory.update("test1").size)
    assertFalse("r2 not satisfied and hence watched", purgatory.checkAndMaybeWatch(r2))
    assertEquals("Still nothing satisfied", 0, purgatory.update("test2").size)
    purgatory.satisfied += r1
    assertEquals("r1 satisfied", mutable.ArrayBuffer(r1), purgatory.update("test1"))
    assertEquals("Nothing satisfied", 0, purgatory.update("test1").size)
    purgatory.satisfied += r2
    assertEquals("r2 satisfied", mutable.ArrayBuffer(r2), purgatory.update("test2"))
    assertEquals("Nothing satisfied", 0, purgatory.update("test2").size)
  }

  @Test
  def testRequestExpiry() {
    val expiration = 20L
    val r1 = new DelayedRequest(Array("test1"), null, expiration)
    val r2 = new DelayedRequest(Array("test1"), null, 200000L)
    val start = System.currentTimeMillis
    assertFalse("r1 not satisfied and hence watched", purgatory.checkAndMaybeWatch(r1))
    assertFalse("r2 not satisfied and hence watched", purgatory.checkAndMaybeWatch(r2))
    purgatory.awaitExpiration(r1)
    val elapsed = System.currentTimeMillis - start
    assertTrue("r1 expired", purgatory.expired.contains(r1))
    assertTrue("r2 hasn't expired", !purgatory.expired.contains(r2))
    assertTrue("Time for expiration %d should at least %d".format(elapsed, expiration), elapsed >= expiration)
  }

  @Test
  def testRequestPurge() {
    val r1 = new DelayedRequest(Array("test1"), null, 100000L)
    val r12 = new DelayedRequest(Array("test1", "test2"), null, 100000L)
    val r23 = new DelayedRequest(Array("test2", "test3"), null, 100000L)
    purgatory.checkAndMaybeWatch(r1)
    purgatory.checkAndMaybeWatch(r12)
    purgatory.checkAndMaybeWatch(r23)

    assertEquals("Purgatory should have 5 watched elements", 5, purgatory.watched())
    assertEquals("Purgatory should have 3 total delayed requests", 3, purgatory.delayed())

    // satisfy one of the requests, it should then be purged from the watch list with purge interval 5
    r12.satisfied.set(true)
    TestUtils.waitUntilTrue(() => purgatory.watched() == 3,
      "Purgatory should have 3 watched elements instead of " +  + purgatory.watched(), 1000L)
    TestUtils.waitUntilTrue(() => purgatory.delayed() == 3,
      "Purgatory should still have 3 total delayed requests instead of " + purgatory.delayed(), 1000L)

    // add two more requests, then the satisfied request should be purged from the delayed queue with purge interval 5
    purgatory.checkAndMaybeWatch(r1)
    purgatory.checkAndMaybeWatch(r1)

    TestUtils.waitUntilTrue(() => purgatory.watched() == 5,
      "Purgatory should have 5 watched elements instead of " + purgatory.watched(), 1000L)
    TestUtils.waitUntilTrue(() => purgatory.delayed() == 4,
      "Purgatory should have 4 total delayed requests instead of " + purgatory.delayed(), 1000L)
  }
  
  class MockRequestPurgatory(purge: Int) extends RequestPurgatory[DelayedRequest](purgeInterval = purge) {
    val satisfied = mutable.Set[DelayedRequest]()
    val expired = mutable.Set[DelayedRequest]()
    def awaitExpiration(delayed: DelayedRequest) = {
      delayed synchronized {
        delayed.wait()
      }
    }
    def checkSatisfied(delayed: DelayedRequest): Boolean = satisfied.contains(delayed)
    def expire(delayed: DelayedRequest) {
      expired += delayed
      delayed synchronized {
        delayed.notify()
      }
    }
  }
  
}