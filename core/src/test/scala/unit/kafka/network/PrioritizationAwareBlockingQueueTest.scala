/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.network

import java.util.concurrent.TimeUnit

import org.junit.Assert._
import org.junit.{Before, Test}

class PrioritizationAwareBlockingQueueTest {

  val capacity = Int.MaxValue
  var queue: PrioritizationAwareBlockingQueue[Int] = null

  @Before
  def setUp() {
    queue = new PrioritizationAwareBlockingQueue[Int](capacity)
  }

  @Test
  def testPollEmptyQueue() {
    assertNull(queue.poll(0, TimeUnit.MILLISECONDS))
  }

  @Test
  def testPollSingleRegularElement() {
    queue.put(2, false)
    assertEquals(2, queue.poll(0, TimeUnit.MILLISECONDS))
    assertNull(queue.poll(0, TimeUnit.MILLISECONDS))
  }

  @Test
  def testPollSinglePrioritizedElement() {
    queue.put(2, true)
    assertEquals(2, queue.poll(0, TimeUnit.MILLISECONDS))
    assertNull(queue.poll(0, TimeUnit.MILLISECONDS))
  }

  @Test
  def testPollMultipleRegularElements() {
    queue.put(2, false)
    queue.put(20, false)
    assertEquals(2, queue.poll(0, TimeUnit.MILLISECONDS))
    assertEquals(20, queue.poll(0, TimeUnit.MILLISECONDS))
    assertNull(queue.poll(0, TimeUnit.MILLISECONDS))
  }

  @Test
  def testPollMultiplePrioritizedElements() {
    queue.put(2, true)
    queue.put(20, true)
    assertEquals(2, queue.poll(0, TimeUnit.MILLISECONDS))
    assertEquals(20, queue.poll(0, TimeUnit.MILLISECONDS))
    assertNull(queue.poll(0, TimeUnit.MILLISECONDS))
  }

  @Test
  def testPollRegularAndPrioritizedElements() {
    queue.put(2, false)
    queue.put(20, true)
    queue.put(4, false)
    queue.put(15, true)
    assertEquals(20, queue.poll(0, TimeUnit.MILLISECONDS))
    assertEquals(15, queue.poll(0, TimeUnit.MILLISECONDS))
    assertEquals(2, queue.poll(0, TimeUnit.MILLISECONDS))
    assertEquals(4, queue.poll(0, TimeUnit.MILLISECONDS))
    assertNull(queue.poll(0, TimeUnit.MILLISECONDS))
  }

  @Test
  def testClear() {
    queue.put(2, false)
    queue.put(20, true)
    queue.put(4, false)
    queue.put(15, true)
    queue.clear()
    assertNull(queue.poll(0, TimeUnit.MILLISECONDS))
  }

  @Test
  def testSize() {
    assertEquals(0, queue.size)
    queue.poll(0, TimeUnit.MILLISECONDS)
    assertEquals(0, queue.size)
    queue.put(2, false)
    assertEquals(1, queue.size)
    queue.put(20, true)
    assertEquals(2, queue.size)
    queue.put(4, false)
    assertEquals(3, queue.size)
    queue.poll(0, TimeUnit.MILLISECONDS)
    assertEquals(2, queue.size)
    queue.put(15, true)
    assertEquals(3, queue.size)
    queue.poll(0, TimeUnit.MILLISECONDS)
    assertEquals(2, queue.size)
    queue.poll(0, TimeUnit.MILLISECONDS)
    assertEquals(1, queue.size)
    queue.poll(0, TimeUnit.MILLISECONDS)
    assertEquals(0, queue.size)
  }
}
