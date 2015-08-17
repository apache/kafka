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

import java.util.concurrent.TimeUnit

import org.junit.Assert._
import org.junit.Test

class ByteBoundedBlockingQueueTest {
  val sizeFunction = (a: String) => a.length
  val queue = new ByteBoundedBlockingQueue[String](5, 15, Some(sizeFunction))

  @Test
  def testByteBoundedBlockingQueue() {
    assertEquals(5, queue.remainingSize)
    assertEquals(15, queue.remainingByteSize)

    //offer a message whose size is smaller than remaining capacity
    val m0 = new String("0123456789")
    assertEquals(true, queue.offer(m0))
    assertEquals(1, queue.size())
    assertEquals(10, queue.byteSize())
    assertEquals(4, queue.remainingSize)
    assertEquals(5, queue.remainingByteSize)

    // offer a message where remaining capacity < message size < capacity limit
    val m1 = new String("1234567890")
    assertEquals(true, queue.offer(m1))
    assertEquals(2, queue.size())
    assertEquals(20, queue.byteSize())
    assertEquals(3, queue.remainingSize)
    assertEquals(0, queue.remainingByteSize)

    // offer a message using timeout, should fail because no space is left
    val m2 = new String("2345678901")
    assertEquals(false, queue.offer(m2, 10, TimeUnit.MILLISECONDS))
    assertEquals(2, queue.size())
    assertEquals(20, queue.byteSize())
    assertEquals(3, queue.remainingSize)
    assertEquals(0, queue.remainingByteSize)

    // take an element out of the queue
    assertEquals("0123456789", queue.take())
    assertEquals(1, queue.size())
    assertEquals(10, queue.byteSize())
    assertEquals(4, queue.remainingSize)
    assertEquals(5, queue.remainingByteSize)

    // add 5 small elements into the queue, first 4 should succeed, the 5th one should fail
    // test put()
    assertEquals(true, queue.put("a"))
    assertEquals(true, queue.offer("b"))
    assertEquals(true, queue.offer("c"))
    assertEquals(4, queue.size())
    assertEquals(13, queue.byteSize())
    assertEquals(1, queue.remainingSize)
    assertEquals(2, queue.remainingByteSize)

    assertEquals(true, queue.offer("d"))
    assertEquals(5, queue.size())
    assertEquals(14, queue.byteSize())
    assertEquals(0, queue.remainingSize)
    assertEquals(1, queue.remainingByteSize)

    assertEquals(false, queue.offer("e"))
    assertEquals(5, queue.size())
    assertEquals(14, queue.byteSize())
    assertEquals(0, queue.remainingSize)
    assertEquals(1, queue.remainingByteSize)

    // try take 6 elements out of the queue, the last poll() should fail as there is no element anymore
    // test take()
    assertEquals("1234567890", queue.poll(10, TimeUnit.MILLISECONDS))
    // test poll
    assertEquals("a", queue.poll())
    assertEquals("b", queue.poll())
    assertEquals("c", queue.poll())
    assertEquals("d", queue.poll())
    assertEquals(null, queue.poll(10, TimeUnit.MILLISECONDS))
  }

}
