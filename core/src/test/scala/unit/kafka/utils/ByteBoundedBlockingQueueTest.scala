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

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.junit.Assert._
import org.junit.Test

import scala.util.Random

class ByteBoundedBlockingQueueTest {
  val sizeFunction: String => Long = (a: String) => a.length
  val queue = new ByteBoundedBlockingQueue[String](5, 15, sizeFunction)

  @Test(expected = classOf[IllegalArgumentException])
  def testNullSizeFunctionOption(): Unit = {
    new ByteBoundedBlockingQueue[String](1, 1, null)
  }

  @Test
  def testEmptyInvariants(): Unit = {
    val queue = new ByteBoundedBlockingQueue[String](1000, 1000, sizeFunction)
    assertEquals(0, queue.size())
    assertEquals(1000, queue.remainingSize)
    assertEquals(0, queue.byteSize())
    assertEquals(1000, queue.remainingByteSize)
  }

  @Test
  def testCountBound(): Unit = {
    val queue = new ByteBoundedBlockingQueue[String](3, Long.MaxValue, sizeFunction)
    assertTrue(queue.offer("one", 10, TimeUnit.MILLISECONDS))
    assertTrue(queue.offer("two", 10, TimeUnit.MILLISECONDS))
    assertTrue(queue.offer("three", 10, TimeUnit.MILLISECONDS))
    assertEquals(3, queue.size())
    assertEquals(0, queue.remainingSize)
    assertEquals(11, queue.byteSize())
    assertEquals(Long.MaxValue - 11, queue.remainingByteSize)
    assertFalse(queue.offer("four", 10, TimeUnit.MILLISECONDS)) //msg count cap reached
    assertEquals(3, queue.size())
    assertEquals(0, queue.remainingSize)
    assertEquals(11, queue.byteSize())
    assertEquals(Long.MaxValue - 11, queue.remainingByteSize)
    assertEquals("one", queue.poll())
    assertTrue(queue.offer("four", 10, TimeUnit.MILLISECONDS))
    assertEquals(3, queue.size())
    assertEquals(0, queue.remainingSize)
    assertEquals(12, queue.byteSize())
    assertEquals(Long.MaxValue - 12, queue.remainingByteSize)
  }

  @Test
  def testSizeBound(): Unit = {
    val queue = new ByteBoundedBlockingQueue[String](1000, 10, sizeFunction)
    assertTrue(queue.offer("one", 10, TimeUnit.MILLISECONDS))
    assertTrue(queue.offer("two", 10, TimeUnit.MILLISECONDS))
    assertTrue(queue.offer("three", 10, TimeUnit.MILLISECONDS))
    assertEquals(3, queue.size())
    assertEquals(1000 - 3, queue.remainingSize)
    assertEquals(11, queue.byteSize())
    assertEquals(0, queue.remainingByteSize) //negatives are returned as 0
    assertFalse(queue.offer("four", 10, TimeUnit.MILLISECONDS)) //msg size cap reached
    assertEquals(3, queue.size())
    assertEquals(1000 - 3, queue.remainingSize)
    assertEquals(11, queue.byteSize())
    assertEquals(0, queue.remainingByteSize) //negatives returned as 0
    assertEquals("one", queue.poll())
    assertEquals(2, queue.size())
    assertEquals(1000 - 2, queue.remainingSize)
    assertEquals(8, queue.byteSize())
    assertEquals(10 - 8, queue.remainingByteSize)
    assertTrue(queue.offer("something big", 10, TimeUnit.MILLISECONDS)) //will be accepted because there's _some_ cap available.
    assertEquals(21, queue.byteSize())
    assertEquals(0, queue.remainingByteSize)
    assertEquals("two", queue.poll())
    assertFalse(queue.offer("four", 10, TimeUnit.MILLISECONDS)) //still over cap
    assertEquals(2, queue.size())
    assertEquals(18, queue.byteSize())
  }

  @Test
  def testOfferBlocksUntilMsgCountAvailable(): Unit = {
    val queue = new ByteBoundedBlockingQueue[String](3, 1000, sizeFunction)
    val started = new CountDownLatch(1)
    val finished = new CountDownLatch(1)
    val success = new AtomicReference[Boolean](false)
    val putter1 = new Thread(new Runnable {
      override def run(): Unit = {
        started.countDown()
        success.set(queue.offer("value", 10, TimeUnit.SECONDS))
        finished.countDown()
      }
    })
    assertTrue(queue.offer("one", 10, TimeUnit.MILLISECONDS))
    assertTrue(queue.offer("two", 10, TimeUnit.MILLISECONDS))
    assertTrue(queue.offer("three", 10, TimeUnit.MILLISECONDS))
    putter1.start()
    assertTrue(started.await(10, TimeUnit.SECONDS)) //thread has started
    Thread.sleep(100)
    assertEquals(1, finished.getCount) //thread still blocked
    assertEquals("one", queue.poll(10, TimeUnit.MILLISECONDS))
    assertTrue(finished.await(10, TimeUnit.SECONDS))
    assertTrue(success.get())
  }

  @Test
  def testOfferBlocksUntilBytesAvailable(): Unit = {
    val queue = new ByteBoundedBlockingQueue[String](1000, 10, sizeFunction)
    val started = new CountDownLatch(1)
    val finished = new CountDownLatch(1)
    val success = new AtomicReference[Boolean](false)
    val putter1 = new Thread(new Runnable {
      override def run(): Unit = {
        started.countDown()
        success.set(queue.offer("value", 10, TimeUnit.SECONDS))
        finished.countDown()
      }
    })
    assertTrue(queue.offer("one", 10, TimeUnit.MILLISECONDS))
    assertTrue(queue.offer("two", 10, TimeUnit.MILLISECONDS))
    assertTrue(queue.offer("three", 10, TimeUnit.MILLISECONDS))
    putter1.start()
    assertTrue(started.await(10, TimeUnit.SECONDS)) //thread has started
    Thread.sleep(100)
    assertEquals(1, finished.getCount) //thread still blocked
    assertEquals("one", queue.poll(10, TimeUnit.MILLISECONDS))
    assertTrue(finished.await(10, TimeUnit.SECONDS))
    assertTrue(success.get())
  }

  @Test
  def testInvariantsMT(): Unit = {
    val queue = new ByteBoundedBlockingQueue[Long](20, 100, (l: Long) => l)
    val rand = new Random()
    val bound = 10L
    val numThreads = 3
    val allDone = new CountDownLatch(2 * numThreads)
    @volatile var die = false
    @volatile var error: Option[Throwable] = None

    def validateInvariants(): Unit = {
      val size = queue.size()
      val bytes = queue.byteSize()
      try {
        assertTrue(size >= 0)
        assertTrue(size <= 20)
        assertTrue(bytes >= 0)
        assertTrue(bytes <= 110)
      } catch {
        case t: Throwable => error = Some(t)
      }
    }

    class Producer extends Runnable {
      override def run(): Unit = {
        while (!die) {
          val element: Long = 1L + ((bound - 1) * rand.nextDouble()).toLong //[1, 10] inclusive
          queue.offer(element, 10, TimeUnit.MILLISECONDS)
          validateInvariants()
        }
        allDone.countDown()
      }
    }

    class Consumer extends Runnable {
      override def run(): Unit = {
        while (!die) {
          val element = queue.poll(10, TimeUnit.MILLISECONDS)
          validateInvariants()
        }
        allDone.countDown()
      }
    }


    for (i <- 1 to numThreads) {
      new Thread(new Producer()).start()
      new Thread(new Consumer()).start()
    }

    Thread.sleep(TimeUnit.SECONDS.toMillis(3))
    die = true
    assertTrue(allDone.await(100, TimeUnit.MILLISECONDS))
    if (error.nonEmpty) {
      throw new AssertionError(error.get)
    }
  }

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
    queue.put("a")
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
