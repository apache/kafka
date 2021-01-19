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
package kafka.utils.timer

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.junit.jupiter.api.Assertions._
import java.util.concurrent.atomic._
import org.junit.jupiter.api.{Test, AfterEach, BeforeEach}

import scala.collection.mutable.ArrayBuffer

class TimerTest {

  private class TestTask(override val delayMs: Long, id: Int, latch: CountDownLatch, output: ArrayBuffer[Int]) extends TimerTask {
    private[this] val completed = new AtomicBoolean(false)
    def run(): Unit = {
      if (completed.compareAndSet(false, true)) {
        output.synchronized { output += id }
        latch.countDown()
      }
    }
  }

  private[this] var timer: Timer = null

  @BeforeEach
  def setup(): Unit = {
    timer = new SystemTimer("test", tickMs = 1, wheelSize = 3)
  }

  @AfterEach
  def teardown(): Unit = {
    timer.shutdown()
  }

  @Test
  def testAlreadyExpiredTask(): Unit = {
    val output = new ArrayBuffer[Int]()


    val latches = (-5 until 0).map { i =>
      val latch = new CountDownLatch(1)
      timer.add(new TestTask(i, i, latch, output))
      latch
    }

    timer.advanceClock(0)

    latches.take(5).foreach { latch =>
      assertEquals(true, latch.await(3, TimeUnit.SECONDS), "already expired tasks should run immediately")
    }

    assertEquals(Set(-5, -4, -3, -2, -1), output.toSet, "output of already expired tasks")
  }

  @Test
  def testTaskExpiration(): Unit = {
    val output = new ArrayBuffer[Int]()

    val tasks = new ArrayBuffer[TestTask]()
    val ids = new ArrayBuffer[Int]()

    val latches =
      (0 until 5).map { i =>
        val latch = new CountDownLatch(1)
        tasks += new TestTask(i, i, latch, output)
        ids += i
        latch
      } ++ (10 until 100).map { i =>
        val latch = new CountDownLatch(2)
        tasks += new TestTask(i, i, latch, output)
        tasks += new TestTask(i, i, latch, output)
        ids += i
        ids += i
        latch
      } ++ (100 until 500).map { i =>
        val latch = new CountDownLatch(1)
        tasks += new TestTask(i, i, latch, output)
        ids += i
        latch
      }

    // randomly submit requests
    tasks.foreach { task => timer.add(task) }

    while (timer.advanceClock(2000)) {}

    latches.foreach { latch => latch.await() }

    assertEquals(ids.sorted, output.toSeq, "output should match")
  }
}
