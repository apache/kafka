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

import java.util.concurrent.{CountDownLatch, ExecutorService, Executors, TimeUnit}

import junit.framework.Assert._
import java.util.concurrent.atomic._
import org.junit.{Test, After, Before}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class TimerTest {

  private class TestTask(override val expirationMs: Long, id: Int, latch: CountDownLatch, output: ArrayBuffer[Int]) extends TimerTask {
    private[this] val completed = new AtomicBoolean(false)
    def run(): Unit = {
      if (completed.compareAndSet(false, true)) {
        output.synchronized { output += id }
        latch.countDown()
      }
    }
  }

  private[this] var executor: ExecutorService = null

  @Before
  def setup() {
    executor = Executors.newSingleThreadExecutor()
  }

  @After
  def teardown(): Unit = {
    executor.shutdown()
    executor = null
  }

  @Test
  def testAlreadyExpiredTask(): Unit = {
    val startTime = System.currentTimeMillis()
    val timer = new Timer(taskExecutor = executor, tickMs = 1, wheelSize = 3, startMs = startTime)
    val output = new ArrayBuffer[Int]()


    val latches = (-5 until 0).map { i =>
      val latch = new CountDownLatch(1)
      timer.add(new TestTask(startTime + i, i, latch, output))
      latch
    }

    latches.take(5).foreach { latch =>
      assertEquals("already expired tasks should run immediately", true, latch.await(3, TimeUnit.SECONDS))
    }

    assertEquals("output of already expired tasks", Set(-5, -4, -3, -2, -1), output.toSet)
  }

  @Test
  def testTaskExpiration(): Unit = {
    val startTime = System.currentTimeMillis()
    val timer = new Timer(taskExecutor = executor, tickMs = 1, wheelSize = 3, startMs = startTime)
    val output = new ArrayBuffer[Int]()

    val tasks = new ArrayBuffer[TestTask]()
    val ids = new ArrayBuffer[Int]()

    val latches =
      (0 until 5).map { i =>
        val latch = new CountDownLatch(1)
        tasks += new TestTask(startTime + i, i, latch, output)
        ids += i
        latch
      } ++ (10 until 100).map { i =>
        val latch = new CountDownLatch(2)
        tasks += new TestTask(startTime + i, i, latch, output)
        tasks += new TestTask(startTime + i, i, latch, output)
        ids += i
        ids += i
        latch
      } ++ (100 until 500).map { i =>
        val latch = new CountDownLatch(1)
        tasks += new TestTask(startTime + i, i, latch, output)
        ids += i
        latch
      }

    // randomly submit requests
    Random.shuffle(tasks.toSeq).foreach { task => timer.add(task) }

    while (timer.advanceClock(1000)) {}

    latches.foreach { latch => latch.await() }

    assertEquals("output should match", ids.sorted, output.toSeq)
  }
}
