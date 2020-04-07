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

import java.util.Properties
import java.util.concurrent.atomic._
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import kafka.log.{Log, LogConfig, LogManager, ProducerStateManager}
import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.utils.TestUtils.retry
import org.junit.Assert._
import org.junit.{After, Before, Test}

class SchedulerTest {

  val scheduler = new KafkaScheduler(1)
  val mockTime = new MockTime
  val counter1 = new AtomicInteger(0)
  val counter2 = new AtomicInteger(0)
  
  @Before
  def setup(): Unit = {
    scheduler.startup()
  }
  
  @After
  def teardown(): Unit = {
    scheduler.shutdown()
  }

  @Test
  def testMockSchedulerNonPeriodicTask(): Unit = {
    mockTime.scheduler.schedule("test1", counter1.getAndIncrement _, delay=1)
    mockTime.scheduler.schedule("test2", counter2.getAndIncrement _, delay=100)
    assertEquals("Counter1 should not be incremented prior to task running.", 0, counter1.get)
    assertEquals("Counter2 should not be incremented prior to task running.", 0, counter2.get)
    mockTime.sleep(1)
    assertEquals("Counter1 should be incremented", 1, counter1.get)
    assertEquals("Counter2 should not be incremented", 0, counter2.get)
    mockTime.sleep(100000)
    assertEquals("More sleeping should not result in more incrementing on counter1.", 1, counter1.get)
    assertEquals("Counter2 should now be incremented.", 1, counter2.get)
  }

  @Test
  def testMockSchedulerPeriodicTask(): Unit = {
    mockTime.scheduler.schedule("test1", counter1.getAndIncrement _, delay=1, period=1)
    mockTime.scheduler.schedule("test2", counter2.getAndIncrement _, delay=100, period=100)
    assertEquals("Counter1 should not be incremented prior to task running.", 0, counter1.get)
    assertEquals("Counter2 should not be incremented prior to task running.", 0, counter2.get)
    mockTime.sleep(1)
    assertEquals("Counter1 should be incremented", 1, counter1.get)
    assertEquals("Counter2 should not be incremented", 0, counter2.get)
    mockTime.sleep(100)
    assertEquals("Counter1 should be incremented 101 times", 101, counter1.get)
    assertEquals("Counter2 should not be incremented once", 1, counter2.get)
  }

  @Test
  def testReentrantTaskInMockScheduler(): Unit = {
    mockTime.scheduler.schedule("test1", () => mockTime.scheduler.schedule("test2", counter2.getAndIncrement _, delay=0), delay=1)
    mockTime.sleep(1)
    assertEquals(1, counter2.get)
  }

  @Test
  def testNonPeriodicTask(): Unit = {
    scheduler.schedule("test", counter1.getAndIncrement _, delay = 0)
    retry(30000) {
      assertEquals(counter1.get, 1)
    }
    Thread.sleep(5)
    assertEquals("Should only run once", 1, counter1.get)
  }

  @Test
  def testPeriodicTask(): Unit = {
    scheduler.schedule("test", counter1.getAndIncrement _, delay = 0, period = 5)
    retry(30000){
      assertTrue("Should count to 20", counter1.get >= 20)
    }
  }

  @Test
  def testRestart(): Unit = {
    // schedule a task to increment a counter
    mockTime.scheduler.schedule("test1", counter1.getAndIncrement _, delay=1)
    mockTime.sleep(1)
    assertEquals(1, counter1.get())

    // restart the scheduler
    mockTime.scheduler.shutdown()
    mockTime.scheduler.startup()

    // schedule another task to increment the counter
    mockTime.scheduler.schedule("test1", counter1.getAndIncrement _, delay=1)
    mockTime.sleep(1)
    assertEquals(2, counter1.get())
  }

  @Test
  def testUnscheduleProducerTask(): Unit = {
    val tmpDir = TestUtils.tempDir()
    val logDir = TestUtils.randomPartitionLogDir(tmpDir)
    val logConfig = LogConfig(new Properties())
    val brokerTopicStats = new BrokerTopicStats
    val recoveryPoint = 0L
    val maxProducerIdExpirationMs = 60 * 60 * 1000
    val topicPartition = Log.parseTopicPartitionName(logDir)
    val producerStateManager = new ProducerStateManager(topicPartition, logDir, maxProducerIdExpirationMs)
    val log = new Log(logDir, logConfig, logStartOffset = 0, recoveryPoint = recoveryPoint, scheduler,
      brokerTopicStats, mockTime, maxProducerIdExpirationMs, LogManager.ProducerIdExpirationCheckIntervalMs,
      topicPartition, producerStateManager, new LogDirFailureChannel(10))
    assertTrue(scheduler.taskRunning(log.producerExpireCheck))
    log.close()
    assertTrue(!(scheduler.taskRunning(log.producerExpireCheck)))
  }

  /**
   * Verify that scheduler lock is not held when invoking task method, allowing new tasks to be scheduled
   * when another is being executed. This is required to avoid deadlocks when:
   *   a) Thread1 executes a task which attempts to acquire LockA
   *   b) Thread2 holding LockA attempts to schedule a new task
   */
  @Test(timeout = 15000)
  def testMockSchedulerLocking(): Unit = {
    val initLatch = new CountDownLatch(1)
    val completionLatch = new CountDownLatch(2)
    val taskLatches = List(new CountDownLatch(1), new CountDownLatch(1))
    def scheduledTask(taskLatch: CountDownLatch): Unit = {
      initLatch.countDown()
      assertTrue("Timed out waiting for latch", taskLatch.await(30, TimeUnit.SECONDS))
      completionLatch.countDown()
    }
    mockTime.scheduler.schedule("test1", () => scheduledTask(taskLatches.head), delay=1)
    val tickExecutor = Executors.newSingleThreadScheduledExecutor()
    try {
      tickExecutor.scheduleWithFixedDelay(() => mockTime.sleep(1), 0, 1, TimeUnit.MILLISECONDS)

      // wait for first task to execute and then schedule the next task while the first one is running
      assertTrue(initLatch.await(10, TimeUnit.SECONDS))
      mockTime.scheduler.schedule("test2", () => scheduledTask(taskLatches(1)), delay = 1)

      taskLatches.foreach(_.countDown())
      assertTrue("Tasks did not complete", completionLatch.await(10, TimeUnit.SECONDS))

    } finally {
      tickExecutor.shutdownNow()
    }
  }
}
