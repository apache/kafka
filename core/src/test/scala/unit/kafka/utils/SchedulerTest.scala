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
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, TimeUnit}
import kafka.log.UnifiedLog
import kafka.utils.TestUtils.retry
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.util.{KafkaScheduler, MockTime}
import org.apache.kafka.storage.internals.log.{LocalLog, LogConfig, LogDirFailureChannel, LogLoader, LogSegments, ProducerStateManager, ProducerStateManagerConfig}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, Timeout}

import scala.jdk.OptionConverters.RichOption


class SchedulerTest {

  val scheduler = new KafkaScheduler(1)
  val mockTime = new MockTime
  val counter1 = new AtomicInteger(0)
  val counter2 = new AtomicInteger(0)

  @BeforeEach
  def setup(): Unit = {
    scheduler.startup()
  }

  @AfterEach
  def teardown(): Unit = {
    scheduler.shutdown()
  }

  @Test
  def testMockSchedulerNonPeriodicTask(): Unit = {
    mockTime.scheduler.scheduleOnce("test1", () => counter1.getAndIncrement(), 1)
    mockTime.scheduler.scheduleOnce("test2", () => counter2.getAndIncrement(), 100)
    assertEquals(0, counter1.get, "Counter1 should not be incremented prior to task running.")
    assertEquals(0, counter2.get, "Counter2 should not be incremented prior to task running.")
    mockTime.sleep(1)
    assertEquals(1, counter1.get, "Counter1 should be incremented")
    assertEquals(0, counter2.get, "Counter2 should not be incremented")
    mockTime.sleep(100000)
    assertEquals(1, counter1.get, "More sleeping should not result in more incrementing on counter1.")
    assertEquals(1, counter2.get, "Counter2 should now be incremented.")
  }

  @Test
  def testMockSchedulerPeriodicTask(): Unit = {
    mockTime.scheduler.schedule("test1", () => counter1.getAndIncrement(), 1, 1)
    mockTime.scheduler.schedule("test2", () => counter2.getAndIncrement(), 100, 100)
    assertEquals(0, counter1.get, "Counter1 should not be incremented prior to task running.")
    assertEquals(0, counter2.get, "Counter2 should not be incremented prior to task running.")
    mockTime.sleep(1)
    assertEquals(1, counter1.get, "Counter1 should be incremented")
    assertEquals(0, counter2.get, "Counter2 should not be incremented")
    mockTime.sleep(100)
    assertEquals(101, counter1.get, "Counter1 should be incremented 101 times")
    assertEquals(1, counter2.get, "Counter2 should not be incremented once")
  }

  @Test
  def testReentrantTaskInMockScheduler(): Unit = {
    mockTime.scheduler.scheduleOnce("test1", () => mockTime.scheduler.scheduleOnce("test2", () => counter2.getAndIncrement(), 0), 1)
    mockTime.sleep(1)
    assertEquals(1, counter2.get)
  }

  @Test
  def testNonPeriodicTask(): Unit = {
    scheduler.scheduleOnce("test", () => counter1.getAndIncrement())
    retry(30000) {
      assertEquals(counter1.get, 1)
    }
    Thread.sleep(5)
    assertEquals(1, counter1.get, "Should only run once")
  }

  @Test
  def testNonPeriodicTaskWhenPeriodIsZero(): Unit = {
    scheduler.schedule("test", () => counter1.getAndIncrement(), 0, 0)
    retry(30000) {
      assertEquals(counter1.get, 1)
    }
    Thread.sleep(5)
    assertEquals(1, counter1.get, "Should only run once")
  }

  @Test
  def testPeriodicTask(): Unit = {
    scheduler.schedule("test", () => counter1.getAndIncrement(), 0, 5)
    retry(30000) {
      assertTrue(counter1.get >= 20, "Should count to 20")
    }
  }

  @Test
  def testRestart(): Unit = {
    // schedule a task to increment a counter
    mockTime.scheduler.scheduleOnce("test1", () => counter1.getAndIncrement(), 1)
    mockTime.sleep(1)
    assertEquals(1, counter1.get())

    // restart the scheduler
    mockTime.scheduler.shutdown()
    mockTime.scheduler.startup()

    // schedule another task to increment the counter
    mockTime.scheduler.scheduleOnce("test1", () => counter1.getAndIncrement(), 1)
    mockTime.sleep(1)
    assertEquals(2, counter1.get())
  }

  @Test
  def testUnscheduleProducerTask(): Unit = {
    val tmpDir = TestUtils.tempDir()
    val logDir = TestUtils.randomPartitionLogDir(tmpDir)
    val logConfig = new LogConfig(new Properties())
    val brokerTopicStats = new BrokerTopicStats
    val maxTransactionTimeoutMs = 5 * 60 * 1000
    val maxProducerIdExpirationMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT
    val producerIdExpirationCheckIntervalMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT
    val topicPartition = UnifiedLog.parseTopicPartitionName(logDir)
    val logDirFailureChannel = new LogDirFailureChannel(10)
    val segments = new LogSegments(topicPartition)
    val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(
      logDir, topicPartition, logDirFailureChannel, logConfig.recordVersion, "", None, mockTime.scheduler)
    val producerStateManager = new ProducerStateManager(topicPartition, logDir,
      maxTransactionTimeoutMs, new ProducerStateManagerConfig(maxProducerIdExpirationMs, false), mockTime)
    val offsets = new LogLoader(
      logDir,
      topicPartition,
      logConfig,
      scheduler,
      mockTime,
      logDirFailureChannel,
      true,
      segments,
      0L,
      0L,
      leaderEpochCache.toJava,
      producerStateManager,
      new ConcurrentHashMap[String, Integer],
      false
    ).load()
    val localLog = new LocalLog(logDir, logConfig, segments, offsets.recoveryPoint,
      offsets.nextOffsetMetadata, scheduler, mockTime, topicPartition, logDirFailureChannel)
    val log = new UnifiedLog(logStartOffset = offsets.logStartOffset,
      localLog = localLog,
      brokerTopicStats, producerIdExpirationCheckIntervalMs,
      leaderEpochCache, producerStateManager,
      _topicId = None, keepPartitionMetadataFile = true)
    assertTrue(scheduler.taskRunning(log.producerExpireCheck))
    log.close()
    assertFalse(scheduler.taskRunning(log.producerExpireCheck))
  }

  /**
   * Verify that scheduler lock is not held when invoking task method, allowing new tasks to be scheduled
   * when another is being executed. This is required to avoid deadlocks when:
   *   a) Thread1 executes a task which attempts to acquire LockA
   *   b) Thread2 holding LockA attempts to schedule a new task
   */
  @Timeout(15)
  @Test
  def testMockSchedulerLocking(): Unit = {
    val initLatch = new CountDownLatch(1)
    val completionLatch = new CountDownLatch(2)
    val taskLatches = List(new CountDownLatch(1), new CountDownLatch(1))
    def scheduledTask(taskLatch: CountDownLatch): Unit = {
      initLatch.countDown()
      assertTrue(taskLatch.await(30, TimeUnit.SECONDS), "Timed out waiting for latch")
      completionLatch.countDown()
    }
    mockTime.scheduler.scheduleOnce("test1", () => scheduledTask(taskLatches.head), 1)
    val tickExecutor = Executors.newSingleThreadScheduledExecutor()
    try {
      tickExecutor.scheduleWithFixedDelay(() => mockTime.sleep(1), 0, 1, TimeUnit.MILLISECONDS)

      // wait for first task to execute and then schedule the next task while the first one is running
      assertTrue(initLatch.await(10, TimeUnit.SECONDS))
      mockTime.scheduler.scheduleOnce("test2", () => scheduledTask(taskLatches(1)), 1)

      taskLatches.foreach(_.countDown())
      assertTrue(completionLatch.await(10, TimeUnit.SECONDS), "Tasks did not complete")

    } finally {
      tickExecutor.shutdownNow()
    }
  }
}
