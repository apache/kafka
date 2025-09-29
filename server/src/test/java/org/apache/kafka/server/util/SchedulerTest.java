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
package org.apache.kafka.server.util;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.LoadedLogOffsets;
import org.apache.kafka.storage.internals.log.LocalLog;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogLoader;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogSegments;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchedulerTest {

    private final KafkaScheduler scheduler = new KafkaScheduler(1);
    private final MockTime mockTime = new MockTime();
    private final AtomicInteger counter1 = new AtomicInteger(0);
    private final AtomicInteger counter2 = new AtomicInteger(0);

    @BeforeEach
    void setup() {
        counter1.set(0);
        counter2.set(0);
        scheduler.startup();
    }

    @AfterEach
    void teardown() throws InterruptedException {
        scheduler.shutdown();
    }

    @Test
    void testMockSchedulerNonPeriodicTask() {
        mockTime.scheduler.scheduleOnce("test1", counter1::getAndIncrement, 1);
        mockTime.scheduler.scheduleOnce("test2", counter2::getAndIncrement, 100);
        assertEquals(0, counter1.get(), "Counter1 should not be incremented prior to task running.");
        assertEquals(0, counter2.get(), "Counter2 should not be incremented prior to task running.");
        mockTime.sleep(1);
        assertEquals(1, counter1.get(), "Counter1 should be incremented");
        assertEquals(0, counter2.get(), "Counter2 should not be incremented");
        mockTime.sleep(100000);
        assertEquals(1, counter1.get(), "More sleeping should not result in more incrementing on counter1.");
        assertEquals(1, counter2.get(), "Counter2 should now be incremented.");
    }

    @Test
    void testMockSchedulerPeriodicTask() {
        mockTime.scheduler.schedule("test1", counter1::getAndIncrement, 1, 1);
        mockTime.scheduler.schedule("test2", counter2::getAndIncrement, 100, 100);
        assertEquals(0, counter1.get(), "Counter1 should not be incremented prior to task running.");
        assertEquals(0, counter2.get(), "Counter2 should not be incremented prior to task running.");
        mockTime.sleep(1);
        assertEquals(1, counter1.get(), "Counter1 should be incremented");
        assertEquals(0, counter2.get(), "Counter2 should not be incremented");
        mockTime.sleep(100);
        assertEquals(101, counter1.get(), "Counter1 should be incremented 101 times");
        assertEquals(1, counter2.get(), "Counter2 should not be incremented once");
    }

    @Test
    void testReentrantTaskInMockScheduler() {
        mockTime.scheduler.scheduleOnce("test1", () -> mockTime.scheduler.scheduleOnce("test2", counter2::getAndIncrement, 0), 1);
        mockTime.sleep(1);
        assertEquals(1, counter2.get());
    }

    @Test
    void testNonPeriodicTask() throws InterruptedException {
        scheduler.scheduleOnce("test", counter1::getAndIncrement);
        TestUtils.waitForCondition(() -> counter1.get() == 1, "Scheduled task was not executed");
        Thread.sleep(5);
        assertEquals(1, counter1.get(), "Should only run once");
    }

    @Test
    void testNonPeriodicTaskWhenPeriodIsZero() throws InterruptedException {
        scheduler.schedule("test", counter1::getAndIncrement, 0, 0);
        TestUtils.waitForCondition(() -> counter1.get() == 1, "Scheduled task was not executed");
        Thread.sleep(5);
        assertEquals(1, counter1.get(), "Should only run once");
    }

    @Test
    void testPeriodicTask() throws InterruptedException {
        scheduler.schedule("test", counter1::getAndIncrement, 0, 5);
        TestUtils.waitForCondition(() -> counter1.get() >= 20, "Should count to 20");
    }

    @Test
    void testRestart() throws InterruptedException {
        // schedule a task to increment a counter
        mockTime.scheduler.scheduleOnce("test1", counter1::getAndIncrement, 1);
        mockTime.sleep(1);
        assertEquals(1, counter1.get());

        // restart the scheduler
        mockTime.scheduler.shutdown();
        mockTime.scheduler.startup();

        // schedule another task to increment the counter
        mockTime.scheduler.scheduleOnce("test1", counter1::getAndIncrement, 1);
        mockTime.sleep(1);
        assertEquals(2, counter1.get());
    }

    @Test
    void testUnscheduleProducerTask() throws IOException {
        File tmpDir = TestUtils.tempDirectory();
        File logDir = TestUtils.randomPartitionLogDir(tmpDir);
        LogConfig logConfig = new LogConfig(new Properties());
        BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
        int maxTransactionTimeoutMs = 5 * 60 * 1000;
        int maxProducerIdExpirationMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT;
        int producerIdExpirationCheckIntervalMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT;
        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(logDir);
        LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(10);
        LogSegments segments = new LogSegments(topicPartition);
        LeaderEpochFileCache leaderEpochCache = UnifiedLog.createLeaderEpochCache(logDir, topicPartition,
                logDirFailureChannel, Optional.empty(), mockTime.scheduler);
        ProducerStateManagerConfig producerStateManagerConfig = new ProducerStateManagerConfig(maxProducerIdExpirationMs, false);
        ProducerStateManager producerStateManager = new ProducerStateManager(topicPartition, logDir,
                maxTransactionTimeoutMs, producerStateManagerConfig, mockTime);
        LoadedLogOffsets offsets = new LogLoader(
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
                leaderEpochCache,
                producerStateManager,
                new ConcurrentHashMap<>(), false).load();
        LocalLog localLog = new LocalLog(logDir, logConfig, segments, offsets.recoveryPoint(),
                offsets.nextOffsetMetadata(), scheduler, mockTime, topicPartition, logDirFailureChannel);
        UnifiedLog log = new UnifiedLog(offsets.logStartOffset(),
                localLog,
                brokerTopicStats,
                producerIdExpirationCheckIntervalMs,
                leaderEpochCache,
                producerStateManager,
                Optional.empty(),
                false,
                LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
        assertTrue(scheduler.taskRunning(log.producerExpireCheck()));
        log.close();
        assertFalse(scheduler.taskRunning(log.producerExpireCheck()));
    }

    /**
     * Verify that scheduler lock is not held when invoking task method, allowing new tasks to be scheduled
     * when another is being executed. This is required to avoid deadlocks when:
     * <ul>
     *     <li>Thread1 executes a task which attempts to acquire LockA</li>
     *     <li>Thread2 holding LockA attempts to schedule a new task</li>
     * </ul>
     */
    @Timeout(15)
    @Test
    void testMockSchedulerLocking() throws InterruptedException {
        CountDownLatch initLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(2);
        List<CountDownLatch> taskLatches = List.of(new CountDownLatch(1), new CountDownLatch(1));
        InterruptedConsumer<CountDownLatch> scheduledTask = taskLatch -> {
            initLatch.countDown();
            assertTrue(taskLatch.await(30, TimeUnit.SECONDS), "Timed out waiting for latch");
            completionLatch.countDown();
        };
        mockTime.scheduler.scheduleOnce("test1", interruptedRunnableWrapper(() -> scheduledTask.accept(taskLatches.get(0))), 1);
        ScheduledExecutorService tickExecutor = Executors.newSingleThreadScheduledExecutor();
        try {
            tickExecutor.scheduleWithFixedDelay(() -> mockTime.sleep(1), 0, 1, TimeUnit.MILLISECONDS);

            // wait for first task to execute and then schedule the next task while the first one is running
            assertTrue(initLatch.await(10, TimeUnit.SECONDS));
            mockTime.scheduler.scheduleOnce("test2", interruptedRunnableWrapper(() -> scheduledTask.accept(taskLatches.get(1))), 1);

            taskLatches.forEach(CountDownLatch::countDown);
            assertTrue(completionLatch.await(10, TimeUnit.SECONDS), "Tasks did not complete");
        } finally {
            tickExecutor.shutdownNow();
        }
    }

    @Test
    void testPendingTaskSize() throws InterruptedException {
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(2);
        scheduler.scheduleOnce("task1", interruptedRunnableWrapper(latch1::await), 0);
        scheduler.scheduleOnce("task2", latch2::countDown, 5);
        scheduler.scheduleOnce("task3", latch2::countDown, 5);
        TestUtils.waitForCondition(() -> scheduler.pendingTaskSize() <= 2, "Scheduled task was not executed");
        latch1.countDown();
        latch2.await();
        TestUtils.waitForCondition(() -> scheduler.pendingTaskSize() == 0, "Scheduled task was not executed");
        scheduler.shutdown();
        assertEquals(0, scheduler.pendingTaskSize());
    }

    @FunctionalInterface
    private interface InterruptedConsumer<T> {
        void accept(T t) throws InterruptedException;
    }

    @FunctionalInterface
    private interface InterruptedRunnable {
        void run() throws InterruptedException;
    }

    private static Runnable interruptedRunnableWrapper(InterruptedRunnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        };
    }
}
