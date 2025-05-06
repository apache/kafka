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

import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchedulerTest {

    private final MockTime mockTime = new MockTime();
    private final KafkaScheduler scheduler = new KafkaScheduler(1);
    private final AtomicInteger counter1 = new AtomicInteger(0);
    private final AtomicInteger counter2 = new AtomicInteger(0);

    @BeforeEach
    public void setup() {
        scheduler.startup();
    }

    @AfterEach
    public void teardown() throws InterruptedException {
        scheduler.shutdown();
    }

    @Test
    public void testMockSchedulerNonPeriodicTask() {
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
    public void testMockSchedulerPeriodicTask() {
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
    public void testReentrantTaskInMockScheduler() {
        mockTime.scheduler.scheduleOnce(
            "test1", 
            () -> mockTime.scheduler.scheduleOnce("test2", counter2::getAndIncrement, 0), 
            1
        );
        mockTime.sleep(1);
        assertEquals(1, counter2.get());
    }

    @Test
    public void testNonPeriodicTask() throws InterruptedException {
        scheduler.scheduleOnce("test", counter1::getAndIncrement);
        retry(() -> assertEquals(1, counter1.get()));
        
        TimeUnit.MILLISECONDS.sleep(5);
        assertEquals(1, counter1.get(), "Should only run once");
    }

    @Test
    public void testNonPeriodicTaskWhenPeriodIsZero() throws InterruptedException {
        scheduler.schedule("test", counter1::getAndIncrement, 0, 0);
        retry(() -> assertEquals(1, counter1.get()));
        
        TimeUnit.MILLISECONDS.sleep(5);
        assertEquals(1, counter1.get(), "Should only run once");
    }

    @Test
    public void testPeriodicTask() {
        scheduler.schedule("test", counter1::getAndIncrement, 0, 5);
        retry(() -> assertTrue(counter1.get() >= 20, "Should count to 20"));
    }

    @Test
    public void testRestart() throws InterruptedException {
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
    public void testUnscheduleProducerTask() throws IOException {
        var tmpDir = TestUtils.tempDirectory();
        var logDir = TestUtils.randomPartitionLogDir(tmpDir);
        var logConfig = new LogConfig(new Properties());
        var brokerTopicStats = new BrokerTopicStats();
        var maxTransactionTimeoutMs = 5 * 60 * 1000;
        var maxProducerIdExpirationMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT;
        var producerIdExpirationCheckIntervalMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT;
        var topicPartition = UnifiedLog.parseTopicPartitionName(logDir);
        var logDirFailureChannel = new LogDirFailureChannel(10);
        var segments = new LogSegments(topicPartition);
        var leaderEpochCache = UnifiedLog.createLeaderEpochCache(
            logDir, 
            topicPartition, 
            logDirFailureChannel, 
            Optional.empty(), 
            mockTime.scheduler
        );
        var producerStateManagerConfig = new ProducerStateManagerConfig(
            maxProducerIdExpirationMs, 
            false
        );
        var producerStateManager = new ProducerStateManager(
            topicPartition, 
            logDir, 
            maxTransactionTimeoutMs,
            producerStateManagerConfig, 
            mockTime
        );
        var offsets = new LogLoader(
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
            new ConcurrentHashMap<>(),
            false
        ).load();
        var localLog = new LocalLog(
            logDir, 
            logConfig, 
            segments, 
            offsets.recoveryPoint,
            offsets.nextOffsetMetadata, 
            scheduler, 
            mockTime, 
            topicPartition, 
            logDirFailureChannel
        );
        var log = new UnifiedLog(
            offsets.logStartOffset,
            localLog,
            brokerTopicStats,
            producerIdExpirationCheckIntervalMs,
            leaderEpochCache,
            producerStateManager,
            Optional.empty(),
            false,
            LogOffsetsListener.NO_OP_OFFSETS_LISTENER
        );
        
        assertTrue(scheduler.taskRunning(log.producerExpireCheck()));
        log.close();
        assertFalse(scheduler.taskRunning(log.producerExpireCheck()));
    }

    /**
     * Verify that scheduler lock is not held when invoking task method, allowing new tasks to be scheduled
     * when another is being executed. This is required to avoid deadlocks when:
     * a) Thread1 executes a task which attempts to acquire LockA
     * b) Thread2 holding LockA attempts to schedule a new task
     */
    @Timeout(15)
    @Test
    public void testMockSchedulerLocking() throws InterruptedException {
        var initLatch = new CountDownLatch(1);
        var completionLatch = new CountDownLatch(2);
        var taskLatches = List.of(new CountDownLatch(1), new CountDownLatch(1));

        mockTime.scheduler.scheduleOnce("test1", () -> {
            initLatch.countDown();
            try {
                assertTrue(taskLatches.get(0)
                    .await(30, TimeUnit.SECONDS), 
                        "Timed out waiting for latch"
                );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            completionLatch.countDown();
        }, 1);

        var tickExecutor = Executors.newSingleThreadScheduledExecutor();
        try {
            tickExecutor.scheduleWithFixedDelay(() -> mockTime.sleep(1), 0, 1, TimeUnit.MILLISECONDS);

            // wait for first task to execute and then schedule the next task while the first one is running
            assertTrue(initLatch.await(10, TimeUnit.SECONDS));
            mockTime.scheduler.scheduleOnce("test2", () -> {
                try {
                    assertTrue(taskLatches.get(1)
                        .await(30, TimeUnit.SECONDS), 
                            "Timed out waiting for latch"
                    );
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                completionLatch.countDown();
            }, 1);

            for (CountDownLatch latch : taskLatches) {
                latch.countDown();
            }
            assertTrue(completionLatch.await(10, TimeUnit.SECONDS), "Tasks did not complete");
        } finally {
            tickExecutor.shutdown();
        }
    }

    @Test
    public void testPendingTaskSize() throws InterruptedException {
        var latch1 = new CountDownLatch(1);
        var latch2 = new CountDownLatch(2);
        Runnable task1 = () -> {
            try {
                latch1.await();
            } catch (InterruptedException ignored) {
                
            }
        };
        scheduler.scheduleOnce("task1", task1, 0);
        scheduler.scheduleOnce("task2", latch2::countDown, 5);
        scheduler.scheduleOnce("task3", latch2::countDown, 5);
        retry(() -> assertEquals(2, scheduler.pendingTaskSize()));
        latch1.countDown();
        latch2.await();
        retry(() -> assertEquals(0, scheduler.pendingTaskSize()));
        scheduler.shutdown();
        assertEquals(0, scheduler.pendingTaskSize());
    }

    private void retry(Runnable assertion) {
        var startTime = System.currentTimeMillis();
        while (true) {
            try {
                assertion.run();
                break;
            } catch (AssertionError e) {
                if (System.currentTimeMillis() > startTime + (long) 30000) {
                    throw e;
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException ignored) {

                }
            }
        }
    }
}
