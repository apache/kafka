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

package org.apache.kafka.server.share.dlq;

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.share.metrics.ShareGroupMetrics;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class DefaultShareGroupDLQManagerTest {
    private static final int FETCH_THREADS = 3;
    private static final String FETCH_THREAD_PREFIX = "share-group-dlq-record-fetcher-";

    private final MockTime time = new MockTime();
    private DefaultShareGroupDLQManager manager;

    @BeforeEach
    public void setUp() {
        manager = (DefaultShareGroupDLQManager) DefaultShareGroupDLQManager.instance(
            new MockClient(time),
            mock(ShareGroupDLQMetadataCacheHelper.class),
            time,
            new MockTimer(time),
            mock(ShareGroupMetrics.class),
            () -> FETCH_THREADS,
            mock(LogReader.class)
        );
    }

    @AfterEach
    public void tearDown() {
        manager.stop();
    }

    @Test
    public void testFetchThreadsAreNamedDaemonThreads() throws Exception {
        ExecutorService executor = manager.fetchExecutor();
        List<Future<Thread>> threads = new ArrayList<>();
        // Hold every thread busy at once so each of the pool's threads is started.
        CountDownLatch allStarted = new CountDownLatch(FETCH_THREADS);
        for (int i = 0; i < FETCH_THREADS; i++) {
            threads.add(executor.submit(() -> {
                allStarted.countDown();
                allStarted.await(10, TimeUnit.SECONDS);
                return Thread.currentThread();
            }));
        }

        for (Future<Thread> future : threads) {
            Thread thread = future.get(10, TimeUnit.SECONDS);
            assertTrue(thread.isDaemon());
            assertTrue(thread.getName().startsWith(FETCH_THREAD_PREFIX), thread.getName());
        }
        assertEquals(FETCH_THREADS,
            threads.stream().map(f -> {
                try {
                    return f.get();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }).distinct().count());
    }

    @Test
    public void testStopTerminatesFetchExecutor() {
        ExecutorService executor = manager.fetchExecutor();
        assertFalse(executor.isShutdown());

        manager.stop();

        assertTrue(executor.isTerminated());
        assertThrows(java.util.concurrent.RejectedExecutionException.class, () -> executor.execute(() -> { }));
    }

    @Test
    public void testStopIsIdempotent() {
        manager.stop();
        manager.stop();

        assertTrue(manager.fetchExecutor().isTerminated());
    }

    @Test
    public void testStopWaitsForReadsAlreadyRunning() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<Boolean> read = manager.fetchExecutor().submit(() -> {
            started.countDown();
            return release.await(10, TimeUnit.SECONDS);
        });
        assertTrue(started.await(10, TimeUnit.SECONDS));

        Thread stopper = new Thread(manager::stop);
        stopper.start();
        // stop() is blocked on the running read until it finishes.
        stopper.join(200);
        assertTrue(stopper.isAlive());
        release.countDown();
        stopper.join(10_000);

        assertFalse(stopper.isAlive());
        assertTrue(read.get(10, TimeUnit.SECONDS));
        assertTrue(manager.fetchExecutor().isTerminated());
    }

    @Test
    public void testStopRunsQueuedReadsAndDoesNotInterruptRunningOnes() throws Exception {
        // Occupy every fetch thread so the next task has to wait in the queue.
        CountDownLatch allStarted = new CountDownLatch(FETCH_THREADS);
        CountDownLatch release = new CountDownLatch(1);
        List<Future<Boolean>> running = new ArrayList<>();
        for (int i = 0; i < FETCH_THREADS; i++) {
            running.add(manager.fetchExecutor().submit(() -> {
                allStarted.countDown();
                // True only if released normally, false if interrupted.
                try {
                    return release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    return false;
                }
            }));
        }
        assertTrue(allStarted.await(10, TimeUnit.SECONDS));
        Future<String> queued = manager.fetchExecutor().submit(() -> "ran");

        Thread stopper = new Thread(manager::stop);
        stopper.start();
        stopper.join(200);
        assertTrue(stopper.isAlive());
        assertFalse(queued.isDone());
        release.countDown();
        stopper.join(10_000);

        assertFalse(stopper.isAlive());
        assertEquals("ran", queued.get(10, TimeUnit.SECONDS));
        for (Future<Boolean> read : running) {
            assertTrue(read.get(10, TimeUnit.SECONDS));
        }
    }
}
