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
package org.apache.kafka.server.purgatory;

import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class DelayedOperationTest {

    private final MockKey test1 = new MockKey("test1");
    private final MockKey test2 = new MockKey("test2");
    private final MockKey test3 = new MockKey("test3");
    private final Random random = new Random();
    private DelayedOperationPurgatory<DelayedOperation> purgatory;
    private ScheduledExecutorService executorService;

    @BeforeEach
    public void setUp() {
        purgatory = new DelayedOperationPurgatory<>("mock", 0);
    }

    @AfterEach
    public void tearDown() throws Exception {
        purgatory.shutdown();
        if (executorService != null)
            executorService.shutdown();
    }

    private static class MockKey implements DelayedOperationKey {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MockKey mockKey = (MockKey) o;
            return Objects.equals(key, mockKey.key);
        }

        @Override
        public int hashCode() {
            return key != null ? key.hashCode() : 0;
        }

        final String key;

        MockKey(String key) {
            this.key = key;
        }

        @Override
        public String keyLabel() {
            return key;
        }
    }

    @Test
    public void testLockInTryCompleteElseWatch() {
        DelayedOperation op = new DelayedOperation(100000L) {
            @Override
            public void onExpiration() {}
            @Override
            public void onComplete() {}
            @Override
            public boolean tryComplete() {
                assertTrue(((ReentrantLock) lock).isHeldByCurrentThread());
                return false;
            }
            @Override
            public boolean safeTryComplete() {
                fail("tryCompleteElseWatch should not use safeTryComplete");
                return super.safeTryComplete();
            }
        };
        purgatory.tryCompleteElseWatch(op, Collections.singletonList(new MockKey("key")));
    }

    private DelayedOperation op(boolean shouldComplete) {
        return new DelayedOperation(100000L) {
            @Override
            public void onExpiration() {}

            @Override
            public void onComplete() {}

            @Override
            public boolean tryComplete() {
                assertTrue(((ReentrantLock) lock).isHeldByCurrentThread());
                return shouldComplete;
            }
        };
    }

    @Test
    public void testSafeTryCompleteOrElse() {
        final AtomicBoolean pass = new AtomicBoolean();
        assertFalse(op(false).safeTryCompleteOrElse(() -> pass.set(true)));
        assertTrue(pass.get());
        assertTrue(op(true).safeTryCompleteOrElse(() -> fail("this method should NOT be executed")));
    }

    @Test
    public void testRequestSatisfaction() {
        MockDelayedOperation r1 = new MockDelayedOperation(100000L);
        MockDelayedOperation r2 = new MockDelayedOperation(100000L);
        assertEquals(0, purgatory.checkAndComplete(test1), "With no waiting requests, nothing should be satisfied");
        assertFalse(purgatory.tryCompleteElseWatch(r1, Collections.singletonList(new MockKey("test1"))), "r1 not satisfied and hence watched");
        assertEquals(0, purgatory.checkAndComplete(test1), "Still nothing satisfied");
        assertFalse(purgatory.tryCompleteElseWatch(r2, Collections.singletonList(new MockKey("test2"))), "r2 not satisfied and hence watched");
        assertEquals(0, purgatory.checkAndComplete(test2), "Still nothing satisfied");
        r1.completable = true;
        assertEquals(1, purgatory.checkAndComplete(test1), "r1 satisfied");
        assertEquals(0, purgatory.checkAndComplete(test1), "Nothing satisfied");
        r2.completable = true;
        assertEquals(1, purgatory.checkAndComplete(test2), "r2 satisfied");
        assertEquals(0, purgatory.checkAndComplete(test2), "Nothing satisfied");
    }

    @Test
    public void testRequestExpiry() throws Exception {
        long expiration = 20L;
        long start = Time.SYSTEM.hiResClockMs();
        MockDelayedOperation r1 = new MockDelayedOperation(expiration);
        MockDelayedOperation r2 = new MockDelayedOperation(200000L);
        assertFalse(purgatory.tryCompleteElseWatch(r1, Collections.singletonList(test1)), "r1 not satisfied and hence watched");
        assertFalse(purgatory.tryCompleteElseWatch(r2, Collections.singletonList(test2)), "r2 not satisfied and hence watched");
        r1.awaitExpiration();
        long elapsed = Time.SYSTEM.hiResClockMs() - start;
        assertTrue(r1.isCompleted(), "r1 completed due to expiration");
        assertFalse(r2.isCompleted(), "r2 hasn't completed");
        assertTrue(elapsed >= expiration, "Time for expiration " + elapsed + " should at least " + expiration);
    }

    @Test
    public void testRequestPurge() {
        MockDelayedOperation r1 = new MockDelayedOperation(100000L);
        MockDelayedOperation r2 = new MockDelayedOperation(100000L);
        MockDelayedOperation r3 = new MockDelayedOperation(100000L);
        purgatory.tryCompleteElseWatch(r1, Collections.singletonList(test1));
        purgatory.tryCompleteElseWatch(r2, Arrays.asList(test1, test2));
        purgatory.tryCompleteElseWatch(r3, Arrays.asList(test1, test2, test3));

        assertEquals(3, purgatory.numDelayed(), "Purgatory should have 3 total delayed operations");
        assertEquals(6, purgatory.watched(), "Purgatory should have 6 watched elements");

        // complete the operations, it should immediately be purged from the delayed operation
        r2.completable = true;
        r2.tryComplete();
        assertEquals(2, purgatory.numDelayed(), "Purgatory should have 2 total delayed operations instead of " + purgatory.numDelayed());

        r3.completable = true;
        r3.tryComplete();
        assertEquals(1, purgatory.numDelayed(), "Purgatory should have 1 total delayed operations instead of " + purgatory.numDelayed());

        // checking a watch should purge the watch list
        purgatory.checkAndComplete(test1);
        assertEquals(4, purgatory.watched(), "Purgatory should have 4 watched elements instead of " + purgatory.watched());

        purgatory.checkAndComplete(test2);
        assertEquals(2, purgatory.watched(), "Purgatory should have 2 watched elements instead of " + purgatory.watched());

        purgatory.checkAndComplete(test3);
        assertEquals(1, purgatory.watched(), "Purgatory should have 1 watched elements instead of " + purgatory.watched());
    }

    @Test
    public void shouldCancelForKeyReturningCancelledOperations() {
        purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Collections.singletonList(test1));
        purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Collections.singletonList(test1));
        purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Collections.singletonList(test2));

        List<DelayedOperation> cancelledOperations = purgatory.cancelForKey(test1);
        assertEquals(2, cancelledOperations.size());
        assertEquals(1, purgatory.numDelayed());
        assertEquals(1, purgatory.watched());
    }

    @Test
    public void shouldReturnNilOperationsOnCancelForKeyWhenKeyDoesntExist() {
        List<DelayedOperation> cancelledOperations = purgatory.cancelForKey(test1);
        assertTrue(cancelledOperations.isEmpty());
    }

    /**
     * Test `tryComplete` with multiple threads to verify that there are no timing windows
     * when completion is not performed even if the thread that makes the operation completable
     * may not be able to acquire the operation lock. Since it is difficult to test all scenarios,
     * this test uses random delays with a large number of threads.
     */
    @Test
    public void testTryCompleteWithMultipleThreads() throws ExecutionException, InterruptedException {
        executorService = Executors.newScheduledThreadPool(20);
        int maxDelayMs = 10;
        int completionAttempts = 20;
        List<TestDelayOperation> ops = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            TestDelayOperation op = new TestDelayOperation(i, completionAttempts, maxDelayMs);
            purgatory.tryCompleteElseWatch(op, Collections.singletonList(op.key));
            ops.add(op);
        }

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 1; i <= completionAttempts; i++) {
            for (TestDelayOperation op : ops) {
                futures.add(scheduleTryComplete(executorService, op, random.nextInt(maxDelayMs)));
            }
        }
        for (Future<?> future : futures) {
            future.get();
        }
        ops.forEach(op -> assertTrue(op.isCompleted(), "Operation " + op.key.keyLabel() + " should have completed"));
    }

    private Future<?> scheduleTryComplete(ScheduledExecutorService executorService, TestDelayOperation op, long delayMs) {
        return executorService.schedule(() -> {
            if (op.completionAttemptsRemaining.decrementAndGet() == 0) {
                op.completable = true;
            }
            purgatory.checkAndComplete(op.key);
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private static class MockDelayedOperation extends DelayedOperation {

        private final Optional<Lock> responseLockOpt;
        boolean completable = false;

        MockDelayedOperation(long delayMs) {
            this(delayMs, Optional.empty());
        }

        MockDelayedOperation(long delayMs, Optional<Lock> responseLockOpt) {
            super(delayMs);
            this.responseLockOpt = responseLockOpt;
        }

        @Override
        public boolean tryComplete() {
            if (completable) {
                return forceComplete();
            } else {
                return false;
            }
        }

        @Override
        public void onExpiration() { }

        @Override
        public void onComplete() {
            responseLockOpt.ifPresent(lock -> {
                if (!lock.tryLock())
                    throw new IllegalStateException("Response callback lock could not be acquired in callback");
            });
            synchronized (this) {
                notify();
            }
        }

        void awaitExpiration() throws InterruptedException {
            synchronized (this) {
                wait();
            }
        }
    }

    private class TestDelayOperation extends MockDelayedOperation {

        private final MockKey key;
        private final AtomicInteger completionAttemptsRemaining;
        private final int maxDelayMs;

        TestDelayOperation(int index, int completionAttempts, int maxDelayMs) {
            super(10000L, Optional.empty());
            key = new MockKey("key" + index);
            completionAttemptsRemaining = new AtomicInteger(completionAttempts);
            this.maxDelayMs = maxDelayMs;
        }

        @Override
        public boolean tryComplete() {
            boolean shouldComplete = completable;
            try {
                Thread.sleep(random.nextInt(maxDelayMs));
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            if (shouldComplete)
                return forceComplete();
            else
                return false;
        }
    }
}
