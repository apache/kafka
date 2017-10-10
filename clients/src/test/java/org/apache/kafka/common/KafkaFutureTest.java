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
package org.apache.kafka.common;

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

/**
 * A unit test for KafkaFuture.
 */
public class KafkaFutureTest {

    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testCompleteFutures() throws Exception {
        KafkaFutureImpl<Integer> future123 = new KafkaFutureImpl<>();
        assertTrue(future123.complete(123));
        assertEquals(Integer.valueOf(123), future123.get());
        assertFalse(future123.complete(456));
        assertTrue(future123.isDone());
        assertFalse(future123.isCancelled());
        assertFalse(future123.isCompletedExceptionally());

        KafkaFuture<Integer> future456 = KafkaFuture.completedFuture(456);
        assertEquals(Integer.valueOf(456), future456.get());

        KafkaFutureImpl<Integer> futureFail = new KafkaFutureImpl<>();
        futureFail.completeExceptionally(new RuntimeException("We require more vespene gas"));
        try {
            futureFail.get();
            Assert.fail("Expected an exception");
        } catch (ExecutionException e) {
            assertEquals(RuntimeException.class, e.getCause().getClass());
            Assert.assertEquals("We require more vespene gas", e.getCause().getMessage());
        }
    }

    @Test
    public void testCompletingFutures() throws Exception {
        final KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        CompleterThread myThread = new CompleterThread(future, "You must construct additional pylons.");
        assertFalse(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        assertFalse(future.isCancelled());
        assertEquals("I am ready", future.getNow("I am ready"));
        myThread.start();
        String str = future.get(5, TimeUnit.MINUTES);
        assertEquals("You must construct additional pylons.", str);
        assertEquals("You must construct additional pylons.", future.getNow("I am ready"));
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        assertFalse(future.isCancelled());
        myThread.join();
        assertEquals(null, myThread.testException);
    }

    private static class CompleterThread<T> extends Thread {

        private final KafkaFutureImpl<T> future;
        private final T value;
        Throwable testException = null;

        CompleterThread(KafkaFutureImpl<T> future, T value) {
            this.future = future;
            this.value = value;
        }

        @Override
        public void run() {
            try {
                try {
                    Thread.sleep(0, 200);
                } catch (InterruptedException e) {
                }
                future.complete(value);
            } catch (Throwable testException) {
                this.testException = testException;
            }
        }
    }

    private static class WaiterThread<T> extends Thread {

        private final KafkaFutureImpl<T> future;
        private final T expected;
        Throwable testException = null;

        WaiterThread(KafkaFutureImpl<T> future, T expected) {
            this.future = future;
            this.expected = expected;
        }

        @Override
        public void run() {
            try {
                T value = future.get();
                assertEquals(expected, value);
            } catch (Throwable testException) {
                this.testException = testException;
            }
        }
    }

    @Test
    public void testAllOfFutures() throws Exception {
        final int numThreads = 5;
        final List<KafkaFutureImpl<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            futures.add(new KafkaFutureImpl<Integer>());
        }
        KafkaFuture<Void> allFuture = KafkaFuture.allOf(futures.toArray(new KafkaFuture[0]));
        final List<CompleterThread> completerThreads = new ArrayList<>();
        final List<WaiterThread> waiterThreads = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            completerThreads.add(new CompleterThread<>(futures.get(i), i));
            waiterThreads.add(new WaiterThread<>(futures.get(i), i));
        }
        assertFalse(allFuture.isDone());
        for (int i = 0; i < numThreads; i++) {
            waiterThreads.get(i).start();
        }
        for (int i = 0; i < numThreads - 1; i++) {
            completerThreads.get(i).start();
        }
        assertFalse(allFuture.isDone());
        completerThreads.get(numThreads - 1).start();
        allFuture.get();
        assertTrue(allFuture.isDone());
        for (int i = 0; i < numThreads; i++) {
            assertEquals(Integer.valueOf(i), futures.get(i).get());
        }
        for (int i = 0; i < numThreads; i++) {
            completerThreads.get(i).join();
            waiterThreads.get(i).join();
            assertEquals(null, completerThreads.get(i).testException);
            assertEquals(null, waiterThreads.get(i).testException);
        }
    }

    @Test
    public void testAllOfFuturesHandlesZeroFutures() throws Exception {
        KafkaFuture<Void> allFuture = KafkaFuture.allOf();
        assertTrue(allFuture.isDone());
        assertFalse(allFuture.isCancelled());
        assertFalse(allFuture.isCompletedExceptionally());
        allFuture.get();
    }

    @Test(expected = TimeoutException.class)
    public void testFutureTimeoutWithZeroWait() throws Exception {
        final KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        future.get(0, TimeUnit.MILLISECONDS);
    }

}
