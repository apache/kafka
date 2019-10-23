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
package org.apache.kafka.connect.util;

import org.junit.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConvertingFutureCallbackTest {
  
    @Test
    public void shouldConvertBeforeGetOnSuccessfulCompletion() throws Exception {
        final Object expectedConversion = new Object();
        TestConvertingFutureCallback testCallback = new TestConvertingFutureCallback();
        testCallback.onCompletion(null, expectedConversion);
        assertEquals(1, testCallback.numberOfConversions());
        assertEquals(expectedConversion, testCallback.get());
    }

    @Test
    public void shouldConvertOnlyOnceBeforeGetOnSuccessfulCompletion() throws Exception {
        final Object expectedConversion = new Object();
        TestConvertingFutureCallback testCallback = new TestConvertingFutureCallback();
        testCallback.onCompletion(null, expectedConversion);
        testCallback.onCompletion(null, 69);
        testCallback.cancel(true);
        testCallback.onCompletion(new RuntimeException(), null);
        assertEquals(1, testCallback.numberOfConversions());
        assertEquals(expectedConversion, testCallback.get());
    }

    @Test
    public void shouldNotConvertBeforeGetOnFailedCompletion() throws Exception {
        final Throwable expectedError = new Throwable();
        TestConvertingFutureCallback testCallback = new TestConvertingFutureCallback();
        testCallback.onCompletion(expectedError, null);
        assertEquals(0, testCallback.numberOfConversions());
        try {
            testCallback.get();
        } catch (ExecutionException e) {
            assertEquals(expectedError, e.getCause());
        }
    }

    @Test
    public void shouldRecordOnlyFirstErrorBeforeGetOnFailedCompletion() throws Exception {
        final Throwable expectedError = new Throwable();
        TestConvertingFutureCallback testCallback = new TestConvertingFutureCallback();
        testCallback.onCompletion(expectedError, null);
        testCallback.onCompletion(new RuntimeException(), null);
        testCallback.cancel(true);
        testCallback.onCompletion(null, "420");
        assertEquals(0, testCallback.numberOfConversions());
        try {
            testCallback.get();
        } catch (ExecutionException e) {
            assertEquals(expectedError, e.getCause());
        }
    }
  
    @Test(expected = CancellationException.class)
    public void shouldCancelBeforeGetIfMayCancelWhileRunning() throws Exception {
        TestConvertingFutureCallback testCallback = new TestConvertingFutureCallback();
        assertTrue(testCallback.cancel(true));
        testCallback.get();
    }
  
    @Test
    public void shouldNotCancelBeforeGetIfMayNotCancelWhileRunning() throws Exception {
        TestConvertingFutureCallback testCallback = new TestConvertingFutureCallback();
        assertFalse(testCallback.cancel(false));
        final Object expectedConversion = new Object();
        testCallback.onCompletion(null, expectedConversion);
        assertEquals(1, testCallback.numberOfConversions());
        assertEquals(expectedConversion, testCallback.get());
    }

    @Test
    public void shouldBlockUntilSuccessfulCompletion() throws Exception {
        AtomicReference<Exception> testThreadException = new AtomicReference<>();
        TestConvertingFutureCallback testCallback = new TestConvertingFutureCallback();
        final Object expectedConversion = new Object();
        runSeparateTestThread(() -> {
            try {
                testCallback.waitForGet();
                testCallback.onCompletion(null, expectedConversion);
            } catch (Exception e) {
                testThreadException.compareAndSet(null, e);
            }
        });
        assertFalse(testCallback.isDone());
        assertEquals(expectedConversion, testCallback.get());
        assertEquals(1, testCallback.numberOfConversions());
        assertTrue(testCallback.isDone());
        if (testThreadException.get() != null) {
            throw testThreadException.get();
        }
    }

    @Test
    public void shouldBlockUntilFailedCompletion() throws Exception {
        AtomicReference<Exception> testThreadException = new AtomicReference<>();
        TestConvertingFutureCallback testCallback = new TestConvertingFutureCallback();
        final Throwable expectedError = new Throwable();
        runSeparateTestThread(() -> {
            try {
                testCallback.waitForGet();
                testCallback.onCompletion(expectedError, null);
            } catch (Exception e) {
                testThreadException.compareAndSet(null, e);
            }
        });
        assertFalse(testCallback.isDone());
        try {
            testCallback.get();
        } catch (ExecutionException e) {
            assertEquals(expectedError, e.getCause());
        }
        assertEquals(0, testCallback.numberOfConversions());
        assertTrue(testCallback.isDone());
        if (testThreadException.get() != null) {
            throw testThreadException.get();
        }
    }

    @Test(expected = CancellationException.class)
    public void shouldBlockUntilCancellation() throws Exception {
        AtomicReference<Exception> testThreadException = new AtomicReference<>();
        TestConvertingFutureCallback testCallback = new TestConvertingFutureCallback();
        runSeparateTestThread(() -> {
            try {
                testCallback.waitForGet();
                testCallback.cancel(true);
            } catch (Exception e) {
                testThreadException.compareAndSet(null, e);
            }
        });
        assertFalse(testCallback.isDone());
        testCallback.get();
        if (testThreadException.get() != null) {
            throw testThreadException.get();
        }
    }

    protected static void runSeparateTestThread(Runnable task) {
        Thread t = new Thread(task);
        t.setDaemon(true);
        t.start();
    }
  
    protected static class TestConvertingFutureCallback extends ConvertingFutureCallback<Object, Object> {
        private AtomicInteger numberOfConversions = new AtomicInteger();
        private CountDownLatch getInvoked = new CountDownLatch(1);
    
        public int numberOfConversions() {
            return numberOfConversions.get();
        }

        public void waitForGet() throws InterruptedException {
            getInvoked.await();
        }
    
        @Override
        public Object convert(Object result) {
            numberOfConversions.incrementAndGet();
            return result;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            getInvoked.countDown();
            return super.get();
        }

        @Override
        public Object get(
            long duration,
            TimeUnit unit
        ) throws InterruptedException, ExecutionException, TimeoutException {
            getInvoked.countDown();
            return super.get(duration, unit);
        }
    }
}
