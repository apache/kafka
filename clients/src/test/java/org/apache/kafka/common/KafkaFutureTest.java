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
import java.util.concurrent.CancellationException;
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

    @Test
    public void testThenApply() throws Exception {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        KafkaFuture<Integer> doubledFuture = future.thenApply(new KafkaFuture.BaseFunction<Integer, Integer>() {
            @Override
            public Integer apply(Integer integer) {
                return 2 * integer;
            }
        });
        assertFalse(doubledFuture.isDone());
        KafkaFuture<Integer> tripledFuture = future.thenApply(new KafkaFuture.Function<Integer, Integer>() {
            @Override
            public Integer apply(Integer integer) {
                return 3 * integer;
            }
        });
        assertFalse(tripledFuture.isDone());
        future.complete(21);
        assertEquals(Integer.valueOf(21), future.getNow(-1));
        assertEquals(Integer.valueOf(42), doubledFuture.getNow(-1));
        assertEquals(Integer.valueOf(63), tripledFuture.getNow(-1));
        KafkaFuture<Integer> quadrupledFuture = future.thenApply(new KafkaFuture.BaseFunction<Integer, Integer>() {
            @Override
            public Integer apply(Integer integer) {
                return 4 * integer;
            }
        });
        assertEquals(Integer.valueOf(84), quadrupledFuture.getNow(-1));

        KafkaFutureImpl<Integer> futureFail = new KafkaFutureImpl<>();
        KafkaFuture<Integer> futureAppliedFail = futureFail.thenApply(new KafkaFuture.BaseFunction<Integer, Integer>() {
            @Override
            public Integer apply(Integer integer) {
                return 2 * integer;
            }
        });
        futureFail.completeExceptionally(new RuntimeException());
        assertTrue(futureFail.isCompletedExceptionally());
        assertTrue(futureAppliedFail.isCompletedExceptionally());
    }

    @Test
    public void testWhenCompleteNormalCompletion() {
        String value = "Ready to roll out!";
        KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        QueryableBiConsumer consumer = new QueryableBiConsumer();
        future.whenComplete(consumer);
        future.complete(value);
        assertTrue(consumer.getThrowable() == null);
        assertEquals(value, consumer.getValue());
    }

    @Test
    public void testWhenCompleteExceptionalCompletion() {
        RuntimeException exception = new RuntimeException("I'm in deep!");
        KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        QueryableBiConsumer consumer = new QueryableBiConsumer();
        future.whenComplete(consumer);
        future.completeExceptionally(exception);
        assertTrue(consumer.getValue() == null);
        assertEquals(exception, consumer.getThrowable());
    }

    @Test
    public void testWhenCompleteChained() {
        String value = "Fueled up, ready to go!";
        RuntimeException exceptionOnCompletion = new RuntimeException("I'm about to drop the hammer");
        KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        QueryableBiConsumer firstConsumer = new QueryableBiConsumer() {
            @Override
            public void accept(String s, Throwable throwable) {
                super.accept(s, throwable);
                throw exceptionOnCompletion;
            }
        };
        QueryableBiConsumer secondConsumer = new QueryableBiConsumer();
        QueryableBiConsumer thirdConsumer = new QueryableBiConsumer();
        KafkaFuture<String> futureByWhenComplete = future.whenComplete(firstConsumer);
        future.whenComplete(secondConsumer);
        futureByWhenComplete.whenComplete(thirdConsumer);
        future.complete(value);
        assertTrue(firstConsumer.getThrowable() == null);
        assertEquals(value, firstConsumer.getValue());
        assertTrue(secondConsumer.getThrowable() == null);
        assertEquals(value, secondConsumer.getValue());
        assertTrue(thirdConsumer.getValue() == null);
        assertEquals(exceptionOnCompletion, thirdConsumer.getThrowable());
    }

    @Test
    public void testWhenCompleteChainedCompletedExceptionally() {
        RuntimeException exceptionOnCompletion = new RuntimeException("Can I take your order?");
        RuntimeException exceptionOnCompleteExceptionally = new RuntimeException("In the pipe, five by five");

        KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        QueryableBiConsumer firstConsumer = new QueryableBiConsumer() {
            @Override
            public void accept(String s, Throwable throwable) {
                super.accept(s, throwable);
                throw exceptionOnCompletion;
            }
        };
        QueryableBiConsumer secondConsumer = new QueryableBiConsumer();
        QueryableBiConsumer thirdConsumer = new QueryableBiConsumer();
        KafkaFuture<String> futureByWhenComplete = future.whenComplete(firstConsumer);
        future.whenComplete(secondConsumer);
        futureByWhenComplete.whenComplete(thirdConsumer);

        future.completeExceptionally(exceptionOnCompleteExceptionally);

        assertTrue(firstConsumer.getThrowable() == exceptionOnCompleteExceptionally);
        assertEquals(null, firstConsumer.getValue());
        assertTrue(secondConsumer.getThrowable() == exceptionOnCompleteExceptionally);
        assertEquals(null, secondConsumer.getValue());
        assertTrue(thirdConsumer.getValue() == null);
        assertEquals(exceptionOnCompleteExceptionally, thirdConsumer.getThrowable());
    }

    @Test
    public void testWhenCompleteAfterThenApplyThrows() {
        String value = "What's our target?";
        RuntimeException exceptionOnApply = new RuntimeException("What is your major malfunction?");
        KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        QueryableBiConsumer consumer = new QueryableBiConsumer();
        KafkaFuture.BaseFunction<String, String> throwerFunction = s -> {
            throw exceptionOnApply;
        };

        KafkaFuture<String> futureByThenApply = future.thenApply(throwerFunction);
        KafkaFuture<String> futureByWhenComplete = futureByThenApply.whenComplete(consumer);
        future.complete(value);
        assertTrue(consumer.getValue() == null);
        assertEquals(exceptionOnApply, consumer.getThrowable());
        assertTrue(futureByWhenComplete.isCompletedExceptionally());
    }

    @Test
    public void testWhenCompleteAfterCancel() {
        KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        QueryableBiConsumer first = new QueryableBiConsumer();
        QueryableBiConsumer second = new QueryableBiConsumer();
        KafkaFuture<String> futureByFirst = future.whenComplete(first);
        KafkaFuture<String> futureBySecond = futureByFirst.whenComplete(second);

        future.cancel(false);

        assertTrue(futureByFirst.isDone());
        assertTrue(futureByFirst.isCancelled());
        assertTrue(first.getThrowable() instanceof CancellationException);
        assertTrue(futureBySecond.isDone());
        assertTrue(futureBySecond.isCancelled());
        assertTrue(second.getThrowable() instanceof CancellationException);


    }

    @Test
    public void testCopyWith() throws Exception {
        String newValue = "I have returned";
        KafkaFutureImpl<String> dependee = new KafkaFutureImpl<>();
        KafkaFutureImpl<Integer> dependent = new KafkaFutureImpl<>();
        dependent.copyWith(dependee, String::length);

        dependee.complete(newValue);

        assertEquals(newValue, dependee.get());
        assertEquals(newValue.length(), (int) dependent.get());
    }

    @Test
    public void testCopyWithFunctionFails() throws Exception {
        String newValue = "Orders received";
        RuntimeException exception = new RuntimeException("I can't build there");
        KafkaFutureImpl<String> dependee = new KafkaFutureImpl<>();
        KafkaFutureImpl<String> dependent = new KafkaFutureImpl<>();
        dependent.copyWith(dependee, s -> {
            throw exception;
        });
        QueryableBiConsumer consumer = new QueryableBiConsumer();
        dependent.whenComplete(consumer);

        dependee.complete(newValue);

        assertEquals(newValue, dependee.get());
        assertTrue(consumer.getValue() == null);
        assertEquals(exception, consumer.getThrowable());
        assertEquals(newValue, dependee.get());

    }

    @Test
    public void testCopyWithCompletedExceptionally() throws Exception {
        String newValue = "Orders received";
        RuntimeException exception = new RuntimeException("I can't build it.");
        KafkaFutureImpl<String> dependee = new KafkaFutureImpl<>();
        KafkaFutureImpl<String> dependent = new KafkaFutureImpl<>();
        dependent.copyWith(dependee, s -> s + "Something in the way");
        QueryableBiConsumer consumer = new QueryableBiConsumer();
        dependent.whenComplete(consumer);

        dependee.completeExceptionally(exception);

        assertTrue(dependent.isCompletedExceptionally());
        assertTrue(consumer.getValue() == null);
        assertEquals(exception, consumer.getThrowable());
        assertTrue(dependee.isCompletedExceptionally());
    }

    @Test
    public void testCopyWithCancelled() throws Exception {
        KafkaFutureImpl<String> dependee = new KafkaFutureImpl<>();
        KafkaFutureImpl<String> dependent = new KafkaFutureImpl<>();
        dependent.copyWith(dependee, s -> "Affirmative");
        QueryableBiConsumer consumer = new QueryableBiConsumer();
        dependent.whenComplete(consumer);

        dependee.cancel(false);

        assertTrue(dependee.isCompletedExceptionally());
        assertTrue(dependent.isDone());
        assertTrue(dependent.isCompletedExceptionally());
        assertTrue(consumer.getValue() == null);
        assertTrue(consumer.getThrowable() instanceof CancellationException);
    }


    @Test
    public void testCancel() {
        KafkaFutureImpl<String> future = new KafkaFutureImpl<>();

        assertTrue("Must be able to cancel", future.cancel(false));

        assertTrue(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
        assertTrue(future.isDone());
    }

    private static class QueryableBiConsumer implements KafkaFuture.BiConsumer<String, Throwable> {

        private String value;
        private Throwable throwable;

        @Override
        public void accept(String s, Throwable throwable) {
            this.value = s;
            this.throwable = throwable;
        }

        public String getValue() {
            return value;
        }

        public Throwable getThrowable() {
            return throwable;
        }
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
