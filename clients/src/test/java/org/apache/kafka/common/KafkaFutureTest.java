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
import org.apache.kafka.common.utils.Java;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A unit test for KafkaFuture.
 */
@Timeout(120)
public class KafkaFutureTest {

    /** Asserts that the given future is done, didn't fail and wasn't cancelled. */
    private void assertIsSuccessful(KafkaFuture<?> future) {
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        assertFalse(future.isCancelled());
    }

    /** Asserts that the given future is done, failed and wasn't cancelled. */
    private void assertIsFailed(KafkaFuture<?> future) {
        assertTrue(future.isDone());
        assertFalse(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
    }

    /** Asserts that the given future is done, didn't fail and was cancelled. */
    private void assertIsCancelled(KafkaFuture<?> future) {
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
        assertThrows(CancellationException.class, () -> future.getNow(null));
        assertThrows(CancellationException.class, () -> future.get(0, TimeUnit.MILLISECONDS));
    }

    private <T> void awaitAndAssertResult(KafkaFuture<T> future,
                                          T expectedResult,
                                          T alternativeValue) {
        assertNotEquals(expectedResult, alternativeValue);
        try {
            assertEquals(expectedResult, future.get(5, TimeUnit.MINUTES));
        } catch (Exception e) {
            throw new AssertionError("Unexpected exception", e);
        }
        try {
            assertEquals(expectedResult, future.get());
        } catch (Exception e) {
            throw new AssertionError("Unexpected exception", e);
        }
        try {
            assertEquals(expectedResult, future.getNow(alternativeValue));
        } catch (Exception e) {
            throw new AssertionError("Unexpected exception", e);
        }
    }

    private Throwable awaitAndAssertFailure(KafkaFuture<?> future,
                                            Class<? extends Throwable> expectedException,
                                            String expectedMessage) {
        ExecutionException executionException = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.MINUTES));
        assertEquals(expectedException, executionException.getCause().getClass());
        assertEquals(expectedMessage, executionException.getCause().getMessage());

        executionException = assertThrows(ExecutionException.class, () -> future.get());
        assertEquals(expectedException, executionException.getCause().getClass());
        assertEquals(expectedMessage, executionException.getCause().getMessage());

        executionException = assertThrows(ExecutionException.class, () -> future.getNow(null));
        assertEquals(expectedException, executionException.getCause().getClass());
        assertEquals(expectedMessage, executionException.getCause().getMessage());
        return executionException.getCause();
    }

    private void awaitAndAssertCancelled(KafkaFuture<?> future, String expectedMessage) {
        CancellationException cancellationException = assertThrows(CancellationException.class, () -> future.get(5, TimeUnit.MINUTES));
        assertEquals(expectedMessage, cancellationException.getMessage());
        assertEquals(CancellationException.class, cancellationException.getClass());

        cancellationException = assertThrows(CancellationException.class, () -> future.get());
        assertEquals(expectedMessage, cancellationException.getMessage());
        assertEquals(CancellationException.class, cancellationException.getClass());

        cancellationException = assertThrows(CancellationException.class, () -> future.getNow(null));
        assertEquals(expectedMessage, cancellationException.getMessage());
        assertEquals(CancellationException.class, cancellationException.getClass());
    }

    @Test
    public void testCompleteFutures() throws Exception {
        KafkaFutureImpl<Integer> future123 = new KafkaFutureImpl<>();
        assertTrue(future123.complete(123));
        assertFalse(future123.complete(456));
        assertFalse(future123.cancel(true));
        assertEquals(Integer.valueOf(123), future123.get());
        assertIsSuccessful(future123);

        KafkaFuture<Integer> future456 = KafkaFuture.completedFuture(456);
        assertFalse(future456.complete(789));
        assertFalse(future456.cancel(true));
        assertEquals(Integer.valueOf(456), future456.get());
        assertIsSuccessful(future456);
    }

    @Test
    public void testCompleteFuturesExceptionally() throws Exception {
        KafkaFutureImpl<Integer> futureFail = new KafkaFutureImpl<>();
        assertTrue(futureFail.completeExceptionally(new RuntimeException("We require more vespene gas")));
        assertIsFailed(futureFail);
        assertFalse(futureFail.completeExceptionally(new RuntimeException("We require more minerals")));
        assertFalse(futureFail.cancel(true));

        ExecutionException executionException = assertThrows(ExecutionException.class, () -> futureFail.get());
        assertEquals(RuntimeException.class, executionException.getCause().getClass());
        assertEquals("We require more vespene gas", executionException.getCause().getMessage());

        KafkaFutureImpl<Integer> tricky1 = new KafkaFutureImpl<>();
        assertTrue(tricky1.completeExceptionally(new CompletionException(new CancellationException())));
        assertIsFailed(tricky1);
        awaitAndAssertFailure(tricky1, CompletionException.class, "java.util.concurrent.CancellationException");
    }

    @Test
    public void testCompleteFuturesViaCancellation() {
        KafkaFutureImpl<Integer> viaCancel = new KafkaFutureImpl<>();
        assertTrue(viaCancel.cancel(true));
        assertIsCancelled(viaCancel);
        awaitAndAssertCancelled(viaCancel, null);

        KafkaFutureImpl<Integer> viaCancellationException = new KafkaFutureImpl<>();
        assertTrue(viaCancellationException.completeExceptionally(new CancellationException("We require more vespene gas")));
        assertIsCancelled(viaCancellationException);
        awaitAndAssertCancelled(viaCancellationException, "We require more vespene gas");
    }

    @Test
    public void testToString() {
        KafkaFutureImpl<Integer> success = new KafkaFutureImpl<>();
        assertEquals("KafkaFuture{value=null,exception=null,done=false}", success.toString());
        success.complete(12);
        assertEquals("KafkaFuture{value=12,exception=null,done=true}", success.toString());

        KafkaFutureImpl<Integer> failure = new KafkaFutureImpl<>();
        failure.completeExceptionally(new RuntimeException("foo"));
        assertEquals("KafkaFuture{value=null,exception=java.lang.RuntimeException: foo,done=true}", failure.toString());

        KafkaFutureImpl<Integer> tricky1 = new KafkaFutureImpl<>();
        tricky1.completeExceptionally(new CompletionException(new CancellationException()));
        assertEquals("KafkaFuture{value=null,exception=java.util.concurrent.CompletionException: java.util.concurrent.CancellationException,done=true}", tricky1.toString());

        KafkaFutureImpl<Integer> cancelled = new KafkaFutureImpl<>();
        cancelled.cancel(true);
        assertEquals("KafkaFuture{value=null,exception=java.util.concurrent.CancellationException,done=true}", cancelled.toString());
    }

    @Test
    public void testCompletingFutures() throws Exception {
        final KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        CompleterThread<String> myThread = new CompleterThread<>(future, "You must construct additional pylons.");
        assertIsNotCompleted(future);
        assertEquals("I am ready", future.getNow("I am ready"));
        myThread.start();
        awaitAndAssertResult(future, "You must construct additional pylons.", "I am ready");
        assertIsSuccessful(future);
        myThread.join();
        assertNull(myThread.testException);
    }

    @Test
    public void testCompletingFuturesExceptionally() throws Exception {
        final KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        CompleterThread<String> myThread = new CompleterThread<>(future, null,
                new RuntimeException("Ultimate efficiency achieved."));
        assertIsNotCompleted(future);
        assertEquals("I am ready", future.getNow("I am ready"));
        myThread.start();
        awaitAndAssertFailure(future, RuntimeException.class, "Ultimate efficiency achieved.");
        assertIsFailed(future);
        myThread.join();
        assertNull(myThread.testException);
    }

    @Test
    public void testCompletingFuturesViaCancellation() throws Exception {
        final KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        CompleterThread<String> myThread = new CompleterThread<>(future, null,
                new CancellationException("Ultimate efficiency achieved."));
        assertIsNotCompleted(future);
        assertEquals("I am ready", future.getNow("I am ready"));
        myThread.start();
        awaitAndAssertCancelled(future, "Ultimate efficiency achieved.");
        assertIsCancelled(future);
        myThread.join();
        assertNull(myThread.testException);
    }

    private void assertIsNotCompleted(KafkaFutureImpl<String> future) {
        assertFalse(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        assertFalse(future.isCancelled());
    }

    @Test
    public void testThenApplyOnSucceededFuture() throws Exception {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        KafkaFuture<Integer> doubledFuture = future.thenApply(integer -> 2 * integer);
        assertFalse(doubledFuture.isDone());
        KafkaFuture<Integer> tripledFuture = future.thenApply(integer -> 3 * integer);
        assertFalse(tripledFuture.isDone());
        future.complete(21);
        assertEquals(Integer.valueOf(21), future.getNow(-1));
        assertEquals(Integer.valueOf(42), doubledFuture.getNow(-1));
        assertEquals(Integer.valueOf(63), tripledFuture.getNow(-1));
        KafkaFuture<Integer> quadrupledFuture = future.thenApply(integer -> 4 * integer);
        assertEquals(Integer.valueOf(84), quadrupledFuture.getNow(-1));
    }

    @Test
    public void testThenApplyOnFailedFuture() {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        KafkaFuture<Integer> dependantFuture = future.thenApply(integer -> 2 * integer);
        future.completeExceptionally(new RuntimeException("We require more vespene gas"));
        assertIsFailed(future);
        assertIsFailed(dependantFuture);
        awaitAndAssertFailure(future, RuntimeException.class, "We require more vespene gas");
        awaitAndAssertFailure(dependantFuture, RuntimeException.class, "We require more vespene gas");
    }

    @Test
    public void testThenApplyOnFailedFutureTricky() {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        KafkaFuture<Integer> dependantFuture = future.thenApply(integer -> 2 * integer);
        future.completeExceptionally(new CompletionException(new RuntimeException("We require more vespene gas")));
        assertIsFailed(future);
        assertIsFailed(dependantFuture);
        awaitAndAssertFailure(future, CompletionException.class, "java.lang.RuntimeException: We require more vespene gas");
        awaitAndAssertFailure(dependantFuture, CompletionException.class, "java.lang.RuntimeException: We require more vespene gas");
    }

    @Test
    public void testThenApplyOnFailedFutureTricky2() {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        KafkaFuture<Integer> dependantFuture = future.thenApply(integer -> 2 * integer);
        future.completeExceptionally(new CompletionException(new CancellationException()));
        assertIsFailed(future);
        assertIsFailed(dependantFuture);
        awaitAndAssertFailure(future, CompletionException.class, "java.util.concurrent.CancellationException");
        awaitAndAssertFailure(dependantFuture, CompletionException.class, "java.util.concurrent.CancellationException");
    }

    @Test
    public void testThenApplyOnSucceededFutureAndFunctionThrows() {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        KafkaFuture<Integer> dependantFuture = future.thenApply(integer -> {
            throw new RuntimeException("We require more vespene gas");
        });
        future.complete(21);
        assertIsSuccessful(future);
        assertIsFailed(dependantFuture);
        awaitAndAssertResult(future, 21, null);
        awaitAndAssertFailure(dependantFuture, RuntimeException.class, "We require more vespene gas");
    }

    @Test
    public void testThenApplyOnSucceededFutureAndFunctionThrowsCompletionException() {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        KafkaFuture<Integer> dependantFuture = future.thenApply(integer -> {
            throw new CompletionException(new RuntimeException("We require more vespene gas"));
        });
        future.complete(21);
        assertIsSuccessful(future);
        assertIsFailed(dependantFuture);
        awaitAndAssertResult(future, 21, null);
        Throwable cause = awaitAndAssertFailure(dependantFuture, CompletionException.class, "java.lang.RuntimeException: We require more vespene gas");
        assertTrue(cause.getCause() instanceof RuntimeException);
        assertEquals(cause.getCause().getMessage(), "We require more vespene gas");
    }

    @Test
    public void testThenApplyOnFailedFutureFunctionNotCalled() {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        boolean[] ran = {false};
        KafkaFuture<Integer> dependantFuture = future.thenApply(integer -> {
            // Because the top level future failed, this should never be called.
            ran[0] = true;
            return null;
        });
        future.completeExceptionally(new RuntimeException("We require more minerals"));
        assertIsFailed(future);
        assertIsFailed(dependantFuture);
        awaitAndAssertFailure(future, RuntimeException.class, "We require more minerals");
        awaitAndAssertFailure(dependantFuture, RuntimeException.class, "We require more minerals");
        assertFalse(ran[0]);
    }

    @Test
    public void testThenApplyOnCancelledFuture() {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        KafkaFuture<Integer> dependantFuture = future.thenApply(integer -> 2 * integer);
        future.cancel(true);
        assertIsCancelled(future);
        assertIsCancelled(dependantFuture);
        awaitAndAssertCancelled(future, null);
        awaitAndAssertCancelled(dependantFuture, null);
    }

    @Test
    public void testWhenCompleteOnSucceededFuture() throws Throwable {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        Throwable[] err = new Throwable[1];
        boolean[] ran = {false};
        KafkaFuture<Integer> dependantFuture = future.whenComplete((integer, ex) -> {
            ran[0] = true;
            try {
                assertEquals(Integer.valueOf(21), integer);
                if (ex != null) {
                    throw ex;
                }
            } catch (Throwable e) {
                err[0] = e;
            }
        });
        assertFalse(dependantFuture.isDone());
        assertTrue(future.complete(21));
        assertTrue(ran[0]);
        if (err[0] != null) {
            throw err[0];
        }
    }

    @Test
    public void testWhenCompleteOnFailedFuture() {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        Throwable[] err = new Throwable[1];
        boolean[] ran = {false};
        KafkaFuture<Integer> dependantFuture = future.whenComplete((integer, ex) -> {
            ran[0] = true;
            err[0] = ex;
            if (integer != null) {
                err[0] = new AssertionError();
            }
        });
        assertFalse(dependantFuture.isDone());
        RuntimeException ex = new RuntimeException("We require more vespene gas");
        assertTrue(future.completeExceptionally(ex));
        assertTrue(ran[0]);
        assertEquals(err[0], ex);
    }

    @Test
    public void testWhenCompleteOnSucceededFutureAndConsumerThrows() {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        boolean[] ran = {false};
        KafkaFuture<Integer> dependantFuture = future.whenComplete((integer, ex) -> {
            ran[0] = true;
            throw new RuntimeException("We require more minerals");
        });
        assertFalse(dependantFuture.isDone());
        assertTrue(future.complete(21));
        assertIsSuccessful(future);
        assertTrue(ran[0]);
        assertIsFailed(dependantFuture);
        awaitAndAssertFailure(dependantFuture, RuntimeException.class, "We require more minerals");
    }

    @Test
    public void testWhenCompleteOnFailedFutureAndConsumerThrows() {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        boolean[] ran = {false};
        KafkaFuture<Integer> dependantFuture = future.whenComplete((integer, ex) -> {
            ran[0] = true;
            throw new RuntimeException("We require more minerals");
        });
        assertFalse(dependantFuture.isDone());
        assertTrue(future.completeExceptionally(new RuntimeException("We require more vespene gas")));
        assertIsFailed(future);
        assertTrue(ran[0]);
        assertIsFailed(dependantFuture);
        awaitAndAssertFailure(dependantFuture, RuntimeException.class, "We require more vespene gas");
    }

    @Test
    public void testWhenCompleteOnCancelledFuture() {
        KafkaFutureImpl<Integer> future = new KafkaFutureImpl<>();
        Throwable[] err = new Throwable[1];
        boolean[] ran = {false};
        KafkaFuture<Integer> dependantFuture = future.whenComplete((integer, ex) -> {
            ran[0] = true;
            err[0] = ex;
            if (integer != null) {
                err[0] = new AssertionError();
            }
        });
        assertFalse(dependantFuture.isDone());
        assertTrue(future.cancel(true));
        assertTrue(ran[0]);
        assertTrue(err[0] instanceof CancellationException);
    }

    private static class CompleterThread<T> extends Thread {

        private final KafkaFutureImpl<T> future;
        private final T value;
        private final Throwable exception;
        Throwable testException = null;

        CompleterThread(KafkaFutureImpl<T> future, T value) {
            this.future = future;
            this.value = value;
            this.exception = null;
        }

        CompleterThread(KafkaFutureImpl<T> future, T value, Exception exception) {
            this.future = future;
            this.value = value;
            this.exception = exception;
        }

        @Override
        public void run() {
            try {
                try {
                    Thread.sleep(0, 200);
                } catch (InterruptedException e) {
                }
                if (exception == null) {
                    future.complete(value);
                } else {
                    future.completeExceptionally(exception);
                }
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
            futures.add(new KafkaFutureImpl<>());
        }
        KafkaFuture<Void> allFuture = KafkaFuture.allOf(futures.toArray(new KafkaFuture[0]));
        final List<CompleterThread<Integer>> completerThreads = new ArrayList<>();
        final List<WaiterThread<Integer>> waiterThreads = new ArrayList<>();
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
        assertIsSuccessful(allFuture);
        for (int i = 0; i < numThreads; i++) {
            assertEquals(Integer.valueOf(i), futures.get(i).get());
        }
        for (int i = 0; i < numThreads; i++) {
            completerThreads.get(i).join();
            waiterThreads.get(i).join();
            assertNull(completerThreads.get(i).testException);
            assertNull(waiterThreads.get(i).testException);
        }
    }

    @Test
    public void testAllOfFuturesWithFailure() throws Exception {
        final int numThreads = 5;
        final List<KafkaFutureImpl<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            futures.add(new KafkaFutureImpl<>());
        }
        KafkaFuture<Void> allFuture = KafkaFuture.allOf(futures.toArray(new KafkaFuture[0]));
        final List<CompleterThread<Integer>> completerThreads = new ArrayList<>();
        final List<WaiterThread<Integer>> waiterThreads = new ArrayList<>();
        int lastIndex = numThreads - 1;
        for (int i = 0; i < lastIndex; i++) {
            completerThreads.add(new CompleterThread<>(futures.get(i), i));
            waiterThreads.add(new WaiterThread<>(futures.get(i), i));
        }
        completerThreads.add(new CompleterThread<>(futures.get(lastIndex), null, new RuntimeException("Last one failed")));
        waiterThreads.add(new WaiterThread<>(futures.get(lastIndex), lastIndex));
        assertFalse(allFuture.isDone());
        for (int i = 0; i < numThreads; i++) {
            waiterThreads.get(i).start();
        }
        for (int i = 0; i < lastIndex; i++) {
            completerThreads.get(i).start();
        }
        assertFalse(allFuture.isDone());
        completerThreads.get(lastIndex).start();
        awaitAndAssertFailure(allFuture, RuntimeException.class, "Last one failed");
        assertIsFailed(allFuture);
        for (int i = 0; i < lastIndex; i++) {
            assertEquals(Integer.valueOf(i), futures.get(i).get());
        }
        assertIsFailed(futures.get(lastIndex));
        for (int i = 0; i < numThreads; i++) {
            completerThreads.get(i).join();
            waiterThreads.get(i).join();
            assertNull(completerThreads.get(i).testException);
            if (i == lastIndex) {
                assertEquals(ExecutionException.class, waiterThreads.get(i).testException.getClass());
                assertEquals(RuntimeException.class, waiterThreads.get(i).testException.getCause().getClass());
                assertEquals("Last one failed", waiterThreads.get(i).testException.getCause().getMessage());
            } else {
                assertNull(waiterThreads.get(i).testException);
            }
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

    @Test
    public void testFutureTimeoutWithZeroWait() {
        final KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        assertThrows(TimeoutException.class, () -> future.get(0, TimeUnit.MILLISECONDS));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLeakCompletableFuture() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final KafkaFutureImpl<String> kfut = new KafkaFutureImpl<>();
        CompletableFuture<String> comfut = kfut.toCompletionStage().toCompletableFuture();
        assertThrows(UnsupportedOperationException.class, () -> comfut.complete(""));
        assertThrows(UnsupportedOperationException.class, () -> comfut.completeExceptionally(new RuntimeException()));
        // Annoyingly CompletableFuture added some more methods in Java 9, but the tests need to run on Java 8
        // so test reflectively
        if (Java.IS_JAVA9_COMPATIBLE) {
            Method completeOnTimeout = CompletableFuture.class.getDeclaredMethod("completeOnTimeout", Object.class, Long.TYPE, TimeUnit.class);
            assertThrows(UnsupportedOperationException.class, () -> {
                try {
                    completeOnTimeout.invoke(comfut, "", 1L, TimeUnit.MILLISECONDS);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });

            Method completeAsync = CompletableFuture.class.getDeclaredMethod("completeAsync", Supplier.class);
            assertThrows(UnsupportedOperationException.class, () -> {
                try {
                    completeAsync.invoke(comfut, (Supplier<String>) () -> "");
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });

            Method obtrudeValue = CompletableFuture.class.getDeclaredMethod("obtrudeValue", Object.class);
            assertThrows(UnsupportedOperationException.class, () -> {
                try {
                    obtrudeValue.invoke(comfut, "");
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });

            Method obtrudeException = CompletableFuture.class.getDeclaredMethod("obtrudeException", Throwable.class);
            assertThrows(UnsupportedOperationException.class, () -> {
                try {
                    obtrudeException.invoke(comfut, new RuntimeException());
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            });

            // Check the CF from a minimal CompletionStage doesn't cause completion of the original KafkaFuture
            Method minimal = CompletableFuture.class.getDeclaredMethod("minimalCompletionStage");
            CompletionStage<String> cs = (CompletionStage<String>) minimal.invoke(comfut);
            cs.toCompletableFuture().complete("");

            assertFalse(kfut.isDone());
            assertFalse(comfut.isDone());
        }
    }

}
