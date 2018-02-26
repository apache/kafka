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
package org.apache.kafka.clients.consumer.internals;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RequestFutureTest {

    @Test
    public void testBasicCompletion() {
        RequestFuture<String> future = new RequestFuture<>();
        String value = "foo";
        future.complete(value);
        assertTrue(future.isDone());
        assertEquals(value, future.value());
    }

    @Test
    public void testBasicFailure() {
        RequestFuture<String> future = new RequestFuture<>();
        RuntimeException exception = new RuntimeException();
        future.raise(exception);
        assertTrue(future.isDone());
        assertEquals(exception, future.exception());
    }

    @Test
    public void testVoidFuture() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.complete(null);
        assertTrue(future.isDone());
        assertNull(future.value());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRuntimeExceptionInComplete() {
        RequestFuture<Exception> future = new RequestFuture<>();
        future.complete(new RuntimeException());
    }

    @Test(expected = IllegalStateException.class)
    public void invokeCompleteAfterAlreadyComplete() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.complete(null);
        future.complete(null);
    }

    @Test(expected = IllegalStateException.class)
    public void invokeCompleteAfterAlreadyFailed() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.raise(new RuntimeException());
        future.complete(null);
    }

    @Test(expected = IllegalStateException.class)
    public void invokeRaiseAfterAlreadyFailed() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.raise(new RuntimeException());
        future.raise(new RuntimeException());
    }

    @Test(expected = IllegalStateException.class)
    public void invokeRaiseAfterAlreadyCompleted() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.complete(null);
        future.raise(new RuntimeException());
    }

    @Test(expected = IllegalStateException.class)
    public void invokeExceptionAfterSuccess() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.complete(null);
        future.exception();
    }

    @Test(expected = IllegalStateException.class)
    public void invokeValueAfterFailure() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.raise(new RuntimeException());
        future.value();
    }

    @Test
    public void listenerInvokedIfAddedBeforeFutureCompletion() {
        RequestFuture<Void> future = new RequestFuture<>();

        MockRequestFutureListener<Void> listener = new MockRequestFutureListener<>();
        future.addListener(listener);

        future.complete(null);

        assertOnSuccessInvoked(listener);
    }

    @Test
    public void listenerInvokedIfAddedBeforeFutureFailure() {
        RequestFuture<Void> future = new RequestFuture<>();

        MockRequestFutureListener<Void> listener = new MockRequestFutureListener<>();
        future.addListener(listener);

        future.raise(new RuntimeException());

        assertOnFailureInvoked(listener);
    }

    @Test
    public void listenerInvokedIfAddedAfterFutureCompletion() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.complete(null);

        MockRequestFutureListener<Void> listener = new MockRequestFutureListener<>();
        future.addListener(listener);

        assertOnSuccessInvoked(listener);
    }

    @Test
    public void listenerInvokedIfAddedAfterFutureFailure() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.raise(new RuntimeException());

        MockRequestFutureListener<Void> listener = new MockRequestFutureListener<>();
        future.addListener(listener);

        assertOnFailureInvoked(listener);
    }

    @Test
    public void listenersInvokedIfAddedBeforeAndAfterFailure() {
        RequestFuture<Void> future = new RequestFuture<>();

        MockRequestFutureListener<Void> beforeListener = new MockRequestFutureListener<>();
        future.addListener(beforeListener);

        future.raise(new RuntimeException());

        MockRequestFutureListener<Void> afterListener = new MockRequestFutureListener<>();
        future.addListener(afterListener);

        assertOnFailureInvoked(beforeListener);
        assertOnFailureInvoked(afterListener);
    }

    @Test
    public void listenersInvokedIfAddedBeforeAndAfterCompletion() {
        RequestFuture<Void> future = new RequestFuture<>();

        MockRequestFutureListener<Void> beforeListener = new MockRequestFutureListener<>();
        future.addListener(beforeListener);

        future.complete(null);

        MockRequestFutureListener<Void> afterListener = new MockRequestFutureListener<>();
        future.addListener(afterListener);

        assertOnSuccessInvoked(beforeListener);
        assertOnSuccessInvoked(afterListener);
    }

    @Test
    public void testComposeSuccessCase() {
        RequestFuture<String> future = new RequestFuture<>();
        RequestFuture<Integer> composed = future.compose(new RequestFutureAdapter<String, Integer>() {
            @Override
            public void onSuccess(String value, RequestFuture<Integer> future) {
                future.complete(value.length());
            }
        });

        future.complete("hello");

        assertTrue(composed.isDone());
        assertTrue(composed.succeeded());
        assertEquals(5, (int) composed.value());
    }

    @Test
    public void testComposeFailureCase() {
        RequestFuture<String> future = new RequestFuture<>();
        RequestFuture<Integer> composed = future.compose(new RequestFutureAdapter<String, Integer>() {
            @Override
            public void onSuccess(String value, RequestFuture<Integer> future) {
                future.complete(value.length());
            }
        });

        RuntimeException e = new RuntimeException();
        future.raise(e);

        assertTrue(composed.isDone());
        assertTrue(composed.failed());
        assertEquals(e, composed.exception());
    }

    private static <T> void assertOnSuccessInvoked(MockRequestFutureListener<T> listener) {
        assertEquals(1, listener.numOnSuccessCalls.get());
        assertEquals(0, listener.numOnFailureCalls.get());
    }

    private static <T> void assertOnFailureInvoked(MockRequestFutureListener<T> listener) {
        assertEquals(0, listener.numOnSuccessCalls.get());
        assertEquals(1, listener.numOnFailureCalls.get());
    }

    private static class MockRequestFutureListener<T> implements RequestFutureListener<T> {
        private final AtomicInteger numOnSuccessCalls = new AtomicInteger(0);
        private final AtomicInteger numOnFailureCalls = new AtomicInteger(0);

        @Override
        public void onSuccess(T value) {
            numOnSuccessCalls.incrementAndGet();
        }

        @Override
        public void onFailure(RuntimeException e) {
            numOnFailureCalls.incrementAndGet();
        }
    }

}
