/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RequestFutureTest {

    @Test
    public void testBasicCompletion() {
        RequestFuture<String> future = new RequestFuture<>();
        String value = "foo";
        future.complete(value);
        assertEquals(value, future.value());
    }

    @Test
    public void testBasicFailure() {
        RequestFuture<String> future = new RequestFuture<>();
        RuntimeException exception = new RuntimeException();
        future.raise(exception);
        assertEquals(exception, future.exception());
    }

    @Test
    public void testVoidFuture() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.complete(null);
        assertNull(future.value());
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

        final AtomicBoolean wasInvoked = new AtomicBoolean(false);
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                wasInvoked.set(true);
            }

            @Override
            public void onFailure(RuntimeException e) {

            }
        });

        future.complete(null);
        assertTrue(wasInvoked.get());
    }

    @Test
    public void listenerInvokedIfAddedBeforeFutureFailure() {
        RequestFuture<Void> future = new RequestFuture<>();

        final AtomicBoolean wasInvoked = new AtomicBoolean(false);
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {

            }

            @Override
            public void onFailure(RuntimeException e) {
                wasInvoked.set(true);
            }
        });

        future.raise(new RuntimeException());
        assertTrue(wasInvoked.get());
    }

    @Test
    public void listenerInvokedIfAddedAfterFutureCompletion() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.complete(null);

        final AtomicBoolean wasInvoked = new AtomicBoolean(false);
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                wasInvoked.set(true);
            }

            @Override
            public void onFailure(RuntimeException e) {

            }
        });

        assertTrue(wasInvoked.get());
    }

    @Test
    public void listenerInvokedIfAddedAfterFutureFailure() {
        RequestFuture<Void> future = new RequestFuture<>();
        future.raise(new RuntimeException());

        final AtomicBoolean wasInvoked = new AtomicBoolean(false);
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {

            }

            @Override
            public void onFailure(RuntimeException e) {
                wasInvoked.set(true);
            }
        });

        assertTrue(wasInvoked.get());
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

}
