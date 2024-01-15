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

package org.apache.kafka.server.network;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final public class EndpointReadyFuturesTest {
    private static final Endpoint EXTERNAL =
            new Endpoint("EXTERNAL", SecurityProtocol.SSL, "127.0.0.1", 9092);

    private static final Endpoint INTERNAL =
            new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "127.0.0.1", 9093);

    private static final KafkaAuthorizerServerInfo INFO = new KafkaAuthorizerServerInfo(
        new ClusterResource("S6-01LPiQOCBhhFIunQUcQ"),
        1,
        Arrays.asList(EXTERNAL, INTERNAL),
        INTERNAL,
        Arrays.asList("INTERNAL"));

    static void assertComplete(
            EndpointReadyFutures readyFutures,
            Endpoint... endpoints
    ) {
        for (Endpoint endpoint : endpoints) {
            String name = endpoint.listenerName().get();
            CompletableFuture<Void> future = readyFutures.futures().get(endpoint);
            assertNotNull(future, "Unable to find future for " + name);
            assertTrue(future.isDone(), "Future for " + name + " is not done.");
            assertFalse(future.isCompletedExceptionally(),
                    "Future for " + name + " is completed exceptionally.");
        }
    }

    static void assertIncomplete(
            EndpointReadyFutures readyFutures,
            Endpoint... endpoints
    ) {
        for (Endpoint endpoint : endpoints) {
            CompletableFuture<Void> future = readyFutures.futures().get(endpoint);
            assertNotNull(future, "Unable to find future for " + endpoint);
            assertFalse(future.isDone(), "Future for " + endpoint + " is done.");
        }
    }

    static void assertException(
            EndpointReadyFutures readyFutures,
            Throwable throwable,
            Endpoint... endpoints
    ) {
        for (Endpoint endpoint : endpoints) {
            CompletableFuture<Void> future = readyFutures.futures().get(endpoint);
            assertNotNull(future, "Unable to find future for " + endpoint);
            assertTrue(future.isCompletedExceptionally(),
                    "Future for " + endpoint + " is not completed exceptionally.");
            Throwable cause = assertThrows(CompletionException.class,
                () -> future.getNow(null)).getCause();
            assertNotNull(cause, "Unable to find CompletionException cause for " + endpoint);
            assertEquals(throwable.getClass(), cause.getClass());
            assertEquals(throwable.getMessage(), cause.getMessage());
        }
    }

    @Test
    public void testImmediateCompletion() {
        EndpointReadyFutures readyFutures = new EndpointReadyFutures.Builder().
                build(Optional.empty(), INFO);
        assertEquals(new HashSet<>(Arrays.asList(EXTERNAL, INTERNAL)),
                readyFutures.futures().keySet());
        assertComplete(readyFutures, EXTERNAL, INTERNAL);
    }

    @Test
    public void testAddReadinessFuture() {
        CompletableFuture<Void> foo = new CompletableFuture<>();
        EndpointReadyFutures readyFutures = new EndpointReadyFutures.Builder().
                addReadinessFuture("foo", foo).
                build(Optional.empty(), INFO);
        assertEquals(new HashSet<>(Arrays.asList(EXTERNAL, INTERNAL)),
                readyFutures.futures().keySet());
        assertIncomplete(readyFutures, EXTERNAL, INTERNAL);
        foo.complete(null);
        assertComplete(readyFutures, EXTERNAL, INTERNAL);
    }

    @Test
    public void testAddMultipleReadinessFutures() {
        CompletableFuture<Void> foo = new CompletableFuture<>();
        CompletableFuture<Void> bar = new CompletableFuture<>();
        EndpointReadyFutures readyFutures = new EndpointReadyFutures.Builder().
                addReadinessFuture("foo", foo).
                addReadinessFuture("bar", bar).
                build(Optional.empty(), INFO);
        assertEquals(new HashSet<>(Arrays.asList(EXTERNAL, INTERNAL)),
                readyFutures.futures().keySet());
        assertIncomplete(readyFutures, EXTERNAL, INTERNAL);
        foo.complete(null);
        assertIncomplete(readyFutures, EXTERNAL, INTERNAL);
        bar.complete(null);
        assertComplete(readyFutures, EXTERNAL, INTERNAL);
    }

    @Test
    public void testAddReadinessFutures() {
        Map<Endpoint, CompletableFuture<Void>> bazFutures = new HashMap<>();
        bazFutures.put(EXTERNAL, new CompletableFuture<>());
        bazFutures.put(INTERNAL, new CompletableFuture<>());
        EndpointReadyFutures readyFutures = new EndpointReadyFutures.Builder().
                addReadinessFutures("baz", bazFutures).
                build(Optional.empty(), INFO);
        assertEquals(new HashSet<>(Arrays.asList(EXTERNAL, INTERNAL)),
                readyFutures.futures().keySet());
        assertIncomplete(readyFutures, EXTERNAL, INTERNAL);
        bazFutures.get(EXTERNAL).complete(null);
        assertComplete(readyFutures, EXTERNAL);
        assertIncomplete(readyFutures, INTERNAL);
        bazFutures.get(INTERNAL).complete(null);
        assertComplete(readyFutures, EXTERNAL, INTERNAL);
    }

    @Test
    public void testFailedReadinessFuture() {
        CompletableFuture<Void> foo = new CompletableFuture<>();
        CompletableFuture<Void> bar = new CompletableFuture<>();
        EndpointReadyFutures readyFutures = new EndpointReadyFutures.Builder().
                addReadinessFuture("foo", foo).
                addReadinessFuture("bar", bar).
                build(Optional.empty(), INFO);
        assertEquals(new HashSet<>(Arrays.asList(EXTERNAL, INTERNAL)),
                readyFutures.futures().keySet());
        assertIncomplete(readyFutures, EXTERNAL, INTERNAL);
        foo.complete(null);
        assertIncomplete(readyFutures, EXTERNAL, INTERNAL);
        bar.completeExceptionally(new RuntimeException("Failed."));
        assertException(readyFutures, new RuntimeException("Failed."),
                EXTERNAL, INTERNAL);
    }
}
