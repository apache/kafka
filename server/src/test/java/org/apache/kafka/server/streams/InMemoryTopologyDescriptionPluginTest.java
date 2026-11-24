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
package org.apache.kafka.server.streams;

import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InMemoryTopologyDescriptionPluginTest {

    @Test
    public void testSetThenGetRoundTrip() throws Exception {
        try (InMemoryTopologyDescriptionPlugin plugin = new InMemoryTopologyDescriptionPlugin()) {
            StreamsGroupTopologyDescription desc = topology("src", "in");
            plugin.setTopology("g1", 7, desc).get();
            StreamsGroupTopologyDescription got = plugin.getTopology("g1", 7).get();
            assertEquals(desc, got);
        }
    }

    @Test
    public void testGetReturnsNullForMissingGroup() throws Exception {
        try (InMemoryTopologyDescriptionPlugin plugin = new InMemoryTopologyDescriptionPlugin()) {
            assertNull(plugin.getTopology("missing", 0).get());
        }
    }

    @Test
    public void testGetReturnsNullWhenEpochDoesNotMatch() throws Exception {
        try (InMemoryTopologyDescriptionPlugin plugin = new InMemoryTopologyDescriptionPlugin()) {
            plugin.setTopology("g1", 3, topology("src", "in")).get();

            assertNull(plugin.getTopology("g1", 4).get());
            assertNotNull(plugin.getTopology("g1", 3).get());
        }
    }

    @Test
    public void testSetIsIdempotentAtSameEpoch() throws Exception {
        try (InMemoryTopologyDescriptionPlugin plugin = new InMemoryTopologyDescriptionPlugin()) {
            StreamsGroupTopologyDescription desc = topology("src", "in");

            plugin.setTopology("g1", 5, desc).get();
            plugin.setTopology("g1", 5, desc).get();
            plugin.setTopology("g1", 5, desc).get();

            assertEquals(desc, plugin.getTopology("g1", 5).get());
        }
    }

    @Test
    public void testEpochAdvanceOverwritesPreviousDescription() throws Exception {
        try (InMemoryTopologyDescriptionPlugin plugin = new InMemoryTopologyDescriptionPlugin()) {
            StreamsGroupTopologyDescription v1 = topology("v1-src", "in1");
            StreamsGroupTopologyDescription v2 = topology("v2-src", "in2");

            plugin.setTopology("g1", 1, v1).get();
            plugin.setTopology("g1", 2, v2).get();

            assertNull(plugin.getTopology("g1", 1).get(), "epoch 1 should no longer be served");
            assertEquals(v2, plugin.getTopology("g1", 2).get());
        }
    }

    @Test
    public void testDeleteTopologyRemovesEntry() throws Exception {
        try (InMemoryTopologyDescriptionPlugin plugin = new InMemoryTopologyDescriptionPlugin()) {
            plugin.setTopology("g1", 1, topology("src", "in")).get();
            plugin.deleteTopology("g1").get();
            assertNull(plugin.getTopology("g1", 1).get());
        }
    }

    @Test
    public void testDeleteIsIdempotentForUnknownGroup() throws Exception {
        try (InMemoryTopologyDescriptionPlugin plugin = new InMemoryTopologyDescriptionPlugin()) {
            plugin.deleteTopology("never-stored").get();
            plugin.deleteTopology("never-stored").get();
        }
    }

    @Test
    public void testDeleteIsScopedToGroup() throws Exception {
        try (InMemoryTopologyDescriptionPlugin plugin = new InMemoryTopologyDescriptionPlugin()) {
            plugin.setTopology("g1", 1, topology("src", "in")).get();
            plugin.setTopology("g2", 1, topology("src", "in")).get();

            plugin.deleteTopology("g1").get();

            assertNull(plugin.getTopology("g1", 1).get());
            assertNotNull(plugin.getTopology("g2", 1).get());
        }
    }

    @Test
    public void testCloseClearsAllState() throws Exception {
        InMemoryTopologyDescriptionPlugin plugin = new InMemoryTopologyDescriptionPlugin();
        plugin.setTopology("g1", 1, topology("src", "in")).get();
        plugin.close();
        assertNull(plugin.getTopology("g1", 1).get());
    }

    @Test
    public void testConfigureIsNoOp() {
        try (InMemoryTopologyDescriptionPlugin plugin = new InMemoryTopologyDescriptionPlugin()) {
            plugin.configure(Map.of("anything", "ignored"));
        }
    }

    @Test
    public void testConcurrentWritesAreSafe() throws Exception {
        try (InMemoryTopologyDescriptionPlugin plugin = new InMemoryTopologyDescriptionPlugin()) {
            StreamsGroupTopologyDescription desc = topology("src", "in");
            int threads = 16;
            ExecutorService exec = Executors.newFixedThreadPool(threads);
            try {
                CountDownLatch start = new CountDownLatch(1);
                CompletableFuture<?>[] futures = new CompletableFuture<?>[threads];
                for (int i = 0; i < threads; i++) {
                    futures[i] = CompletableFuture.runAsync(() -> {
                        try {
                            start.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        plugin.setTopology("g1", 1, desc).join();
                    }, exec);
                }
                start.countDown();
                CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);
            } finally {
                exec.shutdownNow();
            }

            assertEquals(desc, plugin.getTopology("g1", 1).get());
        }
    }

    @Test
    public void testFuturesCompleteSynchronously() throws ExecutionException, InterruptedException {
        try (InMemoryTopologyDescriptionPlugin plugin = new InMemoryTopologyDescriptionPlugin()) {
            CompletableFuture<Void> setFuture = plugin.setTopology("g1", 1, topology("src", "in"));
            CompletableFuture<StreamsGroupTopologyDescription> getFuture = plugin.getTopology("g1", 1);
            CompletableFuture<Void> deleteFuture = plugin.deleteTopology("g1");
            assertTrue(setFuture.isDone());
            assertTrue(getFuture.isDone());
            assertTrue(deleteFuture.isDone());
            assertNotNull(getFuture.get());
        }
    }

    private static StreamsGroupTopologyDescription topology(String sourceName, String topic) {
        StreamsGroupTopologyDescription.Source src = new StreamsGroupTopologyDescription.Source(
            sourceName, Set.of(topic), Set.of("proc"));
        StreamsGroupTopologyDescription.Processor proc = new StreamsGroupTopologyDescription.Processor(
            "proc", Set.of(), Set.of("snk"));
        StreamsGroupTopologyDescription.Sink sink = new StreamsGroupTopologyDescription.Sink(
            "snk", Optional.of("out"), Set.of());
        StreamsGroupTopologyDescription.Subtopology sub = new StreamsGroupTopologyDescription.Subtopology(
            "0", List.of(src, proc, sink));
        return new StreamsGroupTopologyDescription(List.of(sub), List.of());
    }
}
