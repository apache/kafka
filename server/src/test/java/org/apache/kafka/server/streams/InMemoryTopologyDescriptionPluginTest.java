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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InMemoryTopologyDescriptionPluginTest {

    private InMemoryTopologyDescriptionPlugin plugin;

    @BeforeEach
    public void setUp() {
        plugin = new InMemoryTopologyDescriptionPlugin();
        plugin.configure(Map.of());
    }

    @AfterEach
    public void tearDown() throws Exception {
        plugin.close();
    }

    private static StreamsGroupTopologyDescription sampleTopology() {
        return new StreamsGroupTopologyDescription(
            List.of(new StreamsGroupTopologyDescription.Subtopology(
                "0",
                List.of(new StreamsGroupTopologyDescription.Source(
                    "source",
                    new LinkedHashSet<>(List.of("input-topic")),
                    Set.of()
                ))
            )),
            List.of()
        );
    }

    @Test
    public void testSetTopologyRoundTrips() throws ExecutionException, InterruptedException {
        plugin.setTopology("group1", 1, sampleTopology()).get();
        assertEquals(sampleTopology(), plugin.getTopology("group1", 1).get());
    }

    @Test
    public void testSetTopologyOverwritesPrevious() throws ExecutionException, InterruptedException {
        plugin.setTopology("group1", 1, sampleTopology()).get();
        plugin.setTopology("group1", 2, sampleTopology()).get();

        // Old epoch is no longer retrievable; new epoch is.
        assertNull(plugin.getTopology("group1", 1).get());
        assertEquals(sampleTopology(), plugin.getTopology("group1", 2).get());
    }

    @Test
    public void testDeleteTopology() throws ExecutionException, InterruptedException {
        plugin.setTopology("group1", 1, sampleTopology()).get();
        plugin.deleteTopology("group1").get();
        assertNull(plugin.getTopology("group1", 1).get());
    }

    @Test
    public void testDeleteTopologyForNonexistentGroup() {
        // Idempotent on a missing group.
        assertDoesNotThrow(() -> plugin.deleteTopology("nonexistent").get());
    }

    @Test
    public void testMultipleGroups() throws ExecutionException, InterruptedException {
        plugin.setTopology("group1", 1, sampleTopology()).get();
        plugin.setTopology("group2", 5, sampleTopology()).get();

        assertEquals(sampleTopology(), plugin.getTopology("group1", 1).get());
        assertEquals(sampleTopology(), plugin.getTopology("group2", 5).get());

        plugin.deleteTopology("group1").get();
        assertNull(plugin.getTopology("group1", 1).get());
        assertEquals(sampleTopology(), plugin.getTopology("group2", 5).get());
    }

    @Test
    public void testCloseClears() throws Exception {
        plugin.setTopology("group1", 1, sampleTopology()).get();
        plugin.close();
        assertNull(plugin.getTopology("group1", 1).get());
    }

    @Test
    public void testConcurrentSetTopology() throws ExecutionException, InterruptedException {
        // Multiple concurrent setTopology calls for the same (groupId, epoch) — all should succeed.
        CompletableFuture<?>[] futures = new CompletableFuture[10];
        for (int i = 0; i < 10; i++) {
            futures[i] = plugin.setTopology("group1", 1, sampleTopology());
        }
        CompletableFuture.allOf(futures).get();
        assertEquals(sampleTopology(), plugin.getTopology("group1", 1).get());
    }

    @Test
    public void testAllFuturesCompleteSynchronously() {
        assertTrue(plugin.setTopology("g", 0, sampleTopology()).isDone());
        assertTrue(plugin.deleteTopology("g").isDone());
        assertTrue(plugin.getTopology("g", 0).isDone());
    }

    @Test
    public void testGetTopologyReturnsNullWhenEmpty() throws ExecutionException, InterruptedException {
        assertNull(plugin.getTopology("group1", 1).get());
    }

    @Test
    public void testGetTopologyReturnsNullForWrongEpoch() throws ExecutionException, InterruptedException {
        plugin.setTopology("group1", 1, sampleTopology()).get();
        assertNull(plugin.getTopology("group1", 2).get());
    }

    @Test
    public void testGetTopologyFailsWhenFailOnGetSet() {
        plugin.setFailOnGet(true);
        ExecutionException ex = assertThrows(ExecutionException.class,
            () -> plugin.getTopology("group1", 1).get());
        assertTrue(ex.getCause() instanceof RuntimeException);
    }

    @Test
    public void testConfigureAcceptsEmptyConfig() {
        InMemoryTopologyDescriptionPlugin p = new InMemoryTopologyDescriptionPlugin();
        assertDoesNotThrow(() -> p.configure(Map.of()));
        assertDoesNotThrow(() -> p.configure(Map.of("some.key", "some.value")));
    }

    @Test
    public void testSetTopologyReturnValue() throws ExecutionException, InterruptedException {
        Void result = plugin.setTopology("g", 1, sampleTopology()).get();
        assertNull(result);
    }
}
