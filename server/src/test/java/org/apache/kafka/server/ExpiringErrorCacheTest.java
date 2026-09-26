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

package org.apache.kafka.server;

import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExpiringErrorCacheTest {

    private MockTime mockTime;
    private ExpiringErrorCache cache;

    @BeforeEach
    void setUp() {
        mockTime = new MockTime();
    }

    // Basic Functionality Tests

    @Test
    void testPutAndGet() {
        cache = new ExpiringErrorCache(10, mockTime);

        cache.put("topic1", "error1", 1000L);
        cache.put("topic2", "error2", 2000L);

        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2"), mockTime.milliseconds());
        assertEquals(2, errors.size());
        assertEquals("error1", errors.get("topic1"));
        assertEquals("error2", errors.get("topic2"));
    }

    @Test
    void testGetNonExistentTopic() {
        cache = new ExpiringErrorCache(10, mockTime);

        cache.put("topic1", "error1", 1000L);

        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2"), mockTime.milliseconds());
        assertEquals(1, errors.size());
        assertEquals("error1", errors.get("topic1"));
        assertFalse(errors.containsKey("topic2"));
    }

    @Test
    void testUpdateExistingEntry() {
        cache = new ExpiringErrorCache(10, mockTime);

        cache.put("topic1", "error1", 1000L);
        assertEquals("error1", cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds()).get("topic1"));

        // Update with new error
        cache.put("topic1", "error2", 2000L);
        assertEquals("error2", cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds()).get("topic1"));
    }

    @Test
    void testGetMultipleTopics() {
        cache = new ExpiringErrorCache(10, mockTime);

        cache.put("topic1", "error1", 1000L);
        cache.put("topic2", "error2", 1000L);
        cache.put("topic3", "error3", 1000L);

        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic3", "topic4"), mockTime.milliseconds());
        assertEquals(2, errors.size());
        assertEquals("error1", errors.get("topic1"));
        assertEquals("error3", errors.get("topic3"));
        assertFalse(errors.containsKey("topic2"));
        assertFalse(errors.containsKey("topic4"));
    }

    // Expiration Tests

    @Test
    void testExpiredEntryNotReturned() {
        cache = new ExpiringErrorCache(10, mockTime);

        cache.put("topic1", "error1", 1000L);

        // Entry should be available before expiration
        assertEquals(1, cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds()).size());

        // Advance time past expiration
        mockTime.sleep(1001L);

        // Entry should not be returned after expiration
        assertTrue(cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds()).isEmpty());
    }

    @Test
    void testExpiredEntriesCleanedOnPut() {
        cache = new ExpiringErrorCache(10, mockTime);

        // Add entries with different TTLs
        cache.put("topic1", "error1", 1000L);
        cache.put("topic2", "error2", 2000L);

        // Advance time to expire topic1 but not topic2
        mockTime.sleep(1500L);

        // Add a new entry - this should trigger cleanup
        cache.put("topic3", "error3", 1000L);

        // Verify only non-expired entries remain
        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2", "topic3"), mockTime.milliseconds());
        assertEquals(2, errors.size());
        assertFalse(errors.containsKey("topic1"));
        assertEquals("error2", errors.get("topic2"));
        assertEquals("error3", errors.get("topic3"));
    }

    @Test
    void testMixedExpiredAndValidEntries() {
        cache = new ExpiringErrorCache(10, mockTime);

        cache.put("topic1", "error1", 500L);
        cache.put("topic2", "error2", 1000L);
        cache.put("topic3", "error3", 1500L);

        // Advance time to expire only topic1
        mockTime.sleep(600L);

        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2", "topic3"), mockTime.milliseconds());
        assertEquals(2, errors.size());
        assertFalse(errors.containsKey("topic1"));
        assertTrue(errors.containsKey("topic2"));
        assertTrue(errors.containsKey("topic3"));
    }

    // Capacity Enforcement Tests

    @Test
    void testCapacityEnforcement() {
        cache = new ExpiringErrorCache(3, mockTime);

        // Add 5 entries, exceeding capacity of 3
        IntStream.rangeClosed(1, 5).forEach(i -> {
            cache.put("topic" + i, "error" + i, 1000L);
            // Small time advance between entries to ensure different insertion order
            mockTime.sleep(10L);
        });

        var errors = cache.getErrorsForTopics(
                IntStream.rangeClosed(1, 5).mapToObj(i -> "topic" + i).collect(Collectors.toSet()),
                mockTime.milliseconds());
        assertEquals(3, errors.size());

        // The cache evicts by earliest expiration time
        // Since all have same TTL, earliest inserted (topic1, topic2) should be evicted
        assertFalse(errors.containsKey("topic1"));
        assertFalse(errors.containsKey("topic2"));
        assertTrue(errors.containsKey("topic3"));
        assertTrue(errors.containsKey("topic4"));
        assertTrue(errors.containsKey("topic5"));
    }

    @Test
    void testEvictionOrder() {
        cache = new ExpiringErrorCache(3, mockTime);

        // Add entries with different TTLs
        cache.put("topic1", "error1", 3000L); // Expires at 3000
        mockTime.sleep(100L);
        cache.put("topic2", "error2", 1000L); // Expires at 1100
        mockTime.sleep(100L);
        cache.put("topic3", "error3", 2000L); // Expires at 2200
        mockTime.sleep(100L);
        cache.put("topic4", "error4", 500L);  // Expires at 800

        // With capacity 3, topic4 (earliest expiration) should be evicted
        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2", "topic3", "topic4"), mockTime.milliseconds());
        assertEquals(3, errors.size());
        assertTrue(errors.containsKey("topic1"));
        assertTrue(errors.containsKey("topic2"));
        assertTrue(errors.containsKey("topic3"));
        assertFalse(errors.containsKey("topic4"));
    }

    @Test
    void testCapacityWithDifferentTTLs() {
        cache = new ExpiringErrorCache(2, mockTime);

        cache.put("topic1", "error1", 5000L); // Long TTL
        cache.put("topic2", "error2", 100L); // Short TTL
        cache.put("topic3", "error3", 3000L); // Medium TTL

        // topic2 has earliest expiration, so it should be evicted
        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2", "topic3"), mockTime.milliseconds());
        assertEquals(2, errors.size());
        assertTrue(errors.containsKey("topic1"));
        assertFalse(errors.containsKey("topic2"));
        assertTrue(errors.containsKey("topic3"));
    }

    // Update and Stale Entry Tests

    @Test
    void testUpdateDoesNotLeaveStaleEntries() {
        cache = new ExpiringErrorCache(3, mockTime);

        // Fill cache to capacity
        cache.put("topic1", "error1", 1000L);
        cache.put("topic2", "error2", 1000L);
        cache.put("topic3", "error3", 1000L);

        // Update topic2 with longer TTL
        cache.put("topic2", "error2_updated", 5000L);

        // Add new entry to trigger eviction
        cache.put("topic4", "error4", 1000L);

        // Should evict topic1 or topic3 (earliest expiration), not the updated topic2
        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2", "topic3", "topic4"), mockTime.milliseconds());
        assertEquals(3, errors.size());
        assertTrue(errors.containsKey("topic2"));
        assertEquals("error2_updated", errors.get("topic2"));
    }

    @Test
    void testStaleEntriesInQueueHandledCorrectly() {
        cache = new ExpiringErrorCache(10, mockTime);

        // Add and update same topic multiple times
        cache.put("topic1", "error1", 1000L);
        cache.put("topic1", "error2", 2000L);
        cache.put("topic1", "error3", 3000L);

        // Only latest value should be returned
        var errors = cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds());
        assertEquals(1, errors.size());
        assertEquals("error3", errors.get("topic1"));

        // Advance time to expire first two entries
        mockTime.sleep(2500L);

        // Force cleanup by adding new entry
        cache.put("topic2", "error_new", 1000L);

        // topic1 should still be available with latest value
        var errorsAfterCleanup = cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds());
        assertEquals(1, errorsAfterCleanup.size());
        assertEquals("error3", errorsAfterCleanup.get("topic1"));
    }

    // Edge Cases

    @Test
    void testEmptyCache() {
        cache = new ExpiringErrorCache(10, mockTime);

        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2"), mockTime.milliseconds());
        assertTrue(errors.isEmpty());
    }

    @Test
    void testSingleEntryCache() {
        cache = new ExpiringErrorCache(1, mockTime);

        cache.put("topic1", "error1", 1000L);
        cache.put("topic2", "error2", 1000L);

        // Only most recent should remain
        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2"), mockTime.milliseconds());
        assertEquals(1, errors.size());
        assertFalse(errors.containsKey("topic1"));
        assertTrue(errors.containsKey("topic2"));
    }

    @Test
    void testZeroTTL() {
        cache = new ExpiringErrorCache(10, mockTime);

        cache.put("topic1", "error1", 0L);

        // Entry expires immediately
        assertTrue(cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds()).isEmpty());
    }

    @Test
    void testClearOperation() {
        cache = new ExpiringErrorCache(10, mockTime);

        cache.put("topic1", "error1", 1000L);
        cache.put("topic2", "error2", 1000L);

        assertEquals(2, cache.getErrorsForTopics(Set.of("topic1", "topic2"), mockTime.milliseconds()).size());

        cache.clear();

        assertTrue(cache.getErrorsForTopics(Set.of("topic1", "topic2"), mockTime.milliseconds()).isEmpty());
    }

    // Concurrent Access Tests

    @Test
    void testConcurrentPutOperations() {
        cache = new ExpiringErrorCache(100, mockTime);
        var numThreads = 10;
        var numTopicsPerThread = 20;

        var futures = new ArrayList<CompletableFuture<Void>>();

        IntStream.rangeClosed(1, numThreads).forEach(threadId -> {
            final var finalThreadId = threadId;
            var future = CompletableFuture.runAsync(() ->
                IntStream.rangeClosed(1, numTopicsPerThread).forEach(i ->
                    cache.put("topic_" + finalThreadId + "_" + i, "error_" + finalThreadId + "_" + i, 1000L))
            );
            futures.add(future);
        });

        assertDoesNotThrow(() ->
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS));

        // Verify all entries were added
        var allTopics = new HashSet<String>();
        IntStream.rangeClosed(1, numThreads).forEach(threadId ->
            IntStream.rangeClosed(1, numTopicsPerThread).forEach(i -> allTopics.add("topic_" + threadId + "_" + i)));

        var errors = cache.getErrorsForTopics(allTopics, mockTime.milliseconds());
        assertEquals(100, errors.size()); // Limited by cache capacity
    }

    @Test
    void testConcurrentPutAndGet() {
        cache = new ExpiringErrorCache(100, mockTime);
        var numOperations = 1000;
        var random = new Random();
        var topics = IntStream.rangeClosed(1, 50).mapToObj(i -> "topic" + i).toArray(String[]::new);

        var futures = new ArrayList<CompletableFuture<Void>>();
        IntStream.rangeClosed(1, numOperations).forEach(i -> {
            var future = CompletableFuture.runAsync(() -> {
                if (random.nextBoolean()) {
                    // Put operation
                    var topic = topics[random.nextInt(topics.length)];
                    cache.put(topic, "error_" + random.nextInt(), 1000L);
                } else {
                    // Get operation
                    var topicsToGet = Set.of(topics[random.nextInt(topics.length)]);
                    cache.getErrorsForTopics(topicsToGet, mockTime.milliseconds());
                }
            });
            futures.add(future);
        });

        // Wait for all operations to complete
        assertDoesNotThrow(() -> CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join());
    }

    @Test
    void testConcurrentUpdates() {
        cache = new ExpiringErrorCache(50, mockTime);
        var numThreads = 10;
        var numUpdatesPerThread = 100;
        var sharedTopics = IntStream.rangeClosed(1, 10).mapToObj(i -> "shared_topic" + i).toArray(String[]::new);

        var futures = new ArrayList<CompletableFuture<Void>>();
        IntStream.rangeClosed(1, numThreads).forEach(threadId -> {
            var future = CompletableFuture.runAsync(() -> {
                var random = new Random();
                IntStream.rangeClosed(1, numUpdatesPerThread).forEach(i -> {
                    var topic = sharedTopics[random.nextInt(sharedTopics.length)];
                    cache.put(topic, "error_thread" + threadId + "_update" + i, 1000L);
                });
            });
            futures.add(future);
        });

        assertDoesNotThrow(() ->
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS));

        // Verify all shared topics have some value
        var errors = cache.getErrorsForTopics(Set.of(sharedTopics), mockTime.milliseconds());
        for (var topic : sharedTopics) {
            assertTrue(errors.containsKey(topic), "Topic " + topic + " should have a value");
            assertTrue(errors.get(topic).startsWith("error_thread"), "Value should be from one of the threads");
        }
    }
}
