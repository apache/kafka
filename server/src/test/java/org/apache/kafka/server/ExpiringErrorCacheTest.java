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

import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPutAndGet(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(10, mockTime);

        cachePut("topic1", "error1", 1000L, useCreatableTopicResultPut);
        cachePut("topic2", "error2", 1000L, useCreatableTopicResultPut);

        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2"), mockTime.milliseconds());
        assertEquals(2, errors.size());
        assertEquals("error1", errors.get("topic1"));
        assertEquals("error2", errors.get("topic2"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testGetNonExistentTopic(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(10, mockTime);

        cachePut("topic1", "error1", 1000L, useCreatableTopicResultPut);

        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2"), mockTime.milliseconds());
        assertEquals(1, errors.size());
        assertEquals("error1", errors.get("topic1"));
        assertFalse(errors.containsKey("topic2"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUpdateExistingEntry(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(10, mockTime);

        cachePut("topic1", "error1", 1000L, useCreatableTopicResultPut);
        assertEquals("error1", cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds()).get("topic1"));

        // Update with new error
        cachePut("topic1", "error2", 2000L, useCreatableTopicResultPut);
        assertEquals("error2", cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds()).get("topic1"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testGetMultipleTopics(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(10, mockTime);

        cachePut("topic1", "error1", 1000L, useCreatableTopicResultPut);
        cachePut("topic2", "error2", 1000L, useCreatableTopicResultPut);
        cachePut("topic3", "error3", 1000L, useCreatableTopicResultPut);

        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic3", "topic4"), mockTime.milliseconds());
        assertEquals(2, errors.size());
        assertEquals("error1", errors.get("topic1"));
        assertEquals("error3", errors.get("topic3"));
        assertFalse(errors.containsKey("topic2"));
        assertFalse(errors.containsKey("topic4"));
    }

    // Expiration Tests

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testExpiredEntryNotReturned(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(10, mockTime);

        cachePut("topic1", "error1", 1000L, useCreatableTopicResultPut);

        // Entry should be available before expiration
        assertEquals(1, cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds()).size());

        // Advance time past expiration
        mockTime.sleep(1001L);

        // Entry should not be returned after expiration
        assertTrue(cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds()).isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testExpiredEntriesCleanedOnPut(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(10, mockTime);

        // Add entries with different TTLs
        cachePut("topic1", "error1", 1000L, useCreatableTopicResultPut);
        cachePut("topic2", "error2", 2000L, useCreatableTopicResultPut);

        // Advance time to expire topic1 but not topic2
        mockTime.sleep(1500L);

        // Add a new entry - this should trigger cleanup
        cachePut("topic3", "error3", 1000L, useCreatableTopicResultPut);

        // Verify only non-expired entries remain
        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2", "topic3"), mockTime.milliseconds());
        assertEquals(2, errors.size());
        assertFalse(errors.containsKey("topic1"));
        assertEquals("error2", errors.get("topic2"));
        assertEquals("error3", errors.get("topic3"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMixedExpiredAndValidEntries(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(10, mockTime);

        cachePut("topic1", "error1", 500L, useCreatableTopicResultPut);
        cachePut("topic2", "error2", 1000L, useCreatableTopicResultPut);
        cachePut("topic3", "error3", 1500L, useCreatableTopicResultPut);

        // Advance time to expire only topic1
        mockTime.sleep(600L);

        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2", "topic3"), mockTime.milliseconds());
        assertEquals(2, errors.size());
        assertFalse(errors.containsKey("topic1"));
        assertTrue(errors.containsKey("topic2"));
        assertTrue(errors.containsKey("topic3"));
    }

    // Capacity Enforcement Tests

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCapacityEnforcement(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(3, mockTime);

        // Add 5 entries, exceeding capacity of 3
        IntStream.rangeClosed(1, 5).forEach(i -> {
            cachePut("topic" + i, "error" + i, 1000L, useCreatableTopicResultPut);
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEvictionOrder(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(3, mockTime);

        // Add entries with different TTLs
        cachePut("topic1", "error1", 3000L, useCreatableTopicResultPut); // Expires at 3000
        mockTime.sleep(100L);
        cachePut("topic2", "error2", 1000L, useCreatableTopicResultPut); // Expires at 1100
        mockTime.sleep(100L);
        cachePut("topic3", "error3", 2000L, useCreatableTopicResultPut); // Expires at 2200
        mockTime.sleep(100L);
        cachePut("topic4", "error4", 500L, useCreatableTopicResultPut);  // Expires at 800

        // With capacity 3, topic4 (earliest expiration) should be evicted
        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2", "topic3", "topic4"), mockTime.milliseconds());
        assertEquals(3, errors.size());
        assertTrue(errors.containsKey("topic1"));
        assertTrue(errors.containsKey("topic2"));
        assertTrue(errors.containsKey("topic3"));
        assertFalse(errors.containsKey("topic4"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCapacityWithDifferentTTLs(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(2, mockTime);

        cachePut("topic1", "error1", 5000L, useCreatableTopicResultPut); // Long TTL
        cachePut("topic2", "error2", 100L, useCreatableTopicResultPut); // Short TTL
        cachePut("topic3", "error3", 3000L, useCreatableTopicResultPut); // Medium TTL

        // topic2 has earliest expiration, so it should be evicted
        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2", "topic3"), mockTime.milliseconds());
        assertEquals(2, errors.size());
        assertTrue(errors.containsKey("topic1"));
        assertFalse(errors.containsKey("topic2"));
        assertTrue(errors.containsKey("topic3"));
    }

    // Update and Stale Entry Tests

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUpdateDoesNotLeaveStaleEntries(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(3, mockTime);

        // Fill cache to capacity
        cachePut("topic1", "error1", 1000L, useCreatableTopicResultPut);
        cachePut("topic2", "error2", 1000L, useCreatableTopicResultPut);
        cachePut("topic3", "error3", 1000L, useCreatableTopicResultPut);

        // Update topic2 with longer TTL
        cachePut("topic2", "error2_updated", 5000L, useCreatableTopicResultPut);

        // Add new entry to trigger eviction
        cachePut("topic4", "error4", 1000L, useCreatableTopicResultPut);

        // Should evict topic1 or topic3 (earliest expiration), not the updated topic2
        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2", "topic3", "topic4"), mockTime.milliseconds());
        assertEquals(3, errors.size());
        assertTrue(errors.containsKey("topic2"));
        assertEquals("error2_updated", errors.get("topic2"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testStaleEntriesInQueueHandledCorrectly(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(10, mockTime);

        // Add and update same topic multiple times
        cachePut("topic1", "error1", 1000L, useCreatableTopicResultPut);
        cachePut("topic1", "error2", 2000L, useCreatableTopicResultPut);
        cachePut("topic1", "error3", 3000L, useCreatableTopicResultPut);

        // Only latest value should be returned
        var errors = cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds());
        assertEquals(1, errors.size());
        assertEquals("error3", errors.get("topic1"));

        // Advance time to expire first two entries
        mockTime.sleep(2500L);

        // Force cleanup by adding new entry
        cachePut("topic2", "error_new", 1000L, useCreatableTopicResultPut);

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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSingleEntryCache(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(1, mockTime);

        cachePut("topic1", "error1", 1000L, useCreatableTopicResultPut);
        mockTime.sleep(1L);
        cachePut("topic2", "error2", 1000L, useCreatableTopicResultPut);

        // Only most recent should remain
        var errors = cache.getErrorsForTopics(Set.of("topic1", "topic2"), mockTime.milliseconds());
        assertEquals(1, errors.size());
        assertFalse(errors.containsKey("topic1"));
        assertTrue(errors.containsKey("topic2"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testZeroTTL(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(10, mockTime);

        cachePut("topic1", "error1", 0L, useCreatableTopicResultPut);

        // Entry expires immediately
        assertTrue(cache.getErrorsForTopics(Set.of("topic1"), mockTime.milliseconds()).isEmpty());
    }

    // Concurrent Access Tests

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentPutOperations(boolean useCreatableTopicResultPut) {
        cache = new ExpiringErrorCache(100, mockTime);
        var numThreads = 10;
        var numTopicsPerThread = 20;

        var futures = new ArrayList<CompletableFuture<Void>>();

        IntStream.rangeClosed(1, numThreads).forEach(threadId -> {
            final var finalThreadId = threadId;
            var future = CompletableFuture.runAsync(() -> {
                var topicErrors = IntStream.rangeClosed(1, numTopicsPerThread)
                    .boxed()
                    .collect(Collectors.toMap(
                        i -> "topic_" + finalThreadId + "_" + i,
                        i -> "error_" + finalThreadId + "_" + i
                    ));
                topicErrors.forEach((topicName, errorMessage) ->
                    cachePut(topicName, errorMessage, 1000L, useCreatableTopicResultPut));
            });
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentPutAndGet(boolean useCreatableTopicResultPut) {
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
                    cachePut(topic, "error_" + random.nextInt(), 1000L, useCreatableTopicResultPut);
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConcurrentUpdates(boolean useCreatableTopicResultPut) {
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
                    cachePut(topic, "error_thread" + threadId + "_update" + i, 1000L, useCreatableTopicResultPut);
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

    @Test
    void testBothPutMethodsWriteToSameCache() {
        cache = new ExpiringErrorCache(10, mockTime);

        cache.put(Set.of("set-topic"), "set error", 1000L);
        cache.put(List.of(
            creatableTopicResult("result-topic", Errors.UNKNOWN_SERVER_ERROR, "result error")
        ), 1000L);

        var errors = cache.getErrorsForTopics(Set.of("set-topic", "result-topic"), mockTime.milliseconds());
        assertEquals(Map.of(
            "set-topic", "set error",
            "result-topic", "result error"
        ), errors);
    }

    @Test
    void testPutCreatableTopicResultsUsesDefaultErrorMessageWhenMissing() {
        cache = new ExpiringErrorCache(10, mockTime);

        cache.put(List.of(
            creatableTopicResult("null-message-topic", Errors.INVALID_TOPIC_EXCEPTION, null),
            creatableTopicResult("empty-message-topic", Errors.TOPIC_ALREADY_EXISTS, "")
        ), 1000L);

        var errors = cache.getErrorsForTopics(
            Set.of("null-message-topic", "empty-message-topic"), mockTime.milliseconds());
        assertEquals(Errors.INVALID_TOPIC_EXCEPTION.message(), errors.get("null-message-topic"));
        assertEquals(Errors.TOPIC_ALREADY_EXISTS.message(), errors.get("empty-message-topic"));
    }

    private void cachePut(
            String topicName,
            String errorMessage,
            long ttlMs,
            boolean useCreatableTopicResultPut
    ) {
        if (useCreatableTopicResultPut) {
            cache.put(List.of(creatableTopicResult(topicName, Errors.UNKNOWN_SERVER_ERROR, errorMessage)), ttlMs);
        } else {
            cache.put(Set.of(topicName), errorMessage, ttlMs);
        }
    }

    private static CreatableTopicResult creatableTopicResult(String topicName, Errors error, String errorMessage) {
        return new CreatableTopicResult()
            .setName(topicName)
            .setErrorCode(error.code())
            .setErrorMessage(errorMessage);
    }

}
