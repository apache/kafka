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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ProducerMetadataTest {
    private static final long METADATA_IDLE_MS = 60 * 1000;
    private final long refreshBackoffMs = 100;
    private final long refreshBackoffMaxMs = 1000;
    private final long metadataExpireMs = 1000;
    private final ProducerMetadata metadata = new ProducerMetadata(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs, METADATA_IDLE_MS,
            new LogContext(), new ClusterResourceListeners(), Time.SYSTEM);
    private final AtomicReference<Exception> backgroundError = new AtomicReference<>();

    @AfterEach
    public void tearDown() {
        assertNull(backgroundError.get(), "Exception in background thread : " + backgroundError.get());
    }

    @Test
    public void testMetadata() throws Exception {
        long time = Time.SYSTEM.milliseconds();
        String topic = "my-topic";
        metadata.add(topic, time);

        metadata.updateWithCurrentRequestVersion(responseWithTopics(Collections.emptySet()), false, time);
        assertTrue(metadata.timeToNextUpdate(time) > 0, "No update needed.");
        metadata.requestUpdate(true);
        assertTrue(metadata.timeToNextUpdate(time) > 0, "Still no updated needed due to backoff");
        time += (long) (refreshBackoffMs * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
        assertEquals(0, metadata.timeToNextUpdate(time), "Update needed now that backoff time expired");
        Thread t1 = asyncFetch(topic, 500);
        Thread t2 = asyncFetch(topic, 500);
        assertTrue(t1.isAlive(), "Awaiting update");
        assertTrue(t2.isAlive(), "Awaiting update");
        // Perform metadata update when an update is requested on the async fetch thread
        // This simulates the metadata update sequence in KafkaProducer
        while (t1.isAlive() || t2.isAlive()) {
            if (metadata.timeToNextUpdate(time) == 0) {
                metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
                time += (long) (refreshBackoffMs * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
            }
            Thread.sleep(1);
        }
        t1.join();
        t2.join();
        assertTrue(metadata.timeToNextUpdate(time) > 0, "No update needed.");
        time += metadataExpireMs;
        assertEquals(0, metadata.timeToNextUpdate(time), "Update needed due to stale metadata.");
    }

    @Test
    public void testMetadataAwaitAfterClose() throws InterruptedException {
        long time = 0;
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
        assertTrue(metadata.timeToNextUpdate(time) > 0, "No update needed.");
        metadata.requestUpdate(true);
        assertTrue(metadata.timeToNextUpdate(time) > 0, "Still no updated needed due to backoff");
        time += (long) (refreshBackoffMs * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
        assertEquals(0, metadata.timeToNextUpdate(time), "Update needed now that backoff time expired");
        String topic = "my-topic";
        metadata.close();
        Thread t1 = asyncFetch(topic, 500);
        t1.join();
        assertEquals(KafkaException.class, backgroundError.get().getClass());
        assertTrue(backgroundError.get().toString().contains("Requested metadata update after close"));
        clearBackgroundError();
    }

    @Test
    public void testMetadataEquivalentResponsesBackoff() throws InterruptedException {
        long time = 0;
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
        assertTrue(metadata.timeToNextUpdate(time) > 0, "No update needed");
        metadata.requestUpdate(false);
        assertTrue(metadata.timeToNextUpdate(time) > 0, "Still no update needed due to backoff");
        time += (long) (refreshBackoffMs * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
        assertTrue(metadata.timeToNextUpdate(time) > 0, "No update needed after equivalent metadata response");
        metadata.requestUpdate(false);
        assertTrue(metadata.timeToNextUpdate(time) > 0, "Still no update needed due to backoff");
        assertTrue(metadata.timeToNextUpdate(time + refreshBackoffMs) > 0, "Still no updated needed due to exponential backoff");
        time += (long) (refreshBackoffMs * CommonClientConfigs.RETRY_BACKOFF_EXP_BASE * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
        assertEquals(0, metadata.timeToNextUpdate(time), "Update needed now that backoff time expired");
        String topic = "my-topic";
        metadata.close();
        Thread t1 = asyncFetch(topic, 500);
        t1.join();
        assertEquals(KafkaException.class, backgroundError.get().getClass());
        assertTrue(backgroundError.get().toString().contains("Requested metadata update after close"));
        clearBackgroundError();
    }

    /**
     * Tests that {@link org.apache.kafka.clients.producer.internals.ProducerMetadata#awaitUpdate(int, long)} doesn't
     * wait forever with a max timeout value of 0
     *
     * @throws Exception
     * @see <a href=https://issues.apache.org/jira/browse/KAFKA-1836>KAFKA-1836</a>
     */
    @Test
    public void testMetadataUpdateWaitTime() throws Exception {
        long time = 0;
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
        assertTrue(metadata.timeToNextUpdate(time) > 0, "No update needed.");
        // first try with a max wait time of 0 and ensure that this returns back without waiting forever
        try {
            metadata.awaitUpdate(metadata.requestUpdate(true), 0);
            fail("Wait on metadata update was expected to timeout, but it didn't");
        } catch (TimeoutException te) {
            // expected
        }
        // now try with a higher timeout value once
        final long twoSecondWait = 2000;
        try {
            metadata.awaitUpdate(metadata.requestUpdate(true), twoSecondWait);
            fail("Wait on metadata update was expected to timeout, but it didn't");
        } catch (TimeoutException te) {
            // expected
        }
    }

    @Test
    public void testTimeToNextUpdateOverwriteBackoff() {
        long now = 10000;

        // New topic added to fetch set and update requested. It should allow immediate update.
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, now);
        metadata.add("new-topic", now);
        assertEquals(0, metadata.timeToNextUpdate(now));

        // Even though add is called, immediate update isn't necessary if the new topic set isn't
        // containing a new topic,
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, now);
        metadata.add("new-topic", now);
        assertEquals(metadataExpireMs, metadata.timeToNextUpdate(now));

        // If the new set of topics containing a new topic then it should allow immediate update.
        metadata.add("another-new-topic", now);
        assertEquals(0, metadata.timeToNextUpdate(now));
    }

    @Test
    public void testTopicExpiry() {
        // Test that topic is expired if not used within the expiry interval
        long time = 0;
        final String topic1 = "topic1";
        metadata.add(topic1, time);
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
        assertTrue(metadata.containsTopic(topic1));

        time += METADATA_IDLE_MS;
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
        assertFalse(metadata.containsTopic(topic1), "Unused topic not expired");

        // Test that topic is not expired if used within the expiry interval
        final String topic2 = "topic2";
        metadata.add(topic2, time);
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
        for (int i = 0; i < 3; i++) {
            time += METADATA_IDLE_MS / 2;
            metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
            assertTrue(metadata.containsTopic(topic2), "Topic expired even though in use");
            metadata.add(topic2, time);
        }

        // Add a new topic, but update its metadata after the expiry would have occurred.
        // The topic should still be retained.
        final String topic3 = "topic3";
        metadata.add(topic3, time);
        time += METADATA_IDLE_MS * 2;
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
        assertTrue(metadata.containsTopic(topic3), "Topic expired while awaiting metadata");
    }

    @Test
    public void testMetadataWaitAbortedOnFatalException() {
        metadata.fatalError(new AuthenticationException("Fatal exception from test"));
        assertThrows(AuthenticationException.class, () -> metadata.awaitUpdate(0, 1000));
    }

    @Test
    public void testMetadataPartialUpdate() {
        long now = 10000;

        // Add a new topic and fetch its metadata in a partial update.
        final String topic1 = "topic-one";
        metadata.add(topic1, now);
        assertTrue(metadata.updateRequested());
        assertEquals(0, metadata.timeToNextUpdate(now));
        assertEquals(metadata.topics(), Collections.singleton(topic1));
        assertEquals(metadata.newTopics(), Collections.singleton(topic1));

        // Perform the partial update. Verify the topic is no longer considered "new".
        now += 1000;
        metadata.updateWithCurrentRequestVersion(responseWithTopics(Collections.singleton(topic1)), true, now);
        assertFalse(metadata.updateRequested());
        assertEquals(metadata.topics(), Collections.singleton(topic1));
        assertEquals(metadata.newTopics(), Collections.emptySet());

        // Add the topic again. It should not be considered "new".
        metadata.add(topic1, now);
        assertFalse(metadata.updateRequested());
        assertTrue(metadata.timeToNextUpdate(now) > 0);
        assertEquals(metadata.topics(), Collections.singleton(topic1));
        assertEquals(metadata.newTopics(), Collections.emptySet());

        // Add two new topics. However, we'll only apply a partial update for one of them.
        now += 1000;
        final String topic2 = "topic-two";
        metadata.add(topic2, now);

        now += 1000;
        final String topic3 = "topic-three";
        metadata.add(topic3, now);

        assertTrue(metadata.updateRequested());
        assertEquals(0, metadata.timeToNextUpdate(now));
        assertEquals(metadata.topics(), Set.of(topic1, topic2, topic3));
        assertEquals(metadata.newTopics(), Set.of(topic2, topic3));

        // Perform the partial update for a subset of the new topics.
        now += 1000;
        assertTrue(metadata.updateRequested());
        metadata.updateWithCurrentRequestVersion(responseWithTopics(Collections.singleton(topic2)), true, now);
        assertEquals(metadata.topics(), Set.of(topic1, topic2, topic3));
        assertEquals(metadata.newTopics(), Collections.singleton(topic3));
    }

    @Test
    public void testRequestUpdateForTopic() {
        long now = 10000;

        final String topic1 = "topic-1";
        final String topic2 = "topic-2";

        // Add the topics to the metadata.
        metadata.add(topic1, now);
        metadata.add(topic2, now);
        assertTrue(metadata.updateRequested());

        // Request an update for topic1. Since the topic is considered new, it should not trigger
        // the metadata to require a full update.
        metadata.requestUpdateForTopic(topic1);
        assertTrue(metadata.updateRequested());

        // Perform the partial update. Verify no additional (full) updates are requested.
        now += 1000;
        metadata.updateWithCurrentRequestVersion(responseWithTopics(Collections.singleton(topic1)), true, now);
        assertFalse(metadata.updateRequested());

        // Request an update for topic1 again. Such a request may occur when the leader
        // changes, which may affect many topics, and should therefore request a full update.
        metadata.requestUpdateForTopic(topic1);
        assertTrue(metadata.updateRequested());

        // Perform a partial update for the topic. This should not clear the full update.
        now += 1000;
        metadata.updateWithCurrentRequestVersion(responseWithTopics(Collections.singleton(topic1)), true, now);
        assertTrue(metadata.updateRequested());

        // Perform the full update. This should clear the update request.
        now += 1000;
        metadata.updateWithCurrentRequestVersion(responseWithTopics(Set.of(topic1, topic2)), false, now);
        assertFalse(metadata.updateRequested());
    }

    /**
     * Verifies the fast path in {@code add(String, long)}: re-adding an already-known topic
     * refreshes its expiry but does NOT add it to {@code newTopics} or trigger an update request.
     */
    @Test
    public void testAddKnownTopicFastPathDoesNotAddToNewTopics() {
        long now = 0;
        String topic = "known-topic";

        // First add: topic is new, update should be requested.
        metadata.add(topic, now);
        assertTrue(metadata.newTopics().contains(topic));
        assertTrue(metadata.updateRequested());

        // Simulate metadata response so the topic is no longer "new".
        metadata.updateWithCurrentRequestVersion(responseWithTopics(Collections.singleton(topic)), true, now);
        assertFalse(metadata.newTopics().contains(topic));
        assertFalse(metadata.updateRequested());

        // Second add: topic is already known — fast path via replace(), must not touch newTopics or request an update.
        now += 1000;
        metadata.add(topic, now);
        assertFalse(metadata.newTopics().contains(topic), "Known topic must not be re-added to newTopics");
        assertFalse(metadata.updateRequested(), "Re-adding a known topic must not trigger an update request");
    }

    /**
     * Verifies that {@code add(Collection, long)} returns {@code OptionalInt.empty()} when all
     * supplied topics are already known (fast path only, no lock contention).
     */
    @Test
    public void testBatchAddAllKnownTopicsReturnsEmptyOptional() {
        long now = 0;
        List<String> topics = List.of("topic-a", "topic-b", "topic-c");

        // Seed the topics as known.
        for (String t : topics) metadata.add(t, now);
        metadata.updateWithCurrentRequestVersion(responseWithTopics(Set.copyOf(topics)), true, now);
        assertFalse(metadata.updateRequested());

        // Batch-add with all known topics — no new topic, so OptionalInt must be empty.
        now += 1000;
        OptionalInt result = metadata.add(topics, now);
        assertFalse(result.isPresent(), "Batch add of all-known topics must return OptionalInt.empty()");
        assertFalse(metadata.updateRequested());
    }

    /**
     * Verifies that {@code add(Collection, long)} returns a present {@code OptionalInt} and
     * triggers an update request when at least one topic in the batch is new.
     */
    @Test
    public void testBatchAddWithNewTopicReturnsPresentOptional() {
        long now = 0;
        String knownTopic = "known";
        String newTopic = "new";

        metadata.add(knownTopic, now);
        metadata.updateWithCurrentRequestVersion(responseWithTopics(Collections.singleton(knownTopic)), true, now);
        assertFalse(metadata.updateRequested());

        now += 1000;
        OptionalInt result = metadata.add(List.of(knownTopic, newTopic), now);
        assertTrue(result.isPresent(), "Batch add with a new topic must return a present OptionalInt");
        assertTrue(metadata.newTopics().contains(newTopic));
        assertTrue(metadata.updateRequested());
    }

    /**
     * Verifies that multiple threads racing to add the same brand-new topic result in the topic
     * being recorded in {@code newTopics} exactly once. The synchronized slow path must ensure
     * idempotent insertion even under high concurrency.
     */
    @Test
    public void testConcurrentAddOfSameNewTopicIsIdempotent() throws InterruptedException {
        int threadCount = 20;
        String topic = "concurrent-topic";
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean error = new AtomicBoolean(false);
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    metadata.add(topic, 0);
                } catch (Exception e) {
                    error.set(true);
                }
            });
            t.start();
            threads.add(t);
        }

        ready.await();
        start.countDown();
        for (Thread t : threads) t.join();

        assertFalse(error.get(), "No thread should throw during concurrent add");
        assertTrue(metadata.topics().contains(topic));
        assertEquals(1, metadata.newTopics().size(), "Topic must appear in newTopics exactly once");
        assertTrue(metadata.newTopics().contains(topic));
    }

    /**
     * Verifies that a concurrent {@code add()} refreshing a topic's expiry is not undone by a
     * concurrent {@code retainTopic()} that decided to evict based on the old (stale) expiry.
     * The conditional {@code remove(topic, expireMs)} in {@code retainTopic()} must lose the CAS
     * when {@code add()} has already replaced the value, leaving the topic in the map.
     */
    @Test
    public void testRetainTopicConditionalRemovePreservesRefreshedTopic() throws InterruptedException {
        int iterations = 200;
        for (int i = 0; i < iterations; i++) {
            ProducerMetadata m = new ProducerMetadata(refreshBackoffMs, refreshBackoffMaxMs, metadataExpireMs,
                    METADATA_IDLE_MS, new LogContext(), new ClusterResourceListeners(), Time.SYSTEM);
            String topic = "topic";
            m.add(topic, 0);
            m.updateWithCurrentRequestVersion(responseWithTopics(Collections.singleton(topic)), false, 0);

            // At nowMs = METADATA_IDLE_MS the topic is exactly at its expiry boundary.
            long nowMs = METADATA_IDLE_MS;

            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(2);
            AtomicBoolean threadError = new AtomicBoolean(false);

            // Thread 1: refresh the topic expiry (fast path — no lock).
            Thread adder = new Thread(() -> {
                try {
                    start.await();
                    m.add(topic, nowMs);  // replaces expiry to nowMs + METADATA_IDLE_MS
                } catch (Exception e) {
                    threadError.set(true);
                } finally {
                    done.countDown();
                }
            });

            // Thread 2: attempt to evict the topic at the same time.
            Thread retainer = new Thread(() -> {
                try {
                    start.await();
                    m.retainTopic(topic, false, nowMs);
                } catch (Exception e) {
                    threadError.set(true);
                } finally {
                    done.countDown();
                }
            });

            adder.start();
            retainer.start();
            start.countDown();
            done.await();

            assertFalse(threadError.get(), "No thread should throw");
            // After add() refreshes the expiry, the topic must still be present regardless of
            // which thread won the race: either retainTopic saw the new expiry and kept it, or
            // its conditional remove(topic, oldExpiry) lost the CAS because add() had already
            // replaced the value.
            assertTrue(m.containsTopic(topic),
                    "Topic must remain after concurrent add() refresh and retainTopic() eviction attempt (iteration " + i + ")");
        }
    }

    private MetadataResponse responseWithCurrentTopics() {
        return responseWithTopics(metadata.topics());
    }

    private MetadataResponse responseWithTopics(Set<String> topics) {
        Map<String, Integer> partitionCounts = new HashMap<>();
        for (String topic : topics)
            partitionCounts.put(topic, 1);
        return RequestTestUtils.metadataUpdateWith(1, partitionCounts);
    }

    private void clearBackgroundError() {
        backgroundError.set(null);
    }

    private Thread asyncFetch(final String topic, final long maxWaitMs) {
        Thread thread = new Thread(() -> {
            try {
                while (metadata.fetch().partitionsForTopic(topic).isEmpty())
                    metadata.awaitUpdate(metadata.requestUpdate(false), maxWaitMs);
            } catch (Exception e) {
                backgroundError.set(e);
            }
        });
        thread.start();
        return thread;
    }

}
