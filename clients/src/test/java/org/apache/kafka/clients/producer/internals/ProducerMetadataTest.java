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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProducerMetadataTest {
    private static final long METADATA_IDLE_MS = 60 * 1000;
    private long refreshBackoffMs = 100;
    private long metadataExpireMs = 1000;
    private ProducerMetadata metadata = new ProducerMetadata(refreshBackoffMs, metadataExpireMs, METADATA_IDLE_MS,
            new LogContext(), new ClusterResourceListeners(), Time.SYSTEM);
    private AtomicReference<Exception> backgroundError = new AtomicReference<>();

    @After
    public void tearDown() {
        assertNull("Exception in background thread : " + backgroundError.get(), backgroundError.get());
    }

    @Test
    public void testMetadata() throws Exception {
        long time = Time.SYSTEM.milliseconds();
        String topic = "my-topic";
        metadata.add(topic, time);

        metadata.updateWithCurrentRequestVersion(responseWithTopics(Collections.emptySet()), false, time);
        assertTrue("No update needed.", metadata.timeToNextUpdate(time) > 0);
        metadata.requestUpdate();
        assertTrue("Still no updated needed due to backoff", metadata.timeToNextUpdate(time) > 0);
        time += refreshBackoffMs;
        assertEquals("Update needed now that backoff time expired", 0, metadata.timeToNextUpdate(time));
        Thread t1 = asyncFetch(topic, 500);
        Thread t2 = asyncFetch(topic, 500);
        assertTrue("Awaiting update", t1.isAlive());
        assertTrue("Awaiting update", t2.isAlive());
        // Perform metadata update when an update is requested on the async fetch thread
        // This simulates the metadata update sequence in KafkaProducer
        while (t1.isAlive() || t2.isAlive()) {
            if (metadata.timeToNextUpdate(time) == 0) {
                metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
                time += refreshBackoffMs;
            }
            Thread.sleep(1);
        }
        t1.join();
        t2.join();
        assertTrue("No update needed.", metadata.timeToNextUpdate(time) > 0);
        time += metadataExpireMs;
        assertEquals("Update needed due to stale metadata.", 0, metadata.timeToNextUpdate(time));
    }

    @Test
    public void testMetadataAwaitAfterClose() throws InterruptedException {
        long time = 0;
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
        assertTrue("No update needed.", metadata.timeToNextUpdate(time) > 0);
        metadata.requestUpdate();
        assertTrue("Still no updated needed due to backoff", metadata.timeToNextUpdate(time) > 0);
        time += refreshBackoffMs;
        assertEquals("Update needed now that backoff time expired", 0, metadata.timeToNextUpdate(time));
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
        assertTrue("No update needed.", metadata.timeToNextUpdate(time) > 0);
        // first try with a max wait time of 0 and ensure that this returns back without waiting forever
        try {
            metadata.awaitUpdate(metadata.requestUpdate(), 0);
            fail("Wait on metadata update was expected to timeout, but it didn't");
        } catch (TimeoutException te) {
            // expected
        }
        // now try with a higher timeout value once
        final long twoSecondWait = 2000;
        try {
            metadata.awaitUpdate(metadata.requestUpdate(), twoSecondWait);
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
        assertFalse("Unused topic not expired", metadata.containsTopic(topic1));

        // Test that topic is not expired if used within the expiry interval
        final String topic2 = "topic2";
        metadata.add(topic2, time);
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
        for (int i = 0; i < 3; i++) {
            time += METADATA_IDLE_MS / 2;
            metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
            assertTrue("Topic expired even though in use", metadata.containsTopic(topic2));
            metadata.add(topic2, time);
        }

        // Add a new topic, but update its metadata after the expiry would have occurred.
        // The topic should still be retained.
        final String topic3 = "topic3";
        metadata.add(topic3, time);
        time += METADATA_IDLE_MS * 2;
        metadata.updateWithCurrentRequestVersion(responseWithCurrentTopics(), false, time);
        assertTrue("Topic expired while awaiting metadata", metadata.containsTopic(topic3));
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
        assertEquals(metadata.topics(), new HashSet<>(Arrays.asList(topic1, topic2, topic3)));
        assertEquals(metadata.newTopics(), new HashSet<>(Arrays.asList(topic2, topic3)));

        // Perform the partial update for a subset of the new topics.
        now += 1000;
        assertTrue(metadata.updateRequested());
        metadata.updateWithCurrentRequestVersion(responseWithTopics(Collections.singleton(topic2)), true, now);
        assertEquals(metadata.topics(), new HashSet<>(Arrays.asList(topic1, topic2, topic3)));
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
        metadata.updateWithCurrentRequestVersion(responseWithTopics(new HashSet<>(Arrays.asList(topic1, topic2))), false, now);
        assertFalse(metadata.updateRequested());
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
                    metadata.awaitUpdate(metadata.requestUpdate(), maxWaitMs);
            } catch (Exception e) {
                backgroundError.set(e);
            }
        });
        thread.start();
        return thread;
    }

}
