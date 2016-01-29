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
package org.apache.kafka.clients;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MetadataTest {

    private long refreshBackoffMs = 100;
    private long metadataExpireMs = 1000;
    private Metadata metadata = new Metadata(refreshBackoffMs, metadataExpireMs);
    private AtomicReference<String> backgroundError = new AtomicReference<String>();

    @After
    public void tearDown() {
        assertNull("Exception in background thread : " + backgroundError.get(), backgroundError.get());
    }

    @Test
    public void testMetadata() throws Exception {
        long time = 0;
        metadata.update(Cluster.empty(), time);
        assertFalse("No update needed.", metadata.timeToNextUpdate(time) == 0);
        metadata.requestUpdate();
        assertFalse("Still no updated needed due to backoff", metadata.timeToNextUpdate(time) == 0);
        time += refreshBackoffMs;
        assertTrue("Update needed now that backoff time expired", metadata.timeToNextUpdate(time) == 0);
        String topic = "my-topic";
        Thread t1 = asyncFetch(topic);
        Thread t2 = asyncFetch(topic);
        assertTrue("Awaiting update", t1.isAlive());
        assertTrue("Awaiting update", t2.isAlive());
        // Perform metadata update when an update is requested on the async fetch thread
        // This simulates the metadata update sequence in KafkaProducer
        while (t1.isAlive() || t2.isAlive()) {
            if (metadata.timeToNextUpdate(time) == 0) {
                metadata.update(TestUtils.singletonCluster(topic, 1), time);
                time += refreshBackoffMs;
            }
            Thread.sleep(1);
        }
        t1.join();
        t2.join();
        assertFalse("No update needed.", metadata.timeToNextUpdate(time) == 0);
        time += metadataExpireMs;
        assertTrue("Update needed due to stale metadata.", metadata.timeToNextUpdate(time) == 0);
    }

    /**
     * Tests that {@link org.apache.kafka.clients.Metadata#awaitUpdate(int, long)} doesn't
     * wait forever with a max timeout value of 0
     *
     * @throws Exception
     * @see https://issues.apache.org/jira/browse/KAFKA-1836
     */
    @Test
    public void testMetadataUpdateWaitTime() throws Exception {
        long time = 0;
        metadata.update(Cluster.empty(), time);
        assertFalse("No update needed.", metadata.timeToNextUpdate(time) == 0);
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
    public void testFailedUpdate() {
        long time = 100;
        metadata.update(Cluster.empty(), time);

        assertEquals(100, metadata.timeToNextUpdate(1000));
        metadata.failedUpdate(1100);

        assertEquals(100, metadata.timeToNextUpdate(1100));
        assertEquals(100, metadata.lastSuccessfulUpdate());

        metadata.needMetadataForAllTopics(true);
        metadata.update(null, time);
        assertEquals(100, metadata.timeToNextUpdate(1000));
    }

    @Test
    public void testUpdateWithNeedMetadataForAllTopics() {
        long time = 0;
        metadata.update(Cluster.empty(), time);
        metadata.needMetadataForAllTopics(true);

        final List<String> expectedTopics = Collections.singletonList("topic");
        metadata.setTopics(expectedTopics);
        metadata.update(new Cluster(
                Collections.singletonList(new Node(0, "host1", 1000)),
                Arrays.asList(
                    new PartitionInfo("topic", 0, null, null, null),
                    new PartitionInfo("topic1", 0, null, null, null)),
                Collections.<String>emptySet()),
            100);

        assertArrayEquals("Metadata got updated with wrong set of topics.",
            expectedTopics.toArray(), metadata.topics().toArray());

        metadata.needMetadataForAllTopics(false);
    }

    @Test
    public void testListenerGetsNotifiedOfUpdate() {
        long time = 0;
        final Set<String> topics = new HashSet<>();
        metadata.update(Cluster.empty(), time);
        metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster) {
                topics.clear();
                topics.addAll(cluster.topics());
            }
        });

        metadata.update(new Cluster(
                Arrays.asList(new Node(0, "host1", 1000)),
                Arrays.asList(
                    new PartitionInfo("topic", 0, null, null, null),
                    new PartitionInfo("topic1", 0, null, null, null)),
                Collections.<String>emptySet()),
            100);

        assertEquals("Listener did not update topics list correctly",
            new HashSet<>(Arrays.asList("topic", "topic1")), topics);
    }

    @Test
    public void testListenerCanUnregister() {
        long time = 0;
        final Set<String> topics = new HashSet<>();
        metadata.update(Cluster.empty(), time);
        final Metadata.Listener listener = new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster) {
                topics.clear();
                topics.addAll(cluster.topics());
            }
        };
        metadata.addListener(listener);

        metadata.update(new Cluster(
                Collections.singletonList(new Node(0, "host1", 1000)),
                Arrays.asList(
                    new PartitionInfo("topic", 0, null, null, null),
                    new PartitionInfo("topic1", 0, null, null, null)),
                Collections.<String>emptySet()),
            100);

        metadata.removeListener(listener);

        metadata.update(new Cluster(
                Arrays.asList(new Node(0, "host1", 1000)),
                Arrays.asList(
                    new PartitionInfo("topic2", 0, null, null, null),
                    new PartitionInfo("topic3", 0, null, null, null)),
                Collections.<String>emptySet()),
            100);

        assertEquals("Listener did not update topics list correctly",
            new HashSet<>(Arrays.asList("topic", "topic1")), topics);
    }

    private Thread asyncFetch(final String topic) {
        Thread thread = new Thread() {
            public void run() {
                while (metadata.fetch().partitionsForTopic(topic) == null) {
                    try {
                        metadata.awaitUpdate(metadata.requestUpdate(), refreshBackoffMs);
                    } catch (Exception e) {
                        backgroundError.set(e.toString());
                    }
                }
            }
        };
        thread.start();
        return thread;
    }
}
