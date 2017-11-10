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
package org.apache.kafka.clients;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.test.MockClusterResourceListener;
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
    private Metadata metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true);
    private AtomicReference<String> backgroundError = new AtomicReference<>();

    @After
    public void tearDown() {
        assertNull("Exception in background thread : " + backgroundError.get(), backgroundError.get());
    }

    @Test
    public void testMetadata() throws Exception {
        long time = 0;
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        assertFalse("No update needed.", metadata.timeToNextUpdate(time) == 0);
        metadata.requestUpdate();
        assertFalse("Still no updated needed due to backoff", metadata.timeToNextUpdate(time) == 0);
        time += refreshBackoffMs;
        assertTrue("Update needed now that backoff time expired", metadata.timeToNextUpdate(time) == 0);
        String topic = "my-topic";
        Thread t1 = asyncFetch(topic, 500);
        Thread t2 = asyncFetch(topic, 500);
        assertTrue("Awaiting update", t1.isAlive());
        assertTrue("Awaiting update", t2.isAlive());
        // Perform metadata update when an update is requested on the async fetch thread
        // This simulates the metadata update sequence in KafkaProducer
        while (t1.isAlive() || t2.isAlive()) {
            if (metadata.timeToNextUpdate(time) == 0) {
                metadata.update(TestUtils.singletonCluster(topic, 1), Collections.<String>emptySet(), time);
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

    private static void checkTimeToNextUpdate(long refreshBackoffMs, long metadataExpireMs) {
        long now = 10000;

        // Metadata timeToNextUpdate is implicitly relying on the premise that the currentTimeMillis is always
        // larger than the metadataExpireMs or refreshBackoffMs.
        // It won't be a problem practically since all usages of Metadata calls first update() immediately after
        // it's construction.
        if (metadataExpireMs > now || refreshBackoffMs > now) {
            throw new IllegalArgumentException(
                    "metadataExpireMs and refreshBackoffMs must be smaller than 'now'");
        }

        long largerOfBackoffAndExpire = Math.max(refreshBackoffMs, metadataExpireMs);
        Metadata metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true);

        assertEquals(0, metadata.timeToNextUpdate(now));

        // lastSuccessfulRefreshMs updated to now.
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), now);

        // The last update was successful so the remaining time to expire the current metadata should be returned.
        assertEquals(largerOfBackoffAndExpire, metadata.timeToNextUpdate(now));

        // Metadata update requested explicitly
        metadata.requestUpdate();
        // Update requested so metadataExpireMs should no longer take effect.
        assertEquals(refreshBackoffMs, metadata.timeToNextUpdate(now));

        // Reset needUpdate to false.
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), now);
        assertEquals(largerOfBackoffAndExpire, metadata.timeToNextUpdate(now));

        // Both metadataExpireMs and refreshBackoffMs elapsed.
        now += largerOfBackoffAndExpire;
        assertEquals(0, metadata.timeToNextUpdate(now));
        assertEquals(0, metadata.timeToNextUpdate(now + 1));
    }

    @Test
    public void testTimeToNextUpdate() {
        checkTimeToNextUpdate(100, 1000);
        checkTimeToNextUpdate(1000, 100);
        checkTimeToNextUpdate(0, 0);
        checkTimeToNextUpdate(0, 100);
        checkTimeToNextUpdate(100, 0);
    }

    @Test
    public void testTimeToNextUpdate_RetryBackoff() {
        long now = 10000;

        // lastRefreshMs updated to now.
        metadata.failedUpdate(now, null);

        // Backing off. Remaining time until next try should be returned.
        assertEquals(refreshBackoffMs, metadata.timeToNextUpdate(now));

        // Even though metadata update requested explicitly, still respects backoff.
        metadata.requestUpdate();
        assertEquals(refreshBackoffMs, metadata.timeToNextUpdate(now));

        // refreshBackoffMs elapsed.
        now += refreshBackoffMs;
        // It should return 0 to let next try.
        assertEquals(0, metadata.timeToNextUpdate(now));
        assertEquals(0, metadata.timeToNextUpdate(now + 1));
    }

    @Test
    public void testTimeToNextUpdate_OverwriteBackoff() {
        long now = 10000;

        // New topic added to fetch set and update requested. It should allow immediate update.
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), now);
        metadata.add("new-topic");
        assertEquals(0, metadata.timeToNextUpdate(now));

        // Even though setTopics called, immediate update isn't necessary if the new topic set isn't
        // containing a new topic,
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), now);
        metadata.setTopics(metadata.topics());
        assertEquals(metadataExpireMs, metadata.timeToNextUpdate(now));

        // If the new set of topics containing a new topic then it should allow immediate update.
        metadata.setTopics(Collections.singletonList("another-new-topic"));
        assertEquals(0, metadata.timeToNextUpdate(now));

        // If metadata requested for all topics it should allow immediate update.
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), now);
        metadata.needMetadataForAllTopics(true);
        assertEquals(0, metadata.timeToNextUpdate(now));

        // However if metadata is already capable to serve all topics it shouldn't override backoff.
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), now);
        metadata.needMetadataForAllTopics(true);
        assertEquals(metadataExpireMs, metadata.timeToNextUpdate(now));
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
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
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
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);

        assertEquals(100, metadata.timeToNextUpdate(1000));
        metadata.failedUpdate(1100, null);

        assertEquals(100, metadata.timeToNextUpdate(1100));
        assertEquals(100, metadata.lastSuccessfulUpdate());

        metadata.needMetadataForAllTopics(true);
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        assertEquals(100, metadata.timeToNextUpdate(1000));
    }

    @Test
    public void testUpdateWithNeedMetadataForAllTopics() {
        long time = 0;
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        metadata.needMetadataForAllTopics(true);

        final List<String> expectedTopics = Collections.singletonList("topic");
        metadata.setTopics(expectedTopics);
        metadata.update(new Cluster(null,
                Collections.singletonList(new Node(0, "host1", 1000)),
                Arrays.asList(
                    new PartitionInfo("topic", 0, null, null, null),
                    new PartitionInfo("topic1", 0, null, null, null)),
                Collections.<String>emptySet(),
                Collections.<String>emptySet()),
            Collections.<String>emptySet(), 100);

        assertArrayEquals("Metadata got updated with wrong set of topics.",
            expectedTopics.toArray(), metadata.topics().toArray());

        metadata.needMetadataForAllTopics(false);
    }

    @Test
    public void testClusterListenerGetsNotifiedOfUpdate() {
        long time = 0;
        MockClusterResourceListener mockClusterListener = new MockClusterResourceListener();
        ClusterResourceListeners listeners = new ClusterResourceListeners();
        listeners.maybeAdd(mockClusterListener);
        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true, false, listeners);

        String hostName = "www.example.com";
        Cluster cluster = Cluster.bootstrap(Arrays.asList(new InetSocketAddress(hostName, 9002)));
        metadata.update(cluster, Collections.<String>emptySet(), time);
        assertFalse("ClusterResourceListener should not called when metadata is updated with bootstrap Cluster",
                MockClusterResourceListener.IS_ON_UPDATE_CALLED.get());

        metadata.update(new Cluster(
                        "dummy",
                        Arrays.asList(new Node(0, "host1", 1000)),
                        Arrays.asList(
                                new PartitionInfo("topic", 0, null, null, null),
                                new PartitionInfo("topic1", 0, null, null, null)),
                        Collections.<String>emptySet(),
                        Collections.<String>emptySet()),
                Collections.<String>emptySet(), 100);

        assertEquals("MockClusterResourceListener did not get cluster metadata correctly",
                "dummy", mockClusterListener.clusterResource().clusterId());
        assertTrue("MockClusterResourceListener should be called when metadata is updated with non-bootstrap Cluster",
                MockClusterResourceListener.IS_ON_UPDATE_CALLED.get());
    }

    @Test
    public void testListenerGetsNotifiedOfUpdate() {
        long time = 0;
        final Set<String> topics = new HashSet<>();
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
                topics.clear();
                topics.addAll(cluster.topics());
            }
        });

        metadata.update(new Cluster(
                null,
                Arrays.asList(new Node(0, "host1", 1000)),
                Arrays.asList(
                    new PartitionInfo("topic", 0, null, null, null),
                    new PartitionInfo("topic1", 0, null, null, null)),
                Collections.<String>emptySet(),
                Collections.<String>emptySet()),
            Collections.<String>emptySet(), 100);

        assertEquals("Listener did not update topics list correctly",
            new HashSet<>(Arrays.asList("topic", "topic1")), topics);
    }

    @Test
    public void testListenerCanUnregister() {
        long time = 0;
        final Set<String> topics = new HashSet<>();
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        final Metadata.Listener listener = new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
                topics.clear();
                topics.addAll(cluster.topics());
            }
        };
        metadata.addListener(listener);

        metadata.update(new Cluster(
                "cluster",
                Collections.singletonList(new Node(0, "host1", 1000)),
                Arrays.asList(
                    new PartitionInfo("topic", 0, null, null, null),
                    new PartitionInfo("topic1", 0, null, null, null)),
                Collections.<String>emptySet(),
                Collections.<String>emptySet()),
            Collections.<String>emptySet(), 100);

        metadata.removeListener(listener);

        metadata.update(new Cluster(
                "cluster",
                Arrays.asList(new Node(0, "host1", 1000)),
                Arrays.asList(
                    new PartitionInfo("topic2", 0, null, null, null),
                    new PartitionInfo("topic3", 0, null, null, null)),
                Collections.<String>emptySet(),
                Collections.<String>emptySet()),
            Collections.<String>emptySet(), 100);

        assertEquals("Listener did not update topics list correctly",
            new HashSet<>(Arrays.asList("topic", "topic1")), topics);
    }

    @Test
    public void testTopicExpiry() throws Exception {
        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true, true, new ClusterResourceListeners());

        // Test that topic is expired if not used within the expiry interval
        long time = 0;
        metadata.add("topic1");
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        time += Metadata.TOPIC_EXPIRY_MS;
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        assertFalse("Unused topic not expired", metadata.containsTopic("topic1"));

        // Test that topic is not expired if used within the expiry interval
        metadata.add("topic2");
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        for (int i = 0; i < 3; i++) {
            time += Metadata.TOPIC_EXPIRY_MS / 2;
            metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
            assertTrue("Topic expired even though in use", metadata.containsTopic("topic2"));
            metadata.add("topic2");
        }

        // Test that topics added using setTopics expire
        HashSet<String> topics = new HashSet<>();
        topics.add("topic4");
        metadata.setTopics(topics);
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        time += Metadata.TOPIC_EXPIRY_MS;
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        assertFalse("Unused topic not expired", metadata.containsTopic("topic4"));
    }

    @Test
    public void testNonExpiringMetadata() throws Exception {
        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true, false, new ClusterResourceListeners());

        // Test that topic is not expired if not used within the expiry interval
        long time = 0;
        metadata.add("topic1");
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        time += Metadata.TOPIC_EXPIRY_MS;
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        assertTrue("Unused topic expired when expiry disabled", metadata.containsTopic("topic1"));

        // Test that topic is not expired if used within the expiry interval
        metadata.add("topic2");
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        for (int i = 0; i < 3; i++) {
            time += Metadata.TOPIC_EXPIRY_MS / 2;
            metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
            assertTrue("Topic expired even though in use", metadata.containsTopic("topic2"));
            metadata.add("topic2");
        }

        // Test that topics added using setTopics don't expire
        HashSet<String> topics = new HashSet<>();
        topics.add("topic4");
        metadata.setTopics(topics);
        time += metadataExpireMs * 2;
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time);
        assertTrue("Unused topic expired when expiry disabled", metadata.containsTopic("topic4"));
    }

    private Thread asyncFetch(final String topic, final long maxWaitMs) {
        Thread thread = new Thread() {
            public void run() {
                while (metadata.fetch().partitionsForTopic(topic).isEmpty()) {
                    try {
                        metadata.awaitUpdate(metadata.requestUpdate(), maxWaitMs);
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
