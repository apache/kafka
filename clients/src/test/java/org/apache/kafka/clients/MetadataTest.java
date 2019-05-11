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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.MockClusterResourceListener;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MetadataTest {

    private long refreshBackoffMs = 100;
    private long metadataExpireMs = 1000;
    private Metadata metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true);
    private AtomicReference<Exception> backgroundError = new AtomicReference<>();

    @After
    public void tearDown() {
        assertNull("Exception in background thread : " + backgroundError.get(), backgroundError.get());
    }

    private static MetadataResponse emptyMetadataResponse() {
        return new MetadataResponse(
                Collections.emptyList(),
                null,
                -1,
                Collections.emptyList());
    }
    
    @Test
    public void testMetadata() throws Exception {
        long time = 0;
        metadata.update(emptyMetadataResponse(), time);
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
                MetadataResponse response = TestUtils.metadataUpdateWith(1, Collections.singletonMap(topic, 1));
                metadata.update(response, time);
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

    @Test
    public void testMetadataAwaitAfterClose() throws InterruptedException {
        long time = 0;
        metadata.update(emptyMetadataResponse(), time);
        assertFalse("No update needed.", metadata.timeToNextUpdate(time) == 0);
        metadata.requestUpdate();
        assertFalse("Still no updated needed due to backoff", metadata.timeToNextUpdate(time) == 0);
        time += refreshBackoffMs;
        assertTrue("Update needed now that backoff time expired", metadata.timeToNextUpdate(time) == 0);
        String topic = "my-topic";
        metadata.close();
        Thread t1 = asyncFetch(topic, 500);
        t1.join();
        assertTrue(backgroundError.get().getClass() == KafkaException.class);
        assertTrue(backgroundError.get().toString().contains("Requested metadata update after close"));
        clearBackgroundError();
    }

    @Test(expected = IllegalStateException.class)
    public void testMetadataUpdateAfterClose() {
        metadata.close();
        metadata.update(emptyMetadataResponse(), 1000);
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
        metadata.update(emptyMetadataResponse(), now);

        // The last update was successful so the remaining time to expire the current metadata should be returned.
        assertEquals(largerOfBackoffAndExpire, metadata.timeToNextUpdate(now));

        // Metadata update requested explicitly
        metadata.requestUpdate();
        // Update requested so metadataExpireMs should no longer take effect.
        assertEquals(refreshBackoffMs, metadata.timeToNextUpdate(now));

        // Reset needUpdate to false.
        metadata.update(emptyMetadataResponse(), now);
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
        metadata.update(emptyMetadataResponse(), now);
        metadata.add("new-topic");
        assertEquals(0, metadata.timeToNextUpdate(now));

        // Even though setTopics called, immediate update isn't necessary if the new topic set isn't
        // containing a new topic,
        metadata.update(emptyMetadataResponse(), now);
        metadata.setTopics(metadata.topics());
        assertEquals(metadataExpireMs, metadata.timeToNextUpdate(now));

        // If the new set of topics containing a new topic then it should allow immediate update.
        metadata.setTopics(Collections.singletonList("another-new-topic"));
        assertEquals(0, metadata.timeToNextUpdate(now));

        // If metadata requested for all topics it should allow immediate update.
        metadata.update(emptyMetadataResponse(), now);
        metadata.needMetadataForAllTopics(true);
        assertEquals(0, metadata.timeToNextUpdate(now));

        // However if metadata is already capable to serve all topics it shouldn't override backoff.
        metadata.update(emptyMetadataResponse(), now);
        metadata.needMetadataForAllTopics(true);
        assertEquals(metadataExpireMs, metadata.timeToNextUpdate(now));
    }

    /**
     * Tests that {@link org.apache.kafka.clients.Metadata#awaitUpdate(int, long)} doesn't
     * wait forever with a max timeout value of 0
     *
     * @throws Exception
     * @see <a href=https://issues.apache.org/jira/browse/KAFKA-1836>KAFKA-1836</a>
     */
    @Test
    public void testMetadataUpdateWaitTime() throws Exception {
        long time = 0;
        metadata.update(emptyMetadataResponse(), time);
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
        metadata.update(emptyMetadataResponse(), time);

        assertEquals(100, metadata.timeToNextUpdate(1000));
        metadata.failedUpdate(1100, null);

        assertEquals(100, metadata.timeToNextUpdate(1100));
        assertEquals(100, metadata.lastSuccessfulUpdate());

        metadata.needMetadataForAllTopics(true);
        metadata.update(emptyMetadataResponse(), time);
        assertEquals(100, metadata.timeToNextUpdate(1000));
    }

    @Test
    public void testUpdateWithNeedMetadataForAllTopics() {
        long time = 0;
        metadata.update(emptyMetadataResponse(), time);
        metadata.needMetadataForAllTopics(true);

        final List<String> expectedTopics = Collections.singletonList("topic");
        metadata.setTopics(expectedTopics);

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic", 1);
        partitionCounts.put("topic1", 1);
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith(1, partitionCounts);
        metadata.update(metadataResponse, 100);

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
        metadata.bootstrap(Collections.singletonList(new InetSocketAddress(hostName, 9002)), time);
        assertFalse("ClusterResourceListener should not called when metadata is updated with bootstrap Cluster",
                MockClusterResourceListener.IS_ON_UPDATE_CALLED.get());

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic", 1);
        partitionCounts.put("topic1", 1);
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, partitionCounts);
        metadata.update(metadataResponse, 100);

        assertEquals("MockClusterResourceListener did not get cluster metadata correctly",
                "dummy", mockClusterListener.clusterResource().clusterId());
        assertTrue("MockClusterResourceListener should be called when metadata is updated with non-bootstrap Cluster",
                MockClusterResourceListener.IS_ON_UPDATE_CALLED.get());
    }

    @Test
    public void testListenerGetsNotifiedOfUpdate() {
        long time = 0;
        final Set<String> topics = new HashSet<>();
        metadata.update(emptyMetadataResponse(), time);
        metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
                topics.clear();
                topics.addAll(cluster.topics());
            }
        });

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic", 1);
        partitionCounts.put("topic1", 1);
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, partitionCounts);
        metadata.update(metadataResponse, 100);

        assertEquals("Listener did not update topics list correctly",
            new HashSet<>(Arrays.asList("topic", "topic1")), topics);
    }

    @Test
    public void testListenerCanUnregister() {
        long time = 0;
        final Set<String> topics = new HashSet<>();
        metadata.update(emptyMetadataResponse(), time);
        final Metadata.Listener listener = new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
                topics.clear();
                topics.addAll(cluster.topics());
            }
        };
        metadata.addListener(listener);

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic", 1);
        partitionCounts.put("topic1", 1);
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, partitionCounts);
        metadata.update(metadataResponse, 100);

        metadata.removeListener(listener);

        partitionCounts.clear();
        partitionCounts.put("topic2", 1);
        partitionCounts.put("topic3", 1);
        metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, partitionCounts);
        metadata.update(metadataResponse, 100);

        assertEquals("Listener did not update topics list correctly",
            new HashSet<>(Arrays.asList("topic", "topic1")), topics);
    }

    @Test
    public void testTopicExpiry() throws Exception {
        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true, true, new ClusterResourceListeners());

        // Test that topic is expired if not used within the expiry interval
        long time = 0;
        metadata.add("topic1");
        metadata.update(emptyMetadataResponse(), time);
        time += Metadata.TOPIC_EXPIRY_MS;
        metadata.update(emptyMetadataResponse(), time);
        assertFalse("Unused topic not expired", metadata.containsTopic("topic1"));

        // Test that topic is not expired if used within the expiry interval
        metadata.add("topic2");
        metadata.update(emptyMetadataResponse(), time);
        for (int i = 0; i < 3; i++) {
            time += Metadata.TOPIC_EXPIRY_MS / 2;
            metadata.update(emptyMetadataResponse(), time);
            assertTrue("Topic expired even though in use", metadata.containsTopic("topic2"));
            metadata.add("topic2");
        }

        // Test that topics added using setTopics expire
        HashSet<String> topics = new HashSet<>();
        topics.add("topic4");
        metadata.setTopics(topics);
        metadata.update(emptyMetadataResponse(), time);
        time += Metadata.TOPIC_EXPIRY_MS;
        metadata.update(emptyMetadataResponse(), time);
        assertFalse("Unused topic not expired", metadata.containsTopic("topic4"));
    }

    @Test
    public void testNonExpiringMetadata() throws Exception {
        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true, false, new ClusterResourceListeners());

        // Test that topic is not expired if not used within the expiry interval
        long time = 0;
        metadata.add("topic1");
        metadata.update(emptyMetadataResponse(), time);
        time += Metadata.TOPIC_EXPIRY_MS;
        metadata.update(emptyMetadataResponse(), time);
        assertTrue("Unused topic expired when expiry disabled", metadata.containsTopic("topic1"));

        // Test that topic is not expired if used within the expiry interval
        metadata.add("topic2");
        metadata.update(emptyMetadataResponse(), time);
        for (int i = 0; i < 3; i++) {
            time += Metadata.TOPIC_EXPIRY_MS / 2;
            metadata.update(emptyMetadataResponse(), time);
            assertTrue("Topic expired even though in use", metadata.containsTopic("topic2"));
            metadata.add("topic2");
        }

        // Test that topics added using setTopics don't expire
        HashSet<String> topics = new HashSet<>();
        topics.add("topic4");
        metadata.setTopics(topics);
        time += metadataExpireMs * 2;
        metadata.update(emptyMetadataResponse(), time);
        assertTrue("Unused topic expired when expiry disabled", metadata.containsTopic("topic4"));
    }

    @Test
    public void testRequestUpdate() {
        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, true, false, new ClusterResourceListeners());
        assertFalse(metadata.updateRequested());

        int[] epochs =           {42,   42,    41,    41,    42,    43,   43,    42,    41,    44};
        boolean[] updateResult = {true, false, false, false, false, true, false, false, false, true};
        TopicPartition tp = new TopicPartition("topic", 0);

        for (int i = 0; i < epochs.length; i++) {
            metadata.updateLastSeenEpochIfNewer(tp, epochs[i]);
            if (updateResult[i]) {
                assertTrue("Expected metadata update to be requested [" + i + "]", metadata.updateRequested());
            } else {
                assertFalse("Did not expect metadata update to be requested [" + i + "]", metadata.updateRequested());
            }
            metadata.update(emptyMetadataResponse(), 0L);
            assertFalse(metadata.updateRequested());
        }
    }

    @Test
    public void testRejectOldMetadata() {
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic-1", 1);
        TopicPartition tp = new TopicPartition("topic-1", 0);

        metadata.update(emptyMetadataResponse(), 0L);

        // First epoch seen, accept it
        {
            MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts,
                (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                        new MetadataResponse.PartitionMetadata(error, partition, leader, Optional.of(100), replicas, isr, offlineReplicas));
            metadata.update(metadataResponse, 10L);
            assertNotNull(metadata.fetch().partition(tp));
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }

        // Fake an empty ISR, but with an older epoch, should reject it
        {
            MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts,
                (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                        new MetadataResponse.PartitionMetadata(error, partition, leader, Optional.of(99), replicas, Collections.emptyList(), offlineReplicas));
            metadata.update(metadataResponse, 20L);
            assertEquals(metadata.fetch().partition(tp).inSyncReplicas().length, 1);
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }

        // Fake an empty ISR, with same epoch, accept it
        {
            MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts,
                (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                        new MetadataResponse.PartitionMetadata(error, partition, leader, Optional.of(100), replicas, Collections.emptyList(), offlineReplicas));
            metadata.update(metadataResponse, 20L);
            assertEquals(metadata.fetch().partition(tp).inSyncReplicas().length, 0);
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }

        // Empty metadata response, should not keep old partition but should keep the last-seen epoch
        {
            MetadataResponse metadataResponse = TestUtils.metadataUpdateWith(
                    "dummy", 1, Collections.emptyMap(), Collections.emptyMap(), MetadataResponse.PartitionMetadata::new);
            metadata.update(metadataResponse, 20L);
            assertNull(metadata.fetch().partition(tp));
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }

        // Back in the metadata, with old epoch, should not get added
        {
            MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts,
                (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                        new MetadataResponse.PartitionMetadata(error, partition, leader, Optional.of(99), replicas, isr, offlineReplicas));
            metadata.update(metadataResponse, 10L);
            assertNull(metadata.fetch().partition(tp));
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }
    }

    @Test
    public void testMaybeRequestUpdate() {
        TopicPartition tp = new TopicPartition("topic-1", 0);
        metadata.update(emptyMetadataResponse(), 0L);
        assertTrue(metadata.updateLastSeenEpochIfNewer(tp, 1));
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 1);

        metadata.update(emptyMetadataResponse(), 1L);
        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 1));
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 1);

        metadata.update(emptyMetadataResponse(), 2L);
        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 0));
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 1);

        metadata.update(emptyMetadataResponse(), 3L);
        assertTrue(metadata.updateLastSeenEpochIfNewer(tp, 2));
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 2);
    }

    @Test
    public void testOutOfBandEpochUpdate() {
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic-1", 5);
        TopicPartition tp = new TopicPartition("topic-1", 0);

        metadata.update(emptyMetadataResponse(), 0L);

        assertTrue(metadata.updateLastSeenEpochIfNewer(tp, 99));

        // Update epoch to 100
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts,
            (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                    new MetadataResponse.PartitionMetadata(error, partition, leader, Optional.of(100), replicas, isr, offlineReplicas));
        metadata.update(metadataResponse, 10L);
        assertNotNull(metadata.fetch().partition(tp));
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);

        // Simulate a leader epoch from another response, like a fetch response (not yet implemented)
        assertTrue(metadata.updateLastSeenEpochIfNewer(tp, 101));

        // Cache of partition stays, but current partition info is not available since it's stale
        assertNotNull(metadata.fetch().partition(tp));
        assertEquals(metadata.fetch().partitionCountForTopic("topic-1").longValue(), 5);
        assertFalse(metadata.partitionInfoIfCurrent(tp).isPresent());
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 101);

        // Metadata with older epoch is rejected, metadata state is unchanged
        metadata.update(metadataResponse, 20L);
        assertNotNull(metadata.fetch().partition(tp));
        assertEquals(metadata.fetch().partitionCountForTopic("topic-1").longValue(), 5);
        assertFalse(metadata.partitionInfoIfCurrent(tp).isPresent());
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 101);

        // Metadata with equal or newer epoch is accepted
        metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts,
            (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                    new MetadataResponse.PartitionMetadata(error, partition, leader, Optional.of(101), replicas, isr, offlineReplicas));
        metadata.update(metadataResponse, 30L);
        assertNotNull(metadata.fetch().partition(tp));
        assertEquals(metadata.fetch().partitionCountForTopic("topic-1").longValue(), 5);
        assertTrue(metadata.partitionInfoIfCurrent(tp).isPresent());
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 101);

        // Change topic subscription, remove metadata for old topic
        metadata.setTopics(Collections.singletonList("topic-2"));
        assertNull(metadata.fetch().partition(tp));
        assertNull(metadata.fetch().partitionCountForTopic("topic-1"));
        assertFalse(metadata.partitionInfoIfCurrent(tp).isPresent());
    }

    @Test
    public void testNoEpoch() {
        metadata.update(emptyMetadataResponse(), 0L);
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1),
            (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                new MetadataResponse.PartitionMetadata(error, partition, leader, Optional.empty(), replicas, isr, offlineReplicas));
        metadata.update(metadataResponse, 10L);

        TopicPartition tp = new TopicPartition("topic-1", 0);

        // no epoch
        assertFalse(metadata.lastSeenLeaderEpoch(tp).isPresent());

        // still works
        assertTrue(metadata.partitionInfoIfCurrent(tp).isPresent());
        assertEquals(metadata.partitionInfoIfCurrent(tp).get().partition(), 0);
        assertEquals(metadata.partitionInfoIfCurrent(tp).get().leader().id(), 0);
    }

    @Test
    public void testClusterCopy() {
        Map<String, Integer> counts = new HashMap<>();
        Map<String, Errors> errors = new HashMap<>();
        counts.put("topic1", 2);
        counts.put("topic2", 3);
        counts.put(Topic.GROUP_METADATA_TOPIC_NAME, 3);
        errors.put("topic3", Errors.INVALID_TOPIC_EXCEPTION);
        errors.put("topic4", Errors.TOPIC_AUTHORIZATION_FAILED);

        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 4, errors, counts);
        metadata.update(metadataResponse, 0L);

        Cluster cluster = metadata.fetch();
        assertEquals(cluster.clusterResource().clusterId(), "dummy");
        assertEquals(cluster.nodes().size(), 4);

        // topic counts
        assertEquals(cluster.invalidTopics(), Collections.singleton("topic3"));
        assertEquals(cluster.unauthorizedTopics(), Collections.singleton("topic4"));
        assertEquals(cluster.topics().size(), 3);
        assertEquals(cluster.internalTopics(), Collections.singleton(Topic.GROUP_METADATA_TOPIC_NAME));

        // partition counts
        assertEquals(cluster.partitionsForTopic("topic1").size(), 2);
        assertEquals(cluster.partitionsForTopic("topic2").size(), 3);

        // Sentinel instances
        InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 0);
        Cluster fromMetadata = MetadataCache.bootstrap(Collections.singletonList(address)).cluster();
        Cluster fromCluster = Cluster.bootstrap(Collections.singletonList(address));
        assertEquals(fromMetadata, fromCluster);

        Cluster fromMetadataEmpty = MetadataCache.empty().cluster();
        Cluster fromClusterEmpty = Cluster.empty();
        assertEquals(fromMetadataEmpty, fromClusterEmpty);
    }

    @Test
    public void testRequestVersion() {
        Time time = new MockTime();

        metadata.requestUpdate();
        Metadata.MetadataRequestAndVersion versionAndBuilder = metadata.newMetadataRequestAndVersion();
        metadata.update(versionAndBuilder.requestVersion,
                TestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)), time.milliseconds());
        assertFalse(metadata.updateRequested());

        // bump the request version for new topics added to the metadata
        metadata.requestUpdateForNewTopics();

        // simulating a bump while a metadata request is in flight
        versionAndBuilder = metadata.newMetadataRequestAndVersion();
        metadata.requestUpdateForNewTopics();
        metadata.update(versionAndBuilder.requestVersion,
                TestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)), time.milliseconds());

        // metadata update is still needed
        assertTrue(metadata.updateRequested());

        // the next update will resolve it
        versionAndBuilder = metadata.newMetadataRequestAndVersion();
        metadata.update(versionAndBuilder.requestVersion,
                TestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)), time.milliseconds());
        assertFalse(metadata.updateRequested());
    }

    private void clearBackgroundError() {
        backgroundError.set(null);
    }

    private Thread asyncFetch(final String topic, final long maxWaitMs) {
        Thread thread = new Thread() {
            public void run() {
                try {
                    while (metadata.fetch().partitionsForTopic(topic).isEmpty())
                        metadata.awaitUpdate(metadata.requestUpdate(), maxWaitMs);
                } catch (Exception e) {
                    backgroundError.set(e);
                }
            }
        };
        thread.start();
        return thread;
    }
}
