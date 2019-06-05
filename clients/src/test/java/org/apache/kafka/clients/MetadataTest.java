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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.MockClusterResourceListener;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.test.TestUtils.assertOptional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class MetadataTest {

    private long refreshBackoffMs = 100;
    private long metadataExpireMs = 1000;
    private Metadata metadata = new Metadata(refreshBackoffMs, metadataExpireMs, new LogContext(),
            new ClusterResourceListeners());

    private static MetadataResponse emptyMetadataResponse() {
        return MetadataResponse.prepareResponse(
                Collections.emptyList(),
                null,
                -1,
                Collections.emptyList());
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
        Metadata metadata = new Metadata(refreshBackoffMs, metadataExpireMs, new LogContext(),
                new ClusterResourceListeners());

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
    public void testFailedUpdate() {
        long time = 100;
        metadata.update(emptyMetadataResponse(), time);

        assertEquals(100, metadata.timeToNextUpdate(1000));
        metadata.failedUpdate(1100, null);

        assertEquals(100, metadata.timeToNextUpdate(1100));
        assertEquals(100, metadata.lastSuccessfulUpdate());

        metadata.update(emptyMetadataResponse(), time);
        assertEquals(100, metadata.timeToNextUpdate(1000));
    }

    @Test
    public void testClusterListenerGetsNotifiedOfUpdate() {
        long time = 0;
        MockClusterResourceListener mockClusterListener = new MockClusterResourceListener();
        ClusterResourceListeners listeners = new ClusterResourceListeners();
        listeners.maybeAdd(mockClusterListener);
        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, new LogContext(), listeners);

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
    public void testRequestUpdate() {
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
            MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, _tp -> 100);
            metadata.update(metadataResponse, 10L);
            assertNotNull(metadata.fetch().partition(tp));
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }

        // Fake an empty ISR, but with an older epoch, should reject it
        {
            MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, _tp -> 99,
                (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                        new MetadataResponse.PartitionMetadata(error, partition, leader, leaderEpoch, replicas, Collections.emptyList(), offlineReplicas));
            metadata.update(metadataResponse, 20L);
            assertEquals(metadata.fetch().partition(tp).inSyncReplicas().length, 1);
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }

        // Fake an empty ISR, with same epoch, accept it
        {
            MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, _tp -> 100,
                (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                        new MetadataResponse.PartitionMetadata(error, partition, leader, leaderEpoch, replicas, Collections.emptyList(), offlineReplicas));
            metadata.update(metadataResponse, 20L);
            assertEquals(metadata.fetch().partition(tp).inSyncReplicas().length, 0);
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }

        // Empty metadata response, should not keep old partition but should keep the last-seen epoch
        {
            MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), Collections.emptyMap());
            metadata.update(metadataResponse, 20L);
            assertNull(metadata.fetch().partition(tp));
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }

        // Back in the metadata, with old epoch, should not get added
        {
            MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, _tp -> 99);
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
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, _tp -> 100);
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
        metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, _tp -> 101);
        metadata.update(metadataResponse, 30L);
        assertNotNull(metadata.fetch().partition(tp));
        assertEquals(metadata.fetch().partitionCountForTopic("topic-1").longValue(), 5);
        assertTrue(metadata.partitionInfoIfCurrent(tp).isPresent());
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 101);
    }

    @Test
    public void testNoEpoch() {
        metadata.update(emptyMetadataResponse(), 0L);
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1));
        metadata.update(metadataResponse, 10L);

        TopicPartition tp = new TopicPartition("topic-1", 0);

        // no epoch
        assertFalse(metadata.lastSeenLeaderEpoch(tp).isPresent());

        // still works
        assertTrue(metadata.partitionInfoIfCurrent(tp).isPresent());
        assertEquals(metadata.partitionInfoIfCurrent(tp).get().partitionInfo().partition(), 0);
        assertEquals(metadata.partitionInfoIfCurrent(tp).get().partitionInfo().leader().id(), 0);
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

    @Test
    public void testInvalidTopicError() {
        Time time = new MockTime();

        String invalidTopic = "topic dfsa";
        MetadataResponse invalidTopicResponse = TestUtils.metadataUpdateWith("clusterId", 1,
                Collections.singletonMap(invalidTopic, Errors.INVALID_TOPIC_EXCEPTION), Collections.emptyMap());
        metadata.update(invalidTopicResponse, time.milliseconds());

        InvalidTopicException e = assertThrows(InvalidTopicException.class, () -> metadata.maybeThrowException());

        assertEquals(Collections.singleton(invalidTopic), e.invalidTopics());
        // We clear the exception once it has been raised to the user
        assertNull(metadata.getAndClearMetadataException());

        // Reset the invalid topic error
        metadata.update(invalidTopicResponse, time.milliseconds());

        // If we get a good update, the error should clear even if we haven't had a chance to raise it to the user
        metadata.update(emptyMetadataResponse(), time.milliseconds());
        assertNull(metadata.getAndClearMetadataException());
    }

    @Test
    public void testTopicAuthorizationError() {
        Time time = new MockTime();

        String invalidTopic = "foo";
        MetadataResponse unauthorizedTopicResponse = TestUtils.metadataUpdateWith("clusterId", 1,
                Collections.singletonMap(invalidTopic, Errors.TOPIC_AUTHORIZATION_FAILED), Collections.emptyMap());
        metadata.update(unauthorizedTopicResponse, time.milliseconds());

        TopicAuthorizationException e = assertThrows(TopicAuthorizationException.class, () -> metadata.maybeThrowException());
        assertEquals(Collections.singleton(invalidTopic), e.unauthorizedTopics());
        // We clear the exception once it has been raised to the user
        assertNull(metadata.getAndClearMetadataException());

        // Reset the unauthorized topic error
        metadata.update(unauthorizedTopicResponse, time.milliseconds());

        // If we get a good update, the error should clear even if we haven't had a chance to raise it to the user
        metadata.update(emptyMetadataResponse(), time.milliseconds());
        assertNull(metadata.getAndClearMetadataException());
    }

    @Test
    public void testNodeIfOffline() {
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic-1", 1);
        Node node0 = new Node(0, "localhost", 9092);
        Node node1 = new Node(1, "localhost", 9093);

        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("dummy", 2, Collections.emptyMap(), partitionCounts, _tp -> 99,
            (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                new MetadataResponse.PartitionMetadata(error, partition, node0, leaderEpoch,
                    Collections.singletonList(node0), Collections.emptyList(), Collections.singletonList(node1)));
        metadata.update(emptyMetadataResponse(), 0L);
        metadata.update(metadataResponse, 10L);

        TopicPartition tp = new TopicPartition("topic-1", 0);

        assertOptional(metadata.fetch().nodeIfOnline(tp, 0), node -> assertEquals(node.id(), 0));
        assertFalse(metadata.fetch().nodeIfOnline(tp, 1).isPresent());
        assertEquals(metadata.fetch().nodeById(0).id(), 0);
        assertEquals(metadata.fetch().nodeById(1).id(), 1);
    }
}
