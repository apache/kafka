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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBrokerCollection;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopicCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.MockClusterResourceListener;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.test.TestUtils.assertOptional;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataTest {

    private long refreshBackoffMs = 100;
    private long metadataExpireMs = 1000;
    private Metadata metadata = new Metadata(refreshBackoffMs, metadataExpireMs, new LogContext(),
            new ClusterResourceListeners());

    private static MetadataResponse emptyMetadataResponse() {
        return RequestTestUtils.metadataResponse(
                Collections.emptyList(),
                null,
                -1,
                Collections.emptyList());
    }

    @Test
    public void testMetadataUpdateAfterClose() {
        metadata.close();
        assertThrows(IllegalStateException.class, () -> metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, 1000));
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
        metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, now);

        // The last update was successful so the remaining time to expire the current metadata should be returned.
        assertEquals(largerOfBackoffAndExpire, metadata.timeToNextUpdate(now));

        // Metadata update requested explicitly
        metadata.requestUpdate();
        // Update requested so metadataExpireMs should no longer take effect.
        assertEquals(refreshBackoffMs, metadata.timeToNextUpdate(now));

        // Reset needUpdate to false.
        metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, now);
        assertEquals(largerOfBackoffAndExpire, metadata.timeToNextUpdate(now));

        // Both metadataExpireMs and refreshBackoffMs elapsed.
        now += largerOfBackoffAndExpire;
        assertEquals(0, metadata.timeToNextUpdate(now));
        assertEquals(0, metadata.timeToNextUpdate(now + 1));
    }

    @Test
    public void testUpdateMetadataAllowedImmediatelyAfterBootstrap() {
        MockTime time = new MockTime();

        Metadata metadata = new Metadata(refreshBackoffMs, metadataExpireMs, new LogContext(),
                new ClusterResourceListeners());
        metadata.bootstrap(Collections.singletonList(new InetSocketAddress("localhost", 9002)));

        assertEquals(0, metadata.timeToAllowUpdate(time.milliseconds()));
        assertEquals(0, metadata.timeToNextUpdate(time.milliseconds()));
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
    public void testTimeToNextUpdateRetryBackoff() {
        long now = 10000;

        // lastRefreshMs updated to now.
        metadata.failedUpdate(now);

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

    /**
     * Prior to Kafka version 2.4 (which coincides with Metadata version 9), the broker does not propagate leader epoch
     * information accurately while a reassignment is in progress, so we cannot rely on it. This is explained in more
     * detail in MetadataResponse's constructor.
     */
    @Test
    public void testIgnoreLeaderEpochInOlderMetadataResponse() {
        TopicPartition tp = new TopicPartition("topic", 0);

        MetadataResponsePartition partitionMetadata = new MetadataResponsePartition()
                .setPartitionIndex(tp.partition())
                .setLeaderId(5)
                .setLeaderEpoch(10)
                .setReplicaNodes(Arrays.asList(1, 2, 3))
                .setIsrNodes(Arrays.asList(1, 2, 3))
                .setOfflineReplicas(Collections.emptyList())
                .setErrorCode(Errors.NONE.code());

        MetadataResponseTopic topicMetadata = new MetadataResponseTopic()
                .setName(tp.topic())
                .setErrorCode(Errors.NONE.code())
                .setPartitions(Collections.singletonList(partitionMetadata))
                .setIsInternal(false);

        MetadataResponseTopicCollection topics = new MetadataResponseTopicCollection();
        topics.add(topicMetadata);

        MetadataResponseData data = new MetadataResponseData()
                .setClusterId("clusterId")
                .setControllerId(0)
                .setTopics(topics)
                .setBrokers(new MetadataResponseBrokerCollection());

        for (short version = ApiKeys.METADATA.oldestVersion(); version < 9; version++) {
            ByteBuffer buffer = MessageUtil.toByteBuffer(data, version);
            MetadataResponse response = MetadataResponse.parse(buffer, version);
            assertFalse(response.hasReliableLeaderEpochs());
            metadata.updateWithCurrentRequestVersion(response, false, 100);
            assertTrue(metadata.partitionMetadataIfCurrent(tp).isPresent());
            MetadataResponse.PartitionMetadata responseMetadata = this.metadata.partitionMetadataIfCurrent(tp).get();
            assertEquals(Optional.empty(), responseMetadata.leaderEpoch);
        }

        for (short version = 9; version <= ApiKeys.METADATA.latestVersion(); version++) {
            ByteBuffer buffer = MessageUtil.toByteBuffer(data, version);
            MetadataResponse response = MetadataResponse.parse(buffer, version);
            assertTrue(response.hasReliableLeaderEpochs());
            metadata.updateWithCurrentRequestVersion(response, false, 100);
            assertTrue(metadata.partitionMetadataIfCurrent(tp).isPresent());
            MetadataResponse.PartitionMetadata responseMetadata = metadata.partitionMetadataIfCurrent(tp).get();
            assertEquals(Optional.of(10), responseMetadata.leaderEpoch);
        }
    }

    @Test
    public void testStaleMetadata() {
        TopicPartition tp = new TopicPartition("topic", 0);

        MetadataResponsePartition partitionMetadata = new MetadataResponsePartition()
                .setPartitionIndex(tp.partition())
                .setLeaderId(1)
                .setLeaderEpoch(10)
                .setReplicaNodes(Arrays.asList(1, 2, 3))
                .setIsrNodes(Arrays.asList(1, 2, 3))
                .setOfflineReplicas(Collections.emptyList())
                .setErrorCode(Errors.NONE.code());

        MetadataResponseTopic topicMetadata = new MetadataResponseTopic()
                .setName(tp.topic())
                .setErrorCode(Errors.NONE.code())
                .setPartitions(Collections.singletonList(partitionMetadata))
                .setIsInternal(false);

        MetadataResponseTopicCollection topics = new MetadataResponseTopicCollection();
        topics.add(topicMetadata);

        MetadataResponseData data = new MetadataResponseData()
                .setClusterId("clusterId")
                .setControllerId(0)
                .setTopics(topics)
                .setBrokers(new MetadataResponseBrokerCollection());

        metadata.updateWithCurrentRequestVersion(new MetadataResponse(data, ApiKeys.METADATA.latestVersion()), false, 100);

        // Older epoch with changed ISR should be ignored
        partitionMetadata
                .setPartitionIndex(tp.partition())
                .setLeaderId(1)
                .setLeaderEpoch(9)
                .setReplicaNodes(Arrays.asList(1, 2, 3))
                .setIsrNodes(Arrays.asList(1, 2))
                .setOfflineReplicas(Collections.emptyList())
                .setErrorCode(Errors.NONE.code());

        metadata.updateWithCurrentRequestVersion(new MetadataResponse(data, ApiKeys.METADATA.latestVersion()), false, 101);
        assertEquals(Optional.of(10), metadata.lastSeenLeaderEpoch(tp));

        assertTrue(metadata.partitionMetadataIfCurrent(tp).isPresent());
        MetadataResponse.PartitionMetadata responseMetadata = this.metadata.partitionMetadataIfCurrent(tp).get();

        assertEquals(Arrays.asList(1, 2, 3), responseMetadata.inSyncReplicaIds);
        assertEquals(Optional.of(10), responseMetadata.leaderEpoch);
    }

    @Test
    public void testFailedUpdate() {
        long time = 100;
        metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, time);

        assertEquals(100, metadata.timeToNextUpdate(1000));
        metadata.failedUpdate(1100);

        assertEquals(100, metadata.timeToNextUpdate(1100));
        assertEquals(100, metadata.lastSuccessfulUpdate());

        metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, time);
        assertEquals(100, metadata.timeToNextUpdate(1000));
    }

    @Test
    public void testClusterListenerGetsNotifiedOfUpdate() {
        MockClusterResourceListener mockClusterListener = new MockClusterResourceListener();
        ClusterResourceListeners listeners = new ClusterResourceListeners();
        listeners.maybeAdd(mockClusterListener);
        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, new LogContext(), listeners);

        String hostName = "www.example.com";
        metadata.bootstrap(Collections.singletonList(new InetSocketAddress(hostName, 9002)));
        assertFalse(MockClusterResourceListener.IS_ON_UPDATE_CALLED.get(),
            "ClusterResourceListener should not called when metadata is updated with bootstrap Cluster");

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic", 1);
        partitionCounts.put("topic1", 1);
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, partitionCounts);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 100);

        assertEquals("dummy", mockClusterListener.clusterResource().clusterId(),
            "MockClusterResourceListener did not get cluster metadata correctly");
        assertTrue(MockClusterResourceListener.IS_ON_UPDATE_CALLED.get(),
            "MockClusterResourceListener should be called when metadata is updated with non-bootstrap Cluster");
    }

    @Test
    public void testRequestUpdate() {
        assertFalse(metadata.updateRequested());

        int[] epochs =           {42,   42,    41,    41,    42,    43,   43,    42,    41,    44};
        boolean[] updateResult = {true, false, false, false, false, true, false, false, false, true};
        TopicPartition tp = new TopicPartition("topic", 0);

        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), Collections.singletonMap("topic", 1), _tp -> 0);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 10L);

        for (int i = 0; i < epochs.length; i++) {
            metadata.updateLastSeenEpochIfNewer(tp, epochs[i]);
            if (updateResult[i]) {
                assertTrue(metadata.updateRequested(), "Expected metadata update to be requested [" + i + "]");
            } else {
                assertFalse(metadata.updateRequested(), "Did not expect metadata update to be requested [" + i + "]");
            }
            metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, 0L);
            assertFalse(metadata.updateRequested());
        }
    }

    @Test
    public void testUpdateLastEpoch() {
        TopicPartition tp = new TopicPartition("topic-1", 0);

        MetadataResponse metadataResponse = emptyMetadataResponse();
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

        // if we have no leader epoch, this call shouldn't do anything
        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 0));
        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 1));
        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 2));
        assertFalse(metadata.lastSeenLeaderEpoch(tp).isPresent());

        // Metadata with newer epoch is handled
        metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1), _tp -> 10);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 1L);
        assertOptional(metadata.lastSeenLeaderEpoch(tp), leaderAndEpoch -> assertEquals(leaderAndEpoch.intValue(), 10));

        // Don't update to an older one
        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 1));
        assertOptional(metadata.lastSeenLeaderEpoch(tp), leaderAndEpoch -> assertEquals(leaderAndEpoch.intValue(), 10));

        // Don't cause update if it's the same one
        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 10));
        assertOptional(metadata.lastSeenLeaderEpoch(tp), leaderAndEpoch -> assertEquals(leaderAndEpoch.intValue(), 10));

        // Update if we see newer epoch
        assertTrue(metadata.updateLastSeenEpochIfNewer(tp, 12));
        assertOptional(metadata.lastSeenLeaderEpoch(tp), leaderAndEpoch -> assertEquals(leaderAndEpoch.intValue(), 12));

        metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1), _tp -> 12);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 2L);
        assertOptional(metadata.lastSeenLeaderEpoch(tp), leaderAndEpoch -> assertEquals(leaderAndEpoch.intValue(), 12));

        // Don't overwrite metadata with older epoch
        metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1), _tp -> 11);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 3L);
        assertOptional(metadata.lastSeenLeaderEpoch(tp), leaderAndEpoch -> assertEquals(leaderAndEpoch.intValue(), 12));
    }

    @Test
    public void testEpochUpdateAfterTopicDeletion() {
        TopicPartition tp = new TopicPartition("topic-1", 0);

        MetadataResponse metadataResponse = emptyMetadataResponse();
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

        // Start with a Topic topic-1 with a random topic ID
        Map<String, Uuid> topicIds = Collections.singletonMap("topic-1", Uuid.randomUuid());
        metadataResponse = RequestTestUtils.metadataUpdateWithIds("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1), _tp -> 10, topicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 1L);
        assertEquals(Optional.of(10), metadata.lastSeenLeaderEpoch(tp));

        // Topic topic-1 is now deleted so Response contains an Error. LeaderEpoch should still maintain Old value
        metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.singletonMap("topic-1", Errors.UNKNOWN_TOPIC_OR_PARTITION), Collections.emptyMap());
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 1L);
        assertEquals(Optional.of(10), metadata.lastSeenLeaderEpoch(tp));

        // Create topic-1 again but this time with a different topic ID. LeaderEpoch should be updated to new even if lower.
        Map<String, Uuid> newTopicIds = Collections.singletonMap("topic-1", Uuid.randomUuid());
        metadataResponse = RequestTestUtils.metadataUpdateWithIds("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1), _tp -> 5, newTopicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 1L);
        assertEquals(Optional.of(5), metadata.lastSeenLeaderEpoch(tp));
    }

    @Test
    public void testEpochUpdateOnChangedTopicIds() {
        TopicPartition tp = new TopicPartition("topic-1", 0);
        Map<String, Uuid> topicIds = Collections.singletonMap("topic-1", Uuid.randomUuid());

        MetadataResponse metadataResponse = emptyMetadataResponse();
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

        // Start with a topic with no topic ID
        metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1), _tp -> 100);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 1L);
        assertEquals(Optional.of(100), metadata.lastSeenLeaderEpoch(tp));

        // If the older topic ID is null, we should go with the new topic ID as the leader epoch
        metadataResponse = RequestTestUtils.metadataUpdateWithIds("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1), _tp -> 10, topicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 2L);
        assertEquals(Optional.of(10), metadata.lastSeenLeaderEpoch(tp));

        // Don't cause update if it's the same one
        metadataResponse = RequestTestUtils.metadataUpdateWithIds("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1), _tp -> 10, topicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 3L);
        assertEquals(Optional.of(10), metadata.lastSeenLeaderEpoch(tp));

        // Update if we see newer epoch
        metadataResponse = RequestTestUtils.metadataUpdateWithIds("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1), _tp -> 12, topicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 4L);
        assertEquals(Optional.of(12), metadata.lastSeenLeaderEpoch(tp));

        // We should also update if we see a new topicId even if the epoch is lower
        Map<String, Uuid> newTopicIds = Collections.singletonMap("topic-1", Uuid.randomUuid());
        metadataResponse = RequestTestUtils.metadataUpdateWithIds("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1), _tp -> 3, newTopicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 5L);
        assertEquals(Optional.of(3), metadata.lastSeenLeaderEpoch(tp));

        // Finally, update when the topic ID is new and the epoch is higher
        Map<String, Uuid> newTopicIds2 = Collections.singletonMap("topic-1", Uuid.randomUuid());
        metadataResponse = RequestTestUtils.metadataUpdateWithIds("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1), _tp -> 20, newTopicIds2);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 6L);
        assertEquals(Optional.of(20), metadata.lastSeenLeaderEpoch(tp));
    }

    @Test
    public void testRejectOldMetadata() {
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic-1", 1);
        TopicPartition tp = new TopicPartition("topic-1", 0);

        metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, 0L);

        // First epoch seen, accept it
        {
            MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, _tp -> 100);
            metadata.updateWithCurrentRequestVersion(metadataResponse, false, 10L);
            assertNotNull(metadata.fetch().partition(tp));
            assertTrue(metadata.lastSeenLeaderEpoch(tp).isPresent());
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }

        // Fake an empty ISR, but with an older epoch, should reject it
        {
            MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, _tp -> 99,
                (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                        new MetadataResponse.PartitionMetadata(error, partition, leader,
                            leaderEpoch, replicas, Collections.emptyList(), offlineReplicas), ApiKeys.METADATA.latestVersion(), Collections.emptyMap());
            metadata.updateWithCurrentRequestVersion(metadataResponse, false, 20L);
            assertEquals(metadata.fetch().partition(tp).inSyncReplicas().length, 1);
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }

        // Fake an empty ISR, with same epoch, accept it
        {
            MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, _tp -> 100,
                (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                        new MetadataResponse.PartitionMetadata(error, partition, leader,
                            leaderEpoch, replicas, Collections.emptyList(), offlineReplicas), ApiKeys.METADATA.latestVersion(), Collections.emptyMap());
            metadata.updateWithCurrentRequestVersion(metadataResponse, false, 20L);
            assertEquals(metadata.fetch().partition(tp).inSyncReplicas().length, 0);
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }

        // Empty metadata response, should not keep old partition but should keep the last-seen epoch
        {
            MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), Collections.emptyMap());
            metadata.updateWithCurrentRequestVersion(metadataResponse, false, 20L);
            assertNull(metadata.fetch().partition(tp));
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }

        // Back in the metadata, with old epoch, should not get added
        {
            MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, _tp -> 99);
            metadata.updateWithCurrentRequestVersion(metadataResponse, false, 10L);
            assertNull(metadata.fetch().partition(tp));
            assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);
        }
    }

    @Test
    public void testOutOfBandEpochUpdate() {
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic-1", 5);
        TopicPartition tp = new TopicPartition("topic-1", 0);

        metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, 0L);

        assertFalse(metadata.updateLastSeenEpochIfNewer(tp, 99));

        // Update epoch to 100
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, _tp -> 100);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 10L);
        assertNotNull(metadata.fetch().partition(tp));
        assertTrue(metadata.lastSeenLeaderEpoch(tp).isPresent());
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 100);

        // Simulate a leader epoch from another response, like a fetch response or list offsets
        assertTrue(metadata.updateLastSeenEpochIfNewer(tp, 101));

        // Cache of partition stays, but current partition info is not available since it's stale
        assertNotNull(metadata.fetch().partition(tp));
        assertEquals(Objects.requireNonNull(metadata.fetch().partitionCountForTopic("topic-1")).longValue(), 5);
        assertFalse(metadata.partitionMetadataIfCurrent(tp).isPresent());
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 101);

        // Metadata with older epoch is rejected, metadata state is unchanged
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 20L);
        assertNotNull(metadata.fetch().partition(tp));
        assertEquals(Objects.requireNonNull(metadata.fetch().partitionCountForTopic("topic-1")).longValue(), 5);
        assertFalse(metadata.partitionMetadataIfCurrent(tp).isPresent());
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 101);

        // Metadata with equal or newer epoch is accepted
        metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, _tp -> 101);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 30L);
        assertNotNull(metadata.fetch().partition(tp));
        assertEquals(Objects.requireNonNull(metadata.fetch().partitionCountForTopic("topic-1")).longValue(), 5);
        assertTrue(metadata.partitionMetadataIfCurrent(tp).isPresent());
        assertEquals(metadata.lastSeenLeaderEpoch(tp).get().longValue(), 101);
    }

    @Test
    public void testNoEpoch() {
        metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, 0L);
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), Collections.singletonMap("topic-1", 1));
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 10L);

        TopicPartition tp = new TopicPartition("topic-1", 0);

        // no epoch
        assertFalse(metadata.lastSeenLeaderEpoch(tp).isPresent());

        // still works
        assertTrue(metadata.partitionMetadataIfCurrent(tp).isPresent());
        assertEquals(0, metadata.partitionMetadataIfCurrent(tp).get().partition());
        assertEquals(Optional.of(0), metadata.partitionMetadataIfCurrent(tp).get().leaderId);

        // Since epoch was null, this shouldn't update it
        metadata.updateLastSeenEpochIfNewer(tp, 10);
        assertTrue(metadata.partitionMetadataIfCurrent(tp).isPresent());
        assertFalse(metadata.partitionMetadataIfCurrent(tp).get().leaderEpoch.isPresent());
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

        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 4, errors, counts);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

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
        Metadata.MetadataRequestAndVersion versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds());
        metadata.update(versionAndBuilder.requestVersion,
                RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)), false, time.milliseconds());
        assertFalse(metadata.updateRequested());

        // bump the request version for new topics added to the metadata
        metadata.requestUpdateForNewTopics();

        // simulating a bump while a metadata request is in flight
        versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds());
        metadata.requestUpdateForNewTopics();
        metadata.update(versionAndBuilder.requestVersion,
                RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)), true, time.milliseconds());

        // metadata update is still needed
        assertTrue(metadata.updateRequested());

        // the next update will resolve it
        versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds());
        metadata.update(versionAndBuilder.requestVersion,
                RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)), true, time.milliseconds());
        assertFalse(metadata.updateRequested());
    }

    @Test
    public void testPartialMetadataUpdate() {
        Time time = new MockTime();

        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, new LogContext(), new ClusterResourceListeners()) {
                @Override
                protected MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
                    return newMetadataRequestBuilder();
                }
            };

        assertFalse(metadata.updateRequested());

        // Request a metadata update. This must force a full metadata update request.
        metadata.requestUpdate();
        Metadata.MetadataRequestAndVersion versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds());
        assertFalse(versionAndBuilder.isPartialUpdate);
        metadata.update(versionAndBuilder.requestVersion,
                RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)), false, time.milliseconds());
        assertFalse(metadata.updateRequested());

        // Request a metadata update for a new topic. This should perform a partial metadata update.
        metadata.requestUpdateForNewTopics();
        versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds());
        assertTrue(versionAndBuilder.isPartialUpdate);
        metadata.update(versionAndBuilder.requestVersion,
                RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)), true, time.milliseconds());
        assertFalse(metadata.updateRequested());

        // Request both types of metadata updates. This should always perform a full update.
        metadata.requestUpdate();
        metadata.requestUpdateForNewTopics();
        versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds());
        assertFalse(versionAndBuilder.isPartialUpdate);
        metadata.update(versionAndBuilder.requestVersion,
                RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)), false, time.milliseconds());
        assertFalse(metadata.updateRequested());

        // Request only a partial metadata update, but elapse enough time such that a full refresh is needed.
        metadata.requestUpdateForNewTopics();
        final long refreshTimeMs = time.milliseconds() + metadata.metadataExpireMs();
        versionAndBuilder = metadata.newMetadataRequestAndVersion(refreshTimeMs);
        assertFalse(versionAndBuilder.isPartialUpdate);
        metadata.update(versionAndBuilder.requestVersion,
                RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)), true, refreshTimeMs);
        assertFalse(metadata.updateRequested());

        // Request two partial metadata updates that are overlapping.
        metadata.requestUpdateForNewTopics();
        versionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds());
        assertTrue(versionAndBuilder.isPartialUpdate);
        metadata.requestUpdateForNewTopics();
        Metadata.MetadataRequestAndVersion overlappingVersionAndBuilder = metadata.newMetadataRequestAndVersion(time.milliseconds());
        assertTrue(overlappingVersionAndBuilder.isPartialUpdate);
        assertTrue(metadata.updateRequested());
        metadata.update(versionAndBuilder.requestVersion,
                RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic-1", 1)), true, time.milliseconds());
        assertTrue(metadata.updateRequested());
        metadata.update(overlappingVersionAndBuilder.requestVersion,
                RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic-2", 1)), true, time.milliseconds());
        assertFalse(metadata.updateRequested());
    }

    @Test
    public void testInvalidTopicError() {
        Time time = new MockTime();

        String invalidTopic = "topic dfsa";
        MetadataResponse invalidTopicResponse = RequestTestUtils.metadataUpdateWith("clusterId", 1,
                Collections.singletonMap(invalidTopic, Errors.INVALID_TOPIC_EXCEPTION), Collections.emptyMap());
        metadata.updateWithCurrentRequestVersion(invalidTopicResponse, false, time.milliseconds());

        InvalidTopicException e = assertThrows(InvalidTopicException.class, () -> metadata.maybeThrowAnyException());

        assertEquals(Collections.singleton(invalidTopic), e.invalidTopics());
        // We clear the exception once it has been raised to the user
        metadata.maybeThrowAnyException();

        // Reset the invalid topic error
        metadata.updateWithCurrentRequestVersion(invalidTopicResponse, false, time.milliseconds());

        // If we get a good update, the error should clear even if we haven't had a chance to raise it to the user
        metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, time.milliseconds());
        metadata.maybeThrowAnyException();
    }

    @Test
    public void testTopicAuthorizationError() {
        Time time = new MockTime();

        String invalidTopic = "foo";
        MetadataResponse unauthorizedTopicResponse = RequestTestUtils.metadataUpdateWith("clusterId", 1,
                Collections.singletonMap(invalidTopic, Errors.TOPIC_AUTHORIZATION_FAILED), Collections.emptyMap());
        metadata.updateWithCurrentRequestVersion(unauthorizedTopicResponse, false, time.milliseconds());

        TopicAuthorizationException e = assertThrows(TopicAuthorizationException.class, () -> metadata.maybeThrowAnyException());
        assertEquals(Collections.singleton(invalidTopic), e.unauthorizedTopics());
        // We clear the exception once it has been raised to the user
        metadata.maybeThrowAnyException();

        // Reset the unauthorized topic error
        metadata.updateWithCurrentRequestVersion(unauthorizedTopicResponse, false, time.milliseconds());

        // If we get a good update, the error should clear even if we haven't had a chance to raise it to the user
        metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, time.milliseconds());
        metadata.maybeThrowAnyException();
    }

    @Test
    public void testMetadataTopicErrors() {
        Time time = new MockTime();

        Map<String, Errors> topicErrors = new HashMap<>(3);
        topicErrors.put("invalidTopic", Errors.INVALID_TOPIC_EXCEPTION);
        topicErrors.put("sensitiveTopic1", Errors.TOPIC_AUTHORIZATION_FAILED);
        topicErrors.put("sensitiveTopic2", Errors.TOPIC_AUTHORIZATION_FAILED);
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("clusterId", 1, topicErrors, Collections.emptyMap());

        metadata.updateWithCurrentRequestVersion(metadataResponse, false, time.milliseconds());
        TopicAuthorizationException e1 = assertThrows(TopicAuthorizationException.class,
            () -> metadata.maybeThrowExceptionForTopic("sensitiveTopic1"));
        assertEquals(Collections.singleton("sensitiveTopic1"), e1.unauthorizedTopics());
        // We clear the exception once it has been raised to the user
        metadata.maybeThrowAnyException();

        metadata.updateWithCurrentRequestVersion(metadataResponse, false, time.milliseconds());
        TopicAuthorizationException e2 = assertThrows(TopicAuthorizationException.class,
            () -> metadata.maybeThrowExceptionForTopic("sensitiveTopic2"));
        assertEquals(Collections.singleton("sensitiveTopic2"), e2.unauthorizedTopics());
        metadata.maybeThrowAnyException();

        metadata.updateWithCurrentRequestVersion(metadataResponse, false, time.milliseconds());
        InvalidTopicException e3 = assertThrows(InvalidTopicException.class,
            () -> metadata.maybeThrowExceptionForTopic("invalidTopic"));
        assertEquals(Collections.singleton("invalidTopic"), e3.invalidTopics());
        metadata.maybeThrowAnyException();

        // Other topics should not throw exception, but they should clear existing exception
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, time.milliseconds());
        metadata.maybeThrowExceptionForTopic("anotherTopic");
        metadata.maybeThrowAnyException();
    }

    @Test
    public void testNodeIfOffline() {
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic-1", 1);
        Node node0 = new Node(0, "localhost", 9092);
        Node node1 = new Node(1, "localhost", 9093);

        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 2, Collections.emptyMap(), partitionCounts, _tp -> 99,
            (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                new MetadataResponse.PartitionMetadata(error, partition, Optional.of(node0.id()), leaderEpoch,
                    Collections.singletonList(node0.id()), Collections.emptyList(),
                        Collections.singletonList(node1.id())), ApiKeys.METADATA.latestVersion(), Collections.emptyMap());
        metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, 0L);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 10L);

        TopicPartition tp = new TopicPartition("topic-1", 0);

        assertOptional(metadata.fetch().nodeIfOnline(tp, 0), node -> assertEquals(node.id(), 0));
        assertFalse(metadata.fetch().nodeIfOnline(tp, 1).isPresent());
        assertEquals(metadata.fetch().nodeById(0).id(), 0);
        assertEquals(metadata.fetch().nodeById(1).id(), 1);
    }

    @Test
    public void testNodeIfOnlineWhenNotInReplicaSet() {
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put("topic-1", 1);
        Node node0 = new Node(0, "localhost", 9092);

        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 2, Collections.emptyMap(), partitionCounts, _tp -> 99,
            (error, partition, leader, leaderEpoch, replicas, isr, offlineReplicas) ->
                new MetadataResponse.PartitionMetadata(error, partition, Optional.of(node0.id()), leaderEpoch,
                    Collections.singletonList(node0.id()), Collections.emptyList(),
                        Collections.emptyList()), ApiKeys.METADATA.latestVersion(), Collections.emptyMap());
        metadata.updateWithCurrentRequestVersion(emptyMetadataResponse(), false, 0L);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 10L);

        TopicPartition tp = new TopicPartition("topic-1", 0);

        assertEquals(1, metadata.fetch().nodeById(1).id());
        assertFalse(metadata.fetch().nodeIfOnline(tp, 1).isPresent());
    }

    @Test
    public void testNodeIfOnlineNonExistentTopicPartition() {
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(2, Collections.emptyMap());
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

        TopicPartition tp = new TopicPartition("topic-1", 0);

        assertEquals(metadata.fetch().nodeById(0).id(), 0);
        assertNull(metadata.fetch().partition(tp));
        assertEquals(metadata.fetch().nodeIfOnline(tp, 0), Optional.empty());
    }

    @Test
    public void testLeaderMetadataInconsistentWithBrokerMetadata() {
        // Tests a reordering scenario which can lead to inconsistent leader state.
        // A partition initially has one broker offline. That broker comes online and
        // is elected leader. The client sees these two events in the opposite order.

        TopicPartition tp = new TopicPartition("topic", 0);

        Node node0 = new Node(0, "localhost", 9092);
        Node node1 = new Node(1, "localhost", 9093);
        Node node2 = new Node(2, "localhost", 9094);

        // The first metadata received by broker (epoch=10)
        MetadataResponsePartition firstPartitionMetadata = new MetadataResponsePartition()
                .setPartitionIndex(tp.partition())
                .setErrorCode(Errors.NONE.code())
                .setLeaderEpoch(10)
                .setLeaderId(0)
                .setReplicaNodes(Arrays.asList(0, 1, 2))
                .setIsrNodes(Arrays.asList(0, 1, 2))
                .setOfflineReplicas(Collections.emptyList());

        // The second metadata received has stale metadata (epoch=8)
        MetadataResponsePartition secondPartitionMetadata = new MetadataResponsePartition()
                .setPartitionIndex(tp.partition())
                .setErrorCode(Errors.NONE.code())
                .setLeaderEpoch(8)
                .setLeaderId(1)
                .setReplicaNodes(Arrays.asList(0, 1, 2))
                .setIsrNodes(Arrays.asList(1, 2))
                .setOfflineReplicas(Collections.singletonList(0));

        metadata.updateWithCurrentRequestVersion(new MetadataResponse(new MetadataResponseData()
                        .setTopics(buildTopicCollection(tp.topic(), firstPartitionMetadata))
                        .setBrokers(buildBrokerCollection(Arrays.asList(node0, node1, node2))),
                        ApiKeys.METADATA.latestVersion()),
                false, 10L);

        metadata.updateWithCurrentRequestVersion(new MetadataResponse(new MetadataResponseData()
                        .setTopics(buildTopicCollection(tp.topic(), secondPartitionMetadata))
                        .setBrokers(buildBrokerCollection(Arrays.asList(node1, node2))),
                        ApiKeys.METADATA.latestVersion()),
                false, 20L);

        assertNull(metadata.fetch().leaderFor(tp));
        assertEquals(Optional.of(10), metadata.lastSeenLeaderEpoch(tp));
        assertFalse(metadata.currentLeader(tp).leader.isPresent());
    }

    private MetadataResponseTopicCollection buildTopicCollection(String topic, MetadataResponsePartition partitionMetadata) {
        MetadataResponseTopic topicMetadata = new MetadataResponseTopic()
                .setErrorCode(Errors.NONE.code())
                .setName(topic)
                .setIsInternal(false);

        topicMetadata.setPartitions(Collections.singletonList(partitionMetadata));

        MetadataResponseTopicCollection topics = new MetadataResponseTopicCollection();
        topics.add(topicMetadata);
        return topics;
    }

    private MetadataResponseBrokerCollection buildBrokerCollection(List<Node> nodes) {
        MetadataResponseBrokerCollection brokers = new MetadataResponseBrokerCollection();
        for (Node node : nodes) {
            MetadataResponseData.MetadataResponseBroker broker = new MetadataResponseData.MetadataResponseBroker()
                    .setNodeId(node.id())
                    .setHost(node.host())
                    .setPort(node.port())
                    .setRack(node.rack());
            brokers.add(broker);
        }
        return brokers;
    }

    @Test
    public void testMetadataMerge() {
        Time time = new MockTime();
        Map<String, Uuid> topicIds = new HashMap<>();

        final AtomicReference<Set<String>> retainTopics = new AtomicReference<>(new HashSet<>());
        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, new LogContext(), new ClusterResourceListeners()) {
                @Override
                protected boolean retainTopic(String topic, boolean isInternal, long nowMs) {
                    return retainTopics.get().contains(topic);
                }
            };

        // Initialize a metadata instance with two topic variants "old" and "keep". Both will be retained.
        String oldClusterId = "oldClusterId";
        int oldNodes = 2;
        Map<String, Errors> oldTopicErrors = new HashMap<>();
        oldTopicErrors.put("oldInvalidTopic", Errors.INVALID_TOPIC_EXCEPTION);
        oldTopicErrors.put("keepInvalidTopic", Errors.INVALID_TOPIC_EXCEPTION);
        oldTopicErrors.put("oldUnauthorizedTopic", Errors.TOPIC_AUTHORIZATION_FAILED);
        oldTopicErrors.put("keepUnauthorizedTopic", Errors.TOPIC_AUTHORIZATION_FAILED);
        Map<String, Integer> oldTopicPartitionCounts = new HashMap<>();
        oldTopicPartitionCounts.put("oldValidTopic", 2);
        oldTopicPartitionCounts.put("keepValidTopic", 3);

        retainTopics.set(Utils.mkSet(
            "oldInvalidTopic",
            "keepInvalidTopic",
            "oldUnauthorizedTopic",
            "keepUnauthorizedTopic",
            "oldValidTopic",
            "keepValidTopic"));

        topicIds.put("oldValidTopic", Uuid.randomUuid());
        topicIds.put("keepValidTopic", Uuid.randomUuid());
        MetadataResponse metadataResponse =
                RequestTestUtils.metadataUpdateWithIds(oldClusterId, oldNodes, oldTopicErrors, oldTopicPartitionCounts, _tp -> 100, topicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, true, time.milliseconds());
        Map<String, Uuid> metadataTopicIds1 = metadata.topicIds();
        retainTopics.get().forEach(topic -> assertEquals(metadataTopicIds1.get(topic), topicIds.get(topic)));

        // Update the metadata to add a new topic variant, "new", which will be retained with "keep". Note this
        // means that all of the "old" topics should be dropped.
        Cluster cluster = metadata.fetch();
        assertEquals(cluster.clusterResource().clusterId(), oldClusterId);
        assertEquals(cluster.nodes().size(), oldNodes);
        assertEquals(cluster.invalidTopics(), new HashSet<>(Arrays.asList("oldInvalidTopic", "keepInvalidTopic")));
        assertEquals(cluster.unauthorizedTopics(), new HashSet<>(Arrays.asList("oldUnauthorizedTopic", "keepUnauthorizedTopic")));
        assertEquals(cluster.topics(), new HashSet<>(Arrays.asList("oldValidTopic", "keepValidTopic")));
        assertEquals(cluster.partitionsForTopic("oldValidTopic").size(), 2);
        assertEquals(cluster.partitionsForTopic("keepValidTopic").size(), 3);
        assertEquals(new HashSet<>(cluster.topicIds()), new HashSet<>(topicIds.values()));

        String newClusterId = "newClusterId";
        int newNodes = oldNodes + 1;
        Map<String, Errors> newTopicErrors = new HashMap<>();
        newTopicErrors.put("newInvalidTopic", Errors.INVALID_TOPIC_EXCEPTION);
        newTopicErrors.put("newUnauthorizedTopic", Errors.TOPIC_AUTHORIZATION_FAILED);
        Map<String, Integer> newTopicPartitionCounts = new HashMap<>();
        newTopicPartitionCounts.put("keepValidTopic", 2);
        newTopicPartitionCounts.put("newValidTopic", 4);

        retainTopics.set(Utils.mkSet(
            "keepInvalidTopic",
            "newInvalidTopic",
            "keepUnauthorizedTopic",
            "newUnauthorizedTopic",
            "keepValidTopic",
            "newValidTopic"));

        topicIds.put("newValidTopic", Uuid.randomUuid());
        metadataResponse = RequestTestUtils.metadataUpdateWithIds(newClusterId, newNodes, newTopicErrors, newTopicPartitionCounts, _tp -> 200, topicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, true, time.milliseconds());
        topicIds.remove("oldValidTopic");
        Map<String, Uuid> metadataTopicIds2 = metadata.topicIds();
        retainTopics.get().forEach(topic -> assertEquals(metadataTopicIds2.get(topic), topicIds.get(topic)));
        assertNull(metadataTopicIds2.get("oldValidTopic"));

        cluster = metadata.fetch();
        assertEquals(cluster.clusterResource().clusterId(), newClusterId);
        assertEquals(cluster.nodes().size(), newNodes);
        assertEquals(cluster.invalidTopics(), new HashSet<>(Arrays.asList("keepInvalidTopic", "newInvalidTopic")));
        assertEquals(cluster.unauthorizedTopics(), new HashSet<>(Arrays.asList("keepUnauthorizedTopic", "newUnauthorizedTopic")));
        assertEquals(cluster.topics(), new HashSet<>(Arrays.asList("keepValidTopic", "newValidTopic")));
        assertEquals(cluster.partitionsForTopic("keepValidTopic").size(), 2);
        assertEquals(cluster.partitionsForTopic("newValidTopic").size(), 4);
        assertEquals(new HashSet<>(cluster.topicIds()), new HashSet<>(topicIds.values()));

        // Perform another metadata update, but this time all topic metadata should be cleared.
        retainTopics.set(Collections.emptySet());

        metadataResponse = RequestTestUtils.metadataUpdateWithIds(newClusterId, newNodes, newTopicErrors, newTopicPartitionCounts, _tp -> 300, topicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, true, time.milliseconds());
        Map<String, Uuid> metadataTopicIds3 = metadata.topicIds();
        topicIds.forEach((topicName, topicId) -> assertNull(metadataTopicIds3.get(topicName)));

        cluster = metadata.fetch();
        assertEquals(cluster.clusterResource().clusterId(), newClusterId);
        assertEquals(cluster.nodes().size(), newNodes);
        assertEquals(cluster.invalidTopics(), Collections.emptySet());
        assertEquals(cluster.unauthorizedTopics(), Collections.emptySet());
        assertEquals(cluster.topics(), Collections.emptySet());
        assertTrue(cluster.topicIds().isEmpty());
    }

    @Test
    public void testMetadataMergeOnIdDowngrade() {
        Time time = new MockTime();
        Map<String, Uuid> topicIds = new HashMap<>();

        final AtomicReference<Set<String>> retainTopics = new AtomicReference<>(new HashSet<>());
        metadata = new Metadata(refreshBackoffMs, metadataExpireMs, new LogContext(), new ClusterResourceListeners()) {
            @Override
            protected boolean retainTopic(String topic, boolean isInternal, long nowMs) {
                return retainTopics.get().contains(topic);
            }
        };

        // Initialize a metadata instance with two topics. Both will be retained.
        String clusterId = "clusterId";
        int nodes = 2;
        Map<String, Integer> topicPartitionCounts = new HashMap<>();
        topicPartitionCounts.put("validTopic1", 2);
        topicPartitionCounts.put("validTopic2", 3);

        retainTopics.set(Utils.mkSet(
                "validTopic1",
                "validTopic2"));

        topicIds.put("validTopic1", Uuid.randomUuid());
        topicIds.put("validTopic2", Uuid.randomUuid());
        MetadataResponse metadataResponse =
                RequestTestUtils.metadataUpdateWithIds(clusterId, nodes, Collections.emptyMap(), topicPartitionCounts, _tp -> 100, topicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, true, time.milliseconds());
        Map<String, Uuid> metadataTopicIds1 = metadata.topicIds();
        retainTopics.get().forEach(topic -> assertEquals(metadataTopicIds1.get(topic), topicIds.get(topic)));

        // Try removing the topic ID from keepValidTopic (simulating receiving a request from a controller with an older IBP)
        topicIds.remove("validTopic1");
        metadataResponse = RequestTestUtils.metadataUpdateWithIds(clusterId, nodes, Collections.emptyMap(), topicPartitionCounts, _tp -> 200, topicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, true, time.milliseconds());
        Map<String, Uuid> metadataTopicIds2 = metadata.topicIds();
        retainTopics.get().forEach(topic -> assertEquals(metadataTopicIds2.get(topic), topicIds.get(topic)));

        Cluster cluster = metadata.fetch();
        // We still have the topic, but it just doesn't have an ID.
        assertEquals(Utils.mkSet("validTopic1", "validTopic2"), cluster.topics());
        assertEquals(2, cluster.partitionsForTopic("validTopic1").size());
        assertEquals(new HashSet<>(topicIds.values()), new HashSet<>(cluster.topicIds()));
        assertEquals(Uuid.ZERO_UUID, cluster.topicId("validTopic1"));
    }

}
