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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.test.TestUtils.assertOptional;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class OffsetFetcherTest {

    private final String topicName = "test";
    private final Uuid topicId = Uuid.randomUuid();
    private final Map<String, Uuid> topicIds = new HashMap<String, Uuid>() {
        {
            put(topicName, topicId);
        }
    };
    private final TopicPartition tp0 = new TopicPartition(topicName, 0);
    private final TopicPartition tp1 = new TopicPartition(topicName, 1);
    private final TopicPartition tp2 = new TopicPartition(topicName, 2);
    private final TopicPartition tp3 = new TopicPartition(topicName, 3);
    private final int validLeaderEpoch = 0;
    private final MetadataResponse initialUpdateResponse =
        RequestTestUtils.metadataUpdateWithIds(1, singletonMap(topicName, 4), topicIds);

    private final long retryBackoffMs = 100;
    private MockTime time = new MockTime(1);
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private MockClient client;
    private Metrics metrics;
    private final ApiVersions apiVersions = new ApiVersions();
    private ConsumerNetworkClient consumerClient;
    private OffsetFetcher offsetFetcher;

    @BeforeEach
    public void setup() {
    }

    private void assignFromUser(Set<TopicPartition> partitions) {
        subscriptions.assignFromUser(partitions);
        client.updateMetadata(initialUpdateResponse);

        // A dummy metadata update to ensure valid leader epoch.
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
            Collections.emptyMap(), singletonMap(topicName, 4),
            tp -> validLeaderEpoch, topicIds), false, 0L);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (metrics != null)
            this.metrics.close();
    }

    @Test
    public void testUpdateFetchPositionNoOpWithPositionSet() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 5L);

        offsetFetcher.resetPositionsIfNeeded();
        assertFalse(client.hasInFlightRequests());
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testUpdateFetchPositionResetToDefaultOffset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.EARLIEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.NONE, 1L, 5L));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testUpdateFetchPositionResetToLatestOffset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.updateMetadata(initialUpdateResponse);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    /**
     * Make sure the client behaves appropriately when receiving an exception for unavailable offsets
     */
    @Test
    public void testFetchOffsetErrors() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Fail with OFFSET_NOT_AVAILABLE
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.OFFSET_NOT_AVAILABLE, 1L, 5L), false);
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0));

        // Fail with LEADER_NOT_AVAILABLE
        time.sleep(retryBackoffMs);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.LEADER_NOT_AVAILABLE, 1L, 5L), false);
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0));

        // Back to normal
        time.sleep(retryBackoffMs);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L), false);
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertTrue(subscriptions.hasValidPosition(tp0));
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(subscriptions.position(tp0).offset, 5L);
    }

    @Test
    public void testListOffsetSendsReadUncommitted() {
        testListOffsetsSendsIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
    }

    @Test
    public void testListOffsetSendsReadCommitted() {
        testListOffsetsSendsIsolationLevel(IsolationLevel.READ_COMMITTED);
    }

    private void testListOffsetsSendsIsolationLevel(IsolationLevel isolationLevel) {
        buildFetcher(isolationLevel);

        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.prepareResponse(body -> {
            ListOffsetsRequest request = (ListOffsetsRequest) body;
            return request.isolationLevel() == isolationLevel;
        }, listOffsetResponse(Errors.NONE, 1L, 5L));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testresetPositionsSkipsBlackedOutConnections() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST);

        // Check that we skip sending the ListOffset request when the node is blacked out
        client.updateMetadata(initialUpdateResponse);
        Node node = initialUpdateResponse.brokers().iterator().next();
        client.backoff(node, 500);
        offsetFetcher.resetPositionsIfNeeded();
        assertEquals(0, consumerClient.pendingRequestCount());
        consumerClient.pollNoWakeup();
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(OffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(tp0));

        time.sleep(500);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.EARLIEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testUpdateFetchPositionResetToEarliestOffset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.EARLIEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.NONE, 1L, 5L));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testresetPositionsMetadataRefresh() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // First fetch fails with stale metadata
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.NOT_LEADER_OR_FOLLOWER, 1L, 5L), false);
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));

        // Expect a metadata refresh
        client.prepareMetadataUpdate(initialUpdateResponse);
        consumerClient.pollNoWakeup();
        assertFalse(client.hasPendingMetadataUpdates());

        // Next fetch succeeds
        time.sleep(retryBackoffMs);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testListOffsetNoUpdateMissingEpoch() {
        buildFetcher();

        // Set up metadata with no leader epoch
        subscriptions.assignFromUser(singleton(tp0));
        MetadataResponse metadataWithNoLeaderEpochs = RequestTestUtils.metadataUpdateWithIds(
                "kafka-cluster", 1, Collections.emptyMap(), singletonMap(topicName, 4), tp -> null, topicIds);
        client.updateMetadata(metadataWithNoLeaderEpochs);

        // Return a ListOffsets response with leaderEpoch=1, we should ignore it
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(tp0, Errors.NONE, 1L, 5L, 1));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();

        // Reset should be satisfied and no metadata update requested
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(metadata.updateRequested());
        assertFalse(metadata.lastSeenLeaderEpoch(tp0).isPresent());
    }

    @Test
    public void testListOffsetUpdateEpoch() {
        buildFetcher();

        // Set up metadata with leaderEpoch=1
        subscriptions.assignFromUser(singleton(tp0));
        MetadataResponse metadataWithLeaderEpochs = RequestTestUtils.metadataUpdateWithIds(
                "kafka-cluster", 1, Collections.emptyMap(), singletonMap(topicName, 4), tp -> 1, topicIds);
        client.updateMetadata(metadataWithLeaderEpochs);

        // Reset offsets to trigger ListOffsets call
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Now we see a ListOffsets with leaderEpoch=2 epoch, we trigger a metadata update
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP, 1),
                listOffsetResponse(tp0, Errors.NONE, 1L, 5L, 2));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(metadata.updateRequested());
        assertOptional(metadata.lastSeenLeaderEpoch(tp0), epoch -> assertEquals((long) epoch, 2));
    }

    @Test
    public void testUpdateFetchPositionDisconnect() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // First request gets a disconnect
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.NONE, 1L, 5L), true);
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));

        // Expect a metadata refresh
        client.prepareMetadataUpdate(initialUpdateResponse);
        consumerClient.pollNoWakeup();
        assertFalse(client.hasPendingMetadataUpdates());

        // No retry until the backoff passes
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(client.hasInFlightRequests());
        assertFalse(subscriptions.hasValidPosition(tp0));

        // Next one succeeds
        time.sleep(retryBackoffMs);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testAssignmentChangeWithInFlightReset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Send the ListOffsets request to reset the position
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(client.hasInFlightRequests());

        // Now we have an assignment change
        assignFromUser(singleton(tp1));

        // The response returns and is discarded
        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L));
        consumerClient.pollNoWakeup();

        assertFalse(client.hasPendingResponses());
        assertFalse(client.hasInFlightRequests());
        assertFalse(subscriptions.isAssigned(tp0));
    }

    @Test
    public void testSeekWithInFlightReset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Send the ListOffsets request to reset the position
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(client.hasInFlightRequests());

        // Now we get a seek from the user
        subscriptions.seek(tp0, 237);

        // The response returns and is discarded
        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L));
        consumerClient.pollNoWakeup();

        assertFalse(client.hasPendingResponses());
        assertFalse(client.hasInFlightRequests());
        assertEquals(237L, subscriptions.position(tp0).offset);
    }

    private boolean listOffsetMatchesExpectedReset(
        TopicPartition tp,
        OffsetResetStrategy strategy,
        AbstractRequest request
    ) {
        assertInstanceOf(ListOffsetsRequest.class, request);

        ListOffsetsRequest req = (ListOffsetsRequest) request;
        assertEquals(singleton(tp.topic()), req.data().topics().stream()
            .map(ListOffsetsTopic::name).collect(Collectors.toSet()));

        ListOffsetsTopic listTopic = req.data().topics().get(0);
        assertEquals(singleton(tp.partition()), listTopic.partitions().stream()
            .map(ListOffsetsPartition::partitionIndex).collect(Collectors.toSet()));

        ListOffsetsPartition listPartition = listTopic.partitions().get(0);
        if (strategy == OffsetResetStrategy.EARLIEST) {
            assertEquals(ListOffsetsRequest.EARLIEST_TIMESTAMP, listPartition.timestamp());
        } else if (strategy == OffsetResetStrategy.LATEST) {
            assertEquals(ListOffsetsRequest.LATEST_TIMESTAMP, listPartition.timestamp());
        }
        return true;
    }

    @Test
    public void testEarlierOffsetResetArrivesLate() {
        buildFetcher();
        assignFromUser(singleton(tp0));

        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST);
        offsetFetcher.resetPositionsIfNeeded();

        client.prepareResponse(req -> {
            if (listOffsetMatchesExpectedReset(tp0, OffsetResetStrategy.EARLIEST, req)) {
                // Before the response is handled, we get a request to reset to the latest offset
                subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);
                return true;
            } else {
                return false;
            }
        }, listOffsetResponse(Errors.NONE, 1L, 0L));
        consumerClient.pollNoWakeup();

        // The list offset result should be ignored
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(OffsetResetStrategy.LATEST, subscriptions.resetStrategy(tp0));

        offsetFetcher.resetPositionsIfNeeded();
        client.prepareResponse(
            req -> listOffsetMatchesExpectedReset(tp0, OffsetResetStrategy.LATEST, req),
            listOffsetResponse(Errors.NONE, 1L, 10L)
        );
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(10, subscriptions.position(tp0).offset);
    }

    @Test
    public void testChangeResetWithInFlightReset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Send the ListOffsets request to reset the position
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(client.hasInFlightRequests());

        // Now we get a seek from the user
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.EARLIEST);

        // The response returns and is discarded
        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L));
        consumerClient.pollNoWakeup();

        assertFalse(client.hasPendingResponses());
        assertFalse(client.hasInFlightRequests());
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(OffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(tp0));
    }

    @Test
    public void testIdempotentResetWithInFlightReset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // Send the ListOffsets request to reset the position
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertTrue(client.hasInFlightRequests());

        // Now we get a seek from the user
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.respond(listOffsetResponse(Errors.NONE, 1L, 5L));
        consumerClient.pollNoWakeup();

        assertFalse(client.hasInFlightRequests());
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(5L, subscriptions.position(tp0).offset);
    }

    @Test
    public void testResetOffsetsAuthorizationFailure() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        // First request gets a disconnect
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.TOPIC_AUTHORIZATION_FAILED, -1, -1), false);
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.hasValidPosition(tp0));

        try {
            offsetFetcher.resetPositionsIfNeeded();
            fail("Expected authorization error to be raised");
        } catch (TopicAuthorizationException e) {
            assertEquals(singleton(tp0.topic()), e.unauthorizedTopics());
        }

        // The exception should clear after being raised, but no retry until the backoff
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();
        assertFalse(client.hasInFlightRequests());
        assertFalse(subscriptions.hasValidPosition(tp0));

        // Next one succeeds
        time.sleep(retryBackoffMs);
        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(Errors.NONE, 1L, 5L));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(5, subscriptions.position(tp0).offset);
    }

    @Test
    public void testFetchingPendingPartitionsBeforeAndAfterSubscriptionReset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 100);
        assertEquals(100, subscriptions.position(tp0).offset);

        assertTrue(subscriptions.isFetchable(tp0));

        subscriptions.markPendingRevocation(singleton(tp0));
        offsetFetcher.resetPositionsIfNeeded();

        // once a partition is marked pending, it should not be fetchable
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0));
        assertTrue(subscriptions.hasValidPosition(tp0));
        assertEquals(100, subscriptions.position(tp0).offset);

        subscriptions.seek(tp0, 100);
        assertEquals(100, subscriptions.position(tp0).offset);

        // reassignment should enable fetching of the same partition
        subscriptions.unsubscribe();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 100);
        assertEquals(100, subscriptions.position(tp0).offset);
        assertTrue(subscriptions.isFetchable(tp0));
    }

    @Test
    public void testUpdateFetchPositionOfPausedPartitionsRequiringOffsetReset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.pause(tp0); // paused partition does not have a valid position
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP,
            validLeaderEpoch), listOffsetResponse(Errors.NONE, 1L, 10L));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0)); // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp0));
        assertEquals(10, subscriptions.position(tp0).offset);
    }

    @Test
    public void testUpdateFetchPositionOfPausedPartitionsWithoutAValidPosition() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0);
        subscriptions.pause(tp0); // paused partition does not have a valid position

        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0)); // because tp is paused
        assertFalse(subscriptions.hasValidPosition(tp0));
    }

    @Test
    public void testUpdateFetchPositionOfPausedPartitionsWithAValidPosition() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 10);
        subscriptions.pause(tp0); // paused partition already has a valid position

        offsetFetcher.resetPositionsIfNeeded();

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0)); // because tp is paused
        assertTrue(subscriptions.hasValidPosition(tp0));
        assertEquals(10, subscriptions.position(tp0).offset);
    }

    @Test
    public void testGetOffsetsForTimesTimeout() {
        buildFetcher();
        assertThrows(TimeoutException.class, () -> offsetFetcher.offsetsForTimes(
            Collections.singletonMap(new TopicPartition(topicName, 2), 1000L), time.timer(100L)));
    }

    @Test
    public void testGetOffsetsForTimes() {
        buildFetcher();

        // Empty map
        assertTrue(offsetFetcher.offsetsForTimes(new HashMap<>(), time.timer(100L)).isEmpty());
        // Unknown Offset
        testGetOffsetsForTimesWithUnknownOffset();
        // Error code none with unknown offset
        testGetOffsetsForTimesWithError(Errors.NONE, Errors.NONE, -1L, null);
        // Error code none with known offset
        testGetOffsetsForTimesWithError(Errors.NONE, Errors.NONE, 10L, 10L);
        // Test both of partition has error.
        testGetOffsetsForTimesWithError(Errors.NOT_LEADER_OR_FOLLOWER, Errors.INVALID_REQUEST, 10L, 10L);
        // Test the second partition has error.
        testGetOffsetsForTimesWithError(Errors.NONE, Errors.NOT_LEADER_OR_FOLLOWER, 10L, 10L);
        // Test different errors.
        testGetOffsetsForTimesWithError(Errors.NOT_LEADER_OR_FOLLOWER, Errors.NONE, 10L, 10L);
        testGetOffsetsForTimesWithError(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.NONE, 10L, 10L);
        testGetOffsetsForTimesWithError(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, Errors.NONE, 10L, null);
        testGetOffsetsForTimesWithError(Errors.BROKER_NOT_AVAILABLE, Errors.NONE, 10L, 10L);
    }

    @Test
    public void testGetOffsetsFencedLeaderEpoch() {
        buildFetcher();
        subscriptions.assignFromUser(singleton(tp0));
        client.updateMetadata(initialUpdateResponse);

        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.prepareResponse(listOffsetResponse(Errors.FENCED_LEADER_EPOCH, 1L, 5L));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0));
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @Test
    public void testGetOffsetByTimeWithPartitionsRetryCouldTriggerMetadataUpdate() {
        List<Errors> retriableErrors = Arrays.asList(Errors.NOT_LEADER_OR_FOLLOWER,
            Errors.REPLICA_NOT_AVAILABLE, Errors.KAFKA_STORAGE_ERROR, Errors.OFFSET_NOT_AVAILABLE,
            Errors.LEADER_NOT_AVAILABLE, Errors.FENCED_LEADER_EPOCH, Errors.UNKNOWN_LEADER_EPOCH);

        final int newLeaderEpoch = 3;
        MetadataResponse updatedMetadata = RequestTestUtils.metadataUpdateWithIds("dummy", 3,
            singletonMap(topicName, Errors.NONE), singletonMap(topicName, 4), tp -> newLeaderEpoch, topicIds);

        Node originalLeader = initialUpdateResponse.buildCluster().leaderFor(tp1);
        Node newLeader = updatedMetadata.buildCluster().leaderFor(tp1);
        assertNotEquals(originalLeader, newLeader);

        for (Errors retriableError : retriableErrors) {
            buildFetcher();

            subscriptions.assignFromUser(mkSet(tp0, tp1));
            client.updateMetadata(initialUpdateResponse);

            final long fetchTimestamp = 10L;
            ListOffsetsPartitionResponse tp0NoError = new ListOffsetsPartitionResponse()
                .setPartitionIndex(tp0.partition())
                .setErrorCode(Errors.NONE.code())
                .setTimestamp(fetchTimestamp)
                .setOffset(4L);
            List<ListOffsetsTopicResponse> topics = Collections.singletonList(
                    new ListOffsetsTopicResponse()
                        .setName(tp0.topic())
                        .setPartitions(Arrays.asList(
                                tp0NoError,
                                new ListOffsetsPartitionResponse()
                                    .setPartitionIndex(tp1.partition())
                                    .setErrorCode(retriableError.code())
                                    .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
                                    .setOffset(-1L))));
            ListOffsetsResponseData data = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(topics);

            client.prepareResponseFrom(body -> {
                boolean isListOffsetRequest = body instanceof ListOffsetsRequest;
                if (isListOffsetRequest) {
                    ListOffsetsRequest request = (ListOffsetsRequest) body;
                    List<ListOffsetsTopic> expectedTopics = Collections.singletonList(
                            new ListOffsetsTopic()
                                .setName(tp0.topic())
                                .setPartitions(Arrays.asList(
                                        new ListOffsetsPartition()
                                            .setPartitionIndex(tp1.partition())
                                            .setTimestamp(fetchTimestamp)
                                            .setCurrentLeaderEpoch(ListOffsetsResponse.UNKNOWN_EPOCH),
                                        new ListOffsetsPartition()
                                            .setPartitionIndex(tp0.partition())
                                            .setTimestamp(fetchTimestamp)
                                            .setCurrentLeaderEpoch(ListOffsetsResponse.UNKNOWN_EPOCH))));
                    return request.topics().equals(expectedTopics);
                } else {
                    return false;
                }
            }, new ListOffsetsResponse(data), originalLeader);

            client.prepareMetadataUpdate(updatedMetadata);

            // If the metadata wasn't updated before retrying, the fetcher would consult the original leader and hit a NOT_LEADER exception.
            // We will count the answered future response in the end to verify if this is the case.
            List<ListOffsetsTopicResponse> topicsWithFatalError = Collections.singletonList(
                    new ListOffsetsTopicResponse()
                        .setName(tp0.topic())
                        .setPartitions(Arrays.asList(
                                tp0NoError,
                                new ListOffsetsPartitionResponse()
                                    .setPartitionIndex(tp1.partition())
                                    .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                                    .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
                                    .setOffset(-1L))));
            ListOffsetsResponseData dataWithFatalError = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(topicsWithFatalError);
            client.prepareResponseFrom(new ListOffsetsResponse(dataWithFatalError), originalLeader);

            // The request to new leader must only contain one partition tp1 with error.
            client.prepareResponseFrom(body -> {
                boolean isListOffsetRequest = body instanceof ListOffsetsRequest;
                if (isListOffsetRequest) {
                    ListOffsetsRequest request = (ListOffsetsRequest) body;

                    ListOffsetsTopic requestTopic = request.topics().get(0);
                    ListOffsetsPartition expectedPartition = new ListOffsetsPartition()
                            .setPartitionIndex(tp1.partition())
                            .setTimestamp(fetchTimestamp)
                            .setCurrentLeaderEpoch(newLeaderEpoch);
                    return expectedPartition.equals(requestTopic.partitions().get(0));
                } else {
                    return false;
                }
            }, listOffsetResponse(tp1, Errors.NONE, fetchTimestamp, 5L), newLeader);

            Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
                offsetFetcher.offsetsForTimes(
                    Utils.mkMap(Utils.mkEntry(tp0, fetchTimestamp),
                    Utils.mkEntry(tp1, fetchTimestamp)), time.timer(Integer.MAX_VALUE));

            assertEquals(Utils.mkMap(
                Utils.mkEntry(tp0, new OffsetAndTimestamp(4L, fetchTimestamp)),
                Utils.mkEntry(tp1, new OffsetAndTimestamp(5L, fetchTimestamp))), offsetAndTimestampMap);

            // The NOT_LEADER exception future should not be cleared as we already refreshed the metadata before
            // first retry, thus never hitting.
            assertEquals(1, client.numAwaitingResponses());
        }
    }

    @Test
    public void testGetOffsetsUnknownLeaderEpoch() {
        buildFetcher();
        subscriptions.assignFromUser(singleton(tp0));
        subscriptions.requestOffsetReset(tp0, OffsetResetStrategy.LATEST);

        client.prepareResponse(listOffsetResponse(Errors.UNKNOWN_LEADER_EPOCH, 1L, 5L));
        offsetFetcher.resetPositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertFalse(subscriptions.isFetchable(tp0));
        assertFalse(subscriptions.hasValidPosition(tp0));
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @Test
    public void testGetOffsetsIncludesLeaderEpoch() {
        buildFetcher();
        subscriptions.assignFromUser(singleton(tp0));

        client.updateMetadata(initialUpdateResponse);

        // Metadata update with leader epochs
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                Collections.emptyMap(), Collections.singletonMap(topicName, 4), tp -> 99, topicIds);
        client.updateMetadata(metadataResponse);

        // Request latest offset
        subscriptions.requestOffsetReset(tp0);
        offsetFetcher.resetPositionsIfNeeded();

        // Check for epoch in outgoing request
        MockClient.RequestMatcher matcher = body -> {
            if (body instanceof ListOffsetsRequest) {
                ListOffsetsRequest offsetRequest = (ListOffsetsRequest) body;
                int epoch = offsetRequest.topics().get(0).partitions().get(0).currentLeaderEpoch();
                assertTrue(epoch != ListOffsetsResponse.UNKNOWN_EPOCH, "Expected Fetcher to set leader epoch in request");
                assertEquals(epoch, 99, "Expected leader epoch to match epoch from metadata update");
                return true;
            } else {
                fail("Should have seen ListOffsetRequest");
                return false;
            }
        };

        client.prepareResponse(matcher, listOffsetResponse(Errors.NONE, 1L, 5L));
        consumerClient.pollNoWakeup();
    }

    @Test
    public void testGetOffsetsForTimesWhenSomeTopicPartitionLeadersNotKnownInitially() {
        buildFetcher();

        subscriptions.assignFromUser(mkSet(tp0, tp1));
        final String anotherTopic = "another-topic";
        final TopicPartition t2p0 = new TopicPartition(anotherTopic, 0);

        client.reset();

        // Metadata initially has one topic
        MetadataResponse initialMetadata = RequestTestUtils.metadataUpdateWithIds(3, singletonMap(topicName, 2), topicIds);
        client.updateMetadata(initialMetadata);

        // The first metadata refresh should contain one topic
        client.prepareMetadataUpdate(initialMetadata);
        client.prepareResponseFrom(listOffsetResponse(tp0, Errors.NONE, 1000L, 11L),
                metadata.fetch().leaderFor(tp0));
        client.prepareResponseFrom(listOffsetResponse(tp1, Errors.NONE, 1000L, 32L),
                metadata.fetch().leaderFor(tp1));

        // Second metadata refresh should contain two topics
        Map<String, Integer> partitionNumByTopic = new HashMap<>();
        partitionNumByTopic.put(topicName, 2);
        partitionNumByTopic.put(anotherTopic, 1);
        topicIds.put("another-topic", Uuid.randomUuid());
        MetadataResponse updatedMetadata = RequestTestUtils.metadataUpdateWithIds(3, partitionNumByTopic, topicIds);
        client.prepareMetadataUpdate(updatedMetadata);
        client.prepareResponseFrom(listOffsetResponse(t2p0, Errors.NONE, 1000L, 54L),
                metadata.fetch().leaderFor(t2p0));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(tp0, ListOffsetsRequest.LATEST_TIMESTAMP);
        timestampToSearch.put(tp1, ListOffsetsRequest.LATEST_TIMESTAMP);
        timestampToSearch.put(t2p0, ListOffsetsRequest.LATEST_TIMESTAMP);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
            offsetFetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE));

        assertNotNull(offsetAndTimestampMap.get(tp0), "Expect MetadataFetcher.offsetsForTimes() to return non-null result for " + tp0);
        assertNotNull(offsetAndTimestampMap.get(tp1), "Expect MetadataFetcher.offsetsForTimes() to return non-null result for " + tp1);
        assertNotNull(offsetAndTimestampMap.get(t2p0), "Expect MetadataFetcher.offsetsForTimes() to return non-null result for " + t2p0);
        assertEquals(11L, offsetAndTimestampMap.get(tp0).offset());
        assertEquals(32L, offsetAndTimestampMap.get(tp1).offset());
        assertEquals(54L, offsetAndTimestampMap.get(t2p0).offset());
    }

    @Test
    public void testGetOffsetsForTimesWhenSomeTopicPartitionLeadersDisconnectException() {
        buildFetcher();
        final String anotherTopic = "another-topic";
        final TopicPartition t2p0 = new TopicPartition(anotherTopic, 0);
        subscriptions.assignFromUser(mkSet(tp0, t2p0));

        client.reset();

        MetadataResponse initialMetadata = RequestTestUtils.metadataUpdateWithIds(1, singletonMap(topicName, 1), topicIds);
        client.updateMetadata(initialMetadata);

        Map<String, Integer> partitionNumByTopic = new HashMap<>();
        partitionNumByTopic.put(topicName, 1);
        partitionNumByTopic.put(anotherTopic, 1);
        topicIds.put("another-topic", Uuid.randomUuid());
        MetadataResponse updatedMetadata = RequestTestUtils.metadataUpdateWithIds(1, partitionNumByTopic, topicIds);
        client.prepareMetadataUpdate(updatedMetadata);

        client.prepareResponse(listOffsetRequestMatcher(ListOffsetsRequest.LATEST_TIMESTAMP),
                listOffsetResponse(tp0, Errors.NONE, 1000L, 11L), true);
        client.prepareResponseFrom(listOffsetResponse(tp0, Errors.NONE, 1000L, 11L), metadata.fetch().leaderFor(tp0));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(tp0, ListOffsetsRequest.LATEST_TIMESTAMP);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = offsetFetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE));

        assertNotNull(offsetAndTimestampMap.get(tp0), "Expect MetadataFetcher.offsetsForTimes() to return non-null result for " + tp0);
        assertEquals(11L, offsetAndTimestampMap.get(tp0).offset());
        assertNotNull(metadata.fetch().partitionCountForTopic(anotherTopic));
    }

    @Test
    public void testListOffsetsWithZeroTimeout() {
        buildFetcher();

        Map<TopicPartition, Long> offsetsToSearch = new HashMap<>();
        offsetsToSearch.put(tp0, ListOffsetsRequest.EARLIEST_TIMESTAMP);
        offsetsToSearch.put(tp1, ListOffsetsRequest.EARLIEST_TIMESTAMP);

        Map<TopicPartition, OffsetAndTimestamp> offsetsToExpect = new HashMap<>();
        offsetsToExpect.put(tp0, null);
        offsetsToExpect.put(tp1, null);

        assertEquals(offsetsToExpect, offsetFetcher.offsetsForTimes(offsetsToSearch, time.timer(0)));
    }

    @Test
    public void testBatchedListOffsetsMetadataErrors() {
        buildFetcher();

        ListOffsetsResponseData data = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(Collections.singletonList(new ListOffsetsTopicResponse()
                        .setName(tp0.topic())
                        .setPartitions(Arrays.asList(
                                new ListOffsetsPartitionResponse()
                                    .setPartitionIndex(tp0.partition())
                                    .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                                    .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                                    .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET),
                                new ListOffsetsPartitionResponse()
                                    .setPartitionIndex(tp1.partition())
                                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                                    .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                                    .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)))));
        client.prepareResponse(new ListOffsetsResponse(data));

        Map<TopicPartition, Long> offsetsToSearch = new HashMap<>();
        offsetsToSearch.put(tp0, ListOffsetsRequest.EARLIEST_TIMESTAMP);
        offsetsToSearch.put(tp1, ListOffsetsRequest.EARLIEST_TIMESTAMP);

        assertThrows(TimeoutException.class, () -> offsetFetcher.offsetsForTimes(offsetsToSearch, time.timer(1)));
    }

    private void testGetOffsetsForTimesWithError(Errors errorForP0,
                                                 Errors errorForP1,
                                                 long offsetForP0,
                                                 Long expectedOffsetForP0) {
        long offsetForP1 = 100L;
        long expectedOffsetForP1 = 100L;
        client.reset();
        String topicName2 = "topic2";
        TopicPartition t2p0 = new TopicPartition(topicName2, 0);
        // Expect a metadata refresh.
        metadata.bootstrap(ClientUtils.parseAndValidateAddresses(Collections.singletonList("1.1.1.1:1111"),
                ClientDnsLookup.USE_ALL_DNS_IPS));

        Map<String, Integer> partitionNumByTopic = new HashMap<>();
        partitionNumByTopic.put(topicName, 2);
        partitionNumByTopic.put(topicName2, 1);
        MetadataResponse updateMetadataResponse = RequestTestUtils.metadataUpdateWithIds(2, partitionNumByTopic, topicIds);
        Cluster updatedCluster = updateMetadataResponse.buildCluster();

        // The metadata refresh should contain all the topics.
        client.prepareMetadataUpdate(updateMetadataResponse, true);

        // First try should fail due to metadata error.
        client.prepareResponseFrom(listOffsetResponse(t2p0, errorForP0, offsetForP0, offsetForP0),
                updatedCluster.leaderFor(t2p0));
        client.prepareResponseFrom(listOffsetResponse(tp1, errorForP1, offsetForP1, offsetForP1),
                updatedCluster.leaderFor(tp1));
        // Second try should succeed.
        client.prepareResponseFrom(listOffsetResponse(t2p0, Errors.NONE, offsetForP0, offsetForP0),
                updatedCluster.leaderFor(t2p0));
        client.prepareResponseFrom(listOffsetResponse(tp1, Errors.NONE, offsetForP1, offsetForP1),
                updatedCluster.leaderFor(tp1));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(t2p0, 0L);
        timestampToSearch.put(tp1, 0L);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
                offsetFetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE));

        if (expectedOffsetForP0 == null)
            assertNull(offsetAndTimestampMap.get(t2p0));
        else {
            assertEquals(expectedOffsetForP0.longValue(), offsetAndTimestampMap.get(t2p0).timestamp());
            assertEquals(expectedOffsetForP0.longValue(), offsetAndTimestampMap.get(t2p0).offset());
        }

        assertEquals(expectedOffsetForP1, offsetAndTimestampMap.get(tp1).timestamp());
        assertEquals(expectedOffsetForP1, offsetAndTimestampMap.get(tp1).offset());
    }

    private void testGetOffsetsForTimesWithUnknownOffset() {
        client.reset();
        // Ensure metadata has both partitions.
        MetadataResponse initialMetadataUpdate = RequestTestUtils.metadataUpdateWithIds(1, singletonMap(topicName, 1), topicIds);
        client.updateMetadata(initialMetadataUpdate);

        ListOffsetsResponseData data = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(Collections.singletonList(new ListOffsetsTopicResponse()
                        .setName(tp0.topic())
                        .setPartitions(Collections.singletonList(new ListOffsetsPartitionResponse()
                                .setPartitionIndex(tp0.partition())
                                .setErrorCode(Errors.NONE.code())
                                .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                                .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)))));

        client.prepareResponseFrom(new ListOffsetsResponse(data),
                metadata.fetch().leaderFor(tp0));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(tp0, 0L);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
                offsetFetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE));

        assertTrue(offsetAndTimestampMap.containsKey(tp0));
        assertNull(offsetAndTimestampMap.get(tp0));
    }

    @Test
    public void testGetOffsetsForTimesWithUnknownOffsetV0() {
        buildFetcher();
        // Empty map
        assertTrue(offsetFetcher.offsetsForTimes(new HashMap<>(), time.timer(100L)).isEmpty());
        // Unknown Offset
        client.reset();
        // Ensure metadata has both partition.
        MetadataResponse initialMetadataUpdate = RequestTestUtils.metadataUpdateWithIds(1, singletonMap(topicName, 1), topicIds);
        client.updateMetadata(initialMetadataUpdate);
        // Force LIST_OFFSETS version 0
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(), NodeApiVersions.create(
            ApiKeys.LIST_OFFSETS.id, (short) 0, (short) 0));

        ListOffsetsResponseData data = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(Collections.singletonList(new ListOffsetsTopicResponse()
                        .setName(tp0.topic())
                        .setPartitions(Collections.singletonList(new ListOffsetsPartitionResponse()
                                .setPartitionIndex(tp0.partition())
                                .setErrorCode(Errors.NONE.code())
                                .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                                .setOldStyleOffsets(Collections.emptyList())))));

        client.prepareResponseFrom(new ListOffsetsResponse(data),
                metadata.fetch().leaderFor(tp0));

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(tp0, 0L);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
                offsetFetcher.offsetsForTimes(timestampToSearch, time.timer(Long.MAX_VALUE));

        assertTrue(offsetAndTimestampMap.containsKey(tp0));
        assertNull(offsetAndTimestampMap.get(tp0));
    }

    @Test
    public void testOffsetValidationRequestGrouping() {
        buildFetcher();
        assignFromUser(mkSet(tp0, tp1, tp2, tp3));

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 3,
            Collections.emptyMap(), singletonMap(topicName, 4),
            tp -> 5, topicIds), false, 0L);

        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(
                metadata.currentLeader(tp).leader, Optional.of(4));
            subscriptions.seekUnvalidated(tp,
                new SubscriptionState.FetchPosition(0, Optional.of(4), leaderAndEpoch));
        }

        Set<TopicPartition> allRequestedPartitions = new HashSet<>();

        for (Node node : metadata.fetch().nodes()) {
            apiVersions.update(node.idString(), NodeApiVersions.create());

            Set<TopicPartition> expectedPartitions = subscriptions.assignedPartitions().stream()
                .filter(tp ->
                    metadata.currentLeader(tp).leader.equals(Optional.of(node)))
                .collect(Collectors.toSet());

            assertTrue(expectedPartitions.stream().noneMatch(allRequestedPartitions::contains));
            assertFalse(expectedPartitions.isEmpty());
            allRequestedPartitions.addAll(expectedPartitions);

            OffsetForLeaderEpochResponseData data = new OffsetForLeaderEpochResponseData();
            expectedPartitions.forEach(tp -> {
                OffsetForLeaderTopicResult topic = data.topics().find(tp.topic());
                if (topic == null) {
                    topic = new OffsetForLeaderTopicResult().setTopic(tp.topic());
                    data.topics().add(topic);
                }
                topic.partitions().add(new EpochEndOffset()
                    .setPartition(tp.partition())
                    .setErrorCode(Errors.NONE.code())
                    .setLeaderEpoch(4)
                    .setEndOffset(0));
            });

            OffsetsForLeaderEpochResponse response = new OffsetsForLeaderEpochResponse(data);
            client.prepareResponseFrom(body -> {
                OffsetsForLeaderEpochRequest request = (OffsetsForLeaderEpochRequest) body;
                return expectedPartitions.equals(offsetForLeaderPartitionMap(request.data()).keySet());
            }, response, node);
        }

        assertEquals(subscriptions.assignedPartitions(), allRequestedPartitions);

        offsetFetcher.validatePositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertTrue(subscriptions.assignedPartitions()
            .stream().noneMatch(subscriptions::awaitingValidation));
    }

    @Test
    public void testOffsetValidationAwaitsNodeApiVersion() {
        buildFetcher();
        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        final int epochOne = 1;

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochOne, topicIds), false, 0L);

        Node node = metadata.fetch().nodes().get(0);
        assertFalse(client.isConnected(node.idString()));

        // Seek with a position and leader+epoch
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(
                metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(20L, Optional.of(epochOne), leaderAndEpoch));
        assertFalse(client.isConnected(node.idString()));
        assertTrue(subscriptions.awaitingValidation(tp0));

        // No version information is initially available, but the node is now connected
        offsetFetcher.validatePositionsIfNeeded();
        assertTrue(subscriptions.awaitingValidation(tp0));
        assertTrue(client.isConnected(node.idString()));
        apiVersions.update(node.idString(), NodeApiVersions.create());

        // On the next call, the OffsetForLeaderEpoch request is sent and validation completes
        client.prepareResponseFrom(
            prepareOffsetsForLeaderEpochResponse(tp0, epochOne, 30L),
            node);

        offsetFetcher.validatePositionsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.awaitingValidation(tp0));
        assertEquals(20L, subscriptions.position(tp0).offset);
    }

    @Test
    public void testOffsetValidationSkippedForOldBroker() {
        // Old brokers may require CLUSTER permission to use the OffsetForLeaderEpoch API,
        // so we should skip offset validation and not send the request.
        IsolationLevel isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        int maxPollRecords = Integer.MAX_VALUE;
        long metadataExpireMs = Long.MAX_VALUE;
        OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.EARLIEST;
        int minBytes = 1;
        int maxBytes = Integer.MAX_VALUE;
        int maxWaitMs = 0;
        int fetchSize = 1000;

        MetricConfig metricConfig = new MetricConfig();
        LogContext logContext = new LogContext();
        SubscriptionState subscriptionState = new SubscriptionState(logContext, offsetResetStrategy);

        buildFetcher(metricConfig, isolationLevel, metadataExpireMs, subscriptionState, logContext);

        FetchMetricsRegistry metricsRegistry = new FetchMetricsRegistry(metricConfig.tags().keySet(), "consumertest-group");
        FetchConfig fetchConfig = new FetchConfig(
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                maxPollRecords,
                true, // check crc
                CommonClientConfigs.DEFAULT_CLIENT_RACK,
                isolationLevel);
        Fetcher<byte[], byte[]> fetcher = new Fetcher<>(
                logContext,
                consumerClient,
                metadata,
                subscriptions,
                fetchConfig,
                new Deserializers<>(new ByteArrayDeserializer(), new ByteArrayDeserializer()),
                new FetchMetricsManager(metrics, metricsRegistry),
                time,
                apiVersions);

        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        final int epochOne = 1;
        final int epochTwo = 2;

        // Start with metadata, epoch=1
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochOne, topicIds), false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(), NodeApiVersions.create(
            ApiKeys.OFFSET_FOR_LEADER_EPOCH.id, (short) 0, (short) 2));

        {
            // Seek with a position and leader+epoch
            Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(
                    metadata.currentLeader(tp0).leader, Optional.of(epochOne));
            subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0, Optional.of(epochOne), leaderAndEpoch));

            // Update metadata to epoch=2, enter validation
            metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                    Collections.emptyMap(), partitionCounts, tp -> epochTwo, topicIds), false, 0L);
            offsetFetcher.validatePositionsIfNeeded();

            // Offset validation is skipped
            assertFalse(subscriptions.awaitingValidation(tp0));
        }

        {
            // Seek with a position and leader+epoch
            Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(
                    metadata.currentLeader(tp0).leader, Optional.of(epochOne));
            subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0, Optional.of(epochOne), leaderAndEpoch));

            // Update metadata to epoch=2, enter validation
            metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                    Collections.emptyMap(), partitionCounts, tp -> epochTwo, topicIds), false, 0L);

            // Subscription should not stay in AWAITING_VALIDATION in prepareFetchRequest
            offsetFetcher.validatePositionsOnMetadataChange();
            assertEquals(1, fetcher.sendFetches());
            assertFalse(subscriptions.awaitingValidation(tp0));
        }
    }

    @Test
    public void testOffsetValidationSkippedForOldResponse() {
        // Old responses may provide unreliable leader epoch,
        // so we should skip offset validation and not send the request.
        buildFetcher();
        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        final int epochOne = 1;

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
            Collections.emptyMap(), partitionCounts, tp -> epochOne, topicIds), false, 0L);

        Node node = metadata.fetch().nodes().get(0);
        assertFalse(client.isConnected(node.idString()));

        // Seek with a position and leader+epoch
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(
            metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(20L, Optional.of(epochOne), leaderAndEpoch));
        assertFalse(client.isConnected(node.idString()));
        assertTrue(subscriptions.awaitingValidation(tp0));

        // Inject an older version of the metadata response
        final short responseVersion = 8;
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
            Collections.emptyMap(), partitionCounts, tp -> null, MetadataResponse.PartitionMetadata::new, responseVersion, topicIds), false, 0L);
        offsetFetcher.validatePositionsIfNeeded();
        // Offset validation is skipped
        assertFalse(subscriptions.awaitingValidation(tp0));
    }

    @Test
    public void testOffsetValidationresetPositionForUndefinedEpochWithDefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            UNDEFINED_EPOCH, 0L, OffsetResetStrategy.EARLIEST);
    }

    @Test
    public void testOffsetValidationresetPositionForUndefinedOffsetWithDefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            2, UNDEFINED_EPOCH_OFFSET, OffsetResetStrategy.EARLIEST);
    }

    @Test
    public void testOffsetValidationresetPositionForUndefinedEpochWithUndefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            UNDEFINED_EPOCH, 0L, OffsetResetStrategy.NONE);
    }

    @Test
    public void testOffsetValidationresetPositionForUndefinedOffsetWithUndefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            2, UNDEFINED_EPOCH_OFFSET, OffsetResetStrategy.NONE);
    }

    @Test
    public void testOffsetValidationTriggerLogTruncationForBadOffsetWithUndefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            1, 1L, OffsetResetStrategy.NONE);
    }

    private void testOffsetValidationWithGivenEpochOffset(int leaderEpoch,
                                                          long endOffset,
                                                          OffsetResetStrategy offsetResetStrategy) {
        buildFetcher(offsetResetStrategy);
        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        final int epochOne = 1;
        final long initialOffset = 5;

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
            Collections.emptyMap(), partitionCounts, tp -> epochOne, topicIds), false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(), NodeApiVersions.create());

        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(initialOffset, Optional.of(epochOne), leaderAndEpoch));

        offsetFetcher.validatePositionsIfNeeded();

        consumerClient.poll(time.timer(Duration.ZERO));
        assertTrue(subscriptions.awaitingValidation(tp0));
        assertTrue(client.hasInFlightRequests());

        client.respond(
            offsetsForLeaderEpochRequestMatcher(tp0),
            prepareOffsetsForLeaderEpochResponse(tp0, leaderEpoch, endOffset));
        consumerClient.poll(time.timer(Duration.ZERO));

        if (offsetResetStrategy == OffsetResetStrategy.NONE) {
            LogTruncationException thrown =
                assertThrows(LogTruncationException.class, () -> offsetFetcher.validatePositionsIfNeeded());
            assertEquals(singletonMap(tp0, initialOffset), thrown.offsetOutOfRangePartitions());

            if (endOffset == UNDEFINED_EPOCH_OFFSET || leaderEpoch == UNDEFINED_EPOCH) {
                assertEquals(Collections.emptyMap(), thrown.divergentOffsets());
            } else {
                OffsetAndMetadata expectedDivergentOffset = new OffsetAndMetadata(
                    endOffset, Optional.of(leaderEpoch), "");
                assertEquals(singletonMap(tp0, expectedDivergentOffset), thrown.divergentOffsets());
            }
            assertTrue(subscriptions.awaitingValidation(tp0));
        } else {
            offsetFetcher.validatePositionsIfNeeded();
            assertFalse(subscriptions.awaitingValidation(tp0));
        }
    }

    @Test
    public void testOffsetValidationHandlesSeekWithInflightOffsetForLeaderRequest() {
        buildFetcher();
        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        final int epochOne = 1;
        final Optional<Integer> epochOneOpt = Optional.of(epochOne);

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochOne, topicIds), false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(), NodeApiVersions.create());

        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(metadata.currentLeader(tp0).leader, epochOneOpt);
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0, epochOneOpt, leaderAndEpoch));

        offsetFetcher.validatePositionsIfNeeded();
        consumerClient.poll(time.timer(Duration.ZERO));
        assertTrue(subscriptions.awaitingValidation(tp0));
        assertTrue(client.hasInFlightRequests());

        // While the OffsetForLeaderEpoch request is in-flight, we seek to a different offset.
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(5, epochOneOpt, leaderAndEpoch));
        assertTrue(subscriptions.awaitingValidation(tp0));

        client.respond(
            offsetsForLeaderEpochRequestMatcher(tp0),
            prepareOffsetsForLeaderEpochResponse(tp0, 0, 0L));
        consumerClient.poll(time.timer(Duration.ZERO));

        // The response should be ignored since we were validating a different position.
        assertTrue(subscriptions.awaitingValidation(tp0));
    }

    @Test
    public void testOffsetValidationFencing() {
        buildFetcher();
        assignFromUser(singleton(tp0));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);

        final int epochOne = 1;
        final int epochTwo = 2;
        final int epochThree = 3;

        // Start with metadata, epoch=1
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochOne, topicIds), false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(), NodeApiVersions.create());

        // Seek with a position and leader+epoch
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekValidated(tp0, new SubscriptionState.FetchPosition(0, Optional.of(epochOne), leaderAndEpoch));

        // Update metadata to epoch=2, enter validation
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochTwo, topicIds), false, 0L);
        offsetFetcher.validatePositionsIfNeeded();
        assertTrue(subscriptions.awaitingValidation(tp0));

        // Update the position to epoch=3, as we would from a fetch
        subscriptions.completeValidation(tp0);
        SubscriptionState.FetchPosition nextPosition = new SubscriptionState.FetchPosition(
                10,
                Optional.of(epochTwo),
                new Metadata.LeaderAndEpoch(leaderAndEpoch.leader, Optional.of(epochTwo)));
        subscriptions.position(tp0, nextPosition);
        subscriptions.maybeValidatePositionForCurrentLeader(apiVersions, tp0, new Metadata.LeaderAndEpoch(leaderAndEpoch.leader, Optional.of(epochThree)));

        // Prepare offset list response from async validation with epoch=2
        client.prepareResponse(prepareOffsetsForLeaderEpochResponse(tp0, epochTwo, 10L));
        consumerClient.pollNoWakeup();
        assertTrue(subscriptions.awaitingValidation(tp0), "Expected validation to fail since leader epoch changed");

        // Next round of validation, should succeed in validating the position
        offsetFetcher.validatePositionsIfNeeded();
        client.prepareResponse(prepareOffsetsForLeaderEpochResponse(tp0, epochThree, 10L));
        consumerClient.pollNoWakeup();
        assertFalse(subscriptions.awaitingValidation(tp0), "Expected validation to succeed with latest epoch");
    }

    @Test
    public void testBeginningOffsets() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(listOffsetResponse(tp0, Errors.NONE, ListOffsetsRequest.EARLIEST_TIMESTAMP, 2L));
        assertEquals(singletonMap(tp0, 2L), offsetFetcher.beginningOffsets(singleton(tp0), time.timer(5000L)));
    }

    @Test
    public void testBeginningOffsetsDuplicateTopicPartition() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(listOffsetResponse(tp0, Errors.NONE, ListOffsetsRequest.EARLIEST_TIMESTAMP, 2L));
        assertEquals(singletonMap(tp0, 2L), offsetFetcher.beginningOffsets(asList(tp0, tp0), time.timer(5000L)));
    }

    @Test
    public void testBeginningOffsetsMultipleTopicPartitions() {
        buildFetcher();
        Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(tp0, 2L);
        expectedOffsets.put(tp1, 4L);
        expectedOffsets.put(tp2, 6L);
        assignFromUser(expectedOffsets.keySet());
        client.prepareResponse(listOffsetResponse(expectedOffsets, Errors.NONE, ListOffsetsRequest.EARLIEST_TIMESTAMP, ListOffsetsResponse.UNKNOWN_EPOCH));
        assertEquals(expectedOffsets, offsetFetcher.beginningOffsets(asList(tp0, tp1, tp2), time.timer(5000L)));
    }

    @Test
    public void testBeginningOffsetsEmpty() {
        buildFetcher();
        assertEquals(emptyMap(), offsetFetcher.beginningOffsets(emptyList(), time.timer(5000L)));
    }

    @Test
    public void testEndOffsets() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(listOffsetResponse(tp0, Errors.NONE, ListOffsetsRequest.LATEST_TIMESTAMP, 5L));
        assertEquals(singletonMap(tp0, 5L), offsetFetcher.endOffsets(singleton(tp0), time.timer(5000L)));
    }

    @Test
    public void testEndOffsetsDuplicateTopicPartition() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(listOffsetResponse(tp0, Errors.NONE, ListOffsetsRequest.LATEST_TIMESTAMP, 5L));
        assertEquals(singletonMap(tp0, 5L), offsetFetcher.endOffsets(asList(tp0, tp0), time.timer(5000L)));
    }

    @Test
    public void testEndOffsetsMultipleTopicPartitions() {
        buildFetcher();
        Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(tp0, 5L);
        expectedOffsets.put(tp1, 7L);
        expectedOffsets.put(tp2, 9L);
        assignFromUser(expectedOffsets.keySet());
        client.prepareResponse(listOffsetResponse(expectedOffsets, Errors.NONE, ListOffsetsRequest.LATEST_TIMESTAMP, ListOffsetsResponse.UNKNOWN_EPOCH));
        assertEquals(expectedOffsets, offsetFetcher.endOffsets(asList(tp0, tp1, tp2), time.timer(5000L)));
    }

    @Test
    public void testEndOffsetsEmpty() {
        buildFetcher();
        assertEquals(emptyMap(), offsetFetcher.endOffsets(emptyList(), time.timer(5000L)));
    }

    private MockClient.RequestMatcher offsetsForLeaderEpochRequestMatcher(TopicPartition topicPartition) {
        int currentLeaderEpoch = 1;
        int leaderEpoch = 1;
        return request -> {
            OffsetsForLeaderEpochRequest epochRequest = (OffsetsForLeaderEpochRequest) request;
            OffsetForLeaderPartition partition = offsetForLeaderPartitionMap(epochRequest.data())
                .get(topicPartition);
            return partition != null
                && partition.currentLeaderEpoch() == currentLeaderEpoch
                && partition.leaderEpoch() == leaderEpoch;
        };
    }

    private OffsetsForLeaderEpochResponse prepareOffsetsForLeaderEpochResponse(
        TopicPartition topicPartition,
        int leaderEpoch,
        long endOffset
    ) {

        OffsetForLeaderEpochResponseData data = new OffsetForLeaderEpochResponseData();
        data.topics().add(new OffsetForLeaderTopicResult()
            .setTopic(topicPartition.topic())
            .setPartitions(Collections.singletonList(new EpochEndOffset()
                .setPartition(topicPartition.partition())
                .setErrorCode(Errors.NONE.code())
                .setLeaderEpoch(leaderEpoch)
                .setEndOffset(endOffset))));
        return new OffsetsForLeaderEpochResponse(data);
    }

    private Map<TopicPartition, OffsetForLeaderPartition> offsetForLeaderPartitionMap(
        OffsetForLeaderEpochRequestData data
    ) {
        Map<TopicPartition, OffsetForLeaderPartition> result = new HashMap<>();
        data.topics().forEach(topic ->
            topic.partitions().forEach(partition ->
                result.put(new TopicPartition(topic.topic(), partition.partition()), partition)));
        return result;
    }

    private MockClient.RequestMatcher listOffsetRequestMatcher(final long timestamp) {
        return listOffsetRequestMatcher(timestamp, ListOffsetsResponse.UNKNOWN_EPOCH);
    }

    private MockClient.RequestMatcher listOffsetRequestMatcher(final long timestamp, final int leaderEpoch) {
        // matches any list offset request with the provided timestamp
        return body -> {
            ListOffsetsRequest req = (ListOffsetsRequest) body;
            ListOffsetsTopic topic = req.topics().get(0);
            ListOffsetsPartition partition = topic.partitions().get(0);
            return tp0.topic().equals(topic.name())
                    && tp0.partition() == partition.partitionIndex()
                    && timestamp == partition.timestamp()
                    && leaderEpoch == partition.currentLeaderEpoch();
        };
    }

    private ListOffsetsResponse listOffsetResponse(Errors error, long timestamp, long offset) {
        return listOffsetResponse(tp0, error, timestamp, offset);
    }

    private ListOffsetsResponse listOffsetResponse(TopicPartition tp, Errors error, long timestamp, long offset) {
        return listOffsetResponse(tp, error, timestamp, offset, ListOffsetsResponse.UNKNOWN_EPOCH);
    }

    private ListOffsetsResponse listOffsetResponse(TopicPartition tp, Errors error, long timestamp, long offset, int leaderEpoch) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(tp, offset);
        return listOffsetResponse(offsets, error, timestamp, leaderEpoch);
    }

    private ListOffsetsResponse listOffsetResponse(Map<TopicPartition, Long> offsets, Errors error, long timestamp, int leaderEpoch) {
        Map<String, List<ListOffsetsPartitionResponse>> responses = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            responses.putIfAbsent(tp.topic(), new ArrayList<>());
            responses.get(tp.topic()).add(new ListOffsetsPartitionResponse()
                                .setPartitionIndex(tp.partition())
                                .setErrorCode(error.code())
                                .setOffset(entry.getValue())
                                .setTimestamp(timestamp)
                                .setLeaderEpoch(leaderEpoch));
        }
        List<ListOffsetsTopicResponse> topics = new ArrayList<>();
        for (Map.Entry<String, List<ListOffsetsPartitionResponse>> response : responses.entrySet()) {
            topics.add(new ListOffsetsTopicResponse()
                    .setName(response.getKey())
                    .setPartitions(response.getValue()));
        }
        ListOffsetsResponseData data = new ListOffsetsResponseData().setTopics(topics);
        return new ListOffsetsResponse(data);
    }

    private void buildFetcher() {
        buildFetcher(IsolationLevel.READ_UNCOMMITTED);
    }

    private void buildFetcher(OffsetResetStrategy offsetResetStrategy) {
        buildFetcher(new MetricConfig(), offsetResetStrategy, IsolationLevel.READ_UNCOMMITTED);
    }

    private void buildFetcher(IsolationLevel isolationLevel) {
        buildFetcher(new MetricConfig(), OffsetResetStrategy.EARLIEST, isolationLevel);
    }

    private void buildFetcher(MetricConfig metricConfig,
                              OffsetResetStrategy offsetResetStrategy,
                              IsolationLevel isolationLevel) {
        long metadataExpireMs = Long.MAX_VALUE;
        LogContext logContext = new LogContext();
        SubscriptionState subscriptionState = new SubscriptionState(logContext, offsetResetStrategy);
        buildFetcher(metricConfig, isolationLevel, metadataExpireMs,
                subscriptionState, logContext);
    }

    private void buildFetcher(MetricConfig metricConfig,
                              IsolationLevel isolationLevel,
                              long metadataExpireMs,
                              SubscriptionState subscriptionState,
                              LogContext logContext) {
        buildDependencies(metricConfig, metadataExpireMs, subscriptionState, logContext);
        long requestTimeoutMs = 30000;
        offsetFetcher = new OffsetFetcher(logContext,
                consumerClient,
                metadata,
                subscriptions,
                time,
                retryBackoffMs,
                requestTimeoutMs,
                isolationLevel,
                apiVersions);
    }

    private void buildDependencies(MetricConfig metricConfig,
                                   long metadataExpireMs,
                                   SubscriptionState subscriptionState,
                                   LogContext logContext) {
        time = new MockTime(1);
        subscriptions = subscriptionState;
        metadata = new ConsumerMetadata(0, 0, metadataExpireMs, false, false,
                subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        metrics = new Metrics(metricConfig, time);
        consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time,
                100, 1000, Integer.MAX_VALUE);
    }
}
