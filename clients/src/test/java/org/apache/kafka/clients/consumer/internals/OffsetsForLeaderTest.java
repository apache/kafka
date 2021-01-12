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

import org.apache.kafka.clients.ApiVersion;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;
import static org.apache.kafka.test.TestUtils.assertOptional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class OffsetsForLeaderTest {
    private final String topicName = "test";
    private final TopicPartition tp0 = new TopicPartition(topicName, 0);
    private final TopicPartition tp1 = new TopicPartition(topicName, 1);
    private final TopicPartition tp2 = new TopicPartition(topicName, 2);
    private final TopicPartition tp3 = new TopicPartition(topicName, 3);
    private final int validLeaderEpoch = 0;
    private final MetadataResponse initialUpdateResponse = RequestTestUtils.metadataUpdateWith(1, singletonMap(topicName, 4));

    private MockTime time = new MockTime(1);
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private FetcherMetricsRegistry metricsRegistry;
    private MockClient client;
    private Metrics metrics;
    private final ApiVersions apiVersions = new ApiVersions();
    private ConsumerNetworkClient consumerClient;
    private Fetcher<?, ?> fetcher;

    @Before
    public void setup() {
        MemoryRecords partialRecords = buildRecords(4L, 1, 0);
        partialRecords.buffer().putInt(Records.SIZE_OFFSET, 10000);
    }

    private void assignFromUser(Set<TopicPartition> partitions) {
        subscriptions.assignFromUser(partitions);
        client.updateMetadata(initialUpdateResponse);

        // A dummy metadata update to ensure valid leader epoch.
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
            Collections.emptyMap(), singletonMap(topicName, 4),
            tp -> validLeaderEpoch), false, 0L);
    }

    @After
    public void teardown() throws Exception {
        if (metrics != null)
            this.metrics.close();
        if (fetcher != null)
            this.fetcher.close();
    }

    private MemoryRecords buildRecords(long baseOffset, int count, long firstMessageId) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, baseOffset);
        for (int i = 0; i < count; i++)
            builder.append(0L, "key".getBytes(), ("value-" + (firstMessageId + i)).getBytes());
        return builder.build();
    }

    @Test
    public void testOffsetValidationRequestGrouping() {
        buildFetcher();
        assignFromUser(Utils.mkSet(tp0, tp1, tp2, tp3));

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 3,
            Collections.emptyMap(), singletonMap(topicName, 4),
            tp -> 5), false, 0L);

        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(
                metadata.currentLeader(tp).leader, Optional.of(4));
            subscriptions.seekUnvalidated(tp,
                new SubscriptionState.FetchPosition(0, Optional.of(4), leaderAndEpoch));
        }

        Set<TopicPartition> allRequestedPartitions = new HashSet<>();

        for (Node node : metadata.fetch().nodes()) {
            apiVersions.update(node.idString(),
                    NodeApiVersions.create(Collections.singleton(new ApiVersion(ApiKeys.FETCH.id, (short) 10, (short) 10))));

            Set<TopicPartition> expectedPartitions = subscriptions.assignedPartitions().stream()
                .filter(tp ->
                    metadata.currentLeader(tp).leader.equals(Optional.of(node)))
                .collect(Collectors.toSet());

            assertTrue(expectedPartitions.stream().noneMatch(allRequestedPartitions::contains));
            assertTrue(expectedPartitions.size() > 0);
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

        fetcher.validateOffsetsIfNeeded();
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

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochOne), false, 0L);

        Node node = metadata.fetch().nodes().get(0);
        assertFalse(client.isConnected(node.idString()));

        // Seek with a position and leader+epoch
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(
                metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(20L, Optional.of(epochOne), leaderAndEpoch));
        assertFalse(client.isConnected(node.idString()));
        assertTrue(subscriptions.awaitingValidation(tp0));

        // No version information is initially available, but the node is now connected
        fetcher.validateOffsetsIfNeeded();
        assertTrue(subscriptions.awaitingValidation(tp0));
        assertTrue(client.isConnected(node.idString()));
        apiVersions.update(node.idString(),
                NodeApiVersions.create(Collections.singleton(new ApiVersion(ApiKeys.FETCH.id, (short) 10, (short) 10))));

        // On the next call, the OffsetForLeaderEpoch request is sent and validation completes
        client.prepareResponseFrom(
            prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, epochOne, 30L),
            node);

        fetcher.validateOffsetsIfNeeded();
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.awaitingValidation(tp0));
        assertEquals(20L, subscriptions.position(tp0).offset);
    }

    @Test
    public void testOffsetValidationResetOffsetForUndefinedEpochWithDefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            UNDEFINED_EPOCH, 0L, OffsetResetStrategy.EARLIEST);
    }

    @Test
    public void testOffsetValidationResetOffsetForUndefinedOffsetWithDefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            2, UNDEFINED_EPOCH_OFFSET, OffsetResetStrategy.EARLIEST);
    }

    @Test
    public void testOffsetValidationResetOffsetForUndefinedEpochWithUndefinedResetPolicy() {
        testOffsetValidationWithGivenEpochOffset(
            UNDEFINED_EPOCH, 0L, OffsetResetStrategy.NONE);
    }

    @Test
    public void testOffsetValidationResetOffsetForUndefinedOffsetWithUndefinedResetPolicy() {
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

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
            Collections.emptyMap(), partitionCounts, tp -> epochOne), false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(),
                NodeApiVersions.create(Collections.singleton(new ApiVersion(ApiKeys.FETCH.id, (short) 10, (short) 10))));

        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(initialOffset, Optional.of(epochOne), leaderAndEpoch));

        fetcher.validateOffsetsIfNeeded();

        consumerClient.poll(time.timer(Duration.ZERO));
        assertTrue(subscriptions.awaitingValidation(tp0));
        assertTrue(client.hasInFlightRequests());

        client.respond(
            offsetsForLeaderEpochRequestMatcher(tp0, epochOne, epochOne),
            prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, leaderEpoch, endOffset));
        consumerClient.poll(time.timer(Duration.ZERO));

        if (offsetResetStrategy == OffsetResetStrategy.NONE) {
            LogTruncationException thrown =
                assertThrows(LogTruncationException.class, () -> fetcher.validateOffsetsIfNeeded());
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
            fetcher.validateOffsetsIfNeeded();
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

        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochOne), false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(),
                NodeApiVersions.create(Collections.singleton(new ApiVersion(ApiKeys.FETCH.id, (short) 10, (short) 10))));

        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0, Optional.of(epochOne), leaderAndEpoch));

        fetcher.validateOffsetsIfNeeded();
        consumerClient.poll(time.timer(Duration.ZERO));
        assertTrue(subscriptions.awaitingValidation(tp0));
        assertTrue(client.hasInFlightRequests());

        // While the OffsetForLeaderEpoch request is in-flight, we seek to a different offset.
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(5, Optional.of(epochOne), leaderAndEpoch));
        assertTrue(subscriptions.awaitingValidation(tp0));

        client.respond(
            offsetsForLeaderEpochRequestMatcher(tp0, epochOne, epochOne),
            prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, 0, 0L));
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
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochOne), false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(),
                NodeApiVersions.create(Collections.singleton(new ApiVersion(ApiKeys.FETCH.id, (short) 10, (short) 10))));

        // Seek with a position and leader+epoch
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(metadata.currentLeader(tp0).leader, Optional.of(epochOne));
        subscriptions.seekValidated(tp0, new SubscriptionState.FetchPosition(0, Optional.of(epochOne), leaderAndEpoch));

        // Update metadata to epoch=2, enter validation
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith("dummy", 1,
                Collections.emptyMap(), partitionCounts, tp -> epochTwo), false, 0L);
        fetcher.validateOffsetsIfNeeded();
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
        client.prepareResponse(prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, epochTwo, 10L));
        consumerClient.pollNoWakeup();
        assertTrue("Expected validation to fail since leader epoch changed", subscriptions.awaitingValidation(tp0));

        // Next round of validation, should succeed in validating the position
        fetcher.validateOffsetsIfNeeded();
        client.prepareResponse(prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, epochThree, 10L));
        consumerClient.pollNoWakeup();
        assertFalse("Expected validation to succeed with latest epoch", subscriptions.awaitingValidation(tp0));
    }

    @Test
    public void testTruncationDetected() {
        // Create some records that include a leader epoch (1)
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024),
                RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0L,
                RecordBatch.NO_TIMESTAMP,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                1 // record epoch is earlier than the leader epoch on the client
        );
        builder.appendWithOffset(0L, 0L, "key".getBytes(), "value-1".getBytes());
        builder.appendWithOffset(1L, 0L, "key".getBytes(), "value-2".getBytes());
        builder.appendWithOffset(2L, 0L, "key".getBytes(), "value-3".getBytes());
        MemoryRecords records = builder.build();

        buildFetcher();
        assignFromUser(singleton(tp0));

        // Initialize the epoch=2
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("dummy", 1, Collections.emptyMap(), partitionCounts, tp -> 2);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(),
                NodeApiVersions.create(Collections.singleton(new ApiVersion(ApiKeys.FETCH.id, (short) 10, (short) 10))));

        // Seek
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(metadata.currentLeader(tp0).leader, Optional.of(1));
        subscriptions.seekValidated(tp0, new SubscriptionState.FetchPosition(0, Optional.of(1), leaderAndEpoch));

        // Check for truncation, this should cause tp0 to go into validation
        fetcher.validateOffsetsIfNeeded();

        // No fetches sent since we entered validation
        assertEquals(0, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        assertTrue(subscriptions.awaitingValidation(tp0));

        // Prepare OffsetForEpoch response then check that we update the subscription position correctly.
        client.prepareResponse(prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, 1, 10L));
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.awaitingValidation(tp0));

        // Fetch again, now it works
        assertEquals(1, fetcher.sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tp0, records, Errors.NONE, 100L, 0));
        consumerClient.pollNoWakeup();
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        assertEquals(subscriptions.position(tp0).offset, 3L);
        assertOptional(subscriptions.position(tp0).offsetEpoch, value -> assertEquals(value.intValue(), 1));
    }

    private MockClient.RequestMatcher offsetsForLeaderEpochRequestMatcher(
        TopicPartition topicPartition,
        int currentLeaderEpoch,
        int leaderEpoch
    ) {
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
        Errors error,
        int leaderEpoch,
        long endOffset
    ) {
        OffsetForLeaderEpochResponseData data = new OffsetForLeaderEpochResponseData();
        data.topics().add(new OffsetForLeaderTopicResult()
            .setTopic(topicPartition.topic())
            .setPartitions(Collections.singletonList(new EpochEndOffset()
                .setPartition(topicPartition.partition())
                .setErrorCode(error.code())
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

    private FetchResponse<MemoryRecords> fullFetchResponse(TopicPartition tp, MemoryRecords records, Errors error, long hw, int throttleTime) {
        return fullFetchResponse(tp, records, error, hw, FetchResponse.INVALID_LAST_STABLE_OFFSET, throttleTime);
    }

    private FetchResponse<MemoryRecords> fullFetchResponse(TopicPartition tp, MemoryRecords records, Errors error, long hw,
                                            long lastStableOffset, int throttleTime) {
        Map<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> partitions = Collections.singletonMap(tp,
                new FetchResponse.PartitionData<>(error, hw, lastStableOffset, 0L, null, records));
        return new FetchResponse<>(Errors.NONE, new LinkedHashMap<>(partitions), throttleTime, INVALID_SESSION_ID);
    }

    @SuppressWarnings("unchecked")
    private <K, V> Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        return (Map) fetcher.fetchedRecords();
    }

    private void buildFetcher(int maxPollRecords) {
        buildFetcher(OffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                maxPollRecords, IsolationLevel.READ_UNCOMMITTED);
    }

    private void buildFetcher() {
        buildFetcher(Integer.MAX_VALUE);
    }

    private void buildFetcher(OffsetResetStrategy offsetResetStrategy) {
        buildFetcher(new MetricConfig(), offsetResetStrategy,
            new ByteArrayDeserializer(), new ByteArrayDeserializer(),
            Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);
    }

    private <K, V> void buildFetcher(OffsetResetStrategy offsetResetStrategy,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer,
                                     int maxPollRecords,
                                     IsolationLevel isolationLevel) {
        buildFetcher(new MetricConfig(), offsetResetStrategy, keyDeserializer, valueDeserializer,
                maxPollRecords, isolationLevel);
    }

    private <K, V> void buildFetcher(MetricConfig metricConfig,
                                     OffsetResetStrategy offsetResetStrategy,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer,
                                     int maxPollRecords,
                                     IsolationLevel isolationLevel) {
        buildFetcher(metricConfig, offsetResetStrategy, keyDeserializer, valueDeserializer, maxPollRecords, isolationLevel, Long.MAX_VALUE);
    }

    private <K, V> void buildFetcher(MetricConfig metricConfig,
                                     OffsetResetStrategy offsetResetStrategy,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer,
                                     int maxPollRecords,
                                     IsolationLevel isolationLevel,
                                     long metadataExpireMs) {
        LogContext logContext = new LogContext();
        SubscriptionState subscriptionState = new SubscriptionState(logContext, offsetResetStrategy);
        buildFetcher(metricConfig, keyDeserializer, valueDeserializer, maxPollRecords, isolationLevel, metadataExpireMs,
                subscriptionState, logContext);
    }

    private <K, V> void buildFetcher(MetricConfig metricConfig,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer,
                                     int maxPollRecords,
                                     IsolationLevel isolationLevel,
                                     long metadataExpireMs,
                                     SubscriptionState subscriptionState,
                                     LogContext logContext) {
        buildDependencies(metricConfig, metadataExpireMs, subscriptionState, logContext);
        int minBytes = 1;
        int maxBytes = Integer.MAX_VALUE;
        int maxWaitMs = 0;
        int fetchSize = 1000;
        long retryBackoffMs = 100;
        long requestTimeoutMs = 30000;
        fetcher = new Fetcher<>(
                new LogContext(),
                consumerClient,
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                maxPollRecords,
                true, // check crc
                "",
                keyDeserializer,
                valueDeserializer,
                metadata,
                subscriptions,
                metrics,
                metricsRegistry,
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
        metadata = new ConsumerMetadata(0, metadataExpireMs, false, false,
                subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        metrics = new Metrics(metricConfig, time);
        consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time,
                100, 1000, Integer.MAX_VALUE);
        String groupId = "test-group";
        metricsRegistry = new FetcherMetricsRegistry(metricConfig.tags().keySet(), "consumer" + groupId);
    }
}
