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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollContext;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.format;
import static java.util.Collections.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.junit.jupiter.api.Assertions.*;

public class FetchRequestManagerTest {

    private final String topicName = "test";
    private final String groupId = "test-group";
    private final Uuid topicId = Uuid.randomUuid();
    private final Map<String, Uuid> topicIds = new HashMap<String, Uuid>() {
        {
            put(topicName, topicId);
        }
    };
    private final Map<Uuid, String> topicNames = singletonMap(topicId, topicName);
    private final String metricGroup = "consumer" + groupId + "-fetch-manager-metrics";
    private final TopicPartition tp0 = new TopicPartition(topicName, 0);
    private final TopicPartition tp1 = new TopicPartition(topicName, 1);
    private final TopicPartition tp2 = new TopicPartition(topicName, 2);
    private final TopicPartition tp3 = new TopicPartition(topicName, 3);
    private final TopicIdPartition tidp0 = new TopicIdPartition(topicId, tp0);
    private final TopicIdPartition tidp1 = new TopicIdPartition(topicId, tp1);
    private final TopicIdPartition tidp2 = new TopicIdPartition(topicId, tp2);
    private final TopicIdPartition tidp3 = new TopicIdPartition(topicId, tp3);
    private int validLeaderEpoch = 0;
    private int fetchSize = 1000;
    private MockTime time;
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private Metrics metrics;
    private FetchRequestManager<?, ?> fetchRequestManager;
    private MemoryRecords records;
    private MemoryRecords nextRecords;
    private MemoryRecords emptyRecords;
    private MemoryRecords partialRecords;

    @BeforeEach
    public void setup() {
        records = buildRecords(1L, 3, 1);
        nextRecords = buildRecords(4L, 2, 4);
        emptyRecords = buildRecords(0L, 0, 0);
        partialRecords = buildRecords(4L, 1, 0);
        partialRecords.buffer().putInt(Records.SIZE_OFFSET, 10000);
    }

    private void assignFromUser(Set<TopicPartition> partitions) {
        subscriptions.assignFromUser(partitions);

        // A dummy metadata update to ensure valid leader epoch.
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                Collections.emptyMap(), singletonMap(topicName, 4),
                tp -> validLeaderEpoch, topicIds), false, 0L);
    }

    private void assignFromUserNoId(Set<TopicPartition> partitions) {
        subscriptions.assignFromUser(partitions);

        // A dummy metadata update to ensure valid leader epoch.
        metadata.update(9, RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                Collections.emptyMap(), singletonMap("noId", 1),
                tp -> validLeaderEpoch, topicIds), false, 0L);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (metrics != null)
            this.metrics.close();
        if (fetchRequestManager != null)
            this.fetchRequestManager.close(subscriptions);
    }

    @Test
    public void testFetchNormal() {
        buildFetchRequestManager();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        List<UnsentRequest> unsentRequests = sendFetches();
        assertEquals(1, unsentRequests.size());
        assertFalse(fetchRequestManager.hasCompletedFetches());

        UnsentRequest unsentRequest = unsentRequests.get(0);
        prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0), unsentRequest.callback());
        assertTrue(fetchRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchedRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp0);
        assertEquals(3, records.size());
        assertEquals(4L, subscriptions.position(tp0).offset); // this is the next fetching position
        long offset = 1;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            assertEquals(offset, record.offset());
            offset += 1;
        }
    }

    @Test
    public void testInflightFetchOnPendingPartitions() {
        buildFetchRequestManager();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        List<UnsentRequest> unsentRequests = sendFetches();
        assertEquals(1, unsentRequests.size());
        subscriptions.markPendingRevocation(singleton(tp0));

        UnsentRequest unsentRequest = unsentRequests.get(0);
        prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0), unsentRequest.callback());
        assertNull(fetchedRecords().get(tp0));
    }

    @Test
    public void testFetchingPendingPartitions() {
        buildFetchRequestManager();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        List<UnsentRequest> unsentRequests = sendFetches();
        assertEquals(1, unsentRequests.size());
        UnsentRequest unsentRequest = unsentRequests.get(0);
        prepareResponse(fullFetchResponse(tidp0, this.records, Errors.NONE, 100L, 0), unsentRequest.callback());
        assertTrue(fetchRequestManager.hasCompletedFetches());
        fetchedRecords();
        assertEquals(4L, subscriptions.position(tp0).offset); // this is the next fetching position

        // mark partition unfetchable
        subscriptions.markPendingRevocation(singleton(tp0));
        unsentRequests = sendFetches();
        assertEquals(0, unsentRequests.size());
        prepareResponse(fullFetchResponse(tidp0, this.records, Errors.NONE, 100L, 0), unsentRequest.callback());
        assertFalse(fetchRequestManager.hasCompletedFetches());
        fetchedRecords();
        assertEquals(4L, subscriptions.position(tp0).offset);
    }

    private MockClient.RequestMatcher fetchRequestMatcher(
            short expectedVersion,
            TopicIdPartition tp,
            long expectedFetchOffset,
            Optional<Integer> expectedCurrentLeaderEpoch
    ) {
        return fetchRequestMatcher(
                expectedVersion,
                singletonMap(tp, new FetchRequest.PartitionData(
                        tp.topicId(),
                        expectedFetchOffset,
                        FetchRequest.INVALID_LOG_START_OFFSET,
                        fetchSize,
                        expectedCurrentLeaderEpoch
                )),
                emptyList()
        );
    }

    private MockClient.RequestMatcher fetchRequestMatcher(
            short expectedVersion,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetch,
            List<TopicIdPartition> forgotten
    ) {
        return body -> {
            if (body instanceof FetchRequest) {
                FetchRequest fetchRequest = (FetchRequest) body;
                assertEquals(expectedVersion, fetchRequest.version());
                assertEquals(fetch, fetchRequest.fetchData(topicNames(new ArrayList<>(fetch.keySet()))));
                assertEquals(forgotten, fetchRequest.forgottenTopics(topicNames(forgotten)));
                return true;
            } else {
                fail("Should have seen FetchRequest");
                return false;
            }
        };
    }


    private Map<Uuid, String> topicNames(List<TopicIdPartition> partitions) {
        Map<Uuid, String> topicNames = new HashMap<>();
        partitions.forEach(partition -> topicNames.putIfAbsent(partition.topicId(), partition.topic()));
        return topicNames;
    }

    private List<UnsentRequest> sendFetches() {
        PollContext pollContext = new PollContext(metadata, subscriptions, time.milliseconds());
        return fetchRequestManager.poll(pollContext).unsentRequests;
    }

    private void prepareResponse(FetchResponse fetchResponse, RequestCompletionHandler callback) {
        RequestHeader requestHeader = new RequestHeader(ApiKeys.FETCH, (short) 0, "", 1);
        ClientResponse response = new ClientResponse(requestHeader,
                callback,
                "1",
                0,
                0,
                false,
                null,
                null,
                fetchResponse);
        response.onComplete();
    }

    private ConsumerConfig newConfig() {
        Properties props = new Properties();
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new ConsumerConfig(props);
    }

    private FetchResponse fullFetchResponse(TopicIdPartition tp, MemoryRecords records, Errors error, long hw, int throttleTime) {
        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = Collections.singletonMap(tp,
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(tp.topicPartition().partition())
                        .setErrorCode(error.code())
                        .setHighWatermark(hw)
                        .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                        .setLogStartOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                        .setRecords(records));
        return FetchResponse.of(error, throttleTime, INVALID_SESSION_ID, new LinkedHashMap<>(partitions));
    }

    private MemoryRecords buildRecords(long baseOffset, int count, long firstMessageId) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, baseOffset);
        for (int i = 0; i < count; i++)
            builder.append(0L, "key".getBytes(), ("value-" + (firstMessageId + i)).getBytes());
        return builder.build();
    }

    private <K, V> Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        Fetch<K, V> fetch = collectFetch();
        return fetch.records();
    }

    @SuppressWarnings("unchecked")
    private <K, V> Fetch<K, V> collectFetch() {
        FetchRequestManager<K, V> frm = (FetchRequestManager<K, V>) fetchRequestManager;
        return frm.fetch(metadata, subscriptions);
    }

    private void buildFetchRequestManager() {
        MetricConfig metricConfig = new MetricConfig();
        ConsumerConfig config = newConfig();
        LogContext logContext = new LogContext();
        OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.EARLIEST;
        SubscriptionState subscriptionState = new SubscriptionState(logContext, offsetResetStrategy);
        buildFetchRequestManager(metricConfig, config, subscriptionState, logContext);
    }

    private void buildFetchRequestManager(MetricConfig metricConfig,
                                          ConsumerConfig config,
                                          SubscriptionState subscriptionState,
                                          LogContext logContext) {
        buildDependencies(metricConfig, config.getLong(METADATA_MAX_AGE_CONFIG), subscriptionState, logContext);
        FetcherMetricsRegistry metricsRegistry = new FetcherMetricsRegistry();
        FetchContext<byte[], byte[]> fetchContext = new FetchContext<>(
                logContext,
                time,
                config,
                metrics,
                metricsRegistry);
        ApiVersions apiVersions = new ApiVersions();
        RequestState requestState = new RequestState(fetchContext.retryBackoffMs);
        this.fetchRequestManager = new FetchRequestManager<>(fetchContext, apiVersions, new ErrorEventHandler(new LinkedBlockingQueue<>()), requestState);
    }

    private void buildDependencies(MetricConfig metricConfig,
                                   long metadataExpireMs,
                                   SubscriptionState subscriptionState,
                                   LogContext logContext) {
        time = new MockTime(1);
        subscriptions = subscriptionState;
        metadata = new ConsumerMetadata(0,
                metadataExpireMs,
                false,
                false,
                subscriptions,
                logContext,
                new ClusterResourceListeners());
        metrics = new Metrics(metricConfig, time);
    }
}
