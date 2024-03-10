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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ShareFetchRequestManagerTest {

    private final String topicName = "test";
    private final String groupId = "test-group";
    private final Uuid topicId = Uuid.randomUuid();
    private final Map<String, Uuid> topicIds = new HashMap<String, Uuid>() {
        {
            put(topicName, topicId);
        }
    };
    private final TopicPartition tp0 = new TopicPartition(topicName, 0);
    private final TopicIdPartition tip0 = new TopicIdPartition(topicId, tp0);
    private final int validLeaderEpoch = 0;
    private final MetadataResponse initialUpdateResponse =
            RequestTestUtils.metadataUpdateWithIds(1, singletonMap(topicName, 4), topicIds);

    private final long retryBackoffMs = 100;
    private final long requestTimeoutMs = 30000;
    private MockTime time = new MockTime(1);
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private FetchMetricsManager metricsManager;
    private MockClient client;
    private Metrics metrics;
    private TestableShareFetchRequestManager<?, ?> fetcher;
    private TestableNetworkClientDelegate networkClientDelegate;
    private MemoryRecords records;
    private List<ShareFetchResponseData.AcquiredRecords> acquiredRecords;

    @BeforeEach
    public void setup() {
        records = buildRecords(1L, 3, 1);
        acquiredRecords = ShareCompletedFetchTest.acquiredRecords(1L, 3);
    }

    private void assignFromSubscribed(Set<TopicPartition> partitions) {
        partitions.forEach(partition -> {
            subscriptions.subscribeToShareGroup(Collections.singleton(partition.topic()));
            subscriptions.assignFromSubscribed(Collections.singleton(partition));
        });

        client.updateMetadata(initialUpdateResponse);

        // A dummy metadata update to ensure valid leader epoch.
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                Collections.emptyMap(), singletonMap(topicName, 4),
                tp -> validLeaderEpoch, topicIds), false, 0L);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (metrics != null)
            metrics.close();
        if (fetcher != null)
            fetcher.close();
    }

    private int sendFetches() {
        return fetcher.sendFetches();
    }

    @Test
    public void testFetchNormal() {
        buildFetcher();

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp0);
        assertEquals(3, records.size());
    }

    @Test
    public void testCloseShouldBeIdempotent() {
        buildFetcher();

        fetcher.close();
        fetcher.close();
        fetcher.close();

        verify(fetcher, times(1)).closeInternal();
    }

    @Test
    public void testFetchError() {
        buildFetcher();

        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NOT_LEADER_OR_FOLLOWER));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertFalse(partitionRecords.containsKey(tp0));
    }

    @Test
    public void testInvalidDefaultRecordBatch() {
        buildFetcher();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        ByteBufferOutputStream out = new ByteBufferOutputStream(buffer);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(out,
                DefaultRecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0L, 10L, 0L, (short) 0, 0, false, false, 0, 1024);
        builder.append(10L, "key".getBytes(), "value".getBytes());
        builder.close();
        buffer.flip();

        // Garble the CRC
        buffer.position(17);
        buffer.put("beef".getBytes());
        buffer.position(0);

        assignFromSubscribed(singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0,
                MemoryRecords.readableRecords(buffer),
                ShareCompletedFetchTest.acquiredRecords(0L, 1),
                Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        for (int i = 0; i < 2; i++) {
            // the fetchRecords() should always throw exception due to the bad batch.
            assertThrows(KafkaException.class, this::collectFetch);
        }
    }

    @Test
    public void testParseInvalidRecordBatch() {
        buildFetcher();
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                CompressionType.NONE, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));
        ByteBuffer buffer = records.buffer();

        // flip some bits to fail the crc
        buffer.putInt(32, buffer.get(32) ^ 87238423);

        assignFromSubscribed(singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0,
                MemoryRecords.readableRecords(buffer),
                ShareCompletedFetchTest.acquiredRecords(0L, 3),
                Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertThrows(KafkaException.class, this::collectFetch);
    }

    @Test
    public void testHeaders() {
        buildFetcher();

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, 1L);
        builder.append(0L, "key".getBytes(), "value-1".getBytes());

        Header[] headersArray = new Header[1];
        headersArray[0] = new RecordHeader("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        builder.append(0L, "key".getBytes(), "value-2".getBytes(), headersArray);

        Header[] headersArray2 = new Header[2];
        headersArray2[0] = new RecordHeader("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        headersArray2[1] = new RecordHeader("headerKey", "headerValue2".getBytes(StandardCharsets.UTF_8));
        builder.append(0L, "key".getBytes(), "value-3".getBytes(), headersArray2);

        MemoryRecords memoryRecords = builder.build();

        List<ConsumerRecord<byte[], byte[]>> records;
        assignFromSubscribed(singleton(tp0));

        client.prepareResponse(fullFetchResponse(tip0,
                memoryRecords,
                ShareCompletedFetchTest.acquiredRecords(0L, 3),
                Errors.NONE));

        assertEquals(1, sendFetches());
        networkClientDelegate.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchRecords();
        records = recordsByPartition.get(tp0);

        assertEquals(3, records.size());

        Iterator<ConsumerRecord<byte[], byte[]>> recordIterator = records.iterator();

        ConsumerRecord<byte[], byte[]> record = recordIterator.next();
        assertNull(record.headers().lastHeader("headerKey"));

        record = recordIterator.next();
        assertEquals("headerValue", new String(record.headers().lastHeader("headerKey").value(), StandardCharsets.UTF_8));
        assertEquals("headerKey", record.headers().lastHeader("headerKey").key());

        record = recordIterator.next();
        assertEquals("headerValue2", new String(record.headers().lastHeader("headerKey").value(), StandardCharsets.UTF_8));
        assertEquals("headerKey", record.headers().lastHeader("headerKey").key());
    }

    @Test
    public void testUnauthorizedTopic() {
        buildFetcher();

        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.TOPIC_AUTHORIZATION_FAILED));
        networkClientDelegate.poll(time.timer(0));
        try {
            collectFetch();
            fail("collectFetch should have thrown a TopicAuthorizationException");
        } catch (TopicAuthorizationException e) {
            assertEquals(singleton(topicName), e.unauthorizedTopics());
        }
    }

    @Test
    public void testFetchSessionIdError() {
        buildFetcher();
        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        client.prepareResponse(fetchResponseWithTopLevelError(tip0, Errors.FETCH_SESSION_TOPIC_ID_ERROR));
        networkClientDelegate.poll(time.timer(0));
        assertEmptyFetch("Should not return records or advance position on fetch error");
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @ParameterizedTest
    @MethodSource("handleFetchResponseErrorSupplier")
    public void testHandleFetchResponseError(Errors error,
                                             boolean hasTopLevelError,
                                             boolean shouldRequestMetadataUpdate) {
        buildFetcher();
        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());

        final ShareFetchResponse fetchResponse;

        if (hasTopLevelError)
            fetchResponse = fetchResponseWithTopLevelError(tip0, error);
        else
            fetchResponse = fullFetchResponse(tip0, records, acquiredRecords, error);

        client.prepareResponse(fetchResponse);
        networkClientDelegate.poll(time.timer(0));

        assertEmptyFetch("Should not return records or advance position on fetch error");

        long timeToNextUpdate = metadata.timeToNextUpdate(time.milliseconds());

        if (shouldRequestMetadataUpdate)
            assertEquals(0L, timeToNextUpdate, "Should have requested metadata update");
        else
            assertNotEquals(0L, timeToNextUpdate, "Should not have requested metadata update");
    }

    /**
     * Supplies parameters to {@link #testHandleFetchResponseError(Errors, boolean, boolean)}.
     */
    private static Stream<Arguments> handleFetchResponseErrorSupplier() {
        return Stream.of(
                Arguments.of(Errors.NOT_LEADER_OR_FOLLOWER, false, true),
                Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, false, true),
                Arguments.of(Errors.UNKNOWN_TOPIC_ID, false, true),
                Arguments.of(Errors.FETCH_SESSION_TOPIC_ID_ERROR, true, true),
                Arguments.of(Errors.INCONSISTENT_TOPIC_ID, false, true),
                Arguments.of(Errors.FENCED_LEADER_EPOCH, false, true),
                Arguments.of(Errors.UNKNOWN_LEADER_EPOCH, false, false)
        );
    }

    @Test
    public void testFetchDisconnected() {
        buildFetcher();

        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE), true);
        networkClientDelegate.poll(time.timer(0));
        assertEmptyFetch("Should not return records on disconnect");
    }

    @Test
    public void testFetchWithLastRecordMissingFromBatch() {
        buildFetcher();

        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE,
                new SimpleRecord("0".getBytes(), "v".getBytes()),
                new SimpleRecord("1".getBytes(), "v".getBytes()),
                new SimpleRecord("2".getBytes(), "v".getBytes()),
                new SimpleRecord(null, "value".getBytes()));

        // Remove the last record to simulate compaction
        MemoryRecords.FilterResult result = records.filterTo(tp0, new MemoryRecords.RecordFilter(0, 0) {
            @Override
            protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                return new BatchRetentionResult(BatchRetention.DELETE_EMPTY, false);
            }

            @Override
            protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                return record.key() != null;
            }
        }, ByteBuffer.allocate(1024), Integer.MAX_VALUE, BufferSupplier.NO_CACHING);
        result.outputBuffer().flip();
        MemoryRecords compactedRecords = MemoryRecords.readableRecords(result.outputBuffer());

        assignFromSubscribed(singleton(tp0));
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0,
                compactedRecords,
                ShareCompletedFetchTest.acquiredRecords(0L, 3),
                Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> allFetchedRecords = fetchRecords();
        assertTrue(allFetchedRecords.containsKey(tp0));
        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = allFetchedRecords.get(tp0);
        assertEquals(3, fetchedRecords.size());

        for (int i = 0; i < 3; i++) {
            assertEquals(Integer.toString(i), new String(fetchedRecords.get(i).key()));
        }
    }

    private MemoryRecords buildRecords(long baseOffset, int count, long firstMessageId) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE, TimestampType.CREATE_TIME, baseOffset);
        for (int i = 0; i < count; i++)
            builder.append(0L, "key".getBytes(), ("value-" + (firstMessageId + i)).getBytes());
        return builder.build();
    }

    @Test
    public void testCorruptMessageError() {
        buildFetcher();
        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Prepare a response with the CORRUPT_MESSAGE error.
        client.prepareResponse(fullFetchResponse(
                tip0,
                buildRecords(1L, 1, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 1),
                Errors.CORRUPT_MESSAGE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        // Trigger the exception.
        assertThrows(KafkaException.class, this::fetchRecords);
    }

    private ShareFetchResponse fetchResponseWithTopLevelError(TopicIdPartition tp, Errors error) {
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitions = Collections.singletonMap(tp,
                new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(tp.topicPartition().partition())
                        .setErrorCode(error.code()));
        return ShareFetchResponse.of(error, 0, new LinkedHashMap<>(partitions), Collections.emptyList());
    }

    private ShareFetchResponse fullFetchResponse(TopicIdPartition tp,
                                                 MemoryRecords records,
                                                 List<ShareFetchResponseData.AcquiredRecords> acquiredRecords,
                                                 Errors error) {
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitions = Collections.singletonMap(tp,
                new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(tp.topicPartition().partition())
                        .setErrorCode(error.code())
                        .setRecords(records)
                        .setAcquiredRecords(acquiredRecords));
        return ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), Collections.emptyList());
    }

    /**
     * Assert that the {@link Fetcher#collectFetch() latest fetch} does not contain any
     * {@link Fetch#records() user-visible records},
     * and is {@link Fetch#isEmpty() empty}.
     * @param reason the reason to include for assertion methods such as {@link org.junit.jupiter.api.Assertions#assertTrue(boolean, String)}
     */
    private void assertEmptyFetch(String reason) {
        ShareFetch<?, ?> fetch = collectFetch();
        assertEquals(Collections.emptyMap(), fetch.records(), reason);
        assertTrue(fetch.isEmpty(), reason);
    }

    private <K, V> Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchRecords() {
        ShareFetch<K, V> fetch = collectFetch();
        return fetch.records();
    }

    @SuppressWarnings("unchecked")
    private <K, V> ShareFetch<K, V> collectFetch() {
        return (ShareFetch<K, V>) fetcher.collectFetch();
    }

    private void buildFetcher() {
        buildFetcher(new ByteArrayDeserializer(), new ByteArrayDeserializer()
        );
    }

    private <K, V> void buildFetcher(Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer) {
        buildFetcher(new MetricConfig(), keyDeserializer, valueDeserializer
        );
    }

    private <K, V> void buildFetcher(MetricConfig metricConfig,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer) {
        LogContext logContext = new LogContext();
        SubscriptionState subscriptionState = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        buildFetcher(metricConfig, keyDeserializer, valueDeserializer,
                subscriptionState, logContext);
    }

    private <K, V> void buildFetcher(MetricConfig metricConfig,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer,
                                     SubscriptionState subscriptionState,
                                     LogContext logContext) {
        buildDependencies(metricConfig, subscriptionState, logContext);
        Deserializers<K, V> deserializers = new Deserializers<>(keyDeserializer, valueDeserializer);
        int maxWaitMs = 0;
        int maxBytes = Integer.MAX_VALUE;
        int fetchSize = 1000;
        int minBytes = 1;
        FetchConfig fetchConfig = new FetchConfig(
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                Integer.MAX_VALUE,
                true, // check crc
                CommonClientConfigs.DEFAULT_CLIENT_RACK,
                IsolationLevel.READ_UNCOMMITTED);
        ShareFetchCollector<K, V> shareFetchCollector = new ShareFetchCollector<>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
                deserializers);
        fetcher = spy(new TestableShareFetchRequestManager<>(
                logContext,
                groupId,
                metadata,
                subscriptionState,
                fetchConfig,
                new ShareFetchBuffer(logContext),
                metricsManager,
                shareFetchCollector
                ));
    }

    private void buildDependencies(MetricConfig metricConfig,
                                   SubscriptionState subscriptionState,
                                   LogContext logContext) {
        time = new MockTime(1, 0, 0);
        subscriptions = subscriptionState;
        metadata = new ConsumerMetadata(0, 0, Long.MAX_VALUE, false, false,
                subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        metrics = new Metrics(metricConfig, time);
        FetchMetricsRegistry metricsRegistry = new FetchMetricsRegistry(metricConfig.tags().keySet(), "consumer" + groupId);
        metricsManager = new FetchMetricsManager(metrics, metricsRegistry);

        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs));
        properties.setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs));
        ConsumerConfig config = new ConsumerConfig(properties);
        networkClientDelegate = spy(new TestableNetworkClientDelegate(time, config, logContext, client));
    }

    private class TestableShareFetchRequestManager<K, V> extends ShareFetchRequestManager {

        private final ShareFetchCollector<K, V> shareFetchCollector;

        public TestableShareFetchRequestManager(LogContext logContext,
                                           String groupId,
                                           ConsumerMetadata metadata,
                                           SubscriptionState subscriptions,
                                           FetchConfig fetchConfig,
                                           ShareFetchBuffer shareFetchBuffer,
                                           FetchMetricsManager metricsManager,
                                           ShareFetchCollector<K, V> fetchCollector) {
            super(logContext, groupId, metadata, subscriptions, fetchConfig, shareFetchBuffer, metricsManager);
            this.shareFetchCollector = fetchCollector;
        }

        private ShareFetch<K, V> collectFetch() {
            return shareFetchCollector.collect(shareFetchBuffer);
        }

        private int sendFetches() {
            NetworkClientDelegate.PollResult pollResult = poll(time.milliseconds());
            networkClientDelegate.addAll(pollResult.unsentRequests);
            return pollResult.unsentRequests.size();
        }
    }

    private class TestableNetworkClientDelegate extends NetworkClientDelegate {
        private final ConcurrentLinkedQueue<Node> pendingDisconnects = new ConcurrentLinkedQueue<>();

        public TestableNetworkClientDelegate(Time time,
                                             ConsumerConfig config,
                                             LogContext logContext,
                                             KafkaClient client) {
            super(time, config, logContext, client);
        }

        @Override
        public void poll(final long timeoutMs, final long currentTimeMs) {
            handlePendingDisconnects();
            super.poll(timeoutMs, currentTimeMs);
        }

        public void poll(final Timer timer) {
            long pollTimeout = Math.min(timer.remainingMs(), requestTimeoutMs);
            if (client.inFlightRequestCount() == 0)
                pollTimeout = Math.min(pollTimeout, retryBackoffMs);
            poll(pollTimeout, timer.currentTimeMs());
        }

        private Set<Node> unsentRequestNodes() {
            Set<Node> set = new HashSet<>();

            for (UnsentRequest u : unsentRequests())
                u.node().ifPresent(set::add);

            return set;
        }

        private List<UnsentRequest> removeUnsentRequestByNode(Node node) {
            List<UnsentRequest> list = new ArrayList<>();

            Iterator<UnsentRequest> iter = unsentRequests().iterator();

            while (iter.hasNext()) {
                UnsentRequest u = iter.next();

                if (node.equals(u.node().orElse(null))) {
                    iter.remove();
                    list.add(u);
                }
            }

            return list;
        }

        @Override
        protected void checkDisconnects(final long currentTimeMs) {
            // any disconnects affecting requests that have already been transmitted will be handled
            // by NetworkClient, so we just need to check whether connections for any of the unsent
            // requests have been disconnected; if they have, then we complete the corresponding future
            // and set the disconnect flag in the ClientResponse
            for (Node node : unsentRequestNodes()) {
                if (client.connectionFailed(node)) {
                    // Remove entry before invoking request callback to avoid callbacks handling
                    // coordinator failures traversing the unsent list again.
                    for (UnsentRequest unsentRequest : removeUnsentRequestByNode(node)) {
                        FutureCompletionHandler handler = unsentRequest.handler();
                        AuthenticationException authenticationException = client.authenticationException(node);
                        long startMs = unsentRequest.timer().currentTimeMs() - unsentRequest.timer().elapsedMs();
                        handler.onComplete(new ClientResponse(makeHeader(unsentRequest.requestBuilder().latestAllowedVersion()),
                                unsentRequest.handler(), unsentRequest.node().toString(), startMs, currentTimeMs, true,
                                null, authenticationException, null));
                    }
                }
            }
        }

        private RequestHeader makeHeader(short version) {
            return new RequestHeader(
                    new RequestHeaderData()
                            .setRequestApiKey(ApiKeys.SHARE_FETCH.id)
                            .setRequestApiVersion(version),
                    ApiKeys.SHARE_FETCH.requestHeaderVersion(version));
        }

        private void handlePendingDisconnects() {
            while (true) {
                Node node = pendingDisconnects.poll();
                if (node == null)
                    break;

                failUnsentRequests(node);
                client.disconnect(node.idString());
            }
        }

        private void failUnsentRequests(Node node) {
            // clear unsent requests to node and fail their corresponding futures
            for (UnsentRequest unsentRequest : removeUnsentRequestByNode(node)) {
                FutureCompletionHandler handler = unsentRequest.handler();
                handler.onFailure(time.milliseconds(), DisconnectException.INSTANCE);
            }
        }
    }
}
