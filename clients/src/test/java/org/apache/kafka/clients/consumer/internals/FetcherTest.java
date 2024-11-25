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
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.OffsetForLeaderTopicResult;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchRequest.PartitionData;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.DelayedReceive;
import org.apache.kafka.test.MockSelector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.apache.kafka.test.TestUtils.assertOptional;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * If you are adding a test here, do evaluate if a similar test needs to be added in
 * FetchRequestManagerTest.java, which captures the tests for the new consumer as part of the
 * https://cwiki.apache.org/confluence/display/KAFKA/Consumer+threading+refactor+project+overview
 */
public class FetcherTest {
    private static final double EPSILON = 0.0001;

    private final String topicName = "test";
    private final String groupId = "test-group";
    private final Uuid topicId = Uuid.randomUuid();
    private final Map<String, Uuid> topicIds = new HashMap<>() {
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
    private final int validLeaderEpoch = 0;
    private final MetadataResponse initialUpdateResponse =
        RequestTestUtils.metadataUpdateWithIds(1, singletonMap(topicName, 4), topicIds);

    private final int minBytes = 1;
    private final int maxBytes = Integer.MAX_VALUE;
    private final int maxWaitMs = 0;
    private final long retryBackoffMs = 100;
    private final int requestTimeoutMs = 30000;
    private final ApiVersions apiVersions = new ApiVersions();
    private int fetchSize = 1000;
    private MockTime time = new MockTime(1);
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private FetchMetricsRegistry metricsRegistry;
    private FetchMetricsManager metricsManager;
    private MockClient client;
    private Metrics metrics;
    private ConsumerNetworkClient consumerClient;
    private Fetcher<?, ?> fetcher;
    private OffsetFetcher offsetFetcher;
    private MemoryRecords records;
    private MemoryRecords nextRecords;
    private MemoryRecords moreRecords;
    private MemoryRecords emptyRecords;
    private MemoryRecords partialRecords;
    private ExecutorService executorService;

    @BeforeEach
    public void setup() {
        records = buildRecords(1L, 3, 1);
        nextRecords = buildRecords(4L, 2, 4);
        moreRecords = buildRecords(6L, 3, 6);
        emptyRecords = buildRecords(0L, 0, 0);
        partialRecords = buildRecords(4L, 1, 0);
        partialRecords.buffer().putInt(Records.SIZE_OFFSET, 10000);
    }

    private void assignFromUser(Set<TopicPartition> partitions) {
        subscriptions.assignFromUser(partitions);
        client.updateMetadata(initialUpdateResponse);

        // A dummy metadata update to ensure valid leader epoch.
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
            Collections.emptyMap(), singletonMap(topicName, 4),
            tp -> validLeaderEpoch, topicIds), false, 0L);
    }

    private void assignFromUser(TopicPartition partition) {
        subscriptions.assignFromUser(singleton(partition));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, singletonMap(partition.topic(), 1), Collections.emptyMap()));

        // A dummy metadata update to ensure valid leader epoch.
        metadata.update(9, RequestTestUtils.metadataUpdateWithIds("dummy", 1,
            Collections.emptyMap(), singletonMap(partition.topic(), 1),
            tp -> validLeaderEpoch, topicIds), false, 0L);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (metrics != null)
            metrics.close();
        if (fetcher != null)
            fetcher.close();
        if (executorService != null) {
            executorService.shutdownNow();
            assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private int sendFetches() {
        offsetFetcher.validatePositionsOnMetadataChange();
        return fetcher.sendFetches();
    }

    @Test
    public void testFetchNormal() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
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
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, sendFetches());
        subscriptions.markPendingRevocation(singleton(tp0));

        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertNull(fetchRecords().get(tp0));
    }

    @Test
    public void testCloseShouldBeIdempotent() {
        buildFetcher();

        fetcher.close();
        fetcher.close();
        fetcher.close();

        verify(fetcher, times(1)).maybeCloseFetchSessions(any(Timer.class));
    }

    @Test
    public void testFetcherCloseClosesFetchSessionsInBroker() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        final FetchResponse fetchResponse = fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0);
        client.prepareResponse(fetchResponse);
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        assertEquals(0, consumerClient.pendingRequestCount());

        final ArgumentCaptor<FetchRequest.Builder> argument = ArgumentCaptor.forClass(FetchRequest.Builder.class);

        // send request to close the fetcher
        fetcher.close(time.timer(Duration.ofSeconds(10)));

        // validate that Fetcher.close() has sent a request with final epoch. 2 requests are sent, one for the normal
        // fetch earlier and another for the finish fetch here.
        verify(consumerClient, times(2)).send(any(Node.class), argument.capture());
        FetchRequest.Builder builder = argument.getValue();
        // session Id is the same
        assertEquals(fetchResponse.sessionId(), builder.metadata().sessionId());
        // contains final epoch
        assertEquals(FetchMetadata.FINAL_EPOCH, builder.metadata().epoch());  // final epoch indicates we want to close the session
        assertTrue(builder.fetchData().isEmpty()); // partition data should be empty
    }

    @Test
    public void testFetchingPendingPartitions() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();
        assertEquals(4L, subscriptions.position(tp0).offset); // this is the next fetching position

        // mark partition unfetchable
        subscriptions.markPendingRevocation(singleton(tp0));
        assertEquals(0, sendFetches());
        consumerClient.poll(time.timer(0));
        assertFalse(fetcher.hasCompletedFetches());
        fetchRecords();
        assertEquals(4L, subscriptions.position(tp0).offset);
    }

    @Test
    public void testFetchWithNoTopicId() {
        // Should work and default to using old request type.
        buildFetcher();

        TopicIdPartition noId = new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("noId", 0));
        assignFromUser(noId.topicPartition());
        subscriptions.seek(noId.topicPartition(), 0);

        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Fetch should use request version 12
        client.prepareResponse(
            fetchRequestMatcher((short) 12, noId, 0, Optional.of(validLeaderEpoch)),
            fullFetchResponse(noId, records, Errors.NONE, 100L, 0)
        );
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(noId.topicPartition()));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(noId.topicPartition());
        assertEquals(3, records.size());
        assertEquals(4L, subscriptions.position(noId.topicPartition()).offset); // this is the next fetching position
        long offset = 1;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            assertEquals(offset, record.offset());
            offset += 1;
        }
    }

    @Test
    public void testFetchWithTopicId() {
        buildFetcher();

        TopicIdPartition tp = new TopicIdPartition(topicId, new TopicPartition(topicName, 0));
        assignFromUser(singleton(tp.topicPartition()));
        subscriptions.seek(tp.topicPartition(), 0);

        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Fetch should use latest version
        client.prepareResponse(
            fetchRequestMatcher(ApiKeys.FETCH.latestVersion(), tp, 0, Optional.of(validLeaderEpoch)),
            fullFetchResponse(tp, records, Errors.NONE, 100L, 0)
        );
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp.topicPartition()));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp.topicPartition());
        assertEquals(3, records.size());
        assertEquals(4L, subscriptions.position(tp.topicPartition()).offset); // this is the next fetching position
        long offset = 1;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            assertEquals(offset, record.offset());
            offset += 1;
        }
    }

    @Test
    public void testFetchForgetTopicIdWhenUnassigned() {
        buildFetcher();

        TopicIdPartition foo = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition bar = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 0));

        // Assign foo and bar.
        subscriptions.assignFromUser(singleton(foo.topicPartition()));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, singleton(foo), tp -> validLeaderEpoch));
        subscriptions.seek(foo.topicPartition(), 0);

        assertEquals(1, sendFetches());

        // Fetch should use latest version.
        client.prepareResponse(
            fetchRequestMatcher(ApiKeys.FETCH.latestVersion(),
                singletonMap(foo, new PartitionData(
                    foo.topicId(),
                    0,
                    FetchRequest.INVALID_LOG_START_OFFSET,
                    fetchSize,
                    Optional.of(validLeaderEpoch))
                ),
                emptyList()
            ),
            fullFetchResponse(1, foo, records, Errors.NONE, 100L, 0)
        );
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();

        // Assign bar and un-assign foo.
        subscriptions.assignFromUser(singleton(bar.topicPartition()));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, singleton(bar), tp -> validLeaderEpoch));
        subscriptions.seek(bar.topicPartition(), 0);

        // Fetch should use latest version.
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(
            fetchRequestMatcher(ApiKeys.FETCH.latestVersion(),
                singletonMap(bar, new PartitionData(
                    bar.topicId(),
                    0,
                    FetchRequest.INVALID_LOG_START_OFFSET,
                    fetchSize,
                    Optional.of(validLeaderEpoch))
                ),
                singletonList(foo)
            ),
            fullFetchResponse(1, bar, records, Errors.NONE, 100L, 0)
        );
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();
    }

    @Test
    public void testFetchForgetTopicIdWhenReplaced() {
        buildFetcher();

        TopicIdPartition fooWithOldTopicId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition fooWithNewTopicId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        // Assign foo with old topic id.
        subscriptions.assignFromUser(singleton(fooWithOldTopicId.topicPartition()));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, singleton(fooWithOldTopicId), tp -> validLeaderEpoch));
        subscriptions.seek(fooWithOldTopicId.topicPartition(), 0);

        // Fetch should use latest version.
        assertEquals(1, sendFetches());

        client.prepareResponse(
            fetchRequestMatcher(ApiKeys.FETCH.latestVersion(),
                singletonMap(fooWithOldTopicId, new PartitionData(
                    fooWithOldTopicId.topicId(),
                    0,
                    FetchRequest.INVALID_LOG_START_OFFSET,
                    fetchSize,
                    Optional.of(validLeaderEpoch))
                ),
                emptyList()
            ),
            fullFetchResponse(1, fooWithOldTopicId, records, Errors.NONE, 100L, 0)
        );
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();

        // Replace foo with old topic id with foo with new topic id.
        subscriptions.assignFromUser(singleton(fooWithNewTopicId.topicPartition()));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, singleton(fooWithNewTopicId), tp -> validLeaderEpoch));
        subscriptions.seek(fooWithNewTopicId.topicPartition(), 0);

        // Fetch should use latest version.
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // foo with old topic id should be removed from the session.
        client.prepareResponse(
            fetchRequestMatcher(ApiKeys.FETCH.latestVersion(),
                singletonMap(fooWithNewTopicId, new PartitionData(
                    fooWithNewTopicId.topicId(),
                    0,
                    FetchRequest.INVALID_LOG_START_OFFSET,
                    fetchSize,
                    Optional.of(validLeaderEpoch))
                ),
                singletonList(fooWithOldTopicId)
            ),
            fullFetchResponse(1, fooWithNewTopicId, records, Errors.NONE, 100L, 0)
        );
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();
    }

    @Test
    public void testFetchTopicIdUpgradeDowngrade() {
        buildFetcher();

        TopicIdPartition fooWithoutId = new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("foo", 0));

        // Assign foo without a topic id.
        subscriptions.assignFromUser(singleton(fooWithoutId.topicPartition()));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, singleton(fooWithoutId), tp -> validLeaderEpoch));
        subscriptions.seek(fooWithoutId.topicPartition(), 0);

        // Fetch should use version 12.
        assertEquals(1, sendFetches());

        client.prepareResponse(
            fetchRequestMatcher((short) 12,
                singletonMap(fooWithoutId, new PartitionData(
                    fooWithoutId.topicId(),
                    0,
                    FetchRequest.INVALID_LOG_START_OFFSET,
                    fetchSize,
                    Optional.of(validLeaderEpoch))
                ),
                emptyList()
            ),
            fullFetchResponse(1, fooWithoutId, records, Errors.NONE, 100L, 0)
        );
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();

        // Upgrade.
        TopicIdPartition fooWithId = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        subscriptions.assignFromUser(singleton(fooWithId.topicPartition()));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, singleton(fooWithId), tp -> validLeaderEpoch));
        subscriptions.seek(fooWithId.topicPartition(), 0);

        // Fetch should use latest version.
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // foo with old topic id should be removed from the session.
        client.prepareResponse(
            fetchRequestMatcher(ApiKeys.FETCH.latestVersion(),
                singletonMap(fooWithId, new PartitionData(
                    fooWithId.topicId(),
                    0,
                    FetchRequest.INVALID_LOG_START_OFFSET,
                    fetchSize,
                    Optional.of(validLeaderEpoch))
                ),
                emptyList()
            ),
            fullFetchResponse(1, fooWithId, records, Errors.NONE, 100L, 0)
        );
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();

        // Downgrade.
        subscriptions.assignFromUser(singleton(fooWithoutId.topicPartition()));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, singleton(fooWithoutId), tp -> validLeaderEpoch));
        subscriptions.seek(fooWithoutId.topicPartition(), 0);

        // Fetch should use version 12.
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // foo with old topic id should be removed from the session.
        client.prepareResponse(
            fetchRequestMatcher((short) 12,
                singletonMap(fooWithoutId, new PartitionData(
                    fooWithoutId.topicId(),
                    0,
                    FetchRequest.INVALID_LOG_START_OFFSET,
                    fetchSize,
                    Optional.of(validLeaderEpoch))
                ),
                emptyList()
            ),
            fullFetchResponse(1, fooWithoutId, records, Errors.NONE, 100L, 0)
        );
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();
    }

    private MockClient.RequestMatcher fetchRequestMatcher(
        short expectedVersion,
        TopicIdPartition tp,
        long expectedFetchOffset,
        Optional<Integer> expectedCurrentLeaderEpoch
    ) {
        return fetchRequestMatcher(
            expectedVersion,
            singletonMap(tp, new PartitionData(
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
        Map<TopicIdPartition, PartitionData> fetch,
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

    @Test
    public void testMissingLeaderEpochInRecords() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V0,
                Compression.NONE, TimestampType.CREATE_TIME, 0L, System.currentTimeMillis(),
                RecordBatch.NO_PARTITION_LEADER_EPOCH);
        builder.append(0L, "key".getBytes(), "1".getBytes());
        builder.append(0L, "key".getBytes(), "2".getBytes());
        MemoryRecords records = builder.build();

        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertEquals(2, partitionRecords.get(tp0).size());

        for (ConsumerRecord<byte[], byte[]> record : partitionRecords.get(tp0)) {
            assertEquals(Optional.empty(), record.leaderEpoch());
        }
    }

    @Test
    public void testLeaderEpochInConsumerRecord() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        int partitionLeaderEpoch = 1;

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE, TimestampType.CREATE_TIME, 0L, System.currentTimeMillis(),
                partitionLeaderEpoch);
        builder.append(0L, "key".getBytes(), Integer.toString(partitionLeaderEpoch).getBytes());
        builder.append(0L, "key".getBytes(), Integer.toString(partitionLeaderEpoch).getBytes());
        builder.close();

        partitionLeaderEpoch += 7;

        builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
                TimestampType.CREATE_TIME, 2L, System.currentTimeMillis(), partitionLeaderEpoch);
        builder.append(0L, "key".getBytes(), Integer.toString(partitionLeaderEpoch).getBytes());
        builder.close();

        partitionLeaderEpoch += 5;
        builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
                TimestampType.CREATE_TIME, 3L, System.currentTimeMillis(), partitionLeaderEpoch);
        builder.append(0L, "key".getBytes(), Integer.toString(partitionLeaderEpoch).getBytes());
        builder.append(0L, "key".getBytes(), Integer.toString(partitionLeaderEpoch).getBytes());
        builder.append(0L, "key".getBytes(), Integer.toString(partitionLeaderEpoch).getBytes());
        builder.close();

        buffer.flip();
        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertEquals(6, partitionRecords.get(tp0).size());

        for (ConsumerRecord<byte[], byte[]> record : partitionRecords.get(tp0)) {
            int expectedLeaderEpoch = Integer.parseInt(Utils.utf8(record.value()));
            assertEquals(Optional.of(expectedLeaderEpoch), record.leaderEpoch());
        }
    }

    @Test
    public void testClearBufferedDataForTopicPartitions() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        Set<TopicPartition> newAssignedTopicPartitions = new HashSet<>();
        newAssignedTopicPartitions.add(tp1);

        fetcher.clearBufferedDataForUnassignedPartitions(newAssignedTopicPartitions);
        assertFalse(fetcher.hasCompletedFetches());
    }

    @Test
    public void testFetchSkipsBlackedOutNodes() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        Node node = initialUpdateResponse.brokers().iterator().next();

        client.backoff(node, 500);
        assertEquals(0, sendFetches());

        time.sleep(500);
        assertEquals(1, sendFetches());
    }

    @Test
    public void testFetcherIgnoresControlRecords() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        long producerId = 1;
        short producerEpoch = 0;
        int baseSequence = 0;
        int partitionLeaderEpoch = 0;

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.idempotentBuilder(buffer, Compression.NONE, 0L, producerId,
                producerEpoch, baseSequence);
        builder.append(0L, "key".getBytes(), null);
        builder.close();

        MemoryRecords.writeEndTransactionalMarker(buffer, 1L, time.milliseconds(), partitionLeaderEpoch, producerId, producerEpoch,
                new EndTransactionMarker(ControlRecordType.ABORT, 0));

        buffer.flip();

        client.prepareResponse(fullFetchResponse(tidp0, MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp0);
        assertEquals(1, records.size());
        assertEquals(2L, subscriptions.position(tp0).offset);

        ConsumerRecord<byte[], byte[]> record = records.get(0);
        assertArrayEquals("key".getBytes(), record.key());
    }

    @Test
    public void testFetchError() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NOT_LEADER_OR_FOLLOWER, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertFalse(partitionRecords.containsKey(tp0));
    }

    private MockClient.RequestMatcher matchesOffset(final TopicIdPartition tp, final long offset) {
        return body -> {
            FetchRequest fetch = (FetchRequest) body;
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchData = fetch.fetchData(topicNames);
            return fetchData.containsKey(tp) &&
                    fetchData.get(tp).fetchOffset == offset;
        };
    }

    @Test
    public void testFetchedRecordsRaisesOnSerializationErrors() {
        // raise an exception from somewhere in the middle of the fetch response
        // so that we can verify that our position does not advance after raising
        ByteArrayDeserializer deserializer = new ByteArrayDeserializer() {
            int i = 0;
            @Override
            public byte[] deserialize(String topic, byte[] data) {
                if (i++ % 2 == 1) {
                    // Should be blocked on the value deserialization of the first record.
                    assertEquals("value-1", new String(data, StandardCharsets.UTF_8));
                    throw new SerializationException();
                }
                return data;
            }
        };

        buildFetcher(deserializer, deserializer);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        client.prepareResponse(matchesOffset(tidp0, 1), fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));

        assertEquals(1, sendFetches());
        consumerClient.poll(time.timer(0));

        for (int i = 0; i < 2; i++) {
            // The fetcher should throw a Deserialization error
            assertThrows(SerializationException.class, this::collectFetch);
            // the position should not advance since no data has been returned
            assertEquals(1, subscriptions.position(tp0).offset);
        }
    }

    @Test
    public void testParseCorruptedRecord() throws Exception {
        buildFetcher();
        assignFromUser(singleton(tp0));

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(buffer));

        byte magic = RecordBatch.MAGIC_VALUE_V1;
        byte[] key = "foo".getBytes();
        byte[] value = "baz".getBytes();
        long offset = 0;
        long timestamp = 500L;

        int size = LegacyRecord.recordSize(magic, key.length, value.length);
        byte attributes = LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME);
        long crc = LegacyRecord.computeChecksum(magic, attributes, timestamp, key, value);

        // write one valid record
        out.writeLong(offset);
        out.writeInt(size);
        LegacyRecord.write(out, magic, crc, LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME), timestamp, key, value);

        // and one invalid record (note the crc)
        out.writeLong(offset + 1);
        out.writeInt(size);
        LegacyRecord.write(out, magic, crc + 1, LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME), timestamp, key, value);

        // write one valid record
        out.writeLong(offset + 2);
        out.writeInt(size);
        LegacyRecord.write(out, magic, crc, LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME), timestamp, key, value);

        // Write a record whose size field is invalid.
        out.writeLong(offset + 3);
        out.writeInt(1);

        // write one valid record
        out.writeLong(offset + 4);
        out.writeInt(size);
        LegacyRecord.write(out, magic, crc, LegacyRecord.computeAttributes(magic, CompressionType.NONE, TimestampType.CREATE_TIME), timestamp, key, value);

        buffer.flip();

        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(0, Optional.empty(), metadata.currentLeader(tp0)));

        // normal fetch
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        // the first fetchRecords() should return the first valid message
        assertEquals(1, fetchRecords().get(tp0).size());
        assertEquals(1, subscriptions.position(tp0).offset);

        ensureBlockOnRecord(1L);
        seekAndConsumeRecord(buffer, 2L);
        ensureBlockOnRecord(3L);
        try {
            // For a record that cannot be retrieved from the iterator, we cannot seek over it within the batch.
            seekAndConsumeRecord(buffer, 4L);
            fail("Should have thrown exception when fail to retrieve a record from iterator.");
        } catch (KafkaException ke) {
           // let it go
        }
        ensureBlockOnRecord(4L);
    }

    private void ensureBlockOnRecord(long blockedOffset) {
        for (int i = 0; i < 2; i++) {
            // the fetchRecords() should always throw exception due to the invalid message at the starting offset.
            assertThrows(KafkaException.class, this::fetchRecords);
            assertEquals(blockedOffset, subscriptions.position(tp0).offset);
        }
    }

    private void seekAndConsumeRecord(ByteBuffer responseBuffer, long toOffset) {
        // Seek to skip the bad record and fetch again.
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(toOffset, Optional.empty(), metadata.currentLeader(tp0)));
        // Should not throw exception after the seek.
        collectFetch();
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, MemoryRecords.readableRecords(responseBuffer), Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchRecords();
        List<ConsumerRecord<byte[], byte[]>> records = recordsByPartition.get(tp0);
        assertEquals(1, records.size());
        assertEquals(toOffset, records.get(0).offset());
        assertEquals(toOffset + 1, subscriptions.position(tp0).offset);
    }

    @Test
    public void testInvalidDefaultRecordBatch() {
        buildFetcher();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        ByteBufferOutputStream out = new ByteBufferOutputStream(buffer);

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(out,
                                                                DefaultRecordBatch.CURRENT_MAGIC_VALUE,
                                                                Compression.NONE,
                                                                TimestampType.CREATE_TIME,
                                                                0L, 10L, 0L, (short) 0, 0, false, false, 0, 1024);
        builder.append(10L, "key".getBytes(), "value".getBytes());
        builder.close();
        buffer.flip();

        // Garble the CRC
        buffer.position(17);
        buffer.put("beef".getBytes());
        buffer.position(0);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        for (int i = 0; i < 2; i++) {
            // collectFetch() should always throw exception due to the bad batch.
            assertThrows(KafkaException.class, this::collectFetch);
            assertEquals(0, subscriptions.position(tp0).offset);
        }
    }

    @Test
    public void testParseInvalidRecordBatch() {
        buildFetcher();
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                Compression.NONE, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));
        ByteBuffer buffer = records.buffer();

        // flip some bits to fail the crc
        buffer.putInt(32, buffer.get(32) ^ 87238423);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, MemoryRecords.readableRecords(buffer), Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        assertThrows(KafkaException.class, this::collectFetch);
        // the position should not advance since no data has been returned
        assertEquals(0, subscriptions.position(tp0).offset);
    }

    @Test
    public void testHeaders() {
        buildFetcher();

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, 1L);
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
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        client.prepareResponse(matchesOffset(tidp0, 1), fullFetchResponse(tidp0, memoryRecords, Errors.NONE, 100L, 0));

        assertEquals(1, sendFetches());
        consumerClient.poll(time.timer(0));
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
    public void testFetchMaxPollRecords() {
        buildFetcher(2);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        client.prepareResponse(matchesOffset(tidp0, 1), fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        client.prepareResponse(matchesOffset(tidp0, 4), fullFetchResponse(tidp0, nextRecords, Errors.NONE, 100L, 0));

        assertEquals(1, sendFetches());
        consumerClient.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchRecords();
        List<ConsumerRecord<byte[], byte[]>> recordsToTest = recordsByPartition.get(tp0);
        assertEquals(2, recordsToTest.size());
        assertEquals(3L, subscriptions.position(tp0).offset);
        assertEquals(1, recordsToTest.get(0).offset());
        assertEquals(2, recordsToTest.get(1).offset());

        assertEquals(0, sendFetches());
        consumerClient.poll(time.timer(0));
        recordsByPartition = fetchRecords();
        recordsToTest = recordsByPartition.get(tp0);
        assertEquals(1, recordsToTest.size());
        assertEquals(4L, subscriptions.position(tp0).offset);
        assertEquals(3, recordsToTest.get(0).offset());

        assertTrue(sendFetches() > 0);
        consumerClient.poll(time.timer(0));
        recordsByPartition = fetchRecords();
        recordsToTest = recordsByPartition.get(tp0);
        assertEquals(2, recordsToTest.size());
        assertEquals(6L, subscriptions.position(tp0).offset);
        assertEquals(4, recordsToTest.get(0).offset());
        assertEquals(5, recordsToTest.get(1).offset());
    }

    /**
     * KAFKA-15836:
     * Test that max.poll.records is honoured when consuming from multiple topic-partitions and the
     * fetched records are not aligned on max.poll.records boundaries.
     *
     * tp0 has records 1,2,3; tp1 has records 6,7,8
     * max.poll.records is 2
     * 
     * poll 1 should return 1,2
     * poll 2 should return 3,6
     * poll 3 should return 7,8
     * 
     * Or it can be 6,7; then 8,1; then 2,3 because the order of topic-partitions returned is non-deterministic.
     */
    @Test
    public void testFetchMaxPollRecordsUnaligned() {
        final int maxPollRecords = 2;
        buildFetcher(maxPollRecords);

        Set<TopicPartition> tps = new HashSet<>();
        tps.add(tp0);
        tps.add(tp1);
        assignFromUser(tps);
        subscriptions.seek(tp0, 1);
        subscriptions.seek(tp1, 6);

        client.prepareResponse(fetchResponse2(tidp0, records, 100L, tidp1, moreRecords, 100L));
        client.prepareResponse(fullFetchResponse(tidp0, emptyRecords, Errors.NONE, 100L, 0));

        // Send fetch request because we do not have pending fetch responses to process.
        // The first fetch response will return 3 records for tp0 and 3 more for tp1.
        assertEquals(1, sendFetches());
        // The poll returns 2 records from one of the topic-partitions (non-deterministic).
        // This leaves 1 record pending from that topic-partition, and the remaining 3 from the other.
        pollAndValidateMaxPollRecordsNotExceeded(maxPollRecords);

        // See if we need to send another fetch, which we do not because we have records in hand.
        assertEquals(0, sendFetches());
        // The poll returns 2 more records, 1 from the topic-partition we've already been
        // processing, and 1 more from the other topic-partition. This means we have processed
        // all records from the former, and 2 remain from the latter.
        pollAndValidateMaxPollRecordsNotExceeded(maxPollRecords);

        // See if we need to send another fetch, which we do because we've processed all of the records
        // from one of the topic-partitions. The fetch response does not contain any more records.
        assertEquals(1, sendFetches());
        // The poll returns the final 2 records.
        pollAndValidateMaxPollRecordsNotExceeded(maxPollRecords);
    }

    private void pollAndValidateMaxPollRecordsNotExceeded(int maxPollRecords) {
        consumerClient.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchRecords();
        int fetchedRecords = 0;
        for (List<ConsumerRecord<byte[], byte[]>> recordsToTest : recordsByPartition.values()) {
            fetchedRecords += recordsToTest.size();
        }
        assertEquals(maxPollRecords, fetchedRecords);
    }

    /**
     * Test the scenario where a partition with fetched but not consumed records (i.e. max.poll.records is
     * less than the number of fetched records) is unassigned and a different partition is assigned. This is a
     * pattern used by Streams state restoration and KAFKA-5097 would have been caught by this test.
     */
    @Test
    public void testFetchAfterPartitionWithFetchedRecordsIsUnassigned() {
        buildFetcher(2);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        // Returns 3 records while `max.poll.records` is configured to 2
        client.prepareResponse(matchesOffset(tidp0, 1), fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));

        assertEquals(1, sendFetches());
        consumerClient.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchRecords();
        List<ConsumerRecord<byte[], byte[]>> recordsToTest = recordsByPartition.get(tp0);
        assertEquals(2, recordsToTest.size());
        assertEquals(3L, subscriptions.position(tp0).offset);
        assertEquals(1, recordsToTest.get(0).offset());
        assertEquals(2, recordsToTest.get(1).offset());

        assignFromUser(singleton(tp1));
        client.prepareResponse(matchesOffset(tidp1, 4), fullFetchResponse(tidp1, nextRecords, Errors.NONE, 100L, 0));
        subscriptions.seek(tp1, 4);

        assertEquals(1, sendFetches());
        consumerClient.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();
        assertNull(fetchedRecords.get(tp0));
        recordsToTest = fetchedRecords.get(tp1);
        assertEquals(2, recordsToTest.size());
        assertEquals(6L, subscriptions.position(tp1).offset);
        assertEquals(4, recordsToTest.get(0).offset());
        assertEquals(5, recordsToTest.get(1).offset());
    }

    @Test
    public void testFetchNonContinuousRecords() {
        // if we are fetching from a compacted topic, there may be gaps in the returned records
        // this test verifies the fetcher updates the current fetched/consumed positions correctly for this case
        buildFetcher();

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE,
                TimestampType.CREATE_TIME, 0L);
        builder.appendWithOffset(15L, 0L, "key".getBytes(), "value-1".getBytes());
        builder.appendWithOffset(20L, 0L, "key".getBytes(), "value-2".getBytes());
        builder.appendWithOffset(30L, 0L, "key".getBytes(), "value-3".getBytes());
        MemoryRecords records = builder.build();

        List<ConsumerRecord<byte[], byte[]>> consumerRecords;
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsByPartition = fetchRecords();
        consumerRecords = recordsByPartition.get(tp0);
        assertEquals(3, consumerRecords.size());
        assertEquals(31L, subscriptions.position(tp0).offset); // this is the next fetching position

        assertEquals(15L, consumerRecords.get(0).offset());
        assertEquals(20L, consumerRecords.get(1).offset());
        assertEquals(30L, consumerRecords.get(2).offset());
    }

    /**
     * Test the case where the client makes a pre-v3 FetchRequest, but the server replies with only a partial
     * request. This happens when a single message is larger than the per-partition limit.
     */
    @Test
    public void testFetchRequestWhenRecordTooLarge() {
        try {
            buildFetcher();

            client.setNodeApiVersions(NodeApiVersions.create(ApiKeys.FETCH.id, (short) 2, (short) 2));
            makeFetchRequestWithIncompleteRecord();
            assertThrows(RecordTooLargeException.class, this::collectFetch);
            // the position should not advance since no data has been returned
            assertEquals(0, subscriptions.position(tp0).offset);
        } finally {
            client.setNodeApiVersions(NodeApiVersions.create());
        }
    }

    /**
     * Test the case where the client makes a post KIP-74 FetchRequest, but the server replies with only a
     * partial request. For v3 and later FetchRequests, the implementation of KIP-74 changed the behavior
     * so that at least one message is always returned. Therefore, this case should not happen, and it indicates
     * that an internal error has taken place.
     */
    @Test
    public void testFetchRequestInternalError() {
        buildFetcher();
        makeFetchRequestWithIncompleteRecord();
        try {
            collectFetch();
            fail("collectFetch should have thrown a KafkaException");
        } catch (KafkaException e) {
            assertTrue(e.getMessage().startsWith("Failed to make progress reading messages"));
            // the position should not advance since no data has been returned
            assertEquals(0, subscriptions.position(tp0).offset);
        }
    }

    private void makeFetchRequestWithIncompleteRecord() {
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        MemoryRecords partialRecord = MemoryRecords.readableRecords(
            ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}));
        client.prepareResponse(fullFetchResponse(tidp0, partialRecord, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
    }

    @Test
    public void testUnauthorizedTopic() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.TOPIC_AUTHORIZATION_FAILED, 100L, 0));
        consumerClient.poll(time.timer(0));
        try {
            collectFetch();
            fail("collectFetch should have thrown a TopicAuthorizationException");
        } catch (TopicAuthorizationException e) {
            assertEquals(singleton(topicName), e.unauthorizedTopics());
        }
    }

    @Test
    public void testFetchDuringEagerRebalance() {
        buildFetcher();

        subscriptions.subscribe(singleton(topicName), Optional.empty());
        subscriptions.assignFromSubscribed(singleton(tp0));
        subscriptions.seek(tp0, 0);

        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(
            1, singletonMap(topicName, 4), tp -> validLeaderEpoch, topicIds));

        assertEquals(1, sendFetches());

        // Now the eager rebalance happens and fetch positions are cleared
        subscriptions.assignFromSubscribed(Collections.emptyList());

        subscriptions.assignFromSubscribed(singleton(tp0));
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        // The active fetch should be ignored since its position is no longer valid
        assertTrue(fetchRecords().isEmpty());
    }

    @Test
    public void testFetchDuringCooperativeRebalance() {
        buildFetcher();

        subscriptions.subscribe(singleton(topicName), Optional.empty());
        subscriptions.assignFromSubscribed(singleton(tp0));
        subscriptions.seek(tp0, 0);

        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(
            1, singletonMap(topicName, 4), tp -> validLeaderEpoch, topicIds));

        assertEquals(1, sendFetches());

        // Now the cooperative rebalance happens and fetch positions are NOT cleared for unrevoked partitions
        subscriptions.assignFromSubscribed(singleton(tp0));

        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();

        // The active fetch should NOT be ignored since the position for tp0 is still valid
        assertEquals(1, fetchedRecords.size());
        assertEquals(3, fetchedRecords.get(tp0).size());
    }

    @Test
    public void testInFlightFetchOnPausedPartition() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, sendFetches());
        subscriptions.pause(tp0);

        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertNull(fetchRecords().get(tp0));
    }

    @Test
    public void testFetchOnPausedPartition() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        subscriptions.pause(tp0);
        assertFalse(sendFetches() > 0);
        assertTrue(client.requests().isEmpty());
    }

    @Test
    public void testFetchOnCompletedFetchesForPausedAndResumedPartitions() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, sendFetches());

        subscriptions.pause(tp0);

        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        assertEmptyFetch("Should not return any records or advance position when partition is paused");

        assertTrue(fetcher.hasCompletedFetches(), "Should still contain completed fetches");
        assertFalse(fetcher.hasAvailableFetches(), "Should not have any available (non-paused) completed fetches");
        assertEquals(0, sendFetches());

        subscriptions.resume(tp0);

        assertTrue(fetcher.hasAvailableFetches(), "Should have available (non-paused) completed fetches");

        consumerClient.poll(time.timer(0));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();
        assertEquals(1, fetchedRecords.size(), "Should return records when partition is resumed");
        assertNotNull(fetchedRecords.get(tp0));
        assertEquals(3, fetchedRecords.get(tp0).size());

        consumerClient.poll(time.timer(0));
        assertEmptyFetch("Should not return records or advance position after previously paused partitions are fetched");
        assertFalse(fetcher.hasCompletedFetches(), "Should no longer contain completed fetches");
    }

    @Test
    public void testFetchOnCompletedFetchesForSomePausedPartitions() {
        buildFetcher();

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords;

        assignFromUser(Set.of(tp0, tp1));

        // seek to tp0 and tp1 in two polls to generate 2 complete requests and responses

        // #1 seek, request, poll, response
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(1, Optional.empty(), metadata.currentLeader(tp0)));
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        // #2 seek, request, poll, response
        subscriptions.seekUnvalidated(tp1, new SubscriptionState.FetchPosition(1, Optional.empty(), metadata.currentLeader(tp1)));
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp1, nextRecords, Errors.NONE, 100L, 0));

        subscriptions.pause(tp0);
        consumerClient.poll(time.timer(0));

        fetchedRecords = fetchRecords();
        assertEquals(1, fetchedRecords.size(), "Should return completed fetch for unpaused partitions");
        assertTrue(fetcher.hasCompletedFetches(), "Should still contain completed fetches");
        assertNotNull(fetchedRecords.get(tp1));
        assertNull(fetchedRecords.get(tp0));

        assertEmptyFetch("Should not return records or advance position for remaining paused partition");
        assertTrue(fetcher.hasCompletedFetches(), "Should still contain completed fetches");
    }

    @Test
    public void testFetchOnCompletedFetchesForAllPausedPartitions() {
        buildFetcher();

        assignFromUser(Set.of(tp0, tp1));

        // seek to tp0 and tp1 in two polls to generate 2 complete requests and responses

        // #1 seek, request, poll, response
        subscriptions.seekUnvalidated(tp0, new SubscriptionState.FetchPosition(1, Optional.empty(), metadata.currentLeader(tp0)));
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        // #2 seek, request, poll, response
        subscriptions.seekUnvalidated(tp1, new SubscriptionState.FetchPosition(1, Optional.empty(), metadata.currentLeader(tp1)));
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp1, nextRecords, Errors.NONE, 100L, 0));

        subscriptions.pause(tp0);
        subscriptions.pause(tp1);

        consumerClient.poll(time.timer(0));

        assertEmptyFetch("Should not return records or advance position for all paused partitions");
        assertTrue(fetcher.hasCompletedFetches(), "Should still contain completed fetches");
        assertFalse(fetcher.hasAvailableFetches(), "Should not have any available (non-paused) completed fetches");
    }

    @Test
    public void testPartialFetchWithPausedPartitions() {
        // this test sends creates a completed fetch with 3 records and a max poll of 2 records to assert
        // that a fetch that must be returned over at least 2 polls can be cached successfully when its partition is
        // paused, then returned successfully after it has been resumed again later
        buildFetcher(2);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords;

        assignFromUser(Set.of(tp0, tp1));

        subscriptions.seek(tp0, 1);
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        fetchedRecords = fetchRecords();

        assertEquals(2, fetchedRecords.get(tp0).size(), "Should return 2 records from fetch with 3 records");
        assertFalse(fetcher.hasCompletedFetches(), "Should have no completed fetches");

        subscriptions.pause(tp0);
        consumerClient.poll(time.timer(0));

        assertEmptyFetch("Should not return records or advance position for paused partitions");
        assertTrue(fetcher.hasCompletedFetches(), "Should have 1 entry in completed fetches");
        assertFalse(fetcher.hasAvailableFetches(), "Should not have any available (non-paused) completed fetches");

        subscriptions.resume(tp0);

        consumerClient.poll(time.timer(0));

        fetchedRecords = fetchRecords();

        assertEquals(1, fetchedRecords.get(tp0).size(), "Should return last remaining record");
        assertFalse(fetcher.hasCompletedFetches(), "Should have no completed fetches");
    }

    @Test
    public void testFetchDiscardedAfterPausedPartitionResumedAndSoughtToNewOffset() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, sendFetches());
        subscriptions.pause(tp0);
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));

        subscriptions.seek(tp0, 3);
        subscriptions.resume(tp0);
        consumerClient.poll(time.timer(0));

        assertTrue(fetcher.hasCompletedFetches(), "Should have 1 entry in completed fetches");
        Fetch<byte[], byte[]> fetch = collectFetch();
        assertEquals(emptyMap(), fetch.records(), "Should not return any records because we sought to a new offset");
        assertFalse(fetch.positionAdvanced());
        assertFalse(fetcher.hasCompletedFetches(), "Should have no completed fetches");
    }

    @ParameterizedTest
    @MethodSource("handleFetchResponseErrorSupplier")
    public void testHandleFetchResponseError(Errors error,
                                             long highWatermark,
                                             boolean hasTopLevelError,
                                             boolean shouldRequestMetadataUpdate) {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, sendFetches());

        final FetchResponse fetchResponse;

        if (hasTopLevelError)
            fetchResponse = fetchResponseWithTopLevelError(tidp0, error, 0);
        else
            fetchResponse = fullFetchResponse(tidp0, records, error, highWatermark, 0);

        client.prepareResponse(fetchResponse);
        consumerClient.poll(time.timer(0));

        assertEmptyFetch("Should not return records or advance position on fetch error");

        long timeToNextUpdate = metadata.timeToNextUpdate(time.milliseconds());

        if (shouldRequestMetadataUpdate)
            assertEquals(0L, timeToNextUpdate, "Should have requested metadata update");
        else
            assertNotEquals(0L, timeToNextUpdate, "Should not have requested metadata update");
    }

    /**
     * Supplies parameters to {@link #testHandleFetchResponseError(Errors, long, boolean, boolean)}.
     */
    private static Stream<Arguments> handleFetchResponseErrorSupplier() {
        return Stream.of(
                Arguments.of(Errors.NOT_LEADER_OR_FOLLOWER, 100L, false, true),
                Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, 100L, false, true),
                Arguments.of(Errors.UNKNOWN_TOPIC_ID, -1L, false, true),
                Arguments.of(Errors.FETCH_SESSION_TOPIC_ID_ERROR, -1L, true, true),
                Arguments.of(Errors.INCONSISTENT_TOPIC_ID, -1L, false, true),
                Arguments.of(Errors.FENCED_LEADER_EPOCH, 100L, false, true),
                Arguments.of(Errors.UNKNOWN_LEADER_EPOCH, 100L, false, false)
        );
    }

    @Test
    public void testEpochSetInFetchRequest() {
        buildFetcher();
        subscriptions.assignFromUser(singleton(tp0));
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWithIds("dummy", 1,
                Collections.emptyMap(), Collections.singletonMap(topicName, 4), tp -> 99, topicIds);
        client.updateMetadata(metadataResponse);

        subscriptions.seek(tp0, 10);
        assertEquals(1, sendFetches());

        // Check for epoch in outgoing request
        MockClient.RequestMatcher matcher = body -> {
            if (body instanceof FetchRequest) {
                FetchRequest fetchRequest = (FetchRequest) body;
                fetchRequest.fetchData(topicNames).values().forEach(partitionData -> {
                    assertTrue(partitionData.currentLeaderEpoch.isPresent(), "Expected Fetcher to set leader epoch in request");
                    assertEquals(99, partitionData.currentLeaderEpoch.get().longValue(), "Expected leader epoch to match epoch from metadata update");
                });
                return true;
            } else {
                fail("Should have seen FetchRequest");
                return false;
            }
        };
        client.prepareResponse(matcher, fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.pollNoWakeup();
        assertEquals(0, consumerClient.pendingRequestCount());
    }

    @Test
    public void testFetchOffsetOutOfRange() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertEmptyFetch("Should not return records or advance position on fetch error");
        assertTrue(subscriptions.isOffsetResetNeeded(tp0));
        assertNull(subscriptions.validPosition(tp0));
        assertNull(subscriptions.position(tp0));
    }

    @Test
    public void testStaleOutOfRangeError() {
        // verify that an out of range error which arrives after a seek
        // does not cause us to reset our position or throw an exception
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        subscriptions.seek(tp0, 1);
        consumerClient.poll(time.timer(0));
        assertEmptyFetch("Should not return records or advance position on fetch error");
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertEquals(1, subscriptions.position(tp0).offset);
    }

    @Test
    public void testFetchedRecordsAfterSeek() {
        buildFetcher(AutoOffsetResetStrategy.NONE, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), 2, IsolationLevel.READ_UNCOMMITTED);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertTrue(sendFetches() > 0);
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        consumerClient.poll(time.timer(0));

        // The partition is not marked as needing its offset reset because that error handling logic is
        // performed during the fetch collection. When we call seek() before we collect the fetch, the
        // partition's position is updated (to offset 2) which is different from the offset from which
        // we fetched the data (from offset 0).
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        subscriptions.seek(tp0, 2);
        assertEmptyFetch("Should not return records or advance position after seeking to end of topic partition");
    }

    @Test
    public void testFetchOffsetOutOfRangeException() {
        buildFetcher(AutoOffsetResetStrategy.NONE, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), 2, IsolationLevel.READ_UNCOMMITTED);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        sendFetches();
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.OFFSET_OUT_OF_RANGE, 100L, 0));
        consumerClient.poll(time.timer(0));

        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        for (int i = 0; i < 2; i++) {
            OffsetOutOfRangeException e = assertThrows(OffsetOutOfRangeException.class, this::collectFetch);
            assertEquals(singleton(tp0), e.offsetOutOfRangePartitions().keySet());
            assertEquals(0L, e.offsetOutOfRangePartitions().get(tp0).longValue());
        }
    }

    @Test
    public void testFetchPositionAfterException() {
        // verify the advancement in the next fetch offset equals to the number of fetched records when
        // some fetched partitions cause Exception. This ensures that consumer won't lose record upon exception
        buildFetcher(AutoOffsetResetStrategy.NONE, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);
        assignFromUser(Set.of(tp0, tp1));
        subscriptions.seek(tp0, 1);
        subscriptions.seek(tp1, 1);

        assertEquals(1, sendFetches());

        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = new LinkedHashMap<>();
        partitions.put(tidp1, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp1.partition())
                .setHighWatermark(100)
                .setRecords(records));
        partitions.put(tidp0, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition())
                .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code())
                .setHighWatermark(100));
        client.prepareResponse(FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, new LinkedHashMap<>(partitions)));
        consumerClient.poll(time.timer(0));

        List<ConsumerRecord<byte[], byte[]>> allFetchedRecords = new ArrayList<>();
        fetchRecordsInto(allFetchedRecords);

        assertEquals(1, subscriptions.position(tp0).offset);
        assertEquals(4, subscriptions.position(tp1).offset);
        assertEquals(3, allFetchedRecords.size());

        OffsetOutOfRangeException e = assertThrows(OffsetOutOfRangeException.class, () ->
                fetchRecordsInto(allFetchedRecords));

        assertEquals(singleton(tp0), e.offsetOutOfRangePartitions().keySet());
        assertEquals(1L, e.offsetOutOfRangePartitions().get(tp0).longValue());

        assertEquals(1, subscriptions.position(tp0).offset);
        assertEquals(4, subscriptions.position(tp1).offset);
        assertEquals(3, allFetchedRecords.size());
    }

    private void fetchRecordsInto(List<ConsumerRecord<byte[], byte[]>> allFetchedRecords) {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();
        fetchedRecords.values().forEach(allFetchedRecords::addAll);
    }

    @Test
    public void testCompletedFetchRemoval() {
        // Ensure the removal of completed fetches that cause an Exception if and only if they contain empty records.
        buildFetcher(AutoOffsetResetStrategy.NONE, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);
        assignFromUser(Set.of(tp0, tp1, tp2, tp3));

        subscriptions.seek(tp0, 1);
        subscriptions.seek(tp1, 1);
        subscriptions.seek(tp2, 1);
        subscriptions.seek(tp3, 1);

        assertEquals(1, sendFetches());

        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = new LinkedHashMap<>();
        partitions.put(tidp1, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp1.partition())
                .setHighWatermark(100)
                .setRecords(records));
        partitions.put(tidp0, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition())
                .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code())
                .setHighWatermark(100));
        partitions.put(tidp2, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp2.partition())
                .setHighWatermark(100)
                .setLastStableOffset(4)
                .setLogStartOffset(0)
                .setRecords(nextRecords));
        partitions.put(tidp3, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp3.partition())
                .setHighWatermark(100)
                .setLastStableOffset(4)
                .setLogStartOffset(0)
                .setRecords(partialRecords));
        client.prepareResponse(FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, new LinkedHashMap<>(partitions)));
        consumerClient.poll(time.timer(0));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = new ArrayList<>();
        fetchRecordsInto(fetchedRecords);

        assertEquals(fetchedRecords.size(), subscriptions.position(tp1).offset - 1);
        assertEquals(4, subscriptions.position(tp1).offset);
        assertEquals(3, fetchedRecords.size());

        List<OffsetOutOfRangeException> oorExceptions = new ArrayList<>();
        try {
            fetchRecordsInto(fetchedRecords);
        } catch (OffsetOutOfRangeException oor) {
            oorExceptions.add(oor);
        }

        // Should have received one OffsetOutOfRangeException for partition tp1
        assertEquals(1, oorExceptions.size());
        OffsetOutOfRangeException oor = oorExceptions.get(0);
        assertTrue(oor.offsetOutOfRangePartitions().containsKey(tp0));
        assertEquals(oor.offsetOutOfRangePartitions().size(), 1);

        fetchRecordsInto(fetchedRecords);

        // Should not have received an Exception for tp2.
        assertEquals(6, subscriptions.position(tp2).offset);
        assertEquals(5, fetchedRecords.size());

        int numExceptionsExpected = 3;
        List<KafkaException> kafkaExceptions = new ArrayList<>();
        for (int i = 1; i <= numExceptionsExpected; i++) {
            try {
                fetchRecordsInto(fetchedRecords);
            } catch (KafkaException e) {
                kafkaExceptions.add(e);
            }
        }
        // Should have received as much as numExceptionsExpected Kafka exceptions for tp3.
        assertEquals(numExceptionsExpected, kafkaExceptions.size());
    }

    @Test
    public void testSeekBeforeException() {
        buildFetcher(AutoOffsetResetStrategy.NONE, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), 2, IsolationLevel.READ_UNCOMMITTED);

        assignFromUser(Set.of(tp0));
        subscriptions.seek(tp0, 1);
        assertEquals(1, sendFetches());
        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = new HashMap<>();
        partitions.put(tidp0, new FetchResponseData.PartitionData()
                        .setPartitionIndex(tp0.partition())
                        .setHighWatermark(100)
                        .setRecords(records));
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));

        assertEquals(2, fetchRecords().get(tp0).size());

        subscriptions.assignFromUser(Set.of(tp0, tp1));
        subscriptions.seekUnvalidated(tp1, new SubscriptionState.FetchPosition(1, Optional.empty(), metadata.currentLeader(tp1)));

        assertEquals(1, sendFetches());
        partitions = new HashMap<>();
        partitions.put(tidp1, new FetchResponseData.PartitionData()
                        .setPartitionIndex(tp1.partition())
                        .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code())
                        .setHighWatermark(100));
        client.prepareResponse(FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, new LinkedHashMap<>(partitions)));
        consumerClient.poll(time.timer(0));
        assertEquals(1, fetchRecords().get(tp0).size());

        subscriptions.seek(tp1, 10);
        // Should not throw OffsetOutOfRangeException after the seek
        assertEmptyFetch("Should not return records or advance position after seeking to end of topic partitions");
    }

    @Test
    public void testFetchDisconnected() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0), true);
        consumerClient.poll(time.timer(0));
        assertEmptyFetch("Should not return records or advance position on disconnect");

        // disconnects should have no effect on subscription state
        assertFalse(subscriptions.isOffsetResetNeeded(tp0));
        assertTrue(subscriptions.isFetchable(tp0));
        assertEquals(0, subscriptions.position(tp0).offset);
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    public void testQuotaMetrics() {
        buildFetcher();

        MockSelector selector = new MockSelector(time);
        Cluster cluster = TestUtils.singletonCluster("test", 1);
        Node node = cluster.nodes().get(0);
        NetworkClient client = new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE,
                1000, 1000, 64 * 1024, 64 * 1024, 1000, 10 * 1000, 127 * 1000,
                time, true, new ApiVersions(), metricsManager.throttleTimeSensor(), new LogContext(),
                MetadataRecoveryStrategy.NONE);

        ApiVersionsResponse apiVersionsResponse = TestUtils.defaultApiVersionsResponse(
            400, ApiMessageType.ListenerType.ZK_BROKER);
        ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(apiVersionsResponse, ApiKeys.API_VERSIONS.latestVersion(), 0);

        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
        while (!client.ready(node, time.milliseconds())) {
            client.poll(1, time.milliseconds());
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.milliseconds()));
        }
        selector.clear();

        for (int i = 1; i <= 3; i++) {
            int throttleTimeMs = 100 * i;
            FetchRequest.Builder builder = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion(), 100, 100, new LinkedHashMap<>());
            builder.rackId("");
            ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true);
            client.send(request, time.milliseconds());
            client.poll(1, time.milliseconds());
            FetchResponse response = fullFetchResponse(tidp0, nextRecords, Errors.NONE, i, throttleTimeMs);
            buffer = RequestTestUtils.serializeResponseWithHeader(response, ApiKeys.FETCH.latestVersion(), request.correlationId());
            selector.completeReceive(new NetworkReceive(node.idString(), buffer));
            client.poll(1, time.milliseconds());
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.milliseconds()));
            selector.clear();
        }
        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric avgMetric = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchThrottleTimeAvg));
        KafkaMetric maxMetric = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchThrottleTimeMax));
        // Throttle times are ApiVersions=400, Fetch=(100, 200, 300)
        assertEquals(250, (Double) avgMetric.metricValue(), EPSILON);
        assertEquals(400, (Double) maxMetric.metricValue(), EPSILON);
        client.close();
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    public void testFetcherMetrics() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        MetricName maxLagMetric = metrics.metricInstance(metricsRegistry.recordsLagMax);
        Map<String, String> tags = new HashMap<>();
        tags.put("topic", tp0.topic());
        tags.put("partition", String.valueOf(tp0.partition()));
        MetricName partitionLagMetric = metrics.metricName("records-lag", metricGroup, tags);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric recordsFetchLagMax = allMetrics.get(maxLagMetric);

        // recordsFetchLagMax should be initialized to NaN
        assertEquals(Double.NaN, (Double) recordsFetchLagMax.metricValue(), EPSILON);

        // recordsFetchLagMax should be hw - fetchOffset after receiving an empty FetchResponse
        fetchRecords(tidp0, MemoryRecords.EMPTY, Errors.NONE, 100L, 0);
        assertEquals(100, (Double) recordsFetchLagMax.metricValue(), EPSILON);

        KafkaMetric partitionLag = allMetrics.get(partitionLagMetric);
        assertEquals(100, (Double) partitionLag.metricValue(), EPSILON);

        // recordsFetchLagMax should be hw - offset of the last message after receiving a non-empty FetchResponse
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        fetchRecords(tidp0, builder.build(), Errors.NONE, 200L, 0);
        assertEquals(197, (Double) recordsFetchLagMax.metricValue(), EPSILON);
        assertEquals(197, (Double) partitionLag.metricValue(), EPSILON);

        // verify de-registration of partition lag
        subscriptions.unsubscribe();
        sendFetches();
        assertFalse(allMetrics.containsKey(partitionLagMetric));
    }

    @Test
    public void testFetcherLeadMetric() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        MetricName minLeadMetric = metrics.metricInstance(metricsRegistry.recordsLeadMin);
        Map<String, String> tags = new HashMap<>(2);
        tags.put("topic", tp0.topic());
        tags.put("partition", String.valueOf(tp0.partition()));
        MetricName partitionLeadMetric = metrics.metricName("records-lead", metricGroup, "", tags);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric recordsFetchLeadMin = allMetrics.get(minLeadMetric);

        // recordsFetchLeadMin should be initialized to NaN
        assertEquals(Double.NaN, (Double) recordsFetchLeadMin.metricValue(), EPSILON);

        // recordsFetchLeadMin should be position - logStartOffset after receiving an empty FetchResponse
        fetchRecords(tidp0, MemoryRecords.EMPTY, Errors.NONE, 100L, -1L, 0L, 0);
        assertEquals(0L, (Double) recordsFetchLeadMin.metricValue(), EPSILON);

        KafkaMetric partitionLead = allMetrics.get(partitionLeadMetric);
        assertEquals(0L, (Double) partitionLead.metricValue(), EPSILON);

        // recordsFetchLeadMin should be position - logStartOffset after receiving a non-empty FetchResponse
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++) {
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        }
        fetchRecords(tidp0, builder.build(), Errors.NONE, 200L, -1L, 0L, 0);
        assertEquals(0L, (Double) recordsFetchLeadMin.metricValue(), EPSILON);
        assertEquals(3L, (Double) partitionLead.metricValue(), EPSILON);

        // verify de-registration of partition lag
        subscriptions.unsubscribe();
        sendFetches();
        assertFalse(allMetrics.containsKey(partitionLeadMetric));
    }

    @Test
    public void testReadCommittedLagMetric() {
        buildFetcher(AutoOffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        MetricName maxLagMetric = metrics.metricInstance(metricsRegistry.recordsLagMax);

        Map<String, String> tags = new HashMap<>();
        tags.put("topic", tp0.topic());
        tags.put("partition", String.valueOf(tp0.partition()));
        MetricName partitionLagMetric = metrics.metricName("records-lag", metricGroup, tags);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric recordsFetchLagMax = allMetrics.get(maxLagMetric);

        // recordsFetchLagMax should be initialized to NaN
        assertEquals(Double.NaN, (Double) recordsFetchLagMax.metricValue(), EPSILON);

        // recordsFetchLagMax should be lso - fetchOffset after receiving an empty FetchResponse
        fetchRecords(tidp0, MemoryRecords.EMPTY, Errors.NONE, 100L, 50L, 0);
        assertEquals(50, (Double) recordsFetchLagMax.metricValue(), EPSILON);

        KafkaMetric partitionLag = allMetrics.get(partitionLagMetric);
        assertEquals(50, (Double) partitionLag.metricValue(), EPSILON);

        // recordsFetchLagMax should be lso - offset of the last message after receiving a non-empty FetchResponse
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        fetchRecords(tidp0, builder.build(), Errors.NONE, 200L, 150L, 0);
        assertEquals(147, (Double) recordsFetchLagMax.metricValue(), EPSILON);
        assertEquals(147, (Double) partitionLag.metricValue(), EPSILON);

        // verify de-registration of partition lag
        subscriptions.unsubscribe();
        sendFetches();
        assertFalse(allMetrics.containsKey(partitionLagMetric));
    }

    @Test
    public void testFetchResponseMetrics() {
        buildFetcher();

        String topic1 = "foo";
        String topic2 = "bar";
        TopicPartition tp1 = new TopicPartition(topic1, 0);
        TopicPartition tp2 = new TopicPartition(topic2, 0);

        subscriptions.assignFromUser(Set.of(tp1, tp2));

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(topic1, 1);
        partitionCounts.put(topic2, 1);
        topicIds.put(topic1, Uuid.randomUuid());
        topicIds.put(topic2, Uuid.randomUuid());
        TopicIdPartition tidp1 = new TopicIdPartition(topicIds.get(topic1), tp1);
        TopicIdPartition tidp2 = new TopicIdPartition(topicIds.get(topic2), tp2);
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, partitionCounts, tp -> validLeaderEpoch, topicIds));

        int expectedBytes = 0;
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> fetchPartitionData = new LinkedHashMap<>();

        for (TopicIdPartition tp : Set.of(tidp1, tidp2)) {
            subscriptions.seek(tp.topicPartition(), 0);

            MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE,
                    TimestampType.CREATE_TIME, 0L);
            for (int v = 0; v < 3; v++)
                builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
            MemoryRecords records = builder.build();
            for (Record record : records.records())
                expectedBytes += record.sizeInBytes();

            fetchPartitionData.put(tp, new FetchResponseData.PartitionData()
                    .setPartitionIndex(tp.topicPartition().partition())
                    .setHighWatermark(15)
                    .setLogStartOffset(0)
                    .setRecords(records));
        }

        assertEquals(1, sendFetches());
        client.prepareResponse(FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, fetchPartitionData));
        consumerClient.poll(time.timer(0));

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();
        assertEquals(3, fetchedRecords.get(tp1).size());
        assertEquals(3, fetchedRecords.get(tp2).size());

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchSizeAvg));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg));
        assertEquals(expectedBytes, (Double) fetchSizeAverage.metricValue(), EPSILON);
        assertEquals(6, (Double) recordsCountAverage.metricValue(), EPSILON);
    }

    @Test
    public void testFetchResponseMetricsWithSkippedOffset() {
        buildFetcher();

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 1);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchSizeAvg));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg));

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        MemoryRecords records = builder.build();

        int expectedBytes = 0;
        for (Record record : records.records()) {
            if (record.offset() >= 1)
                expectedBytes += record.sizeInBytes();
        }

        fetchRecords(tidp0, records, Errors.NONE, 100L, 0);
        assertEquals(expectedBytes, (Double) fetchSizeAverage.metricValue(), EPSILON);
        assertEquals(2, (Double) recordsCountAverage.metricValue(), EPSILON);
    }

    @Test
    public void testFetchResponseMetricsWithOnePartitionError() {
        buildFetcher();
        assignFromUser(Set.of(tp0, tp1));
        subscriptions.seek(tp0, 0);
        subscriptions.seek(tp1, 0);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchSizeAvg));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg));

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        MemoryRecords records = builder.build();

        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = new HashMap<>();
        partitions.put(tidp0, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition())
                .setHighWatermark(100)
                .setLogStartOffset(0)
                .setRecords(records));
        partitions.put(tidp1, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp1.partition())
                .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code())
                .setHighWatermark(100)
                .setLogStartOffset(0));

        assertEquals(1, sendFetches());
        client.prepareResponse(FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, new LinkedHashMap<>(partitions)));
        consumerClient.poll(time.timer(0));
        collectFetch();

        int expectedBytes = 0;
        for (Record record : records.records())
            expectedBytes += record.sizeInBytes();

        assertEquals(expectedBytes, (Double) fetchSizeAverage.metricValue(), EPSILON);
        assertEquals(3, (Double) recordsCountAverage.metricValue(), EPSILON);
    }

    @Test
    public void testFetchResponseMetricsWithOnePartitionAtTheWrongOffset() {
        buildFetcher();

        assignFromUser(Set.of(tp0, tp1));
        subscriptions.seek(tp0, 0);
        subscriptions.seek(tp1, 0);

        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric fetchSizeAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.fetchSizeAvg));
        KafkaMetric recordsCountAverage = allMetrics.get(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg));

        // send the fetch and then seek to a new offset
        assertEquals(1, sendFetches());
        subscriptions.seek(tp1, 5);

        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int v = 0; v < 3; v++)
            builder.appendWithOffset(v, RecordBatch.NO_TIMESTAMP, "key".getBytes(), ("value-" + v).getBytes());
        MemoryRecords records = builder.build();

        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = new HashMap<>();
        partitions.put(tidp0, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition())
                .setHighWatermark(100)
                .setLogStartOffset(0)
                .setRecords(records));
        partitions.put(tidp1, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp1.partition())
                .setHighWatermark(100)
                .setLogStartOffset(0)
                .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("val".getBytes()))));

        client.prepareResponse(FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, new LinkedHashMap<>(partitions)));
        consumerClient.poll(time.timer(0));
        collectFetch();

        // we should have ignored the record at the wrong offset
        int expectedBytes = 0;
        for (Record record : records.records())
            expectedBytes += record.sizeInBytes();

        assertEquals(expectedBytes, (Double) fetchSizeAverage.metricValue(), EPSILON);
        assertEquals(3, (Double) recordsCountAverage.metricValue(), EPSILON);
    }

    @Test
    public void testFetcherMetricsTemplates() {
        Map<String, String> clientTags = Collections.singletonMap("client-id", "clientA");
        buildFetcher(new MetricConfig().tags(clientTags), AutoOffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);

        // Fetch from topic to generate topic metrics
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        // Verify that all metrics except metrics-count have registered templates
        Set<MetricNameTemplate> allMetrics = new HashSet<>();
        for (MetricName n : metrics.metrics().keySet()) {
            String name = n.name().replaceAll(tp0.toString(), "{topic}-{partition}");
            if (!n.group().equals("kafka-metrics-count"))
                allMetrics.add(new MetricNameTemplate(name, n.group(), "", n.tags().keySet()));
        }
        TestUtils.checkEquals(allMetrics, new HashSet<>(metricsRegistry.getAllTemplates()), "metrics", "templates");
    }

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchRecords(
            TopicIdPartition tp, MemoryRecords records, Errors error, long hw, int throttleTime) {
        return fetchRecords(tp, records, error, hw, FetchResponse.INVALID_LAST_STABLE_OFFSET, throttleTime);
    }

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchRecords(
            TopicIdPartition tp, MemoryRecords records, Errors error, long hw, long lastStableOffset, int throttleTime) {
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tp, records, error, hw, lastStableOffset, throttleTime));
        consumerClient.poll(time.timer(0));
        return fetchRecords();
    }

    private Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchRecords(
            TopicIdPartition tp, MemoryRecords records, Errors error, long hw, long lastStableOffset, long logStartOffset, int throttleTime) {
        assertEquals(1, sendFetches());
        client.prepareResponse(fetchResponse(tp, records, error, hw, lastStableOffset, logStartOffset, throttleTime));
        consumerClient.poll(time.timer(0));
        return fetchRecords();
    }

    @Test
    public void testSkippingAbortedTransactions() {
        buildFetcher(AutoOffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int currentOffset = 0;

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));

        abortTransaction(buffer, 1L, currentOffset);

        buffer.flip();

        List<FetchResponseData.AbortedTransaction> abortedTransactions = Collections.singletonList(
                new FetchResponseData.AbortedTransaction().setProducerId(1).setFirstOffset(0));
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Fetch<byte[], byte[]> fetch = collectFetch();
        assertEquals(emptyMap(), fetch.records());
        assertTrue(fetch.positionAdvanced());
    }

    @Test
    public void testReturnCommittedTransactions() {
        buildFetcher(AutoOffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int currentOffset = 0;

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));

        commitTransaction(buffer, 1L, currentOffset);
        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        client.prepareResponse(body -> {
            FetchRequest request = (FetchRequest) body;
            assertEquals(IsolationLevel.READ_COMMITTED, request.isolationLevel());
            return true;
        }, fullFetchResponseWithAbortedTransactions(records, Collections.emptyList(), Errors.NONE, 100L, 100L, 0));

        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
        assertEquals(fetchedRecords.get(tp0).size(), 2);
    }

    @Test
    public void testReadCommittedWithCommittedAndAbortedTransactions() {
        buildFetcher(AutoOffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        List<FetchResponseData.AbortedTransaction> abortedTransactions = new ArrayList<>();

        long pid1 = 1L;
        long pid2 = 2L;

        // Appends for producer 1 (eventually committed)
        appendTransactionalRecords(buffer, pid1, 0L,
                new SimpleRecord("commit1-1".getBytes(), "value".getBytes()),
                new SimpleRecord("commit1-2".getBytes(), "value".getBytes()));

        // Appends for producer 2 (eventually aborted)
        appendTransactionalRecords(buffer, pid2, 2L,
                new SimpleRecord("abort2-1".getBytes(), "value".getBytes()));

        // commit producer 1
        commitTransaction(buffer, pid1, 3L);

        // append more for producer 2 (eventually aborted)
        appendTransactionalRecords(buffer, pid2, 4L,
                new SimpleRecord("abort2-2".getBytes(), "value".getBytes()));

        // abort producer 2
        abortTransaction(buffer, pid2, 5L);
        abortedTransactions.add(new FetchResponseData.AbortedTransaction().setProducerId(pid2).setFirstOffset(2L));

        // New transaction for producer 1 (eventually aborted)
        appendTransactionalRecords(buffer, pid1, 6L,
                new SimpleRecord("abort1-1".getBytes(), "value".getBytes()));

        // New transaction for producer 2 (eventually committed)
        appendTransactionalRecords(buffer, pid2, 7L,
                new SimpleRecord("commit2-1".getBytes(), "value".getBytes()));

        // Add messages for producer 1 (eventually aborted)
        appendTransactionalRecords(buffer, pid1, 8L,
                new SimpleRecord("abort1-2".getBytes(), "value".getBytes()));

        // abort producer 1
        abortTransaction(buffer, pid1, 9L);
        abortedTransactions.add(new FetchResponseData.AbortedTransaction().setProducerId(1).setFirstOffset(6));

        // commit producer 2
        commitTransaction(buffer, pid2, 10L);

        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
        // There are only 3 committed records
        List<ConsumerRecord<byte[], byte[]>> fetchedConsumerRecords = fetchedRecords.get(tp0);
        Set<String> fetchedKeys = new HashSet<>();
        for (ConsumerRecord<byte[], byte[]> consumerRecord : fetchedConsumerRecords) {
            fetchedKeys.add(new String(consumerRecord.key(), StandardCharsets.UTF_8));
        }
        assertEquals(Set.of("commit1-1", "commit1-2", "commit2-1"), fetchedKeys);
    }

    @Test
    public void testMultipleAbortMarkers() {
        buildFetcher(AutoOffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int currentOffset = 0;

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "abort1-1".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "abort1-2".getBytes(), "value".getBytes()));

        currentOffset += abortTransaction(buffer, 1L, currentOffset);
        // Duplicate abort -- should be ignored.
        currentOffset += abortTransaction(buffer, 1L, currentOffset);
        // Now commit a transaction.
        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "commit1-1".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "commit1-2".getBytes(), "value".getBytes()));
        commitTransaction(buffer, 1L, currentOffset);
        buffer.flip();

        List<FetchResponseData.AbortedTransaction> abortedTransactions = Collections.singletonList(
            new FetchResponseData.AbortedTransaction().setProducerId(1).setFirstOffset(0)
        );
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
        assertEquals(fetchedRecords.get(tp0).size(), 2);
        List<ConsumerRecord<byte[], byte[]>> fetchedConsumerRecords = fetchedRecords.get(tp0);
        Set<String> expectedCommittedKeys = new HashSet<>(Arrays.asList("commit1-1", "commit1-2"));
        Set<String> actuallyCommittedKeys = new HashSet<>();
        for (ConsumerRecord<byte[], byte[]> consumerRecord : fetchedConsumerRecords) {
            actuallyCommittedKeys.add(new String(consumerRecord.key(), StandardCharsets.UTF_8));
        }
        assertEquals(expectedCommittedKeys, actuallyCommittedKeys);
    }

    @Test
    public void testReadCommittedAbortMarkerWithNoData() {
        buildFetcher(AutoOffsetResetStrategy.EARLIEST, new StringDeserializer(),
                new StringDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        long producerId = 1L;

        abortTransaction(buffer, producerId, 5L);

        appendTransactionalRecords(buffer, producerId, 6L,
                new SimpleRecord("6".getBytes(), null),
                new SimpleRecord("7".getBytes(), null),
                new SimpleRecord("8".getBytes(), null));

        commitTransaction(buffer, producerId, 9L);

        buffer.flip();

        // send the fetch
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, sendFetches());

        // prepare the response. the aborted transactions begin at offsets which are no longer in the log
        List<FetchResponseData.AbortedTransaction> abortedTransactions = Collections.singletonList(
            new FetchResponseData.AbortedTransaction().setProducerId(producerId).setFirstOffset(0L));

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(MemoryRecords.readableRecords(buffer),
                abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<String, String>>> allFetchedRecords = fetchRecords();
        assertTrue(allFetchedRecords.containsKey(tp0));
        List<ConsumerRecord<String, String>> fetchedRecords = allFetchedRecords.get(tp0);
        assertEquals(3, fetchedRecords.size());
        assertEquals(Arrays.asList(6L, 7L, 8L), collectRecordOffsets(fetchedRecords));
    }

    @Test
    public void testUpdatePositionWithLastRecordMissingFromBatch() {
        buildFetcher();

        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE,
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

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, compactedRecords, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> allFetchedRecords = fetchRecords();
        assertTrue(allFetchedRecords.containsKey(tp0));
        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = allFetchedRecords.get(tp0);
        assertEquals(3, fetchedRecords.size());

        for (int i = 0; i < 3; i++) {
            assertEquals(Integer.toString(i), new String(fetchedRecords.get(i).key()));
        }

        // The next offset should point to the next batch
        assertEquals(4L, subscriptions.position(tp0).offset);
    }

    @Test
    public void testUpdatePositionOnEmptyBatch() {
        buildFetcher();

        long producerId = 1;
        short producerEpoch = 0;
        int sequence = 1;
        long baseOffset = 37;
        long lastOffset = 54;
        int partitionLeaderEpoch = 7;
        ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        DefaultRecordBatch.writeEmptyHeader(buffer, RecordBatch.CURRENT_MAGIC_VALUE, producerId, producerEpoch,
                sequence, baseOffset, lastOffset, partitionLeaderEpoch, TimestampType.CREATE_TIME,
                System.currentTimeMillis(), false, false);
        buffer.flip();
        MemoryRecords recordsWithEmptyBatch = MemoryRecords.readableRecords(buffer);

        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tidp0, recordsWithEmptyBatch, Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Fetch<byte[], byte[]> fetch = collectFetch();
        assertEquals(emptyMap(), fetch.records());
        assertTrue(fetch.positionAdvanced());

        // The next offset should point to the next batch
        assertEquals(lastOffset + 1, subscriptions.position(tp0).offset);
    }

    @Test
    public void testReadCommittedWithCompactedTopic() {
        buildFetcher(AutoOffsetResetStrategy.EARLIEST, new StringDeserializer(),
                new StringDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        long pid1 = 1L;
        long pid2 = 2L;
        long pid3 = 3L;

        appendTransactionalRecords(buffer, pid3, 3L,
                new SimpleRecord("3".getBytes(), "value".getBytes()),
                new SimpleRecord("4".getBytes(), "value".getBytes()));

        appendTransactionalRecords(buffer, pid2, 15L,
                new SimpleRecord("15".getBytes(), "value".getBytes()),
                new SimpleRecord("16".getBytes(), "value".getBytes()),
                new SimpleRecord("17".getBytes(), "value".getBytes()));

        appendTransactionalRecords(buffer, pid1, 22L,
                new SimpleRecord("22".getBytes(), "value".getBytes()),
                new SimpleRecord("23".getBytes(), "value".getBytes()));

        abortTransaction(buffer, pid2, 28L);

        appendTransactionalRecords(buffer, pid3, 30L,
                new SimpleRecord("30".getBytes(), "value".getBytes()),
                new SimpleRecord("31".getBytes(), "value".getBytes()),
                new SimpleRecord("32".getBytes(), "value".getBytes()));

        commitTransaction(buffer, pid3, 35L);

        appendTransactionalRecords(buffer, pid1, 39L,
                new SimpleRecord("39".getBytes(), "value".getBytes()),
                new SimpleRecord("40".getBytes(), "value".getBytes()));

        // transaction from pid1 is aborted, but the marker is not included in the fetch

        buffer.flip();

        // send the fetch
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        assertEquals(1, sendFetches());

        // prepare the response. the aborted transactions begin at offsets which are no longer in the log
        List<FetchResponseData.AbortedTransaction> abortedTransactions = Arrays.asList(
            new FetchResponseData.AbortedTransaction().setProducerId(pid2).setFirstOffset(6),
            new FetchResponseData.AbortedTransaction().setProducerId(pid1).setFirstOffset(0)
        );

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(MemoryRecords.readableRecords(buffer),
                abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<String, String>>> allFetchedRecords = fetchRecords();
        assertTrue(allFetchedRecords.containsKey(tp0));
        List<ConsumerRecord<String, String>> fetchedRecords = allFetchedRecords.get(tp0);
        assertEquals(5, fetchedRecords.size());
        assertEquals(Arrays.asList(3L, 4L, 30L, 31L, 32L), collectRecordOffsets(fetchedRecords));
    }

    @Test
    public void testReturnAbortedTransactionsInUncommittedMode() {
        buildFetcher(AutoOffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int currentOffset = 0;

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));

        abortTransaction(buffer, 1L, currentOffset);

        buffer.flip();

        List<FetchResponseData.AbortedTransaction> abortedTransactions = Collections.singletonList(
            new FetchResponseData.AbortedTransaction().setProducerId(1).setFirstOffset(0));
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
    }

    @Test
    public void testConsumerPositionUpdatedWhenSkippingAbortedTransactions() {
        buildFetcher(AutoOffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        long currentOffset = 0;

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "abort1-1".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "abort1-2".getBytes(), "value".getBytes()));

        currentOffset += abortTransaction(buffer, 1L, currentOffset);
        buffer.flip();

        List<FetchResponseData.AbortedTransaction> abortedTransactions = Collections.singletonList(
            new FetchResponseData.AbortedTransaction().setProducerId(1).setFirstOffset(0));
        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponseWithAbortedTransactions(records, abortedTransactions, Errors.NONE, 100L, 100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();

        // Ensure that we don't return any of the aborted records, but yet advance the consumer position.
        assertFalse(fetchedRecords.containsKey(tp0));
        assertEquals(currentOffset, subscriptions.position(tp0).offset);
    }

    @Test
    public void testConsumingViaIncrementalFetchRequests() {
        buildFetcher(2);

        assignFromUser(new HashSet<>(Arrays.asList(tp0, tp1)));
        subscriptions.seekValidated(tp0, new SubscriptionState.FetchPosition(0, Optional.empty(), metadata.currentLeader(tp0)));
        subscriptions.seekValidated(tp1, new SubscriptionState.FetchPosition(1, Optional.empty(), metadata.currentLeader(tp1)));

        // Fetch some records and establish an incremental fetch session.
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> partitions1 = new LinkedHashMap<>();
        partitions1.put(tidp0, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition())
                .setHighWatermark(2)
                .setLastStableOffset(2)
                .setLogStartOffset(0)
                .setRecords(records));
        partitions1.put(tidp1, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp1.partition())
                .setHighWatermark(100)
                .setLogStartOffset(0)
                .setRecords(emptyRecords));
        FetchResponse resp1 = FetchResponse.of(Errors.NONE, 0, 123, partitions1);
        client.prepareResponse(resp1);
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();
        assertFalse(fetchedRecords.containsKey(tp1));
        List<ConsumerRecord<byte[], byte[]>> records = fetchedRecords.get(tp0);
        assertEquals(2, records.size());
        assertEquals(3L, subscriptions.position(tp0).offset);
        assertEquals(1L, subscriptions.position(tp1).offset);
        assertEquals(1, records.get(0).offset());
        assertEquals(2, records.get(1).offset());

        // There is still a buffered record.
        assertEquals(0, sendFetches());
        fetchedRecords = fetchRecords();
        assertFalse(fetchedRecords.containsKey(tp1));
        records = fetchedRecords.get(tp0);
        assertEquals(1, records.size());
        assertEquals(3, records.get(0).offset());
        assertEquals(4L, subscriptions.position(tp0).offset);

        // The second response contains no new records.
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> partitions2 = new LinkedHashMap<>();
        FetchResponse resp2 = FetchResponse.of(Errors.NONE, 0, 123, partitions2);
        client.prepareResponse(resp2);
        assertEquals(1, sendFetches());
        consumerClient.poll(time.timer(0));
        fetchedRecords = fetchRecords();
        assertTrue(fetchedRecords.isEmpty());
        assertEquals(4L, subscriptions.position(tp0).offset);
        assertEquals(1L, subscriptions.position(tp1).offset);

        // The third response contains some new records for tp0.
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> partitions3 = new LinkedHashMap<>();
        partitions3.put(tidp0, new FetchResponseData.PartitionData()
                .setPartitionIndex(tp0.partition())
                .setHighWatermark(100)
                .setLastStableOffset(4)
                .setLogStartOffset(0)
                .setRecords(nextRecords));
        FetchResponse resp3 = FetchResponse.of(Errors.NONE, 0, 123, partitions3);
        client.prepareResponse(resp3);
        assertEquals(1, sendFetches());
        consumerClient.poll(time.timer(0));
        fetchedRecords = fetchRecords();
        assertFalse(fetchedRecords.containsKey(tp1));
        records = fetchedRecords.get(tp0);
        assertEquals(2, records.size());
        assertEquals(6L, subscriptions.position(tp0).offset);
        assertEquals(1L, subscriptions.position(tp1).offset);
        assertEquals(4, records.get(0).offset());
        assertEquals(5, records.get(1).offset());
    }

    @Test
    public void testFetcherConcurrency() throws Exception {
        int numPartitions = 20;
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (int i = 0; i < numPartitions; i++)
            topicPartitions.add(new TopicPartition(topicName, i));

        LogContext logContext = new LogContext();
        buildDependencies(new MetricConfig(), Long.MAX_VALUE, new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST), logContext);

        IsolationLevel isolationLevel = IsolationLevel.READ_UNCOMMITTED;

        offsetFetcher = new OffsetFetcher(logContext,
                consumerClient,
                metadata,
                subscriptions,
                time,
                retryBackoffMs,
                requestTimeoutMs,
                isolationLevel,
                apiVersions);

        Deserializers<byte[], byte[]> deserializers = new Deserializers<>(new ByteArrayDeserializer(), new ByteArrayDeserializer());
        FetchConfig fetchConfig = new FetchConfig(
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                2 * numPartitions,
                true, // check crcs
                CommonClientConfigs.DEFAULT_CLIENT_RACK,
                isolationLevel);
        fetcher = new Fetcher<>(
                logContext,
                consumerClient,
                metadata,
                subscriptions,
                fetchConfig,
                deserializers,
                metricsManager,
                time,
                apiVersions) {
            @Override
            protected FetchSessionHandler sessionHandler(int node) {
                final FetchSessionHandler handler = super.sessionHandler(node);
                if (handler == null)
                    return null;
                else {
                    return new FetchSessionHandler(new LogContext(), node) {
                        @Override
                        public Builder newBuilder() {
                            verifySessionPartitions();
                            return handler.newBuilder();
                        }

                        @Override
                        public boolean handleResponse(FetchResponse response, short version) {
                            verifySessionPartitions();
                            return handler.handleResponse(response, version);
                        }

                        @Override
                        public void handleError(Throwable t) {
                            verifySessionPartitions();
                            handler.handleError(t);
                        }
                        
                        @Override
                        public Map<Uuid, String> sessionTopicNames() {
                            return handler.sessionTopicNames();
                        }

                        // Verify that session partitions can be traversed safely.
                        private void verifySessionPartitions() {
                            try {
                                Field field = FetchSessionHandler.class.getDeclaredField("sessionPartitions");
                                field.setAccessible(true);
                                LinkedHashMap<?, ?> sessionPartitions =
                                        (LinkedHashMap<?, ?>) field.get(handler);
                                for (Map.Entry<?, ?> entry : sessionPartitions.entrySet()) {
                                    // If `sessionPartitions` are modified on another thread, Thread.yield will increase the
                                    // possibility of ConcurrentModificationException if appropriate synchronization is not used.
                                    Thread.yield();
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };
                }
            }
        };

        MetadataResponse initialMetadataResponse = RequestTestUtils.metadataUpdateWithIds(1,
                singletonMap(topicName, numPartitions), tp -> validLeaderEpoch, topicIds);
        client.updateMetadata(initialMetadataResponse);
        fetchSize = 10000;

        assignFromUser(topicPartitions);
        topicPartitions.forEach(tp -> subscriptions.seek(tp, 0L));

        AtomicInteger fetchesRemaining = new AtomicInteger(400);
        executorService = Executors.newSingleThreadExecutor();
        Future<?> future = executorService.submit(() -> {
            while (fetchesRemaining.get() > 0) {
                synchronized (consumerClient) {
                    if (!client.requests().isEmpty()) {
                        ClientRequest request = client.requests().peek();
                        FetchRequest fetchRequest = (FetchRequest) request.requestBuilder().build();
                        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> responseMap = new LinkedHashMap<>();
                        for (Map.Entry<TopicIdPartition, FetchRequest.PartitionData> entry : fetchRequest.fetchData(topicNames).entrySet()) {
                            TopicIdPartition tp = entry.getKey();
                            long offset = entry.getValue().fetchOffset;
                            responseMap.put(tp, new FetchResponseData.PartitionData()
                                    .setPartitionIndex(tp.topicPartition().partition())
                                    .setHighWatermark(offset + 2)
                                    .setLastStableOffset(offset + 2)
                                    .setLogStartOffset(0)
                                    .setRecords(buildRecords(offset, 2, offset)));
                        }
                        client.respondToRequest(request, FetchResponse.of(Errors.NONE, 0, 123, responseMap));
                        consumerClient.poll(time.timer(0));
                    }
                }
            }
            return fetchesRemaining.get();
        });
        Map<TopicPartition, Long> nextFetchOffsets = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), t -> 0L));
        while (fetchesRemaining.get() > 0 && !future.isDone()) {
            if (sendFetches() == 1) {
                synchronized (consumerClient) {
                    consumerClient.poll(time.timer(0));
                }
            }
            if (fetcher.hasCompletedFetches()) {
                Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();
                if (!fetchedRecords.isEmpty()) {
                    fetchesRemaining.decrementAndGet();
                    fetchedRecords.forEach((tp, records) -> {
                        assertEquals(2, records.size());
                        long nextOffset = nextFetchOffsets.get(tp);
                        assertEquals(nextOffset, records.get(0).offset());
                        assertEquals(nextOffset + 1, records.get(1).offset());
                        nextFetchOffsets.put(tp, nextOffset + 2);
                    });
                }
            }
        }
        assertEquals(0, future.get());
    }

    @Test
    public void testFetcherSessionEpochUpdate() throws Exception {
        buildFetcher(2);

        MetadataResponse initialMetadataResponse = RequestTestUtils.metadataUpdateWithIds(1, singletonMap(topicName, 1), topicIds);
        client.updateMetadata(initialMetadataResponse);
        assignFromUser(Collections.singleton(tp0));
        subscriptions.seek(tp0, 0L);

        AtomicInteger fetchesRemaining = new AtomicInteger(1000);
        executorService = Executors.newSingleThreadExecutor();
        Future<?> future = executorService.submit(() -> {
            long nextOffset = 0;
            long nextEpoch = 0;
            while (fetchesRemaining.get() > 0) {
                synchronized (consumerClient) {
                    if (!client.requests().isEmpty()) {
                        ClientRequest request = client.requests().peek();
                        FetchRequest fetchRequest = (FetchRequest) request.requestBuilder().build();
                        int epoch = fetchRequest.metadata().epoch();
                        assertTrue(epoch == 0 || epoch == nextEpoch,
                            String.format("Unexpected epoch expected %d got %d", nextEpoch, epoch));
                        nextEpoch++;
                        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> responseMap = new LinkedHashMap<>();
                        responseMap.put(tidp0, new FetchResponseData.PartitionData()
                                .setPartitionIndex(tp0.partition())
                                .setHighWatermark(nextOffset + 2)
                                .setLastStableOffset(nextOffset + 2)
                                .setLogStartOffset(0)
                                .setRecords(buildRecords(nextOffset, 2, nextOffset)));
                        nextOffset += 2;
                        client.respondToRequest(request, FetchResponse.of(Errors.NONE, 0, 123, responseMap));
                        consumerClient.poll(time.timer(0));
                    }
                }
            }
            return fetchesRemaining.get();
        });
        long nextFetchOffset = 0;
        while (fetchesRemaining.get() > 0 && !future.isDone()) {
            if (sendFetches() == 1) {
                synchronized (consumerClient) {
                    consumerClient.poll(time.timer(0));
                }
            }
            if (fetcher.hasCompletedFetches()) {
                Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();
                if (!fetchedRecords.isEmpty()) {
                    fetchesRemaining.decrementAndGet();
                    List<ConsumerRecord<byte[], byte[]>> records = fetchedRecords.get(tp0);
                    assertEquals(2, records.size());
                    assertEquals(nextFetchOffset, records.get(0).offset());
                    assertEquals(nextFetchOffset + 1, records.get(1).offset());
                    nextFetchOffset += 2;
                }
                assertTrue(fetchRecords().isEmpty());
            }
        }
        assertEquals(0, future.get());
    }

    @Test
    public void testEmptyControlBatch() {
        buildFetcher(AutoOffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(),
                new ByteArrayDeserializer(), Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int currentOffset = 1;

        // Empty control batch should not cause an exception
        DefaultRecordBatch.writeEmptyHeader(buffer, RecordBatch.MAGIC_VALUE_V2, 1L,
                (short) 0, -1, 0, 0,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, TimestampType.CREATE_TIME, time.milliseconds(),
                true, true);

        currentOffset += appendTransactionalRecords(buffer, 1L, currentOffset,
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()),
                new SimpleRecord(time.milliseconds(), "key".getBytes(), "value".getBytes()));

        commitTransaction(buffer, 1L, currentOffset);
        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assignFromUser(singleton(tp0));

        subscriptions.seek(tp0, 0);

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        client.prepareResponse(body -> {
            FetchRequest request = (FetchRequest) body;
            assertEquals(IsolationLevel.READ_COMMITTED, request.isolationLevel());
            return true;
        }, fullFetchResponseWithAbortedTransactions(records, Collections.emptyList(), Errors.NONE, 100L, 100L, 0));

        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> fetchedRecords = fetchRecords();
        assertTrue(fetchedRecords.containsKey(tp0));
        assertEquals(fetchedRecords.get(tp0).size(), 2);
    }

    private MemoryRecords buildRecords(long baseOffset, int count, long firstMessageId) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, baseOffset);
        for (int i = 0; i < count; i++)
            builder.append(0L, "key".getBytes(), ("value-" + (firstMessageId + i)).getBytes());
        return builder.build();
    }

    private int appendTransactionalRecords(ByteBuffer buffer, long pid, long baseOffset, int baseSequence, SimpleRecord... records) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
                TimestampType.CREATE_TIME, baseOffset, time.milliseconds(), pid, (short) 0, baseSequence, true,
                RecordBatch.NO_PARTITION_LEADER_EPOCH);

        for (SimpleRecord record : records) {
            builder.append(record);
        }
        builder.build();
        return records.length;
    }

    private int appendTransactionalRecords(ByteBuffer buffer, long pid, long baseOffset, SimpleRecord... records) {
        return appendTransactionalRecords(buffer, pid, baseOffset, (int) baseOffset, records);
    }

    private void commitTransaction(ByteBuffer buffer, long producerId, long baseOffset) {
        short producerEpoch = 0;
        int partitionLeaderEpoch = 0;
        MemoryRecords.writeEndTransactionalMarker(buffer, baseOffset, time.milliseconds(), partitionLeaderEpoch, producerId, producerEpoch,
                new EndTransactionMarker(ControlRecordType.COMMIT, 0));
    }

    private int abortTransaction(ByteBuffer buffer, long producerId, long baseOffset) {
        short producerEpoch = 0;
        int partitionLeaderEpoch = 0;
        MemoryRecords.writeEndTransactionalMarker(buffer, baseOffset, time.milliseconds(), partitionLeaderEpoch, producerId, producerEpoch,
                new EndTransactionMarker(ControlRecordType.ABORT, 0));
        return 1;
    }

    @Test
    public void testSubscriptionPositionUpdatedWithEpoch() {
        // Create some records that include a leader epoch (1)
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024),
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,
                RecordBatch.NO_TIMESTAMP,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                1
        );
        builder.appendWithOffset(0L, 0L, "key".getBytes(), "value-1".getBytes());
        builder.appendWithOffset(1L, 0L, "key".getBytes(), "value-2".getBytes());
        builder.appendWithOffset(2L, 0L, "key".getBytes(), "value-3".getBytes());
        MemoryRecords records = builder.build();

        buildFetcher();
        assignFromUser(singleton(tp0));

        // Initialize the epoch=1
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWithIds("dummy", 1, Collections.emptyMap(), partitionCounts, tp -> 1, topicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

        // Seek
        subscriptions.seek(tp0, 0);

        // Do a normal fetch
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.pollNoWakeup();
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        assertEquals(subscriptions.position(tp0).offset, 3L);
        assertOptional(subscriptions.position(tp0).offsetEpoch, value -> assertEquals(value.intValue(), 1));
    }

    @Test
    public void testTruncationDetected() {
        // Create some records that include a leader epoch (1)
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024),
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
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

        buildFetcher(AutoOffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);
        assignFromUser(singleton(tp0));

        // Initialize the epoch=2
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(tp0.topic(), 4);
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWithIds("dummy", 1, Collections.emptyMap(),
                partitionCounts, tp -> 2, topicIds);
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

        // Offset validation requires OffsetForLeaderEpoch request v3 or higher
        Node node = metadata.fetch().nodes().get(0);
        apiVersions.update(node.idString(), NodeApiVersions.create());

        // Seek
        Metadata.LeaderAndEpoch leaderAndEpoch = new Metadata.LeaderAndEpoch(metadata.currentLeader(tp0).leader, Optional.of(1));
        subscriptions.seekValidated(tp0, new SubscriptionState.FetchPosition(0, Optional.of(1), leaderAndEpoch));

        // Check for truncation, this should cause tp0 to go into validation
        OffsetFetcher offsetFetcher = new OffsetFetcher(new LogContext(),
                consumerClient,
                metadata,
                subscriptions,
                time,
                retryBackoffMs,
                requestTimeoutMs,
                IsolationLevel.READ_UNCOMMITTED,
                apiVersions);
        offsetFetcher.validatePositionsIfNeeded();

        // No fetches sent since we entered validation
        assertEquals(0, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        assertTrue(subscriptions.awaitingValidation(tp0));

        // Prepare OffsetForEpoch response then check that we update the subscription position correctly.
        client.prepareResponse(prepareOffsetsForLeaderEpochResponse(tp0, Errors.NONE, 1, 10L));
        consumerClient.pollNoWakeup();

        assertFalse(subscriptions.awaitingValidation(tp0));

        // Fetch again, now it works
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0));
        consumerClient.pollNoWakeup();
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        assertEquals(subscriptions.position(tp0).offset, 3L);
        assertOptional(subscriptions.position(tp0).offsetEpoch, value -> assertEquals(value.intValue(), 1));
    }

    @Test
    public void testPreferredReadReplica() {
        buildFetcher(new MetricConfig(), AutoOffsetResetStrategy.EARLIEST, new BytesDeserializer(), new BytesDeserializer(),
                Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED, Duration.ofMinutes(5).toMillis());

        subscriptions.assignFromUser(singleton(tp0));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(2, singletonMap(topicName, 4), tp -> validLeaderEpoch, topicIds, false));
        subscriptions.seek(tp0, 0);

        // Take note of the preferred replica before the first fetch response
        Node selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(-1, selected.id());

        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Set preferred read replica to node=1
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(1)));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        // Verify
        selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(1, selected.id());


        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Set preferred read replica to node=2, which isn't in our metadata, should revert to leader
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(2)));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();
        selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(-1, selected.id());
    }

    @Test
    public void testFetchDisconnectedShouldClearPreferredReadReplica() {
        buildFetcher(new MetricConfig(), AutoOffsetResetStrategy.EARLIEST, new BytesDeserializer(), new BytesDeserializer(),
                Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED, Duration.ofMinutes(5).toMillis());

        subscriptions.assignFromUser(singleton(tp0));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(2, singletonMap(topicName, 4), tp -> validLeaderEpoch, topicIds, false));
        subscriptions.seek(tp0, 0);
        assertEquals(1, sendFetches());

        // Set preferred read replica to node=1
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(1)));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();

        // Verify
        Node selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(1, selected.id());
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Disconnect - preferred read replica should be cleared.
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0), true);

        consumerClient.poll(time.timer(0));
        assertFalse(fetcher.hasCompletedFetches());
        fetchRecords();
        selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(-1, selected.id());
    }

    @Test
    public void testFetchDisconnectedShouldNotClearPreferredReadReplicaIfUnassigned() {
        buildFetcher(new MetricConfig(), AutoOffsetResetStrategy.EARLIEST, new BytesDeserializer(), new BytesDeserializer(),
            Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED, Duration.ofMinutes(5).toMillis());

        subscriptions.assignFromUser(singleton(tp0));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(2, singletonMap(topicName, 4), tp -> validLeaderEpoch, topicIds, false));
        subscriptions.seek(tp0, 0);
        assertEquals(1, sendFetches());

        // Set preferred read replica to node=1
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(1)));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();

        // Verify
        Node selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(1, selected.id());
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Disconnect and remove tp0 from assignment
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L, 0), true);
        subscriptions.assignFromUser(emptySet());

        // Preferred read replica should not be cleared
        consumerClient.poll(time.timer(0));
        assertFalse(fetcher.hasCompletedFetches());
        fetchRecords();
        selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(-1, selected.id());
    }

    @Test
    public void testFetchErrorShouldClearPreferredReadReplica() {
        buildFetcher(new MetricConfig(), AutoOffsetResetStrategy.EARLIEST, new BytesDeserializer(), new BytesDeserializer(),
                Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED, Duration.ofMinutes(5).toMillis());

        subscriptions.assignFromUser(singleton(tp0));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(2, singletonMap(topicName, 4), tp -> validLeaderEpoch, topicIds, false));
        subscriptions.seek(tp0, 0);
        assertEquals(1, sendFetches());

        // Set preferred read replica to node=1
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(1)));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();

        // Verify
        Node selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(1, selected.id());
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Error - preferred read replica should be cleared. An actual error response will contain -1 as the
        // preferred read replica. In the test we want to ensure that we are handling the error.
        client.prepareResponse(fullFetchResponse(tidp0, MemoryRecords.EMPTY, Errors.NOT_LEADER_OR_FOLLOWER, -1L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(1)));

        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        fetchRecords();
        selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(-1, selected.id());
    }

    @Test
    public void testPreferredReadReplicaOffsetError() {
        buildFetcher(new MetricConfig(), AutoOffsetResetStrategy.EARLIEST, new BytesDeserializer(), new BytesDeserializer(),
                Integer.MAX_VALUE, IsolationLevel.READ_COMMITTED, Duration.ofMinutes(5).toMillis());

        subscriptions.assignFromUser(singleton(tp0));
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(2, singletonMap(topicName, 4), tp -> validLeaderEpoch, topicIds, false));

        subscriptions.seek(tp0, 0);

        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.NONE, 100L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(1)));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        fetchRecords();

        Node selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(selected.id(), 1);

        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Return an error, should unset the preferred read replica
        client.prepareResponse(fullFetchResponse(tidp0, records, Errors.OFFSET_OUT_OF_RANGE, 100L,
                FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.empty()));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        fetchRecords();

        selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(selected.id(), -1);
    }

    @Test
    public void testFetchCompletedBeforeHandlerAdded() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);
        sendFetches();
        client.prepareResponse(fullFetchResponse(tidp0, buildRecords(1L, 1, 1), Errors.NONE, 100L, 0));
        consumerClient.poll(time.timer(0));
        fetchRecords();

        Metadata.LeaderAndEpoch leaderAndEpoch = subscriptions.position(tp0).currentLeader;
        assertTrue(leaderAndEpoch.leader.isPresent());
        Node readReplica = fetcher.selectReadReplica(tp0, leaderAndEpoch.leader.get(), time.milliseconds());

        AtomicBoolean wokenUp = new AtomicBoolean(false);
        client.setWakeupHook(() -> {
            if (!wokenUp.getAndSet(true)) {
                consumerClient.disconnectAsync(readReplica);
                consumerClient.poll(time.timer(0));
            }
        });

        assertEquals(1, sendFetches());

        consumerClient.disconnectAsync(readReplica);
        consumerClient.poll(time.timer(0));

        assertEquals(1, sendFetches());
    }

    @Test
    public void testCorruptMessageError() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        subscriptions.seek(tp0, 0);

        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());

        // Prepare a response with the CORRUPT_MESSAGE error.
        client.prepareResponse(fullFetchResponse(
                tidp0,
                buildRecords(1L, 1, 1),
                Errors.CORRUPT_MESSAGE,
                100L, 0));
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());

        // Trigger the exception.
        assertThrows(KafkaException.class, this::fetchRecords);
    }

    /**
     * Test the scenario that FetchResponse returns with an error indicating leadership change for the partition, but it
     * does not contain new leader info(defined in KIP-951).
     */
    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER"})
    public void testWhenFetchResponseReturnsALeaderShipChangeErrorButNoNewLeaderInformation(Errors error) {
        // The test runs with 2 partitions where 1 partition is fetched without errors, and
        // 2nd partition faces errors due to leadership changes.
        buildFetcher(new MetricConfig(), AutoOffsetResetStrategy.EARLIEST, new BytesDeserializer(),
            new BytesDeserializer(),
            Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED,
            Duration.ofMinutes(5).toMillis());

        // Setup so that tp0 & tp1 are subscribed and will be fetched from.
        // Also, setup client's metadata for tp0 & tp1.
        subscriptions.assignFromUser(new HashSet<>(Arrays.asList(tp0, tp1)));
        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, singletonMap(topicName, 4),
                tp -> validLeaderEpoch, topicIds, false));
        Node tp0Leader = metadata.fetch().leaderFor(tp0);
        Node tp1Leader = metadata.fetch().leaderFor(tp1);
        Node nodeId0 = metadata.fetch().nodeById(0);
        Cluster startingClusterMetadata = metadata.fetch();
        subscriptions.seek(tp0, 0);
        subscriptions.seek(tp1, 0);

        // Setup preferred read replica to node=1 by doing a fetch for both partitions.
        assertEquals(2, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        client.prepareResponseFrom(fullFetchResponse(tidp0, this.records, Errors.NONE, 100L,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(nodeId0.id())), tp0Leader);
        client.prepareResponseFrom(fullFetchResponse(tidp1, this.records, Errors.NONE, 100L,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(nodeId0.id())), tp1Leader);
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));
        // Validate setup of preferred read replica for tp0 & tp1 is done correctly.
        Node selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(nodeId0.id(), selected.id());
        selected = fetcher.selectReadReplica(tp1, Node.noNode(), time.milliseconds());
        assertEquals(nodeId0.id(), selected.id());

        // Send next fetch request.
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        // Verify that metadata-update isn't requested as metadata is considered upto-date.
        assertFalse(metadata.updateRequested());

        // TEST that next fetch returns an error(due to leadership change) but new leader info is not returned
        // in the FetchResponse. This is the behaviour prior to KIP-951, should keep on working.
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> partitions = new LinkedHashMap<>();
        partitions.put(tidp0,
            new FetchResponseData.PartitionData()
                .setPartitionIndex(tidp0.topicPartition().partition())
                .setErrorCode(error.code()));
        partitions.put(tidp1,
            new FetchResponseData.PartitionData()
                .setPartitionIndex(tidp1.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setHighWatermark(100L)
                .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                .setLogStartOffset(0)
                .setRecords(nextRecords));
        client.prepareResponseFrom(FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, partitions), nodeId0);
        consumerClient.poll(time.timer(0));
        partitionRecords = fetchRecords();
        assertFalse(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        // Validate metadata is unchanged, as FetchResponse didn't have new leader information.
        assertEquals(startingClusterMetadata, metadata.fetch());

        // Validate metadata-update is requested due to the leadership-error on tp0.
        assertTrue(metadata.updateRequested());

        // Validate preferred-read-replica is cleared for tp0 due to the error.
        assertEquals(Optional.empty(),
            subscriptions.preferredReadReplica(tp0, time.milliseconds()));
        // Validate preferred-read-replica is still set for tp1 as previous fetch for it was ok.
        assertEquals(Optional.of(nodeId0.id()),
            subscriptions.preferredReadReplica(tp1, time.milliseconds()));

        // Validate subscription is still valid & fetch-able for both tp0 & tp1. And tp0 points to original leader.
        assertTrue(subscriptions.isFetchable(tp0));
        Metadata.LeaderAndEpoch currentLeader = subscriptions.position(tp0).currentLeader;
        assertEquals(tp0Leader.id(), currentLeader.leader.get().id());
        assertEquals(validLeaderEpoch, currentLeader.epoch.get());
        assertTrue(subscriptions.isFetchable(tp1));
    }

    /**
     * Test the scenario that FetchResponse returns with an error indicating leadership change for the partition, along with
     * new leader info(defined in KIP-951).
     */
    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER"})
    public void testWhenFetchResponseReturnsALeaderShipChangeErrorAndNewLeaderInformation(Errors error) {
        // The test runs with 2 partitions where 1 partition is fetched without errors, and
        // 2nd partition faces errors due to leadership changes.
        buildFetcher(new MetricConfig(), AutoOffsetResetStrategy.EARLIEST, new BytesDeserializer(),
            new BytesDeserializer(),
            Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED,
            Duration.ofMinutes(5).toMillis());

        // Setup so that tp0 & tp1 are subscribed and will be fetched from.
        // Also, setup client's metadata for tp0 & tp1.
        subscriptions.assignFromUser(new HashSet<>(Arrays.asList(tp0, tp1)));
        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, singletonMap(topicName, 4),
                tp -> validLeaderEpoch, topicIds, false));
        Node tp0Leader = metadata.fetch().leaderFor(tp0);
        Node tp1Leader = metadata.fetch().leaderFor(tp1);
        Node nodeId0 = metadata.fetch().nodeById(0);
        Cluster startingClusterMetadata = metadata.fetch();
        subscriptions.seek(tp0, 0);
        subscriptions.seek(tp1, 0);

        // Setup preferred read replica to node=1 by doing a fetch for both partitions.
        assertEquals(2, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        client.prepareResponseFrom(fullFetchResponse(tidp0, this.records, Errors.NONE, 100L,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(nodeId0.id())), tp0Leader);
        client.prepareResponseFrom(fullFetchResponse(tidp1, this.records, Errors.NONE, 100L,
            FetchResponse.INVALID_LAST_STABLE_OFFSET, 0, Optional.of(nodeId0.id())), tp1Leader);
        consumerClient.poll(time.timer(0));
        assertTrue(fetcher.hasCompletedFetches());
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));
        // Validate setup of preferred read replica for tp0 & tp1 is done correctly.
        Node selected = fetcher.selectReadReplica(tp0, Node.noNode(), time.milliseconds());
        assertEquals(nodeId0.id(), selected.id());
        selected = fetcher.selectReadReplica(tp1, Node.noNode(), time.milliseconds());
        assertEquals(nodeId0.id(), selected.id());

        // Send next fetch request.
        assertEquals(1, sendFetches());
        assertFalse(fetcher.hasCompletedFetches());
        // Validate metadata-update isn't requested as no errors seen yet.
        assertFalse(metadata.updateRequested());

        // Test that next fetch returns an error(due to leadership change) and new leader info is returned, as introduced
        // in KIP-951. The new leader is a new node, id = 999. For tp1 fetch returns with no error.
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> partitions = new LinkedHashMap<>();
        Node newNode = new Node(999, "newnode", 999, "newrack");
        FetchResponseData.PartitionData tp0Data = new FetchResponseData.PartitionData()
            .setPartitionIndex(tidp0.topicPartition().partition())
            .setErrorCode(error.code());
        tp0Data.currentLeader().setLeaderId(newNode.id());
        int tp0NewLeaderEpoch = validLeaderEpoch + 100;
        tp0Data.currentLeader().setLeaderEpoch(tp0NewLeaderEpoch);
        partitions.put(tidp0, tp0Data);
        partitions.put(tidp1,
            new FetchResponseData.PartitionData()
                .setPartitionIndex(tidp1.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setHighWatermark(100L)
                .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                .setLogStartOffset(0)
                .setRecords(nextRecords));
        client.prepareResponseFrom(FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, partitions, singletonList(newNode)), nodeId0);
        consumerClient.poll(time.timer(0));
        partitionRecords = fetchRecords();
        assertFalse(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        // Validate metadata is changed, as previous FetchResponse had new leader info for tp0.
        assertNotEquals(startingClusterMetadata, metadata.fetch());
        // Validate new-node(id=999) is part of the metadata
        assertEquals(newNode, metadata.fetch().nodeById(999));
        // Validate metadata returns the new leader info for tp0.
        Metadata.LeaderAndEpoch currentLeaderTp0 = metadata.currentLeader(tp0);
        assertEquals(Optional.of(newNode), currentLeaderTp0.leader);
        assertEquals(Optional.of(tp0NewLeaderEpoch), currentLeaderTp0.epoch);

        // Validate metadata-update is requested due to the leadership-error for tp0.
        assertTrue(metadata.updateRequested());

        // Validate preferred-read-replica is cleared for tp0 due to the error.
        assertEquals(Optional.empty(),
            subscriptions.preferredReadReplica(tp0, time.milliseconds()));
        // Validate preferred-read-replica is still set for tp1 as previous fetch is ok.
        assertEquals(Optional.of(nodeId0.id()),
            subscriptions.preferredReadReplica(tp1, time.milliseconds()));

        // Validate subscription is valid & fetch-able, and points to the new leader.
        assertTrue(subscriptions.isFetchable(tp0));
        Metadata.LeaderAndEpoch currentLeader = subscriptions.position(tp0).currentLeader;
        assertEquals(newNode.id(), currentLeader.leader.get().id());
        assertEquals(tp0NewLeaderEpoch, currentLeader.epoch.get());

        // Validate subscription is still valid & fetch-able for tp1.
        assertTrue(subscriptions.isFetchable(tp1));
    }
    
    @Test
    public void testFetcherDontCacheAnyData() {
        short version = 17;
        FetchResponse fetchResponse = fetchResponse(tidp0, records, Errors.NONE, 100L, -1L, 0L, 0);
        LinkedHashMap<TopicPartition, FetchResponseData.PartitionData> responseData = fetchResponse.responseData(topicNames, version);
        assertEquals(topicNames.size(), responseData.size());
        responseData.forEach((topicPartition, partitionData) -> assertEquals(records, partitionData.records()));
        LinkedHashMap<TopicPartition, FetchResponseData.PartitionData> nonResponseData = fetchResponse.responseData(emptyMap(), version);
        assertEquals(emptyMap().size(), nonResponseData.size());
        nonResponseData.forEach((topicPartition, partitionData) -> assertEquals(MemoryRecords.EMPTY, partitionData.records()));
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

    private FetchResponse fetchResponseWithTopLevelError(TopicIdPartition tp, Errors error, int throttleTime) {
        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = Collections.singletonMap(tp,
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(tp.topicPartition().partition())
                        .setErrorCode(error.code())
                        .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK));
        return FetchResponse.of(error, throttleTime, INVALID_SESSION_ID, new LinkedHashMap<>(partitions));
    }

    private FetchResponse fullFetchResponseWithAbortedTransactions(MemoryRecords records,
                                                                   List<FetchResponseData.AbortedTransaction> abortedTransactions,
                                                                   Errors error,
                                                                   long lastStableOffset,
                                                                   long hw,
                                                                   int throttleTime) {
        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = Collections.singletonMap(tidp0,
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(tp0.partition())
                        .setErrorCode(error.code())
                        .setHighWatermark(hw)
                        .setLastStableOffset(lastStableOffset)
                        .setLogStartOffset(0)
                        .setAbortedTransactions(abortedTransactions)
                        .setRecords(records));
        return FetchResponse.of(Errors.NONE, throttleTime, INVALID_SESSION_ID, new LinkedHashMap<>(partitions));
    }

    private FetchResponse fullFetchResponse(int sessionId, TopicIdPartition tp, MemoryRecords records, Errors error, long hw, int throttleTime) {
        return fullFetchResponse(sessionId, tp, records, error, hw, FetchResponse.INVALID_LAST_STABLE_OFFSET, throttleTime);
    }

    private FetchResponse fullFetchResponse(TopicIdPartition tp, MemoryRecords records, Errors error, long hw, int throttleTime) {
        return fullFetchResponse(tp, records, error, hw, FetchResponse.INVALID_LAST_STABLE_OFFSET, throttleTime);
    }

    private FetchResponse fullFetchResponse(TopicIdPartition tp, MemoryRecords records, Errors error, long hw,
                                            long lastStableOffset, int throttleTime) {
        return fullFetchResponse(INVALID_SESSION_ID, tp, records, error, hw, lastStableOffset, throttleTime);
    }

    private FetchResponse fullFetchResponse(int sessionId, TopicIdPartition tp, MemoryRecords records, Errors error, long hw,
                                            long lastStableOffset, int throttleTime) {
        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = Collections.singletonMap(tp,
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(tp.topicPartition().partition())
                        .setErrorCode(error.code())
                        .setHighWatermark(hw)
                        .setLastStableOffset(lastStableOffset)
                        .setLogStartOffset(0)
                        .setRecords(records));
        return FetchResponse.of(Errors.NONE, throttleTime, sessionId, new LinkedHashMap<>(partitions));
    }

    private FetchResponse fullFetchResponse(TopicIdPartition tp, MemoryRecords records, Errors error, long hw,
                                            long lastStableOffset, int throttleTime, Optional<Integer> preferredReplicaId) {
        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = Collections.singletonMap(tp,
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(tp.topicPartition().partition())
                        .setErrorCode(error.code())
                        .setHighWatermark(hw)
                        .setLastStableOffset(lastStableOffset)
                        .setLogStartOffset(0)
                        .setRecords(records)
                        .setPreferredReadReplica(preferredReplicaId.orElse(FetchResponse.INVALID_PREFERRED_REPLICA_ID)));
        return FetchResponse.of(Errors.NONE, throttleTime, INVALID_SESSION_ID, new LinkedHashMap<>(partitions));
    }

    private FetchResponse fetchResponse(TopicIdPartition tp, MemoryRecords records, Errors error, long hw,
                                        long lastStableOffset, long logStartOffset, int throttleTime) {
        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = Collections.singletonMap(tp,
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(tp.topicPartition().partition())
                        .setErrorCode(error.code())
                        .setHighWatermark(hw)
                        .setLastStableOffset(lastStableOffset)
                        .setLogStartOffset(logStartOffset)
                        .setRecords(records));
        return FetchResponse.of(Errors.NONE, throttleTime, INVALID_SESSION_ID, new LinkedHashMap<>(partitions));
    }

    private FetchResponse fetchResponse2(TopicIdPartition tp1, MemoryRecords records1, long hw1,
                                         TopicIdPartition tp2, MemoryRecords records2, long hw2) {
        Map<TopicIdPartition, FetchResponseData.PartitionData> partitions = new HashMap<>();
        partitions.put(tp1,
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(tp1.topicPartition().partition())
                        .setErrorCode(Errors.NONE.code())
                        .setHighWatermark(hw1)
                        .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                        .setLogStartOffset(0)
                        .setRecords(records1));
        partitions.put(tp2,
                new FetchResponseData.PartitionData()
                        .setPartitionIndex(tp2.topicPartition().partition())
                        .setErrorCode(Errors.NONE.code())
                        .setHighWatermark(hw2)
                        .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                        .setLogStartOffset(0)
                        .setRecords(records2));
        return FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, new LinkedHashMap<>(partitions));
    }

    /**
     * Assert that the {@link Fetcher#collectFetch() latest fetch} does not contain any
     * {@link Fetch#records() user-visible records}, did not
     * {@link Fetch#positionAdvanced() advance the consumer's position},
     * and is {@link Fetch#isEmpty() empty}.
     * @param reason the reason to include for assertion methods such as {@link org.junit.jupiter.api.Assertions#assertTrue(boolean, String)}
     */
    private void assertEmptyFetch(String reason) {
        Fetch<?, ?> fetch = collectFetch();
        assertEquals(Collections.emptyMap(), fetch.records(), reason);
        assertFalse(fetch.positionAdvanced(), reason);
        assertTrue(fetch.isEmpty(), reason);
    }

    private <K, V> Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchRecords() {
        Fetch<K, V> fetch = collectFetch();
        return fetch.records();
    }

    @SuppressWarnings("unchecked")
    private <K, V> Fetch<K, V> collectFetch() {
        return (Fetch<K, V>) fetcher.collectFetch();
    }

    private void buildFetcher(int maxPollRecords) {
        buildFetcher(AutoOffsetResetStrategy.EARLIEST, new ByteArrayDeserializer(), new ByteArrayDeserializer(),
                maxPollRecords, IsolationLevel.READ_UNCOMMITTED);
    }

    private void buildFetcher() {
        buildFetcher(Integer.MAX_VALUE);
    }

    private void buildFetcher(Deserializer<?> keyDeserializer,
                              Deserializer<?> valueDeserializer) {
        buildFetcher(AutoOffsetResetStrategy.EARLIEST, keyDeserializer, valueDeserializer,
                Integer.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED);
    }

    private <K, V> void buildFetcher(AutoOffsetResetStrategy offsetResetStrategy,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer,
                                     int maxPollRecords,
                                     IsolationLevel isolationLevel) {
        buildFetcher(new MetricConfig(), offsetResetStrategy, keyDeserializer, valueDeserializer,
                maxPollRecords, isolationLevel);
    }

    private <K, V> void buildFetcher(MetricConfig metricConfig,
                                     AutoOffsetResetStrategy offsetResetStrategy,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer,
                                     int maxPollRecords,
                                     IsolationLevel isolationLevel) {
        buildFetcher(metricConfig, offsetResetStrategy, keyDeserializer, valueDeserializer, maxPollRecords, isolationLevel, Long.MAX_VALUE);
    }

    private <K, V> void buildFetcher(MetricConfig metricConfig,
                                     AutoOffsetResetStrategy offsetResetStrategy,
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
        FetchConfig fetchConfig = new FetchConfig(
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                maxPollRecords,
                true, // check crc
                CommonClientConfigs.DEFAULT_CLIENT_RACK,
                isolationLevel);
        fetcher = spy(new Fetcher<>(
                logContext,
                consumerClient,
                metadata,
                subscriptionState,
                fetchConfig,
                new Deserializers<>(keyDeserializer, valueDeserializer),
                metricsManager,
                time,
                apiVersions));
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
        consumerClient = spy(new ConsumerNetworkClient(logContext, client, metadata, time,
                100, 1000, Integer.MAX_VALUE));
        metricsRegistry = new FetchMetricsRegistry(metricConfig.tags().keySet(), "consumer" + groupId);
        metricsManager = new FetchMetricsManager(metrics, metricsRegistry);
    }

    private <T> List<Long> collectRecordOffsets(List<ConsumerRecord<T, T>> records) {
        return records.stream().map(ConsumerRecord::offset).collect(Collectors.toList());
    }
}
