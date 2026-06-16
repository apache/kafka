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

package org.apache.kafka.server.share.dlq;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.share.dlq.ShareGroupDLQMetadataCacheHelper.TopicPartitionData;
import org.apache.kafka.server.share.metrics.ShareGroupMetrics;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.kafka.server.share.dlq.ShareGroupDLQStateManager.ProduceRequestHandler.HEADER_DLQ_ERRORS_DELIVERY_COUNT;
import static org.apache.kafka.server.share.dlq.ShareGroupDLQStateManager.ProduceRequestHandler.HEADER_DLQ_ERRORS_GROUP;
import static org.apache.kafka.server.share.dlq.ShareGroupDLQStateManager.ProduceRequestHandler.HEADER_DLQ_ERRORS_MESSAGE;
import static org.apache.kafka.server.share.dlq.ShareGroupDLQStateManager.ProduceRequestHandler.HEADER_DLQ_ERRORS_OFFSET;
import static org.apache.kafka.server.share.dlq.ShareGroupDLQStateManager.ProduceRequestHandler.HEADER_DLQ_ERRORS_PARTITION;
import static org.apache.kafka.server.share.dlq.ShareGroupDLQStateManager.ProduceRequestHandler.HEADER_DLQ_ERRORS_TOPIC;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ShareGroupDLQStateManagerTest {
    private static final MockTime MOCK_TIME = new MockTime();
    private static final String HOST = "localhost";
    private static final int PORT = 9092;
    private static final String GROUP_ID = "test-group";
    private static final String DLQ_TOPIC = "dlq-topic";
    private static final Uuid DLQ_TOPIC_ID = Uuid.randomUuid();
    private static final Uuid SOURCE_TOPIC_ID = Uuid.randomUuid();
    private static final Node DEFAULT_LEADER = new Node(0, HOST, PORT);
    private static final LogReader MOCK_LOG_READER = mock(LogReader.class);

    private final MockTimer mockTimer = new MockTimer(MOCK_TIME);
    private final ShareGroupMetrics mockMetrics = mock(ShareGroupMetrics.class);
    private ShareGroupDLQStateManager stateManager;

    @AfterEach
    public void tearDown() throws Exception {
        if (stateManager != null) {
            stateManager.stop();
        }
    }

    private final class Builder {
        private KafkaClient client;
        private Time time = MOCK_TIME;
        private Timer timer;
        private ShareGroupDLQMetadataCacheHelper cacheHelper;
        private ShareGroupMetrics shareGroupMetrics;
        private LogReader logReader;

        Builder withClient(KafkaClient client) {
            this.client = client;
            return this;
        }

        Builder withCacheHelper(ShareGroupDLQMetadataCacheHelper cacheHelper) {
            this.cacheHelper = cacheHelper;
            return this;
        }

        Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        Builder withTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        Builder withShareGroupMetrics(ShareGroupMetrics shareGroupMetrics) {
            this.shareGroupMetrics = shareGroupMetrics;
            return this;
        }

        Builder withLogReader(LogReader logReader) {
            this.logReader = logReader;
            return this;
        }

        ShareGroupDLQStateManager build() {
            // Default to the test-class mockMetrics field so tests can verify interactions
            // without having to thread a custom metrics mock through the builder.
            return new ShareGroupDLQStateManager(
                client != null ? client : new MockClient(MOCK_TIME),
                cacheHelper != null ? cacheHelper : cacheHelper(DEFAULT_LEADER),
                time,
                timer != null ? timer : mockTimer,
                shareGroupMetrics != null ? shareGroupMetrics : mockMetrics,
                logReader == null ? MOCK_LOG_READER : logReader
            );
        }
    }

    private Builder builder() {
        return new Builder();
    }

    private static ShareGroupDLQRecordParameter param() {
        return new ShareGroupDLQRecordParameter(
            GROUP_ID,
            new TopicIdPartition(SOURCE_TOPIC_ID, 0, "source-topic"),
            0L,
            2L,
            Optional.of((short) 1),
            Optional.of(new RuntimeException("simulated cause"))
        );
    }

    /**
     * Util method to populate DLQ metadata cache with sensible defaults.
     *
     * @param leader Node representing leader node for DLQ partition.
     * @return Populated DLQ cache helper object.
     */
    private static ShareGroupDLQMetadataCacheHelper cacheHelper(Node leader) {
        ShareGroupDLQMetadataCacheHelper helper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(helper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.of(DLQ_TOPIC));
        when(helper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());
        when(helper.containsTopic(DLQ_TOPIC)).thenReturn(true);
        when(helper.isDlqEnabledOnTopic(DLQ_TOPIC)).thenReturn(true);
        when(helper.isDlqAutoTopicCreateEnabled()).thenReturn(true);
        when(helper.getClusterNodes()).thenReturn(List.of(leader));
        when(helper.topicName(SOURCE_TOPIC_ID)).thenReturn(Optional.of("source-topic"));
        when(helper.topicPartitionData(DLQ_TOPIC)).thenReturn(new TopicPartitionData(
            DLQ_TOPIC,
            Optional.of(1),
            Optional.of(DLQ_TOPIC_ID),
            List.of(leader)
        ));
        when(helper.isShareGroupDlqCopyRecordEnabled(GROUP_ID)).thenReturn(false);
        return helper;
    }

    private static Throwable getCause(CompletableFuture<Void> future) {
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Expected the future to complete exceptionally");
            return null;
        } catch (ExecutionException ee) {
            return ee.getCause();
        } catch (InterruptedException | TimeoutException e) {
            fail("Future did not complete", e);
            return null;
        }
    }

    /**
     * Expected headers and offset range for one DLQ partition's records inside a captured produce
     * request. {@code sharedHeaders} are expected to be identical on every record in that partition;
     * the offset header is built per-record from {@code firstOffset}..{@code lastOffset}.
     */
    private record ExpectedDlqPartition(long firstOffset, long lastOffset, Map<String, String> sharedHeaders,
                                        List<byte[]> keys, List<byte[]> values) {
    }

    /**
     * Verifies record-level headers for every partition of the (single) topic in a captured produce
     * request. Keys of {@code expectedByPartitionIndex} are DLQ partition indices (as reported by
     * {@link ProduceRequestData.PartitionProduceData#index()}); the set of partitions present in the
     * request must match the keys exactly.
     */
    private static void assertDlqProduceRecordHeaders(
        ProduceRequest request,
        Map<Integer, ExpectedDlqPartition> expectedByPartitionIndex
    ) {
        ProduceRequestData.TopicProduceData topic = request.data().topicData().iterator().next();
        Set<Integer> actualPartitionIndices = topic.partitionData().stream()
            .map(ProduceRequestData.PartitionProduceData::index)
            .collect(Collectors.toSet());
        assertEquals(expectedByPartitionIndex.keySet(), actualPartitionIndices,
            "Unexpected set of DLQ partitions in produce request");

        for (ProduceRequestData.PartitionProduceData partition : topic.partitionData()) {
            ExpectedDlqPartition expected = expectedByPartitionIndex.get(partition.index());
            MemoryRecords records = (MemoryRecords) partition.records();

            long expectedOffset = expected.firstOffset();
            int recordCount = 0;
            for (Record record : records.records()) {
                Map<String, String> actualHeaders = new HashMap<>();
                for (Header h : record.headers()) {
                    actualHeaders.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
                }
                Map<String, String> expectedHeaders = new HashMap<>(expected.sharedHeaders());
                expectedHeaders.put(HEADER_DLQ_ERRORS_OFFSET, Long.toString(expectedOffset));
                assertEquals(expectedHeaders, actualHeaders,
                    "Partition " + partition.index() + " record at offset " + expectedOffset + " has unexpected headers");

                if (!expected.keys().isEmpty()) {
                    assertKeyValue(record, expected.keys.get(recordCount), true);
                } else {
                    assertFalse(record.hasKey());
                }

                if (!expected.values().isEmpty()) {
                    assertKeyValue(record, expected.values.get(recordCount), false);
                } else {
                    assertFalse(record.hasValue());
                }

                expectedOffset++;
                recordCount++;
            }
            assertEquals((int) (expected.lastOffset() - expected.firstOffset() + 1), recordCount,
                "Partition " + partition.index() + " has unexpected number of records");
        }
    }

    private static void assertKeyValue(Record record, byte[] expectedData, boolean isKey) {
        if (expectedData == null) {
            if (isKey) {
                assertFalse(record.hasKey());
            } else {
                assertFalse(record.hasValue());
            }
        } else {
            if (isKey && !record.hasValue()) {
                fail("record key not found");
            }

            if (!isKey && !record.hasValue()) {
                fail("record value not found");
            }

            byte[] actualChunk = new byte[expectedData.length];
            if (isKey) {
                record.key().get(actualChunk);
            } else {
                record.value().get(actualChunk);
            }
            assertArrayEquals(expectedData, actualChunk);
        }
    }

    // ---- Constructor null-check tests ----

    @Test
    public void testConstructorRejectsNullClient() {
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        assertThrows(IllegalArgumentException.class,
            () -> new ShareGroupDLQStateManager(null, cacheHelper, MOCK_TIME, mockTimer, mockMetrics, MOCK_LOG_READER));
    }

    @Test
    public void testConstructorRejectsNullCacheHelper() {
        KafkaClient client = mock(KafkaClient.class);
        assertThrows(IllegalArgumentException.class,
            () -> new ShareGroupDLQStateManager(client, null, MOCK_TIME, mockTimer, mockMetrics, MOCK_LOG_READER));
    }

    @Test
    public void testConstructorRejectsNullTime() {
        KafkaClient client = mock(KafkaClient.class);
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        assertThrows(IllegalArgumentException.class,
            () -> new ShareGroupDLQStateManager(client, cacheHelper, null, mockTimer, mockMetrics, MOCK_LOG_READER));
    }

    @Test
    public void testConstructorRejectsNullTimer() {
        KafkaClient client = mock(KafkaClient.class);
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        assertThrows(IllegalArgumentException.class,
            () -> new ShareGroupDLQStateManager(client, cacheHelper, MOCK_TIME, null, mockMetrics, MOCK_LOG_READER));
    }

    @Test
    public void testConstructorRejectsNullShareGroupMetrics() {
        KafkaClient client = mock(KafkaClient.class);
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        assertThrows(IllegalArgumentException.class,
            () -> new ShareGroupDLQStateManager(client, cacheHelper, MOCK_TIME, mockTimer, null, MOCK_LOG_READER));
    }

    @Test
    public void testConstructorRejectsNullLogReader() {
        KafkaClient client = mock(KafkaClient.class);
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        assertThrows(IllegalArgumentException.class,
            () -> new ShareGroupDLQStateManager(client, cacheHelper, MOCK_TIME, mockTimer, mockMetrics, null));
    }

    // ---- Lifecycle tests ----

    @Test
    public void testStartIsIdempotent() {
        stateManager = builder().build();

        stateManager.start();
        stateManager.start();
        // tearDown will call stateManager.stop() and must not throw.
        verifyNoInteractions(mockMetrics);
    }

    @Test
    public void testStopWithoutStartIsNoOp() {
        stateManager = builder().build();
        // tearDown will call stateManager.stop() without a prior start() and must not throw.
        verifyNoInteractions(mockMetrics);
    }

    // ---- DLQ topic validation tests (no thread start required) ----

    @Test
    public void testDlqEmptyTopicNameFailsValidation() throws Exception {
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.empty());
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());

        stateManager = builder().withCacheHelper(cacheHelper).build();
        Throwable cause = getCause(stateManager.dlq(param()));
        assertInstanceOf(ConfigException.class, cause);
        assertTrue(cause.getMessage().contains("empty"));
        verifyNoInteractions(mockMetrics);
    }

    @Test
    public void testDlqTopicStartingWithUnderscoreFailsValidation() throws Exception {
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.of("__internal_dlq"));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());

        stateManager = builder().withCacheHelper(cacheHelper).build();
        Throwable cause = getCause(stateManager.dlq(param()));
        assertInstanceOf(ConfigException.class, cause);
        assertTrue(cause.getMessage().contains("__"));
        verifyNoInteractions(mockMetrics);
    }

    @Test
    public void testDlqExistingTopicWithoutDlqConfigFailsValidation() throws Exception {
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.of(DLQ_TOPIC));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());
        when(cacheHelper.containsTopic(DLQ_TOPIC)).thenReturn(true);
        when(cacheHelper.isDlqEnabledOnTopic(DLQ_TOPIC)).thenReturn(false);

        stateManager = builder().withCacheHelper(cacheHelper).build();
        Throwable cause = getCause(stateManager.dlq(param()));
        assertInstanceOf(ConfigException.class, cause);
        assertTrue(cause.getMessage().contains("DLQ is not enabled"));
        verifyNoInteractions(mockMetrics);
    }

    @Test
    public void testDlqTopicMissingAndAutoCreateDisabledFailsValidation() throws Exception {
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.of(DLQ_TOPIC));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());
        when(cacheHelper.containsTopic(DLQ_TOPIC)).thenReturn(false);
        when(cacheHelper.isDlqAutoTopicCreateEnabled()).thenReturn(false);

        stateManager = builder().withCacheHelper(cacheHelper).build();
        Throwable cause = getCause(stateManager.dlq(param()));
        assertInstanceOf(ConfigException.class, cause);
        assertTrue(cause.getMessage().contains("auto create is disabled"));
        verifyNoInteractions(mockMetrics);
    }

    @Test
    public void testDlqTopicPrefixMismatchFailsValidation() throws Exception {
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.of(DLQ_TOPIC));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.of("required-prefix-"));
        when(cacheHelper.containsTopic(DLQ_TOPIC)).thenReturn(true);
        when(cacheHelper.isDlqEnabledOnTopic(DLQ_TOPIC)).thenReturn(true);

        stateManager = builder().withCacheHelper(cacheHelper).build();
        Throwable cause = getCause(stateManager.dlq(param()));
        assertInstanceOf(ConfigException.class, cause);
        assertTrue(cause.getMessage().contains("does not comply with the DLQ topic prefix"));
        verifyNoInteractions(mockMetrics);
    }

    @Test
    public void testDlqValidationFailureCompletesFutureBeforeStart() throws Exception {
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.empty());
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());

        // validateDlqTopic runs synchronously inside dlq(), so it should fail without the sender thread.
        stateManager = builder().withCacheHelper(cacheHelper).build();
        CompletableFuture<Void> result = stateManager.dlq(param());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFalse(result.isCancelled());
        verifyNoInteractions(mockMetrics);
    }

    // ---- Full integration tests ----

    @Test
    public void testDlqHappyPathExistingTopic() throws Exception {
        MockClient client = new MockClient(MOCK_TIME);
        List<ProduceRequest> capturedProduces = new ArrayList<>();
        client.prepareResponseFrom(
            body -> {
                if (body instanceof ProduceRequest pr) {
                    capturedProduces.add(pr);
                    return true;
                }
                return false;
            },
            successfulProduceResponse(0),
            DEFAULT_LEADER
        );

        stateManager = builder().withClient(client).build();
        stateManager.start();
        assertNull(stateManager.dlq(param()).get(10, TimeUnit.SECONDS));

        assertEquals(1, capturedProduces.size());
        assertDlqProduceRecordHeaders(capturedProduces.get(0), Map.of(
            0, new ExpectedDlqPartition(0L, 2L, Map.of(
                HEADER_DLQ_ERRORS_TOPIC, "source-topic",
                HEADER_DLQ_ERRORS_PARTITION, "0",
                HEADER_DLQ_ERRORS_GROUP, GROUP_ID,
                HEADER_DLQ_ERRORS_DELIVERY_COUNT, "1",
                HEADER_DLQ_ERRORS_MESSAGE, "simulated cause"
            ), List.of(), List.of())
        ));
        verify(mockMetrics).recordDLQProduce(GROUP_ID);
        verify(mockMetrics).recordDLQRecordWrite(GROUP_ID, 3);
        verify(mockMetrics, never()).recordDLQProduceFailed(any());
    }

    @Test
    public void testDlqTopicPrefixEmptyStringSkipsPrefixCheck() throws Exception {
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.of(DLQ_TOPIC));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.of(""));
        when(cacheHelper.containsTopic(DLQ_TOPIC)).thenReturn(true);
        when(cacheHelper.isDlqEnabledOnTopic(DLQ_TOPIC)).thenReturn(true);
        when(cacheHelper.isDlqAutoTopicCreateEnabled()).thenReturn(true);
        when(cacheHelper.topicName(SOURCE_TOPIC_ID)).thenReturn(Optional.of("source-topic"));
        when(cacheHelper.topicPartitionData(DLQ_TOPIC)).thenReturn(new TopicPartitionData(
            DLQ_TOPIC,
            Optional.of(1),
            Optional.of(DLQ_TOPIC_ID),
            List.of(DEFAULT_LEADER)
        ));
        when(cacheHelper.getClusterNodes()).thenReturn(List.of(DEFAULT_LEADER));

        MockClient client = new MockClient(MOCK_TIME);
        List<ProduceRequest> capturedProduces = new ArrayList<>();
        client.prepareResponseFrom(
            body -> {
                if (body instanceof ProduceRequest pr) {
                    capturedProduces.add(pr);
                    return true;
                }
                return false;
            },
            successfulProduceResponse(0),
            DEFAULT_LEADER
        );

        stateManager = builder()
            .withClient(client)
            .withCacheHelper(cacheHelper)
            .build();
        stateManager.start();
        assertNull(stateManager.dlq(param()).get(10, TimeUnit.SECONDS));

        assertEquals(1, capturedProduces.size());
        assertDlqProduceRecordHeaders(capturedProduces.get(0), Map.of(
            0, new ExpectedDlqPartition(0L, 2L, Map.of(
                HEADER_DLQ_ERRORS_TOPIC, "source-topic",
                HEADER_DLQ_ERRORS_PARTITION, "0",
                HEADER_DLQ_ERRORS_GROUP, GROUP_ID,
                HEADER_DLQ_ERRORS_DELIVERY_COUNT, "1",
                HEADER_DLQ_ERRORS_MESSAGE, "simulated cause"
            ), List.of(), List.of())
        ));
        verify(mockMetrics).recordDLQProduce(GROUP_ID);
        verify(mockMetrics).recordDLQRecordWrite(GROUP_ID, 3);
        verify(mockMetrics, never()).recordDLQProduceFailed(any());
    }

    @Test
    public void testDlqCreateTopicThenProduceSucceeds() throws Exception {
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.of(DLQ_TOPIC));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());
        when(cacheHelper.isDlqEnabledOnTopic(DLQ_TOPIC)).thenReturn(true);
        when(cacheHelper.isDlqAutoTopicCreateEnabled()).thenReturn(true);
        when(cacheHelper.getClusterNodes()).thenReturn(List.of(DEFAULT_LEADER));
        when(cacheHelper.topicName(SOURCE_TOPIC_ID)).thenReturn(Optional.of("source-topic"));
        when(cacheHelper.topicPartitionData(DLQ_TOPIC)).thenReturn(new TopicPartitionData(
            DLQ_TOPIC,
            Optional.of(1),
            Optional.of(DLQ_TOPIC_ID),
            List.of(DEFAULT_LEADER)
        ));
        when(cacheHelper.containsTopic(DLQ_TOPIC)).thenReturn(false);

        MockClient client = new MockClient(MOCK_TIME);
        List<ProduceRequest> capturedProduces = new ArrayList<>();
        client.prepareResponseFrom(
            body -> body instanceof CreateTopicsRequest,
            successfulCreateTopicsResponse(),
            DEFAULT_LEADER
        );
        client.prepareResponseFrom(
            body -> {
                if (body instanceof ProduceRequest pr) {
                    capturedProduces.add(pr);
                    return true;
                }
                return false;
            },
            successfulProduceResponse(0),
            DEFAULT_LEADER
        );

        stateManager = builder()
            .withClient(client)
            .withCacheHelper(cacheHelper)
            .build();
        stateManager.start();
        assertNull(stateManager.dlq(param()).get(10, TimeUnit.SECONDS));

        assertEquals(1, capturedProduces.size());
        assertDlqProduceRecordHeaders(capturedProduces.get(0), Map.of(
            0, new ExpectedDlqPartition(0L, 2L, Map.of(
                HEADER_DLQ_ERRORS_TOPIC, "source-topic",
                HEADER_DLQ_ERRORS_PARTITION, "0",
                HEADER_DLQ_ERRORS_GROUP, GROUP_ID,
                HEADER_DLQ_ERRORS_DELIVERY_COUNT, "1",
                HEADER_DLQ_ERRORS_MESSAGE, "simulated cause"
            ), List.of(), List.of())
        ));
        verify(mockMetrics).recordDLQProduce(GROUP_ID);
        verify(mockMetrics).recordDLQRecordWrite(GROUP_ID, 3);
        verify(mockMetrics, never()).recordDLQProduceFailed(any());
    }

    @Test
    public void testDlqCreateTopicFatalErrorFailsFuture() throws Exception {
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.of(DLQ_TOPIC));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());
        when(cacheHelper.isDlqAutoTopicCreateEnabled()).thenReturn(true);
        when(cacheHelper.getClusterNodes()).thenReturn(List.of(DEFAULT_LEADER));
        when(cacheHelper.containsTopic(DLQ_TOPIC)).thenReturn(false);

        MockClient client = new MockClient(MOCK_TIME);
        client.prepareResponseFrom(
            body -> body instanceof CreateTopicsRequest,
            createTopicsResponse(Errors.INVALID_REPLICATION_FACTOR),
            DEFAULT_LEADER
        );

        stateManager = builder()
            .withClient(client)
            .withCacheHelper(cacheHelper)
            .build();
        stateManager.start();
        Throwable cause = getCause(stateManager.dlq(param()));
        assertNotNull(cause);
        assertEquals(Errors.INVALID_REPLICATION_FACTOR.exception().getClass(), cause.getClass());
        // CreateTopics failed; produce was never attempted, so no DLQ metrics should fire.
        verifyNoInteractions(mockMetrics);
    }

    @Test
    public void testDlqCreateTopicNoClusterNodesFailsFuture() throws Exception {
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.of(DLQ_TOPIC));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());
        when(cacheHelper.isDlqAutoTopicCreateEnabled()).thenReturn(true);
        when(cacheHelper.containsTopic(DLQ_TOPIC)).thenReturn(false);
        when(cacheHelper.getClusterNodes()).thenReturn(List.of());

        stateManager = builder().withCacheHelper(cacheHelper).build();
        stateManager.start();
        Throwable cause = getCause(stateManager.dlq(param()));
        assertNotNull(cause);
        assertEquals(Errors.BROKER_NOT_AVAILABLE.exception().getClass(), cause.getClass());
        // No cluster node was available to send CreateTopics to; produce never attempted.
        verifyNoInteractions(mockMetrics);
    }

    @Test
    public void testDlqCreateTopicRetriesExhaustedFailsWithNetworkException() throws Exception {
        int maxAttempts = 3;
        // Force the create-topic path: configured DLQ topic does not yet exist in metadata.
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.of(DLQ_TOPIC));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());
        when(cacheHelper.isDlqAutoTopicCreateEnabled()).thenReturn(true);
        when(cacheHelper.containsTopic(DLQ_TOPIC)).thenReturn(false);
        when(cacheHelper.getClusterNodes()).thenReturn(List.of(DEFAULT_LEADER));

        // Real timer with tiny backoffs lets the exhaustion path actually fire in a few ms.
        Timer realTimer = new SystemTimerReaper("shareGroupDLQTestTimer",
            new SystemTimer("shareGroupDLQTestTimer"));
        try {
            MockClient client = new MockClient(MOCK_TIME);
            for (int i = 0; i < maxAttempts; i++) {
                client.prepareResponseFrom(
                    body -> body instanceof CreateTopicsRequest,
                    null,
                    DEFAULT_LEADER,
                    true
                );
            }

            stateManager = builder()
                .withClient(client)
                .withCacheHelper(cacheHelper)
                .withTimer(realTimer)
                .build();
            stateManager.start();

            Throwable cause = getCause(stateManager.dlq(param(), 1L, 5L, maxAttempts));
            assertNotNull(cause);
            assertEquals(Errors.NETWORK_EXCEPTION.exception().getClass(), cause.getClass());
            // CreateTopics retries exhausted; produce never attempted, so no DLQ metrics should fire.
            verifyNoInteractions(mockMetrics);
        } finally {
            Utils.closeQuietly(realTimer, "shareGroupDLQTestTimer");
        }
    }

    @Test
    public void testDlqCreateTopicPartialFailuresThenSucceeds() throws Exception {
        int maxAttempts = 3;
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.of(DLQ_TOPIC));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());
        when(cacheHelper.isDlqEnabledOnTopic(DLQ_TOPIC)).thenReturn(true);
        when(cacheHelper.isDlqAutoTopicCreateEnabled()).thenReturn(true);
        when(cacheHelper.containsTopic(DLQ_TOPIC)).thenReturn(false);
        when(cacheHelper.getClusterNodes()).thenReturn(List.of(DEFAULT_LEADER));
        when(cacheHelper.topicName(SOURCE_TOPIC_ID)).thenReturn(Optional.of("source-topic"));
        when(cacheHelper.topicPartitionData(DLQ_TOPIC)).thenReturn(new TopicPartitionData(
            DLQ_TOPIC,
            Optional.of(1),
            Optional.of(DLQ_TOPIC_ID),
            List.of(DEFAULT_LEADER)
        ));

        Timer realTimer = new SystemTimerReaper("shareGroupDLQTestTimer",
            new SystemTimer("shareGroupDLQTestTimer"));
        try {
            MockClient client = new MockClient(MOCK_TIME);
            List<ProduceRequest> capturedProduces = new ArrayList<>();
            // Two CreateTopics disconnects (retriable), then a successful create, then the produce succeeds.
            client.prepareResponseFrom(body -> body instanceof CreateTopicsRequest, null, DEFAULT_LEADER, true);
            client.prepareResponseFrom(body -> body instanceof CreateTopicsRequest, null, DEFAULT_LEADER, true);
            client.prepareResponseFrom(
                body -> body instanceof CreateTopicsRequest,
                successfulCreateTopicsResponse(),
                DEFAULT_LEADER
            );
            client.prepareResponseFrom(
                body -> {
                    if (body instanceof ProduceRequest pr) {
                        capturedProduces.add(pr);
                        return true;
                    }
                    return false;
                },
                successfulProduceResponse(0),
                DEFAULT_LEADER
            );

            stateManager = builder()
                .withClient(client)
                .withCacheHelper(cacheHelper)
                .withTimer(realTimer)
                .build();
            stateManager.start();

            assertNull(stateManager.dlq(param(), 1L, 5L, maxAttempts).get(5, TimeUnit.SECONDS));

            assertEquals(1, capturedProduces.size());
            assertDlqProduceRecordHeaders(capturedProduces.get(0), Map.of(
                0, new ExpectedDlqPartition(0L, 2L, Map.of(
                    HEADER_DLQ_ERRORS_TOPIC, "source-topic",
                    HEADER_DLQ_ERRORS_PARTITION, "0",
                    HEADER_DLQ_ERRORS_GROUP, GROUP_ID,
                    HEADER_DLQ_ERRORS_DELIVERY_COUNT, "1",
                    HEADER_DLQ_ERRORS_MESSAGE, "simulated cause"
                ), List.of(), List.of())
            ));
            // CreateTopics retried, but produce only ran once (after the eventual create success).
            verify(mockMetrics).recordDLQProduce(GROUP_ID);
            verify(mockMetrics).recordDLQRecordWrite(GROUP_ID, 3);
            verify(mockMetrics, never()).recordDLQProduceFailed(any());
        } finally {
            Utils.closeQuietly(realTimer, "shareGroupDLQTestTimer");
        }
    }

    @Test
    public void testDlqProducePartialFailuresThenSucceeds() throws Exception {
        int maxAttempts = 3;
        Timer realTimer = new SystemTimerReaper("shareGroupDLQTestTimer",
            new SystemTimer("shareGroupDLQTestTimer"));
        try {
            MockClient client = new MockClient(MOCK_TIME);
            List<ProduceRequest> capturedProduces = new ArrayList<>();
            // Two Produce disconnects (retriable), then a successful produce. Capture all attempts;
            // the retried attempts must carry the same headers as the successful one.
            MockClient.RequestMatcher captureProduce = body -> {
                if (body instanceof ProduceRequest pr) {
                    capturedProduces.add(pr);
                    return true;
                }
                return false;
            };
            client.prepareResponseFrom(captureProduce, null, DEFAULT_LEADER, true);
            client.prepareResponseFrom(captureProduce, null, DEFAULT_LEADER, true);
            client.prepareResponseFrom(captureProduce, successfulProduceResponse(0), DEFAULT_LEADER);

            stateManager = builder()
                .withClient(client)
                .withTimer(realTimer)
                .build();
            stateManager.start();

            assertNull(stateManager.dlq(param(), 1L, 5L, maxAttempts).get(5, TimeUnit.SECONDS));

            assertEquals(maxAttempts, capturedProduces.size());
            Map<Integer, ExpectedDlqPartition> expectedByPartition = Map.of(
                0, new ExpectedDlqPartition(0L, 2L, Map.of(
                    HEADER_DLQ_ERRORS_TOPIC, "source-topic",
                    HEADER_DLQ_ERRORS_PARTITION, "0",
                    HEADER_DLQ_ERRORS_GROUP, GROUP_ID,
                    HEADER_DLQ_ERRORS_DELIVERY_COUNT, "1",
                    HEADER_DLQ_ERRORS_MESSAGE, "simulated cause"
                ), List.of(), List.of())
            );
            for (ProduceRequest pr : capturedProduces) {
                assertDlqProduceRecordHeaders(pr, expectedByPartition);
            }
            // Each attempt (including retries) goes through generateRequests, so recordDLQProduce
            // is invoked once per attempt. recordDLQRecordWrite fires only on the final success.
            verify(mockMetrics, times(maxAttempts)).recordDLQProduce(GROUP_ID);
            verify(mockMetrics).recordDLQRecordWrite(GROUP_ID, 3);
            verify(mockMetrics, never()).recordDLQProduceFailed(any());
        } finally {
            Utils.closeQuietly(realTimer, "shareGroupDLQTestTimer");
        }
    }

    @Test
    public void testDlqProduceFatalErrorFailsFuture() throws Exception {
        MockClient client = new MockClient(MOCK_TIME);
        client.prepareResponseFrom(
            body -> body instanceof ProduceRequest,
            produceResponseWithError(Errors.INVALID_TOPIC_EXCEPTION),
            DEFAULT_LEADER
        );

        stateManager = builder().withClient(client).build();
        stateManager.start();
        Throwable cause = getCause(stateManager.dlq(param()));
        assertNotNull(cause);
        assertEquals(Errors.INVALID_TOPIC_EXCEPTION.exception().getClass(), cause.getClass());
        verify(mockMetrics).recordDLQProduce(GROUP_ID);
        verify(mockMetrics).recordDLQProduceFailed(GROUP_ID);
        verify(mockMetrics, never()).recordDLQRecordWrite(any(), anyInt());
    }

    @Test
    public void testDlqProduceEmptyResponseFailsFuture() throws Exception {
        MockClient client = new MockClient(MOCK_TIME);
        client.prepareResponseFrom(
            body -> body instanceof ProduceRequest,
            new ProduceResponse(new ProduceResponseData()),
            DEFAULT_LEADER
        );

        stateManager = builder().withClient(client).build();
        stateManager.start();
        Throwable cause = getCause(stateManager.dlq(param()));
        assertNotNull(cause);
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.exception().getClass(), cause.getClass());
        verify(mockMetrics).recordDLQProduce(GROUP_ID);
        // Empty produce-response paths return UNKNOWN_SERVER_ERROR via requestErrorResponse without
        // touching recordDLQProduceFailed (that's only invoked from the inner/outer default cases).
        verify(mockMetrics, never()).recordDLQProduceFailed(any());
        verify(mockMetrics, never()).recordDLQRecordWrite(any(), anyInt());
    }

    @Test
    public void testDlqProduceDisconnectIsRetriedNotImmediatelyFailed() throws Exception {
        MockClient client = new MockClient(MOCK_TIME);
        // Null response body + disconnected=true triggers the wasDisconnected() branch in
        // ShareGroupDLQStateManager#checkResponseError. Since the disconnect is retriable, the
        // future must NOT complete on the first attempt - we just verify the retry was scheduled
        // rather than waiting for full retry exhaustion (which can take ~30s due to the
        // hard-coded exponential backoff in ShareGroupDLQStateManager).
        client.prepareResponseFrom(
            body -> body instanceof ProduceRequest,
            null,
            DEFAULT_LEADER,
            true
        );

        stateManager = builder().withClient(client).build();
        stateManager.start();
        CompletableFuture<Void> result = stateManager.dlq(param());
        // Brief wait so the disconnect response can be processed; the future should remain
        // pending because the retry has been scheduled rather than completing exceptionally.
        try {
            result.get(500, TimeUnit.MILLISECONDS);
            fail("Expected the future to remain incomplete while retry is pending");
        } catch (TimeoutException expected) {
            assertFalse(result.isDone());
        }
        // The first attempt did go out to the wire, so recordDLQProduce fired once. The retry is
        // still pending in the timer, so neither success nor failure metrics should have landed.
        verify(mockMetrics, times(1)).recordDLQProduce(GROUP_ID);
        verify(mockMetrics, never()).recordDLQRecordWrite(any(), anyInt());
        verify(mockMetrics, never()).recordDLQProduceFailed(any());
    }

    @Test
    public void testDlqProduceRetriesExhaustedFailsWithNetworkException() throws Exception {
        int maxAttempts = 3;
        // Real timer with tiny backoffs lets the exhaustion path actually fire in a few ms,
        // rather than the ~30s a MockTimer-less production-like setup would take.
        Timer realTimer = new SystemTimerReaper("shareGroupDLQTestTimer",
            new SystemTimer("shareGroupDLQTestTimer"));
        try {
            MockClient client = new MockClient(MOCK_TIME);
            // Each retry consumes one prepared response; stage one disconnect per attempt.
            for (int i = 0; i < maxAttempts; i++) {
                client.prepareResponseFrom(
                    body -> body instanceof ProduceRequest,
                    null,
                    DEFAULT_LEADER,
                    true
                );
            }

            stateManager = builder()
                .withClient(client)
                .withTimer(realTimer)
                .build();
            stateManager.start();

            Throwable cause = getCause(stateManager.dlq(param(), 1L, 5L, maxAttempts));
            assertNotNull(cause);
            assertEquals(Errors.NETWORK_EXCEPTION.exception().getClass(), cause.getClass());
            // NETWORK_EXCEPTION exhaustion now records the failure metric (production records it
            // on the canAttempt()==false branch before requestErrorResponse). Only the final
            // exhaustion records the failure - the earlier retried attempts don't.
            verify(mockMetrics, times(maxAttempts)).recordDLQProduce(GROUP_ID);
            verify(mockMetrics).recordDLQProduceFailed(GROUP_ID);
            verify(mockMetrics, never()).recordDLQRecordWrite(any(), anyInt());
        } finally {
            Utils.closeQuietly(realTimer, "shareGroupDLQTestTimer");
        }
    }

    @Test
    public void testDlqProduceNotLeaderOrFollowerExhaustsAndRecordsFailure() throws Exception {
        int maxAttempts = 3;
        // Real timer with tiny backoffs keeps the exhaustion path within milliseconds.
        Timer realTimer = new SystemTimerReaper("shareGroupDLQTestTimer",
            new SystemTimer("shareGroupDLQTestTimer"));
        try {
            MockClient client = new MockClient(MOCK_TIME);
            // Each attempt returns a partition-level NOT_LEADER_OR_FOLLOWER on partition 0,
            // which is retriable up to maxAttempts.
            for (int i = 0; i < maxAttempts; i++) {
                client.prepareResponseFrom(
                    body -> body instanceof ProduceRequest,
                    produceResponseWithError(Errors.NOT_LEADER_OR_FOLLOWER),
                    DEFAULT_LEADER
                );
            }

            stateManager = builder()
                .withClient(client)
                .withTimer(realTimer)
                .build();
            stateManager.start();

            Throwable cause = getCause(stateManager.dlq(param(), 1L, 5L, maxAttempts));
            assertNotNull(cause);
            // The exhaustion path raises a generic Exception (not Errors.NOT_LEADER_OR_FOLLOWER's
            // typed exception), so just check the message rather than the class.
            assertTrue(cause.getMessage().contains("Exhausted max retries"));

            // Each attempt is its own outgoing produce request -> recordDLQProduce fires
            // maxAttempts times. The final attempt exhausts retries and records the failure
            // (the new inner NOT_LEADER_OR_FOLLOWER exhaustion branch).
            verify(mockMetrics, times(maxAttempts)).recordDLQProduce(GROUP_ID);
            verify(mockMetrics).recordDLQProduceFailed(GROUP_ID);
            verify(mockMetrics, never()).recordDLQRecordWrite(any(), anyInt());
        } finally {
            Utils.closeQuietly(realTimer, "shareGroupDLQTestTimer");
        }
    }

    @Test
    public void testDlqTwoEnqueuedRecordsBothComplete() throws Exception {
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(GROUP_ID)).thenReturn(Optional.of(DLQ_TOPIC));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());
        when(cacheHelper.containsTopic(DLQ_TOPIC)).thenReturn(true);
        when(cacheHelper.isDlqEnabledOnTopic(DLQ_TOPIC)).thenReturn(true);
        when(cacheHelper.isDlqAutoTopicCreateEnabled()).thenReturn(true);
        when(cacheHelper.getClusterNodes()).thenReturn(List.of(DEFAULT_LEADER));
        when(cacheHelper.topicName(SOURCE_TOPIC_ID)).thenReturn(Optional.of("source-topic"));
        when(cacheHelper.topicPartitionData(DLQ_TOPIC)).thenReturn(new TopicPartitionData(
            DLQ_TOPIC,
            Optional.of(2),
            Optional.of(DLQ_TOPIC_ID),
            List.of(DEFAULT_LEADER, DEFAULT_LEADER)
        ));

        // Whether the two handlers end up coalesced into a single produce request or are sent as
        // two separate requests depends on internal scheduling. Provide a multi-partition response
        // that satisfies either case: each request will see partition indices 0 and 1 in the
        // response, and the handler picks out the index that matches its destination partition.
        ProduceResponseData.TopicProduceResponse topicResp = new ProduceResponseData.TopicProduceResponse()
            .setTopicId(DLQ_TOPIC_ID)
            .setPartitionResponses(List.of(
                new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(0)
                    .setErrorCode(Errors.NONE.code()),
                new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(1)
                    .setErrorCode(Errors.NONE.code())
            ));
        ProduceResponseData.TopicProduceResponseCollection collection =
            new ProduceResponseData.TopicProduceResponseCollection();
        collection.add(topicResp);

        MockClient client = new MockClient(MOCK_TIME);
        List<ProduceRequest> capturedProduces = new ArrayList<>();
        MockClient.RequestMatcher captureProduce = body -> {
            if (body instanceof ProduceRequest pr) {
                capturedProduces.add(pr);
                return true;
            }
            return false;
        };
        // Two identical responses cover the non-coalesced path.
        client.prepareResponseFrom(captureProduce,
            new ProduceResponse(new ProduceResponseData().setResponses(collection.duplicate())),
            DEFAULT_LEADER);
        client.prepareResponseFrom(captureProduce,
            new ProduceResponse(new ProduceResponseData().setResponses(collection.duplicate())),
            DEFAULT_LEADER);

        stateManager = builder()
            .withClient(client)
            .withCacheHelper(cacheHelper)
            .build();
        stateManager.start();
        ShareGroupDLQRecordParameter p0 = new ShareGroupDLQRecordParameter(
            GROUP_ID,
            new TopicIdPartition(SOURCE_TOPIC_ID, 0, "source-topic"),
            0L, 0L,
            Optional.empty(), Optional.empty());
        ShareGroupDLQRecordParameter p1 = new ShareGroupDLQRecordParameter(
            GROUP_ID,
            new TopicIdPartition(SOURCE_TOPIC_ID, 1, "source-topic"),
            0L, 0L,
            Optional.empty(), Optional.empty());

        CompletableFuture<Void> r0 = stateManager.dlq(p0);
        CompletableFuture<Void> r1 = stateManager.dlq(p1);

        assertNull(r0.get(10, TimeUnit.SECONDS));
        assertNull(r1.get(10, TimeUnit.SECONDS));

        // Two source partitions map to two distinct DLQ partition indices (0 and 1, given 2
        // DLQ partitions). Whether they end up coalesced into a single request (one topic, two
        // partitions) or sent as two requests (each with one partition) depends on scheduling.
        ExpectedDlqPartition expectedDlqPartition0 = new ExpectedDlqPartition(0L, 0L, Map.of(
            HEADER_DLQ_ERRORS_TOPIC, "source-topic",
            HEADER_DLQ_ERRORS_PARTITION, "0",
            HEADER_DLQ_ERRORS_GROUP, GROUP_ID
        ), List.of(), List.of());
        ExpectedDlqPartition expectedDlqPartition1 = new ExpectedDlqPartition(0L, 0L, Map.of(
            HEADER_DLQ_ERRORS_TOPIC, "source-topic",
            HEADER_DLQ_ERRORS_PARTITION, "1",
            HEADER_DLQ_ERRORS_GROUP, GROUP_ID
        ), List.of(), List.of());
        if (capturedProduces.size() == 1) {
            assertDlqProduceRecordHeaders(capturedProduces.get(0), Map.of(
                0, expectedDlqPartition0,
                1, expectedDlqPartition1
            ));
        } else {
            assertEquals(2, capturedProduces.size(),
                "Expected coalesced (1 request) or non-coalesced (2 requests), got " + capturedProduces.size());
            for (ProduceRequest pr : capturedProduces) {
                int dlqPartitionIndex = pr.data().topicData().iterator().next().partitionData().get(0).index();
                ExpectedDlqPartition expected = dlqPartitionIndex == 0 ? expectedDlqPartition0 : expectedDlqPartition1;
                assertDlqProduceRecordHeaders(pr, Map.of(dlqPartitionIndex, expected));
            }
        }
        // Each handler succeeds with 1 record (offsets 0..0). recordDLQProduce is now invoked
        // once per handler inside topicProduceData() (not deduped per-group-per-request as it
        // was previously in coalesceProduceRequests), so two handlers always produce two calls
        // regardless of whether the SendThread coalesces them into a single produce request.
        verify(mockMetrics, times(2)).recordDLQRecordWrite(GROUP_ID, 1);
        verify(mockMetrics, times(2)).recordDLQProduce(GROUP_ID);
        verify(mockMetrics, never()).recordDLQProduceFailed(any());
    }

    @Test
    public void testDlqResolvesSourceTopicNameViaCacheHelperWhenMissing() throws Exception {
        MockClient client = new MockClient(MOCK_TIME);
        List<ProduceRequest> capturedProduces = new ArrayList<>();
        client.prepareResponseFrom(
            body -> {
                if (body instanceof ProduceRequest pr) {
                    capturedProduces.add(pr);
                    return true;
                }
                return false;
            },
            successfulProduceResponse(0),
            DEFAULT_LEADER
        );

        stateManager = builder().withClient(client).build();
        stateManager.start();
        ShareGroupDLQRecordParameter p = new ShareGroupDLQRecordParameter(
            GROUP_ID,
            new TopicIdPartition(SOURCE_TOPIC_ID, 0, null),
            0L, 0L,
            Optional.empty(), Optional.empty());
        assertNull(stateManager.dlq(p).get(10, TimeUnit.SECONDS));

        assertEquals(1, capturedProduces.size());
        // Source topic name was null in the parameter; the manager must have resolved it via
        // ShareGroupDLQMetadataCacheHelper.topicName(SOURCE_TOPIC_ID), which the happy helper
        // returns as "source-topic".
        assertDlqProduceRecordHeaders(capturedProduces.get(0), Map.of(
            0, new ExpectedDlqPartition(0L, 0L, Map.of(
                HEADER_DLQ_ERRORS_TOPIC, "source-topic",
                HEADER_DLQ_ERRORS_PARTITION, "0",
                HEADER_DLQ_ERRORS_GROUP, GROUP_ID
            ), List.of(), List.of())
        ));
        verify(mockMetrics).recordDLQProduce(GROUP_ID);
        verify(mockMetrics).recordDLQRecordWrite(GROUP_ID, 1);
        verify(mockMetrics, never()).recordDLQProduceFailed(any());
    }

    @Test
    public void testDlqMultipleGroupsWithMixedOutcomes() throws Exception {
        String groupA = "group-a";
        String groupB = "group-b";
        String groupC = "group-c";

        // All three groups share the same DLQ topic with 3 partitions on the same leader, so
        // partition 0 -> group-a, partition 1 -> group-b, partition 2 -> group-c (via the
        // sourcePartition % numPartitions mapping in populateDLQTopicData).
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(anyString())).thenReturn(Optional.of(DLQ_TOPIC));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());
        when(cacheHelper.containsTopic(DLQ_TOPIC)).thenReturn(true);
        when(cacheHelper.isDlqEnabledOnTopic(DLQ_TOPIC)).thenReturn(true);
        when(cacheHelper.isDlqAutoTopicCreateEnabled()).thenReturn(true);
        when(cacheHelper.getClusterNodes()).thenReturn(List.of(DEFAULT_LEADER));
        when(cacheHelper.topicName(SOURCE_TOPIC_ID)).thenReturn(Optional.of("source-topic"));
        when(cacheHelper.topicPartitionData(DLQ_TOPIC)).thenReturn(new TopicPartitionData(
            DLQ_TOPIC,
            Optional.of(3),
            Optional.of(DLQ_TOPIC_ID),
            List.of(DEFAULT_LEADER, DEFAULT_LEADER, DEFAULT_LEADER)
        ));

        // Each prepared produce response carries partition 0=NONE, 1=INVALID_TOPIC_EXCEPTION,
        // 2=NONE so that whichever physical request lands at the broker - one fully coalesced
        // request, two requests, or three separate requests, depending on SendThread/inFlight
        // timing - each handler still finds its own destination partition in the response.
        ProduceResponseData.TopicProduceResponse topicResp = new ProduceResponseData.TopicProduceResponse()
            .setTopicId(DLQ_TOPIC_ID)
            .setPartitionResponses(List.of(
                new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(0)
                    .setErrorCode(Errors.NONE.code()),
                new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(1)
                    .setErrorCode(Errors.INVALID_TOPIC_EXCEPTION.code())
                    .setErrorMessage(Errors.INVALID_TOPIC_EXCEPTION.message()),
                new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(2)
                    .setErrorCode(Errors.NONE.code())
            ));
        ProduceResponseData.TopicProduceResponseCollection collection =
            new ProduceResponseData.TopicProduceResponseCollection();
        collection.add(topicResp);
        ProduceResponse mixedResp = new ProduceResponse(new ProduceResponseData().setResponses(collection));

        MockClient client = new MockClient(MOCK_TIME);
        // Over-prepare: there may be 1, 2, or 3 outgoing produce requests depending on how the
        // SendThread coalesces. Unused prepared responses are harmless.
        for (int i = 0; i < 3; i++) {
            client.prepareResponseFrom(body -> body instanceof ProduceRequest, mixedResp, DEFAULT_LEADER);
        }

        stateManager = builder().withClient(client).withCacheHelper(cacheHelper).build();
        stateManager.start();

        ShareGroupDLQRecordParameter pA = new ShareGroupDLQRecordParameter(
            groupA,
            new TopicIdPartition(SOURCE_TOPIC_ID, 0, "source-topic"),
            0L, 0L,
            Optional.empty(), Optional.empty());
        ShareGroupDLQRecordParameter pB = new ShareGroupDLQRecordParameter(
            groupB,
            new TopicIdPartition(SOURCE_TOPIC_ID, 1, "source-topic"),
            0L, 0L,
            Optional.empty(), Optional.empty());
        ShareGroupDLQRecordParameter pC = new ShareGroupDLQRecordParameter(
            groupC,
            new TopicIdPartition(SOURCE_TOPIC_ID, 2, "source-topic"),
            0L, 0L,
            Optional.empty(), Optional.empty());

        CompletableFuture<Void> rA = stateManager.dlq(pA);
        CompletableFuture<Void> rB = stateManager.dlq(pB);
        CompletableFuture<Void> rC = stateManager.dlq(pC);

        assertNull(rA.get(5, TimeUnit.SECONDS));
        Throwable causeB = getCause(rB);
        assertEquals(Errors.INVALID_TOPIC_EXCEPTION.exception().getClass(), causeB.getClass());
        assertNull(rC.get(5, TimeUnit.SECONDS));

        // recordDLQProduce is invoked once per handler inside topicProduceData(). Each group
        // has a single handler that runs through exactly one attempt (no retries on this path),
        // so each group sees exactly one metric call regardless of how the SendThread coalesces.
        verify(mockMetrics).recordDLQProduce(groupA);
        verify(mockMetrics).recordDLQProduce(groupB);
        verify(mockMetrics).recordDLQProduce(groupC);

        // Per-group write count records only for the successful groups.
        verify(mockMetrics).recordDLQRecordWrite(groupA, 1);
        verify(mockMetrics).recordDLQRecordWrite(groupC, 1);
        verify(mockMetrics, never()).recordDLQRecordWrite(eq(groupB), anyInt());

        // Failure metric fires only for groupB (the INVALID_TOPIC_EXCEPTION partition). Production
        // now records the metric before completing the future, so no timeout-bridge is needed.
        verify(mockMetrics).recordDLQProduceFailed(groupB);
        verify(mockMetrics, never()).recordDLQProduceFailed(groupA);
        verify(mockMetrics, never()).recordDLQProduceFailed(groupC);

        // Aggregate sanity check: total recordDLQProduce calls (3) strictly exceeds total
        // recordDLQProduceFailed calls (1), demonstrating that "some failed and failed < total".
        long produceCount = Mockito.mockingDetails(mockMetrics).getInvocations().stream()
            .filter(inv -> inv.getMethod().getName().equals("recordDLQProduce"))
            .count();
        long produceFailedCount = Mockito.mockingDetails(mockMetrics).getInvocations().stream()
            .filter(inv -> inv.getMethod().getName().equals("recordDLQProduceFailed"))
            .count();
        assertEquals(3, produceCount);
        assertEquals(1, produceFailedCount);
        assertTrue(produceFailedCount < produceCount,
            "Expected recordDLQProduceFailed count (" + produceFailedCount + ") < recordDLQProduce count (" + produceCount + ")");
    }

    @Test
    public void testMultipleAccumulatedHandlersInNodeRPCMap() throws Exception {
        MockClient client = new MockClient(MOCK_TIME);
        ExecutorService executor = Executors.newFixedThreadPool(1);

        client.prepareResponseFrom(body -> true, null, DEFAULT_LEADER);

        stateManager = builder().withClient(client).withCacheHelper(cacheHelper(DEFAULT_LEADER)).build();

        Future<Boolean> done = executor.submit(() -> {
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start <= TestUtils.DEFAULT_MAX_WAIT_MS) {    // keep checking for a few secs
                List<ShareGroupDLQStateManager.ProduceRequestHandler> handlers = stateManager.nodeRPCMap().get(DEFAULT_LEADER);
                if (handlers != null && handlers.size() > 2) {
                    return true;
                }
            }
            return false;
        });

        stateManager.start();

        // Multiple dlq() calls for the same group. They all target the same leader (DEFAULT_LEADER),
        // so after the first iteration marks that node as in-flight, the rest accumulate in
        // nodeRPCMap and never get cleared.
        for (int i = 0; i < 10; i++) {
            stateManager.dlq(new ShareGroupDLQRecordParameter(
                GROUP_ID,
                new TopicIdPartition(SOURCE_TOPIC_ID, 0, "source-topic"),
                0L, 0L,
                Optional.empty(), Optional.empty()));
        }

        // Wait until the callback observes nodeRPCMap with more than 2 handlers piled up.
        TestUtils.waitForCondition(done::get, TestUtils.DEFAULT_MAX_WAIT_MS, 10L, () -> {
            executor.shutdown();
            return "unable to verify batching";
        });
        executor.shutdown();
    }

    // ---- Direct unit tests for coalesceProduceRequests ----

    @Test
    public void testCoalesceProduceRequestsWithEmptyHandlerListProducesEmptyRequest() {
        ShareGroupDLQStateManager.CoalesceResults result =
            ShareGroupDLQStateManager.coalesceProduceRequests(List.of());

        assertTrue(result.liveHandlers().isEmpty());
        ProduceRequest request = ((ProduceRequest.Builder) result.request()).build();
        assertTrue(request.data().topicData().isEmpty());
    }

    @Test
    public void testCoalesceProduceRequestsWithSingleHandlerProducesOneTopicOnePartition() throws Exception {
        stateManager = builder().build();
        ShareGroupDLQStateManager.ProduceRequestHandler handler =
            newHandlerForCoalesceTest(stateManager, GROUP_ID, 0);
        handler.populateDLQTopicData();

        ShareGroupDLQStateManager.CoalesceResults result =
            ShareGroupDLQStateManager.coalesceProduceRequests(List.of(handler));

        assertEquals(List.of(handler), result.liveHandlers());

        ProduceRequest request = ((ProduceRequest.Builder) result.request()).build();
        assertEquals(1, request.data().topicData().size());
        ProduceRequestData.TopicProduceData topic = request.data().topicData().iterator().next();
        assertEquals(DLQ_TOPIC_ID, topic.topicId());
        assertEquals(1, topic.partitionData().size());
        assertEquals(0, topic.partitionData().get(0).index());

        // topicProduceData() fires recordDLQProduce once per handler as a side effect.
        verify(mockMetrics).recordDLQProduce(GROUP_ID);
    }

    @Test
    public void testCoalesceProduceRequestsMergesPartitionsForSameDlqTopic() throws Exception {
        // Cache helper exposes a single DLQ topic with 2 partitions on the same leader so that
        // two handlers (source partitions 0 and 1) map to DLQ partitions 0 and 1 respectively.
        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(anyString())).thenReturn(Optional.of(DLQ_TOPIC));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());
        when(cacheHelper.containsTopic(DLQ_TOPIC)).thenReturn(true);
        when(cacheHelper.isDlqEnabledOnTopic(DLQ_TOPIC)).thenReturn(true);
        when(cacheHelper.isDlqAutoTopicCreateEnabled()).thenReturn(true);
        when(cacheHelper.topicPartitionData(DLQ_TOPIC)).thenReturn(new TopicPartitionData(
            DLQ_TOPIC,
            Optional.of(2),
            Optional.of(DLQ_TOPIC_ID),
            List.of(DEFAULT_LEADER, DEFAULT_LEADER)
        ));

        stateManager = builder().withCacheHelper(cacheHelper).build();
        ShareGroupDLQStateManager.ProduceRequestHandler h0 =
            newHandlerForCoalesceTest(stateManager, GROUP_ID, 0);
        ShareGroupDLQStateManager.ProduceRequestHandler h1 =
            newHandlerForCoalesceTest(stateManager, GROUP_ID, 1);
        h0.populateDLQTopicData();
        h1.populateDLQTopicData();

        ShareGroupDLQStateManager.CoalesceResults result =
            ShareGroupDLQStateManager.coalesceProduceRequests(List.of(h0, h1));

        assertEquals(List.of(h0, h1), result.liveHandlers());

        ProduceRequest request = ((ProduceRequest.Builder) result.request()).build();
        assertEquals(1, request.data().topicData().size(),
            "Both handlers share a DLQ topic so they should coalesce into a single topic entry");
        ProduceRequestData.TopicProduceData topic = request.data().topicData().iterator().next();
        assertEquals(DLQ_TOPIC_ID, topic.topicId());
        Set<Integer> partitionIndices = topic.partitionData().stream()
            .map(ProduceRequestData.PartitionProduceData::index)
            .collect(Collectors.toSet());
        assertEquals(Set.of(0, 1), partitionIndices);

        // recordDLQProduce fires once per handler (the metric lives in topicProduceData()).
        verify(mockMetrics, times(2)).recordDLQProduce(GROUP_ID);
    }

    @Test
    public void testCoalesceProduceRequestsKeepsDifferentDlqTopicsSeparate() throws Exception {
        String groupA = "group-a";
        String groupB = "group-b";
        String dlqTopicA = "dlq-topic-a";
        String dlqTopicB = "dlq-topic-b";
        Uuid dlqTopicAId = Uuid.randomUuid();
        Uuid dlqTopicBId = Uuid.randomUuid();

        ShareGroupDLQMetadataCacheHelper cacheHelper = mock(ShareGroupDLQMetadataCacheHelper.class);
        when(cacheHelper.shareGroupDlqTopic(groupA)).thenReturn(Optional.of(dlqTopicA));
        when(cacheHelper.shareGroupDlqTopic(groupB)).thenReturn(Optional.of(dlqTopicB));
        when(cacheHelper.shareGroupDlqTopicPrefix()).thenReturn(Optional.empty());
        when(cacheHelper.containsTopic(dlqTopicA)).thenReturn(true);
        when(cacheHelper.containsTopic(dlqTopicB)).thenReturn(true);
        when(cacheHelper.isDlqEnabledOnTopic(anyString())).thenReturn(true);
        when(cacheHelper.isDlqAutoTopicCreateEnabled()).thenReturn(true);
        when(cacheHelper.topicPartitionData(dlqTopicA)).thenReturn(new TopicPartitionData(
            dlqTopicA, Optional.of(1), Optional.of(dlqTopicAId), List.of(DEFAULT_LEADER)));
        when(cacheHelper.topicPartitionData(dlqTopicB)).thenReturn(new TopicPartitionData(
            dlqTopicB, Optional.of(1), Optional.of(dlqTopicBId), List.of(DEFAULT_LEADER)));

        stateManager = builder().withCacheHelper(cacheHelper).build();
        ShareGroupDLQStateManager.ProduceRequestHandler hA =
            newHandlerForCoalesceTest(stateManager, groupA, 0);
        ShareGroupDLQStateManager.ProduceRequestHandler hB =
            newHandlerForCoalesceTest(stateManager, groupB, 0);
        hA.populateDLQTopicData();
        hB.populateDLQTopicData();

        ShareGroupDLQStateManager.CoalesceResults result =
            ShareGroupDLQStateManager.coalesceProduceRequests(List.of(hA, hB));

        assertEquals(List.of(hA, hB), result.liveHandlers());

        ProduceRequest request = ((ProduceRequest.Builder) result.request()).build();
        assertEquals(2, request.data().topicData().size(),
            "Different DLQ topic ids must remain in separate TopicProduceData entries");
        Set<Uuid> topicIds = new HashSet<>();
        request.data().topicData().forEach(topic -> topicIds.add(topic.topicId()));
        assertEquals(Set.of(dlqTopicAId, dlqTopicBId), topicIds);

        verify(mockMetrics).recordDLQProduce(groupA);
        verify(mockMetrics).recordDLQProduce(groupB);
    }

    @Test
    public void testCoalesceProduceRequestsSkipsHandlerWhoseTopicProduceDataThrows() throws Exception {
        stateManager = builder().build();

        // Good handler: populateDLQTopicData() has been called, so topicProduceData() succeeds.
        CompletableFuture<Void> goodFuture = new CompletableFuture<>();
        ShareGroupDLQStateManager.ProduceRequestHandler good = stateManager.new ProduceRequestHandler(
            new ShareGroupDLQRecordParameter(GROUP_ID,
                new TopicIdPartition(SOURCE_TOPIC_ID, 0, "source-topic"),
                0L, 0L,
                Optional.empty(), Optional.empty()),
            goodFuture,
            ShareGroupDLQStateManager.REQUEST_BACKOFF_MS,
            ShareGroupDLQStateManager.REQUEST_BACKOFF_MAX_MS,
            3);
        good.populateDLQTopicData();

        // Broken handler: populateDLQTopicData() was never called, so dlqTopicPartitionData is
        // null and topicProduceData() will NPE. coalesceProduceRequests must catch, call
        // requestErrorResponse to fail the future, and drop the handler from liveHandlers.
        CompletableFuture<Void> brokenFuture = new CompletableFuture<>();
        ShareGroupDLQStateManager.ProduceRequestHandler broken = stateManager.new ProduceRequestHandler(
            new ShareGroupDLQRecordParameter(GROUP_ID,
                new TopicIdPartition(SOURCE_TOPIC_ID, 0, "source-topic"),
                0L, 0L,
                Optional.empty(), Optional.empty()),
            brokenFuture,
            ShareGroupDLQStateManager.REQUEST_BACKOFF_MS,
            ShareGroupDLQStateManager.REQUEST_BACKOFF_MAX_MS,
            3);

        ShareGroupDLQStateManager.CoalesceResults result =
            ShareGroupDLQStateManager.coalesceProduceRequests(List.of(good, broken));

        assertEquals(List.of(good), result.liveHandlers(),
            "Only the handler whose topicProduceData() succeeded should appear in liveHandlers");
        assertFalse(goodFuture.isDone(), "Good handler's future must be untouched by coalesce");
        assertTrue(brokenFuture.isCompletedExceptionally(),
            "Broken handler's future must be completed exceptionally");
        Throwable cause = getCause(brokenFuture);
        assertInstanceOf(NullPointerException.class, cause);

        // The resulting request still contains the surviving handler's data.
        ProduceRequest request = ((ProduceRequest.Builder) result.request()).build();
        assertEquals(1, request.data().topicData().size());
        assertEquals(DLQ_TOPIC_ID, request.data().topicData().iterator().next().topicId());
    }

    // --- DLQ record with copy record enabled ---

    @Test
    public void testDLQRecordCopyEnabled() throws Exception {
        MockClient client = new MockClient(MOCK_TIME);
        List<ProduceRequest> capturedProduces = new ArrayList<>();
        client.prepareResponseFrom(
            body -> {
                if (body instanceof ProduceRequest pr) {
                    capturedProduces.add(pr);
                    return true;
                }
                return false;
            },
            successfulProduceResponse(0),
            DEFAULT_LEADER
        );

        ShareGroupDLQRecordParameter param = param();
        byte[] keyData1 = "key1".getBytes(StandardCharsets.UTF_8);
        byte[] valueData1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] keyData2 = "key2".getBytes(StandardCharsets.UTF_8);
        byte[] valueData2 = "value2".getBytes(StandardCharsets.UTF_8);
        byte[] keyData3 = "key3".getBytes(StandardCharsets.UTF_8);
        byte[] valueData3 = "value3".getBytes(StandardCharsets.UTF_8);
        LogReader logReader = mock(LogReader.class);
        LogReadResult readResult = mock(LogReadResult.class);
        when(readResult.error()).thenReturn(Errors.NONE);
        when(readResult.info()).thenReturn(new FetchDataInfo(
            null,
            MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(MOCK_TIME.milliseconds(), keyData1, valueData1),
                new SimpleRecord(MOCK_TIME.milliseconds(), keyData2, valueData2),
                new SimpleRecord(MOCK_TIME.milliseconds(), keyData3, valueData3)
            )
        ));
        LinkedHashMap<TopicIdPartition, LogReadResult> readResultMap = new LinkedHashMap<>();
        readResultMap.put(param.topicIdPartition(), readResult);
        when(logReader.read(any(), anySet(), any(), any()))
            .thenReturn(readResultMap);

        ShareGroupDLQMetadataCacheHelper cacheHelper = cacheHelper(DEFAULT_LEADER);
        when(cacheHelper.isShareGroupDlqCopyRecordEnabled(any())).thenReturn(true);
        stateManager = builder().withClient(client).withLogReader(logReader).withCacheHelper(cacheHelper).build();
        stateManager.start();
        assertNull(stateManager.dlq(param).get(10, TimeUnit.SECONDS));

        assertEquals(1, capturedProduces.size());
        assertDlqProduceRecordHeaders(capturedProduces.get(0), Map.of(
            0, new ExpectedDlqPartition(0L, 2L, Map.of(
                HEADER_DLQ_ERRORS_TOPIC, "source-topic",
                HEADER_DLQ_ERRORS_PARTITION, "0",
                HEADER_DLQ_ERRORS_GROUP, GROUP_ID,
                HEADER_DLQ_ERRORS_DELIVERY_COUNT, "1",
                HEADER_DLQ_ERRORS_MESSAGE, "simulated cause"
            ), List.of(keyData1, keyData2, keyData3), List.of(valueData1, valueData2, valueData3))
        ));
        verify(mockMetrics).recordDLQProduce(GROUP_ID);
        verify(mockMetrics).recordDLQRecordWrite(GROUP_ID, 3);
        verify(mockMetrics, never()).recordDLQProduceFailed(any());
    }

    @Test
    public void testDLQRecordCopyEnabledWithPartialLogReaderRecords() throws Exception {
        MockClient client = new MockClient(MOCK_TIME);
        List<ProduceRequest> capturedProduces = new ArrayList<>();
        client.prepareResponseFrom(
            body -> {
                if (body instanceof ProduceRequest pr) {
                    capturedProduces.add(pr);
                    return true;
                }
                return false;
            },
            successfulProduceResponse(0),
            DEFAULT_LEADER
        );

        ShareGroupDLQRecordParameter param = param();   // 3 offset requested
        byte[] keyData1 = "key1".getBytes(StandardCharsets.UTF_8);
        byte[] valueData1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] keyData2 = "key2".getBytes(StandardCharsets.UTF_8);
        byte[] valueData2 = "value2".getBytes(StandardCharsets.UTF_8);
        byte[] keyData3 = "key3".getBytes(StandardCharsets.UTF_8);
        byte[] valueData3 = "value3".getBytes(StandardCharsets.UTF_8);
        LogReader logReader = mock(LogReader.class);
        LogReadResult readResult1 = mock(LogReadResult.class);
        when(readResult1.error()).thenReturn(Errors.NONE);
        // Return 2 records only
        when(readResult1.info()).thenReturn(new FetchDataInfo(
            null,
            MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(MOCK_TIME.milliseconds(), keyData1, valueData1),
                new SimpleRecord(MOCK_TIME.milliseconds(), keyData2, valueData2)
            )
        ));
        LogReadResult readResult2 = mock(LogReadResult.class);
        when(readResult2.error()).thenReturn(Errors.NONE);
        // Next read contains the 3rd offset as well
        when(readResult2.info()).thenReturn(new FetchDataInfo(
            null,
            MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(MOCK_TIME.milliseconds(), keyData1, valueData1),
                new SimpleRecord(MOCK_TIME.milliseconds(), keyData2, valueData2),
                new SimpleRecord(MOCK_TIME.milliseconds(), keyData3, valueData3)
            )
        ));

        LinkedHashMap<TopicIdPartition, LogReadResult> readResultMap1 = new LinkedHashMap<>();
        LinkedHashMap<TopicIdPartition, LogReadResult> readResultMap2 = new LinkedHashMap<>();
        readResultMap1.put(param.topicIdPartition(), readResult1);
        readResultMap2.put(param.topicIdPartition(), readResult2);
        when(logReader.read(any(), anySet(), any(), any()))
            .thenReturn(readResultMap1)
            .thenReturn(readResultMap2);

        ShareGroupDLQMetadataCacheHelper cacheHelper = cacheHelper(DEFAULT_LEADER);
        when(cacheHelper.isShareGroupDlqCopyRecordEnabled(any())).thenReturn(true);
        stateManager = builder().withClient(client).withLogReader(logReader).withCacheHelper(cacheHelper).build();
        stateManager.start();
        assertNull(stateManager.dlq(param).get(10, TimeUnit.SECONDS));

        assertEquals(1, capturedProduces.size());
        assertDlqProduceRecordHeaders(capturedProduces.get(0), Map.of(
            0, new ExpectedDlqPartition(0L, 2L, Map.of(
                HEADER_DLQ_ERRORS_TOPIC, "source-topic",
                HEADER_DLQ_ERRORS_PARTITION, "0",
                HEADER_DLQ_ERRORS_GROUP, GROUP_ID,
                HEADER_DLQ_ERRORS_DELIVERY_COUNT, "1",
                HEADER_DLQ_ERRORS_MESSAGE, "simulated cause"
            ), Arrays.asList(keyData1, keyData2, keyData3), Arrays.asList(valueData1, valueData2, valueData3))
        ));
        verify(mockMetrics).recordDLQProduce(GROUP_ID);
        verify(mockMetrics).recordDLQRecordWrite(GROUP_ID, 3);
        verify(mockMetrics, never()).recordDLQProduceFailed(any());
    }

    @Test
    public void testDLQRecordCopyEnabledWithMultipleLogReaderIterations() throws Exception {
        MockClient client = new MockClient(MOCK_TIME);
        List<ProduceRequest> capturedProduces = new ArrayList<>();
        client.prepareResponseFrom(
            body -> {
                if (body instanceof ProduceRequest pr) {
                    capturedProduces.add(pr);
                    return true;
                }
                return false;
            },
            successfulProduceResponse(0),
            DEFAULT_LEADER
        );

        ShareGroupDLQRecordParameter param = param();   // 3 offsets requested
        byte[] keyData1 = "key1".getBytes(StandardCharsets.UTF_8);
        byte[] valueData1 = "value1".getBytes(StandardCharsets.UTF_8);
        byte[] keyData2 = "key2".getBytes(StandardCharsets.UTF_8);
        byte[] valueData2 = "value2".getBytes(StandardCharsets.UTF_8);
        LogReader logReader = mock(LogReader.class);
        LogReadResult readResult = mock(LogReadResult.class);
        when(readResult.error()).thenReturn(Errors.NONE);
        // Return 2 records only
        when(readResult.info()).thenReturn(new FetchDataInfo(
            null,
            MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(MOCK_TIME.milliseconds(), keyData1, valueData1),
                new SimpleRecord(MOCK_TIME.milliseconds(), keyData2, valueData2)
            )
        ));
        LinkedHashMap<TopicIdPartition, LogReadResult> readResultMap = new LinkedHashMap<>();
        readResultMap.put(param.topicIdPartition(), readResult);
        when(logReader.read(any(), anySet(), any(), any()))
            .thenReturn(readResultMap);

        ShareGroupDLQMetadataCacheHelper cacheHelper = cacheHelper(DEFAULT_LEADER);
        when(cacheHelper.isShareGroupDlqCopyRecordEnabled(any())).thenReturn(true);
        stateManager = builder().withClient(client).withLogReader(logReader).withCacheHelper(cacheHelper).build();
        stateManager.start();
        assertNull(stateManager.dlq(param).get(10, TimeUnit.SECONDS));

        assertEquals(1, capturedProduces.size());
        assertDlqProduceRecordHeaders(capturedProduces.get(0), Map.of(
            0, new ExpectedDlqPartition(0L, 2L, Map.of(
                HEADER_DLQ_ERRORS_TOPIC, "source-topic",
                HEADER_DLQ_ERRORS_PARTITION, "0",
                HEADER_DLQ_ERRORS_GROUP, GROUP_ID,
                HEADER_DLQ_ERRORS_DELIVERY_COUNT, "1",
                HEADER_DLQ_ERRORS_MESSAGE, "simulated cause"
            ), Arrays.asList(keyData1, keyData2, null), Arrays.asList(valueData1, valueData2, null))
        ));
        verify(mockMetrics).recordDLQProduce(GROUP_ID);
        verify(mockMetrics).recordDLQRecordWrite(GROUP_ID, 3);
        verify(mockMetrics, never()).recordDLQProduceFailed(any());
    }

    @Test
    public void testDLQRecordCopyEnabledWithErrorOnLogRead() throws Exception {
        MockClient client = new MockClient(MOCK_TIME);
        List<ProduceRequest> capturedProduces = new ArrayList<>();
        client.prepareResponseFrom(
            body -> {
                if (body instanceof ProduceRequest pr) {
                    capturedProduces.add(pr);
                    return true;
                }
                return false;
            },
            successfulProduceResponse(0),
            DEFAULT_LEADER
        );

        ShareGroupDLQRecordParameter param = param();
        LogReader logReader = mock(LogReader.class);
        LogReadResult readResult = mock(LogReadResult.class);
        when(readResult.error()).thenReturn(Errors.UNKNOWN_SERVER_ERROR);
        LinkedHashMap<TopicIdPartition, LogReadResult> readResultMap = new LinkedHashMap<>();
        readResultMap.put(param.topicIdPartition(), readResult);
        when(logReader.read(any(), anySet(), any(), any()))
            .thenReturn(readResultMap);

        ShareGroupDLQMetadataCacheHelper cacheHelper = cacheHelper(DEFAULT_LEADER);
        when(cacheHelper.isShareGroupDlqCopyRecordEnabled(any())).thenReturn(true);
        stateManager = builder().withClient(client).withLogReader(logReader).withCacheHelper(cacheHelper).build();
        stateManager.start();
        assertNull(stateManager.dlq(param).get(10, TimeUnit.SECONDS));

        assertEquals(1, capturedProduces.size());
        assertDlqProduceRecordHeaders(capturedProduces.get(0), Map.of(
            0, new ExpectedDlqPartition(0L, 2L, Map.of(
                HEADER_DLQ_ERRORS_TOPIC, "source-topic",
                HEADER_DLQ_ERRORS_PARTITION, "0",
                HEADER_DLQ_ERRORS_GROUP, GROUP_ID,
                HEADER_DLQ_ERRORS_DELIVERY_COUNT, "1",
                HEADER_DLQ_ERRORS_MESSAGE, "simulated cause"
            ), List.of(), List.of())
        ));
        verify(mockMetrics).recordDLQProduce(GROUP_ID);
        verify(mockMetrics).recordDLQRecordWrite(GROUP_ID, 3);
        verify(mockMetrics, never()).recordDLQProduceFailed(any());
    }

    private static ShareGroupDLQStateManager.ProduceRequestHandler newHandlerForCoalesceTest(
        ShareGroupDLQStateManager manager,
        String groupId,
        int sourcePartition
    ) {
        ShareGroupDLQRecordParameter param = new ShareGroupDLQRecordParameter(
            groupId,
            new TopicIdPartition(SOURCE_TOPIC_ID, sourcePartition, "source-topic"),
            0L, 0L,
            Optional.empty(), Optional.empty());
        return manager.new ProduceRequestHandler(
            param,
            new CompletableFuture<>(),
            ShareGroupDLQStateManager.REQUEST_BACKOFF_MS,
            ShareGroupDLQStateManager.REQUEST_BACKOFF_MAX_MS,
            3);
    }

    // ---- Response builder helpers ----

    private static ProduceResponse successfulProduceResponse(int partition) {
        return produceResponseFor(partition, Errors.NONE);
    }

    private static ProduceResponse produceResponseWithError(Errors error) {
        return produceResponseFor(0, error);
    }

    private static ProduceResponse produceResponseFor(int partition, Errors error) {
        // Don't set name: the manager looks up the TopicProduceResponse using only topicId, which
        // implies the lookup-key name is the default empty string.
        ProduceResponseData.TopicProduceResponse topicResp = new ProduceResponseData.TopicProduceResponse()
            .setTopicId(DLQ_TOPIC_ID)
            .setPartitionResponses(List.of(
                new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(partition)
                    .setErrorCode(error.code())
                    .setErrorMessage(error.message())
            ));
        ProduceResponseData.TopicProduceResponseCollection collection =
            new ProduceResponseData.TopicProduceResponseCollection();
        collection.add(topicResp);
        return new ProduceResponse(new ProduceResponseData().setResponses(collection));
    }

    private static CreateTopicsResponse successfulCreateTopicsResponse() {
        return createTopicsResponse(Errors.NONE);
    }

    private static CreateTopicsResponse createTopicsResponse(Errors error) {
        CreateTopicsResponseData data = new CreateTopicsResponseData();
        data.topics().add(new CreateTopicsResponseData.CreatableTopicResult()
            .setName(DLQ_TOPIC)
            .setTopicId(DLQ_TOPIC_ID)
            .setNumPartitions(1)
            .setReplicationFactor((short) 1)
            .setErrorCode(error.code())
            .setErrorMessage(error.message()));
        return new CreateTopicsResponse(data);
    }
}
