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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementCommitCallbackEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
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
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
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
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class ShareConsumeRequestManagerTest {
    private final String topicName = "test";
    private final String topicName2 = "test-2";
    private final String groupId = "test-group";
    private final Uuid topicId = Uuid.randomUuid();
    private final Uuid topicId2 = Uuid.randomUuid();
    private final Map<String, Uuid> topicIds = new HashMap<>() {
        {
            put(topicName, topicId);
            put(topicName2, topicId2);
        }
    };
    private final Map<String, Integer> topicPartitionCounts = new HashMap<>() {
        {
            put(topicName, 2);
            put(topicName2, 1);
        }
    };
    private final TopicPartition tp0 = new TopicPartition(topicName, 0);
    private final TopicIdPartition tip0 = new TopicIdPartition(topicId, tp0);
    private final TopicPartition tp1 = new TopicPartition(topicName, 1);
    private final TopicIdPartition tip1 = new TopicIdPartition(topicId, tp1);
    private final TopicPartition t2p0 = new TopicPartition(topicName2, 0);
    private final TopicIdPartition t2ip0 = new TopicIdPartition(topicId2, t2p0);
    private final int validLeaderEpoch = 0;
    private final MetadataResponse initialUpdateResponse =
            RequestTestUtils.metadataUpdateWithIds(1, singletonMap(topicName, 2), topicIds);

    private final long retryBackoffMs = 100;
    private final long requestTimeoutMs = 30000;
    private MockTime time = new MockTime(1);
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private ShareFetchMetricsManager metricsManager;
    private MockClient client;
    private Metrics metrics;
    private TestableShareConsumeRequestManager<?, ?> shareConsumeRequestManager;
    private TestableNetworkClientDelegate networkClientDelegate;
    private MemoryRecords records;
    private List<ShareFetchResponseData.AcquiredRecords> acquiredRecords;
    private List<ShareFetchResponseData.AcquiredRecords> emptyAcquiredRecords;
    private ShareFetchMetricsRegistry shareFetchMetricsRegistry;
    private List<Map<TopicIdPartition, Acknowledgements>> completedAcknowledgements;

    @BeforeEach
    public void setup() {
        records = buildRecords(1L, 3, 1);
        acquiredRecords = ShareCompletedFetchTest.acquiredRecords(1L, 3);
        emptyAcquiredRecords = new ArrayList<>();
        completedAcknowledgements = new LinkedList<>();
    }

    private void assignFromSubscribed(Set<TopicPartition> partitions) {
        subscriptions.subscribeToShareGroup(partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet()));
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(initialUpdateResponse);

        // A dummy metadata update to ensure valid leader epoch.
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("kafka-cluster", 1,
                Collections.emptyMap(), topicPartitionCounts,
                tp -> validLeaderEpoch, topicIds), false, 0L);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (metrics != null)
            metrics.close();
        if (shareConsumeRequestManager != null)
            shareConsumeRequestManager.close();
    }

    private int sendFetches() {
        return shareConsumeRequestManager.sendFetches();
    }

    @Test
    public void testFetchNormal() {
        buildRequestManager();

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp0);
        assertEquals(3, records.size());
    }

    @Test
    public void testFetchWithAcquiredRecords() {
        buildRequestManager();

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records,
                ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        // As only 1 record was acquired, we must fetch only 1 record.
        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());
    }

    @Test
    public void testMultipleFetches() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records,
                ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        // As only 1 record was acquired, we must fetch only 1 record.
        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Collections.singletonMap(tip0, acknowledgements));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records,
                ShareCompletedFetchTest.acquiredRecords(2L, 1), Errors.NONE, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
        assertEquals(1.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertEquals(Collections.singletonMap(tip0, acknowledgements), completedAcknowledgements.get(0));
        completedAcknowledgements.clear();

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(2L, AcknowledgeType.REJECT);
        shareConsumeRequestManager.fetch(Collections.singletonMap(tip0, acknowledgements2));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        // Preparing a response with an acknowledgement error.
        client.prepareResponse(fullFetchResponse(tip0, records,
                Collections.emptyList(), Errors.NONE, Errors.INVALID_RECORD_STATE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
        assertEquals(2.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());
        assertEquals(1.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementErrorTotal)).metricValue());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.isEmpty());
        assertEquals(Collections.singletonMap(tip0, acknowledgements2), completedAcknowledgements.get(0));
    }

    @Test
    public void testCommitSync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);
        acknowledgements.add(3L, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitSync(Collections.singletonMap(tip0, acknowledgements), time.milliseconds() + 2000);

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Collections.singletonMap(tip0, acknowledgements), completedAcknowledgements.get(0));
        completedAcknowledgements.clear();
    }

    @Test
    public void testCommitAsync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);
        acknowledgements.add(3L, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Collections.singletonMap(tip0, acknowledgements));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Collections.singletonMap(tip0, acknowledgements), completedAcknowledgements.get(0));
        completedAcknowledgements.clear();
    }

    @Test
    public void testServerDisconnectedOnShareAcknowledge() throws InterruptedException {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        fetchRecords();

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitAsync(Collections.singletonMap(tip0, acknowledgements));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(3L, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Collections.singletonMap(tip0, acknowledgements2));

        client.prepareResponse(null, true);
        networkClientDelegate.poll(time.timer(0));

        assertEquals(Collections.singletonMap(tip0, acknowledgements), completedAcknowledgements.get(0));
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, completedAcknowledgements.get(0).get(tip0).getAcknowledgeErrorCode());
        completedAcknowledgements.clear();

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));

        TestUtils.retryOnExceptionWithTimeout(() -> {
            assertEquals(0, shareConsumeRequestManager.sendAcknowledgements());
            // We expect the remaining acknowledgements to be cleared due to share session epoch being set to 0.
            assertNull(shareConsumeRequestManager.requestStates(0));
            // The callback for these unsent acknowledgements will be invoked with an error code.
            assertEquals(Collections.singletonMap(tip0, acknowledgements2), completedAcknowledgements.get(0));
            assertEquals(Errors.SHARE_SESSION_NOT_FOUND, completedAcknowledgements.get(0).get(tip0).getAcknowledgeErrorCode());
        });

        // Attempt a normal fetch to check if nodesWithPendingRequests is empty.
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
    }

    @Test
    public void testAcknowledgeOnClose() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);

        // Piggyback acknowledgements
        shareConsumeRequestManager.fetch(Collections.singletonMap(tip0, acknowledgements));

        // Remaining acknowledgements sent with close().
        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(2L, AcknowledgeType.ACCEPT);
        acknowledgements2.add(3L, AcknowledgeType.REJECT);

        shareConsumeRequestManager.acknowledgeOnClose(Collections.singletonMap(tip0, acknowledgements2),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertEquals(1, completedAcknowledgements.size());

        Acknowledgements mergedAcks = acknowledgements.merge(acknowledgements2);
        mergedAcks.setAcknowledgeErrorCode(Errors.NONE);
        // Verifying that all 3 offsets were acknowledged as part of the final ShareAcknowledge on close.
        assertEquals(mergedAcks.getAcknowledgementsTypeMap(), completedAcknowledgements.get(0).get(tip0).getAcknowledgementsTypeMap());
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
    }

    @Test
    public void testAcknowledgeOnCloseWithPendingCommitAsync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);
        acknowledgements.add(3L, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Collections.singletonMap(tip0, acknowledgements));
        shareConsumeRequestManager.acknowledgeOnClose(Collections.emptyMap(),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        client.prepareResponse(emptyAcknowledgeResponse());
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Collections.singletonMap(tip0, acknowledgements), completedAcknowledgements.get(0));
        completedAcknowledgements.clear();
    }

    @Test
    public void testAcknowledgeOnCloseWithPendingCommitSync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);
        acknowledgements.add(3L, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitSync(Collections.singletonMap(tip0, acknowledgements),
                calculateDeadlineMs(time.timer(100)));
        shareConsumeRequestManager.acknowledgeOnClose(Collections.emptyMap(),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        client.prepareResponse(emptyAcknowledgeResponse());
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Collections.singletonMap(tip0, acknowledgements), completedAcknowledgements.get(0));
        completedAcknowledgements.clear();
    }

    @Test
    public void testBatchingAcknowledgeRequestStates() {
        buildRequestManager();

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);
        acknowledgements.add(3L, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Collections.singletonMap(tip0, acknowledgements));

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements.add(4L, AcknowledgeType.ACCEPT);
        acknowledgements.add(5L, AcknowledgeType.ACCEPT);
        acknowledgements.add(6L, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitAsync(Collections.singletonMap(tip0, acknowledgements2));

        assertEquals(6, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));
    }

    @Test
    public void testPendingCommitAsyncBeforeCommitSync() {
        buildRequestManager();

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);
        acknowledgements.add(3L, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Collections.singletonMap(tip0, acknowledgements));

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(4L, AcknowledgeType.ACCEPT);
        acknowledgements2.add(5L, AcknowledgeType.ACCEPT);
        acknowledgements2.add(6L, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitSync(Collections.singletonMap(tip0, acknowledgements2), 60000L);

        assertEquals(3, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));
        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(3, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        assertEquals(3, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));
        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(3, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(3, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));
    }

    @Test
    public void testRetryAcknowledgements() throws InterruptedException {
        buildRequestManager();

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);
        acknowledgements.add(3L, AcknowledgeType.REJECT);
        acknowledgements.add(4L, AcknowledgeType.ACCEPT);
        acknowledgements.add(5L, AcknowledgeType.RELEASE);
        acknowledgements.add(6L, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitSync(Collections.singletonMap(tip0, acknowledgements), 60000L);
        assertNull(shareConsumeRequestManager.requestStates(0).getAsyncRequest());

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.REQUEST_TIMED_OUT));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getIncompleteAcknowledgementsCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        TestUtils.retryOnExceptionWithTimeout(() -> assertEquals(1, shareConsumeRequestManager.sendAcknowledgements()));

        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getIncompleteAcknowledgementsCount(tip0));
    }

    @Test
    public void testPiggybackAcknowledgementsInFlight() {
        buildRequestManager();

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);

        // Reading records from the share fetch buffer.
        fetchRecords();

        // Piggyback acknowledgements
        shareConsumeRequestManager.fetch(Collections.singletonMap(tip0, acknowledgements));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(2.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(3L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Collections.singletonMap(tip0, acknowledgements2));

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        fetchRecords();

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
        assertEquals(3.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());
    }

    @Test
    public void testCommitAsyncWithSubscriptionChange() {
        buildRequestManager();

        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);
        acknowledgements.add(3L, AcknowledgeType.REJECT);

        assignFromSubscribed(singleton(tp1));

        shareConsumeRequestManager.commitAsync(Collections.singletonMap(tip0, acknowledgements));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
    }

    @Test
    public void testShareFetchWithSubscriptionChange() {
        buildRequestManager();

        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(0L, AcknowledgeType.ACCEPT);
        acknowledgements.add(1L, AcknowledgeType.RELEASE);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);

        // Send acknowledgements via ShareFetch
        shareConsumeRequestManager.fetch(Collections.singletonMap(tip0, acknowledgements));
        fetchRecords();
        // Subscription changes.
        assignFromSubscribed(singleton(tp1));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
        assertEquals(3.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());
    }

    @Test
    public void testRetryAcknowledgementsWithLeaderChange() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, singletonMap(topicName, 1),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        LinkedList<Node> nodeList = new LinkedList<>(Arrays.asList(nodeId0, nodeId1));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, buildRecords(1L, 6, 1),
            ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);
        acknowledgements.add(3L, AcknowledgeType.REJECT);
        acknowledgements.add(4L, AcknowledgeType.ACCEPT);
        acknowledgements.add(5L, AcknowledgeType.RELEASE);
        acknowledgements.add(6L, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitSync(Collections.singletonMap(tip0, acknowledgements), 60000L);
        assertNull(shareConsumeRequestManager.requestStates(0).getAsyncRequest());

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        // Fail the acknowledgement and provide the new current leader information - this should stop the retry
        client.prepareResponse(fullAcknowledgeResponse(tip0,
            Errors.NOT_LEADER_OR_FOLLOWER,
            new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(nodeId1.id()).setLeaderEpoch(validLeaderEpoch + 1),
            nodeList));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getIncompleteAcknowledgementsCount(tip0));
    }

    @Test
    public void testCallbackHandlerConfig() throws InterruptedException {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Collections.singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitAsync(Collections.singletonMap(tip0, acknowledgements));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Collections.singletonMap(tip0, acknowledgements), completedAcknowledgements.get(0));

        completedAcknowledgements.clear();

        // Setting the boolean to false, indicating there is no callback handler registered.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(false);

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(3L, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitAsync(Collections.singletonMap(tip0, acknowledgements2));

        TestUtils.retryOnExceptionWithTimeout(() -> assertEquals(1, shareConsumeRequestManager.sendAcknowledgements()));

        client.prepareResponse(fullAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // We expect no acknowledgements to be added as the callback handler is not configured.
        assertEquals(0, completedAcknowledgements.size());
    }

    @Test
    public void testAcknowledgementCommitCallbackMultiplePartitionCommitAsync() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(t2p0);

        assignFromSubscribed(partitions);

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
        partitionDataMap.put(tip0, partitionDataForFetch(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE));
        partitionDataMap.put(t2ip0, partitionDataForFetch(t2ip0, records, acquiredRecords, Errors.NONE, Errors.NONE));
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, partitionDataMap, Collections.emptyList()));

        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(0L, AcknowledgeType.ACCEPT);
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements.add(2L, AcknowledgeType.ACCEPT);

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(0L, AcknowledgeType.ACCEPT);
        acknowledgements2.add(1L, AcknowledgeType.ACCEPT);
        acknowledgements2.add(2L, AcknowledgeType.ACCEPT);

        Map<TopicIdPartition, Acknowledgements> acks = new HashMap<>();
        acks.put(tip0, acknowledgements);
        acks.put(t2ip0, acknowledgements2);

        shareConsumeRequestManager.commitAsync(acks);

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        Map<TopicIdPartition, Errors> errorsMap = new HashMap<>();
        errorsMap.put(tip0, Errors.NONE);
        errorsMap.put(t2ip0, Errors.NONE);
        client.prepareResponse(fullAcknowledgeResponse(errorsMap));

        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // Verifying that the acknowledgement commit callback is invoked for both the partitions.
        assertEquals(2, completedAcknowledgements.size());
        assertEquals(1, completedAcknowledgements.get(0).size());
        assertEquals(1, completedAcknowledgements.get(1).size());
    }

    @Test
    public void testMultipleTopicsFetch() {
        buildRequestManager();
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(t2p0);

        assignFromSubscribed(partitions);

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
        partitionDataMap.put(tip0, partitionDataForFetch(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE));
        partitionDataMap.put(t2ip0, partitionDataForFetch(t2ip0, records, emptyAcquiredRecords, Errors.TOPIC_AUTHORIZATION_FAILED, Errors.NONE));
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, partitionDataMap, Collections.emptyList()));

        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        ShareFetch<Object, Object> shareFetch = collectFetch();
        assertEquals(1, shareFetch.records().size());
        // The first topic-partition is fetched successfully and returns all the records.
        assertEquals(3, shareFetch.records().get(tp0).size());
        // As the second topic failed authorization, we do not get the records in the ShareFetch.
        assertThrows(NullPointerException.class, (Executable) shareFetch.records().get(t2p0));
        assertThrows(TopicAuthorizationException.class, this::collectFetch);
    }

    @Test
    public void testMultipleTopicsFetchError() {
        buildRequestManager();
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(t2p0);

        assignFromSubscribed(partitions);

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap = new LinkedHashMap<>();
        partitionDataMap.put(t2ip0, partitionDataForFetch(t2ip0, records, emptyAcquiredRecords, Errors.TOPIC_AUTHORIZATION_FAILED, Errors.NONE));
        partitionDataMap.put(tip0, partitionDataForFetch(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE));
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, partitionDataMap, Collections.emptyList()));

        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // The first call throws TopicAuthorizationException because there are no records ready to return when the
        // exception is noticed.
        assertThrows(TopicAuthorizationException.class, this::collectFetch);
        // And then a second iteration returns the records.
        ShareFetch<Object, Object> shareFetch = collectFetch();
        assertEquals(1, shareFetch.records().size());
        // The first topic-partition is fetched successfully and returns all the records.
        assertEquals(3, shareFetch.records().get(tp0).size());
        // As the second topic failed authorization, we do not get the records in the ShareFetch.
        assertThrows(NullPointerException.class, (Executable) shareFetch.records().get(t2p0));
    }

    @Test
    public void testCloseShouldBeIdempotent() {
        buildRequestManager();

        shareConsumeRequestManager.close();
        shareConsumeRequestManager.close();
        shareConsumeRequestManager.close();

        verify(shareConsumeRequestManager, times(1)).closeInternal();
    }

    @Test
    public void testFetchError() {
        buildRequestManager();

        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullFetchResponse(tip0, records, emptyAcquiredRecords, Errors.NOT_LEADER_OR_FOLLOWER));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertFalse(partitionRecords.containsKey(tp0));
    }

    @Test
    public void testInvalidDefaultRecordBatch() {
        buildRequestManager();

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

        assignFromSubscribed(singleton(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0,
                MemoryRecords.readableRecords(buffer),
                ShareCompletedFetchTest.acquiredRecords(0L, 1),
                Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        // The first call to collectFetch, throws an exception
        assertThrows(KafkaException.class, this::collectFetch);

        // The exception is cleared once thrown
        ShareFetch<String, String> fetch = this.collectFetch();
        assertTrue(fetch.isEmpty());
    }

    @Test
    public void testParseInvalidRecordBatch() {
        buildRequestManager();
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                Compression.NONE, TimestampType.CREATE_TIME,
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
        buildRequestManager();

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
        assignFromSubscribed(singleton(tp0));

        client.prepareResponse(fullFetchResponse(tip0,
                memoryRecords,
                ShareCompletedFetchTest.acquiredRecords(1L, 3),
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
        buildRequestManager();

        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0, records, emptyAcquiredRecords, Errors.TOPIC_AUTHORIZATION_FAILED));
        networkClientDelegate.poll(time.timer(0));
        try {
            collectFetch();
            fail("collectFetch should have thrown a TopicAuthorizationException");
        } catch (TopicAuthorizationException e) {
            assertEquals(singleton(topicName), e.unauthorizedTopics());
        }
    }

    @Test
    public void testUnknownTopicIdError() {
        buildRequestManager();
        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        client.prepareResponse(fetchResponseWithTopLevelError(tip0, Errors.UNKNOWN_TOPIC_ID));
        networkClientDelegate.poll(time.timer(0));
        assertEmptyFetch("Should not return records on fetch error");
        assertEquals(0L, metadata.timeToNextUpdate(time.milliseconds()));
    }

    @ParameterizedTest
    @MethodSource("handleFetchResponseErrorSupplier")
    public void testHandleFetchResponseError(Errors error,
                                             boolean hasTopLevelError,
                                             boolean shouldRequestMetadataUpdate) {
        buildRequestManager();
        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());

        final ShareFetchResponse fetchResponse;

        if (hasTopLevelError)
            fetchResponse = fetchResponseWithTopLevelError(tip0, error);
        else
            fetchResponse = fullFetchResponse(tip0, records, emptyAcquiredRecords, error);

        client.prepareResponse(fetchResponse);
        networkClientDelegate.poll(time.timer(0));

        assertEmptyFetch("Should not return records on fetch error");

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
                Arguments.of(Errors.UNKNOWN_TOPIC_ID, true, true),
                Arguments.of(Errors.INCONSISTENT_TOPIC_ID, false, true),
                Arguments.of(Errors.FENCED_LEADER_EPOCH, false, true),
                Arguments.of(Errors.UNKNOWN_LEADER_EPOCH, false, false)
        );
    }

    @Test
    public void testFetchDisconnected() {
        buildRequestManager();

        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0, records, acquiredRecords, Errors.NONE), true);
        networkClientDelegate.poll(time.timer(0));
        assertEmptyFetch("Should not return records on disconnect");
    }

    @Test
    public void testFetchWithLastRecordMissingFromBatch() {
        buildRequestManager();

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

        assignFromSubscribed(singleton(tp0));
        assertEquals(1, sendFetches());
        client.prepareResponse(fullFetchResponse(tip0,
                compactedRecords,
                ShareCompletedFetchTest.acquiredRecords(0L, 3),
                Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> allFetchedRecords = fetchRecords();
        assertTrue(allFetchedRecords.containsKey(tp0));
        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = allFetchedRecords.get(tp0);
        assertEquals(3, fetchedRecords.size());

        for (int i = 0; i < 3; i++) {
            assertEquals(Integer.toString(i), new String(fetchedRecords.get(i).key()));
        }
    }

    private MemoryRecords buildRecords(long baseOffset, int count, long firstMessageId) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, baseOffset);
        for (int i = 0; i < count; i++)
            builder.append(0L, "key".getBytes(), ("value-" + (firstMessageId + i)).getBytes());
        return builder.build();
    }

    @Test
    public void testCorruptMessageError() {
        buildRequestManager();
        assignFromSubscribed(singleton(tp0));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        // Prepare a response with the CORRUPT_MESSAGE error.
        client.prepareResponse(fullFetchResponse(
                tip0,
                buildRecords(1L, 1, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 1),
                Errors.CORRUPT_MESSAGE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // Trigger the exception.
        assertThrows(KafkaException.class, this::fetchRecords);
    }

    /**
     * Test the scenario that ShareFetchResponse returns with an error indicating leadership change for the partition,
     * but it does not contain new leader info (defined in KIP-951).
     */
    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER"})
    public void testWhenShareFetchResponseReturnsALeadershipChangeErrorButNoNewLeaderInformation(Errors error) {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, singletonMap(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        Node tp0Leader = metadata.fetch().leaderFor(tp0);
        Node tp1Leader = metadata.fetch().leaderFor(tp1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList()), nodeId0);
        partitionData.clear();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setErrorCode(error.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList()), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertFalse(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Collections.singletonMap(tip0, acknowledgements));

        assertEquals(startingClusterMetadata, metadata.fetch());

        // Validate metadata update is requested due to the leadership error
        assertTrue(metadata.updateRequested());

        // Move the leadership of tp1 onto node 1
        HashMap<TopicPartition, Metadata.LeaderIdAndEpoch> partitionLeaders = new HashMap<>();
        partitionLeaders.put(tp1, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId0.id()), Optional.of(validLeaderEpoch + 1)));
        LinkedList<Node> leaderNodes = new LinkedList<>(Arrays.asList(tp0Leader, tp1Leader));
        metadata.updatePartitionLeadership(partitionLeaders, leaderNodes);

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // And now the partitions are on the same leader so only one fetch is sent
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        partitionData.clear();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(2L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList()), nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());
        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(1, fetchedRecords.size());
    }

    /**
     * Test the scenario that ShareFetchResponse returns with an error indicating leadership change for the partition,
     * along with new leader info (defined in KIP-951).
     */
    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER"})
    public void testWhenFetchResponseReturnsWithALeadershipChangeErrorAndNewLeaderInformation(Errors error) {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Collections.singleton(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, singletonMap(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        Node tp0Leader = metadata.fetch().leaderFor(tp0);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList()), nodeId0);
        partitionData.clear();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setErrorCode(error.code())
                .setCurrentLeader(new ShareFetchResponseData.LeaderIdAndEpoch()
                    .setLeaderId(tp0Leader.id())
                    .setLeaderEpoch(validLeaderEpoch + 1)));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, singletonList(tp0Leader)), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertFalse(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Collections.singletonMap(tip0, acknowledgements));

        // The metadata snapshot will have been updated with the new leader information
        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // Validate metadata update is still requested even though the current leader was returned
        assertTrue(metadata.updateRequested());

        // And now the partitions are on the same leader so only one fetch is sent
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        partitionData.clear();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(2L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, Collections.emptyList()), nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());
        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(1, fetchedRecords.size());
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
        return fullFetchResponse(tp, records, acquiredRecords, error, Errors.NONE);
    }

    private ShareFetchResponse fullFetchResponse(TopicIdPartition tp,
                                                 MemoryRecords records,
                                                 List<ShareFetchResponseData.AcquiredRecords> acquiredRecords,
                                                 Errors error,
                                                 Errors acknowledgeError) {
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitions = Collections.singletonMap(tp,
                partitionDataForFetch(tp, records, acquiredRecords, error, acknowledgeError));
        return ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), Collections.emptyList());
    }

    private ShareAcknowledgeResponse emptyAcknowledgeResponse() {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = Collections.emptyMap();
        return ShareAcknowledgeResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), Collections.emptyList());
    }

    private ShareAcknowledgeResponse fullAcknowledgeResponse(TopicIdPartition tp, Errors error) {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = Collections.singletonMap(tp,
                partitionDataForAcknowledge(tp, error));
        return ShareAcknowledgeResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), Collections.emptyList());
    }

    private ShareAcknowledgeResponse fullAcknowledgeResponse(Map<TopicIdPartition, Errors> partitionErrorsMap) {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = new HashMap<>();
        partitionErrorsMap.forEach((tip, error) -> partitions.put(tip, partitionDataForAcknowledge(tip, error)));
        return ShareAcknowledgeResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), Collections.emptyList());
    }

    private ShareAcknowledgeResponse fullAcknowledgeResponse(TopicIdPartition tp,
                                                             Errors error,
                                                             ShareAcknowledgeResponseData.LeaderIdAndEpoch currentLeader,
                                                             List<Node> nodeEndpoints) {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = Collections.singletonMap(tp,
            partitionDataForAcknowledge(tp, error, currentLeader));
        return ShareAcknowledgeResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), nodeEndpoints);
    }

    private ShareFetchResponseData.PartitionData partitionDataForFetch(TopicIdPartition tp,
                                                                       MemoryRecords records,
                                                                       List<ShareFetchResponseData.AcquiredRecords> acquiredRecords,
                                                                       Errors error,
                                                                       Errors acknowledgeError) {
        return new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tp.topicPartition().partition())
                .setErrorCode(error.code())
                .setAcknowledgeErrorCode(acknowledgeError.code())
                .setRecords(records)
                .setAcquiredRecords(acquiredRecords);
    }

    private ShareAcknowledgeResponseData.PartitionData partitionDataForAcknowledge(TopicIdPartition tp, Errors error) {
        return new ShareAcknowledgeResponseData.PartitionData()
                .setPartitionIndex(tp.topicPartition().partition())
                .setErrorCode(error.code());
    }

    private ShareAcknowledgeResponseData.PartitionData partitionDataForAcknowledge(TopicIdPartition tp,
                                                                                   Errors error,
                                                                                   ShareAcknowledgeResponseData.LeaderIdAndEpoch currentLeader) {
        return new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(tp.topicPartition().partition())
            .setErrorCode(error.code())
            .setCurrentLeader(currentLeader);
    }

    /**
     * Assert that the {@link ShareFetchCollector#collect(ShareFetchBuffer) latest fetch} does not contain any
     * {@link ShareFetch#records() user-visible records}, and is {@link ShareFetch#isEmpty() empty}.
     *
     * @param reason the reason to include for assertion methods such as {@link org.junit.jupiter.api.Assertions#assertTrue(boolean, String)}
     */
    private void assertEmptyFetch(String reason) {
        ShareFetch<?, ?> fetch = collectFetch();
        assertEquals(Collections.emptyMap(), fetch.records(), reason);
        assertTrue(fetch.isEmpty(), reason);
    }

    private <K, V> Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchRecords() {
        ShareFetch<K, V> fetch = collectFetch();
        if (fetch.isEmpty()) {
            return Collections.emptyMap();
        }
        return fetch.records();
    }

    @SuppressWarnings("unchecked")
    private <K, V> ShareFetch<K, V> collectFetch() {
        return (ShareFetch<K, V>) shareConsumeRequestManager.collectFetch();
    }

    private void buildRequestManager() {
        buildRequestManager(new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private <K, V> void buildRequestManager(Deserializer<K> keyDeserializer,
                                            Deserializer<V> valueDeserializer) {
        buildRequestManager(new MetricConfig(), keyDeserializer, valueDeserializer);
    }

    private <K, V> void buildRequestManager(MetricConfig metricConfig,
                                            Deserializer<K> keyDeserializer,
                                            Deserializer<V> valueDeserializer) {
        LogContext logContext = new LogContext();
        SubscriptionState subscriptionState = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        buildRequestManager(metricConfig, keyDeserializer, valueDeserializer,
                subscriptionState, logContext);
    }

    private <K, V> void buildRequestManager(MetricConfig metricConfig,
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
        BackgroundEventHandler backgroundEventHandler = new TestableBackgroundEventHandler(time, completedAcknowledgements);
        shareConsumeRequestManager = spy(new TestableShareConsumeRequestManager<>(
                logContext,
                groupId,
                metadata,
                subscriptionState,
                fetchConfig,
                new ShareFetchBuffer(logContext),
                backgroundEventHandler,
                metricsManager,
                shareFetchCollector));
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
        shareFetchMetricsRegistry = new ShareFetchMetricsRegistry(metricConfig.tags().keySet(), "consumer-share" + groupId);
        metricsManager = new ShareFetchMetricsManager(metrics, shareFetchMetricsRegistry);

        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs));
        properties.setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs));
        ConsumerConfig config = new ConsumerConfig(properties);
        networkClientDelegate = spy(new TestableNetworkClientDelegate(
            time, config, logContext, client, metadata,
            new BackgroundEventHandler(new LinkedBlockingQueue<>(), time, mock(AsyncConsumerMetrics.class)), false));
    }

    private class TestableShareConsumeRequestManager<K, V> extends ShareConsumeRequestManager {

        private final ShareFetchCollector<K, V> shareFetchCollector;

        public TestableShareConsumeRequestManager(LogContext logContext,
                                                  String groupId,
                                                  ConsumerMetadata metadata,
                                                  SubscriptionState subscriptions,
                                                  FetchConfig fetchConfig,
                                                  ShareFetchBuffer shareFetchBuffer,
                                                  BackgroundEventHandler backgroundEventHandler,
                                                  ShareFetchMetricsManager metricsManager,
                                                  ShareFetchCollector<K, V> fetchCollector) {
            super(time, logContext, groupId, metadata, subscriptions, fetchConfig, shareFetchBuffer,
                    backgroundEventHandler, metricsManager, retryBackoffMs, 1000);
            this.shareFetchCollector = fetchCollector;
            onMemberEpochUpdated(Optional.empty(), Uuid.randomUuid().toString());
        }

        private ShareFetch<K, V> collectFetch() {
            return shareFetchCollector.collect(shareFetchBuffer);
        }

        private int sendFetches() {
            fetch(new HashMap<>());
            NetworkClientDelegate.PollResult pollResult = poll(time.milliseconds());
            networkClientDelegate.addAll(pollResult.unsentRequests);
            return pollResult.unsentRequests.size();
        }

        private int sendAcknowledgements() {
            NetworkClientDelegate.PollResult pollResult = poll(time.milliseconds());
            networkClientDelegate.addAll(pollResult.unsentRequests);
            return pollResult.unsentRequests.size();
        }

        public Tuple<AcknowledgeRequestState> requestStates(int nodeId) {
            return super.requestStates(nodeId);
        }
    }

    private class TestableNetworkClientDelegate extends NetworkClientDelegate {
        private final ConcurrentLinkedQueue<Node> pendingDisconnects = new ConcurrentLinkedQueue<>();

        public TestableNetworkClientDelegate(Time time,
                                             ConsumerConfig config,
                                             LogContext logContext,
                                             KafkaClient client,
                                             Metadata metadata,
                                             BackgroundEventHandler backgroundEventHandler,
                                             boolean notifyMetadataErrorsViaErrorQueue) {
            super(time, config, logContext, client, metadata, backgroundEventHandler, notifyMetadataErrorsViaErrorQueue, mock(AsyncConsumerMetrics.class));
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

            Iterator<UnsentRequest> it = unsentRequests().iterator();

            while (it.hasNext()) {
                UnsentRequest u = it.next();

                if (node.equals(u.node().orElse(null))) {
                    it.remove();
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

    private static class TestableBackgroundEventHandler extends BackgroundEventHandler {
        List<Map<TopicIdPartition, Acknowledgements>> completedAcknowledgements;

        public TestableBackgroundEventHandler(Time time, List<Map<TopicIdPartition, Acknowledgements>> completedAcknowledgements) {
            super(new LinkedBlockingQueue<>(), time, mock(AsyncConsumerMetrics.class));
            this.completedAcknowledgements = completedAcknowledgements;
        }

        public void add(BackgroundEvent event) {
            if (event.type() == BackgroundEvent.Type.SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK) {
                ShareAcknowledgementCommitCallbackEvent shareAcknowledgementCommitCallbackEvent = (ShareAcknowledgementCommitCallbackEvent) event;
                completedAcknowledgements.add(shareAcknowledgementCommitCallbackEvent.acknowledgementsMap());
            }
        }
    }
}
