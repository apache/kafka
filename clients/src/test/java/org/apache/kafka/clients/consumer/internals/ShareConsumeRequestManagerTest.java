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
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEvent;
import org.apache.kafka.clients.consumer.internals.events.ShareAcknowledgementEventHandler;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.ByteBufferOutputStream;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.common.utils.internals.SingleByteBufferOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity", "JavaNCSS"})
public class ShareConsumeRequestManagerTest {
    private final String topicName = "test";
    private final String topicName2 = "test-2";
    private final String groupId = "test-group";
    private final Uuid topicId = Uuid.randomUuid();
    private final Uuid topicId2 = Uuid.randomUuid();
    private final Map<String, Uuid> topicIds = Map.of(
        topicName, topicId,
        topicName2, topicId2
    );
    private final Map<String, Integer> topicPartitionCounts = Map.of(
        topicName, 2,
        topicName2, 1
    );
    private final TopicPartition tp0 = new TopicPartition(topicName, 0);
    private final TopicIdPartition tip0 = new TopicIdPartition(topicId, tp0);
    private final TopicPartition tp1 = new TopicPartition(topicName, 1);
    private final TopicIdPartition tip1 = new TopicIdPartition(topicId, tp1);
    private final TopicPartition t2p0 = new TopicPartition(topicName2, 0);
    private final TopicIdPartition t2ip0 = new TopicIdPartition(topicId2, t2p0);
    private final int validLeaderEpoch = 0;
    private final MetadataResponse initialUpdateResponse =
            RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName, 2), topicIds);

    private final long retryBackoffMs = 100;
    private final long requestTimeoutMs = 30000;
    private final long defaultApiTimeoutMs = 60000;
    private MockTime time = new MockTime(1);
    private SubscriptionState subscriptions;
    private ShareConsumerMetadata metadata;
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
    private HashSet<Long> renewedRecords;

    @BeforeEach
    public void setup() {
        records = buildRecords(1L, 3, 1);
        acquiredRecords = ShareCompletedFetchTest.acquiredRecords(1L, 3);
        emptyAcquiredRecords = new ArrayList<>();
        completedAcknowledgements = new LinkedList<>();
        renewedRecords = new HashSet<>();
    }

    private void assignFromSubscribed(Set<TopicPartition> partitions) {
        subscriptions.subscribeToShareGroup(partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet()));
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(initialUpdateResponse);

        // A dummy metadata update to ensure valid leader epoch.
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("kafka-cluster", 1,
                Map.of(), topicPartitionCounts,
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
    public void testShareFetchNormal() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        List<ConsumerRecord<byte[], byte[]>> records = partitionRecords.get(tp0);
        assertEquals(3, records.size());
    }

    @Test
    public void testShareFetchWithAcquiredRecords() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE);

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
        assignFromSubscribed(Set.of(tp0));

        sendFetchAndVerifyResponse(records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        // As only 1 record was acquired, we must fetch only 1 record.
        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        sendFetchAndVerifyResponse(records, ShareCompletedFetchTest.acquiredRecords(2L, 1), Errors.NONE);
        assertEquals(1.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
        completedAcknowledgements.clear();

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(2L, AcknowledgeType.REJECT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)));

        // Preparing a response with an acknowledgement error.
        sendFetchAndVerifyResponse(records, List.of(), Errors.NONE, Errors.INVALID_RECORD_STATE);

        assertEquals(2.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());
        assertEquals(1.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementErrorTotal)).metricValue());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.isEmpty());
        assertEquals(Map.of(tip0, acknowledgements2), completedAcknowledgements.get(0));
    }

    @Test
    public void testCommitSync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(2000)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
    }

    @Test
    public void testCommitAsync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
    }

    @Test
    public void testServerDisconnectedOnShareAcknowledge() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        fetchRecords();

        Acknowledgements acknowledgements = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(3L, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        client.prepareResponse(null, true);
        networkClientDelegate.poll(time.timer(0));

        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
        assertInstanceOf(UnknownServerException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
        completedAcknowledgements.clear();

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));

        // Wait for backoff time before sending the next request.
        time.sleep(retryBackoffMs);

        assertEquals(0, shareConsumeRequestManager.sendAcknowledgements());
        // We expect the remaining acknowledgements to be cleared due to share session epoch being set to 0.
        assertNull(shareConsumeRequestManager.requestStates(0));
        // The callback for these unsent acknowledgements will be invoked with an error code.
        assertEquals(Map.of(tip0, acknowledgements2), completedAcknowledgements.get(0));
        assertInstanceOf(NetworkException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        // Attempt a normal fetch to check if nodesWithPendingRequests is empty.
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullShareFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
    }

    @Test
    public void testAcknowledgeOnClose() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);

        // Piggyback acknowledgements
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        // Remaining acknowledgements sent with close().
        Acknowledgements acknowledgements2 = getAcknowledgements(2, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        CompletableFuture<Void> closeFuture = shareConsumeRequestManager.acknowledgeOnClose(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertEquals(1, completedAcknowledgements.size());

        Acknowledgements mergedAcks = acknowledgements.merge(acknowledgements2);
        mergedAcks.complete(null);
        // Verifying that all 3 offsets were acknowledged as part of the final ShareAcknowledge on close.
        assertEquals(mergedAcks.getAcknowledgementsTypeMap(), completedAcknowledgements.get(0).get(tip0).getAcknowledgementsTypeMap());
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // Polling once more to complete the closeFuture.
        shareConsumeRequestManager.sendFetches();
        assertTrue(closeFuture.isDone());
    }

    @Test
    public void testCloseFutureCompletedWhenMemberIdIsNull() {
        buildRequestManager(new MetricConfig(), new ByteArrayDeserializer(), new ByteArrayDeserializer(), null, ShareAcquireMode.BATCH_OPTIMIZED);
        assignFromSubscribed(Set.of(tp0));

        CompletableFuture<Void> closeFuture = shareConsumeRequestManager.acknowledgeOnClose(Map.of(),
                calculateDeadlineMs(time.timer(100)));

        assertFalse(closeFuture.isDone());

        // The subsequent poll should complete the closeFuture as the memberId is null.
        shareConsumeRequestManager.sendFetches();
        assertTrue(closeFuture.isDone());
    }

    @Test
    public void testAcknowledgeOnCloseWithPendingCommitAsync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));
        shareConsumeRequestManager.acknowledgeOnClose(Map.of(),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        client.prepareResponse(emptyShareAcknowledgeResponse());
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
    }

    @Test
    public void testAcknowledgeOnCloseWithPendingCommitSync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(100)));
        shareConsumeRequestManager.acknowledgeOnClose(Map.of(),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        client.prepareResponse(emptyShareAcknowledgeResponse());
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
    }

    @Test
    public void testResultHandlerOnCommitAsync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        ShareConsumeRequestManager.ResultHandler resultHandler = shareConsumeRequestManager.buildResultHandler(null, Optional.empty());

        // Passing null acknowledgements should mean we do not send the background event at all.
        resultHandler.complete(tip0, null, ShareConsumeRequestManager.AcknowledgeRequestType.COMMIT_ASYNC, false, Optional.empty());
        assertEquals(0, completedAcknowledgements.size());

        // Setting the request type to COMMIT_SYNC should still not send any background event
        // as we have initialized remainingResults to null.
        resultHandler.complete(tip0, acknowledgements, ShareConsumeRequestManager.AcknowledgeRequestType.COMMIT_SYNC, false, Optional.empty());
        assertEquals(0, completedAcknowledgements.size());

        // Sending non-null acknowledgements means we do send the background event
        resultHandler.complete(tip0, acknowledgements, ShareConsumeRequestManager.AcknowledgeRequestType.COMMIT_ASYNC, false, Optional.empty());
        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
    }

    @Test
    public void testResultHandlerOnCommitSync() {
        buildRequestManager();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        final CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future = new CompletableFuture<>();

        // Initializing resultCount to 3.
        AtomicInteger resultCount = new AtomicInteger(3);

        ShareConsumeRequestManager.ResultHandler resultHandler = shareConsumeRequestManager.buildResultHandler(resultCount, Optional.of(future));

        // We only send the background event after all results have been completed.
        resultHandler.complete(tip0, acknowledgements, ShareConsumeRequestManager.AcknowledgeRequestType.COMMIT_SYNC, false, Optional.empty());
        assertEquals(0, completedAcknowledgements.size());
        assertFalse(future.isDone());

        resultHandler.complete(t2ip0, null, ShareConsumeRequestManager.AcknowledgeRequestType.COMMIT_SYNC, false, Optional.empty());
        assertEquals(0, completedAcknowledgements.size());
        assertFalse(future.isDone());

        // After third response is received, we send the background event.
        resultHandler.complete(tip1, acknowledgements, ShareConsumeRequestManager.AcknowledgeRequestType.COMMIT_SYNC, false, Optional.empty());
        assertEquals(1, completedAcknowledgements.size());
        assertEquals(2, completedAcknowledgements.get(0).size());
        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(3, completedAcknowledgements.get(0).get(tip1).size());
        assertTrue(future.isDone());
    }

    @Test
    public void testResultHandlerCompleteIfEmpty() {
        buildRequestManager();

        final CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future = new CompletableFuture<>();

        // Initializing resultCount to 1.
        AtomicInteger resultCount = new AtomicInteger(1);

        ShareConsumeRequestManager.ResultHandler resultHandler = shareConsumeRequestManager.buildResultHandler(resultCount, Optional.of(future));

        resultHandler.completeIfEmpty();
        assertFalse(future.isDone());

        resultCount.decrementAndGet();

        resultHandler.completeIfEmpty();
        assertTrue(future.isDone());
    }

    @Test
    public void testBatchingAcknowledgeRequestStates() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        Acknowledgements acknowledgements2 = getAcknowledgements(4, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(6, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));
    }

    @Test
    public void testPendingCommitAsyncBeforeCommitSync() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        Acknowledgements acknowledgements2 = getAcknowledgements(4, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)),
                calculateDeadlineMs(time.timer(60000L)));

        assertEquals(3, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));
        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(3, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        assertEquals(3, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));
        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(3, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(3, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));
    }

    @Test
    public void testRetryAcknowledgements() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT,
                AcknowledgeType.ACCEPT, AcknowledgeType.RELEASE, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), 60000L);
        assertNull(shareConsumeRequestManager.requestStates(0).getAsyncRequest());

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.REQUEST_TIMED_OUT));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getIncompleteAcknowledgementsCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        // Wait for backoff time before sending the next request.
        // After the first attempt, it can maximum be 1.2x of the configured backoff when acknowledge fails. (jitter = 0.2)
        time.sleep((long) (1.5 * retryBackoffMs));
        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getIncompleteAcknowledgementsCount(tip0));
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER", "UNKNOWN_TOPIC_OR_PARTITION"})
    public void testFatalErrorsAcknowledgementResponse(Errors error) {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        client.prepareResponse(fullShareAcknowledgeResponse(tip0, error));
        networkClientDelegate.poll(time.timer(0));

        // Assert these errors are not retried even if they are retriable. They are treated as fatal and a metadata update is triggered.
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getIncompleteAcknowledgementsCount(tip0));
        assertEquals(1, completedAcknowledgements.size());
        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
    }

    @Test
    public void testRetryAcknowledgementsMultipleCommitAsync() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        // commitAsync() acknowledges the first 2 records.
        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), calculateDeadlineMs(time, 1000L));

        assertEquals(2, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(2, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));

        // Response contains a retriable exception, so we retry.
        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.REQUEST_TIMED_OUT));
        networkClientDelegate.poll(time.timer(0));

        Acknowledgements acknowledgements1 = getAcknowledgements(3, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        // 2nd commitAsync() acknowledges the next 2 records.
        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements1)), calculateDeadlineMs(time, 1000L));
        assertEquals(2, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getIncompleteAcknowledgementsCount(tip0));

        Acknowledgements acknowledgements2 = getAcknowledgements(5, AcknowledgeType.RELEASE, AcknowledgeType.ACCEPT);

        // 3rd commitAsync() acknowledges the next 2 records.
        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)), calculateDeadlineMs(time, 1000L));

        time.sleep(2000L);

        // As the timer for the initial commitAsync() was 1000ms, the request times out, and we fill the callback with a timeout exception.
        assertEquals(0, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(1, completedAcknowledgements.size());
        assertEquals(2, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(Errors.REQUEST_TIMED_OUT.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
        completedAcknowledgements.clear();

        // Further requests which came before the timeout are processed as expected.
        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(4, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getInFlightAcknowledgementsCount(tip0));
        assertEquals(1, completedAcknowledgements.size());
        assertEquals(4, completedAcknowledgements.get(0).get(tip0).size());
        assertNull(completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
    }

    @Test
    public void testRetryAcknowledgementsMultipleCommitSync() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        // commitSync() for the first 2 acknowledgements.
        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)), calculateDeadlineMs(time, 1000L));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        // Response contains a retriable exception.
        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.REQUEST_TIMED_OUT));
        networkClientDelegate.poll(time.timer(0));
        assertEquals(2, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getIncompleteAcknowledgementsCount(tip0));

        // We expire the commitSync request as it had a timer of 1000ms.
        time.sleep(2000L);

        Acknowledgements acknowledgements1 = getAcknowledgements(3, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT, AcknowledgeType.RELEASE, AcknowledgeType.ACCEPT);

        // commitSync() for the next 4 acknowledgements.
        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements1)), calculateDeadlineMs(time, 1000L));

        // We send the 2nd commitSync request, and fail the first one as timer has expired.
        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(2, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(Errors.REQUEST_TIMED_OUT.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
        completedAcknowledgements.clear();

        // We get a successful response for the 2nd commitSync request.
        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(1, completedAcknowledgements.size());
        assertEquals(4, completedAcknowledgements.get(0).get(tip0).size());
        assertNull(completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
    }

    @Test
    public void testPiggybackAcknowledgementsInFlight() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        // Reading records from the share fetch buffer.
        fetchRecords();

        // Piggyback acknowledgements
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(2.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(3L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)));

        client.prepareResponse(fullShareFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        fetchRecords();

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
        assertEquals(3.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());
    }

    @Test
    public void testAcknowledgeErrorMessagePropagatedFromShareFetchResponse() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        fetchRecords();

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        String acknowledgeErrorMessage = "ack failure with broker context";
        ShareFetchResponse response = fullShareFetchResponse(
            tip0,
            records,
            acquiredRecords,
            Errors.NONE,
            Errors.UNKNOWN_SERVER_ERROR
        );
        response.data().responses().forEach(topicResponse ->
            topicResponse.partitions().forEach(partition ->
                partition.setAcknowledgeErrorMessage(acknowledgeErrorMessage)
            )
        );

        client.prepareResponse(response);
        networkClientDelegate.poll(time.timer(0));

        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
        fetchRecords();

        assertEquals(1, completedAcknowledgements.size());
        Map<TopicIdPartition, Acknowledgements> acknowledgementsMap = completedAcknowledgements.get(0);
        assertSame(acknowledgements, acknowledgementsMap.get(tip0));

        KafkaException acknowledgeException = acknowledgementsMap.get(tip0).getAcknowledgeException();
        assertInstanceOf(ApiException.class, acknowledgeException);
        assertEquals(acknowledgeErrorMessage, acknowledgeException.getMessage());
    }

    @Test
    public void testCommitAsyncWithSubscriptionChange() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        subscriptions.subscribeToShareGroup(Set.of(topicName2));
        subscriptions.assignFromSubscribed(Set.of(t2p0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName2, 1),
                        tp -> validLeaderEpoch, topicIds, false));

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertNull(completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        // We should send a fetch to the newly subscribed partition.
        assertEquals(1, sendFetches());

        client.prepareResponse(fullShareFetchResponse(t2ip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
    }

    @Test
    public void testCommitSyncWithSubscriptionChange() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        subscriptions.subscribeToShareGroup(Set.of(topicName2));
        subscriptions.assignFromSubscribed(Set.of(t2p0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName2, 1),
                        tp -> validLeaderEpoch, topicIds, false));

        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertNull(completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        // We should send a fetch to the newly subscribed partition.
        assertEquals(1, sendFetches());

        client.prepareResponse(fullShareFetchResponse(t2ip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCommitWithRenewAcknowledgements(boolean commitSync) {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);
        ShareFetch<byte[], byte[]> fetch = collectFetch();
        assertEquals(3, fetch.numRecords());

        int nodeId0 = metadata.fetch().leaderFor(tp0).id();

        // The application renews the delivered records. They are still logically held by the consumer.
        fetch.acknowledgeAll(AcknowledgeType.RENEW);
        Map<TopicIdPartition, NodeAcknowledgements> renewAcknowledgements = fetch.takeAcknowledgedRecords();
        Acknowledgements acknowledgements = renewAcknowledgements.get(tip0).acknowledgements();

        // Renew acknowledgements are committed through a ShareAcknowledge request (not piggybacked on a ShareFetch).
        CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future = null;
        if (commitSync) {
            future = shareConsumeRequestManager.commitSync(renewAcknowledgements, calculateDeadlineMs(time.timer(2000)));
        } else {
            shareConsumeRequestManager.commitAsync(renewAcknowledgements, calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));
        }

        // The partition is no longer assigned, so the share session would normally be tidied up and eventually closed.
        // However, the delivery of the records with renew acknowledgements must be completed before that can happen.
        subscriptions.assignFromSubscribed(Set.of());

        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        ShareAcknowledgeRequest.Builder builder = (ShareAcknowledgeRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
        assertTrue(builder.data().isRenewAck());
        networkClientDelegate.addAll(pollResult.unsentRequests);
        assertEquals(0, renewedRecords.size());

        // While the renew acknowledgements are in flight, the share session is not closed.
        assertEquals(0, sendFetches());
        assertNotNull(shareConsumeRequestManager.sessionHandler(nodeId0));

        // The broker responds to the renew acknowledgements and the records are reported to the application.
        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));
        assertEquals(Set.of(1L, 2L, 3L), renewedRecords);
        if (commitSync) {
            assertTrue(future.isDone());
        }

        // The revoked partition is removed from the share session, but the session cannot be closed while the records are still held.
        NetworkClientDelegate.PollResult removeResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, removeResult.unsentRequests.size());
        ShareFetchRequest.Builder removeBuilder = (ShareFetchRequest.Builder) removeResult.unsentRequests.get(0).requestBuilder();
        assertNotEquals(ShareRequestMetadata.FINAL_EPOCH, removeBuilder.data().shareSessionEpoch());
        assertEquals(1, removeBuilder.data().forgottenTopicsData().size());
        assertEquals(tip0.topicId(), removeBuilder.data().forgottenTopicsData().get(0).topicId());
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.sendFetchesReturnPollResult().unsentRequests.size());
        assertNotNull(shareConsumeRequestManager.sessionHandler(nodeId0));

        fetch.renew(Map.of(tip0, acknowledgements), Optional.empty());
        fetch.takeRenewedRecords();
        assertEquals(3, fetch.numRecords());
        assertEquals(0, shareConsumeRequestManager.sendFetchesReturnPollResult().unsentRequests.size());
        assertNotNull(shareConsumeRequestManager.sessionHandler(nodeId0));

        // The application finally accepts the records and the acknowledgements are sent.
        fetch.acknowledgeAll(AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(fetch.takeAcknowledgedRecords());
        NetworkClientDelegate.PollResult ackResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, ackResult.unsentRequests.size());

        ShareFetchRequest.Builder ackBuilder = (ShareFetchRequest.Builder) ackResult.unsentRequests.get(0).requestBuilder();
        assertNotEquals(ShareRequestMetadata.FINAL_EPOCH, ackBuilder.data().shareSessionEpoch());
        client.prepareResponse(fullShareFetchResponse(tip0, MemoryRecords.EMPTY, emptyAcquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        // The application consumes the fetch response corresponding to the piggybacked acknowledgements.
        fetchRecords();

        // With the record finally acknowledged, the empty share session is closed.
        NetworkClientDelegate.PollResult closeResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, closeResult.unsentRequests.size());
        ShareFetchRequest.Builder closeBuilder = (ShareFetchRequest.Builder) closeResult.unsentRequests.get(0).requestBuilder();
        assertEquals(ShareRequestMetadata.FINAL_EPOCH, closeBuilder.data().shareSessionEpoch());
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0));
        networkClientDelegate.poll(time.timer(0));

        assertNull(shareConsumeRequestManager.sessionHandler(nodeId0));
    }

    @Test
    public void testCloseWithSubscriptionChange() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        subscriptions.subscribeToShareGroup(Set.of(topicName2));
        subscriptions.assignFromSubscribed(Set.of(t2p0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName2, 1),
                        tp -> validLeaderEpoch, topicIds, false));

        shareConsumeRequestManager.acknowledgeOnClose(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(100)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertNull(completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        // As we are closing, we would not send any more fetches.
        assertEquals(0, sendFetches());
    }

    @Test
    public void testShareFetchWithSubscriptionChange() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.RELEASE, AcknowledgeType.ACCEPT);

        // Send acknowledgements via ShareFetch
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));
        fetchRecords();

        // Subscription changes.
        subscriptions.subscribeToShareGroup(Set.of(topicName2));
        subscriptions.assignFromSubscribed(Set.of(t2p0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName2, 1),
                        tp -> validLeaderEpoch, topicIds, false));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
        assertEquals(3.0,
                metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());
    }

    @Test
    public void testShareFetchWithSubscriptionChangeMultipleNodes() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        Node tp0Leader = metadata.fetch().leaderFor(tp0);
        Node tp1Leader = metadata.fetch().leaderFor(tp1);

        assertEquals(nodeId0, tp0Leader);
        assertEquals(nodeId1, tp1Leader);

        sendFetchAndVerifyResponse(records, emptyAcquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(0, AcknowledgeType.ACCEPT, AcknowledgeType.RELEASE, AcknowledgeType.ACCEPT);

        // Send acknowledgements via ShareFetch
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));
        fetchRecords();

        // Subscription changes.
        subscriptions.assignFromSubscribed(List.of(tp1));

        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(2, pollResult.unsentRequests.size());

        ShareFetchRequest.Builder builder1, builder2;
        if (pollResult.unsentRequests.get(0).node().get() == nodeId0) {
            builder1 = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
            builder2 = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(1).requestBuilder();
            assertEquals(nodeId1, pollResult.unsentRequests.get(1).node().get());
        } else {
            builder1 = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(1).requestBuilder();
            builder2 = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
            assertEquals(nodeId0, pollResult.unsentRequests.get(1).node().get());
            assertEquals(nodeId1, pollResult.unsentRequests.get(0).node().get());
        }

        // Verify the builder data for node0.
        assertEquals(1, builder1.data().topics().size());
        ShareFetchRequestData.FetchTopic fetchTopic = builder1.data().topics().stream().findFirst().get();
        assertEquals(tip0.topicId(), fetchTopic.topicId());
        assertEquals(1, fetchTopic.partitions().size());
        ShareFetchRequestData.FetchPartition fetchPartition = fetchTopic.partitions().stream().findFirst().get();
        assertEquals(0, fetchPartition.partitionIndex());
        assertEquals(1, fetchPartition.acknowledgementBatches().size());
        assertEquals(0L, fetchPartition.acknowledgementBatches().get(0).firstOffset());
        assertEquals(2L, fetchPartition.acknowledgementBatches().get(0).lastOffset());

        assertEquals(1, builder1.data().forgottenTopicsData().size());
        assertEquals(tip0.topicId(), builder1.data().forgottenTopicsData().get(0).topicId());
        assertEquals(1, builder1.data().forgottenTopicsData().get(0).partitions().size());
        assertEquals(0, builder1.data().forgottenTopicsData().get(0).partitions().get(0));

        // Verify the builder data for node1.
        assertEquals(1, builder2.data().topics().size());
        fetchTopic = builder2.data().topics().stream().findFirst().get();
        assertEquals(tip1.topicId(), fetchTopic.topicId());
        assertEquals(1, fetchTopic.partitions().size());
        assertEquals(1, fetchTopic.partitions().stream().findFirst().get().partitionIndex());
    }

    @Test
    public void testShareFetchWithSubscriptionChangeMultipleNodesEmptyAcknowledgements() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        Node tp0Leader = metadata.fetch().leaderFor(tp0);
        Node tp1Leader = metadata.fetch().leaderFor(tp1);

        assertEquals(nodeId0, tp0Leader);
        assertEquals(nodeId1, tp1Leader);

        // Send the first ShareFetch with an empty response
        sendFetchAndVerifyResponse(records, emptyAcquiredRecords, Errors.NONE);

        fetchRecords();

        // Change the subscription.
        subscriptions.assignFromSubscribed(List.of(tp1));

        // We build a request to node 1 to fetch tip1, and a request to node 0 to remove tip0
        // from the share session even though there are no acknowledgements to send.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(2, pollResult.unsentRequests.size());

        ShareFetchRequest.Builder node0Builder, node1Builder;
        if (pollResult.unsentRequests.get(0).node().get() == nodeId0) {
            node0Builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
            node1Builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(1).requestBuilder();
            assertEquals(nodeId1, pollResult.unsentRequests.get(1).node().get());
        } else {
            node0Builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(1).requestBuilder();
            node1Builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
            assertEquals(nodeId0, pollResult.unsentRequests.get(1).node().get());
            assertEquals(nodeId1, pollResult.unsentRequests.get(0).node().get());
        }

        // node1 fetches the newly assigned partition tip1.
        assertEquals(1, node1Builder.data().topics().size());
        ShareFetchRequestData.FetchTopic fetchTopic = node1Builder.data().topics().stream().findFirst().get();
        assertEquals(tip1.topicId(), fetchTopic.topicId());
        assertEquals(1, fetchTopic.partitions().size());
        assertEquals(1, fetchTopic.partitions().stream().findFirst().get().partitionIndex());
        assertEquals(0, node1Builder.data().forgottenTopicsData().size());

        // node0 removes tip0 from the share session and fetches nothing.
        assertEquals(0, node0Builder.data().topics().size());
        assertEquals(1, node0Builder.data().forgottenTopicsData().size());
        assertEquals(tip0.topicId(), node0Builder.data().forgottenTopicsData().get(0).topicId());
        assertEquals(1, node0Builder.data().forgottenTopicsData().get(0).partitions().size());
        assertEquals(0, node0Builder.data().forgottenTopicsData().get(0).partitions().get(0));
    }

    @Test
    public void testShareFetchRemovesUnassignedPartitionFromSession() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));

        // Establish the share session by fetching from tp0.
        sendFetchAndVerifyResponse(records, emptyAcquiredRecords, Errors.NONE);
        fetchRecords();

        // The partition is no longer assigned and there are no acknowledgements to send.
        subscriptions.assignFromSubscribed(Set.of());

        // We still build a ShareFetch to remove tip0 from the share session on the broker.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());

        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
        assertEquals(0, builder.data().topics().size());
        assertEquals(1, builder.data().forgottenTopicsData().size());
        assertEquals(tip0.topicId(), builder.data().forgottenTopicsData().get(0).topicId());
        assertEquals(1, builder.data().forgottenTopicsData().get(0).partitions().size());
        assertEquals(0, builder.data().forgottenTopicsData().get(0).partitions().get(0));

        // The partition has already been removed from the session, so no further ShareFetch is built.
        assertEquals(0, shareConsumeRequestManager.sendFetches());
    }

    @Test
    public void testCloseSessionWhenNoSharePartitions() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));

        // Establish the share session by fetching from tp0 and consume the (empty) buffered fetch.
        sendFetchAndVerifyResponse(records, emptyAcquiredRecords, Errors.NONE);
        fetchRecords();

        Node node0 = metadata.fetch().leaderFor(tp0);
        int nodeId0 = node0.id();
        assertNotNull(shareConsumeRequestManager.sessionHandler(nodeId0));

        // The partition is no longer assigned, so a ShareFetch is built to remove tip0 from the share session,
        // leaving it empty.
        subscriptions.assignFromSubscribed(Set.of());
        assertEquals(1, sendFetches());
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0));
        networkClientDelegate.poll(time.timer(0));

        // The next poll closes the now-empty share session using a final-epoch ShareFetch.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());
        assertEquals(node0, pollResult.unsentRequests.get(0).node().get());
        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
        assertEquals(ShareRequestMetadata.FINAL_EPOCH, builder.data().shareSessionEpoch());
        assertTrue(builder.data().topics().isEmpty());
        assertTrue(builder.data().forgottenTopicsData().isEmpty());

        // The session handle is only removed once the close response is received.
        assertNotNull(shareConsumeRequestManager.sessionHandler(nodeId0));
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0));
        networkClientDelegate.poll(time.timer(0));
        assertNull(shareConsumeRequestManager.sessionHandler(nodeId0));

        // With the session closed, there is nothing left to send.
        assertEquals(0, shareConsumeRequestManager.sendFetches());
    }

    @Test
    public void testSecondFetchBeforeCollectClearsInflightRecords() {
        buildRequestManager();
        assignFromSubscribed(Set.of(tp0));

        // Send and receive a successful response for tip0.
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        // Call fetch() again, what the next poll() call does, calls fetchMoreRecords and lets
        // a second fetch for the same partition go out and complete before the first has been collected.
        MemoryRecords secondRecords = buildRecords(4L, 2, 4);
        List<ShareFetchResponseData.AcquiredRecords> secondAcquiredRecords = ShareCompletedFetchTest.acquiredRecords(4L, 2);
        assertEquals(1, sendFetches());
        client.prepareResponse(fullShareFetchResponse(tip0, secondRecords, secondAcquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        // Drain both completed fetches for tip0 in one collect() call.
        ShareFetch<byte[], byte[]> fetch = collectFetch();
        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = fetch.records().get(tp0);
        assertEquals(5, fetchedRecords.size(), "records from both fetches should be visible after the merge");

        // Acknowledge every record the application actually received.
        fetch.acknowledgeAll(AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(fetch.takeAcknowledgedRecords());

        // Nothing should still look buffered/pending i.e. every record the application received has been
        // acknowledged. If the merged-away second batch's records were never cleared, tip0 leaks.
        assertTrue(shareConsumeRequestManager.shareFetchBuffer.bufferedPartitions().isEmpty(),
            "No partition should still be considered buffered after all delivered records are acknowledged");
    }

    @Test
    public void testEmptyResponseThenDataResponseBeforeCollectClearsInflightRecords() {
        buildRequestManager();
        assignFromSubscribed(Set.of(tp0));

        // The first request returns no acquired records. The empty result is still placed in the fetch buffer.
        assertEquals(1, sendFetches());
        client.prepareResponse(fullShareFetchResponse(tip0, MemoryRecords.EMPTY, emptyAcquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // Because no records were acquired, the request manager polls the same node straight away, without the
        // application having collected anything. The second response carries records and lands behind the empty
        // completed fetch for the same partition.
        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        client.prepareResponse(fullShareFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        // Drive the application side the way poll() does in implicit mode: collect, acknowledge everything,
        // take the acknowledgements, and repeat until the buffer has been drained.
        int recordsDelivered = 0;
        int acksTaken = 0;
        for (int i = 0; i < 10; i++) {
            ShareFetch<byte[], byte[]> fetch = collectFetch();
            recordsDelivered += fetch.numRecords();
            fetch.acknowledgeAll(AcknowledgeType.ACCEPT);
            Map<TopicIdPartition, NodeAcknowledgements> acks = fetch.takeAcknowledgedRecords();
            if (acks.containsKey(tip0)) {
                acksTaken += acks.get(tip0).acknowledgements().size();
            }
            assertTrue(fetch.isEmpty());
            if (!shareConsumeRequestManager.hasCompletedFetches()) {
                break;
            }
        }
        assertEquals(3, recordsDelivered);
        assertEquals(3, acksTaken);

        // Every delivered record has been acknowledged, so no partition should still be considered buffered.
        assertTrue(shareConsumeRequestManager.shareFetchBuffer.bufferedPartitions().isEmpty(),
            "No partition should still be considered buffered after all delivered records are acknowledged");
    }

    @Test
    public void testDoesNotCloseSessionWhileRecordsBuffered() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));

        // Establish the share session, leaving the fetched records unconsumed in the buffer.
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);
        Node node0 = metadata.fetch().leaderFor(tp0);
        int nodeId0 = node0.id();

        // Remove the partition from the session so the session becomes empty.
        subscriptions.assignFromSubscribed(Set.of());
        assertEquals(1, sendFetches());
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0));
        networkClientDelegate.poll(time.timer(0));

        // The session is empty, but records for the node remain in the buffer, so the session is not closed.
        assertEquals(0, shareConsumeRequestManager.sendFetchesReturnPollResult().unsentRequests.size());
        assertNotNull(shareConsumeRequestManager.sessionHandler(nodeId0));

        // Consuming the records hands them to the application, but leaves the acknowledgements outstanding and the session
        // is not closed.
        ShareFetch<byte[], byte[]> fetch = collectFetch();
        assertEquals(0, shareConsumeRequestManager.sendFetchesReturnPollResult().unsentRequests.size());
        assertNotNull(shareConsumeRequestManager.sessionHandler(nodeId0));

        // Acknowledge the records and send the acknowledgements. This is not a close request.
        fetch.acknowledgeAll(AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(fetch.takeAcknowledgedRecords());
        NetworkClientDelegate.PollResult ackResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, ackResult.unsentRequests.size());
        ShareFetchRequest.Builder ackBuilder = (ShareFetchRequest.Builder) ackResult.unsentRequests.get(0).requestBuilder();
        assertNotEquals(ShareRequestMetadata.FINAL_EPOCH, ackBuilder.data().shareSessionEpoch());
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0));
        networkClientDelegate.poll(time.timer(0));

        // Once the acknowledgements have been sent, the empty session is closed.
        NetworkClientDelegate.PollResult closeResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, closeResult.unsentRequests.size());
        assertEquals(node0, closeResult.unsentRequests.get(0).node().get());
        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) closeResult.unsentRequests.get(0).requestBuilder();
        assertEquals(ShareRequestMetadata.FINAL_EPOCH, builder.data().shareSessionEpoch());
    }

    @Test
    public void testDoesNotCloseSessionWhileAcknowledgementsPending() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));

        // Establish the share session by fetching from tp0 and consume the (empty) buffered fetch.
        sendFetchAndVerifyResponse(records, emptyAcquiredRecords, Errors.NONE);
        fetchRecords();

        Node node0 = metadata.fetch().leaderFor(tp0);
        int nodeId0 = node0.id();
        assertNotNull(shareConsumeRequestManager.sessionHandler(nodeId0));

        // Remove the partition from the session so the session becomes empty.
        subscriptions.assignFromSubscribed(Set.of());
        assertEquals(1, sendFetches());
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0));
        networkClientDelegate.poll(time.timer(0));

        // There are acknowledgements still to be delivered for the node, so the session is not closed.
        Acknowledgements acknowledgements = getAcknowledgements(0, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(nodeId0, acknowledgements)));

        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.poll(time.milliseconds());
        assertEquals(1, pollResult.unsentRequests.size());
        assertEquals(node0, pollResult.unsentRequests.get(0).node().get());
        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
        assertNotEquals(ShareRequestMetadata.FINAL_EPOCH, builder.data().shareSessionEpoch());
        assertNotNull(shareConsumeRequestManager.sessionHandler(nodeId0));
    }

    @Test
    public void testShareFetchAndCloseMultipleNodes() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0, tp1));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullShareFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        client.prepareResponse(fullShareFetchResponse(tip1, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);
        Acknowledgements acknowledgements1 = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        Map<TopicIdPartition, NodeAcknowledgements> acknowledgementsMap = new HashMap<>();
        acknowledgementsMap.put(tip0, new NodeAcknowledgements(0, acknowledgements));
        acknowledgementsMap.put(tip1, new NodeAcknowledgements(1, acknowledgements1));
        shareConsumeRequestManager.acknowledgeOnClose(acknowledgementsMap, calculateDeadlineMs(time, 1000L));

        assertEquals(2, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        client.prepareResponse(fullShareAcknowledgeResponse(tip1, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(3, completedAcknowledgements.get(0).get(tip1).size());

        assertEquals(0, shareConsumeRequestManager.sendAcknowledgements());
        assertNull(shareConsumeRequestManager.requestStates(0));
        assertNull(shareConsumeRequestManager.requestStates(1));
    }

    @Test
    public void testRetryAcknowledgementsWithLeaderChange() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 1),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        LinkedList<Node> nodeList = new LinkedList<>(Arrays.asList(nodeId0, nodeId1));

        sendFetchAndVerifyResponse(buildRecords(1L, 6, 1),
                ShareCompletedFetchTest.acquiredRecords(1L, 6), Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT,
                AcknowledgeType.ACCEPT, AcknowledgeType.RELEASE, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
            calculateDeadlineMs(time.timer(60000L)));
        assertNull(shareConsumeRequestManager.requestStates(0).getAsyncRequest());

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().size());
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getAcknowledgementsToSendCount(tip0));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertEquals(6, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));

        // Fail the acknowledgement and provide the new current leader information - this should stop the retry
        client.prepareResponse(fullShareAcknowledgeResponse(tip0,
            Errors.NOT_LEADER_OR_FOLLOWER,
            new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(nodeId1.id()).setLeaderEpoch(validLeaderEpoch + 1),
            nodeList));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getInFlightAcknowledgementsCount(tip0));
        assertEquals(0, shareConsumeRequestManager.requestStates(0).getSyncRequestQueue().peek().getIncompleteAcknowledgementsCount(tip0));
    }

    @Test
    public void testCallbackHandlerConfig() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));

        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(Map.of(tip0, acknowledgements), completedAcknowledgements.get(0));

        completedAcknowledgements.clear();

        // Setting the boolean to false, indicating there is no callback handler registered.
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(false);

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(3L, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        // Wait for backoff time before sending the next request.
        time.sleep(retryBackoffMs);
        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.NONE));
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

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap =
                buildPartitionDataMap(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE);
        partitionDataMap.put(t2ip0, partitionDataForShareFetch(t2ip0, records, acquiredRecords, Errors.NONE, Errors.NONE));
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, partitionDataMap, List.of(), 0));

        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        Acknowledgements acknowledgements2 = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        Map<TopicIdPartition, NodeAcknowledgements> acks = new HashMap<>();
        acks.put(tip0, new NodeAcknowledgements(0, acknowledgements));
        acks.put(t2ip0, new NodeAcknowledgements(0, acknowledgements2));

        shareConsumeRequestManager.commitAsync(acks, calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        Map<TopicIdPartition, Errors> errorsMap = new HashMap<>();
        errorsMap.put(tip0, Errors.NONE);
        errorsMap.put(t2ip0, Errors.NONE);
        client.prepareResponse(fullShareAcknowledgeResponse(errorsMap));

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

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap =
                buildPartitionDataMap(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE);
        partitionDataMap.put(t2ip0, partitionDataForShareFetch(t2ip0, records, emptyAcquiredRecords, Errors.TOPIC_AUTHORIZATION_FAILED, Errors.NONE));
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, partitionDataMap, List.of(), 0));

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

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionDataMap =
                buildPartitionDataMap(t2ip0, records, emptyAcquiredRecords, Errors.TOPIC_AUTHORIZATION_FAILED, Errors.NONE);
        partitionDataMap.put(tip0, partitionDataForShareFetch(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE));
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, partitionDataMap, List.of(), 0));

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
    public void testShareFetchInvalidResponse() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(Set.of(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName, 1),
                        tp -> validLeaderEpoch, topicIds, false));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullShareFetchResponse(t2ip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
    }

    @Test
    public void testShareAcknowledgeInvalidResponse() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(Set.of(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName, 1),
                        tp -> validLeaderEpoch, topicIds, false));

        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        fetchRecords();

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        // If a top-level error is received, we still retry the acknowledgements independent of the topic-partitions received in the response.
        client.prepareResponse(shareAcknowledgeResponseWithTopLevelError(t2ip0, Errors.LEADER_NOT_AVAILABLE));
        networkClientDelegate.poll(time.timer(0));

        assertEquals(1, shareConsumeRequestManager.requestStates(0).getAsyncRequest().getIncompleteAcknowledgementsCount(tip0));

        // Wait for backoff time before sending the next request. (it can maximum be 1.2x of the configured backoff when acknowledge fails.)
        time.sleep((long) (1.5 * retryBackoffMs));
        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());

        client.prepareResponse(fullShareAcknowledgeResponse(t2ip0, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));

        // If we do not get the expected partitions in the response, we fail these acknowledgements with InvalidRecordStateException.
        assertEquals(InvalidRecordStateException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException().getClass());
        completedAcknowledgements.clear();

        // Send remaining acknowledgements through piggybacking on the next fetch.
        Acknowledgements acknowledgements1 = getAcknowledgements(2,
                AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements1)));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullShareFetchResponse(t2ip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        // If we do not get the expected partitions in the response, we fail these acknowledgements with InvalidRecordStateException.
        assertEquals(InvalidRecordStateException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException().getClass());
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
    public void testShareFetchError() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, emptyAcquiredRecords, Errors.NOT_LEADER_OR_FOLLOWER);

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertFalse(partitionRecords.containsKey(tp0));
    }

    @Test
    public void testPiggybackAcknowledgementsOnInitialShareSessionError() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));

        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);

        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());
        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
        assertEquals(1, builder.data().topics().size());
        // We should not add the acknowledgements as part of the request.
        assertEquals(0, builder.data().topics().find(tip0.topicId()).partitions().find(0).acknowledgementBatches().size());

        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(Errors.NETWORK_EXCEPTION.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
    }

    @Test
    public void testPiggybackAcknowledgementsOnInitialShareSessionErrorTopicRemovedFromMetadata() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        fetchRecords();

        // Simulate a broker restart, but no leader change, this resets share session epoch to 0.
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
        client.prepareResponse(shareFetchResponseWithTopLevelError(tip0, Errors.SHARE_SESSION_NOT_FOUND));
        networkClientDelegate.poll(time.timer(0));

        // Simulate a metadata update with no topics in the response.
        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(),
                        tp -> validLeaderEpoch, null, false));

        // The acknowledgements for the initial fetch from tip0 are processed now and sent to the background thread.
        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        assertEquals(0, completedAcknowledgements.size());

        // Next fetch would not include any acknowledgements, but it will include tip-0 because it's a new share session.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());
        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
        assertEquals(1, builder.data().topics().size());

        // We should fail any waiting acknowledgements for tip-0 as it would have a share session epoch equal to 0.
        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(Errors.NETWORK_EXCEPTION.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
    }

    @Test
    public void testPiggybackAcknowledgementsOnInitialShareSession_ShareSessionNotFound() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        fetchRecords();

        // The acknowledgements for the initial fetch from tip0 are processed now and sent to the background thread.
        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        // We attempt to send the acknowledgements piggybacking on the fetch.
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        // Simulate a broker restart, but no leader change, this resets share session epoch to 0.
        client.prepareResponse(shareFetchResponseWithTopLevelError(tip0, Errors.SHARE_SESSION_NOT_FOUND));
        networkClientDelegate.poll(time.timer(0));

        // We would complete these acknowledgements with the error code from the response.
        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());
        assertEquals(Errors.NETWORK_EXCEPTION.exception(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        // Next fetch would proceed as expected and would not include any acknowledgements.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());
        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
        assertEquals(0, builder.data().topics().find(topicId).partitions().find(0).acknowledgementBatches().size());
    }

    @Test
    public void testRecordLatencyOnFetchResponseLevelError() {
        // Latency is recorded on response-level errors (ex: SHARE_SESSION_NOT_FOUND) since the round-trip completed.
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);
        verify(metricsManager, times(1)).recordLatency(anyString(), anyLong());

        assertEquals(1, sendFetches());
        client.prepareResponse(shareFetchResponseWithTopLevelError(tip0, Errors.SHARE_SESSION_NOT_FOUND));
        networkClientDelegate.poll(time.timer(0));

        verify(metricsManager, times(2)).recordLatency(anyString(), anyLong());
    }

    @Test
    public void testInvalidDefaultRecordBatch() {
        buildRequestManager();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        ByteBufferOutputStream out = new SingleByteBufferOutputStream(buffer);

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

        assignFromSubscribed(Set.of(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        client.prepareResponse(fullShareFetchResponse(tip0,
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

        assignFromSubscribed(Set.of(tp0));

        // normal fetch
        assertEquals(1, sendFetches());
        client.prepareResponse(fullShareFetchResponse(tip0,
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
        assignFromSubscribed(Set.of(tp0));

        client.prepareResponse(fullShareFetchResponse(tip0,
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

        assignFromSubscribed(Set.of(tp0));

        assertEquals(1, sendFetches());
        client.prepareResponse(fullShareFetchResponse(tip0, records, emptyAcquiredRecords, Errors.TOPIC_AUTHORIZATION_FAILED));
        networkClientDelegate.poll(time.timer(0));
        TopicAuthorizationException e = assertThrows(TopicAuthorizationException.class, () -> collectFetch(), "collectFetch should have thrown a TopicAuthorizationException");
        assertEquals(Set.of(topicName), e.unauthorizedTopics());
    }

    @ParameterizedTest
    @MethodSource("handleShareFetchResponseErrorSupplier")
    public void testHandleShareFetchResponseError(Errors error,
                                                  boolean shouldRequestMetadataUpdate) {
        buildRequestManager();
        assignFromSubscribed(Set.of(tp0));

        assertEquals(1, sendFetches());

        final ShareFetchResponse fetchResponse;

        fetchResponse = fullShareFetchResponse(tip0, records, emptyAcquiredRecords, error);

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
     * Supplies parameters to {@link #testHandleShareFetchResponseError(Errors, boolean)}.
     */
    private static Stream<Arguments> handleShareFetchResponseErrorSupplier() {
        return Stream.of(
                Arguments.of(Errors.NOT_LEADER_OR_FOLLOWER, true),
                Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, true),
                Arguments.of(Errors.UNKNOWN_TOPIC_ID, true),
                Arguments.of(Errors.INCONSISTENT_TOPIC_ID, true),
                Arguments.of(Errors.FENCED_LEADER_EPOCH, true),
                Arguments.of(Errors.UNKNOWN_LEADER_EPOCH, false)
        );
    }

    @Test
    public void testShareFetchDisconnected() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));

        assertEquals(1, sendFetches());
        client.prepareResponse(fullShareFetchResponse(tip0, records, acquiredRecords, Errors.NONE), true);
        networkClientDelegate.poll(time.timer(0));
        assertEmptyFetch("Should not return records on disconnect");
    }

    @Test
    public void testShareFetchWithLastRecordMissingFromBatch() {
        buildRequestManager();

        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord("0".getBytes(), "v".getBytes()),
                new SimpleRecord("1".getBytes(), "v".getBytes()),
                new SimpleRecord("2".getBytes(), "v".getBytes()),
                new SimpleRecord(null, "value".getBytes()));

        // Remove the last record to simulate compaction
        MemoryRecords.FilterResult result = records.filterTo(new MemoryRecords.RecordFilter(0, 0) {
            @Override
            protected BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                return new BatchRetentionResult(BatchRetention.DELETE_EMPTY, false);
            }

            @Override
            protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                return record.key() != null;
            }
        }, ByteBuffer.allocate(1024), BufferSupplier.NO_CACHING, Records.SOFT_MAX_ARRAY_LENGTH);
        result.outputBuffer().flip();
        MemoryRecords compactedRecords = MemoryRecords.readableRecords(result.outputBuffer());

        assignFromSubscribed(Set.of(tp0));
        assertEquals(1, sendFetches());
        client.prepareResponse(fullShareFetchResponse(tip0,
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
        assignFromSubscribed(Set.of(tp0));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        // Prepare a response with the CORRUPT_MESSAGE error.
        client.prepareResponse(fullShareFetchResponse(
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

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        Node tp0Leader = metadata.fetch().leaderFor(tp0);
        Node tp1Leader = metadata.fetch().leaderFor(tp1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData =
                buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        partitionData = new LinkedHashMap<>();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setErrorCode(error.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertFalse(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        assertEquals(startingClusterMetadata, metadata.fetch());

        // Validate metadata update is requested due to the leadership error
        assertTrue(metadata.updateRequested());

        // Move the leadership of tp1 onto node 1
        LinkedList<Node> leaderNodes = new LinkedList<>(Arrays.asList(tp0Leader, tp1Leader));
        metadata.updatePartitionLeadership(Map.of(tp1, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId0.id()), Optional.of(validLeaderEpoch + 1))), leaderNodes);

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // And now the partitions are on the same leader but a fetch is still sent to the former leader to remove the partition from the share session
        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        partitionData = buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(2L, 1), Errors.NONE, Errors.NONE);
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        partitionData = new LinkedHashMap<>();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
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
    public void testWhenShareFetchResponseReturnsWithALeadershipChangeErrorAndNewLeaderInformation(Errors error) {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        Node tp0Leader = metadata.fetch().leaderFor(tp0);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData =
                buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        partitionData = new LinkedHashMap<>();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setErrorCode(error.code())
                .setCurrentLeader(new ShareFetchResponseData.LeaderIdAndEpoch()
                    .setLeaderId(tp0Leader.id())
                    .setLeaderEpoch(validLeaderEpoch + 1)));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(tp0Leader), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertFalse(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        // The metadata snapshot will have been updated with the new leader information
        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // Validate metadata update is still requested even though the current leader was returned
        assertTrue(metadata.updateRequested());

        // And now the partitions are on the same leader but a fetch is still sent to the former leader to remove the partition from the share session
        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        partitionData = buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(2L, 1), Errors.NONE, Errors.NONE);
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        partitionData = new LinkedHashMap<>();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
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
     * Test the scenario that the metadata indicated a change in leadership between ShareFetch requests such
     * as could occur when metadata is periodically updated. The metadata holds a newer leader epoch, so the
     * share consumer follows it without waiting for the former leader to redirect it.
     */
    @Test
    public void testWhenLeadershipChangeBetweenShareFetchRequests() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData =
                buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        partitionData = new LinkedHashMap<>();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertFalse(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1L, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        assertEquals(startingClusterMetadata, metadata.fetch());

        // Move the leadership of tp0 onto node 1 with a newer leader epoch
        metadata.updatePartitionLeadership(Map.of(tp0, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1))), List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // Both partitions are now fetched from node 1. A request is still sent to node 0 to remove tp0 from the share
        // session on the previous leader. The acknowledgements for records fetched from the previous leader cannot be
        // sent there any more, so they are failed with NOT_LEADER_OR_FOLLOWER without a round trip to the broker.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(2, pollResult.unsentRequests.size());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
        Map<Integer, ShareFetchRequestData> requestsByNode = new HashMap<>();
        pollResult.unsentRequests.forEach(unsentRequest ->
            requestsByNode.put(unsentRequest.node().get().id(), ((ShareFetchRequest.Builder) unsentRequest.requestBuilder()).data()));
        assertEquals(Set.of(nodeId0.id(), nodeId1.id()), requestsByNode.keySet());

        ShareFetchRequestData node0Request = requestsByNode.get(nodeId0.id());
        assertTrue(node0Request.topics().isEmpty());
        assertEquals(1, node0Request.forgottenTopicsData().size());
        assertEquals(List.of(tip0.partition()), node0Request.forgottenTopicsData().get(0).partitions());

        // tp1 is already in the share session on node 1, so the incremental request only adds tp0.
        ShareFetchRequestData node1Request = requestsByNode.get(nodeId1.id());
        assertEquals(1, node1Request.topics().size());
        assertEquals(1, node1Request.topics().find(tip0.topicId()).partitions().size());
        assertNotNull(node1Request.topics().find(tip0.topicId()).partitions().find(tip0.partition()));

        assertEquals(1, completedAcknowledgements.size());
        assertEquals(acknowledgements, completedAcknowledgements.get(0).get(tip0));
        assertInstanceOf(NotLeaderOrFollowerException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());

        networkClientDelegate.addAll(pollResult.unsentRequests);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0), nodeId0);
        partitionData = buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE, Errors.NONE);
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
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

    @Test
    void testLeadershipChangeAfterFetchBeforeCommitAsync() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData =
                buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        partitionData = buildPartitionDataMap(tip1, records, ShareCompletedFetchTest.acquiredRecords(1L, 2), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(2, fetchedRecords.size());

        Acknowledgements acknowledgementsTp0 = Acknowledgements.empty();
        acknowledgementsTp0.add(1L, AcknowledgeType.ACCEPT);

        Acknowledgements acknowledgementsTp1 = getAcknowledgements(1,
                        AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        Map<TopicIdPartition, NodeAcknowledgements> commitAcks = new HashMap<>();
        commitAcks.put(tip0, new NodeAcknowledgements(0, acknowledgementsTp0));
        commitAcks.put(tip1, new NodeAcknowledgements(1, acknowledgementsTp1));

        // Move the leadership of tp0 onto node 1.
        metadata.updatePartitionLeadership(Map.of(tp0, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1))), List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // The acknowledgements are sent to the nodes the records were fetched from.
        shareConsumeRequestManager.commitAsync(commitAcks, calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));
        assertTrue(completedAcknowledgements.isEmpty());
        assertEquals(2, shareConsumeRequestManager.sendAcknowledgements());

        // The former leader for tp0 rejects the acknowledgements with NOT_LEADER_OR_FOLLOWER and points to the new leader.
        client.prepareResponseFrom(fullShareAcknowledgeResponse(tip0,
            Errors.NOT_LEADER_OR_FOLLOWER,
            new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(nodeId1.id()).setLeaderEpoch(validLeaderEpoch + 1),
            List.of(nodeId0, nodeId1)), nodeId0);
        client.prepareResponseFrom(fullShareAcknowledgeResponse(tip1, Errors.NONE), nodeId1);
        networkClientDelegate.poll(time.timer(0));

        Map<TopicIdPartition, Acknowledgements> completed = new HashMap<>();
        completedAcknowledgements.forEach(completed::putAll);
        assertEquals(2, completed.size());
        assertEquals(acknowledgementsTp0, completed.get(tip0));
        assertInstanceOf(NotLeaderOrFollowerException.class, completed.get(tip0).getAcknowledgeException());
        assertEquals(acknowledgementsTp1, completed.get(tip1));
        assertNull(completed.get(tip1).getAcknowledgeException());

        assertEquals(nodeId1.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));
    }

    @Test
    void testLeadershipChangeAfterFetchBeforeCommitSync() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0, tp1));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData =
                buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        partitionData = buildPartitionDataMap(tip1, records, ShareCompletedFetchTest.acquiredRecords(1L, 2), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(2, fetchedRecords.size());

        Acknowledgements acknowledgementsTp0 = Acknowledgements.empty();
        acknowledgementsTp0.add(1L, AcknowledgeType.ACCEPT);

        Acknowledgements acknowledgementsTp1 = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        Map<TopicIdPartition, NodeAcknowledgements> commitAcks = new HashMap<>();
        commitAcks.put(tip0, new NodeAcknowledgements(0, acknowledgementsTp0));
        commitAcks.put(tip1, new NodeAcknowledgements(1, acknowledgementsTp1));

        // Move the leadership of tp0 onto node 1.
        metadata.updatePartitionLeadership(Map.of(tp0, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1))), List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // The acknowledgements are sent to the nodes the records were fetched from.
        shareConsumeRequestManager.commitSync(commitAcks, calculateDeadlineMs(time.timer(100)));
        assertEquals(2, shareConsumeRequestManager.sendAcknowledgements());

        // The former leader for tp0 rejects the acknowledgements with NOT_LEADER_OR_FOLLOWER and points to the new leader.
        client.prepareResponseFrom(fullShareAcknowledgeResponse(tip0,
            Errors.NOT_LEADER_OR_FOLLOWER,
            new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(nodeId1.id()).setLeaderEpoch(validLeaderEpoch + 1),
            List.of(nodeId0, nodeId1)), nodeId0);
        client.prepareResponseFrom(fullShareAcknowledgeResponse(tip1, Errors.NONE), nodeId1);
        networkClientDelegate.poll(time.timer(0));

        Map<TopicIdPartition, Acknowledgements> completed = new HashMap<>();
        completedAcknowledgements.forEach(completed::putAll);
        assertEquals(2, completed.size());
        assertEquals(acknowledgementsTp0, completed.get(tip0));
        assertInstanceOf(NotLeaderOrFollowerException.class, completed.get(tip0).getAcknowledgeException());
        assertEquals(acknowledgementsTp1, completed.get(tip1));
        assertNull(completed.get(tip1).getAcknowledgeException());

        assertEquals(nodeId1.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));
    }

    @Test
    void testLeadershipChangeAfterFetchBeforeCloseMove() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData =
                buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        partitionData = buildPartitionDataMap(tip1, records, ShareCompletedFetchTest.acquiredRecords(1L, 2), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(2, fetchedRecords.size());

        Acknowledgements acknowledgementsTp0 = Acknowledgements.empty();
        acknowledgementsTp0.add(1L, AcknowledgeType.ACCEPT);

        Acknowledgements acknowledgementsTp1 = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.fetch(Map.of(tip1, new NodeAcknowledgements(1, acknowledgementsTp1)));

        // Move the leadership of tp0 onto node 1.
        metadata.updatePartitionLeadership(Map.of(tp0, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1))), List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // The acknowledgements are sent to the nodes the records were fetched from.
        shareConsumeRequestManager.acknowledgeOnClose(Map.of(tip0, new NodeAcknowledgements(0, acknowledgementsTp0)),
                calculateDeadlineMs(time.timer(100)));
        assertTrue(completedAcknowledgements.isEmpty());
        assertEquals(2, shareConsumeRequestManager.sendAcknowledgements());

        // The former leader for tp0 rejects the acknowledgements with NOT_LEADER_OR_FOLLOWER and points to the new leader.
        client.prepareResponseFrom(fullShareAcknowledgeResponse(tip0,
            Errors.NOT_LEADER_OR_FOLLOWER,
            new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(nodeId1.id()).setLeaderEpoch(validLeaderEpoch + 1),
            List.of(nodeId0, nodeId1)), nodeId0);
        client.prepareResponseFrom(fullShareAcknowledgeResponse(tip1, Errors.NONE), nodeId1);
        networkClientDelegate.poll(time.timer(0));

        Map<TopicIdPartition, Acknowledgements> completed = new HashMap<>();
        completedAcknowledgements.forEach(completed::putAll);
        assertEquals(2, completed.size());
        assertEquals(acknowledgementsTp0, completed.get(tip0));
        assertInstanceOf(NotLeaderOrFollowerException.class, completed.get(tip0).getAcknowledgeException());
        assertEquals(acknowledgementsTp1, completed.get(tip1));
        assertNull(completed.get(tip1).getAcknowledgeException());
    }

    @Test
    void testLeadershipChangeAfterFetchMoveBeforeClose() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData =
            buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        partitionData = buildPartitionDataMap(tip1, records, ShareCompletedFetchTest.acquiredRecords(1L, 2), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(2, fetchedRecords.size());

        Acknowledgements acknowledgementsTp0 = Acknowledgements.empty();
        acknowledgementsTp0.add(1L, AcknowledgeType.ACCEPT);

        Acknowledgements acknowledgementsTp1 = getAcknowledgements(1,
            AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.fetch(Map.of(tip1, new NodeAcknowledgements(1, acknowledgementsTp1)));

        // Move the leadership of tp1 onto node 0.
        metadata.updatePartitionLeadership(Map.of(tp1, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId0.id()), Optional.of(validLeaderEpoch + 1))), List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // The acknowledgements are sent to the nodes the records were fetched from.
        shareConsumeRequestManager.acknowledgeOnClose(Map.of(tip0, new NodeAcknowledgements(0, acknowledgementsTp0)),
            calculateDeadlineMs(time.timer(100)));
        assertTrue(completedAcknowledgements.isEmpty());
        assertEquals(2, shareConsumeRequestManager.sendAcknowledgements());

        // The former leader for tp0 rejects the acknowledgements with NOT_LEADER_OR_FOLLOWER and points to the new leader.
        client.prepareResponseFrom(fullShareAcknowledgeResponse(tip0, Errors.NONE), nodeId0);
        client.prepareResponseFrom(fullShareAcknowledgeResponse(tip1,
            Errors.NOT_LEADER_OR_FOLLOWER,
            new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(nodeId0.id()).setLeaderEpoch(validLeaderEpoch + 1),
            List.of(nodeId0, nodeId1)), nodeId1);
        networkClientDelegate.poll(time.timer(0));

        Map<TopicIdPartition, Acknowledgements> completed = new HashMap<>();
        completedAcknowledgements.forEach(completed::putAll);
        assertEquals(2, completed.size());
        assertEquals(acknowledgementsTp0, completed.get(tip0));
        assertNull(completed.get(tip0).getAcknowledgeException());
        assertEquals(acknowledgementsTp1.getAcknowledgementsTypeMap(), completed.get(tip1).getAcknowledgementsTypeMap());
        assertInstanceOf(NotLeaderOrFollowerException.class, completed.get(tip1).getAcknowledgeException());
    }

    @Test
    void testLeadershipChangeAfterFetchMoveBeforeCloseMove() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData =
            buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        partitionData = buildPartitionDataMap(tip1, records, ShareCompletedFetchTest.acquiredRecords(1L, 2), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(2, fetchedRecords.size());

        Acknowledgements acknowledgementsTp0 = Acknowledgements.empty();
        acknowledgementsTp0.add(1L, AcknowledgeType.ACCEPT);

        Acknowledgements acknowledgementsTp1 = getAcknowledgements(1,
            AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        shareConsumeRequestManager.fetch(Map.of(tip1, new NodeAcknowledgements(1, acknowledgementsTp1)));

        // Move the leadership of tp1 onto node 0, and tp0 onto node 1.
        metadata.updatePartitionLeadership(Map.of(tp1, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId0.id()), Optional.of(validLeaderEpoch + 1))), List.of());
        metadata.updatePartitionLeadership(Map.of(tp0, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1))), List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // The acknowledgements are sent to the nodes the records were fetched from.
        shareConsumeRequestManager.acknowledgeOnClose(Map.of(tip0, new NodeAcknowledgements(0, acknowledgementsTp0)),
            calculateDeadlineMs(time.timer(100)));
        assertEquals(2, shareConsumeRequestManager.sendAcknowledgements());

        // The former leaders reject the acknowledgements with NOT_LEADER_OR_FOLLOWER and point to the new leaders.
        client.prepareResponseFrom(fullShareAcknowledgeResponse(tip0,
            Errors.NOT_LEADER_OR_FOLLOWER,
            new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(nodeId1.id()).setLeaderEpoch(validLeaderEpoch + 1),
            List.of(nodeId0, nodeId1)), nodeId0);
        client.prepareResponseFrom(fullShareAcknowledgeResponse(tip1,
            Errors.NOT_LEADER_OR_FOLLOWER,
            new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(nodeId0.id()).setLeaderEpoch(validLeaderEpoch + 1),
            List.of(nodeId0, nodeId1)), nodeId1);
        networkClientDelegate.poll(time.timer(0));

        Map<TopicIdPartition, Acknowledgements> completed = new HashMap<>();
        completedAcknowledgements.forEach(completed::putAll);
        assertEquals(2, completed.size());
        assertEquals(acknowledgementsTp0.getAcknowledgementsTypeMap(), completed.get(tip0).getAcknowledgementsTypeMap());
        assertInstanceOf(NotLeaderOrFollowerException.class, completed.get(tip0).getAcknowledgeException());
        assertEquals(acknowledgementsTp1.getAcknowledgementsTypeMap(), completed.get(tip1).getAcknowledgementsTypeMap());
        assertInstanceOf(NotLeaderOrFollowerException.class, completed.get(tip1).getAcknowledgeException());
    }

    @Test
    void testStaleMetadataLeadershipChangeDoesNotFailAcknowledgements() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        Cluster startingClusterMetadata = metadata.fetch();
        assertFalse(metadata.updateRequested());

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData =
            buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        partitionData = buildPartitionDataMap(tip1, records, ShareCompletedFetchTest.acquiredRecords(1L, 2), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        Acknowledgements acknowledgementsTp0 = Acknowledgements.empty();
        acknowledgementsTp0.add(1L, AcknowledgeType.ACCEPT);

        Acknowledgements acknowledgementsTp1 = getAcknowledgements(1,
            AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);

        Map<TopicIdPartition, NodeAcknowledgements> commitAcks = new HashMap<>();
        commitAcks.put(tip0, new NodeAcknowledgements(0, acknowledgementsTp0));
        commitAcks.put(tip1, new NodeAcknowledgements(1, acknowledgementsTp1));

        // The metadata reports tp0 has moved to node 1, but the cached leader used for fetching (node 0) is unchanged.
        metadata.updatePartitionLeadership(Map.of(tp0, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1))), List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        // The acknowledgements are sent to the nodes the records were fetched from, so nothing fails synchronously.
        shareConsumeRequestManager.commitAsync(commitAcks, calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));
        assertTrue(completedAcknowledgements.isEmpty());

        // We send acknowledgements for tip0 to node0 and tip1 to node1.
        assertEquals(2, shareConsumeRequestManager.sendAcknowledgements());

        // Both cached leaders accept the acknowledgements despite the stale metadata for tp0.
        client.prepareResponseFrom(fullShareAcknowledgeResponse(tip0, Errors.NONE), nodeId0);
        client.prepareResponseFrom(fullShareAcknowledgeResponse(tip1, Errors.NONE), nodeId1);
        networkClientDelegate.poll(time.timer(0));

        Map<TopicIdPartition, Acknowledgements> completed = new HashMap<>();
        completedAcknowledgements.forEach(completed::putAll);
        assertEquals(2, completed.size());
        assertEquals(acknowledgementsTp0, completed.get(tip0));
        assertNull(completed.get(tip0).getAcknowledgeException());
        assertEquals(acknowledgementsTp1, completed.get(tip1));
        assertNull(completed.get(tip1).getAcknowledgeException());

        // The cached leader for tp0 is unchanged - the broker success did not repoint it to the stale metadata leader.
        assertEquals(nodeId0.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));
    }

    @Test
    void testWhenLeadershipChangedAfterDisconnected() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

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
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        partitionData.clear();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertFalse(partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        assertEquals(startingClusterMetadata, metadata.fetch());

        Acknowledgements acknowledgements1 = Acknowledgements.empty();
        acknowledgements1.add(1, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements1)));

        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        partitionData.clear();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0, true);
        partitionData.clear();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // The node was disconnected, so the acknowledgements for tp0 failed
        assertInstanceOf(DisconnectException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
        completedAcknowledgements.clear();

        // The disconnect lost the share session on node 0, so the cached leader for tp0 is forgotten.
        assertEquals(-1, shareConsumeRequestManager.shareSessionNodeId(tip0));

        partitionRecords = fetchRecords();
        assertFalse(partitionRecords.containsKey(tp0));
        assertTrue(partitionRecords.containsKey(tp1));

        fetchedRecords = partitionRecords.get(tp1);
        assertEquals(1, fetchedRecords.size());

        // Move the leadership of tp0 onto node 1
        metadata.updatePartitionLeadership(Map.of(tp0, new Metadata.LeaderIdAndEpoch(Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1))), List.of());

        assertNotEquals(startingClusterMetadata, metadata.fetch());

        Acknowledgements acknowledgements2 = Acknowledgements.empty();
        acknowledgements2.add(1, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip1, new NodeAcknowledgements(1, acknowledgements2)));

        // tp0 is re-seeded from the metadata and fetched from node 1. The share session on node 0 was lost, but its
        // handler still holds tp0, and a new share session is opened on node 0 containing tp0.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(2, pollResult.unsentRequests.size());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
        assertEquals(nodeId1.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));
        Map<Integer, ShareFetchRequestData> requestsByNode = new HashMap<>();
        pollResult.unsentRequests.forEach(unsentRequest ->
            requestsByNode.put(unsentRequest.node().get().id(), ((ShareFetchRequest.Builder) unsentRequest.requestBuilder()).data()));
        assertEquals(Set.of(nodeId0.id(), nodeId1.id()), requestsByNode.keySet());
        ShareFetchRequestData node0Request = requestsByNode.get(nodeId0.id());
        assertEquals(ShareRequestMetadata.INITIAL_EPOCH, node0Request.shareSessionEpoch());
        assertNotNull(node0Request.topics().find(tip0.topicId()).partitions().find(tip0.partition()));
        networkClientDelegate.addAll(pollResult.unsentRequests);

        partitionData.clear();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setRecords(records)
                .setAcquiredRecords(ShareCompletedFetchTest.acquiredRecords(1L, 1))
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);

        // Node 0 accepts the new share session, but it is no longer the leader for tp0, so it redirects to node 1.
        partitionData.clear();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                .setCurrentLeader(new ShareFetchResponseData.LeaderIdAndEpoch()
                    .setLeaderId(nodeId1.id())
                    .setLeaderEpoch(validLeaderEpoch + 1)));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(nodeId1), 0), nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertNull(completedAcknowledgements.get(0).get(tip1).getAcknowledgeException());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertFalse(partitionRecords.containsKey(tp1));

        fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        // The redirect leaves tp0 on node 1. The next poll removes tp0 from the share session on node 0.
        assertEquals(nodeId1.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));
        pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        requestsByNode.clear();
        pollResult.unsentRequests.forEach(unsentRequest ->
            requestsByNode.put(unsentRequest.node().get().id(), ((ShareFetchRequest.Builder) unsentRequest.requestBuilder()).data()));
        node0Request = requestsByNode.get(nodeId0.id());
        assertNotNull(node0Request);
        assertTrue(node0Request.topics().isEmpty());
        assertEquals(1, node0Request.forgottenTopicsData().size());
        assertEquals(List.of(tip0.partition()), node0Request.forgottenTopicsData().get(0).partitions());
    }

    /**
     * A disconnect forgets the cached leader, but if the metadata still names the same node, the next fetch simply
     * retries that node with a new share session containing the partition.
     */
    @Test
    void testDisconnectWithoutLeadershipChangeRetriesSameNode() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(Set.of(tp0));

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 1),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        assertEquals(nodeId0, metadata.fetch().leaderFor(tp0));

        // Establish the share session on node 0.
        assertEquals(1, sendFetches());
        client.prepareResponseFrom(
            ShareFetchResponse.of(Errors.NONE, 0,
                buildPartitionDataMap(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE), List.of(), 0),
            nodeId0);
        networkClientDelegate.poll(time.timer(0));
        fetchRecords();
        assertEquals(nodeId0.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));

        // The next fetch is disconnected, which forgets the cached leader.
        assertEquals(1, sendFetches());
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0), nodeId0, true);
        networkClientDelegate.poll(time.timer(0));
        assertEquals(-1, shareConsumeRequestManager.shareSessionNodeId(tip0));

        // The metadata is unchanged, so tp0 is re-seeded onto node 0 and a new share session is opened there.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());
        assertEquals(nodeId0, pollResult.unsentRequests.get(0).node().get());
        ShareFetchRequestData requestData = ((ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder()).data();
        assertEquals(ShareRequestMetadata.INITIAL_EPOCH, requestData.shareSessionEpoch());
        assertNotNull(requestData.topics().find(tip0.topicId()));
        assertNotNull(requestData.topics().find(tip0.topicId()).partitions().find(tip0.partition()));
        assertEquals(nodeId0.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));
    }

    /**
     * If leadership moves while the cached leader is unreachable, the former leader cannot redirect the consumer.
     * When the leader epoch does not advance, the metadata cannot replace the cached leader either, since that only
     * happens for a strictly newer epoch. Forgetting the cached leader on a disconnect means the next fetch is seeded
     * from the metadata and so follows the new leader as soon as the metadata names it.
     */
    @Test
    void testDisconnectFollowsMetadataLeaderChangeWithUnchangedEpoch() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(Set.of(tp0));

        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 1),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        assertEquals(nodeId0, metadata.fetch().leaderFor(tp0));

        // Establish the share session on node 0.
        assertEquals(1, sendFetches());
        client.prepareResponseFrom(
            ShareFetchResponse.of(Errors.NONE, 0,
                buildPartitionDataMap(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE), List.of(), 0),
            nodeId0);
        networkClientDelegate.poll(time.timer(0));
        fetchRecords();
        assertEquals(nodeId0.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));

        // The next fetch is disconnected, which forgets the cached leader.
        assertEquals(1, sendFetches());
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0), nodeId0, true);
        networkClientDelegate.poll(time.timer(0));
        assertEquals(-1, shareConsumeRequestManager.shareSessionNodeId(tip0));

        // A metadata refresh names node 1 as the leader at the same leader epoch. Node 0 is still in the cluster.
        metadata.updateWithCurrentRequestVersion(
            metadataResponseWithLeader(List.of(nodeId0, nodeId1), nodeId1, validLeaderEpoch), false, time.milliseconds());
        assertEquals(nodeId1, metadata.fetch().leaderFor(tp0));

        // tp0 is re-seeded from the metadata, so the next fetch for it goes to node 1 with a new share session.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(nodeId1.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));
        Map<Integer, ShareFetchRequestData> requestsByNode = new HashMap<>();
        pollResult.unsentRequests.forEach(unsentRequest ->
            requestsByNode.put(unsentRequest.node().get().id(), ((ShareFetchRequest.Builder) unsentRequest.requestBuilder()).data()));
        ShareFetchRequestData node1Request = requestsByNode.get(nodeId1.id());
        assertNotNull(node1Request);
        assertEquals(ShareRequestMetadata.INITIAL_EPOCH, node1Request.shareSessionEpoch());
        assertNotNull(node1Request.topics().find(tip0.topicId()));
        assertNotNull(node1Request.topics().find(tip0.topicId()).partitions().find(tip0.partition()));
    }

    /**
     * When a broker is fenced, it is dropped from the {@code brokers} list of subsequent MetadataResponses,
     * so {@code Cluster.nodeById()} returns null for it, while leadership for its partitions fails over to
     * another live broker. This test checks that once the cached leader node disappears from the metadata
     * and leadership moves to a still-present node, the next fetch is routed to the new leader.
     */
    @Test
    public void testShareFetchRecoversWhenCachedLeaderNodeDisappearsFromMetadata() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0));

        // Two-node cluster; tp0 is led by node 0, so the share session and the cached leader are on node 0.
        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        assertEquals(nodeId0, metadata.fetch().leaderFor(tp0));

        // Establish the share session and cache the leader (node 0) for tp0.
        assertEquals(1, sendFetches());
        client.prepareResponseFrom(
                ShareFetchResponse.of(Errors.NONE, 0,
                        buildPartitionDataMap(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE),
                        List.of(), 0),
                nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
        fetchRecords();

        // Node 0 is fenced: it disappears from the brokers list of the next MetadataResponse and leadership
        // for tp0 fails over to node 1 with a higher leader epoch.
        MetadataResponse.PartitionMetadata tp0Metadata = new MetadataResponse.PartitionMetadata(
                Errors.NONE, tp0, Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1),
                List.of(nodeId1.id()), List.of(nodeId1.id()), List.of());
        MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(
                Errors.NONE, topicName, topicId, false, List.of(tp0Metadata),
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
        MetadataResponse metadataWithoutNode0 = RequestTestUtils.metadataResponse(
                List.of(nodeId1), "kafka-cluster", 1, List.of(topicMetadata));
        metadata.updateWithCurrentRequestVersion(metadataWithoutNode0, false, time.milliseconds());

        // Node 0 is gone from the cluster; node 1 is now the leader for tp0.
        assertNull(metadata.fetch().nodeById(0));
        assertEquals(nodeId1, metadata.fetch().leaderFor(tp0));

        // The next fetch should be routed to the new leader (node 1), not silently skipped.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size(),
                "Expected a fetch to the new leader after the cached leader node disappeared from metadata");
        assertEquals(nodeId1, pollResult.unsentRequests.get(0).node().get());
        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
        assertEquals(1, builder.data().topics().size());
        assertEquals(tip0.topicId(), builder.data().topics().stream().findFirst().get().topicId());
    }

    /**
     * When the node backing a share session disappears from the cluster metadata, the stale session handler
     * must be removed and any acknowledgements that were queued to be piggybacked on a fetch to that node
     * must be failed with NETWORK_EXCEPTION.
     */
    @Test
    public void testSessionHandlerRemovedAndPiggybackAcksFailedWhenNodeDisappears() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0));

        // Two-node cluster; tp0 is led by node 0, so the share session and the cached leader are on node 0.
        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        assertEquals(nodeId0, metadata.fetch().leaderFor(tp0));

        // Establish the share session on node 0 and fetch records.
        assertEquals(1, sendFetches());
        client.prepareResponseFrom(
                ShareFetchResponse.of(Errors.NONE, 0,
                        buildPartitionDataMap(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE),
                        List.of(), 0),
                nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
        fetchRecords();
        assertNotNull(shareConsumeRequestManager.sessionHandler(nodeId0.id()));

        // Queue acknowledgements to be piggybacked on the next fetch to node 0.
        Acknowledgements acknowledgements = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        // Node 0 is fenced: it disappears from the metadata and leadership for tp0 fails over to node 1.
        MetadataResponse.PartitionMetadata tp0Metadata = new MetadataResponse.PartitionMetadata(
                Errors.NONE, tp0, Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1),
                List.of(nodeId1.id()), List.of(nodeId1.id()), List.of());
        MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(
                Errors.NONE, topicName, topicId, false, List.of(tp0Metadata),
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
        MetadataResponse metadataWithoutNode0 = RequestTestUtils.metadataResponse(
                List.of(nodeId1), "kafka-cluster", 1, List.of(topicMetadata));
        metadata.updateWithCurrentRequestVersion(metadataWithoutNode0, false, time.milliseconds());
        assertNull(metadata.fetch().nodeById(0));

        // The next poll re-routes the fetch to node 1, removes the stale session handler for node 0, and fails
        // the piggyback acknowledgements that could no longer be sent to node 0.
        assertEquals(1, sendFetches());
        assertNull(shareConsumeRequestManager.sessionHandler(nodeId0.id()),
                "Session handler for the disappeared node should have been removed");
        assertNotNull(shareConsumeRequestManager.sessionHandler(nodeId1.id()));

        assertEquals(1, completedAcknowledgements.size());
        assertEquals(acknowledgements, completedAcknowledgements.get(0).get(tip0));
        assertInstanceOf(NetworkException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
    }

    private MetadataResponse metadataResponseWithLeader(List<Node> nodes, Node leader, int leaderEpoch) {
        MetadataResponse.PartitionMetadata tp0Metadata = new MetadataResponse.PartitionMetadata(
                Errors.NONE, tp0, Optional.of(leader.id()), Optional.of(leaderEpoch),
                nodes.stream().map(Node::id).collect(Collectors.toList()),
                nodes.stream().map(Node::id).collect(Collectors.toList()), List.of());
        MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(
                Errors.NONE, topicName, topicId, false, List.of(tp0Metadata),
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
        return RequestTestUtils.metadataResponse(nodes, "kafka-cluster", 1, List.of(topicMetadata));
    }

    /**
     * The cached leader is only redirected by the broker if the broker is reachable. If the leader changes while the
     * cached leader is unreachable, a metadata refresh is the only way to learn about the new leader. When the metadata
     * holds a newer leader epoch than the cached leader, the cached leader must be replaced.
     */
    @Test
    public void testShareFetchFollowsNewerLeaderEpochInMetadata() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0));

        // tp0's leader is node0.
        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 1),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        assertEquals(nodeId0, metadata.fetch().leaderFor(tp0));

        // Establish the share session on node0 and cache the leader (node0, validLeaderEpoch).
        assertEquals(1, sendFetches());
        client.prepareResponseFrom(
                ShareFetchResponse.of(Errors.NONE, 0,
                        buildPartitionDataMap(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE),
                        List.of(), 0),
                nodeId0);
        networkClientDelegate.poll(time.timer(0));
        fetchRecords();
        assertEquals(nodeId0.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));

        // A metadata refresh shows that leadership moved to node1 with a newer epoch. Node0 is still in the cluster,
        // so this is not the node-disappeared case.
        metadata.updateWithCurrentRequestVersion(
                metadataResponseWithLeader(List.of(nodeId0, nodeId1), nodeId1, validLeaderEpoch + 1), false, time.milliseconds());
        assertEquals(nodeId1, metadata.fetch().leaderFor(tp0));

        // The next poll fetches tp0 from node1, and also sends a request to node0 which removes tp0 from the
        // share session on the former leader.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(nodeId1.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));
        assertEquals(2, pollResult.unsentRequests.size());
        Map<Integer, ShareFetchRequestData> requestsByNode = new HashMap<>();
        pollResult.unsentRequests.forEach(unsentRequest ->
            requestsByNode.put(unsentRequest.node().get().id(), ((ShareFetchRequest.Builder) unsentRequest.requestBuilder()).data()));
        assertEquals(Set.of(nodeId0.id(), nodeId1.id()), requestsByNode.keySet());

        ShareFetchRequestData node1Request = requestsByNode.get(nodeId1.id());
        assertEquals(1, node1Request.topics().size());
        assertNotNull(node1Request.topics().find(tip0.topicId()));
        assertNotNull(node1Request.topics().find(tip0.topicId()).partitions().find(tip0.partition()));

        ShareFetchRequestData node0Request = requestsByNode.get(nodeId0.id());
        assertTrue(node0Request.topics().isEmpty());
        assertEquals(1, node0Request.forgottenTopicsData().size());
        assertEquals(tip0.topicId(), node0Request.forgottenTopicsData().get(0).topicId());
        assertEquals(List.of(tip0.partition()), node0Request.forgottenTopicsData().get(0).partitions());
    }

    /**
     * Metadata accepts a refresh at an unchanged leader epoch, but the cached leader is only replaced from metadata
     * when the epoch is strictly newer.
     */
    @Test
    public void testShareFetchIgnoresMetadataLeaderChangeWithUnchangedEpoch() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 1),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        assertEquals(nodeId0, metadata.fetch().leaderFor(tp0));

        assertEquals(1, sendFetches());
        client.prepareResponseFrom(
                ShareFetchResponse.of(Errors.NONE, 0,
                        buildPartitionDataMap(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE),
                        List.of(), 0),
                nodeId0);
        networkClientDelegate.poll(time.timer(0));
        fetchRecords();
        assertEquals(nodeId0.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));

        // A metadata refresh names node1 as leader, but at the same leader epoch.
        metadata.updateWithCurrentRequestVersion(
                metadataResponseWithLeader(List.of(nodeId0, nodeId1), nodeId1, validLeaderEpoch), false, time.milliseconds());
        assertEquals(nodeId1, metadata.fetch().leaderFor(tp0));

        // The cached leader is unchanged, so the next fetch still goes only to node0.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(nodeId0.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));
        assertEquals(1, pollResult.unsentRequests.size());
        assertEquals(nodeId0, pollResult.unsentRequests.get(0).node().get());
    }

    /**
     * A ShareFetch partition error which says the cached leader is wrong, but which carries no new leader
     * information, must request a metadata refresh in the request manager itself. The fetch collector also requests
     * one, but that only happens when the application thread next polls, which could be much later.
     */
    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"NOT_LEADER_OR_FOLLOWER", "FENCED_LEADER_EPOCH", "UNKNOWN_TOPIC_OR_PARTITION", "UNKNOWN_TOPIC_ID"})
    public void testShareFetchPartitionErrorWithoutLeaderInfoRequestsMetadataUpdate(Errors error) {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName, 1),
                        tp -> validLeaderEpoch, topicIds, false));
        assertFalse(metadata.updateRequested());

        assertEquals(1, sendFetches());
        // The broker sets the current leader to -1/-1 when it does not know the new leader.
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(error.code())
                .setCurrentLeader(new ShareFetchResponseData.LeaderIdAndEpoch().setLeaderId(-1).setLeaderEpoch(-1)));
        client.prepareResponse(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0));
        networkClientDelegate.poll(time.timer(0));

        // The refresh is requested before the fetch collector has seen the error.
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
        assertTrue(metadata.updateRequested());
    }

    /**
     * A ShareAcknowledge partition error which says the cached leader is wrong, but which carries no new leader
     * information, must request a metadata refresh. Unlike ShareFetch, no fetch collector sees these errors, so
     * without this the stale leader would persist until the periodic metadata refresh.
     */
    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"NOT_LEADER_OR_FOLLOWER", "FENCED_LEADER_EPOCH", "UNKNOWN_TOPIC_OR_PARTITION", "UNKNOWN_TOPIC_ID"})
    public void testShareAcknowledgePartitionErrorWithoutLeaderInfoRequestsMetadataUpdate(Errors error) {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName, 1),
                        tp -> validLeaderEpoch, topicIds, false));
        assertFalse(metadata.updateRequested());

        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);
        fetchRecords();

        Acknowledgements acknowledgements = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);
        CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future =
                shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                        calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));
        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        assertFalse(metadata.updateRequested());

        // The acknowledgements fail with no new leader information. The broker sets the current leader to -1/-1
        // when it does not know the new leader.
        client.prepareResponse(fullShareAcknowledgeResponse(tip0, error,
                new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(-1).setLeaderEpoch(-1), List.of()));
        networkClientDelegate.poll(time.timer(0));

        assertTrue(metadata.updateRequested());
        assertTrue(future.isDone());
        assertEquals(1, completedAcknowledgements.size());
        assertEquals(error.exception().getClass(), completedAcknowledgements.get(0).get(tip0).getAcknowledgeException().getClass());
    }

    /**
     * A redirect which the share session leader cache accepts can still be refused by the metadata, which only
     * applies a strictly newer leader epoch in accordance with KIP-951. When the new leader is a broker the metadata
     * has never seen, the cache names a node which is not in the cluster, so the next poll would fall back to the stale
     * leader in the metadata and be redirected again, indefinitely, until the periodic refresh. The request manager must
     * request a refresh as soon as the metadata refuses a redirect the cache accepted.
     */
    @Test
    public void testShareFetchRedirectRefusedByMetadataRequestsMetadataUpdate() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(Set.of(tp0));

        // A single-node cluster. Node 1 exists but the metadata has not yet seen it.
        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName, 1),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = new Node(1, "localhost", 9093);
        assertNull(metadata.fetch().nodeById(nodeId1.id()));
        assertFalse(metadata.updateRequested());

        // Establish the share session on node 0.
        assertEquals(1, sendFetches());
        assertEquals(nodeId0.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));

        // Node 0 redirects to node 1 without advancing the leader epoch. The cache accepts the redirect, but the
        // metadata refuses it, so it does not learn node 1's endpoint from the response.
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                .setCurrentLeader(new ShareFetchResponseData.LeaderIdAndEpoch()
                    .setLeaderId(nodeId1.id())
                    .setLeaderEpoch(validLeaderEpoch)));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(nodeId1), 0), nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(nodeId1.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));
        assertEquals(nodeId0, metadata.fetch().leaderFor(tp0));
        assertNull(metadata.fetch().nodeById(nodeId1.id()));

        // The cache and the metadata disagree, so a refresh is requested before the fetch collector runs.
        assertTrue(metadata.updateRequested());
        fetchRecords();

        // The refresh arrives, naming node 1 as the leader at the same epoch and including node 1 in the cluster.
        metadata.updateWithCurrentRequestVersion(
            metadataResponseWithLeader(List.of(nodeId0, nodeId1), nodeId1, validLeaderEpoch), false, time.milliseconds());
        assertEquals(nodeId1, metadata.fetch().leaderFor(tp0));

        // The next poll fetches tp0 from node 1 and removes it from the share session on node 0.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(nodeId1.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));
        assertEquals(2, pollResult.unsentRequests.size());
        Map<Integer, ShareFetchRequestData> requestsByNode = new HashMap<>();
        pollResult.unsentRequests.forEach(unsentRequest ->
            requestsByNode.put(unsentRequest.node().get().id(), ((ShareFetchRequest.Builder) unsentRequest.requestBuilder()).data()));
        assertEquals(Set.of(nodeId0.id(), nodeId1.id()), requestsByNode.keySet());
        assertNotNull(requestsByNode.get(nodeId1.id()).topics().find(tip0.topicId()).partitions().find(tip0.partition()));
        assertTrue(requestsByNode.get(nodeId0.id()).topics().isEmpty());
        assertEquals(List.of(tip0.partition()), requestsByNode.get(nodeId0.id()).forgottenTopicsData().get(0).partitions());
    }

    /**
     * Records fetched from a node may still be buffered when that node disappears from the cluster metadata and
     * its session handler is removed. When the application later acknowledges those records, the acknowledgements
     * are queued for the vanished node. They can never be sent, so they must be failed rather than left pending forever.
     */
    @Test
    public void testPiggybackAcksQueuedAfterNodeDisappearsAreFailed() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        // Establish the share session on node 0 and fetch records.
        assertEquals(1, sendFetches());
        client.prepareResponseFrom(
                ShareFetchResponse.of(Errors.NONE, 0,
                        buildPartitionDataMap(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE),
                        List.of(), 0),
                nodeId0);
        networkClientDelegate.poll(time.timer(0));
        fetchRecords();
        assertNotNull(shareConsumeRequestManager.sessionHandler(nodeId0.id()));

        // Node 0 is fenced: it disappears from the metadata and leadership for tp0 fails over to node 1.
        MetadataResponse.PartitionMetadata tp0Metadata = new MetadataResponse.PartitionMetadata(
                Errors.NONE, tp0, Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1),
                List.of(nodeId1.id()), List.of(nodeId1.id()), List.of());
        MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(
                Errors.NONE, topicName, topicId, false, List.of(tp0Metadata),
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
        MetadataResponse metadataWithoutNode0 = RequestTestUtils.metadataResponse(
                List.of(nodeId1), "kafka-cluster", 1, List.of(topicMetadata));
        metadata.updateWithCurrentRequestVersion(metadataWithoutNode0, false, time.milliseconds());
        assertNull(metadata.fetch().nodeById(0));

        // The next poll re-routes the fetch to node 1 and removes the stale session handler for node 0.
        assertEquals(1, sendFetches());
        assertNull(shareConsumeRequestManager.sessionHandler(nodeId0.id()));
        assertEquals(0, completedAcknowledgements.size());

        // The application now acknowledges the records it fetched from node 0. There is no session handler to
        // send them on, and leadership has moved to node 1, so they must be failed with NOT_LEADER_OR_FOLLOWER.
        Acknowledgements acknowledgements = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        // Node 1 has a request in flight, so no new fetch is sent, but the orphaned acknowledgements must still fail.
        assertEquals(0, sendFetches());
        assertEquals(1, completedAcknowledgements.size());
        assertEquals(acknowledgements, completedAcknowledgements.get(0).get(tip0));
        assertInstanceOf(NotLeaderOrFollowerException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
    }

    /**
     * An acknowledge request state is created for the node that led the partition at the time of the commit.
     * If that node then disappears from the cluster metadata before the request is sent, the acknowledgements must be failed with
     * NOT_LEADER_OR_FOLLOWER.
     */
    @Test
    public void testAcknowledgeRequestFailedWhenNodeDisappearsBeforeSend() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(List.of(tp0));

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);

        // Establish the share session on node 0 and fetch records.
        assertEquals(1, sendFetches());
        client.prepareResponseFrom(
                ShareFetchResponse.of(Errors.NONE, 0,
                        buildPartitionDataMap(tip0, records, acquiredRecords, Errors.NONE, Errors.NONE),
                        List.of(), 0),
                nodeId0);
        networkClientDelegate.poll(time.timer(0));
        fetchRecords();

        // commitSync enqueues an acknowledge request state for node 0, which is still the leader at this point.
        Acknowledgements acknowledgements = getAcknowledgements(1,
                AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);
        CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future =
                shareConsumeRequestManager.commitSync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                        calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));
        assertFalse(future.isDone());

        // Node 0 is fenced: it disappears from the metadata and leadership for tp0 fails over to node 1.
        MetadataResponse.PartitionMetadata tp0Metadata = new MetadataResponse.PartitionMetadata(
                Errors.NONE, tp0, Optional.of(nodeId1.id()), Optional.of(validLeaderEpoch + 1),
                List.of(nodeId1.id()), List.of(nodeId1.id()), List.of());
        MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(
                Errors.NONE, topicName, topicId, false, List.of(tp0Metadata),
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
        MetadataResponse metadataWithoutNode0 = RequestTestUtils.metadataResponse(
                List.of(nodeId1), "kafka-cluster", 1, List.of(topicMetadata));
        metadata.updateWithCurrentRequestVersion(metadataWithoutNode0, false, time.milliseconds());
        assertNull(metadata.fetch().nodeById(0));

        // No request is sent to the vanished node, and the commit completes with NOT_LEADER_OR_FOLLOWER
        // rather than hanging.
        assertEquals(0, shareConsumeRequestManager.sendAcknowledgements());
        assertTrue(future.isDone());
        assertEquals(1, completedAcknowledgements.size());
        assertEquals(acknowledgements, completedAcknowledgements.get(0).get(tip0));
        assertInstanceOf(NotLeaderOrFollowerException.class, completedAcknowledgements.get(0).get(tip0).getAcknowledgeException());
    }

    /**
     * When a topic is deleted and recreated, its topic ID changes. The request manager caches the mapping
     * from topic-partition to topic ID (and leader). A partition-level UNKNOWN_TOPIC_ID error in a ShareFetch
     * response must clear that cached mapping so that, once the metadata reflects the recreated topic, the stale
     * topic ID is forgotten from the share session and the new one is fetched.
     */
    @Test
    public void testShareFetchResponseWithUnknownTopicIdRefreshesTopicIdOnRecreation() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));

        // Establish the share session and cache the topic ID and leader for tp0.
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);
        fetchRecords();

        // A subsequent fetch returns a partition-level UNKNOWN_TOPIC_ID error, which causes the request
        // manager to forget the cached topic ID and leader for the partition.
        assertEquals(1, sendFetches());
        client.prepareResponse(fullShareFetchResponse(tip0, records, emptyAcquiredRecords, Errors.UNKNOWN_TOPIC_ID));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
        fetchRecords();

        // The topic is recreated with a new topic ID.
        Uuid recreatedTopicId = Uuid.randomUuid();
        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, Map.of(topicName, recreatedTopicId), false));

        // The next ShareFetch fetches the recreated topic ID and forgets the stale one from the share session.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());
        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();

        // The recreated topic ID is fetched.
        assertEquals(1, builder.data().topics().size());
        assertEquals(recreatedTopicId, builder.data().topics().stream().findFirst().get().topicId());

        // The stale topic ID is forgotten.
        assertEquals(1, builder.data().forgottenTopicsData().size());
        assertEquals(topicId, builder.data().forgottenTopicsData().get(0).topicId());
        assertEquals(1, builder.data().forgottenTopicsData().get(0).partitions().size());
        assertEquals(0, builder.data().forgottenTopicsData().get(0).partitions().get(0));
    }

    /**
     * As {@link #testShareFetchResponseWithUnknownTopicIdRefreshesTopicIdOnRecreation()} but the UNKNOWN_TOPIC_ID
     * error arrives in a ShareAcknowledge response rather than a ShareFetch response.
     */
    @Test
    public void testShareAcknowledgeResponseWithUnknownTopicIdRefreshesTopicIdOnRecreation() {
        buildRequestManager();
        shareConsumeRequestManager.setAcknowledgementCommitCallbackRegistered(true);

        assignFromSubscribed(Set.of(tp0));

        // Establish the share session and cache the topic ID and leader for tp0.
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);
        fetchRecords();

        // Acknowledge some records and receive a partition-level UNKNOWN_TOPIC_ID error, which causes the request
        // manager to forget the cached topic ID and leader for the partition.
        Acknowledgements acknowledgements = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.REJECT);
        shareConsumeRequestManager.commitAsync(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)),
                calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));

        assertEquals(1, shareConsumeRequestManager.sendAcknowledgements());
        client.prepareResponse(fullShareAcknowledgeResponse(tip0, Errors.UNKNOWN_TOPIC_ID));
        networkClientDelegate.poll(time.timer(0));

        // The acknowledgements are completed with the error rather than retried.
        assertEquals(3, completedAcknowledgements.get(0).get(tip0).size());

        // The topic is recreated with a new topic ID.
        Uuid recreatedTopicId = Uuid.randomUuid();
        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, Map.of(topicName, recreatedTopicId), false));

        // The next ShareFetch fetches the recreated topic ID and forgets the stale one from the share session.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());
        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();

        assertEquals(1, builder.data().topics().size());
        assertEquals(recreatedTopicId, builder.data().topics().stream().findFirst().get().topicId());

        assertEquals(1, builder.data().forgottenTopicsData().size());
        assertEquals(topicId, builder.data().forgottenTopicsData().get(0).topicId());
        assertEquals(1, builder.data().forgottenTopicsData().get(0).partitions().size());
        assertEquals(0, builder.data().forgottenTopicsData().get(0).partitions().get(0));
    }

    /**
     * The cached leader for a partition in the share session should only be replaced when the ShareFetch response
     * carries a newer leader epoch, following the same rules as {@link Metadata#updateLastSeenEpochIfNewer}. A stale
     * leader with an older epoch must not overwrite the cached leader, so subsequent fetches continue to go to the
     * node with the newest known leader epoch.
     */
    @Test
    public void testStaleLeaderEpochDoesNotDowngradeShareSessionLeader() {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(Set.of(tp0));

        // tp0's leader is node0 with a high leader epoch.
        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 1),
                        tp -> validLeaderEpoch + 5, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        assertEquals(nodeId0, metadata.fetch().leaderFor(tp0));

        // The first fetch goes to node0 and caches the leader (node0, epoch + 5).
        assertEquals(1, sendFetches());
        assertEquals(nodeId0, metadata.fetch().leaderFor(tp0));

        // node0 responds with a leadership error naming node1 as the new leader, but with an older leader epoch.
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
                new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(tip0.topicPartition().partition())
                        .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                        .setCurrentLeader(new ShareFetchResponseData.LeaderIdAndEpoch()
                                .setLeaderId(nodeId1.id())
                                .setLeaderEpoch(validLeaderEpoch + 2)));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(nodeId1), 0), nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        // The cache rejected the stale redirect, so the request manager has no disagreement with the metadata to
        // resolve and does not request a refresh. The share fetch collector requests a metadata refresh when it
        // sees the error.
        assertFalse(metadata.updateRequested());
        fetchRecords();

        // The stale leader must not have replaced the cached leader, so the next fetch still goes only to node0.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());
        assertEquals(nodeId0, pollResult.unsentRequests.get(0).node().get());
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"NOT_LEADER_OR_FOLLOWER", "FENCED_LEADER_EPOCH"})
    public void testLeaderChangeWithUnchangedEpochUpdatesShareSessionLeader(Errors error) {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(Set.of(tp0));

        // tp0's leader is node0.
        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 1),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        assertEquals(nodeId0, metadata.fetch().leaderFor(tp0));

        // The first fetch goes to node0 and caches the leader (node0, validLeaderEpoch).
        assertEquals(1, sendFetches());
        assertEquals(nodeId0.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));

        // node0 responds with a leadership error naming node1 as the new leader, but the leader epoch has not
        // advanced. Some broker implementations change the leader without incrementing the leader epoch.
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(error.code())
                .setCurrentLeader(new ShareFetchResponseData.LeaderIdAndEpoch()
                    .setLeaderId(nodeId1.id())
                    .setLeaderEpoch(validLeaderEpoch)));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(nodeId1), 0), nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
        fetchRecords();

        // The redirect must be trusted even though the epoch is unchanged, so the cached leader is now node1.
        assertEquals(nodeId1.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));

        // The next poll fetches tp0 from node1, and also sends a request to node0 which removes tp0 from the
        // share session on the former leader.
        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(2, pollResult.unsentRequests.size());
        Map<Integer, ShareFetchRequestData> requestsByNode = new HashMap<>();
        pollResult.unsentRequests.forEach(unsentRequest ->
            requestsByNode.put(unsentRequest.node().get().id(), ((ShareFetchRequest.Builder) unsentRequest.requestBuilder()).data()));
        assertEquals(Set.of(nodeId0.id(), nodeId1.id()), requestsByNode.keySet());

        ShareFetchRequestData node1Request = requestsByNode.get(nodeId1.id());
        assertEquals(1, node1Request.topics().size());
        ShareFetchRequestData.FetchTopic node1Topic = node1Request.topics().find(tip0.topicId());
        assertNotNull(node1Topic);
        assertEquals(1, node1Topic.partitions().size());
        assertNotNull(node1Topic.partitions().find(tip0.partition()));

        ShareFetchRequestData node0Request = requestsByNode.get(nodeId0.id());
        assertTrue(node0Request.topics().isEmpty());
        assertEquals(1, node0Request.forgottenTopicsData().size());
        assertEquals(tip0.topicId(), node0Request.forgottenTopicsData().get(0).topicId());
        assertEquals(List.of(tip0.partition()), node0Request.forgottenTopicsData().get(0).partitions());

        // node1 serves the fetch successfully and node0 acknowledges the removal.
        networkClientDelegate.addAll(pollResult.unsentRequests);
        partitionData = buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE, Errors.NONE);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0), nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));
        assertEquals(1, partitionRecords.get(tp0).size());

        // The successful fetch leaves the cached leader on node1.
        assertEquals(nodeId1.id(), shareConsumeRequestManager.shareSessionNodeId(tip0));
    }

    @Test
    public void testFetchOneNodeAtATimeForRecordLimitMode() {
        // We will simulate two nodes, each with one partition. The first node will have more records
        buildRequestManager(ShareAcquireMode.RECORD_LIMIT);

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        Set<TopicPartition> partitions = new LinkedHashSet<>();
        partitions.add(tp0);
        partitions.add(tp1);
        subscriptions.assignFromSubscribed(partitions);

        client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                        tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        Node tp0Leader = metadata.fetch().leaderFor(tp0);
        Node tp1Leader = metadata.fetch().leaderFor(tp1);

        assertEquals(nodeId0, tp0Leader);
        assertEquals(nodeId1, tp1Leader);

        // The first poll sends ShareFetch to both nodes
        // - node 0 - fetching records from tp0
        // - node 1 - establishing the share session, but not fetching records
        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        // Prepare responses from both nodes.
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> partitionData = new LinkedHashMap<>();
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);
        partitionData = buildPartitionDataMap(tip0, records, ShareCompletedFetchTest.acquiredRecords(1L, 1), Errors.NONE, Errors.NONE);

        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0) && !partitionRecords.containsKey(tp1));

        List<ConsumerRecord<byte[], byte[]>> fetchedRecords = partitionRecords.get(tp0);
        assertEquals(1, fetchedRecords.size());

        Acknowledgements acknowledgements = Acknowledgements.empty();
        acknowledgements.add(1, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        // The second poll sends ShareFetch to both nodes
        // - node 0 - acknowledges records from tp0, but not fetching records
        // - node 1 - fetching records from tp1
        assertEquals(2, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        partitionData.clear();
        // Let's assume there are no records for tp1 to fetch.
        partitionData.put(tip1,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip1.topicPartition().partition())
                .setErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId1);

        partitionData.clear();
        partitionData.put(tip0,
            new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(tip0.topicPartition().partition())
                .setErrorCode(Errors.NONE.code())
                .setAcknowledgeErrorCode(Errors.NONE.code()));
        client.prepareResponseFrom(ShareFetchResponse.of(Errors.NONE, 0, partitionData, List.of(), 0), nodeId0);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        fetchRecords();

        // The third poll sends ShareFetch to only 1 node.
        // - node 0 - sends a share fetch to fetch records from tp0.
        // - node 1 - does not send, as we do not have any acks to send, it will wait until node0 has returned a response.
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
    }

    @Test
    public void testCloseInternalClosesShareFetchMetricsManager() {
        buildRequestManager();

        // Define all sensor names that should be created and removed
        String[] sensorNames = {
            "fetch-throttle-time",
            "bytes-fetched",
            "records-fetched",
            "fetch-latency",
            "sent-acknowledgements",
            "failed-acknowledgements"
        };

        // Verify that sensors exist before closing
        for (String sensorName : sensorNames) {
            assertNotNull(metrics.getSensor(sensorName),
                "Sensor " + sensorName + " should exist before closing");
        }

        // Close the request manager
        shareConsumeRequestManager.close();

        // Verify that all sensors are removed after closing
        for (String sensorName : sensorNames) {
            assertNull(metrics.getSensor(sensorName),
                "Sensor " + sensorName + " should be removed after closing");
        }
    }

    @Test
    public void testShareFetchWithRenewAcknowledgement() {
        buildRequestManager();

        assignFromSubscribed(Set.of(tp0));
        sendFetchAndVerifyResponse(records, acquiredRecords, Errors.NONE);

        Acknowledgements acknowledgements = getAcknowledgements(1,
            AcknowledgeType.RENEW, AcknowledgeType.RENEW, AcknowledgeType.RENEW);

        // Reading records from the share fetch buffer.
        fetchRecords();

        // Piggyback acknowledgements
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements)));

        NetworkClientDelegate.PollResult pollResult = shareConsumeRequestManager.sendFetchesReturnPollResult();
        assertEquals(1, pollResult.unsentRequests.size());
        ShareFetchRequest.Builder builder = (ShareFetchRequest.Builder) pollResult.unsentRequests.get(0).requestBuilder();
        assertTrue(builder.data().isRenewAck());

        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(3.0,
            metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());

        assertEquals(0, renewedRecords.size());

        client.prepareResponse(fullShareFetchResponse(tip0, MemoryRecords.EMPTY, List.of(), Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        assertEquals(3, renewedRecords.size());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> partitionRecords = fetchRecords();
        assertTrue(partitionRecords.isEmpty());

        Acknowledgements acknowledgements2 = getAcknowledgements(1,
            AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);
        shareConsumeRequestManager.fetch(Map.of(tip0, new NodeAcknowledgements(0, acknowledgements2)));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        client.prepareResponse(fullShareFetchResponse(tip0, records, acquiredRecords, Errors.NONE));
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());

        partitionRecords = fetchRecords();
        assertTrue(partitionRecords.containsKey(tp0));

        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());
        assertEquals(6.0,
            metrics.metrics().get(metrics.metricInstance(shareFetchMetricsRegistry.acknowledgementSendTotal)).metricValue());
    }

    /**
     * A commitSync() which carries RENEW acknowledgements for a partition on one node and ordinary acknowledgements
     * for a partition on another node. Whatever order the two nodes respond in, the application thread must be told
     * about the completed renewals so that the renewed records can be moved back to in-flight. No acknowledgement
     * commit callback is registered, so the only reason to raise the event is the renewals.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCommitSyncRenewOnOneNodeAcceptOnAnother(boolean renewNodeRespondsLast) {
        buildRequestManager();

        subscriptions.subscribeToShareGroup(Set.of(topicName));
        subscriptions.assignFromSubscribed(Set.of(tp0, tp1));
        client.updateMetadata(
            RequestTestUtils.metadataUpdateWithIds(2, Map.of(topicName, 2),
                tp -> validLeaderEpoch, topicIds, false));
        Node nodeId0 = metadata.fetch().nodeById(0);
        Node nodeId1 = metadata.fetch().nodeById(1);
        assertEquals(nodeId0, metadata.fetch().leaderFor(tp0));
        assertEquals(nodeId1, metadata.fetch().leaderFor(tp1));

        // Fetch records from both partitions so that both nodes have established share sessions.
        assertEquals(2, sendFetches());
        client.prepareResponseFrom(fullShareFetchResponse(tip0, records, acquiredRecords, Errors.NONE), nodeId0);
        client.prepareResponseFrom(fullShareFetchResponse(tip1, records, acquiredRecords, Errors.NONE), nodeId1);
        networkClientDelegate.poll(time.timer(0));
        assertEquals(2, fetchRecords().size());

        // Renew the records from tp0 and accept the records from tp1 in a single commitSync().
        Acknowledgements renewAcks = getAcknowledgements(1, AcknowledgeType.RENEW, AcknowledgeType.RENEW, AcknowledgeType.RENEW);
        Acknowledgements acceptAcks = getAcknowledgements(1, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT, AcknowledgeType.ACCEPT);
        CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future = shareConsumeRequestManager.commitSync(
            Map.of(tip0, new NodeAcknowledgements(0, renewAcks), tip1, new NodeAcknowledgements(1, acceptAcks)),
            calculateDeadlineMs(time.timer(defaultApiTimeoutMs)));
        assertEquals(2, shareConsumeRequestManager.sendAcknowledgements());

        // Send both ShareAcknowledge requests, then answer them one at a time in the chosen order.
        networkClientDelegate.poll(time.timer(0));
        assertEquals(2, client.inFlightRequestCount());

        Node first = renewNodeRespondsLast ? nodeId1 : nodeId0;
        TopicIdPartition firstTip = renewNodeRespondsLast ? tip1 : tip0;
        Node last = renewNodeRespondsLast ? nodeId0 : nodeId1;
        TopicIdPartition lastTip = renewNodeRespondsLast ? tip0 : tip1;

        client.respondFrom(fullShareAcknowledgeResponse(firstTip, Errors.NONE), first);
        networkClientDelegate.poll(time.timer(0));
        assertFalse(future.isDone());

        client.respondFrom(fullShareAcknowledgeResponse(lastTip, Errors.NONE), last);
        networkClientDelegate.poll(time.timer(0));
        assertTrue(future.isDone());
        assertTrue(future.join().get(tip0).isCompleted());
        assertTrue(future.join().get(tip1).isCompleted());

        // The renewals for tp0 completed successfully, so the application thread must receive an event telling it
        // to move the renewed records back to in-flight, regardless of which node happened to respond last.
        assertEquals(Set.of(1L, 2L, 3L), renewedRecords,
            "renewed records were not reported to the application thread");
    }

    private ShareFetchResponse shareFetchResponseWithTopLevelError(TopicIdPartition tp, Errors error) {
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitions = Map.of(tp,
                new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(tp.topicPartition().partition())
                        .setErrorCode(error.code()));
        return ShareFetchResponse.of(error, 0, new LinkedHashMap<>(partitions), List.of(), 0);
    }

    private ShareFetchResponse fullShareFetchResponse(TopicIdPartition tp,
                                                      MemoryRecords records,
                                                      List<ShareFetchResponseData.AcquiredRecords> acquiredRecords,
                                                      Errors error) {
        return fullShareFetchResponse(tp, records, acquiredRecords, error, Errors.NONE);
    }

    private ShareFetchResponse fullShareFetchResponse(TopicIdPartition tp,
                                                      MemoryRecords records,
                                                      List<ShareFetchResponseData.AcquiredRecords> acquiredRecords,
                                                      Errors error,
                                                      Errors acknowledgeError) {
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> partitions = Map.of(tp,
                partitionDataForShareFetch(tp, records, acquiredRecords, error, acknowledgeError));
        return ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), List.of(), 0);
    }

    private ShareAcknowledgeResponse emptyShareAcknowledgeResponse() {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = Map.of();
        return ShareAcknowledgeResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), List.of(), 0);
    }

    private ShareAcknowledgeResponse shareAcknowledgeResponseWithTopLevelError(TopicIdPartition tp, Errors error) {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = Map.of(tp,
                partitionDataForShareAcknowledge(tp, Errors.NONE));
        return ShareAcknowledgeResponse.of(error, 0, new LinkedHashMap<>(partitions), List.of(), 0);
    }

    private ShareAcknowledgeResponse fullShareAcknowledgeResponse(TopicIdPartition tp, Errors error) {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = Map.of(tp,
                partitionDataForShareAcknowledge(tp, error));
        return ShareAcknowledgeResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), List.of(), 0);
    }

    private ShareAcknowledgeResponse fullShareAcknowledgeResponse(Map<TopicIdPartition, Errors> partitionErrorsMap) {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = new HashMap<>();
        partitionErrorsMap.forEach((tip, error) -> partitions.put(tip, partitionDataForShareAcknowledge(tip, error)));
        return ShareAcknowledgeResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), List.of(), 0);
    }

    private ShareAcknowledgeResponse fullShareAcknowledgeResponse(TopicIdPartition tp,
                                                                  Errors error,
                                                                  ShareAcknowledgeResponseData.LeaderIdAndEpoch currentLeader,
                                                                  List<Node> nodeEndpoints) {
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> partitions = Map.of(tp,
            partitionDataForShareAcknowledge(tp, error, currentLeader));
        return ShareAcknowledgeResponse.of(Errors.NONE, 0, new LinkedHashMap<>(partitions), nodeEndpoints, 0);
    }

    private ShareFetchResponseData.PartitionData partitionDataForShareFetch(TopicIdPartition tp,
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

    private ShareAcknowledgeResponseData.PartitionData partitionDataForShareAcknowledge(TopicIdPartition tp, Errors error) {
        return new ShareAcknowledgeResponseData.PartitionData()
                .setPartitionIndex(tp.topicPartition().partition())
                .setErrorCode(error.code());
    }

    private ShareAcknowledgeResponseData.PartitionData partitionDataForShareAcknowledge(TopicIdPartition tp,
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
        assertEquals(Map.of(), fetch.records(), reason);
        assertTrue(fetch.isEmpty(), reason);
    }

    private Acknowledgements getAcknowledgements(int startIndex, AcknowledgeType... acknowledgeTypes) {
        Acknowledgements acknowledgements = Acknowledgements.empty();
        int index = startIndex;
        for (AcknowledgeType type : acknowledgeTypes) {
            acknowledgements.add(index++, type);
        }
        return acknowledgements;
    }

    private <K, V> Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchRecords() {
        ShareFetch<K, V> fetch = collectFetch();
        if (fetch.isEmpty()) {
            return Map.of();
        }
        return fetch.records();
    }

    @SuppressWarnings("unchecked")
    private <K, V> ShareFetch<K, V> collectFetch() {
        return (ShareFetch<K, V>) shareConsumeRequestManager.collectFetch();
    }

    private void buildRequestManager() {
        buildRequestManager(new ByteArrayDeserializer(), new ByteArrayDeserializer(), ShareAcquireMode.BATCH_OPTIMIZED);
    }

    private void buildRequestManager(ShareAcquireMode shareAcquireMode) {
        buildRequestManager(new ByteArrayDeserializer(), new ByteArrayDeserializer(), shareAcquireMode);
    }

    private <K, V> void buildRequestManager(Deserializer<K> keyDeserializer,
                                            Deserializer<V> valueDeserializer,
                                            ShareAcquireMode shareAcquireMode) {
        buildRequestManager(new MetricConfig(), keyDeserializer, valueDeserializer, Uuid.randomUuid().toString(), shareAcquireMode);
    }

    private <K, V> void buildRequestManager(MetricConfig metricConfig,
                                            Deserializer<K> keyDeserializer,
                                            Deserializer<V> valueDeserializer,
                                            String memberId,
                                            ShareAcquireMode shareAcquireMode) {
        LogContext logContext = new LogContext();
        SubscriptionState subscriptionState = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        buildRequestManager(metricConfig, keyDeserializer, valueDeserializer,
                subscriptionState, logContext, memberId, shareAcquireMode);
    }

    private <K, V> void buildRequestManager(MetricConfig metricConfig,
                                            Deserializer<K> keyDeserializer,
                                            Deserializer<V> valueDeserializer,
                                            SubscriptionState subscriptionState,
                                            LogContext logContext,
                                            String memberId,
                                                                                   ShareAcquireMode shareAcquireMode) {
        buildDependencies(metricConfig, subscriptionState, logContext);
        Deserializers<K, V> deserializers = new Deserializers<>(keyDeserializer, valueDeserializer, metrics);
        int maxWaitMs = 0;
        int maxBytes = Integer.MAX_VALUE;
        int fetchSize = 1000;
        int minBytes = 1;
        ShareFetchConfig shareFetchConfig = new ShareFetchConfig(
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                Integer.MAX_VALUE,
                true, // check crc
                CommonClientConfigs.DEFAULT_CLIENT_RACK,
                IsolationLevel.READ_UNCOMMITTED,
                shareAcquireMode);
        ShareFetchCollector<K, V> shareFetchCollector = new ShareFetchCollector<>(logContext,
                metadata,
                subscriptions,
                shareFetchConfig,
                deserializers);
        ShareAcknowledgementEventHandler acknowledgementEventHandler = new TestableShareAcknowledgementEventHandler(completedAcknowledgements, renewedRecords);
        shareConsumeRequestManager = spy(new TestableShareConsumeRequestManager<>(
                logContext,
                groupId,
                metadata,
                subscriptionState,
                shareFetchConfig,
                new ShareFetchBuffer(logContext),
                acknowledgementEventHandler,
                metricsManager,
                shareFetchCollector,
                memberId));
    }

    private void buildDependencies(MetricConfig metricConfig,
                                   SubscriptionState subscriptionState,
                                   LogContext logContext) {
        time = new MockTime(1, 0, 0);
        subscriptions = subscriptionState;
        metadata = new ShareConsumerMetadata(0, 0, Long.MAX_VALUE, false,
                subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        metrics = new Metrics(metricConfig, time);
        shareFetchMetricsRegistry = new ShareFetchMetricsRegistry(metricConfig.tags().keySet(), "consumer-share" + groupId);
        metricsManager = spy(new ShareFetchMetricsManager(metrics, shareFetchMetricsRegistry));

        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs));
        properties.setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs));
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        ConsumerConfig config = new ConsumerConfig(properties);
        networkClientDelegate = spy(new TestableNetworkClientDelegate(
            time, config, logContext, client, metadata,
            new BackgroundEventHandler(new LinkedBlockingQueue<>(), time, mock(AsyncConsumerMetrics.class)), false));
    }

    private class TestableShareConsumeRequestManager<K, V> extends ShareConsumeRequestManager {

        private final ShareFetchCollector<K, V> shareFetchCollector;

        public TestableShareConsumeRequestManager(LogContext logContext,
                                                  String groupId,
                                                  ShareConsumerMetadata metadata,
                                                  SubscriptionState subscriptions,
                                                  ShareFetchConfig shareFetchConfig,
                                                  ShareFetchBuffer shareFetchBuffer,
                                                  ShareAcknowledgementEventHandler acknowledgementEventHandler,
                                                  ShareFetchMetricsManager metricsManager,
                                                  ShareFetchCollector<K, V> fetchCollector,
                                                  String memberId) {
            super(time, logContext, groupId, metadata, subscriptions, shareFetchConfig, shareFetchBuffer,
                acknowledgementEventHandler, metricsManager, retryBackoffMs, 1000);
            this.shareFetchCollector = fetchCollector;
            if (memberId != null) {
                onMemberEpochUpdated(Optional.empty(), memberId);
            }
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

        private NetworkClientDelegate.PollResult sendFetchesReturnPollResult() {
            fetch(new HashMap<>());
            NetworkClientDelegate.PollResult pollResult = poll(time.milliseconds());
            networkClientDelegate.addAll(pollResult.unsentRequests);
            return pollResult;
        }

        private int sendAcknowledgements() {
            NetworkClientDelegate.PollResult pollResult = poll(time.milliseconds());
            networkClientDelegate.addAll(pollResult.unsentRequests);
            return pollResult.unsentRequests.size();
        }

        public ResultHandler buildResultHandler(final AtomicInteger remainingResults,
                                                final Optional<CompletableFuture<Map<TopicIdPartition, Acknowledgements>>> future) {
            return new ResultHandler(remainingResults, future);
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
        protected void checkDisconnects(final long currentTimeMs, boolean onClose) {
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

    private static class TestableShareAcknowledgementEventHandler extends ShareAcknowledgementEventHandler {
        List<Map<TopicIdPartition, Acknowledgements>> completedAcknowledgements;
        Set<Long> renewedRecords;

        public TestableShareAcknowledgementEventHandler(List<Map<TopicIdPartition, Acknowledgements>> completedAcknowledgements, Set<Long> renewedRecords) {
            super(new LinkedBlockingQueue<>());
            this.completedAcknowledgements = completedAcknowledgements;
            this.renewedRecords = renewedRecords;
        }

        public void add(ShareAcknowledgementEvent event) {
            completedAcknowledgements.add(event.acknowledgementsMap());
            if (event.checkForRenewAcknowledgements()) {
                event.acknowledgementsMap().values().forEach(acks ->
                    acks.getAcknowledgementsTypeMap().forEach((offset, ackType) -> renewedRecords.add(offset)));
            }
        }
    }

    private void sendFetchAndVerifyResponse(MemoryRecords records,
                                    List<ShareFetchResponseData.AcquiredRecords> acquiredRecords,
                                    Errors... error) {
        // normal fetch
        assertEquals(1, sendFetches());
        assertFalse(shareConsumeRequestManager.hasCompletedFetches());

        if (error.length > 1) {
            client.prepareResponse(fullShareFetchResponse(tip0, records, acquiredRecords, error[0], error[1]));
        } else {
            client.prepareResponse(fullShareFetchResponse(tip0, records, acquiredRecords, error[0]));
        }
        networkClientDelegate.poll(time.timer(0));
        assertTrue(shareConsumeRequestManager.hasCompletedFetches());
    }

    // Helper methods to reduce PartitionData creation boilerplate
    private LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> buildPartitionDataMap(
            TopicIdPartition tip, MemoryRecords records,
            List<ShareFetchResponseData.AcquiredRecords> acquiredRecords, Errors error, Errors ackError) {
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> map = new LinkedHashMap<>();
        map.put(tip, partitionDataForShareFetch(tip, records, acquiredRecords, error, ackError));
        return map;
    }
}
