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
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.AsyncCoordinatorTestUtils.MockCommitCallback;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.consumer.internals.AsyncCoordinatorTestUtils.groupCoordinatorResponse;
import static org.apache.kafka.clients.consumer.internals.AsyncCoordinatorTestUtils.heartbeatResponse;
import static org.apache.kafka.clients.consumer.internals.AsyncCoordinatorTestUtils.offsetCommitRequestMatcher;
import static org.apache.kafka.clients.consumer.internals.AsyncCoordinatorTestUtils.offsetCommitResponse;
import static org.apache.kafka.clients.consumer.internals.AsyncCoordinatorTestUtils.prepareOffsetCommitRequest;
import static org.apache.kafka.clients.consumer.internals.AsyncCoordinatorTestUtils.prepareOffsetCommitRequestDisconnect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultAsyncCoordinatorTest {
    private MockTime time;
    private final int rebalanceTimeoutMs = 60000;
    private final int sessionTimeoutMs = 10000;
    private final int heartbeatIntervalMs = 5000;
    private final long retryBackoffMs = 100;
    private final int requestTimeoutMs = 30000;
    private final String topic1 = "test1";
    private final String topic2 = "test2";
    private final TopicPartition t1p = new TopicPartition(topic1, 0);
    private final TopicPartition t2p = new TopicPartition(topic2, 0);
    private final String groupId = "test-group";
    private final String consumerId = "consumer";

    private MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(1, new HashMap<String, Integer>() {
        {
            put(topic1, 1);
            put(topic2, 1);
        }
    });
    private Node node = metadataResponse.brokers().iterator().next();
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private MockClient client;
    private ConsumerNetworkClient consumerClient;
    private Metrics metrics;
    private MockRebalanceListener rebalanceListener;
    private MockCommitCallback mockOffsetCommitCallback;
    private GroupRebalanceConfig rebalanceConfig;
    private DefaultAsyncCoordinator coordinator;
    private RequestFuture<Void> commitFuture;
    private BlockingQueue<BackgroundEvent> bq;

    @BeforeEach
    public void setup() {
        LogContext logContext = new LogContext();
        this.time = new MockTime(0);
        this.subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        this.metadata = new ConsumerMetadata(
                0,
                Long.MAX_VALUE,
                false,
                false,
                subscriptions,
                logContext,
                new ClusterResourceListeners());
        this.client = new MockClient(time, metadata);
        this.client.updateMetadata(metadataResponse);
        this.consumerClient = new ConsumerNetworkClient(
                logContext,
                client,
                metadata,
                time,
                100,
                requestTimeoutMs,
                Integer.MAX_VALUE);
        this.metrics = new Metrics(time);
        this.rebalanceListener = new MockRebalanceListener();
        this.mockOffsetCommitCallback = new MockCommitCallback();
        this.rebalanceConfig = buildRebalanceConfig(Optional.empty());
        this.bq = new LinkedBlockingQueue<>();
        this.coordinator = buildCoordinator(
                rebalanceConfig,
                metrics,
                subscriptions,
                bq);
        this.commitFuture = new RequestFuture<>();
    }

    @AfterEach
    public void teardown() {
        this.metrics.close();
        this.coordinator.close(time.timer(0));
    }

    @Test
    public void testMetrics() {
        assertNotNull(getMetric("commit-latency-avg"));
        assertNotNull(getMetric("commit-latency-max"));
        assertNotNull(getMetric("commit-rate"));
        assertNotNull(getMetric("commit-total"));
        assertNotNull(getMetric("partition-revoked-latency-avg"));
        assertNotNull(getMetric("partition-revoked-latency-max"));
        assertNotNull(getMetric("partition-assigned-latency-avg"));
        assertNotNull(getMetric("partition-assigned-latency-max"));
        assertNotNull(getMetric("partition-lost-latency-avg"));
        assertNotNull(getMetric("partition-lost-latency-max"));
        assertNotNull(getMetric("assigned-partitions"));

        metrics.sensor("commit-latency").record(1.0d);
        metrics.sensor("commit-latency").record(6.0d);
        metrics.sensor("commit-latency").record(2.0d);

        assertEquals(3.0d, getMetric("commit-latency-avg").metricValue());
        assertEquals(6.0d, getMetric("commit-latency-max").metricValue());
        assertEquals(0.1d, getMetric("commit-rate").metricValue());
        assertEquals(3.0d, getMetric("commit-total").metricValue());

        metrics.sensor("partition-revoked-latency").record(1.0d);
        metrics.sensor("partition-revoked-latency").record(2.0d);
        metrics.sensor("partition-assigned-latency").record(1.0d);
        metrics.sensor("partition-assigned-latency").record(2.0d);
        metrics.sensor("partition-lost-latency").record(1.0d);
        metrics.sensor("partition-lost-latency").record(2.0d);

        assertEquals(1.5d, getMetric("partition-revoked-latency-avg").metricValue());
        assertEquals(2.0d, getMetric("partition-revoked-latency-max").metricValue());
        assertEquals(1.5d, getMetric("partition-assigned-latency-avg").metricValue());
        assertEquals(2.0d, getMetric("partition-assigned-latency-max").metricValue());
        assertEquals(1.5d, getMetric("partition-lost-latency-avg").metricValue());
        assertEquals(2.0d, getMetric("partition-lost-latency-max").metricValue());

        assertEquals(0.0d, getMetric("assigned-partitions").metricValue());
        subscriptions.assignFromUser(Collections.singleton(t1p));
        assertEquals(1.0d, getMetric("assigned-partitions").metricValue());
        subscriptions.assignFromUser(Utils.mkSet(t1p, t2p));
        assertEquals(2.0d, getMetric("assigned-partitions").metricValue());
    }

    private KafkaMetric getMetric(final String name) {
        return metrics.metrics().get(metrics.metricName(name, consumerId + groupId + "-coordinator-metrics"));
    }

    @Test
    public void testCommitOffsetMetadata() {
        subscriptions.assignFromUser(singleton(t1p));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE, client);

        Optional<OffsetCommitCallback> cb = Optional.of(new MockCommitCallback());
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p, new OffsetAndMetadata(100L, "hello"));
        RequestFuture<Void> requestFuture = new RequestFuture<>();
        coordinator.commitOffsets(offsets, cb, requestFuture);
        assertTrue(requestFuture.succeeded());
    }

    @Test
    public void testCommitOffsetRequestSyncWithFencedInstanceIdException() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.FENCED_INSTANCE_ID, client);

        Optional<OffsetCommitCallback> cb = Optional.of(new MockCommitCallback());
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p, new OffsetAndMetadata(100L, "hello"));
        RequestFuture<Void> requestFuture = new RequestFuture<>();
        coordinator.commitOffsets(offsets, cb, requestFuture);
        assertTrue(requestFuture.failed());
        assertTrue(requestFuture.exception() instanceof FencedInstanceIdException);
    }

    @Test
    public void testCommitOffsetNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // async commit with not coordinator
        Optional<OffsetCommitCallback> cb = Optional.of(new MockCommitCallback());

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NOT_COORDINATOR, client);
        RequestFuture<Void> requestFuture = new RequestFuture<>();
        coordinator.commitOffsets(singletonMap(t1p, new OffsetAndMetadata(100L)), cb, requestFuture);

        assertTrue(coordinator.coordinatorUnknown());
        assertTrue(requestFuture.failed());
        assertTrue(requestFuture.exception() instanceof NotCoordinatorException);
    }

    @Test
    public void testCommitOffsetDisconnected() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // async commit with coordinator disconnected
        Optional<OffsetCommitCallback> cb = Optional.of(new MockCommitCallback());
        prepareOffsetCommitRequestDisconnect(singletonMap(t1p, 100L), client);
        RequestFuture<Void> requestFuture = new RequestFuture<>();
        coordinator.commitOffsets(singletonMap(t1p, new OffsetAndMetadata(100L)), cb, requestFuture);

        assertTrue(coordinator.coordinatorUnknown());
        assertTrue(requestFuture.failed());
        assertTrue(requestFuture.exception() instanceof DisconnectException);
    }

    @Test
    public void testCommitOffsetWhenCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        RequestFuture<Void> commitFuture = new RequestFuture<>();
        Optional<OffsetCommitCallback> cb = Optional.of(new MockCommitCallback());
        // async commit with coordinator not available
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE, client);
        coordinator.commitOffsets(singletonMap(t1p, new OffsetAndMetadata(100L)), cb, commitFuture);

        assertTrue(coordinator.coordinatorUnknown());
        assertTrue(commitFuture.failed());
        System.out.println(commitFuture.exception());
        assertTrue(commitFuture.exception() instanceof CoordinatorNotAvailableException);
    }

    @Test
    public void testCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // COORDINATOR_NOT_AVAILABLE will mark coordinator as unknown
        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.COORDINATOR_NOT_AVAILABLE));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(time.timer(0));

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.exception(), future.exception());
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testRetryCommitUnknownTopicOrPartition() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        client.prepareResponse(offsetCommitResponse(singletonMap(t1p, Errors.UNKNOWN_TOPIC_OR_PARTITION)));

        RequestFuture<Void> commitFuture = new RequestFuture<>();
        Optional<OffsetCommitCallback> cb = Optional.of(new MockCommitCallback());
        coordinator.commitOffsets(singletonMap(t1p, new OffsetAndMetadata(100L, "metadata")), cb, commitFuture);
        assertTrue(commitFuture.failed());
        assertTrue(commitFuture.exception() instanceof UnknownTopicOrPartitionException);
    }

    @Test
    public void testCommitOffsetMetadataTooLarge() {
        // since offset metadata is provided by the user, we have to propagate the exception so they can handle it
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.OFFSET_METADATA_TOO_LARGE, client);
        RequestFuture<Void> commitFuture = new RequestFuture<>();
        Optional<OffsetCommitCallback> cb = Optional.of(new MockCommitCallback());
        coordinator.commitOffsets(singletonMap(t1p, new OffsetAndMetadata(100L, "metadata")), cb, commitFuture);
        assertTrue(commitFuture.failed());
        assertTrue(commitFuture.exception() instanceof OffsetMetadataTooLarge);
    }

    @Test
    public void testManyInFlightAsyncCommitsWithCoordinatorDisconnect() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        int numRequests = 1000;
        TopicPartition tp = new TopicPartition("foo", 0);
        final AtomicInteger responses = new AtomicInteger(0);
        List<RequestFuture<Void>> commitFutures = new ArrayList<>();

        for (int i = 0; i < numRequests; i++) {
            Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(tp, new OffsetAndMetadata(i));
            RequestFuture<Void> future = new RequestFuture<>();
            Optional<OffsetCommitCallback> cb = Optional.of(new MockCommitCallback());
            coordinator.commitOffsets(offsets, cb, future);
            future.addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                }

                @Override
                public void onFailure(RuntimeException e) {
                    responses.incrementAndGet();
                    assertTrue(e instanceof DisconnectException,
                            "Unexpected exception cause type: " + (e == null ? null :
                                    e.getClass()));
                }
            });
            commitFutures.add(future);
        }

        coordinator.markCoordinatorUnknown("test cause");
        consumerClient.pollNoWakeup();
        assertEquals(numRequests, responses.get());
    }

    @Test
    public void testCoordinatorUnknownInUnsentCallbacksAfterCoordinatorDead() {
        // When the coordinator is marked dead, all unsent or in-flight requests are cancelled
        // with a disconnect error. This test case ensures that the corresponding callbacks see
        // the coordinator as unknown which prevents additional retries to the same coordinator.

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        final AtomicBoolean asyncCallbackInvoked = new AtomicBoolean(false);

        OffsetCommitRequestData offsetCommitRequestData = new OffsetCommitRequestData()
                .setGroupId(groupId)
                .setTopics(Collections.singletonList(new
                                OffsetCommitRequestData.OffsetCommitRequestTopic()
                                .setName("foo")
                                .setPartitions(Collections.singletonList(
                                        new OffsetCommitRequestData.OffsetCommitRequestPartition()
                                                .setPartitionIndex(0)
                                                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                                .setCommittedMetadata("")
                                                .setCommittedOffset(13L)
                                                .setCommitTimestamp(0)
                                ))
                        )
                );

        consumerClient.send(coordinator.checkAndGetCoordinator(), new OffsetCommitRequest.Builder(offsetCommitRequestData))
                .compose(new RequestFutureAdapter<ClientResponse, Object>() {
                    @Override
                    public void onSuccess(ClientResponse value, RequestFuture<Object> future) {}

                    @Override
                    public void onFailure(RuntimeException e, RequestFuture<Object> future) {
                        assertTrue(e instanceof DisconnectException, "Unexpected exception type: " + e.getClass());
                        assertTrue(coordinator.coordinatorUnknown());
                        asyncCallbackInvoked.set(true);
                    }
                });

        coordinator.markCoordinatorUnknown("test cause");
        consumerClient.pollNoWakeup();
        assertTrue(asyncCallbackInvoked.get());
    }

    @Test
    public void testNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // not_coordinator will mark coordinator as unknown
        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.NOT_COORDINATOR));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(time.timer(0));

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertEquals(Errors.NOT_COORDINATOR.exception(), future.exception());
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testCoordinatorDisconnectAfterNotCoordinatorError() {
        testInFlightRequestsFailedAfterCoordinatorMarkedDead(Errors.NOT_COORDINATOR);
    }

    @Test
    public void testCoordinatorDisconnectAfterCoordinatorNotAvailableError() {
        testInFlightRequestsFailedAfterCoordinatorMarkedDead(Errors.COORDINATOR_NOT_AVAILABLE);
    }

    private void testInFlightRequestsFailedAfterCoordinatorMarkedDead(Errors error) {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));
        assertTrue(coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE)));

        // Send two async commits and fail the first one with an error.
        // This should cause a coordinator disconnect which will cancel the second request.

        RequestFuture<Void> firstFuture = new RequestFuture<>();
        RequestFuture<Void> secondFuture = new RequestFuture<>();

        coordinator.commitOffsets(singletonMap(t1p, new OffsetAndMetadata(100L)), Optional.of(new MockCommitCallback()), firstFuture);
        coordinator.commitOffsets(singletonMap(t1p, new OffsetAndMetadata(100L)), Optional.of(new MockCommitCallback()), secondFuture);

        respondToOffsetCommitRequest(singletonMap(t1p, 100L), error);
        consumerClient.pollNoWakeup();
        consumerClient.pollNoWakeup();

        assertTrue(coordinator.coordinatorUnknown());
        assertTrue(firstFuture.failed());
        assertTrue(secondFuture.failed());
    }

    private void respondToOffsetCommitRequest(final Map<TopicPartition, Long> expectedOffsets, Errors error) {
        Map<TopicPartition, Errors> errors = partitionErrors(expectedOffsets.keySet(), error);
        client.respond(offsetCommitRequestMatcher(expectedOffsets), offsetCommitResponse(errors));
    }

    private Map<TopicPartition, Errors> partitionErrors(Collection<TopicPartition> partitions, Errors error) {
        final Map<TopicPartition, Errors> errors = new HashMap<>();
        for (TopicPartition partition : partitions) {
            errors.put(partition, error);
        }
        return errors;
    }

    private DefaultAsyncCoordinator buildCoordinator(final GroupRebalanceConfig rebalanceConfig,
                                                     final Metrics metrics,
                                                     final SubscriptionState subscriptionState,
                                                     final BlockingQueue<BackgroundEvent> bq) {
        return new DefaultAsyncCoordinator(
                this.time,
                new LogContext(),
                rebalanceConfig,
                this.consumerClient,
                subscriptionState,
                bq,
                metrics,
                consumerId + groupId);
    }

    private GroupRebalanceConfig buildRebalanceConfig(Optional<String> groupInstanceId) {
        return new GroupRebalanceConfig(sessionTimeoutMs,
                rebalanceTimeoutMs,
                heartbeatIntervalMs,
                groupId,
                groupInstanceId,
                retryBackoffMs,
                !groupInstanceId.isPresent());
    }
}
