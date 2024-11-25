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
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol.COOPERATIVE;
import static org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol.EAGER;
import static org.apache.kafka.clients.consumer.CooperativeStickyAssignor.COOPERATIVE_STICKY_ASSIGNOR_NAME;
import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class ConsumerCoordinatorTest {
    private final String topic1 = "test1";
    private final String topic2 = "test2";
    private final TopicPartition t1p = new TopicPartition(topic1, 0);
    private final TopicPartition t2p = new TopicPartition(topic2, 0);
    private final String groupId = "test-group";
    private final Optional<String> groupInstanceId = Optional.of("test-instance");
    private final int rebalanceTimeoutMs = 60000;
    private final int sessionTimeoutMs = 10000;
    private final int heartbeatIntervalMs = 5000;
    private final long retryBackoffMs = 100;
    private final long retryBackoffMaxMs = 1000;
    private final int autoCommitIntervalMs = 2000;
    private final int requestTimeoutMs = 30000;
    private final int throttleMs = 10;
    private final MockTime time = new MockTime();
    private GroupRebalanceConfig rebalanceConfig;

    private final ConsumerPartitionAssignor.RebalanceProtocol protocol;
    private final MockPartitionAssignor partitionAssignor;
    private final ThrowOnAssignmentAssignor throwOnAssignmentAssignor;
    private final ThrowOnAssignmentAssignor throwFatalErrorOnAssignmentAssignor;
    private final List<ConsumerPartitionAssignor> assignors;
    private final Map<String, MockPartitionAssignor> assignorMap;
    private final String consumerId = "consumer";
    private final String consumerId2 = "consumer2";

    private MockClient client;
    private final MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(1, new HashMap<String, Integer>() {
        {
            put(topic1, 1);
            put(topic2, 1);
        }
    });
    private final Node node = metadataResponse.brokers().iterator().next();
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private Metrics metrics;
    private ConsumerNetworkClient consumerClient;
    private MockRebalanceListener rebalanceListener;
    private MockCommitCallback mockOffsetCommitCallback;
    private ConsumerCoordinator coordinator;

    public ConsumerCoordinatorTest(final ConsumerPartitionAssignor.RebalanceProtocol protocol) {
        this.protocol = protocol;

        this.partitionAssignor = new MockPartitionAssignor(Collections.singletonList(protocol));
        this.throwOnAssignmentAssignor = new ThrowOnAssignmentAssignor(Collections.singletonList(protocol),
            new KafkaException("Kaboom for assignment!"),
            "throw-on-assignment-assignor");
        this.throwFatalErrorOnAssignmentAssignor = new ThrowOnAssignmentAssignor(Collections.singletonList(protocol),
            new IllegalStateException("Illegal state for assignment!"),
            "throw-fatal-error-on-assignment-assignor");
        this.assignors = Arrays.asList(partitionAssignor, throwOnAssignmentAssignor, throwFatalErrorOnAssignmentAssignor);
        this.assignorMap = mkMap(mkEntry(partitionAssignor.name(), partitionAssignor),
            mkEntry(throwOnAssignmentAssignor.name(), throwOnAssignmentAssignor),
            mkEntry(throwFatalErrorOnAssignmentAssignor.name(), throwFatalErrorOnAssignmentAssignor));
    }

    @BeforeEach
    public void setup() {
        LogContext logContext = new LogContext();
        this.subscriptions = new SubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        this.metadata = new ConsumerMetadata(0, 0, Long.MAX_VALUE, false,
                false, subscriptions, logContext, new ClusterResourceListeners());
        this.client = new MockClient(time, metadata);
        this.client.updateMetadata(metadataResponse);
        this.consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time, 100,
                requestTimeoutMs, Integer.MAX_VALUE);
        this.metrics = new Metrics(time);
        this.rebalanceListener = new MockRebalanceListener();
        this.mockOffsetCommitCallback = new MockCommitCallback();
        this.partitionAssignor.clear();
        this.rebalanceConfig = buildRebalanceConfig(Optional.empty());
        this.coordinator = buildCoordinator(rebalanceConfig,
                                            metrics,
                                            assignors,
                                            false,
                                            subscriptions);
    }

    private GroupRebalanceConfig buildRebalanceConfig(Optional<String> groupInstanceId) {
        return new GroupRebalanceConfig(sessionTimeoutMs,
                                        rebalanceTimeoutMs,
                                        heartbeatIntervalMs,
                                        groupId,
                                        groupInstanceId,
                                        retryBackoffMs,
                                        retryBackoffMaxMs,
                                        !groupInstanceId.isPresent());
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
        subscriptions.assignFromUser(Set.of(t1p, t2p));
        assertEquals(2.0d, getMetric("assigned-partitions").metricValue());
    }

    private KafkaMetric getMetric(final String name) {
        return metrics.metrics().get(metrics.metricName(name, consumerId + groupId + "-coordinator-metrics"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPerformAssignmentShouldUpdateGroupSubscriptionAfterAssignmentIfNeeded() {
        SubscriptionState mockSubscriptionState = Mockito.mock(SubscriptionState.class);

        // the consumer only subscribed to "topic1"
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));

        List<JoinGroupResponseData.JoinGroupResponseMember> metadata = new ArrayList<>();
        for (Map.Entry<String, List<String>> subscriptionEntry : memberSubscriptions.entrySet()) {
            ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(subscriptionEntry.getValue());
            ByteBuffer buf = ConsumerProtocol.serializeSubscription(subscription);
            metadata.add(new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId(subscriptionEntry.getKey())
                .setMetadata(buf.array()));
        }

        // normal case: the assignment result will have partitions for only the subscribed topic: "topic1"
        partitionAssignor.prepare(Collections.singletonMap(consumerId, singletonList(t1p)));

        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, false, mockSubscriptionState)) {
            coordinator.onLeaderElected("1", partitionAssignor.name(), metadata, false);

            ArgumentCaptor<Collection<String>> topicsCaptor = ArgumentCaptor.forClass(Collection.class);
            // groupSubscribe should be only called 1 time, which is before assignment,
            // because the assigned topics are the same as the subscribed topics
            Mockito.verify(mockSubscriptionState, Mockito.times(1)).groupSubscribe(topicsCaptor.capture());

            List<Collection<String>> capturedTopics = topicsCaptor.getAllValues();

            // expected the final group subscribed topics to be updated to "topic1"
            assertEquals(Collections.singleton(topic1), capturedTopics.get(0));
        }

        Mockito.clearInvocations(mockSubscriptionState);

        // unsubscribed topic partition assigned case: the assignment result will have partitions for (1) subscribed topic: "topic1"
        // and (2) the additional unsubscribed topic: "topic2". We should add "topic2" into group subscription list
        partitionAssignor.prepare(Collections.singletonMap(consumerId, Arrays.asList(t1p, t2p)));

        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, false, mockSubscriptionState)) {
            coordinator.onLeaderElected("1", partitionAssignor.name(), metadata, false);

            ArgumentCaptor<Collection<String>> topicsCaptor = ArgumentCaptor.forClass(Collection.class);
            // groupSubscribe should be called 2 times, once before assignment, once after assignment
            // (because the assigned topics are not the same as the subscribed topics)
            Mockito.verify(mockSubscriptionState, Mockito.times(2)).groupSubscribe(topicsCaptor.capture());

            List<Collection<String>> capturedTopics = topicsCaptor.getAllValues();

            // expected the final group subscribed topics to be updated to "topic1" and "topic2"
            Set<String> expectedTopicsGotCalled = new HashSet<>(Arrays.asList(topic1, topic2));
            assertEquals(expectedTopicsGotCalled, capturedTopics.get(1));
        }
    }

    public ByteBuffer subscriptionUserData(int generation) {
        final String generationKeyName = "generation";
        final Schema cooperativeStickyAssignorUserDataV0 = new Schema(
            new Field(generationKeyName, Type.INT32));
        Struct struct = new Struct(cooperativeStickyAssignorUserDataV0);

        struct.set(generationKeyName, generation);
        ByteBuffer buffer = ByteBuffer.allocate(cooperativeStickyAssignorUserDataV0.sizeOf(struct));
        cooperativeStickyAssignorUserDataV0.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    private List<JoinGroupResponseData.JoinGroupResponseMember> validateCooperativeAssignmentTestSetup() {
        // consumer1 and consumer2 subscribed to "topic1" with 2 partitions: t1p, t2p
        Map<String, List<String>> memberSubscriptions = new HashMap<>();
        List<String> subscribedTopics = singletonList(topic1);
        memberSubscriptions.put(consumerId, subscribedTopics);
        memberSubscriptions.put(consumerId2, subscribedTopics);

        // the ownedPartition for consumer1 is t1p, t2p
        ConsumerPartitionAssignor.Subscription subscriptionConsumer1 = new ConsumerPartitionAssignor.Subscription(
            subscribedTopics, subscriptionUserData(1), Arrays.asList(t1p, t2p));

        // the ownedPartition for consumer2 is empty
        ConsumerPartitionAssignor.Subscription subscriptionConsumer2 = new ConsumerPartitionAssignor.Subscription(
            subscribedTopics, subscriptionUserData(1), emptyList());

        List<JoinGroupResponseData.JoinGroupResponseMember> metadata = new ArrayList<>();
        for (Map.Entry<String, List<String>> subscriptionEntry : memberSubscriptions.entrySet()) {
            ByteBuffer buf = null;
            if (subscriptionEntry.getKey().equals(consumerId)) {
                buf = ConsumerProtocol.serializeSubscription(subscriptionConsumer1);
            } else {
                buf = ConsumerProtocol.serializeSubscription(subscriptionConsumer2);
            }

            metadata.add(new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId(subscriptionEntry.getKey())
                .setMetadata(buf.array()));
        }

        return metadata;
    }

    @Test
    public void testPerformAssignmentShouldValidateCooperativeAssignment() {
        SubscriptionState mockSubscriptionState = Mockito.mock(SubscriptionState.class);
        List<JoinGroupResponseData.JoinGroupResponseMember> metadata = validateCooperativeAssignmentTestSetup();

        // simulate the custom cooperative assignor didn't revoke the partition first before assign to other consumer
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        assignment.put(consumerId, singletonList(t1p));
        assignment.put(consumerId2, singletonList(t2p));
        partitionAssignor.prepare(assignment);

        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, false, mockSubscriptionState)) {
            if (protocol == COOPERATIVE) {
                // in cooperative protocol, we should throw exception when validating cooperative assignment
                Exception e = assertThrows(IllegalStateException.class,
                    () -> coordinator.onLeaderElected("1", partitionAssignor.name(), metadata, false));
                assertTrue(e.getMessage().contains("Assignor supporting the COOPERATIVE protocol violates its requirements"));
            } else {
                // in eager protocol, we should not validate assignment
                coordinator.onLeaderElected("1", partitionAssignor.name(), metadata, false);
            }
        }
    }

    @Test
    public void testOnLeaderElectedShouldSkipAssignment() {
        SubscriptionState mockSubscriptionState = Mockito.mock(SubscriptionState.class);
        ConsumerPartitionAssignor assignor = Mockito.mock(ConsumerPartitionAssignor.class);
        String assignorName = "mock-assignor";
        Mockito.when(assignor.name()).thenReturn(assignorName);
        Mockito.when(assignor.supportedProtocols()).thenReturn(Collections.singletonList(protocol));

        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));

        List<JoinGroupResponseData.JoinGroupResponseMember> metadata = new ArrayList<>();
        for (Map.Entry<String, List<String>> subscriptionEntry : memberSubscriptions.entrySet()) {
            ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(subscriptionEntry.getValue());
            ByteBuffer buf = ConsumerProtocol.serializeSubscription(subscription);
            metadata.add(new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId(subscriptionEntry.getKey())
                .setMetadata(buf.array()));
        }

        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), Collections.singletonList(assignor), false, mockSubscriptionState)) {
            assertEquals(Collections.emptyMap(), coordinator.onLeaderElected("1", assignorName, metadata, true));
            assertTrue(coordinator.isLeader());
        }

        Mockito.verify(assignor, Mockito.never()).assign(Mockito.any(), Mockito.any());
    }

    @Test
    public void testPerformAssignmentShouldSkipValidateCooperativeAssignmentForBuiltInCooperativeStickyAssignor() {
        SubscriptionState mockSubscriptionState = Mockito.mock(SubscriptionState.class);
        List<JoinGroupResponseData.JoinGroupResponseMember> metadata = validateCooperativeAssignmentTestSetup();

        List<ConsumerPartitionAssignor> assignorsWithCooperativeStickyAssignor = new ArrayList<>(assignors);
        // create a mockPartitionAssignor with the same name as cooperative sticky assignor
        MockPartitionAssignor mockCooperativeStickyAssignor = new MockPartitionAssignor(Collections.singletonList(protocol)) {
            @Override
            public String name() {
                return COOPERATIVE_STICKY_ASSIGNOR_NAME;
            }
        };
        assignorsWithCooperativeStickyAssignor.add(mockCooperativeStickyAssignor);

        // simulate the cooperative sticky assignor do the assignment with out-of-date ownedPartition
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        assignment.put(consumerId, singletonList(t1p));
        assignment.put(consumerId2, singletonList(t2p));
        mockCooperativeStickyAssignor.prepare(assignment);

        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignorsWithCooperativeStickyAssignor, false, mockSubscriptionState)) {
            // should not validate assignment for built-in cooperative sticky assignor
            coordinator.onLeaderElected("1", mockCooperativeStickyAssignor.name(), metadata, false);
        }
    }

    @Test
    public void testSelectRebalanceProtocol() {
        List<ConsumerPartitionAssignor> assignors = new ArrayList<>();
        assignors.add(new MockPartitionAssignor(Collections.singletonList(ConsumerPartitionAssignor.RebalanceProtocol.EAGER)));
        assignors.add(new MockPartitionAssignor(Collections.singletonList(COOPERATIVE)));

        // no commonly supported protocols
        assertThrows(IllegalArgumentException.class, () -> buildCoordinator(rebalanceConfig, new Metrics(), assignors, false, subscriptions));

        assignors.clear();
        assignors.add(new MockPartitionAssignor(Arrays.asList(ConsumerPartitionAssignor.RebalanceProtocol.EAGER, COOPERATIVE)));
        assignors.add(new MockPartitionAssignor(Arrays.asList(ConsumerPartitionAssignor.RebalanceProtocol.EAGER, COOPERATIVE)));

        // select higher indexed (more advanced) protocols
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, false, subscriptions)) {
            assertEquals(COOPERATIVE, coordinator.getProtocol());
        }
    }

    @Test
    public void testNormalHeartbeat() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal heartbeat
        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.NONE));
        consumerClient.poll(time.timer(0));

        assertTrue(future.isDone());
        assertTrue(future.succeeded());
    }

    @Test
    public void testGroupDescribeUnauthorized() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.GROUP_AUTHORIZATION_FAILED));
        assertThrows(GroupAuthorizationException.class, () -> coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE)));
    }

    @Test
    public void testGroupReadUnauthorized() {
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        client.prepareResponse(joinGroupLeaderResponse(0, "memberId", Collections.emptyMap(),
                Errors.GROUP_AUTHORIZATION_FAILED));
        assertThrows(GroupAuthorizationException.class, () -> coordinator.poll(time.timer(Long.MAX_VALUE)));
    }

    @Test
    public void testCoordinatorNotAvailableWithUserAssignedType() {
        subscriptions.assignFromUser(Collections.singleton(t1p));
        // should mark coordinator unknown after COORDINATOR_NOT_AVAILABLE error
        client.prepareResponse(groupCoordinatorResponse(node, Errors.COORDINATOR_NOT_AVAILABLE));
        // set timeout to 0 because we don't want to retry after the error
        coordinator.poll(time.timer(0));
        assertTrue(coordinator.coordinatorUnknown());

        // should not try to find coordinator since we are in manual assignment
        // hence the prepared response should not be returned
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testAutoCommitAsyncWithUserAssignedType() {
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true, subscriptions)) {
            subscriptions.assignFromUser(Collections.singleton(t1p));
            // set timeout to 0 because we expect no requests sent
            coordinator.poll(time.timer(0));
            assertTrue(coordinator.coordinatorUnknown());
            assertFalse(client.hasInFlightRequests());

            // elapse auto commit interval and set committable position
            time.sleep(autoCommitIntervalMs);
            subscriptions.seekUnvalidated(t1p, new SubscriptionState.FetchPosition(100L));

            // should try to find coordinator since we are auto committing
            coordinator.poll(time.timer(0));
            assertTrue(coordinator.coordinatorUnknown());
            assertTrue(client.hasInFlightRequests());

            client.respond(groupCoordinatorResponse(node, Errors.NONE));
            coordinator.poll(time.timer(0));
            assertFalse(coordinator.coordinatorUnknown());
            // after we've discovered the coordinator we should send
            // out the commit request immediately
            assertTrue(client.hasInFlightRequests());
        }
    }

    @Test
    public void testCommitAsyncWithUserAssignedType() {
        subscriptions.assignFromUser(Collections.singleton(t1p));
        // set timeout to 0 because we expect no requests sent
        coordinator.poll(time.timer(0));
        assertTrue(coordinator.coordinatorUnknown());
        assertFalse(client.hasInFlightRequests());

        // should try to find coordinator since we are commit async
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), (offsets, exception) -> {
            fail("Commit should not get responses, but got offsets:" + offsets + ", and exception:" + exception);
        });
        coordinator.poll(time.timer(0));
        assertTrue(coordinator.coordinatorUnknown());
        assertTrue(client.hasInFlightRequests());
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);

        client.respond(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.poll(time.timer(0));
        assertFalse(coordinator.coordinatorUnknown());
        // after we've discovered the coordinator we should send
        // out the commit request immediately
        assertTrue(client.hasInFlightRequests());
        assertEquals(coordinator.inFlightAsyncCommits.get(), 1);
    }

    @Test
    public void testCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
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
    public void testEnsureCompletingAsyncCommitsWhenSyncCommitWithoutOffsets() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        TopicPartition tp = new TopicPartition("foo", 0);
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(tp, new OffsetAndMetadata(123));

        final AtomicBoolean committed = new AtomicBoolean();
        coordinator.commitOffsetsAsync(offsets, (committedOffsets, exception) -> {
            committed.set(true);
        });

        assertFalse(coordinator.commitOffsetsSync(Collections.emptyMap(), time.timer(100L)), "expected sync commit to fail");
        assertFalse(committed.get());
        assertEquals(coordinator.inFlightAsyncCommits.get(), 1);

        prepareOffsetCommitRequest(singletonMap(tp, 123L), Errors.NONE);

        assertTrue(coordinator.commitOffsetsSync(Collections.emptyMap(), time.timer(Long.MAX_VALUE)), "expected sync commit to succeed");
        assertTrue(committed.get(), "expected commit callback to be invoked");
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);
    }

    @Test
    public void testManyInFlightAsyncCommitsWithCoordinatorDisconnect() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        int numRequests = 1000;
        TopicPartition tp = new TopicPartition("foo", 0);
        final AtomicInteger responses = new AtomicInteger(0);

        for (int i = 0; i < numRequests; i++) {
            Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(tp, new OffsetAndMetadata(i));
            coordinator.commitOffsetsAsync(offsets, (offsets1, exception) -> {
                responses.incrementAndGet();
                Throwable cause = exception.getCause();
                assertInstanceOf(DisconnectException.class, cause,
                    "Unexpected exception cause type: " + (cause == null ? null : cause.getClass()));
            });
        }
        assertEquals(coordinator.inFlightAsyncCommits.get(), numRequests);

        coordinator.markCoordinatorUnknown("test cause");
        consumerClient.pollNoWakeup();
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertEquals(numRequests, responses.get());
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);
    }

    @Test
    public void testCoordinatorUnknownInUnsentCallbacksAfterCoordinatorDead() {
        // When the coordinator is marked dead, all unsent or in-flight requests are cancelled
        // with a disconnect error. This test case ensures that the corresponding callbacks see
        // the coordinator as unknown which prevents additional retries to the same coordinator.

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
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
                        assertInstanceOf(DisconnectException.class, e, "Unexpected exception type: " + e.getClass());
                        assertTrue(coordinator.coordinatorUnknown());
                        asyncCallbackInvoked.set(true);
                    }
                });

        coordinator.markCoordinatorUnknown("test cause");
        consumerClient.pollNoWakeup();
        assertTrue(asyncCallbackInvoked.get());
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);
    }

    @Test
    public void testNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
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
    public void testIllegalGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // illegal_generation will cause re-partition
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        subscriptions.assignFromSubscribed(Collections.singletonList(t1p));

        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.ILLEGAL_GENERATION));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(time.timer(0));

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertEquals(Errors.ILLEGAL_GENERATION.exception(), future.exception());
        assertTrue(coordinator.rejoinNeededOrPending());

        coordinator.poll(time.timer(0));

        assertEquals(1, rebalanceListener.lostCount);
        assertEquals(Collections.singleton(t1p), rebalanceListener.lost);
    }

    @Test
    public void testUnsubscribeWithValidGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        ByteBuffer buffer = ConsumerProtocol.serializeAssignment(
            new ConsumerPartitionAssignor.Assignment(Collections.singletonList(t1p), ByteBuffer.wrap(new byte[0])));
        coordinator.onJoinComplete(1, "memberId", partitionAssignor.name(), buffer);

        coordinator.onLeavePrepare();
        assertEquals(1, rebalanceListener.lostCount);
        assertEquals(0, rebalanceListener.revokedCount);
    }

    @Test
    public void testRevokeExceptionThrownFirstNonBlockingSubCallbacks() {
        MockRebalanceListener throwOnRevokeListener = new MockRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                super.onPartitionsRevoked(partitions);
                throw new KafkaException("Kaboom on revoke!");
            }
        };

        if (protocol == COOPERATIVE) {
            verifyOnCallbackExceptions(throwOnRevokeListener,
                throwOnAssignmentAssignor.name(), "Kaboom on revoke!", null);
        } else {
            // Eager protocol doesn't revoke partitions.
            verifyOnCallbackExceptions(throwOnRevokeListener,
                throwOnAssignmentAssignor.name(), "Kaboom for assignment!", null);
        }
    }

    @Test
    public void testOnAssignmentExceptionThrownFirstNonBlockingSubCallbacks() {
        MockRebalanceListener throwOnAssignListener = new MockRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                super.onPartitionsAssigned(partitions);
                throw new KafkaException("Kaboom on partition assign!");
            }
        };

        verifyOnCallbackExceptions(throwOnAssignListener,
            throwOnAssignmentAssignor.name(), "Kaboom for assignment!", null);
    }

    @Test
    public void testOnPartitionsAssignExceptionThrownWhenNoPreviousThrownCallbacks() {
        MockRebalanceListener throwOnAssignListener = new MockRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                super.onPartitionsAssigned(partitions);
                throw new KafkaException("Kaboom on partition assign!");
            }
        };

        verifyOnCallbackExceptions(throwOnAssignListener,
            partitionAssignor.name(), "Kaboom on partition assign!", null);
    }

    @Test
    public void testOnRevokeExceptionShouldBeRenderedIfNotKafkaException() {
        MockRebalanceListener throwOnRevokeListener = new MockRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                super.onPartitionsRevoked(partitions);
                throw new IllegalStateException("Illegal state on partition revoke!");
            }
        };

        if (protocol == COOPERATIVE) {
            verifyOnCallbackExceptions(throwOnRevokeListener,
                throwOnAssignmentAssignor.name(),
                "User rebalance callback throws an error", "Illegal state on partition revoke!");
        } else {
            // Eager protocol doesn't revoke partitions.
            verifyOnCallbackExceptions(throwOnRevokeListener,
                throwOnAssignmentAssignor.name(), "Kaboom for assignment!", null);
        }
    }

    @Test
    public void testOnAssignmentExceptionShouldBeRenderedIfNotKafkaException() {
        MockRebalanceListener throwOnAssignListener = new MockRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                super.onPartitionsAssigned(partitions);
                throw new KafkaException("Kaboom on partition assign!");
            }
        };
        verifyOnCallbackExceptions(throwOnAssignListener,
            throwFatalErrorOnAssignmentAssignor.name(),
            "User rebalance callback throws an error", "Illegal state for assignment!");
    }

    @Test
    public void testOnPartitionsAssignExceptionShouldBeRenderedIfNotKafkaException() {
        MockRebalanceListener throwOnAssignListener = new MockRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                super.onPartitionsAssigned(partitions);
                throw new IllegalStateException("Illegal state on partition assign!");
            }
        };

        verifyOnCallbackExceptions(throwOnAssignListener,
            partitionAssignor.name(), "User rebalance callback throws an error",
            "Illegal state on partition assign!");
    }

    private void verifyOnCallbackExceptions(final MockRebalanceListener rebalanceListener,
                                            final String assignorName,
                                            final String exceptionMessage,
                                            final String causeMessage) {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        ByteBuffer buffer = ConsumerProtocol.serializeAssignment(
            new ConsumerPartitionAssignor.Assignment(Collections.singletonList(t1p), ByteBuffer.wrap(new byte[0])));
        subscriptions.assignFromSubscribed(singleton(t2p));

        if (exceptionMessage != null) {
            final Exception exception = assertThrows(KafkaException.class,
                () -> coordinator.onJoinComplete(1, "memberId", assignorName, buffer));
            assertEquals(exceptionMessage, exception.getMessage());
            if (causeMessage != null) {
                assertEquals(causeMessage, exception.getCause().getMessage());
            }
        }

        // Eager doesn't trigger on partition revoke.
        assertEquals(protocol == COOPERATIVE ? 1 : 0, rebalanceListener.revokedCount);
        assertEquals(0, rebalanceListener.lostCount);
        assertEquals(1, rebalanceListener.assignedCount);
        assertTrue(assignorMap.containsKey(assignorName), "Unknown assignor name: " + assignorName);
        assertEquals(1, assignorMap.get(assignorName).numAssignment());
    }

    @Test
    public void testUnsubscribeWithInvalidGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        subscriptions.assignFromSubscribed(Collections.singletonList(t1p));

        coordinator.onLeavePrepare();
        assertEquals(1, rebalanceListener.lostCount);
        assertEquals(0, rebalanceListener.revokedCount);
    }

    @Test
    public void testUnknownMemberId() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // illegal_generation will cause re-partition
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        subscriptions.assignFromSubscribed(Collections.singletonList(t1p));

        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.UNKNOWN_MEMBER_ID));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(time.timer(0));

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.exception(), future.exception());
        assertTrue(coordinator.rejoinNeededOrPending());

        coordinator.poll(time.timer(0));

        assertEquals(1, rebalanceListener.lostCount);
        assertEquals(Collections.singleton(t1p), rebalanceListener.lost);
    }

    @Test
    public void testCoordinatorDisconnect() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // coordinator disconnect will mark coordinator as unknown
        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.NONE), true); // return disconnected
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(time.timer(0));

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertInstanceOf(DisconnectException.class, future.exception());
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testJoinGroupInvalidGroupId() {
        final String consumerId = "leader";

        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        // ensure metadata is up-to-date for leader
        client.updateMetadata(metadataResponse);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        client.prepareResponse(joinGroupLeaderResponse(0, consumerId, Collections.emptyMap(),
                Errors.INVALID_GROUP_ID));
        assertThrows(ApiException.class, () -> coordinator.poll(time.timer(Long.MAX_VALUE)));
    }

    @Test
    public void testNormalJoinGroupLeader() {
        final String consumerId = "leader";
        final Set<String> subscription = singleton(topic1);
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> assigned = singletonList(t1p);

        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        // ensure metadata is up-to-date for leader
        client.updateMetadata(metadataResponse);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, assigned));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data().memberId().equals(consumerId) &&
                    sync.data().generationId() == 1 &&
                    sync.groupAssignments().containsKey(consumerId);
        }, syncGroupResponse(assigned, Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(Set.copyOf(assigned), subscriptions.assignedPartitions());
        assertEquals(subscription, subscriptions.metadataTopics());
        assertEquals(0, rebalanceListener.revokedCount);
        assertNull(rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);
    }

    @Test
    public void testOutdatedCoordinatorAssignment() {
        final String consumerId = "outdated_assignment";
        final List<TopicPartition> owned = Collections.emptyList();
        final List<String> oldSubscription = singletonList(topic2);
        final List<TopicPartition> oldAssignment = singletonList(t2p);
        final List<String> newSubscription = singletonList(topic1);
        final List<TopicPartition> newAssignment = singletonList(t1p);

        subscriptions.subscribe(Set.copyOf(oldSubscription), Optional.of(rebalanceListener));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // Test coordinator returning unsubscribed partitions
        partitionAssignor.prepare(singletonMap(consumerId, newAssignment));

        // First incorrect assignment for subscription
        client.prepareResponse(
                joinGroupLeaderResponse(
                    1, consumerId, singletonMap(consumerId, oldSubscription), Errors.NONE));
        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data().memberId().equals(consumerId) &&
                    sync.data().generationId() == 1 &&
                    sync.groupAssignments().containsKey(consumerId);
        }, syncGroupResponse(oldAssignment, Errors.NONE));

        // Second correct assignment for subscription
        client.prepareResponse(
                joinGroupLeaderResponse(
                    1, consumerId, singletonMap(consumerId, newSubscription), Errors.NONE));
        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data().memberId().equals(consumerId) &&
                    sync.data().generationId() == 1 &&
                    sync.groupAssignments().containsKey(consumerId);
        }, syncGroupResponse(newAssignment, Errors.NONE));

        // Poll once so that the join group future gets created and complete
        coordinator.poll(time.timer(0));

        // Before the sync group response gets completed change the subscription
        subscriptions.subscribe(Set.copyOf(newSubscription), Optional.of(rebalanceListener));
        coordinator.poll(time.timer(0));

        coordinator.poll(time.timer(Long.MAX_VALUE));

        final Collection<TopicPartition> assigned = getAdded(owned, newAssignment);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(Set.copyOf(newAssignment), subscriptions.assignedPartitions());
        assertEquals(Set.copyOf(newSubscription), subscriptions.metadataTopics());
        assertEquals(protocol == EAGER ? 1 : 0, rebalanceListener.revokedCount);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(assigned, rebalanceListener.assigned);
    }

    @Test
    public void testMetadataTopicsDuringSubscriptionChange() {
        final String consumerId = "subscription_change";
        final List<String> oldSubscription = singletonList(topic1);
        final List<TopicPartition> oldAssignment = Collections.singletonList(t1p);
        final List<String> newSubscription = singletonList(topic2);
        final List<TopicPartition> newAssignment = Collections.singletonList(t2p);

        subscriptions.subscribe(Set.copyOf(oldSubscription), Optional.of(rebalanceListener));
        assertEquals(Set.copyOf(oldSubscription), subscriptions.metadataTopics());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareJoinAndSyncResponse(consumerId, 1, oldSubscription, oldAssignment);

        coordinator.poll(time.timer(0));
        assertEquals(Set.copyOf(oldSubscription), subscriptions.metadataTopics());

        subscriptions.subscribe(Set.copyOf(newSubscription), Optional.of(rebalanceListener));
        assertEquals(Set.of(topic1, topic2), subscriptions.metadataTopics());

        prepareJoinAndSyncResponse(consumerId, 2, newSubscription, newAssignment);
        coordinator.poll(time.timer(Long.MAX_VALUE));
        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(Set.copyOf(newAssignment), subscriptions.assignedPartitions());
        assertEquals(Set.copyOf(newSubscription), subscriptions.metadataTopics());
    }

    @Test
    public void testPatternJoinGroupLeader() {
        final String consumerId = "leader";
        final List<TopicPartition> assigned = Arrays.asList(t1p, t2p);
        final List<TopicPartition> owned = Collections.emptyList();

        subscriptions.subscribe(Pattern.compile("test.*"), Optional.of(rebalanceListener));

        // partially update the metadata with one topic first,
        // let the leader to refresh metadata during assignment
        client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, assigned));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data().memberId().equals(consumerId) &&
                    sync.data().generationId() == 1 &&
                    sync.groupAssignments().containsKey(consumerId);
        }, syncGroupResponse(assigned, Errors.NONE));
        // expect client to force updating the metadata, if yes gives it both topics
        client.prepareMetadataUpdate(metadataResponse);

        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(2, subscriptions.numAssignedPartitions());
        assertEquals(2, subscriptions.metadataTopics().size());
        assertEquals(2, subscriptions.subscription().size());
        // callback not triggered at all since there's nothing to be revoked
        assertEquals(0, rebalanceListener.revokedCount);
        assertNull(rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);
    }

    @Test
    public void testMetadataRefreshDuringRebalance() {
        final String consumerId = "leader";
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> oldAssigned = singletonList(t1p);
        subscriptions.subscribe(Pattern.compile(".*"), Optional.of(rebalanceListener));
        client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));
        coordinator.maybeUpdateSubscriptionMetadata();

        assertEquals(singleton(topic1), subscriptions.subscription());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        Map<String, List<String>> initialSubscription = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, oldAssigned));

        // the metadata will be updated in flight with a new topic added
        final List<String> updatedSubscription = Arrays.asList(topic1, topic2);

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, initialSubscription, Errors.NONE));
        client.prepareResponse(body -> {
            final Map<String, Integer> updatedPartitions = new HashMap<>();
            for (String topic : updatedSubscription)
                updatedPartitions.put(topic, 1);
            client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, updatedPartitions));
            return true;
        }, syncGroupResponse(oldAssigned, Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        // rejoin will only be set in the next poll call
        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(topic1), subscriptions.subscription());
        assertEquals(Set.copyOf(oldAssigned), subscriptions.assignedPartitions());
        // nothing to be revoked and hence no callback triggered
        assertEquals(0, rebalanceListener.revokedCount);
        assertNull(rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, oldAssigned), rebalanceListener.assigned);

        List<TopicPartition> newAssigned = Arrays.asList(t1p, t2p);

        final Map<String, List<String>> updatedSubscriptions = singletonMap(consumerId, Arrays.asList(topic1, topic2));
        partitionAssignor.prepare(singletonMap(consumerId, newAssigned));

        // we expect to see a second rebalance with the new-found topics
        client.prepareResponse(body -> {
            JoinGroupRequest join = (JoinGroupRequest) body;
            Iterator<JoinGroupRequestData.JoinGroupRequestProtocol> protocolIterator =
                    join.data().protocols().iterator();
            assertTrue(protocolIterator.hasNext());
            JoinGroupRequestData.JoinGroupRequestProtocol protocolMetadata = protocolIterator.next();

            ByteBuffer metadata = ByteBuffer.wrap(protocolMetadata.metadata());
            ConsumerPartitionAssignor.Subscription subscription = ConsumerProtocol.deserializeSubscription(metadata);
            metadata.rewind();
            return subscription.topics().containsAll(updatedSubscription);
        }, joinGroupLeaderResponse(2, consumerId, updatedSubscriptions, Errors.NONE));
        // update the metadata again back to topic1
        client.prepareResponse(body -> {
            client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));
            return true;
        }, syncGroupResponse(newAssigned, Errors.NONE));

        coordinator.poll(time.timer(Long.MAX_VALUE));

        Collection<TopicPartition> revoked = getRevoked(oldAssigned, newAssigned);
        int revokedCount = revoked.isEmpty() ? 0 : 1;

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(Set.copyOf(updatedSubscription), subscriptions.subscription());
        assertEquals(Set.copyOf(newAssigned), subscriptions.assignedPartitions());
        assertEquals(revokedCount, rebalanceListener.revokedCount);
        assertEquals(revoked.isEmpty() ? null : revoked, rebalanceListener.revoked);
        assertEquals(2, rebalanceListener.assignedCount);
        assertEquals(getAdded(oldAssigned, newAssigned), rebalanceListener.assigned);

        // we expect to see a third rebalance with the new-found topics
        partitionAssignor.prepare(singletonMap(consumerId, oldAssigned));

        client.prepareResponse(body -> {
            JoinGroupRequest join = (JoinGroupRequest) body;
            Iterator<JoinGroupRequestData.JoinGroupRequestProtocol> protocolIterator =
                join.data().protocols().iterator();
            assertTrue(protocolIterator.hasNext());
            JoinGroupRequestData.JoinGroupRequestProtocol protocolMetadata = protocolIterator.next();

            ByteBuffer metadata = ByteBuffer.wrap(protocolMetadata.metadata());
            ConsumerPartitionAssignor.Subscription subscription = ConsumerProtocol.deserializeSubscription(metadata);
            metadata.rewind();
            return subscription.topics().contains(topic1);
        }, joinGroupLeaderResponse(3, consumerId, initialSubscription, Errors.NONE));
        client.prepareResponse(syncGroupResponse(oldAssigned, Errors.NONE));

        coordinator.poll(time.timer(Long.MAX_VALUE));

        revoked = getRevoked(newAssigned, oldAssigned);
        assertFalse(revoked.isEmpty());
        revokedCount += 1;
        Collection<TopicPartition> added = getAdded(newAssigned, oldAssigned);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(topic1), subscriptions.subscription());
        assertEquals(Set.copyOf(oldAssigned), subscriptions.assignedPartitions());
        assertEquals(revokedCount, rebalanceListener.revokedCount);
        assertEquals(revoked.isEmpty() ? null : revoked, rebalanceListener.revoked);
        assertEquals(3, rebalanceListener.assignedCount);
        assertEquals(added, rebalanceListener.assigned);
        assertEquals(0, rebalanceListener.lostCount);
    }

    @Test
    public void testForceMetadataRefreshForPatternSubscriptionDuringRebalance() {
        // Set up a non-leader consumer with pattern subscription and a cluster containing one topic matching the
        // pattern.
        subscriptions.subscribe(Pattern.compile(".*"), Optional.of(rebalanceListener));
        client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));
        coordinator.maybeUpdateSubscriptionMetadata();
        assertEquals(singleton(topic1), subscriptions.subscription());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // Instrument the test so that metadata will contain two topics after next refresh.
        client.prepareMetadataUpdate(metadataResponse);

        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data().memberId().equals(consumerId) &&
                sync.data().generationId() == 1 &&
                sync.groupAssignments().isEmpty();
        }, syncGroupResponse(singletonList(t1p), Errors.NONE));

        partitionAssignor.prepare(singletonMap(consumerId, singletonList(t1p)));

        // This will trigger rebalance.
        coordinator.poll(time.timer(Long.MAX_VALUE));

        // Make sure that the metadata was refreshed during the rebalance and thus subscriptions now contain two topics.
        final Set<String> updatedSubscriptionSet = new HashSet<>(Arrays.asList(topic1, topic2));
        assertEquals(updatedSubscriptionSet, subscriptions.subscription());

        // Refresh the metadata again. Since there have been no changes since the last refresh, it won't trigger
        // rebalance again.
        metadata.requestUpdate(true);
        consumerClient.poll(time.timer(Long.MAX_VALUE));
        assertFalse(coordinator.rejoinNeededOrPending());
    }

    @Test
    public void testForceMetadataDeleteForPatternSubscriptionDuringRebalance() {
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true, subscriptions)) {
            subscriptions.subscribe(Pattern.compile("test.*"), Optional.of(rebalanceListener));
            client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, new HashMap<String, Integer>() {
                {
                    put(topic1, 1);
                    put(topic2, 1);
                }
            }));
            coordinator.maybeUpdateSubscriptionMetadata();
            assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), subscriptions.subscription());

            client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
            coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

            MetadataResponse deletedMetadataResponse = RequestTestUtils.metadataUpdateWith(1, new HashMap<String, Integer>() {
                {
                    put(topic1, 1);
                }
            });
            // Instrument the test so that metadata will contain only one topic after next refresh.
            client.prepareMetadataUpdate(deletedMetadataResponse);

            client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
            client.prepareResponse(body -> {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.data().memberId().equals(consumerId) &&
                        sync.data().generationId() == 1 &&
                        sync.groupAssignments().isEmpty();
            }, syncGroupResponse(singletonList(t1p), Errors.NONE));

            partitionAssignor.prepare(singletonMap(consumerId, singletonList(t1p)));

            // This will trigger rebalance.
            coordinator.poll(time.timer(Long.MAX_VALUE));

            // Make sure that the metadata was refreshed during the rebalance and thus subscriptions now contain only one topic.
            assertEquals(singleton(topic1), subscriptions.subscription());

            // Refresh the metadata again. Since there have been no changes since the last refresh, it won't trigger
            // rebalance again.
            metadata.requestUpdate(true);
            consumerClient.poll(time.timer(Long.MAX_VALUE));
            assertFalse(coordinator.rejoinNeededOrPending());
        }
    }

    @Test
    public void testOnJoinPrepareWithOffsetCommitShouldSuccessAfterRetry() {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, Optional.empty(), false)) {
            int generationId = 42;
            String memberId = "consumer-42";

            Timer pollTimer = time.timer(100L);
            client.prepareResponse(offsetCommitResponse(singletonMap(t1p, Errors.UNKNOWN_TOPIC_OR_PARTITION)));
            boolean res = coordinator.onJoinPrepare(pollTimer, generationId, memberId);
            assertFalse(res);

            pollTimer = time.timer(100L);
            client.prepareResponse(offsetCommitResponse(singletonMap(t1p, Errors.NONE)));
            res = coordinator.onJoinPrepare(pollTimer, generationId, memberId);
            assertTrue(res);

            assertFalse(client.hasPendingResponses());
            assertFalse(client.hasInFlightRequests());
            assertFalse(coordinator.coordinatorUnknown());
        }
    }

    @Test
    public void testOnJoinPrepareWithOffsetCommitShouldKeepJoinAfterNonRetryableException() {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, Optional.empty(), false)) {
            int generationId = 42;
            String memberId = "consumer-42";

            Timer pollTimer = time.timer(100L);
            client.prepareResponse(offsetCommitResponse(singletonMap(t1p, Errors.UNKNOWN_MEMBER_ID)));
            boolean res = coordinator.onJoinPrepare(pollTimer, generationId, memberId);
            assertTrue(res);

            assertFalse(client.hasPendingResponses());
            assertFalse(client.hasInFlightRequests());
            assertFalse(coordinator.coordinatorUnknown());
        }
    }

    @Test
    public void testOnJoinPrepareWithOffsetCommitShouldKeepJoinAfterRebalanceTimeout() {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, Optional.empty(), false)) {
            int generationId = 42;
            String memberId = "consumer-42";

            Timer pollTimer = time.timer(0L);
            boolean res = coordinator.onJoinPrepare(pollTimer, generationId, memberId);
            assertFalse(res);

            pollTimer = time.timer(100L);
            time.sleep(rebalanceTimeoutMs);
            client.respond(offsetCommitResponse(singletonMap(t1p, Errors.UNKNOWN_TOPIC_OR_PARTITION)));
            res = coordinator.onJoinPrepare(pollTimer, generationId, memberId);
            assertTrue(res);

            assertFalse(client.hasPendingResponses());
            assertFalse(client.hasInFlightRequests());
            assertFalse(coordinator.coordinatorUnknown());
        }
    }

    @Test
    public void testJoinPrepareWithDisableAutoCommit() {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, Optional.of("group-id"), true)) {
            coordinator.ensureActiveGroup();

            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);

            int generationId = 42;
            String memberId = "consumer-42";

            boolean res = coordinator.onJoinPrepare(time.timer(0L), generationId, memberId);

            assertTrue(res);
            assertTrue(client.hasPendingResponses());
            assertFalse(client.hasInFlightRequests());
            assertFalse(coordinator.coordinatorUnknown());
        }
    }

    @Test
    public void testJoinPrepareAndCommitCompleted() {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, Optional.of("group-id"), true)) {
            coordinator.ensureActiveGroup();

            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
            int generationId = 42;
            String memberId = "consumer-42";

            boolean res = coordinator.onJoinPrepare(time.timer(0L), generationId, memberId);
            coordinator.invokeCompletedOffsetCommitCallbacks();

            assertTrue(res);
            assertFalse(client.hasPendingResponses());
            assertFalse(client.hasInFlightRequests());
            assertFalse(coordinator.coordinatorUnknown());
        }
    }

    @Test
    public void testJoinPrepareAndCommitWithCoordinatorNotAvailable() {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, Optional.of("group-id"), true)) {
            coordinator.ensureActiveGroup();

            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);

            int generationId = 42;
            String memberId = "consumer-42";

            boolean res = coordinator.onJoinPrepare(time.timer(0L), generationId, memberId);
            coordinator.invokeCompletedOffsetCommitCallbacks();

            assertFalse(res);
            assertFalse(client.hasPendingResponses());
            assertFalse(client.hasInFlightRequests());
            assertTrue(coordinator.coordinatorUnknown());
        }
    }

    @Test
    public void testJoinPrepareAndCommitWithUnknownMemberId() {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, Optional.of("group-id"), true)) {
            coordinator.ensureActiveGroup();

            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.UNKNOWN_MEMBER_ID);

            int generationId = 42;
            String memberId = "consumer-42";

            boolean res = coordinator.onJoinPrepare(time.timer(0L), generationId, memberId);
            coordinator.invokeCompletedOffsetCommitCallbacks();

            assertTrue(res);
            assertFalse(client.hasPendingResponses());
            assertFalse(client.hasInFlightRequests());
            assertFalse(coordinator.coordinatorUnknown());
        }
    }

    /**
     * Verifies that the consumer re-joins after a metadata change. If JoinGroup fails
     * and metadata reverts to its original value, the consumer should still retry JoinGroup.
     */
    @Test
    public void testRebalanceWithMetadataChange() {
        MetadataResponse metadataResponse1 = RequestTestUtils.metadataUpdateWith(1,
                Utils.mkMap(Utils.mkEntry(topic1, 1), Utils.mkEntry(topic2, 1)));
        MetadataResponse metadataResponse2 = RequestTestUtils.metadataUpdateWith(1, singletonMap(topic1, 1));
        verifyRebalanceWithMetadataChange(Optional.empty(), partitionAssignor, metadataResponse1, metadataResponse2, true);
    }

    @Test
    public void testRackAwareConsumerRebalanceWithDifferentRacks() {
        verifyRackAwareConsumerRebalance(
                Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 2), Arrays.asList(2, 0)),
                Arrays.asList(Arrays.asList(0, 2), Arrays.asList(1, 2), Arrays.asList(2, 0)),
                true, true);
    }

    @Test
    public void testNonRackAwareConsumerRebalanceWithDifferentRacks() {
        verifyRackAwareConsumerRebalance(
                Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 2), Arrays.asList(2, 0)),
                Arrays.asList(Arrays.asList(0, 2), Arrays.asList(1, 2), Arrays.asList(2, 0)),
                false, false);
    }

    @Test
    public void testRackAwareConsumerRebalanceWithAdditionalRacks() {
        verifyRackAwareConsumerRebalance(
                Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 2), Arrays.asList(2, 0)),
                Arrays.asList(Arrays.asList(0, 1, 2), Arrays.asList(1, 2), Arrays.asList(2, 0)),
                true, true);
    }

    @Test
    public void testRackAwareConsumerRebalanceWithLessRacks() {
        verifyRackAwareConsumerRebalance(
                Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 2), Arrays.asList(2, 0)),
                Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 2), Collections.singletonList(2)),
                true, true);
    }

    @Test
    public void testRackAwareConsumerRebalanceWithNewPartitions() {
        verifyRackAwareConsumerRebalance(
                Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 2), Arrays.asList(2, 0)),
                Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 2), Arrays.asList(2, 0), Arrays.asList(0, 1)),
                true, true);
    }

    @Test
    public void testRackAwareConsumerRebalanceWithNoMetadataChange() {
        verifyRackAwareConsumerRebalance(
                Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 2), Arrays.asList(2, 0)),
                Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 2), Arrays.asList(2, 0)),
                true, false);
    }

    @Test
    public void testRackAwareConsumerRebalanceWithNoRackChange() {
        verifyRackAwareConsumerRebalance(
                Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 2), Arrays.asList(2, 0)),
                Arrays.asList(Arrays.asList(3, 4), Arrays.asList(4, 5), Arrays.asList(5, 3)),
                true, false);
    }

    @Test
    public void testRackAwareConsumerRebalanceWithNewReplicasOnSameRacks() {
        verifyRackAwareConsumerRebalance(
                Arrays.asList(Arrays.asList(0, 1), Arrays.asList(1, 2), Arrays.asList(2, 0)),
                Arrays.asList(Arrays.asList(0, 1, 3), Arrays.asList(1, 2, 5), Arrays.asList(2, 0, 3)),
                true, false);
    }

    private void verifyRackAwareConsumerRebalance(List<List<Integer>> partitionReplicas1,
                                                  List<List<Integer>> partitionReplicas2,
                                                  boolean rackAwareConsumer,
                                                  boolean expectRebalance) {
        List<String> racks = Arrays.asList("rack-a", "rack-b", "rack-c");
        MockPartitionAssignor assignor = partitionAssignor;
        String consumerRackId = null;
        if (rackAwareConsumer) {
            consumerRackId = racks.get(0);
            assignor = new RackAwareAssignor(protocol);
            createRackAwareCoordinator(consumerRackId, assignor);
        }

        MetadataResponse metadataResponse1 = rackAwareMetadata(6, racks, Collections.singletonMap(topic1, partitionReplicas1));
        MetadataResponse metadataResponse2 = rackAwareMetadata(6, racks, Collections.singletonMap(topic1, partitionReplicas2));
        verifyRebalanceWithMetadataChange(Optional.ofNullable(consumerRackId), assignor, metadataResponse1, metadataResponse2, expectRebalance);
    }

    private void verifyRebalanceWithMetadataChange(Optional<String> rackId,
                                                   MockPartitionAssignor partitionAssignor,
                                                   MetadataResponse metadataResponse1,
                                                   MetadataResponse metadataResponse2,
                                                   boolean expectRebalance) {
        final String consumerId = "leader";
        final List<String> topics = Arrays.asList(topic1, topic2);
        final List<TopicPartition> partitions = metadataResponse1.topicMetadata().stream()
                .flatMap(t -> t.partitionMetadata().stream().map(p -> new TopicPartition(t.topic(), p.partition())))
                .collect(Collectors.toList());
        subscriptions.subscribe(Set.copyOf(topics), Optional.of(rebalanceListener));
        client.updateMetadata(metadataResponse1);
        coordinator.maybeUpdateSubscriptionMetadata();

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        Map<String, List<String>> initialSubscription = singletonMap(consumerId, topics);
        partitionAssignor.prepare(singletonMap(consumerId, partitions));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, initialSubscription, false, Errors.NONE, rackId));
        client.prepareResponse(syncGroupResponse(partitions, Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        // rejoin will only be set in the next poll call
        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(Set.copyOf(topics), subscriptions.subscription());
        assertEquals(Set.copyOf(partitions), subscriptions.assignedPartitions());
        assertEquals(0, rebalanceListener.revokedCount);
        assertNull(rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);

        // Change metadata to trigger rebalance.
        client.updateMetadata(metadataResponse2);
        coordinator.poll(time.timer(0));

        if (!expectRebalance) {
            assertEquals(0, client.requests().size());
            return;
        }
        assertEquals(1, client.requests().size());

        // Revert metadata to original value. Fail pending JoinGroup. Another
        // JoinGroup should be sent, which will be completed successfully.
        client.updateMetadata(metadataResponse1);
        client.respond(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NOT_COORDINATOR));
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        assertFalse(client.hasInFlightRequests());
        coordinator.poll(time.timer(1));
        assertTrue(coordinator.rejoinNeededOrPending());

        client.respond(request -> {
            if (!(request instanceof JoinGroupRequest)) {
                return false;
            } else {
                JoinGroupRequest joinRequest = (JoinGroupRequest) request;
                return consumerId.equals(joinRequest.data().memberId());
            }
        }, joinGroupLeaderResponse(2, consumerId, initialSubscription, false, Errors.NONE, rackId));
        client.prepareResponse(syncGroupResponse(partitions, Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        Collection<TopicPartition> revoked = getRevoked(partitions, partitions);
        assertEquals(revoked.isEmpty() ? 0 : 1, rebalanceListener.revokedCount);
        assertEquals(revoked.isEmpty() ? null : revoked, rebalanceListener.revoked);
        // No partitions have been lost since the rebalance failure was not fatal
        assertEquals(0, rebalanceListener.lostCount);
        assertNull(rebalanceListener.lost);

        Collection<TopicPartition> added = getAdded(partitions, partitions);
        assertEquals(2, rebalanceListener.assignedCount);
        assertEquals(added.isEmpty() ? Collections.emptySet() : Set.copyOf(partitions), rebalanceListener.assigned);
        assertEquals(Set.copyOf(partitions), subscriptions.assignedPartitions());
    }

    @Test
    public void testWakeupDuringJoin() {
        final String consumerId = "leader";
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> assigned = singletonList(t1p);

        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        // ensure metadata is up-to-date for leader
        client.updateMetadata(metadataResponse);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, assigned));

        // prepare only the first half of the join and then trigger the wakeup
        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        consumerClient.wakeup();

        try {
            coordinator.poll(time.timer(Long.MAX_VALUE));
        } catch (WakeupException e) {
            // ignore
        }

        // now complete the second half
        client.prepareResponse(syncGroupResponse(assigned, Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(Set.copyOf(assigned), subscriptions.assignedPartitions());
        assertEquals(0, rebalanceListener.revokedCount);
        assertNull(rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);
    }

    @Test
    public void testNormalJoinGroupFollower() {
        final Set<String> subscription = singleton(topic1);
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> assigned = singletonList(t1p);

        subscriptions.subscribe(subscription, Optional.of(rebalanceListener));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data().memberId().equals(consumerId) &&
                    sync.data().generationId() == 1 &&
                    sync.groupAssignments().isEmpty();
        }, syncGroupResponse(assigned, Errors.NONE));

        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(Set.copyOf(assigned), subscriptions.assignedPartitions());
        assertEquals(subscription, subscriptions.metadataTopics());
        assertEquals(0, rebalanceListener.revokedCount);
        assertNull(rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);
    }

    @Test
    public void testUpdateLastHeartbeatPollWhenCoordinatorUnknown() throws Exception {
        // If we are part of an active group and we cannot find the coordinator, we should nevertheless
        // continue to update the last poll time so that we do not expire the consumer
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // Join the group, but signal a coordinator change after the first heartbeat
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
        client.prepareResponse(heartbeatResponse(Errors.NOT_COORDINATOR));

        coordinator.poll(time.timer(Long.MAX_VALUE));
        time.sleep(heartbeatIntervalMs);

        // Await the first heartbeat which forces us to find a new coordinator
        TestUtils.waitForCondition(() -> !client.hasPendingResponses(),
                "Failed to observe expected heartbeat from background thread");

        assertTrue(coordinator.coordinatorUnknown());
        assertFalse(coordinator.poll(time.timer(0)));
        assertEquals(time.milliseconds(), coordinator.heartbeat().lastPollTime());

        time.sleep(rebalanceTimeoutMs - 1);
        assertFalse(coordinator.heartbeat().pollTimeoutExpired(time.milliseconds()));
    }

    @Test
    public void testPatternJoinGroupFollower() {
        final Set<String> subscription = Set.of(topic1, topic2);
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> assigned = Arrays.asList(t1p, t2p);

        subscriptions.subscribe(Pattern.compile("test.*"), Optional.of(rebalanceListener));

        // partially update the metadata with one topic first,
        // let the leader to refresh metadata during assignment
        client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data().memberId().equals(consumerId) &&
                    sync.data().generationId() == 1 &&
                    sync.groupAssignments().isEmpty();
        }, syncGroupResponse(assigned, Errors.NONE));
        // expect client to force updating the metadata, if yes gives it both topics
        client.prepareMetadataUpdate(metadataResponse);

        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(assigned.size(), subscriptions.numAssignedPartitions());
        assertEquals(subscription, subscriptions.subscription());
        assertEquals(0, rebalanceListener.revokedCount);
        assertNull(rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);
    }

    @Test
    public void testLeaveGroupOnClose() {

        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        joinAsFollowerAndReceiveAssignment(coordinator, singletonList(t1p));

        final AtomicBoolean received = new AtomicBoolean(false);
        client.prepareResponse(body -> {
            received.set(true);
            LeaveGroupRequest leaveRequest = (LeaveGroupRequest) body;
            return validateLeaveGroup(groupId, consumerId, leaveRequest);
        }, new LeaveGroupResponse(
            new LeaveGroupResponseData().setErrorCode(Errors.NONE.code())));
        coordinator.close(time.timer(0));
        assertTrue(received.get());
    }

    @Test
    public void testMaybeLeaveGroup() {
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        joinAsFollowerAndReceiveAssignment(coordinator, singletonList(t1p));

        final AtomicBoolean received = new AtomicBoolean(false);
        client.prepareResponse(body -> {
            received.set(true);
            LeaveGroupRequest leaveRequest = (LeaveGroupRequest) body;
            return validateLeaveGroup(groupId, consumerId, leaveRequest);
        }, new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code())));
        coordinator.maybeLeaveGroup("test maybe leave group");
        assertTrue(received.get());

        AbstractCoordinator.Generation generation = coordinator.generationIfStable();
        assertNull(generation);
    }

    private boolean validateLeaveGroup(String groupId,
                                       String consumerId,
                                       LeaveGroupRequest leaveRequest) {
        List<MemberIdentity> members = leaveRequest.data().members();
        return leaveRequest.data().groupId().equals(groupId) &&
                   members.size() == 1 &&
                   members.get(0).memberId().equals(consumerId);
    }

    /**
     * This test checks if a consumer that has a valid member ID but an invalid generation
     * ({@link org.apache.kafka.clients.consumer.internals.AbstractCoordinator.Generation#NO_GENERATION})
     * can still execute a leave group request. Such a situation may arise when a consumer has initiated a JoinGroup
     * request without a memberId, but is shutdown or restarted before it has a chance to initiate and complete the
     * second request.
     */
    @Test
    public void testPendingMemberShouldLeaveGroup() {
        final String consumerId = "consumer-id";
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // here we return a DEFAULT_GENERATION_ID, but valid member id and leader id.
        client.prepareResponse(joinGroupFollowerResponse(-1, consumerId, "leader-id", Errors.MEMBER_ID_REQUIRED));

        // execute join group
        coordinator.joinGroupIfNeeded(time.timer(0));

        final AtomicBoolean received = new AtomicBoolean(false);
        client.prepareResponse(body -> {
            received.set(true);
            LeaveGroupRequest leaveRequest = (LeaveGroupRequest) body;
            return validateLeaveGroup(groupId, consumerId, leaveRequest);
        }, new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code())));

        coordinator.maybeLeaveGroup("pending member leaves");
        assertTrue(received.get());
    }

    @Test
    public void testUnexpectedErrorOnSyncGroup() {
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.emptyList(), Errors.UNKNOWN_SERVER_ERROR));
        assertThrows(KafkaException.class, () -> coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE)));
    }

    @Test
    public void testUnknownMemberIdOnSyncGroup() {
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // join initially, but let coordinator returns unknown member id
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.emptyList(), Errors.UNKNOWN_MEMBER_ID));

        // now we should see a new join with the empty UNKNOWN_MEMBER_ID
        client.prepareResponse(body -> {
            JoinGroupRequest joinRequest = (JoinGroupRequest) body;
            return joinRequest.data().memberId().equals(JoinGroupRequest.UNKNOWN_MEMBER_ID);
        }, joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
    }

    @Test
    public void testRebalanceInProgressOnSyncGroup() {
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.emptyList(), Errors.REBALANCE_IN_PROGRESS));

        // then let the full join/sync finish successfully
        client.prepareResponse(joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
    }

    @Test
    public void testIllegalGenerationOnSyncGroup() {
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.emptyList(), Errors.ILLEGAL_GENERATION));

        // then let the full join/sync finish successfully
        client.prepareResponse(body -> {
            JoinGroupRequest joinRequest = (JoinGroupRequest) body;
            // member ID should not be reset under ILLEGAL_GENERATION error
            return joinRequest.data().memberId().equals(consumerId);
        }, joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
    }

    @Test
    public void testMetadataChangeTriggersRebalance() {
        // ensure metadata is up-to-date for leader
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        client.updateMetadata(metadataResponse);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, singletonList(t1p)));

        // the leader is responsible for picking up metadata changes and forcing a group rebalance
        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());

        // a new partition is added to the topic
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith(1, singletonMap(topic1, 2)), false, time.milliseconds());
        coordinator.maybeUpdateSubscriptionMetadata();

        // we should detect the change and ask for reassignment
        assertTrue(coordinator.rejoinNeededOrPending());
    }

    @Test
    public void testStaticLeaderRejoinsGroupAndCanTriggersRebalance() {
        // ensure metadata is up-to-date for leader
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        client.updateMetadata(metadataResponse);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // the leader is responsible for picking up metadata changes and forcing a group rebalance.
        // note that `MockPartitionAssignor.prepare` is not called therefore calling `MockPartitionAssignor.assign`
        // will throw a IllegalStateException. this indirectly verifies that `assign` is correctly skipped.
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, true, Errors.NONE, Optional.empty()));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(topic1), coordinator.subscriptionState().metadataTopics());

        // a new partition is added to the topic
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith(1, singletonMap(topic1, 2)), false, time.milliseconds());
        coordinator.maybeUpdateSubscriptionMetadata();

        // we should detect the change and ask for reassignment
        assertTrue(coordinator.rejoinNeededOrPending());
    }

    @Test
    public void testStaticLeaderRejoinsGroupAndCanDetectMetadataChangesForOtherMembers() {
        // ensure metadata is up-to-date for leader
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        client.updateMetadata(metadataResponse);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // the leader is responsible for picking up metadata changes and forcing a group rebalance.
        // note that `MockPartitionAssignor.prepare` is not called therefore calling `MockPartitionAssignor.assign`
        // will throw a IllegalStateException. this indirectly verifies that `assign` is correctly skipped.
        Map<String, List<String>> memberSubscriptions = mkMap(
            mkEntry(consumerId, singletonList(topic1)),
            mkEntry(consumerId2, singletonList(topic2))
        );
        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, true, Errors.NONE, Optional.empty()));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(Set.of(topic1, topic2), coordinator.subscriptionState().metadataTopics());

        // a new partition is added to the topic2 that only consumerId2 is subscribed to
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWith(1, singletonMap(topic2, 2)), false, time.milliseconds());
        coordinator.maybeUpdateSubscriptionMetadata();

        // we should detect the change and ask for reassignment
        assertTrue(coordinator.rejoinNeededOrPending());
    }

    @Test
    public void testUpdateMetadataDuringRebalance() {
        final String topic1 = "topic1";
        final String topic2 = "topic2";
        TopicPartition tp1 = new TopicPartition(topic1, 0);
        TopicPartition tp2 = new TopicPartition(topic2, 0);
        final String consumerId = "leader";

        List<String> topics = Arrays.asList(topic1, topic2);

        subscriptions.subscribe(new HashSet<>(topics), Optional.of(rebalanceListener));

        // we only have metadata for one topic initially
        client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // prepare initial rebalance
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, topics);
        partitionAssignor.prepare(singletonMap(consumerId, singletonList(tp1)));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            if (sync.data().memberId().equals(consumerId) &&
                    sync.data().generationId() == 1 &&
                    sync.groupAssignments().containsKey(consumerId)) {
                // trigger the metadata update including both topics after the sync group request has been sent
                Map<String, Integer> topicPartitionCounts = new HashMap<>();
                topicPartitionCounts.put(topic1, 1);
                topicPartitionCounts.put(topic2, 1);
                client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, topicPartitionCounts));
                return true;
            }
            return false;
        }, syncGroupResponse(Collections.singletonList(tp1), Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        // the metadata update should trigger a second rebalance
        client.prepareResponse(joinGroupLeaderResponse(2, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(Arrays.asList(tp1, tp2), Errors.NONE));

        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(new HashSet<>(Arrays.asList(tp1, tp2)), subscriptions.assignedPartitions());
    }

    /**
     * Verifies that subscription change updates SubscriptionState correctly even after JoinGroup failures
     * that don't re-invoke onJoinPrepare.
     */
    @Test
    public void testSubscriptionChangeWithAuthorizationFailure() {
        // Subscribe to two topics of which only one is authorized and verify that metadata failure is propagated.
        subscriptions.subscribe(Set.of(topic1, topic2), Optional.of(rebalanceListener));
        client.prepareMetadataUpdate(RequestTestUtils.metadataUpdateWith("kafka-cluster", 1,
                Collections.singletonMap(topic2, Errors.TOPIC_AUTHORIZATION_FAILED), singletonMap(topic1, 1)));
        assertThrows(TopicAuthorizationException.class, () -> coordinator.poll(time.timer(Long.MAX_VALUE)));

        client.respond(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // Fail the first JoinGroup request
        client.prepareResponse(joinGroupLeaderResponse(0, consumerId, Collections.emptyMap(),
                Errors.GROUP_AUTHORIZATION_FAILED));
        assertThrows(GroupAuthorizationException.class, () -> coordinator.poll(time.timer(Long.MAX_VALUE)));

        // Change subscription to include only the authorized topic. Complete rebalance and check that
        // references to topic2 have been removed from SubscriptionState.
        subscriptions.subscribe(Set.of(topic1), Optional.of(rebalanceListener));
        assertEquals(Collections.singleton(topic1), subscriptions.metadataTopics());
        client.prepareMetadataUpdate(RequestTestUtils.metadataUpdateWith("kafka-cluster", 1,
                Collections.emptyMap(), singletonMap(topic1, 1)));

        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, singletonList(t1p)));
        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertEquals(singleton(topic1), subscriptions.subscription());
        assertEquals(singleton(topic1), subscriptions.metadataTopics());
    }

    @Test
    public void testWakeupFromAssignmentCallback() {
        final String topic = "topic1";
        TopicPartition partition = new TopicPartition(topic, 0);
        final String consumerId = "follower";
        Set<String> topics = Collections.singleton(topic);
        MockRebalanceListener rebalanceListener = new MockRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                boolean raiseWakeup = this.assignedCount == 0;
                super.onPartitionsAssigned(partitions);

                if (raiseWakeup)
                    throw new WakeupException();
            }
        };

        subscriptions.subscribe(topics, Optional.of(rebalanceListener));

        // we only have metadata for one topic initially
        client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // prepare initial rebalance
        partitionAssignor.prepare(singletonMap(consumerId, Collections.singletonList(partition)));

        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.singletonList(partition), Errors.NONE));

        // The first call to poll should raise the exception from the rebalance listener
        try {
            coordinator.poll(time.timer(Long.MAX_VALUE));
            fail("Expected exception thrown from assignment callback");
        } catch (WakeupException e) {
        }

        // The second call should retry the assignment callback and succeed
        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(0, rebalanceListener.revokedCount);
        assertEquals(2, rebalanceListener.assignedCount);
    }

    @Test
    public void testRebalanceAfterTopicUnavailableWithSubscribe() {
        unavailableTopicTest(false, Collections.emptySet());
    }

    @Test
    public void testRebalanceAfterTopicUnavailableWithPatternSubscribe() {
        unavailableTopicTest(true, Collections.emptySet());
    }

    @Test
    public void testRebalanceAfterNotMatchingTopicUnavailableWithPatternSubscribe() {
        unavailableTopicTest(true, Collections.singleton("notmatching"));
    }

    private void unavailableTopicTest(boolean patternSubscribe, Set<String> unavailableTopicsInLastMetadata) {
        if (patternSubscribe)
            subscriptions.subscribe(Pattern.compile("test.*"), Optional.of(rebalanceListener));
        else
            subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        client.prepareMetadataUpdate(RequestTestUtils.metadataUpdateWith("kafka-cluster", 1,
                Collections.singletonMap(topic1, Errors.UNKNOWN_TOPIC_OR_PARTITION), Collections.emptyMap()));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(Collections.emptyMap());

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.emptyList(), Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));
        assertFalse(coordinator.rejoinNeededOrPending());
        // callback not triggered since there's nothing to be assigned
        assertEquals(Collections.emptySet(), rebalanceListener.assigned);
        assertTrue(metadata.updateRequested(), "Metadata refresh not requested for unavailable partitions");

        Map<String, Errors> topicErrors = new HashMap<>();
        for (String topic : unavailableTopicsInLastMetadata)
            topicErrors.put(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION);

        client.prepareMetadataUpdate(RequestTestUtils.metadataUpdateWith("kafka-cluster", 1,
                topicErrors, singletonMap(topic1, 1)));

        consumerClient.poll(time.timer(0));
        client.prepareResponse(joinGroupLeaderResponse(2, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(metadata.updateRequested(), "Metadata refresh requested unnecessarily");
        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), rebalanceListener.assigned);
    }

    @Test
    public void testExcludeInternalTopicsConfigOption() {
        testInternalTopicInclusion(false);
    }

    @Test
    public void testIncludeInternalTopicsConfigOption() {
        testInternalTopicInclusion(true);
    }

    private void testInternalTopicInclusion(boolean includeInternalTopics) {
        metadata = new ConsumerMetadata(0, 0, Long.MAX_VALUE, includeInternalTopics,
                false, subscriptions, new LogContext(), new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, false, subscriptions)) {
            subscriptions.subscribe(Pattern.compile(".*"), Optional.of(rebalanceListener));
            Node node = new Node(0, "localhost", 9999);
            MetadataResponse.PartitionMetadata partitionMetadata =
                new MetadataResponse.PartitionMetadata(Errors.NONE, new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0),
                        Optional.of(node.id()), Optional.empty(), singletonList(node.id()), singletonList(node.id()),
                        singletonList(node.id()));
            MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(Errors.NONE,
                Topic.GROUP_METADATA_TOPIC_NAME, true, singletonList(partitionMetadata));

            client.updateMetadata(RequestTestUtils.metadataResponse(singletonList(node), "clusterId", node.id(),
                singletonList(topicMetadata)));
            coordinator.maybeUpdateSubscriptionMetadata();

            assertEquals(includeInternalTopics, subscriptions.subscription().contains(Topic.GROUP_METADATA_TOPIC_NAME));
        }
    }

    @Test
    public void testRejoinGroup() {
        String otherTopic = "otherTopic";
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> assigned = singletonList(t1p);

        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        // join the group once
        joinAsFollowerAndReceiveAssignment(coordinator, assigned);

        assertEquals(0, rebalanceListener.revokedCount);
        assertNull(rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);

        // and join the group again
        rebalanceListener.revoked = null;
        rebalanceListener.assigned = null;
        subscriptions.subscribe(new HashSet<>(Arrays.asList(topic1, otherTopic)), Optional.of(rebalanceListener));
        client.prepareResponse(joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(assigned, Errors.NONE));
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        Collection<TopicPartition> revoked = getRevoked(assigned, assigned);
        Collection<TopicPartition> added = getAdded(assigned, assigned);
        assertEquals(revoked.isEmpty() ? 0 : 1, rebalanceListener.revokedCount);
        assertEquals(revoked.isEmpty() ? null : revoked, rebalanceListener.revoked);
        assertEquals(2, rebalanceListener.assignedCount);
        assertEquals(added, rebalanceListener.assigned);
    }

    @Test
    public void testDisconnectInJoin() {
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> assigned = singletonList(t1p);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // disconnected from original coordinator will cause re-discover and join again
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE), true);
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(assigned, Errors.NONE));
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(Set.copyOf(assigned), subscriptions.assignedPartitions());
        // nothing to be revoked hence callback not triggered
        assertEquals(0, rebalanceListener.revokedCount);
        assertNull(rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);
    }

    @Test
    public void testInvalidSessionTimeout() {
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // coordinator doesn't like the session timeout
        client.prepareResponse(joinGroupFollowerResponse(0, consumerId, "", Errors.INVALID_SESSION_TIMEOUT));
        assertThrows(ApiException.class, () -> coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE)));
    }

    @Test
    public void testCommitOffsetOnly() {
        subscriptions.assignFromUser(singleton(t1p));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);

        AtomicBoolean success = new AtomicBoolean(false);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), callback(success));
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertTrue(success.get());
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
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // Send two async commits and fail the first one with an error.
        // This should cause a coordinator disconnect which will cancel the second request.

        MockCommitCallback firstCommitCallback = new MockCommitCallback();
        MockCommitCallback secondCommitCallback = new MockCommitCallback();
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), firstCommitCallback);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), secondCommitCallback);
        assertEquals(coordinator.inFlightAsyncCommits.get(), 2);

        respondToOffsetCommitRequest(singletonMap(t1p, 100L), error);
        consumerClient.pollNoWakeup();
        consumerClient.pollNoWakeup(); // second poll since coordinator disconnect is async
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertInstanceOf(RetriableCommitFailedException.class, firstCommitCallback.exception);
        assertInstanceOf(RetriableCommitFailedException.class, secondCommitCallback.exception);
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);
    }

    @Test
    public void testAutoCommitDynamicAssignment() {
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true, subscriptions)) {
            subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
            joinAsFollowerAndReceiveAssignment(coordinator, singletonList(t1p));
            subscriptions.seek(t1p, 100);
            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
            time.sleep(autoCommitIntervalMs);
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertFalse(client.hasPendingResponses());
        }
    }

    @Test
    public void testAutoCommitRetryBackoff() {
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true, subscriptions)) {
            subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
            joinAsFollowerAndReceiveAssignment(coordinator, singletonList(t1p));

            subscriptions.seek(t1p, 100);
            time.sleep(autoCommitIntervalMs);

            // Send an offset commit, but let it fail with a retriable error
            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NOT_COORDINATOR);
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertTrue(coordinator.coordinatorUnknown());

            // After the disconnect, we should rediscover the coordinator
            client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
            coordinator.poll(time.timer(Long.MAX_VALUE));

            subscriptions.seek(t1p, 200);

            // Until the retry backoff has expired, we should not retry the offset commit
            time.sleep(retryBackoffMs / 2);
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertEquals(0, client.inFlightRequestCount());

            // Once the backoff expires, we should retry
            time.sleep(retryBackoffMs / 2);
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertEquals(1, client.inFlightRequestCount());
            respondToOffsetCommitRequest(singletonMap(t1p, 200L), Errors.NONE);
        }
    }

    @Test
    public void testAutoCommitAwaitsInterval() {
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true, subscriptions)) {
            subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
            joinAsFollowerAndReceiveAssignment(coordinator, singletonList(t1p));

            subscriptions.seek(t1p, 100);
            time.sleep(autoCommitIntervalMs);

            // Send the offset commit request, but do not respond
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertEquals(1, client.inFlightRequestCount());

            time.sleep(autoCommitIntervalMs / 2);

            // Ensure that no additional offset commit is sent
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertEquals(1, client.inFlightRequestCount());

            respondToOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertEquals(0, client.inFlightRequestCount());

            subscriptions.seek(t1p, 200);

            // If we poll again before the auto-commit interval, there should be no new sends
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertEquals(0, client.inFlightRequestCount());

            // After the remainder of the interval passes, we send a new offset commit
            time.sleep(autoCommitIntervalMs / 2);
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertEquals(1, client.inFlightRequestCount());
            respondToOffsetCommitRequest(singletonMap(t1p, 200L), Errors.NONE);
        }
    }

    @Test
    public void testAutoCommitDynamicAssignmentRebalance() {
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true, subscriptions)) {
            subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
            client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
            coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

            // haven't joined, so should not cause a commit
            time.sleep(autoCommitIntervalMs);
            consumerClient.poll(time.timer(0));

            client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
            client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
            coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

            subscriptions.seek(t1p, 100);

            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
            time.sleep(autoCommitIntervalMs);
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertFalse(client.hasPendingResponses());
        }
    }

    @Test
    public void testAutoCommitManualAssignment() {
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true, subscriptions)) {
            subscriptions.assignFromUser(singleton(t1p));
            subscriptions.seek(t1p, 100);
            client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
            coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
            time.sleep(autoCommitIntervalMs);
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertFalse(client.hasPendingResponses());
        }
    }

    @Test
    public void testAutoCommitManualAssignmentCoordinatorUnknown() {
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true, subscriptions)) {
            subscriptions.assignFromUser(singleton(t1p));
            subscriptions.seek(t1p, 100);

            // no commit initially since coordinator is unknown
            consumerClient.poll(time.timer(0));
            time.sleep(autoCommitIntervalMs);
            consumerClient.poll(time.timer(0));

            // now find the coordinator
            client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
            coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

            // sleep only for the retry backoff
            time.sleep(retryBackoffMs);
            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertFalse(client.hasPendingResponses());
        }
    }

    @Test
    public void testCommitOffsetMetadataAsync() {
        subscriptions.assignFromUser(singleton(t1p));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);

        AtomicBoolean success = new AtomicBoolean(false);

        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(100L, "hello");
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p, offsetAndMetadata);
        coordinator.commitOffsetsAsync(offsets, callback(offsets, success));
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertTrue(success.get());
        assertEquals(0, coordinator.inFlightAsyncCommits.get());
    }

    @Test
    public void testCommitOffsetMetadataSync() {
        subscriptions.assignFromUser(singleton(t1p));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);

        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(100L, "hello");
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p, offsetAndMetadata);
        boolean success = coordinator.commitOffsetsSync(offsets, time.timer(Long.MAX_VALUE));
        assertTrue(success);
    }

    @Test
    public void testCommitOffsetAsyncWithDefaultCallback() {
        int invokedBeforeTest = mockOffsetCommitCallback.invoked;
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), mockOffsetCommitCallback);
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertEquals(invokedBeforeTest + 1, mockOffsetCommitCallback.invoked);
        assertNull(mockOffsetCommitCallback.exception);
    }

    @Test
    public void testCommitAfterLeaveGroup() {
        // enable auto-assignment
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        joinAsFollowerAndReceiveAssignment(coordinator, singletonList(t1p));

        // now switch to manual assignment
        client.prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                .setErrorCode(Errors.NONE.code())));
        subscriptions.unsubscribe();
        coordinator.maybeLeaveGroup("test commit after leave");
        subscriptions.assignFromUser(singleton(t1p));

        // the client should not reuse generation/memberId from auto-subscribed generation
        client.prepareResponse(body -> {
            OffsetCommitRequest commitRequest = (OffsetCommitRequest) body;
            return commitRequest.data().memberId().equals(OffsetCommitRequest.DEFAULT_MEMBER_ID) &&
                    commitRequest.data().generationIdOrMemberEpoch() == OffsetCommitRequest.DEFAULT_GENERATION_ID;
        }, offsetCommitResponse(singletonMap(t1p, Errors.NONE)));

        AtomicBoolean success = new AtomicBoolean(false);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), callback(success));
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertTrue(success.get());
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);
    }

    @Test
    public void testCommitOffsetAsyncFailedWithDefaultCallback() {
        int invokedBeforeTest = mockOffsetCommitCallback.invoked;
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), mockOffsetCommitCallback);
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertEquals(invokedBeforeTest + 1, mockOffsetCommitCallback.invoked);
        assertInstanceOf(RetriableCommitFailedException.class, mockOffsetCommitCallback.exception);
    }

    @Test
    public void testCommitOffsetAsyncCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // async commit with coordinator not available
        MockCommitCallback cb = new MockCommitCallback();
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), cb);
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertInstanceOf(RetriableCommitFailedException.class, cb.exception);
    }

    @Test
    public void testCommitOffsetAsyncNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // async commit with not coordinator
        MockCommitCallback cb = new MockCommitCallback();
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NOT_COORDINATOR);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), cb);
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertInstanceOf(RetriableCommitFailedException.class, cb.exception);
    }

    @Test
    public void testCommitOffsetAsyncDisconnected() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // async commit with coordinator disconnected
        MockCommitCallback cb = new MockCommitCallback();
        prepareOffsetCommitRequestDisconnect(singletonMap(t1p, 100L));
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), cb);
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertInstanceOf(RetriableCommitFailedException.class, cb.exception);
    }

    @Test
    public void testCommitOffsetSyncNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // sync commit with coordinator disconnected (should connect, get metadata, and then submit the commit request)
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NOT_COORDINATOR);
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), time.timer(Long.MAX_VALUE));
    }

    @Test
    public void testCommitOffsetSyncCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // sync commit with coordinator disconnected (should connect, get metadata, and then submit the commit request)
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), time.timer(Long.MAX_VALUE));
    }

    @Test
    public void testCommitOffsetSyncCoordinatorDisconnected() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // sync commit with coordinator disconnected (should connect, get metadata, and then submit the commit request)
        prepareOffsetCommitRequestDisconnect(singletonMap(t1p, 100L));
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), time.timer(Long.MAX_VALUE));
    }

    @Test
    public void testAsyncCommitCallbacksInvokedPriorToSyncCommitCompletion() throws Exception {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        final List<OffsetAndMetadata> committedOffsets = Collections.synchronizedList(new ArrayList<>());
        final OffsetAndMetadata firstOffset = new OffsetAndMetadata(0L);
        final OffsetAndMetadata secondOffset = new OffsetAndMetadata(1L);

        coordinator.commitOffsetsAsync(singletonMap(t1p, firstOffset), new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                committedOffsets.add(firstOffset);
            }
        });

        // Do a synchronous commit in the background so that we can send both responses at the same time
        Thread thread = new Thread() {
            @Override
            public void run() {
                coordinator.commitOffsetsSync(singletonMap(t1p, secondOffset), time.timer(10000));
                committedOffsets.add(secondOffset);
            }
        };

        assertEquals(coordinator.inFlightAsyncCommits.get(), 1);
        thread.start();

        client.waitForRequests(2, 5000);
        respondToOffsetCommitRequest(singletonMap(t1p, firstOffset.offset()), Errors.NONE);
        respondToOffsetCommitRequest(singletonMap(t1p, secondOffset.offset()), Errors.NONE);

        thread.join();
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);

        assertEquals(Arrays.asList(firstOffset, secondOffset), committedOffsets);
    }

    @Test
    public void testRetryCommitUnknownTopicOrPartition() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        client.prepareResponse(offsetCommitResponse(singletonMap(t1p, Errors.UNKNOWN_TOPIC_OR_PARTITION)));
        client.prepareResponse(offsetCommitResponse(singletonMap(t1p, Errors.NONE)));

        assertTrue(coordinator.commitOffsetsSync(singletonMap(t1p,
                new OffsetAndMetadata(100L, "metadata")), time.timer(10000)));
    }

    @Test
    public void testCommitOffsetMetadataTooLarge() {
        // since offset metadata is provided by the user, we have to propagate the exception so they can handle it
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.OFFSET_METADATA_TOO_LARGE);
        assertThrows(OffsetMetadataTooLarge.class, () -> coordinator.commitOffsetsSync(singletonMap(t1p,
                new OffsetAndMetadata(100L, "metadata")), time.timer(Long.MAX_VALUE)));
    }

    @Test
    public void testCommitOffsetIllegalGeneration() {
        // we cannot retry if a rebalance occurs before the commit completed
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.ILLEGAL_GENERATION);
        assertThrows(CommitFailedException.class, () -> coordinator.commitOffsetsSync(singletonMap(t1p,
                new OffsetAndMetadata(100L, "metadata")), time.timer(Long.MAX_VALUE)));
    }

    @Test
    public void testCommitOffsetUnknownMemberId() {
        // we cannot retry if a rebalance occurs before the commit completed
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.UNKNOWN_MEMBER_ID);
        assertThrows(CommitFailedException.class, () -> coordinator.commitOffsetsSync(singletonMap(t1p,
                new OffsetAndMetadata(100L, "metadata")), time.timer(Long.MAX_VALUE)));
    }

    @Test
    public void testCommitOffsetIllegalGenerationWithNewGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        final AbstractCoordinator.Generation currGen = new AbstractCoordinator.Generation(
            1,
            "memberId",
            null);
        coordinator.setNewGeneration(currGen);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.ILLEGAL_GENERATION);
        RequestFuture<Void> future = coordinator.sendOffsetCommitRequest(singletonMap(t1p,
            new OffsetAndMetadata(100L, "metadata")));

        // change the generation
        final AbstractCoordinator.Generation newGen = new AbstractCoordinator.Generation(
            2,
            "memberId-new",
            null);
        coordinator.setNewGeneration(newGen);
        coordinator.setNewState(AbstractCoordinator.MemberState.PREPARING_REBALANCE);

        assertTrue(consumerClient.poll(future, time.timer(30000)));
        assertInstanceOf(future.exception().getClass(), Errors.REBALANCE_IN_PROGRESS.exception());

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation());
    }

    @Test
    public void testCommitOffsetIllegalGenerationShouldResetGenerationId() {
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.emptyList(), Errors.NONE));

        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.ILLEGAL_GENERATION);
        RequestFuture<Void> future = coordinator.sendOffsetCommitRequest(singletonMap(t1p,
            new OffsetAndMetadata(100L, "metadata")));

        assertTrue(consumerClient.poll(future, time.timer(30000)));

        assertEquals(AbstractCoordinator.Generation.NO_GENERATION.generationId, coordinator.generation().generationId);
        assertEquals(AbstractCoordinator.Generation.NO_GENERATION.protocolName, coordinator.generation().protocolName);
        // member ID should not be reset
        assertEquals(consumerId, coordinator.generation().memberId);
    }

    @Test
    public void testCommitOffsetIllegalGenerationWithResetGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        final AbstractCoordinator.Generation currGen = new AbstractCoordinator.Generation(
            1,
            "memberId",
            null);
        coordinator.setNewGeneration(currGen);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.ILLEGAL_GENERATION);
        RequestFuture<Void> future = coordinator.sendOffsetCommitRequest(singletonMap(t1p,
            new OffsetAndMetadata(100L, "metadata")));

        // reset the generation
        coordinator.setNewGeneration(AbstractCoordinator.Generation.NO_GENERATION);

        assertTrue(consumerClient.poll(future, time.timer(30000)));
        assertInstanceOf(future.exception().getClass(), new CommitFailedException());

        // the generation should not be reset
        assertEquals(AbstractCoordinator.Generation.NO_GENERATION, coordinator.generation());
    }

    @Test
    public void testCommitOffsetUnknownMemberWithNewGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        final AbstractCoordinator.Generation currGen = new AbstractCoordinator.Generation(
            1,
            "memberId",
            null);
        coordinator.setNewGeneration(currGen);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.UNKNOWN_MEMBER_ID);
        RequestFuture<Void> future = coordinator.sendOffsetCommitRequest(singletonMap(t1p,
            new OffsetAndMetadata(100L, "metadata")));

        // change the generation
        final AbstractCoordinator.Generation newGen = new AbstractCoordinator.Generation(
            2,
            "memberId-new",
            null);
        coordinator.setNewGeneration(newGen);
        coordinator.setNewState(AbstractCoordinator.MemberState.PREPARING_REBALANCE);

        assertTrue(consumerClient.poll(future, time.timer(30000)));
        assertInstanceOf(future.exception().getClass(), Errors.REBALANCE_IN_PROGRESS.exception());

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation());
    }

    @Test
    public void testCommitOffsetUnknownMemberWithResetGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        final AbstractCoordinator.Generation currGen = new AbstractCoordinator.Generation(
            1,
            "memberId",
            null);
        coordinator.setNewGeneration(currGen);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.UNKNOWN_MEMBER_ID);
        RequestFuture<Void> future = coordinator.sendOffsetCommitRequest(singletonMap(t1p,
            new OffsetAndMetadata(100L, "metadata")));

        // reset the generation
        coordinator.setNewGeneration(AbstractCoordinator.Generation.NO_GENERATION);

        assertTrue(consumerClient.poll(future, time.timer(30000)));
        assertInstanceOf(future.exception().getClass(), new CommitFailedException());

        // the generation should be reset
        assertEquals(AbstractCoordinator.Generation.NO_GENERATION, coordinator.generation());
    }

    @Test
    public void testCommitOffsetUnknownMemberShouldResetToNoGeneration() {
        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.emptyList(), Errors.NONE));

        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.UNKNOWN_MEMBER_ID);
        RequestFuture<Void> future = coordinator.sendOffsetCommitRequest(singletonMap(t1p,
            new OffsetAndMetadata(100L, "metadata")));

        assertTrue(consumerClient.poll(future, time.timer(30000)));

        assertEquals(AbstractCoordinator.Generation.NO_GENERATION, coordinator.generation());
    }

    @Test
    public void testCommitOffsetFencedInstanceWithRebalancingGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        final AbstractCoordinator.Generation currGen = new AbstractCoordinator.Generation(
            1,
            "memberId",
            null);
        coordinator.setNewGeneration(currGen);
        coordinator.setNewState(AbstractCoordinator.MemberState.PREPARING_REBALANCE);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.FENCED_INSTANCE_ID);
        RequestFuture<Void> future = coordinator.sendOffsetCommitRequest(singletonMap(t1p,
            new OffsetAndMetadata(100L, "metadata")));

        // change the generation
        final AbstractCoordinator.Generation newGen = new AbstractCoordinator.Generation(
            2,
            "memberId-new",
            null);
        coordinator.setNewGeneration(newGen);

        assertTrue(consumerClient.poll(future, time.timer(30000)));
        assertInstanceOf(future.exception().getClass(), Errors.REBALANCE_IN_PROGRESS.exception());

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation());
    }

    @Test
    public void testCommitOffsetFencedInstanceWithNewGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        final AbstractCoordinator.Generation currGen = new AbstractCoordinator.Generation(
            1,
            "memberId",
            null);
        coordinator.setNewGeneration(currGen);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.FENCED_INSTANCE_ID);
        RequestFuture<Void> future = coordinator.sendOffsetCommitRequest(singletonMap(t1p,
            new OffsetAndMetadata(100L, "metadata")));

        // change the generation
        final AbstractCoordinator.Generation newGen = new AbstractCoordinator.Generation(
            2,
            "memberId-new",
            null);
        coordinator.setNewGeneration(newGen);

        assertTrue(consumerClient.poll(future, time.timer(30000)));
        assertInstanceOf(future.exception().getClass(), new CommitFailedException());

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation());
    }

    @Test
    public void testCommitOffsetShouldNotSetInstanceIdIfMemberIdIsUnknown() {
        rebalanceConfig = buildRebalanceConfig(groupInstanceId);
        ConsumerCoordinator coordinator = buildCoordinator(
            rebalanceConfig,
            new Metrics(),
            assignors,
            false,
            subscriptions
        );

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(5000));

        client.prepareResponse(body -> {
            OffsetCommitRequestData data = ((OffsetCommitRequest) body).data();
            return data.groupInstanceId() == null && data.memberId().isEmpty();
        }, offsetCommitResponse(Collections.emptyMap()));

        RequestFuture<Void> future = coordinator.sendOffsetCommitRequest(singletonMap(t1p,
            new OffsetAndMetadata(100L, "metadata")));

        assertTrue(consumerClient.poll(future, time.timer(5000)));
        assertFalse(future.failed());
    }

    @Test
    public void testCommitOffsetRebalanceInProgress() {
        // we cannot retry if a rebalance occurs before the commit completed
        final String consumerId = "leader";

        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));

        // ensure metadata is up-to-date for leader
        client.updateMetadata(metadataResponse);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, singletonList(t1p)));

        coordinator.ensureActiveGroup(time.timer(0L));

        assertTrue(coordinator.rejoinNeededOrPending());
        assertNull(coordinator.generationIfStable());

        // when the state is REBALANCING, we would not even send out the request but fail immediately
        assertThrows(RebalanceInProgressException.class, () -> coordinator.commitOffsetsSync(singletonMap(t1p,
            new OffsetAndMetadata(100L, "metadata")), time.timer(Long.MAX_VALUE)));

        final Node coordinatorNode = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        client.respondFrom(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE), coordinatorNode);

        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data().memberId().equals(consumerId) &&
                sync.data().generationId() == 1 &&
                sync.groupAssignments().containsKey(consumerId);
        }, syncGroupResponse(singletonList(t1p), Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        AbstractCoordinator.Generation expectedGeneration = new AbstractCoordinator.Generation(1, consumerId, partitionAssignor.name());
        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(expectedGeneration, coordinator.generationIfStable());

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.REBALANCE_IN_PROGRESS);
        assertThrows(RebalanceInProgressException.class, () -> coordinator.commitOffsetsSync(singletonMap(t1p,
            new OffsetAndMetadata(100L, "metadata")), time.timer(Long.MAX_VALUE)));

        assertTrue(coordinator.rejoinNeededOrPending());
        assertEquals(expectedGeneration, coordinator.generationIfStable());
    }

    @Test
    public void testCommitOffsetSyncCallbackWithNonRetriableException() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // sync commit with invalid partitions should throw if we have no callback
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.UNKNOWN_SERVER_ERROR);
        assertThrows(KafkaException.class, () -> coordinator.commitOffsetsSync(singletonMap(t1p,
            new OffsetAndMetadata(100L)), time.timer(Long.MAX_VALUE)));
    }

    @Test
    public void testCommitOffsetSyncWithoutFutureGetsCompleted() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
        assertFalse(coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), time.timer(0)));
    }

    @Test
    public void testRefreshOffset() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L));
        coordinator.initWithCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(Collections.emptySet(), subscriptions.initializingPartitions());
        assertTrue(subscriptions.hasAllFetchPositions());
        assertEquals(100L, subscriptions.position(t1p).offset);
    }

    @Test
    public void testRefreshOffsetWithValidation() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));

        // Initial leader epoch of 4
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith("kafka-cluster", 1,
                Collections.emptyMap(), singletonMap(topic1, 1), tp -> 4);
        client.updateMetadata(metadataResponse);

        // Load offsets from previous epoch
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L, Optional.of(3)));
        coordinator.initWithCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        // Offset gets loaded, but requires validation
        assertEquals(Collections.emptySet(), subscriptions.initializingPartitions());
        assertFalse(subscriptions.hasAllFetchPositions());
        assertTrue(subscriptions.awaitingValidation(t1p));
        assertEquals(subscriptions.position(t1p).offset, 100L);
        assertNull(subscriptions.validPosition(t1p));
    }

    @Test
    public void testFetchCommittedOffsets() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        long offset = 500L;
        String metadata = "blahblah";
        Optional<Integer> leaderEpoch = Optional.of(15);
        OffsetFetchResponse.PartitionData data = new OffsetFetchResponse.PartitionData(offset, leaderEpoch,
                metadata, Errors.NONE);

        client.prepareResponse(offsetFetchResponse(Errors.NONE, singletonMap(t1p, data)));
        Map<TopicPartition, OffsetAndMetadata> fetchedOffsets = coordinator.fetchCommittedOffsets(singleton(t1p),
                time.timer(Long.MAX_VALUE));

        assertNotNull(fetchedOffsets);
        assertEquals(new OffsetAndMetadata(offset, leaderEpoch, metadata), fetchedOffsets.get(t1p));
    }

    @Test
    public void testTopicAuthorizationFailedInOffsetFetch() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        OffsetFetchResponse.PartitionData data = new OffsetFetchResponse.PartitionData(-1, Optional.empty(),
                "", Errors.TOPIC_AUTHORIZATION_FAILED);

        client.prepareResponse(offsetFetchResponse(Errors.NONE, singletonMap(t1p, data)));
        TopicAuthorizationException exception = assertThrows(TopicAuthorizationException.class, () ->
                coordinator.fetchCommittedOffsets(singleton(t1p), time.timer(Long.MAX_VALUE)));

        assertEquals(singleton(topic1), exception.unauthorizedTopics());
    }

    @Test
    public void testRefreshOffsetsGroupNotAuthorized() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(Errors.GROUP_AUTHORIZATION_FAILED, Collections.emptyMap()));
        try {
            coordinator.initWithCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));
            fail("Expected group authorization error");
        } catch (GroupAuthorizationException e) {
            assertEquals(groupId, e.groupId());
        }
    }

    @Test
    public void testRefreshOffsetWithPendingTransactions() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.UNSTABLE_OFFSET_COMMIT, "", -1L));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L));
        assertEquals(Collections.singleton(t1p), subscriptions.initializingPartitions());
        coordinator.initWithCommittedOffsetsIfNeeded(time.timer(0L));
        assertEquals(Collections.singleton(t1p), subscriptions.initializingPartitions());
        coordinator.initWithCommittedOffsetsIfNeeded(time.timer(0L));

        assertEquals(Collections.emptySet(), subscriptions.initializingPartitions());
        assertTrue(subscriptions.hasAllFetchPositions());
        assertEquals(100L, subscriptions.position(t1p).offset);
    }

    @Test
    public void testRefreshOffsetUnknownTopicOrPartition() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.UNKNOWN_TOPIC_OR_PARTITION, "", 100L));
        assertThrows(KafkaException.class, () -> coordinator.initWithCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE)));
    }

    @ParameterizedTest
    @CsvSource({
        "NOT_COORDINATOR, true",
        "COORDINATOR_NOT_AVAILABLE, true",
        "COORDINATOR_LOAD_IN_PROGRESS, false",
        "NETWORK_EXCEPTION, false",
    })
    public void testRefreshOffsetRetriableErrorCoordinatorLookup(Errors error, boolean expectCoordinatorRelookup) {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(error, Collections.emptyMap()));
        if (expectCoordinatorRelookup) {
            client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        }
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L));
        coordinator.initWithCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(Collections.emptySet(), subscriptions.initializingPartitions());
        assertTrue(subscriptions.hasAllFetchPositions());
        assertEquals(100L, subscriptions.position(t1p).offset);
    }

    @Test
    public void testRefreshOffsetWithNoFetchableOffsets() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", -1L));
        coordinator.initWithCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(Collections.singleton(t1p), subscriptions.initializingPartitions());
        assertEquals(Collections.emptySet(), subscriptions.partitionsNeedingReset(time.milliseconds()));
        assertFalse(subscriptions.hasAllFetchPositions());
        assertNull(subscriptions.position(t1p));
    }

    @Test
    public void testNoCoordinatorDiscoveryIfPositionsKnown() {
        assertTrue(coordinator.coordinatorUnknown());

        subscriptions.assignFromUser(singleton(t1p));
        subscriptions.seek(t1p, 500L);
        coordinator.initWithCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(Collections.emptySet(), subscriptions.initializingPartitions());
        assertTrue(subscriptions.hasAllFetchPositions());
        assertEquals(500L, subscriptions.position(t1p).offset);
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testNoCoordinatorDiscoveryIfPartitionAwaitingReset() {
        assertTrue(coordinator.coordinatorUnknown());

        subscriptions.assignFromUser(singleton(t1p));
        subscriptions.requestOffsetReset(t1p, AutoOffsetResetStrategy.EARLIEST);
        coordinator.initWithCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(Collections.emptySet(), subscriptions.initializingPartitions());
        assertFalse(subscriptions.hasAllFetchPositions());
        assertEquals(Collections.singleton(t1p), subscriptions.partitionsNeedingReset(time.milliseconds()));
        assertEquals(AutoOffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(t1p));
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testAuthenticationFailureInEnsureActiveGroup() {
        client.createPendingAuthenticationError(node, 300);

        try {
            coordinator.ensureActiveGroup();
            fail("Expected an authentication error.");
        } catch (AuthenticationException e) {
            // OK
        }
    }

    @Test
    public void testThreadSafeAssignedPartitionsMetric() throws Exception {
        // Get the assigned-partitions metric
        final Metric metric = metrics.metric(new MetricName("assigned-partitions", consumerId + groupId + "-coordinator-metrics",
                "", Collections.emptyMap()));

        // Start polling the metric in the background
        final AtomicBoolean doStop = new AtomicBoolean();
        final AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        final AtomicInteger observedSize = new AtomicInteger();

        Thread poller = new Thread() {
            @Override
            public void run() {
                // Poll as fast as possible to reproduce ConcurrentModificationException
                while (!doStop.get()) {
                    try {
                        int size = ((Double) metric.metricValue()).intValue();
                        observedSize.set(size);
                    } catch (Exception e) {
                        exceptionHolder.set(e);
                        return;
                    }
                }
            }
        };
        poller.start();

        // Assign two partitions to trigger a metric change that can lead to ConcurrentModificationException
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // Change the assignment several times to increase likelihood of concurrent updates
        Set<TopicPartition> partitions = new HashSet<>();
        int totalPartitions = 10;
        for (int partition = 0; partition < totalPartitions; partition++) {
            partitions.add(new TopicPartition(topic1, partition));
            subscriptions.assignFromUser(partitions);
        }

        // Wait for the metric poller to observe the final assignment change or raise an error
        TestUtils.waitForCondition(
            () -> observedSize.get() == totalPartitions ||
            exceptionHolder.get() != null, "Failed to observe expected assignment change");

        doStop.set(true);
        poller.join();

        assertNull(exceptionHolder.get(), "Failed fetching the metric at least once");
    }

    @Test
    public void testCloseDynamicAssignment() {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, Optional.empty(), true)) {
            gracefulCloseTest(coordinator, true);
        }
    }

    @Test
    public void testCloseManualAssignment() {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(false, true, Optional.empty(), true)) {
            gracefulCloseTest(coordinator, false);
        }
    }

    @Test
    public void testCloseCoordinatorNotKnownManualAssignment() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(false, true, Optional.empty(), true)) {
            makeCoordinatorUnknown(coordinator, Errors.NOT_COORDINATOR);
            time.sleep(autoCommitIntervalMs);
            closeVerifyTimeout(coordinator, 1000, 1000, 1000);
        }
    }

    @Test
    public void testCloseCoordinatorNotKnownNoCommits() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, Optional.empty(), true)) {
            makeCoordinatorUnknown(coordinator, Errors.NOT_COORDINATOR);
            closeVerifyTimeout(coordinator, 1000, 0, 0);
        }
    }

    @Test
    public void testCloseCoordinatorNotKnownWithCommits() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, Optional.empty(), true)) {
            makeCoordinatorUnknown(coordinator, Errors.NOT_COORDINATOR);
            time.sleep(autoCommitIntervalMs);
            closeVerifyTimeout(coordinator, 1000, 1000, 1000);
        }
    }

    @Test
    public void testCloseCoordinatorUnavailableNoCommits() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, Optional.empty(), true)) {
            makeCoordinatorUnknown(coordinator, Errors.COORDINATOR_NOT_AVAILABLE);
            closeVerifyTimeout(coordinator, 1000, 0, 0);
        }
    }

    @Test
    public void testCloseTimeoutCoordinatorUnavailableForCommit() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, groupInstanceId, true)) {
            makeCoordinatorUnknown(coordinator, Errors.COORDINATOR_NOT_AVAILABLE);
            time.sleep(autoCommitIntervalMs);
            closeVerifyTimeout(coordinator, 1000, 1000, 1000);
        }
    }

    @Test
    public void testCloseMaxWaitCoordinatorUnavailableForCommit() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, groupInstanceId, true)) {
            makeCoordinatorUnknown(coordinator, Errors.COORDINATOR_NOT_AVAILABLE);
            time.sleep(autoCommitIntervalMs);
            closeVerifyTimeout(coordinator, Long.MAX_VALUE, requestTimeoutMs, requestTimeoutMs);
        }
    }

    @Test
    public void testCloseNoResponseForCommit() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, groupInstanceId, true)) {
            time.sleep(autoCommitIntervalMs);
            closeVerifyTimeout(coordinator, Long.MAX_VALUE, requestTimeoutMs, requestTimeoutMs);
        }
    }

    @Test
    public void testCloseNoResponseForLeaveGroup() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, Optional.empty(), true)) {
            closeVerifyTimeout(coordinator, Long.MAX_VALUE, requestTimeoutMs, requestTimeoutMs);
        }
    }

    @Test
    public void testCloseNoWait() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, groupInstanceId, true)) {
            time.sleep(autoCommitIntervalMs);
            closeVerifyTimeout(coordinator, 0, 0, 0);
        }
    }

    @Test
    public void testHeartbeatThreadClose() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, groupInstanceId, true)) {
            coordinator.ensureActiveGroup();
            time.sleep(heartbeatIntervalMs + 100);
            Thread.yield(); // Give heartbeat thread a chance to attempt heartbeat
            closeVerifyTimeout(coordinator, Long.MAX_VALUE, requestTimeoutMs, requestTimeoutMs);
            Thread[] threads = new Thread[Thread.activeCount()];
            int threadCount = Thread.enumerate(threads);
            for (int i = 0; i < threadCount; i++) {
                assertFalse(threads[i].getName().contains(groupId), "Heartbeat thread active after close");
            }
        }
    }

    @Test
    public void testAutoCommitAfterCoordinatorBackToService() {
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true, subscriptions)) {
            subscriptions.assignFromUser(Collections.singleton(t1p));
            subscriptions.seek(t1p, 100L);

            coordinator.markCoordinatorUnknown("test cause");
            assertTrue(coordinator.coordinatorUnknown());
            client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);

            // async commit offset should find coordinator
            time.sleep(autoCommitIntervalMs); // sleep for a while to ensure auto commit does happen
            coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());
            assertFalse(coordinator.coordinatorUnknown());
            assertEquals(100L, subscriptions.position(t1p).offset);
        }
    }

    @Test
    public void testCommitOffsetRequestSyncWithFencedInstanceIdException() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // sync commit with invalid partitions should throw if we have no callback
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.FENCED_INSTANCE_ID);
        assertThrows(FencedInstanceIdException.class, () -> coordinator.commitOffsetsSync(singletonMap(t1p,
            new OffsetAndMetadata(100L)), time.timer(Long.MAX_VALUE)));
    }

    @Test
    public void testCommitOffsetRequestAsyncWithFencedInstanceIdException() {
        assertThrows(FencedInstanceIdException.class, this::receiveFencedInstanceIdException);
    }

    @Test
    public void testCommitOffsetRequestAsyncAlwaysReceiveFencedException() {
        // Once we get fenced exception once, we should always hit fencing case.
        assertThrows(FencedInstanceIdException.class, this::receiveFencedInstanceIdException);
        assertThrows(FencedInstanceIdException.class, () ->
                coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), new MockCommitCallback()));
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);
        assertThrows(FencedInstanceIdException.class, () ->
                coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), time.timer(Long.MAX_VALUE)));
    }

    @Test
    public void testGetGroupMetadata() {
        final ConsumerGroupMetadata groupMetadata = coordinator.groupMetadata();
        assertNotNull(groupMetadata);
        assertEquals(groupId, groupMetadata.groupId());
        assertEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, groupMetadata.generationId());
        assertEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, groupMetadata.memberId());
        assertFalse(groupMetadata.groupInstanceId().isPresent());

        try (final ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, groupInstanceId, true)) {
            coordinator.ensureActiveGroup();

            final ConsumerGroupMetadata joinedGroupMetadata = coordinator.groupMetadata();
            assertNotNull(joinedGroupMetadata);
            assertEquals(groupId, joinedGroupMetadata.groupId());
            assertEquals(1, joinedGroupMetadata.generationId());
            assertEquals(consumerId, joinedGroupMetadata.memberId());
            assertEquals(groupInstanceId, joinedGroupMetadata.groupInstanceId());
        }
    }

    @Test
    public void shouldUpdateConsumerGroupMetadataBeforeCallbacks() {
        final MockRebalanceListener rebalanceListener = new MockRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                assertEquals(2, coordinator.groupMetadata().generationId());
            }
        };

        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(
                new ConsumerPartitionAssignor.Assignment(Collections.singletonList(t1p), ByteBuffer.wrap(new byte[0])));
            coordinator.onJoinComplete(1, "memberId", partitionAssignor.name(), buffer);
        }

        ByteBuffer buffer = ConsumerProtocol.serializeAssignment(
            new ConsumerPartitionAssignor.Assignment(Collections.emptyList(), ByteBuffer.wrap(new byte[0])));
        coordinator.onJoinComplete(2, "memberId", partitionAssignor.name(), buffer);
    }

    @Test
    public void testPrepareJoinAndRejoinAfterFailedRebalance() {
        final List<TopicPartition> partitions = singletonList(t1p);
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, Optional.of("group-id"), true)) {
            coordinator.ensureActiveGroup();

            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.REBALANCE_IN_PROGRESS);

            assertThrows(RebalanceInProgressException.class, () -> coordinator.commitOffsetsSync(
                singletonMap(t1p, new OffsetAndMetadata(100L)),
                time.timer(Long.MAX_VALUE)));

            assertFalse(client.hasPendingResponses());
            assertFalse(client.hasInFlightRequests());

            int generationId = 42;
            String memberId = "consumer-42";

            client.prepareResponse(joinGroupFollowerResponse(generationId, memberId, "leader", Errors.NONE));

            MockTime time = new MockTime(1);

            // onJoinPrepare will be executed and onJoinComplete will not.
            boolean res = coordinator.joinGroupIfNeeded(time.timer(100));

            assertFalse(res);
            assertFalse(client.hasPendingResponses());
            // SynGroupRequest not responded.
            assertEquals(1, client.inFlightRequestCount());
            assertEquals(generationId, coordinator.generation().generationId);
            assertEquals(memberId, coordinator.generation().memberId);

            // Imitating heartbeat thread that clears generation data.
            coordinator.maybeLeaveGroup("Clear generation data.");

            assertEquals(AbstractCoordinator.Generation.NO_GENERATION, coordinator.generation());

            client.respond(syncGroupResponse(partitions, Errors.NONE));

            // Join future should succeed but generation already cleared so result of join is false.
            res = coordinator.joinGroupIfNeeded(time.timer(1));

            assertFalse(res);

            // should have retried sending a join group request already
            assertFalse(client.hasPendingResponses());
            assertEquals(1, client.inFlightRequestCount());

            // Retry join should then succeed
            client.respond(joinGroupFollowerResponse(generationId, memberId, "leader", Errors.NONE));
            client.prepareResponse(syncGroupResponse(partitions, Errors.NONE));

            res = coordinator.joinGroupIfNeeded(time.timer(3000));

            assertTrue(res);
            assertFalse(client.hasPendingResponses());
            assertFalse(client.hasInFlightRequests());
        }
        Collection<TopicPartition> lost = getLost(partitions);
        assertEquals(lost.isEmpty() ? null : lost, rebalanceListener.lost);
        assertEquals(lost.size(), rebalanceListener.lostCount);
    }

    @Test
    public void shouldLoseAllOwnedPartitionsBeforeRejoiningAfterDroppingOutOfTheGroup() {
        final List<TopicPartition> partitions = singletonList(t1p);
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, Optional.of("group-id"), true)) {
            final Time realTime = Time.SYSTEM;
            coordinator.ensureActiveGroup();

            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.REBALANCE_IN_PROGRESS);

            assertThrows(RebalanceInProgressException.class, () -> coordinator.commitOffsetsSync(
                singletonMap(t1p, new OffsetAndMetadata(100L)),
                time.timer(Long.MAX_VALUE)));

            int generationId = 42;
            String memberId = "consumer-42";

            client.prepareResponse(joinGroupFollowerResponse(generationId, memberId, "leader", Errors.NONE));
            client.prepareResponse(syncGroupResponse(Collections.emptyList(), Errors.UNKNOWN_MEMBER_ID));

            boolean res = coordinator.joinGroupIfNeeded(realTime.timer(1000));

            assertFalse(res);
            assertEquals(AbstractCoordinator.Generation.NO_GENERATION, coordinator.generation());
            assertEquals("", coordinator.generation().memberId);

            res = coordinator.joinGroupIfNeeded(realTime.timer(1000));
            assertFalse(res);
        }
        Collection<TopicPartition> lost = getLost(partitions);
        assertEquals(lost.isEmpty() ? 0 : 1, rebalanceListener.lostCount);
        assertEquals(lost.isEmpty() ? null : lost, rebalanceListener.lost);
    }

    @Test
    public void shouldLoseAllOwnedPartitionsBeforeRejoiningAfterResettingGenerationId() {
        final List<TopicPartition> partitions = singletonList(t1p);
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, Optional.of("group-id"), true)) {
            final Time realTime = Time.SYSTEM;
            coordinator.ensureActiveGroup();

            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.REBALANCE_IN_PROGRESS);

            assertThrows(RebalanceInProgressException.class, () -> coordinator.commitOffsetsSync(
                singletonMap(t1p, new OffsetAndMetadata(100L)),
                time.timer(Long.MAX_VALUE)));

            int generationId = 42;
            String memberId = "consumer-42";

            client.prepareResponse(joinGroupFollowerResponse(generationId, memberId, "leader", Errors.NONE));
            client.prepareResponse(syncGroupResponse(Collections.emptyList(), Errors.ILLEGAL_GENERATION));

            boolean res = coordinator.joinGroupIfNeeded(realTime.timer(1000));

            assertFalse(res);
            assertEquals(AbstractCoordinator.Generation.NO_GENERATION.generationId, coordinator.generation().generationId);
            assertEquals(AbstractCoordinator.Generation.NO_GENERATION.protocolName, coordinator.generation().protocolName);
            // member ID should not be reset
            assertEquals(memberId, coordinator.generation().memberId);

            res = coordinator.joinGroupIfNeeded(realTime.timer(1000));
            assertFalse(res);
        }
        Collection<TopicPartition> lost = getLost(partitions);
        assertEquals(lost.isEmpty() ? 0 : 1, rebalanceListener.lostCount);
        assertEquals(lost.isEmpty() ? null : lost, rebalanceListener.lost);
    }

    @Test
    public void testSubscriptionRackId() {

        String rackId = "rack-a";
        RackAwareAssignor assignor = new RackAwareAssignor(protocol);
        createRackAwareCoordinator(rackId, assignor);

        subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
        client.updateMetadata(metadataResponse);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        assignor.prepare(singletonMap(consumerId, singletonList(t1p)));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, false, Errors.NONE, Optional.of(rackId)));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.poll(time.timer(Long.MAX_VALUE));
        assertEquals(singleton(t1p), coordinator.subscriptionState().assignedPartitions());
        assertEquals(singleton(rackId), assignor.rackIds);
    }

    @Test
    public void testThrowOnUnsupportedStableFlag() {
        supportStableFlag((short) 6, true);
    }

    @Test
    public void testNoThrowWhenStableFlagIsSupported() {
        supportStableFlag((short) 7, false);
    }

    private void supportStableFlag(final short upperVersion, final boolean expectThrows) {
        ConsumerCoordinator coordinator = new ConsumerCoordinator(
            rebalanceConfig,
            new LogContext(),
            consumerClient,
            assignors,
            metadata,
            subscriptions,
            new Metrics(time),
            consumerId + groupId,
            time,
            false,
            autoCommitIntervalMs,
            null,
            true,
            null,
            Optional.empty());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        client.setNodeApiVersions(NodeApiVersions.create(ApiKeys.OFFSET_FETCH.id, (short) 0, upperVersion));

        long offset = 500L;
        String metadata = "blahblah";
        Optional<Integer> leaderEpoch = Optional.of(15);
        OffsetFetchResponse.PartitionData data = new OffsetFetchResponse.PartitionData(offset, leaderEpoch,
            metadata, Errors.NONE);

        if (upperVersion < 8) {
            client.prepareResponse(new OffsetFetchResponse(Errors.NONE, singletonMap(t1p, data)));
        } else {
            client.prepareResponse(offsetFetchResponse(Errors.NONE, singletonMap(t1p, data)));
        }
        if (expectThrows) {
            assertThrows(UnsupportedVersionException.class,
                () -> coordinator.fetchCommittedOffsets(singleton(t1p), time.timer(Long.MAX_VALUE)));
        } else {
            Map<TopicPartition, OffsetAndMetadata> fetchedOffsets = coordinator.fetchCommittedOffsets(singleton(t1p),
                time.timer(Long.MAX_VALUE));

            assertNotNull(fetchedOffsets);
            assertEquals(new OffsetAndMetadata(offset, leaderEpoch, metadata), fetchedOffsets.get(t1p));
        }
    }

    private void receiveFencedInstanceIdException() {
        subscriptions.assignFromUser(singleton(t1p));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.FENCED_INSTANCE_ID);

        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), new MockCommitCallback());
        assertEquals(coordinator.inFlightAsyncCommits.get(), 0);
        coordinator.invokeCompletedOffsetCommitCallbacks();
    }

    private ConsumerCoordinator prepareCoordinatorForCloseTest(final boolean useGroupManagement,
                                                               final boolean autoCommit,
                                                               final Optional<String> groupInstanceId,
                                                               final boolean shouldPoll) {
        rebalanceConfig = buildRebalanceConfig(groupInstanceId);
        ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig,
                                                           new Metrics(),
                                                           assignors,
                                                           autoCommit,
                                                           subscriptions);
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
        if (useGroupManagement) {
            subscriptions.subscribe(singleton(topic1), Optional.of(rebalanceListener));
            client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
            client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
            coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));
        } else {
            subscriptions.assignFromUser(singleton(t1p));
        }

        subscriptions.seek(t1p, 100);
        if (shouldPoll) {
            coordinator.poll(time.timer(Long.MAX_VALUE));
        }

        return coordinator;
    }

    private void makeCoordinatorUnknown(ConsumerCoordinator coordinator, Errors error) {
        time.sleep(sessionTimeoutMs);
        coordinator.sendHeartbeatRequest();
        client.prepareResponse(heartbeatResponse(error));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(time.timer(0));
        assertTrue(coordinator.coordinatorUnknown());
    }

    private void closeVerifyTimeout(final ConsumerCoordinator coordinator,
                                    final long closeTimeoutMs,
                                    final long expectedMinTimeMs,
                                    final long expectedMaxTimeMs) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            boolean coordinatorUnknown = coordinator.coordinatorUnknown();
            // Run close on a different thread. Coordinator is locked by this thread, so it is
            // not safe to use the coordinator from the main thread until the task completes.
            Future<?> future = executor.submit(
                () -> coordinator.close(time.timer(Math.min(closeTimeoutMs, requestTimeoutMs))));
            // Wait for close to start. If coordinator is known, wait for close to queue
            // at least one request. Otherwise, sleep for a short time.
            if (!coordinatorUnknown)
                client.waitForRequests(1, 1000);
            else
                Thread.sleep(200);
            if (expectedMinTimeMs > 0) {
                time.sleep(expectedMinTimeMs - 1);
                try {
                    future.get(500, TimeUnit.MILLISECONDS);
                    fail("Close completed ungracefully without waiting for timeout");
                } catch (TimeoutException e) {
                    // Expected timeout
                }
            }
            if (expectedMaxTimeMs >= 0)
                time.sleep(expectedMaxTimeMs - expectedMinTimeMs + 2);
            future.get(2000, TimeUnit.MILLISECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    private void gracefulCloseTest(ConsumerCoordinator coordinator, boolean shouldLeaveGroup) {
        final AtomicBoolean commitRequested = new AtomicBoolean();
        final AtomicBoolean leaveGroupRequested = new AtomicBoolean();
        client.prepareResponse(body -> {
            commitRequested.set(true);
            OffsetCommitRequest commitRequest = (OffsetCommitRequest) body;
            return commitRequest.data().groupId().equals(groupId);
        }, new OffsetCommitResponse(new OffsetCommitResponseData()));
        if (shouldLeaveGroup)
            client.prepareResponse(body -> {
                leaveGroupRequested.set(true);
                LeaveGroupRequest leaveRequest = (LeaveGroupRequest) body;
                return leaveRequest.data().groupId().equals(groupId);
            }, new LeaveGroupResponse(new LeaveGroupResponseData()
                    .setErrorCode(Errors.NONE.code())));
        client.prepareResponse(body -> {
            commitRequested.set(true);
            OffsetCommitRequest commitRequest = (OffsetCommitRequest) body;
            return commitRequest.data().groupId().equals(groupId);
        }, new OffsetCommitResponse(new OffsetCommitResponseData()));

        coordinator.close();
        assertTrue(commitRequested.get(), "Commit not requested");
        assertEquals(shouldLeaveGroup, leaveGroupRequested.get(), "leaveGroupRequested should be " + shouldLeaveGroup);

        if (shouldLeaveGroup) {
            assertEquals(1, rebalanceListener.revokedCount);
            assertEquals(singleton(t1p), rebalanceListener.revoked);
        }
    }

    private ConsumerCoordinator buildCoordinator(final GroupRebalanceConfig rebalanceConfig,
                                                 final Metrics metrics,
                                                 final List<ConsumerPartitionAssignor> assignors,
                                                 final boolean autoCommitEnabled,
                                                 final SubscriptionState subscriptionState) {
        return new ConsumerCoordinator(
                rebalanceConfig,
                new LogContext(),
                consumerClient,
                assignors,
                metadata,
                subscriptionState,
                metrics,
                consumerId + groupId,
                time,
                autoCommitEnabled,
                autoCommitIntervalMs,
                null,
                false,
                null,
                Optional.empty());
    }

    private Collection<TopicPartition> getRevoked(final List<TopicPartition> owned,
                                                  final List<TopicPartition> assigned) {
        switch (protocol) {
            case EAGER:
                return Set.copyOf(owned);
            case COOPERATIVE:
                final List<TopicPartition> revoked = new ArrayList<>(owned);
                revoked.removeAll(assigned);
                return Set.copyOf(revoked);
            default:
                throw new IllegalStateException("This should not happen");
        }
    }

    private Collection<TopicPartition> getLost(final List<TopicPartition> owned) {
        switch (protocol) {
            case EAGER:
                return emptySet();
            case COOPERATIVE:
                return Set.copyOf(owned);
            default:
                throw new IllegalStateException("This should not happen");
        }
    }

    private Collection<TopicPartition> getAdded(final List<TopicPartition> owned,
                                                final List<TopicPartition> assigned) {
        switch (protocol) {
            case EAGER:
                return Set.copyOf(assigned);
            case COOPERATIVE:
                final List<TopicPartition> added = new ArrayList<>(assigned);
                added.removeAll(owned);
                return Set.copyOf(added);
            default:
                throw new IllegalStateException("This should not happen");
        }
    }

    private FindCoordinatorResponse groupCoordinatorResponse(Node node, Errors error) {
        return FindCoordinatorResponse.prepareResponse(error, groupId, node);
    }

    private HeartbeatResponse heartbeatResponse(Errors error) {
        return new HeartbeatResponse(new HeartbeatResponseData().setErrorCode(error.code()));
    }

    private JoinGroupResponse joinGroupLeaderResponse(
        int generationId,
        String memberId,
        Map<String, List<String>> subscriptions,
        Errors error
    ) {
        return joinGroupLeaderResponse(generationId, memberId, subscriptions, false, error, Optional.empty());
    }

    private JoinGroupResponse joinGroupLeaderResponse(
        int generationId,
        String memberId,
        Map<String, List<String>> subscriptions,
        boolean skipAssignment,
        Errors error,
        Optional<String> rackId
    ) {
        List<JoinGroupResponseData.JoinGroupResponseMember> metadata = new ArrayList<>();
        for (Map.Entry<String, List<String>> subscriptionEntry : subscriptions.entrySet()) {
            ConsumerPartitionAssignor.Subscription subscription = new ConsumerPartitionAssignor.Subscription(subscriptionEntry.getValue(),
                    null, Collections.emptyList(), DEFAULT_GENERATION, rackId);
            ByteBuffer buf = ConsumerProtocol.serializeSubscription(subscription);
            metadata.add(new JoinGroupResponseData.JoinGroupResponseMember()
                    .setMemberId(subscriptionEntry.getKey())
                    .setMetadata(buf.array()));
        }

        return new JoinGroupResponse(
                new JoinGroupResponseData()
                        .setErrorCode(error.code())
                        .setGenerationId(generationId)
                        .setProtocolName(partitionAssignor.name())
                        .setLeader(memberId)
                        .setSkipAssignment(skipAssignment)
                        .setMemberId(memberId)
                        .setMembers(metadata),
                ApiKeys.JOIN_GROUP.latestVersion()
        );
    }

    private JoinGroupResponse joinGroupFollowerResponse(int generationId, String memberId, String leaderId, Errors error) {
        return new JoinGroupResponse(
                new JoinGroupResponseData()
                        .setErrorCode(error.code())
                        .setGenerationId(generationId)
                        .setProtocolName(partitionAssignor.name())
                        .setLeader(leaderId)
                        .setMemberId(memberId)
                        .setMembers(Collections.emptyList()),
                ApiKeys.JOIN_GROUP.latestVersion()
        );
    }

    private SyncGroupResponse syncGroupResponse(List<TopicPartition> partitions, Errors error) {
        ByteBuffer buf = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(partitions));
        return new SyncGroupResponse(
                new SyncGroupResponseData()
                        .setErrorCode(error.code())
                        .setAssignment(Utils.toArray(buf))
        );
    }

    private OffsetCommitResponse offsetCommitResponse(Map<TopicPartition, Errors> responseData) {
        return new OffsetCommitResponse(responseData);
    }

    private OffsetFetchResponse offsetFetchResponse(Errors error, Map<TopicPartition, PartitionData> responseData) {
        return new OffsetFetchResponse(throttleMs,
                                       singletonMap(groupId, error),
                                       singletonMap(groupId, responseData));
    }

    private OffsetFetchResponse offsetFetchResponse(TopicPartition tp, Errors partitionLevelError, String metadata, long offset) {
        return offsetFetchResponse(tp, partitionLevelError, metadata, offset, Optional.empty());
    }

    private OffsetFetchResponse offsetFetchResponse(TopicPartition tp, Errors partitionLevelError, String metadata, long offset, Optional<Integer> epoch) {
        OffsetFetchResponse.PartitionData data = new OffsetFetchResponse.PartitionData(offset,
                epoch, metadata, partitionLevelError);
        return offsetFetchResponse(Errors.NONE, singletonMap(tp, data));
    }

    private OffsetCommitCallback callback(final AtomicBoolean success) {
        return (offsets, exception) -> {
            if (exception == null)
                success.set(true);
        };
    }

    private void joinAsFollowerAndReceiveAssignment(ConsumerCoordinator coordinator,
                                                    List<TopicPartition> assignment) {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(assignment, Errors.NONE));
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));
    }

    private void prepareOffsetCommitRequest(Map<TopicPartition, Long> expectedOffsets, Errors error) {
        prepareOffsetCommitRequest(expectedOffsets, error, false);
    }

    private void prepareOffsetCommitRequestDisconnect(Map<TopicPartition, Long> expectedOffsets) {
        prepareOffsetCommitRequest(expectedOffsets, Errors.NONE, true);
    }

    private void prepareOffsetCommitRequest(final Map<TopicPartition, Long> expectedOffsets,
                                            Errors error,
                                            boolean disconnected) {
        Map<TopicPartition, Errors> errors = partitionErrors(expectedOffsets.keySet(), error);
        client.prepareResponse(offsetCommitRequestMatcher(expectedOffsets), offsetCommitResponse(errors), disconnected);
    }

    private void prepareJoinAndSyncResponse(String consumerId, int generation, List<String> subscription, List<TopicPartition> assignment) {
        partitionAssignor.prepare(singletonMap(consumerId, assignment));
        client.prepareResponse(
                joinGroupLeaderResponse(
                        generation, consumerId, singletonMap(consumerId, subscription), Errors.NONE));
        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data().memberId().equals(consumerId) &&
                    sync.data().generationId() == generation &&
                    sync.groupAssignments().containsKey(consumerId);
        }, syncGroupResponse(assignment, Errors.NONE));
    }

    private Map<TopicPartition, Errors> partitionErrors(Collection<TopicPartition> partitions, Errors error) {
        final Map<TopicPartition, Errors> errors = new HashMap<>();
        for (TopicPartition partition : partitions) {
            errors.put(partition, error);
        }
        return errors;
    }

    private void respondToOffsetCommitRequest(final Map<TopicPartition, Long> expectedOffsets, Errors error) {
        Map<TopicPartition, Errors> errors = partitionErrors(expectedOffsets.keySet(), error);
        client.respond(offsetCommitRequestMatcher(expectedOffsets), offsetCommitResponse(errors));
    }

    private MockClient.RequestMatcher offsetCommitRequestMatcher(final Map<TopicPartition, Long> expectedOffsets) {
        return body -> {
            OffsetCommitRequest req = (OffsetCommitRequest) body;
            Map<TopicPartition, Long> offsets = req.offsets();
            if (offsets.size() != expectedOffsets.size())
                return false;

            for (Map.Entry<TopicPartition, Long> expectedOffset : expectedOffsets.entrySet()) {
                if (!offsets.containsKey(expectedOffset.getKey())) {
                    return false;
                } else {
                    Long actualOffset = offsets.get(expectedOffset.getKey());
                    if (!actualOffset.equals(expectedOffset.getValue())) {
                        return false;
                    }
                }
            }
            return true;
        };
    }

    private OffsetCommitCallback callback(final Map<TopicPartition, OffsetAndMetadata> expectedOffsets,
                                          final AtomicBoolean success) {
        return (offsets, exception) -> {
            if (expectedOffsets.equals(offsets) && exception == null)
                success.set(true);
        };
    }

    private void createRackAwareCoordinator(String rackId, MockPartitionAssignor assignor) {
        metrics.close();
        coordinator.close(time.timer(0));

        metrics = new Metrics(time);

        coordinator = new ConsumerCoordinator(rebalanceConfig, new LogContext(), consumerClient,
                Collections.singletonList(assignor), metadata, subscriptions,
                metrics, consumerId + groupId, time, false, autoCommitIntervalMs, null, false, rackId, Optional.empty());
    }

    private static MetadataResponse rackAwareMetadata(int numNodes,
                                                      List<String> racks,
                                                      Map<String, List<List<Integer>>> partitionReplicas) {
        final List<Node> nodes = new ArrayList<>(numNodes);
        for (int i = 0; i < numNodes; i++)
            nodes.add(new Node(i, "localhost", 1969 + i, racks.get(i % racks.size())));

        List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
        for (Map.Entry<String, List<List<Integer>>> topicPartitionCountEntry : partitionReplicas.entrySet()) {
            String topic = topicPartitionCountEntry.getKey();
            int numPartitions = topicPartitionCountEntry.getValue().size();

            List<MetadataResponse.PartitionMetadata> partitionMetadata = new ArrayList<>(numPartitions);
            for (int i = 0; i < numPartitions; i++) {
                TopicPartition tp = new TopicPartition(topic, i);
                List<Integer> replicaIds = topicPartitionCountEntry.getValue().get(i);
                partitionMetadata.add(new PartitionMetadata(
                        Errors.NONE, tp, Optional.of(replicaIds.get(0)), Optional.empty(),
                        replicaIds, replicaIds, Collections.emptyList()));
            }

            topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, topic, Uuid.ZERO_UUID,
                    Topic.isInternal(topic), partitionMetadata, MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED));
        }

        return RequestTestUtils.metadataResponse(nodes, "kafka-cluster", 0, topicMetadata, ApiKeys.METADATA.latestVersion());
    }

    private static class MockCommitCallback implements OffsetCommitCallback {
        public int invoked = 0;
        public Exception exception = null;

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            invoked++;
            this.exception = exception;
        }
    }

    private static class RackAwareAssignor extends MockPartitionAssignor {
        private final Set<String> rackIds = new HashSet<>();

        RackAwareAssignor(RebalanceProtocol rebalanceProtocol) {
            super(Collections.singletonList(rebalanceProtocol));
        }

        @Override
        public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
            subscriptions.forEach((consumer, subscription) -> {
                if (!subscription.rackId().isPresent())
                    throw new IllegalStateException("Rack id not provided in subscription for " + consumer);
                rackIds.add(subscription.rackId().get());
            });
            return super.assign(partitionsPerTopic, subscriptions);
        }
    }
}
