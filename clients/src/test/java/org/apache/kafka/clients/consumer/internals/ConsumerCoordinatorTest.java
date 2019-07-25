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
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.test.TestUtils.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(value = Parameterized.class)
public class ConsumerCoordinatorTest {
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
    private final int autoCommitIntervalMs = 2000;
    private final int requestTimeoutMs = 30000;
    private final MockTime time = new MockTime();
    private GroupRebalanceConfig rebalanceConfig;

    private final PartitionAssignor.RebalanceProtocol protocol;
    private final MockPartitionAssignor partitionAssignor;
    private final List<PartitionAssignor> assignors;
    private MockClient client;
    private MetadataResponse metadataResponse = TestUtils.metadataUpdateWith(1, new HashMap<String, Integer>() {
        {
            put(topic1, 1);
            put(topic2, 1);
        }
    });
    private Node node = metadataResponse.brokers().iterator().next();
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private Metrics metrics;
    private ConsumerNetworkClient consumerClient;
    private MockRebalanceListener rebalanceListener;
    private MockCommitCallback mockOffsetCommitCallback;
    private ConsumerCoordinator coordinator;

    public ConsumerCoordinatorTest(final PartitionAssignor.RebalanceProtocol protocol) {
        this.protocol = protocol;
        this.partitionAssignor = new MockPartitionAssignor(Collections.singletonList(protocol));
        this.assignors = Collections.singletonList(partitionAssignor);
    }

    @Parameterized.Parameters(name = "rebalance protocol = {0}")
    public static Collection<Object[]> data() {
        final List<Object[]> values = new ArrayList<>();
        for (final PartitionAssignor.RebalanceProtocol protocol: PartitionAssignor.RebalanceProtocol.values()) {
            values.add(new Object[]{protocol});
        }
        return values;
    }

    @Before
    public void setup() {
        LogContext logContext = new LogContext();
        this.subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        this.metadata = new ConsumerMetadata(0, Long.MAX_VALUE, false,
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
                                            false);
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

    @After
    public void teardown() {
        this.metrics.close();
        this.coordinator.close(time.timer(0));
    }

    @Test
    public void testSelectRebalanceProtcol() {
        List<PartitionAssignor> assignors = new ArrayList<>();
        assignors.add(new MockPartitionAssignor(Collections.singletonList(PartitionAssignor.RebalanceProtocol.EAGER)));
        assignors.add(new MockPartitionAssignor(Collections.singletonList(PartitionAssignor.RebalanceProtocol.COOPERATIVE)));

        // no commonly supported protocols
        assertThrows(IllegalArgumentException.class, () -> buildCoordinator(rebalanceConfig, new Metrics(), assignors, false));

        assignors.clear();
        assignors.add(new MockPartitionAssignor(Arrays.asList(PartitionAssignor.RebalanceProtocol.EAGER, PartitionAssignor.RebalanceProtocol.COOPERATIVE)));
        assignors.add(new MockPartitionAssignor(Arrays.asList(PartitionAssignor.RebalanceProtocol.EAGER, PartitionAssignor.RebalanceProtocol.COOPERATIVE)));

        // select higher indexed (more advanced) protocols
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, false)) {
            assertEquals(PartitionAssignor.RebalanceProtocol.COOPERATIVE, coordinator.getProtocol());
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

    @Test(expected = GroupAuthorizationException.class)
    public void testGroupDescribeUnauthorized() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.GROUP_AUTHORIZATION_FAILED));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
    }

    @Test(expected = GroupAuthorizationException.class)
    public void testGroupReadUnauthorized() {
        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        client.prepareResponse(joinGroupLeaderResponse(0, "memberId", Collections.<String, List<String>>emptyMap(),
                Errors.GROUP_AUTHORIZATION_FAILED));
        coordinator.poll(time.timer(Long.MAX_VALUE));
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
    public void testManyInFlightAsyncCommitsWithCoordinatorDisconnect() throws Exception {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        int numRequests = 1000;
        TopicPartition tp = new TopicPartition("foo", 0);
        final AtomicInteger responses = new AtomicInteger(0);

        for (int i = 0; i < numRequests; i++) {
            Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(tp, new OffsetAndMetadata(i));
            coordinator.commitOffsetsAsync(offsets, new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    responses.incrementAndGet();
                    Throwable cause = exception.getCause();
                    assertTrue("Unexpected exception cause type: " + (cause == null ? null : cause.getClass()),
                            cause instanceof DisconnectException);
                }
            });
        }

        coordinator.markCoordinatorUnknown();
        consumerClient.pollNoWakeup();
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertEquals(numRequests, responses.get());
    }

    @Test
    public void testCoordinatorUnknownInUnsentCallbacksAfterCoordinatorDead() throws Exception {
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
                        assertTrue("Unexpected exception type: " + e.getClass(), e instanceof DisconnectException);
                        assertTrue(coordinator.coordinatorUnknown());
                        asyncCallbackInvoked.set(true);
                    }
                });

        coordinator.markCoordinatorUnknown();
        consumerClient.pollNoWakeup();
        assertTrue(asyncCallbackInvoked.get());
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
        subscriptions.subscribe(singleton(topic1), rebalanceListener);
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
    }

    @Test
    public void testUnknownConsumerId() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // illegal_generation will cause re-partition
        subscriptions.subscribe(singleton(topic1), rebalanceListener);
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
        assertTrue(future.exception() instanceof DisconnectException);
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test(expected = ApiException.class)
    public void testJoinGroupInvalidGroupId() {
        final String consumerId = "leader";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        // ensure metadata is up-to-date for leader
        client.updateMetadata(metadataResponse);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        client.prepareResponse(joinGroupLeaderResponse(0, consumerId, Collections.<String, List<String>>emptyMap(),
                Errors.INVALID_GROUP_ID));
        coordinator.poll(time.timer(Long.MAX_VALUE));
    }

    @Test
    public void testNormalJoinGroupLeader() {
        final String consumerId = "leader";
        final Set<String> subscription = singleton(topic1);
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> assigned = Arrays.asList(t1p);

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        // ensure metadata is up-to-date for leader
        client.updateMetadata(metadataResponse);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, assigned));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.data.memberId().equals(consumerId) &&
                        sync.data.generationId() == 1 &&
                        sync.groupAssignments().containsKey(consumerId);
            }
        }, syncGroupResponse(assigned, Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(toSet(assigned), subscriptions.assignedPartitions());
        assertEquals(subscription, subscriptions.groupSubscription());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(getRevoked(owned, assigned), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);
    }

    @Test
    public void testOutdatedCoordinatorAssignment() {
        final String consumerId = "outdated_assignment";
        final List<TopicPartition> owned = Collections.emptyList();
        final List<String> oldSubscription = singletonList(topic2);
        final List<TopicPartition> oldAssignment = Arrays.asList(t2p);
        final List<String> newSubscription = singletonList(topic1);
        final List<TopicPartition> newAssignment = Arrays.asList(t1p);

        subscriptions.subscribe(toSet(oldSubscription), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // Test coordinator returning unsubscribed partitions
        partitionAssignor.prepare(singletonMap(consumerId, newAssignment));

        // First incorrect assignment for subscription
        client.prepareResponse(
                joinGroupLeaderResponse(
                    1, consumerId, singletonMap(consumerId, oldSubscription), Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.data.memberId().equals(consumerId) &&
                        sync.data.generationId() == 1 &&
                        sync.groupAssignments().containsKey(consumerId);
            }
        }, syncGroupResponse(oldAssignment, Errors.NONE));

        // Second correct assignment for subscription
        client.prepareResponse(
                joinGroupLeaderResponse(
                    1, consumerId, singletonMap(consumerId, newSubscription), Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.data.memberId().equals(consumerId) &&
                        sync.data.generationId() == 1 &&
                        sync.groupAssignments().containsKey(consumerId);
            }
        }, syncGroupResponse(newAssignment, Errors.NONE));

        // Poll once so that the join group future gets created and complete
        coordinator.poll(time.timer(0));

        // Before the sync group response gets completed change the subscription
        subscriptions.subscribe(toSet(newSubscription), rebalanceListener);
        coordinator.poll(time.timer(0));

        coordinator.poll(time.timer(Long.MAX_VALUE));

        final Collection<TopicPartition> revoked = getRevoked(owned, newAssignment);
        final Collection<TopicPartition> assigned = getAdded(owned, newAssignment);

        int revokeCount = 1;
        final int addCount = 1;

        // with eager protocol we will call revoke on the old assignment as well
        if (protocol == PartitionAssignor.RebalanceProtocol.EAGER) {
            revokeCount += 1;
        }

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(toSet(newAssignment), subscriptions.assignedPartitions());
        assertEquals(toSet(newSubscription), subscriptions.groupSubscription());
        assertEquals(revokeCount, rebalanceListener.revokedCount);
        assertEquals(revoked, rebalanceListener.revoked);
        assertEquals(addCount, rebalanceListener.assignedCount);
        assertEquals(assigned, rebalanceListener.assigned);
    }

    @Test
    public void testPatternJoinGroupLeader() {
        final String consumerId = "leader";
        final List<TopicPartition> assigned = Arrays.asList(t1p, t2p);
        final List<TopicPartition> owned = Collections.emptyList();

        subscriptions.subscribe(Pattern.compile("test.*"), rebalanceListener);

        // partially update the metadata with one topic first,
        // let the leader to refresh metadata during assignment
        client.updateMetadata(TestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, assigned));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.data.memberId().equals(consumerId) &&
                        sync.data.generationId() == 1 &&
                        sync.groupAssignments().containsKey(consumerId);
            }
        }, syncGroupResponse(assigned, Errors.NONE));
        // expect client to force updating the metadata, if yes gives it both topics
        client.prepareMetadataUpdate(metadataResponse);

        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(2, subscriptions.numAssignedPartitions());
        assertEquals(2, subscriptions.groupSubscription().size());
        assertEquals(2, subscriptions.subscription().size());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(getRevoked(owned, assigned), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);
    }

    @Test
    public void testMetadataRefreshDuringRebalance() {
        final String consumerId = "leader";
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> oldAssigned = Arrays.asList(t1p);


        subscriptions.subscribe(Pattern.compile(".*"), rebalanceListener);
        client.updateMetadata(TestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));
        coordinator.maybeUpdateSubscriptionMetadata();

        assertEquals(singleton(topic1), subscriptions.subscription());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        Map<String, List<String>> initialSubscription = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, oldAssigned));

        // the metadata will be updated in flight with a new topic added
        final List<String> updatedSubscription = Arrays.asList(topic1, topic2);

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, initialSubscription, Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                final Map<String, Integer> updatedPartitions = new HashMap<>();
                for (String topic : updatedSubscription)
                    updatedPartitions.put(topic, 1);
                client.updateMetadata(TestUtils.metadataUpdateWith(1, updatedPartitions));
                return true;
            }
        }, syncGroupResponse(oldAssigned, Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        // rejoin will only be set in the next poll call
        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(topic1), subscriptions.subscription());
        assertEquals(toSet(oldAssigned), subscriptions.assignedPartitions());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(getRevoked(owned, oldAssigned), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, oldAssigned), rebalanceListener.assigned);

        List<TopicPartition> newAssigned = Arrays.asList(t1p, t2p);

        Map<String, List<String>> updatedSubscriptions = singletonMap(consumerId, Arrays.asList(topic1, topic2));
        partitionAssignor.prepare(singletonMap(consumerId, newAssigned));

        // we expect to see a second rebalance with the new-found topics
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                JoinGroupRequest join = (JoinGroupRequest) body;
                Iterator<JoinGroupRequestData.JoinGroupRequestProtocol> protocolIterator =
                        join.data().protocols().iterator();
                assertTrue(protocolIterator.hasNext());
                JoinGroupRequestData.JoinGroupRequestProtocol protocolMetadata = protocolIterator.next();

                ByteBuffer metadata = ByteBuffer.wrap(protocolMetadata.metadata());
                PartitionAssignor.Subscription subscription = ConsumerProtocol.deserializeSubscription(metadata);
                metadata.rewind();
                return subscription.topics().containsAll(updatedSubscription);
            }
        }, joinGroupLeaderResponse(2, consumerId, updatedSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(newAssigned, Errors.NONE));

        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(toSet(updatedSubscription), subscriptions.subscription());
        assertEquals(toSet(newAssigned), subscriptions.assignedPartitions());
        assertEquals(2, rebalanceListener.revokedCount);
        assertEquals(getRevoked(oldAssigned, newAssigned), rebalanceListener.revoked);
        assertEquals(2, rebalanceListener.assignedCount);
        assertEquals(getAdded(oldAssigned, newAssigned), rebalanceListener.assigned);
    }

    @Test
    public void testForceMetadataRefreshForPatternSubscriptionDuringRebalance() {
        // Set up a non-leader consumer with pattern subscription and a cluster containing one topic matching the
        // pattern.
        final String consumerId = "consumer";

        subscriptions.subscribe(Pattern.compile(".*"), rebalanceListener);
        client.updateMetadata(TestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));
        coordinator.maybeUpdateSubscriptionMetadata();
        assertEquals(singleton(topic1), subscriptions.subscription());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // Instrument the test so that metadata will contain two topics after next refresh.
        client.prepareMetadataUpdate(metadataResponse);

        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.data.memberId().equals(consumerId) &&
                    sync.data.generationId() == 1 &&
                    sync.groupAssignments().isEmpty();
            }
        }, syncGroupResponse(singletonList(t1p), Errors.NONE));

        partitionAssignor.prepare(singletonMap(consumerId, singletonList(t1p)));

        // This will trigger rebalance.
        coordinator.poll(time.timer(Long.MAX_VALUE));

        // Make sure that the metadata was refreshed during the rebalance and thus subscriptions now contain two topics.
        final Set<String> updatedSubscriptionSet = new HashSet<>(Arrays.asList(topic1, topic2));
        assertEquals(updatedSubscriptionSet, subscriptions.subscription());

        // Refresh the metadata again. Since there have been no changes since the last refresh, it won't trigger
        // rebalance again.
        metadata.requestUpdate();
        consumerClient.poll(time.timer(Long.MAX_VALUE));
        assertFalse(coordinator.rejoinNeededOrPending());
    }

    @Test
    public void testWakeupDuringJoin() {
        final String consumerId = "leader";
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> assigned = Arrays.asList(t1p);

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

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
        assertEquals(toSet(assigned), subscriptions.assignedPartitions());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(getRevoked(owned, assigned), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);
    }

    @Test
    public void testNormalJoinGroupFollower() {
        final String consumerId = "consumer";
        final Set<String> subscription = singleton(topic1);
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> assigned = Arrays.asList(t1p);

        subscriptions.subscribe(subscription, rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.data.memberId().equals(consumerId) &&
                        sync.data.generationId() == 1 &&
                        sync.groupAssignments().isEmpty();
            }
        }, syncGroupResponse(assigned, Errors.NONE));

        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(toSet(assigned), subscriptions.assignedPartitions());
        assertEquals(subscription, subscriptions.groupSubscription());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(getRevoked(owned, assigned), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);
    }

    @Test
    public void testUpdateLastHeartbeatPollWhenCoordinatorUnknown() throws Exception {
        // If we are part of an active group and we cannot find the coordinator, we should nevertheless
        // continue to update the last poll time so that we do not expire the consumer
        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // Join the group, but signal a coordinator change after the first heartbeat
        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE));
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
        final String consumerId = "consumer";
        final Set<String> subscription = Utils.mkSet(topic1, topic2);
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> assigned = Arrays.asList(t1p, t2p);

        subscriptions.subscribe(Pattern.compile("test.*"), rebalanceListener);

        // partially update the metadata with one topic first,
        // let the leader to refresh metadata during assignment
        client.updateMetadata(TestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.data.memberId().equals(consumerId) &&
                        sync.data.generationId() == 1 &&
                        sync.groupAssignments().isEmpty();
            }
        }, syncGroupResponse(assigned, Errors.NONE));
        // expect client to force updating the metadata, if yes gives it both topics
        client.prepareMetadataUpdate(metadataResponse);

        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(assigned.size(), subscriptions.numAssignedPartitions());
        assertEquals(subscription, subscriptions.subscription());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(getRevoked(owned, assigned), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);
    }

    @Test
    public void testLeaveGroupOnClose() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);
        joinAsFollowerAndReceiveAssignment(consumerId, coordinator, singletonList(t1p));

        final AtomicBoolean received = new AtomicBoolean(false);
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                received.set(true);
                LeaveGroupRequest leaveRequest = (LeaveGroupRequest) body;
                return leaveRequest.data().memberId().equals(consumerId) &&
                        leaveRequest.data().groupId().equals(groupId);
            }
        }, new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code())));
        coordinator.close(time.timer(0));
        assertTrue(received.get());
    }

    @Test
    public void testMaybeLeaveGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);
        joinAsFollowerAndReceiveAssignment(consumerId, coordinator, singletonList(t1p));

        final AtomicBoolean received = new AtomicBoolean(false);
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                received.set(true);
                LeaveGroupRequest leaveRequest = (LeaveGroupRequest) body;
                return leaveRequest.data().memberId().equals(consumerId) &&
                        leaveRequest.data().groupId().equals(groupId);
            }
        }, new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code())));
        coordinator.maybeLeaveGroup("test maybe leave group");
        assertTrue(received.get());

        AbstractCoordinator.Generation generation = coordinator.generation();
        assertNull(generation);
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
        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // here we return a DEFAULT_GENERATION_ID, but valid member id and leader id.
        client.prepareResponse(joinGroupFollowerResponse(-1, consumerId, "leader-id", Errors.MEMBER_ID_REQUIRED));

        // execute join group
        coordinator.joinGroupIfNeeded(time.timer(0));

        final AtomicBoolean received = new AtomicBoolean(false);
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                received.set(true);
                LeaveGroupRequest leaveRequest = (LeaveGroupRequest) body;
                return leaveRequest.data().memberId().equals(consumerId);
            }
        }, new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code())));

        coordinator.maybeLeaveGroup("pending member leaves");
        assertTrue(received.get());
    }

    @Test(expected = KafkaException.class)
    public void testUnexpectedErrorOnSyncGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.emptyList(), Errors.UNKNOWN_SERVER_ERROR));
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));
    }

    @Test
    public void testUnknownMemberIdOnSyncGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // join initially, but let coordinator returns unknown member id
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.<TopicPartition>emptyList(), Errors.UNKNOWN_MEMBER_ID));

        // now we should see a new join with the empty UNKNOWN_MEMBER_ID
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                JoinGroupRequest joinRequest = (JoinGroupRequest) body;
                return joinRequest.data().memberId().equals(JoinGroupRequest.UNKNOWN_MEMBER_ID);
            }
        }, joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
    }

    @Test
    public void testRebalanceInProgressOnSyncGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.<TopicPartition>emptyList(), Errors.REBALANCE_IN_PROGRESS));

        // then let the full join/sync finish successfully
        client.prepareResponse(joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
    }

    @Test
    public void testIllegalGenerationOnSyncGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.<TopicPartition>emptyList(), Errors.ILLEGAL_GENERATION));

        // then let the full join/sync finish successfully
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                JoinGroupRequest joinRequest = (JoinGroupRequest) body;
                return joinRequest.data().memberId().equals(JoinGroupRequest.UNKNOWN_MEMBER_ID);
            }
        }, joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
    }

    @Test
    public void testMetadataChangeTriggersRebalance() {
        final String consumerId = "consumer";

        // ensure metadata is up-to-date for leader
        subscriptions.subscribe(singleton(topic1), rebalanceListener);
        client.updateMetadata(metadataResponse);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, Arrays.asList(t1p)));

        // the leader is responsible for picking up metadata changes and forcing a group rebalance
        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());

        // a new partition is added to the topic
        metadata.update(TestUtils.metadataUpdateWith(1, singletonMap(topic1, 2)), time.milliseconds());
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

        subscriptions.subscribe(new HashSet<>(topics), rebalanceListener);

        // we only have metadata for one topic initially
        client.updateMetadata(TestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // prepare initial rebalance
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, topics);
        partitionAssignor.prepare(singletonMap(consumerId, Arrays.asList(tp1)));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                if (sync.data.memberId().equals(consumerId) &&
                        sync.data.generationId() == 1 &&
                        sync.groupAssignments().containsKey(consumerId)) {
                    // trigger the metadata update including both topics after the sync group request has been sent
                    Map<String, Integer> topicPartitionCounts = new HashMap<>();
                    topicPartitionCounts.put(topic1, 1);
                    topicPartitionCounts.put(topic2, 1);
                    client.updateMetadata(TestUtils.metadataUpdateWith(1, topicPartitionCounts));
                    return true;
                }
                return false;
            }
        }, syncGroupResponse(Collections.singletonList(tp1), Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        // the metadata update should trigger a second rebalance
        client.prepareResponse(joinGroupLeaderResponse(2, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(Arrays.asList(tp1, tp2), Errors.NONE));

        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(new HashSet<>(Arrays.asList(tp1, tp2)), subscriptions.assignedPartitions());
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

        subscriptions.subscribe(topics, rebalanceListener);

        // we only have metadata for one topic initially
        client.updateMetadata(TestUtils.metadataUpdateWith(1, singletonMap(topic1, 1)));

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
        assertEquals(1, rebalanceListener.revokedCount);
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
        final String consumerId = "consumer";

        if (patternSubscribe)
            subscriptions.subscribe(Pattern.compile("test.*"), rebalanceListener);
        else
            subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareMetadataUpdate(TestUtils.metadataUpdateWith("kafka-cluster", 1,
                Collections.singletonMap(topic1, Errors.UNKNOWN_TOPIC_OR_PARTITION), Collections.emptyMap()));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(Collections.emptyMap());

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.emptyList(), Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));
        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(Collections.emptySet(), rebalanceListener.assigned);
        assertTrue("Metadata refresh not requested for unavailable partitions", metadata.updateRequested());

        Map<String, Errors> topicErrors = new HashMap<>();
        for (String topic : unavailableTopicsInLastMetadata)
            topicErrors.put(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION);

        client.prepareMetadataUpdate(TestUtils.metadataUpdateWith("kafka-cluster", 1,
                topicErrors, singletonMap(topic1, 1)));

        consumerClient.poll(time.timer(0));
        client.prepareResponse(joinGroupLeaderResponse(2, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        assertFalse("Metadata refresh requested unnecessarily", metadata.updateRequested());
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
        metadata = new ConsumerMetadata(0, Long.MAX_VALUE, includeInternalTopics,
                false, subscriptions, new LogContext(), new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, false)) {
            subscriptions.subscribe(Pattern.compile(".*"), rebalanceListener);
            Node node = new Node(0, "localhost", 9999);
            MetadataResponse.PartitionMetadata partitionMetadata =
                new MetadataResponse.PartitionMetadata(Errors.NONE, 0, node, Optional.empty(),
                    singletonList(node), singletonList(node), singletonList(node));
            MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(Errors.NONE,
                Topic.GROUP_METADATA_TOPIC_NAME, true, singletonList(partitionMetadata));

            client.updateMetadata(MetadataResponse.prepareResponse(singletonList(node), "clusterId", node.id(),
                singletonList(topicMetadata)));
            coordinator.maybeUpdateSubscriptionMetadata();

            assertEquals(includeInternalTopics, subscriptions.subscription().contains(Topic.GROUP_METADATA_TOPIC_NAME));
        }
    }

    @Test
    public void testRejoinGroup() {
        String otherTopic = "otherTopic";
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> assigned = Arrays.asList(t1p);

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        // join the group once
        joinAsFollowerAndReceiveAssignment("consumer", coordinator, assigned);

        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(getRevoked(owned, assigned), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);

        // and join the group again
        rebalanceListener.revoked = null;
        rebalanceListener.assigned = null;
        subscriptions.subscribe(new HashSet<>(Arrays.asList(topic1, otherTopic)), rebalanceListener);
        client.prepareResponse(joinGroupFollowerResponse(2, "consumer", "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(assigned, Errors.NONE));
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(2, rebalanceListener.revokedCount);
        assertEquals(getRevoked(assigned, assigned), rebalanceListener.revoked);
        assertEquals(2, rebalanceListener.assignedCount);
        assertEquals(getAdded(assigned, assigned), rebalanceListener.assigned);
    }

    @Test
    public void testDisconnectInJoin() {
        subscriptions.subscribe(singleton(topic1), rebalanceListener);
        final List<TopicPartition> owned = Collections.emptyList();
        final List<TopicPartition> assigned = Arrays.asList(t1p);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // disconnected from original coordinator will cause re-discover and join again
        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE), true);
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(assigned, Errors.NONE));
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(toSet(assigned), subscriptions.assignedPartitions());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(getRevoked(owned, assigned), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(getAdded(owned, assigned), rebalanceListener.assigned);
    }

    @Test(expected = ApiException.class)
    public void testInvalidSessionTimeout() {
        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // coordinator doesn't like the session timeout
        client.prepareResponse(joinGroupFollowerResponse(0, "consumer", "", Errors.INVALID_SESSION_TIMEOUT));
        coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));
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

        respondToOffsetCommitRequest(singletonMap(t1p, 100L), error);
        consumerClient.pollNoWakeup();
        consumerClient.pollNoWakeup(); // second poll since coordinator disconnect is async
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertTrue(firstCommitCallback.exception instanceof RetriableCommitFailedException);
        assertTrue(secondCommitCallback.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testAutoCommitDynamicAssignment() {
        final String consumerId = "consumer";

        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors,
            true)
        ) {
            subscriptions.subscribe(singleton(topic1), rebalanceListener);
            joinAsFollowerAndReceiveAssignment(consumerId, coordinator, singletonList(t1p));
            subscriptions.seek(t1p, 100);
            prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
            time.sleep(autoCommitIntervalMs);
            coordinator.poll(time.timer(Long.MAX_VALUE));
            assertFalse(client.hasPendingResponses());
        }
    }

    @Test
    public void testAutoCommitRetryBackoff() {
        final String consumerId = "consumer";

        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors,
            true)) {
            subscriptions.subscribe(singleton(topic1), rebalanceListener);
            joinAsFollowerAndReceiveAssignment(consumerId, coordinator, singletonList(t1p));

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
        final String consumerId = "consumer";
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true)) {
            subscriptions.subscribe(singleton(topic1), rebalanceListener);
            joinAsFollowerAndReceiveAssignment(consumerId, coordinator, singletonList(t1p));

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
        final String consumerId = "consumer";

        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors,
            true)) {
            subscriptions.subscribe(singleton(topic1), rebalanceListener);
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
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true)) {
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
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true)) {
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
    public void testCommitOffsetMetadata() {
        subscriptions.assignFromUser(singleton(t1p));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);

        AtomicBoolean success = new AtomicBoolean(false);

        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(t1p, new OffsetAndMetadata(100L, "hello"));
        coordinator.commitOffsetsAsync(offsets, callback(offsets, success));
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertTrue(success.get());
    }

    @Test
    public void testCommitOffsetAsyncWithDefaultCallback() {
        int invokedBeforeTest = mockOffsetCommitCallback.invoked;
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), mockOffsetCommitCallback);
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertEquals(invokedBeforeTest + 1, mockOffsetCommitCallback.invoked);
        assertNull(mockOffsetCommitCallback.exception);
    }

    @Test
    public void testCommitAfterLeaveGroup() {
        // enable auto-assignment
        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        joinAsFollowerAndReceiveAssignment("consumer", coordinator, singletonList(t1p));

        // now switch to manual assignment
        client.prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                .setErrorCode(Errors.NONE.code())));
        subscriptions.unsubscribe();
        coordinator.maybeLeaveGroup("test commit after leave");
        subscriptions.assignFromUser(singleton(t1p));

        // the client should not reuse generation/memberId from auto-subscribed generation
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                OffsetCommitRequest commitRequest = (OffsetCommitRequest) body;
                return commitRequest.data().memberId().equals(OffsetCommitRequest.DEFAULT_MEMBER_ID) &&
                        commitRequest.data().generationId() == OffsetCommitRequest.DEFAULT_GENERATION_ID;
            }
        }, offsetCommitResponse(singletonMap(t1p, Errors.NONE)));

        AtomicBoolean success = new AtomicBoolean(false);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), callback(success));
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertTrue(success.get());
    }

    @Test
    public void testCommitOffsetAsyncFailedWithDefaultCallback() {
        int invokedBeforeTest = mockOffsetCommitCallback.invoked;
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), mockOffsetCommitCallback);
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertEquals(invokedBeforeTest + 1, mockOffsetCommitCallback.invoked);
        assertTrue(mockOffsetCommitCallback.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testCommitOffsetAsyncCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // async commit with coordinator not available
        MockCommitCallback cb = new MockCommitCallback();
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), cb);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertTrue(cb.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testCommitOffsetAsyncNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // async commit with not coordinator
        MockCommitCallback cb = new MockCommitCallback();
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), cb);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertTrue(cb.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testCommitOffsetAsyncDisconnected() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // async commit with coordinator disconnected
        MockCommitCallback cb = new MockCommitCallback();
        prepareOffsetCommitRequestDisconnect(singletonMap(t1p, 100L));
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), cb);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertTrue(cb.exception instanceof RetriableCommitFailedException);
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

        final List<OffsetAndMetadata> committedOffsets = Collections.synchronizedList(new ArrayList<OffsetAndMetadata>());
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

        thread.start();

        client.waitForRequests(2, 5000);
        respondToOffsetCommitRequest(singletonMap(t1p, firstOffset.offset()), Errors.NONE);
        respondToOffsetCommitRequest(singletonMap(t1p, secondOffset.offset()), Errors.NONE);

        thread.join();

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

    @Test(expected = OffsetMetadataTooLarge.class)
    public void testCommitOffsetMetadataTooLarge() {
        // since offset metadata is provided by the user, we have to propagate the exception so they can handle it
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.OFFSET_METADATA_TOO_LARGE);
        coordinator.commitOffsetsSync(singletonMap(t1p,
                new OffsetAndMetadata(100L, "metadata")), time.timer(Long.MAX_VALUE));
    }

    @Test(expected = CommitFailedException.class)
    public void testCommitOffsetIllegalGeneration() {
        // we cannot retry if a rebalance occurs before the commit completed
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.ILLEGAL_GENERATION);
        coordinator.commitOffsetsSync(singletonMap(t1p,
                new OffsetAndMetadata(100L, "metadata")), time.timer(Long.MAX_VALUE));
    }

    @Test(expected = CommitFailedException.class)
    public void testCommitOffsetUnknownMemberId() {
        // we cannot retry if a rebalance occurs before the commit completed
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.UNKNOWN_MEMBER_ID);
        coordinator.commitOffsetsSync(singletonMap(t1p,
                new OffsetAndMetadata(100L, "metadata")), time.timer(Long.MAX_VALUE));
    }

    @Test
    public void testCommitOffsetRebalanceInProgress() {
        // we cannot retry if a rebalance occurs before the commit completed
        final String consumerId = "leader";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        // ensure metadata is up-to-date for leader
        client.updateMetadata(metadataResponse);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, singletonList(t1p)));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data.memberId().equals(consumerId) &&
                sync.data.generationId() == 1 &&
                sync.groupAssignments().containsKey(consumerId);
        }, syncGroupResponse(singletonList(t1p), Errors.NONE));
        coordinator.poll(time.timer(Long.MAX_VALUE));

        AbstractCoordinator.Generation expectedGeneration = new AbstractCoordinator.Generation(1, consumerId, partitionAssignor.name());
        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(expectedGeneration, coordinator.generation());

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.REBALANCE_IN_PROGRESS);

        assertThrows(CommitFailedException.class, () -> coordinator.commitOffsetsSync(singletonMap(t1p,
            new OffsetAndMetadata(100L, "metadata")), time.timer(Long.MAX_VALUE)));

        assertTrue(coordinator.rejoinNeededOrPending());
        assertEquals(expectedGeneration, coordinator.generation());
    }

    @Test(expected = KafkaException.class)
    public void testCommitOffsetSyncCallbackWithNonRetriableException() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // sync commit with invalid partitions should throw if we have no callback
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.UNKNOWN_SERVER_ERROR);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), time.timer(Long.MAX_VALUE));
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
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(Collections.emptySet(), subscriptions.missingFetchPositions());
        assertTrue(subscriptions.hasAllFetchPositions());
        assertEquals(100L, subscriptions.position(t1p).offset);
    }

    @Test
    public void testRefreshOffsetWithValidation() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));

        // Initial leader epoch of 4
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("kafka-cluster", 1,
                Collections.emptyMap(), singletonMap(topic1, 1), tp -> 4);
        client.updateMetadata(metadataResponse);

        // Load offsets from previous epoch
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L, 3));
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        // Offset gets loaded, but requires validation
        assertEquals(Collections.emptySet(), subscriptions.missingFetchPositions());
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

        client.prepareResponse(new OffsetFetchResponse(Errors.NONE, singletonMap(t1p, data)));
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

        client.prepareResponse(new OffsetFetchResponse(Errors.NONE, singletonMap(t1p, data)));
        TopicAuthorizationException exception = assertThrows(TopicAuthorizationException.class, () ->
                coordinator.fetchCommittedOffsets(singleton(t1p), time.timer(Long.MAX_VALUE)));

        assertEquals(singleton(topic1), exception.unauthorizedTopics());
    }

    @Test
    public void testRefreshOffsetLoadInProgress() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L));
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(Collections.emptySet(), subscriptions.missingFetchPositions());
        assertTrue(subscriptions.hasAllFetchPositions());
        assertEquals(100L, subscriptions.position(t1p).offset);
    }

    @Test
    public void testRefreshOffsetsGroupNotAuthorized() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(Errors.GROUP_AUTHORIZATION_FAILED));
        try {
            coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));
            fail("Expected group authorization error");
        } catch (GroupAuthorizationException e) {
            assertEquals(groupId, e.groupId());
        }
    }

    @Test(expected = KafkaException.class)
    public void testRefreshOffsetUnknownTopicOrPartition() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.UNKNOWN_TOPIC_OR_PARTITION, "", 100L));
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));
    }

    @Test
    public void testRefreshOffsetNotCoordinatorForConsumer() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(Errors.NOT_COORDINATOR));
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L));
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(Collections.emptySet(), subscriptions.missingFetchPositions());
        assertTrue(subscriptions.hasAllFetchPositions());
        assertEquals(100L, subscriptions.position(t1p).offset);
    }

    @Test
    public void testRefreshOffsetWithNoFetchableOffsets() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", -1L));
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(Collections.singleton(t1p), subscriptions.missingFetchPositions());
        assertEquals(Collections.emptySet(), subscriptions.partitionsNeedingReset(time.milliseconds()));
        assertFalse(subscriptions.hasAllFetchPositions());
        assertEquals(null, subscriptions.position(t1p));
    }

    @Test
    public void testNoCoordinatorDiscoveryIfPositionsKnown() {
        assertTrue(coordinator.coordinatorUnknown());

        subscriptions.assignFromUser(singleton(t1p));
        subscriptions.seek(t1p, 500L);
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(Collections.emptySet(), subscriptions.missingFetchPositions());
        assertTrue(subscriptions.hasAllFetchPositions());
        assertEquals(500L, subscriptions.position(t1p).offset);
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testNoCoordinatorDiscoveryIfPartitionAwaitingReset() {
        assertTrue(coordinator.coordinatorUnknown());

        subscriptions.assignFromUser(singleton(t1p));
        subscriptions.requestOffsetReset(t1p, OffsetResetStrategy.EARLIEST);
        coordinator.refreshCommittedOffsetsIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(Collections.emptySet(), subscriptions.missingFetchPositions());
        assertFalse(subscriptions.hasAllFetchPositions());
        assertEquals(Collections.singleton(t1p), subscriptions.partitionsNeedingReset(time.milliseconds()));
        assertEquals(OffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(t1p));
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
        final Metric metric = metrics.metric(new MetricName("assigned-partitions", "consumer" + groupId + "-coordinator-metrics",
                "", Collections.<String, String>emptyMap()));

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
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return observedSize.get() == totalPartitions || exceptionHolder.get() != null;
            }
        }, "Failed to observe expected assignment change");

        doStop.set(true);
        poller.join();

        assertNull("Failed fetching the metric at least once", exceptionHolder.get());
    }

    @Test
    public void testCloseDynamicAssignment() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, Optional.empty())) {
            gracefulCloseTest(coordinator, true);
        }
    }

    @Test
    public void testCloseManualAssignment() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(false, true, Optional.empty())) {
            gracefulCloseTest(coordinator, false);
        }
    }

    @Test
    public void testCloseCoordinatorNotKnownManualAssignment() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(false, true, Optional.empty())) {
            makeCoordinatorUnknown(coordinator, Errors.NOT_COORDINATOR);
            time.sleep(autoCommitIntervalMs);
            closeVerifyTimeout(coordinator, 1000, 1000, 1000);
        }
    }

    @Test
    public void testCloseCoordinatorNotKnownNoCommits() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, Optional.empty())) {
            makeCoordinatorUnknown(coordinator, Errors.NOT_COORDINATOR);
            closeVerifyTimeout(coordinator, 1000, 0, 0);
        }
    }

    @Test
    public void testCloseCoordinatorNotKnownWithCommits() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, Optional.empty())) {
            makeCoordinatorUnknown(coordinator, Errors.NOT_COORDINATOR);
            time.sleep(autoCommitIntervalMs);
            closeVerifyTimeout(coordinator, 1000, 1000, 1000);
        }
    }

    @Test
    public void testCloseCoordinatorUnavailableNoCommits() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, Optional.empty())) {
            makeCoordinatorUnknown(coordinator, Errors.COORDINATOR_NOT_AVAILABLE);
            closeVerifyTimeout(coordinator, 1000, 0, 0);
        }
    }

    @Test
    public void testCloseTimeoutCoordinatorUnavailableForCommit() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, groupInstanceId)) {
            makeCoordinatorUnknown(coordinator, Errors.COORDINATOR_NOT_AVAILABLE);
            time.sleep(autoCommitIntervalMs);
            closeVerifyTimeout(coordinator, 1000, 1000, 1000);
        }
    }

    @Test
    public void testCloseMaxWaitCoordinatorUnavailableForCommit() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, groupInstanceId)) {
            makeCoordinatorUnknown(coordinator, Errors.COORDINATOR_NOT_AVAILABLE);
            time.sleep(autoCommitIntervalMs);
            closeVerifyTimeout(coordinator, Long.MAX_VALUE, requestTimeoutMs, requestTimeoutMs);
        }
    }

    @Test
    public void testCloseNoResponseForCommit() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, groupInstanceId)) {
            time.sleep(autoCommitIntervalMs);
            closeVerifyTimeout(coordinator, Long.MAX_VALUE, requestTimeoutMs, requestTimeoutMs);
        }
    }

    @Test
    public void testCloseNoResponseForLeaveGroup() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, Optional.empty())) {
            closeVerifyTimeout(coordinator, Long.MAX_VALUE, requestTimeoutMs, requestTimeoutMs);
        }
    }

    @Test
    public void testCloseNoWait() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, groupInstanceId)) {
            time.sleep(autoCommitIntervalMs);
            closeVerifyTimeout(coordinator, 0, 0, 0);
        }
    }

    @Test
    public void testHeartbeatThreadClose() throws Exception {
        try (ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, groupInstanceId)) {
            coordinator.ensureActiveGroup();
            time.sleep(heartbeatIntervalMs + 100);
            Thread.yield(); // Give heartbeat thread a chance to attempt heartbeat
            closeVerifyTimeout(coordinator, Long.MAX_VALUE, requestTimeoutMs, requestTimeoutMs);
            Thread[] threads = new Thread[Thread.activeCount()];
            int threadCount = Thread.enumerate(threads);
            for (int i = 0; i < threadCount; i++) {
                assertFalse("Heartbeat thread active after close", threads[i].getName().contains(groupId));
            }
        }
    }

    @Test
    public void testAutoCommitAfterCoordinatorBackToService() {
        try (ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig, new Metrics(), assignors, true)) {
            subscriptions.assignFromUser(Collections.singleton(t1p));
            subscriptions.seek(t1p, 100L);

            coordinator.markCoordinatorUnknown();
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

    @Test(expected = FencedInstanceIdException.class)
    public void testCommitOffsetRequestSyncWithFencedInstanceIdException() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // sync commit with invalid partitions should throw if we have no callback
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.FENCED_INSTANCE_ID);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), time.timer(Long.MAX_VALUE));
    }

    @Test(expected = FencedInstanceIdException.class)
    public void testCommitOffsetRequestAsyncWithFencedInstanceIdException() {
        receiveFencedInstanceIdException();
    }

    @Test
    public void testCommitOffsetRequestAsyncAlwaysReceiveFencedException() {
        // Once we get fenced exception once, we should always hit fencing case.
        assertThrows(FencedInstanceIdException.class, this::receiveFencedInstanceIdException);
        assertThrows(FencedInstanceIdException.class, () ->
                coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), new MockCommitCallback()));
        assertThrows(FencedInstanceIdException.class, () ->
                coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), time.timer(Long.MAX_VALUE)));
    }

    private void receiveFencedInstanceIdException() {
        subscriptions.assignFromUser(singleton(t1p));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.FENCED_INSTANCE_ID);

        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), new MockCommitCallback());
        coordinator.invokeCompletedOffsetCommitCallbacks();
    }

    private ConsumerCoordinator prepareCoordinatorForCloseTest(final boolean useGroupManagement,
                                                               final boolean autoCommit,
                                                               final Optional<String> groupInstanceId) {
        final String consumerId = "consumer";
        rebalanceConfig = buildRebalanceConfig(groupInstanceId);
        ConsumerCoordinator coordinator = buildCoordinator(rebalanceConfig,
                                                           new Metrics(),
                                                           assignors,
                                                           autoCommit);
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
        if (useGroupManagement) {
            subscriptions.subscribe(singleton(topic1), rebalanceListener);
            client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
            client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
            coordinator.joinGroupIfNeeded(time.timer(Long.MAX_VALUE));
        } else
            subscriptions.assignFromUser(singleton(t1p));

        subscriptions.seek(t1p, 100);
        coordinator.poll(time.timer(Long.MAX_VALUE));

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
            Future<?> future = executor.submit(new Runnable() {
                @Override
                public void run() {
                    coordinator.close(time.timer(Math.min(closeTimeoutMs, requestTimeoutMs)));
                }
            });
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

    private void gracefulCloseTest(ConsumerCoordinator coordinator, boolean shouldLeaveGroup) throws Exception {
        final AtomicBoolean commitRequested = new AtomicBoolean();
        final AtomicBoolean leaveGroupRequested = new AtomicBoolean();
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                commitRequested.set(true);
                OffsetCommitRequest commitRequest = (OffsetCommitRequest) body;
                return commitRequest.data().groupId().equals(groupId);
            }
        }, new OffsetCommitResponse(new OffsetCommitResponseData()));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                leaveGroupRequested.set(true);
                LeaveGroupRequest leaveRequest = (LeaveGroupRequest) body;
                return leaveRequest.data().groupId().equals(groupId);
            }
        }, new LeaveGroupResponse(new LeaveGroupResponseData()
                .setErrorCode(Errors.NONE.code())));

        coordinator.close();
        assertTrue("Commit not requested", commitRequested.get());
        assertEquals("leaveGroupRequested should be " + shouldLeaveGroup, shouldLeaveGroup, leaveGroupRequested.get());
    }

    private ConsumerCoordinator buildCoordinator(final GroupRebalanceConfig rebalanceConfig,
                                                 final Metrics metrics,
                                                 final List<PartitionAssignor> assignors,
                                                 final boolean autoCommitEnabled) {
        return new ConsumerCoordinator(
                rebalanceConfig,
                new LogContext(),
                consumerClient,
                assignors,
                null,
                metadata,
                subscriptions,
                metrics,
                "consumer" + groupId,
                time,
                autoCommitEnabled,
                autoCommitIntervalMs,
                null);
    }

    private Collection<TopicPartition> getRevoked(final List<TopicPartition> owned,
                                                  final List<TopicPartition> assigned) {
        switch (protocol) {
            case EAGER:
                return toSet(owned);
            case COOPERATIVE:
                final List<TopicPartition> revoked = new ArrayList<>(owned);
                revoked.removeAll(assigned);
                return toSet(revoked);
            default:
                throw new IllegalStateException("This should not happen");
        }
    }

    private Collection<TopicPartition> getAdded(final List<TopicPartition> owned,
                                                final List<TopicPartition> assigned) {
        switch (protocol) {
            case EAGER:
                return toSet(assigned);
            case COOPERATIVE:
                final List<TopicPartition> added = new ArrayList<>(assigned);
                added.removeAll(owned);
                return toSet(added);
            default:
                throw new IllegalStateException("This should not happen");
        }
    }

    private FindCoordinatorResponse groupCoordinatorResponse(Node node, Errors error) {
        return FindCoordinatorResponse.prepareResponse(error, node);
    }

    private HeartbeatResponse heartbeatResponse(Errors error) {
        return new HeartbeatResponse(new HeartbeatResponseData().setErrorCode(error.code()));
    }

    private JoinGroupResponse joinGroupLeaderResponse(int generationId,
                                                      String memberId,
                                                      Map<String, List<String>> subscriptions,
                                                      Errors error) {
        List<JoinGroupResponseData.JoinGroupResponseMember> metadata = new ArrayList<>();
        for (Map.Entry<String, List<String>> subscriptionEntry : subscriptions.entrySet()) {
            PartitionAssignor.Subscription subscription = new PartitionAssignor.Subscription(subscriptionEntry.getValue());
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
                        .setMemberId(memberId)
                        .setMembers(metadata)
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
                        .setMembers(Collections.emptyList())
        );
    }

    private SyncGroupResponse syncGroupResponse(List<TopicPartition> partitions, Errors error) {
        ByteBuffer buf = ConsumerProtocol.serializeAssignment(new PartitionAssignor.Assignment(partitions));
        return new SyncGroupResponse(
                new SyncGroupResponseData()
                        .setErrorCode(error.code())
                        .setAssignment(Utils.toArray(buf))
        );
    }

    private OffsetCommitResponse offsetCommitResponse(Map<TopicPartition, Errors> responseData) {
        return new OffsetCommitResponse(responseData);
    }

    private OffsetFetchResponse offsetFetchResponse(Errors topLevelError) {
        return new OffsetFetchResponse(topLevelError, Collections.emptyMap());
    }

    private OffsetFetchResponse offsetFetchResponse(TopicPartition tp, Errors partitionLevelError, String metadata, long offset) {
        OffsetFetchResponse.PartitionData data = new OffsetFetchResponse.PartitionData(offset,
                Optional.empty(), metadata, partitionLevelError);
        return new OffsetFetchResponse(Errors.NONE, singletonMap(tp, data));
    }

    private OffsetFetchResponse offsetFetchResponse(TopicPartition tp, Errors partitionLevelError, String metadata, long offset, int epoch) {
        OffsetFetchResponse.PartitionData data = new OffsetFetchResponse.PartitionData(offset,
                Optional.of(epoch), metadata, partitionLevelError);
        return new OffsetFetchResponse(Errors.NONE, singletonMap(tp, data));
    }

    private OffsetCommitCallback callback(final AtomicBoolean success) {
        return new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception == null)
                    success.set(true);
            }
        };
    }

    private void joinAsFollowerAndReceiveAssignment(String consumerId,
                                                    ConsumerCoordinator coordinator,
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
        return new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
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
            }
        };
    }

    private OffsetCommitCallback callback(final Map<TopicPartition, OffsetAndMetadata> expectedOffsets,
                                          final AtomicBoolean success) {
        return new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (expectedOffsets.equals(offsets) && exception == null)
                    success.set(true);
            }
        };
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

    private static class MockRebalanceListener implements ConsumerRebalanceListener {
        public Collection<TopicPartition> revoked;
        public Collection<TopicPartition> assigned;
        public int revokedCount = 0;
        public int assignedCount = 0;


        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            this.assigned = partitions;
            assignedCount++;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            this.revoked = partitions;
            revokedCount++;
        }

    }
}
