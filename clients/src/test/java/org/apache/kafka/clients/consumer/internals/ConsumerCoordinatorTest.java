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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConsumerCoordinatorTest {

    private String topic1 = "test1";
    private String topic2 = "test2";
    private String groupId = "test-group";
    private TopicPartition t1p = new TopicPartition(topic1, 0);
    private TopicPartition t2p = new TopicPartition(topic2, 0);
    private int rebalanceTimeoutMs = 60000;
    private int sessionTimeoutMs = 10000;
    private int heartbeatIntervalMs = 5000;
    private long retryBackoffMs = 100;
    private int autoCommitIntervalMs = 2000;
    private MockPartitionAssignor partitionAssignor = new MockPartitionAssignor();
    private List<PartitionAssignor> assignors = Collections.<PartitionAssignor>singletonList(partitionAssignor);
    private MockTime time;
    private MockClient client;
    private Cluster cluster = TestUtils.clusterWith(1, new HashMap<String, Integer>() {
        {
            put(topic1, 1);
            put(topic2, 1);
        }
    });
    private Node node = cluster.nodes().get(0);
    private SubscriptionState subscriptions;
    private Metadata metadata;
    private Metrics metrics;
    private ConsumerNetworkClient consumerClient;
    private MockRebalanceListener rebalanceListener;
    private MockCommitCallback mockOffsetCommitCallback;
    private ConsumerCoordinator coordinator;

    @Before
    public void setup() {
        this.time = new MockTime();
        this.subscriptions = new SubscriptionState(OffsetResetStrategy.EARLIEST);
        this.metadata = new Metadata(0, Long.MAX_VALUE, true);
        this.metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());
        this.client = new MockClient(time, metadata);
        this.consumerClient = new ConsumerNetworkClient(new LogContext(), client, metadata, time, 100, 1000, Integer.MAX_VALUE);
        this.metrics = new Metrics(time);
        this.rebalanceListener = new MockRebalanceListener();
        this.mockOffsetCommitCallback = new MockCommitCallback();
        this.partitionAssignor.clear();

        client.setNode(node);
        this.coordinator = buildCoordinator(metrics, assignors, ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, false, true);
    }

    @After
    public void teardown() {
        this.metrics.close();
    }

    @Test
    public void testNormalHeartbeat() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // normal heartbeat
        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.NONE));
        consumerClient.poll(0);

        assertTrue(future.isDone());
        assertTrue(future.succeeded());
    }

    @Test(expected = GroupAuthorizationException.class)
    public void testGroupDescribeUnauthorized() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.GROUP_AUTHORIZATION_FAILED));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);
    }

    @Test(expected = GroupAuthorizationException.class)
    public void testGroupReadUnauthorized() {
        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        client.prepareResponse(joinGroupLeaderResponse(0, "memberId", Collections.<String, List<String>>emptyMap(),
                Errors.GROUP_AUTHORIZATION_FAILED));
        coordinator.poll(Long.MAX_VALUE);
    }

    @Test
    public void testCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // COORDINATOR_NOT_AVAILABLE will mark coordinator as unknown
        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.COORDINATOR_NOT_AVAILABLE));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(0);

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.exception(), future.exception());
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testManyInFlightAsyncCommitsWithCoordinatorDisconnect() throws Exception {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

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
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        final AtomicBoolean asyncCallbackInvoked = new AtomicBoolean(false);
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsets = singletonMap(
                new TopicPartition("foo", 0), new OffsetCommitRequest.PartitionData(13L, ""));
        consumerClient.send(coordinator.checkAndGetCoordinator(), new OffsetCommitRequest.Builder(groupId, offsets))
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
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // not_coordinator will mark coordinator as unknown
        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.NOT_COORDINATOR));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(0);

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertEquals(Errors.NOT_COORDINATOR.exception(), future.exception());
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testIllegalGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // illegal_generation will cause re-partition
        subscriptions.subscribe(singleton(topic1), rebalanceListener);
        subscriptions.assignFromSubscribed(Collections.singletonList(t1p));

        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.ILLEGAL_GENERATION));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(0);

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertEquals(Errors.ILLEGAL_GENERATION.exception(), future.exception());
        assertTrue(coordinator.rejoinNeededOrPending());
    }

    @Test
    public void testUnknownConsumerId() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // illegal_generation will cause re-partition
        subscriptions.subscribe(singleton(topic1), rebalanceListener);
        subscriptions.assignFromSubscribed(Collections.singletonList(t1p));

        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.UNKNOWN_MEMBER_ID));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(0);

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.exception(), future.exception());
        assertTrue(coordinator.rejoinNeededOrPending());
    }

    @Test
    public void testCoordinatorDisconnect() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // coordinator disconnect will mark coordinator as unknown
        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.NONE), true); // return disconnected
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(0);

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
        metadata.setTopics(singletonList(topic1));
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        client.prepareResponse(joinGroupLeaderResponse(0, consumerId, Collections.<String, List<String>>emptyMap(),
                Errors.INVALID_GROUP_ID));
        coordinator.poll(Long.MAX_VALUE);
    }

    @Test
    public void testNormalJoinGroupLeader() {
        final String consumerId = "leader";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        // ensure metadata is up-to-date for leader
        metadata.setTopics(singletonList(topic1));
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // normal join group
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, singletonList(t1p)));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.memberId().equals(consumerId) &&
                        sync.generationId() == 1 &&
                        sync.groupAssignment().containsKey(consumerId);
            }
        }, syncGroupResponse(singletonList(t1p), Errors.NONE));
        coordinator.poll(Long.MAX_VALUE);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
        assertEquals(singleton(topic1), subscriptions.groupSubscription());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(Collections.emptySet(), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(singleton(t1p), rebalanceListener.assigned);
    }

    @Test
    public void testPatternJoinGroupLeader() {
        final String consumerId = "leader";

        subscriptions.subscribe(Pattern.compile("test.*"), rebalanceListener);

        // partially update the metadata with one topic first,
        // let the leader to refresh metadata during assignment
        metadata.setTopics(singletonList(topic1));
        metadata.update(TestUtils.singletonCluster(topic1, 1), Collections.<String>emptySet(), time.milliseconds());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // normal join group
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, Arrays.asList(t1p, t2p)));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.memberId().equals(consumerId) &&
                        sync.generationId() == 1 &&
                        sync.groupAssignment().containsKey(consumerId);
            }
        }, syncGroupResponse(Arrays.asList(t1p, t2p), Errors.NONE));
        // expect client to force updating the metadata, if yes gives it both topics
        client.prepareMetadataUpdate(cluster, Collections.<String>emptySet());

        coordinator.poll(Long.MAX_VALUE);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(2, subscriptions.assignedPartitions().size());
        assertEquals(2, subscriptions.groupSubscription().size());
        assertEquals(2, subscriptions.subscription().size());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(Collections.emptySet(), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(2, rebalanceListener.assigned.size());
    }

    @Test
    public void testMetadataRefreshDuringRebalance() {
        final String consumerId = "leader";

        subscriptions.subscribe(Pattern.compile(".*"), rebalanceListener);
        metadata.needMetadataForAllTopics(true);
        metadata.update(TestUtils.singletonCluster(topic1, 1), Collections.<String>emptySet(), time.milliseconds());

        assertEquals(singleton(topic1), subscriptions.subscription());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        Map<String, List<String>> initialSubscription = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, singletonList(t1p)));

        // the metadata will be updated in flight with a new topic added
        final List<String> updatedSubscription = Arrays.asList(topic1, topic2);
        final Set<String> updatedSubscriptionSet = new HashSet<>(updatedSubscription);

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, initialSubscription, Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                final Map<String, Integer> updatedPartitions = new HashMap<>();
                for (String topic : updatedSubscription)
                    updatedPartitions.put(topic, 1);
                metadata.update(TestUtils.clusterWith(1, updatedPartitions), Collections.<String>emptySet(), time.milliseconds());
                return true;
            }
        }, syncGroupResponse(singletonList(t1p), Errors.NONE));

        List<TopicPartition> newAssignment = Arrays.asList(t1p, t2p);
        Set<TopicPartition> newAssignmentSet = new HashSet<>(newAssignment);

        Map<String, List<String>> updatedSubscriptions = singletonMap(consumerId, Arrays.asList(topic1, topic2));
        partitionAssignor.prepare(singletonMap(consumerId, newAssignment));

        // we expect to see a second rebalance with the new-found topics
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                JoinGroupRequest join = (JoinGroupRequest) body;
                ProtocolMetadata protocolMetadata = join.groupProtocols().iterator().next();
                PartitionAssignor.Subscription subscription = ConsumerProtocol.deserializeSubscription(protocolMetadata.metadata());
                protocolMetadata.metadata().rewind();
                return subscription.topics().containsAll(updatedSubscriptionSet);
            }
        }, joinGroupLeaderResponse(2, consumerId, updatedSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(newAssignment, Errors.NONE));

        coordinator.poll(Long.MAX_VALUE);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(updatedSubscriptionSet, subscriptions.subscription());
        assertEquals(newAssignmentSet, subscriptions.assignedPartitions());
        assertEquals(2, rebalanceListener.revokedCount);
        assertEquals(singleton(t1p), rebalanceListener.revoked);
        assertEquals(2, rebalanceListener.assignedCount);
        assertEquals(newAssignmentSet, rebalanceListener.assigned);
    }

    @Test
    public void testWakeupDuringJoin() {
        final String consumerId = "leader";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        // ensure metadata is up-to-date for leader
        metadata.setTopics(singletonList(topic1));
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, singletonList(t1p)));

        // prepare only the first half of the join and then trigger the wakeup
        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        consumerClient.wakeup();

        try {
            coordinator.poll(Long.MAX_VALUE);
        } catch (WakeupException e) {
            // ignore
        }

        // now complete the second half
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
        coordinator.poll(Long.MAX_VALUE);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(Collections.emptySet(), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(singleton(t1p), rebalanceListener.assigned);
    }

    @Test
    public void testNormalJoinGroupFollower() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // normal join group
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.memberId().equals(consumerId) &&
                        sync.generationId() == 1 &&
                        sync.groupAssignment().isEmpty();
            }
        }, syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.joinGroupIfNeeded(Long.MAX_VALUE);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
        assertEquals(singleton(topic1), subscriptions.groupSubscription());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(Collections.emptySet(), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(singleton(t1p), rebalanceListener.assigned);
    }

    @Test
    public void testPatternJoinGroupFollower() {
        final String consumerId = "consumer";

        subscriptions.subscribe(Pattern.compile("test.*"), rebalanceListener);

        // partially update the metadata with one topic first,
        // let the leader to refresh metadata during assignment
        metadata.setTopics(singletonList(topic1));
        metadata.update(TestUtils.singletonCluster(topic1, 1), Collections.<String>emptySet(), time.milliseconds());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // normal join group
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.memberId().equals(consumerId) &&
                        sync.generationId() == 1 &&
                        sync.groupAssignment().isEmpty();
            }
        }, syncGroupResponse(Arrays.asList(t1p, t2p), Errors.NONE));
        // expect client to force updating the metadata, if yes gives it both topics
        client.prepareMetadataUpdate(cluster, Collections.<String>emptySet());

        coordinator.joinGroupIfNeeded(Long.MAX_VALUE);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(2, subscriptions.assignedPartitions().size());
        assertEquals(2, subscriptions.subscription().size());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(2, rebalanceListener.assigned.size());
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
                return leaveRequest.memberId().equals(consumerId) &&
                        leaveRequest.groupId().equals(groupId);
            }
        }, new LeaveGroupResponse(Errors.NONE));
        coordinator.close(0);
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
                return leaveRequest.memberId().equals(consumerId) &&
                        leaveRequest.groupId().equals(groupId);
            }
        }, new LeaveGroupResponse(Errors.NONE));
        coordinator.maybeLeaveGroup();
        assertTrue(received.get());

        AbstractCoordinator.Generation generation = coordinator.generation();
        assertNull(generation);
    }

    @Test(expected = KafkaException.class)
    public void testUnexpectedErrorOnSyncGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.<TopicPartition>emptyList(), Errors.UNKNOWN_SERVER_ERROR));
        coordinator.joinGroupIfNeeded(Long.MAX_VALUE);
    }

    @Test
    public void testUnknownMemberIdOnSyncGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // join initially, but let coordinator returns unknown member id
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.<TopicPartition>emptyList(), Errors.UNKNOWN_MEMBER_ID));

        // now we should see a new join with the empty UNKNOWN_MEMBER_ID
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                JoinGroupRequest joinRequest = (JoinGroupRequest) body;
                return joinRequest.memberId().equals(JoinGroupRequest.UNKNOWN_MEMBER_ID);
            }
        }, joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.joinGroupIfNeeded(Long.MAX_VALUE);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
    }

    @Test
    public void testRebalanceInProgressOnSyncGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.<TopicPartition>emptyList(), Errors.REBALANCE_IN_PROGRESS));

        // then let the full join/sync finish successfully
        client.prepareResponse(joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.joinGroupIfNeeded(Long.MAX_VALUE);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
    }

    @Test
    public void testIllegalGenerationOnSyncGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.<TopicPartition>emptyList(), Errors.ILLEGAL_GENERATION));

        // then let the full join/sync finish successfully
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                JoinGroupRequest joinRequest = (JoinGroupRequest) body;
                return joinRequest.memberId().equals(JoinGroupRequest.UNKNOWN_MEMBER_ID);
            }
        }, joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.joinGroupIfNeeded(Long.MAX_VALUE);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
    }

    @Test
    public void testMetadataChangeTriggersRebalance() {
        final String consumerId = "consumer";

        // ensure metadata is up-to-date for leader
        metadata.setTopics(singletonList(topic1));
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(singletonMap(consumerId, singletonList(t1p)));

        // the leader is responsible for picking up metadata changes and forcing a group rebalance
        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));

        coordinator.poll(Long.MAX_VALUE);

        assertFalse(coordinator.rejoinNeededOrPending());

        // a new partition is added to the topic
        metadata.update(TestUtils.singletonCluster(topic1, 2), Collections.<String>emptySet(), time.milliseconds());

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
        metadata.setTopics(topics);

        // we only have metadata for one topic initially
        metadata.update(TestUtils.singletonCluster(topic1, 1), Collections.<String>emptySet(), time.milliseconds());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // prepare initial rebalance
        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, topics);
        partitionAssignor.prepare(singletonMap(consumerId, Collections.singletonList(tp1)));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                if (sync.memberId().equals(consumerId) &&
                        sync.generationId() == 1 &&
                        sync.groupAssignment().containsKey(consumerId)) {
                    // trigger the metadata update including both topics after the sync group request has been sent
                    Map<String, Integer> topicPartitionCounts = new HashMap<>();
                    topicPartitionCounts.put(topic1, 1);
                    topicPartitionCounts.put(topic2, 1);
                    metadata.update(TestUtils.singletonCluster(topicPartitionCounts), Collections.<String>emptySet(), time.milliseconds());
                    return true;
                }
                return false;
            }
        }, syncGroupResponse(Collections.singletonList(tp1), Errors.NONE));

        // the metadata update should trigger a second rebalance
        client.prepareResponse(joinGroupLeaderResponse(2, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(Arrays.asList(tp1, tp2), Errors.NONE));

        coordinator.poll(Long.MAX_VALUE);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(new HashSet<>(Arrays.asList(tp1, tp2)), subscriptions.assignedPartitions());
    }

    @Test
    public void testRebalanceAfterTopicUnavailableWithSubscribe() {
        unavailableTopicTest(false, false, Collections.<String>emptySet());
    }

    @Test
    public void testRebalanceAfterTopicUnavailableWithPatternSubscribe() {
        unavailableTopicTest(true, false, Collections.<String>emptySet());
    }

    @Test
    public void testRebalanceAfterNotMatchingTopicUnavailableWithPatternSSubscribe() {
        unavailableTopicTest(true, false, Collections.singleton("notmatching"));
    }

    @Test
    public void testAssignWithTopicUnavailable() {
        unavailableTopicTest(true, false, Collections.<String>emptySet());
    }

    private void unavailableTopicTest(boolean patternSubscribe, boolean assign, Set<String> unavailableTopicsInLastMetadata) {
        final String consumerId = "consumer";

        metadata.setTopics(singletonList(topic1));
        client.prepareMetadataUpdate(Cluster.empty(), Collections.singleton("test1"));

        if (assign)
            subscriptions.assignFromUser(singleton(t1p));
        else if (patternSubscribe)
            subscriptions.subscribe(Pattern.compile("test.*"), rebalanceListener);
        else
            subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        Map<String, List<String>> memberSubscriptions = singletonMap(consumerId, singletonList(topic1));
        partitionAssignor.prepare(Collections.<String, List<TopicPartition>>emptyMap());

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(Collections.<TopicPartition>emptyList(), Errors.NONE));
        coordinator.poll(Long.MAX_VALUE);
        if (!assign) {
            assertFalse(coordinator.rejoinNeededOrPending());
            assertEquals(Collections.<TopicPartition>emptySet(), rebalanceListener.assigned);
        }
        assertTrue("Metadata refresh not requested for unavailable partitions", metadata.updateRequested());

        client.prepareMetadataUpdate(cluster, unavailableTopicsInLastMetadata);
        client.poll(0, time.milliseconds());
        client.prepareResponse(joinGroupLeaderResponse(2, consumerId, memberSubscriptions, Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
        coordinator.poll(Long.MAX_VALUE);

        assertFalse("Metadata refresh requested unnecessarily", metadata.updateRequested());
        if (!assign) {
            assertFalse(coordinator.rejoinNeededOrPending());
            assertEquals(singleton(t1p), rebalanceListener.assigned);
        }
    }

    @Test
    public void testExcludeInternalTopicsConfigOption() {
        subscriptions.subscribe(Pattern.compile(".*"), rebalanceListener);

        metadata.update(TestUtils.singletonCluster(TestUtils.GROUP_METADATA_TOPIC_NAME, 2), Collections.<String>emptySet(), time.milliseconds());

        assertFalse(subscriptions.subscription().contains(TestUtils.GROUP_METADATA_TOPIC_NAME));
    }

    @Test
    public void testIncludeInternalTopicsConfigOption() {
        coordinator = buildCoordinator(new Metrics(), assignors, false, false, true);
        subscriptions.subscribe(Pattern.compile(".*"), rebalanceListener);

        metadata.update(TestUtils.singletonCluster(TestUtils.GROUP_METADATA_TOPIC_NAME, 2), Collections.<String>emptySet(), time.milliseconds());

        assertTrue(subscriptions.subscription().contains(TestUtils.GROUP_METADATA_TOPIC_NAME));
    }

    @Test
    public void testRejoinGroup() {
        String otherTopic = "otherTopic";

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        // join the group once
        joinAsFollowerAndReceiveAssignment("consumer", coordinator, singletonList(t1p));

        assertEquals(1, rebalanceListener.revokedCount);
        assertTrue(rebalanceListener.revoked.isEmpty());
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(singleton(t1p), rebalanceListener.assigned);

        // and join the group again
        subscriptions.subscribe(new HashSet<>(Arrays.asList(topic1, otherTopic)), rebalanceListener);
        client.prepareResponse(joinGroupFollowerResponse(2, "consumer", "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
        coordinator.joinGroupIfNeeded(Long.MAX_VALUE);

        assertEquals(2, rebalanceListener.revokedCount);
        assertEquals(singleton(t1p), rebalanceListener.revoked);
        assertEquals(2, rebalanceListener.assignedCount);
        assertEquals(singleton(t1p), rebalanceListener.assigned);
    }

    @Test
    public void testDisconnectInJoin() {
        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // disconnected from original coordinator will cause re-discover and join again
        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE), true);
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
        coordinator.joinGroupIfNeeded(Long.MAX_VALUE);

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(singleton(t1p), subscriptions.assignedPartitions());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(singleton(t1p), rebalanceListener.assigned);
    }

    @Test(expected = ApiException.class)
    public void testInvalidSessionTimeout() {
        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // coordinator doesn't like the session timeout
        client.prepareResponse(joinGroupFollowerResponse(0, "consumer", "", Errors.INVALID_SESSION_TIMEOUT));
        coordinator.joinGroupIfNeeded(Long.MAX_VALUE);
    }

    @Test
    public void testCommitOffsetOnly() {
        subscriptions.assignFromUser(singleton(t1p));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

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
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

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

        ConsumerCoordinator coordinator = buildCoordinator(new Metrics(), assignors,
                ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, true, true);

        subscriptions.subscribe(singleton(topic1), rebalanceListener);
        joinAsFollowerAndReceiveAssignment(consumerId, coordinator, singletonList(t1p));
        subscriptions.seek(t1p, 100);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        time.sleep(autoCommitIntervalMs);
        coordinator.poll(Long.MAX_VALUE);
        assertFalse(client.hasPendingResponses());
    }

    @Test
    public void testAutoCommitRetryBackoff() {
        final String consumerId = "consumer";
        ConsumerCoordinator coordinator = buildCoordinator(new Metrics(), assignors,
                ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, true, true);
        subscriptions.subscribe(singleton(topic1), rebalanceListener);
        joinAsFollowerAndReceiveAssignment(consumerId, coordinator, singletonList(t1p));

        subscriptions.seek(t1p, 100);
        time.sleep(autoCommitIntervalMs);

        // Send an offset commit, but let it fail with a retriable error
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NOT_COORDINATOR);
        coordinator.poll(Long.MAX_VALUE);
        assertTrue(coordinator.coordinatorUnknown());

        // After the disconnect, we should rediscover the coordinator
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.poll(Long.MAX_VALUE);

        subscriptions.seek(t1p, 200);

        // Until the retry backoff has expired, we should not retry the offset commit
        time.sleep(retryBackoffMs / 2);
        coordinator.poll(Long.MAX_VALUE);
        assertEquals(0, client.inFlightRequestCount());

        // Once the backoff expires, we should retry
        time.sleep(retryBackoffMs / 2);
        coordinator.poll(Long.MAX_VALUE);
        assertEquals(1, client.inFlightRequestCount());
        respondToOffsetCommitRequest(singletonMap(t1p, 200L), Errors.NONE);
    }

    @Test
    public void testAutoCommitAwaitsInterval() {
        final String consumerId = "consumer";
        ConsumerCoordinator coordinator = buildCoordinator(new Metrics(), assignors,
                ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, true, true);
        subscriptions.subscribe(singleton(topic1), rebalanceListener);
        joinAsFollowerAndReceiveAssignment(consumerId, coordinator, singletonList(t1p));

        subscriptions.seek(t1p, 100);
        time.sleep(autoCommitIntervalMs);

        // Send the offset commit request, but do not respond
        coordinator.poll(Long.MAX_VALUE);
        assertEquals(1, client.inFlightRequestCount());

        time.sleep(autoCommitIntervalMs / 2);

        // Ensure that no additional offset commit is sent
        coordinator.poll(Long.MAX_VALUE);
        assertEquals(1, client.inFlightRequestCount());

        respondToOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        coordinator.poll(Long.MAX_VALUE);
        assertEquals(0, client.inFlightRequestCount());

        subscriptions.seek(t1p, 200);

        // If we poll again before the auto-commit interval, there should be no new sends
        coordinator.poll(Long.MAX_VALUE);
        assertEquals(0, client.inFlightRequestCount());

        // After the remainder of the interval passes, we send a new offset commit
        time.sleep(autoCommitIntervalMs / 2);
        coordinator.poll(Long.MAX_VALUE);
        assertEquals(1, client.inFlightRequestCount());
        respondToOffsetCommitRequest(singletonMap(t1p, 200L), Errors.NONE);
    }

    @Test
    public void testAutoCommitDynamicAssignmentRebalance() {
        final String consumerId = "consumer";

        ConsumerCoordinator coordinator = buildCoordinator(new Metrics(), assignors,
                ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, true, true);

        subscriptions.subscribe(singleton(topic1), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // haven't joined, so should not cause a commit
        time.sleep(autoCommitIntervalMs);
        consumerClient.poll(0);

        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
        coordinator.joinGroupIfNeeded(Long.MAX_VALUE);

        subscriptions.seek(t1p, 100);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        time.sleep(autoCommitIntervalMs);
        coordinator.poll(Long.MAX_VALUE);
        assertFalse(client.hasPendingResponses());
    }

    @Test
    public void testAutoCommitManualAssignment() {
        ConsumerCoordinator coordinator = buildCoordinator(new Metrics(), assignors,
                ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, true, true);

        subscriptions.assignFromUser(singleton(t1p));
        subscriptions.seek(t1p, 100);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        time.sleep(autoCommitIntervalMs);
        coordinator.poll(Long.MAX_VALUE);
        assertFalse(client.hasPendingResponses());
    }

    @Test
    public void testAutoCommitManualAssignmentCoordinatorUnknown() {
        ConsumerCoordinator coordinator = buildCoordinator(new Metrics(), assignors,
                ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, true, true);

        subscriptions.assignFromUser(singleton(t1p));
        subscriptions.seek(t1p, 100);

        // no commit initially since coordinator is unknown
        consumerClient.poll(0);
        time.sleep(autoCommitIntervalMs);
        consumerClient.poll(0);

        // now find the coordinator
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // sleep only for the retry backoff
        time.sleep(retryBackoffMs);
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        coordinator.poll(Long.MAX_VALUE);
        assertFalse(client.hasPendingResponses());
    }

    @Test
    public void testCommitOffsetMetadata() {
        subscriptions.assignFromUser(singleton(t1p));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

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
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);
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
        client.prepareResponse(new LeaveGroupResponse(Errors.NONE));
        subscriptions.unsubscribe();
        coordinator.maybeLeaveGroup();
        subscriptions.assignFromUser(singleton(t1p));

        // the client should not reuse generation/memberId from auto-subscribed generation
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                OffsetCommitRequest commitRequest = (OffsetCommitRequest) body;
                return commitRequest.memberId().equals(OffsetCommitRequest.DEFAULT_MEMBER_ID) &&
                        commitRequest.generationId() == OffsetCommitRequest.DEFAULT_GENERATION_ID;
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
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), mockOffsetCommitCallback);
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertEquals(invokedBeforeTest + 1, mockOffsetCommitCallback.invoked);
        assertTrue(mockOffsetCommitCallback.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testCommitOffsetAsyncCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

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
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

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
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

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
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // sync commit with coordinator disconnected (should connect, get metadata, and then submit the commit request)
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NOT_COORDINATOR);
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), Long.MAX_VALUE);
    }

    @Test
    public void testCommitOffsetSyncCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // sync commit with coordinator disconnected (should connect, get metadata, and then submit the commit request)
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), Long.MAX_VALUE);
    }

    @Test
    public void testCommitOffsetSyncCoordinatorDisconnected() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // sync commit with coordinator disconnected (should connect, get metadata, and then submit the commit request)
        prepareOffsetCommitRequestDisconnect(singletonMap(t1p, 100L));
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.NONE);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), Long.MAX_VALUE);
    }

    @Test
    public void testAsyncCommitCallbacksInvokedPriorToSyncCommitCompletion() throws Exception {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

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
                coordinator.commitOffsetsSync(singletonMap(t1p, secondOffset), 10000);
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
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        client.prepareResponse(offsetCommitResponse(singletonMap(t1p, Errors.UNKNOWN_TOPIC_OR_PARTITION)));
        client.prepareResponse(offsetCommitResponse(singletonMap(t1p, Errors.NONE)));

        assertTrue(coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L, "metadata")), 10000));
    }

    @Test(expected = OffsetMetadataTooLarge.class)
    public void testCommitOffsetMetadataTooLarge() {
        // since offset metadata is provided by the user, we have to propagate the exception so they can handle it
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.OFFSET_METADATA_TOO_LARGE);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L, "metadata")), Long.MAX_VALUE);
    }

    @Test(expected = CommitFailedException.class)
    public void testCommitOffsetIllegalGeneration() {
        // we cannot retry if a rebalance occurs before the commit completed
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.ILLEGAL_GENERATION);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L, "metadata")), Long.MAX_VALUE);
    }

    @Test(expected = CommitFailedException.class)
    public void testCommitOffsetUnknownMemberId() {
        // we cannot retry if a rebalance occurs before the commit completed
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.UNKNOWN_MEMBER_ID);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L, "metadata")), Long.MAX_VALUE);
    }

    @Test(expected = CommitFailedException.class)
    public void testCommitOffsetRebalanceInProgress() {
        // we cannot retry if a rebalance occurs before the commit completed
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.REBALANCE_IN_PROGRESS);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L, "metadata")), Long.MAX_VALUE);
    }

    @Test(expected = KafkaException.class)
    public void testCommitOffsetSyncCallbackWithNonRetriableException() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        // sync commit with invalid partitions should throw if we have no callback
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.UNKNOWN_SERVER_ERROR);
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), Long.MAX_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCommitSyncNegativeOffset() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(-1L)), Long.MAX_VALUE);
    }

    @Test
    public void testCommitAsyncNegativeOffset() {
        int invokedBeforeTest = mockOffsetCommitCallback.invoked;
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(-1L)), mockOffsetCommitCallback);
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertEquals(invokedBeforeTest + 1, mockOffsetCommitCallback.invoked);
        assertTrue(mockOffsetCommitCallback.exception instanceof IllegalArgumentException);
    }

    @Test
    public void testCommitOffsetSyncWithoutFutureGetsCompleted() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);
        assertFalse(coordinator.commitOffsetsSync(singletonMap(t1p, new OffsetAndMetadata(100L)), 0));
    }

    @Test
    public void testRefreshOffset() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L));
        coordinator.refreshCommittedOffsetsIfNeeded(Long.MAX_VALUE);

        assertEquals(Collections.emptySet(), subscriptions.missingFetchPositions());
        assertTrue(subscriptions.hasAllFetchPositions());
        assertEquals(100L, subscriptions.position(t1p).longValue());
    }

    @Test
    public void testRefreshOffsetLoadInProgress() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L));
        coordinator.refreshCommittedOffsetsIfNeeded(Long.MAX_VALUE);

        assertEquals(Collections.emptySet(), subscriptions.missingFetchPositions());
        assertTrue(subscriptions.hasAllFetchPositions());
        assertEquals(100L, subscriptions.position(t1p).longValue());
    }

    @Test
    public void testRefreshOffsetsGroupNotAuthorized() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(Errors.GROUP_AUTHORIZATION_FAILED));
        try {
            coordinator.refreshCommittedOffsetsIfNeeded(Long.MAX_VALUE);
            fail("Expected group authorization error");
        } catch (GroupAuthorizationException e) {
            assertEquals(groupId, e.groupId());
        }
    }

    @Test(expected = KafkaException.class)
    public void testRefreshOffsetUnknownTopicOrPartition() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.UNKNOWN_TOPIC_OR_PARTITION, "", 100L));
        coordinator.refreshCommittedOffsetsIfNeeded(Long.MAX_VALUE);
    }

    @Test
    public void testRefreshOffsetNotCoordinatorForConsumer() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(Errors.NOT_COORDINATOR));
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", 100L));
        coordinator.refreshCommittedOffsetsIfNeeded(Long.MAX_VALUE);

        assertEquals(Collections.emptySet(), subscriptions.missingFetchPositions());
        assertTrue(subscriptions.hasAllFetchPositions());
        assertEquals(100L, subscriptions.position(t1p).longValue());
    }

    @Test
    public void testRefreshOffsetWithNoFetchableOffsets() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);

        subscriptions.assignFromUser(singleton(t1p));
        client.prepareResponse(offsetFetchResponse(t1p, Errors.NONE, "", -1L));
        coordinator.refreshCommittedOffsetsIfNeeded(Long.MAX_VALUE);

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
        coordinator.refreshCommittedOffsetsIfNeeded(Long.MAX_VALUE);

        assertEquals(Collections.emptySet(), subscriptions.missingFetchPositions());
        assertTrue(subscriptions.hasAllFetchPositions());
        assertEquals(500L, subscriptions.position(t1p).longValue());
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testNoCoordinatorDiscoveryIfPartitionAwaitingReset() {
        assertTrue(coordinator.coordinatorUnknown());

        subscriptions.assignFromUser(singleton(t1p));
        subscriptions.requestOffsetReset(t1p, OffsetResetStrategy.EARLIEST);
        coordinator.refreshCommittedOffsetsIfNeeded(Long.MAX_VALUE);

        assertEquals(Collections.emptySet(), subscriptions.missingFetchPositions());
        assertFalse(subscriptions.hasAllFetchPositions());
        assertEquals(Collections.singleton(t1p), subscriptions.partitionsNeedingReset(time.milliseconds()));
        assertEquals(OffsetResetStrategy.EARLIEST, subscriptions.resetStrategy(t1p));
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testEnsureActiveGroupWithinBlackoutPeriodAfterAuthenticationFailure() {
        client.authenticationFailed(node, 300);

        try {
            coordinator.ensureActiveGroup();
            fail("Expected an authentication error.");
        } catch (AuthenticationException e) {
            // OK
        }

        time.sleep(30); // wait less than the blackout period
        assertTrue(client.connectionFailed(node));

        try {
            coordinator.ensureActiveGroup();
            fail("Expected an authentication error.");
        } catch (AuthenticationException e) {
            // OK
        }
    }

    @Test
    public void testProtocolMetadataOrder() {
        RoundRobinAssignor roundRobin = new RoundRobinAssignor();
        RangeAssignor range = new RangeAssignor();

        try (Metrics metrics = new Metrics(time)) {
            ConsumerCoordinator coordinator = buildCoordinator(metrics, Arrays.<PartitionAssignor>asList(roundRobin, range),
                                                               ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, false, true);
            List<ProtocolMetadata> metadata = coordinator.metadata();
            assertEquals(2, metadata.size());
            assertEquals(roundRobin.name(), metadata.get(0).name());
            assertEquals(range.name(), metadata.get(1).name());
        }

        try (Metrics metrics = new Metrics(time)) {
            ConsumerCoordinator coordinator = buildCoordinator(metrics, Arrays.<PartitionAssignor>asList(range, roundRobin),
                                                               ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, false, true);
            List<ProtocolMetadata> metadata = coordinator.metadata();
            assertEquals(2, metadata.size());
            assertEquals(range.name(), metadata.get(0).name());
            assertEquals(roundRobin.name(), metadata.get(1).name());
        }
    }

    @Test
    public void testCloseDynamicAssignment() throws Exception {
        ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, true);
        gracefulCloseTest(coordinator, true);
    }

    @Test
    public void testCloseManualAssignment() throws Exception {
        ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(false, true, true);
        gracefulCloseTest(coordinator, false);
    }

    @Test
    public void shouldNotLeaveGroupWhenLeaveGroupFlagIsFalse() throws Exception {
        final ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, false);
        gracefulCloseTest(coordinator, false);
    }

    @Test
    public void testCloseCoordinatorNotKnownManualAssignment() throws Exception {
        ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(false, true, true);
        makeCoordinatorUnknown(coordinator, Errors.NOT_COORDINATOR);
        time.sleep(autoCommitIntervalMs);
        closeVerifyTimeout(coordinator, 1000, 60000, 1000, 1000);
    }

    @Test
    public void testCloseCoordinatorNotKnownNoCommits() throws Exception {
        ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, true);
        makeCoordinatorUnknown(coordinator, Errors.NOT_COORDINATOR);
        closeVerifyTimeout(coordinator, 1000, 60000, 0, 0);
    }

    @Test
    public void testCloseCoordinatorNotKnownWithCommits() throws Exception {
        ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, true);
        makeCoordinatorUnknown(coordinator, Errors.NOT_COORDINATOR);
        time.sleep(autoCommitIntervalMs);
        closeVerifyTimeout(coordinator, 1000, 60000, 1000, 1000);
    }

    @Test
    public void testCloseCoordinatorUnavailableNoCommits() throws Exception {
        ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, true);
        makeCoordinatorUnknown(coordinator, Errors.COORDINATOR_NOT_AVAILABLE);
        closeVerifyTimeout(coordinator, 1000, 60000, 0, 0);
    }

    @Test
    public void testCloseTimeoutCoordinatorUnavailableForCommit() throws Exception {
        ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, true);
        makeCoordinatorUnknown(coordinator, Errors.COORDINATOR_NOT_AVAILABLE);
        time.sleep(autoCommitIntervalMs);
        closeVerifyTimeout(coordinator, 1000, 60000, 1000, 1000);
    }

    @Test
    public void testCloseMaxWaitCoordinatorUnavailableForCommit() throws Exception {
        ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, true);
        makeCoordinatorUnknown(coordinator, Errors.COORDINATOR_NOT_AVAILABLE);
        time.sleep(autoCommitIntervalMs);
        closeVerifyTimeout(coordinator, Long.MAX_VALUE, 60000, 60000, 60000);
    }

    @Test
    public void testCloseNoResponseForCommit() throws Exception {
        ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, true);
        time.sleep(autoCommitIntervalMs);
        closeVerifyTimeout(coordinator, Long.MAX_VALUE, 60000, 60000, 60000);
    }

    @Test
    public void testCloseNoResponseForLeaveGroup() throws Exception {
        ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, false, true);
        closeVerifyTimeout(coordinator, Long.MAX_VALUE, 60000, 60000, 60000);
    }

    @Test
    public void testCloseNoWait() throws Exception {
        ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, true);
        time.sleep(autoCommitIntervalMs);
        closeVerifyTimeout(coordinator, 0, 60000, 0, 0);
    }

    @Test
    public void testHeartbeatThreadClose() throws Exception {
        groupId = "testCloseTimeoutWithHeartbeatThread";
        ConsumerCoordinator coordinator = prepareCoordinatorForCloseTest(true, true, true);
        coordinator.ensureActiveGroup();
        time.sleep(heartbeatIntervalMs + 100);
        Thread.yield(); // Give heartbeat thread a chance to attempt heartbeat
        closeVerifyTimeout(coordinator, Long.MAX_VALUE, 60000, 60000, 60000);
        Thread[] threads = new Thread[Thread.activeCount()];
        int threadCount = Thread.enumerate(threads);
        for (int i = 0; i < threadCount; i++)
            assertFalse("Heartbeat thread active after close", threads[i].getName().contains(groupId));
    }

    @Test
    public void testAutoCommitAfterCoordinatorBackToService() {
        ConsumerCoordinator coordinator = buildCoordinator(new Metrics(), assignors,
                ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, true, true);
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
        assertEquals(100L, subscriptions.position(t1p).longValue());
    }

    private ConsumerCoordinator prepareCoordinatorForCloseTest(final boolean useGroupManagement,
                                                               final boolean autoCommit,
                                                               final boolean leaveGroup) {
        final String consumerId = "consumer";
        ConsumerCoordinator coordinator = buildCoordinator(new Metrics(), assignors,
                ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, autoCommit, leaveGroup);
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);
        if (useGroupManagement) {
            subscriptions.subscribe(singleton(topic1), rebalanceListener);
            client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
            client.prepareResponse(syncGroupResponse(singletonList(t1p), Errors.NONE));
            coordinator.joinGroupIfNeeded(Long.MAX_VALUE);
        } else
            subscriptions.assignFromUser(singleton(t1p));

        subscriptions.seek(t1p, 100);
        coordinator.poll(Long.MAX_VALUE);

        return coordinator;
    }

    private void makeCoordinatorUnknown(ConsumerCoordinator coordinator, Errors error) {
        time.sleep(sessionTimeoutMs);
        coordinator.sendHeartbeatRequest();
        client.prepareResponse(heartbeatResponse(error));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(0);
        assertTrue(coordinator.coordinatorUnknown());
    }
    private void closeVerifyTimeout(final ConsumerCoordinator coordinator,
            final long closeTimeoutMs, final long requestTimeoutMs,
            long expectedMinTimeMs, long expectedMaxTimeMs) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            boolean coordinatorUnknown = coordinator.coordinatorUnknown();
            // Run close on a different thread. Coordinator is locked by this thread, so it is
            // not safe to use the coordinator from the main thread until the task completes.
            Future<?> future = executor.submit(new Runnable() {
                @Override
                public void run() {
                    coordinator.close(Math.min(closeTimeoutMs, requestTimeoutMs));
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
                return commitRequest.groupId().equals(groupId);
            }
        }, new OffsetCommitResponse(new HashMap<TopicPartition, Errors>()));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                leaveGroupRequested.set(true);
                LeaveGroupRequest leaveRequest = (LeaveGroupRequest) body;
                return leaveRequest.groupId().equals(groupId);
            }
        }, new LeaveGroupResponse(Errors.NONE));

        coordinator.close();
        assertTrue("Commit not requested", commitRequested.get());
        assertEquals("leaveGroupRequested should be " + shouldLeaveGroup, shouldLeaveGroup, leaveGroupRequested.get());
    }

    private ConsumerCoordinator buildCoordinator(final Metrics metrics,
                                                 final List<PartitionAssignor> assignors,
                                                 final boolean excludeInternalTopics,
                                                 final boolean autoCommitEnabled,
                                                 final boolean leaveGroup) {
        return new ConsumerCoordinator(
                new LogContext(),
                consumerClient,
                groupId,
                rebalanceTimeoutMs,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                assignors,
                metadata,
                subscriptions,
                metrics,
                "consumer" + groupId,
                time,
                retryBackoffMs,
                autoCommitEnabled,
                autoCommitIntervalMs,
                null,
                excludeInternalTopics,
                leaveGroup);
    }

    private FindCoordinatorResponse groupCoordinatorResponse(Node node, Errors error) {
        return new FindCoordinatorResponse(error, node);
    }

    private HeartbeatResponse heartbeatResponse(Errors error) {
        return new HeartbeatResponse(error);
    }

    private JoinGroupResponse joinGroupLeaderResponse(int generationId,
                                                      String memberId,
                                                      Map<String, List<String>> subscriptions,
                                                      Errors error) {
        Map<String, ByteBuffer> metadata = new HashMap<>();
        for (Map.Entry<String, List<String>> subscriptionEntry : subscriptions.entrySet()) {
            PartitionAssignor.Subscription subscription = new PartitionAssignor.Subscription(subscriptionEntry.getValue());
            ByteBuffer buf = ConsumerProtocol.serializeSubscription(subscription);
            metadata.put(subscriptionEntry.getKey(), buf);
        }
        return new JoinGroupResponse(error, generationId, partitionAssignor.name(), memberId, memberId, metadata);
    }

    private JoinGroupResponse joinGroupFollowerResponse(int generationId, String memberId, String leaderId, Errors error) {
        return new JoinGroupResponse(error, generationId, partitionAssignor.name(), memberId, leaderId,
                Collections.<String, ByteBuffer>emptyMap());
    }

    private SyncGroupResponse syncGroupResponse(List<TopicPartition> partitions, Errors error) {
        ByteBuffer buf = ConsumerProtocol.serializeAssignment(new PartitionAssignor.Assignment(partitions));
        return new SyncGroupResponse(error, buf);
    }

    private OffsetCommitResponse offsetCommitResponse(Map<TopicPartition, Errors> responseData) {
        return new OffsetCommitResponse(responseData);
    }

    private OffsetFetchResponse offsetFetchResponse(Errors topLevelError) {
        return new OffsetFetchResponse(topLevelError, Collections.<TopicPartition, OffsetFetchResponse.PartitionData>emptyMap());
    }

    private OffsetFetchResponse offsetFetchResponse(TopicPartition tp, Errors partitionLevelError, String metadata, long offset) {
        OffsetFetchResponse.PartitionData data = new OffsetFetchResponse.PartitionData(offset, metadata, partitionLevelError);
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
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(assignment, Errors.NONE));
        coordinator.joinGroupIfNeeded(Long.MAX_VALUE);
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
                Map<TopicPartition, OffsetCommitRequest.PartitionData> offsets = req.offsetData();
                if (offsets.size() != expectedOffsets.size())
                    return false;

                for (Map.Entry<TopicPartition, Long> expectedOffset : expectedOffsets.entrySet()) {
                    if (!offsets.containsKey(expectedOffset.getKey()))
                        return false;

                    OffsetCommitRequest.PartitionData offsetCommitData = offsets.get(expectedOffset.getKey());
                    if (offsetCommitData.offset != expectedOffset.getValue())
                        return false;
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
