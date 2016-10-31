/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.clients.ClientRequest;
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
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConsumerCoordinatorTest {

    private String topicName = "test";
    private String groupId = "test-group";
    private TopicPartition tp = new TopicPartition(topicName, 0);
    private int rebalanceTimeoutMs = 60000;
    private int sessionTimeoutMs = 10000;
    private int heartbeatIntervalMs = 5000;
    private long retryBackoffMs = 100;
    private boolean autoCommitEnabled = false;
    private int autoCommitIntervalMs = 2000;
    private MockPartitionAssignor partitionAssignor = new MockPartitionAssignor();
    private List<PartitionAssignor> assignors = Collections.<PartitionAssignor>singletonList(partitionAssignor);
    private MockTime time;
    private MockClient client;
    private Cluster cluster = TestUtils.singletonCluster(topicName, 1);
    private Node node = cluster.nodes().get(0);
    private SubscriptionState subscriptions;
    private Metadata metadata;
    private Metrics metrics;
    private ConsumerNetworkClient consumerClient;
    private MockRebalanceListener rebalanceListener;
    private MockCommitCallback defaultOffsetCommitCallback;
    private ConsumerCoordinator coordinator;


    @Before
    public void setup() {
        this.time = new MockTime();
        this.client = new MockClient(time);
        this.subscriptions = new SubscriptionState(OffsetResetStrategy.EARLIEST);
        this.metadata = new Metadata(0, Long.MAX_VALUE);
        this.metadata.update(cluster, time.milliseconds());
        this.consumerClient = new ConsumerNetworkClient(client, metadata, time, 100, 1000);
        this.metrics = new Metrics(time);
        this.rebalanceListener = new MockRebalanceListener();
        this.defaultOffsetCommitCallback = new MockCommitCallback();
        this.partitionAssignor.clear();

        client.setNode(node);
        this.coordinator = buildCoordinator(metrics, assignors, ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, autoCommitEnabled);
    }

    @After
    public void teardown() {
        this.metrics.close();
    }

    @Test
    public void testNormalHeartbeat() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // normal heartbeat
        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.NONE.code()));
        consumerClient.poll(0);

        assertTrue(future.isDone());
        assertTrue(future.succeeded());
    }

    @Test(expected = GroupAuthorizationException.class)
    public void testGroupDescribeUnauthorized() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.GROUP_AUTHORIZATION_FAILED.code()));
        coordinator.ensureCoordinatorReady();
    }

    @Test(expected = GroupAuthorizationException.class)
    public void testGroupReadUnauthorized() {
        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(joinGroupLeaderResponse(0, "memberId", Collections.<String, List<String>>emptyMap(),
                Errors.GROUP_AUTHORIZATION_FAILED.code()));
        coordinator.poll(time.milliseconds());
    }

    @Test
    public void testCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // GROUP_COORDINATOR_NOT_AVAILABLE will mark coordinator as unknown
        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code()));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(0);

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertEquals(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.exception(), future.exception());
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // not_coordinator will mark coordinator as unknown
        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.NOT_COORDINATOR_FOR_GROUP.code()));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(0);

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertEquals(Errors.NOT_COORDINATOR_FOR_GROUP.exception(), future.exception());
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testIllegalGeneration() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // illegal_generation will cause re-partition
        subscriptions.subscribe(singleton(topicName), rebalanceListener);
        subscriptions.assignFromSubscribed(Collections.singletonList(tp));

        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.ILLEGAL_GENERATION.code()));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(0);

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertEquals(Errors.ILLEGAL_GENERATION.exception(), future.exception());
        assertTrue(coordinator.needRejoin());
    }

    @Test
    public void testUnknownConsumerId() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // illegal_generation will cause re-partition
        subscriptions.subscribe(singleton(topicName), rebalanceListener);
        subscriptions.assignFromSubscribed(Collections.singletonList(tp));

        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.UNKNOWN_MEMBER_ID.code()));
        time.sleep(sessionTimeoutMs);
        consumerClient.poll(0);

        assertTrue(future.isDone());
        assertTrue(future.failed());
        assertEquals(Errors.UNKNOWN_MEMBER_ID.exception(), future.exception());
        assertTrue(coordinator.needRejoin());
    }

    @Test
    public void testCoordinatorDisconnect() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // coordinator disconnect will mark coordinator as unknown
        time.sleep(sessionTimeoutMs);
        RequestFuture<Void> future = coordinator.sendHeartbeatRequest(); // should send out the heartbeat
        assertEquals(1, consumerClient.pendingRequestCount());
        assertFalse(future.isDone());

        client.prepareResponse(heartbeatResponse(Errors.NONE.code()), true); // return disconnected
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

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        // ensure metadata is up-to-date for leader
        metadata.setTopics(Arrays.asList(topicName));
        metadata.update(cluster, time.milliseconds());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(joinGroupLeaderResponse(0, consumerId, Collections.<String, List<String>>emptyMap(),
                Errors.INVALID_GROUP_ID.code()));
        coordinator.poll(time.milliseconds());
    }

    @Test
    public void testNormalJoinGroupLeader() {
        final String consumerId = "leader";

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        // ensure metadata is up-to-date for leader
        metadata.setTopics(Arrays.asList(topicName));
        metadata.update(cluster, time.milliseconds());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // normal join group
        Map<String, List<String>> memberSubscriptions = Collections.singletonMap(consumerId, Arrays.asList(topicName));
        partitionAssignor.prepare(Collections.singletonMap(consumerId, singletonList(tp)));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE.code()));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                SyncGroupRequest sync = new SyncGroupRequest(request.request().body());
                return sync.memberId().equals(consumerId) &&
                        sync.generationId() == 1 &&
                        sync.groupAssignment().containsKey(consumerId);
            }
        }, syncGroupResponse(singletonList(tp), Errors.NONE.code()));
        coordinator.poll(time.milliseconds());

        assertFalse(coordinator.needRejoin());
        assertEquals(singleton(tp), subscriptions.assignedPartitions());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(Collections.emptySet(), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(singleton(tp), rebalanceListener.assigned);
    }

    @Test
    public void testMetadataRefreshDuringRebalance() {
        final String consumerId = "leader";
        final String otherTopicName = "otherTopic";
        TopicPartition otherPartition = new TopicPartition(otherTopicName, 0);

        subscriptions.subscribe(Pattern.compile(".*"), rebalanceListener);
        metadata.needMetadataForAllTopics(true);
        metadata.update(cluster, time.milliseconds());

        assertEquals(singleton(topicName), subscriptions.subscription());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        Map<String, List<String>> initialSubscription = singletonMap(consumerId, Arrays.asList(topicName));
        partitionAssignor.prepare(singletonMap(consumerId, singletonList(tp)));

        // the metadata will be updated in flight with a new topic added
        final List<String> updatedSubscription = Arrays.asList(topicName, otherTopicName);
        final Set<String> updatedSubscriptionSet = new HashSet<>(updatedSubscription);

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, initialSubscription, Errors.NONE.code()));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                final Map<String, Integer> updatedPartitions = new HashMap<>();
                for (String topic : updatedSubscription)
                    updatedPartitions.put(topic, 1);
                metadata.update(TestUtils.clusterWith(1, updatedPartitions), time.milliseconds());
                return true;
            }
        }, syncGroupResponse(singletonList(tp), Errors.NONE.code()));

        List<TopicPartition> newAssignment = Arrays.asList(tp, otherPartition);
        Set<TopicPartition> newAssignmentSet = new HashSet<>(newAssignment);

        Map<String, List<String>> updatedSubscriptions = singletonMap(consumerId, Arrays.asList(topicName, otherTopicName));
        partitionAssignor.prepare(singletonMap(consumerId, newAssignment));

        // we expect to see a second rebalance with the new-found topics
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                JoinGroupRequest join = new JoinGroupRequest(request.request().body());
                ProtocolMetadata protocolMetadata = join.groupProtocols().iterator().next();
                PartitionAssignor.Subscription subscription = ConsumerProtocol.deserializeSubscription(protocolMetadata.metadata());
                protocolMetadata.metadata().rewind();
                return subscription.topics().containsAll(updatedSubscriptionSet);
            }
        }, joinGroupLeaderResponse(2, consumerId, updatedSubscriptions, Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(newAssignment, Errors.NONE.code()));

        coordinator.poll(time.milliseconds());

        assertFalse(coordinator.needRejoin());
        assertEquals(updatedSubscriptionSet, subscriptions.subscription());
        assertEquals(newAssignmentSet, subscriptions.assignedPartitions());
        assertEquals(2, rebalanceListener.revokedCount);
        assertEquals(singleton(tp), rebalanceListener.revoked);
        assertEquals(2, rebalanceListener.assignedCount);
        assertEquals(newAssignmentSet, rebalanceListener.assigned);
    }

    @Test
    public void testWakeupDuringJoin() {
        final String consumerId = "leader";

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        // ensure metadata is up-to-date for leader
        metadata.setTopics(Arrays.asList(topicName));
        metadata.update(cluster, time.milliseconds());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        Map<String, List<String>> memberSubscriptions = Collections.singletonMap(consumerId, Arrays.asList(topicName));
        partitionAssignor.prepare(Collections.singletonMap(consumerId, singletonList(tp)));

        // prepare only the first half of the join and then trigger the wakeup
        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE.code()));
        consumerClient.wakeup();

        try {
            coordinator.poll(time.milliseconds());
        } catch (WakeupException e) {
            // ignore
        }

        // now complete the second half
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));
        coordinator.poll(time.milliseconds());

        assertFalse(coordinator.needRejoin());
        assertEquals(singleton(tp), subscriptions.assignedPartitions());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(Collections.emptySet(), rebalanceListener.revoked);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(singleton(tp), rebalanceListener.assigned);
    }

    @Test
    public void testNormalJoinGroupFollower() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // normal join group
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE.code()));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                SyncGroupRequest sync = new SyncGroupRequest(request.request().body());
                return sync.memberId().equals(consumerId) &&
                        sync.generationId() == 1 &&
                        sync.groupAssignment().isEmpty();
            }
        }, syncGroupResponse(singletonList(tp), Errors.NONE.code()));

        coordinator.joinGroupIfNeeded();

        assertFalse(coordinator.needRejoin());
        assertEquals(singleton(tp), subscriptions.assignedPartitions());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(singleton(tp), rebalanceListener.assigned);
    }

    @Test
    public void testLeaveGroupOnClose() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));
        coordinator.joinGroupIfNeeded();

        final AtomicBoolean received = new AtomicBoolean(false);
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                received.set(true);
                LeaveGroupRequest leaveRequest = new LeaveGroupRequest(request.request().body());
                return leaveRequest.memberId().equals(consumerId) &&
                        leaveRequest.groupId().equals(groupId);
            }
        }, new LeaveGroupResponse(Errors.NONE.code()).toStruct());
        coordinator.close();
        assertTrue(received.get());
    }

    @Test
    public void testMaybeLeaveGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));
        coordinator.joinGroupIfNeeded();

        final AtomicBoolean received = new AtomicBoolean(false);
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                received.set(true);
                LeaveGroupRequest leaveRequest = new LeaveGroupRequest(request.request().body());
                return leaveRequest.memberId().equals(consumerId) &&
                        leaveRequest.groupId().equals(groupId);
            }
        }, new LeaveGroupResponse(Errors.NONE.code()).toStruct());
        coordinator.maybeLeaveGroup();
        assertTrue(received.get());

        AbstractCoordinator.Generation generation = coordinator.generation();
        assertNull(generation);
    }

    @Test(expected = KafkaException.class)
    public void testUnexpectedErrorOnSyncGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(Collections.<TopicPartition>emptyList(), Errors.UNKNOWN.code()));
        coordinator.joinGroupIfNeeded();
    }

    @Test
    public void testUnknownMemberIdOnSyncGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // join initially, but let coordinator returns unknown member id
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(Collections.<TopicPartition>emptyList(), Errors.UNKNOWN_MEMBER_ID.code()));

        // now we should see a new join with the empty UNKNOWN_MEMBER_ID
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                JoinGroupRequest joinRequest = new JoinGroupRequest(request.request().body());
                return joinRequest.memberId().equals(JoinGroupRequest.UNKNOWN_MEMBER_ID);
            }
        }, joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));

        coordinator.joinGroupIfNeeded();

        assertFalse(coordinator.needRejoin());
        assertEquals(singleton(tp), subscriptions.assignedPartitions());
    }

    @Test
    public void testRebalanceInProgressOnSyncGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(Collections.<TopicPartition>emptyList(), Errors.REBALANCE_IN_PROGRESS.code()));

        // then let the full join/sync finish successfully
        client.prepareResponse(joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));

        coordinator.joinGroupIfNeeded();

        assertFalse(coordinator.needRejoin());
        assertEquals(singleton(tp), subscriptions.assignedPartitions());
    }

    @Test
    public void testIllegalGenerationOnSyncGroup() {
        final String consumerId = "consumer";

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // join initially, but let coordinator rebalance on sync
        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(Collections.<TopicPartition>emptyList(), Errors.ILLEGAL_GENERATION.code()));

        // then let the full join/sync finish successfully
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                JoinGroupRequest joinRequest = new JoinGroupRequest(request.request().body());
                return joinRequest.memberId().equals(JoinGroupRequest.UNKNOWN_MEMBER_ID);
            }
        }, joinGroupFollowerResponse(2, consumerId, "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));

        coordinator.joinGroupIfNeeded();

        assertFalse(coordinator.needRejoin());
        assertEquals(singleton(tp), subscriptions.assignedPartitions());
    }

    @Test
    public void testMetadataChangeTriggersRebalance() { 
        final String consumerId = "consumer";

        // ensure metadata is up-to-date for leader
        metadata.setTopics(Arrays.asList(topicName));
        metadata.update(cluster, time.milliseconds());

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        Map<String, List<String>> memberSubscriptions = Collections.singletonMap(consumerId, Arrays.asList(topicName));
        partitionAssignor.prepare(Collections.singletonMap(consumerId, singletonList(tp)));

        // the leader is responsible for picking up metadata changes and forcing a group rebalance
        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));

        coordinator.poll(time.milliseconds());

        assertFalse(coordinator.needRejoin());

        // a new partition is added to the topic
        metadata.update(TestUtils.singletonCluster(topicName, 2), time.milliseconds());

        // we should detect the change and ask for reassignment
        assertTrue(coordinator.needRejoin());
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
        metadata.update(TestUtils.singletonCluster(topic1, 1), time.milliseconds());

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // prepare initial rebalance
        Map<String, List<String>> memberSubscriptions = Collections.singletonMap(consumerId, topics);
        partitionAssignor.prepare(Collections.singletonMap(consumerId, Arrays.asList(tp1)));

        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberSubscriptions, Errors.NONE.code()));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                SyncGroupRequest sync = new SyncGroupRequest(request.request().body());
                if (sync.memberId().equals(consumerId) &&
                        sync.generationId() == 1 &&
                        sync.groupAssignment().containsKey(consumerId)) {
                    // trigger the metadata update including both topics after the sync group request has been sent
                    Map<String, Integer> topicPartitionCounts = new HashMap<>();
                    topicPartitionCounts.put(topic1, 1);
                    topicPartitionCounts.put(topic2, 1);
                    metadata.update(TestUtils.singletonCluster(topicPartitionCounts), time.milliseconds());
                    return true;
                }
                return false;
            }
        }, syncGroupResponse(Arrays.asList(tp1), Errors.NONE.code()));

        // the metadata update should trigger a second rebalance
        client.prepareResponse(joinGroupLeaderResponse(2, consumerId, memberSubscriptions, Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(Arrays.asList(tp1, tp2), Errors.NONE.code()));

        coordinator.poll(time.milliseconds());

        assertFalse(coordinator.needRejoin());
        assertEquals(new HashSet<>(Arrays.asList(tp1, tp2)), subscriptions.assignedPartitions());
    }


    @Test
    public void testExcludeInternalTopicsConfigOption() { 
        subscriptions.subscribe(Pattern.compile(".*"), rebalanceListener);

        metadata.update(TestUtils.singletonCluster(TestUtils.GROUP_METADATA_TOPIC_NAME, 2), time.milliseconds());

        assertFalse(subscriptions.subscription().contains(TestUtils.GROUP_METADATA_TOPIC_NAME));
    }

    @Test
    public void testIncludeInternalTopicsConfigOption() {
        coordinator = buildCoordinator(new Metrics(), assignors, false, false);
        subscriptions.subscribe(Pattern.compile(".*"), rebalanceListener);

        metadata.update(TestUtils.singletonCluster(TestUtils.GROUP_METADATA_TOPIC_NAME, 2), time.milliseconds());

        assertTrue(subscriptions.subscription().contains(TestUtils.GROUP_METADATA_TOPIC_NAME));
    }
    
    @Test
    public void testRejoinGroup() {
        String otherTopic = "otherTopic";

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // join the group once
        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));
        coordinator.joinGroupIfNeeded();

        assertEquals(1, rebalanceListener.revokedCount);
        assertTrue(rebalanceListener.revoked.isEmpty());
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(singleton(tp), rebalanceListener.assigned);

        // and join the group again
        subscriptions.subscribe(new HashSet<>(Arrays.asList(topicName, otherTopic)), rebalanceListener);
        client.prepareResponse(joinGroupFollowerResponse(2, "consumer", "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));
        coordinator.joinGroupIfNeeded();

        assertEquals(2, rebalanceListener.revokedCount);
        assertEquals(singleton(tp), rebalanceListener.revoked);
        assertEquals(2, rebalanceListener.assignedCount);
        assertEquals(singleton(tp), rebalanceListener.assigned);
    }

    @Test
    public void testDisconnectInJoin() {
        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // disconnected from original coordinator will cause re-discover and join again
        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE.code()), true);
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));
        coordinator.joinGroupIfNeeded();

        assertFalse(coordinator.needRejoin());
        assertEquals(singleton(tp), subscriptions.assignedPartitions());
        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(1, rebalanceListener.assignedCount);
        assertEquals(singleton(tp), rebalanceListener.assigned);
    }

    @Test(expected = ApiException.class)
    public void testInvalidSessionTimeout() {
        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // coordinator doesn't like the session timeout
        client.prepareResponse(joinGroupFollowerResponse(0, "consumer", "", Errors.INVALID_SESSION_TIMEOUT.code()));
        coordinator.joinGroupIfNeeded();
    }

    @Test
    public void testCommitOffsetOnly() {
        subscriptions.assignFromUser(singleton(tp));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));

        AtomicBoolean success = new AtomicBoolean(false);
        coordinator.commitOffsetsAsync(Collections.singletonMap(tp, new OffsetAndMetadata(100L)), callback(success));
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertTrue(success.get());

        assertEquals(100L, subscriptions.committed(tp).offset());
    }

    @Test
    public void testAutoCommitDynamicAssignment() {
        final String consumerId = "consumer";

        ConsumerCoordinator coordinator = buildCoordinator(new Metrics(), assignors,
                ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, true);

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));
        coordinator.joinGroupIfNeeded();

        subscriptions.seek(tp, 100);

        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));
        time.sleep(autoCommitIntervalMs);
        coordinator.poll(time.milliseconds());

        assertEquals(100L, subscriptions.committed(tp).offset());
    }

    @Test
    public void testAutoCommitDynamicAssignmentRebalance() {
        final String consumerId = "consumer";

        ConsumerCoordinator coordinator = buildCoordinator(new Metrics(), assignors,
                ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, true);

        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // haven't joined, so should not cause a commit
        time.sleep(autoCommitIntervalMs);
        consumerClient.poll(0);

        client.prepareResponse(joinGroupFollowerResponse(1, consumerId, "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));
        coordinator.joinGroupIfNeeded();

        subscriptions.seek(tp, 100);

        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));
        time.sleep(autoCommitIntervalMs);
        coordinator.poll(time.milliseconds());

        assertEquals(100L, subscriptions.committed(tp).offset());
    }

    @Test
    public void testAutoCommitManualAssignment() {
        ConsumerCoordinator coordinator = buildCoordinator(new Metrics(), assignors,
                ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, true);

        subscriptions.assignFromUser(singleton(tp));
        subscriptions.seek(tp, 100);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));
        time.sleep(autoCommitIntervalMs);
        coordinator.poll(time.milliseconds());

        assertEquals(100L, subscriptions.committed(tp).offset());
    }

    @Test
    public void testAutoCommitManualAssignmentCoordinatorUnknown() {
        ConsumerCoordinator coordinator = buildCoordinator(new Metrics(), assignors,
                ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, true);

        subscriptions.assignFromUser(singleton(tp));
        subscriptions.seek(tp, 100);

        // no commit initially since coordinator is unknown
        consumerClient.poll(0);
        time.sleep(autoCommitIntervalMs);
        consumerClient.poll(0);

        assertNull(subscriptions.committed(tp));

        // now find the coordinator
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // sleep only for the retry backoff
        time.sleep(retryBackoffMs);
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));
        coordinator.poll(time.milliseconds());

        assertEquals(100L, subscriptions.committed(tp).offset());
    }

    @Test
    public void testCommitOffsetMetadata() {
        subscriptions.assignFromUser(singleton(tp));

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));

        AtomicBoolean success = new AtomicBoolean(false);
        coordinator.commitOffsetsAsync(Collections.singletonMap(tp, new OffsetAndMetadata(100L, "hello")), callback(success));
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertTrue(success.get());

        assertEquals(100L, subscriptions.committed(tp).offset());
        assertEquals("hello", subscriptions.committed(tp).metadata());
    }

    @Test
    public void testCommitOffsetAsyncWithDefaultCallback() {
        int invokedBeforeTest = defaultOffsetCommitCallback.invoked;
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));
        coordinator.commitOffsetsAsync(Collections.singletonMap(tp, new OffsetAndMetadata(100L)), null);
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertEquals(invokedBeforeTest + 1, defaultOffsetCommitCallback.invoked);
        assertNull(defaultOffsetCommitCallback.exception);
    }

    @Test
    public void testCommitAfterLeaveGroup() {
        // enable auto-assignment
        subscriptions.subscribe(singleton(topicName), rebalanceListener);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE.code()));
        client.prepareResponse(syncGroupResponse(singletonList(tp), Errors.NONE.code()));
        coordinator.joinGroupIfNeeded();

        // now switch to manual assignment
        client.prepareResponse(new LeaveGroupResponse(Errors.NONE.code()).toStruct());
        subscriptions.unsubscribe();
        coordinator.maybeLeaveGroup();
        subscriptions.assignFromUser(singleton(tp));

        // the client should not reuse generation/memberId from auto-subscribed generation
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                OffsetCommitRequest commitRequest = new OffsetCommitRequest(request.request().body());
                return commitRequest.memberId().equals(OffsetCommitRequest.DEFAULT_MEMBER_ID) &&
                        commitRequest.generationId() == OffsetCommitRequest.DEFAULT_GENERATION_ID;
            }
        }, offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));

        AtomicBoolean success = new AtomicBoolean(false);
        coordinator.commitOffsetsAsync(Collections.singletonMap(tp, new OffsetAndMetadata(100L)), callback(success));
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertTrue(success.get());
    }

    @Test
    public void testCommitOffsetAsyncFailedWithDefaultCallback() {
        int invokedBeforeTest = defaultOffsetCommitCallback.invoked;
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code())));
        coordinator.commitOffsetsAsync(Collections.singletonMap(tp, new OffsetAndMetadata(100L)), null);
        coordinator.invokeCompletedOffsetCommitCallbacks();
        assertEquals(invokedBeforeTest + 1, defaultOffsetCommitCallback.invoked);
        assertTrue(defaultOffsetCommitCallback.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testCommitOffsetAsyncCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // async commit with coordinator not available
        MockCommitCallback cb = new MockCommitCallback();
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code())));
        coordinator.commitOffsetsAsync(Collections.singletonMap(tp, new OffsetAndMetadata(100L)), cb);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertTrue(cb.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testCommitOffsetAsyncNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // async commit with not coordinator
        MockCommitCallback cb = new MockCommitCallback();
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NOT_COORDINATOR_FOR_GROUP.code())));
        coordinator.commitOffsetsAsync(Collections.singletonMap(tp, new OffsetAndMetadata(100L)), cb);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertTrue(cb.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testCommitOffsetAsyncDisconnected() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // async commit with coordinator disconnected
        MockCommitCallback cb = new MockCommitCallback();
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())), true);
        coordinator.commitOffsetsAsync(Collections.singletonMap(tp, new OffsetAndMetadata(100L)), cb);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertTrue(cb.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testCommitOffsetSyncNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // sync commit with coordinator disconnected (should connect, get metadata, and then submit the commit request)
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NOT_COORDINATOR_FOR_GROUP.code())));
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));
        coordinator.commitOffsetsSync(Collections.singletonMap(tp, new OffsetAndMetadata(100L)));
    }

    @Test
    public void testCommitOffsetSyncCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // sync commit with coordinator disconnected (should connect, get metadata, and then submit the commit request)
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code())));
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));
        coordinator.commitOffsetsSync(Collections.singletonMap(tp, new OffsetAndMetadata(100L)));
    }

    @Test
    public void testCommitOffsetSyncCoordinatorDisconnected() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // sync commit with coordinator disconnected (should connect, get metadata, and then submit the commit request)
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())), true);
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));
        coordinator.commitOffsetsSync(Collections.singletonMap(tp, new OffsetAndMetadata(100L)));
    }

    @Test(expected = KafkaException.class)
    public void testCommitUnknownTopicOrPartition() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.UNKNOWN_TOPIC_OR_PARTITION.code())));
        coordinator.commitOffsetsSync(Collections.singletonMap(tp, new OffsetAndMetadata(100L, "metadata")));
    }

    @Test(expected = OffsetMetadataTooLarge.class)
    public void testCommitOffsetMetadataTooLarge() {
        // since offset metadata is provided by the user, we have to propagate the exception so they can handle it
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.OFFSET_METADATA_TOO_LARGE.code())));
        coordinator.commitOffsetsSync(Collections.singletonMap(tp, new OffsetAndMetadata(100L, "metadata")));
    }

    @Test(expected = CommitFailedException.class)
    public void testCommitOffsetIllegalGeneration() {
        // we cannot retry if a rebalance occurs before the commit completed
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.ILLEGAL_GENERATION.code())));
        coordinator.commitOffsetsSync(Collections.singletonMap(tp, new OffsetAndMetadata(100L, "metadata")));
    }

    @Test(expected = CommitFailedException.class)
    public void testCommitOffsetUnknownMemberId() {
        // we cannot retry if a rebalance occurs before the commit completed
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.UNKNOWN_MEMBER_ID.code())));
        coordinator.commitOffsetsSync(Collections.singletonMap(tp, new OffsetAndMetadata(100L, "metadata")));
    }

    @Test(expected = CommitFailedException.class)
    public void testCommitOffsetRebalanceInProgress() {
        // we cannot retry if a rebalance occurs before the commit completed
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.REBALANCE_IN_PROGRESS.code())));
        coordinator.commitOffsetsSync(Collections.singletonMap(tp, new OffsetAndMetadata(100L, "metadata")));
    }

    @Test(expected = KafkaException.class)
    public void testCommitOffsetSyncCallbackWithNonRetriableException() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        // sync commit with invalid partitions should throw if we have no callback
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.UNKNOWN.code())), false);
        coordinator.commitOffsetsSync(Collections.singletonMap(tp, new OffsetAndMetadata(100L)));
    }

    @Test
    public void testRefreshOffset() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        subscriptions.assignFromUser(singleton(tp));
        subscriptions.needRefreshCommits();
        client.prepareResponse(offsetFetchResponse(tp, Errors.NONE.code(), "", 100L));
        coordinator.refreshCommittedOffsetsIfNeeded();
        assertFalse(subscriptions.refreshCommitsNeeded());
        assertEquals(100L, subscriptions.committed(tp).offset());
    }

    @Test
    public void testRefreshOffsetLoadInProgress() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        subscriptions.assignFromUser(singleton(tp));
        subscriptions.needRefreshCommits();
        client.prepareResponse(offsetFetchResponse(tp, Errors.GROUP_LOAD_IN_PROGRESS.code(), "", 100L));
        client.prepareResponse(offsetFetchResponse(tp, Errors.NONE.code(), "", 100L));
        coordinator.refreshCommittedOffsetsIfNeeded();
        assertFalse(subscriptions.refreshCommitsNeeded());
        assertEquals(100L, subscriptions.committed(tp).offset());
    }

    @Test(expected = KafkaException.class)
    public void testRefreshOffsetUnknownTopicOrPartition() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        subscriptions.assignFromUser(singleton(tp));
        subscriptions.needRefreshCommits();
        client.prepareResponse(offsetFetchResponse(tp, Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), "", 100L));
        coordinator.refreshCommittedOffsetsIfNeeded();
    }

    @Test
    public void testRefreshOffsetNotCoordinatorForConsumer() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        subscriptions.assignFromUser(singleton(tp));
        subscriptions.needRefreshCommits();
        client.prepareResponse(offsetFetchResponse(tp, Errors.NOT_COORDINATOR_FOR_GROUP.code(), "", 100L));
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        client.prepareResponse(offsetFetchResponse(tp, Errors.NONE.code(), "", 100L));
        coordinator.refreshCommittedOffsetsIfNeeded();
        assertFalse(subscriptions.refreshCommitsNeeded());
        assertEquals(100L, subscriptions.committed(tp).offset());
    }

    @Test
    public void testRefreshOffsetWithNoFetchableOffsets() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        coordinator.ensureCoordinatorReady();

        subscriptions.assignFromUser(singleton(tp));
        subscriptions.needRefreshCommits();
        client.prepareResponse(offsetFetchResponse(tp, Errors.NONE.code(), "", -1L));
        coordinator.refreshCommittedOffsetsIfNeeded();
        assertFalse(subscriptions.refreshCommitsNeeded());
        assertEquals(null, subscriptions.committed(tp));
    }

    @Test
    public void testProtocolMetadataOrder() {
        RoundRobinAssignor roundRobin = new RoundRobinAssignor();
        RangeAssignor range = new RangeAssignor();

        try (Metrics metrics = new Metrics(time)) {
            ConsumerCoordinator coordinator = buildCoordinator(metrics, Arrays.<PartitionAssignor>asList(roundRobin, range),
                    ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, false);
            List<ProtocolMetadata> metadata = coordinator.metadata();
            assertEquals(2, metadata.size());
            assertEquals(roundRobin.name(), metadata.get(0).name());
            assertEquals(range.name(), metadata.get(1).name());
        }

        try (Metrics metrics = new Metrics(time)) {
            ConsumerCoordinator coordinator = buildCoordinator(metrics, Arrays.<PartitionAssignor>asList(range, roundRobin),
                    ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, false);
            List<ProtocolMetadata> metadata = coordinator.metadata();
            assertEquals(2, metadata.size());
            assertEquals(range.name(), metadata.get(0).name());
            assertEquals(roundRobin.name(), metadata.get(1).name());
        }
    }

    private ConsumerCoordinator buildCoordinator(Metrics metrics,
                                                 List<PartitionAssignor> assignors,
                                                 boolean excludeInternalTopics,
                                                 boolean autoCommitEnabled) {
        return new ConsumerCoordinator(
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
                defaultOffsetCommitCallback,
                autoCommitEnabled,
                autoCommitIntervalMs,
                null,
                excludeInternalTopics);
    }

    private Struct groupCoordinatorResponse(Node node, short error) {
        GroupCoordinatorResponse response = new GroupCoordinatorResponse(error, node);
        return response.toStruct();
    }

    private Struct heartbeatResponse(short error) {
        HeartbeatResponse response = new HeartbeatResponse(error);
        return response.toStruct();
    }

    private Struct joinGroupLeaderResponse(int generationId,
                                           String memberId,
                                           Map<String, List<String>> subscriptions,
                                           short error) {
        Map<String, ByteBuffer> metadata = new HashMap<>();
        for (Map.Entry<String, List<String>> subscriptionEntry : subscriptions.entrySet()) {
            PartitionAssignor.Subscription subscription = new PartitionAssignor.Subscription(subscriptionEntry.getValue());
            ByteBuffer buf = ConsumerProtocol.serializeSubscription(subscription);
            metadata.put(subscriptionEntry.getKey(), buf);
        }
        return new JoinGroupResponse(error, generationId, partitionAssignor.name(), memberId, memberId, metadata).toStruct();
    }

    private Struct joinGroupFollowerResponse(int generationId, String memberId, String leaderId, short error) {
        return new JoinGroupResponse(error, generationId, partitionAssignor.name(), memberId, leaderId,
                Collections.<String, ByteBuffer>emptyMap()).toStruct();
    }

    private Struct syncGroupResponse(List<TopicPartition> partitions, short error) {
        ByteBuffer buf = ConsumerProtocol.serializeAssignment(new PartitionAssignor.Assignment(partitions));
        return new SyncGroupResponse(error, buf).toStruct();
    }

    private Struct offsetCommitResponse(Map<TopicPartition, Short> responseData) {
        OffsetCommitResponse response = new OffsetCommitResponse(responseData);
        return response.toStruct();
    }

    private Struct offsetFetchResponse(TopicPartition tp, Short error, String metadata, long offset) {
        OffsetFetchResponse.PartitionData data = new OffsetFetchResponse.PartitionData(offset, metadata, error);
        OffsetFetchResponse response = new OffsetFetchResponse(Collections.singletonMap(tp, data));
        return response.toStruct();
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
