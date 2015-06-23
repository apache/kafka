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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.ConsumerMetadataResponse;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;


public class CoordinatorTest {

    private String topicName = "test";
    private String groupId = "test-group";
    private TopicPartition tp = new TopicPartition(topicName, 0);
    private int sessionTimeoutMs = 10;
    private String rebalanceStrategy = "not-matter";
    private MockTime time = new MockTime();
    private MockClient client = new MockClient(time);
    private Cluster cluster = TestUtils.singletonCluster(topicName, 1);
    private Node node = cluster.nodes().get(0);
    private SubscriptionState subscriptions = new SubscriptionState(OffsetResetStrategy.EARLIEST);
    private Metrics metrics = new Metrics(time);
    private Map<String, String> metricTags = new LinkedHashMap<String, String>();

    private Coordinator coordinator = new Coordinator(client,
        groupId,
        sessionTimeoutMs,
        rebalanceStrategy,
        subscriptions,
        metrics,
        "consumer" + groupId,
        metricTags,
        time);

    @Before
    public void setup() {
        client.setNode(node);
    }

    @Test
    public void testNormalHeartbeat() {
        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        // normal heartbeat
        time.sleep(sessionTimeoutMs);
        coordinator.maybeHeartbeat(time.milliseconds()); // should send out the heartbeat
        assertEquals(1, client.inFlightRequestCount());
        client.respond(heartbeatResponse(Errors.NONE.code()));
        assertEquals(1, client.poll(0, time.milliseconds()).size());
    }

    @Test
    public void testCoordinatorNotAvailable() {
        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        // consumer_coordinator_not_available will mark coordinator as unknown
        time.sleep(sessionTimeoutMs);
        coordinator.maybeHeartbeat(time.milliseconds()); // should send out the heartbeat
        assertEquals(1, client.inFlightRequestCount());
        client.respond(heartbeatResponse(Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()));
        time.sleep(sessionTimeoutMs);
        assertEquals(1, client.poll(0, time.milliseconds()).size());
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testNotCoordinator() {
        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        // not_coordinator will mark coordinator as unknown
        time.sleep(sessionTimeoutMs);
        coordinator.maybeHeartbeat(time.milliseconds()); // should send out the heartbeat
        assertEquals(1, client.inFlightRequestCount());
        client.respond(heartbeatResponse(Errors.NOT_COORDINATOR_FOR_CONSUMER.code()));
        time.sleep(sessionTimeoutMs);
        assertEquals(1, client.poll(0, time.milliseconds()).size());
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testIllegalGeneration() {
        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        // illegal_generation will cause re-partition
        subscriptions.subscribe(topicName);
        subscriptions.changePartitionAssignment(Collections.singletonList(tp));

        time.sleep(sessionTimeoutMs);
        coordinator.maybeHeartbeat(time.milliseconds()); // should send out the heartbeat
        assertEquals(1, client.inFlightRequestCount());
        client.respond(heartbeatResponse(Errors.ILLEGAL_GENERATION.code()));
        time.sleep(sessionTimeoutMs);
        assertEquals(1, client.poll(0, time.milliseconds()).size());
        assertTrue(subscriptions.partitionAssignmentNeeded());
    }

    @Test
    public void testCoordinatorDisconnect() {
        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        // coordinator disconnect will mark coordinator as unknown
        time.sleep(sessionTimeoutMs);
        coordinator.maybeHeartbeat(time.milliseconds()); // should send out the heartbeat
        assertEquals(1, client.inFlightRequestCount());
        client.respond(heartbeatResponse(Errors.NONE.code()), true); // return disconnected
        time.sleep(sessionTimeoutMs);
        assertEquals(1, client.poll(0, time.milliseconds()).size());
        assertTrue(coordinator.coordinatorUnknown());
    }

    @Test
    public void testNormalJoinGroup() {
        subscriptions.subscribe(topicName);
        subscriptions.needReassignment();

        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        // normal join group
        client.prepareResponse(joinGroupResponse(1, "consumer", Collections.singletonList(tp), Errors.NONE.code()));
        coordinator.assignPartitions(time.milliseconds());
        client.poll(0, time.milliseconds());

        assertFalse(subscriptions.partitionAssignmentNeeded());
        assertEquals(Collections.singleton(tp), subscriptions.assignedPartitions());
    }

    @Test
    public void testReJoinGroup() {
        subscriptions.subscribe(topicName);
        subscriptions.needReassignment();

        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());
        assertTrue(subscriptions.partitionAssignmentNeeded());

        // diconnected from original coordinator will cause re-discover and join again
        client.prepareResponse(joinGroupResponse(1, "consumer", Collections.singletonList(tp), Errors.NONE.code()), true);
        coordinator.assignPartitions(time.milliseconds());
        client.poll(0, time.milliseconds());
        assertTrue(subscriptions.partitionAssignmentNeeded());

        // rediscover the coordinator
        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        // try assigning partitions again
        client.prepareResponse(joinGroupResponse(1, "consumer", Collections.singletonList(tp), Errors.NONE.code()));
        coordinator.assignPartitions(time.milliseconds());
        client.poll(0, time.milliseconds());
        assertFalse(subscriptions.partitionAssignmentNeeded());
        assertEquals(Collections.singleton(tp), subscriptions.assignedPartitions());
    }


    @Test
    public void testCommitOffsetNormal() {
        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        // With success flag
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));
        RequestFuture<Void> result = coordinator.commitOffsets(Collections.singletonMap(tp, 100L), time.milliseconds());
        assertEquals(1, client.poll(0, time.milliseconds()).size());
        assertTrue(result.isDone());
        assertTrue(result.succeeded());

        // Without success flag
        coordinator.commitOffsets(Collections.singletonMap(tp, 100L), time.milliseconds());
        client.respond(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));
        assertEquals(1, client.poll(0, time.milliseconds()).size());
    }

    @Test
    public void testCommitOffsetError() {
        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        // async commit with coordinator not available
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code())));
        coordinator.commitOffsets(Collections.singletonMap(tp, 100L), time.milliseconds());
        assertEquals(1, client.poll(0, time.milliseconds()).size());
        assertTrue(coordinator.coordinatorUnknown());
        // resume
        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        // async commit with not coordinator
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NOT_COORDINATOR_FOR_CONSUMER.code())));
        coordinator.commitOffsets(Collections.singletonMap(tp, 100L), time.milliseconds());
        assertEquals(1, client.poll(0, time.milliseconds()).size());
        assertTrue(coordinator.coordinatorUnknown());
        // resume
        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        // sync commit with not_coordinator
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NOT_COORDINATOR_FOR_CONSUMER.code())));
        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));
        RequestFuture<Void> result = coordinator.commitOffsets(Collections.singletonMap(tp, 100L), time.milliseconds());
        assertEquals(1, client.poll(0, time.milliseconds()).size());
        assertTrue(result.isDone());
        assertEquals(RequestFuture.RetryAction.FIND_COORDINATOR, result.retryAction());

        // sync commit with coordinator disconnected
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())), true);
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp, Errors.NONE.code())));
        result = coordinator.commitOffsets(Collections.singletonMap(tp, 100L), time.milliseconds());

        assertEquals(0, client.poll(0, time.milliseconds()).size());
        assertTrue(result.isDone());
        assertEquals(RequestFuture.RetryAction.FIND_COORDINATOR, result.retryAction());

        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        result = coordinator.commitOffsets(Collections.singletonMap(tp, 100L), time.milliseconds());
        assertEquals(1, client.poll(0, time.milliseconds()).size());
        assertTrue(result.isDone());
        assertTrue(result.succeeded());
    }


    @Test
    public void testFetchOffset() {

        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        // normal fetch
        client.prepareResponse(offsetFetchResponse(tp, Errors.NONE.code(), "", 100L));
        RequestFuture<Map<TopicPartition, Long>> result = coordinator.fetchOffsets(Collections.singleton(tp), time.milliseconds());
        client.poll(0, time.milliseconds());
        assertTrue(result.isDone());
        assertEquals(100L, (long) result.value().get(tp));

        // fetch with loading in progress
        client.prepareResponse(offsetFetchResponse(tp, Errors.OFFSET_LOAD_IN_PROGRESS.code(), "", 100L));
        client.prepareResponse(offsetFetchResponse(tp, Errors.NONE.code(), "", 100L));

        result = coordinator.fetchOffsets(Collections.singleton(tp), time.milliseconds());
        client.poll(0, time.milliseconds());
        assertTrue(result.isDone());
        assertTrue(result.failed());
        assertEquals(RequestFuture.RetryAction.BACKOFF, result.retryAction());

        result = coordinator.fetchOffsets(Collections.singleton(tp), time.milliseconds());
        client.poll(0, time.milliseconds());
        assertTrue(result.isDone());
        assertEquals(100L, (long) result.value().get(tp));

        // fetch with not coordinator
        client.prepareResponse(offsetFetchResponse(tp, Errors.NOT_COORDINATOR_FOR_CONSUMER.code(), "", 100L));
        client.prepareResponse(consumerMetadataResponse(node, Errors.NONE.code()));
        client.prepareResponse(offsetFetchResponse(tp, Errors.NONE.code(), "", 100L));

        result = coordinator.fetchOffsets(Collections.singleton(tp), time.milliseconds());
        client.poll(0, time.milliseconds());
        assertTrue(result.isDone());
        assertTrue(result.failed());
        assertEquals(RequestFuture.RetryAction.FIND_COORDINATOR, result.retryAction());

        coordinator.discoverConsumerCoordinator();
        client.poll(0, time.milliseconds());

        result = coordinator.fetchOffsets(Collections.singleton(tp), time.milliseconds());
        client.poll(0, time.milliseconds());
        assertTrue(result.isDone());
        assertEquals(100L, (long) result.value().get(tp));

        // fetch with no fetchable offsets
        client.prepareResponse(offsetFetchResponse(tp, Errors.NONE.code(), "", -1L));
        result = coordinator.fetchOffsets(Collections.singleton(tp), time.milliseconds());
        client.poll(0, time.milliseconds());
        assertTrue(result.isDone());
        assertTrue(result.value().isEmpty());

        // fetch with offset topic unknown
        client.prepareResponse(offsetFetchResponse(tp, Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), "", 100L));
        result = coordinator.fetchOffsets(Collections.singleton(tp), time.milliseconds());
        client.poll(0, time.milliseconds());
        assertTrue(result.isDone());
        assertTrue(result.value().isEmpty());

        // fetch with offset -1
        client.prepareResponse(offsetFetchResponse(tp, Errors.NONE.code(), "", -1L));
        result = coordinator.fetchOffsets(Collections.singleton(tp), time.milliseconds());
        client.poll(0, time.milliseconds());
        assertTrue(result.isDone());
        assertTrue(result.value().isEmpty());
    }

    private Struct consumerMetadataResponse(Node node, short error) {
        ConsumerMetadataResponse response = new ConsumerMetadataResponse(error, node);
        return response.toStruct();
    }

    private Struct heartbeatResponse(short error) {
        HeartbeatResponse response = new HeartbeatResponse(error);
        return response.toStruct();
    }

    private Struct joinGroupResponse(int generationId, String consumerId, List<TopicPartition> assignedPartitions, short error) {
        JoinGroupResponse response = new JoinGroupResponse(error, generationId, consumerId, assignedPartitions);
        return response.toStruct();
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
}
