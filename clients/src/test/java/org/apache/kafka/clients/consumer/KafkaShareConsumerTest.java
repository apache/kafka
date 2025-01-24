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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// This test exercises the KafkaShareConsumer with the MockClient to validate the Kafka protocol RPCs
@Timeout(value = 120)
@SuppressWarnings({"ClassDataAbstractionCoupling"})
public class KafkaShareConsumerTest {

    private final String groupId = "test-group";
    private final String clientId1 = "client-id-1";

    private final String topic1 = "test1";
    private final Uuid topicId1 = Uuid.randomUuid();
    private final TopicPartition t1p0 = new TopicPartition(topic1, 0);
    private final TopicIdPartition ti1p0 = new TopicIdPartition(topicId1, t1p0);

    private final Map<String, Uuid> topicIds = Map.of(topic1, topicId1);

    private final int batchSize = 10;
    private final int heartbeatIntervalMs = 1000;

    private final Time time = new MockTime();
    private final SubscriptionState subscription = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.EARLIEST);

    @Test
    public void testVerifyHeartbeats() throws InterruptedException {
        ConsumerMetadata metadata = new ConsumerMetadata(0, 0, Long.MAX_VALUE, false, false,
            subscription, new LogContext(), new ClusterResourceListeners());
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Map.of(topic1, 1));
        Node node = metadata.fetch().nodes().get(0);

        Node coordinator = findCoordinator(client, node);

        // The member ID is created by the consumer and then sent in the initial request. The responses
        // need to contain the same member ID.
        final AtomicReference<Uuid> memberId = new AtomicReference<>();
        final AtomicInteger heartbeatsReceived = new AtomicInteger();
        client.prepareResponseFrom(body -> {
            ShareGroupHeartbeatRequest request = (ShareGroupHeartbeatRequest) body;
            memberId.set(Uuid.fromString(request.data().memberId()));
            boolean matches = request.data().memberEpoch() == 0;
            heartbeatsReceived.addAndGet(1);

            client.prepareResponseFrom(body2 -> {
                ShareGroupHeartbeatRequest request2 = (ShareGroupHeartbeatRequest) body2;
                boolean matches2 = request2.data().memberId().equals(memberId.get().toString()) && request2.data().memberEpoch() == 1;
                heartbeatsReceived.addAndGet(1);
                return matches2;
            }, shareGroupHeartbeatResponse(memberId.get(), 2, ti1p0), coordinator);

            return matches;
        }, shareGroupHeartbeatResponse(memberId.get(), 1, ti1p0), coordinator);

        try (KafkaShareConsumer<String, String> consumer = newShareConsumer(clientId1, metadata, client)) {
            consumer.subscribe(Set.of(topic1));
            consumer.poll(Duration.ZERO);

            Thread.sleep(heartbeatIntervalMs);

            assertEquals(2, heartbeatsReceived.get());
            assertTrue(client.futureResponses().isEmpty());

            consumer.close(Duration.ZERO);
        }
    }

    @Test
    public void testVerifyFetchAndAcknowledgeSync() throws InterruptedException {
        ConsumerMetadata metadata = new ConsumerMetadata(0, 0, Long.MAX_VALUE, false, false,
            subscription, new LogContext(), new ClusterResourceListeners());
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Map.of(topic1, 1));
        Node node = metadata.fetch().nodes().get(0);

        Node coordinator = findCoordinator(client, node);

        final AtomicReference<Uuid> memberId = new AtomicReference<>();
        final AtomicBoolean memberLeft = shareGroupHeartbeatGenerator(client, coordinator, memberId, ti1p0);

        // [A] A SHARE_FETCH in a new share session, fetching from topic topicId1, with no acknowledgements included.
        // The response includes 2 records which are acquired.
        client.prepareResponseFrom(body -> {
            ShareFetchRequest request = (ShareFetchRequest) body;
            return request.data().groupId().equals(groupId) &&
                request.data().shareSessionEpoch() == 0 &&
                request.data().batchSize() == batchSize &&
                request.data().topics().get(0).topicId().equals(topicId1) &&
                request.data().topics().get(0).partitions().size() == 1 &&
                request.data().topics().get(0).partitions().get(0).acknowledgementBatches().isEmpty();
        }, shareFetchResponse(ti1p0, 2), node);

        // [B] A SHARE_ACKNOWLEDGE for the two records acquired in [A].
        client.prepareResponseFrom(body -> {
            ShareAcknowledgeRequest request = (ShareAcknowledgeRequest) body;
            return request.data().groupId().equals(groupId) &&
                request.data().shareSessionEpoch() == 1 &&
                request.data().topics().get(0).partitions().get(0).acknowledgementBatches().get(0).firstOffset() == 0 &&
                request.data().topics().get(0).partitions().get(0).acknowledgementBatches().get(0).lastOffset() == 1 &&
                request.data().topics().get(0).partitions().get(0).acknowledgementBatches().get(0).acknowledgeTypes().size() == 1 &&
                request.data().topics().get(0).partitions().get(0).acknowledgementBatches().get(0).acknowledgeTypes().get(0) == (byte) 1;
        }, shareAcknowledgeResponse(ti1p0), node);

        // [C] A SHARE_ACKNOWLEDGE which closes the share session.
        client.prepareResponseFrom(body -> {
            ShareAcknowledgeRequest request = (ShareAcknowledgeRequest) body;
            return request.data().groupId().equals(groupId) &&
                request.data().shareSessionEpoch() == -1 &&
                request.data().topics().isEmpty();
        }, shareAcknowledgeResponse(), node);

        try (KafkaShareConsumer<String, String> consumer = newShareConsumer(clientId1, metadata, client)) {

            consumer.subscribe(Set.of(topic1));

            // This will be a SHARE_GROUP_HEARTBEAT to establish the membership and then a SHARE_FETCH [A]
            consumer.poll(Duration.ofMillis(heartbeatIntervalMs));

            // This will be a SHARE_ACKNOWLEDGE [B]
            consumer.commitSync();

            // This will be a SHARE_ACKNOWLEDGE [C] and a final SHARE_GROUP_HEARTBEAT to leave the group
            consumer.close(Duration.ZERO);

            assertTrue(memberLeft.get());
            assertTrue(client.futureResponses().isEmpty());
        }
    }

    private KafkaShareConsumer<String, String> newShareConsumer(String clientId,
                                                                ConsumerMetadata metadata,
                                                                KafkaClient client) {
        LogContext logContext = new LogContext();
        Deserializer<String> keyDeserializer = new StringDeserializer();
        Deserializer<String> valueDeserializer = new StringDeserializer();
        ConsumerConfig config = newConsumerConfig(clientId);

        return new KafkaShareConsumer<>(
            logContext,
            clientId,
            groupId,
            config,
            keyDeserializer,
            valueDeserializer,
            time,
            client,
            subscription,
            metadata
        );
    }

    private ConsumerConfig newConsumerConfig(String clientId) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchSize);
        return new ConsumerConfig(configs);
    }

    private void initMetadata(MockClient client, Map<String, Integer> partitions) {
        // It is important that this MetadataResponse contains the topic IDs for the topics which are being
        // subscribed to, otherwise the ShareMembershipManager will cause a second MetadataRequest to be sent out
        Map<String, Uuid> metadataTopicIds = new HashMap<>();
        for (String name : partitions.keySet()) {
            metadataTopicIds.put(name, topicIds.get(name));
        }
        MetadataResponse initialMetadata = RequestTestUtils.metadataUpdateWithIds(1, partitions, metadataTopicIds);
        client.updateMetadata(initialMetadata);
    }

    private Node findCoordinator(MockClient client, Node node) {
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        return new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
    }

    // This method generates a sequence of prepared SHARE_GROUP_HEARTBEAT responses with increasing member epochs.
    // Each time that a SHARE_GROUP_HEARTBEAT response matches the prepared response matcher, the next prepared
    // response is added, until the matching requests member epoch is -1, indicating that the member is leaving
    // the group.
    private AtomicBoolean shareGroupHeartbeatGenerator(MockClient client, Node coordinator, AtomicReference<Uuid> memberId, TopicIdPartition tip) {
        AtomicBoolean memberLeft = new AtomicBoolean();
        AtomicInteger heartbeatsReceived = new AtomicInteger();
        shareGroupHeartbeat(client, coordinator, memberId, 0, tip, heartbeatsReceived, memberLeft);
        return memberLeft;
    }

    private void shareGroupHeartbeat(MockClient client, Node coordinator, AtomicReference<Uuid> memberId, int memberEpoch, TopicIdPartition tip, AtomicInteger heartbeatsReceived, AtomicBoolean memberLeft) {
        client.prepareResponseFrom(body -> {
            ShareGroupHeartbeatRequest request = (ShareGroupHeartbeatRequest) body;
            if (request.data().memberEpoch() == 0) {
                memberId.set(Uuid.fromString(request.data().memberId()));
            }
            if (request.data().memberEpoch() == -1) {
                memberLeft.set(true);
            } else {
                shareGroupHeartbeat(client, coordinator, memberId, memberEpoch + 1, tip, heartbeatsReceived, memberLeft);
            }
            heartbeatsReceived.addAndGet(1);
            return true;
        }, shareGroupHeartbeatResponse(memberId.get(), memberEpoch, tip), coordinator);
    }

    private ShareGroupHeartbeatResponse shareGroupHeartbeatResponse(Uuid memberId, int memberEpoch, TopicIdPartition tip) {
        if (memberEpoch != -1) {
            List<ShareGroupHeartbeatResponseData.TopicPartitions> assignedPartitions = new LinkedList<>();
            assignedPartitions.add(new ShareGroupHeartbeatResponseData.TopicPartitions().setTopicId(tip.topicId()).setPartitions(List.of(tip.partition())));

            return new ShareGroupHeartbeatResponse(
                new ShareGroupHeartbeatResponseData()
                    .setMemberId(memberId != null ? memberId.toString() : null)
                    .setMemberEpoch(memberEpoch)
                    .setHeartbeatIntervalMs(heartbeatIntervalMs)
                    .setAssignment(new ShareGroupHeartbeatResponseData.Assignment()
                        .setTopicPartitions(assignedPartitions))
            );
        } else {
            return new ShareGroupHeartbeatResponse(
                new ShareGroupHeartbeatResponseData()
                    .setMemberId(memberId != null ? memberId.toString() : null)
                    .setMemberEpoch(memberEpoch)
                    .setHeartbeatIntervalMs(heartbeatIntervalMs)
            );
        }
    }

    private ShareFetchResponse shareFetchResponse(TopicIdPartition tip, int count) {
        MemoryRecords records;
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE,
            TimestampType.CREATE_TIME, 0)) {
            for (int i = 0; i < count; i++) {
                builder.append(0L, ("key-" + i).getBytes(), ("value-" + i).getBytes());
            }
            records = builder.build();
        }
        ShareFetchResponseData.PartitionData partData = new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(tip.partition())
            .setRecords(records)
            .setAcquiredRecords(List.of(new ShareFetchResponseData.AcquiredRecords().setFirstOffset(0).setLastOffset(count - 1).setDeliveryCount((short) 1)));
        ShareFetchResponseData.ShareFetchableTopicResponse topicResponse = new ShareFetchResponseData.ShareFetchableTopicResponse()
            .setTopicId(tip.topicId())
            .setPartitions(List.of(partData));
        return new ShareFetchResponse(
            new ShareFetchResponseData()
                .setResponses(List.of(topicResponse))
        );
    }

    private ShareAcknowledgeResponse shareAcknowledgeResponse() {
        return new ShareAcknowledgeResponse(
            new ShareAcknowledgeResponseData()
        );
    }

    private ShareAcknowledgeResponse shareAcknowledgeResponse(TopicIdPartition tip) {
        ShareAcknowledgeResponseData.PartitionData partData = new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(tip.partition())
            .setErrorCode(Errors.NONE.code());
        ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse topicResponse = new ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse()
            .setTopicId(tip.topicId())
            .setPartitions(List.of(partData));
        return new ShareAcknowledgeResponse(
            new ShareAcknowledgeResponseData()
                .setResponses(List.of(topicResponse))
        );
    }
}