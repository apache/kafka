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
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.clients.consumer.internals.MockRebalanceListener;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.internals.LegacyKafkaConsumer.DEFAULT_REASON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LegacyKafkaConsumerTest {
    private final String topic = "test";
    private final Uuid topicId = Uuid.randomUuid();
    private final TopicPartition tp0 = new TopicPartition(topic, 0);

    private final String topic2 = "test2";
    private final Uuid topicId2 = Uuid.randomUuid();
    private final TopicPartition t2p0 = new TopicPartition(topic2, 0);

    private final String topic3 = "test3";
    private final Uuid topicId3 = Uuid.randomUuid();

    private final int sessionTimeoutMs = 10000;
    private final int defaultApiTimeoutMs = 60000;
    private final int heartbeatIntervalMs = 1000;

    // Set auto commit interval lower than heartbeat so we don't need to deal with
    // a concurrent heartbeat request
    private final int autoCommitIntervalMs = 500;

    private final String groupId = "mock-group";
    private final String memberId = "memberId";
    private final String leaderId = "leaderId";
    private final String groupInstanceId = "mock-instance";
    private final Map<String, Uuid> topicIds = Stream.of(
                    new AbstractMap.SimpleEntry<>(topic, topicId),
                    new AbstractMap.SimpleEntry<>(topic2, topicId2),
                    new AbstractMap.SimpleEntry<>(topic3, topicId3))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    private final SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
    private final ConsumerPartitionAssignor assignor = new RoundRobinAssignor();

    private KafkaConsumer<?, ?> consumer;

    @AfterEach
    public void cleanup() {
        if (consumer != null) {
            consumer.close(Duration.ZERO);
        }
    }

    private <K, V> KafkaConsumer<K, V> newConsumer(Map<String, Object> configs,
                                                   Deserializer<K> keyDeserializer,
                                                   Deserializer<V> valueDeserializer) {
        return new KafkaConsumer<>(new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    private KafkaConsumer<String, String> newConsumer(Time time,
                                                      KafkaClient client,
                                                      SubscriptionState subscriptions,
                                                      ConsumerMetadata metadata) {
        Deserializer<String> keyDeserializer = new StringDeserializer();
        Deserializer<String> valueDeserializer = new StringDeserializer();
        LogContext logContext = new LogContext();
        List<ConsumerPartitionAssignor> assignors = singletonList(assignor);
        String clientId = "mock-consumer";
        long retryBackoffMs = 100;
        long retryBackoffMaxMs = 1000;
        int minBytes = 1;
        int maxBytes = Integer.MAX_VALUE;
        int maxWaitMs = 500;
        int fetchSize = 1024 * 1024;
        int maxPollRecords = Integer.MAX_VALUE;
        boolean checkCrcs = true;
        int rebalanceTimeoutMs = 60000;
        int requestTimeoutMs = defaultApiTimeoutMs / 2;

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs);
        configs.put(ConsumerConfig.CHECK_CRCS_CONFIG, checkCrcs);
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        configs.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeoutMs);
        configs.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, maxBytes);
        configs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, maxWaitMs);
        configs.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, minBytes);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        configs.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, fetchSize);
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, rebalanceTimeoutMs);
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        configs.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        configs.put(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, retryBackoffMaxMs);
        configs.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        configs.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);

        return new KafkaConsumer<>(
                logContext,
                time,
                new ConsumerConfig(configs),
                keyDeserializer,
                valueDeserializer,
                client,
                subscriptions,
                metadata,
                assignors
        );
    }

    private ConsumerMetadata createMetadata(SubscriptionState subscription) {
        return new ConsumerMetadata(0, 0, Long.MAX_VALUE, false, false,
                subscription, new LogContext(), new ClusterResourceListeners());
    }

    private void initMetadata(MockClient mockClient, Map<String, Integer> partitionCounts) {
        Map<String, Uuid> metadataIds = new HashMap<>();
        for (String name : partitionCounts.keySet()) {
            metadataIds.put(name, topicIds.get(name));
        }
        MetadataResponse initialMetadata = RequestTestUtils.metadataUpdateWithIds(1, partitionCounts, metadataIds);

        mockClient.updateMetadata(initialMetadata);
    }

    private Node prepareRebalance(MockClient client, Node node, ConsumerPartitionAssignor assignor, List<TopicPartition> partitions, Node coordinator) {
        if (coordinator == null) {
            // lookup coordinator
            client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
            coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        }

        // join group
        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, memberId, leaderId, Errors.NONE), coordinator);

        // sync group
        client.prepareResponseFrom(syncGroupResponse(partitions, Errors.NONE), coordinator);

        return coordinator;
    }

    private SyncGroupResponse syncGroupResponse(List<TopicPartition> partitions, Errors error) {
        ByteBuffer buf = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(partitions));
        return new SyncGroupResponse(
                new SyncGroupResponseData()
                        .setErrorCode(error.code())
                        .setAssignment(Utils.toArray(buf))
        );
    }

    private JoinGroupResponse joinGroupFollowerResponse(ConsumerPartitionAssignor assignor, int generationId, String memberId, String leaderId, Errors error) {
        return new JoinGroupResponse(
                new JoinGroupResponseData()
                        .setErrorCode(error.code())
                        .setGenerationId(generationId)
                        .setProtocolName(assignor.name())
                        .setLeader(leaderId)
                        .setMemberId(memberId)
                        .setMembers(Collections.emptyList()),
                ApiKeys.JOIN_GROUP.latestVersion()
        );
    }

    private void prepareJoinGroupAndVerifyReason(
            MockClient client,
            Node node,
            String expectedReason
    ) {
        client.prepareResponseFrom(
                body -> {
                    JoinGroupRequest joinGroupRequest = (JoinGroupRequest) body;
                    return expectedReason.equals(joinGroupRequest.data().reason());
                },
                joinGroupFollowerResponse(assignor, 1, memberId, leaderId, Errors.NONE),
                node
        );
    }

    @Test
    public void testSubscriptionWithEmptyPartitionAssignment() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumer = newConsumer(configs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertThrows(IllegalStateException.class,
                () -> consumer.subscribe(singletonList(topic)));
    }


    // NOTE: this test uses the enforceRebalance API which is not implemented in the CONSUMER group protocol.
    @Test
    public void testEnforceRebalanceWithManualAssignment() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        consumer = newConsumer(configs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.assign(singleton(new TopicPartition("topic", 0)));
        assertThrows(IllegalStateException.class, consumer::enforceRebalance);
    }

    // NOTE: this test uses the enforceRebalance API which is not implemented in the CONSUMER group protocol.
    @Test
    public void testEnforceRebalanceTriggersRebalanceOnNextPoll() {
        Time time = new MockTime(1L);
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        KafkaConsumer<String, String> consumer = newConsumer(
                time,
                client,
                subscription,
                metadata
        );
        MockRebalanceListener countingRebalanceListener = new MockRebalanceListener();
        initMetadata(client, Utils.mkMap(Utils.mkEntry(topic, 1), Utils.mkEntry(topic2, 1), Utils.mkEntry(topic3, 1)));

        consumer.subscribe(Arrays.asList(topic, topic2), countingRebalanceListener);
        Node node = metadata.fetch().nodes().get(0);
        prepareRebalance(client, node, assignor, Arrays.asList(tp0, t2p0), null);

        // a first rebalance to get the assignment, we need two poll calls since we need two round trips to finish join / sync-group
        consumer.poll(Duration.ZERO);
        consumer.poll(Duration.ZERO);

        // onPartitionsRevoked is not invoked when first joining the group
        assertEquals(countingRebalanceListener.revokedCount, 0);
        assertEquals(countingRebalanceListener.assignedCount, 1);

        consumer.enforceRebalance();

        // the next poll should trigger a rebalance
        consumer.poll(Duration.ZERO);

        assertEquals(countingRebalanceListener.revokedCount, 1);
    }

    // NOTE: this test uses the enforceRebalance API which is not implemented in the CONSUMER group protocol.
    @Test
    public void testEnforceRebalanceReason() {
        Time time = new MockTime(1L);

        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Utils.mkMap(Utils.mkEntry(topic, 1)));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(
                time,
                client,
                subscription,
                metadata
        );
        consumer.subscribe(Collections.singletonList(topic));

        // Lookup coordinator.
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        consumer.poll(Duration.ZERO);

        // Initial join sends an empty reason.
        prepareJoinGroupAndVerifyReason(client, node, "");
        consumer.poll(Duration.ZERO);

        // A null reason should be replaced by the default reason.
        consumer.enforceRebalance(null);
        prepareJoinGroupAndVerifyReason(client, node, DEFAULT_REASON);
        consumer.poll(Duration.ZERO);

        // An empty reason should be replaced by the default reason.
        consumer.enforceRebalance("");
        prepareJoinGroupAndVerifyReason(client, node, DEFAULT_REASON);
        consumer.poll(Duration.ZERO);

        // A non-null and non-empty reason is sent as-is.
        String customReason = "user provided reason";
        consumer.enforceRebalance(customReason);
        prepareJoinGroupAndVerifyReason(client, node, customReason);
        consumer.poll(Duration.ZERO);
    }

    @Test
    public void testAssignorNameConflict() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC);
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                Arrays.asList(RangeAssignor.class.getName(), ConsumerPartitionAssignorTest.TestConsumerPartitionAssignor.class.getName()));

        assertThrows(KafkaException.class,
                () -> newConsumer(configs, new StringDeserializer(), new StringDeserializer()));
    }
}
