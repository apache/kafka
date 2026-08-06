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

package org.apache.kafka.server;

import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.SecurityUtils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.quota.ControllerMutationQuota;
import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.SHARE_GROUP_STATE_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultAutoTopicCreationManagerTest {

    private final int requestTimeout = 100;
    private final int testCacheCapacity = 3;
    private AbstractKafkaConfig config;
    private final TestTopicCreator topicCreator = new TestTopicCreator();
    private AutoTopicCreationManager autoTopicCreationManager;
    private final MockTime mockTime = new MockTime(0L, 0L);

    private final int internalTopicPartitions = 2;
    private final short internalTopicReplicationFactor = 2;

    @BeforeEach
    public void setup() {
        var props = new Properties();
        props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1");
        props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
        props.setProperty(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeout));
        props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER");

        props.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, String.valueOf(internalTopicReplicationFactor));
        props.setProperty(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, String.valueOf(internalTopicReplicationFactor));
        props.setProperty(ShareCoordinatorConfig.STATE_TOPIC_REPLICATION_FACTOR_CONFIG, String.valueOf(internalTopicReplicationFactor));

        props.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, String.valueOf(internalTopicPartitions));
        props.setProperty(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, String.valueOf(internalTopicPartitions));
        props.setProperty(ShareCoordinatorConfig.STATE_TOPIC_NUM_PARTITIONS_CONFIG, String.valueOf(internalTopicPartitions));

        props.setProperty(ServerLogConfigs.NUM_PARTITIONS_CONFIG, "1");
        props.setProperty(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "1");

        config = new AbstractKafkaConfig(AbstractKafkaConfig.CONFIG_DEF, props, Map.of(), false) { };
        topicCreator.reset();
    }

    @Test
    public void testCreateOffsetTopic() {
        testCreateTopic(GROUP_METADATA_TOPIC_NAME, true, internalTopicPartitions, internalTopicReplicationFactor);
    }

    @Test
    public void testCreateTxnTopic() {
        testCreateTopic(TRANSACTION_STATE_TOPIC_NAME, true, internalTopicPartitions, internalTopicReplicationFactor);
    }

    @Test
    public void testCreateShareStateTopic() {
        testCreateTopic(SHARE_GROUP_STATE_TOPIC_NAME, true, internalTopicPartitions, internalTopicReplicationFactor);
    }

    @Test
    public void testCreateNonInternalTopic() {
        testCreateTopic("topic", false, 1, (short) 1);
    }

    private void testCreateTopic(
        String topicName,
        boolean isInternal,
        int numPartitions,
        short replicationFactor
    ) {
        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                testCacheCapacity);

        // Set up the topicCreator to return a successful response
        var createTopicsResponseData = new CreateTopicsResponseData();
        createTopicsResponseData.topics().add(new CreatableTopicResult()
                .setName(topicName)
                .setErrorCode(Errors.NONE.code()));
        var response = new CreateTopicsResponse(createTopicsResponseData);
        topicCreator.setResponseForWithoutPrincipal(response);

        // First call to create topic - should trigger the topic creator
        createTopicAndVerifyResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicName, isInternal);

        assertEquals(1, topicCreator.withoutPrincipalCallCount(),
                "Should have called createTopicWithoutPrincipal once");

        // Reset the topicCreator to verify the second call
        topicCreator.reset();
        topicCreator.setResponseForWithoutPrincipal(response);

        // Second call - should also trigger topicCreator because inflight is cleared after first call completes
        createTopicAndVerifyResult(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicName, isInternal);

        assertEquals(1, topicCreator.withoutPrincipalCallCount(),
                "Should have called createTopicWithoutPrincipal once more");

        // Verify the request builder matches expected values
        var capturedRequest = topicCreator.getWithoutPrincipalCalls().get(0).build();
        assertEquals(requestTimeout, capturedRequest.data().timeoutMs());
        assertEquals(1, capturedRequest.data().topics().size());

        // Validate request
        var topic = capturedRequest.data().topics().iterator().next();
        assertEquals(topicName, topic.name());
        assertEquals(numPartitions, topic.numPartitions());
        assertEquals(replicationFactor, topic.replicationFactor());
    }

    @Test
    public void testTopicCreationWithMetadataContext() throws UnknownHostException {
        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                testCacheCapacity);

        var topicName = "topic";
        var userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user");
        var principalSerde = new KafkaPrincipalSerde() {
            @Override
            public byte[] serialize(KafkaPrincipal principal) {
                return Utils.utf8(principal.toString());
            }
            @Override
            public KafkaPrincipal deserialize(byte[] bytes) {
                return SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes));
            }
        };

        var requestContext = initializeRequestContext(userPrincipal, Optional.of(principalSerde));

        var createTopicsResponseData = new CreateTopicsResponseData();
        createTopicsResponseData.topics().add(new CreatableTopicResult()
                .setName(topicName)
                .setErrorCode(Errors.NONE.code()));
        var response = new CreateTopicsResponse(createTopicsResponseData);
        topicCreator.setResponseForWithPrincipal(response);

        autoTopicCreationManager.createTopics(Set.of(topicName), ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA, requestContext);

        assertEquals(1, topicCreator.withPrincipalCallCount(),
                "Should have called createTopicWithPrincipal once");
        var calls = topicCreator.getWithPrincipalCalls();
        assertEquals(requestContext, calls.get(0).requestContext());

        var capturedRequest = calls.get(0).request().build();
        assertEquals(1, capturedRequest.data().topics().size());
        assertEquals(topicName, capturedRequest.data().topics().iterator().next().name());
    }

    @Test
    public void testCreateStreamsInternalTopics() throws UnknownHostException {
        var topicConfig = new CreatableTopicConfigCollection();
        topicConfig.add(new CreatableTopicConfig().setName("cleanup.policy").setValue("compact"));

        var topics = Map.of(
                "stream-topic-1", new CreatableTopic()
                        .setName("stream-topic-1")
                        .setNumPartitions(3)
                        .setReplicationFactor((short) 2)
                        .setConfigs(topicConfig),
                "stream-topic-2", new CreatableTopic()
                        .setName("stream-topic-2")
                        .setNumPartitions(1)
                        .setReplicationFactor((short) 1));
        var requestContext = initializeRequestContextWithUserPrincipal();

        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                testCacheCapacity);

        var createTopicsResponseData = new CreateTopicsResponseData();
        createTopicsResponseData.topics().add(new CreatableTopicResult()
                .setName("stream-topic-1")
                .setErrorCode(Errors.NONE.code()));
        createTopicsResponseData.topics().add(new CreatableTopicResult()
                .setName("stream-topic-2")
                .setErrorCode(Errors.NONE.code()));
        var response = new CreateTopicsResponse(createTopicsResponseData);
        topicCreator.setResponseForWithPrincipal(response);

        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext,
                new GroupCoordinatorConfig(config).streamsGroupHeartbeatIntervalMs() * 2L);

        assertEquals(1, topicCreator.withPrincipalCallCount(),
                "Should have called createTopicWithPrincipal once");
        var calls = topicCreator.getWithPrincipalCalls();
        assertEquals(requestContext, calls.get(0).requestContext());

        var capturedRequest = calls.get(0).request().build();
        assertEquals(requestTimeout, capturedRequest.data().timeoutMs());
        assertEquals(2, capturedRequest.data().topics().size());
        var topicNames = new HashSet<String>();
        capturedRequest.data().topics().forEach(t -> topicNames.add(t.name()));
        assertTrue(topicNames.contains("stream-topic-1"));
        assertTrue(topicNames.contains("stream-topic-2"));
    }

    @Test
    public void testCreateStreamsInternalTopicsWithEmptyTopics() throws UnknownHostException {
        var topics = Map.<String, CreatableTopic>of();
        var requestContext = initializeRequestContextWithUserPrincipal();

        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                testCacheCapacity);

        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext,
                new GroupCoordinatorConfig(config).streamsGroupHeartbeatIntervalMs() * 2L);

        assertEquals(0, topicCreator.withPrincipalCallCount(),
                "Should not have called createTopicWithPrincipal");
    }

    @Test
    public void testCreateStreamsInternalTopicsPassesRequestContext() throws UnknownHostException {
        var topics = Map.of(
                "stream-topic-1",
                new CreatableTopic()
                        .setName("stream-topic-1")
                        .setNumPartitions(-1)
                        .setReplicationFactor((short) -1));
        var requestContext = initializeRequestContextWithUserPrincipal();

        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                testCacheCapacity);

        var createTopicsResponseData = new CreateTopicsResponseData();
        createTopicsResponseData.topics().add(new CreatableTopicResult()
                .setName("stream-topic-1")
                .setErrorCode(Errors.NONE.code()));
        var response = new CreateTopicsResponse(createTopicsResponseData);
        topicCreator.setResponseForWithPrincipal(response);

        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext,
                new GroupCoordinatorConfig(config).streamsGroupHeartbeatIntervalMs() * 2L);

        assertEquals(1, topicCreator.withPrincipalCallCount(),
                "Should have called createTopicWithPrincipal once");
        var calls = topicCreator.getWithPrincipalCalls();
        assertEquals(requestContext, calls.get(0).requestContext());
    }

    @Test
    public void testTopicCreationErrorCaching() throws UnknownHostException {
        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                testCacheCapacity);

        var topics = Map.of(
                "test-topic-1", new CreatableTopic().setName("test-topic-1").setNumPartitions(1).setReplicationFactor((short) 1));
        var requestContext = initializeRequestContextWithUserPrincipal();

        // Simulate a CreateTopicsResponse with errors
        var createTopicsResponseData = new CreateTopicsResponseData();
        createTopicsResponseData.topics().add(new CreatableTopicResult()
                .setName("test-topic-1")
                .setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
                .setErrorMessage("Topic 'test-topic-1' already exists."));
        topicCreator.setResponseForWithPrincipal(new CreateTopicsResponse(createTopicsResponseData));

        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext,
                new GroupCoordinatorConfig(config).streamsGroupHeartbeatIntervalMs() * 2L);

        // Verify that the error was cached
        var cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(
                Set.of("test-topic-1"), mockTime.milliseconds());
        assertEquals(1, cachedErrors.size());
        assertTrue(cachedErrors.containsKey("test-topic-1"));
        assertEquals("Topic 'test-topic-1' already exists.", cachedErrors.get("test-topic-1"));
    }

    @Test
    public void testGetTopicCreationErrorsWithMultipleTopics() throws UnknownHostException {
        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                testCacheCapacity);

        var topics = Map.of(
                "success-topic",
                new CreatableTopic().setName("success-topic").setNumPartitions(1).setReplicationFactor((short) 1),
                "failed-topic",
                new CreatableTopic().setName("failed-topic").setNumPartitions(1).setReplicationFactor((short) 1));
        var requestContext = initializeRequestContextWithUserPrincipal();

        // Simulate mixed response - one success, one failure
        var createTopicsResponseData = new CreateTopicsResponseData();
        createTopicsResponseData.topics().add(new CreatableTopicResult()
                .setName("success-topic")
                .setErrorCode(Errors.NONE.code()));
        createTopicsResponseData.topics().add(new CreatableTopicResult()
                .setName("failed-topic")
                .setErrorCode(Errors.POLICY_VIOLATION.code())
                .setErrorMessage("Policy violation"));
        topicCreator.setResponseForWithPrincipal(new CreateTopicsResponse(createTopicsResponseData));

        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext,
                new GroupCoordinatorConfig(config).streamsGroupHeartbeatIntervalMs() * 2L);

        // Only the failed topic should be cached
        var cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(
                Set.of("success-topic", "failed-topic", "nonexistent-topic"), mockTime.milliseconds());
        assertEquals(1, cachedErrors.size());
        assertTrue(cachedErrors.containsKey("failed-topic"));
        assertEquals("Policy violation", cachedErrors.get("failed-topic"));
    }

    @Test
    public void testErrorCacheTTL() throws UnknownHostException {
        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                testCacheCapacity);

        // First cache an error by simulating topic creation failure
        var topics = Map.of("test-topic",
                new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor((short) 1));
        var requestContext = initializeRequestContextWithUserPrincipal();
        long shortTtlMs = 1000L;

        // Simulate a CreateTopicsResponse with error
        var createTopicsResponseData = new CreateTopicsResponseData();
        createTopicsResponseData.topics().add(new CreatableTopicResult()
                .setName("test-topic")
                .setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code())
                .setErrorMessage("Invalid replication factor"));
        topicCreator.setResponseForWithPrincipal(new CreateTopicsResponse(createTopicsResponseData));

        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, shortTtlMs);

        // Verify error is cached and accessible within TTL
        var cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(
                Set.of("test-topic"), mockTime.milliseconds());
        assertEquals(1, cachedErrors.size());
        assertEquals("Invalid replication factor", cachedErrors.get("test-topic"));

        // Advance time beyond TTL
        mockTime.sleep(shortTtlMs + 100);

        // Verify error is now expired and proactively cleaned up
        var expiredErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(
                Set.of("test-topic"), mockTime.milliseconds());
        assertTrue(expiredErrors.isEmpty(), "Expired errors should be proactively cleaned up");
    }

    @Test
    public void testErrorCacheExpirationBasedEviction() throws UnknownHostException {
        // Create manager with small cache size for testing
        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                3);

        var requestContext = initializeRequestContextWithUserPrincipal();

        // Create 5 topics to exceed the cache size of 3
        List<String> topicNames = IntStream.rangeClosed(1, 5).mapToObj(i -> "test-topic-" + i).toList();

        // Add errors for all 5 topics to the cache
        for (String topicName : topicNames) {
            var topics = Map.of(topicName,
                    new CreatableTopic().setName(topicName).setNumPartitions(1).setReplicationFactor((short) 1));

            // Simulate error response for this topic
            var createTopicsResponseData = new CreateTopicsResponseData();
            createTopicsResponseData.topics().add(new CreatableTopicResult()
                    .setName(topicName)
                    .setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code())
                    .setErrorMessage("Topic '" + topicName + "' already exists."));
            topicCreator.setResponseForWithPrincipal(new CreateTopicsResponse(createTopicsResponseData));

            autoTopicCreationManager.createStreamsInternalTopics(
                    topics, requestContext, new GroupCoordinatorConfig(config).streamsGroupHeartbeatIntervalMs() * 2L);

            // Advance time slightly between additions to ensure different timestamps
            mockTime.sleep(10);
        }

        // With cache size of 3, topics 1 and 2 should have been evicted
        var cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(
                Set.copyOf(topicNames), mockTime.milliseconds());

        // Only the last 3 topics should be in the cache (topics 3, 4, 5)
        assertEquals(3, cachedErrors.size(), "Cache should contain only the most recent 3 entries");
        assertTrue(cachedErrors.containsKey("test-topic-3"), "test-topic-3 should be in cache");
        assertTrue(cachedErrors.containsKey("test-topic-4"), "test-topic-4 should be in cache");
        assertTrue(cachedErrors.containsKey("test-topic-5"), "test-topic-5 should be in cache");
        assertFalse(cachedErrors.containsKey("test-topic-1"), "test-topic-1 should have been evicted");
        assertFalse(cachedErrors.containsKey("test-topic-2"), "test-topic-2 should have been evicted");
    }

    @Test
    public void testTopicsInBackoffAreNotRetried() throws UnknownHostException {
        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                testCacheCapacity);

        var topics = Map.of("test-topic",
                new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor((short) 1));
        var requestContext = initializeRequestContextWithUserPrincipal();
        long timeoutMs = 5000L;

        // Simulate error response to cache the error
        var createTopicsResponseData = new CreateTopicsResponseData();
        createTopicsResponseData.topics().add(new CreatableTopicResult()
                .setName("test-topic")
                .setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code())
                .setErrorMessage("Invalid replication factor"));
        topicCreator.setResponseForWithPrincipal(new CreateTopicsResponse(createTopicsResponseData));

        // First attempt - trigger topic creation
        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs);

        // Verify error is cached
        var cachedErrors = autoTopicCreationManager.getStreamsInternalTopicCreationErrors(
                Set.of("test-topic"), mockTime.milliseconds());
        assertEquals(1, cachedErrors.size());

        // Second attempt - should NOT send request because topic is in back-off
        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs);

        // Verify still only one request was sent (not retried during back-off)
        assertEquals(1, topicCreator.withPrincipalCallCount(),
                "Should have called createTopicWithPrincipal once");
    }

    @Test
    public void testTopicsOutOfBackoffCanBeRetried() throws UnknownHostException {
        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                testCacheCapacity);

        var topics = Map.of("test-topic",
                new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor((short) 1));
        var requestContext = initializeRequestContextWithUserPrincipal();
        long shortTtlMs = 1000L;

        // Simulate error response to cache the error
        var createTopicsResponseData = new CreateTopicsResponseData();
        createTopicsResponseData.topics().add(new CreatableTopicResult()
                .setName("test-topic")
                .setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code())
                .setErrorMessage("Invalid replication factor"));
        topicCreator.setResponseForWithPrincipal(new CreateTopicsResponse(createTopicsResponseData));

        // First attempt - trigger topic creation
        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, shortTtlMs);

        // Verify error is cached
        assertEquals(1, autoTopicCreationManager.getStreamsInternalTopicCreationErrors(
                Set.of("test-topic"), mockTime.milliseconds()).size());

        // Advance time beyond TTL to exit back-off period
        mockTime.sleep(shortTtlMs + 100);

        // Verify error is expired
        assertTrue(autoTopicCreationManager.getStreamsInternalTopicCreationErrors(
                Set.of("test-topic"), mockTime.milliseconds()).isEmpty(),
                "Error should be expired after TTL");

        // Second attempt - should send request because topic is out of back-off
        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, shortTtlMs);

        // Verify a second request was sent (retry allowed after back-off expires)
        assertEquals(2, topicCreator.withPrincipalCallCount(),
                "Should have called createTopicWithPrincipal twice");
    }

    @Test
    public void testInflightTopicsAreNotRetriedConcurrently() throws UnknownHostException {
        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                testCacheCapacity);

        var topics = Map.of("test-topic",
                new CreatableTopic().setName("test-topic").setNumPartitions(1).setReplicationFactor((short) 1));
        var requestContext = initializeRequestContextWithUserPrincipal();
        long timeoutMs = 5000L;

        // Use a future that doesn't complete immediately to simulate in-flight state
        var future = new CompletableFuture<CreateTopicsResponse>();
        topicCreator.setFutureForWithPrincipal(future);

        // First call - should send request and mark topic as in-flight
        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs);

        assertEquals(1, topicCreator.withPrincipalCallCount(),
                "Should have called createTopicWithPrincipal once");

        // Second concurrent call - should NOT send request because topic is in-flight
        autoTopicCreationManager.createStreamsInternalTopics(topics, requestContext, timeoutMs);

        // Verify still only one request was sent (concurrent request blocked)
        assertEquals(1, topicCreator.withPrincipalCallCount(),
                "Should have called createTopicWithPrincipal once");
    }

    @Test
    public void testBackoffAndInflightInteraction() throws UnknownHostException {
        autoTopicCreationManager = new DefaultAutoTopicCreationManager(
                config,
                Map::of,
                Map::of,
                Map::of,
                topicCreator,
                mockTime,
                testCacheCapacity);

        var requestContext = initializeRequestContextWithUserPrincipal();
        long timeoutMs = 5000L;

        // Simulate error response for backoff-topic
        var backoffResponseData = new CreateTopicsResponseData();
        backoffResponseData.topics().add(new CreatableTopicResult()
                .setName("backoff-topic")
                .setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code())
                .setErrorMessage("Invalid replication factor"));
        topicCreator.setResponseForWithPrincipal(new CreateTopicsResponse(backoffResponseData));

        // Create error for backoff-topic
        var backoffTopics = Map.of("backoff-topic",
                new CreatableTopic().setName("backoff-topic").setNumPartitions(1).setReplicationFactor((short) 1));
        autoTopicCreationManager.createStreamsInternalTopics(backoffTopics, requestContext, timeoutMs);

        // Make inflight-topic in-flight (without completing the request)
        var inflightFuture = new CompletableFuture<CreateTopicsResponse>();
        topicCreator.setFutureForWithPrincipal(inflightFuture);

        var inflightTopics = Map.of("inflight-topic",
                new CreatableTopic().setName("inflight-topic").setNumPartitions(1).setReplicationFactor((short) 1));
        autoTopicCreationManager.createStreamsInternalTopics(inflightTopics, requestContext, timeoutMs);

        // Now attempt to create all three topics together
        var normalResponseData = new CreateTopicsResponseData();
        normalResponseData.topics().add(new CreatableTopicResult()
                .setName("normal-topic")
                .setErrorCode(Errors.NONE.code()));
        topicCreator.setResponseForWithPrincipal(new CreateTopicsResponse(normalResponseData));

        var allTopics = Map.of(
                "backoff-topic", new CreatableTopic().setName("backoff-topic").setNumPartitions(1).setReplicationFactor((short) 1),
                "inflight-topic", new CreatableTopic().setName("inflight-topic").setNumPartitions(1).setReplicationFactor((short) 1),
                "normal-topic", new CreatableTopic().setName("normal-topic").setNumPartitions(1).setReplicationFactor((short) 1));
        autoTopicCreationManager.createStreamsInternalTopics(allTopics, requestContext, timeoutMs);

        // Total 3 requests: 1 for backoff-topic, 1 for inflight-topic, 1 for normal-topic only
        assertEquals(3, topicCreator.withPrincipalCallCount(),
                "Should have called createTopicWithPrincipal 3 times");

        // Verify that only normal-topic was included in the last request
        var calls = topicCreator.getWithPrincipalCalls();
        var lastRequest = calls.get(2).request().build();
        var topicNamesInLastRequest = new HashSet<String>();
        lastRequest.data().topics().forEach(t -> topicNamesInLastRequest.add(t.name()));
        assertEquals(1, topicNamesInLastRequest.size(), "Only normal-topic should be created");
        assertTrue(topicNamesInLastRequest.contains("normal-topic"), "normal-topic should be in the request");
        assertFalse(topicNamesInLastRequest.contains("backoff-topic"), "backoff-topic should be filtered (in back-off)");
        assertFalse(topicNamesInLastRequest.contains("inflight-topic"), "inflight-topic should be filtered (in-flight)");
    }

    private RequestContext initializeRequestContextWithUserPrincipal() throws UnknownHostException {
        var userPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user");
        var principalSerde = new KafkaPrincipalSerde() {
            @Override
            public byte[] serialize(KafkaPrincipal principal) {
                return Utils.utf8(principal.toString());
            }
            @Override
            public KafkaPrincipal deserialize(byte[] bytes) {
                return SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes));
            }
        };
        return initializeRequestContext(userPrincipal, Optional.of(principalSerde));
    }

    private RequestContext initializeRequestContext(
            KafkaPrincipal kafkaPrincipal,
            Optional<KafkaPrincipalSerde> principalSerde
    ) throws UnknownHostException {
        var requestHeader = new RequestHeader(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion(),
                "clientId", 0);
        return new RequestContext(requestHeader, "1", InetAddress.getLocalHost(), Optional.empty(),
                kafkaPrincipal, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false, principalSerde);
    }

    private void createTopicAndVerifyResult(
            Errors error,
            String topicName,
            boolean isInternal
    ) {
        var topicResponses = autoTopicCreationManager.createTopics(Set.of(topicName), ControllerMutationQuota.UNBOUNDED_CONTROLLER_MUTATION_QUOTA);

        var expectedResponses = List.of(new MetadataResponseTopic()
                .setErrorCode(error.code())
                .setIsInternal(isInternal)
                .setName(topicName));

        assertEquals(expectedResponses, topicResponses);
    }

    /**
     * Test implementation of TopicCreator that tracks method calls and allows configuring responses.
     */
    static class TestTopicCreator implements TopicCreator {

        record WithPrincipalCall(RequestContext requestContext, CreateTopicsRequest.Builder request) { }

        private final List<WithPrincipalCall> withPrincipalCalls = new ArrayList<>();
        private final List<CreateTopicsRequest.Builder> withoutPrincipalCalls = new ArrayList<>();
        private CompletableFuture<CreateTopicsResponse> withPrincipalResponse;
        private CompletableFuture<CreateTopicsResponse> withoutPrincipalResponse;

        @Override
        public CompletableFuture<CreateTopicsResponse> createTopicWithPrincipal(
                RequestContext requestContext,
                CreateTopicsRequest.Builder request
        ) {
            withPrincipalCalls.add(new WithPrincipalCall(requestContext, request));
            return withPrincipalResponse != null ? withPrincipalResponse : CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<CreateTopicsResponse> createTopicWithoutPrincipal(
                CreateTopicsRequest.Builder request
        ) {
            withoutPrincipalCalls.add(request);
            return withoutPrincipalResponse != null ? withoutPrincipalResponse : CompletableFuture.completedFuture(null);
        }

        void setResponseForWithPrincipal(CreateTopicsResponse response) {
            withPrincipalResponse = CompletableFuture.completedFuture(response);
        }

        void setResponseForWithoutPrincipal(CreateTopicsResponse response) {
            withoutPrincipalResponse = CompletableFuture.completedFuture(response);
        }

        void setFutureForWithPrincipal(CompletableFuture<CreateTopicsResponse> future) {
            withPrincipalResponse = future;
        }

        void setFutureForWithoutPrincipal(CompletableFuture<CreateTopicsResponse> future) {
            withoutPrincipalResponse = future;
        }

        List<WithPrincipalCall> getWithPrincipalCalls() {
            return List.copyOf(withPrincipalCalls);
        }

        List<CreateTopicsRequest.Builder> getWithoutPrincipalCalls() {
            return List.copyOf(withoutPrincipalCalls);
        }

        int withPrincipalCallCount() {
            return withPrincipalCalls.size();
        }

        int withoutPrincipalCallCount() {
            return withoutPrincipalCalls.size();
        }

        void reset() {
            withPrincipalCalls.clear();
            withoutPrincipalCalls.clear();
            withPrincipalResponse = null;
            withoutPrincipalResponse = null;
        }
    }
}
