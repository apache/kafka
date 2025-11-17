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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NetworkPartitionMetadataClientTest {
    private static final MockTime MOCK_TIME = new MockTime();
    private static final MetadataCache METADATA_CACHE = mock(MetadataCache.class);
    private static final Supplier<KafkaClient> KAFKA_CLIENT_SUPPLIER = () -> mock(KafkaClient.class);
    private static final String HOST = "localhost";
    private static final int PORT = 9092;
    private static final ListenerName LISTENER_NAME = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);
    private static final String TOPIC = "test-topic";
    private static final int PARTITION = 0;
    private static final Node LEADER_NODE = new Node(1, HOST, PORT);

    private NetworkPartitionMetadataClient networkPartitionMetadataClient;

    private static class NetworkPartitionMetadataClientBuilder {
        private MetadataCache metadataCache = METADATA_CACHE;
        private Supplier<KafkaClient> kafkaClientSupplier = KAFKA_CLIENT_SUPPLIER;

        NetworkPartitionMetadataClientBuilder withMetadataCache(MetadataCache metadataCache) {
            this.metadataCache = metadataCache;
            return this;
        }

        NetworkPartitionMetadataClientBuilder withKafkaClientSupplier(Supplier<KafkaClient> kafkaClientSupplier) {
            this.kafkaClientSupplier = kafkaClientSupplier;
            return this;
        }

        static NetworkPartitionMetadataClientBuilder builder() {
            return new NetworkPartitionMetadataClientBuilder();
        }

        NetworkPartitionMetadataClient build() {
            return new NetworkPartitionMetadataClient(metadataCache, kafkaClientSupplier, MOCK_TIME, LISTENER_NAME);
        }
    }

    @BeforeEach
    public void setUp() {
        networkPartitionMetadataClient = null;
    }

    @AfterEach
    public void tearDown() {
        if (networkPartitionMetadataClient != null) {
            try {
                networkPartitionMetadataClient.close();
            } catch (Exception e) {
                fail("Failed to close NetworkPartitionMetadataClient", e);
            }
        }
    }

    @Test
    public void testListLatestOffsetsSuccess() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        long expectedOffset = 100L;
        MetadataCache metadataCache = mock(MetadataCache.class);
        MockClient client = new MockClient(MOCK_TIME);

        // Mock metadata cache to return leader node
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, PARTITION, LISTENER_NAME))
            .thenReturn(Optional.of(LEADER_NODE));

        // Prepare response for ListOffsets request
        client.prepareResponseFrom(body -> {
            if (body instanceof ListOffsetsRequest request) {
                ListOffsetsTopic requestTopic = request.data().topics().get(0);
                return requestTopic.name().equals(TOPIC) &&
                    requestTopic.partitions().get(0).partitionIndex() == PARTITION;
            }
            return false;
        }, new ListOffsetsResponse(
            new org.apache.kafka.common.message.ListOffsetsResponseData()
                .setTopics(List.of(
                    new ListOffsetsTopicResponse()
                        .setName(TOPIC)
                        .setPartitions(List.of(
                            new ListOffsetsPartitionResponse()
                                .setPartitionIndex(PARTITION)
                                .setErrorCode(Errors.NONE.code())
                                .setOffset(expectedOffset)
                                .setTimestamp(System.currentTimeMillis())
                        ))
                ))
        ), LEADER_NODE);

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .withKafkaClientSupplier(() -> client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        PartitionMetadataClient.OffsetResponse response = futures.get(tp).get();
        assertTrue(futures.get(tp).isDone() && !futures.get(tp).isCompletedExceptionally());
        assertNotNull(response);
        assertEquals(Errors.NONE.code(), response.error().code());
        assertEquals(expectedOffset, response.offset());
    }

    @Test
    public void testListLatestOffsetsEmptyPartitionLeader() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        MetadataCache metadataCache = mock(MetadataCache.class);

        // Mock metadata cache to return leader node
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, PARTITION, LISTENER_NAME))
            .thenReturn(Optional.empty());

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        PartitionMetadataClient.OffsetResponse response = futures.get(tp).get();
        assertTrue(futures.get(tp).isDone() && !futures.get(tp).isCompletedExceptionally());
        assertNotNull(response);
        assertEquals(Errors.LEADER_NOT_AVAILABLE.code(), response.error().code());
    }

    @Test
    public void testListLatestOffsetsNoNodePartitionLeader() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        MetadataCache metadataCache = mock(MetadataCache.class);

        // Mock metadata cache to return leader node
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, PARTITION, LISTENER_NAME))
            .thenReturn(Optional.of(Node.noNode()));

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        PartitionMetadataClient.OffsetResponse response = futures.get(tp).get();
        assertTrue(futures.get(tp).isDone() && !futures.get(tp).isCompletedExceptionally());
        assertNotNull(response);
        assertEquals(Errors.LEADER_NOT_AVAILABLE.code(), response.error().code());
    }

    @Test
    public void testListLatestOffsetsNullResponseBody() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        MetadataCache metadataCache = mock(MetadataCache.class);
        MockClient client = new MockClient(MOCK_TIME);

        // Mock metadata cache to return leader node
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, PARTITION, LISTENER_NAME))
            .thenReturn(Optional.of(LEADER_NODE));

        // Prepare null response
        client.prepareResponseFrom(body -> {
            if (body instanceof ListOffsetsRequest) {
                ListOffsetsRequest request = (ListOffsetsRequest) body;
                ListOffsetsTopic requestTopic = request.data().topics().get(0);
                return requestTopic.name().equals(TOPIC) &&
                    requestTopic.partitions().get(0).partitionIndex() == PARTITION;
            }
            return false;
        }, null, LEADER_NODE);

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .withKafkaClientSupplier(() -> client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        PartitionMetadataClient.OffsetResponse response = futures.get(tp).get();
        assertTrue(futures.get(tp).isDone() && !futures.get(tp).isCompletedExceptionally());
        assertNotNull(response);
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), response.error().code());
    }

    @Test
    public void testListLatestOffsetsNullResponse() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        CompletableFuture<PartitionMetadataClient.OffsetResponse> partitionFuture = new CompletableFuture<>();
        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures = Map.of(
            tp,
            partitionFuture
        );
        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder().build();
        // Pass null as clientResponse.
        networkPartitionMetadataClient.handleResponse(futures, null);
        assertTrue(partitionFuture.isDone() && !partitionFuture.isCompletedExceptionally());
        PartitionMetadataClient.OffsetResponse response = partitionFuture.get();
        assertEquals(-1, response.offset());
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), response.error().code());
    }

    @Test
    public void testListLatestOffsetsAuthenticationError() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        CompletableFuture<PartitionMetadataClient.OffsetResponse> partitionFuture = new CompletableFuture<>();
        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures = Map.of(
            tp,
            partitionFuture
        );
        AuthenticationException authenticationException = new AuthenticationException("Test authentication exception");
        ClientResponse clientResponse = mock(ClientResponse.class);
        // Mock authentication exception in client response.
        when(clientResponse.authenticationException()).thenReturn(authenticationException);
        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder().build();
        networkPartitionMetadataClient.handleResponse(futures, clientResponse);
        assertTrue(partitionFuture.isDone() && !partitionFuture.isCompletedExceptionally());
        PartitionMetadataClient.OffsetResponse response = partitionFuture.get();
        assertEquals(-1, response.offset());
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), response.error().code());
    }

    @Test
    public void testListLatestOffsetsVersionMismatch() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        CompletableFuture<PartitionMetadataClient.OffsetResponse> partitionFuture = new CompletableFuture<>();
        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures = Map.of(
            tp,
            partitionFuture
        );
        UnsupportedVersionException unsupportedVersionException = new UnsupportedVersionException("Test unsupportedVersionException exception");
        ClientResponse clientResponse = mock(ClientResponse.class);
        when(clientResponse.authenticationException()).thenReturn(null);
        // Mock version mismatch exception in client response.
        when(clientResponse.versionMismatch()).thenReturn(unsupportedVersionException);
        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder().build();
        networkPartitionMetadataClient.handleResponse(futures, clientResponse);
        assertTrue(partitionFuture.isDone() && !partitionFuture.isCompletedExceptionally());
        PartitionMetadataClient.OffsetResponse response = partitionFuture.get();
        assertEquals(-1, response.offset());
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), response.error().code());
    }

    @Test
    public void testListLatestOffsetsDisconnected() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        CompletableFuture<PartitionMetadataClient.OffsetResponse> partitionFuture = new CompletableFuture<>();
        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures = Map.of(
            tp,
            partitionFuture
        );
        ClientResponse clientResponse = mock(ClientResponse.class);
        when(clientResponse.authenticationException()).thenReturn(null);
        when(clientResponse.versionMismatch()).thenReturn(null);
        // Mock disconnected in client response.
        when(clientResponse.wasDisconnected()).thenReturn(true);
        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder().build();
        networkPartitionMetadataClient.handleResponse(futures, clientResponse);
        assertTrue(partitionFuture.isDone() && !partitionFuture.isCompletedExceptionally());
        PartitionMetadataClient.OffsetResponse response = partitionFuture.get();
        assertEquals(-1, response.offset());
        assertEquals(Errors.NETWORK_EXCEPTION.code(), response.error().code());
    }

    @Test
    public void testListLatestOffsetsTimedOut() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        CompletableFuture<PartitionMetadataClient.OffsetResponse> partitionFuture = new CompletableFuture<>();
        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures = Map.of(
            tp,
            partitionFuture
        );
        ClientResponse clientResponse = mock(ClientResponse.class);
        when(clientResponse.authenticationException()).thenReturn(null);
        when(clientResponse.versionMismatch()).thenReturn(null);
        when(clientResponse.wasDisconnected()).thenReturn(false);
        // Mock timed out in client response.
        when(clientResponse.wasTimedOut()).thenReturn(true);
        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder().build();
        networkPartitionMetadataClient.handleResponse(futures, clientResponse);
        assertTrue(partitionFuture.isDone() && !partitionFuture.isCompletedExceptionally());
        PartitionMetadataClient.OffsetResponse response = partitionFuture.get();
        assertEquals(-1, response.offset());
        assertEquals(Errors.REQUEST_TIMED_OUT.code(), response.error().code());
    }

    @Test
    public void testListLatestOffsetsMultiplePartitionsSameLeader() throws ExecutionException, InterruptedException {
        TopicPartition tp1 = new TopicPartition(TOPIC, PARTITION);
        TopicPartition tp2 = new TopicPartition(TOPIC, 1);
        long expectedOffset1 = 100L;
        long expectedOffset2 = 200L;

        MetadataCache metadataCache = mock(MetadataCache.class);
        MockClient client = new MockClient(MOCK_TIME);

        // Mock metadata cache to return same leader for both partitions
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, 0, LISTENER_NAME))
            .thenReturn(Optional.of(LEADER_NODE));
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, 1, LISTENER_NAME))
            .thenReturn(Optional.of(LEADER_NODE));

        // Prepare response for ListOffsets request with both partitions
        client.prepareResponseFrom(body -> {
            if (body instanceof ListOffsetsRequest) {
                ListOffsetsRequest request = (ListOffsetsRequest) body;
                ListOffsetsTopic requestTopic = request.data().topics().get(0);
                return requestTopic.name().equals(TOPIC) &&
                    requestTopic.partitions().size() == 2;
            }
            return false;
        }, new ListOffsetsResponse(
            new org.apache.kafka.common.message.ListOffsetsResponseData()
                .setTopics(List.of(
                    new ListOffsetsTopicResponse()
                        .setName(TOPIC)
                        .setPartitions(List.of(
                            new ListOffsetsPartitionResponse()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                                .setOffset(expectedOffset1)
                                .setTimestamp(System.currentTimeMillis()),
                            new ListOffsetsPartitionResponse()
                                .setPartitionIndex(1)
                                .setErrorCode(Errors.NONE.code())
                                .setOffset(expectedOffset2)
                                .setTimestamp(System.currentTimeMillis())
                        ))
                ))
        ), LEADER_NODE);

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .withKafkaClientSupplier(() -> client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp1);
        partitions.add(tp2);

        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(2, futures.size());
        assertTrue(futures.containsKey(tp1));
        assertTrue(futures.containsKey(tp2));

        PartitionMetadataClient.OffsetResponse response1 = futures.get(tp1).get();
        assertTrue(futures.get(tp1).isDone() && !futures.get(tp1).isCompletedExceptionally());
        assertNotNull(response1);
        assertEquals(Errors.NONE.code(), response1.error().code());
        assertEquals(expectedOffset1, response1.offset());

        PartitionMetadataClient.OffsetResponse response2 = futures.get(tp2).get();
        assertTrue(futures.get(tp2).isDone() && !futures.get(tp2).isCompletedExceptionally());
        assertNotNull(response2);
        assertEquals(Errors.NONE.code(), response2.error().code());
        assertEquals(expectedOffset2, response2.offset());
    }

    @Test
    public void testListLatestOffsetsMultiplePartitionsDifferentLeaders() throws ExecutionException, InterruptedException {
        TopicPartition tp1 = new TopicPartition(TOPIC, 0);
        TopicPartition tp2 = new TopicPartition(TOPIC, 1);
        Node leaderNode1 = LEADER_NODE;
        Node leaderNode2 = new Node(2, HOST, PORT + 1);
        long expectedOffset1 = 100L;
        long expectedOffset2 = 200L;

        MetadataCache metadataCache = mock(MetadataCache.class);
        MockClient client = new MockClient(MOCK_TIME);

        // Mock metadata cache to return different leaders
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, 0, LISTENER_NAME))
            .thenReturn(Optional.of(leaderNode1));
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, 1, LISTENER_NAME))
            .thenReturn(Optional.of(leaderNode2));

        // Prepare response for first leader
        client.prepareResponseFrom(body -> {
            if (body instanceof ListOffsetsRequest) {
                ListOffsetsRequest request = (ListOffsetsRequest) body;
                ListOffsetsTopic requestTopic = request.data().topics().get(0);
                return requestTopic.name().equals(TOPIC) &&
                    requestTopic.partitions().size() == 1 &&
                    requestTopic.partitions().get(0).partitionIndex() == 0;
            }
            return false;
        }, new ListOffsetsResponse(
            new org.apache.kafka.common.message.ListOffsetsResponseData()
                .setTopics(List.of(
                    new ListOffsetsTopicResponse()
                        .setName(TOPIC)
                        .setPartitions(List.of(
                            new ListOffsetsPartitionResponse()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                                .setOffset(expectedOffset1)
                                .setTimestamp(System.currentTimeMillis())
                        ))
                ))
        ), leaderNode1);

        // Prepare response for second leader
        client.prepareResponseFrom(body -> {
            if (body instanceof ListOffsetsRequest) {
                ListOffsetsRequest request = (ListOffsetsRequest) body;
                ListOffsetsTopic requestTopic = request.data().topics().get(0);
                return requestTopic.name().equals(TOPIC) &&
                    requestTopic.partitions().size() == 1 &&
                    requestTopic.partitions().get(0).partitionIndex() == 1;
            }
            return false;
        }, new ListOffsetsResponse(
            new org.apache.kafka.common.message.ListOffsetsResponseData()
                .setTopics(List.of(
                    new ListOffsetsTopicResponse()
                        .setName(TOPIC)
                        .setPartitions(List.of(
                            new ListOffsetsPartitionResponse()
                                .setPartitionIndex(1)
                                .setErrorCode(Errors.NONE.code())
                                .setOffset(expectedOffset2)
                                .setTimestamp(System.currentTimeMillis())
                        ))
                ))
        ), leaderNode2);

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .withKafkaClientSupplier(() -> client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp1);
        partitions.add(tp2);

        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(2, futures.size());
        assertTrue(futures.containsKey(tp1));
        assertTrue(futures.containsKey(tp2));

        PartitionMetadataClient.OffsetResponse response1 = futures.get(tp1).get();
        assertTrue(futures.get(tp1).isDone() && !futures.get(tp1).isCompletedExceptionally());
        assertNotNull(response1);
        assertEquals(Errors.NONE.code(), response1.error().code());
        assertEquals(expectedOffset1, response1.offset());

        PartitionMetadataClient.OffsetResponse response2 = futures.get(tp2).get();
        assertTrue(futures.get(tp2).isDone() && !futures.get(tp2).isCompletedExceptionally());
        assertNotNull(response2);
        assertEquals(Errors.NONE.code(), response2.error().code());
        assertEquals(expectedOffset2, response2.offset());
    }

    @Test
    public void testListLatestOffsetsMultipleTopics() throws ExecutionException, InterruptedException {
        String topic1 = TOPIC;
        String topic2 = "test-topic-2";
        TopicPartition tp1 = new TopicPartition(topic1, 0);
        TopicPartition tp2 = new TopicPartition(topic2, 0);
        long expectedOffset1 = 100L;
        long expectedOffset2 = 200L;

        MetadataCache metadataCache = mock(MetadataCache.class);
        MockClient client = new MockClient(MOCK_TIME);

        // Mock metadata cache to return same leader for both topics
        when(metadataCache.getPartitionLeaderEndpoint(topic1, 0, LISTENER_NAME))
            .thenReturn(Optional.of(LEADER_NODE));
        when(metadataCache.getPartitionLeaderEndpoint(topic2, 0, LISTENER_NAME))
            .thenReturn(Optional.of(LEADER_NODE));

        // Prepare response for ListOffsets request with both topics
        client.prepareResponseFrom(body -> {
            if (body instanceof ListOffsetsRequest) {
                ListOffsetsRequest request = (ListOffsetsRequest) body;
                return request.data().topics().size() == 2;
            }
            return false;
        }, new ListOffsetsResponse(
            new org.apache.kafka.common.message.ListOffsetsResponseData()
                .setTopics(List.of(
                    new ListOffsetsTopicResponse()
                        .setName(topic1)
                        .setPartitions(List.of(
                            new ListOffsetsPartitionResponse()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                                .setOffset(expectedOffset1)
                                .setTimestamp(System.currentTimeMillis())
                        )),
                    new ListOffsetsTopicResponse()
                        .setName(topic2)
                        .setPartitions(List.of(
                            new ListOffsetsPartitionResponse()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code())
                                .setOffset(expectedOffset2)
                                .setTimestamp(System.currentTimeMillis())
                        ))
                ))
        ), LEADER_NODE);

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .withKafkaClientSupplier(() -> client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp1);
        partitions.add(tp2);

        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(2, futures.size());
        assertTrue(futures.containsKey(tp1));
        assertTrue(futures.containsKey(tp2));

        PartitionMetadataClient.OffsetResponse response1 = futures.get(tp1).get();
        assertTrue(futures.get(tp1).isDone() && !futures.get(tp1).isCompletedExceptionally());
        assertNotNull(response1);
        assertEquals(Errors.NONE.code(), response1.error().code());
        assertEquals(expectedOffset1, response1.offset());

        PartitionMetadataClient.OffsetResponse response2 = futures.get(tp2).get();
        assertTrue(futures.get(tp2).isDone() && !futures.get(tp2).isCompletedExceptionally());
        assertNotNull(response2);
        assertEquals(Errors.NONE.code(), response2.error().code());
        assertEquals(expectedOffset2, response2.offset());
    }

    @Test
    public void testListLatestOffsetsNullPartitions() {
        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder().build();

        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(null);

        assertNotNull(futures);
        assertTrue(futures.isEmpty());
    }

    @Test
    public void testListLatestOffsetsEmptyPartitions() {
        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder().build();

        Set<TopicPartition> partitions = new HashSet<>();

        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertTrue(futures.isEmpty());
    }

    @Test
    public void testListLatestOffsetsServerError() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        MetadataCache metadataCache = mock(MetadataCache.class);
        MockClient client = new MockClient(MOCK_TIME);

        // Mock metadata cache to return leader node
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, PARTITION, LISTENER_NAME))
            .thenReturn(Optional.of(LEADER_NODE));

        // Prepare error response
        client.prepareResponseFrom(body -> {
            if (body instanceof ListOffsetsRequest) {
                ListOffsetsRequest request = (ListOffsetsRequest) body;
                ListOffsetsTopic requestTopic = request.data().topics().get(0);
                return requestTopic.name().equals(TOPIC) &&
                    requestTopic.partitions().get(0).partitionIndex() == PARTITION;
            }
            return false;
        }, new ListOffsetsResponse(
            new org.apache.kafka.common.message.ListOffsetsResponseData()
                .setTopics(List.of(
                    new ListOffsetsTopicResponse()
                        .setName(TOPIC)
                        .setPartitions(List.of(
                            new ListOffsetsPartitionResponse()
                                .setPartitionIndex(PARTITION)
                                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                        ))
                ))
        ), LEADER_NODE);

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .withKafkaClientSupplier(() -> client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        PartitionMetadataClient.OffsetResponse response = futures.get(tp).get();
        assertTrue(futures.get(tp).isDone() && !futures.get(tp).isCompletedExceptionally());
        assertNotNull(response);
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), response.error().code());
    }

    @Test
    public void testListLatestOffsetsMissingPartitionInResponse() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        MetadataCache metadataCache = mock(MetadataCache.class);
        MockClient client = new MockClient(MOCK_TIME);

        // Mock metadata cache to return leader node
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, PARTITION, LISTENER_NAME))
            .thenReturn(Optional.of(LEADER_NODE));

        // Prepare response without the requested partition
        client.prepareResponseFrom(body -> {
            if (body instanceof ListOffsetsRequest) {
                ListOffsetsRequest request = (ListOffsetsRequest) body;
                ListOffsetsTopic requestTopic = request.data().topics().get(0);
                return requestTopic.name().equals(TOPIC) &&
                    requestTopic.partitions().get(0).partitionIndex() == PARTITION;
            }
            return false;
        }, new ListOffsetsResponse(
            new org.apache.kafka.common.message.ListOffsetsResponseData()
                .setTopics(List.of(
                    new ListOffsetsTopicResponse()
                        .setName(TOPIC)
                        .setPartitions(List.of())
                ))
        ), LEADER_NODE);

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .withKafkaClientSupplier(() -> client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<PartitionMetadataClient.OffsetResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        PartitionMetadataClient.OffsetResponse response = futures.get(tp).get();
        assertTrue(futures.get(tp).isDone() && !futures.get(tp).isCompletedExceptionally());
        assertNotNull(response);
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), response.error().code());
    }

    @Test
    public void testClose() {
        KafkaClient client = mock(KafkaClient.class);
        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withKafkaClientSupplier(() -> client)
            .build();
        try {
            verify(client, times(0)).close();
            // Ensure send thread is initialized.
            networkPartitionMetadataClient.ensureSendThreadInitialized();
            networkPartitionMetadataClient.close();
            // KafkaClient is closed when NetworkPartitionMetadataClient is closed.
            verify(client, times(1)).close();
        } catch (Exception e) {
            fail("unexpected exception", e);
        }
    }

    @Test
    public void testLazyInitialization() {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        long expectedOffset = 100L;
        MetadataCache metadataCache = mock(MetadataCache.class);
        MockClient client = new MockClient(MOCK_TIME);

        // Track if client supplier was called
        final boolean[] supplierCalled = {false};
        Supplier<KafkaClient> kafkaClientSupplier = () -> {
            supplierCalled[0] = true;
            return client;
        };

        // Mock metadata cache to return leader node
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, PARTITION, LISTENER_NAME))
            .thenReturn(Optional.of(LEADER_NODE));

        // Prepare response for ListOffsets request
        client.prepareResponseFrom(body -> {
            if (body instanceof ListOffsetsRequest request) {
                ListOffsetsTopic requestTopic = request.data().topics().get(0);
                return requestTopic.name().equals(TOPIC) &&
                    requestTopic.partitions().get(0).partitionIndex() == PARTITION;
            }
            return false;
        }, new ListOffsetsResponse(
            new org.apache.kafka.common.message.ListOffsetsResponseData()
                .setTopics(List.of(
                    new ListOffsetsTopicResponse()
                        .setName(TOPIC)
                        .setPartitions(List.of(
                            new ListOffsetsPartitionResponse()
                                .setPartitionIndex(PARTITION)
                                .setErrorCode(Errors.NONE.code())
                                .setOffset(expectedOffset)
                                .setTimestamp(System.currentTimeMillis())
                        ))
                ))
        ), LEADER_NODE);

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .withKafkaClientSupplier(kafkaClientSupplier)
            .build();

        // Verify supplier was not called before listLatestOffsets
        assertFalse(supplierCalled[0]);

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        networkPartitionMetadataClient.listLatestOffsets(partitions);

        // Verify supplier was called during listLatestOffsets
        assertTrue(supplierCalled[0]);
    }

    @Test
    public void testCloseWithoutInitialization() throws IOException {
        KafkaClient client = mock(KafkaClient.class);
        final boolean[] supplierCalled = {false};
        Supplier<KafkaClient> clientSupplier = () -> {
            supplierCalled[0] = true;
            return client;
        };

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withKafkaClientSupplier(clientSupplier)
            .build();

        // Close without calling listLatestOffsets
        networkPartitionMetadataClient.close();

        // Verify supplier was never called
        assertFalse(supplierCalled[0]);
        // Verify client.close() was never called since sendThread was never initialized
        verify(client, never()).close();
    }

    @Test
    public void testLazyInitializationWithEmptyPartitions() {
        MetadataCache metadataCache = mock(MetadataCache.class);
        final boolean[] supplierCalled = {false};
        Supplier<KafkaClient> clientSupplier = () -> {
            supplierCalled[0] = true;
            return mock(KafkaClient.class);
        };

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .withKafkaClientSupplier(clientSupplier)
            .build();

        // Call listLatestOffsets with empty partitions
        networkPartitionMetadataClient.listLatestOffsets(new HashSet<>());

        // Verify supplier was not called since no partitions were provided.
        assertFalse(supplierCalled[0]);
    }

    @Test
    public void testLazyInitializationWithNullPartitions() {
        MetadataCache metadataCache = mock(MetadataCache.class);
        final boolean[] supplierCalled = {false};
        Supplier<KafkaClient> clientSupplier = () -> {
            supplierCalled[0] = true;
            return mock(KafkaClient.class);
        };

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .withKafkaClientSupplier(clientSupplier)
            .build();

        // Call listLatestOffsets with null partitions
        networkPartitionMetadataClient.listLatestOffsets(null);

        // Verify supplier was not called since no partitions were provided.
        assertFalse(supplierCalled[0]);
    }

    @Test
    public void testLazyInitializationOnlyOnce() {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        long expectedOffset = 100L;
        MetadataCache metadataCache = mock(MetadataCache.class);
        MockClient client = new MockClient(MOCK_TIME);

        // Track how many times supplier was called
        final int[] supplierCallCount = {0};
        Supplier<KafkaClient> clientSupplier = () -> {
            supplierCallCount[0]++;
            return client;
        };

        // Mock metadata cache to return leader node
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, PARTITION, LISTENER_NAME))
            .thenReturn(Optional.of(LEADER_NODE));

        // Prepare multiple responses for multiple calls
        client.prepareResponseFrom(body -> {
            if (body instanceof ListOffsetsRequest request) {
                ListOffsetsTopic requestTopic = request.data().topics().get(0);
                return requestTopic.name().equals(TOPIC) &&
                    requestTopic.partitions().get(0).partitionIndex() == PARTITION;
            }
            return false;
        }, new ListOffsetsResponse(
            new org.apache.kafka.common.message.ListOffsetsResponseData()
                .setTopics(List.of(
                    new ListOffsetsTopicResponse()
                        .setName(TOPIC)
                        .setPartitions(List.of(
                            new ListOffsetsPartitionResponse()
                                .setPartitionIndex(PARTITION)
                                .setErrorCode(Errors.NONE.code())
                                .setOffset(expectedOffset)
                                .setTimestamp(System.currentTimeMillis())
                        ))
                ))
        ), LEADER_NODE);

        client.prepareResponseFrom(body -> {
            if (body instanceof ListOffsetsRequest request) {
                ListOffsetsTopic requestTopic = request.data().topics().get(0);
                return requestTopic.name().equals(TOPIC) &&
                    requestTopic.partitions().get(0).partitionIndex() == PARTITION;
            }
            return false;
        }, new ListOffsetsResponse(
            new org.apache.kafka.common.message.ListOffsetsResponseData()
                .setTopics(List.of(
                    new ListOffsetsTopicResponse()
                        .setName(TOPIC)
                        .setPartitions(List.of(
                            new ListOffsetsPartitionResponse()
                                .setPartitionIndex(PARTITION)
                                .setErrorCode(Errors.NONE.code())
                                .setOffset(expectedOffset + 1)
                                .setTimestamp(System.currentTimeMillis())
                        ))
                ))
        ), LEADER_NODE);

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.builder()
            .withMetadataCache(metadataCache)
            .withKafkaClientSupplier(clientSupplier)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        // First call to listLatestOffsets
        networkPartitionMetadataClient.listLatestOffsets(partitions);

        // Verify supplier was called once
        assertEquals(1, supplierCallCount[0]);

        // Second call to listLatestOffsets
        networkPartitionMetadataClient.listLatestOffsets(partitions);

        // Verify supplier was still only called once (not again)
        assertEquals(1, supplierCallCount[0]);
    }
}
