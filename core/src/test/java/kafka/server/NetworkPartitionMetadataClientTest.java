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
package kafka.server;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NetworkPartitionMetadataClientTest {
    private static final MockTime MOCK_TIME = new MockTime();
    private static final MetadataCache METADATA_CACHE = mock(MetadataCache.class);
    private static final KafkaClient CLIENT = mock(KafkaClient.class);
    private static final String HOST = "localhost";
    private static final int PORT = 9092;
    private static final ListenerName LISTENER_NAME = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);
    private static final String TOPIC = "test-topic";
    private static final int PARTITION = 0;
    private static final Node LEADER_NODE = new Node(1, HOST, PORT);
    private static final long REQUEST_TIMEOUT_MS = CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS;
    
    private NetworkPartitionMetadataClient networkPartitionMetadataClient;

    private static class NetworkPartitionMetadataClientBuilder {
        private MetadataCache metadataCache = METADATA_CACHE;
        private KafkaClient client = CLIENT;
        private Time time = MOCK_TIME;
        private ListenerName listenerName = LISTENER_NAME;

        NetworkPartitionMetadataClientBuilder withMetadataCache(MetadataCache metadataCache) {
            this.metadataCache = metadataCache;
            return this;
        }

        NetworkPartitionMetadataClientBuilder withKafkaClient(KafkaClient client) {
            this.client = client;
            return this;
        }

        NetworkPartitionMetadataClientBuilder withTime(Time time) {
            this.time = time;
            return this;
        }

        NetworkPartitionMetadataClientBuilder withListenerName(ListenerName listenerName) {
            this.listenerName = listenerName;
            return this;
        }

        public static NetworkPartitionMetadataClientBuilder bulider() {
            return new NetworkPartitionMetadataClientBuilder();
        }

        public NetworkPartitionMetadataClient build() {
            return new NetworkPartitionMetadataClient(metadataCache, client, time, listenerName);
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

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider()
            .withMetadataCache(metadataCache)
            .withKafkaClient(client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        ListOffsetsPartitionResponse response = futures.get(tp).get();
        assertNotNull(response);
        assertEquals(PARTITION, response.partitionIndex());
        assertEquals(Errors.NONE.code(), response.errorCode());
        assertEquals(expectedOffset, response.offset());
    }

    @Test
    public void testListLatestOffsetsEmptyPartitionLeader() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        MetadataCache metadataCache = mock(MetadataCache.class);

        // Mock metadata cache to return leader node
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, PARTITION, LISTENER_NAME))
            .thenReturn(Optional.empty());

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider()
            .withMetadataCache(metadataCache)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        ListOffsetsPartitionResponse response = futures.get(tp).get();
        assertNotNull(response);
        assertEquals(PARTITION, response.partitionIndex());
        assertEquals(Errors.LEADER_NOT_AVAILABLE.code(), response.errorCode());
    }

    @Test
    public void testListLatestOffsetsNoNodePartitionLeader() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        MetadataCache metadataCache = mock(MetadataCache.class);

        // Mock metadata cache to return leader node
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, PARTITION, LISTENER_NAME))
            .thenReturn(Optional.of(Node.noNode()));

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider()
            .withMetadataCache(metadataCache)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        ListOffsetsPartitionResponse response = futures.get(tp).get();
        assertNotNull(response);
        assertEquals(PARTITION, response.partitionIndex());
        assertEquals(Errors.LEADER_NOT_AVAILABLE.code(), response.errorCode());
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

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider()
            .withMetadataCache(metadataCache)
            .withKafkaClient(client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        ListOffsetsPartitionResponse response = futures.get(tp).get();
        assertNotNull(response);
        assertEquals(PARTITION, response.partitionIndex());
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), response.errorCode());
    }

    @Test
    public void testListLatestOffsetsDisconnected() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        MetadataCache metadataCache = mock(MetadataCache.class);
        MockClient client = new MockClient(MOCK_TIME);

        // Mock metadata cache to return leader node
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, PARTITION, LISTENER_NAME))
            .thenReturn(Optional.of(LEADER_NODE));

        // Set node as unreachable to simulate disconnect
        client.setUnreachable(LEADER_NODE, REQUEST_TIMEOUT_MS + 1);

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider()
            .withMetadataCache(metadataCache)
            .withKafkaClient(client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        ListOffsetsPartitionResponse response = futures.get(tp).get();
        assertNotNull(response);
        assertEquals(PARTITION, response.partitionIndex());
        assertEquals(Errors.NETWORK_EXCEPTION.code(), response.errorCode());
    }

    @Test
    public void testListLatestOffsetsHasNoResponse() throws ExecutionException, InterruptedException {
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        MetadataCache metadataCache = mock(MetadataCache.class);
        MockClient client = new MockClient(MOCK_TIME);

        // Mock metadata cache to return leader node
        when(metadataCache.getPartitionLeaderEndpoint(TOPIC, PARTITION, LISTENER_NAME))
            .thenReturn(Optional.of(LEADER_NODE));

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider()
            .withMetadataCache(metadataCache)
            .withKafkaClient(client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        // Wait for the request to be sent
        TestUtils.waitForCondition(() -> !client.requests().isEmpty(), REQUEST_TIMEOUT_MS,
            "Request was not sent");

        // Get the pending request
        ClientRequest request = client.requests().poll();
        assertNotNull(request, "Expected a request to be queued");

        // Create a ClientResponse with null responseBody
        ClientResponse clientResponse = new ClientResponse(
            request.makeHeader((short) 0),
            request.callback(),
            request.destination(),
            request.createdTimeMs(),
            MOCK_TIME.milliseconds(),
            false,
            false,
            null,
            null,
            null // responseBody
        );

        // Add the response to MockClient's responses queue
        client.responses().add(clientResponse);

        // Poll the client to process the response
        client.poll(0, MOCK_TIME.milliseconds());

        ListOffsetsPartitionResponse response = futures.get(tp).get();
        assertNotNull(response);
        assertEquals(PARTITION, response.partitionIndex());
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), response.errorCode());
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

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider()
            .withMetadataCache(metadataCache)
            .withKafkaClient(client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp1);
        partitions.add(tp2);

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(2, futures.size());
        assertTrue(futures.containsKey(tp1));
        assertTrue(futures.containsKey(tp2));

        ListOffsetsPartitionResponse response1 = futures.get(tp1).get();
        assertNotNull(response1);
        assertEquals(0, response1.partitionIndex());
        assertEquals(Errors.NONE.code(), response1.errorCode());
        assertEquals(expectedOffset1, response1.offset());

        ListOffsetsPartitionResponse response2 = futures.get(tp2).get();
        assertNotNull(response2);
        assertEquals(1, response2.partitionIndex());
        assertEquals(Errors.NONE.code(), response2.errorCode());
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

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider()
            .withMetadataCache(metadataCache)
            .withKafkaClient(client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp1);
        partitions.add(tp2);

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(2, futures.size());
        assertTrue(futures.containsKey(tp1));
        assertTrue(futures.containsKey(tp2));

        ListOffsetsPartitionResponse response1 = futures.get(tp1).get();
        assertNotNull(response1);
        assertEquals(0, response1.partitionIndex());
        assertEquals(Errors.NONE.code(), response1.errorCode());
        assertEquals(expectedOffset1, response1.offset());

        ListOffsetsPartitionResponse response2 = futures.get(tp2).get();
        assertNotNull(response2);
        assertEquals(1, response2.partitionIndex());
        assertEquals(Errors.NONE.code(), response2.errorCode());
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

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider()
            .withMetadataCache(metadataCache)
            .withKafkaClient(client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp1);
        partitions.add(tp2);

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(2, futures.size());
        assertTrue(futures.containsKey(tp1));
        assertTrue(futures.containsKey(tp2));

        ListOffsetsPartitionResponse response1 = futures.get(tp1).get();
        assertNotNull(response1);
        assertEquals(0, response1.partitionIndex());
        assertEquals(Errors.NONE.code(), response1.errorCode());
        assertEquals(expectedOffset1, response1.offset());

        ListOffsetsPartitionResponse response2 = futures.get(tp2).get();
        assertNotNull(response2);
        assertEquals(0, response2.partitionIndex());
        assertEquals(Errors.NONE.code(), response2.errorCode());
        assertEquals(expectedOffset2, response2.offset());
    }

    @Test
    public void testListLatestOffsetsNullPartitions() {
        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider().build();

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(null);

        assertNotNull(futures);
        assertTrue(futures.isEmpty());
    }

    @Test
    public void testListLatestOffsetsEmptyPartitions() {
        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider().build();

        Set<TopicPartition> partitions = new HashSet<>();

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
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
                                .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
                                .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                        ))
                ))
        ), LEADER_NODE);

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider()
            .withMetadataCache(metadataCache)
            .withKafkaClient(client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        ListOffsetsPartitionResponse response = futures.get(tp).get();
        assertNotNull(response);
        assertEquals(PARTITION, response.partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), response.errorCode());
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

        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider()
            .withMetadataCache(metadataCache)
            .withKafkaClient(client)
            .build();

        Set<TopicPartition> partitions = new HashSet<>();
        partitions.add(tp);

        Map<TopicPartition, CompletableFuture<ListOffsetsPartitionResponse>> futures =
            networkPartitionMetadataClient.listLatestOffsets(partitions);

        assertNotNull(futures);
        assertEquals(1, futures.size());
        assertTrue(futures.containsKey(tp));

        ListOffsetsPartitionResponse response = futures.get(tp).get();
        assertNotNull(response);
        assertEquals(PARTITION, response.partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), response.errorCode());
    }


    @Test
    public void testClose() throws InterruptedException {
        KafkaClient client = mock(KafkaClient.class);
        networkPartitionMetadataClient = NetworkPartitionMetadataClientBuilder.bulider()
            .withKafkaClient(client)
            .build();
        try {
            verify(client, times(0)).close();
            networkPartitionMetadataClient.close();
            // KafkaClient is closed when NetworkPartitionMetadataClient is closed.
            verify(client, times(1)).close();
        } catch (Exception e) {
            fail("unexpected exception", e);
        }
    }
}
