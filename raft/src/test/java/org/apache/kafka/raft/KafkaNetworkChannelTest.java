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
package org.apache.kafka.raft;

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochResponse;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.EndQuorumEpochResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FetchSnapshotResponse;
import org.apache.kafka.common.requests.UpdateRaftVoterResponse;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.VoteResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaNetworkChannelTest {

    private static class StubMetadataUpdater implements MockClient.MockMetadataUpdater {

        @Override
        public List<Node> fetchNodes() {
            return Collections.emptyList();
        }

        @Override
        public boolean isUpdateNeeded() {
            return false;
        }

        @Override
        public void update(Time time, MockClient.MetadataUpdate update) { }
    }

    private static final List<ApiKeys> RAFT_APIS = asList(
        ApiKeys.VOTE,
        ApiKeys.BEGIN_QUORUM_EPOCH,
        ApiKeys.END_QUORUM_EPOCH,
        ApiKeys.FETCH,
        ApiKeys.FETCH_SNAPSHOT,
        ApiKeys.UPDATE_RAFT_VOTER
    );

    private final int requestTimeoutMs = 30000;
    private final Time time = new MockTime();
    private final MockClient client = new MockClient(time, new StubMetadataUpdater());
    private final TopicPartition topicPartition = new TopicPartition("topic", 0);
    private final Uuid topicId = Uuid.randomUuid();
    private final KafkaNetworkChannel channel = new KafkaNetworkChannel(
        time,
        ListenerName.normalised("NAME"),
        client,
        requestTimeoutMs,
        "test-raft"
    );

    private Node nodeWithId(boolean withId) {
        int id = withId ? 2 : -2;
        return new Node(id, "127.0.0.1", 9092);
    }

    @BeforeEach
    public void setupSupportedApis() {
        List<ApiVersionsResponseData.ApiVersion> supportedApis = RAFT_APIS
            .stream()
            .map(ApiVersionsResponse::toApiVersion)
            .collect(Collectors.toList());
        client.setNodeApiVersions(NodeApiVersions.create(supportedApis));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSendToBlackedOutDestination(boolean withDestinationId) throws ExecutionException, InterruptedException {
        Node destination = nodeWithId(withDestinationId);
        client.backoff(destination, 500);
        assertBrokerNotAvailable(destination);
    }

    @Test
    public void testWakeupClientOnSend() throws InterruptedException, ExecutionException {
        int destinationId = 2;
        Node destinationNode = new Node(destinationId, "127.0.0.1", 9092);

        client.enableBlockingUntilWakeup(1);

        Thread ioThread = new Thread(() -> {
            // Block in poll until we get the expected wakeup
            channel.pollOnce();

            // Poll a second time to send request and receive response
            channel.pollOnce();
        });

        AbstractResponse response = buildResponse(buildTestErrorResponse(ApiKeys.FETCH, Errors.INVALID_REQUEST));
        client.prepareResponseFrom(response, destinationNode, false);

        ioThread.start();
        RaftRequest.Outbound request = sendTestRequest(ApiKeys.FETCH, destinationNode);

        ioThread.join();
        assertResponseCompleted(request, Errors.INVALID_REQUEST);
    }

    @Test
    public void testSendAndDisconnect() throws ExecutionException, InterruptedException {
        int destinationId = 2;
        Node destinationNode = new Node(destinationId, "127.0.0.1", 9092);

        for (ApiKeys apiKey : RAFT_APIS) {
            AbstractResponse response = buildResponse(buildTestErrorResponse(apiKey, Errors.INVALID_REQUEST));
            client.prepareResponseFrom(response, destinationNode, true);
            sendAndAssertErrorResponse(apiKey, destinationNode, Errors.BROKER_NOT_AVAILABLE);
        }
    }

    @Test
    public void testSendAndFailAuthentication() throws ExecutionException, InterruptedException {
        int destinationId = 2;
        Node destinationNode = new Node(destinationId, "127.0.0.1", 9092);

        for (ApiKeys apiKey : RAFT_APIS) {
            client.createPendingAuthenticationError(destinationNode, 100);
            sendAndAssertErrorResponse(apiKey, destinationNode, Errors.NETWORK_EXCEPTION);

            // reset to clear backoff time
            client.reset();
        }
    }

    private void assertBrokerNotAvailable(Node destination) throws ExecutionException, InterruptedException {
        for (ApiKeys apiKey : RAFT_APIS) {
            sendAndAssertErrorResponse(apiKey, destination, Errors.BROKER_NOT_AVAILABLE);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSendAndReceiveOutboundRequest(boolean withDestinationId) throws ExecutionException, InterruptedException {
        Node destination = nodeWithId(withDestinationId);

        for (ApiKeys apiKey : RAFT_APIS) {
            Errors expectedError = Errors.INVALID_REQUEST;
            AbstractResponse response = buildResponse(buildTestErrorResponse(apiKey, expectedError));
            client.prepareResponseFrom(response, destination);
            System.out.println("api key " + apiKey + ", response " + response);
            sendAndAssertErrorResponse(apiKey, destination, expectedError);
        }
    }

    @Test
    public void testUnsupportedVersionError() throws ExecutionException, InterruptedException {
        int destinationId = 2;
        Node destinationNode = new Node(destinationId, "127.0.0.1", 9092);

        for (ApiKeys apiKey : RAFT_APIS) {
            client.prepareUnsupportedVersionResponse(request -> request.apiKey() == apiKey);
            sendAndAssertErrorResponse(apiKey, destinationNode, Errors.UNSUPPORTED_VERSION);
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.FETCH)
    public void testFetchRequestDowngrade(short version) {
        int destinationId = 2;
        Node destinationNode = new Node(destinationId, "127.0.0.1", 9092);
        sendTestRequest(ApiKeys.FETCH, destinationNode);
        channel.pollOnce();

        assertEquals(1, client.requests().size());
        AbstractRequest request = client.requests().peek().requestBuilder().build(version);

        if (version < 15) {
            assertEquals(1, ((FetchRequest) request).data().replicaId());
            assertEquals(-1, ((FetchRequest) request).data().replicaState().replicaId());
        } else {
            assertEquals(-1, ((FetchRequest) request).data().replicaId());
            assertEquals(1, ((FetchRequest) request).data().replicaState().replicaId());
        }
    }

    private RaftRequest.Outbound sendTestRequest(ApiKeys apiKey, Node destination) {
        int correlationId = channel.newCorrelationId();
        long createdTimeMs = time.milliseconds();
        ApiMessage apiRequest = buildTestRequest(apiKey);
        RaftRequest.Outbound request = new RaftRequest.Outbound(
            correlationId,
            apiRequest,
            destination,
            createdTimeMs
        );
        channel.send(request);
        return request;
    }

    private void assertResponseCompleted(
        RaftRequest.Outbound request,
        Errors expectedError
    ) throws ExecutionException, InterruptedException {
        assertTrue(request.completion.isDone());

        RaftResponse.Inbound response = request.completion.get();
        assertEquals(request.destination(), response.source());
        assertEquals(request.correlationId(), response.correlationId());
        assertEquals(request.data().apiKey(), response.data().apiKey());
        assertEquals(expectedError, extractError(response.data()));
    }

    private void sendAndAssertErrorResponse(
        ApiKeys apiKey,
        Node destination,
        Errors error
    ) throws ExecutionException, InterruptedException {
        RaftRequest.Outbound request = sendTestRequest(apiKey, destination);
        channel.pollOnce();
        assertResponseCompleted(request, error);
    }

    private ApiMessage buildTestRequest(ApiKeys key) {
        int leaderEpoch = 5;
        int leaderId = 1;
        String clusterId = "clusterId";
        switch (key) {
            case BEGIN_QUORUM_EPOCH:
                return BeginQuorumEpochRequest.singletonRequest(topicPartition, clusterId, leaderEpoch, leaderId);

            case END_QUORUM_EPOCH:
                return EndQuorumEpochRequest.singletonRequest(
                    topicPartition,
                    clusterId,
                    leaderId,
                    leaderEpoch,
                    Collections.singletonList(2)
                );

            case VOTE:
                int lastEpoch = 4;
                return VoteRequest.singletonRequest(topicPartition, clusterId, leaderEpoch, leaderId, lastEpoch, 329);

            case FETCH:
                FetchRequestData request = RaftUtil.singletonFetchRequest(topicPartition, topicId, fetchPartition ->
                    fetchPartition
                        .setCurrentLeaderEpoch(5)
                        .setFetchOffset(333)
                        .setLastFetchedEpoch(5)
                );
                request.setReplicaState(new FetchRequestData.ReplicaState().setReplicaId(1));
                return request;

            case FETCH_SNAPSHOT:
                return RaftUtil.singletonFetchSnapshotRequest(
                    clusterId,
                    ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID),
                    topicPartition,
                    5,
                    new OffsetAndEpoch(323, 4),
                    1024,
                    10
                );

            case UPDATE_RAFT_VOTER:
                return RaftUtil.updateVoterRequest(
                    clusterId,
                    ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID),
                    5,
                    new SupportedVersionRange((short) 1, (short) 1),
                    Endpoints.empty()
                );

            default:
                throw new AssertionError("Unexpected api " + key);
        }
    }

    private ApiMessage buildTestErrorResponse(ApiKeys key, Errors error) {
        switch (key) {
            case BEGIN_QUORUM_EPOCH:
                return new BeginQuorumEpochResponseData().setErrorCode(error.code());
            case END_QUORUM_EPOCH:
                return new EndQuorumEpochResponseData().setErrorCode(error.code());
            case VOTE:
                return new VoteResponseData()
                    .setErrorCode(error.code())
                    .setTopics(
                        Collections.singletonList(
                            new VoteResponseData.TopicData()
                                .setTopicName(topicPartition.topic())
                                .setPartitions(
                                    Collections.singletonList(
                                        new VoteResponseData.PartitionData()
                                            .setErrorCode(Errors.NONE.code())
                                            .setLeaderId(1)
                                            .setLeaderEpoch(5)
                                            .setVoteGranted(false)
                                    )
                                )
                        )
                    );
            case FETCH:
                return new FetchResponseData().setErrorCode(error.code());
            case FETCH_SNAPSHOT:
                return new FetchSnapshotResponseData().setErrorCode(error.code());
            case UPDATE_RAFT_VOTER:
                return new UpdateRaftVoterResponseData().setErrorCode(error.code());
            default:
                throw new AssertionError("Unexpected api " + key);
        }
    }

    private Errors extractError(ApiMessage response) {
        short code;
        if (response instanceof BeginQuorumEpochResponseData) {
            code = ((BeginQuorumEpochResponseData) response).errorCode();
        } else if (response instanceof EndQuorumEpochResponseData) {
            code = ((EndQuorumEpochResponseData) response).errorCode();
        } else if (response instanceof FetchResponseData) {
            code = ((FetchResponseData) response).errorCode();
        } else if (response instanceof VoteResponseData) {
            code = ((VoteResponseData) response).errorCode();
        } else if (response instanceof FetchSnapshotResponseData) {
            code = ((FetchSnapshotResponseData) response).errorCode();
        } else if (response instanceof UpdateRaftVoterResponseData) {
            code = ((UpdateRaftVoterResponseData) response).errorCode();
        } else {
            throw new IllegalArgumentException("Unexpected type for responseData: " + response);
        }

        return Errors.forCode(code);
    }

    private AbstractResponse buildResponse(ApiMessage responseData) {
        if (responseData instanceof VoteResponseData) {
            return new VoteResponse((VoteResponseData) responseData);
        } else if (responseData instanceof BeginQuorumEpochResponseData) {
            return new BeginQuorumEpochResponse((BeginQuorumEpochResponseData) responseData);
        } else if (responseData instanceof EndQuorumEpochResponseData) {
            return new EndQuorumEpochResponse((EndQuorumEpochResponseData) responseData);
        } else if (responseData instanceof FetchResponseData) {
            return new FetchResponse((FetchResponseData) responseData);
        } else if (responseData instanceof FetchSnapshotResponseData) {
            return new FetchSnapshotResponse((FetchSnapshotResponseData) responseData);
        } else if (responseData instanceof UpdateRaftVoterResponseData) {
            return new UpdateRaftVoterResponse((UpdateRaftVoterResponseData) responseData);
        } else {
            throw new IllegalArgumentException("Unexpected type for responseData: " + responseData);
        }
    }
}
