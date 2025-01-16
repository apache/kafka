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

package org.apache.kafka.server.share.persister;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateRequest;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.WriteShareGroupStateRequest;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class PersisterStateManagerTest {
    private static final KafkaClient CLIENT = mock(KafkaClient.class);
    private static final Time MOCK_TIME = new MockTime();
    private static final Timer MOCK_TIMER = new MockTimer((MockTime) MOCK_TIME);
    private static final ShareCoordinatorMetadataCacheHelper CACHE_HELPER = mock(ShareCoordinatorMetadataCacheHelper.class);
    private static final int MAX_RPC_RETRY_ATTEMPTS = 5;
    public static final long REQUEST_BACKOFF_MS = 100L;
    public static final long REQUEST_BACKOFF_MAX_MS = 3000L;

    private static final String HOST = "localhost";
    private static final int PORT = 9092;

    private static class PersisterStateManagerBuilder {

        private KafkaClient client = CLIENT;
        private Time time = MOCK_TIME;
        private Timer timer = MOCK_TIMER;
        private ShareCoordinatorMetadataCacheHelper cacheHelper = CACHE_HELPER;

        private PersisterStateManagerBuilder withKafkaClient(KafkaClient client) {
            this.client = client;
            return this;
        }

        private PersisterStateManagerBuilder withCacheHelper(ShareCoordinatorMetadataCacheHelper cacheHelper) {
            this.cacheHelper = cacheHelper;
            return this;
        }

        private PersisterStateManagerBuilder withTime(Time time) {
            this.time = time;
            return this;
        }

        private PersisterStateManagerBuilder withTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        public static PersisterStateManagerBuilder builder() {
            return new PersisterStateManagerBuilder();
        }

        public PersisterStateManager build() {
            return new PersisterStateManager(client, cacheHelper, time, timer);
        }
    }

    private abstract class TestStateHandler extends PersisterStateManager.PersisterStateManagerHandler {
        private final CompletableFuture<TestHandlerResponse> result;

        private class TestHandlerResponseData extends WriteShareGroupStateResponseData {
        }

        private class TestHandlerResponse extends WriteShareGroupStateResponse {
            public TestHandlerResponse(WriteShareGroupStateResponseData data) {
                super(data);
            }
        }

        TestStateHandler(
            PersisterStateManager stateManager,
            String groupId,
            Uuid topicId,
            int partition,
            CompletableFuture<TestHandlerResponse> result,
            long backoffMs,
            long backoffMaxMs,
            int maxFindCoordAttempts) {
            stateManager.super(groupId, topicId, partition, backoffMs, backoffMaxMs, maxFindCoordAttempts);
            this.result = result;
        }

        @Override
        protected void handleRequestResponse(ClientResponse response) {
            this.result.complete(new TestHandlerResponse(new TestHandlerResponseData()
                .setResults(Collections.singletonList(new WriteShareGroupStateResponseData.WriteStateResult()
                    .setPartitions(Collections.singletonList(new WriteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partitionKey().partition())
                        .setErrorMessage(Errors.NONE.message())
                        .setErrorCode(Errors.NONE.code()))
                    )
                ))
            ));
        }

        @Override
        protected boolean isResponseForRequest(ClientResponse response) {
            return true;
        }

        @Override
        protected void findCoordinatorErrorResponse(Errors error, Exception exception) {
            this.result.complete(new TestHandlerResponse(new TestHandlerResponseData()
                .setResults(Collections.singletonList(new WriteShareGroupStateResponseData.WriteStateResult()
                    .setTopicId(partitionKey().topicId())
                    .setPartitions(Collections.singletonList(new WriteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partitionKey().partition())
                        .setErrorMessage(exception == null ? error.message() : exception.getMessage())
                        .setErrorCode(error.code()))
                    )
                ))
            ));
        }

        @Override
        protected String name() {
            return "TestStateHandler";
        }

        @Override
        protected boolean isBatchable() {
            return false;
        }

        @Override
        protected PersisterStateManager.RPCType rpcType() {
            return PersisterStateManager.RPCType.UNKNOWN;
        }

        @Override
        protected CompletableFuture<TestHandlerResponse> result() {
            return this.result;
        }
    }

    private ShareCoordinatorMetadataCacheHelper getDefaultCacheHelper(Node suppliedNode) {
        return new ShareCoordinatorMetadataCacheHelper() {
            @Override
            public boolean containsTopic(String topic) {
                return false;
            }

            @Override
            public Node getShareCoordinator(SharePartitionKey key, String internalTopicName) {
                return Node.noNode();
            }

            @Override
            public List<Node> getClusterNodes() {
                return Collections.singletonList(suppliedNode);
            }
        };
    }

    private ShareCoordinatorMetadataCacheHelper getCoordinatorCacheHelper(Node coordinatorNode) {
        return new ShareCoordinatorMetadataCacheHelper() {
            @Override
            public boolean containsTopic(String topic) {
                return true;
            }

            @Override
            public Node getShareCoordinator(SharePartitionKey key, String internalTopicName) {
                return coordinatorNode;
            }

            @Override
            public List<Node> getClusterNodes() {
                return Collections.emptyList();
            }
        };
    }

    private static Timer mockTimer;

    @BeforeEach
    public void setUp() {
        mockTimer = new SystemTimerReaper("persisterStateManagerTestTimer",
            new SystemTimer("persisterStateManagerTestTimer"));
    }

    @AfterEach
    public void tearDown() throws Exception {
        Utils.closeQuietly(mockTimer, "persisterStateManagerTestTimer");
    }

    @Test
    public void testFindCoordinatorFatalError() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;

        Node suppliedNode = new Node(0, HOST, PORT);

        String coordinatorKey = SharePartitionKey.asCoordinatorKey(groupId, topicId, partition);

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setKey(coordinatorKey)
                            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                            .setHost(Node.noNode().host())
                            .setNodeId(Node.noNode().id())
                            .setPort(Node.noNode().port())
                    ))
            ),
            suppliedNode
        );

        ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<TestStateHandler.TestHandlerResponse> future = new CompletableFuture<>();

        TestStateHandler handler = spy(new TestStateHandler(
            stateManager,
            groupId,
            topicId,
            partition,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS
        ) {
            @Override
            protected AbstractRequest.Builder<? extends AbstractRequest> requestBuilder() {
                return null;
            }
        });

        stateManager.enqueue(handler);

        TestStateHandler.TestHandlerResponse result = null;
        try {
            result = handler.result().get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), result.data().results().get(0).partitions().get(0).errorCode());
        verify(handler, times(1)).findShareCoordinatorBuilder();

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testFindCoordinatorAttemptsExhausted() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;

        Node suppliedNode = new Node(0, HOST, PORT);

        String coordinatorKey = SharePartitionKey.asCoordinatorKey(groupId, topicId, partition);

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setKey(coordinatorKey)
                            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                            .setHost(Node.noNode().host())
                            .setNodeId(Node.noNode().id())
                            .setPort(Node.noNode().port())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setKey(coordinatorKey)
                            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                            .setHost(Node.noNode().host())
                            .setNodeId(Node.noNode().id())
                            .setPort(Node.noNode().port())
                    ))
            ),
            suppliedNode
        );

        ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<TestStateHandler.TestHandlerResponse> future = new CompletableFuture<>();

        int maxAttempts = 2;

        TestStateHandler handler = spy(new TestStateHandler(
            stateManager,
            groupId,
            topicId,
            partition,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            maxAttempts
        ) {
            @Override
            protected AbstractRequest.Builder<? extends AbstractRequest> requestBuilder() {
                return null;
            }
        });

        stateManager.enqueue(handler);

        TestStateHandler.TestHandlerResponse result = null;
        try {
            result = handler.result.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code(), result.data().results().get(0).partitions().get(0).errorCode());
        verify(handler, times(2)).findShareCoordinatorBuilder();

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testFindCoordinatorSuccess() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;

        Node suppliedNode = new Node(0, HOST, PORT);
        Node coordinatorNode = new Node(1, HOST, PORT);

        String coordinatorKey = SharePartitionKey.asCoordinatorKey(groupId, topicId, partition);

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setNodeId(1)
                            .setHost(HOST)
                            .setPort(PORT)
                            .setErrorCode(Errors.NONE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> {
            ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new ReadShareGroupStateResponse(
            new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new ReadShareGroupStateResponseData.ReadStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                                .setStateEpoch(1)
                                .setStartOffset(0)
                                .setStateBatches(Collections.emptyList())
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<ReadShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.ReadStateHandler handler = spy(stateManager.new ReadStateHandler(
            groupId,
            topicId,
            partition,
            0,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS,
            null
        ));

        stateManager.enqueue(handler);

        CompletableFuture<ReadShareGroupStateResponse> resultFuture = handler.result();

        ReadShareGroupStateResponse result = null;
        try {
            result = resultFuture.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        verify(handler, times(1)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testWriteStateRequestCoordinatorFoundSuccessfully() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;
        List<PersisterStateBatch> stateBatches = Arrays.asList(
            new PersisterStateBatch(0, 9, (byte) 0, (short) 1),
            new PersisterStateBatch(10, 19, (byte) 1, (short) 1)
        );

        Node suppliedNode = new Node(0, HOST, PORT);
        Node coordinatorNode = new Node(1, HOST, PORT);

        String coordinatorKey = SharePartitionKey.asCoordinatorKey(groupId, topicId, partition);

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setNodeId(1)
                            .setHost(HOST)
                            .setPort(PORT)
                            .setErrorCode(Errors.NONE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> {
            WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new WriteShareGroupStateResponse(
            new WriteShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new WriteShareGroupStateResponseData.WriteStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<WriteShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.WriteStateHandler handler = spy(stateManager.new WriteStateHandler(
            groupId,
            topicId,
            partition,
            0,
            0,
            0,
            stateBatches,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS
        ));

        stateManager.enqueue(handler);

        CompletableFuture<WriteShareGroupStateResponse> resultFuture = handler.result();

        WriteShareGroupStateResponse result = null;
        try {
            result = resultFuture.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        WriteShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(1)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();

        // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned in correct
        assertEquals(partition, partitionResult.partition());
        assertEquals(Errors.NONE.code(), partitionResult.errorCode());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testWriteStateRequestRetryWithNotCoordinatorSuccessfulOnRetry() throws InterruptedException, ExecutionException {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;
        List<PersisterStateBatch> stateBatches = Arrays.asList(
            new PersisterStateBatch(0, 9, (byte) 0, (short) 1),
            new PersisterStateBatch(10, 19, (byte) 1, (short) 1)
        );

        Node suppliedNode = new Node(0, HOST, PORT);
        Node coordinatorNode = new Node(1, HOST, PORT);

        String coordinatorKey = SharePartitionKey.asCoordinatorKey(groupId, topicId, partition);

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setErrorCode(Errors.NOT_COORDINATOR.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setNodeId(1)
                            .setHost(HOST)
                            .setPort(PORT)
                            .setErrorCode(Errors.NONE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> {
            WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new WriteShareGroupStateResponse(
            new WriteShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new WriteShareGroupStateResponseData.WriteStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<WriteShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.WriteStateHandler handler = spy(stateManager.new WriteStateHandler(
            groupId,
            topicId,
            partition,
            0,
            0,
            0,
            stateBatches,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS
        ));

        stateManager.enqueue(handler);

        CompletableFuture<WriteShareGroupStateResponse> resultFuture = handler.result();

        TestUtils.waitForCondition(resultFuture::isDone, TestUtils.DEFAULT_MAX_WAIT_MS, 10L, () -> "Failed to get result from future");

        WriteShareGroupStateResponse result = resultFuture.get();
        WriteShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(2)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();

        // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned is correct
        assertEquals(partition, partitionResult.partition());
        assertEquals(Errors.NONE.code(), partitionResult.errorCode());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testWriteStateRequestCoordinatorFoundOnRetry() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;
        List<PersisterStateBatch> stateBatches = Arrays.asList(
            new PersisterStateBatch(0, 9, (byte) 0, (short) 1),
            new PersisterStateBatch(10, 19, (byte) 1, (short) 1)
        );

        Node suppliedNode = new Node(0, HOST, PORT);
        Node coordinatorNode = new Node(1, HOST, PORT);

        String coordinatorKey = SharePartitionKey.asCoordinatorKey(groupId, topicId, partition);

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setNodeId(1)
                            .setHost(HOST)
                            .setPort(PORT)
                            .setErrorCode(Errors.NONE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> {
            WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new WriteShareGroupStateResponse(
            new WriteShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new WriteShareGroupStateResponseData.WriteStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<WriteShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.WriteStateHandler handler = spy(stateManager.new WriteStateHandler(
            groupId,
            topicId,
            partition,
            0,
            0,
            0,
            stateBatches,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS
        ));

        stateManager.enqueue(handler);

        CompletableFuture<WriteShareGroupStateResponse> resultFuture = handler.result();

        WriteShareGroupStateResponse result = null;
        try {
            result = resultFuture.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        WriteShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(2)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();

        // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned in correct
        assertEquals(partition, partitionResult.partition());
        assertEquals(Errors.NONE.code(), partitionResult.errorCode());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testWriteStateRequestWithCoordinatorNodeLookup() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;
        List<PersisterStateBatch> stateBatches = Arrays.asList(
            new PersisterStateBatch(0, 9, (byte) 0, (short) 1),
            new PersisterStateBatch(10, 19, (byte) 1, (short) 1)
        );

        Node coordinatorNode = new Node(1, HOST, PORT);

        client.prepareResponseFrom(body -> {
            WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new WriteShareGroupStateResponse(
            new WriteShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new WriteShareGroupStateResponseData.WriteStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getCoordinatorCacheHelper(coordinatorNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<WriteShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.WriteStateHandler handler = spy(stateManager.new WriteStateHandler(
            groupId,
            topicId,
            partition,
            0,
            0,
            0,
            stateBatches,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS
        ));

        stateManager.enqueue(handler);

        CompletableFuture<WriteShareGroupStateResponse> resultFuture = handler.result();

        WriteShareGroupStateResponse result = null;
        try {
            result = resultFuture.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        WriteShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(0)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();
        verify(handler, times(1)).onComplete(any());

        // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned in correct
        assertEquals(partition, partitionResult.partition());
        assertEquals(Errors.NONE.code(), partitionResult.errorCode());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testWriteStateRequestWithRetryAndCoordinatorNodeLookup() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;
        List<PersisterStateBatch> stateBatches = Arrays.asList(
            new PersisterStateBatch(0, 9, (byte) 0, (short) 1),
            new PersisterStateBatch(10, 19, (byte) 1, (short) 1)
        );

        Node coordinatorNode = new Node(1, HOST, PORT);

        client.prepareResponseFrom(body -> {
            WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new WriteShareGroupStateResponse(
            new WriteShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new WriteShareGroupStateResponseData.WriteStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                                .setErrorMessage("")
                        ))
                ))
        ), coordinatorNode);

        client.prepareResponseFrom(body -> {
            WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new WriteShareGroupStateResponse(
            new WriteShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new WriteShareGroupStateResponseData.WriteStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getCoordinatorCacheHelper(coordinatorNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<WriteShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.WriteStateHandler handler = spy(stateManager.new WriteStateHandler(
            groupId,
            topicId,
            partition,
            0,
            0,
            0,
            stateBatches,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS
        ));

        stateManager.enqueue(handler);

        CompletableFuture<WriteShareGroupStateResponse> resultFuture = handler.result();

        WriteShareGroupStateResponse result = null;
        try {
            result = resultFuture.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        WriteShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(0)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();
        verify(handler, times(2)).onComplete(any());

        // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned in correct
        assertEquals(partition, partitionResult.partition());
        assertEquals(Errors.NONE.code(), partitionResult.errorCode());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testWriteStateRequestFailedMaxRetriesExhausted() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;
        List<PersisterStateBatch> stateBatches = Arrays.asList(
            new PersisterStateBatch(0, 9, (byte) 0, (short) 1),
            new PersisterStateBatch(10, 19, (byte) 1, (short) 1)
        );

        Node coordinatorNode = new Node(1, HOST, PORT);

        client.prepareResponseFrom(body -> {
            WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new WriteShareGroupStateResponse(
            new WriteShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new WriteShareGroupStateResponseData.WriteStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                                .setErrorMessage("")
                        ))
                ))
        ), coordinatorNode);

        client.prepareResponseFrom(body -> {
            WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new WriteShareGroupStateResponse(
            new WriteShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new WriteShareGroupStateResponseData.WriteStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                                .setErrorMessage("")
                        ))
                ))
        ), coordinatorNode);

        client.prepareResponseFrom(body -> {
            WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new WriteShareGroupStateResponse(
            new WriteShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new WriteShareGroupStateResponseData.WriteStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getCoordinatorCacheHelper(coordinatorNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<WriteShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.WriteStateHandler handler = spy(stateManager.new WriteStateHandler(
            groupId,
            topicId,
            partition,
            0,
            0,
            0,
            stateBatches,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            2
        ));

        stateManager.enqueue(handler);

        CompletableFuture<WriteShareGroupStateResponse> resultFuture = handler.result();

        WriteShareGroupStateResponse result = null;
        try {
            result = resultFuture.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        WriteShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(0)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();
        verify(handler, times(2)).onComplete(any());

        // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned in correct
        assertEquals(partition, partitionResult.partition());
        assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS.code(), partitionResult.errorCode());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testWriteStateRequestBatchingWithCoordinatorNodeLookup() throws ExecutionException, Exception {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;
        List<PersisterStateBatch> stateBatches = Arrays.asList(
            new PersisterStateBatch(0, 9, (byte) 0, (short) 1),
            new PersisterStateBatch(10, 19, (byte) 1, (short) 1)
        );

        Node coordinatorNode = new Node(1, HOST, PORT);

        client.prepareResponseFrom(body -> {
            WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new WriteShareGroupStateResponse(
            new WriteShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new WriteShareGroupStateResponseData.WriteStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getCoordinatorCacheHelper(coordinatorNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        AtomicBoolean isBatchingSuccess = new AtomicBoolean(false);
        stateManager.setGenerateCallback(() -> {
            Map<PersisterStateManager.RPCType, Map<String, List<PersisterStateManager.PersisterStateManagerHandler>>> handlersPerType = stateManager.nodeRPCMap().get(coordinatorNode);
            if (handlersPerType != null && handlersPerType.containsKey(PersisterStateManager.RPCType.WRITE) && handlersPerType.get(PersisterStateManager.RPCType.WRITE).containsKey(groupId)) {
                if (handlersPerType.get(PersisterStateManager.RPCType.WRITE).get(groupId).size() > 2)
                    isBatchingSuccess.set(true);
            }
        });

        stateManager.start();

        CompletableFuture<WriteShareGroupStateResponse> future = new CompletableFuture<>();

        List<PersisterStateManager.WriteStateHandler> handlers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            PersisterStateManager.WriteStateHandler handler = spy(stateManager.new WriteStateHandler(
                groupId,
                topicId,
                partition,
                0,
                0,
                0,
                stateBatches,
                future,
                REQUEST_BACKOFF_MS,
                REQUEST_BACKOFF_MAX_MS,
                MAX_RPC_RETRY_ATTEMPTS
            ));
            handlers.add(handler);
            stateManager.enqueue(handler);
        }

        CompletableFuture.allOf(handlers.stream()
            .map(PersisterStateManager.WriteStateHandler::result).toArray(CompletableFuture[]::new)).get();

        TestUtils.waitForCondition(isBatchingSuccess::get, TestUtils.DEFAULT_MAX_WAIT_MS, 10L, () -> "unable to verify batching");
    }

    @Test
    public void testReadStateRequestCoordinatorFoundSuccessfully() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;

        Node suppliedNode = new Node(0, HOST, PORT);
        Node coordinatorNode = new Node(1, HOST, PORT);

        String coordinatorKey = SharePartitionKey.asCoordinatorKey(groupId, topicId, partition);

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setNodeId(1)
                            .setHost(HOST)
                            .setPort(PORT)
                            .setErrorCode(Errors.NONE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> {
            ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new ReadShareGroupStateResponse(
            new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new ReadShareGroupStateResponseData.ReadStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                                .setStateEpoch(1)
                                .setStartOffset(0)
                                .setStateBatches(Collections.emptyList())
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<ReadShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.ReadStateHandler handler = spy(stateManager.new ReadStateHandler(
            groupId,
            topicId,
            partition,
            0,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS,
            null
        ));

        stateManager.enqueue(handler);

        CompletableFuture<ReadShareGroupStateResponse> resultFuture = handler.result();

        ReadShareGroupStateResponse result = null;
        try {
            result = resultFuture.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        ReadShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(1)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();

        // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned in correct
        assertEquals(partition, partitionResult.partition());
        assertEquals(Errors.NONE.code(), partitionResult.errorCode());
        assertEquals(1, partitionResult.stateEpoch());
        assertEquals(0, partitionResult.startOffset());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testReadStateRequestIllegalStateCoordinatorFoundSuccessfully() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;

        Node suppliedNode = new Node(0, HOST, PORT);
        Node coordinatorNode = new Node(1, HOST, PORT);

        String coordinatorKey = SharePartitionKey.asCoordinatorKey(groupId, topicId, partition);

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setNodeId(1)
                            .setHost(HOST)
                            .setPort(PORT)
                            .setErrorCode(Errors.NONE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> {
            ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new ReadShareGroupStateResponse(
            new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new ReadShareGroupStateResponseData.ReadStateResult()
                        .setTopicId(Uuid.randomUuid())
                        .setPartitions(Collections.singletonList(
                            new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(500)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                                .setStateEpoch(1)
                                .setStartOffset(0)
                                .setStateBatches(Collections.emptyList())
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<ReadShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.ReadStateHandler handler = spy(stateManager.new ReadStateHandler(
            groupId,
            topicId,
            partition,
            0,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS,
            null
        ));

        stateManager.enqueue(handler);

        CompletableFuture<ReadShareGroupStateResponse> resultFuture = handler.result();

        ReadShareGroupStateResponse result = null;
        try {
            result = resultFuture.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        ReadShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(1)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();

        // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned in correct
        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), partitionResult.errorCode());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testReadStateRequestRetryWithNotCoordinatorSuccessfulOnRetry() throws ExecutionException, InterruptedException {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;

        Node suppliedNode = new Node(0, HOST, PORT);
        Node coordinatorNode = new Node(1, HOST, PORT);

        String coordinatorKey = SharePartitionKey.asCoordinatorKey(groupId, topicId, partition);

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setErrorCode(Errors.NOT_COORDINATOR.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setNodeId(1)
                            .setHost(HOST)
                            .setPort(PORT)
                            .setErrorCode(Errors.NONE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> {
            ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new ReadShareGroupStateResponse(
            new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new ReadShareGroupStateResponseData.ReadStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                                .setStateEpoch(1)
                                .setStartOffset(0)
                                .setStateBatches(Collections.emptyList())
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<ReadShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.ReadStateHandler handler = spy(stateManager.new ReadStateHandler(
            groupId,
            topicId,
            partition,
            0,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS,
            null
        ));

        stateManager.enqueue(handler);

        CompletableFuture<ReadShareGroupStateResponse> resultFuture = handler.result();

        TestUtils.waitForCondition(resultFuture::isDone, TestUtils.DEFAULT_MAX_WAIT_MS, 10L, () -> "Failed to get result from future");

        ReadShareGroupStateResponse result = resultFuture.get();
        ReadShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(2)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();

        // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned in correct
        assertEquals(partition, partitionResult.partition());
        assertEquals(Errors.NONE.code(), partitionResult.errorCode());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testReadStateRequestCoordinatorFoundOnRetry() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;

        Node suppliedNode = new Node(0, HOST, PORT);
        Node coordinatorNode = new Node(1, HOST, PORT);

        String coordinatorKey = SharePartitionKey.asCoordinatorKey(groupId, topicId, partition);

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setErrorCode(Errors.NOT_COORDINATOR.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setNodeId(1)
                            .setHost(HOST)
                            .setPort(PORT)
                            .setErrorCode(Errors.NONE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> {
            ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new ReadShareGroupStateResponse(
            new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new ReadShareGroupStateResponseData.ReadStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                                .setStateEpoch(1)
                                .setStartOffset(0)
                                .setStateBatches(Collections.emptyList())
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<ReadShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.ReadStateHandler handler = spy(stateManager.new ReadStateHandler(
            groupId,
            topicId,
            partition,
            0,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS,
            null
        ));

        stateManager.enqueue(handler);

        CompletableFuture<ReadShareGroupStateResponse> resultFuture = handler.result();

        ReadShareGroupStateResponse result = null;
        try {
            result = resultFuture.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        ReadShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(2)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();

        // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned in correct
        assertEquals(partition, partitionResult.partition());
        assertEquals(Errors.NONE.code(), partitionResult.errorCode());
        assertEquals(1, partitionResult.stateEpoch());
        assertEquals(0, partitionResult.startOffset());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testReadStateRequestWithCoordinatorNodeLookup() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;

        Node coordinatorNode = new Node(1, HOST, PORT);

        client.prepareResponseFrom(body -> {
            ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new ReadShareGroupStateResponse(
            new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new ReadShareGroupStateResponseData.ReadStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                                .setStateEpoch(1)
                                .setStartOffset(0)
                                .setStateBatches(Collections.emptyList())
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getCoordinatorCacheHelper(coordinatorNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<ReadShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.ReadStateHandler handler = spy(stateManager.new ReadStateHandler(
            groupId,
            topicId,
            partition,
            0,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS,
            null
        ));

        stateManager.enqueue(handler);

        CompletableFuture<ReadShareGroupStateResponse> resultFuture = handler.result();

        ReadShareGroupStateResponse result = null;
        try {
            result = resultFuture.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        ReadShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(0)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();
        verify(handler, times(1)).onComplete(any());

        // Verifying the coordinator node was populated correctly by the constructor
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned in correct
        assertEquals(partition, partitionResult.partition());
        assertEquals(Errors.NONE.code(), partitionResult.errorCode());
        assertEquals(1, partitionResult.stateEpoch());
        assertEquals(0, partitionResult.startOffset());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testReadStateRequestRetryWithCoordinatorNodeLookup() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;

        Node coordinatorNode = new Node(1, HOST, PORT);

        client.prepareResponseFrom(body -> {
            ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new ReadShareGroupStateResponse(
            new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new ReadShareGroupStateResponseData.ReadStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                                .setErrorMessage("")
                                .setStateEpoch(1)
                                .setStartOffset(0)
                                .setStateBatches(Collections.emptyList())
                        ))
                ))
        ), coordinatorNode);

        client.prepareResponseFrom(body -> {
            ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new ReadShareGroupStateResponse(
            new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new ReadShareGroupStateResponseData.ReadStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.NONE.code())
                                .setErrorMessage("")
                                .setStateEpoch(1)
                                .setStartOffset(0)
                                .setStateBatches(Collections.emptyList())
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getCoordinatorCacheHelper(coordinatorNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<ReadShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.ReadStateHandler handler = spy(stateManager.new ReadStateHandler(
            groupId,
            topicId,
            partition,
            0,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            MAX_RPC_RETRY_ATTEMPTS,
            null
        ));

        stateManager.enqueue(handler);

        CompletableFuture<ReadShareGroupStateResponse> resultFuture = handler.result();

        ReadShareGroupStateResponse result = null;
        try {
            result = resultFuture.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        ReadShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(0)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();
        verify(handler, times(2)).onComplete(any());

        // Verifying the coordinator node was populated correctly by the constructor
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned in correct
        assertEquals(partition, partitionResult.partition());
        assertEquals(Errors.NONE.code(), partitionResult.errorCode());
        assertEquals(1, partitionResult.stateEpoch());
        assertEquals(0, partitionResult.startOffset());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testReadStateRequestFailureMaxRetriesExhausted() {
        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 10;

        Node coordinatorNode = new Node(1, HOST, PORT);

        client.prepareResponseFrom(body -> {
            ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new ReadShareGroupStateResponse(
            new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new ReadShareGroupStateResponseData.ReadStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                                .setErrorMessage("")
                                .setStateEpoch(1)
                                .setStartOffset(0)
                                .setStateBatches(Collections.emptyList())
                        ))
                ))
        ), coordinatorNode);

        client.prepareResponseFrom(body -> {
            ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
            String requestGroupId = request.data().groupId();
            Uuid requestTopicId = request.data().topics().get(0).topicId();
            int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

            return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
        }, new ReadShareGroupStateResponse(
            new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                    new ReadShareGroupStateResponseData.ReadStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(
                            new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(partition)
                                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                                .setErrorMessage("")
                                .setStateEpoch(1)
                                .setStartOffset(0)
                                .setStateBatches(Collections.emptyList())
                        ))
                ))
        ), coordinatorNode);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getCoordinatorCacheHelper(coordinatorNode);

        PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
            .withKafkaClient(client)
            .withTimer(mockTimer)
            .withCacheHelper(cacheHelper)
            .build();

        stateManager.start();

        CompletableFuture<ReadShareGroupStateResponse> future = new CompletableFuture<>();

        PersisterStateManager.ReadStateHandler handler = spy(stateManager.new ReadStateHandler(
            groupId,
            topicId,
            partition,
            0,
            future,
            REQUEST_BACKOFF_MS,
            REQUEST_BACKOFF_MAX_MS,
            2,
            null
        ));

        stateManager.enqueue(handler);

        CompletableFuture<ReadShareGroupStateResponse> resultFuture = handler.result();

        ReadShareGroupStateResponse result = null;
        try {
            result = resultFuture.get();
        } catch (Exception e) {
            fail("Failed to get result from future", e);
        }

        ReadShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

        verify(handler, times(0)).findShareCoordinatorBuilder();
        verify(handler, times(0)).requestBuilder();
        verify(handler, times(2)).onComplete(any());

        // Verifying the coordinator node was populated correctly by the constructor
        assertEquals(coordinatorNode, handler.getCoordinatorNode());

        // Verifying the result returned in correct
        assertEquals(partition, partitionResult.partition());
        assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS.code(), partitionResult.errorCode());

        try {
            // Stopping the state manager
            stateManager.stop();
        } catch (Exception e) {
            fail("Failed to stop state manager", e);
        }
    }

    @Test
    public void testPersisterStateManagerClose() {
        KafkaClient client = mock(KafkaClient.class);
        Timer timer = mock(Timer.class);
        PersisterStateManager psm = PersisterStateManagerBuilder
            .builder()
            .withTimer(timer)
            .withKafkaClient(client)
            .build();

        try {
            verify(client, times(0)).close();
            verify(timer, times(0)).close();

            psm.start();
            psm.stop();

            verify(client, times(1)).close();
            verify(timer, times(1)).close();
        } catch (Exception e) {
            fail("unexpected exception", e);
        }
    }
}
