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

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateRequest;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.WriteShareGroupStateRequest;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.server.util.timer.Timer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class DefaultStatePersisterTest {
    private static final KafkaClient CLIENT = mock(KafkaClient.class);
    private static final Time MOCK_TIME = new MockTime();
    private static final Timer MOCK_TIMER = new MockTimer();
    private static final ShareCoordinatorMetadataCacheHelper CACHE_HELPER = mock(ShareCoordinatorMetadataCacheHelper.class);

    private static final String HOST = "localhost";
    private static final int PORT = 9092;

    private static class DefaultStatePersisterBuilder {

        private KafkaClient client = CLIENT;
        private Time time = MOCK_TIME;
        private Timer timer = MOCK_TIMER;
        private ShareCoordinatorMetadataCacheHelper cacheHelper = CACHE_HELPER;

        private DefaultStatePersisterBuilder withKafkaClient(KafkaClient client) {
            this.client = client;
            return this;
        }

        private DefaultStatePersisterBuilder withCacheHelper(ShareCoordinatorMetadataCacheHelper cacheHelper) {
            this.cacheHelper = cacheHelper;
            return this;
        }

        private DefaultStatePersisterBuilder withTime(Time time) {
            this.time = time;
            return this;
        }

        private DefaultStatePersisterBuilder withTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        public static DefaultStatePersisterBuilder builder() {
            return new DefaultStatePersisterBuilder();
        }

        public DefaultStatePersister build() {
            PersisterStateManager persisterStateManager = new PersisterStateManager(client, cacheHelper, time, timer);
            return new DefaultStatePersister(persisterStateManager);
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

    @Test
    public void testWriteStateValidate() {

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        int incorrectPartition = -1;

        // Request Parameters are null
        DefaultStatePersister defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        CompletableFuture<WriteShareGroupStateResult> result = defaultStatePersister.writeState(null);
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);

        // groupTopicPartitionData is null
        defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        result = defaultStatePersister.writeState(new WriteShareGroupStateParameters.Builder().setGroupTopicPartitionData(null).build());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);

        // groupId is null
        defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        result = defaultStatePersister.writeState(new WriteShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionStateBatchData>()
                .setGroupId(null).build()).build());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);

        // topicsData is empty
        defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        result = defaultStatePersister.writeState(new WriteShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionStateBatchData>()
                .setGroupId(groupId)
                .setTopicsData(Collections.emptyList()).build()).build());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);

        // topicId is null
        defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        result = defaultStatePersister.writeState(new WriteShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionStateBatchData>()
                .setGroupId(groupId)
                .setTopicsData(Collections.singletonList(new TopicData<>(null,
                        Collections.singletonList(PartitionFactory.newPartitionStateBatchData(
                            partition, 1, 0, 0, null))))).build()).build());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);

        // partitionData is empty
        defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        result = defaultStatePersister.writeState(new WriteShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionStateBatchData>()
                .setGroupId(groupId)
                .setTopicsData(Collections.singletonList(new TopicData<>(topicId, Collections.emptyList()))).build()).build());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);

        // partition value is incorrect
        defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        result = defaultStatePersister.writeState(new WriteShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionStateBatchData>()
                .setGroupId(groupId)
                .setTopicsData(Collections.singletonList(new TopicData<>(topicId,
                        Collections.singletonList(PartitionFactory.newPartitionStateBatchData(
                            incorrectPartition, 1, 0, 0, null))))).build()).build());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);
    }

    @Test
    public void testReadStateValidate() {

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;
        int incorrectPartition = -1;

        // Request Parameters are null
        DefaultStatePersister defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        CompletableFuture<ReadShareGroupStateResult> result = defaultStatePersister.readState(null);
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);

        // groupTopicPartitionData is null
        defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        result = defaultStatePersister.readState(new ReadShareGroupStateParameters.Builder().setGroupTopicPartitionData(null).build());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);

        // groupId is null
        defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        result = defaultStatePersister.readState(new ReadShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdLeaderEpochData>()
                .setGroupId(null).build()).build());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);

        // topicsData is empty
        defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        result = defaultStatePersister.readState(new ReadShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdLeaderEpochData>()
                .setGroupId(groupId)
                .setTopicsData(Collections.emptyList()).build()).build());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);

        // topicId is null
        defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        result = defaultStatePersister.readState(new ReadShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdLeaderEpochData>()
                .setGroupId(groupId)
                .setTopicsData(Collections.singletonList(new TopicData<>(null,
                        Collections.singletonList(PartitionFactory.newPartitionIdLeaderEpochData(partition, 1))))
                ).build()).build());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);

        // partitionData is empty
        defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        result = defaultStatePersister.readState(new ReadShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdLeaderEpochData>()
                .setGroupId(groupId)
                .setTopicsData(Collections.singletonList(new TopicData<>(topicId, Collections.emptyList()))).build()).build());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);

        // partition value is incorrect
        defaultStatePersister = DefaultStatePersisterBuilder.builder().build();
        result = defaultStatePersister.readState(new ReadShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdLeaderEpochData>()
                .setGroupId(groupId)
                .setTopicsData(Collections.singletonList(new TopicData<>(topicId,
                        Collections.singletonList(PartitionFactory.newPartitionIdLeaderEpochData(incorrectPartition, 1))))).build()).build());
        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());
        assertFutureThrows(result, IllegalArgumentException.class);
    }

    @Test
    public void testWriteStateSuccess() {

        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId1 = Uuid.randomUuid();
        int partition1 = 10;

        Uuid topicId2 = Uuid.randomUuid();
        int partition2 = 8;

        Node suppliedNode = new Node(0, HOST, PORT);
        Node coordinatorNode1 = new Node(5, HOST, PORT);
        Node coordinatorNode2 = new Node(6, HOST, PORT);

        String coordinatorKey1 = SharePartitionKey.asCoordinatorKey(groupId, topicId1, partition1);
        String coordinatorKey2 = SharePartitionKey.asCoordinatorKey(groupId, topicId2, partition2);

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey1),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setNodeId(5)
                            .setHost(HOST)
                            .setPort(PORT)
                            .setErrorCode(Errors.NONE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey2),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setNodeId(6)
                            .setHost(HOST)
                            .setPort(PORT)
                            .setErrorCode(Errors.NONE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(
            body -> {
                WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
                String requestGroupId = request.data().groupId();
                Uuid requestTopicId = request.data().topics().get(0).topicId();
                int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

                return requestGroupId.equals(groupId) && requestTopicId == topicId1 && requestPartition == partition1;
            },
            new WriteShareGroupStateResponse(WriteShareGroupStateResponse.toResponseData(topicId1, partition1)),
            coordinatorNode1);

        client.prepareResponseFrom(
            body -> {
                WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
                String requestGroupId = request.data().groupId();
                Uuid requestTopicId = request.data().topics().get(0).topicId();
                int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

                return requestGroupId.equals(groupId) && requestTopicId == topicId2 && requestPartition == partition2;
            },
            new WriteShareGroupStateResponse(WriteShareGroupStateResponse.toResponseData(topicId2, partition2)),
            coordinatorNode2);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

        DefaultStatePersister defaultStatePersister = DefaultStatePersisterBuilder.builder()
            .withKafkaClient(client)
            .withCacheHelper(cacheHelper)
            .build();

        WriteShareGroupStateParameters request = WriteShareGroupStateParameters.from(
            new WriteShareGroupStateRequestData()
                .setGroupId(groupId)
                .setTopics(Arrays.asList(
                    new WriteShareGroupStateRequestData.WriteStateData()
                        .setTopicId(topicId1)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition1)
                                .setStateEpoch(0)
                                .setLeaderEpoch(1)
                                .setStartOffset(0)
                                .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                                    .setFirstOffset(0)
                                    .setLastOffset(10)
                                    .setDeliveryCount((short) 1)
                                    .setDeliveryState((byte) 0)))
                        )),
                    new WriteShareGroupStateRequestData.WriteStateData()
                        .setTopicId(topicId2)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition2)
                                .setStateEpoch(0)
                                .setLeaderEpoch(1)
                                .setStartOffset(0)
                                .setStateBatches(Arrays.asList(
                                    new WriteShareGroupStateRequestData.StateBatch()
                                        .setFirstOffset(0)
                                        .setLastOffset(10)
                                        .setDeliveryCount((short) 1)
                                        .setDeliveryState((byte) 0),
                                    new WriteShareGroupStateRequestData.StateBatch()
                                        .setFirstOffset(11)
                                        .setLastOffset(20)
                                        .setDeliveryCount((short) 1)
                                        .setDeliveryState((byte) 0)))
                        ))
                ))
        );

        CompletableFuture<WriteShareGroupStateResult> resultFuture = defaultStatePersister.writeState(request);

        WriteShareGroupStateResult result = null;
        try {
            // adding long delay to allow for environment/GC issues
            result = resultFuture.get(10L, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail("Unexpected exception", e);
        }

        HashSet<PartitionData> resultMap = new HashSet<>();
        result.topicsData().forEach(
            topicData -> topicData.partitions().forEach(
                partitionData -> resultMap.add((PartitionData) partitionData)
            )
        );


        HashSet<PartitionData> expectedResultMap = new HashSet<>();
        expectedResultMap.add((PartitionData) PartitionFactory.newPartitionErrorData(partition1, Errors.NONE.code(), null));

        expectedResultMap.add((PartitionData) PartitionFactory.newPartitionErrorData(partition2, Errors.NONE.code(), null));

        assertEquals(2, result.topicsData().size());
        assertEquals(expectedResultMap, resultMap);
    }

    @Test
    public void testReadStateSuccess() {

        MockClient client = new MockClient(MOCK_TIME);

        String groupId = "group1";
        Uuid topicId1 = Uuid.randomUuid();
        int partition1 = 10;

        Uuid topicId2 = Uuid.randomUuid();
        int partition2 = 8;

        Node suppliedNode = new Node(0, HOST, PORT);
        Node coordinatorNode1 = new Node(5, HOST, PORT);
        Node coordinatorNode2 = new Node(6, HOST, PORT);

        String coordinatorKey1 = SharePartitionKey.asCoordinatorKey(groupId, topicId1, partition1);
        String coordinatorKey2 = SharePartitionKey.asCoordinatorKey(groupId, topicId2, partition2);

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey1),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setNodeId(5)
                            .setHost(HOST)
                            .setPort(PORT)
                            .setErrorCode(Errors.NONE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
                && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
                && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey2),
            new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setNodeId(6)
                            .setHost(HOST)
                            .setPort(PORT)
                            .setErrorCode(Errors.NONE.code())
                    ))
            ),
            suppliedNode
        );

        client.prepareResponseFrom(
            body -> {
                ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
                String requestGroupId = request.data().groupId();
                Uuid requestTopicId = request.data().topics().get(0).topicId();
                int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

                return requestGroupId.equals(groupId) && requestTopicId == topicId1 && requestPartition == partition1;
            },
            new ReadShareGroupStateResponse(ReadShareGroupStateResponse.toResponseData(topicId1, partition1, 0, 1,
                Collections.singletonList(new ReadShareGroupStateResponseData.StateBatch()
                    .setFirstOffset(0)
                    .setLastOffset(10)
                    .setDeliveryCount((short) 1)
                    .setDeliveryState((byte) 0)))),
            coordinatorNode1);

        client.prepareResponseFrom(
            body -> {
                ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
                String requestGroupId = request.data().groupId();
                Uuid requestTopicId = request.data().topics().get(0).topicId();
                int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

                return requestGroupId.equals(groupId) && requestTopicId == topicId2 && requestPartition == partition2;
            },
            new ReadShareGroupStateResponse(ReadShareGroupStateResponse.toResponseData(topicId2, partition2, 0, 1,
                Arrays.asList(new ReadShareGroupStateResponseData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0),
                    new ReadShareGroupStateResponseData.StateBatch()
                        .setFirstOffset(11)
                        .setLastOffset(20)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)))),
            coordinatorNode2);

        ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

        DefaultStatePersister defaultStatePersister = DefaultStatePersisterBuilder.builder()
            .withKafkaClient(client)
            .withCacheHelper(cacheHelper)
            .build();

        ReadShareGroupStateParameters request = ReadShareGroupStateParameters.from(
            new ReadShareGroupStateRequestData()
                .setGroupId(groupId)
                .setTopics(Arrays.asList(
                    new ReadShareGroupStateRequestData.ReadStateData()
                        .setTopicId(topicId1)
                        .setPartitions(Collections.singletonList(
                            new ReadShareGroupStateRequestData.PartitionData()
                                .setPartition(partition1)
                                .setLeaderEpoch(1)
                        )),
                    new ReadShareGroupStateRequestData.ReadStateData()
                        .setTopicId(topicId2)
                        .setPartitions(Collections.singletonList(
                            new ReadShareGroupStateRequestData.PartitionData()
                                .setPartition(partition2)
                                .setLeaderEpoch(1)
                        ))
                ))
        );

        CompletableFuture<ReadShareGroupStateResult> resultFuture = defaultStatePersister.readState(request);

        ReadShareGroupStateResult result = null;
        try {
            // adding long delay to allow for environment/GC issues
            result = resultFuture.get(10L, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail("Unexpected exception", e);
        }

        HashSet<PartitionData> resultMap = new HashSet<>();
        result.topicsData().forEach(
            topicData -> topicData.partitions().forEach(
                partitionData -> resultMap.add((PartitionData) partitionData)
            )
        );

        HashSet<PartitionData> expectedResultMap = new HashSet<>();
        expectedResultMap.add(
            (PartitionData) PartitionFactory.newPartitionAllData(partition1, 1, 0, Errors.NONE.code(),
                null, Collections.singletonList(new PersisterStateBatch(0, 10, (byte) 0, (short) 1)
                )));

        expectedResultMap.add(
            (PartitionData) PartitionFactory.newPartitionAllData(partition2, 1, 0, Errors.NONE.code(),
                null, Arrays.asList(
                    new PersisterStateBatch(0, 10, (byte) 0, (short) 1),
                    new PersisterStateBatch(11, 20, (byte) 0, (short) 1)
                )));

        assertEquals(2, result.topicsData().size());
        assertEquals(expectedResultMap, resultMap);
    }

    @Test
    public void testWriteStateResponseToResultPartialResults() {
        Map<Uuid, Map<Integer, CompletableFuture<WriteShareGroupStateResponse>>> futureMap = new HashMap<>();
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), 1, null);
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), 1, null);

        // one entry has valid results
        futureMap.computeIfAbsent(tp1.topicId(), k -> new HashMap<>())
            .put(tp1.partition(), CompletableFuture.completedFuture(
                    new WriteShareGroupStateResponse(
                        WriteShareGroupStateResponse.toResponseData(
                            tp1.topicId(),
                            tp1.partition()
                        )
                    )
                )
            );

        // one entry has error
        futureMap.computeIfAbsent(tp2.topicId(), k -> new HashMap<>())
            .put(tp2.partition(), CompletableFuture.completedFuture(
                    new WriteShareGroupStateResponse(
                        WriteShareGroupStateResponse.toErrorResponseData(
                            tp2.topicId(),
                            tp2.partition(),
                            Errors.UNKNOWN_TOPIC_OR_PARTITION,
                            "unknown tp"
                        )
                    )
                )
            );

        PersisterStateManager psm = mock(PersisterStateManager.class);
        DefaultStatePersister dsp = new DefaultStatePersister(psm);

        WriteShareGroupStateResult results = dsp.writeResponsesToResult(futureMap);

        // results should contain partial results
        assertEquals(2, results.topicsData().size());
        assertTrue(
            results.topicsData().contains(
                new TopicData<>(
                    tp1.topicId(),
                    Collections.singletonList(PartitionFactory.newPartitionErrorData(tp1.partition(), Errors.NONE.code(), null))
                )
            )
        );
        assertTrue(
            results.topicsData().contains(
                new TopicData<>(
                    tp2.topicId(),
                    Collections.singletonList(PartitionFactory.newPartitionErrorData(tp2.partition(), Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), "unknown tp"))
                )
            )
        );
    }

    @Test
    public void testWriteStateResponseToResultFailedFuture() {
        Map<Uuid, Map<Integer, CompletableFuture<WriteShareGroupStateResponse>>> futureMap = new HashMap<>();
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), 1, null);
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), 1, null);

        // one entry has valid results
        futureMap.computeIfAbsent(tp1.topicId(), k -> new HashMap<>())
            .put(tp1.partition(), CompletableFuture.completedFuture(
                    new WriteShareGroupStateResponse(
                        WriteShareGroupStateResponse.toResponseData(
                            tp1.topicId(),
                            tp1.partition()
                        )
                    )
                )
            );

        // one entry has failed future
        futureMap.computeIfAbsent(tp2.topicId(), k -> new HashMap<>())
            .put(tp2.partition(), CompletableFuture.failedFuture(new Exception("scary stuff")));

        PersisterStateManager psm = mock(PersisterStateManager.class);
        DefaultStatePersister dsp = new DefaultStatePersister(psm);

        WriteShareGroupStateResult results = dsp.writeResponsesToResult(futureMap);

        // results should contain partial results
        assertEquals(2, results.topicsData().size());
        assertTrue(
            results.topicsData().contains(
                new TopicData<>(
                    tp1.topicId(),
                    Collections.singletonList(PartitionFactory.newPartitionErrorData(tp1.partition(), Errors.NONE.code(), null))
                )
            )
        );
        assertTrue(
            results.topicsData().contains(
                new TopicData<>(
                    tp2.topicId(),
                    Collections.singletonList(PartitionFactory.newPartitionErrorData(tp2.partition(), Errors.UNKNOWN_SERVER_ERROR.code(), "Error writing state to share coordinator: java.lang.Exception: scary stuff"))
                )
            )
        );
    }

    @Test
    public void testReadStateResponseToResultPartialResults() {
        Map<Uuid, Map<Integer, CompletableFuture<ReadShareGroupStateResponse>>> futureMap = new HashMap<>();
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), 1, null);
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), 1, null);

        // one entry has valid results
        futureMap.computeIfAbsent(tp1.topicId(), k -> new HashMap<>())
            .put(tp1.partition(), CompletableFuture.completedFuture(
                    new ReadShareGroupStateResponse(
                        ReadShareGroupStateResponse.toResponseData(
                            tp1.topicId(),
                            tp1.partition(),
                            1L,
                            2,
                            Collections.emptyList()
                        )
                    )
                )
            );

        // one entry has error
        futureMap.computeIfAbsent(tp2.topicId(), k -> new HashMap<>())
            .put(tp2.partition(), CompletableFuture.completedFuture(
                    new ReadShareGroupStateResponse(
                        ReadShareGroupStateResponse.toErrorResponseData(
                            tp2.topicId(),
                            tp2.partition(),
                            Errors.UNKNOWN_TOPIC_OR_PARTITION,
                            "unknown tp"
                        )
                    )
                )
            );

        PersisterStateManager psm = mock(PersisterStateManager.class);
        DefaultStatePersister dsp = new DefaultStatePersister(psm);

        ReadShareGroupStateResult results = dsp.readResponsesToResult(futureMap);

        // results should contain partial results
        assertEquals(2, results.topicsData().size());
        assertTrue(
            results.topicsData().contains(
                new TopicData<>(
                    tp1.topicId(),
                    Collections.singletonList(PartitionFactory.newPartitionAllData(tp1.partition(), 2, 1L, Errors.NONE.code(), null, Collections.emptyList()))
                )
            )
        );
        assertTrue(
            results.topicsData().contains(
                new TopicData<>(
                    tp2.topicId(),
                    Collections.singletonList(PartitionFactory.newPartitionAllData(tp2.partition(), 0, 0, Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), "unknown tp", Collections.emptyList()))
                )
            )
        );
    }

    @Test
    public void testReadStateResponseToResultFailedFuture() {
        Map<Uuid, Map<Integer, CompletableFuture<ReadShareGroupStateResponse>>> futureMap = new HashMap<>();
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), 1, null);
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), 1, null);

        // one entry has valid results
        futureMap.computeIfAbsent(tp1.topicId(), k -> new HashMap<>())
            .put(tp1.partition(), CompletableFuture.completedFuture(
                    new ReadShareGroupStateResponse(
                        ReadShareGroupStateResponse.toResponseData(
                            tp1.topicId(),
                            tp1.partition(),
                            1L,
                            2,
                            Collections.emptyList()
                        )
                    )
                )
            );

        // one entry has failed future
        futureMap.computeIfAbsent(tp2.topicId(), k -> new HashMap<>())
            .put(tp2.partition(), CompletableFuture.failedFuture(new Exception("scary stuff")));

        PersisterStateManager psm = mock(PersisterStateManager.class);
        DefaultStatePersister dsp = new DefaultStatePersister(psm);

        ReadShareGroupStateResult results = dsp.readResponsesToResult(futureMap);

        // results should contain partial results
        assertEquals(2, results.topicsData().size());
        assertTrue(
            results.topicsData().contains(
                new TopicData<>(
                    tp1.topicId(),
                    Collections.singletonList(PartitionFactory.newPartitionAllData(tp1.partition(), 2, 1L, Errors.NONE.code(), null, Collections.emptyList()))
                )
            )
        );
        assertTrue(
            results.topicsData().contains(
                new TopicData<>(
                    tp2.topicId(),
                    Collections.singletonList(PartitionFactory.newPartitionAllData(tp2.partition(), -1, -1L, Errors.UNKNOWN_SERVER_ERROR.code(), "Error reading state from share coordinator: java.lang.Exception: scary stuff", Collections.emptyList()))
                )
            )
        );
    }

    @Test
    public void testDefaultPersisterClose() {
        PersisterStateManager psm = mock(PersisterStateManager.class);
        DefaultStatePersister dsp = new DefaultStatePersister(psm);
        try {
            verify(psm, times(0)).stop();

            dsp.stop();
            
            verify(psm, times(1)).stop();
        } catch (Exception e) {
            fail("Unexpected exception", e);
        }
    }
}
