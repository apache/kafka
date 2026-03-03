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

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidFetchSizeException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.StreamsInvalidTopologyEpochException;
import org.apache.kafka.common.errors.StreamsInvalidTopologyException;
import org.apache.kafka.common.errors.StreamsTopologyFencedException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.AlterShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.AlterShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;
import org.apache.kafka.common.message.DeleteShareGroupStateResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.share.persister.DefaultStatePersister;
import org.apache.kafka.server.share.persister.DeleteShareGroupStateParameters;
import org.apache.kafka.server.share.persister.DeleteShareGroupStateResult;
import org.apache.kafka.server.share.persister.GroupTopicPartitionData;
import org.apache.kafka.server.share.persister.InitializeShareGroupStateParameters;
import org.apache.kafka.server.share.persister.InitializeShareGroupStateResult;
import org.apache.kafka.server.share.persister.NoOpStatePersister;
import org.apache.kafka.server.share.persister.PartitionFactory;
import org.apache.kafka.server.share.persister.PartitionIdData;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.persister.ReadShareGroupStateSummaryParameters;
import org.apache.kafka.server.share.persister.ReadShareGroupStateSummaryResult;
import org.apache.kafka.server.share.persister.TopicData;
import org.apache.kafka.server.util.PartitionMetadataClient;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.coordinator.common.runtime.TestUtil.requestContext;
import static org.apache.kafka.coordinator.group.GroupConfigManagerTest.createConfigManager;
import static org.apache.kafka.server.share.persister.DeleteShareGroupStateParameters.EMPTY_PARAMS;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GroupCoordinatorServiceTest {

    private static final String TOPIC_NAME = "test-topic";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();

    private static Stream<Arguments> testGroupHeartbeatWithExceptionSource() {
        return Stream.of(
            Arguments.arguments(new UnknownTopicOrPartitionException(), Errors.COORDINATOR_NOT_AVAILABLE.code(), null),
            Arguments.arguments(new NotEnoughReplicasException(), Errors.COORDINATOR_NOT_AVAILABLE.code(), null),
            Arguments.arguments(new org.apache.kafka.common.errors.TimeoutException(), Errors.COORDINATOR_NOT_AVAILABLE.code(), null),
            Arguments.arguments(new NotLeaderOrFollowerException(), Errors.NOT_COORDINATOR.code(), null),
            Arguments.arguments(new KafkaStorageException(), Errors.NOT_COORDINATOR.code(), null),
            Arguments.arguments(new RecordTooLargeException(), Errors.UNKNOWN_SERVER_ERROR.code(), null),
            Arguments.arguments(new RecordBatchTooLargeException(), Errors.UNKNOWN_SERVER_ERROR.code(), null),
            Arguments.arguments(new InvalidFetchSizeException(""), Errors.UNKNOWN_SERVER_ERROR.code(), null),
            Arguments.arguments(new InvalidRequestException("Invalid"), Errors.INVALID_REQUEST.code(), "Invalid")
        );
    }

    @SuppressWarnings("unchecked")
    private CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> mockRuntime() {
        return (CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord>) mock(CoordinatorRuntime.class);
    }

    private GroupCoordinatorConfig createConfig() {
        return GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 600000L, 24);
    }

    @Test
    public void testStartupShutdown() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setRuntime(runtime)
            .setConfig(createConfig())
            .build(true);

        service.shutdown();

        verify(runtime, times(1)).close();
    }

    @Test
    public void testConsumerGroupHeartbeatWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData()
            .setGroupId("foo");

        CompletableFuture<ConsumerGroupHeartbeatResponseData> future = service.consumerGroupHeartbeat(
            requestContext(ApiKeys.CONSUMER_GROUP_HEARTBEAT),
            request
        );

        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @Test
    public void testConsumerGroupHeartbeat() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setRuntime(runtime)
            .setConfig(createConfig())
            .build(true);

        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData()
            .setGroupId("foo")
            .setMemberId(Uuid.randomUuid().toString())
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setTopicPartitions(List.of());

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("consumer-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            new ConsumerGroupHeartbeatResponseData()
        ));

        CompletableFuture<ConsumerGroupHeartbeatResponseData> future = service.consumerGroupHeartbeat(
            requestContext(ApiKeys.CONSUMER_GROUP_HEARTBEAT),
            request
        );

        assertEquals(new ConsumerGroupHeartbeatResponseData(), future.get(5, TimeUnit.SECONDS));
    }

    @ParameterizedTest
    @MethodSource("testGroupHeartbeatWithExceptionSource")
    public void testConsumerGroupHeartbeatWithException(
        Throwable exception,
        short expectedErrorCode,
        String expectedErrorMessage
    ) throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData()
            .setGroupId("foo")
            .setMemberId(Uuid.randomUuid().toString())
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(List.of("foo", "bar"))
            .setTopicPartitions(List.of());

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("consumer-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(exception));

        CompletableFuture<ConsumerGroupHeartbeatResponseData> future = service.consumerGroupHeartbeat(
            requestContext(ApiKeys.CONSUMER_GROUP_HEARTBEAT),
            request
        );

        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(expectedErrorCode)
                .setErrorMessage(expectedErrorMessage),
            future.get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testConsumerHeartbeatRequestValidation() throws ExecutionException, InterruptedException, TimeoutException {
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(mockRuntime())
            .build(true);

        AuthorizableRequestContext context = mock(AuthorizableRequestContext.class);
        when(context.requestVersion()).thenReturn((int) ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion());

        String memberId = Uuid.randomUuid().toString();

        // MemberId must be present in all requests.
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("MemberId can't be empty."),
            service.consumerGroupHeartbeat(
                context,
                new ConsumerGroupHeartbeatRequestData()
            ).get(5, TimeUnit.SECONDS)
        );

        // GroupId must be present in all requests.
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("GroupId can't be empty."),
            service.consumerGroupHeartbeat(
                context,
                new ConsumerGroupHeartbeatRequestData()
                    .setMemberId(memberId)
            ).get(5, TimeUnit.SECONDS)
        );

        // GroupId can't be all whitespaces.
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("GroupId can't be empty."),
            service.consumerGroupHeartbeat(
                context,
                new ConsumerGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("   ")
            ).get(5, TimeUnit.SECONDS)
        );

        // RebalanceTimeoutMs must be present in the first request (epoch == 0).
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("RebalanceTimeoutMs must be provided in first request."),
            service.consumerGroupHeartbeat(
                context,
                new ConsumerGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(0)
            ).get(5, TimeUnit.SECONDS)
        );

        // TopicPartitions must be present and empty in the first request (epoch == 0).
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("TopicPartitions must be empty when (re-)joining."),
            service.consumerGroupHeartbeat(
                context,
                new ConsumerGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(5000)
            ).get(5, TimeUnit.SECONDS)
        );

        // SubscribedTopicNames or SubscribedTopicRegex must be present in the first request (epoch == 0).
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("Either SubscribedTopicNames or SubscribedTopicRegex must be non-null when (re-)joining."),
            service.consumerGroupHeartbeat(
                context,
                new ConsumerGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(5000)
                    .setTopicPartitions(List.of())
            ).get(5, TimeUnit.SECONDS)
        );

        // InstanceId must be non-empty if provided in all requests.
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("InstanceId can't be empty."),
            service.consumerGroupHeartbeat(
                context,
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId("foo")
                    .setMemberId(memberId)
                    .setMemberEpoch(1)
                    .setInstanceId("")
            ).get(5, TimeUnit.SECONDS)
        );

        // RackId must be non-empty if provided in all requests.
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("RackId can't be empty."),
            service.consumerGroupHeartbeat(
                context,
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId("foo")
                    .setMemberId(memberId)
                    .setMemberEpoch(1)
                    .setRackId("")
            ).get(5, TimeUnit.SECONDS)
        );

        // ServerAssignor must exist if provided in all requests.
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.UNSUPPORTED_ASSIGNOR.code())
                .setErrorMessage("ServerAssignor bar is not supported. Supported assignors: range."),
            service.consumerGroupHeartbeat(
                context,
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId("foo")
                    .setMemberId(memberId)
                    .setMemberEpoch(1)
                    .setServerAssignor("bar")
            ).get(5, TimeUnit.SECONDS)
        );

        // InstanceId must be non-empty if provided in all requests.
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("InstanceId can't be null."),
            service.consumerGroupHeartbeat(
                context,
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId("foo")
                    .setMemberId(memberId)
                    .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(List.of("foo", "bar"))
                    .setTopicPartitions(List.of())
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testStreamsGroupHeartbeatWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        StreamsGroupHeartbeatRequestData request = new StreamsGroupHeartbeatRequestData()
            .setGroupId("foo");

        CompletableFuture<StreamsGroupHeartbeatResult> future = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT),
            request
        );

        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData().setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
                Map.of()
            ),
            future.get()
        );
    }

    @Test
    public void testStreamsGroupHeartbeat() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setRuntime(runtime)
            .setConfig(createConfig())
            .build(true);

        StreamsGroupHeartbeatRequestData request = new StreamsGroupHeartbeatRequestData()
            .setGroupId("foo")
            .setMemberId(Uuid.randomUuid().toString())
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(1500)
            .setActiveTasks(List.of())
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of())
            .setTopology(new StreamsGroupHeartbeatRequestData.Topology());

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("streams-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData(),
                Map.of()
            )
        ));

        CompletableFuture<StreamsGroupHeartbeatResult> future = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT),
            request
        );

        assertEquals(new StreamsGroupHeartbeatResult(new StreamsGroupHeartbeatResponseData(), Map.of()), future.get(5, TimeUnit.SECONDS));
    }

    private static Stream<Arguments> testStreamsGroupHeartbeatWithExceptionSource() {
        return Stream.of(
            Arguments.arguments(new UnknownTopicOrPartitionException(), Errors.COORDINATOR_NOT_AVAILABLE.code(), null),
            Arguments.arguments(new NotEnoughReplicasException(), Errors.COORDINATOR_NOT_AVAILABLE.code(), null),
            Arguments.arguments(new org.apache.kafka.common.errors.TimeoutException(), Errors.COORDINATOR_NOT_AVAILABLE.code(), null),
            Arguments.arguments(new NotLeaderOrFollowerException(), Errors.NOT_COORDINATOR.code(), null),
            Arguments.arguments(new KafkaStorageException(), Errors.NOT_COORDINATOR.code(), null),
            Arguments.arguments(new RecordTooLargeException(), Errors.UNKNOWN_SERVER_ERROR.code(), null),
            Arguments.arguments(new RecordBatchTooLargeException(), Errors.UNKNOWN_SERVER_ERROR.code(), null),
            Arguments.arguments(new InvalidFetchSizeException(""), Errors.UNKNOWN_SERVER_ERROR.code(), null),
            Arguments.arguments(new InvalidRequestException("Invalid"), Errors.INVALID_REQUEST.code(), "Invalid"),
            Arguments.arguments(new StreamsInvalidTopologyException("Invalid"), Errors.STREAMS_INVALID_TOPOLOGY.code(), "Invalid"),
            Arguments.arguments(new StreamsTopologyFencedException("Invalid"), Errors.STREAMS_TOPOLOGY_FENCED.code(), "Invalid"),
            Arguments.arguments(new StreamsInvalidTopologyEpochException("Invalid"), Errors.STREAMS_INVALID_TOPOLOGY_EPOCH.code(), "Invalid")
        );
    }

    @ParameterizedTest
    @MethodSource("testStreamsGroupHeartbeatWithExceptionSource")
    public void testStreamsGroupHeartbeatWithException(
        Throwable exception,
        short expectedErrorCode,
        String expectedErrorMessage
    ) throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setRuntime(runtime)
            .setConfig(createConfig())
            .build(true);

        StreamsGroupHeartbeatRequestData request = new StreamsGroupHeartbeatRequestData()
            .setGroupId("foo")
            .setMemberId(Uuid.randomUuid().toString())
            .setMemberEpoch(0)
            .setRebalanceTimeoutMs(1500)
            .setActiveTasks(List.of())
            .setStandbyTasks(List.of())
            .setWarmupTasks(List.of())
            .setTopology(new StreamsGroupHeartbeatRequestData.Topology());

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("streams-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(exception));

        CompletableFuture<StreamsGroupHeartbeatResult> future = service.streamsGroupHeartbeat(
            requestContext(ApiKeys.STREAMS_GROUP_HEARTBEAT),
            request
        );

        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(expectedErrorCode)
                    .setErrorMessage(expectedErrorMessage),
                Map.of()
            ),
            future.get(5, TimeUnit.SECONDS)
        );
    }
    @Test
    public void testStreamsGroupHeartbeatFailsForUnsupportedFeatures() throws Exception {

        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(mockRuntime())
            .build(true);

        AuthorizableRequestContext context = mock(AuthorizableRequestContext.class);
        when(context.requestVersion()).thenReturn((int) ApiKeys.STREAMS_GROUP_HEARTBEAT.latestVersion());

        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("Static membership is not yet supported."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setInstanceId(Uuid.randomUuid().toString())
            ).get(5, TimeUnit.SECONDS)
        );

        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("TaskOffsets are not supported yet."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setTaskOffsets(List.of(new StreamsGroupHeartbeatRequestData.TaskOffset()))
            ).get(5, TimeUnit.SECONDS)
        );

        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("TaskEndOffsets are not supported yet."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setTaskEndOffsets(List.of(new StreamsGroupHeartbeatRequestData.TaskOffset()))
            ).get(5, TimeUnit.SECONDS)
        );

        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("WarmupTasks are not supported yet."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setWarmupTasks(List.of(new StreamsGroupHeartbeatRequestData.TaskIds()))
            ).get(5, TimeUnit.SECONDS)
        );

        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("Regular expressions for source topics are not supported yet."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setTopology(new StreamsGroupHeartbeatRequestData.Topology()
                        .setSubtopologies(List.of(new StreamsGroupHeartbeatRequestData.Subtopology()
                            .setSourceTopicRegex(List.of("foo.*"))
                        ))
                    )
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @SuppressWarnings("MethodLength")
    @Test
    public void testStreamsHeartbeatRequestValidation() throws ExecutionException, InterruptedException, TimeoutException {
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(mockRuntime())
            .build(true);

        AuthorizableRequestContext context = mock(AuthorizableRequestContext.class);
        when(context.requestVersion()).thenReturn((int) ApiKeys.STREAMS_GROUP_HEARTBEAT.latestVersion());

        String memberId = Uuid.randomUuid().toString();

        // MemberId must be present in all requests.
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("MemberId can't be empty."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
            ).get(5, TimeUnit.SECONDS)
        );

        // MemberId can't be all whitespaces.
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("MemberId can't be empty."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setMemberId("   ")
            ).get(5, TimeUnit.SECONDS)
        );

        // GroupId must be present in all requests.
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("GroupId can't be empty."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setMemberId(memberId)
            ).get(5, TimeUnit.SECONDS)
        );

        // GroupId can't be all whitespaces.
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("GroupId can't be empty."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("   ")
            ).get(5, TimeUnit.SECONDS)
        );

        // RebalanceTimeoutMs must be present in the first request (epoch == 0).
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("RebalanceTimeoutMs must be provided in first request."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(0)
            ).get(5, TimeUnit.SECONDS)
        );

        // ActiveTasks must be present and empty in the first request (epoch == 0).
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("ActiveTasks must be empty when (re-)joining."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(1500)
            ).get(5, TimeUnit.SECONDS)
        );

        // StandbyTasks must be present and empty in the first request (epoch == 0).
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("StandbyTasks must be empty when (re-)joining."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(1500)
                    .setActiveTasks(List.of())
            ).get(5, TimeUnit.SECONDS)
        );

        // WarmupTasks must be present and empty in the first request (epoch == 0).
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("WarmupTasks must be empty when (re-)joining."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(1500)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
            ).get(5, TimeUnit.SECONDS)
        );

        // Topology must be present in the first request (epoch == 0).
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("Topology must be non-null when (re-)joining."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(1500)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of())
            ).get(5, TimeUnit.SECONDS)
        );

        // RackId must be non-empty if provided in all requests.
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("RackId can't be empty."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId("foo")
                    .setMemberId(memberId)
                    .setMemberEpoch(1)
                    .setRackId("")
            ).get(5, TimeUnit.SECONDS)
        );

        // Instance id cannot be null when leaving with -2.
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("InstanceId can't be null."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setGroupId("foo")
                    .setMemberId(memberId)
                    .setMemberEpoch(LEAVE_GROUP_STATIC_MEMBER_EPOCH)
                    .setRebalanceTimeoutMs(1500)
                    .setTopology(new StreamsGroupHeartbeatRequestData.Topology())
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of())
            ).get(5, TimeUnit.SECONDS)
        );

        // Member epoch cannot be < -2
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("MemberEpoch is -3, but must be greater than or equal to -2."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(-3)
                    .setRebalanceTimeoutMs(1500)
            ).get(5, TimeUnit.SECONDS)
        );

        // Topology must not be present in the later requests (epoch != 0).
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("Topology can only be provided when (re-)joining."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(1)
                    .setRebalanceTimeoutMs(1500)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of())
                    .setTopology(new StreamsGroupHeartbeatRequestData.Topology())
            ).get(5, TimeUnit.SECONDS)
        );

        // Topology must not contain changelog topics with fixed partition numbers
        assertEquals(
            new StreamsGroupHeartbeatResult(
                new StreamsGroupHeartbeatResponseData()
                    .setErrorCode(Errors.STREAMS_INVALID_TOPOLOGY.code())
                    .setErrorMessage("Changelog topic changelog_topic_with_fixed_partition must have an undefined partition count, but it is set to 3."),
                Map.of()
            ),
            service.streamsGroupHeartbeat(
                context,
                new StreamsGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(1500)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of())
                    .setTopology(new StreamsGroupHeartbeatRequestData.Topology().setSubtopologies(
                        List.of(
                            new StreamsGroupHeartbeatRequestData.Subtopology()
                                .setStateChangelogTopics(
                                    List.of(
                                        new StreamsGroupHeartbeatRequestData.TopicInfo()
                                            .setName("changelog_topic_with_fixed_partition")
                                            .setPartitions(3)
                                    )
                                )
                        )
                    ))
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testPartitionFor() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        assertThrows(CoordinatorNotAvailableException.class,
            () -> service.partitionFor("foo"));

        service.startup(() -> 10);

        assertEquals(Utils.abs("foo".hashCode()) % 10, service.partitionFor("foo"));
    }

    @Test
    public void testGroupMetadataTopicConfigs() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        Properties expectedProperties = new Properties();
        expectedProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        expectedProperties.put(TopicConfig.COMPRESSION_TYPE_CONFIG, BrokerCompressionType.PRODUCER.name);
        expectedProperties.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1000");

        assertEquals(expectedProperties, service.groupMetadataTopicConfigs());
    }

    @Test
    public void testOnElection() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        assertThrows(CoordinatorNotAvailableException.class,
            () -> service.onElection(5, 10));

        service.startup(() -> 1);
        service.onElection(5, 10);

        verify(runtime, times(1)).scheduleLoadOperation(
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 5),
            10
        );
    }

    @Test
    public void testOnResignation() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        assertThrows(CoordinatorNotAvailableException.class,
            () -> service.onResignation(5, OptionalInt.of(10)));

        service.startup(() -> 1);
        service.onResignation(5, OptionalInt.of(10));

        verify(runtime, times(1)).scheduleUnloadOperation(
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 5),
            OptionalInt.of(10)
        );
    }

    @Test
    public void testOnResignationWithEmptyLeaderEpoch() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        service.onResignation(5, OptionalInt.empty());

        verify(runtime, times(1)).scheduleUnloadOperation(
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 5),
            OptionalInt.empty()
        );
    }

    @Test
    public void testJoinGroup() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        JoinGroupRequestData request = new JoinGroupRequestData()
            .setGroupId("foo")
            .setSessionTimeoutMs(1000);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-join"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            new JoinGroupResponseData()
        ));

        CompletableFuture<JoinGroupResponseData> responseFuture = service.joinGroup(
            requestContext(ApiKeys.JOIN_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertFalse(responseFuture.isDone());
    }

    @Test
    public void testJoinGroupWithException() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        JoinGroupRequestData request = new JoinGroupRequestData()
            .setGroupId("foo")
            .setSessionTimeoutMs(1000);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-join"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(new IllegalStateException()));

        CompletableFuture<JoinGroupResponseData> future = service.joinGroup(
            requestContext(ApiKeys.JOIN_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new JoinGroupResponseData()
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()),
            future.get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testJoinGroupInvalidGroupId() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        JoinGroupRequestData request = new JoinGroupRequestData()
            .setGroupId(null)
            .setMemberId(UNKNOWN_MEMBER_ID);

        RequestContext context = new RequestContext(
            new RequestHeader(
                ApiKeys.JOIN_GROUP,
                ApiKeys.JOIN_GROUP.latestVersion(),
                "client",
                0
            ),
            "1",
            InetAddress.getLoopbackAddress(),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false
        );

        CompletableFuture<JoinGroupResponseData> response = service.joinGroup(
            context,
            request,
            BufferSupplier.NO_CACHING
        );

        assertTrue(response.isDone());
        JoinGroupResponseData expectedResponse = new JoinGroupResponseData()
            .setErrorCode(Errors.INVALID_GROUP_ID.code())
            .setMemberId(UNKNOWN_MEMBER_ID);

        assertEquals(expectedResponse, response.get());
    }

    @Test
    public void testJoinGroupWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        JoinGroupRequestData request = new JoinGroupRequestData()
            .setGroupId("foo");

        CompletableFuture<JoinGroupResponseData> future = service.joinGroup(
            requestContext(ApiKeys.JOIN_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new JoinGroupResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {120 - 1, 10 * 5 * 1000 + 1})
    public void testJoinGroupInvalidSessionTimeout(int sessionTimeoutMs) throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        JoinGroupRequestData request = new GroupMetadataManagerTestContext.JoinGroupRequestBuilder()
            .withGroupId("group-id")
            .withMemberId(UNKNOWN_MEMBER_ID)
            .withSessionTimeoutMs(sessionTimeoutMs)
            .build();

        CompletableFuture<JoinGroupResponseData> future = service.joinGroup(
            requestContext(ApiKeys.JOIN_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new JoinGroupResponseData()
                .setErrorCode(Errors.INVALID_SESSION_TIMEOUT.code()),
            future.get()
        );
    }

    @Test
    public void testSyncGroup() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        SyncGroupRequestData request = new SyncGroupRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-sync"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            new SyncGroupResponseData()
        ));

        CompletableFuture<SyncGroupResponseData> responseFuture = service.syncGroup(
            requestContext(ApiKeys.SYNC_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertFalse(responseFuture.isDone());
    }

    @Test
    public void testSyncGroupWithException() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        SyncGroupRequestData request = new SyncGroupRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-sync"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(new IllegalStateException()));

        CompletableFuture<SyncGroupResponseData> future = service.syncGroup(
            requestContext(ApiKeys.SYNC_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertTrue(future.isDone());
        assertEquals(
            new SyncGroupResponseData()
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()),
            future.get()
        );
    }

    @Test
    public void testSyncGroupInvalidGroupId() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        SyncGroupRequestData request = new SyncGroupRequestData()
            .setGroupId(null)
            .setMemberId(UNKNOWN_MEMBER_ID);

        CompletableFuture<SyncGroupResponseData> response = service.syncGroup(
            requestContext(ApiKeys.SYNC_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertTrue(response.isDone());
        SyncGroupResponseData expectedResponse = new SyncGroupResponseData()
            .setErrorCode(Errors.INVALID_GROUP_ID.code());

        assertEquals(expectedResponse, response.get());
    }

    @Test
    public void testSyncGroupWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        SyncGroupRequestData request = new SyncGroupRequestData()
            .setGroupId("foo");

        CompletableFuture<SyncGroupResponseData> future = service.syncGroup(
            requestContext(ApiKeys.SYNC_GROUP),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new SyncGroupResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @Test
    public void testHeartbeat() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            new HeartbeatResponseData()
        ));

        CompletableFuture<HeartbeatResponseData> future = service.heartbeat(
            requestContext(ApiKeys.HEARTBEAT),
            request
        );

        assertTrue(future.isDone());
        assertEquals(new HeartbeatResponseData(), future.get());
    }

    @Test
    public void testHeartbeatCoordinatorNotAvailableException() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            new CoordinatorLoadInProgressException(null)
        ));

        CompletableFuture<HeartbeatResponseData> future = service.heartbeat(
            requestContext(ApiKeys.HEARTBEAT),
            request
        );

        assertTrue(future.isDone());
        assertEquals(new HeartbeatResponseData(), future.get());
    }

    @Test
    public void testHeartbeatCoordinatorException() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            new RebalanceInProgressException()
        ));

        CompletableFuture<HeartbeatResponseData> future = service.heartbeat(
            requestContext(ApiKeys.HEARTBEAT),
            request
        );

        assertTrue(future.isDone());
        assertEquals(
            new HeartbeatResponseData().setErrorCode(Errors.REBALANCE_IN_PROGRESS.code()),
            future.get()
        );
    }

    @Test
    public void testHeartbeatWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        HeartbeatRequestData request = new HeartbeatRequestData()
            .setGroupId("foo");

        CompletableFuture<HeartbeatResponseData> future = service.heartbeat(
            requestContext(ApiKeys.CONSUMER_GROUP_HEARTBEAT),
            request
        );

        assertEquals(
            new HeartbeatResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @Test
    public void testListGroups() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        service.startup(() -> 3);

        List<ListGroupsResponseData.ListedGroup> expectedResults = Arrays.asList(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId("group0")
                .setProtocolType("protocol1")
                .setGroupState("Stable")
                .setGroupType("classic"),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId("group1")
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState("Empty")
                .setGroupType("consumer"),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId("group2")
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState("Dead")
                .setGroupType("consumer")
        );

        when(runtime.scheduleReadAllOperation(
            ArgumentMatchers.eq("list-groups"),
            ArgumentMatchers.any()
        )).thenReturn(Arrays.asList(
            CompletableFuture.completedFuture(List.of(expectedResults.get(0))),
            CompletableFuture.completedFuture(List.of(expectedResults.get(1))),
            CompletableFuture.completedFuture(List.of(expectedResults.get(2)))
        ));

        CompletableFuture<ListGroupsResponseData> responseFuture = service.listGroups(
            requestContext(ApiKeys.LIST_GROUPS),
            new ListGroupsRequestData()
        );

        assertEquals(expectedResults, responseFuture.get(5, TimeUnit.SECONDS).groups());
    }

    @Test
    public void testListGroupsFailedWithNotCoordinatorException()
        throws InterruptedException, ExecutionException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        service.startup(() -> 3);

        List<ListGroupsResponseData.ListedGroup> expectedResults = Arrays.asList(
            new ListGroupsResponseData.ListedGroup()
                .setGroupId("group0")
                .setProtocolType("protocol1")
                .setGroupState("Stable")
                .setGroupType("classic"),
            new ListGroupsResponseData.ListedGroup()
                .setGroupId("group1")
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState("Empty")
                .setGroupType("consumer")
        );

        when(runtime.scheduleReadAllOperation(
            ArgumentMatchers.eq("list-groups"),
            ArgumentMatchers.any()
        )).thenReturn(Arrays.asList(
            CompletableFuture.completedFuture(List.of(expectedResults.get(0))),
            CompletableFuture.completedFuture(List.of(expectedResults.get(1))),
            CompletableFuture.failedFuture(new NotCoordinatorException(""))
        ));

        CompletableFuture<ListGroupsResponseData> responseFuture = service.listGroups(
            requestContext(ApiKeys.LIST_GROUPS),
            new ListGroupsRequestData()
        );

        assertEquals(expectedResults, responseFuture.get(5, TimeUnit.SECONDS).groups());
    }

    @Test
    public void testListGroupsWithFailure()
        throws InterruptedException, ExecutionException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        service.startup(() -> 3);

        when(runtime.scheduleReadAllOperation(
            ArgumentMatchers.eq("list-groups"),
            ArgumentMatchers.any()
        )).thenReturn(Arrays.asList(
            CompletableFuture.completedFuture(List.of()),
            CompletableFuture.completedFuture(List.of()),
            CompletableFuture.failedFuture(new CoordinatorLoadInProgressException(""))
        ));

        CompletableFuture<ListGroupsResponseData> responseFuture = service.listGroups(
            requestContext(ApiKeys.LIST_GROUPS),
            new ListGroupsRequestData()
        );

        assertEquals(
            new ListGroupsResponseData()
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code()),
            responseFuture.get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testListGroupsWithEmptyTopicPartitions() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 0;
        service.startup(() -> partitionCount);

        ListGroupsRequestData request = new ListGroupsRequestData();

        CompletableFuture<ListGroupsResponseData> future = service.listGroups(
            requestContext(ApiKeys.LIST_GROUPS),
            request
        );

        assertEquals(
            new ListGroupsResponseData(),
            future.get()
        );
    }

    @Test
    public void testListGroupsWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        ListGroupsRequestData request = new ListGroupsRequestData();

        CompletableFuture<ListGroupsResponseData> future = service.listGroups(
            requestContext(ApiKeys.LIST_GROUPS),
            request
        );

        assertEquals(
            new ListGroupsResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @Test
    public void testDescribeGroups() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build();
        int partitionCount = 2;
        service.startup(() -> partitionCount);

        DescribeGroupsResponseData.DescribedGroup describedGroup1 = new DescribeGroupsResponseData.DescribedGroup()
            .setGroupId("group-id-1");
        DescribeGroupsResponseData.DescribedGroup describedGroup2 = new DescribeGroupsResponseData.DescribedGroup()
            .setGroupId("group-id-2");
        List<DescribeGroupsResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            describedGroup1,
            describedGroup2
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("describe-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(List.of(describedGroup1)));

        CompletableFuture<Object> describedGroupFuture = new CompletableFuture<>();
        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("describe-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.any()
        )).thenReturn(describedGroupFuture);

        CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> future =
            service.describeGroups(requestContext(ApiKeys.DESCRIBE_GROUPS), Arrays.asList("group-id-1", "group-id-2"));

        assertFalse(future.isDone());
        describedGroupFuture.complete(List.of(describedGroup2));
        assertEquals(expectedDescribedGroups, future.get());
    }

    @Test
    public void testDescribeGroupsInvalidGroupId() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        DescribeGroupsResponseData.DescribedGroup describedGroup = new DescribeGroupsResponseData.DescribedGroup()
            .setGroupId("");
        List<DescribeGroupsResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("")
                .setErrorCode(Errors.INVALID_GROUP_ID.code()),
            describedGroup
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("describe-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(List.of(describedGroup)));

        CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> future =
            service.describeGroups(requestContext(ApiKeys.DESCRIBE_GROUPS), Arrays.asList("", null));

        assertEquals(expectedDescribedGroups, future.get());
    }

    @Test
    public void testDescribeGroupCoordinatorLoadInProgress() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("describe-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            new CoordinatorLoadInProgressException(null)
        ));

        CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> future =
            service.describeGroups(requestContext(ApiKeys.DESCRIBE_GROUPS), List.of("group-id"));

        assertEquals(
            List.of(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            ),
            future.get()
        );
    }

    @Test
    public void testDescribeGroupsWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> future = service.describeGroups(
            requestContext(ApiKeys.DESCRIBE_GROUPS),
            List.of("group-id")
        );

        assertEquals(
            List.of(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            ),
            future.get()
        );
    }

    @ParameterizedTest
    @CsvSource({
        "false, false",
        "false, true",
        "true, false",
        "true, true",
    })
    public void testFetchOffsets(
        boolean fetchAllOffsets,
        boolean requireStable
    ) throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        OffsetFetchRequestData.OffsetFetchRequestGroup request =
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group");

        if (fetchAllOffsets) {
            request.setTopics(null);
        } else {
            request.setTopics(List.of(new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(List.of(0))
            ));
        }

        OffsetFetchResponseData.OffsetFetchResponseGroup response =
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("group")
                .setTopics(List.of(new OffsetFetchResponseData.OffsetFetchResponseTopics()
                    .setName("foo")
                    .setPartitions(List.of(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                        .setPartitionIndex(0)
                        .setCommittedOffset(100L)))));

        if (requireStable) {
            when(runtime.scheduleWriteOperation(
                ArgumentMatchers.eq(fetchAllOffsets ? "fetch-all-offsets" : "fetch-offsets"),
                ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
                ArgumentMatchers.any()
            )).thenReturn(CompletableFuture.completedFuture(response));
        } else {
            when(runtime.scheduleReadOperation(
                ArgumentMatchers.eq(fetchAllOffsets ? "fetch-all-offsets" : "fetch-offsets"),
                ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
                ArgumentMatchers.any()
            )).thenReturn(CompletableFuture.completedFuture(response));
        }

        CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> future = service.fetchOffsets(
            requestContext(ApiKeys.OFFSET_FETCH),
            request,
            requireStable
        );

        assertEquals(response, future.get(5, TimeUnit.SECONDS));
    }

    @ParameterizedTest
    @CsvSource({
        "false, false",
        "false, true",
        "true, false",
        "true, true",
    })
    public void testFetchOffsetsWhenNotStarted(
        boolean fetchAllOffsets,
        boolean requireStable
    ) throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        OffsetFetchRequestData.OffsetFetchRequestGroup request =
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group");

        if (fetchAllOffsets) {
            request.setTopics(null);
        } else {
            request.setTopics(List.of(new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(List.of(0))
            ));
        }

        CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> future = service.fetchOffsets(
            requestContext(ApiKeys.OFFSET_FETCH),
            request,
            requireStable
        );

        assertEquals(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("group")
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @ParameterizedTest
    @CsvSource({
        "false, UNKNOWN_TOPIC_OR_PARTITION, NOT_COORDINATOR",
        "false, NOT_ENOUGH_REPLICAS, NOT_COORDINATOR",
        "false, REQUEST_TIMED_OUT, NOT_COORDINATOR",
        "false, NOT_LEADER_OR_FOLLOWER, NOT_COORDINATOR",
        "false, KAFKA_STORAGE_ERROR, NOT_COORDINATOR",
        "true, UNKNOWN_TOPIC_OR_PARTITION, NOT_COORDINATOR",
        "true, NOT_ENOUGH_REPLICAS, NOT_COORDINATOR",
        "true, REQUEST_TIMED_OUT, NOT_COORDINATOR",
        "true, NOT_LEADER_OR_FOLLOWER, NOT_COORDINATOR",
        "true, KAFKA_STORAGE_ERROR, NOT_COORDINATOR",
    })
    public void testFetchOffsetsWithWrappedError(
        boolean fetchAllOffsets,
        Errors error,
        Errors expectedError
    ) throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        OffsetFetchRequestData.OffsetFetchRequestGroup request =
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId("group");

        if (fetchAllOffsets) {
            request.setTopics(null);
        } else {
            request.setTopics(List.of(new OffsetFetchRequestData.OffsetFetchRequestTopics()
                .setName("foo")
                .setPartitionIndexes(List.of(0))
            ));
        }

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq(fetchAllOffsets ? "fetch-all-offsets" : "fetch-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(new CompletionException(error.exception())));

        CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> future = service.fetchOffsets(
            requestContext(ApiKeys.OFFSET_FETCH),
            request,
            true
        );

        assertEquals(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("group")
                .setErrorCode(expectedError.code()),
            future.get()
        );
    }

    @Test
    public void testLeaveGroup() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        LeaveGroupRequestData request = new LeaveGroupRequestData()
            .setGroupId("foo");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-leave"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            new LeaveGroupResponseData()
        ));

        CompletableFuture<LeaveGroupResponseData> future = service.leaveGroup(
            requestContext(ApiKeys.LEAVE_GROUP),
            request
        );

        assertTrue(future.isDone());
        assertEquals(new LeaveGroupResponseData(), future.get());
    }

    @Test
    public void testLeaveGroupThrowsUnknownMemberIdException() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        LeaveGroupRequestData request = new LeaveGroupRequestData()
            .setGroupId("foo")
            .setMembers(Arrays.asList(
                new LeaveGroupRequestData.MemberIdentity()
                    .setMemberId("member-1")
                    .setGroupInstanceId("instance-1"),
                new LeaveGroupRequestData.MemberIdentity()
                    .setMemberId("member-2")
                    .setGroupInstanceId("instance-2")
            ));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("classic-group-leave"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            new UnknownMemberIdException()
        ));

        CompletableFuture<LeaveGroupResponseData> future = service.leaveGroup(
            requestContext(ApiKeys.LEAVE_GROUP),
            request
        );

        assertTrue(future.isDone());
        LeaveGroupResponseData expectedResponse = new LeaveGroupResponseData()
            .setErrorCode(Errors.NONE.code())
            .setMembers(Arrays.asList(
                new LeaveGroupResponseData.MemberResponse()
                    .setMemberId("member-1")
                    .setGroupInstanceId("instance-1")
                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()),
                new LeaveGroupResponseData.MemberResponse()
                    .setMemberId("member-2")
                    .setGroupInstanceId("instance-2")
                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
            ));

        assertEquals(expectedResponse, future.get());
    }

    @Test
    public void testLeaveGroupWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        LeaveGroupRequestData request = new LeaveGroupRequestData()
            .setGroupId("foo");

        CompletableFuture<LeaveGroupResponseData> future = service.leaveGroup(
            requestContext(ApiKeys.LEAVE_GROUP),
            request
        );

        assertEquals(
            new LeaveGroupResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @Test
    public void testConsumerGroupDescribe() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 2;
        service.startup(() -> partitionCount);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<CoordinatorRuntime.CoordinatorReadOperation<GroupCoordinatorShard, List<ConsumerGroupDescribeResponseData.DescribedGroup>>> readOperationCaptor =
            ArgumentCaptor.forClass(CoordinatorRuntime.CoordinatorReadOperation.class);

        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup1 = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId("group-id-1");
        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup2 = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId("group-id-2");
        List<ConsumerGroupDescribeResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            describedGroup1,
            describedGroup2
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("consumer-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            readOperationCaptor.capture()
        )).thenReturn(CompletableFuture.completedFuture(List.of(describedGroup1)));

        CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> describedGroupFuture = new CompletableFuture<>();
        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("consumer-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            readOperationCaptor.capture()
        )).thenReturn(describedGroupFuture);

        CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> future =
            service.consumerGroupDescribe(requestContext(ApiKeys.CONSUMER_GROUP_DESCRIBE), Arrays.asList("group-id-1", "group-id-2"));

        assertFalse(future.isDone());
        describedGroupFuture.complete(List.of(describedGroup2));
        assertEquals(expectedDescribedGroups, future.get());

        // Validate that the captured read operations, on the first and the second partition
        GroupCoordinatorShard shard = mock(GroupCoordinatorShard.class);
        readOperationCaptor.getAllValues().forEach(x -> x.generateResponse(shard, 100));
        verify(shard).consumerGroupDescribe(List.of("group-id-2"), 100);
        verify(shard).consumerGroupDescribe(List.of("group-id-1"), 100);
    }

    @Test
    public void testConsumerGroupDescribeInvalidGroupId() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId("")
            .setErrorCode(Errors.INVALID_GROUP_ID.code());
        List<ConsumerGroupDescribeResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupId("")
                .setErrorCode(Errors.INVALID_GROUP_ID.code()),
            describedGroup
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("consumer-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(List.of(describedGroup)));

        CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> future =
            service.consumerGroupDescribe(requestContext(ApiKeys.CONSUMER_GROUP_DESCRIBE), Arrays.asList("", null));

        assertEquals(expectedDescribedGroups, future.get());
    }

    @Test
    public void testConsumerGroupDescribeCoordinatorLoadInProgress() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("consumer-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            new CoordinatorLoadInProgressException(null)
        ));

        CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> future =
            service.consumerGroupDescribe(requestContext(ApiKeys.CONSUMER_GROUP_DESCRIBE), List.of("group-id"));

        assertEquals(
            List.of(new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            ),
            future.get()
        );
    }

    @Test
    public void testConsumerGroupDescribeCoordinatorNotActive() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("consumer-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            Errors.COORDINATOR_NOT_AVAILABLE.exception()
        ));

        CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> future =
            service.consumerGroupDescribe(requestContext(ApiKeys.CONSUMER_GROUP_DESCRIBE), List.of("group-id"));

        assertEquals(
            List.of(new ConsumerGroupDescribeResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            ),
            future.get()
        );
    }

    @Test
    public void testStreamsGroupDescribe() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 2;
        service.startup(() -> partitionCount);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<CoordinatorRuntime.CoordinatorReadOperation<GroupCoordinatorShard, List<StreamsGroupDescribeResponseData.DescribedGroup>>> readOperationCaptor =
            ArgumentCaptor.forClass(CoordinatorRuntime.CoordinatorReadOperation.class);

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup1 = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId("group-id-1");
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup2 = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId("group-id-2");
        List<StreamsGroupDescribeResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            describedGroup1,
            describedGroup2
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("streams-group-describe"),
            ArgumentMatchers.eq(new TopicPartition("__consumer_offsets", 0)),
            readOperationCaptor.capture()
        )).thenReturn(CompletableFuture.completedFuture(List.of(describedGroup1)));

        CompletableFuture<List<StreamsGroupDescribeResponseData.DescribedGroup>> describedGroupFuture = new CompletableFuture<>();
        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("streams-group-describe"),
            ArgumentMatchers.eq(new TopicPartition("__consumer_offsets", 1)),
            readOperationCaptor.capture()
        )).thenReturn(describedGroupFuture);

        CompletableFuture<List<StreamsGroupDescribeResponseData.DescribedGroup>> future =
            service.streamsGroupDescribe(requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), Arrays.asList("group-id-1", "group-id-2"));

        assertFalse(future.isDone());
        describedGroupFuture.complete(List.of(describedGroup2));
        assertEquals(expectedDescribedGroups, future.get());

        // Validate that the captured read operations, on the first and the second partition
        GroupCoordinatorShard shard = mock(GroupCoordinatorShard.class);
        readOperationCaptor.getAllValues().forEach(x -> x.generateResponse(shard, 100));
        verify(shard).streamsGroupDescribe(List.of("group-id-2"), 100);
        verify(shard).streamsGroupDescribe(List.of("group-id-1"), 100);
    }

    @Test
    public void testStreamsGroupDescribeInvalidGroupId() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId("")
            .setErrorCode(Errors.INVALID_GROUP_ID.code());
        List<StreamsGroupDescribeResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupId("")
                .setErrorCode(Errors.INVALID_GROUP_ID.code()),
            describedGroup
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("streams-group-describe"),
            ArgumentMatchers.eq(new TopicPartition("__consumer_offsets", 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(List.of(describedGroup)));

        CompletableFuture<List<StreamsGroupDescribeResponseData.DescribedGroup>> future =
            service.streamsGroupDescribe(requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), Arrays.asList("", null));

        assertEquals(expectedDescribedGroups, future.get());
    }

    @Test
    public void testStreamsGroupDescribeCoordinatorLoadInProgress() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("streams-group-describe"),
            ArgumentMatchers.eq(new TopicPartition("__consumer_offsets", 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            new CoordinatorLoadInProgressException(null)
        ));

        CompletableFuture<List<StreamsGroupDescribeResponseData.DescribedGroup>> future =
            service.streamsGroupDescribe(requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("group-id"));

        assertEquals(
            List.of(new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            ),
            future.get()
        );
    }

    @Test
    public void testStreamsGroupDescribeCoordinatorNotActive() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("streams-group-describe"),
            ArgumentMatchers.eq(new TopicPartition("__consumer_offsets", 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            Errors.COORDINATOR_NOT_AVAILABLE.exception()
        ));

        CompletableFuture<List<StreamsGroupDescribeResponseData.DescribedGroup>> future =
            service.streamsGroupDescribe(requestContext(ApiKeys.STREAMS_GROUP_DESCRIBE), List.of("group-id"));

        assertEquals(
            List.of(new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupId("group-id")
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            ),
            future.get()
        );
    }

    @Test
    public void testDeleteOffsets() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build(true);

        OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection requestTopicCollection =
            new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection(List.of(
                new OffsetDeleteRequestData.OffsetDeleteRequestTopic()
                    .setName(TOPIC_NAME)
                    .setPartitions(List.of(
                        new OffsetDeleteRequestData.OffsetDeleteRequestPartition().setPartitionIndex(0)
                    ))
            ).iterator());
        OffsetDeleteRequestData request = new OffsetDeleteRequestData()
            .setGroupId("group")
            .setTopics(requestTopicCollection);

        OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection responsePartitionCollection =
            new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection(List.of(
                new OffsetDeleteResponseData.OffsetDeleteResponsePartition().setPartitionIndex(0)
            ).iterator());
        OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection responseTopicCollection =
            new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection(List.of(
                new OffsetDeleteResponseData.OffsetDeleteResponseTopic().setPartitions(responsePartitionCollection)
            ).iterator());
        OffsetDeleteResponseData response = new OffsetDeleteResponseData()
            .setTopics(responseTopicCollection);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<OffsetDeleteResponseData> future = service.deleteOffsets(
            requestContext(ApiKeys.OFFSET_DELETE),
            request,
            BufferSupplier.NO_CACHING
        );

        assertTrue(future.isDone());
        assertEquals(response, future.get());
    }

    @Test
    public void testDeleteOffsetsInvalidGroupId() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build(true);

        OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection requestTopicCollection =
            new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection(List.of(
                new OffsetDeleteRequestData.OffsetDeleteRequestTopic()
                    .setName(TOPIC_NAME)
                    .setPartitions(List.of(
                        new OffsetDeleteRequestData.OffsetDeleteRequestPartition().setPartitionIndex(0)
                    ))
            ).iterator());
        OffsetDeleteRequestData request = new OffsetDeleteRequestData().setGroupId("")
            .setTopics(requestTopicCollection);

        OffsetDeleteResponseData response = new OffsetDeleteResponseData()
            .setErrorCode(Errors.INVALID_GROUP_ID.code());

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<OffsetDeleteResponseData> future = service.deleteOffsets(
            requestContext(ApiKeys.OFFSET_DELETE),
            request,
            BufferSupplier.NO_CACHING
        );

        assertTrue(future.isDone());
        assertEquals(response, future.get());
    }

    @ParameterizedTest
    @MethodSource("testGroupHeartbeatWithExceptionSource")
    public void testDeleteOffsetsWithException(
        Throwable exception,
        short expectedErrorCode
    ) throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build(true);

        OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection requestTopicCollection =
            new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection(List.of(
                new OffsetDeleteRequestData.OffsetDeleteRequestTopic()
                    .setName(TOPIC_NAME)
                    .setPartitions(List.of(
                        new OffsetDeleteRequestData.OffsetDeleteRequestPartition().setPartitionIndex(0)
                    ))
            ).iterator());
        OffsetDeleteRequestData request = new OffsetDeleteRequestData()
            .setGroupId("group")
            .setTopics(requestTopicCollection);

        OffsetDeleteResponseData response = new OffsetDeleteResponseData()
            .setErrorCode(expectedErrorCode);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(exception));

        CompletableFuture<OffsetDeleteResponseData> future = service.deleteOffsets(
            requestContext(ApiKeys.OFFSET_DELETE),
            request,
            BufferSupplier.NO_CACHING
        );

        assertTrue(future.isDone());
        assertEquals(response, future.get());
    }

    @Test
    public void testDeleteOffsetsWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        OffsetDeleteRequestData request = new OffsetDeleteRequestData()
            .setGroupId("foo");

        CompletableFuture<OffsetDeleteResponseData> future = service.deleteOffsets(
            requestContext(ApiKeys.OFFSET_DELETE),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new OffsetDeleteResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()),
            future.get()
        );
    }

    @Test
    public void testDeleteGroups() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build();
        service.startup(() -> 3);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection1 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        DeleteGroupsResponseData.DeletableGroupResult result1 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("group-id-1");
        resultCollection1.add(result1);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection2 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        DeleteGroupsResponseData.DeletableGroupResult result2 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("group-id-2");
        resultCollection2.add(result2);

        DeleteGroupsResponseData.DeletableGroupResult result3 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("group-id-3")
            .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code());

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        expectedResultCollection.addAll(Arrays.asList(
            new DeleteGroupsResponseData.DeletableGroupResult().setGroupId(null).setErrorCode(Errors.INVALID_GROUP_ID.code()),
            result2.duplicate(),
            result3.duplicate(),
            result1.duplicate()
        ));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of()));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 2)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(resultCollection1));

        CompletableFuture<Object> resultCollectionFuture = new CompletableFuture<>();
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(resultCollectionFuture);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(Errors.COORDINATOR_LOAD_IN_PROGRESS.exception()));

        List<String> groupIds = Arrays.asList("group-id-1", "group-id-2", "group-id-3", null);
        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(requestContext(ApiKeys.DELETE_GROUPS), groupIds, BufferSupplier.NO_CACHING);

        assertFalse(future.isDone());
        resultCollectionFuture.complete(resultCollection2);

        assertTrue(future.isDone());
        assertEquals(expectedResultCollection, future.get());
    }

    @Test
    public void testDeleteWithShareGroups() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .setPersister(persister)
            .build();
        service.startup(() -> 3);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection1 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // share group
        DeleteGroupsResponseData.DeletableGroupResult result1 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("share-group-id-1");
        resultCollection1.add(result1);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection2 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // non-share group
        DeleteGroupsResponseData.DeletableGroupResult result2 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("group-id-2");
        resultCollection2.add(result2);

        // null
        DeleteGroupsResponseData.DeletableGroupResult result3 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId(null)
            .setErrorCode(Errors.INVALID_GROUP_ID.code());

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        expectedResultCollection.addAll(List.of(
            result3.duplicate(),
            result2.duplicate(),
            result1.duplicate()
        ));

        Uuid shareGroupTopicId = Uuid.randomUuid();

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            Map.of("share-group-id-1", Map.entry(createDeleteShareRequest("share-group-id-1", shareGroupTopicId, List.of(0, 1)), Errors.NONE))
        )).thenReturn(CompletableFuture.completedFuture(Map.of()));   // non-share group

        when(persister.deleteState(ArgumentMatchers.any())).thenReturn(CompletableFuture.completedFuture(
            new DeleteShareGroupStateResult.Builder()
                .setTopicsData(List.of(
                    new TopicData<>(
                        shareGroupTopicId,
                        List.of(
                            PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message()),
                            PartitionFactory.newPartitionErrorData(1, Errors.NONE.code(), Errors.NONE.message())
                        ))
                ))
                .build()
        ));

        // share-group-id-1
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(resultCollection1));

        // group-id-2
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(resultCollection2));

        List<String> groupIds = Arrays.asList("share-group-id-1", "group-id-2", null);
        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(requestContext(ApiKeys.DELETE_GROUPS), groupIds, BufferSupplier.NO_CACHING);

        future.getNow(null);
        assertEquals(expectedResultCollection, future.get());
        verify(persister, times(1)).deleteState(ArgumentMatchers.any());
    }

    @Test
    public void testDeleteShareGroupPersisterError() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .setPersister(persister)
            .build();
        service.startup(() -> 3);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection1 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // share group err
        DeleteGroupsResponseData.DeletableGroupResult result1 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("share-group-id-1")
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
        resultCollection1.add(result1);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection2 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // share group success
        DeleteGroupsResponseData.DeletableGroupResult result2 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("share-group-id-2");
        resultCollection2.add(result2);

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        expectedResultCollection.addAll(Arrays.asList(
            result1.duplicate(),
            result2.duplicate()));

        Uuid shareGroupTopicId = Uuid.randomUuid();
        Uuid shareGroupTopicId2 = Uuid.randomUuid();
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            Map.of("share-group-id-1", Map.entry(createDeleteShareRequest("share-group-id-1", shareGroupTopicId, List.of(0, 1)), Errors.NONE))
        )).thenReturn(CompletableFuture.completedFuture(
            Map.of("share-group-id-2", Map.entry(createDeleteShareRequest("share-group-id-2", shareGroupTopicId2, List.of(0, 1)), Errors.NONE))
        ));

        when(persister.deleteState(ArgumentMatchers.any())).thenReturn(CompletableFuture.completedFuture(
            new DeleteShareGroupStateResult.Builder()
                .setTopicsData(List.of(
                    new TopicData<>(
                        shareGroupTopicId,
                        List.of(
                            PartitionFactory.newPartitionErrorData(0, Errors.UNKNOWN_SERVER_ERROR.code(), Errors.UNKNOWN_SERVER_ERROR.message()),
                            PartitionFactory.newPartitionErrorData(1, Errors.UNKNOWN_SERVER_ERROR.code(), Errors.UNKNOWN_SERVER_ERROR.message())
                        ))
                ))
                .build()
        )).thenReturn(CompletableFuture.completedFuture(
            new DeleteShareGroupStateResult.Builder()
                .setTopicsData(List.of(
                    new TopicData<>(
                        shareGroupTopicId2,
                        List.of(
                            PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message()),
                            PartitionFactory.newPartitionErrorData(1, Errors.NONE.code(), Errors.NONE.message())
                        ))
                ))
                .build()
        ));

        // share-group-id-1
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(resultCollection1));

        // share-group-id-2
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 2)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(resultCollection2));

        List<String> groupIds = List.of("share-group-id-1", "share-group-id-2");
        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(requestContext(ApiKeys.DELETE_GROUPS), groupIds, BufferSupplier.NO_CACHING);

        future.getNow(null);
        assertEquals(expectedResultCollection, future.get());
        verify(persister, times(2)).deleteState(ArgumentMatchers.any());
    }

    @Test
    public void testDeleteShareGroupCoordinatorShareSpecificWriteError() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .setPersister(persister)
            .build();
        service.startup(() -> 3);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection1 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // share group err
        DeleteGroupsResponseData.DeletableGroupResult result1 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("share-group-id-1")
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());
        resultCollection1.add(result1);

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        expectedResultCollection.add(
            result1.duplicate()
        );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            Errors.COORDINATOR_NOT_AVAILABLE.exception()
        ));

        // share-group-id-1
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(resultCollection1));

        List<String> groupIds = List.of("share-group-id-1");
        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(requestContext(ApiKeys.DELETE_GROUPS), groupIds, BufferSupplier.NO_CACHING);

        future.getNow(null);
        assertEquals(expectedResultCollection, future.get());
        verify(persister, times(0)).deleteState(ArgumentMatchers.any());
    }

    @Test
    public void testDeleteShareGroupNotEmptyError() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .setPersister(persister)
            .build();
        service.startup(() -> 3);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection1 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // share group err
        DeleteGroupsResponseData.DeletableGroupResult result1 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("share-group-id-1")
            .setErrorCode(Errors.forException(new GroupNotEmptyException("bad stuff")).code());
        resultCollection1.add(result1);

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        expectedResultCollection.add(
            result1.duplicate()
        );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            Map.of("share-group-id-1", Map.entry(EMPTY_PARAMS, Errors.forException(new GroupNotEmptyException("bad stuff"))))
        ));

        List<String> groupIds = List.of("share-group-id-1");
        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(requestContext(ApiKeys.DELETE_GROUPS), groupIds, BufferSupplier.NO_CACHING);

        future.getNow(null);
        assertEquals(expectedResultCollection, future.get());
        // If there is error creating share group delete req
        // neither persister call nor general delete groups call is made.
        verify(persister, times(0)).deleteState(ArgumentMatchers.any());
        verify(runtime, times(0)).scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        );
    }

    @Test
    public void testDeleteShareGroupCoordinatorGeneralWriteError() throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .setPersister(persister)
            .build();
        service.startup(() -> 3);

        DeleteGroupsResponseData.DeletableGroupResultCollection resultCollection1 =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        // share group err
        DeleteGroupsResponseData.DeletableGroupResult result1 = new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId("share-group-id-1")
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
        resultCollection1.add(result1);

        DeleteGroupsResponseData.DeletableGroupResultCollection expectedResultCollection =
            new DeleteGroupsResponseData.DeletableGroupResultCollection();
        expectedResultCollection.add(
            result1.duplicate()
        );

        Uuid shareGroupTopicId = Uuid.randomUuid();
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            Map.of("share-group-id-1", Map.entry(createDeleteShareRequest("share-group-id-1", shareGroupTopicId, List.of(0, 1)), Errors.NONE))
        ));

        // share-group-id-1
        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(Errors.CLUSTER_AUTHORIZATION_FAILED.exception()));

        when(persister.deleteState(ArgumentMatchers.any())).thenReturn(CompletableFuture.completedFuture(new DeleteShareGroupStateResult.Builder()
            .setTopicsData(List.of(
                new TopicData<>(
                    shareGroupTopicId,
                    List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message()),
                        PartitionFactory.newPartitionErrorData(1, Errors.NONE.code(), Errors.NONE.message())
                    ))
            ))
            .build()
        ));

        List<String> groupIds = List.of("share-group-id-1");
        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(requestContext(ApiKeys.DELETE_GROUPS), groupIds, BufferSupplier.NO_CACHING);

        future.getNow(null);
        assertEquals(expectedResultCollection, future.get());
        verify(persister, times(1)).deleteState(ArgumentMatchers.any());
    }

    @ParameterizedTest
    @MethodSource("testGroupHeartbeatWithExceptionSource")
    public void testDeleteGroupsWithException(
        Throwable exception,
        short expectedErrorCode
    ) throws Exception {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build(true);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-share-groups"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of()));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("delete-groups"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(exception));

        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future =
            service.deleteGroups(
                requestContext(ApiKeys.DELETE_GROUPS),
                List.of("group-id"),
                BufferSupplier.NO_CACHING
            );

        assertEquals(
            new DeleteGroupsResponseData.DeletableGroupResultCollection(List.of(
                new DeleteGroupsResponseData.DeletableGroupResult()
                    .setGroupId("group-id")
                    .setErrorCode(expectedErrorCode)
            ).iterator()),
            future.get()
        );
    }

    @Test
    public void testDeleteGroupsWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build();

        CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> future = service.deleteGroups(
            requestContext(ApiKeys.DELETE_GROUPS),
            List.of("foo"),
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new DeleteGroupsResponseData.DeletableGroupResultCollection(
                List.of(new DeleteGroupsResponseData.DeletableGroupResult()
                    .setGroupId("foo")
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                ).iterator()
            ),
            future.get()
        );
    }

    @Test
    public void testCommitTransactionalOffsetsWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        TxnOffsetCommitRequestData request = new TxnOffsetCommitRequestData()
            .setGroupId("foo")
            .setTransactionalId("transactional-id")
            .setMemberId("member-id")
            .setGenerationId(10)
            .setTopics(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                .setName(TOPIC_NAME)
                .setPartitions(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                    .setPartitionIndex(0)
                    .setCommittedOffset(100)))));

        CompletableFuture<TxnOffsetCommitResponseData> future = service.commitTransactionalOffsets(
            requestContext(ApiKeys.TXN_OFFSET_COMMIT),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new TxnOffsetCommitResponseData()
                .setTopics(List.of(new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                    .setName(TOPIC_NAME)
                    .setPartitions(List.of(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                        .setPartitionIndex(0)
                        .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()))))),
            future.get()
        );
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {""})
    public void testCommitTransactionalOffsetsWithInvalidGroupId(String groupId) throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        TxnOffsetCommitRequestData request = new TxnOffsetCommitRequestData()
            .setGroupId(groupId)
            .setTransactionalId("transactional-id")
            .setMemberId("member-id")
            .setGenerationId(10)
            .setTopics(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                .setName(TOPIC_NAME)
                .setPartitions(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                    .setPartitionIndex(0)
                    .setCommittedOffset(100)))));

        CompletableFuture<TxnOffsetCommitResponseData> future = service.commitTransactionalOffsets(
            requestContext(ApiKeys.TXN_OFFSET_COMMIT),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(
            new TxnOffsetCommitResponseData()
                .setTopics(List.of(new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                    .setName(TOPIC_NAME)
                    .setPartitions(List.of(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                        .setPartitionIndex(0)
                        .setErrorCode(Errors.INVALID_GROUP_ID.code()))))),
            future.get()
        );
    }

    @ParameterizedTest
    @ValueSource(shorts = {4, 5})
    public void testCommitTransactionalOffsets(short txnOffsetCommitVersion) throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        TxnOffsetCommitRequestData request = new TxnOffsetCommitRequestData()
            .setGroupId("foo")
            .setTransactionalId("transactional-id")
            .setProducerId(10L)
            .setProducerEpoch((short) 5)
            .setMemberId("member-id")
            .setGenerationId(10)
            .setTopics(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                .setName(TOPIC_NAME)
                .setPartitions(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                    .setPartitionIndex(0)
                    .setCommittedOffset(100)))));

        TxnOffsetCommitResponseData response = new TxnOffsetCommitResponseData()
            .setTopics(List.of(new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                .setName(TOPIC_NAME)
                .setPartitions(List.of(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                    .setPartitionIndex(0)
                    .setErrorCode(Errors.NONE.code())))));

        when(runtime.scheduleTransactionalWriteOperation(
            ArgumentMatchers.eq("txn-commit-offset"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq("transactional-id"),
            ArgumentMatchers.eq(10L),
            ArgumentMatchers.eq((short) 5),
            ArgumentMatchers.any(),
            ArgumentMatchers.eq((int) txnOffsetCommitVersion)
        )).thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<TxnOffsetCommitResponseData> future = service.commitTransactionalOffsets(
            requestContext(ApiKeys.TXN_OFFSET_COMMIT, txnOffsetCommitVersion),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(response, future.get());
    }

    @ParameterizedTest
    @CsvSource({
        "NOT_ENOUGH_REPLICAS, COORDINATOR_NOT_AVAILABLE",
        "NETWORK_EXCEPTION, COORDINATOR_LOAD_IN_PROGRESS"
    })
    public void testCommitTransactionalOffsetsWithWrappedError(
        Errors error,
        Errors expectedError
    ) throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        TxnOffsetCommitRequestData request = new TxnOffsetCommitRequestData()
            .setGroupId("foo")
            .setTransactionalId("transactional-id")
            .setProducerId(10L)
            .setProducerEpoch((short) 5)
            .setMemberId("member-id")
            .setGenerationId(10)
            .setTopics(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                .setName(TOPIC_NAME)
                .setPartitions(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                    .setPartitionIndex(0)
                    .setCommittedOffset(100)))));

        TxnOffsetCommitResponseData response = new TxnOffsetCommitResponseData()
            .setTopics(List.of(new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                .setName(TOPIC_NAME)
                .setPartitions(List.of(new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                    .setPartitionIndex(0)
                    .setErrorCode(expectedError.code())))));

        when(runtime.scheduleTransactionalWriteOperation(
            ArgumentMatchers.eq("txn-commit-offset"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq("transactional-id"),
            ArgumentMatchers.eq(10L),
            ArgumentMatchers.eq((short) 5),
            ArgumentMatchers.any(),
            ArgumentMatchers.eq((int) ApiKeys.TXN_OFFSET_COMMIT.latestVersion())
        )).thenReturn(CompletableFuture.failedFuture(new CompletionException(error.exception())));

        CompletableFuture<TxnOffsetCommitResponseData> future = service.commitTransactionalOffsets(
            requestContext(ApiKeys.TXN_OFFSET_COMMIT),
            request,
            BufferSupplier.NO_CACHING
        );

        assertEquals(response, future.get());
    }

    @Test
    public void testCompleteTransaction() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        when(runtime.scheduleTransactionCompletion(
            ArgumentMatchers.eq("write-txn-marker"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(100L),
            ArgumentMatchers.eq((short) 5),
            ArgumentMatchers.eq(10),
            ArgumentMatchers.eq(TransactionResult.COMMIT),
            ArgumentMatchers.eq(TransactionVersion.TV_1.featureLevel())
        )).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> future = service.completeTransaction(
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0),
            100L,
            (short) 5,
            10,
            TransactionResult.COMMIT,
            TransactionVersion.TV_1.featureLevel()
        );

        assertNull(future.get());
    }

    @Test
    public void testCompleteTransactionWhenNotCoordinatorServiceStarted() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        CompletableFuture<Void> future = service.completeTransaction(
            new TopicPartition("foo", 0),
            100L,
            (short) 5,
            10,
            TransactionResult.COMMIT,
            TransactionVersion.TV_1.featureLevel()
        );

        assertFutureThrows(CoordinatorNotAvailableException.class, future);
    }

    @Test
    public void testCompleteTransactionWithUnexpectedPartition() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        CompletableFuture<Void> future = service.completeTransaction(
            new TopicPartition("foo", 0),
            100L,
            (short) 5,
            10,
            TransactionResult.COMMIT,
            TransactionVersion.TV_1.featureLevel()
        );

        assertFutureThrows(IllegalStateException.class, future);
    }

    @Test
    public void testOnMetadataUpdateWhenNotStarted() {
        var runtime = mockRuntime();
        var service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        var image = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "foo", 1)
            .build();
        var delta = new MetadataDelta(image);

        assertThrows(CoordinatorNotAvailableException.class,
            () -> service.onMetadataUpdate(delta, image));
    }

    @Test
    public void testOnMetadataUpdateSchedulesOperationsWhenTopicsDeleted() throws ExecutionException, InterruptedException, TimeoutException {
        var runtime = mockRuntime();
        var service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        service.startup(() -> 3);

        var topicId = Uuid.randomUuid();
        var initialImage = new MetadataImageBuilder()
            .addTopic(topicId, "foo", 1)
            .build();

        // Create a delta that deletes the topic.
        var delta = new MetadataDelta(initialImage);
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));
        var newImage = delta.apply(new MetadataProvenance(1, 0, 0L, true));

        // Use incomplete futures to verify method blocks.
        var offsetFutures = List.of(
            new CompletableFuture<>(),
            new CompletableFuture<>(),
            new CompletableFuture<>()
        );
        var shareFutures = List.of(
            new CompletableFuture<>(),
            new CompletableFuture<>(),
            new CompletableFuture<>()
        );

        when(runtime.scheduleWriteAllOperation(
            ArgumentMatchers.eq("on-topics-deleted"),
            ArgumentMatchers.any()
        )).thenReturn(offsetFutures);

        when(runtime.scheduleWriteAllOperation(
            ArgumentMatchers.eq("maybe-cleanup-share-group-state"),
            ArgumentMatchers.any()
        )).thenReturn(shareFutures);

        // Run onMetadataUpdate in a separate thread.
        var resultFuture = CompletableFuture.runAsync(() -> service.onMetadataUpdate(delta, newImage));

        // Wait for the operations to be scheduled and verify method is blocked.
        verify(runtime, timeout(5000).times(1)).scheduleWriteAllOperation(
            ArgumentMatchers.eq("on-topics-deleted"),
            ArgumentMatchers.any()
        );
        verify(runtime, timeout(5000).times(1)).scheduleWriteAllOperation(
            ArgumentMatchers.eq("maybe-cleanup-share-group-state"),
            ArgumentMatchers.any()
        );
        assertFalse(resultFuture.isDone());

        // Complete all futures.
        offsetFutures.forEach(f -> f.complete(null));
        shareFutures.forEach(f -> f.complete(null));

        // Verify method completes.
        resultFuture.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testOnMetadataUpdateDoesNotScheduleOperationsWhenNoTopicsDeleted() {
        var runtime = mockRuntime();
        var service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        service.startup(() -> 3);

        // Create an image with a topic and a delta with no deletions.
        var image = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), "foo", 1)
            .build();
        var delta = new MetadataDelta(image);

        assertDoesNotThrow(() -> service.onMetadataUpdate(delta, image));

        // Verify no operations scheduled.
        verify(runtime, times(0)).scheduleWriteAllOperation(
            ArgumentMatchers.eq("on-topics-deleted"),
            ArgumentMatchers.any()
        );
        verify(runtime, times(0)).scheduleWriteAllOperation(
            ArgumentMatchers.eq("maybe-cleanup-share-group-state"),
            ArgumentMatchers.any()
        );
    }

    @Test
    public void testOnMetadataUpdateSwallowsErrorsWhenTopicsDeleted() {
        var runtime = mockRuntime();
        var service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        service.startup(() -> 3);

        var topicId = Uuid.randomUuid();
        var initialImage = new MetadataImageBuilder()
            .addTopic(topicId, "foo", 1)
            .build();

        // Create a delta that deletes the topic.
        var delta = new MetadataDelta(initialImage);
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));
        var newImage = delta.apply(new MetadataProvenance(1, 0, 0L, true));

        // Mock operations with 3 futures, some failing.
        when(runtime.scheduleWriteAllOperation(
            ArgumentMatchers.eq("on-topics-deleted"),
            ArgumentMatchers.any()
        )).thenReturn(Arrays.asList(
            CompletableFuture.completedFuture(null),
            CompletableFuture.completedFuture(null),
            CompletableFuture.failedFuture(Errors.COORDINATOR_LOAD_IN_PROGRESS.exception())
        ));

        when(runtime.scheduleWriteAllOperation(
            ArgumentMatchers.eq("maybe-cleanup-share-group-state"),
            ArgumentMatchers.any()
        )).thenReturn(Arrays.asList(
            CompletableFuture.completedFuture(null),
            CompletableFuture.completedFuture(null),
            CompletableFuture.failedFuture(Errors.COORDINATOR_LOAD_IN_PROGRESS.exception())
        ));

        // Verify no exception thrown.
        assertDoesNotThrow(() -> service.onMetadataUpdate(delta, newImage));

        // Verify operations were still scheduled exactly once.
        verify(runtime, times(1)).scheduleWriteAllOperation(
            ArgumentMatchers.eq("on-topics-deleted"),
            ArgumentMatchers.any()
        );
        verify(runtime, times(1)).scheduleWriteAllOperation(
            ArgumentMatchers.eq("maybe-cleanup-share-group-state"),
            ArgumentMatchers.any()
        );
    }

    @Test
    public void testShareGroupHeartbeatWhenNotStarted() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        ShareGroupHeartbeatRequestData request = new ShareGroupHeartbeatRequestData()
            .setGroupId("foo");

        CompletableFuture<ShareGroupHeartbeatResponseData> future = service.shareGroupHeartbeat(
            requestContext(ApiKeys.SHARE_GROUP_HEARTBEAT),
            request
        );

        assertEquals(new ShareGroupHeartbeatResponseData().setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()), future.get());
    }

    @Test
    public void testShareGroupHeartbeat() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        ShareGroupHeartbeatRequestData request = new ShareGroupHeartbeatRequestData()
            .setGroupId("foo")
            .setMemberId(Uuid.randomUuid().toString())
            .setMemberEpoch(0)
            .setSubscribedTopicNames(List.of("foo", "bar"));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("share-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(
            Map.entry(
                new ShareGroupHeartbeatResponseData(),
                Optional.empty()
            )
        ));

        CompletableFuture<ShareGroupHeartbeatResponseData> future = service.shareGroupHeartbeat(
            requestContext(ApiKeys.SHARE_GROUP_HEARTBEAT),
            request
        );

        assertEquals(new ShareGroupHeartbeatResponseData(), future.get(5, TimeUnit.SECONDS));
    }

    @ParameterizedTest
    @MethodSource("testGroupHeartbeatWithExceptionSource")
    public void testShareGroupHeartbeatWithException(
        Throwable exception,
        short expectedErrorCode,
        String expectedErrorMessage
    ) throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);

        ShareGroupHeartbeatRequestData request = new ShareGroupHeartbeatRequestData()
            .setGroupId("foo")
            .setMemberId(Uuid.randomUuid().toString())
            .setMemberEpoch(0)
            .setSubscribedTopicNames(List.of("foo", "bar"));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("share-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(exception));

        CompletableFuture<ShareGroupHeartbeatResponseData> future = service.shareGroupHeartbeat(
            requestContext(ApiKeys.SHARE_GROUP_HEARTBEAT),
            request
        );

        assertEquals(
            new ShareGroupHeartbeatResponseData()
                .setErrorCode(expectedErrorCode)
                .setErrorMessage(expectedErrorMessage),
            future.get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testShareGroupHeartbeatRequestValidation() throws ExecutionException, InterruptedException, TimeoutException {
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(mockRuntime())
            .build(true);

        AuthorizableRequestContext context = mock(AuthorizableRequestContext.class);
        when(context.requestVersion()).thenReturn((int) ApiKeys.SHARE_GROUP_HEARTBEAT.latestVersion());

        String memberId = Uuid.randomUuid().toString();

        // MemberId must be present in all requests.
        assertEquals(
            new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("MemberId can't be empty."),
            service.shareGroupHeartbeat(
                context,
                new ShareGroupHeartbeatRequestData()
            ).get(5, TimeUnit.SECONDS)
        );

        // GroupId must be present in all requests.
        assertEquals(
            new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("GroupId can't be empty."),
            service.shareGroupHeartbeat(
                context,
                new ShareGroupHeartbeatRequestData()
                    .setMemberId(memberId)
            ).get(5, TimeUnit.SECONDS)
        );

        // GroupId can't be all whitespaces.
        assertEquals(
            new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("GroupId can't be empty."),
            service.shareGroupHeartbeat(
                context,
                new ShareGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("   ")
            ).get(5, TimeUnit.SECONDS)
        );

        // SubscribedTopicNames must be present and empty in the first request (epoch == 0).
        assertEquals(
            new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("SubscribedTopicNames must be set in first request."),
            service.shareGroupHeartbeat(
                context,
                new ShareGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(0)
            ).get(5, TimeUnit.SECONDS)
        );

        // MemberId must be non-empty in all requests except for the first one where it
        // could be empty (epoch != 0).
        assertEquals(
            new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("MemberId can't be empty."),
            service.shareGroupHeartbeat(
                context,
                new ShareGroupHeartbeatRequestData()
                    .setGroupId("foo")
                    .setMemberEpoch(1)
            ).get(5, TimeUnit.SECONDS)
        );

        // RackId must be non-empty if provided in all requests.
        assertEquals(
            new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("RackId can't be empty."),
            service.shareGroupHeartbeat(
                context,
                new ShareGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(1)
                    .setRackId("")
            ).get(5, TimeUnit.SECONDS)
        );

        // Invalid member epoch.
        assertEquals(
            new ShareGroupHeartbeatResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("MemberEpoch is invalid."),
            service.shareGroupHeartbeat(
                context,
                new ShareGroupHeartbeatRequestData()
                    .setMemberId(memberId)
                    .setGroupId("foo")
                    .setMemberEpoch(-10)
            ).get(5, TimeUnit.SECONDS)
        );
    }


    @Test
    public void testShareGroupDescribe() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 2;
        service.startup(() -> partitionCount);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<CoordinatorRuntime.CoordinatorReadOperation<GroupCoordinatorShard, List<ShareGroupDescribeResponseData.DescribedGroup>>> readOperationCaptor =
            ArgumentCaptor.forClass(CoordinatorRuntime.CoordinatorReadOperation.class);

        ShareGroupDescribeResponseData.DescribedGroup describedGroup1 = new ShareGroupDescribeResponseData.DescribedGroup()
            .setGroupId("share-group-id-1");
        ShareGroupDescribeResponseData.DescribedGroup describedGroup2 = new ShareGroupDescribeResponseData.DescribedGroup()
            .setGroupId("share-group-id-2");
        List<ShareGroupDescribeResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            describedGroup1,
            describedGroup2
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            readOperationCaptor.capture()
        )).thenReturn(CompletableFuture.completedFuture(List.of(describedGroup1)));

        CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> describedGroupFuture = new CompletableFuture<>();
        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)),
            readOperationCaptor.capture()
        )).thenReturn(describedGroupFuture);

        CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> future =
            service.shareGroupDescribe(requestContext(ApiKeys.SHARE_GROUP_DESCRIBE), Arrays.asList("share-group-id-1", "share-group-id-2"));

        assertFalse(future.isDone());
        describedGroupFuture.complete(List.of(describedGroup2));
        assertEquals(expectedDescribedGroups, future.get());

        // Validate that the captured read operations, on the first and the second partition
        GroupCoordinatorShard shard = mock(GroupCoordinatorShard.class);
        readOperationCaptor.getAllValues().forEach(x -> x.generateResponse(shard, 100));
        verify(shard).shareGroupDescribe(List.of("share-group-id-2"), 100);
        verify(shard).shareGroupDescribe(List.of("share-group-id-1"), 100);
    }

    @Test
    public void testShareGroupDescribeInvalidGroupId() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        ShareGroupDescribeResponseData.DescribedGroup describedGroup = new ShareGroupDescribeResponseData.DescribedGroup()
            .setGroupId("")
            .setErrorCode(Errors.INVALID_GROUP_ID.code());
        List<ShareGroupDescribeResponseData.DescribedGroup> expectedDescribedGroups = Arrays.asList(
            new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId("")
                .setErrorCode(Errors.INVALID_GROUP_ID.code()),
            describedGroup
        );

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(List.of(describedGroup)));

        CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> future =
            service.shareGroupDescribe(requestContext(ApiKeys.SHARE_GROUP_DESCRIBE), Arrays.asList("", null));

        assertEquals(expectedDescribedGroups, future.get());
    }

    @Test
    public void testShareGroupDescribeCoordinatorLoadInProgress() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        int partitionCount = 1;
        service.startup(() -> partitionCount);

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            new CoordinatorLoadInProgressException(null)
        ));

        CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> future =
            service.shareGroupDescribe(requestContext(ApiKeys.SHARE_GROUP_DESCRIBE), List.of("share-group-id"));

        assertEquals(
            List.of(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId("share-group-id")
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                .setErrorMessage(Errors.COORDINATOR_LOAD_IN_PROGRESS.message())
            ),
            future.get()
        );
    }

    @Test
    public void testShareGroupDescribeCoordinatorNotActive() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();
        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-describe"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            Errors.COORDINATOR_NOT_AVAILABLE.exception()
        ));

        CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> future =
            service.shareGroupDescribe(requestContext(ApiKeys.SHARE_GROUP_DESCRIBE), List.of("share-group-id"));

        assertEquals(
            List.of(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId("share-group-id")
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                .setErrorMessage(Errors.COORDINATOR_NOT_AVAILABLE.message())
            ),
            future.get()
        );
    }

    @Test
    public void testDescribeShareGroupOffsetsWithNoOpPersister() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);
        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setTopics(
                List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                    .setTopicName(TOPIC_NAME)
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partition)
                        .setStartOffset(PartitionFactory.UNINITIALIZED_START_OFFSET)
                        .setLag(PartitionFactory.UNINITIALIZED_LAG))))
            );

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupOffsetsWithDefaultPersister() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);

        PartitionMetadataClient partitionMetadataClient = mock(PartitionMetadataClient.class);

        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .setPartitionMetadataClient(partitionMetadataClient)
            .build(true);
        service.startup(() -> 1);

        Set<TopicPartition> partitionsToComputeLag = new HashSet<>(Set.of(new TopicPartition(TOPIC_NAME, 1)));
        when(partitionMetadataClient.listLatestOffsets(partitionsToComputeLag))
            .thenReturn(Map.of(new TopicPartition(TOPIC_NAME, 1),
                CompletableFuture.completedFuture(new PartitionMetadataClient.OffsetResponse(41L, Errors.NONE))));

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        ReadShareGroupStateSummaryRequestData readShareGroupStateSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
            .setGroupId("share-group-id")
            .setTopics(List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new ReadShareGroupStateSummaryRequestData.PartitionData()
                        .setPartition(partition)))));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setTopics(
                List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                    .setTopicName(TOPIC_NAME)
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partition)
                        .setStartOffset(21)
                        .setLag(10L))))
            );

        ReadShareGroupStateSummaryResponseData readShareGroupStateSummaryResponseData = new ReadShareGroupStateSummaryResponseData()
            .setResults(
                List.of(new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                        .setPartition(partition)
                        .setStartOffset(21)
                        .setDeliveryCompleteCount(10)
                        .setStateEpoch(1)))
                )
            );

        ReadShareGroupStateSummaryParameters readShareGroupStateSummaryParameters = ReadShareGroupStateSummaryParameters.from(readShareGroupStateSummaryRequestData);
        ReadShareGroupStateSummaryResult readShareGroupStateSummaryResult = ReadShareGroupStateSummaryResult.from(readShareGroupStateSummaryResponseData);
        when(persister.readSummary(
            ArgumentMatchers.eq(readShareGroupStateSummaryParameters)
            )).thenReturn(CompletableFuture.completedFuture(readShareGroupStateSummaryResult));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupOffsetsNonexistentTopicWithDefaultPersister() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName("badtopic")
                .setPartitions(List.of(partition))
            ));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setTopics(
                List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                    .setTopicName("badtopic")
                    .setTopicId(Uuid.ZERO_UUID)
                    .setPartitions(List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partition)
                        .setStartOffset(PartitionFactory.UNINITIALIZED_START_OFFSET))))
            );

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupOffsetsWithDefaultPersisterThrowsError() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        when(persister.readSummary(ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.failedFuture(new Exception("Unable to validate read state summary request")));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);
        assertFutureThrows(Exception.class, future, "Unable to validate read state summary request");
    }

    @Test
    public void testDescribeShareGroupOffsetsWithDefaultPersisterNullResult() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        when(persister.readSummary(ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);
        assertFutureThrows(IllegalStateException.class, future, "Result is null for the read state summary");
    }

    @Test
    public void testDescribeShareGroupOffsetsWithDefaultPersisterNullTopicData() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        ReadShareGroupStateSummaryResult readShareGroupStateSummaryResult =
            new ReadShareGroupStateSummaryResult.Builder().setTopicsData(null).build();

        when(persister.readSummary(ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.completedFuture(readShareGroupStateSummaryResult));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);
        assertFutureThrows(IllegalStateException.class, future, "Result is null for the read state summary");
    }

    @Test
    public void testDescribeShareGroupOffsetsWithDefaultPersisterReadSummaryPartitionError() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);

        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        ReadShareGroupStateSummaryRequestData readShareGroupStateSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
            .setGroupId("share-group-id")
            .setTopics(List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateSummaryRequestData.PartitionData()
                    .setPartition(partition)))));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setTopics(
                List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                    .setTopicName(TOPIC_NAME)
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partition)
                        .setStartOffset(PartitionFactory.UNINITIALIZED_START_OFFSET)
                        .setLeaderEpoch(PartitionFactory.DEFAULT_LEADER_EPOCH)
                        .setLag(PartitionFactory.UNINITIALIZED_LAG))))
            );

        ReadShareGroupStateSummaryResponseData readShareGroupStateSummaryResponseData = new ReadShareGroupStateSummaryResponseData()
            .setResults(
                List.of(new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                        .setPartition(partition)
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message())
                    ))
                )
            );

        ReadShareGroupStateSummaryParameters readShareGroupStateSummaryParameters = ReadShareGroupStateSummaryParameters.from(readShareGroupStateSummaryRequestData);
        ReadShareGroupStateSummaryResult readShareGroupStateSummaryResult = ReadShareGroupStateSummaryResult.from(readShareGroupStateSummaryResponseData);
        when(persister.readSummary(
            ArgumentMatchers.eq(readShareGroupStateSummaryParameters)
        )).thenReturn(CompletableFuture.completedFuture(readShareGroupStateSummaryResult));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupOffsetsWithDefaultPersisterLatestOffsetThrowsError() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);

        PartitionMetadataClient partitionMetadataClient = mock(PartitionMetadataClient.class);

        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .setPartitionMetadataClient(partitionMetadataClient)
            .build(true);
        service.startup(() -> 1);

        Exception ex = new UnknownServerException("failure");

        Set<TopicPartition> partitionsToComputeLag = new HashSet<>(Set.of(new TopicPartition(TOPIC_NAME, 1)));
        when(partitionMetadataClient.listLatestOffsets(partitionsToComputeLag))
            .thenReturn(Map.of(new TopicPartition(TOPIC_NAME, 1), CompletableFuture.failedFuture(ex)));

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        ReadShareGroupStateSummaryRequestData readShareGroupStateSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
            .setGroupId("share-group-id")
            .setTopics(List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateSummaryRequestData.PartitionData()
                    .setPartition(partition)))));

        ReadShareGroupStateSummaryResponseData readShareGroupStateSummaryResponseData = new ReadShareGroupStateSummaryResponseData()
            .setResults(
                List.of(new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                        .setPartition(partition)
                        .setStartOffset(21)
                        .setDeliveryCompleteCount(10)
                        .setStateEpoch(1)))
                )
            );

        ReadShareGroupStateSummaryParameters readShareGroupStateSummaryParameters = ReadShareGroupStateSummaryParameters.from(readShareGroupStateSummaryRequestData);
        ReadShareGroupStateSummaryResult readShareGroupStateSummaryResult = ReadShareGroupStateSummaryResult.from(readShareGroupStateSummaryResponseData);
        when(persister.readSummary(
            ArgumentMatchers.eq(readShareGroupStateSummaryParameters)
        )).thenReturn(CompletableFuture.completedFuture(readShareGroupStateSummaryResult));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        CompletionException responseException = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(UnknownServerException.class, responseException.getCause());
        assertEquals("failure", responseException.getCause().getMessage());
    }

    @Test
    public void testDescribeShareGroupOffsetsWithDefaultPersisterLatestOffsetReturnsError() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);

        PartitionMetadataClient partitionMetadataClient = mock(PartitionMetadataClient.class);

        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .setPartitionMetadataClient(partitionMetadataClient)
            .build(true);
        service.startup(() -> 1);

        Set<TopicPartition> partitionsToComputeLag = new HashSet<>(Set.of(new TopicPartition(TOPIC_NAME, 1)));
        when(partitionMetadataClient.listLatestOffsets(partitionsToComputeLag))
            .thenReturn(Map.of(new TopicPartition(TOPIC_NAME, 1),
                CompletableFuture.completedFuture(new PartitionMetadataClient.OffsetResponse(-1, Errors.NETWORK_EXCEPTION))));

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        ReadShareGroupStateSummaryRequestData readShareGroupStateSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
            .setGroupId("share-group-id")
            .setTopics(List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateSummaryRequestData.PartitionData()
                    .setPartition(partition)))));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setTopics(
                List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                    .setTopicName(TOPIC_NAME)
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partition)
                        .setErrorCode(Errors.NETWORK_EXCEPTION.code())
                        .setErrorMessage(Errors.NETWORK_EXCEPTION.message())
                    ))
                )
            );

        ReadShareGroupStateSummaryResponseData readShareGroupStateSummaryResponseData = new ReadShareGroupStateSummaryResponseData()
            .setResults(
                List.of(new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                        .setPartition(partition)
                        .setStartOffset(21)
                        .setDeliveryCompleteCount(10)
                        .setStateEpoch(1)))
                )
            );

        ReadShareGroupStateSummaryParameters readShareGroupStateSummaryParameters = ReadShareGroupStateSummaryParameters.from(readShareGroupStateSummaryRequestData);
        ReadShareGroupStateSummaryResult readShareGroupStateSummaryResult = ReadShareGroupStateSummaryResult.from(readShareGroupStateSummaryResponseData);
        when(persister.readSummary(
            ArgumentMatchers.eq(readShareGroupStateSummaryParameters)
        )).thenReturn(CompletableFuture.completedFuture(readShareGroupStateSummaryResult));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupAllOffsetsLatestOffsetError() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);

        PartitionMetadataClient partitionMetadataClient = mock(PartitionMetadataClient.class);

        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .setPartitionMetadataClient(partitionMetadataClient)
            .build(true);

        Exception ex = new UnknownServerException("failure");

        Set<TopicPartition> partitionsToComputeLag = new HashSet<>(Set.of(new TopicPartition(TOPIC_NAME, 1)));
        when(partitionMetadataClient.listLatestOffsets(partitionsToComputeLag))
            .thenReturn(Map.of(new TopicPartition(TOPIC_NAME, 1), CompletableFuture.failedFuture(ex)));

        MetadataImage image = new MetadataImageBuilder()
            .addTopic(TOPIC_ID, TOPIC_NAME, 3)
            .build();

        service.onMetadataUpdate(new MetadataDelta(image), image);

        int partition = 1;

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-initialized-partitions"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of(TOPIC_ID, Set.of(partition))));

        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(null);

        ReadShareGroupStateSummaryRequestData readShareGroupStateSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
            .setGroupId("share-group-id")
            .setTopics(List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateSummaryRequestData.PartitionData()
                    .setPartition(partition)))));

        ReadShareGroupStateSummaryResponseData readShareGroupStateSummaryResponseData = new ReadShareGroupStateSummaryResponseData()
            .setResults(
                List.of(new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                        .setPartition(partition)
                        .setStartOffset(21)
                        .setDeliveryCompleteCount(10)
                        .setStateEpoch(1)))
                )
            );

        ReadShareGroupStateSummaryParameters readShareGroupStateSummaryParameters = ReadShareGroupStateSummaryParameters.from(readShareGroupStateSummaryRequestData);
        ReadShareGroupStateSummaryResult readShareGroupStateSummaryResult = ReadShareGroupStateSummaryResult.from(readShareGroupStateSummaryResponseData);
        when(persister.readSummary(
            ArgumentMatchers.eq(readShareGroupStateSummaryParameters)
        )).thenReturn(CompletableFuture.completedFuture(readShareGroupStateSummaryResult));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupAllOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        CompletionException responseException = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(UnknownServerException.class, responseException.getCause());
        assertEquals("failure", responseException.getCause().getMessage());
    }

    @Test
    public void testDescribeShareGroupOffsetsCoordinatorNotActive() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            .setErrorMessage(Errors.COORDINATOR_NOT_AVAILABLE.message());

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupOffsetsMetadataImageNull() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(false);

        service.startup(() -> 1);

        int partition = 1;
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
                .setPartitions(List.of(partition))
            ));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            .setErrorMessage(Errors.COORDINATOR_NOT_AVAILABLE.message());

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupAllOffsets() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);

        PartitionMetadataClient partitionMetadataClient = mock(PartitionMetadataClient.class);

        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .setPartitionMetadataClient(partitionMetadataClient)
            .build(true);

        Set<TopicPartition> partitionsToComputeLag = new HashSet<>(Set.of(new TopicPartition(TOPIC_NAME, 1)));
        when(partitionMetadataClient.listLatestOffsets(partitionsToComputeLag))
            .thenReturn(Map.of(new TopicPartition(TOPIC_NAME, 1),
                CompletableFuture.completedFuture(new PartitionMetadataClient.OffsetResponse(41L, Errors.NONE))));

        MetadataImage image = new MetadataImageBuilder()
            .addTopic(TOPIC_ID, TOPIC_NAME, 3)
            .build();

        service.onMetadataUpdate(new MetadataDelta(image), image);

        int partition = 1;

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-initialized-partitions"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of(TOPIC_ID, Set.of(partition))));

        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(null);

        ReadShareGroupStateSummaryRequestData readShareGroupStateSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
            .setGroupId("share-group-id")
            .setTopics(List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateSummaryRequestData.PartitionData()
                    .setPartition(partition)))));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setTopics(
                List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                    .setTopicName(TOPIC_NAME)
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partition)
                        .setStartOffset(21)
                        .setLag(10L))))
            );

        ReadShareGroupStateSummaryResponseData readShareGroupStateSummaryResponseData = new ReadShareGroupStateSummaryResponseData()
            .setResults(
                List.of(new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                        .setPartition(partition)
                        .setStartOffset(21)
                        .setDeliveryCompleteCount(10)
                        .setStateEpoch(1)))
                )
            );

        ReadShareGroupStateSummaryParameters readShareGroupStateSummaryParameters = ReadShareGroupStateSummaryParameters.from(readShareGroupStateSummaryRequestData);
        ReadShareGroupStateSummaryResult readShareGroupStateSummaryResult = ReadShareGroupStateSummaryResult.from(readShareGroupStateSummaryResponseData);
        when(persister.readSummary(
            ArgumentMatchers.eq(readShareGroupStateSummaryParameters)
        )).thenReturn(CompletableFuture.completedFuture(readShareGroupStateSummaryResult));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupAllOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupAllOffsetsThrowsError() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);

        MetadataImage image = new MetadataImageBuilder()
            .addTopic(TOPIC_ID, TOPIC_NAME, 3)
            .build();

        service.onMetadataUpdate(new MetadataDelta(image), image);

        int partition = 1;

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-initialized-partitions"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of(TOPIC_ID, Set.of(partition))));

        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(null);

        when(persister.readSummary(ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.failedFuture(new Exception("Unable to validate read state summary request")));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupAllOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);
        assertFutureThrows(Exception.class, future, "Unable to validate read state summary request");
    }

    @Test
    public void testDescribeShareGroupAllOffsetsNullResult() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);

        MetadataImage image = new MetadataImageBuilder()
            .addTopic(TOPIC_ID, TOPIC_NAME, 3)
            .build();

        service.onMetadataUpdate(new MetadataDelta(image), image);

        int partition = 1;

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-initialized-partitions"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of(TOPIC_ID, Set.of(partition))));

        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(null);

        when(persister.readSummary(ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupAllOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);
        assertFutureThrows(IllegalStateException.class, future, "Result is null for the read state summary");
    }

    @Test
    public void testDescribeShareGroupAllOffsetsReadSummaryPartitionError() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);

        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);

        MetadataImage image = new MetadataImageBuilder()
            .addTopic(TOPIC_ID, TOPIC_NAME, 3)
            .build();

        service.onMetadataUpdate(new MetadataDelta(image), image);

        int partition = 1;

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("share-group-initialized-partitions"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(Map.of(TOPIC_ID, Set.of(partition))));

        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(null);

        ReadShareGroupStateSummaryRequestData readShareGroupStateSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
            .setGroupId("share-group-id")
            .setTopics(List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new ReadShareGroupStateSummaryRequestData.PartitionData()
                    .setPartition(partition)))));

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setTopics(
                List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                    .setTopicName(TOPIC_NAME)
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partition)
                        .setStartOffset(PartitionFactory.UNINITIALIZED_START_OFFSET)
                        .setLeaderEpoch(PartitionFactory.DEFAULT_LEADER_EPOCH)
                        .setLag(PartitionFactory.UNINITIALIZED_LAG))))
            );

        ReadShareGroupStateSummaryResponseData readShareGroupStateSummaryResponseData = new ReadShareGroupStateSummaryResponseData()
            .setResults(
                List.of(new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                        .setPartition(partition)
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message())
                    ))
                )
            );

        ReadShareGroupStateSummaryParameters readShareGroupStateSummaryParameters = ReadShareGroupStateSummaryParameters.from(readShareGroupStateSummaryRequestData);
        ReadShareGroupStateSummaryResult readShareGroupStateSummaryResult = ReadShareGroupStateSummaryResult.from(readShareGroupStateSummaryResponseData);
        when(persister.readSummary(
            ArgumentMatchers.eq(readShareGroupStateSummaryParameters)
        )).thenReturn(CompletableFuture.completedFuture(readShareGroupStateSummaryResult));

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupAllOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupAllOffsetsCoordinatorNotActive() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(null);

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            .setErrorMessage(Errors.COORDINATOR_NOT_AVAILABLE.message());

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupAllOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDescribeShareGroupAllOffsetsMetadataImageNull() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(false);

        service.startup(() -> 1);

        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup requestData = new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
            .setGroupId("share-group-id")
            .setTopics(null);

        DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup responseData = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
            .setGroupId("share-group-id")
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            .setErrorMessage(Errors.COORDINATOR_NOT_AVAILABLE.message());

        CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> future =
            service.describeShareGroupAllOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsWithNoOpPersister() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        int partition = 1;
        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
            ));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setResponses(
                List.of(new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(TOPIC_NAME)
                    .setTopicId(TOPIC_ID)
                    .setErrorCode(PartitionFactory.DEFAULT_ERROR_CODE)
                    .setErrorMessage(null))
            );

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NONE.code(),
                null,
                List.of(),
                DeleteShareGroupStateParameters.from(
                    new DeleteShareGroupStateRequestData()
                        .setGroupId(groupId)
                        .setTopics(List.of(
                            new DeleteShareGroupStateRequestData.DeleteStateData()
                                .setTopicId(TOPIC_ID)
                                .setPartitions(List.of(
                                    new DeleteShareGroupStateRequestData.PartitionData()
                                        .setPartition(partition)
                                ))
                        ))
                )
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("complete-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(responseData));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsWithDefaultPersister() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        int partition = 1;
        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
            ));

        DeleteShareGroupStateRequestData deleteShareGroupStateRequestData = new DeleteShareGroupStateRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new DeleteShareGroupStateRequestData.PartitionData()
                    .setPartition(partition)))));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setResponses(
                List.of(new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(TOPIC_NAME)
                    .setTopicId(TOPIC_ID)
                    .setErrorCode(Errors.NONE.code())
                    .setErrorMessage(null))
            );

        DeleteShareGroupStateResponseData deleteShareGroupStateResponseData = new DeleteShareGroupStateResponseData()
            .setResults(
                List.of(new DeleteShareGroupStateResponseData.DeleteStateResult()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new DeleteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition)
                        .setErrorCode(Errors.NONE.code())
                        .setErrorMessage(null)))
                )
            );

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NONE.code(),
                null,
                List.of(),
                DeleteShareGroupStateParameters.from(deleteShareGroupStateRequestData)
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("complete-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(responseData));

        DeleteShareGroupStateParameters deleteShareGroupStateParameters = DeleteShareGroupStateParameters.from(deleteShareGroupStateRequestData);
        DeleteShareGroupStateResult deleteShareGroupStateResult = DeleteShareGroupStateResult.from(deleteShareGroupStateResponseData);
        when(persister.deleteState(
            ArgumentMatchers.eq(deleteShareGroupStateParameters)
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupStateResult));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsCoordinatorNotActive() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build();

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
            ));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            .setErrorMessage(Errors.COORDINATOR_NOT_AVAILABLE.message());

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsMetadataImageNull() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(false);

        service.startup(() -> 1);

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId("share-group-id")
            .setTopics(List.of(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
            ));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            .setErrorMessage(Errors.COORDINATOR_NOT_AVAILABLE.message());

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsInvalidGroupId() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId("")
            .setTopics(List.of(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
            ));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(Errors.INVALID_GROUP_ID.code())
            .setErrorMessage(Errors.INVALID_GROUP_ID.message());

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsEmptyRequest() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId("share-group-id")
            .setTopics(List.of());

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData();

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NONE.code(),
                null,
                null,
                null
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsEmptyTopicsInRequest() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId("share-group-id")
            .setTopics(List.of());

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData();

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NONE.code(),
                null,
                null,
                null
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsRequestThrowsError() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
            ));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
            .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message());

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(Errors.UNKNOWN_SERVER_ERROR.exception()));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsRequestNullResultHolder() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
            ));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
            .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message());

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsRequestReturnsGroupIdNotFound() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of());

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
            .setErrorMessage(Errors.GROUP_ID_NOT_FOUND.message());

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.GROUP_ID_NOT_FOUND.code(),
                Errors.GROUP_ID_NOT_FOUND.message(),
                null,
                null
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsRequestReturnsGroupNotEmpty() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
            ));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(Errors.NON_EMPTY_GROUP.code())
            .setErrorMessage(Errors.NON_EMPTY_GROUP.message());

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NON_EMPTY_GROUP.code(),
                Errors.NON_EMPTY_GROUP.message(),
                null,
                null
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsRequestReturnsNullParameters() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
            ));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData();

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NONE.code(),
                null,
                null,
                null
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsRequestReturnsNullParametersWithErrorTopics() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String badTopicName = "bad-topic";
        Uuid badTopicId = Uuid.randomUuid();
        String groupId = "share-group-id";

        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(TOPIC_NAME),
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(badTopicName)
            ));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setResponses(List.of(new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                .setTopicName(badTopicName)
                .setTopicId(badTopicId)
                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message())
            ));

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NONE.code(),
                null,
                List.of(new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(badTopicName)
                    .setTopicId(badTopicId)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message())
                    ),
                null
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsPersisterThrowsError() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        int partition = 1;
        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
            ));

        Exception persisterException = new Exception("Unable to validate delete share group state request");

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(Errors.forException(persisterException).code())
            .setErrorMessage(Errors.forException(persisterException).message());

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NONE.code(),
                null,
                List.of(),
                DeleteShareGroupStateParameters.from(
                    new DeleteShareGroupStateRequestData()
                        .setGroupId(groupId)
                        .setTopics(List.of(
                            new DeleteShareGroupStateRequestData.DeleteStateData()
                                .setTopicId(TOPIC_ID)
                                .setPartitions(List.of(
                                    new DeleteShareGroupStateRequestData.PartitionData()
                                        .setPartition(partition)
                                ))
                        ))
                )
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        when(persister.deleteState(ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.failedFuture(persisterException));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsPersisterReturnsNull() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        int partition = 1;
        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
            ));

        Exception persisterException = new IllegalStateException("Result is null for the delete share group state");

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(Errors.forException(persisterException).code())
            .setErrorMessage(Errors.forException(persisterException).message());

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NONE.code(),
                null,
                List.of(),
                DeleteShareGroupStateParameters.from(
                    new DeleteShareGroupStateRequestData()
                        .setGroupId(groupId)
                        .setTopics(List.of(
                            new DeleteShareGroupStateRequestData.DeleteStateData()
                                .setTopicId(TOPIC_ID)
                                .setPartitions(List.of(
                                    new DeleteShareGroupStateRequestData.PartitionData()
                                        .setPartition(partition)
                                ))
                        ))
                )
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        when(persister.deleteState(ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsPersisterReturnsNullTopicData() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        int partition = 1;
        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME)
            ));

        Exception persisterException = new IllegalStateException("Result is null for the delete share group state");

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(Errors.forException(persisterException).code())
            .setErrorMessage(Errors.forException(persisterException).message());

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NONE.code(),
                null,
                List.of(),
                DeleteShareGroupStateParameters.from(
                    new DeleteShareGroupStateRequestData()
                        .setGroupId(groupId)
                        .setTopics(List.of(
                            new DeleteShareGroupStateRequestData.DeleteStateData()
                                .setTopicId(TOPIC_ID)
                                .setPartitions(List.of(
                                    new DeleteShareGroupStateRequestData.PartitionData()
                                        .setPartition(partition)
                                ))
                        ))
                )
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        DeleteShareGroupStateResult deleteShareGroupStateResult =
            new DeleteShareGroupStateResult.Builder().setTopicsData(null).build();

        when(persister.deleteState(ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.completedFuture(deleteShareGroupStateResult));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsPersisterReturnsNoSuccessfulTopics() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        String badTopicName = "bad-topic";
        Uuid badTopicId = Uuid.randomUuid();

        int partition = 1;
        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                .setTopicName(TOPIC_NAME),
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(badTopicName)
            ));

        DeleteShareGroupStateRequestData deleteShareGroupStateRequestData = new DeleteShareGroupStateRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new DeleteShareGroupStateRequestData.PartitionData()
                    .setPartition(partition)))));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setResponses(List.of(
                new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(badTopicName)
                    .setTopicId(badTopicId)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message()),
                new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(TOPIC_NAME)
                    .setTopicId(TOPIC_ID)
                    .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message())
            ));

        DeleteShareGroupStateResponseData deleteShareGroupStateResponseData = new DeleteShareGroupStateResponseData()
            .setResults(
                List.of(new DeleteShareGroupStateResponseData.DeleteStateResult()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(new DeleteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition)
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message())))
                )
            );

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NONE.code(),
                null,
                List.of(
                    new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                        .setTopicId(badTopicId)
                        .setTopicName(badTopicName)
                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                        .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message())
                ),
                DeleteShareGroupStateParameters.from(deleteShareGroupStateRequestData)
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        DeleteShareGroupStateParameters deleteShareGroupStateParameters = DeleteShareGroupStateParameters.from(deleteShareGroupStateRequestData);
        DeleteShareGroupStateResult deleteShareGroupStateResult = DeleteShareGroupStateResult.from(deleteShareGroupStateResponseData);
        when(persister.deleteState(
            ArgumentMatchers.eq(deleteShareGroupStateParameters)
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupStateResult));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsCompleteDeleteStateReturnsError() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        int partition = 1;
        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(TOPIC_NAME)
            ));

        DeleteShareGroupStateRequestData deleteShareGroupStateRequestData = new DeleteShareGroupStateRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(new DeleteShareGroupStateRequestData.DeleteStateData()
                .setTopicId(TOPIC_ID)
                .setPartitions(List.of(new DeleteShareGroupStateRequestData.PartitionData()
                    .setPartition(partition)))));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
            .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message());

        DeleteShareGroupStateResponseData deleteShareGroupStateResponseData = new DeleteShareGroupStateResponseData()
            .setResults(
                List.of(new DeleteShareGroupStateResponseData.DeleteStateResult()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(
                        new DeleteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition)
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage(null)
                    ))
                )
            );

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NONE.code(),
                null,
                List.of(),
                DeleteShareGroupStateParameters.from(deleteShareGroupStateRequestData)
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        DeleteShareGroupStateParameters deleteShareGroupStateParameters = DeleteShareGroupStateParameters.from(deleteShareGroupStateRequestData);
        DeleteShareGroupStateResult deleteShareGroupStateResult = DeleteShareGroupStateResult.from(deleteShareGroupStateResponseData);
        when(persister.deleteState(
            ArgumentMatchers.eq(deleteShareGroupStateParameters)
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupStateResult));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("complete-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(Errors.UNKNOWN_SERVER_ERROR.exception()));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testDeleteShareGroupOffsetsSuccessWithErrorTopics() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group-id";

        String badTopicName1 = "bad-topic-1";
        Uuid badTopicId1 = Uuid.randomUuid();
        String badTopicName2 = "bad-topic-2";
        Uuid badTopicId2 = Uuid.randomUuid();

        int partition = 1;
        DeleteShareGroupOffsetsRequestData requestData = new DeleteShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(TOPIC_NAME),
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(badTopicName1),
                new DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic()
                    .setTopicName(badTopicName2)
            ));

        DeleteShareGroupStateRequestData deleteShareGroupStateRequestData = new DeleteShareGroupStateRequestData()
            .setGroupId(groupId)
            .setTopics(List.of(
                new DeleteShareGroupStateRequestData.DeleteStateData()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(
                        new DeleteShareGroupStateRequestData.PartitionData()
                            .setPartition(partition)
                    )),
                new DeleteShareGroupStateRequestData.DeleteStateData()
                    .setTopicId(badTopicId2)
                    .setPartitions(List.of(
                        new DeleteShareGroupStateRequestData.PartitionData()
                            .setPartition(partition)
                    ))
            ));

        DeleteShareGroupOffsetsResponseData responseData = new DeleteShareGroupOffsetsResponseData()
            .setResponses(List.of(
                new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(badTopicName1)
                    .setTopicId(badTopicId1)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message()),
                new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(badTopicName2)
                    .setTopicId(badTopicId2)
                    .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                    .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message()),
                new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                    .setTopicName(TOPIC_NAME)
                    .setTopicId(TOPIC_ID)
                    .setErrorCode(Errors.NONE.code())
                    .setErrorMessage(null)
            ));

        DeleteShareGroupStateResponseData deleteShareGroupStateResponseData = new DeleteShareGroupStateResponseData()
            .setResults(List.of(
                new DeleteShareGroupStateResponseData.DeleteStateResult()
                    .setTopicId(badTopicId2)
                    .setPartitions(List.of(
                        new DeleteShareGroupStateResponseData.PartitionResult()
                            .setPartition(partition)
                            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                            .setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message())
                    )),
                new DeleteShareGroupStateResponseData.DeleteStateResult()
                    .setTopicId(TOPIC_ID)
                    .setPartitions(List.of(
                        new DeleteShareGroupStateResponseData.PartitionResult()
                            .setPartition(partition)
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage(null)
                    ))
            ));

        GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder deleteShareGroupOffsetsResultHolder =
            new GroupCoordinatorShard.DeleteShareGroupOffsetsResultHolder(
                Errors.NONE.code(),
                null,
                List.of(
                    new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                        .setTopicId(badTopicId1)
                        .setTopicName(badTopicName1)
                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                        .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message())
                ),
                DeleteShareGroupStateParameters.from(deleteShareGroupStateRequestData)
            );

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initiate-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupOffsetsResultHolder));

        DeleteShareGroupStateParameters deleteShareGroupStateParameters = DeleteShareGroupStateParameters.from(deleteShareGroupStateRequestData);
        DeleteShareGroupStateResult deleteShareGroupStateResult = DeleteShareGroupStateResult.from(deleteShareGroupStateResponseData);
        when(persister.deleteState(
            ArgumentMatchers.eq(deleteShareGroupStateParameters)
        )).thenReturn(CompletableFuture.completedFuture(deleteShareGroupStateResult));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("complete-delete-share-group-offsets"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(responseData));

        CompletableFuture<DeleteShareGroupOffsetsResponseData> future =
            service.deleteShareGroupOffsets(requestContext(ApiKeys.DELETE_SHARE_GROUP_OFFSETS), requestData);

        assertEquals(responseData, future.get());
    }

    @Test
    public void testPersisterInitializeSuccess() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister mockPersister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(mockPersister)
            .build(true);

        String groupId = "share-group";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId, "topic-name", 3)
            .build();

        service.onMetadataUpdate(new MetadataDelta(image), image);

        when(mockPersister.initializeState(ArgumentMatchers.any())).thenReturn(CompletableFuture.completedFuture(
            new InitializeShareGroupStateResult.Builder()
                .setTopicsData(List.of(
                    new TopicData<>(topicId, List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())
                    ))
                )).build()
        ));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        ShareGroupHeartbeatResponseData defaultResponse = new ShareGroupHeartbeatResponseData();
        InitializeShareGroupStateParameters params = new InitializeShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(
                new GroupTopicPartitionData<>(groupId,
                    List.of(
                        new TopicData<>(topicId, List.of(PartitionFactory.newPartitionStateData(0, 0, -1)))
                    ))
            )
            .build();

        assertEquals(defaultResponse, service.persisterInitialize(params, defaultResponse).getNow(null));
        verify(runtime, times(1)).scheduleWriteOperation(
            ArgumentMatchers.eq("initialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        );
        verify(mockPersister, times(1)).initializeState(ArgumentMatchers.any());
    }

    @Test
    public void testPersisterInitializeFailure() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister mockPersister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(mockPersister)
            .build(true);

        String groupId = "share-group";
        Uuid topicId = Uuid.randomUuid();
        Exception exp = new NotCoordinatorException("bad stuff");

        when(mockPersister.initializeState(ArgumentMatchers.any())).thenReturn(CompletableFuture.failedFuture(exp));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("uninitialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        ShareGroupHeartbeatResponseData defaultResponse = new ShareGroupHeartbeatResponseData();
        InitializeShareGroupStateParameters params = new InitializeShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(
                new GroupTopicPartitionData<>(groupId,
                    List.of(
                        new TopicData<>(topicId, List.of(PartitionFactory.newPartitionStateData(0, 0, -1)))
                    ))
            ).build();

        assertEquals(Errors.forException(exp).code(), service.persisterInitialize(params, defaultResponse).getNow(null).errorCode());
        verify(runtime, times(0)).scheduleWriteOperation(
            ArgumentMatchers.eq("initialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        );
        verify(runtime, times(1)).scheduleWriteOperation(
            ArgumentMatchers.eq("uninitialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        );
        verify(mockPersister, times(1)).initializeState(ArgumentMatchers.any());
    }

    @Test
    public void testPersisterInitializePartialFailure() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister mockPersister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(mockPersister)
            .build(true);

        String groupId = "share-group";
        Uuid topicId = Uuid.randomUuid();

        when(mockPersister.initializeState(ArgumentMatchers.any())).thenReturn(CompletableFuture.completedFuture(
            new InitializeShareGroupStateResult.Builder()
                .setTopicsData(List.of(
                    new TopicData<>(topicId, List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message()),
                        PartitionFactory.newPartitionErrorData(1, Errors.TOPIC_AUTHORIZATION_FAILED.code(), Errors.TOPIC_AUTHORIZATION_FAILED.message())
                    ))
                )).build()
        ));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("uninitialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        ShareGroupHeartbeatResponseData defaultResponse = new ShareGroupHeartbeatResponseData();
        InitializeShareGroupStateParameters params = new InitializeShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(
                new GroupTopicPartitionData<>(groupId,
                    List.of(
                        new TopicData<>(topicId, List.of(
                            PartitionFactory.newPartitionStateData(0, 0, -1),
                            PartitionFactory.newPartitionStateData(1, 0, -1)
                        ))
                    ))
            ).build();

        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), service.persisterInitialize(params, defaultResponse).getNow(null).errorCode());
        verify(runtime, times(0)).scheduleWriteOperation(
            ArgumentMatchers.eq("initialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        );
        verify(runtime, times(1)).scheduleWriteOperation(
            ArgumentMatchers.eq("uninitialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        );
        verify(mockPersister, times(1)).initializeState(ArgumentMatchers.any());
    }

    @Test
    public void testPersisterInitializeGroupInitializeFailure() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister mockPersister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(mockPersister)
            .build(true);

        String groupId = "share-group";
        Uuid topicId = Uuid.randomUuid();
        Exception exp = new CoordinatorNotAvailableException("bad stuff");
        MetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId, "topic-name", 3)
            .build();

        service.onMetadataUpdate(new MetadataDelta(image), image);

        when(mockPersister.initializeState(ArgumentMatchers.any())).thenReturn(CompletableFuture.completedFuture(
            new InitializeShareGroupStateResult.Builder()
                .setTopicsData(List.of(
                    new TopicData<>(topicId, List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())
                    ))
                )).build()
        ));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(exp));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("uninitialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        ShareGroupHeartbeatResponseData defaultResponse = new ShareGroupHeartbeatResponseData();
        InitializeShareGroupStateParameters params = new InitializeShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(
                new GroupTopicPartitionData<>(groupId,
                    List.of(
                        new TopicData<>(topicId, List.of(
                            PartitionFactory.newPartitionStateData(0, 0, -1)
                        ))
                    ))
            ).build();

        assertEquals(Errors.forException(exp).code(), service.persisterInitialize(params, defaultResponse).getNow(null).errorCode());

        verify(runtime, times(1)).scheduleWriteOperation(
            ArgumentMatchers.eq("initialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        );
        verify(runtime, times(1)).scheduleWriteOperation(
            ArgumentMatchers.eq("uninitialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        );
        verify(mockPersister, times(1)).initializeState(ArgumentMatchers.any());
    }

    @Test
    public void testAlterShareGroupOffsetsMetadataImageNull() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .build(false);

        service.startup(() -> 1);

        String groupId = "share-group";
        AlterShareGroupOffsetsRequestData request = new AlterShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(null);

        AlterShareGroupOffsetsResponseData response = new AlterShareGroupOffsetsResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                .setErrorMessage(Errors.COORDINATOR_NOT_AVAILABLE.message());

        CompletableFuture<AlterShareGroupOffsetsResponseData> future = service.alterShareGroupOffsets(
            requestContext(ApiKeys.ALTER_SHARE_GROUP_OFFSETS),
            groupId,
            request
        );
        assertEquals(response, future.get());
    }

    @Test
    public void testAlterShareGroupOffsetsInvalidGroupId() throws InterruptedException, ExecutionException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build(true);

        String groupId = "";
        AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopicCollection requestTopicCollection =
            new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopicCollection(List.of(
                new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopic()
                    .setTopicName(TOPIC_NAME)
                    .setPartitions(List.of(
                        new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
                            .setPartitionIndex(0)
                            .setStartOffset(0L)
                    ))
            ).iterator());
        AlterShareGroupOffsetsRequestData request = new AlterShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(requestTopicCollection);

        AlterShareGroupOffsetsResponseData response = new AlterShareGroupOffsetsResponseData()
            .setErrorCode(Errors.INVALID_GROUP_ID.code())
            .setErrorMessage(Errors.INVALID_GROUP_ID.message());

        CompletableFuture<AlterShareGroupOffsetsResponseData> future = service.alterShareGroupOffsets(
            requestContext(ApiKeys.ALTER_SHARE_GROUP_OFFSETS),
            groupId,
            request
        );
        assertEquals(response, future.get());
    }

    @Test
    public void testAlterShareGroupOffsetsEmptyRequest() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setMetrics(mock(GroupCoordinatorMetrics.class))
            .build(true);

        String groupId = "share-group";
        AlterShareGroupOffsetsRequestData request = new AlterShareGroupOffsetsRequestData()
            .setGroupId(groupId);

        AlterShareGroupOffsetsResponseData data = new AlterShareGroupOffsetsResponseData();

        Map.Entry<AlterShareGroupOffsetsResponseData, InitializeShareGroupStateParameters> alterShareGroupOffsetsIntermediate =
            Map.entry(
                new AlterShareGroupOffsetsResponseData()
                    .setResponses(new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopicCollection()),
                new InitializeShareGroupStateParameters.Builder()
                    .setGroupTopicPartitionData(new GroupTopicPartitionData<>("share-group", List.of()))
                    .build());

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("share-group-offsets-alter"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(alterShareGroupOffsetsIntermediate));

        CompletableFuture<AlterShareGroupOffsetsResponseData> future = service.alterShareGroupOffsets(
            requestContext(ApiKeys.ALTER_SHARE_GROUP_OFFSETS),
            groupId,
            request
        );

        assertEquals(data, future.get());
    }

    @Test
    public void testAlterShareGroupOffsetsRequestReturnsGroupNotEmpty() throws ExecutionException, InterruptedException {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister persister = mock(DefaultStatePersister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(persister)
            .build(true);
        service.startup(() -> 1);

        String groupId = "share-group";

        AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopicCollection requestTopicCollection =
            new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopicCollection(List.of(
                new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopic()
                    .setTopicName(TOPIC_NAME)
                    .setPartitions(List.of(
                        new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestPartition()
                            .setPartitionIndex(0)
                            .setStartOffset(0L)
                    ))
            ).iterator());

        AlterShareGroupOffsetsRequestData request = new AlterShareGroupOffsetsRequestData()
            .setGroupId(groupId)
            .setTopics(requestTopicCollection);

        AlterShareGroupOffsetsResponseData response = new AlterShareGroupOffsetsResponseData()
            .setErrorCode(Errors.NON_EMPTY_GROUP.code())
            .setErrorMessage("bad stuff");

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("share-group-offsets-alter"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.failedFuture(
            new GroupNotEmptyException("bad stuff")
        ));

        CompletableFuture<AlterShareGroupOffsetsResponseData> future =
            service.alterShareGroupOffsets(requestContext(ApiKeys.ALTER_SHARE_GROUP_OFFSETS), groupId, request);
        assertEquals(response, future.get());
    }

    @Test
    public void testPersisterInitializeForAlterShareGroupOffsetsResponseSuccess() {
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Persister mockPersister = mock(Persister.class);
        GroupCoordinatorService service = new GroupCoordinatorServiceBuilder()
            .setConfig(createConfig())
            .setRuntime(runtime)
            .setPersister(mockPersister)
            .build(true);

        String groupId = "share-group";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage image = new MetadataImageBuilder()
            .addTopic(topicId, "topic-name", 1)
            .build();

        service.onMetadataUpdate(new MetadataDelta(image), image);

        when(mockPersister.initializeState(ArgumentMatchers.any())).thenReturn(CompletableFuture.completedFuture(
            new InitializeShareGroupStateResult.Builder()
                .setTopicsData(List.of(
                    new TopicData<>(topicId, List.of(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code(), Errors.NONE.message())
                    ))
                )).build()
        ));

        AlterShareGroupOffsetsResponseData defaultResponse = new AlterShareGroupOffsetsResponseData();
        InitializeShareGroupStateParameters params = new InitializeShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(
                new GroupTopicPartitionData<>(groupId,
                    List.of(
                        new TopicData<>(topicId, List.of(PartitionFactory.newPartitionStateData(0, 0, 0)))
                    ))
            ).build();

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("initialize-share-group-state"),
            ArgumentMatchers.eq(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        )).thenReturn(CompletableFuture.completedFuture(null));

        assertEquals(defaultResponse, service.persisterInitialize(params, defaultResponse).getNow(null));
        verify(runtime, times(1)).scheduleWriteOperation(
            ArgumentMatchers.eq("initialize-share-group-state"),
            ArgumentMatchers.any(),
            ArgumentMatchers.any()
        );
        verify(mockPersister, times(1)).initializeState(ArgumentMatchers.any());
    }

    private static class GroupCoordinatorServiceBuilder {
        private final LogContext logContext = new LogContext();
        private final GroupConfigManager configManager = createConfigManager();
        private GroupCoordinatorConfig config;
        private CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime;
        private GroupCoordinatorMetrics metrics = new GroupCoordinatorMetrics();
        private Persister persister = new NoOpStatePersister();
        private MetadataImage metadataImage = null;
        private PartitionMetadataClient partitionMetadataClient = null;

        GroupCoordinatorService build() {
            return build(false);
        }

        GroupCoordinatorService build(boolean serviceStartup) {
            if (metadataImage == null) {
                metadataImage = new MetadataImageBuilder()
                    .addTopic(TOPIC_ID, TOPIC_NAME, 1)
                    .build();
            }

            var service = new GroupCoordinatorService(
                logContext,
                config,
                runtime,
                metrics,
                configManager,
                persister,
                new MockTimer(),
                partitionMetadataClient
            );

            if (serviceStartup) {
                service.startup(() -> 1);
                service.onMetadataUpdate(new MetadataDelta(metadataImage), metadataImage);
            }

            return service;
        }

        public GroupCoordinatorServiceBuilder setConfig(GroupCoordinatorConfig config) {
            this.config = config;
            return this;
        }

        public GroupCoordinatorServiceBuilder setRuntime(CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime) {
            this.runtime = runtime;
            return this;
        }

        public GroupCoordinatorServiceBuilder setPersister(Persister persister) {
            this.persister = persister;
            return this;
        }

        public GroupCoordinatorServiceBuilder setMetrics(GroupCoordinatorMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public GroupCoordinatorServiceBuilder setPartitionMetadataClient(PartitionMetadataClient partitionMetadataClient) {
            this.partitionMetadataClient = partitionMetadataClient;
            return this;
        }
    }

    private static DeleteShareGroupStateParameters createDeleteShareRequest(String groupId, Uuid topic, List<Integer> partitions) {
        TopicData<PartitionIdData> topicData = new TopicData<>(topic,
            partitions.stream().map(PartitionFactory::newPartitionIdData).toList()
        );

        return new DeleteShareGroupStateParameters.Builder()
            .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdData>()
                .setGroupId(groupId)
                .setTopicsData(List.of(topicData))
                .build())
            .build();
    }
}
