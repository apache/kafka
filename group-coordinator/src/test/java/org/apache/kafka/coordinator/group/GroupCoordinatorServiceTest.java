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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.InvalidFetchSizeException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.util.FutureUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;

import java.util.Collections;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.apache.kafka.coordinator.group.TestUtil.requestContext;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GroupCoordinatorServiceTest {

    @SuppressWarnings("unchecked")
    private CoordinatorRuntime<ReplicatedGroupCoordinator, Record> mockRuntime() {
        return (CoordinatorRuntime<ReplicatedGroupCoordinator, Record>) mock(CoordinatorRuntime.class);
    }

    private GroupCoordinatorConfig createConfig() {
        return new GroupCoordinatorConfig(
            1,
            45,
            5,
            Integer.MAX_VALUE,
            Collections.singletonList(new RangeAssignor()),
            1000
        );
    }

    @Test
    public void testStartupShutdown() throws Exception {
        CoordinatorRuntime<ReplicatedGroupCoordinator, Record> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorService(
            new LogContext(),
            createConfig(),
            runtime
        );

        service.startup(() -> 1);
        service.shutdown();

        verify(runtime, times(1)).close();
    }

    @Test
    public void testConsumerGroupHeartbeatWhenNotStarted() {
        CoordinatorRuntime<ReplicatedGroupCoordinator, Record> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorService(
            new LogContext(),
            createConfig(),
            runtime
        );

        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData()
            .setGroupId("foo");

        assertFutureThrows(
            service.consumerGroupHeartbeat(
                requestContext(ApiKeys.CONSUMER_GROUP_HEARTBEAT),
                request
            ),
            CoordinatorNotAvailableException.class
        );
    }

    @Test
    public void testConsumerGroupHeartbeat() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ReplicatedGroupCoordinator, Record> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorService(
            new LogContext(),
            createConfig(),
            runtime
        );

        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData()
            .setGroupId("foo");

        service.startup(() -> 1);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("consumer-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition("__consumer_offsets", 0)),
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

    private static Stream<Arguments> testConsumerGroupHeartbeatWithExceptionSource() {
        return Stream.of(
            Arguments.arguments(new UnknownTopicOrPartitionException(), Errors.COORDINATOR_NOT_AVAILABLE.code(), null),
            Arguments.arguments(new NotEnoughReplicasException(), Errors.COORDINATOR_NOT_AVAILABLE.code(), null),
            Arguments.arguments(new NotLeaderOrFollowerException(), Errors.NOT_COORDINATOR.code(), null),
            Arguments.arguments(new KafkaStorageException(), Errors.NOT_COORDINATOR.code(), null),
            Arguments.arguments(new RecordTooLargeException(), Errors.UNKNOWN_SERVER_ERROR.code(), null),
            Arguments.arguments(new RecordBatchTooLargeException(), Errors.UNKNOWN_SERVER_ERROR.code(), null),
            Arguments.arguments(new InvalidFetchSizeException(""), Errors.UNKNOWN_SERVER_ERROR.code(), null),
            Arguments.arguments(new InvalidRequestException("Invalid"), Errors.INVALID_REQUEST.code(), "Invalid")
        );
    }

    @ParameterizedTest
    @MethodSource("testConsumerGroupHeartbeatWithExceptionSource")
    public void testConsumerGroupHeartbeatWithException(
        Throwable exception,
        short expectedErrorCode,
        String expectedErrorMessage
    ) throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ReplicatedGroupCoordinator, Record> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorService(
            new LogContext(),
            createConfig(),
            runtime
        );

        ConsumerGroupHeartbeatRequestData request = new ConsumerGroupHeartbeatRequestData()
            .setGroupId("foo");

        service.startup(() -> 1);

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("consumer-group-heartbeat"),
            ArgumentMatchers.eq(new TopicPartition("__consumer_offsets", 0)),
            ArgumentMatchers.any()
        )).thenReturn(FutureUtils.failedFuture(exception));

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
    public void testPartitionFor() {
        CoordinatorRuntime<ReplicatedGroupCoordinator, Record> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorService(
            new LogContext(),
            createConfig(),
            runtime
        );

        assertThrows(CoordinatorNotAvailableException.class,
            () -> service.partitionFor("foo"));

        service.startup(() -> 10);

        assertEquals(Utils.abs("foo".hashCode()) % 10, service.partitionFor("foo"));
    }

    @Test
    public void testGroupMetadataTopicConfigs() {
        CoordinatorRuntime<ReplicatedGroupCoordinator, Record> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorService(
            new LogContext(),
            createConfig(),
            runtime
        );

        Properties expectedProperties = new Properties();
        expectedProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        expectedProperties.put(TopicConfig.COMPRESSION_TYPE_CONFIG, BrokerCompressionType.PRODUCER.name);
        expectedProperties.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1000");

        assertEquals(expectedProperties, service.groupMetadataTopicConfigs());
    }

    @Test
    public void testOnElection() {
        CoordinatorRuntime<ReplicatedGroupCoordinator, Record> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorService(
            new LogContext(),
            createConfig(),
            runtime
        );

        assertThrows(CoordinatorNotAvailableException.class,
            () -> service.onElection(5, 10));

        service.startup(() -> 1);
        service.onElection(5, 10);

        verify(runtime, times(1)).scheduleLoadOperation(
            new TopicPartition("__consumer_offsets", 5),
            10
        );
    }

    @Test
    public void testOnResignation() {
        CoordinatorRuntime<ReplicatedGroupCoordinator, Record> runtime = mockRuntime();
        GroupCoordinatorService service = new GroupCoordinatorService(
            new LogContext(),
            createConfig(),
            runtime
        );

        assertThrows(CoordinatorNotAvailableException.class,
            () -> service.onResignation(5, OptionalInt.of(10)));

        service.startup(() -> 1);
        service.onResignation(5, OptionalInt.of(10));

        verify(runtime, times(1)).scheduleUnloadOperation(
            new TopicPartition("__consumer_offsets", 5),
            10
        );
    }
}
