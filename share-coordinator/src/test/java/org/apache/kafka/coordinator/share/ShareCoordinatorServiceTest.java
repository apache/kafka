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

package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics;
import org.apache.kafka.server.util.FutureUtils;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.coordinator.common.runtime.TestUtil.requestContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ShareCoordinatorServiceTest {

    @SuppressWarnings("unchecked")
    private CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> mockRuntime() {
        return (CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord>) mock(CoordinatorRuntime.class);
    }

    @Test
    public void testStartupShutdown() throws Exception {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorConfigTest.createConfig(ShareCoordinatorConfigTest.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM
        );

        service.startup(() -> 1);
        service.shutdown();

        verify(runtime, times(1)).close();
    }

    @Test
    public void testWriteStateSuccess() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Metrics metrics = new Metrics();
        ShareCoordinatorMetrics coordinatorMetrics = new ShareCoordinatorMetrics(metrics);
        Time time = mock(Time.class);
        when(time.hiResClockMs()).thenReturn(0L).thenReturn(100L).thenReturn(150L);
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorConfigTest.createConfig(ShareCoordinatorConfigTest.testConfigMap()),
            runtime,
            coordinatorMetrics,
            time
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId1 = Uuid.randomUuid();
        int partition1 = 0;

        Uuid topicId2 = Uuid.randomUuid();
        int partition2 = 1;

        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(groupId)
            .setTopics(Arrays.asList(
                    new WriteShareGroupStateRequestData.WriteStateData()
                        .setTopicId(topicId1)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition1)
                                .setStartOffset(0)
                                .setStateEpoch(1)
                                .setLeaderEpoch(1)
                                .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                                    .setFirstOffset(0)
                                    .setLastOffset(10)
                                    .setDeliveryCount((short) 1)
                                    .setDeliveryState((byte) 0))
                                )
                        )),
                    new WriteShareGroupStateRequestData.WriteStateData()
                        .setTopicId(topicId2)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition2)
                                .setStartOffset(0)
                                .setStateEpoch(1)
                                .setLeaderEpoch(1)
                                .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                                    .setFirstOffset(0)
                                    .setLastOffset(10)
                                    .setDeliveryCount((short) 1)
                                    .setDeliveryState((byte) 0))
                                )
                        ))
                )
            );

        WriteShareGroupStateResponseData response1 = new WriteShareGroupStateResponseData()
            .setResults(Collections.singletonList(
                new WriteShareGroupStateResponseData.WriteStateResult()
                    .setTopicId(topicId1)
                    .setPartitions(Collections.singletonList(new WriteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition1)))
            ));

        WriteShareGroupStateResponseData response2 = new WriteShareGroupStateResponseData()
            .setResults(Collections.singletonList(
                new WriteShareGroupStateResponseData.WriteStateResult()
                    .setTopicId(topicId2)
                    .setPartitions(Collections.singletonList(new WriteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition2)))
            ));

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("write-share-group-state"),
            ArgumentMatchers.eq(new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0)),
            ArgumentMatchers.eq(Duration.ofMillis(5000)),
            ArgumentMatchers.any()
        ))
            .thenReturn(CompletableFuture.completedFuture(response1))
            .thenReturn(CompletableFuture.completedFuture(response2));

        CompletableFuture<WriteShareGroupStateResponseData> future = service.writeState(
            requestContext(ApiKeys.WRITE_SHARE_GROUP_STATE),
            request
        );

        HashSet<WriteShareGroupStateResponseData.WriteStateResult> result = new HashSet<>(future.get(5, TimeUnit.SECONDS).results());

        HashSet<WriteShareGroupStateResponseData.WriteStateResult> expectedResult = new HashSet<>(Arrays.asList(
            new WriteShareGroupStateResponseData.WriteStateResult()
                .setTopicId(topicId2)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition2))),
            new WriteShareGroupStateResponseData.WriteStateResult()
                .setTopicId(topicId1)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition1)))));
        assertEquals(expectedResult, result);
        verify(time, times(2)).hiResClockMs();
        Set<MetricName> expectedMetrics = new HashSet<>(Arrays.asList(
            metrics.metricName("write-latency-avg", ShareCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("write-latency-max", ShareCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("write-rate", ShareCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("write-total", ShareCoordinatorMetrics.METRICS_GROUP)
        ));
        expectedMetrics.forEach(metric -> assertTrue(metrics.metrics().containsKey(metric)));
    }

    @Test
    public void testReadStateSuccess() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorConfigTest.createConfig(ShareCoordinatorConfigTest.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId1 = Uuid.randomUuid();
        int partition1 = 0;

        Uuid topicId2 = Uuid.randomUuid();
        int partition2 = 1;

        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
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
                )
            );

        ReadShareGroupStateResponseData.ReadStateResult topicData1 = new ReadShareGroupStateResponseData.ReadStateResult()
            .setTopicId(topicId1)
            .setPartitions(Collections.singletonList(new ReadShareGroupStateResponseData.PartitionResult()
                .setPartition(partition1)
                .setErrorCode(Errors.NONE.code())
                .setStateEpoch(1)
                .setStartOffset(0)
                .setStateBatches(Collections.singletonList(new ReadShareGroupStateResponseData.StateBatch()
                    .setFirstOffset(0)
                    .setLastOffset(10)
                    .setDeliveryCount((short) 1)
                    .setDeliveryState((byte) 0))
                ))
            );

        ReadShareGroupStateResponseData.ReadStateResult topicData2 = new ReadShareGroupStateResponseData.ReadStateResult()
            .setTopicId(topicId2)
            .setPartitions(Collections.singletonList(new ReadShareGroupStateResponseData.PartitionResult()
                .setPartition(partition2)
                .setErrorCode(Errors.NONE.code())
                .setStateEpoch(1)
                .setStartOffset(0)
                .setStateBatches(Arrays.asList(
                    new ReadShareGroupStateResponseData.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0),
                    new ReadShareGroupStateResponseData.StateBatch()
                        .setFirstOffset(11)
                        .setLastOffset(20)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0)
                )))
            );

        when(runtime.scheduleWriteOperation(any(), any(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        when(runtime.scheduleReadOperation(
            ArgumentMatchers.eq("read-share-group-state"),
            ArgumentMatchers.eq(new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0)),
            ArgumentMatchers.any()
        ))
            .thenReturn(CompletableFuture.completedFuture(new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(topicData1))))
            .thenReturn(CompletableFuture.completedFuture(new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(topicData2))));

        CompletableFuture<ReadShareGroupStateResponseData> future = service.readState(
            requestContext(ApiKeys.READ_SHARE_GROUP_STATE),
            request
        );

        HashSet<ReadShareGroupStateResponseData.ReadStateResult> result = new HashSet<>(future.get(5, TimeUnit.SECONDS).results());

        HashSet<ReadShareGroupStateResponseData.ReadStateResult> expectedResult = new HashSet<>(Arrays.asList(
            topicData1,
            topicData2));
        assertEquals(expectedResult, result);
    }

    @Test
    public void testWriteStateValidationsError() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorConfigTest.createConfig(ShareCoordinatorConfigTest.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        // 1. Empty topicsData
        assertEquals(new WriteShareGroupStateResponseData(),
            service.writeState(
                requestContext(ApiKeys.WRITE_SHARE_GROUP_STATE),
                new WriteShareGroupStateRequestData().setGroupId(groupId)
            ).get(5, TimeUnit.SECONDS)
        );

        // 2. Empty partitionsData
        assertEquals(new WriteShareGroupStateResponseData(),
            service.writeState(
                requestContext(ApiKeys.WRITE_SHARE_GROUP_STATE),
                new WriteShareGroupStateRequestData().setGroupId(groupId).setTopics(Collections.singletonList(
                    new WriteShareGroupStateRequestData.WriteStateData().setTopicId(topicId)))
            ).get(5, TimeUnit.SECONDS)
        );

        // 3. Invalid groupId
        assertEquals(new WriteShareGroupStateResponseData(),
            service.writeState(
                requestContext(ApiKeys.WRITE_SHARE_GROUP_STATE),
                new WriteShareGroupStateRequestData().setGroupId(null).setTopics(Collections.singletonList(
                    new WriteShareGroupStateRequestData.WriteStateData().setTopicId(topicId).setPartitions(Collections.singletonList(
                        new WriteShareGroupStateRequestData.PartitionData().setPartition(partition)))))
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testReadStateValidationsError() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorConfigTest.createConfig(ShareCoordinatorConfigTest.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        // 1. Empty topicsData
        assertEquals(new ReadShareGroupStateResponseData(),
            service.readState(
                requestContext(ApiKeys.READ_SHARE_GROUP_STATE),
                new ReadShareGroupStateRequestData().setGroupId(groupId)
            ).get(5, TimeUnit.SECONDS)
        );

        // 2. Empty partitionsData
        assertEquals(new ReadShareGroupStateResponseData(),
            service.readState(
                requestContext(ApiKeys.READ_SHARE_GROUP_STATE),
                new ReadShareGroupStateRequestData().setGroupId(groupId).setTopics(Collections.singletonList(
                    new ReadShareGroupStateRequestData.ReadStateData().setTopicId(topicId)))
            ).get(5, TimeUnit.SECONDS)
        );

        // 3. Invalid groupId
        assertEquals(new ReadShareGroupStateResponseData(),
            service.readState(
                requestContext(ApiKeys.READ_SHARE_GROUP_STATE),
                new ReadShareGroupStateRequestData().setGroupId(null).setTopics(Collections.singletonList(
                    new ReadShareGroupStateRequestData.ReadStateData().setTopicId(topicId).setPartitions(Collections.singletonList(
                        new ReadShareGroupStateRequestData.PartitionData().setPartition(partition)))))
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testWriteStateWhenNotStarted() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorConfigTest.createConfig(ShareCoordinatorConfigTest.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM
        );

        String groupId = "group1";
        Uuid topicId1 = Uuid.randomUuid();
        int partition1 = 0;

        Uuid topicId2 = Uuid.randomUuid();
        int partition2 = 1;

        WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
            .setGroupId(groupId)
            .setTopics(Arrays.asList(
                    new WriteShareGroupStateRequestData.WriteStateData()
                        .setTopicId(topicId1)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition1)
                                .setStartOffset(0)
                                .setStateEpoch(1)
                                .setLeaderEpoch(1)
                                .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                                    .setFirstOffset(0)
                                    .setLastOffset(10)
                                    .setDeliveryCount((short) 1)
                                    .setDeliveryState((byte) 0))
                                )
                        )),
                    new WriteShareGroupStateRequestData.WriteStateData()
                        .setTopicId(topicId2)
                        .setPartitions(Collections.singletonList(
                            new WriteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition2)
                                .setStartOffset(0)
                                .setStateEpoch(1)
                                .setLeaderEpoch(1)
                                .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                                    .setFirstOffset(0)
                                    .setLastOffset(10)
                                    .setDeliveryCount((short) 1)
                                    .setDeliveryState((byte) 0))
                                )
                        ))
                )
            );

        CompletableFuture<WriteShareGroupStateResponseData> future = service.writeState(
            requestContext(ApiKeys.WRITE_SHARE_GROUP_STATE),
            request
        );

        HashSet<WriteShareGroupStateResponseData.WriteStateResult> result = new HashSet<>(future.get(5, TimeUnit.SECONDS).results());

        HashSet<WriteShareGroupStateResponseData.WriteStateResult> expectedResult = new HashSet<>(Arrays.asList(
            new WriteShareGroupStateResponseData.WriteStateResult()
                .setTopicId(topicId2)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition2)
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                    .setErrorMessage("Share coordinator is not available."))),
            new WriteShareGroupStateResponseData.WriteStateResult()
                .setTopicId(topicId1)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition1)
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                    .setErrorMessage("Share coordinator is not available.")))));
        assertEquals(expectedResult, result);
    }

    @Test
    public void testReadStateWhenNotStarted() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorConfigTest.createConfig(ShareCoordinatorConfigTest.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM
        );

        String groupId = "group1";
        Uuid topicId1 = Uuid.randomUuid();
        int partition1 = 0;

        Uuid topicId2 = Uuid.randomUuid();
        int partition2 = 1;

        ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
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
                )
            );

        CompletableFuture<ReadShareGroupStateResponseData> future = service.readState(
            requestContext(ApiKeys.READ_SHARE_GROUP_STATE),
            request
        );

        HashSet<ReadShareGroupStateResponseData.ReadStateResult> result = new HashSet<>(future.get(5, TimeUnit.SECONDS).results());

        HashSet<ReadShareGroupStateResponseData.ReadStateResult> expectedResult = new HashSet<>(Arrays.asList(
            new ReadShareGroupStateResponseData.ReadStateResult()
                .setTopicId(topicId2)
                .setPartitions(Collections.singletonList(new ReadShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition2)
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                    .setErrorMessage("Share coordinator is not available."))),
            new ReadShareGroupStateResponseData.ReadStateResult()
                .setTopicId(topicId1)
                .setPartitions(Collections.singletonList(new ReadShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition1)
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                    .setErrorMessage("Share coordinator is not available.")))));
        assertEquals(expectedResult, result);
    }

    @Test
    public void testWriteFutureReturnsError() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorConfigTest.createConfig(ShareCoordinatorConfigTest.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        when(runtime.scheduleWriteOperation(any(), any(), any(), any()))
            .thenReturn(FutureUtils.failedFuture(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception()));

        assertEquals(new WriteShareGroupStateResponseData()
                .setResults(Collections.singletonList(new WriteShareGroupStateResponseData.WriteStateResult()
                    .setTopicId(topicId)
                    .setPartitions(Collections.singletonList(new WriteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition)
                        .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                        .setErrorMessage("Unable to write share group state: This server does not host this topic-partition."))))),
            service.writeState(
                requestContext(ApiKeys.WRITE_SHARE_GROUP_STATE),
                new WriteShareGroupStateRequestData().setGroupId(groupId)
                    .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                            .setPartition(partition)
                            .setLeaderEpoch(1)
                            .setStartOffset(1)
                            .setStateEpoch(1)
                            .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                                .setFirstOffset(2)
                                .setLastOffset(10)
                                .setDeliveryCount((short) 1)
                                .setDeliveryState((byte) 1)))
                        ))
                    ))
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testReadFutureReturnsError() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorConfigTest.createConfig(ShareCoordinatorConfigTest.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        when(runtime.scheduleWriteOperation(any(), any(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        when(runtime.scheduleReadOperation(any(), any(), any()))
            .thenReturn(FutureUtils.failedFuture(Errors.UNKNOWN_SERVER_ERROR.exception()));

        assertEquals(new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(new ReadShareGroupStateResponseData.ReadStateResult()
                    .setTopicId(topicId)
                    .setPartitions(Collections.singletonList(new ReadShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition)
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setErrorMessage("Unable to read share group state: The server experienced an unexpected error when processing the request."))))),
            service.readState(
                requestContext(ApiKeys.READ_SHARE_GROUP_STATE),
                new ReadShareGroupStateRequestData().setGroupId(groupId)
                    .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                            .setPartition(partition)
                            .setLeaderEpoch(1)
                        ))
                    ))
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testReadStateCallFailedDueToLeaderEpochUpdateFail() throws InterruptedException, ExecutionException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorConfigTest.createConfig(ShareCoordinatorConfigTest.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        when(runtime.scheduleWriteOperation(
            ArgumentMatchers.eq("write-share-group-state"),
            ArgumentMatchers.eq(new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0)),
            any(),
            any()))
            .thenReturn(FutureUtils.failedFuture(Errors.FENCED_LEADER_EPOCH.exception()));

        assertEquals(new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(new ReadShareGroupStateResponseData.ReadStateResult()
                    .setTopicId(topicId)
                    .setPartitions(Collections.singletonList(new ReadShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition)
                        .setErrorCode(Errors.FENCED_LEADER_EPOCH.code())
                        .setErrorMessage("Unable to update leader epoch during read request: The leader epoch in the request is older than the epoch on the broker."))))),
            service.readState(
                requestContext(ApiKeys.READ_SHARE_GROUP_STATE),
                new ReadShareGroupStateRequestData().setGroupId(groupId)
                    .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                            .setPartition(partition)
                            .setLeaderEpoch(1)
                        ))
                    ))
            ).get(5, TimeUnit.SECONDS)
        );

        verify(runtime, times(1)).scheduleWriteOperation(any(), any(), any(), any());
        verify(runtime, times(0)).scheduleReadOperation(any(), any(), any());
    }
}
