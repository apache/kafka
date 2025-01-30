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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;
import org.apache.kafka.common.message.DeleteShareGroupStateResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.PartitionWriter;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.server.util.timer.Timer;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.coordinator.common.runtime.TestUtil.requestContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ShareCoordinatorServiceTest {

    @SuppressWarnings("unchecked")
    private CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> mockRuntime() {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mock(CoordinatorRuntime.class);
        when(runtime.activeTopicPartitions())
            .thenReturn(List.of(new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0)));
        return runtime;
    }

    @Test
    public void testStartupShutdown() throws Exception {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            new MockTimer(),
            mock(PartitionWriter.class)
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
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            coordinatorMetrics,
            time,
            mock(Timer.class),
            mock(PartitionWriter.class)
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
                        .setPartitions(List.of(
                            new WriteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition1)
                                .setStartOffset(0)
                                .setStateEpoch(1)
                                .setLeaderEpoch(1)
                                .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
                                    .setFirstOffset(0)
                                    .setLastOffset(10)
                                    .setDeliveryCount((short) 1)
                                    .setDeliveryState((byte) 0))
                                )
                        )),
                    new WriteShareGroupStateRequestData.WriteStateData()
                        .setTopicId(topicId2)
                        .setPartitions(List.of(
                            new WriteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition2)
                                .setStartOffset(0)
                                .setStateEpoch(1)
                                .setLeaderEpoch(1)
                                .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
                                    .setFirstOffset(0)
                                    .setLastOffset(10)
                                    .setDeliveryCount((short) 1)
                                    .setDeliveryState((byte) 0))
                                )
                        ))
                )
            );

        WriteShareGroupStateResponseData response1 = new WriteShareGroupStateResponseData()
            .setResults(List.of(
                new WriteShareGroupStateResponseData.WriteStateResult()
                    .setTopicId(topicId1)
                    .setPartitions(List.of(new WriteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition1)))
            ));

        WriteShareGroupStateResponseData response2 = new WriteShareGroupStateResponseData()
            .setResults(List.of(
                new WriteShareGroupStateResponseData.WriteStateResult()
                    .setTopicId(topicId2)
                    .setPartitions(List.of(new WriteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition2)))
            ));

        when(runtime.scheduleWriteOperation(
            eq("write-share-group-state"),
            eq(new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0)),
            eq(Duration.ofMillis(5000)),
            any()
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
                .setPartitions(List.of(new WriteShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition2))),
            new WriteShareGroupStateResponseData.WriteStateResult()
                .setTopicId(topicId1)
                .setPartitions(List.of(new WriteShareGroupStateResponseData.PartitionResult()
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
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
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
                        .setPartitions(List.of(
                            new ReadShareGroupStateRequestData.PartitionData()
                                .setPartition(partition1)
                                .setLeaderEpoch(1)
                        )),
                    new ReadShareGroupStateRequestData.ReadStateData()
                        .setTopicId(topicId2)
                        .setPartitions(List.of(
                            new ReadShareGroupStateRequestData.PartitionData()
                                .setPartition(partition2)
                                .setLeaderEpoch(1)
                        ))
                )
            );

        ReadShareGroupStateResponseData.ReadStateResult topicData1 = new ReadShareGroupStateResponseData.ReadStateResult()
            .setTopicId(topicId1)
            .setPartitions(List.of(new ReadShareGroupStateResponseData.PartitionResult()
                .setPartition(partition1)
                .setErrorCode(Errors.NONE.code())
                .setStateEpoch(1)
                .setStartOffset(0)
                .setStateBatches(List.of(new ReadShareGroupStateResponseData.StateBatch()
                    .setFirstOffset(0)
                    .setLastOffset(10)
                    .setDeliveryCount((short) 1)
                    .setDeliveryState((byte) 0))
                ))
            );

        ReadShareGroupStateResponseData.ReadStateResult topicData2 = new ReadShareGroupStateResponseData.ReadStateResult()
            .setTopicId(topicId2)
            .setPartitions(List.of(new ReadShareGroupStateResponseData.PartitionResult()
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

        when(runtime.scheduleWriteOperation(
            eq("read-update-leader-epoch-state"),
            eq(new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0)),
            any(),
            any()
        ))
            .thenReturn(CompletableFuture.completedFuture(new ReadShareGroupStateResponseData()
                .setResults(List.of(topicData1))))
            .thenReturn(CompletableFuture.completedFuture(new ReadShareGroupStateResponseData()
                .setResults(List.of(topicData2))));

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
    public void testReadStateSummarySuccess() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.createConfig(ShareCoordinatorTestConfig.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId1 = Uuid.randomUuid();
        int partition1 = 0;

        Uuid topicId2 = Uuid.randomUuid();
        int partition2 = 1;

        ReadShareGroupStateSummaryRequestData request = new ReadShareGroupStateSummaryRequestData()
            .setGroupId(groupId)
            .setTopics(Arrays.asList(
                    new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                        .setTopicId(topicId1)
                        .setPartitions(List.of(
                            new ReadShareGroupStateSummaryRequestData.PartitionData()
                                .setPartition(partition1)
                                .setLeaderEpoch(1)
                        )),
                    new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                        .setTopicId(topicId2)
                        .setPartitions(List.of(
                            new ReadShareGroupStateSummaryRequestData.PartitionData()
                                .setPartition(partition2)
                                .setLeaderEpoch(1)
                        ))
                )
            );

        ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult topicData1 = new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
            .setTopicId(topicId1)
            .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                .setPartition(partition1)
                .setErrorCode(Errors.NONE.code())
                .setStateEpoch(1)
                .setStartOffset(0)
            ));

        ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult topicData2 = new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
            .setTopicId(topicId2)
            .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                .setPartition(partition2)
                .setErrorCode(Errors.NONE.code())
                .setStateEpoch(1)
                .setStartOffset(0)
            ));

        when(runtime.scheduleWriteOperation(
            eq("read-share-group-state-summary"),
            eq(new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0)),
            any(),
            any()
        ))
            .thenReturn(CompletableFuture.completedFuture(new ReadShareGroupStateSummaryResponseData()
                .setResults(List.of(topicData1))))
            .thenReturn(CompletableFuture.completedFuture(new ReadShareGroupStateSummaryResponseData()
                .setResults(List.of(topicData2))));

        CompletableFuture<ReadShareGroupStateSummaryResponseData> future = service.readStateSummary(
            requestContext(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY),
            request
        );

        HashSet<ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult> result = new HashSet<>(future.get(5, TimeUnit.SECONDS).results());

        HashSet<ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult> expectedResult = new HashSet<>(Arrays.asList(
            topicData1,
            topicData2));
        assertEquals(expectedResult, result);
    }

    @Test
    public void testDeleteStateSuccess() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        Metrics metrics = new Metrics();
        ShareCoordinatorMetrics coordinatorMetrics = new ShareCoordinatorMetrics(metrics);
        Time time = mock(Time.class);
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            coordinatorMetrics,
            time,
            mock(Timer.class),
            mock(PartitionWriter.class)
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId1 = Uuid.randomUuid();
        int partition1 = 0;

        Uuid topicId2 = Uuid.randomUuid();
        int partition2 = 1;

        DeleteShareGroupStateRequestData request = new DeleteShareGroupStateRequestData()
            .setGroupId(groupId)
            .setTopics(Arrays.asList(
                    new DeleteShareGroupStateRequestData.DeleteStateData()
                        .setTopicId(topicId1)
                        .setPartitions(List.of(
                            new DeleteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition1)
                        )),
                    new DeleteShareGroupStateRequestData.DeleteStateData()
                        .setTopicId(topicId2)
                        .setPartitions(List.of(
                            new DeleteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition2)
                        ))
                )
            );

        DeleteShareGroupStateResponseData response1 = new DeleteShareGroupStateResponseData()
            .setResults(List.of(
                new DeleteShareGroupStateResponseData.DeleteStateResult()
                    .setTopicId(topicId1)
                    .setPartitions(List.of(new DeleteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition1)))
            ));

        DeleteShareGroupStateResponseData response2 = new DeleteShareGroupStateResponseData()
            .setResults(List.of(
                new DeleteShareGroupStateResponseData.DeleteStateResult()
                    .setTopicId(topicId2)
                    .setPartitions(List.of(new DeleteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition2)))
            ));

        when(runtime.scheduleWriteOperation(
            eq("delete-share-group-state"),
            eq(new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0)),
            eq(Duration.ofMillis(5000)),
            any()
        ))
            .thenReturn(CompletableFuture.completedFuture(response1))
            .thenReturn(CompletableFuture.completedFuture(response2));

        CompletableFuture<DeleteShareGroupStateResponseData> future = service.deleteState(
            requestContext(ApiKeys.DELETE_SHARE_GROUP_STATE),
            request
        );

        HashSet<DeleteShareGroupStateResponseData.DeleteStateResult> result = new HashSet<>(future.get(5, TimeUnit.SECONDS).results());

        HashSet<DeleteShareGroupStateResponseData.DeleteStateResult> expectedResult = new HashSet<>(Arrays.asList(
            new DeleteShareGroupStateResponseData.DeleteStateResult()
                .setTopicId(topicId2)
                .setPartitions(List.of(new DeleteShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition2))),
            new DeleteShareGroupStateResponseData.DeleteStateResult()
                .setTopicId(topicId1)
                .setPartitions(List.of(new DeleteShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition1)))));
        assertEquals(expectedResult, result);
    }

    @Test
    public void testWriteStateValidationsError() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
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
                new WriteShareGroupStateRequestData().setGroupId(groupId).setTopics(List.of(
                    new WriteShareGroupStateRequestData.WriteStateData().setTopicId(topicId)))
            ).get(5, TimeUnit.SECONDS)
        );

        // 3. Invalid groupId
        assertEquals(new WriteShareGroupStateResponseData(),
            service.writeState(
                requestContext(ApiKeys.WRITE_SHARE_GROUP_STATE),
                new WriteShareGroupStateRequestData().setGroupId(null).setTopics(List.of(
                    new WriteShareGroupStateRequestData.WriteStateData().setTopicId(topicId).setPartitions(List.of(
                        new WriteShareGroupStateRequestData.PartitionData().setPartition(partition)))))
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testReadStateValidationsError() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
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
                new ReadShareGroupStateRequestData().setGroupId(groupId).setTopics(List.of(
                    new ReadShareGroupStateRequestData.ReadStateData().setTopicId(topicId)))
            ).get(5, TimeUnit.SECONDS)
        );

        // 3. Invalid groupId
        assertEquals(new ReadShareGroupStateResponseData(),
            service.readState(
                requestContext(ApiKeys.READ_SHARE_GROUP_STATE),
                new ReadShareGroupStateRequestData().setGroupId(null).setTopics(List.of(
                    new ReadShareGroupStateRequestData.ReadStateData().setTopicId(topicId).setPartitions(List.of(
                        new ReadShareGroupStateRequestData.PartitionData().setPartition(partition)))))
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testReadStateSummaryValidationsError() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.createConfig(ShareCoordinatorTestConfig.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        // 1. Empty topicsData
        assertEquals(new ReadShareGroupStateSummaryResponseData(),
            service.readStateSummary(
                requestContext(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY),
                new ReadShareGroupStateSummaryRequestData().setGroupId(groupId)
            ).get(5, TimeUnit.SECONDS)
        );

        // 2. Empty partitionsData
        assertEquals(new ReadShareGroupStateSummaryResponseData(),
            service.readStateSummary(
                requestContext(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY),
                new ReadShareGroupStateSummaryRequestData().setGroupId(groupId).setTopics(List.of(
                    new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData().setTopicId(topicId)))
            ).get(5, TimeUnit.SECONDS)
        );

        // 3. Invalid groupId
        assertEquals(new ReadShareGroupStateSummaryResponseData(),
            service.readStateSummary(
                requestContext(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY),
                new ReadShareGroupStateSummaryRequestData().setGroupId(null).setTopics(List.of(
                    new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData().setTopicId(topicId).setPartitions(List.of(
                        new ReadShareGroupStateSummaryRequestData.PartitionData().setPartition(partition)))))
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testDeleteStateValidationsError() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        // 1. Empty topicsData
        assertEquals(new DeleteShareGroupStateResponseData(),
            service.deleteState(
                requestContext(ApiKeys.DELETE_SHARE_GROUP_STATE),
                new DeleteShareGroupStateRequestData().setGroupId(groupId)
            ).get(5, TimeUnit.SECONDS)
        );

        // 2. Empty partitionsData
        assertEquals(new DeleteShareGroupStateResponseData(),
            service.deleteState(
                requestContext(ApiKeys.DELETE_SHARE_GROUP_STATE),
                new DeleteShareGroupStateRequestData().setGroupId(groupId).setTopics(List.of(
                    new DeleteShareGroupStateRequestData.DeleteStateData().setTopicId(topicId)))
            ).get(5, TimeUnit.SECONDS)
        );

        // 3. Invalid groupId
        assertEquals(new DeleteShareGroupStateResponseData(),
            service.deleteState(
                requestContext(ApiKeys.DELETE_SHARE_GROUP_STATE),
                new DeleteShareGroupStateRequestData().setGroupId(null).setTopics(List.of(
                    new DeleteShareGroupStateRequestData.DeleteStateData().setTopicId(topicId).setPartitions(List.of(
                        new DeleteShareGroupStateRequestData.PartitionData().setPartition(partition)))))
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testWriteStateWhenNotStarted() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
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
                        .setPartitions(List.of(
                            new WriteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition1)
                                .setStartOffset(0)
                                .setStateEpoch(1)
                                .setLeaderEpoch(1)
                                .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
                                    .setFirstOffset(0)
                                    .setLastOffset(10)
                                    .setDeliveryCount((short) 1)
                                    .setDeliveryState((byte) 0))
                                )
                        )),
                    new WriteShareGroupStateRequestData.WriteStateData()
                        .setTopicId(topicId2)
                        .setPartitions(List.of(
                            new WriteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition2)
                                .setStartOffset(0)
                                .setStateEpoch(1)
                                .setLeaderEpoch(1)
                                .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
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
                .setPartitions(List.of(new WriteShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition2)
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                    .setErrorMessage("Share coordinator is not available."))),
            new WriteShareGroupStateResponseData.WriteStateResult()
                .setTopicId(topicId1)
                .setPartitions(List.of(new WriteShareGroupStateResponseData.PartitionResult()
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
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
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
                        .setPartitions(List.of(
                            new ReadShareGroupStateRequestData.PartitionData()
                                .setPartition(partition1)
                                .setLeaderEpoch(1)
                        )),
                    new ReadShareGroupStateRequestData.ReadStateData()
                        .setTopicId(topicId2)
                        .setPartitions(List.of(
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
                .setPartitions(List.of(new ReadShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition2)
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                    .setErrorMessage("Share coordinator is not available."))),
            new ReadShareGroupStateResponseData.ReadStateResult()
                .setTopicId(topicId1)
                .setPartitions(List.of(new ReadShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition1)
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                    .setErrorMessage("Share coordinator is not available.")))));
        assertEquals(expectedResult, result);
    }

    @Test
    public void testReadStateSummaryWhenNotStarted() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.createConfig(ShareCoordinatorTestConfig.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
        );

        String groupId = "group1";
        Uuid topicId1 = Uuid.randomUuid();
        int partition1 = 0;

        Uuid topicId2 = Uuid.randomUuid();
        int partition2 = 1;

        ReadShareGroupStateSummaryRequestData request = new ReadShareGroupStateSummaryRequestData()
            .setGroupId(groupId)
            .setTopics(Arrays.asList(
                    new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                        .setTopicId(topicId1)
                        .setPartitions(List.of(
                            new ReadShareGroupStateSummaryRequestData.PartitionData()
                                .setPartition(partition1)
                                .setLeaderEpoch(1)
                        )),
                    new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                        .setTopicId(topicId2)
                        .setPartitions(List.of(
                            new ReadShareGroupStateSummaryRequestData.PartitionData()
                                .setPartition(partition2)
                                .setLeaderEpoch(1)
                        ))
                )
            );

        CompletableFuture<ReadShareGroupStateSummaryResponseData> future = service.readStateSummary(
            requestContext(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY),
            request
        );

        HashSet<ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult> result = new HashSet<>(future.get(5, TimeUnit.SECONDS).results());

        HashSet<ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult> expectedResult = new HashSet<>(Arrays.asList(
            new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                .setTopicId(topicId2)
                .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                    .setPartition(partition2)
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                    .setErrorMessage("Share coordinator is not available."))),
            new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                .setTopicId(topicId1)
                .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                    .setPartition(partition1)
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                    .setErrorMessage("Share coordinator is not available.")))));
        assertEquals(expectedResult, result);
    }

    @Test
    public void testDeleteStateWhenNotStarted() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
        );

        String groupId = "group1";
        Uuid topicId1 = Uuid.randomUuid();
        int partition1 = 0;

        Uuid topicId2 = Uuid.randomUuid();
        int partition2 = 1;

        DeleteShareGroupStateRequestData request = new DeleteShareGroupStateRequestData()
            .setGroupId(groupId)
            .setTopics(Arrays.asList(
                    new DeleteShareGroupStateRequestData.DeleteStateData()
                        .setTopicId(topicId1)
                        .setPartitions(List.of(
                            new DeleteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition1)
                        )),
                    new DeleteShareGroupStateRequestData.DeleteStateData()
                        .setTopicId(topicId2)
                        .setPartitions(List.of(
                            new DeleteShareGroupStateRequestData.PartitionData()
                                .setPartition(partition2)
                        ))
                )
            );

        CompletableFuture<DeleteShareGroupStateResponseData> future = service.deleteState(
            requestContext(ApiKeys.DELETE_SHARE_GROUP_STATE),
            request
        );

        HashSet<DeleteShareGroupStateResponseData.DeleteStateResult> result = new HashSet<>(future.get(5, TimeUnit.SECONDS).results());

        HashSet<DeleteShareGroupStateResponseData.DeleteStateResult> expectedResult = new HashSet<>(Arrays.asList(
            new DeleteShareGroupStateResponseData.DeleteStateResult()
                .setTopicId(topicId2)
                .setPartitions(List.of(new DeleteShareGroupStateResponseData.PartitionResult()
                    .setPartition(partition2)
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                    .setErrorMessage("Share coordinator is not available."))),
            new DeleteShareGroupStateResponseData.DeleteStateResult()
                .setTopicId(topicId1)
                .setPartitions(List.of(new DeleteShareGroupStateResponseData.PartitionResult()
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
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        when(runtime.scheduleWriteOperation(any(), any(), any(), any()))
            .thenReturn(FutureUtils.failedFuture(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception()));

        assertEquals(new WriteShareGroupStateResponseData()
                .setResults(List.of(new WriteShareGroupStateResponseData.WriteStateResult()
                    .setTopicId(topicId)
                    .setPartitions(List.of(new WriteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition)
                        .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                        .setErrorMessage("Unable to write share group state: This server does not host this topic-partition."))))),
            service.writeState(
                requestContext(ApiKeys.WRITE_SHARE_GROUP_STATE),
                new WriteShareGroupStateRequestData().setGroupId(groupId)
                    .setTopics(List.of(new WriteShareGroupStateRequestData.WriteStateData()
                        .setTopicId(topicId)
                        .setPartitions(List.of(new WriteShareGroupStateRequestData.PartitionData()
                            .setPartition(partition)
                            .setLeaderEpoch(1)
                            .setStartOffset(1)
                            .setStateEpoch(1)
                            .setStateBatches(List.of(new WriteShareGroupStateRequestData.StateBatch()
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
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        when(runtime.scheduleWriteOperation(any(), any(), any(), any()))
            .thenReturn(FutureUtils.failedFuture(Errors.UNKNOWN_SERVER_ERROR.exception()));

        assertEquals(new ReadShareGroupStateResponseData()
                .setResults(List.of(new ReadShareGroupStateResponseData.ReadStateResult()
                    .setTopicId(topicId)
                    .setPartitions(List.of(new ReadShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition)
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setErrorMessage("Unable to read share group state: The server experienced an unexpected error when processing the request."))))),
            service.readState(
                requestContext(ApiKeys.READ_SHARE_GROUP_STATE),
                new ReadShareGroupStateRequestData().setGroupId(groupId)
                    .setTopics(List.of(new ReadShareGroupStateRequestData.ReadStateData()
                        .setTopicId(topicId)
                        .setPartitions(List.of(new ReadShareGroupStateRequestData.PartitionData()
                            .setPartition(partition)
                            .setLeaderEpoch(1)
                        ))
                    ))
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testReadSummaryFutureReturnsError() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.createConfig(ShareCoordinatorTestConfig.testConfigMap()),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        when(runtime.scheduleWriteOperation(any(), any(), any(), any()))
            .thenReturn(FutureUtils.failedFuture(Errors.UNKNOWN_SERVER_ERROR.exception()));

        assertEquals(new ReadShareGroupStateSummaryResponseData()
                .setResults(List.of(new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                    .setTopicId(topicId)
                    .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                        .setPartition(partition)
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setErrorMessage("Unable to read share group state summary: The server experienced an unexpected error when processing the request."))))),
            service.readStateSummary(
                requestContext(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY),
                new ReadShareGroupStateSummaryRequestData().setGroupId(groupId)
                    .setTopics(List.of(new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
                        .setTopicId(topicId)
                        .setPartitions(List.of(new ReadShareGroupStateSummaryRequestData.PartitionData()
                            .setPartition(partition)
                            .setLeaderEpoch(1)
                        ))
                    ))
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testDeleteFutureReturnsError() throws ExecutionException, InterruptedException, TimeoutException {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        when(runtime.scheduleWriteOperation(any(), any(), any(), any()))
            .thenReturn(FutureUtils.failedFuture(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception()));

        assertEquals(new DeleteShareGroupStateResponseData()
                .setResults(List.of(new DeleteShareGroupStateResponseData.DeleteStateResult()
                    .setTopicId(topicId)
                    .setPartitions(List.of(new DeleteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partition)
                        .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                        .setErrorMessage("Unable to delete share group state: This server does not host this topic-partition."))))),
            service.deleteState(
                requestContext(ApiKeys.DELETE_SHARE_GROUP_STATE),
                new DeleteShareGroupStateRequestData().setGroupId(groupId)
                    .setTopics(List.of(new DeleteShareGroupStateRequestData.DeleteStateData()
                        .setTopicId(topicId)
                        .setPartitions(List.of(new DeleteShareGroupStateRequestData.PartitionData()
                            .setPartition(partition)
                        ))
                    ))
            ).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testTopicPartitionFor() {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
        );

        service.startup(() -> 1);

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        TopicPartition tp = service.topicPartitionFor(new SharePartitionKey(groupId, new TopicIdPartition(topicId, partition, null)));
        assertEquals(Topic.SHARE_GROUP_STATE_TOPIC_NAME, tp.topic());
        int expectedPartition = tp.partition();

        // The presence of a topic name should not affect the choice of partition.
        tp = service.topicPartitionFor(new SharePartitionKey(groupId, new TopicIdPartition(topicId, partition, "whatever")));
        assertEquals(Topic.SHARE_GROUP_STATE_TOPIC_NAME, tp.topic());
        assertEquals(expectedPartition, tp.partition());
    }

    @Test
    public void testPartitionFor() {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        ShareCoordinatorService service = new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(),
            Time.SYSTEM,
            mock(Timer.class),
            mock(PartitionWriter.class)
        );

        String groupId = "group1";
        Uuid topicId = Uuid.randomUuid();
        int partition = 0;

        // Inactive shard should throw exception.
        assertThrows(CoordinatorNotAvailableException.class, () -> service.partitionFor(SharePartitionKey.getInstance(groupId, topicId, partition)));

        final int numPartitions = 1;
        service.startup(() -> numPartitions);

        final SharePartitionKey key1 = SharePartitionKey.getInstance(groupId, new TopicIdPartition(topicId, partition, null));
        assertEquals(Utils.abs(key1.asCoordinatorKey().hashCode()) % numPartitions, service.partitionFor(key1));

        // The presence of a topic name should not affect the choice of partition.
        final SharePartitionKey key2 = new SharePartitionKey(groupId, new TopicIdPartition(topicId, partition, "whatever"));
        assertEquals(Utils.abs(key2.asCoordinatorKey().hashCode()) % numPartitions, service.partitionFor(key2));

        // asCoordinatorKey does not discriminate on topic name.
        assertEquals(key1.asCoordinatorKey(), key2.asCoordinatorKey());
    }

    @Test
    public void testRecordPruningTaskPeriodicityWithAllSuccess() throws Exception {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        MockTime time = new MockTime();
        MockTimer timer = new MockTimer(time);
        PartitionWriter writer = mock(PartitionWriter.class);

        when(writer.deleteRecords(
            any(),
            eq(10L)
        )).thenReturn(
            CompletableFuture.completedFuture(null)
        );

        when(runtime.scheduleWriteOperation(
            eq("write-state-record-prune"),
            any(),
            any(),
            any()
        )).thenReturn(
            CompletableFuture.completedFuture(Optional.of(10L))
        ).thenReturn(
            CompletableFuture.completedFuture(Optional.of(11L))
        );

        Metrics metrics = new Metrics();

        ShareCoordinatorService service = spy(new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(metrics),
            time,
            timer,
            writer
        ));

        service.startup(() -> 1);
        verify(runtime, times(0))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        timer.advanceClock(30005L); // prune should be called
        verify(runtime, times(1))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        timer.advanceClock(30005L); // prune should be called
        verify(runtime, times(2))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        verify(writer, times(2))
            .deleteRecords(any(), anyLong());

        checkMetrics(metrics);
        checkPruneMetric(metrics, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0, true);

        service.shutdown();
    }

    @Test
    public void testRecordPruningTaskPeriodicityWithSomeFailures() throws Exception {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        MockTime time = new MockTime();
        MockTimer timer = new MockTimer(time);
        PartitionWriter writer = mock(PartitionWriter.class);
        TopicPartition tp1 = new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0);
        TopicPartition tp2 = new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, 1);

        when(runtime.activeTopicPartitions())
            .thenReturn(List.of(tp1, tp2));

        when(writer.deleteRecords(
            any(),
            eq(10L)
        )).thenReturn(
            CompletableFuture.completedFuture(null)
        );

        when(writer.deleteRecords(
            any(),
            eq(20L)
        )).thenReturn(
            CompletableFuture.failedFuture(new Exception("bad stuff"))
        );

        when(runtime.scheduleWriteOperation(
            eq("write-state-record-prune"),
            eq(tp1),
            any(),
            any()
        )).thenReturn(
            CompletableFuture.completedFuture(Optional.of(10L))
        ).thenReturn(
            CompletableFuture.completedFuture(Optional.of(11L))
        );

        when(runtime.scheduleWriteOperation(
            eq("write-state-record-prune"),
            eq(tp2),
            any(),
            any()
        )).thenReturn(
            CompletableFuture.completedFuture(Optional.of(20L))
        ).thenReturn(
            CompletableFuture.completedFuture(Optional.of(21L))
        );

        Metrics metrics = new Metrics();

        ShareCoordinatorService service = spy(new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(metrics),
            time,
            timer,
            writer
        ));

        service.startup(() -> 2);
        verify(runtime, times(0))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        timer.advanceClock(30005L); // Prune should be called.
        verify(runtime, times(2))   // For 2 topic partitions.
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        timer.advanceClock(30005L); // Prune should be called as future completes exceptionally.
        verify(runtime, times(4))   // Second prune with 2 topic partitions.
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        verify(writer, times(4))
            .deleteRecords(any(), anyLong());

        checkMetrics(metrics);
        checkPruneMetric(metrics, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0, true);
        checkPruneMetric(metrics, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 1, false);

        service.shutdown();
    }

    @Test
    public void testRecordPruningTaskException() throws Exception {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        MockTime time = new MockTime();
        MockTimer timer = new MockTimer(time);
        PartitionWriter writer = mock(PartitionWriter.class);

        when(runtime.scheduleWriteOperation(
            eq("write-state-record-prune"),
            any(),
            any(),
            any()
        )).thenReturn(CompletableFuture.failedFuture(Errors.UNKNOWN_SERVER_ERROR.exception()));

        Metrics metrics = new Metrics();

        ShareCoordinatorService service = spy(new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(metrics),
            time,
            timer,
            writer
        ));

        service.startup(() -> 1);
        verify(runtime, times(0))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        timer.advanceClock(30005L); // prune should be called
        verify(runtime, times(1))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        verify(writer, times(0))
            .deleteRecords(any(), anyLong());

        checkMetrics(metrics);
        checkPruneMetric(metrics, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0, false);

        service.shutdown();
    }

    @Test
    public void testRecordPruningTaskSuccess() throws Exception {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        MockTime time = new MockTime();
        MockTimer timer = new MockTimer(time);
        PartitionWriter writer = mock(PartitionWriter.class);

        when(runtime.scheduleWriteOperation(
            eq("write-state-record-prune"),
            any(),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Optional.of(20L)));

        Metrics metrics = new Metrics();

        ShareCoordinatorService service = spy(new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(metrics),
            time,
            timer,
            writer
        ));

        service.startup(() -> 1);
        verify(runtime, times(0))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        timer.advanceClock(30005L); // prune should be called
        verify(runtime, times(1))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        verify(writer, times(1))
            .deleteRecords(any(), eq(20L));

        checkMetrics(metrics);

        service.shutdown();
    }

    @Test
    public void testRecordPruningTaskEmptyOffsetReturned() throws Exception {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        MockTime time = new MockTime();
        MockTimer timer = new MockTimer(time);
        PartitionWriter writer = mock(PartitionWriter.class);

        when(runtime.scheduleWriteOperation(
            eq("write-state-record-prune"),
            any(),
            any(),
            any()
        )).thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        Metrics metrics = new Metrics();
        ShareCoordinatorService service = spy(new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(metrics),
            time,
            timer,
            writer
        ));

        service.startup(() -> 1);
        verify(runtime, times(0))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        timer.advanceClock(30005L); // prune should be called
        verify(runtime, times(1))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        verify(writer, times(0))
            .deleteRecords(any(), anyLong());

        checkMetrics(metrics);
        checkPruneMetric(metrics, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0, false);

        service.shutdown();
    }

    @Test
    public void testRecordPruningTaskRepeatedSameOffsetForTopic() throws Exception {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        MockTime time = new MockTime();
        MockTimer timer = new MockTimer(time);
        PartitionWriter writer = mock(PartitionWriter.class);

        when(writer.deleteRecords(
            any(),
            eq(10L)
        )).thenReturn(
            CompletableFuture.completedFuture(null)
        );

        when(runtime.scheduleWriteOperation(
            eq("write-state-record-prune"),
            any(),
            any(),
            any()
        )).thenReturn(
            CompletableFuture.completedFuture(Optional.of(10L))
        ).thenReturn(
            CompletableFuture.completedFuture(Optional.of(10L))
        );

        Metrics metrics = new Metrics();
        ShareCoordinatorService service = spy(new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(metrics),
            time,
            timer,
            writer
        ));

        service.startup(() -> 1);
        verify(runtime, times(0))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        timer.advanceClock(30005L); // prune should be called
        verify(runtime, times(1))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        timer.advanceClock(30005L); // prune should be called
        verify(runtime, times(2))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        verify(writer, times(1))
            .deleteRecords(any(), anyLong());

        checkMetrics(metrics);
        checkPruneMetric(metrics, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0, true);

        service.shutdown();
    }

    @Test
    public void testRecordPruningTaskRetriesRepeatedSameOffsetForTopic() throws Exception {
        CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mockRuntime();
        MockTime time = new MockTime();
        MockTimer timer = new MockTimer(time);
        PartitionWriter writer = mock(PartitionWriter.class);
        CompletableFuture<Void> fut1 = new CompletableFuture<>();
        fut1.completeExceptionally(new Exception("bad stuff"));

        when(writer.deleteRecords(
            any(),
            eq(10L)
        )).thenReturn(
            fut1
        ).thenReturn(
            CompletableFuture.completedFuture(null)
        );

        when(runtime.scheduleWriteOperation(
            eq("write-state-record-prune"),
            any(),
            any(),
            any()
        )).thenReturn(
            CompletableFuture.completedFuture(Optional.of(10L))
        ).thenReturn(
            CompletableFuture.completedFuture(Optional.of(10L))
        );

        Metrics metrics = new Metrics();

        ShareCoordinatorService service = spy(new ShareCoordinatorService(
            new LogContext(),
            ShareCoordinatorTestConfig.testConfig(),
            runtime,
            new ShareCoordinatorMetrics(metrics),
            time,
            timer,
            writer
        ));

        service.startup(() -> 1);
        verify(runtime, times(0))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        timer.advanceClock(30005L); // prune should be called
        verify(runtime, times(1))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        timer.advanceClock(30005L); // prune should be called
        verify(runtime, times(2))
            .scheduleWriteOperation(
                eq("write-state-record-prune"),
                any(),
                any(),
                any());

        verify(writer, times(2))
            .deleteRecords(any(), anyLong());

        checkMetrics(metrics);
        checkPruneMetric(metrics, Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0, true);

        service.shutdown();
    }

    private void checkMetrics(Metrics metrics) {
        Set<MetricName> usualMetrics = new HashSet<>(Arrays.asList(
            metrics.metricName("write-latency-avg", ShareCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("write-latency-max", ShareCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("write-rate", ShareCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("write-total", ShareCoordinatorMetrics.METRICS_GROUP)
        ));

        usualMetrics.forEach(metric -> assertTrue(metrics.metrics().containsKey(metric)));
    }

    private void checkPruneMetric(Metrics metrics, String topic, int partition, boolean checkPresence) {
        boolean isPresent = metrics.metrics().containsKey(
            metrics.metricName(
                "last-pruned-offset",
                ShareCoordinatorMetrics.METRICS_GROUP,
                "The offset at which the share-group state topic was last pruned.",
                Map.of(
                    "topic", topic,
                    "partition", Integer.toString(partition)
                )
            )
        );
        assertEquals(checkPresence, isPresent);
    }
}
