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

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AssignReplicasToDirsRequest;
import org.apache.kafka.common.requests.AssignReplicasToDirsResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.metadata.AssignmentsHelper;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.metadata.AssignmentsHelper.buildRequestData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AssignmentsManagerTest {

    private static final Uuid TOPIC_1 = Uuid.fromString("88rnFIqYSZykX4ZSKv81bg");
    private static final Uuid TOPIC_2 = Uuid.fromString("VKCnzHdhR5uDQc1shqBYrQ");
    private static final Uuid DIR_1 = Uuid.fromString("cbgD8WdLQCyzLrFIMBhv3w");
    private static final Uuid DIR_2 = Uuid.fromString("zO0bDc0vSuam7Db9iH7rYQ");
    private static final Uuid DIR_3 = Uuid.fromString("CGBWbrFkRkeJQy6Aryzq2Q");

    private MockTime time;
    private NodeToControllerChannelManager channelManager;
    private AssignmentsManager manager;

    @BeforeEach
    public void setup() {
        time = new MockTime();
        channelManager = mock(NodeToControllerChannelManager.class);
        manager = new AssignmentsManager(time, channelManager, 8, () -> 100L);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        manager.close();
    }

    AssignReplicasToDirsRequestData normalize(AssignReplicasToDirsRequestData request) {
        request = request.duplicate();
        request.directories().sort(Comparator.comparing(
            AssignReplicasToDirsRequestData.DirectoryData::id));
        for (AssignReplicasToDirsRequestData.DirectoryData directory : request.directories()) {
            directory.topics().sort(Comparator.comparing(
                AssignReplicasToDirsRequestData.TopicData::topicId));
            for (AssignReplicasToDirsRequestData.TopicData topic : directory.topics()) {
                topic.partitions().sort(Comparator.comparing(
                    AssignReplicasToDirsRequestData.PartitionData::partitionIndex));
            }
        }
        return request;
    }


    void assertRequestEquals(
        AssignReplicasToDirsRequestData expected,
        AssignReplicasToDirsRequestData actual
    ) {
        assertEquals(normalize(expected), normalize(actual));
    }

    @Test
    void testBuildRequestData() {
        Map<TopicIdPartition, Uuid> assignment = new HashMap<TopicIdPartition, Uuid>() {{
                put(new TopicIdPartition(TOPIC_1, 1), DIR_1);
                put(new TopicIdPartition(TOPIC_1, 2), DIR_2);
                put(new TopicIdPartition(TOPIC_1, 3), DIR_3);
                put(new TopicIdPartition(TOPIC_1, 4), DIR_1);
                put(new TopicIdPartition(TOPIC_2, 5), DIR_2);
            }};
        AssignReplicasToDirsRequestData built = AssignmentsManager.buildRequestData(8, 100L, assignment);
        AssignReplicasToDirsRequestData expected = new AssignReplicasToDirsRequestData()
            .setBrokerId(8)
            .setBrokerEpoch(100L)
            .setDirectories(Arrays.asList(
                new AssignReplicasToDirsRequestData.DirectoryData()
                    .setId(DIR_2)
                    .setTopics(Arrays.asList(
                        new AssignReplicasToDirsRequestData.TopicData()
                            .setTopicId(TOPIC_1)
                            .setPartitions(Collections.singletonList(
                                    new AssignReplicasToDirsRequestData.PartitionData()
                                            .setPartitionIndex(2))),
                new AssignReplicasToDirsRequestData.TopicData()
                    .setTopicId(TOPIC_2)
                    .setPartitions(Collections.singletonList(
                            new AssignReplicasToDirsRequestData.PartitionData()
                                    .setPartitionIndex(5))))),
            new AssignReplicasToDirsRequestData.DirectoryData()
                .setId(DIR_3)
                .setTopics(Collections.singletonList(
                    new AssignReplicasToDirsRequestData.TopicData()
                        .setTopicId(TOPIC_1)
                        .setPartitions(Collections.singletonList(
                            new AssignReplicasToDirsRequestData.PartitionData()
                                    .setPartitionIndex(3))))),
            new AssignReplicasToDirsRequestData.DirectoryData()
                .setId(DIR_1)
                .setTopics(Collections.singletonList(
                    new AssignReplicasToDirsRequestData.TopicData()
                        .setTopicId(TOPIC_1)
                        .setPartitions(Arrays.asList(
                            new AssignReplicasToDirsRequestData.PartitionData()
                                .setPartitionIndex(4),
                            new AssignReplicasToDirsRequestData.PartitionData()
                                .setPartitionIndex(1)))))));
        assertRequestEquals(expected, built);
    }

    @Test
    public void testAssignmentAggregation() throws InterruptedException {
        CountDownLatch readyToAssert = new CountDownLatch(1);
        doAnswer(invocation -> {
            if (readyToAssert.getCount() > 0) {
                readyToAssert.countDown();
            }
            return null;
        }).when(channelManager).sendRequest(any(AssignReplicasToDirsRequest.Builder.class),
            any(ControllerRequestCompletionHandler.class));

        manager.onAssignment(new TopicIdPartition(TOPIC_1, 1), DIR_1, () -> { });
        manager.onAssignment(new TopicIdPartition(TOPIC_1, 2), DIR_2, () -> { });
        manager.onAssignment(new TopicIdPartition(TOPIC_1, 3), DIR_3, () -> { });
        manager.onAssignment(new TopicIdPartition(TOPIC_1, 4), DIR_1, () -> { });
        manager.onAssignment(new TopicIdPartition(TOPIC_2, 5), DIR_2, () -> { });
        while (!readyToAssert.await(1, TimeUnit.MILLISECONDS)) {
            time.sleep(100);
            manager.wakeup();
        }

        ArgumentCaptor<AssignReplicasToDirsRequest.Builder> captor =
            ArgumentCaptor.forClass(AssignReplicasToDirsRequest.Builder.class);
        verify(channelManager, times(1)).start();
        verify(channelManager).sendRequest(captor.capture(), any(ControllerRequestCompletionHandler.class));
        verify(channelManager, atMostOnce()).shutdown();
        verifyNoMoreInteractions(channelManager);
        assertEquals(1, captor.getAllValues().size());
        AssignReplicasToDirsRequestData actual = captor.getValue().build().data();
        AssignReplicasToDirsRequestData expected = buildRequestData(
            8, 100L, new HashMap<TopicIdPartition, Uuid>() {{
                    put(new TopicIdPartition(TOPIC_1, 1), DIR_1);
                    put(new TopicIdPartition(TOPIC_1, 2), DIR_2);
                    put(new TopicIdPartition(TOPIC_1, 3), DIR_3);
                    put(new TopicIdPartition(TOPIC_1, 4), DIR_1);
                    put(new TopicIdPartition(TOPIC_2, 5), DIR_2);
                }}
        );
        assertRequestEquals(expected, actual);
    }

    @Test
    void testRequeuesFailedAssignmentPropagations() throws InterruptedException {
        CountDownLatch readyToAssert = new CountDownLatch(5);
        doAnswer(invocation -> {
            if (readyToAssert.getCount() > 0) {
                readyToAssert.countDown();
            }
            if (readyToAssert.getCount() == 4) {
                invocation.getArgument(1, ControllerRequestCompletionHandler.class).onTimeout();
                manager.onAssignment(new TopicIdPartition(TOPIC_1, 2), DIR_3, () -> { });
            }
            if (readyToAssert.getCount() == 3) {
                invocation.getArgument(1, ControllerRequestCompletionHandler.class).onComplete(
                    new ClientResponse(null, null, null, 0L, 0L, false, false,
                        new UnsupportedVersionException("test unsupported version exception"), null, null));

                // duplicate should be ignored
                manager.onAssignment(new TopicIdPartition(TOPIC_1, 2), DIR_3, () -> { });

                manager.onAssignment(new TopicIdPartition(TOPIC_1, 3),
                     Uuid.fromString("xHLCnG54R9W3lZxTPnpk1Q"), () -> { });
            }
            if (readyToAssert.getCount() == 2) {
                invocation.getArgument(1, ControllerRequestCompletionHandler.class).onComplete(
                        new ClientResponse(null, null, null, 0L, 0L, false, false, null,
                                new AuthenticationException("test authentication exception"), null)
                );

                // duplicate should be ignored
                manager.onAssignment(new TopicIdPartition(TOPIC_1, 3),
                     Uuid.fromString("xHLCnG54R9W3lZxTPnpk1Q"), () -> { }); 

                manager.onAssignment(new TopicIdPartition(TOPIC_1, 4),
                     Uuid.fromString("RCYu1A0CTa6eEIpuKDOfxw"), () -> { });
            }
            if (readyToAssert.getCount() == 1) {
                invocation.getArgument(1, ControllerRequestCompletionHandler.class).onComplete(
                    new ClientResponse(null, null, null, 0L, 0L, false, false, null, null,
                        new AssignReplicasToDirsResponse(new AssignReplicasToDirsResponseData()
                            .setErrorCode(Errors.NOT_CONTROLLER.code())
                            .setThrottleTimeMs(0))));
            }
            return null;
        }).when(channelManager).sendRequest(any(AssignReplicasToDirsRequest.Builder.class),
            any(ControllerRequestCompletionHandler.class));

        manager.onAssignment(new TopicIdPartition(TOPIC_1, 1), DIR_1, () -> { });
        while (!readyToAssert.await(1, TimeUnit.MILLISECONDS)) {
            time.sleep(TimeUnit.SECONDS.toMillis(1));
            manager.wakeup();
        }

        ArgumentCaptor<AssignReplicasToDirsRequest.Builder> captor =
            ArgumentCaptor.forClass(AssignReplicasToDirsRequest.Builder.class);
        verify(channelManager, times(1)).start();
        verify(channelManager, times(5)).sendRequest(captor.capture(),
            any(ControllerRequestCompletionHandler.class));
        verify(channelManager, atMostOnce()).shutdown();
        verifyNoMoreInteractions(channelManager);
        assertEquals(5, captor.getAllValues().size());
        assertRequestEquals(buildRequestData(
            8, 100L, new HashMap<TopicIdPartition, Uuid>() {{
                    put(new TopicIdPartition(TOPIC_1, 1), DIR_1);
                }}
        ), captor.getAllValues().get(0).build().data());
        assertRequestEquals(buildRequestData(
            8, 100L, new HashMap<TopicIdPartition, Uuid>() {{
                    put(new TopicIdPartition(TOPIC_1, 1), DIR_1);
                    put(new TopicIdPartition(TOPIC_1, 2), DIR_3);
                }}
        ), captor.getAllValues().get(1).build().data());
        assertRequestEquals(buildRequestData(
            8, 100L, new HashMap<TopicIdPartition, Uuid>() {{
                    put(new TopicIdPartition(TOPIC_1, 1), DIR_1);
                    put(new TopicIdPartition(TOPIC_1, 2), DIR_3);
                    put(new TopicIdPartition(TOPIC_1, 3), Uuid.fromString("xHLCnG54R9W3lZxTPnpk1Q"));
                    put(new TopicIdPartition(TOPIC_1, 4), Uuid.fromString("RCYu1A0CTa6eEIpuKDOfxw"));
                }}
        ), captor.getAllValues().get(4).build().data());
    }

    @Timeout(30)
    @Test
    void testOnCompletion() throws Exception {
        CountDownLatch readyToAssert = new CountDownLatch(300);
        doAnswer(invocation -> {
            AssignReplicasToDirsRequestData request = invocation.getArgument(0, AssignReplicasToDirsRequest.Builder.class).build().data();
            ControllerRequestCompletionHandler completionHandler = invocation.getArgument(1, ControllerRequestCompletionHandler.class);
            Map<Uuid, Map<TopicIdPartition, Errors>> errors = new HashMap<>();
            for (AssignReplicasToDirsRequestData.DirectoryData directory : request.directories()) {
                for (AssignReplicasToDirsRequestData.TopicData topic : directory.topics()) {
                    for (AssignReplicasToDirsRequestData.PartitionData partition : topic.partitions()) {
                        TopicIdPartition topicIdPartition = new TopicIdPartition(topic.topicId(), partition.partitionIndex());
                        errors.computeIfAbsent(directory.id(), d -> new HashMap<>()).put(topicIdPartition, Errors.NONE);
                    }
                }
            }
            AssignReplicasToDirsResponseData responseData = AssignmentsHelper.buildResponseData(Errors.NONE.code(), 0, errors);
            completionHandler.onComplete(new ClientResponse(null, null, null,
                    0L, 0L, false, false, null, null,
                            new AssignReplicasToDirsResponse(responseData)));

            return null;
        }).when(channelManager).sendRequest(any(AssignReplicasToDirsRequest.Builder.class),
                any(ControllerRequestCompletionHandler.class));

        for (int i = 0; i < 300; i++) {
            manager.onAssignment(new TopicIdPartition(TOPIC_1, i % 5), DIR_1, readyToAssert::countDown);
        }

        while (!readyToAssert.await(1, TimeUnit.MILLISECONDS)) {
            time.sleep(TimeUnit.SECONDS.toMillis(1));
            manager.wakeup();
        }
    }

    static Metric findMetric(String name) {
        for (Map.Entry<MetricName, Metric> entry : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            MetricName metricName = entry.getKey();
            if (AssignmentsManager.class.getSimpleName().equals(metricName.getType()) && metricName.getName().equals(name)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("metric named " + name + " not found");
    }

    @SuppressWarnings("unchecked")
    @Test
    void testQueuedReplicaToDirAssignmentsMetric() throws Exception {
        CountDownLatch readyToAssert = new CountDownLatch(1);
        doAnswer(invocation -> {
            readyToAssert.countDown();
            return null;
        }).when(channelManager).sendRequest(any(AssignReplicasToDirsRequest.Builder.class), any(ControllerRequestCompletionHandler.class));

        Gauge<Integer> queuedReplicaToDirAssignments = (Gauge<Integer>) findMetric(AssignmentsManager.QUEUE_REPLICA_TO_DIR_ASSIGNMENTS_METRIC_NAME);
        assertEquals(0, queuedReplicaToDirAssignments.value());

        for (int i = 0; i < 4; i++) {
            manager.onAssignment(new TopicIdPartition(TOPIC_1, i), DIR_1, () -> { });
        }
        while (!readyToAssert.await(1, TimeUnit.MILLISECONDS)) {
            time.sleep(100);
        }
        assertEquals(4, queuedReplicaToDirAssignments.value());

        for (int i = 4; i < 8; i++) {
            manager.onAssignment(new TopicIdPartition(TOPIC_1, i), DIR_1, () -> { });
        }
        TestUtils.retryOnExceptionWithTimeout(5_000, () -> assertEquals(8, queuedReplicaToDirAssignments.value()));
    }
}
