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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsResponseData;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.AssignReplicasToDirsRequest;
import org.apache.kafka.common.requests.AssignReplicasToDirsResponse;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;

import static org.apache.kafka.server.AssignmentsManager.QUEUED_REPLICA_TO_DIR_ASSIGNMENTS_METRIC;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssignmentsManagerTest {
    private static final Logger LOG = LoggerFactory.getLogger(AssignmentsManagerTest.class);
    private static final Uuid TOPIC_1 = Uuid.fromString("88rnFIqYSZykX4ZSKv81bg");
    private static final Uuid TOPIC_2 = Uuid.fromString("VKCnzHdhR5uDQc1shqBYrQ");
    private static final Uuid TOPIC_3 = Uuid.fromString("ZeAwvYt-Ro2suQudGUdbRg");
    private static final Uuid DIR_1 = Uuid.fromString("cbgD8WdLQCyzLrFIMBhv3w");
    private static final Uuid DIR_2 = Uuid.fromString("zO0bDc0vSuam7Db9iH7rYQ");
    private static final Uuid DIR_3 = Uuid.fromString("CGBWbrFkRkeJQy6Aryzq2Q");

    private static final MetadataImage TEST_IMAGE;

    static {
        MetadataDelta delta = new MetadataDelta.Builder().
            setImage(MetadataImage.EMPTY).
            build();
        delta.replay(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(MetadataVersion.IBP_3_8_IV0.featureLevel()));
        delta.replay(new RegisterBrokerRecord().
            setBrokerId(0).
            setIncarnationId(Uuid.fromString("JJsH6zB0R7eKbr0Sy49ULw")).
            setBrokerEpoch(123));
        delta.replay(new RegisterBrokerRecord().
            setBrokerId(1).
            setIncarnationId(Uuid.fromString("DtnWclXyQ4qNDvL97JlnvQ")).
            setBrokerEpoch(456));
        delta.replay(new RegisterBrokerRecord().
            setBrokerId(2).
            setIncarnationId(Uuid.fromString("UFa_RKgLR4mxEXyquEPEmg")).
            setBrokerEpoch(789));
        delta.replay(new RegisterBrokerRecord().
            setBrokerId(3).
            setIncarnationId(Uuid.fromString("jj-cnHYASAmb_H9JR6nmtQ")).
            setBrokerEpoch(987));
        delta.replay(new TopicRecord().
            setName("foo").
            setTopicId(TOPIC_1));
        delta.replay(new PartitionRecord().
            setPartitionId(0).
            setTopicId(TOPIC_1).
            setReplicas(Arrays.asList(0, 1, 2)).
            setIsr(Arrays.asList(0, 1, 2)).
            setLeader(1));
        delta.replay(new PartitionRecord().
            setPartitionId(1).
            setTopicId(TOPIC_1).
            setReplicas(Arrays.asList(1, 2, 3)).
            setIsr(Arrays.asList(1, 2, 3)).
            setLeader(1));
        delta.replay(new TopicRecord().
            setName("bar").
            setTopicId(TOPIC_2));
        delta.replay(new PartitionRecord().
            setPartitionId(0).
            setTopicId(TOPIC_2).
            setReplicas(Arrays.asList(0, 3, 2)).
            setIsr(Arrays.asList(0, 3, 2)).
            setLeader(1));
        delta.replay(new PartitionRecord().
            setPartitionId(1).
            setTopicId(TOPIC_2).
            setReplicas(Arrays.asList(1, 2, 3)).
            setIsr(Arrays.asList(2)).
            setLeader(2));
        delta.replay(new PartitionRecord().
            setPartitionId(2).
            setTopicId(TOPIC_2).
            setReplicas(Arrays.asList(3, 2, 1)).
            setIsr(Arrays.asList(3, 2, 1)).
            setLeader(3));
        TEST_IMAGE = delta.apply(MetadataProvenance.EMPTY);
    }

    static class MockNodeToControllerChannelManager implements NodeToControllerChannelManager {
        LinkedBlockingDeque<Map.Entry<AssignReplicasToDirsRequestData, ControllerRequestCompletionHandler>> callbacks =
            new LinkedBlockingDeque<>();

        @Override
        public void start() {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public Optional<NodeApiVersions> controllerApiVersions() {
            return Optional.empty();
        }

        @Override
        public void sendRequest(
            AbstractRequest.Builder<? extends AbstractRequest> request,
            ControllerRequestCompletionHandler callback
        ) {
            AssignReplicasToDirsRequest inputRequest = (AssignReplicasToDirsRequest) request.build();
            synchronized (this) {
                callbacks.add(new AbstractMap.SimpleEntry<>(inputRequest.data(), callback));
            }
        }

        @Override
        public long getTimeoutMs() {
            return 0L;
        }

        void completeCallback(Function<AssignReplicasToDirsRequestData, Optional<ClientResponse>> completionist) throws InterruptedException {
            Map.Entry<AssignReplicasToDirsRequestData, ControllerRequestCompletionHandler> entry = callbacks.take();
            Optional<ClientResponse> clientResponse = completionist.apply(entry.getKey());
            if (clientResponse.isPresent()) {
                entry.getValue().onComplete(clientResponse.get());
            } else {
                entry.getValue().onTimeout();
            }
        }
    }

    static class TestEnv implements AutoCloseable {
        final ExponentialBackoff backoff;
        final MockNodeToControllerChannelManager channelManager;
        final MetricsRegistry metricsRegistry = new MetricsRegistry();
        final AssignmentsManager assignmentsManager;
        final Map<TopicIdPartition, Integer> successes;

        TestEnv() {
            this.backoff = new ExponentialBackoff(1, 2, 4, 0);
            this.channelManager = new MockNodeToControllerChannelManager();
            this.assignmentsManager = new AssignmentsManager(
                    backoff, Time.SYSTEM, channelManager, 1, () -> TEST_IMAGE,
                        t -> t.toString(), metricsRegistry);
            this.successes = new HashMap<>();
        }

        void onAssignment(TopicIdPartition topicIdPartition, Uuid directoryId) {
            assignmentsManager.onAssignment(topicIdPartition, directoryId, "test", () -> {
                synchronized (successes) {
                    successes.put(topicIdPartition, successes.getOrDefault(topicIdPartition, 0) + 1);
                }
            });
        }

        int success(TopicIdPartition topicIdPartition) {
            synchronized (successes) {
                return successes.getOrDefault(topicIdPartition, 0);
            }
        }

        void successfullyCompleteCallbackOfRequestAssigningTopic1ToDir1() throws Exception {
            channelManager.completeCallback(req -> {
                AssignReplicasToDirsRequestData.DirectoryData directoryData = req.directories().get(0);
                assertEquals(DIR_1, directoryData.id());
                AssignReplicasToDirsRequestData.TopicData topicData = directoryData.topics().get(0);
                assertEquals(TOPIC_1, topicData.topicId());
                assertEquals(0, topicData.partitions().get(0).partitionIndex());
                return mockClientResponse(new AssignReplicasToDirsResponseData().
                    setDirectories(Arrays.asList(new AssignReplicasToDirsResponseData.DirectoryData().
                        setId(DIR_1).
                        setTopics(Arrays.asList(new AssignReplicasToDirsResponseData.TopicData().
                            setTopicId(TOPIC_1).
                            setPartitions(Arrays.asList(new AssignReplicasToDirsResponseData.PartitionData().
                                setPartitionIndex(0).
                                setErrorCode((short) 0))))))));
            });
        }

        Metric findMetric(MetricName name) {
            for (Map.Entry<MetricName, Metric> entry : metricsRegistry.allMetrics().entrySet()) {
                if (name.equals(entry.getKey())) {
                    return entry.getValue();
                }
            }
            throw new IllegalArgumentException("metric named " + name + " not found");
        }

        @SuppressWarnings("unchecked") // do not warn about Gauge typecast.
        int queuedReplicaToDirAssignments() {
            Gauge<Integer> queuedReplicaToDirAssignments =
                    (Gauge<Integer>) findMetric(QUEUED_REPLICA_TO_DIR_ASSIGNMENTS_METRIC);
            return queuedReplicaToDirAssignments.value();
        }

        @Override
        public void close() throws Exception {
            try {
                assignmentsManager.close();
            } catch (Exception e) {
                LOG.error("error shutting down assignmentsManager", e);
            }
            try {
                metricsRegistry.shutdown();
            } catch (Exception e) {
                LOG.error("error shutting down metricsRegistry", e);
            }
        }
    }

    static Optional<ClientResponse> mockClientResponse(AssignReplicasToDirsResponseData data) {
        return Optional.of(new ClientResponse(null, null, "", 0, 0, false,
            null, null, new AssignReplicasToDirsResponse(data)));
    }

    @Test
    public void testStartAndShutdown() throws Exception {
        try (TestEnv testEnv = new TestEnv()) {
        }
    }

    @Test
    public void testSuccessfulAssignment() throws Exception {
        try (TestEnv testEnv = new TestEnv()) {
            assertEquals(0, testEnv.queuedReplicaToDirAssignments());
            testEnv.onAssignment(new TopicIdPartition(TOPIC_1, 0), DIR_1);
            TestUtils.retryOnExceptionWithTimeout(60_000, () -> {
                assertEquals(1, testEnv.assignmentsManager.numPending());
                assertEquals(1, testEnv.queuedReplicaToDirAssignments());
            });
            assertEquals(0, testEnv.assignmentsManager.previousGlobalFailures());
            assertEquals(1, testEnv.assignmentsManager.numInFlight());
            testEnv.successfullyCompleteCallbackOfRequestAssigningTopic1ToDir1();
            TestUtils.retryOnExceptionWithTimeout(60_000, () -> {
                assertEquals(0, testEnv.assignmentsManager.numPending());
                assertEquals(0, testEnv.queuedReplicaToDirAssignments());
                assertEquals(1, testEnv.success(new TopicIdPartition(TOPIC_1, 0)));
            });
            assertEquals(0, testEnv.assignmentsManager.previousGlobalFailures());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalidRequest", "timeout"})
    public void testUnSuccessfulRequestCausesRetransmission(String failureType) throws Exception {
        try (TestEnv testEnv = new TestEnv()) {
            testEnv.onAssignment(new TopicIdPartition(TOPIC_1, 0), DIR_1);
            TestUtils.retryOnExceptionWithTimeout(60_000, () -> {
                assertEquals(1, testEnv.assignmentsManager.numPending());
            });
            if (failureType.equals("invalidRequest")) {
                testEnv.channelManager.completeCallback(req -> {
                    return mockClientResponse(new AssignReplicasToDirsResponseData().
                        setErrorCode(Errors.INVALID_REQUEST.code()));
                });
            } else if (failureType.equals("timeout")) {
                testEnv.channelManager.completeCallback(req -> Optional.empty());
            }
            TestUtils.retryOnExceptionWithTimeout(60_000, () -> {
                assertEquals(1, testEnv.assignmentsManager.numPending());
                assertEquals(0, testEnv.success(new TopicIdPartition(TOPIC_1, 0)));
            });
            assertEquals(1, testEnv.assignmentsManager.previousGlobalFailures());
            testEnv.successfullyCompleteCallbackOfRequestAssigningTopic1ToDir1();
            TestUtils.retryOnExceptionWithTimeout(60_000, () -> {
                assertEquals(0, testEnv.assignmentsManager.numPending());
                assertEquals(1, testEnv.success(new TopicIdPartition(TOPIC_1, 0)));
            });
            assertEquals(0, testEnv.assignmentsManager.previousGlobalFailures());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"missingTopic", "missingPartition", "notReplica"})
    public void testMismatchedInputDoesNotTriggerCompletion(String mismatchType) throws Exception {
        try (TestEnv testEnv = new TestEnv()) {
            TopicIdPartition target;
            if (mismatchType.equals("missingTopic")) {
                target = new TopicIdPartition(TOPIC_3, 0);
            } else if (mismatchType.equals("missingPartition")) {
                target = new TopicIdPartition(TOPIC_1, 2);
            } else if (mismatchType.equals("notReplica")) {
                target = new TopicIdPartition(TOPIC_2, 0);
            } else {
                throw new RuntimeException("invalid mismatchType argument.");
            }
            testEnv.onAssignment(target, DIR_1);
            TestUtils.retryOnExceptionWithTimeout(60_000, () -> {
                assertEquals(0, testEnv.assignmentsManager.numPending());
                assertEquals(0, testEnv.success(target));
            });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"missingResult", "errorResult"})
    public void testOneAssignmentFailsOneSucceeds(String failureType) throws Exception {
        try (TestEnv testEnv = new TestEnv()) {
            testEnv.onAssignment(new TopicIdPartition(TOPIC_1, 0), DIR_1);
            testEnv.onAssignment(new TopicIdPartition(TOPIC_1, 1), DIR_1);
            TestUtils.retryOnExceptionWithTimeout(60_000, () -> {
                assertEquals(2, testEnv.assignmentsManager.numPending());
                assertEquals(0, testEnv.success(new TopicIdPartition(TOPIC_1, 0)));
                assertEquals(0, testEnv.success(new TopicIdPartition(TOPIC_1, 1)));
            });
            testEnv.channelManager.completeCallback(req -> {
                AssignReplicasToDirsRequestData.DirectoryData directoryData = req.directories().get(0);
                assertEquals(DIR_1, directoryData.id());
                AssignReplicasToDirsRequestData.TopicData topicData = directoryData.topics().get(0);
                assertEquals(TOPIC_1, topicData.topicId());
                HashSet<Integer> foundPartitions = new HashSet<>();
                topicData.partitions().forEach(p -> foundPartitions.add(p.partitionIndex()));
                List<AssignReplicasToDirsResponseData.PartitionData> partitions = new ArrayList<>();
                if (foundPartitions.contains(0)) {
                    partitions.add(new AssignReplicasToDirsResponseData.PartitionData().
                        setPartitionIndex(0).
                        setErrorCode((short) 0));
                }
                if (foundPartitions.contains(1)) {
                    if (failureType.equals("missingResult")) {
                        // do nothing
                    } else if (failureType.equals("errorResult")) {
                        partitions.add(new AssignReplicasToDirsResponseData.PartitionData().
                            setPartitionIndex(1).
                            setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code()));
                    } else {
                        throw new RuntimeException("invalid failureType argument.");
                    }
                }
                return mockClientResponse(new AssignReplicasToDirsResponseData().
                    setDirectories(Arrays.asList(new AssignReplicasToDirsResponseData.DirectoryData().
                        setId(DIR_1).
                        setTopics(Arrays.asList(new AssignReplicasToDirsResponseData.TopicData().
                            setTopicId(TOPIC_1).
                            setPartitions(partitions))))));
            });
            TestUtils.retryOnExceptionWithTimeout(60_000, () -> {
                assertEquals(1, testEnv.assignmentsManager.numPending());
                assertEquals(1, testEnv.assignmentsManager.numInFlight());
                assertEquals(1, testEnv.success(new TopicIdPartition(TOPIC_1, 0)));
                assertEquals(0, testEnv.success(new TopicIdPartition(TOPIC_1, 1)));
                assertEquals(0, testEnv.assignmentsManager.previousGlobalFailures());
            });
        }
    }

    @Test
    public void testGlobalResponseErrorTimeout() {
        assertEquals(Optional.of("Timeout"),
            AssignmentsManager.globalResponseError(Optional.empty()));
    }

    @Test
    public void testNoGlobalResponseError() {
        assertEquals(Optional.empty(),
            AssignmentsManager.globalResponseError(Optional.of(
                new ClientResponse(null, null, "", 0, 0, false, null,
                    null, new AssignReplicasToDirsResponse(
                        new AssignReplicasToDirsResponseData())))));
    }

    @Test
    public void testGlobalResponseErrorAuthenticationException() {
        assertEquals(Optional.of("AuthenticationException"),
            AssignmentsManager.globalResponseError(Optional.of(
                new ClientResponse(null, null, "", 0, 0, false, null,
                    new AuthenticationException("failed"), null))));
    }

    @Test
    public void testGlobalResponseErrorUnsupportedVersionException() {
        assertEquals(Optional.of("UnsupportedVersionException"),
            AssignmentsManager.globalResponseError(Optional.of(
                new ClientResponse(null, null, "", 0, 0, false,
                    new UnsupportedVersionException("failed"), null, null))));
    }

    @Test
    public void testGlobalResponseErrorDisconnectedTimedOut() {
        assertEquals(Optional.of("Disonnected[Timeout]"),
            AssignmentsManager.globalResponseError(Optional.of(
                new ClientResponse(null, null, "", 0, 0, true, true,
                   null, null, null))));
    }

    @Test
    public void testGlobalResponseErrorEmptyResponse() {
        assertEquals(Optional.of("EmptyResponse"),
            AssignmentsManager.globalResponseError(Optional.of(
                new ClientResponse(null, null, "", 0, 0, false, false,
                        null, null, null))));
    }

    @Test
    public void testGlobalResponseErrorClassCastException() {
        assertEquals(Optional.of("ClassCastException"),
            AssignmentsManager.globalResponseError(Optional.of(
                new ClientResponse(null, null, "", 0, 0, false, false,
                    null, null, new ApiVersionsResponse(new ApiVersionsResponseData())))));
    }

    @Test
    public void testGlobalResponseErrorResponseLevelError() {
        assertEquals(Optional.of("Response-level error: INVALID_REQUEST"),
            AssignmentsManager.globalResponseError(Optional.of(
                new ClientResponse(null, null, "", 0, 0, false, false,
                        null, null, new AssignReplicasToDirsResponse(
                            new AssignReplicasToDirsResponseData().
                                setErrorCode(Errors.INVALID_REQUEST.code()))))));
    }

    @Test
    void testBuildRequestData() {
        Map<TopicIdPartition, Uuid> assignments = new LinkedHashMap<>();
        assignments.put(new TopicIdPartition(TOPIC_1, 1), DIR_1);
        assignments.put(new TopicIdPartition(TOPIC_1, 2), DIR_2);
        assignments.put(new TopicIdPartition(TOPIC_1, 3), DIR_3);
        assignments.put(new TopicIdPartition(TOPIC_1, 4), DIR_1);
        assignments.put(new TopicIdPartition(TOPIC_2, 5), DIR_2);
        Map<TopicIdPartition, Assignment> targetAssignments = new LinkedHashMap<>();
        assignments.entrySet().forEach(e -> targetAssignments.put(e.getKey(),
            new Assignment(e.getKey(), e.getValue(), 0, () -> { })));
        AssignReplicasToDirsRequestData built =
            AssignmentsManager.buildRequestData(8, 100L, targetAssignments);
        AssignReplicasToDirsRequestData expected = new AssignReplicasToDirsRequestData().
            setBrokerId(8).
            setBrokerEpoch(100L).
            setDirectories(Arrays.asList(
                new AssignReplicasToDirsRequestData.DirectoryData().
                    setId(DIR_2).
                    setTopics(Arrays.asList(
                        new AssignReplicasToDirsRequestData.TopicData().
                            setTopicId(TOPIC_1).
                            setPartitions(Collections.singletonList(
                                new AssignReplicasToDirsRequestData.PartitionData().
                                    setPartitionIndex(2))),
                        new AssignReplicasToDirsRequestData.TopicData().
                            setTopicId(TOPIC_2).
                            setPartitions(Collections.singletonList(
                                new AssignReplicasToDirsRequestData.PartitionData().
                                    setPartitionIndex(5))))),
                new AssignReplicasToDirsRequestData.DirectoryData().
                    setId(DIR_3).
                    setTopics(Collections.singletonList(
                        new AssignReplicasToDirsRequestData.TopicData().
                            setTopicId(TOPIC_1).
                            setPartitions(Collections.singletonList(
                                new AssignReplicasToDirsRequestData.PartitionData().
                                    setPartitionIndex(3))))),
                new AssignReplicasToDirsRequestData.DirectoryData().
                    setId(DIR_1).
                    setTopics(Collections.singletonList(
                        new AssignReplicasToDirsRequestData.TopicData().
                            setTopicId(TOPIC_1).
                            setPartitions(Arrays.asList(
                                new AssignReplicasToDirsRequestData.PartitionData().
                                    setPartitionIndex(1),
                                new AssignReplicasToDirsRequestData.PartitionData().
                                    setPartitionIndex(4)))))));
        assertEquals(expected, built);
    }
}
