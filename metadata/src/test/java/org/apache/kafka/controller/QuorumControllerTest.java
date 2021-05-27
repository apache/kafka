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

package org.apache.kafka.controller;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.Listener;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.ListenerCollection;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignmentCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.controller.BrokersToIsrs.TopicIdPartition;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metalog.LocalLogManagerTestEnv;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.BROKER0;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.CONFIGS;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class QuorumControllerTest {
    private static final Logger log =
        LoggerFactory.getLogger(QuorumControllerTest.class);

    /**
     * Test creating a new QuorumController and closing it.
     */
    @Test
    public void testCreateAndClose() throws Throwable {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv(1, Optional.empty())) {
            try (QuorumControllerTestEnv controlEnv =
                     new QuorumControllerTestEnv(logEnv, __ -> { })) {
            }
        }
    }

    /**
     * Test setting some configuration values and reading them back.
     */
    @Test
    public void testConfigurationOperations() throws Throwable {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv(1, Optional.empty())) {
            try (QuorumControllerTestEnv controlEnv =
                     new QuorumControllerTestEnv(logEnv, b -> b.setConfigDefs(CONFIGS))) {
                testConfigurationOperations(controlEnv.activeController());
            }
        }
    }

    private void testConfigurationOperations(QuorumController controller) throws Throwable {
        assertEquals(Collections.singletonMap(BROKER0, ApiError.NONE),
            controller.incrementalAlterConfigs(Collections.singletonMap(
                BROKER0, Collections.singletonMap("baz", entry(SET, "123"))), true).get());
        assertEquals(Collections.singletonMap(BROKER0,
            new ResultOrError<>(Collections.emptyMap())),
            controller.describeConfigs(Collections.singletonMap(
                BROKER0, Collections.emptyList())).get());
        assertEquals(Collections.singletonMap(BROKER0, ApiError.NONE),
            controller.incrementalAlterConfigs(Collections.singletonMap(
                BROKER0, Collections.singletonMap("baz", entry(SET, "123"))), false).get());
        assertEquals(Collections.singletonMap(BROKER0, new ResultOrError<>(Collections.
                singletonMap("baz", "123"))),
            controller.describeConfigs(Collections.singletonMap(
                BROKER0, Collections.emptyList())).get());
    }

    /**
     * Test that an incrementalAlterConfigs operation doesn't complete until the records
     * can be written to the metadata log.
     */
    @Test
    public void testDelayedConfigurationOperations() throws Throwable {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv(1, Optional.empty())) {
            try (QuorumControllerTestEnv controlEnv =
                     new QuorumControllerTestEnv(logEnv, b -> b.setConfigDefs(CONFIGS))) {
                testDelayedConfigurationOperations(logEnv, controlEnv.activeController());
            }
        }
    }

    private void testDelayedConfigurationOperations(LocalLogManagerTestEnv logEnv,
                                                    QuorumController controller)
                                                    throws Throwable {
        logEnv.logManagers().forEach(m -> m.setMaxReadOffset(0L));
        CompletableFuture<Map<ConfigResource, ApiError>> future1 =
            controller.incrementalAlterConfigs(Collections.singletonMap(
                BROKER0, Collections.singletonMap("baz", entry(SET, "123"))), false);
        assertFalse(future1.isDone());
        assertEquals(Collections.singletonMap(BROKER0,
            new ResultOrError<>(Collections.emptyMap())),
            controller.describeConfigs(Collections.singletonMap(
                BROKER0, Collections.emptyList())).get());
        logEnv.logManagers().forEach(m -> m.setMaxReadOffset(1L));
        assertEquals(Collections.singletonMap(BROKER0, ApiError.NONE), future1.get());
    }

    @Test
    public void testUnregisterBroker() throws Throwable {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv(1, Optional.empty())) {
            try (QuorumControllerTestEnv controlEnv =
                     new QuorumControllerTestEnv(logEnv, b -> b.setConfigDefs(CONFIGS))) {
                ListenerCollection listeners = new ListenerCollection();
                listeners.add(new Listener().setName("PLAINTEXT").
                    setHost("localhost").setPort(9092));
                QuorumController active = controlEnv.activeController();
                CompletableFuture<BrokerRegistrationReply> reply = active.registerBroker(
                    new BrokerRegistrationRequestData().
                        setBrokerId(0).
                        setClusterId("06B-K3N1TBCNYFgruEVP0Q").
                        setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwBA")).
                        setListeners(listeners));
                assertEquals(0L, reply.get().epoch());
                CreateTopicsRequestData createTopicsRequestData =
                    new CreateTopicsRequestData().setTopics(
                        new CreatableTopicCollection(Collections.singleton(
                            new CreatableTopic().setName("foo").setNumPartitions(1).
                                setReplicationFactor((short) 1)).iterator()));
                assertEquals(Errors.INVALID_REPLICATION_FACTOR.code(), active.createTopics(
                    createTopicsRequestData).get().topics().find("foo").errorCode());
                assertEquals("Unable to replicate the partition 1 time(s): All brokers " +
                    "are currently fenced.", active.createTopics(
                    createTopicsRequestData).get().topics().find("foo").errorMessage());
                assertEquals(new BrokerHeartbeatReply(true, false, false, false),
                    active.processBrokerHeartbeat(new BrokerHeartbeatRequestData().
                            setWantFence(false).setBrokerEpoch(0L).setBrokerId(0).
                            setCurrentMetadataOffset(100000L)).get());
                assertEquals(Errors.NONE.code(), active.createTopics(
                    createTopicsRequestData).get().topics().find("foo").errorCode());
                CompletableFuture<TopicIdPartition> topicPartitionFuture = active.appendReadEvent(
                    "debugGetPartition", () -> {
                        Iterator<TopicIdPartition> iterator = active.
                            replicationControl().brokersToIsrs().iterator(0, true);
                        assertTrue(iterator.hasNext());
                        return iterator.next();
                    });
                assertEquals(0, topicPartitionFuture.get().partitionId());
                active.unregisterBroker(0).get();
                topicPartitionFuture = active.appendReadEvent(
                    "debugGetPartition", () -> {
                        Iterator<TopicIdPartition> iterator = active.
                            replicationControl().brokersToIsrs().partitionsWithNoLeader();
                        assertTrue(iterator.hasNext());
                        return iterator.next();
                    });
                assertEquals(0, topicPartitionFuture.get().partitionId());
            }
        }
    }

    static class MockSnapshotWriterBuilder implements Function<Long, SnapshotWriter> {
        final LinkedBlockingDeque<MockSnapshotWriter> writers = new LinkedBlockingDeque<>();

        @Override
        public SnapshotWriter apply(Long epoch) {
            MockSnapshotWriter writer = new MockSnapshotWriter(epoch);
            writers.add(writer);
            return writer;
        }
    }

    @Test
    public void testSnapshotSaveAndLoad() throws Throwable {
        MockSnapshotWriterBuilder snapshotWriterBuilder = new MockSnapshotWriterBuilder();
        final int numBrokers = 4;
        MockSnapshotWriter writer = null;
        Map<Integer, Long> brokerEpochs = new HashMap<>();
        Uuid fooId;
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv(3, Optional.empty())) {
            try (QuorumControllerTestEnv controlEnv =
                     new QuorumControllerTestEnv(logEnv, b -> b.setConfigDefs(CONFIGS).
                         setSnapshotWriterBuilder(snapshotWriterBuilder))) {
                QuorumController active = controlEnv.activeController();
                for (int i = 0; i < numBrokers; i++) {
                    BrokerRegistrationReply reply = active.registerBroker(
                        new BrokerRegistrationRequestData().
                            setBrokerId(i).
                            setRack(null).
                            setClusterId("06B-K3N1TBCNYFgruEVP0Q").
                            setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB" + i)).
                            setListeners(new ListenerCollection(Arrays.asList(new Listener().
                                setName("PLAINTEXT").setHost("localhost").
                                setPort(9092 + i)).iterator()))).get();
                    brokerEpochs.put(i, reply.epoch());
                }
                for (int i = 0; i < numBrokers - 1; i++) {
                    assertEquals(new BrokerHeartbeatReply(true, false, false, false),
                        active.processBrokerHeartbeat(new BrokerHeartbeatRequestData().
                            setWantFence(false).setBrokerEpoch(brokerEpochs.get(i)).
                            setBrokerId(i).setCurrentMetadataOffset(100000L)).get());
                }
                CreateTopicsResponseData fooData = active.createTopics(
                    new CreateTopicsRequestData().setTopics(
                        new CreatableTopicCollection(Collections.singleton(
                            new CreatableTopic().setName("foo").setNumPartitions(-1).
                                setReplicationFactor((short) -1).
                                setAssignments(new CreatableReplicaAssignmentCollection(
                                    Arrays.asList(new CreatableReplicaAssignment().
                                        setPartitionIndex(0).
                                        setBrokerIds(Arrays.asList(0, 1, 2)),
                                    new CreatableReplicaAssignment().
                                        setPartitionIndex(1).
                                        setBrokerIds(Arrays.asList(1, 2, 0))).
                                            iterator()))).iterator()))).get();
                fooId = fooData.topics().find("foo").topicId();
                long snapshotEpoch = active.beginWritingSnapshot().get();
                writer = snapshotWriterBuilder.writers.takeFirst();
                assertEquals(snapshotEpoch, writer.epoch());
                writer.waitForCompletion();
                checkSnapshotContents(fooId, brokerEpochs, writer.batches().iterator());
            }
        }

        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv(3, Optional.of(writer.toReader()))) {
            try (QuorumControllerTestEnv controlEnv =
                     new QuorumControllerTestEnv(logEnv, b -> b.setConfigDefs(CONFIGS).
                         setSnapshotWriterBuilder(snapshotWriterBuilder))) {
                QuorumController active = controlEnv.activeController();
                long snapshotEpoch = active.beginWritingSnapshot().get();
                writer = snapshotWriterBuilder.writers.takeFirst();
                assertEquals(snapshotEpoch, writer.epoch());
                writer.waitForCompletion();
                checkSnapshotContents(fooId, brokerEpochs, writer.batches().iterator());
            }
        }
    }

    private void checkSnapshotContents(Uuid fooId,
                                       Map<Integer, Long> brokerEpochs,
                                       Iterator<List<ApiMessageAndVersion>> iterator) throws Exception {
        ControllerTestUtils.assertBatchIteratorContains(Arrays.asList(
            Arrays.asList(new ApiMessageAndVersion(new TopicRecord().
                    setName("foo").setTopicId(fooId), (short) 0),
                new ApiMessageAndVersion(new PartitionRecord().setPartitionId(0).
                    setTopicId(fooId).setReplicas(Arrays.asList(0, 1, 2)).
                    setIsr(Arrays.asList(0, 1, 2)).setRemovingReplicas(null).
                    setAddingReplicas(null).setLeader(0).setLeaderEpoch(0).
                    setPartitionEpoch(0), (short) 0),
                new ApiMessageAndVersion(new PartitionRecord().setPartitionId(1).
                    setTopicId(fooId).setReplicas(Arrays.asList(1, 2, 0)).
                    setIsr(Arrays.asList(1, 2, 0)).setRemovingReplicas(null).
                    setAddingReplicas(null).setLeader(1).setLeaderEpoch(0).
                    setPartitionEpoch(0), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new RegisterBrokerRecord().
                    setBrokerId(0).setBrokerEpoch(brokerEpochs.get(0)).
                    setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB0")).
                    setEndPoints(new BrokerEndpointCollection(Arrays.asList(
                        new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                            setPort(9092).setSecurityProtocol((short) 0)).iterator())).
                    setRack(null), (short) 0),
                new ApiMessageAndVersion(new UnfenceBrokerRecord().
                    setId(0).setEpoch(brokerEpochs.get(0)), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new RegisterBrokerRecord().
                    setBrokerId(1).setBrokerEpoch(brokerEpochs.get(1)).
                    setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB1")).
                    setEndPoints(new BrokerEndpointCollection(Arrays.asList(
                        new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                            setPort(9093).setSecurityProtocol((short) 0)).iterator())).
                    setRack(null), (short) 0),
                new ApiMessageAndVersion(new UnfenceBrokerRecord().
                    setId(1).setEpoch(brokerEpochs.get(1)), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new RegisterBrokerRecord().
                    setBrokerId(2).setBrokerEpoch(brokerEpochs.get(2)).
                    setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB2")).
                    setEndPoints(new BrokerEndpointCollection(Arrays.asList(
                        new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                            setPort(9094).setSecurityProtocol((short) 0)).iterator())).
                    setRack(null), (short) 0),
                new ApiMessageAndVersion(new UnfenceBrokerRecord().
                    setId(2).setEpoch(brokerEpochs.get(2)), (short) 0)),
            Arrays.asList(new ApiMessageAndVersion(new RegisterBrokerRecord().
                setBrokerId(3).setBrokerEpoch(brokerEpochs.get(3)).
                setIncarnationId(Uuid.fromString("kxAT73dKQsitIedpiPtwB3")).
                setEndPoints(new BrokerEndpointCollection(Arrays.asList(
                    new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").
                        setPort(9095).setSecurityProtocol((short) 0)).iterator())).
                setRack(null), (short) 0))),
            iterator);
    }

    /**
     * Test that certain controller operations time out if they stay on the controller
     * queue for too long.
     */
    @Test
    public void testTimeouts() throws Throwable {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv(1, Optional.empty())) {
            try (QuorumControllerTestEnv controlEnv =
                     new QuorumControllerTestEnv(logEnv, b -> b.setConfigDefs(CONFIGS))) {
                QuorumController controller = controlEnv.activeController();
                CountDownLatch countDownLatch = controller.pause();
                CompletableFuture<CreateTopicsResponseData> createFuture =
                    controller.createTopics(new CreateTopicsRequestData().setTimeoutMs(0).
                        setTopics(new CreatableTopicCollection(Collections.singleton(
                            new CreatableTopic().setName("foo")).iterator())));
                long now = controller.time().nanoseconds();
                CompletableFuture<Map<Uuid, ApiError>> deleteFuture =
                    controller.deleteTopics(now, Collections.singletonList(Uuid.ZERO_UUID));
                CompletableFuture<Map<String, ResultOrError<Uuid>>> findTopicIdsFuture =
                    controller.findTopicIds(now, Collections.singletonList("foo"));
                CompletableFuture<Map<Uuid, ResultOrError<String>>> findTopicNamesFuture =
                    controller.findTopicNames(now, Collections.singletonList(Uuid.ZERO_UUID));
                CompletableFuture<List<CreatePartitionsTopicResult>> createPartitionsFuture =
                    controller.createPartitions(now, Collections.singletonList(
                        new CreatePartitionsTopic()));
                CompletableFuture<ElectLeadersResponseData> electLeadersFuture =
                    controller.electLeaders(new ElectLeadersRequestData().setTimeoutMs(0).
                        setTopicPartitions(null));
                CompletableFuture<AlterPartitionReassignmentsResponseData> alterReassignmentsFuture =
                    controller.alterPartitionReassignments(
                        new AlterPartitionReassignmentsRequestData().setTimeoutMs(0).
                            setTopics(Collections.singletonList(new ReassignableTopic())));
                CompletableFuture<ListPartitionReassignmentsResponseData> listReassignmentsFuture =
                    controller.listPartitionReassignments(
                        new ListPartitionReassignmentsRequestData().setTimeoutMs(0));
                while (controller.time().nanoseconds() == now) {
                    Thread.sleep(0, 10);
                }
                countDownLatch.countDown();
                assertYieldsTimeout(createFuture);
                assertYieldsTimeout(deleteFuture);
                assertYieldsTimeout(findTopicIdsFuture);
                assertYieldsTimeout(findTopicNamesFuture);
                assertYieldsTimeout(createPartitionsFuture);
                assertYieldsTimeout(electLeadersFuture);
                assertYieldsTimeout(alterReassignmentsFuture);
                assertYieldsTimeout(listReassignmentsFuture);
            }
        }
    }

    private static void assertYieldsTimeout(Future<?> future) {
        assertEquals(TimeoutException.class, assertThrows(ExecutionException.class,
            () -> future.get()).getCause().getClass());
    }

    /**
     * Test that certain controller operations finish immediately without putting an event
     * on the controller queue, if there is nothing to do.
     */
    @Test
    public void testEarlyControllerResults() throws Throwable {
        try (LocalLogManagerTestEnv logEnv = new LocalLogManagerTestEnv(1, Optional.empty())) {
            try (QuorumControllerTestEnv controlEnv =
                     new QuorumControllerTestEnv(logEnv, b -> b.setConfigDefs(CONFIGS))) {
                QuorumController controller = controlEnv.activeController();
                CountDownLatch countDownLatch = controller.pause();
                CompletableFuture<CreateTopicsResponseData> createFuture =
                    controller.createTopics(new CreateTopicsRequestData().setTimeoutMs(120000));
                long deadlineMs = controller.time().nanoseconds() + HOURS.toNanos(1);
                CompletableFuture<Map<Uuid, ApiError>> deleteFuture =
                    controller.deleteTopics(deadlineMs, Collections.emptyList());
                CompletableFuture<Map<String, ResultOrError<Uuid>>> findTopicIdsFuture =
                    controller.findTopicIds(deadlineMs, Collections.emptyList());
                CompletableFuture<Map<Uuid, ResultOrError<String>>> findTopicNamesFuture =
                    controller.findTopicNames(deadlineMs, Collections.emptyList());
                CompletableFuture<List<CreatePartitionsTopicResult>> createPartitionsFuture =
                    controller.createPartitions(deadlineMs, Collections.emptyList());
                CompletableFuture<ElectLeadersResponseData> electLeadersFuture =
                    controller.electLeaders(new ElectLeadersRequestData().setTimeoutMs(120000));
                CompletableFuture<AlterPartitionReassignmentsResponseData> alterReassignmentsFuture =
                    controller.alterPartitionReassignments(
                        new AlterPartitionReassignmentsRequestData().setTimeoutMs(12000));
                createFuture.get();
                deleteFuture.get();
                findTopicIdsFuture.get();
                findTopicNamesFuture.get();
                createPartitionsFuture.get();
                electLeadersFuture.get();
                alterReassignmentsFuture.get();
                countDownLatch.countDown();
            }
        }
    }
}
