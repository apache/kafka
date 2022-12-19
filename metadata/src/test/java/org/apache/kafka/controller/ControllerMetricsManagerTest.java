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
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class ControllerMetricsManagerTest {
    @Test
    public void testActiveBrokerRegistration() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        manager.replay(brokerRegistration(1, 1, false));
        assertEquals(1, metrics.activeBrokerCount());
        assertEquals(0, metrics.fencedBrokerCount());
    }

    @Test
    public void testFenceBrokerRegistration() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        manager.replay(brokerRegistration(1, 1, true));
        assertEquals(0, metrics.activeBrokerCount());
        assertEquals(1, metrics.fencedBrokerCount());
    }

    @Test
    public void testBrokerChangedToActive() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        manager.replay(brokerRegistration(1, 1, true));
        manager.replay(brokerChange(1, 1, BrokerRegistrationFencingChange.UNFENCE));
        assertEquals(1, metrics.activeBrokerCount());
        assertEquals(0, metrics.fencedBrokerCount());
    }

    @Test
    public void testBrokerLegacyChangedToActive() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        manager.replay(brokerRegistration(1, 1, true));
        manager.replay(brokerUnfence(1, 1));
        assertEquals(1, metrics.activeBrokerCount());
        assertEquals(0, metrics.fencedBrokerCount());
    }

    @Test
    public void testBrokerChangedToFence() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        manager.replay(brokerRegistration(1, 1, false));
        manager.replay(brokerChange(1, 1, BrokerRegistrationFencingChange.FENCE));
        assertEquals(0, metrics.activeBrokerCount());
        assertEquals(1, metrics.fencedBrokerCount());
    }


    @Test
    public void testBrokerLegacyChangedToFence() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        manager.replay(brokerRegistration(1, 1, false));
        manager.replay(brokerFence(1, 1));
        assertEquals(0, metrics.activeBrokerCount());
        assertEquals(1, metrics.fencedBrokerCount());
    }

    @Test
    public void testBrokerUnchanged() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        manager.replay(brokerRegistration(1, 1, true));
        manager.replay(brokerChange(1, 1, BrokerRegistrationFencingChange.NONE));
        assertEquals(0, metrics.activeBrokerCount());
        assertEquals(1, metrics.fencedBrokerCount());
    }

    @Test
    public void testBrokerUnregister() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        manager.replay(brokerRegistration(1, 1, true));
        manager.replay(brokerRegistration(2, 1, false));
        assertEquals(1, metrics.activeBrokerCount());
        assertEquals(1, metrics.fencedBrokerCount());
        manager.replay(brokerUnregistration(1, 1));
        assertEquals(1, metrics.activeBrokerCount());
        assertEquals(0, metrics.fencedBrokerCount());
        manager.replay(brokerUnregistration(2, 1));
        assertEquals(0, metrics.activeBrokerCount());
        assertEquals(0, metrics.fencedBrokerCount());
    }

    @Test
    public void testReplayBatch() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        manager.replayBatch(
            0,
            Arrays.asList(
                new ApiMessageAndVersion(brokerRegistration(1, 1, true), (short) 0),
                new ApiMessageAndVersion(brokerChange(1, 1, BrokerRegistrationFencingChange.UNFENCE), (short) 0)
            )
        );
        assertEquals(1, metrics.activeBrokerCount());
        assertEquals(0, metrics.fencedBrokerCount());
    }

    @Test
    public void testTopicCountIncreased() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        manager.replay(topicRecord("test"));
        assertEquals(1, metrics.globalTopicCount());
    }

    @Test
    public void testTopicCountDecreased() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);
        
        Uuid id = Uuid.randomUuid();
        manager.replay(topicRecord("test", id));
        manager.replay(removeTopicRecord(id));
        assertEquals(0, metrics.globalTopicCount());
    }

    @Test
    public void testPartitionCountIncreased() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        Uuid id = Uuid.randomUuid();
        manager.replay(topicRecord("test", id));
        assertEquals(0, metrics.globalPartitionCount());
        manager.replay(partitionRecord(id, 0, 0, Arrays.asList(0, 1, 2)));
        assertEquals(1, metrics.globalPartitionCount());
        manager.replay(partitionRecord(id, 1, 0, Arrays.asList(0, 1, 2)));
        assertEquals(2, metrics.globalPartitionCount());
    }

    @Test
    public void testPartitionCountDecreased() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        Uuid id = Uuid.randomUuid();
        manager.replay(topicRecord("test", id));
        manager.replay(partitionRecord(id, 0, 0, Arrays.asList(0, 1, 2)));
        manager.replay(partitionRecord(id, 1, 0, Arrays.asList(0, 1, 2)));
        manager.replay(removeTopicRecord(id));
        assertEquals(0, metrics.globalPartitionCount());
    }

    @Test
    public void testOfflinePartition() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        Uuid id = Uuid.randomUuid();
        manager.replay(topicRecord("test", id));
        manager.replay(partitionRecord(id, 0, NO_LEADER, Arrays.asList(0, 1, 2)));
        assertEquals(1, metrics.offlinePartitionCount());
    }

    @Test
    public void testImbalancedPartition() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        Uuid id = Uuid.randomUuid();
        manager.replay(topicRecord("test", id));
        manager.replay(partitionRecord(id, 0, 1, Arrays.asList(0, 1, 2)));
        assertEquals(1, metrics.preferredReplicaImbalanceCount());
    }

    @Test
    public void testPartitionChange() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        Uuid id = Uuid.randomUuid();
        manager.replay(topicRecord("test", id));
        manager.replay(partitionRecord(id, 0, 0, Arrays.asList(0, 1, 2)));

        manager.replay(partitionChangeRecord(id, 0, OptionalInt.of(NO_LEADER), Optional.empty()));
        assertEquals(1, metrics.offlinePartitionCount());

        manager.replay(partitionChangeRecord(id, 0, OptionalInt.of(1), Optional.empty()));
        assertEquals(0, metrics.offlinePartitionCount());
        assertEquals(1, metrics.preferredReplicaImbalanceCount());

        manager.replay(partitionChangeRecord(id, 0, OptionalInt.of(0), Optional.empty()));
        assertEquals(0, metrics.preferredReplicaImbalanceCount());

        manager.replay(partitionChangeRecord(id, 0, OptionalInt.empty(), Optional.of(Arrays.asList(1, 2, 0))));
        assertEquals(1, metrics.preferredReplicaImbalanceCount());

        manager.replay(partitionChangeRecord(id, 0, OptionalInt.of(2), Optional.of(Arrays.asList(2, 0, 1))));
        assertEquals(0, metrics.preferredReplicaImbalanceCount());
    }

    @Test
    public void testStartingMetrics() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        assertEquals(0, metrics.activeBrokerCount());
        assertEquals(0, metrics.fencedBrokerCount());
        assertEquals(0, metrics.globalTopicCount());
        assertEquals(0, metrics.globalPartitionCount());
        assertEquals(0, metrics.offlinePartitionCount());
        assertEquals(0, metrics.preferredReplicaImbalanceCount());
    }

    @Test
    public void testReset() {
        ControllerMetrics metrics = new MockControllerMetrics();
        ControllerMetricsManager manager = new ControllerMetricsManager(metrics);

        manager.replay(brokerRegistration(1, 1, true));

        Uuid id = Uuid.randomUuid();
        manager.replay(topicRecord("test", id));
        manager.replay(partitionRecord(id, 0, 0, Arrays.asList(0, 1, 2)));

        manager.reset();

        assertEquals(0, metrics.activeBrokerCount());
        assertEquals(0, metrics.fencedBrokerCount());
        assertEquals(0, metrics.globalTopicCount());
        assertEquals(0, metrics.globalPartitionCount());
        assertEquals(0, metrics.offlinePartitionCount());
        assertEquals(0, metrics.preferredReplicaImbalanceCount());
    }

    private static RegisterBrokerRecord brokerRegistration(
        int brokerId,
        long epoch,
        boolean fenced
    ) {
        return new RegisterBrokerRecord()
            .setBrokerId(brokerId)
            .setIncarnationId(Uuid.randomUuid())
            .setBrokerEpoch(epoch)
            .setFenced(fenced);
    }

    private static UnregisterBrokerRecord brokerUnregistration(
        int brokerId,
        long epoch
    ) {
        return new UnregisterBrokerRecord()
            .setBrokerId(brokerId)
            .setBrokerEpoch(epoch);
    }

    private static BrokerRegistrationChangeRecord brokerChange(
        int brokerId,
        long epoch,
        BrokerRegistrationFencingChange fencing
    ) {
        return new BrokerRegistrationChangeRecord()
            .setBrokerId(brokerId)
            .setBrokerEpoch(epoch)
            .setFenced(fencing.value());
    }

    private static UnfenceBrokerRecord brokerUnfence(int brokerId, long epoch) {
        return new UnfenceBrokerRecord()
            .setId(brokerId)
            .setEpoch(epoch);
    }

    private static FenceBrokerRecord brokerFence(int brokerId, long epoch) {
        return new FenceBrokerRecord()
            .setId(brokerId)
            .setEpoch(epoch);
    }

    private static TopicRecord topicRecord(String name) {
        return new TopicRecord().setName(name).setTopicId(Uuid.randomUuid());
    }

    private static TopicRecord topicRecord(String name, Uuid id) {
        return new TopicRecord().setName(name).setTopicId(id);
    }

    private static RemoveTopicRecord removeTopicRecord(Uuid id) {
        return new RemoveTopicRecord().setTopicId(id);
    }

    private static PartitionRecord partitionRecord(
        Uuid id,
        int partition,
        int leader,
        List<Integer> replicas
    ) {
        return new PartitionRecord()
            .setPartitionId(partition)
            .setTopicId(id)
            .setReplicas(replicas)
            .setIsr(replicas)
            .setLeader(leader);
    }

    private static PartitionChangeRecord partitionChangeRecord(
        Uuid id,
        int partition,
        OptionalInt leader,
        Optional<List<Integer>> replicas
    ) {
        PartitionChangeRecord record = new PartitionChangeRecord();
        leader.ifPresent(record::setLeader);
        replicas.ifPresent(record::setReplicas);
        replicas.ifPresent(record::setIsr);

        return record
            .setPartitionId(partition)
            .setTopicId(id);
    }
}
