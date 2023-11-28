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
package org.apache.kafka.coordinator.group.metrics;

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.generic.GenericGroup;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.kafka.coordinator.group.generic.GenericGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.DEAD;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.STABLE;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_CONSUMER_GROUPS;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_CONSUMER_GROUPS_ASSIGNING;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_CONSUMER_GROUPS_DEAD;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_CONSUMER_GROUPS_EMPTY;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_CONSUMER_GROUPS_RECONCILING;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_CONSUMER_GROUPS_STABLE;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_GENERIC_GROUPS;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_GENERIC_GROUPS_COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_GENERIC_GROUPS_DEAD;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_GENERIC_GROUPS_EMPTY;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_GENERIC_GROUPS_PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_GENERIC_GROUPS_STABLE;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.NUM_OFFSETS;
import static org.apache.kafka.coordinator.group.metrics.MetricsTestUtils.assertGaugeValue;
import static org.apache.kafka.coordinator.group.metrics.MetricsTestUtils.metricName;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GroupCoordinatorMetricsShardTest {

    @Test
    public void testLocalGauges() {
        MetricsRegistry registry = new MetricsRegistry();
        Metrics metrics = new Metrics();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        TopicPartition tp = new TopicPartition("__consumer_offsets", 0);
        GroupCoordinatorMetrics coordinatorMetrics = new GroupCoordinatorMetrics(registry, metrics);
        GroupCoordinatorMetricsShard shard = coordinatorMetrics.newMetricsShard(snapshotRegistry, tp);

        shard.incrementLocalGauge(NUM_OFFSETS);
        shard.incrementLocalGauge(NUM_GENERIC_GROUPS);
        shard.incrementLocalGauge(NUM_CONSUMER_GROUPS);
        shard.incrementLocalGauge(NUM_CONSUMER_GROUPS_EMPTY);
        shard.incrementLocalGauge(NUM_CONSUMER_GROUPS_ASSIGNING);
        shard.incrementLocalGauge(NUM_CONSUMER_GROUPS_RECONCILING);
        shard.incrementLocalGauge(NUM_CONSUMER_GROUPS_STABLE);
        shard.incrementLocalGauge(NUM_CONSUMER_GROUPS_DEAD);

        snapshotRegistry.getOrCreateSnapshot(1000);
        // The value should not be updated until the offset has been committed.
        assertEquals(0, shard.localGaugeValue(NUM_OFFSETS));
        assertEquals(0, shard.localGaugeValue(NUM_GENERIC_GROUPS));
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS));
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_EMPTY));
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_ASSIGNING));
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_RECONCILING));
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_STABLE));
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_DEAD));

        shard.commitUpTo(1000);
        assertEquals(1, shard.localGaugeValue(NUM_OFFSETS));
        assertEquals(1, shard.localGaugeValue(NUM_GENERIC_GROUPS));
        assertEquals(1, shard.localGaugeValue(NUM_CONSUMER_GROUPS));
        assertEquals(1, shard.localGaugeValue(NUM_CONSUMER_GROUPS_EMPTY));
        assertEquals(1, shard.localGaugeValue(NUM_CONSUMER_GROUPS_ASSIGNING));
        assertEquals(1, shard.localGaugeValue(NUM_CONSUMER_GROUPS_RECONCILING));
        assertEquals(1, shard.localGaugeValue(NUM_CONSUMER_GROUPS_STABLE));
        assertEquals(1, shard.localGaugeValue(NUM_CONSUMER_GROUPS_DEAD));

        shard.decrementLocalGauge(NUM_OFFSETS);
        shard.decrementLocalGauge(NUM_GENERIC_GROUPS);
        shard.decrementLocalGauge(NUM_CONSUMER_GROUPS);
        shard.decrementLocalGauge(NUM_CONSUMER_GROUPS_EMPTY);
        shard.decrementLocalGauge(NUM_CONSUMER_GROUPS_ASSIGNING);
        shard.decrementLocalGauge(NUM_CONSUMER_GROUPS_RECONCILING);
        shard.decrementLocalGauge(NUM_CONSUMER_GROUPS_STABLE);
        shard.decrementLocalGauge(NUM_CONSUMER_GROUPS_DEAD);

        snapshotRegistry.getOrCreateSnapshot(2000);
        shard.commitUpTo(2000);
        assertEquals(0, shard.localGaugeValue(NUM_OFFSETS));
        assertEquals(0, shard.localGaugeValue(NUM_GENERIC_GROUPS));
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS));
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_EMPTY));
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_ASSIGNING));
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_RECONCILING));
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_STABLE));
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_DEAD));
    }

    @Test
    public void testGenericGroupStateTransitionMetrics() {
        MetricsRegistry registry = new MetricsRegistry();
        Metrics metrics = new Metrics();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        TopicPartition tp = new TopicPartition("__consumer_offsets", 0);
        GroupCoordinatorMetrics coordinatorMetrics = new GroupCoordinatorMetrics(registry, metrics);
        GroupCoordinatorMetricsShard shard = coordinatorMetrics.newMetricsShard(snapshotRegistry, tp);
        coordinatorMetrics.activateMetricsShard(shard);

        LogContext logContext = new LogContext();
        GenericGroup group0 = new GenericGroup(logContext, "groupId0", EMPTY, Time.SYSTEM, shard);
        GenericGroup group1 = new GenericGroup(logContext, "groupId1", EMPTY, Time.SYSTEM, shard);
        GenericGroup group2 = new GenericGroup(logContext, "groupId2", EMPTY, Time.SYSTEM, shard);
        GenericGroup group3 = new GenericGroup(logContext, "groupId3", EMPTY, Time.SYSTEM, shard);

        snapshotRegistry.getOrCreateSnapshot(1000);
        shard.commitUpTo(1000);
        assertEquals(4, shard.localGaugeValue(NUM_GENERIC_GROUPS));

        group0.transitionTo(PREPARING_REBALANCE);
        group0.transitionTo(COMPLETING_REBALANCE);
        group1.transitionTo(PREPARING_REBALANCE);
        group2.transitionTo(DEAD);

        snapshotRegistry.getOrCreateSnapshot(2000);
        shard.commitUpTo(2000);
        assertEquals(1, shard.globalGaugeValue(NUM_GENERIC_GROUPS_EMPTY));
        assertEquals(1, shard.globalGaugeValue(NUM_GENERIC_GROUPS_PREPARING_REBALANCE));
        assertEquals(1, shard.globalGaugeValue(NUM_GENERIC_GROUPS_COMPLETING_REBALANCE));
        assertEquals(1, shard.globalGaugeValue(NUM_GENERIC_GROUPS_DEAD));
        assertEquals(0, shard.globalGaugeValue(NUM_GENERIC_GROUPS_STABLE));

        group0.transitionTo(STABLE);
        group1.transitionTo(COMPLETING_REBALANCE);
        group3.transitionTo(DEAD);

        snapshotRegistry.getOrCreateSnapshot(3000);
        shard.commitUpTo(3000);
        assertEquals(0, shard.globalGaugeValue(NUM_GENERIC_GROUPS_EMPTY));
        assertEquals(0, shard.globalGaugeValue(NUM_GENERIC_GROUPS_PREPARING_REBALANCE));
        assertEquals(1, shard.globalGaugeValue(NUM_GENERIC_GROUPS_COMPLETING_REBALANCE));
        assertEquals(2, shard.globalGaugeValue(NUM_GENERIC_GROUPS_DEAD));
        assertEquals(1, shard.globalGaugeValue(NUM_GENERIC_GROUPS_STABLE));

        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumGroups"), 4);
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumGroupsEmpty"), 0);
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumGroupsPreparingRebalance"), 0);
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumGroupsCompletingRebalance"), 1);
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumGroupsDead"), 2);
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumGroupsStable"), 1);
    }

    @Test
    public void testConsumerGroupStateTransitionMetrics() {
        MetricsRegistry registry = new MetricsRegistry();
        Metrics metrics = new Metrics();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        TopicPartition tp = new TopicPartition("__consumer_offsets", 0);
        GroupCoordinatorMetrics coordinatorMetrics = new GroupCoordinatorMetrics(registry, metrics);
        GroupCoordinatorMetricsShard shard = coordinatorMetrics.newMetricsShard(snapshotRegistry, tp);
        coordinatorMetrics.activateMetricsShard(shard);

        ConsumerGroup group0 = new ConsumerGroup(
            snapshotRegistry,
            "group-0",
            shard
        );
        ConsumerGroup group1 = new ConsumerGroup(
            snapshotRegistry,
            "group-1",
            shard
        );
        ConsumerGroup group2 = new ConsumerGroup(
            snapshotRegistry,
            "group-2",
            shard
        );
        ConsumerGroup group3 = new ConsumerGroup(
            snapshotRegistry,
            "group-3",
            shard
        );

        snapshotRegistry.getOrCreateSnapshot(1000);
        shard.commitUpTo(1000);
        assertEquals(4, shard.localGaugeValue(NUM_CONSUMER_GROUPS));
        assertEquals(4, shard.localGaugeValue(NUM_CONSUMER_GROUPS_EMPTY));

        ConsumerGroupMember member0 = group0.getOrMaybeCreateMember("member-id", true);
        ConsumerGroupMember member1 = group1.getOrMaybeCreateMember("member-id", true);
        ConsumerGroupMember member2 = group2.getOrMaybeCreateMember("member-id", true);
        ConsumerGroupMember member3 = group3.getOrMaybeCreateMember("member-id", true);
        group0.updateMember(member0);
        group1.updateMember(member1);
        group2.updateMember(member2);
        group3.updateMember(member3);

        snapshotRegistry.getOrCreateSnapshot(2000);
        shard.commitUpTo(2000);
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_EMPTY));
        assertEquals(4, shard.localGaugeValue(NUM_CONSUMER_GROUPS_STABLE));

        group2.setGroupEpoch(1);
        group3.setGroupEpoch(1);

        snapshotRegistry.getOrCreateSnapshot(3000);
        shard.commitUpTo(3000);
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_EMPTY));
        assertEquals(2, shard.localGaugeValue(NUM_CONSUMER_GROUPS_ASSIGNING));
        assertEquals(2, shard.localGaugeValue(NUM_CONSUMER_GROUPS_STABLE));

        group2.setTargetAssignmentEpoch(1);
        // Set member2 to ASSIGNING state.
        new ConsumerGroupMember.Builder(member2)
            .setPartitionsPendingAssignment(Collections.singletonMap(Uuid.ZERO_UUID, Collections.singleton(0)))
            .build();

        snapshotRegistry.getOrCreateSnapshot(4000);
        shard.commitUpTo(4000);
        assertEquals(0, shard.localGaugeValue(NUM_CONSUMER_GROUPS_EMPTY));
        assertEquals(1, shard.localGaugeValue(NUM_CONSUMER_GROUPS_ASSIGNING));
        assertEquals(1, shard.localGaugeValue(NUM_CONSUMER_GROUPS_RECONCILING));
        assertEquals(2, shard.localGaugeValue(NUM_CONSUMER_GROUPS_STABLE));

        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumConsumerGroups"), 4);
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumConsumerGroupsEmpty"), 0);
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumConsumerGroupsAssigning"), 1);
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumConsumerGroupsReconciling"), 1);
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumConsumerGroupsStable"), 2);
        assertGaugeValue(registry, metricName("GroupMetadataManager", "NumConsumerGroupsDead"), 0);
    }
}
