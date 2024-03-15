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
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.stream.IntStream;

import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.DEAD;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;
import static org.apache.kafka.coordinator.group.metrics.MetricsTestUtils.assertGaugeValue;
import static org.apache.kafka.coordinator.group.metrics.MetricsTestUtils.metricName;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GroupCoordinatorMetricsShardTest {

    @Test
    public void testTimelineGaugeCounters() {
        MetricsRegistry registry = new MetricsRegistry();
        Metrics metrics = new Metrics();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        TopicPartition tp = new TopicPartition("__consumer_offsets", 0);
        GroupCoordinatorMetrics coordinatorMetrics = new GroupCoordinatorMetrics(registry, metrics);
        GroupCoordinatorMetricsShard shard = coordinatorMetrics.newMetricsShard(snapshotRegistry, tp);

        shard.incrementNumOffsets();
        shard.incrementNumConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY);
        shard.incrementNumConsumerGroups(ConsumerGroup.ConsumerGroupState.ASSIGNING);
        shard.incrementNumConsumerGroups(ConsumerGroup.ConsumerGroupState.RECONCILING);
        shard.incrementNumConsumerGroups(ConsumerGroup.ConsumerGroupState.STABLE);
        shard.incrementNumConsumerGroups(ConsumerGroup.ConsumerGroupState.DEAD);

        snapshotRegistry.getOrCreateSnapshot(1000);
        // The value should not be updated until the offset has been committed.
        assertEquals(0, shard.numOffsets());
        assertEquals(0, shard.numConsumerGroups());
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.ASSIGNING));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.RECONCILING));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.STABLE));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.DEAD));

        shard.commitUpTo(1000);
        assertEquals(1, shard.numOffsets());
        assertEquals(5, shard.numConsumerGroups());
        assertEquals(1, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY));
        assertEquals(1, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.ASSIGNING));
        assertEquals(1, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.RECONCILING));
        assertEquals(1, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.STABLE));
        assertEquals(1, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.DEAD));

        shard.decrementNumOffsets();
        shard.decrementNumConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY);
        shard.decrementNumConsumerGroups(ConsumerGroup.ConsumerGroupState.ASSIGNING);
        shard.decrementNumConsumerGroups(ConsumerGroup.ConsumerGroupState.RECONCILING);
        shard.decrementNumConsumerGroups(ConsumerGroup.ConsumerGroupState.STABLE);
        shard.decrementNumConsumerGroups(ConsumerGroup.ConsumerGroupState.DEAD);

        snapshotRegistry.getOrCreateSnapshot(2000);
        shard.commitUpTo(2000);
        assertEquals(0, shard.numOffsets());
        assertEquals(0, shard.numConsumerGroups());
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.ASSIGNING));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.RECONCILING));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.STABLE));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.DEAD));
    }

    @Test
    public void testGenericGroupStateTransitionMetrics() {
        MetricsRegistry registry = new MetricsRegistry();
        Metrics metrics = new Metrics();
        TopicPartition tp = new TopicPartition("__consumer_offsets", 0);
        GroupCoordinatorMetrics coordinatorMetrics = new GroupCoordinatorMetrics(registry, metrics);
        GroupCoordinatorMetricsShard shard = coordinatorMetrics.newMetricsShard(new SnapshotRegistry(new LogContext()), tp);
        coordinatorMetrics.activateMetricsShard(shard);

        LogContext logContext = new LogContext();
        ClassicGroup group0 = new ClassicGroup(logContext, "groupId0", EMPTY, Time.SYSTEM, shard);
        ClassicGroup group1 = new ClassicGroup(logContext, "groupId1", EMPTY, Time.SYSTEM, shard);
        ClassicGroup group2 = new ClassicGroup(logContext, "groupId2", EMPTY, Time.SYSTEM, shard);
        ClassicGroup group3 = new ClassicGroup(logContext, "groupId3", EMPTY, Time.SYSTEM, shard);

        IntStream.range(0, 4).forEach(__ -> shard.incrementNumClassicGroups(EMPTY));

        assertEquals(4, shard.numClassicGroups());

        group0.transitionTo(PREPARING_REBALANCE);
        group0.transitionTo(COMPLETING_REBALANCE);
        group1.transitionTo(PREPARING_REBALANCE);
        group2.transitionTo(DEAD);

        assertEquals(1, shard.numClassicGroups(ClassicGroupState.EMPTY));
        assertEquals(1, shard.numClassicGroups(ClassicGroupState.PREPARING_REBALANCE));
        assertEquals(1, shard.numClassicGroups(ClassicGroupState.COMPLETING_REBALANCE));
        assertEquals(1, shard.numClassicGroups(ClassicGroupState.DEAD));
        assertEquals(0, shard.numClassicGroups(ClassicGroupState.STABLE));

        group0.transitionTo(STABLE);
        group1.transitionTo(COMPLETING_REBALANCE);
        group3.transitionTo(DEAD);

        assertEquals(0, shard.numClassicGroups(ClassicGroupState.EMPTY));
        assertEquals(0, shard.numClassicGroups(ClassicGroupState.PREPARING_REBALANCE));
        assertEquals(1, shard.numClassicGroups(ClassicGroupState.COMPLETING_REBALANCE));
        assertEquals(2, shard.numClassicGroups(ClassicGroupState.DEAD));
        assertEquals(1, shard.numClassicGroups(ClassicGroupState.STABLE));

        assertGaugeValue(
            metrics,
            metrics.metricName("group-count", "group-coordinator-metrics", Collections.singletonMap("protocol", "classic")),
            4
        );
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

        IntStream.range(0, 4).forEach(__ -> shard.incrementNumConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY));

        snapshotRegistry.getOrCreateSnapshot(1000);
        shard.commitUpTo(1000);
        assertEquals(4, shard.numConsumerGroups());
        assertEquals(4, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY));

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
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY));
        assertEquals(4, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.STABLE));

        group2.setGroupEpoch(1);
        group3.setGroupEpoch(1);

        snapshotRegistry.getOrCreateSnapshot(3000);
        shard.commitUpTo(3000);
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY));
        assertEquals(2, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.ASSIGNING));
        assertEquals(2, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.STABLE));

        group2.setTargetAssignmentEpoch(1);

        // Set member2 to ASSIGNING state.
        new ConsumerGroupMember.Builder(member2)
            .setPartitionsPendingRevocation(Collections.singletonMap(Uuid.ZERO_UUID, Collections.singleton(0)))
            .build();

        snapshotRegistry.getOrCreateSnapshot(4000);
        shard.commitUpTo(4000);
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY));
        assertEquals(1, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.ASSIGNING));
        assertEquals(1, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.RECONCILING));
        assertEquals(2, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.STABLE));

        assertGaugeValue(metrics, metrics.metricName("group-count", "group-coordinator-metrics",
            Collections.singletonMap("protocol", "consumer")), 4);
        assertGaugeValue(metrics, metrics.metricName("consumer-group-count", "group-coordinator-metrics",
            Collections.singletonMap("state", ConsumerGroup.ConsumerGroupState.EMPTY.toString())), 0);
        assertGaugeValue(metrics, metrics.metricName("consumer-group-count", "group-coordinator-metrics",
            Collections.singletonMap("state", ConsumerGroup.ConsumerGroupState.ASSIGNING.toString())), 1);
        assertGaugeValue(metrics, metrics.metricName("consumer-group-count", "group-coordinator-metrics",
            Collections.singletonMap("state", ConsumerGroup.ConsumerGroupState.RECONCILING.toString())), 1);
        assertGaugeValue(metrics, metrics.metricName("consumer-group-count", "group-coordinator-metrics",
            Collections.singletonMap("state", ConsumerGroup.ConsumerGroupState.STABLE.toString())), 2);
        assertGaugeValue(metrics, metrics.metricName("consumer-group-count", "group-coordinator-metrics",
            Collections.singletonMap("state", ConsumerGroup.ConsumerGroupState.DEAD.toString())), 0);
    }
}
