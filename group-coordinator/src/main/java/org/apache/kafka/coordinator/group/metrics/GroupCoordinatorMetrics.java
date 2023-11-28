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
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.generic.GenericGroupState;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * These are the metrics which are managed by the {@link org.apache.kafka.coordinator.group.GroupMetadataManager} class.
 * They generally pertain to aspects of group management, such as the number of groups in different states.
 */
public class GroupCoordinatorMetrics extends CoordinatorMetrics implements AutoCloseable {

    public static final String METRICS_GROUP = "group-coordinator-metrics";

    public final static com.yammer.metrics.core.MetricName NUM_OFFSETS = getMetricName(
        "GroupMetadataManager", "NumOffsets");
    public final static com.yammer.metrics.core.MetricName NUM_GENERIC_GROUPS_PREPARING_REBALANCE = getMetricName(
        "GroupMetadataManager", "NumGroupsPreparingRebalance");
    public final static com.yammer.metrics.core.MetricName NUM_GENERIC_GROUPS_COMPLETING_REBALANCE = getMetricName(
        "GroupMetadataManager", "NumGroupsCompletingRebalance");
    public final static com.yammer.metrics.core.MetricName NUM_GENERIC_GROUPS_STABLE = getMetricName(
        "GroupMetadataManager", "NumGroupsStable");
    public final static com.yammer.metrics.core.MetricName NUM_GENERIC_GROUPS_DEAD = getMetricName(
        "GroupMetadataManager", "NumGroupsDead");
    public final static com.yammer.metrics.core.MetricName NUM_GENERIC_GROUPS_EMPTY = getMetricName(
        "GroupMetadataManager", "NumGroupsEmpty");

    public final static String GROUPS_COUNT_METRIC_NAME = "groups-count";
    public final static String GROUPS_COUNT_TYPE_TAG = "type";
    public final static String CONSUMER_GROUPS_COUNT_METRIC_NAME = "consumer-groups-count";
    public final static String CONSUMER_GROUPS_COUNT_STATE_TAG = "state";

    public static final String OFFSET_COMMITS_SENSOR_NAME = "OffsetCommits";
    public static final String OFFSET_EXPIRED_SENSOR_NAME = "OffsetExpired";
    public static final String OFFSET_DELETIONS_SENSOR_NAME = "OffsetDeletions";
    public static final String GENERIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME = "CompletedRebalances";
    public static final String GENERIC_GROUP_REBALANCES_SENSOR_NAME = "GenericGroupRebalances";
    public static final String CONSUMER_GROUP_REBALANCES_SENSOR_NAME = "ConsumerGroupRebalances";

    private final MetricsRegistry registry;
    private final Metrics metrics;
    private final Map<TopicPartition, GroupCoordinatorMetricsShard> shards = new ConcurrentHashMap<>();

    /**
     * Global sensors. These are shared across all metrics shards.
     */
    public final Map<String, Sensor> globalSensors;

    public GroupCoordinatorMetrics() {
        this(KafkaYammerMetrics.defaultRegistry(), new Metrics());
    }

    public GroupCoordinatorMetrics(MetricsRegistry registry, Metrics metrics) {
        this.registry = Objects.requireNonNull(registry);
        this.metrics = Objects.requireNonNull(metrics);

        registerGauges();

        Sensor offsetCommitsSensor = metrics.sensor(OFFSET_COMMITS_SENSOR_NAME);
        offsetCommitsSensor.add(new Meter(
            metrics.metricName("offset-commit-rate",
                METRICS_GROUP,
                "The rate of committed offsets"),
            metrics.metricName("offset-commit-count",
                METRICS_GROUP,
                "The total number of committed offsets")));

        Sensor offsetExpiredSensor = metrics.sensor(OFFSET_EXPIRED_SENSOR_NAME);
        offsetExpiredSensor.add(new Meter(
            metrics.metricName("offset-expiration-rate",
                METRICS_GROUP,
                "The rate of expired offsets"),
            metrics.metricName("offset-expiration-count",
                METRICS_GROUP,
                "The total number of expired offsets")));

        Sensor offsetDeletionsSensor = metrics.sensor(OFFSET_DELETIONS_SENSOR_NAME);
        offsetDeletionsSensor.add(new Meter(
            metrics.metricName("offset-deletion-rate",
                METRICS_GROUP,
                "The rate of administrative deleted offsets"),
            metrics.metricName("offset-deletion-count",
                METRICS_GROUP,
                "The total number of administrative deleted offsets")));

        Sensor genericGroupCompletedRebalancesSensor = metrics.sensor(GENERIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME);
        genericGroupCompletedRebalancesSensor.add(new Meter(
            metrics.metricName("group-completed-rebalance-rate",
                METRICS_GROUP,
                "The rate of generic group completed rebalances"),
            metrics.metricName("group-completed-rebalance-count",
                METRICS_GROUP,
                "The total number of generic group completed rebalances")));

        Sensor genericGroupPreparingRebalancesSensor = metrics.sensor(GENERIC_GROUP_REBALANCES_SENSOR_NAME);
        genericGroupPreparingRebalancesSensor.add(new Meter(
            metrics.metricName("group-rebalance-rate",
                METRICS_GROUP,
                "The rate of generic group preparing rebalances"),
            metrics.metricName("group-rebalance-count",
                METRICS_GROUP,
                "The total number of generic group preparing rebalances")));

        Sensor consumerGroupRebalanceSensor = metrics.sensor(CONSUMER_GROUP_REBALANCES_SENSOR_NAME);
        consumerGroupRebalanceSensor.add(new Meter(
            metrics.metricName("consumer-group-rebalance-rate",
                METRICS_GROUP,
                "The rate of consumer group rebalances"),
            metrics.metricName("consumer-group-rebalance-count",
                METRICS_GROUP,
                "The total number of consumer group rebalances")));

        globalSensors = Collections.unmodifiableMap(Utils.mkMap(
            Utils.mkEntry(OFFSET_COMMITS_SENSOR_NAME, offsetCommitsSensor),
            Utils.mkEntry(OFFSET_EXPIRED_SENSOR_NAME, offsetExpiredSensor),
            Utils.mkEntry(OFFSET_DELETIONS_SENSOR_NAME, offsetDeletionsSensor),
            Utils.mkEntry(GENERIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME, genericGroupCompletedRebalancesSensor),
            Utils.mkEntry(GENERIC_GROUP_REBALANCES_SENSOR_NAME, genericGroupPreparingRebalancesSensor),
            Utils.mkEntry(CONSUMER_GROUP_REBALANCES_SENSOR_NAME, consumerGroupRebalanceSensor)
        ));
    }

    private Long numOffsets() {
        return shards.values().stream().mapToLong(GroupCoordinatorMetricsShard::numOffsets).sum();
    }

    private Long numGenericGroups() {
        return shards.values().stream().mapToLong(shard -> shard.numGenericGroups(null)).sum();
    }

    private Long numGenericGroupsPreparingRebalance() {
        return shards.values().stream().mapToLong(shard -> shard.numGenericGroups(GenericGroupState.PREPARING_REBALANCE)).sum();
    }

    private Long numGenericGroupsCompletingRebalance() {
        return shards.values().stream().mapToLong(shard -> shard.numGenericGroups(GenericGroupState.COMPLETING_REBALANCE)).sum();
    }
    private Long numGenericGroupsStable() {
        return shards.values().stream().mapToLong(shard -> shard.numGenericGroups(GenericGroupState.STABLE)).sum();
    }

    private Long numGenericGroupsDead() {
        return shards.values().stream().mapToLong(shard -> shard.numGenericGroups(GenericGroupState.DEAD)).sum();
    }

    private Long numGenericGroupsEmpty() {
        return shards.values().stream().mapToLong(shard -> shard.numGenericGroups(GenericGroupState.EMPTY)).sum();
    }

    private long numConsumerGroups() {
        return shards.values().stream().mapToLong(shard -> shard.numConsumerGroups(null)).sum();
    }

    private long numConsumerGroupsEmpty() {
        return shards.values().stream().mapToLong(shard -> shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY)).sum();
    }

    private long numConsumerGroupsAssigning() {
        return shards.values().stream().mapToLong(shard -> shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.ASSIGNING)).sum();
    }

    private long numConsumerGroupsReconciling() {
        return shards.values().stream().mapToLong(shard -> shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.RECONCILING)).sum();
    }

    private long numConsumerGroupsStable() {
        return shards.values().stream().mapToLong(shard -> shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.STABLE)).sum();
    }

    private long numConsumerGroupsDead() {
        return shards.values().stream().mapToLong(shard -> shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.DEAD)).sum();
    }

    @Override
    public void close() {
        Arrays.asList(
            NUM_OFFSETS,
            NUM_GENERIC_GROUPS_PREPARING_REBALANCE,
            NUM_GENERIC_GROUPS_COMPLETING_REBALANCE,
            NUM_GENERIC_GROUPS_STABLE,
            NUM_GENERIC_GROUPS_DEAD,
            NUM_GENERIC_GROUPS_EMPTY
        ).forEach(registry::removeMetric);

        metrics.removeMetric(metrics.metricName(CONSUMER_GROUPS_COUNT_METRIC_NAME, METRICS_GROUP));

        Arrays.asList(
            Group.GroupType.GENERIC,
            Group.GroupType.CONSUMER
        ).forEach(tag -> metrics.removeMetric(metrics.metricName(
            GROUPS_COUNT_METRIC_NAME,
            METRICS_GROUP,
            Collections.singletonMap(GROUPS_COUNT_TYPE_TAG, tag.toString())
        )));

        Arrays.asList(
            ConsumerGroup.ConsumerGroupState.EMPTY,
            ConsumerGroup.ConsumerGroupState.ASSIGNING,
            ConsumerGroup.ConsumerGroupState.RECONCILING,
            ConsumerGroup.ConsumerGroupState.STABLE,
            ConsumerGroup.ConsumerGroupState.DEAD
        ).forEach(tag -> metrics.removeMetric(metrics.metricName(
            CONSUMER_GROUPS_COUNT_METRIC_NAME,
            METRICS_GROUP,
            Collections.singletonMap(CONSUMER_GROUPS_COUNT_STATE_TAG, tag.toString())
        )));

        Arrays.asList(
            OFFSET_COMMITS_SENSOR_NAME,
            OFFSET_EXPIRED_SENSOR_NAME,
            OFFSET_DELETIONS_SENSOR_NAME,
            GENERIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME,
            GENERIC_GROUP_REBALANCES_SENSOR_NAME,
            CONSUMER_GROUP_REBALANCES_SENSOR_NAME
        ).forEach(metrics::removeSensor);
    }

    @Override
    public GroupCoordinatorMetricsShard newMetricsShard(SnapshotRegistry snapshotRegistry, TopicPartition tp) {
        return new GroupCoordinatorMetricsShard(snapshotRegistry, globalSensors, tp);
    }

    @Override
    public void activateMetricsShard(CoordinatorMetricsShard shard) {
        if (!(shard instanceof GroupCoordinatorMetricsShard)) {
            throw new IllegalArgumentException("GroupCoordinatorMetrics can only activate GroupCoordinatorMetricShard");
        }
        shards.put(shard.topicPartition(), (GroupCoordinatorMetricsShard) shard);
    }

    @Override
    public void deactivateMetricsShard(CoordinatorMetricsShard shard) {
        shards.remove(shard.topicPartition());
    }

    @Override
    public MetricsRegistry registry() {
        return this.registry;
    }

    @Override
    public void onUpdateLastCommittedOffset(TopicPartition tp, long offset) {
        CoordinatorMetricsShard shard = shards.get(tp);
        if (shard != null) {
            shard.commitUpTo(offset);
        }
    }

    public static com.yammer.metrics.core.MetricName getMetricName(String type, String name) {
        return getMetricName("kafka.coordinator.group", type, name);
    }

    private void registerGauges() {
        registry.newGauge(NUM_OFFSETS, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numOffsets();
            }
        });

        registry.newGauge(NUM_GENERIC_GROUPS_PREPARING_REBALANCE, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numGenericGroupsPreparingRebalance();
            }
        });

        registry.newGauge(NUM_GENERIC_GROUPS_COMPLETING_REBALANCE, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numGenericGroupsCompletingRebalance();
            }
        });

        registry.newGauge(NUM_GENERIC_GROUPS_STABLE, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numGenericGroupsStable();
            }
        });

        registry.newGauge(NUM_GENERIC_GROUPS_DEAD, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numGenericGroupsDead();
            }
        });

        registry.newGauge(NUM_GENERIC_GROUPS_EMPTY, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numGenericGroupsEmpty();
            }
        });
        metrics.addMetric(
            metrics.metricName(GROUPS_COUNT_METRIC_NAME, METRICS_GROUP, Collections.singletonMap(
                GROUPS_COUNT_TYPE_TAG, Group.GroupType.GENERIC.toString()
            )),
            (Gauge<Long>) (config, now) -> numGenericGroups()
        );

        metrics.addMetric(
            metrics.metricName(GROUPS_COUNT_METRIC_NAME, METRICS_GROUP, Collections.singletonMap(
                GROUPS_COUNT_TYPE_TAG, Group.GroupType.CONSUMER.toString()
            )),
            (Gauge<Long>) (config, now) -> numConsumerGroups()
        );

        metrics.addMetric(
            metrics.metricName(CONSUMER_GROUPS_COUNT_METRIC_NAME, METRICS_GROUP, Collections.singletonMap(
                CONSUMER_GROUPS_COUNT_STATE_TAG, ConsumerGroup.ConsumerGroupState.EMPTY.toString()
            )),
            (Gauge<Long>) (config, now) -> numConsumerGroupsEmpty()
        );

        metrics.addMetric(
            metrics.metricName(CONSUMER_GROUPS_COUNT_METRIC_NAME, METRICS_GROUP, Collections.singletonMap(
                CONSUMER_GROUPS_COUNT_STATE_TAG, ConsumerGroup.ConsumerGroupState.ASSIGNING.toString()
            )),
            (Gauge<Long>) (config, now) -> numConsumerGroupsAssigning()
        );

        metrics.addMetric(
            metrics.metricName(CONSUMER_GROUPS_COUNT_METRIC_NAME, METRICS_GROUP, Collections.singletonMap(
                CONSUMER_GROUPS_COUNT_STATE_TAG, ConsumerGroup.ConsumerGroupState.RECONCILING.toString()
            )),
            (Gauge<Long>) (config, now) -> numConsumerGroupsReconciling()
        );

        metrics.addMetric(
            metrics.metricName(CONSUMER_GROUPS_COUNT_METRIC_NAME, METRICS_GROUP, Collections.singletonMap(
                CONSUMER_GROUPS_COUNT_STATE_TAG, ConsumerGroup.ConsumerGroupState.STABLE.toString()
            )),
            (Gauge<Long>) (config, now) -> numConsumerGroupsStable()
        );

        metrics.addMetric(
            metrics.metricName(CONSUMER_GROUPS_COUNT_METRIC_NAME, METRICS_GROUP, Collections.singletonMap(
                CONSUMER_GROUPS_COUNT_STATE_TAG, ConsumerGroup.ConsumerGroupState.DEAD.toString()
            )),
            (Gauge<Long>) (config, now) -> numConsumerGroupsDead()
        );
    }
}
