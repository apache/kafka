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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
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

    /**
     * Old classic group count metric. To be deprecated.
     */
    public final static com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS = getMetricName(
        "GroupMetadataManager", "NumGroups");
    public final static com.yammer.metrics.core.MetricName NUM_OFFSETS = getMetricName(
        "GroupMetadataManager", "NumOffsets");
    public final static com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_PREPARING_REBALANCE = getMetricName(
        "GroupMetadataManager", "NumGroupsPreparingRebalance");
    public final static com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_COMPLETING_REBALANCE = getMetricName(
        "GroupMetadataManager", "NumGroupsCompletingRebalance");
    public final static com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_STABLE = getMetricName(
        "GroupMetadataManager", "NumGroupsStable");
    public final static com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_DEAD = getMetricName(
        "GroupMetadataManager", "NumGroupsDead");
    public final static com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_EMPTY = getMetricName(
        "GroupMetadataManager", "NumGroupsEmpty");

    public final static String GROUP_COUNT_METRIC_NAME = "group-count";
    public final static String GROUP_COUNT_PROTOCOL_TAG = "protocol";
    public final static String CONSUMER_GROUP_COUNT_METRIC_NAME = "consumer-group-count";
    public final static String CONSUMER_GROUP_COUNT_STATE_TAG = "state";

    public static final String OFFSET_COMMITS_SENSOR_NAME = "OffsetCommits";
    public static final String OFFSET_EXPIRED_SENSOR_NAME = "OffsetExpired";
    public static final String OFFSET_DELETIONS_SENSOR_NAME = "OffsetDeletions";
    public static final String CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME = "CompletedRebalances";
    public static final String CONSUMER_GROUP_REBALANCES_SENSOR_NAME = "ConsumerGroupRebalances";

    private final MetricName classicGroupCountMetricName;
    private final MetricName consumerGroupCountMetricName;
    private final MetricName consumerGroupCountEmptyMetricName;
    private final MetricName consumerGroupCountAssigningMetricName;
    private final MetricName consumerGroupCountReconcilingMetricName;
    private final MetricName consumerGroupCountStableMetricName;
    private final MetricName consumerGroupCountDeadMetricName;

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

        classicGroupCountMetricName = metrics.metricName(
            GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The total number of groups using the classic rebalance protocol.",
            Collections.singletonMap(GROUP_COUNT_PROTOCOL_TAG, Group.GroupType.CLASSIC.toString())
        );

        consumerGroupCountMetricName = metrics.metricName(
            GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The total number of groups using the consumer rebalance protocol.",
            Collections.singletonMap(GROUP_COUNT_PROTOCOL_TAG, Group.GroupType.CONSUMER.toString())
        );

        consumerGroupCountEmptyMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in empty state.",
            Collections.singletonMap(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.EMPTY.toString())
        );

        consumerGroupCountAssigningMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in assigning state.",
            Collections.singletonMap(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.ASSIGNING.toString())
        );

        consumerGroupCountReconcilingMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in reconciling state.",
            Collections.singletonMap(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.RECONCILING.toString())
        );

        consumerGroupCountStableMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in stable state.",
            Collections.singletonMap(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.STABLE.toString())
        );

        consumerGroupCountDeadMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in dead state.",
            Collections.singletonMap(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.DEAD.toString())
        );

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

        Sensor classicGroupCompletedRebalancesSensor = metrics.sensor(CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME);
        classicGroupCompletedRebalancesSensor.add(new Meter(
            metrics.metricName("group-completed-rebalance-rate",
                METRICS_GROUP,
                "The rate of classic group completed rebalances"),
            metrics.metricName("group-completed-rebalance-count",
                METRICS_GROUP,
                "The total number of classic group completed rebalances")));

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
            Utils.mkEntry(CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME, classicGroupCompletedRebalancesSensor),
            Utils.mkEntry(CONSUMER_GROUP_REBALANCES_SENSOR_NAME, consumerGroupRebalanceSensor)
        ));
    }

    private Long numOffsets() {
        return shards.values().stream().mapToLong(GroupCoordinatorMetricsShard::numOffsets).sum();
    }

    private Long numClassicGroups() {
        return shards.values().stream().mapToLong(GroupCoordinatorMetricsShard::numClassicGroups).sum();
    }

    private Long numClassicGroups(ClassicGroupState state) {
        return shards.values().stream().mapToLong(shard -> shard.numClassicGroups(state)).sum();
    }

    private long numConsumerGroups() {
        return shards.values().stream().mapToLong(GroupCoordinatorMetricsShard::numConsumerGroups).sum();
    }

    private long numConsumerGroups(ConsumerGroupState state) {
        return shards.values().stream().mapToLong(shard -> shard.numConsumerGroups(state)).sum();
    }

    @Override
    public void close() {
        Arrays.asList(
            NUM_OFFSETS,
            NUM_CLASSIC_GROUPS,
            NUM_CLASSIC_GROUPS_PREPARING_REBALANCE,
            NUM_CLASSIC_GROUPS_COMPLETING_REBALANCE,
            NUM_CLASSIC_GROUPS_STABLE,
            NUM_CLASSIC_GROUPS_DEAD,
            NUM_CLASSIC_GROUPS_EMPTY
        ).forEach(registry::removeMetric);

        Arrays.asList(
            classicGroupCountMetricName,
            consumerGroupCountMetricName,
            consumerGroupCountEmptyMetricName,
            consumerGroupCountAssigningMetricName,
            consumerGroupCountReconcilingMetricName,
            consumerGroupCountStableMetricName,
            consumerGroupCountDeadMetricName
        ).forEach(metrics::removeMetric);

        Arrays.asList(
            OFFSET_COMMITS_SENSOR_NAME,
            OFFSET_EXPIRED_SENSOR_NAME,
            OFFSET_DELETIONS_SENSOR_NAME,
            CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME,
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

        registry.newGauge(NUM_CLASSIC_GROUPS, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numClassicGroups();
            }
        });

        registry.newGauge(NUM_CLASSIC_GROUPS_PREPARING_REBALANCE, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numClassicGroups(ClassicGroupState.PREPARING_REBALANCE);
            }
        });

        registry.newGauge(NUM_CLASSIC_GROUPS_COMPLETING_REBALANCE, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numClassicGroups(ClassicGroupState.COMPLETING_REBALANCE);
            }
        });

        registry.newGauge(NUM_CLASSIC_GROUPS_STABLE, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numClassicGroups(ClassicGroupState.STABLE);
            }
        });

        registry.newGauge(NUM_CLASSIC_GROUPS_DEAD, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numClassicGroups(ClassicGroupState.DEAD);
            }
        });

        registry.newGauge(NUM_CLASSIC_GROUPS_EMPTY, new com.yammer.metrics.core.Gauge<Long>() {
            @Override
            public Long value() {
                return numClassicGroups(ClassicGroupState.EMPTY);
            }
        });

        metrics.addMetric(
            classicGroupCountMetricName,
            (Gauge<Long>) (config, now) -> numClassicGroups()
        );

        metrics.addMetric(
            consumerGroupCountMetricName,
            (Gauge<Long>) (config, now) -> numConsumerGroups()
        );

        metrics.addMetric(
            consumerGroupCountEmptyMetricName,
            (Gauge<Long>) (config, now) -> numConsumerGroups(ConsumerGroupState.EMPTY)
        );

        metrics.addMetric(
            consumerGroupCountAssigningMetricName,
            (Gauge<Long>) (config, now) -> numConsumerGroups(ConsumerGroupState.ASSIGNING)
        );

        metrics.addMetric(
            consumerGroupCountReconcilingMetricName,
            (Gauge<Long>) (config, now) -> numConsumerGroups(ConsumerGroupState.RECONCILING)
        );

        metrics.addMetric(
            consumerGroupCountStableMetricName,
            (Gauge<Long>) (config, now) -> numConsumerGroups(ConsumerGroupState.STABLE)
        );

        metrics.addMetric(
            consumerGroupCountDeadMetricName,
            (Gauge<Long>) (config, now) -> numConsumerGroups(ConsumerGroupState.DEAD)
        );
    }
}
