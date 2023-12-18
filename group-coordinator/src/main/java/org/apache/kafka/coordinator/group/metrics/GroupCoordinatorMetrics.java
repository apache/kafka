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

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * These are the metrics which are managed by the {@link org.apache.kafka.coordinator.group.GroupMetadataManager} class.
 * They generally pertain to aspects of group management, such as the number of groups in different states.
 */
public class GroupCoordinatorMetrics extends CoordinatorMetrics implements AutoCloseable {
    public static final String METRICS_GROUP = "group-coordinator-metrics";

    public final static MetricName NUM_OFFSETS = getMetricName(
        "GroupMetadataManager", "NumOffsets");
    public final static MetricName NUM_GENERIC_GROUPS = getMetricName(
        "GroupMetadataManager", "NumGroups");
    public final static MetricName NUM_GENERIC_GROUPS_PREPARING_REBALANCE = getMetricName(
        "GroupMetadataManager", "NumGroupsPreparingRebalance");
    public final static MetricName NUM_GENERIC_GROUPS_COMPLETING_REBALANCE = getMetricName(
        "GroupMetadataManager", "NumGroupsCompletingRebalance");
    public final static MetricName NUM_GENERIC_GROUPS_STABLE = getMetricName(
        "GroupMetadataManager", "NumGroupsStable");
    public final static MetricName NUM_GENERIC_GROUPS_DEAD = getMetricName(
        "GroupMetadataManager", "NumGroupsDead");
    public final static MetricName NUM_GENERIC_GROUPS_EMPTY = getMetricName(
        "GroupMetadataManager", "NumGroupsEmpty");
    public final static MetricName NUM_CONSUMER_GROUPS = getMetricName(
        "GroupMetadataManager", "NumConsumerGroups");
    public final static MetricName NUM_CONSUMER_GROUPS_EMPTY = getMetricName(
        "GroupMetadataManager", "NumConsumerGroupsEmpty");
    public final static MetricName NUM_CONSUMER_GROUPS_ASSIGNING = getMetricName(
        "GroupMetadataManager", "NumConsumerGroupsAssigning");
    public final static MetricName NUM_CONSUMER_GROUPS_RECONCILING = getMetricName(
        "GroupMetadataManager", "NumConsumerGroupsReconciling");
    public final static MetricName NUM_CONSUMER_GROUPS_STABLE = getMetricName(
        "GroupMetadataManager", "NumConsumerGroupsStable");
    public final static MetricName NUM_CONSUMER_GROUPS_DEAD = getMetricName(
        "GroupMetadataManager", "NumConsumerGroupsDead");

    public static final String OFFSET_COMMITS_SENSOR_NAME = "OffsetCommits";
    public static final String OFFSET_EXPIRED_SENSOR_NAME = "OffsetExpired";
    public static final String OFFSET_DELETIONS_SENSOR_NAME = "OffsetDeletions";
    public static final String GENERIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME = "CompletedRebalances";
    public static final String GENERIC_GROUP_REBALANCES_SENSOR_NAME = "GenericGroupRebalances";
    public static final String CONSUMER_GROUP_REBALANCES_SENSOR_NAME = "ConsumerGroupRebalances";

    private final MetricsRegistry registry;
    private final Metrics metrics;
    private final Map<TopicPartition, CoordinatorMetricsShard> shards = new HashMap<>();
    private static final AtomicLong NUM_GENERIC_GROUPS_PREPARING_REBALANCE_COUNTER = new AtomicLong(0);
    private static final AtomicLong NUM_GENERIC_GROUPS_COMPLETING_REBALANCE_COUNTER = new AtomicLong(0);
    private static final AtomicLong NUM_GENERIC_GROUPS_STABLE_COUNTER = new AtomicLong(0);
    private static final AtomicLong NUM_GENERIC_GROUPS_DEAD_COUNTER = new AtomicLong(0);
    private static final AtomicLong NUM_GENERIC_GROUPS_EMPTY_COUNTER = new AtomicLong(0);

    /**
     * Global sensors. These are shared across all metrics shards.
     */
    public final Map<String, Sensor> globalSensors;

    /**
     * Global gauge counters. These are shared across all metrics shards.
     */
    public static final Map<String, AtomicLong> GLOBAL_GAUGES = Collections.unmodifiableMap(Utils.mkMap(
        Utils.mkEntry(NUM_GENERIC_GROUPS_PREPARING_REBALANCE.getName(), NUM_GENERIC_GROUPS_PREPARING_REBALANCE_COUNTER),
        Utils.mkEntry(NUM_GENERIC_GROUPS_COMPLETING_REBALANCE.getName(), NUM_GENERIC_GROUPS_COMPLETING_REBALANCE_COUNTER),
        Utils.mkEntry(NUM_GENERIC_GROUPS_STABLE.getName(), NUM_GENERIC_GROUPS_STABLE_COUNTER),
        Utils.mkEntry(NUM_GENERIC_GROUPS_DEAD.getName(), NUM_GENERIC_GROUPS_DEAD_COUNTER),
        Utils.mkEntry(NUM_GENERIC_GROUPS_EMPTY.getName(), NUM_GENERIC_GROUPS_EMPTY_COUNTER)
    ));

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

    public Long numOffsets() {
        return shards.values().stream().mapToLong(shard -> shard.localGaugeValue(NUM_OFFSETS)).sum();
    }

    public Long numGenericGroups() {
        return shards.values().stream().mapToLong(shard -> shard.localGaugeValue(NUM_GENERIC_GROUPS)).sum();
    }

    public Long numGenericGroupsPreparingRebalanceCount() {
        return NUM_GENERIC_GROUPS_PREPARING_REBALANCE_COUNTER.get();
    }

    public Long numGenericGroupsCompletingRebalanceCount() {
        return NUM_GENERIC_GROUPS_COMPLETING_REBALANCE_COUNTER.get();
    }
    public Long numGenericGroupsStableCount() {
        return NUM_GENERIC_GROUPS_STABLE_COUNTER.get();
    }

    public Long numGenericGroupsDeadCount() {
        return NUM_GENERIC_GROUPS_DEAD_COUNTER.get();
    }

    public Long numGenericGroupsEmptyCount() {
        return NUM_GENERIC_GROUPS_EMPTY_COUNTER.get();
    }

    public long numConsumerGroups() {
        return shards.values().stream().mapToLong(shard -> shard.localGaugeValue(NUM_CONSUMER_GROUPS)).sum();
    }

    public long numConsumerGroupsEmpty() {
        return shards.values().stream().mapToLong(shard -> shard.localGaugeValue(NUM_CONSUMER_GROUPS_EMPTY)).sum();
    }

    public long numConsumerGroupsAssigning() {
        return shards.values().stream().mapToLong(shard -> shard.localGaugeValue(NUM_CONSUMER_GROUPS_ASSIGNING)).sum();
    }

    public long numConsumerGroupsReconciling() {
        return shards.values().stream().mapToLong(shard -> shard.localGaugeValue(NUM_CONSUMER_GROUPS_RECONCILING)).sum();
    }

    public long numConsumerGroupsStable() {
        return shards.values().stream().mapToLong(shard -> shard.localGaugeValue(NUM_CONSUMER_GROUPS_STABLE)).sum();
    }

    public long numConsumerGroupsDead() {
        return shards.values().stream().mapToLong(shard -> shard.localGaugeValue(NUM_CONSUMER_GROUPS_DEAD)).sum();
    }

    @Override
    public void close() {
        Arrays.asList(
            NUM_OFFSETS,
            NUM_GENERIC_GROUPS,
            NUM_GENERIC_GROUPS_PREPARING_REBALANCE,
            NUM_GENERIC_GROUPS_COMPLETING_REBALANCE,
            NUM_GENERIC_GROUPS_STABLE,
            NUM_GENERIC_GROUPS_DEAD,
            NUM_GENERIC_GROUPS_EMPTY,
            NUM_CONSUMER_GROUPS,
            NUM_CONSUMER_GROUPS_EMPTY,
            NUM_CONSUMER_GROUPS_ASSIGNING,
            NUM_CONSUMER_GROUPS_RECONCILING,
            NUM_CONSUMER_GROUPS_STABLE,
            NUM_CONSUMER_GROUPS_DEAD
        ).forEach(registry::removeMetric);

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
        return new GroupCoordinatorMetricsShard(snapshotRegistry, globalSensors, GLOBAL_GAUGES, tp);
    }

    @Override
    public void activateMetricsShard(CoordinatorMetricsShard shard) {
        shards.put(shard.topicPartition(), shard);
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

    public static MetricName getMetricName(String type, String name) {
        return getMetricName("kafka.coordinator.group", type, name);
    }

    private void registerGauges() {
        registry.newGauge(NUM_OFFSETS, new Gauge<Long>() {
            @Override
            public Long value() {
                return numOffsets();
            }
        });

        registry.newGauge(NUM_GENERIC_GROUPS, new Gauge<Long>() {
            @Override
            public Long value() {
                return numGenericGroups();
            }
        });

        registry.newGauge(NUM_GENERIC_GROUPS_PREPARING_REBALANCE, new Gauge<Long>() {
            @Override
            public Long value() {
                return numGenericGroupsPreparingRebalanceCount();
            }
        });

        registry.newGauge(NUM_GENERIC_GROUPS_COMPLETING_REBALANCE, new Gauge<Long>() {
            @Override
            public Long value() {
                return numGenericGroupsCompletingRebalanceCount();
            }
        });

        registry.newGauge(NUM_GENERIC_GROUPS_STABLE, new Gauge<Long>() {
            @Override
            public Long value() {
                return numGenericGroupsStableCount();
            }
        });

        registry.newGauge(NUM_GENERIC_GROUPS_DEAD, new Gauge<Long>() {
            @Override
            public Long value() {
                return numGenericGroupsDeadCount();
            }
        });

        registry.newGauge(NUM_GENERIC_GROUPS_EMPTY, new Gauge<Long>() {
            @Override
            public Long value() {
                return numGenericGroupsEmptyCount();
            }
        });

        registry.newGauge(NUM_CONSUMER_GROUPS, new Gauge<Long>() {
            @Override
            public Long value() {
                return numConsumerGroups();
            }
        });

        registry.newGauge(NUM_CONSUMER_GROUPS_EMPTY, new Gauge<Long>() {
            @Override
            public Long value() {
                return numConsumerGroupsEmpty();
            }
        });

        registry.newGauge(NUM_CONSUMER_GROUPS_ASSIGNING, new Gauge<Long>() {
            @Override
            public Long value() {
                return numConsumerGroupsAssigning();
            }
        });

        registry.newGauge(NUM_CONSUMER_GROUPS_RECONCILING, new Gauge<Long>() {
            @Override
            public Long value() {
                return numConsumerGroupsReconciling();
            }
        });

        registry.newGauge(NUM_CONSUMER_GROUPS_STABLE, new Gauge<Long>() {
            @Override
            public Long value() {
                return numConsumerGroupsStable();
            }
        });

        registry.newGauge(NUM_CONSUMER_GROUPS_DEAD, new Gauge<Long>() {
            @Override
            public Long value() {
                return numConsumerGroupsDead();
            }
        });
    }
}
