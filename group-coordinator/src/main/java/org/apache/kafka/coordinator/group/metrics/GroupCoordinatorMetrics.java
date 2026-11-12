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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.yammer.metrics.core.MetricsRegistry;

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
     * @deprecated Since 4.4. Use kafka.server:type=group-coordinator-metrics,name=group-count,protocol=classic
     *             instead. This metric will be removed in Kafka 5.0.
     */
    @Deprecated(since = "4.4", forRemoval = true)
    public static final com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS = getMetricName(
        "GroupMetadataManager", "NumGroups");
    /**
     * @deprecated Since 4.4. Use kafka.server:type=group-coordinator-metrics,name=offset-count instead.
     *             This metric will be removed in Kafka 5.0.
     */
    @Deprecated(since = "4.4", forRemoval = true)
    public static final com.yammer.metrics.core.MetricName NUM_OFFSETS = getMetricName(
        "GroupMetadataManager", "NumOffsets");
    /**
     * @deprecated Since 4.4. Use
     *             kafka.server:type=group-coordinator-metrics,name=classic-group-count,state=PreparingRebalance
     *             instead. This metric will be removed in Kafka 5.0.
     */
    @Deprecated(since = "4.4", forRemoval = true)
    public static final com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_PREPARING_REBALANCE = getMetricName(
        "GroupMetadataManager", "NumGroupsPreparingRebalance");
    /**
     * @deprecated Since 4.4. Use
     *             kafka.server:type=group-coordinator-metrics,name=classic-group-count,state=CompletingRebalance
     *             instead. This metric will be removed in Kafka 5.0.
     */
    @Deprecated(since = "4.4", forRemoval = true)
    public static final com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_COMPLETING_REBALANCE = getMetricName(
        "GroupMetadataManager", "NumGroupsCompletingRebalance");
    /**
     * @deprecated Since 4.4. Use kafka.server:type=group-coordinator-metrics,name=classic-group-count,state=Stable
     *             instead. This metric will be removed in Kafka 5.0.
     */
    @Deprecated(since = "4.4", forRemoval = true)
    public static final com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_STABLE = getMetricName(
        "GroupMetadataManager", "NumGroupsStable");
    /**
     * @deprecated Since 4.4. Use kafka.server:type=group-coordinator-metrics,name=classic-group-count,state=Dead
     *             instead. This metric will be removed in Kafka 5.0.
     */
    @Deprecated(since = "4.4", forRemoval = true)
    public static final com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_DEAD = getMetricName(
        "GroupMetadataManager", "NumGroupsDead");
    /**
     * @deprecated Since 4.4. Use kafka.server:type=group-coordinator-metrics,name=classic-group-count,state=Empty
     *             instead. This metric will be removed in Kafka 5.0.
     */
    @Deprecated(since = "4.4", forRemoval = true)
    public static final com.yammer.metrics.core.MetricName NUM_CLASSIC_GROUPS_EMPTY = getMetricName(
        "GroupMetadataManager", "NumGroupsEmpty");

    public static final String GROUP_COUNT_METRIC_NAME = "group-count";
    public static final String GROUP_COUNT_PROTOCOL_TAG = "protocol";
    public static final String SHARE_GROUP_PROTOCOL_TAG = GROUP_COUNT_PROTOCOL_TAG;
    public static final String CONSUMER_GROUP_COUNT_METRIC_NAME = "consumer-group-count";
    public static final String SHARE_GROUP_COUNT_METRIC_NAME = "share-group-count";
    public static final String CONSUMER_GROUP_COUNT_STATE_TAG = "state";
    public static final String SHARE_GROUP_COUNT_STATE_TAG = CONSUMER_GROUP_COUNT_STATE_TAG;
    public static final String STREAMS_GROUP_COUNT_METRIC_NAME = "streams-group-count";
    public static final String STREAMS_GROUP_COUNT_STATE_TAG = "state";

    public static final String CLASSIC_GROUP_COUNT_METRIC_NAME = "classic-group-count";
    public static final String CLASSIC_GROUP_COUNT_STATE_TAG = "state";
    public static final String OFFSET_COUNT_METRIC_NAME = "offset-count";

    public static final String OFFSET_COMMITS_SENSOR_NAME = "OffsetCommits";
    public static final String OFFSET_EXPIRED_SENSOR_NAME = "OffsetExpired";
    public static final String OFFSET_DELETIONS_SENSOR_NAME = "OffsetDeletions";
    public static final String CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME = "CompletedRebalances";
    public static final String CONSUMER_GROUP_REBALANCES_SENSOR_NAME = "ConsumerGroupRebalances";
    public static final String SHARE_GROUP_REBALANCES_SENSOR_NAME = "ShareGroupRebalances";
    public static final String STREAMS_GROUP_REBALANCES_SENSOR_NAME = "StreamsGroupRebalances";
    public static final String STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_CYCLE_RUNS_SENSOR_NAME = "StreamsGroupTopologyDescriptionCleanupCycleRuns";
    public static final String STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_ELIGIBLE_GROUPS_SENSOR_NAME = "StreamsGroupTopologyDescriptionCleanupEligibleGroups";
    public static final String STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_SUCCESS_SENSOR_NAME = "StreamsGroupTopologyDescriptionDeleteSuccess";
    public static final String STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_ERROR_SENSOR_NAME = "StreamsGroupTopologyDescriptionDeleteError";
    public static final String STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_SUCCESS_SENSOR_NAME = "StreamsGroupTopologyDescriptionSetSuccess";
    public static final String STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_ERROR_SENSOR_NAME = "StreamsGroupTopologyDescriptionSetError";
    public static final String STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_SUCCESS_SENSOR_NAME = "StreamsGroupTopologyDescriptionGetSuccess";
    public static final String STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_ERROR_SENSOR_NAME = "StreamsGroupTopologyDescriptionGetError";

    private final MetricName offsetCountMetricName;
    private final MetricName classicGroupCountMetricName;
    private final MetricName classicGroupCountPreparingRebalanceMetricName;
    private final MetricName classicGroupCountCompletingRebalanceMetricName;
    private final MetricName classicGroupCountStableMetricName;
    private final MetricName classicGroupCountDeadMetricName;
    private final MetricName classicGroupCountEmptyMetricName;
    private final MetricName consumerGroupCountMetricName;
    private final MetricName consumerGroupCountEmptyMetricName;
    private final MetricName consumerGroupCountAssigningMetricName;
    private final MetricName consumerGroupCountReconcilingMetricName;
    private final MetricName consumerGroupCountStableMetricName;
    private final MetricName consumerGroupCountDeadMetricName;
    private final MetricName shareGroupCountMetricName;
    private final MetricName shareGroupCountEmptyMetricName;
    private final MetricName shareGroupCountStableMetricName;
    private final MetricName shareGroupCountDeadMetricName;
    private final MetricName streamsGroupCountMetricName;
    private final MetricName streamsGroupCountEmptyMetricName;
    private final MetricName streamsGroupCountAssigningMetricName;
    private final MetricName streamsGroupCountReconcilingMetricName;
    private final MetricName streamsGroupCountStableMetricName;
    private final MetricName streamsGroupCountDeadMetricName;
    private final MetricName streamsGroupCountNotReadyMetricName;

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

    @SuppressWarnings("MethodLength")
    public GroupCoordinatorMetrics(MetricsRegistry registry, Metrics metrics) {
        this.registry = Objects.requireNonNull(registry);
        this.metrics = Objects.requireNonNull(metrics);

        offsetCountMetricName = metrics.metricName(
            OFFSET_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of offsets currently retained for Classic, Consumer, and Streams Groups."
        );

        classicGroupCountMetricName = metrics.metricName(
            GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The total number of groups using the classic rebalance protocol.",
            Map.of(GROUP_COUNT_PROTOCOL_TAG, Group.GroupType.CLASSIC.toString())
        );

        classicGroupCountPreparingRebalanceMetricName = metrics.metricName(
            CLASSIC_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of classic groups in preparing rebalance state.",
            Map.of(CLASSIC_GROUP_COUNT_STATE_TAG, ClassicGroupState.PREPARING_REBALANCE.toString())
        );

        classicGroupCountCompletingRebalanceMetricName = metrics.metricName(
            CLASSIC_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of classic groups in completing rebalance state.",
            Map.of(CLASSIC_GROUP_COUNT_STATE_TAG, ClassicGroupState.COMPLETING_REBALANCE.toString())
        );

        classicGroupCountStableMetricName = metrics.metricName(
            CLASSIC_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of classic groups in stable state.",
            Map.of(CLASSIC_GROUP_COUNT_STATE_TAG, ClassicGroupState.STABLE.toString())
        );

        classicGroupCountDeadMetricName = metrics.metricName(
            CLASSIC_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of classic groups in dead state.",
            Map.of(CLASSIC_GROUP_COUNT_STATE_TAG, ClassicGroupState.DEAD.toString())
        );

        classicGroupCountEmptyMetricName = metrics.metricName(
            CLASSIC_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of classic groups in empty state.",
            Map.of(CLASSIC_GROUP_COUNT_STATE_TAG, ClassicGroupState.EMPTY.toString())
        );

        consumerGroupCountMetricName = metrics.metricName(
            GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The total number of groups using the consumer rebalance protocol.",
            Map.of(GROUP_COUNT_PROTOCOL_TAG, Group.GroupType.CONSUMER.toString())
        );

        consumerGroupCountEmptyMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in empty state.",
            Map.of(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.EMPTY.toString())
        );

        consumerGroupCountAssigningMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in assigning state.",
            Map.of(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.ASSIGNING.toString())
        );

        consumerGroupCountReconcilingMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in reconciling state.",
            Map.of(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.RECONCILING.toString())
        );

        consumerGroupCountStableMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in stable state.",
            Map.of(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.STABLE.toString())
        );

        consumerGroupCountDeadMetricName = metrics.metricName(
            CONSUMER_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of consumer groups in dead state.",
            Map.of(CONSUMER_GROUP_COUNT_STATE_TAG, ConsumerGroupState.DEAD.toString())
        );

        shareGroupCountMetricName = metrics.metricName(
            GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The total number of share groups.",
            Map.of(SHARE_GROUP_PROTOCOL_TAG, Group.GroupType.SHARE.toString())
        );

        shareGroupCountEmptyMetricName = metrics.metricName(
            SHARE_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of share groups in empty state.",
            Map.of(SHARE_GROUP_COUNT_STATE_TAG, ShareGroup.ShareGroupState.EMPTY.toString())
        );

        shareGroupCountStableMetricName = metrics.metricName(
            SHARE_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of share groups in stable state.",
            Map.of(SHARE_GROUP_COUNT_STATE_TAG, ShareGroup.ShareGroupState.STABLE.toString())
        );

        shareGroupCountDeadMetricName = metrics.metricName(
            SHARE_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of share groups in dead state.",
            Map.of(SHARE_GROUP_COUNT_STATE_TAG, ShareGroup.ShareGroupState.DEAD.toString())
        );

        streamsGroupCountMetricName = metrics.metricName(
            GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The total number of groups using the streams rebalance protocol.",
            Map.of(GROUP_COUNT_PROTOCOL_TAG, Group.GroupType.STREAMS.toString())
        );

        streamsGroupCountEmptyMetricName = metrics.metricName(
            STREAMS_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of streams groups in empty state.",
            Map.of(STREAMS_GROUP_COUNT_STATE_TAG, StreamsGroupState.EMPTY.toString())
        );

        streamsGroupCountAssigningMetricName = metrics.metricName(
            STREAMS_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of streams groups in assigning state.",
            Map.of(STREAMS_GROUP_COUNT_STATE_TAG, StreamsGroupState.ASSIGNING.toString())
        );

        streamsGroupCountReconcilingMetricName = metrics.metricName(
            STREAMS_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of streams groups in reconciling state.",
            Map.of(STREAMS_GROUP_COUNT_STATE_TAG, StreamsGroupState.RECONCILING.toString())
        );

        streamsGroupCountStableMetricName = metrics.metricName(
            STREAMS_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of streams groups in stable state.",
            Map.of(STREAMS_GROUP_COUNT_STATE_TAG, StreamsGroupState.STABLE.toString())
        );

        streamsGroupCountDeadMetricName = metrics.metricName(
            STREAMS_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of streams groups in dead state.",
            Map.of(STREAMS_GROUP_COUNT_STATE_TAG, StreamsGroupState.DEAD.toString())
        );

        streamsGroupCountNotReadyMetricName = metrics.metricName(
            STREAMS_GROUP_COUNT_METRIC_NAME,
            METRICS_GROUP,
            "The number of streams groups in not ready state.",
            Map.of(STREAMS_GROUP_COUNT_STATE_TAG, StreamsGroupState.NOT_READY.toString())
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

        Sensor shareGroupRebalanceSensor = metrics.sensor(SHARE_GROUP_REBALANCES_SENSOR_NAME);
        shareGroupRebalanceSensor.add(new Meter(
            metrics.metricName("share-group-rebalance-rate",
                METRICS_GROUP,
                "The rate of share group rebalances"),
            metrics.metricName("share-group-rebalance-count",
                METRICS_GROUP,
                "The total number of share group rebalances")));
        
        Sensor streamsGroupRebalanceSensor = metrics.sensor(STREAMS_GROUP_REBALANCES_SENSOR_NAME);
        streamsGroupRebalanceSensor.add(new Meter(
            metrics.metricName("streams-group-rebalance-rate",
                METRICS_GROUP,
                "The rate of streams group rebalances"),
            metrics.metricName("streams-group-rebalance-count",
                METRICS_GROUP,
                "The total number of streams group rebalances")));

        Sensor streamsGroupTopologyDescriptionCleanupCycleRunsSensor =
            metrics.sensor(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_CYCLE_RUNS_SENSOR_NAME);
        streamsGroupTopologyDescriptionCleanupCycleRunsSensor.add(new Meter(
            metrics.metricName("streams-group-topology-description-cleanup-cycle-rate",
                METRICS_GROUP,
                "The rate at which the topology-description cleanup cycle fires"),
            metrics.metricName("streams-group-topology-description-cleanup-cycle-count",
                METRICS_GROUP,
                "The total number of topology-description cleanup cycles that ran")));

        Sensor streamsGroupTopologyDescriptionCleanupEligibleGroupsSensor =
            metrics.sensor(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_ELIGIBLE_GROUPS_SENSOR_NAME);
        streamsGroupTopologyDescriptionCleanupEligibleGroupsSensor.add(new Meter(
            metrics.metricName("streams-group-topology-description-cleanup-eligible-rate",
                METRICS_GROUP,
                "The rate of streams groups identified as eligible for topology-description cleanup"),
            metrics.metricName("streams-group-topology-description-cleanup-eligible-count",
                METRICS_GROUP,
                "The total number of streams groups identified as eligible for topology-description cleanup")));

        Sensor streamsGroupTopologyDescriptionDeleteSuccessSensor =
            metrics.sensor(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_SUCCESS_SENSOR_NAME);
        streamsGroupTopologyDescriptionDeleteSuccessSensor.add(new Meter(
            metrics.metricName("streams-group-topology-description-delete-success-rate",
                METRICS_GROUP,
                "The rate of successful plugin.deleteTopology calls (DeleteGroups and periodic cleanup combined)"),
            metrics.metricName("streams-group-topology-description-delete-success-count",
                METRICS_GROUP,
                "The total number of successful plugin.deleteTopology calls (DeleteGroups and periodic cleanup combined)")));

        Sensor streamsGroupTopologyDescriptionDeleteErrorSensor =
            metrics.sensor(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_ERROR_SENSOR_NAME);
        streamsGroupTopologyDescriptionDeleteErrorSensor.add(new Meter(
            metrics.metricName("streams-group-topology-description-delete-error-rate",
                METRICS_GROUP,
                "The rate of failed plugin.deleteTopology calls (DeleteGroups and periodic cleanup combined)"),
            metrics.metricName("streams-group-topology-description-delete-error-count",
                METRICS_GROUP,
                "The total number of failed plugin.deleteTopology calls (DeleteGroups and periodic cleanup combined)")));

        Sensor streamsGroupTopologyDescriptionSetSuccessSensor =
            metrics.sensor(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_SUCCESS_SENSOR_NAME);
        streamsGroupTopologyDescriptionSetSuccessSensor.add(new Meter(
            metrics.metricName("streams-group-topology-description-set-success-rate",
                METRICS_GROUP,
                "The rate of successful plugin.setTopology calls"),
            metrics.metricName("streams-group-topology-description-set-success-count",
                METRICS_GROUP,
                "The total number of successful plugin.setTopology calls")));

        Sensor streamsGroupTopologyDescriptionSetErrorSensor =
            metrics.sensor(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_ERROR_SENSOR_NAME);
        streamsGroupTopologyDescriptionSetErrorSensor.add(new Meter(
            metrics.metricName("streams-group-topology-description-set-error-rate",
                METRICS_GROUP,
                "The rate of failed plugin.setTopology calls (any failure: permanent, transient, or otherwise)"),
            metrics.metricName("streams-group-topology-description-set-error-count",
                METRICS_GROUP,
                "The total number of failed plugin.setTopology calls (any failure: permanent, transient, or otherwise)")));

        Sensor streamsGroupTopologyDescriptionGetSuccessSensor =
            metrics.sensor(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_SUCCESS_SENSOR_NAME);
        streamsGroupTopologyDescriptionGetSuccessSensor.add(new Meter(
            metrics.metricName("streams-group-topology-description-get-success-rate",
                METRICS_GROUP,
                "The rate of successful getTopology calls (a call returning null counts as success)"),
            metrics.metricName("streams-group-topology-description-get-success-count",
                METRICS_GROUP,
                "The total number of successful plugin.getTopology calls (a call returning null counts as success)")));

        Sensor streamsGroupTopologyDescriptionGetErrorSensor =
            metrics.sensor(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_ERROR_SENSOR_NAME);
        streamsGroupTopologyDescriptionGetErrorSensor.add(new Meter(
            metrics.metricName("streams-group-topology-description-get-error-rate",
                METRICS_GROUP,
                "The rate of failed getTopology operations (plugin errors, SPI contract violations, or conversion failures)"),
            metrics.metricName("streams-group-topology-description-get-error-count",
                METRICS_GROUP,
                "The total number of failed plugin.getTopology calls")));

        globalSensors = Collections.unmodifiableMap(Utils.mkMap(
            Utils.mkEntry(OFFSET_COMMITS_SENSOR_NAME, offsetCommitsSensor),
            Utils.mkEntry(OFFSET_EXPIRED_SENSOR_NAME, offsetExpiredSensor),
            Utils.mkEntry(OFFSET_DELETIONS_SENSOR_NAME, offsetDeletionsSensor),
            Utils.mkEntry(CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME, classicGroupCompletedRebalancesSensor),
            Utils.mkEntry(CONSUMER_GROUP_REBALANCES_SENSOR_NAME, consumerGroupRebalanceSensor),
            Utils.mkEntry(SHARE_GROUP_REBALANCES_SENSOR_NAME, shareGroupRebalanceSensor),
            Utils.mkEntry(STREAMS_GROUP_REBALANCES_SENSOR_NAME, streamsGroupRebalanceSensor),
            Utils.mkEntry(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_CYCLE_RUNS_SENSOR_NAME,
                streamsGroupTopologyDescriptionCleanupCycleRunsSensor),
            Utils.mkEntry(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_ELIGIBLE_GROUPS_SENSOR_NAME,
                streamsGroupTopologyDescriptionCleanupEligibleGroupsSensor),
            Utils.mkEntry(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_SUCCESS_SENSOR_NAME,
                streamsGroupTopologyDescriptionDeleteSuccessSensor),
            Utils.mkEntry(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_ERROR_SENSOR_NAME,
                streamsGroupTopologyDescriptionDeleteErrorSensor),
            Utils.mkEntry(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_SUCCESS_SENSOR_NAME,
                streamsGroupTopologyDescriptionSetSuccessSensor),
            Utils.mkEntry(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_ERROR_SENSOR_NAME,
                streamsGroupTopologyDescriptionSetErrorSensor),
            Utils.mkEntry(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_SUCCESS_SENSOR_NAME,
                streamsGroupTopologyDescriptionGetSuccessSensor),
            Utils.mkEntry(STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_ERROR_SENSOR_NAME,
                streamsGroupTopologyDescriptionGetErrorSensor)
        ));
    }

    /**
     * Record a single observation against a global sensor by name. No-op if the sensor is not
     * configured (e.g. tests that build the metrics without the streams plugin scaffolding).
     */
    public void recordSensor(String name) {
        Sensor sensor = globalSensors.get(name);
        if (sensor != null) sensor.record();
    }

    /**
     * Record a numeric observation against a global sensor by name. No-op if the sensor is
     * not configured. Used by the topology-description cleanup cycle to report the eligible
     * group count per cycle.
     */
    public void recordSensor(String name, double value) {
        Sensor sensor = globalSensors.get(name);
        if (sensor != null) sensor.record(value);
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

    private long numStreamsGroups() {
        return shards.values().stream().mapToLong(GroupCoordinatorMetricsShard::numStreamsGroups).sum();
    }

    private long numStreamsGroups(StreamsGroupState state) {
        return shards.values().stream().mapToLong(shard -> shard.numStreamsGroups(state)).sum();
    }
    
    private long numShareGroups() {
        return shards.values().stream().mapToLong(GroupCoordinatorMetricsShard::numShareGroups).sum();
    }

    private long numShareGroups(ShareGroup.ShareGroupState state) {
        return shards.values().stream().mapToLong(shard -> shard.numShareGroups(state)).sum();
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
            offsetCountMetricName,
            classicGroupCountMetricName,
            classicGroupCountPreparingRebalanceMetricName,
            classicGroupCountCompletingRebalanceMetricName,
            classicGroupCountStableMetricName,
            classicGroupCountDeadMetricName,
            classicGroupCountEmptyMetricName,
            consumerGroupCountMetricName,
            consumerGroupCountEmptyMetricName,
            consumerGroupCountAssigningMetricName,
            consumerGroupCountReconcilingMetricName,
            consumerGroupCountStableMetricName,
            consumerGroupCountDeadMetricName,
            shareGroupCountMetricName,
            shareGroupCountEmptyMetricName,
            shareGroupCountStableMetricName,
            shareGroupCountDeadMetricName,
            streamsGroupCountMetricName,
            streamsGroupCountEmptyMetricName,
            streamsGroupCountAssigningMetricName,
            streamsGroupCountReconcilingMetricName,
            streamsGroupCountStableMetricName,
            streamsGroupCountDeadMetricName,
            streamsGroupCountNotReadyMetricName
        ).forEach(metrics::removeMetric);

        Arrays.asList(
            OFFSET_COMMITS_SENSOR_NAME,
            OFFSET_EXPIRED_SENSOR_NAME,
            OFFSET_DELETIONS_SENSOR_NAME,
            CLASSIC_GROUP_COMPLETED_REBALANCES_SENSOR_NAME,
            CONSUMER_GROUP_REBALANCES_SENSOR_NAME,
            SHARE_GROUP_REBALANCES_SENSOR_NAME,
            STREAMS_GROUP_REBALANCES_SENSOR_NAME,
            STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_CYCLE_RUNS_SENSOR_NAME,
            STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_ELIGIBLE_GROUPS_SENSOR_NAME,
            STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_SUCCESS_SENSOR_NAME,
            STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_ERROR_SENSOR_NAME,
            STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_SUCCESS_SENSOR_NAME,
            STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_ERROR_SENSOR_NAME,
            STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_SUCCESS_SENSOR_NAME,
            STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_ERROR_SENSOR_NAME
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
            offsetCountMetricName,
            (Gauge<Long>) (config, now) -> numOffsets()
        );

        metrics.addMetric(
            classicGroupCountMetricName,
            (Gauge<Long>) (config, now) -> numClassicGroups()
        );

        metrics.addMetric(
            classicGroupCountPreparingRebalanceMetricName,
            (Gauge<Long>) (config, now) -> numClassicGroups(ClassicGroupState.PREPARING_REBALANCE)
        );

        metrics.addMetric(
            classicGroupCountCompletingRebalanceMetricName,
            (Gauge<Long>) (config, now) -> numClassicGroups(ClassicGroupState.COMPLETING_REBALANCE)
        );

        metrics.addMetric(
            classicGroupCountStableMetricName,
            (Gauge<Long>) (config, now) -> numClassicGroups(ClassicGroupState.STABLE)
        );

        metrics.addMetric(
            classicGroupCountDeadMetricName,
            (Gauge<Long>) (config, now) -> numClassicGroups(ClassicGroupState.DEAD)
        );

        metrics.addMetric(
            classicGroupCountEmptyMetricName,
            (Gauge<Long>) (config, now) -> numClassicGroups(ClassicGroupState.EMPTY)
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

        metrics.addMetric(
            shareGroupCountMetricName,
            (Gauge<Long>) (config, now) -> numShareGroups()
        );

        metrics.addMetric(
            shareGroupCountEmptyMetricName,
            (Gauge<Long>) (config, now) -> numShareGroups(ShareGroup.ShareGroupState.EMPTY)
        );

        metrics.addMetric(
            shareGroupCountStableMetricName,
            (Gauge<Long>) (config, now) -> numShareGroups(ShareGroup.ShareGroupState.STABLE)
        );

        metrics.addMetric(
            shareGroupCountDeadMetricName,
            (Gauge<Long>) (config, now) -> numShareGroups(ShareGroup.ShareGroupState.DEAD)
        );

        metrics.addMetric(
            streamsGroupCountMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups()
        );

        metrics.addMetric(
            streamsGroupCountEmptyMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups(StreamsGroupState.EMPTY)
        );

        metrics.addMetric(
            streamsGroupCountAssigningMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups(StreamsGroupState.ASSIGNING)
        );

        metrics.addMetric(
            streamsGroupCountReconcilingMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups(StreamsGroupState.RECONCILING)
        );

        metrics.addMetric(
            streamsGroupCountStableMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups(StreamsGroupState.STABLE)
        );

        metrics.addMetric(
            streamsGroupCountDeadMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups(StreamsGroupState.DEAD)
        );

        metrics.addMetric(
            streamsGroupCountNotReadyMetricName,
            (Gauge<Long>) (config, now) -> numStreamsGroups(StreamsGroupState.NOT_READY)
        );
    }
}
