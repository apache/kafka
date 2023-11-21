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

import com.yammer.metrics.core.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.generic.GenericGroupState;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineLong;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

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

/**
 * This class is mapped to a single {@link org.apache.kafka.coordinator.group.GroupCoordinatorShard}. It will
 * record all metrics that the shard handles with respect to {@link org.apache.kafka.coordinator.group.OffsetMetadataManager}
 * and {@link org.apache.kafka.coordinator.group.GroupMetadataManager} operations.
 *
 * Local gauges will be recorded in this class which will be gathered by {@link GroupCoordinatorMetrics} to
 * report.
 */
public class GroupCoordinatorMetricsShard implements CoordinatorMetricsShard {

    /**
     * This class represents a gauge counter for this shard. The TimelineLong object represents a gauge backed by
     * the snapshot registry. Once we commit to a certain offset in the snapshot registry, we write the given
     * TimelineLong's value to the AtomicLong. This AtomicLong represents the actual gauge counter that is queried
     * when reporting the value to {@link GroupCoordinatorMetrics}.
     */
    private static class TimelineGaugeCounter {

        final TimelineLong timelineLong;

        final AtomicLong atomicLong;

        public TimelineGaugeCounter(TimelineLong timelineLong, AtomicLong atomicLong) {
            this.timelineLong = timelineLong;
            this.atomicLong = atomicLong;
        }
    }

    /**
     * Local timeline gauge counters keyed by the metric name.
     */
    private final Map<String, TimelineGaugeCounter> localGauges;

    /**
     * All sensors keyed by the sensor name. A Sensor object is shared across all metrics shards.
     */
    private final Map<String, Sensor> globalSensors;

    /**
     * Global gauge counters keyed by the metric name. The same counter is shared across all metrics shards.
     */
    private final Map<String, AtomicLong> globalGauges;

    /**
     * The topic partition.
     */
    private final TopicPartition topicPartition;

    public GroupCoordinatorMetricsShard(
        SnapshotRegistry snapshotRegistry,
        Map<String, Sensor> globalSensors,
        Map<String, AtomicLong> globalGauges,
        TopicPartition topicPartition
    ) {
        Objects.requireNonNull(snapshotRegistry);
        TimelineLong numOffsetsTimeline = new TimelineLong(snapshotRegistry);
        TimelineLong numGenericGroupsTimeline = new TimelineLong(snapshotRegistry);
        TimelineLong numConsumerGroupsTimeline = new TimelineLong(snapshotRegistry);
        TimelineLong numConsumerGroupsEmptyTimeline = new TimelineLong(snapshotRegistry);
        TimelineLong numConsumerGroupsAssigningTimeline = new TimelineLong(snapshotRegistry);
        TimelineLong numConsumerGroupsReconcilingTimeline = new TimelineLong(snapshotRegistry);
        TimelineLong numConsumerGroupsStableTimeline = new TimelineLong(snapshotRegistry);
        TimelineLong numConsumerGroupsDeadTimeline = new TimelineLong(snapshotRegistry);

        this.localGauges = Collections.unmodifiableMap(Utils.mkMap(
            Utils.mkEntry(NUM_OFFSETS.getName(),
                new TimelineGaugeCounter(numOffsetsTimeline, new AtomicLong(0))),
            Utils.mkEntry(NUM_GENERIC_GROUPS.getName(),
                new TimelineGaugeCounter(numGenericGroupsTimeline, new AtomicLong(0))),
            Utils.mkEntry(NUM_CONSUMER_GROUPS.getName(),
                new TimelineGaugeCounter(numConsumerGroupsTimeline, new AtomicLong(0))),
            Utils.mkEntry(NUM_CONSUMER_GROUPS_EMPTY.getName(),
                new TimelineGaugeCounter(numConsumerGroupsEmptyTimeline, new AtomicLong(0))),
            Utils.mkEntry(NUM_CONSUMER_GROUPS_ASSIGNING.getName(),
                new TimelineGaugeCounter(numConsumerGroupsAssigningTimeline, new AtomicLong(0))),
            Utils.mkEntry(NUM_CONSUMER_GROUPS_RECONCILING.getName(),
                new TimelineGaugeCounter(numConsumerGroupsReconcilingTimeline, new AtomicLong(0))),
            Utils.mkEntry(NUM_CONSUMER_GROUPS_STABLE.getName(),
                new TimelineGaugeCounter(numConsumerGroupsStableTimeline, new AtomicLong(0))),
            Utils.mkEntry(NUM_CONSUMER_GROUPS_DEAD.getName(),
                new TimelineGaugeCounter(numConsumerGroupsDeadTimeline, new AtomicLong(0)))
        ));

        this.globalSensors = Objects.requireNonNull(globalSensors);
        this.globalGauges = Objects.requireNonNull(globalGauges);
        this.topicPartition = Objects.requireNonNull(topicPartition);
    }

    @Override
    public void incrementGlobalGauge(MetricName metricName) {
        AtomicLong gaugeCounter = globalGauges.get(metricName.getName());
        if (gaugeCounter != null) {
            gaugeCounter.incrementAndGet();
        }
    }

    @Override
    public void incrementLocalGauge(MetricName metricName) {
        TimelineGaugeCounter gaugeCounter = localGauges.get(metricName.getName());
        if (gaugeCounter != null) {
            synchronized (gaugeCounter.timelineLong) {
                gaugeCounter.timelineLong.increment();
            }
        }
    }

    @Override
    public void decrementGlobalGauge(MetricName metricName) {
        AtomicLong gaugeCounter = globalGauges.get(metricName.getName());
        if (gaugeCounter != null) {
            gaugeCounter.decrementAndGet();
        }
    }

    @Override
    public void decrementLocalGauge(MetricName metricName) {
        TimelineGaugeCounter gaugeCounter = localGauges.get(metricName.getName());
        if (gaugeCounter != null) {
            synchronized (gaugeCounter.timelineLong) {
                gaugeCounter.timelineLong.decrement();
            }
        }
    }

    @Override
    public long globalGaugeValue(MetricName metricName) {
        AtomicLong gaugeCounter = globalGauges.get(metricName.getName());
        if (gaugeCounter != null) {
            return gaugeCounter.get();
        }
        return 0;
    }

    @Override
    public long localGaugeValue(MetricName metricName) {
        TimelineGaugeCounter gaugeCounter = localGauges.get(metricName.getName());
        if (gaugeCounter != null) {
            return gaugeCounter.atomicLong.get();
        }
        return 0;
    }

    @Override
    public void record(String sensorName) {
        Sensor sensor = globalSensors.get(sensorName);
        if (sensor != null) {
            sensor.record();
        }
    }

    @Override
    public void record(String sensorName, double val) {
        Sensor sensor = globalSensors.get(sensorName);
        if (sensor != null) {
            sensor.record(val);
        }
    }

    @Override
    public TopicPartition topicPartition() {
        return this.topicPartition;
    }

    @Override
    public void commitUpTo(long offset) {
        this.localGauges.forEach((__, gaugeCounter) -> {
            long value;
            synchronized (gaugeCounter.timelineLong) {
                value = gaugeCounter.timelineLong.get(offset);
            }
            gaugeCounter.atomicLong.set(value);
        });
    }

    /**
     * Called when a generic group's state has changed. Increment/decrement
     * the counter accordingly.
     *
     * @param oldState The previous state. null value means that it's a new group.
     * @param newState The next state. null value means that the group has been removed.
     */
    public void onGenericGroupStateTransition(GenericGroupState oldState, GenericGroupState newState) {
        if (newState != null) {
            switch (newState) {
                case PREPARING_REBALANCE:
                    incrementGlobalGauge(NUM_GENERIC_GROUPS_PREPARING_REBALANCE);
                    break;
                case COMPLETING_REBALANCE:
                    incrementGlobalGauge(NUM_GENERIC_GROUPS_COMPLETING_REBALANCE);
                    break;
                case STABLE:
                    incrementGlobalGauge(NUM_GENERIC_GROUPS_STABLE);
                    break;
                case DEAD:
                    incrementGlobalGauge(NUM_GENERIC_GROUPS_DEAD);
                    break;
                case EMPTY:
                    incrementGlobalGauge(NUM_GENERIC_GROUPS_EMPTY);
            }
        } else {
            decrementLocalGauge(NUM_GENERIC_GROUPS);
        }

        if (oldState != null) {
            switch (oldState) {
                case PREPARING_REBALANCE:
                    decrementGlobalGauge(NUM_GENERIC_GROUPS_PREPARING_REBALANCE);
                    break;
                case COMPLETING_REBALANCE:
                    decrementGlobalGauge(NUM_GENERIC_GROUPS_COMPLETING_REBALANCE);
                    break;
                case STABLE:
                    decrementGlobalGauge(NUM_GENERIC_GROUPS_STABLE);
                    break;
                case DEAD:
                    decrementGlobalGauge(NUM_GENERIC_GROUPS_DEAD);
                    break;
                case EMPTY:
                    decrementGlobalGauge(NUM_GENERIC_GROUPS_EMPTY);
            }
        } else {
            incrementLocalGauge(NUM_GENERIC_GROUPS);
        }
    }

    /**
     * Called when a consumer group's state has changed. Increment/decrement
     * the counter accordingly.
     *
     * @param oldState The previous state. null value means that it's a new group.
     * @param newState The next state. null value means that the group has been removed.
     */
    public void onConsumerGroupStateTransition(
        ConsumerGroup.ConsumerGroupState oldState,
        ConsumerGroup.ConsumerGroupState newState
    ) {
        if (newState != null) {
            switch (newState) {
                case EMPTY:
                    incrementLocalGauge(NUM_CONSUMER_GROUPS_EMPTY);
                    break;
                case ASSIGNING:
                    incrementLocalGauge(NUM_CONSUMER_GROUPS_ASSIGNING);
                    break;
                case RECONCILING:
                    incrementLocalGauge(NUM_CONSUMER_GROUPS_RECONCILING);
                    break;
                case STABLE:
                    incrementLocalGauge(NUM_CONSUMER_GROUPS_STABLE);
                    break;
                case DEAD:
                    incrementLocalGauge(NUM_CONSUMER_GROUPS_DEAD);
            }
        } else {
            decrementLocalGauge(NUM_CONSUMER_GROUPS);
        }

        if (oldState != null) {
            switch (oldState) {
                case EMPTY:
                    decrementLocalGauge(NUM_CONSUMER_GROUPS_EMPTY);
                    break;
                case ASSIGNING:
                    decrementLocalGauge(NUM_CONSUMER_GROUPS_ASSIGNING);
                    break;
                case RECONCILING:
                    decrementLocalGauge(NUM_CONSUMER_GROUPS_RECONCILING);
                    break;
                case STABLE:
                    decrementLocalGauge(NUM_CONSUMER_GROUPS_STABLE);
                    break;
                case DEAD:
                    decrementLocalGauge(NUM_CONSUMER_GROUPS_DEAD);
            }
        } else {
            incrementLocalGauge(NUM_CONSUMER_GROUPS);
        }
    }
}
