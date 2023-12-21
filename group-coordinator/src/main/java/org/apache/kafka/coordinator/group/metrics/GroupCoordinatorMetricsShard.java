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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup.ConsumerGroupState;
import org.apache.kafka.coordinator.group.generic.GenericGroupState;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineLong;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

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
     * Consumer group size gauge counters keyed by the metric name.
     */
    private final Map<GenericGroupState, AtomicLong> genericGroupGauges;

    /**
     * Consumer group size gauge counters keyed by the metric name.
     */
    private final Map<ConsumerGroupState, TimelineGaugeCounter> consumerGroupGauges;

    /**
     * All sensors keyed by the sensor name. A Sensor object is shared across all metrics shards.
     */
    private final Map<String, Sensor> globalSensors;

    /**
     * The number of offsets gauge counter.
     */
    private final TimelineGaugeCounter numOffsetsTimelineGaugeCounter;

    /**
     * The number of generic groups metric counter.
     */
    private final TimelineGaugeCounter numGenericGroupsTimelineCounter;

    /**
     * The topic partition.
     */
    private final TopicPartition topicPartition;

    public GroupCoordinatorMetricsShard(
        SnapshotRegistry snapshotRegistry,
        Map<String, Sensor> globalSensors,
        TopicPartition topicPartition
    ) {
        Objects.requireNonNull(snapshotRegistry);
        numOffsetsTimelineGaugeCounter = new TimelineGaugeCounter(new TimelineLong(snapshotRegistry), new AtomicLong(0));
        numGenericGroupsTimelineCounter = new TimelineGaugeCounter(new TimelineLong(snapshotRegistry), new AtomicLong(0));

        this.genericGroupGauges = Utils.mkMap(
            Utils.mkEntry(GenericGroupState.PREPARING_REBALANCE, new AtomicLong(0)),
            Utils.mkEntry(GenericGroupState.COMPLETING_REBALANCE, new AtomicLong(0)),
            Utils.mkEntry(GenericGroupState.STABLE, new AtomicLong(0)),
            Utils.mkEntry(GenericGroupState.DEAD, new AtomicLong(0)),
            Utils.mkEntry(GenericGroupState.EMPTY, new AtomicLong(0))
        );

        this.consumerGroupGauges = Utils.mkMap(
            Utils.mkEntry(ConsumerGroupState.EMPTY,
                new TimelineGaugeCounter(new TimelineLong(snapshotRegistry), new AtomicLong(0))),
            Utils.mkEntry(ConsumerGroupState.ASSIGNING,
                new TimelineGaugeCounter(new TimelineLong(snapshotRegistry), new AtomicLong(0))),
            Utils.mkEntry(ConsumerGroupState.RECONCILING,
                new TimelineGaugeCounter(new TimelineLong(snapshotRegistry), new AtomicLong(0))),
            Utils.mkEntry(ConsumerGroupState.STABLE,
                new TimelineGaugeCounter(new TimelineLong(snapshotRegistry), new AtomicLong(0))),
            Utils.mkEntry(ConsumerGroupState.DEAD,
                new TimelineGaugeCounter(new TimelineLong(snapshotRegistry), new AtomicLong(0)))
        );

        this.globalSensors = Objects.requireNonNull(globalSensors);
        this.topicPartition = Objects.requireNonNull(topicPartition);
    }

    public void incrementNumGenericGroups(GenericGroupState state) {
        AtomicLong counter = genericGroupGauges.get(state);
        if (counter != null) {
            counter.incrementAndGet();
        }
    }

    /**
     * Increment the number of offsets.
     */
    public void incrementNumOffsets() {
        synchronized (numOffsetsTimelineGaugeCounter.timelineLong) {
            numOffsetsTimelineGaugeCounter.timelineLong.increment();
        }
    }

    /**
     * Increment the number of consumer groups.
     *
     * @param state the consumer group state.
     */
    public void incrementNumConsumerGroups(ConsumerGroupState state) {
        TimelineGaugeCounter gaugeCounter = consumerGroupGauges.get(state);
        if (gaugeCounter != null) {
            synchronized (gaugeCounter.timelineLong) {
                gaugeCounter.timelineLong.increment();
            }
        }
    }

    /**
     * Decrement the number of offsets.
     */
    public void decrementNumOffsets() {
        synchronized (numOffsetsTimelineGaugeCounter.timelineLong) {
            numOffsetsTimelineGaugeCounter.timelineLong.decrement();
        }
    }

    /**
     * Decrement the number of consumer groups.
     *
     * @param state the consumer group state.
     */
    public void decrementNumGenericGroups(GenericGroupState state) {
        AtomicLong counter = genericGroupGauges.get(state);
        if (counter != null) {
            counter.decrementAndGet();
        }
    }

    /**
     * Decrement the number of consumer groups.
     *
     * @param state the consumer group state.
     */
    public void decrementNumConsumerGroups(ConsumerGroupState state) {
        TimelineGaugeCounter gaugeCounter = consumerGroupGauges.get(state);
        if (gaugeCounter != null) {
            synchronized (gaugeCounter.timelineLong) {
                gaugeCounter.timelineLong.decrement();
            }
        }
    }

    /**
     * @return The number of offsets.
     */
    public long numOffsets() {
        return numOffsetsTimelineGaugeCounter.atomicLong.get();
    }

    /**
     * Obtain the number of generic groups in the specified state.
     *
     * @param state  The generic group state.
     *
     * @return   The number of generic groups in `state`.
     */
    public long numGenericGroups(GenericGroupState state) {
        AtomicLong counter = genericGroupGauges.get(state);
        if (counter != null) {
            return counter.get();
        }
        return 0L;
    }

    /**
     * @return The total number of generic groups.
     */
    public long numGenericGroups() {
        return genericGroupGauges.values().stream()
            .mapToLong(AtomicLong::get).sum();
    }

    /**
     * Obtain the number of consumer groups in the specified state.
     *
     * @param state  the consumer group state.
     *
     * @return   The number of consumer groups in `state`.
     */
    public long numConsumerGroups(ConsumerGroupState state) {
        TimelineGaugeCounter gaugeCounter = consumerGroupGauges.get(state);
        if (gaugeCounter != null) {
            return gaugeCounter.atomicLong.get();
        }
        return 0L;
    }

    /**
     * @return The total number of generic groups.
     */
    public long numConsumerGroups() {
        return consumerGroupGauges.values().stream()
            .mapToLong(timelineGaugeCounter -> timelineGaugeCounter.atomicLong.get()).sum();
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
        this.consumerGroupGauges.forEach((__, gaugeCounter) -> {
            long value;
            synchronized (gaugeCounter.timelineLong) {
                value = gaugeCounter.timelineLong.get(offset);
            }
            gaugeCounter.atomicLong.set(value);
        });

        synchronized (numGenericGroupsTimelineCounter.timelineLong) {
            long value = numGenericGroupsTimelineCounter.timelineLong.get(offset);
            numGenericGroupsTimelineCounter.atomicLong.set(value);
        }

        synchronized (numOffsetsTimelineGaugeCounter.timelineLong) {
            long value = numOffsetsTimelineGaugeCounter.timelineLong.get(offset);
            numOffsetsTimelineGaugeCounter.atomicLong.set(value);
        }
    }

    /**
     * Called when a generic group's state has changed. Increment/decrement
     * the counter accordingly.
     *
     * @param oldState The previous state. null value means that it's a new group.
     * @param newState The next state. null value means that the group has been removed.
     */
    public void onGenericGroupStateTransition(
        GenericGroupState oldState,
        GenericGroupState newState
    ) {
        if (newState != null) {
            switch (newState) {
                case PREPARING_REBALANCE:
                    incrementNumGenericGroups(GenericGroupState.PREPARING_REBALANCE);
                    break;
                case COMPLETING_REBALANCE:
                    incrementNumGenericGroups(GenericGroupState.COMPLETING_REBALANCE);
                    break;
                case STABLE:
                    incrementNumGenericGroups(GenericGroupState.STABLE);
                    break;
                case DEAD:
                    incrementNumGenericGroups(GenericGroupState.DEAD);
                    break;
                case EMPTY:
                    incrementNumGenericGroups(GenericGroupState.EMPTY);
            }
        }

        if (oldState != null) {
            switch (oldState) {
                case PREPARING_REBALANCE:
                    decrementNumGenericGroups(GenericGroupState.PREPARING_REBALANCE);
                    break;
                case COMPLETING_REBALANCE:
                    decrementNumGenericGroups(GenericGroupState.COMPLETING_REBALANCE);
                    break;
                case STABLE:
                    decrementNumGenericGroups(GenericGroupState.STABLE);
                    break;
                case DEAD:
                    decrementNumGenericGroups(GenericGroupState.DEAD);
                    break;
                case EMPTY:
                    decrementNumGenericGroups(GenericGroupState.EMPTY);
            }
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
        ConsumerGroupState oldState,
        ConsumerGroupState newState
    ) {
        if (newState != null) {
            switch (newState) {
                case EMPTY:
                    incrementNumConsumerGroups(ConsumerGroupState.EMPTY);
                    break;
                case ASSIGNING:
                    incrementNumConsumerGroups(ConsumerGroupState.ASSIGNING);
                    break;
                case RECONCILING:
                    incrementNumConsumerGroups(ConsumerGroupState.RECONCILING);
                    break;
                case STABLE:
                    incrementNumConsumerGroups(ConsumerGroupState.STABLE);
                    break;
                case DEAD:
                    incrementNumConsumerGroups(ConsumerGroupState.DEAD);
            }
        }

        if (oldState != null) {
            switch (oldState) {
                case EMPTY:
                    decrementNumConsumerGroups(ConsumerGroupState.EMPTY);
                    break;
                case ASSIGNING:
                    decrementNumConsumerGroups(ConsumerGroupState.ASSIGNING);
                    break;
                case RECONCILING:
                    decrementNumConsumerGroups(ConsumerGroupState.RECONCILING);
                    break;
                case STABLE:
                    decrementNumConsumerGroups(ConsumerGroupState.STABLE);
                    break;
                case DEAD:
                    decrementNumConsumerGroups(ConsumerGroupState.DEAD);
            }
        }
    }
}
