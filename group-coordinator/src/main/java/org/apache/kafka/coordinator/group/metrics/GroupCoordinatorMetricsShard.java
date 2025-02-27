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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup.ShareGroupState;
import org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
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

    private static final Logger log = LoggerFactory.getLogger(GroupCoordinatorMetricsShard.class);

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
     * Classic group size gauge counters keyed by the metric name.
     */
    private volatile Map<ClassicGroupState, Long> classicGroupGauges;

    /**
     * Consumer group size gauge counters keyed by the metric name.
     */
    private volatile Map<ConsumerGroupState, Long> consumerGroupGauges;

    /**
     * Share group size gauge counters keyed by the metric name.
     */
    private volatile Map<ShareGroupState, Long> shareGroupGauges;

    /**
     * Streams group size gauge counters keyed by the metric name.
     */
    private volatile Map<StreamsGroupState, Long> streamsGroupGauges;

    /**
     * All sensors keyed by the sensor name. A Sensor object is shared across all metrics shards.
     */
    private final Map<String, Sensor> globalSensors;

    /**
     * The number of offsets gauge counter.
     */
    private final TimelineGaugeCounter numOffsetsTimelineGaugeCounter;

    /**
     * The number of classic groups metric counter.
     */
    private final TimelineGaugeCounter numClassicGroupsTimelineCounter;

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
        numClassicGroupsTimelineCounter = new TimelineGaugeCounter(new TimelineLong(snapshotRegistry), new AtomicLong(0));

        this.classicGroupGauges = Collections.emptyMap();
        this.consumerGroupGauges = Collections.emptyMap();
        this.streamsGroupGauges = Collections.emptyMap();
        this.shareGroupGauges = Collections.emptyMap();

        this.globalSensors = Objects.requireNonNull(globalSensors);
        this.topicPartition = Objects.requireNonNull(topicPartition);
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
     * Set the number of consumer groups.
     * This method should be the only way to update the map and is called by the scheduled task
     * that updates the metrics in {@link org.apache.kafka.coordinator.group.GroupCoordinatorShard}.
     * Breaking this will result in inconsistent behavior.
     *
     * @param consumerGroupGauges The map counting the number of consumer groups in each state.
     */
    public void setConsumerGroupGauges(Map<ConsumerGroupState, Long> consumerGroupGauges) {
        this.consumerGroupGauges = consumerGroupGauges;
    }
    
    /**
     * Set the number of streams groups.
     * This method should be the only way to update the map and is called by the scheduled task
     * that updates the metrics in {@link org.apache.kafka.coordinator.group.GroupCoordinatorShard}.
     * Breaking this will result in inconsistent behavior.
     *
     * @param streamsGroupGauges The map counting the number of streams groups in each state.
     */
    public void setStreamsGroupGauges(Map<StreamsGroupState, Long> streamsGroupGauges) {
        this.streamsGroupGauges = streamsGroupGauges;
    }

    /**
     * Set the number of share groups.
     * This method should be the only way to update the map and is called by the scheduled task
     * that updates the metrics in {@link org.apache.kafka.coordinator.group.GroupCoordinatorShard}.
     * Breaking this will result in inconsistent behavior.
     *
     * @param shareGroupGauges The map counting the number of share groups in each state.
     */
    public void setShareGroupGauges(Map<ShareGroupState, Long> shareGroupGauges) {
        this.shareGroupGauges = shareGroupGauges;
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
     * @return The number of offsets.
     */
    public long numOffsets() {
        return numOffsetsTimelineGaugeCounter.atomicLong.get();
    }

    /**
     * Obtain the number of classic groups in the specified state.
     *
     * @param state  The classic group state.
     *
     * @return   The number of classic groups in `state`.
     */
    public long numClassicGroups(ClassicGroupState state) {
        Long counter = classicGroupGauges.get(state);
        if (counter != null) {
            return counter;
        }
        return 0L;
    }

    /**
     * @return The total number of classic groups.
     */
    public long numClassicGroups() {
        return classicGroupGauges.values().stream()
                                 .mapToLong(Long::longValue).sum();
    }

    /**
     * Obtain the number of consumer groups in the specified state.
     *
     * @param state  the consumer group state.
     *
     * @return   The number of consumer groups in `state`.
     */
    public long numConsumerGroups(ConsumerGroupState state) {
        Long counter = consumerGroupGauges.get(state);
        if (counter != null) {
            return counter;
        }
        return 0L;
    }

    /**
     * @return The total number of consumer groups.
     */
    public long numConsumerGroups() {
        return consumerGroupGauges.values().stream()
            .mapToLong(Long::longValue).sum();
    }
    
    /**
     * Get the number of streams groups in the specified state.
     *
     * @param state  the streams group state.
     *
     * @return   The number of streams groups in `state`.
     */
    public long numStreamsGroups(StreamsGroupState state) {
        Long counter = streamsGroupGauges.get(state);
        if (counter != null) {
            return counter;
        }
        return 0L;
    }

    /**
     * @return The total number of streams groups.
     */
    public long numStreamsGroups() {
        return streamsGroupGauges.values().stream()
            .mapToLong(Long::longValue).sum();
    }

    /**
     * Get the number of share groups in the specified state.
     *
     * @param state  the share group state.
     *
     * @return   The number of share groups in `state`.
     */
    public long numShareGroups(ShareGroupState state) {
        Long counter = shareGroupGauges.get(state);
        if (counter != null) {
            return counter;
        }
        return 0L;
    }

    /**
     * @return The total number of share groups.
     */
    public long numShareGroups() {
        return shareGroupGauges.values().stream()
            .mapToLong(Long::longValue).sum();
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
        synchronized (numClassicGroupsTimelineCounter.timelineLong) {
            long value = numClassicGroupsTimelineCounter.timelineLong.get(offset);
            numClassicGroupsTimelineCounter.atomicLong.set(value);
        }

        synchronized (numOffsetsTimelineGaugeCounter.timelineLong) {
            long value = numOffsetsTimelineGaugeCounter.timelineLong.get(offset);
            numOffsetsTimelineGaugeCounter.atomicLong.set(value);
        }
    }

    /**
     * Sets the classicGroupGauges.
     * This method should be the only way to update the map and is called by the scheduled task
     * that updates the metrics in {@link org.apache.kafka.coordinator.group.GroupCoordinatorShard}.
     * Breaking this will result in inconsistent behavior.
     *
     * @param classicGroupGauges The map counting the number of classic groups in each state.
     */
    public void setClassicGroupGauges(
        Map<ClassicGroupState, Long> classicGroupGauges
    ) {
        this.classicGroupGauges = classicGroupGauges;
    }
}
