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
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime.CoordinatorState;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class GroupCoordinatorRuntimeMetrics implements CoordinatorRuntimeMetrics {
    /**
     * The metrics group.
     */
    public static final String METRICS_GROUP = "group-coordinator-metrics";

    /**
     * The partition count metric name.
     */
    public static final String NUM_PARTITIONS_METRIC_NAME = "num-partitions";

    /**
     * Metric to count the number of partitions in Loading state.
     */
    private final MetricName numPartitionsLoading;
    private final AtomicLong numPartitionsLoadingCounter = new AtomicLong(0);

    /**
     * Metric to count the number of partitions in Active state.
     */
    private final MetricName numPartitionsActive;
    private final AtomicLong numPartitionsActiveCounter = new AtomicLong(0);

    /**
     * Metric to count the number of partitions in Failed state.
     */
    private final MetricName numPartitionsFailed;
    private final AtomicLong numPartitionsFailedCounter = new AtomicLong(0);

    /**
     * Metric to count the size of the processor queue.
     */
    private final MetricName eventQueueSize;

    /**
     * The Kafka metrics registry.
     */
    private final Metrics metrics;

    /**
     * The partition load sensor.
     */
    private Sensor partitionLoadSensor;

    /**
     * The thread idle sensor.
     */
    private Sensor threadIdleRatioSensor;

    public GroupCoordinatorRuntimeMetrics(Metrics metrics) {
        this.metrics = Objects.requireNonNull(metrics);

        this.numPartitionsLoading = kafkaMetricName(
            NUM_PARTITIONS_METRIC_NAME,
            "The number of partitions in Loading state.",
            "state", "loading"
        );

        this.numPartitionsActive = kafkaMetricName(
            NUM_PARTITIONS_METRIC_NAME,
            "The number of partitions in Active state.",
            "state", "active"
        );

        this.numPartitionsFailed = kafkaMetricName(
            NUM_PARTITIONS_METRIC_NAME,
            "The number of partitions in Failed state.",
            "state", "failed"
        );

        this.eventQueueSize = kafkaMetricName("event-queue-size", "The event accumulator queue size.");

        metrics.addMetric(numPartitionsLoading, (Gauge<Long>) (config, now) -> numPartitionsLoadingCounter.get());
        metrics.addMetric(numPartitionsActive, (Gauge<Long>) (config, now) -> numPartitionsActiveCounter.get());
        metrics.addMetric(numPartitionsFailed, (Gauge<Long>) (config, now) -> numPartitionsFailedCounter.get());

        this.partitionLoadSensor = metrics.sensor("GroupPartitionLoadTime");
        this.partitionLoadSensor.add(
            metrics.metricName(
                "partition-load-time-max",
                METRICS_GROUP,
                "The max time it took to load the partitions in the last 30 sec."
            ), new Max());
        this.partitionLoadSensor.add(
            metrics.metricName(
                "partition-load-time-avg",
                METRICS_GROUP,
                "The average time it took to load the partitions in the last 30 sec."
            ), new Avg());

        this.threadIdleRatioSensor = metrics.sensor("ThreadIdleRatio");
        this.threadIdleRatioSensor.add(
            metrics.metricName(
                "thread-idle-ratio-min",
                METRICS_GROUP,
                "The minimum thread idle ratio over the last 30 seconds."
            ), new Min());
        this.threadIdleRatioSensor.add(
            metrics.metricName(
                "thread-idle-ratio-avg",
                METRICS_GROUP,
                "The average thread idle ratio over the last 30 seconds."
            ), new Avg());
    }

    /**
     * Retrieve the kafka metric name.
     *
     * @param name The name of the metric.
     *
     * @return The kafka metric name.
     */
    private MetricName kafkaMetricName(String name, String description, String... keyValue) {
        return metrics.metricName(name, METRICS_GROUP, description, keyValue);
    }

    @Override
    public void close() {
        Arrays.asList(
            numPartitionsLoading,
            numPartitionsActive,
            numPartitionsFailed,
            eventQueueSize
        ).forEach(metrics::removeMetric);

        metrics.removeSensor(partitionLoadSensor.name());
        metrics.removeSensor(threadIdleRatioSensor.name());
    }

    /**
     * Called when the partition state changes. Decrement the old state and increment the new state.
     *
     * @param oldState The old state.
     * @param newState The new state to transition to.
     */
    @Override
    public void recordPartitionStateChange(CoordinatorState oldState, CoordinatorState newState) {
        switch (oldState) {
            case INITIAL:
            case CLOSED:
                break;
            case LOADING:
                numPartitionsLoadingCounter.decrementAndGet();
                break;
            case ACTIVE:
                numPartitionsActiveCounter.decrementAndGet();
                break;
            case FAILED:
                numPartitionsFailedCounter.decrementAndGet();
        }

        switch (newState) {
            case INITIAL:
            case CLOSED:
                break;
            case LOADING:
                numPartitionsLoadingCounter.incrementAndGet();
                break;
            case ACTIVE:
                numPartitionsActiveCounter.incrementAndGet();
                break;
            case FAILED:
                numPartitionsFailedCounter.incrementAndGet();
        }
    }

    @Override
    public void recordPartitionLoadSensor(long startTimeMs, long endTimeMs) {
        this.partitionLoadSensor.record(endTimeMs - startTimeMs, endTimeMs, false);
    }

    @Override
    public void recordEventQueueTime(long durationMs) { }

    @Override
    public void recordEventQueueProcessingTime(long durationMs) { }

    @Override
    public void recordThreadIdleRatio(double ratio) {
        threadIdleRatioSensor.record(ratio);
    }

    @Override
    public void registerEventQueueSizeGauge(Supplier<Integer> sizeSupplier) {
        metrics.addMetric(eventQueueSize, (Gauge<Long>) (config, now) -> (long) sizeSupplier.get());
    }
}
