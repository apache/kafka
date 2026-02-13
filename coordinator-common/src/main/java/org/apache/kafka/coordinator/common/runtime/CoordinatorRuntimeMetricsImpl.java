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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorState;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class CoordinatorRuntimeMetricsImpl implements CoordinatorRuntimeMetrics {

    /**
     * The metrics group
     */
    private final String metricsGroup;

    /**
     * The partition count metric name.
     */
    public static final String NUM_PARTITIONS_METRIC_NAME = "num-partitions";

    /**
     * The event queue time metric name.
     */
    public static final String EVENT_QUEUE_TIME_METRIC_NAME = "event-queue-time-ms";

    /**
     * The event queue time metric name.
     */
    public static final String EVENT_PROCESSING_TIME_METRIC_NAME = "event-processing-time-ms";

    /**
     * The event purgatory time metric name.
     */
    public static final String EVENT_PURGATORY_TIME_METRIC_NAME = "event-purgatory-time-ms";

    /**
     * The effective batch linger time metric name.
     */
    public static final String BATCH_LINGER_TIME_METRIC_NAME = "batch-linger-time-ms";

    /**
     * The flush time metric name.
     */
    public static final String BATCH_FLUSH_TIME_METRIC_NAME = "batch-flush-time-ms";

    /**
     * The buffer cache size metric name.
     */
    public static final String BATCH_BUFFER_CACHE_SIZE_METRIC_NAME = "batch-buffer-cache-size-bytes";

    /**
     * The buffer cache discard count metric name.
     */
    public static final String BATCH_BUFFER_CACHE_DISCARD_COUNT_METRIC_NAME = "batch-buffer-cache-discard-count";

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
     * Metric to count the size of the cached buffers.
     */
    private final MetricName bufferCacheSize;

    /**
     * Metric to count the number of over-sized append buffers that were discarded.
     */
    private final MetricName bufferCacheDiscardCount;
    private final AtomicLong bufferCacheDiscardCounter = new AtomicLong(0);

    /**
     * The Kafka metrics registry.
     */
    private final Metrics metrics;

    /**
     * The partition load sensor.
     */
    private final Sensor partitionLoadSensor;

    /**
     * The thread idle sensor.
     */
    private final Sensor threadIdleSensor;

    /**
     * The event queue time sensor.
     */
    private final Sensor eventQueueTimeSensor;

    /**
     * The event processing time sensor.
     */
    private final Sensor eventProcessingTimeSensor;

    /**
     * Sensor to measure the time an event stays in the purgatory.
     */
    private final Sensor eventPurgatoryTimeSensor;

    /**
     * Sensor to measure the effective batch linger time.
     */
    private final Sensor lingerTimeSensor;

    /**
     * Sensor to measure the flush time and rate.
     */
    private final Sensor flushSensor;

    public CoordinatorRuntimeMetricsImpl(Metrics metrics, String metricsGroup) {
        this.metrics = Objects.requireNonNull(metrics);
        this.metricsGroup = Objects.requireNonNull(metricsGroup);

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

        this.bufferCacheSize = kafkaMetricName(
            BATCH_BUFFER_CACHE_SIZE_METRIC_NAME,
            "The current total size in bytes of the append buffers being held in the coordinator's cache."
        );

        this.bufferCacheDiscardCount = kafkaMetricName(
            BATCH_BUFFER_CACHE_DISCARD_COUNT_METRIC_NAME,
            "The count of over-sized append buffers that were discarded instead of being cached upon release."
        );
        
        metrics.addMetric(numPartitionsLoading, (Gauge<Long>) (config, now) -> numPartitionsLoadingCounter.get());
        metrics.addMetric(numPartitionsActive, (Gauge<Long>) (config, now) -> numPartitionsActiveCounter.get());
        metrics.addMetric(numPartitionsFailed, (Gauge<Long>) (config, now) -> numPartitionsFailedCounter.get());
        metrics.addMetric(bufferCacheDiscardCount, (Gauge<Long>) (config, now) -> bufferCacheDiscardCounter.get());

        this.partitionLoadSensor = metrics.sensor(this.metricsGroup + "-PartitionLoadTime");
        this.partitionLoadSensor.add(
            metrics.metricName(
                "partition-load-time-max",
                this.metricsGroup,
                "The max time it took to load the partitions in the last 30 sec."
            ), new Max());
        this.partitionLoadSensor.add(
            metrics.metricName(
                "partition-load-time-avg",
                this.metricsGroup,
                "The average time it took to load the partitions in the last 30 sec."
            ), new Avg());

        this.threadIdleSensor = metrics.sensor(this.metricsGroup + "-ThreadIdleRatio");
        this.threadIdleSensor.add(
            metrics.metricName(
                "thread-idle-ratio-avg",
                this.metricsGroup,
                "The fraction of time the threads spent waiting for an event. This is an average across " +
                    "all coordinator event processor threads."),
            new Rate(TimeUnit.MILLISECONDS));

        KafkaMetricHistogram eventQueueTimeHistogram = KafkaMetricHistogram.newLatencyHistogram(
            suffix -> kafkaMetricName(
                EVENT_QUEUE_TIME_METRIC_NAME + "-" + suffix,
                "The " + suffix + " event queue time in milliseconds"
            )
        );
        this.eventQueueTimeSensor = metrics.sensor(this.metricsGroup + "-EventQueueTime");
        this.eventQueueTimeSensor.add(eventQueueTimeHistogram);

        KafkaMetricHistogram eventProcessingTimeHistogram = KafkaMetricHistogram.newLatencyHistogram(
            suffix -> kafkaMetricName(
                EVENT_PROCESSING_TIME_METRIC_NAME + "-" + suffix,
                "The " + suffix + " event processing time in milliseconds"
            )
        );
        this.eventProcessingTimeSensor = metrics.sensor(this.metricsGroup + "-EventProcessingTime");
        this.eventProcessingTimeSensor.add(eventProcessingTimeHistogram);

        KafkaMetricHistogram eventPurgatoryTimeHistogram = KafkaMetricHistogram.newLatencyHistogram(
            suffix -> kafkaMetricName(
                EVENT_PURGATORY_TIME_METRIC_NAME + "-" + suffix,
                "The " + suffix + " event purgatory time in milliseconds"
            )
        );
        this.eventPurgatoryTimeSensor = metrics.sensor(this.metricsGroup + "-EventPurgatoryTime");
        this.eventPurgatoryTimeSensor.add(eventPurgatoryTimeHistogram);

        KafkaMetricHistogram lingerTimeHistogram = KafkaMetricHistogram.newLatencyHistogram(
            suffix -> kafkaMetricName(
                BATCH_LINGER_TIME_METRIC_NAME + "-" + suffix,
                "The " + suffix + " effective linger time in milliseconds"
            )
        );
        this.lingerTimeSensor = metrics.sensor(this.metricsGroup + "-LingerTime");
        this.lingerTimeSensor.add(lingerTimeHistogram);

        KafkaMetricHistogram flushTimeHistogram = KafkaMetricHistogram.newLatencyHistogram(
            suffix -> kafkaMetricName(
                BATCH_FLUSH_TIME_METRIC_NAME + "-" + suffix,
                "The " + suffix + " flush time in milliseconds"
            )
        );
        this.flushSensor = metrics.sensor(this.metricsGroup + "-Flush");
        this.flushSensor.add(flushTimeHistogram);
        this.flushSensor.add(
            metrics.metricName(
                "batch-flush-rate",
                this.metricsGroup,
                "The flushes per second."),
            new Rate(TimeUnit.SECONDS, new WindowedCount()));
    }

    /**
     * Retrieve the kafka metric name.
     *
     * @param name        The name of the metric.
     * @param description The description of the metric.
     * @param keyValue    The additional metric tags as key/value pairs.
     *
     * @return The kafka metric name.
     */
    private MetricName kafkaMetricName(String name, String description, String... keyValue) {
        return metrics.metricName(name, this.metricsGroup, description, keyValue);
    }

    @Override
    public void close() {
        Arrays.asList(
            numPartitionsLoading,
            numPartitionsActive,
            numPartitionsFailed,
            eventQueueSize,
            bufferCacheSize,
            bufferCacheDiscardCount
        ).forEach(metrics::removeMetric);

        metrics.removeSensor(partitionLoadSensor.name());
        metrics.removeSensor(threadIdleSensor.name());
        metrics.removeSensor(eventQueueTimeSensor.name());
        metrics.removeSensor(eventProcessingTimeSensor.name());
        metrics.removeSensor(eventPurgatoryTimeSensor.name());
        metrics.removeSensor(lingerTimeSensor.name());
        metrics.removeSensor(flushSensor.name());
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
    public void recordEventQueueTime(long durationMs) {
        eventQueueTimeSensor.record(durationMs);
    }

    @Override
    public void recordEventProcessingTime(long durationMs) {
        eventProcessingTimeSensor.record(durationMs);
    }

    @Override
    public void recordEventPurgatoryTime(long purgatoryTimeMs) {
        eventPurgatoryTimeSensor.record(purgatoryTimeMs);
    }

    @Override
    public void recordLingerTime(long durationMs) {
        lingerTimeSensor.record(durationMs);
    }

    @Override
    public void recordFlushTime(long durationMs) {
        flushSensor.record(durationMs);
    }

    @Override
    public void recordThreadIdleTime(double idleTimeMs) {
        threadIdleSensor.record(idleTimeMs);
    }

    @Override
    public void registerEventQueueSizeGauge(Supplier<Integer> sizeSupplier) {
        metrics.addMetric(eventQueueSize, (Gauge<Long>) (config, now) -> (long) sizeSupplier.get());
    }

    @Override
    public void registerBufferCacheSizeGauge(Supplier<Long> bufferCacheSizeSupplier) {
        metrics.addMetric(bufferCacheSize, (Gauge<Long>) (config, now) -> bufferCacheSizeSupplier.get());
    }

    @Override
    public void recordBufferCacheDiscarded() {
        bufferCacheDiscardCounter.incrementAndGet();
    }
}
