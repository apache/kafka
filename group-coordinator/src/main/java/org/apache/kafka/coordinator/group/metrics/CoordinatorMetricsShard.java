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

/**
 * A CoordinatorMetricsShard is mapped to a single CoordinatorShard. For gauges, each metrics shard increments/decrements
 * based on the operations performed. Then, {@link CoordinatorMetrics} will perform aggregations across all shards.
 *
 * For sensors, each shard individually records the observed values.
 */
public interface CoordinatorMetricsShard {
    /**
     * Increment a global gauge.
     *
     * @param metricName the metric name.
     */
    void incrementGlobalGauge(MetricName metricName);

    /**
     * Increment a local gauge.
     *
     * @param metricName the metric name.
     */
    void incrementLocalGauge(MetricName metricName);

    /**
     * Decrement a global gauge.
     *
     * @param metricName the metric name.
     */
    void decrementGlobalGauge(MetricName metricName);

    /**
     * Decrement a local gauge.
     *
     * @param metricName the metric name.
     */
    void decrementLocalGauge(MetricName metricName);

    /**
     * Obtain the current value of a global gauge.
     *
     * @param metricName the metric name.
     */
    long globalGaugeValue(MetricName metricName);

    /**
     * Obtain the current value of a local gauge.
     *
     * @param metricName the metric name.
     */
    long localGaugeValue(MetricName metricName);

    /**
     * Increment the value of a sensor.
     *
     * @param sensorName the sensor name.
     */
    void record(String sensorName);

    /**
     * Record a sensor with a value.
     *
     * @param sensorName the sensor name.
     * @param val the value to record.
     */
    void record(String sensorName, double val);

    /**
     * @return The topic partition.
     */
    TopicPartition topicPartition();

    /**
     * Commits all gauges backed by the snapshot registry.
     *
     * @param offset The last committed offset.
     */
    void commitUpTo(long offset);
}
