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

/**
 * A CoordinatorMetricsShard is mapped to a single CoordinatorShard. The metrics shard records sensors that have been
 * defined in {@link CoordinatorMetrics}. Coordinator specific gauges and related methods are exposed in the
 * implementation of CoordinatorMetricsShard (i.e. {@link GroupCoordinatorMetricsShard}).
 *
 * For sensors, each shard individually records the observed values.
 */
public interface CoordinatorMetricsShard {
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
