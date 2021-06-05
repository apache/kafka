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
package org.apache.kafka.streams;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;

import java.util.Map;

/**
 * The Kafka Streams metrics interface for adding metric sensors and collecting metric values.
 */
public interface StreamsMetrics {

    /**
     * Get read-only handle on global metrics registry.
     *
     * @return Map of all metrics.
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * Add a latency, rate and total sensor for a specific operation, which will include the following metrics:
     * <ol>
     * <li>average latency</li>
     * <li>max latency</li>
     * <li>invocation rate (num.operations / seconds)</li>
     * <li>total invocation count</li>
     * </ol>
     * Whenever a user records this sensor via {@link Sensor#record(double)} etc, it will be counted as one invocation
     * of the operation, and hence the rate / count metrics will be updated accordingly; and the recorded latency value
     * will be used to update the average / max latency as well.
     *
     * Note that you can add more metrics to this sensor after you created it, which can then be updated upon
     * {@link Sensor#record(double)} calls.
     *
     * The added sensor and its metrics can be removed with {@link #removeSensor(Sensor) removeSensor()}.
     *
     * @param scopeName          name of the scope, which will be used as part of the metric type, e.g.: "stream-[scope]-metrics".
     * @param entityName         name of the entity, which will be used as part of the metric tags, e.g.: "[scope]-id" = "[entity]".
     * @param operationName      name of the operation, which will be used as the name of the metric, e.g.: "[operation]-latency-avg".
     * @param recordingLevel     the recording level (e.g., INFO or DEBUG) for this sensor.
     * @param tags               additional tags of the sensor
     * @return The added sensor.
     * @see #addRateTotalSensor(String, String, String, Sensor.RecordingLevel, String...)
     * @see #removeSensor(Sensor)
     * @see #addSensor(String, Sensor.RecordingLevel, Sensor...)
     */
    Sensor addLatencyRateTotalSensor(final String scopeName,
                                     final String entityName,
                                     final String operationName,
                                     final Sensor.RecordingLevel recordingLevel,
                                     final String... tags);

    /**
     * Add a rate and a total sensor for a specific operation, which will include the following metrics:
     * <ol>
     * <li>invocation rate (num.operations / time unit)</li>
     * <li>total invocation count</li>
     * </ol>
     * Whenever a user records this sensor via {@link Sensor#record(double)} etc,
     * it will be counted as one invocation of the operation, and hence the rate / count metrics will be updated accordingly.
     *
     * Note that you can add more metrics to this sensor after you created it, which can then be updated upon
     * {@link Sensor#record(double)} calls.
     *
     * The added sensor and its metrics can be removed with {@link #removeSensor(Sensor) removeSensor()}.
     *
     * @param scopeName          name of the scope, which will be used as part of the metrics type, e.g.: "stream-[scope]-metrics".
     * @param entityName         name of the entity, which will be used as part of the metric tags, e.g.: "[scope]-id" = "[entity]".
     * @param operationName      name of the operation, which will be used as the name of the metric, e.g.: "[operation]-total".
     * @param recordingLevel     the recording level (e.g., INFO or DEBUG) for this sensor.
     * @param tags               additional tags of the sensor
     * @return The added sensor.
     * @see #addLatencyRateTotalSensor(String, String, String, Sensor.RecordingLevel, String...)
     * @see #removeSensor(Sensor)
     * @see #addSensor(String, Sensor.RecordingLevel, Sensor...)
     */
    Sensor addRateTotalSensor(final String scopeName,
                              final String entityName,
                              final String operationName,
                              final Sensor.RecordingLevel recordingLevel,
                              final String... tags);

    /**
     * Generic method to create a sensor.
     * Note that for most cases it is advisable to use
     * {@link #addRateTotalSensor(String, String, String, Sensor.RecordingLevel, String...) addRateTotalSensor()}
     * or {@link #addLatencyRateTotalSensor(String, String, String, Sensor.RecordingLevel, String...) addLatencyRateTotalSensor()}
     * to ensure metric name well-formedness and conformity with the rest of the Kafka Streams code base.
     * However, if the above two methods are not sufficient, this method can also be used.
     *
     * @param name           name of the sensor.
     * @param recordingLevel the recording level (e.g., INFO or DEBUG) for this sensor
     * @return The added sensor.
     * @see #addRateTotalSensor(String, String, String, Sensor.RecordingLevel, String...)
     * @see #addLatencyRateTotalSensor(String, String, String, Sensor.RecordingLevel, String...)
     * @see #removeSensor(Sensor)
     */
    Sensor addSensor(final String name,
                     final Sensor.RecordingLevel recordingLevel);

    /**
     * Generic method to create a sensor with parent sensors.
     * Note that for most cases it is advisable to use
     * {@link #addRateTotalSensor(String, String, String, Sensor.RecordingLevel, String...) addRateTotalSensor()}
     * or {@link #addLatencyRateTotalSensor(String, String, String, Sensor.RecordingLevel, String...) addLatencyRateTotalSensor()}
     * to ensure metric name well-formedness and conformity with the rest of the Kafka Streams code base.
     * However, if the above two methods are not sufficient, this method can also be used.
     *
     * @param name           name of the sensor
     * @param recordingLevel the recording level (e.g., INFO or DEBUG) for this sensor
     * @return The added sensor.
     * @see #addRateTotalSensor(String, String, String, Sensor.RecordingLevel, String...)
     * @see #addLatencyRateTotalSensor(String, String, String, Sensor.RecordingLevel, String...)
     * @see #removeSensor(Sensor)
     */
    Sensor addSensor(final String name,
                     final Sensor.RecordingLevel recordingLevel,
                     final Sensor... parents);

    /**
     * Remove a sensor.
     * @param sensor sensor to be removed
     */
    void removeSensor(final Sensor sensor);
}


