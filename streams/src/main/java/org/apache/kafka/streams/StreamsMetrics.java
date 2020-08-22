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
     * Add a latency and throughput sensor for a specific operation, which will include the following sensors:
     * <ol>
     *   <li>average latency</li>
     *   <li>max latency</li>
     *   <li>throughput (num.operations / time unit)</li>
     * </ol>
     * Also create a parent sensor with the same metrics that aggregates all entities with the same operation under the
     * same scope if it has not been created.
     *
     * @param scopeName      name of the scope, could be the type of the state store, etc.
     * @param entityName     name of the entity, could be the name of the state store instance, etc.
     * @param operationName  name of the operation, could be get / put / delete / etc.
     * @param recordingLevel the recording level (e.g., INFO or DEBUG) for this sensor.
     * @param tags           additional tags of the sensor
     * @return The added sensor.
     * @deprecated since 2.5. Use {@link #addLatencyRateTotalSensor(String, String, String, Sensor.RecordingLevel, String...) addLatencyRateTotalSensor()}
     * instead.
     */
    @Deprecated
    Sensor addLatencyAndThroughputSensor(final String scopeName,
                                         final String entityName,
                                         final String operationName,
                                         final Sensor.RecordingLevel recordingLevel,
                                         final String... tags);

    /**
     * Record the given latency value of the sensor.
     * If the passed sensor includes throughput metrics, e.g., when created by the
     * {@link #addLatencyAndThroughputSensor(String, String, String, Sensor.RecordingLevel, String...)} method, then the
     * throughput metrics will also be recorded from this event.
     *
     * @param sensor  sensor whose latency we are recording.
     * @param startNs start of measurement time in nanoseconds.
     * @param endNs   end of measurement time in nanoseconds.
     * @deprecated since 2.5. Use {@link Sensor#record(double) Sensor#record()} instead.
     */
    @Deprecated
    void recordLatency(final Sensor sensor,
                       final long startNs,
                       final long endNs);

    /**
     * Add a throughput sensor for a specific operation:
     * <ol>
     *   <li>throughput (num.operations / time unit)</li>
     * </ol>
     * Also create a parent sensor with the same metrics that aggregates all entities with the same operation under the
     * same scope if it has not been created.
     * This sensor is a strict subset of the sensors created by
     * {@link #addLatencyAndThroughputSensor(String, String, String, Sensor.RecordingLevel, String...)}.
     *
     * @param scopeName      name of the scope, could be the type of the state store, etc.
     * @param entityName     name of the entity, could be the name of the state store instance, etc.
     * @param operationName  name of the operation, could be get / put / delete / etc.
     * @param recordingLevel the recording level (e.g., INFO or DEBUG) for this sensor.
     * @param tags           additional tags of the sensor
     * @return The added sensor.
     * @deprecated since 2.5. Use {@link #addRateTotalSensor(String, String, String, Sensor.RecordingLevel, String...)
     * addRateTotalSensor()} instead.
     */
    @Deprecated
    Sensor addThroughputSensor(final String scopeName,
                               final String entityName,
                               final String operationName,
                               final Sensor.RecordingLevel recordingLevel,
                               final String... tags);

    /**
     * Record the throughput value of a sensor.
     *
     * @param sensor add Sensor whose throughput we are recording
     * @param value  throughput value
     * @deprecated since 2.5. Use {@link Sensor#record() Sensor#record()} instead.
     */
    @Deprecated
    void recordThroughput(final Sensor sensor,
                          final long value);


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


