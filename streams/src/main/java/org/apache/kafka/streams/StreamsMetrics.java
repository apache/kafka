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
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.metrics.Sensor;

import java.util.Map;

/**
 * The Kafka Streams metrics interface for adding metric sensors and collecting metric values.
 */
@InterfaceStability.Evolving
public interface StreamsMetrics {

    /**
     * Get read-only handle on global metrics registry.
     *
     * @return Map of all metrics.
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * Add a latency and throughput sensor for a specific operation, which will include the following metrics:
     * <ol>
     *   <li>average latency</li>
     *   <li>max latency</li>
     *   <li>invocation rate (num.operations / time unit)</li>
     *   <li>total invocation count</li>
     * </ol>
     *
     * Whenever a user record this sensor via {@link Sensor#record(double)} etc,
     * it will be counted as one invocation of the operation, and hence the rate / count metrics will be updated accordingly;
     * and the recorded latency value will be used to update the average / max latency as well. The time unit of the latency can be defined
     * by the user.
     *
     * Note that you can add more metrics to this sensor after created it, which can then be updated upon {@link Sensor#record(double)} calls;
     * but additional user-customized metrics will not be managed by {@link StreamsMetrics}.
     *
     * @param scopeName          name of the scope, which will be used as part of the metrics type, e.g.: "stream-[scope]-metrics".
     * @param entityName         name of the entity, which will be used as part of the metric tags, e.g.: "[scope]-id" = "[entity]".
     * @param operationName      name of the operation, which will be used as the name of the metric, e.g.: "[operation]-latency-avg".
     * @param recordingLevel     the recording level (e.g., INFO or DEBUG) for this sensor.
     * @param tags               additional tags of the sensor
     * @return The added sensor.
     */
    Sensor addLatencyAndThroughputSensor(final String scopeName,
                                         final String entityName,
                                         final String operationName,
                                         final Sensor.RecordingLevel recordingLevel,
                                         final String... tags);

    /**
     * Add a throughput sensor for a specific operation, which will include the following metrics:
     * <ol>
     *   <li>invocation rate (num.operations / time unit)</li>
     *   <li>total invocation count</li>
     * </ol>
     *
     * Whenever a user record this sensor via {@link Sensor#record(double)} etc,
     * it will be counted as one invocation of the operation, and hence the rate / count metrics will be updated accordingly;
     * whatever recorded values would be ignored since it would not be used to update any metrics.
     *
     * The metrics included in the returned sensor is a strict subset of the metrics included in the sensor returned by
     * {@link #addLatencyAndThroughputSensor(String, String, String, Sensor.RecordingLevel, String...)}.
     *
     * Note that you can add more metrics to this sensor after created it, which can then be updated upon {@link Sensor#record(double)} calls;
     * but additional user-customized metrics will not be managed by {@link StreamsMetrics}.
     *
     * @param scopeName          name of the scope, which will be used as part of the metrics type, e.g.: "stream-[scope]-metrics".
     * @param entityName         name of the entity, which will be used as part of the metric tags, e.g.: "[scope]-id" = "[entity]".
     * @param operationName      name of the operation, which will be used as the name of the metric, e.g.: "[operation]-latency-avg".
     * @param recordingLevel     the recording level (e.g., INFO or DEBUG) for this sensor.
     * @param tags               additional tags of the sensor
     * @return The added sensor.
     */
    Sensor addThroughputSensor(final String scopeName,
                               final String entityName,
                               final String operationName,
                               final Sensor.RecordingLevel recordingLevel,
                               final String... tags);

    /**
     * Record the given latency value of the sensor.
     *
     * @param sensor  sensor whose latency we are recording.
     * @param startNs start of measurement time in nanoseconds.
     * @param endNs   end of measurement time in nanoseconds.
     * @deprecated    since 2.3. Use {@link Sensor#record(double)} directly instead.
     */
    @Deprecated
    void recordLatency(final Sensor sensor,
                       final long startNs,
                       final long endNs);

    /**
     * Record the throughput value of a sensor.
     *
     * @param sensor  sensor whose throughput we are recording
     * @param value   throughput value
     * @deprecated    since 2.3. Use {@link Sensor#record(double)} directly instead.
     */
    @Deprecated
    void recordThroughput(final Sensor sensor,
                          final long value);


    /**
     * Generic method to create a sensor.
     *
     * Note that for most cases it is advisable to use
     * {@link #addThroughputSensor(String, String, String, Sensor.RecordingLevel, String...)}
     * or {@link #addLatencyAndThroughputSensor(String, String, String, Sensor.RecordingLevel, String...)} to ensure
     * metric name well-formedness and conformity with the rest of the streams code base.
     * However, if the above two methods are not sufficient, this method can also be used.
     *
     * @param name           name of the sensor.
     * @param recordingLevel the recording level (e.g., INFO or DEBUG) for this sensor
     * @return The added sensor.
     */
    Sensor addSensor(final String name,
                     final Sensor.RecordingLevel recordingLevel);

    /**
     * Generic method to create a sensor with parent sensors.
     *
     * Note that for most cases it is advisable to use
     * {@link #addThroughputSensor(String, String, String, Sensor.RecordingLevel, String...)}
     * or {@link #addLatencyAndThroughputSensor(String, String, String, Sensor.RecordingLevel, String...)} to ensure
     * metric name well-formedness and conformity with the rest of the streams code base.
     * However, if the above two methods are not sufficient, this method can also be used.
     *
     * @param name           name of the sensor
     * @param recordingLevel the recording level (e.g., INFO or DEBUG) for this sensor
     * @return The added sensor.
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


