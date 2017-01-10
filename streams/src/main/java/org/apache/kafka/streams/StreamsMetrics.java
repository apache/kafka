/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
@InterfaceStability.Unstable
public interface StreamsMetrics {

    /**
     * Get read-only handle on global metrics registry
     * @return Map of all metrics.
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * Add a latency sensor and default associated metrics. Metrics include both latency ones
     * (average and max latency) and throughput ones (operations/time unit).
     *
     * @param scopeName Name of the scope, could be the type of the state store, etc.
     * @param entityName Name of the entity, could be the name of the state store instance, etc.
     * @param recordingLevel The recording level (e.g., INFO or DEBUG) for this sensor.
     * @param operationName Name of the operation, could be get / put / delete / etc.
     * @param tags Additional tags of the sensor.
     * @return The added sensor.
     */
    Sensor addLatencySensor(String scopeName, String entityName, String operationName, Sensor.RecordingLevel recordingLevel, String... tags);

    /**
     * Record the given latency value of the sensor.
     * @param sensor sensor whose latency we are recording.
     * @param startNs start of measurement time in nanoseconds.
     * @param endNs end of measurement time in nanoseconds.
     */
    void recordLatency(Sensor sensor, long startNs, long endNs);

    /**
     * Add a throughput sensor and default associated metrics. Metrics include throughput ones
     * (operations/time unit).
     *
     * @param scopeName Name of the scope, could be the type of the state store, etc.
     * @param entityName Name of the entity, could be the name of the state store instance, etc.
     * @param recordingLevel The recording level (e.g., INFO or DEBUG) for this sensor.
     * @param operationName Name of the operation, could be get / put / delete / etc.
     * @param tags Additional tags of the sensor.
     * @return The added sensor.
     */
    Sensor addThroughputSensor(String scopeName, String entityName, String operationName, Sensor.RecordingLevel recordingLevel, String... tags);

    /**
     * Records the throughput value of a sensor.
     * @param sensor addSensor whose throughput we are recording.
     * @param value throughput value.
     */
    void recordThroughput(Sensor sensor, long value);


    /**
     * Generic method to create a sensor.
     * Note that for most cases it is advisable to use {@link #addThroughputSensor(String, String, String, Sensor.RecordingLevel, String...)}
     * or {@link #addLatencySensor(String, String, String, Sensor.RecordingLevel, String...)} to ensure
     * metric name well-formedness and conformity with the rest of the streams code base. However,
     * if the above two methods are not sufficient, this method can also be used.
     * @param name Name of the sensor.
     * @param recordingLevel The recording level (e.g., INFO or DEBUG) for this sensor.
     */
    Sensor addSensor(String name, Sensor.RecordingLevel recordingLevel);

    /**
     * Generic method to create a sensor with parent sensors.
     * Note that for most cases it is advisable to use {@link #addThroughputSensor(String, String, String, Sensor.RecordingLevel, String...)}
     * or {@link #addLatencySensor(String, String, String, Sensor.RecordingLevel, String...)} to ensure
     * metric name well-formedness and conformity with the rest of the streams code base. However,
     * if the above two methods are not sufficient, this method can also be used.
     * @param name Name of the sensor.
     * @param recordingLevel The recording level (e.g., INFO or DEBUG) for this sensor.
     */
    Sensor addSensor(String name, Sensor.RecordingLevel recordingLevel, Sensor... parents);

    /**
     * Remove a sensor.
     * @param sensor Sensor to be removed.
     */
    void removeSensor(Sensor sensor);
}


