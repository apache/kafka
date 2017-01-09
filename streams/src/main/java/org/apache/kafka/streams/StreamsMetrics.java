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
     * Add a latency addSensor. This is equivalent to adding a addSensor with metrics on latency and rate.
     *
     * @param scopeName Name of the scope, could be the type of the state store, etc.
     * @param entityName Name of the entity, could be the name of the state store instance, etc.
     * @param recordLevel The recording level (e.g., INFO or DEBUG) for this addSensor.
     * @param operationName Name of the operation, could be get / put / delete / etc.
     * @param tags Additional tags of the addSensor.
     * @return The added addSensor.
     */
    Sensor addLatencySensor(String scopeName, String entityName, String operationName, Sensor.RecordLevel recordLevel, String... tags);

    /**
     * Record the given latency value of the addSensor.
     * @param sensor addSensor whose latency we are recording.
     * @param startNs start of measurement time in nanoseconds.
     * @param endNs end of measurement time in nanoseconds.
     */
    void recordLatency(Sensor sensor, long startNs, long endNs);

    /**
     * Add a throughput addSensor. This is equivalent to adding a addSensor with metrics rate.
     *
     * @param scopeName Name of the scope, could be the type of the state store, etc.
     * @param entityName Name of the entity, could be the name of the state store instance, etc.
     * @param recordLevel The recording level (e.g., INFO or DEBUG) for this addSensor.
     * @param operationName Name of the operation, could be get / put / delete / etc.
     * @param tags Additional tags of the addSensor.
     * @return The added addSensor.
     */
    Sensor addThroughputSensor(String scopeName, String entityName, String operationName, Sensor.RecordLevel recordLevel, String... tags);

    /**
     * Records the throughput value of a addSensor.
     * @param sensor addSensor whose throughput we are recording.
     * @param value throughput value.
     */
    void recordThroughput(Sensor sensor, long value);


    /**
     * Generic addSensor creation. Note that for most cases it is advisable to use {@link #addThroughputSensor(String, String, String, Sensor.RecordLevel, String...)}
     * or {@link #addLatencySensor(String, String, String, Sensor.RecordLevel, String...)} to ensure metric name well-formedness and conformity with the rest
     * of the streams code base.
     * @param name Name of the sensor.
     * @param recordLevel The recording level (e.g., INFO or DEBUG) for this addSensor.
     */
    Sensor addSensor(String name, Sensor.RecordLevel recordLevel);

    /**
     * Same as previous constructor {@link #addSensor(String, Sensor.RecordLevel, Sensor...)} addSensor}, but takes a set of parents as well.
     *
     */
    Sensor addSensor(String name, Sensor.RecordLevel recordLevel, Sensor... parents);

    /**
     * Remove a addSensor with the given name.
     * @param sensor Sensor to be removed.
     */
    void removeSensor(Sensor sensor);
}


