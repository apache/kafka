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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

/**
 * The Kafka Streams metrics interface for adding metric sensors and collecting metric values.
 */
public interface StreamsMetrics {

    /**
     * @return The base registry where all the metrics are recorded.
     */
    Metrics registry();

    /**
     * Add a latency sensor. This is equivalent to adding a sensor with metrics on latency and rate.
     *
     * @param scopeName Name of the scope, could be the type of the state store, etc.
     * @param entityName Name of the entity, could be the name of the state store instance, etc.
     * @param recordLevel The recording level (e.g., INFO or DEBUG) for this sensor.
     * @param operationName Name of the operation, could be get / put / delete / etc.
     * @param tags Additional tags of the sensor.
     * @return The added sensor.
     */
    Sensor addLatencySensor(String scopeName, String entityName, String operationName, Sensor.RecordLevel recordLevel, String... tags);

    /**
     * Record the given latency value of the sensor.
     * @param sensor sensor whose latency we are recording.
     * @param startNs start of measurement time in nanoseconds.
     * @param endNs end of measurement time in nanoseconds.
     */
    void recordLatency(Sensor sensor, long startNs, long endNs);

    /**
     * Add a throughput sensor. This is equivalent to adding a sensor with metrics rate.
     *
     * @param scopeName Name of the scope, could be the type of the state store, etc.
     * @param entityName Name of the entity, could be the name of the state store instance, etc.
     * @param recordLevel The recording level (e.g., INFO or DEBUG) for this sensor.
     * @param operationName Name of the operation, could be get / put / delete / etc.
     * @param tags Additional tags of the sensor.
     * @return The added sensor.
     */
    Sensor addThroughputSensor(String scopeName, String entityName, String operationName, Sensor.RecordLevel recordLevel, String... tags);

    /**
     * Records the throughput value of a sensor.
     * @param sensor sensor whose throughput we are recording.
     * @param value throughput value.
     */
    void recordThroughput(Sensor sensor, long value);

    /**
     * Remove a sensor with the given name.
     * @param name Sensor name to be removed.
     */
    void removeSensor(String name);

}


