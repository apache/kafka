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

import org.apache.kafka.common.metrics.Sensor;

/**
 * The Kafka Streams metrics interface for adding metric sensors and collecting metric values.
 */
public interface StreamsMetrics {

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
     * Generic sensor creation. Note that for most cases it is advisable to use {@link #addThroughputSensor(String, String, String, Sensor.RecordLevel, String...)}
     * of {@link #addLatencySensor(String, String, String, Sensor.RecordLevel, String...)} to ensure metric name well-formedness and conformity with the rest
     * of the streams code base.
     * @param scopeName Name of the scope, could be the type of the state store, etc.
     * @param entityName Name of the entity, could be the name of the state store instance, etc.
     * @param recordLevel The recording level (e.g., INFO or DEBUG) for this sensor.
     * @param operationName Name of the operation, could be get / put / delete / etc.
     */
    Sensor sensor(String scopeName, String entityName, String operationName, Sensor.RecordLevel recordLevel);

    /**
     * Same as previous constructor {@link #sensor(String, String, String, Sensor.RecordLevel, Sensor...)} sensor}, but takes a set of parents as well.
     */
    Sensor sensor(String scopeName, String entityName, String operationName, Sensor.RecordLevel recordLevel, Sensor... parents);

    /**
     * Remove a sensor with the given name.
     * @param name Sensor name to be removed.
     */
    void removeSensor(String name);

}


