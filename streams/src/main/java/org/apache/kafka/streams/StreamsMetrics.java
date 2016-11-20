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
     * Add the latency sensor.
     *
     * @param scopeName Name of the scope, could be the type of the state store, etc.
     * @param entityName Name of the entity, could be the name of the state store instance, etc.
     * @param operationName Name of the operation, could be get / put / delete / etc.
     * @param tags Additional tags of the sensor.
     * @return The added sensor.
     */
    Sensor addLatencySensor(String scopeName, String entityName, String operationName, String... tags);

    /**
     * Record the given latency value of the sensor.
     */
    void recordLatency(Sensor sensor, long startNs, long endNs);
}
