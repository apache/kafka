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

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.Sensor;

/**
 * The Kafka Streams metrics interface for adding metric sensors and collecting metric values.
 */
public interface ThreadCacheMetrics {

    /**
     * Add the hit ratio sensor.
     * @param entityName Name of the entity, could be the name of the cache instance, etc.
     * @param operationName Name of the operation, could be "hit ratio".
     * @param tags Additional tags of the sensor.
     * @return The added sensor.
     */
    Sensor addCacheSensor(String entityName, String operationName, String... tags);

    /**
     * Record the given value of the sensor.
     */
    void recordCacheSensor(Sensor sensor, double value);
}
