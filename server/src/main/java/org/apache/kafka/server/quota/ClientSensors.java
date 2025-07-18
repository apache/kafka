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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.metrics.Sensor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the sensors aggregated per client
 */
public final class ClientSensors {
    private final Map<String, String> metricTags;
    private final Sensor quotaSensor;
    private final Sensor throttleTimeSensor;

    /**
     * @param metricTags quota metric tags for the client
     * @param quotaSensor sensor that tracks the quota
     * @param throttleTimeSensor sensor that tracks the throttle time
     */
    public ClientSensors(Map<String, String> metricTags,
                         Sensor quotaSensor,
                         Sensor throttleTimeSensor) {
        this.metricTags = new LinkedHashMap<>(metricTags);
        this.quotaSensor = Objects.requireNonNull(quotaSensor);
        this.throttleTimeSensor = Objects.requireNonNull(throttleTimeSensor);
    }

    public Map<String, String> metricTags() {
        return metricTags;
    }

    public Sensor quotaSensor() {
        return quotaSensor;
    }

    public Sensor throttleTimeSensor() {
        return throttleTimeSensor;
    }

    @Override
    public String toString() {
        return "ClientSensors{" +
                "metricTags=" + metricTags +
                ", quotaSensor=" + quotaSensor +
                ", throttleTimeSensor=" + throttleTimeSensor +
                '}';
    }
}