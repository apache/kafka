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
package org.apache.kafka.server.log.remote.quota;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.server.quota.SensorAccess;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RLMQuotaMetrics {

    private final Sensor sensor;

    public RLMQuotaMetrics(Metrics metrics, String name, String group, String descriptionFormat, long expirationTime) {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        SensorAccess sensorAccess = new SensorAccess(lock, metrics);
        this.sensor = sensorAccess.getOrCreate(name, expirationTime, s -> {
            s.add(metrics.metricName(name + "-avg", group, String.format(descriptionFormat, "average")), new Avg());
            s.add(metrics.metricName(name + "-max", group, String.format(descriptionFormat, "maximum")), new Max());
        });
    }

    public Sensor sensor() {
        return sensor;
    }
}
