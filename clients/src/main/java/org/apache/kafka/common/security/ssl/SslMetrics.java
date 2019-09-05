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
package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SslMetrics {
    private static final String SENSOR_NAME = "ssl-cipher-counts";

    private final Metrics metrics;

    private final Sensor sensor;

    private final String metricsGroup;

    private final Map<String, CumulativeSum> cipherCounts;

    public SslMetrics(Metrics metrics, String metricsGroup) {
        this.metrics = metrics;
        this.sensor = metrics.sensor(SENSOR_NAME);
        this.metricsGroup = metricsGroup;
        this.cipherCounts = new HashMap<>();
    }

    public synchronized void addCipherUse(String cipherName) {
        CumulativeSum stat = cipherCounts.get(cipherName);
        if (stat == null) {
            stat = new CumulativeSum();
            sensor.add(createMetricName(cipherName), stat);
        }
        stat.record(1.0);
    }

    private MetricName createMetricName(String cipherName) {
        return new MetricName(
            cipherName,
            metricsGroup,
            "The number of SSL handshakes that have been completed with the " +
                cipherName + " cipher.",
            Collections.emptyMap());
    }

    public synchronized void close() {
        metrics.removeSensor(SENSOR_NAME);
    }
}
