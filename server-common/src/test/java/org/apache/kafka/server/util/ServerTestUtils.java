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

package org.apache.kafka.server.util;

import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;

public final class ServerTestUtils {

    /**
     * Clear all the yammer metrics.
     */
    public static void clearYammerMetrics() {
        KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().forEach(
                metricName -> KafkaYammerMetrics.defaultRegistry().removeMetric(metricName));
    }

    /**
     * Fetch the gauge value from the yammer metrics.
     *
     * @param name The name of the metric.
     * @return The gauge value as a number.
     */
    public static Number yammerMetricValue(String name) {
        Gauge<?> gauge = (Gauge<?>) KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(e -> e.getKey().getMBeanName().contains(name))
            .findFirst()
            .orElseThrow()
            .getValue();
        return (Number) gauge.value();
    }

}
