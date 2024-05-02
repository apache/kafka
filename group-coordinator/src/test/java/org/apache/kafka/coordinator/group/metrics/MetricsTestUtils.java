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
package org.apache.kafka.coordinator.group.metrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;

import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetricsTestUtils {
    static void assertMetricsForTypeEqual(
        MetricsRegistry registry,
        String expectedPrefix,
        Set<String> expected
    ) {
        Set<String> actual = new TreeSet<>();
        registry.allMetrics().forEach((name, __) -> {
            StringBuilder bld = new StringBuilder();
            bld.append(name.getGroup());
            bld.append(":type=").append(name.getType());
            bld.append(",name=").append(name.getName());
            if (bld.toString().startsWith(expectedPrefix)) {
                actual.add(bld.toString());
            }
        });
        assertEquals(new TreeSet<>(expected), actual);
    }

    static void assertGaugeValue(MetricsRegistry registry, com.yammer.metrics.core.MetricName metricName, long count) {
        Gauge gauge = (Gauge) registry.allMetrics().get(metricName);

        assertEquals(count, (long) gauge.value());
    }

    static void assertGaugeValue(Metrics metrics, MetricName metricName, long count) {
        Long metricVal = (Long) metrics.metrics().get(metricName).metricValue();
        assertEquals(count, metricVal);
    }

    static com.yammer.metrics.core.MetricName metricName(String type, String name) {
        String mBeanName = String.format("kafka.coordinator.group:type=%s,name=%s", type, name);
        return new com.yammer.metrics.core.MetricName("kafka.coordinator.group", type, name, null, mBeanName);
    }
}
