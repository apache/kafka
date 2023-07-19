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
package org.apache.kafka.server.metrics;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.kafka.common.utils.Sanitizer;

public class KafkaMetricsGroup {
    private final Class<?> klass;

    public KafkaMetricsGroup(Class<?> klass) {
        this.klass = klass;
    }

    /**
     * Creates a new MetricName object for gauges, meters, etc. created for this
     * metrics group.
     * @param name Descriptive name of the metric.
     * @param tags Additional attributes which mBean will have.
     * @return Sanitized metric name object.
     */
    public MetricName metricName(String name, Map<String, String> tags) {
        String pkg = klass.getPackage() == null ? "" : klass.getPackage().getName();
        String simpleName = klass.getSimpleName().replaceAll("\\$$", "");
        return explicitMetricName(pkg, simpleName, name, tags);
    }

    public static MetricName explicitMetricName(String group, String typeName,
                                                String name, Map<String, String> tags) {
        StringBuilder nameBuilder = new StringBuilder(100);
        nameBuilder.append(group);
        nameBuilder.append(":type=");
        nameBuilder.append(typeName);

        if (!name.isEmpty()) {
            nameBuilder.append(",name=");
            nameBuilder.append(name);
        }

        String scope = toScope(tags).orElse(null);
        Optional<String> tagsName = toMBeanName(tags);
        tagsName.ifPresent(s -> nameBuilder.append(",").append(s));

        return new MetricName(group, typeName, name, scope, nameBuilder.toString());
    }

    public final <T> Gauge<T> newGauge(String name, Gauge<T> metric, Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newGauge(metricName(name, tags), metric);
    }

    public final <T> Gauge<T> newGauge(String name, Gauge<T> metric) {
        return newGauge(name, metric, Collections.emptyMap());
    }

    public final Meter newMeter(String name, String eventType,
                                TimeUnit timeUnit, Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newMeter(metricName(name, tags), eventType, timeUnit);
    }

    public final Meter newMeter(String name, String eventType,
                                TimeUnit timeUnit) {
        return newMeter(name, eventType, timeUnit, Collections.emptyMap());
    }

    public final Meter newMeter(MetricName metricName, String eventType, TimeUnit timeUnit) {
        return KafkaYammerMetrics.defaultRegistry().newMeter(metricName, eventType, timeUnit);
    }

    public final Histogram newHistogram(String name, boolean biased, Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newHistogram(metricName(name, tags), biased);
    }

    public final Histogram newHistogram(String name) {
        return newHistogram(name, true, Collections.emptyMap());
    }

    public final Timer newTimer(String name, TimeUnit durationUnit, TimeUnit rateUnit, Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newTimer(metricName(name, tags), durationUnit, rateUnit);
    }

    public final Timer newTimer(String name, TimeUnit durationUnit, TimeUnit rateUnit) {
        return newTimer(name, durationUnit, rateUnit, Collections.emptyMap());
    }

    public final void removeMetric(String name, Map<String, String> tags) {
        KafkaYammerMetrics.defaultRegistry().removeMetric(metricName(name, tags));
    }

    public final void removeMetric(String name) {
        removeMetric(name, Collections.emptyMap());
    }

    private static Optional<String> toMBeanName(Map<String, String> tags) {
        List<Map.Entry<String, String>> filteredTags = tags.entrySet().stream()
                .filter(entry -> !entry.getValue().equals(""))
                .collect(Collectors.toList());
        if (!filteredTags.isEmpty()) {
            String tagsString = filteredTags.stream()
                    .map(entry -> entry.getKey() + "=" + Sanitizer.jmxSanitize(entry.getValue()))
                    .collect(Collectors.joining(","));
            return Optional.of(tagsString);
        } else {
            return Optional.empty();
        }
    }

    private static Optional<String> toScope(Map<String, String> tags) {
        List<Map.Entry<String, String>> filteredTags = tags.entrySet().stream()
                .filter(entry -> !entry.getValue().equals(""))
                .collect(Collectors.toList());
        if (!filteredTags.isEmpty()) {
            // convert dot to _ since reporters like Graphite typically use dot to represent hierarchy
            String tagsString = filteredTags.stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getKey() + "." + entry.getValue().replaceAll("\\.", "_"))
                    .collect(Collectors.joining("."));
            return Optional.of(tagsString);
        } else {
            return Optional.empty();
        }
    }
}
