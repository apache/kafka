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
package org.apache.kafka.server.log.internals;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

public class KafkaMetricsGroup {
    private final Class<?> klass;

    public KafkaMetricsGroup(final Class<?> klass) {
        this.klass = klass;
    }

    /**
     * Creates a new MetricName object for gauges, meters, etc. created for this
     * metrics group.
     * @param name Descriptive name of the metric.
     * @param tags Additional attributes which mBean will have.
     * @return Sanitized metric name object.
     */
    public final MetricName metricName(final String name, final Map<String, String> tags) {
        final String pkg;
        if (klass.getPackage() == null) {
            pkg = "";
        } else {
            pkg = klass.getPackage().getName();
        }
        final String simpleName = klass.getSimpleName().replaceAll("\\$$", "");
        return explicitMetricName(pkg, simpleName, name, tags);
    }

    public final MetricName explicitMetricName(final String group, final String typeName,
                                               final String name, final Map<String, String> tags) {
        final StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(group);
        nameBuilder.append(":type=");
        nameBuilder.append(typeName);

        if (!name.isEmpty()) {
            nameBuilder.append(",name=");
            nameBuilder.append(name);
        }

        final String scope = toScope(tags).orElse(null);
        final Optional<String> tagsName = toMBeanName(tags);
        tagsName.ifPresent(s -> nameBuilder.append(",").append(s));

        return new MetricName(group, typeName, name, scope, nameBuilder.toString());
    }

    public final <T> Gauge<T> newGauge(final String name, final Gauge<T> metric, final Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newGauge(metricName(name, tags), metric);
    }

    public final <T> Gauge<T> newGauge(final String name, final Gauge<T> metric) {
        return newGauge(name, metric, Collections.emptyMap());
    }

    public final Meter newMeter(final String name, final String eventType,
                                final TimeUnit timeUnit, final Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newMeter(metricName(name, tags), eventType, timeUnit);
    }

    public final Meter newMeter(final String name, final String eventType,
                                final TimeUnit timeUnit) {
        return newMeter(name, eventType, timeUnit, Collections.emptyMap());
    }

    public final Meter newMeter(final MetricName metricName, final String eventType, final TimeUnit timeUnit) {
        return KafkaYammerMetrics.defaultRegistry().newMeter(metricName, eventType, timeUnit);
    }

    public final Histogram newHistogram(final String name, final boolean biased, final Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newHistogram(metricName(name, tags), biased);
    }

    public final Histogram newHistogram(final String name) {
        return newHistogram(name, true, Collections.emptyMap());
    }

    public final Histogram newHistogram(final String name, final boolean biased) {
        return newHistogram(name, biased, Collections.emptyMap());
    }

    public final Histogram newHistogram(final String name, final Map<String, String> tags) {
        return newHistogram(name, true, tags);
    }

    public final Timer newTimer(final String name, final TimeUnit durationUnit, final TimeUnit rateUnit,
                                   final Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().newTimer(metricName(name, tags), durationUnit, rateUnit);
    }

    public final Timer newTimer(final String name, final TimeUnit durationUnit, final TimeUnit rateUnit) {
        return newTimer(name, durationUnit, rateUnit, Collections.emptyMap());
    }

    public final void removeMetric(final String name, final Map<String, String> tags) {
        KafkaYammerMetrics.defaultRegistry().removeMetric(metricName(name, tags));
    }

    public final void removeMetric(final String name) {
        removeMetric(name, Collections.emptyMap());
    }

    private Optional<String> toMBeanName(final Map<String, String> tags) {
        final List<Map.Entry<String, String>> filteredTags = tags.entrySet().stream()
                .filter(entry -> !entry.getValue().equals(""))
                .collect(Collectors.toList());
        if (!filteredTags.isEmpty()) {
            final String tagsString = filteredTags.stream()
                    .map(entry -> String.format("%s=%s", entry.getKey(), Sanitizer.jmxSanitize(entry.getValue())))
                    .collect(Collectors.joining("."));
            return Optional.of(tagsString);
        } else {
            return Optional.empty();
        }
    }

    private Optional<String> toScope(final Map<String, String> tags) {
        final List<Map.Entry<String, String>> filteredTags = tags.entrySet().stream()
                .filter(entry -> !entry.getValue().equals(""))
                .collect(Collectors.toList());
        if (!filteredTags.isEmpty()) {
            // convert dot to _ since reporters like Graphite typically use dot to represent hierarchy
            final String tagsString = filteredTags.stream()
                    .sorted(Map.Entry.comparingByValue())
                    .map(entry -> String.format("%s.%s", entry.getKey(), entry.getValue().replaceAll("\\.", "_")))
                    .collect(Collectors.joining("."));
            return Optional.of(tagsString);
        } else {
            return Optional.empty();
        }
    }
}
