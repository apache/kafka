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
package org.apache.kafka.common.telemetry.internals;

import org.apache.kafka.common.MetricName;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This class encapsulates naming and mapping conventions defined as part of
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability#KIP714:Clientmetricsandobservability-Metricsnamingandformat">Metrics naming and format</a>
 */
public class TelemetryMetricNamingConvention {

    private static final String NAME_JOINER = ".";
    private static final String TAG_JOINER = "_";

    // remove metrics as it is redundant for telemetry metrics naming convention
    private final static Pattern GROUP_PATTERN = Pattern.compile("\\.(metrics)");

    public static MetricNamingStrategy<MetricName> getClientTelemetryMetricNamingStrategy(String prefix) {
        Objects.requireNonNull(prefix, "prefix cannot be null");

        return new MetricNamingStrategy<MetricName>() {
            @Override
            public MetricKey metricKey(MetricName metricName) {
                Objects.requireNonNull(metricName, "metric name cannot be null");

                return new MetricKey(fullMetricName(prefix, metricName.group(), metricName.name()),
                    Collections.unmodifiableMap(cleanTags(metricName.tags())));
            }

            @Override
            public MetricKey derivedMetricKey(MetricKey key, String derivedComponent) {
                Objects.requireNonNull(derivedComponent, "derived component cannot be null");
                return new MetricKey(key.name() + NAME_JOINER + derivedComponent, key.tags());
            }
        };
    }

    /**
     * Creates a metric name from the given prefix, group, and name. The new String follows the following
     * conventions and rules:
     *
     * <ul>
     *   <li>prefix is expected to be a host-name like value, e.g. {@code org.apache.kafka}</li>
     *   <li>group is cleaned of redundant words: "-metrics"</li>
     *   <li>the group and metric name is dot separated</li>
     *   <li>The name is created by joining the three components, e.g.:
     *     {@code org.apache.kafka.producer.connection.creation.rate}</li>
     * </ul>
     */
    private static String fullMetricName(String prefix, String group, String name) {
        return prefix
            + NAME_JOINER
            + cleanGroup(group)
            + NAME_JOINER
            + cleanMetric(name);
    }

    /**
     * This method maps a group name to follow conventions and cleans up the result to be more legible:
     * <ul>
     *  <li> converts names to lower case conventions
     *  <li> normalizes artifacts of hyphen case in group name to dot separated conversion
     *  <li> strips redundant parts of the metric name, such as -metrics
     * </ul>
     */
    private static String cleanGroup(String group) {
        group = clean(group, NAME_JOINER);
        return GROUP_PATTERN.matcher(group).replaceAll("");
    }

    /**
     * This method maps a metric name to follow conventions and cleans up the result to be more legible:
     * <ul>
     *  <li> converts names to lower case conventions
     *  <li> normalizes artifacts of hyphen case in metric name to dot separated conversion
     * </ul>
     */
    private static String cleanMetric(String metric) {
        return clean(metric, NAME_JOINER);
    }

    /**
     * Converts a tag name to match the telemetry naming conventions by converting into snake_case.
     * <p>
     * Kafka metrics have tags name in lower case separated by hyphens. Eg: total-errors
     *
     * @param raw the input map
     * @return the new map with keys replaced by snake_case representations.
     */
    private static Map<String, String> cleanTags(Map<String, String> raw) {
        return raw.entrySet()
            .stream()
            .collect(Collectors.toMap(s -> clean(s.getKey(), TAG_JOINER), Entry::getValue));
    }

    private static String clean(String raw, String joiner) {
        Objects.requireNonNull(raw, "metric data cannot be null");
        String lowerCase = raw.toLowerCase(Locale.ROOT);
        return lowerCase.replaceAll("-", joiner);
    }
}
