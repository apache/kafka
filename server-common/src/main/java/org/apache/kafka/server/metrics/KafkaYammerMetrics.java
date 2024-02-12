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

import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Sanitizer;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This class encapsulates the default yammer metrics registry for Kafka server,
 * and configures the set of exported JMX metrics for Yammer metrics.
 * <br>
 * KafkaYammerMetrics.defaultRegistry() should always be used instead of Metrics.defaultRegistry()
 */
public class KafkaYammerMetrics implements Reconfigurable {

    public static final KafkaYammerMetrics INSTANCE = new KafkaYammerMetrics();

    /**
     * convenience method to replace {@link com.yammer.metrics.Metrics#defaultRegistry()}
     */
    public static MetricsRegistry defaultRegistry() {
        return INSTANCE.metricsRegistry;
    }

    private final MetricsRegistry metricsRegistry = new MetricsRegistry();
    private final FilteringJmxReporter jmxReporter = new FilteringJmxReporter(metricsRegistry,
        metricName -> true);

    private KafkaYammerMetrics() {
        jmxReporter.start();
        Exit.addShutdownHook("kafka-jmx-shutdown-hook", jmxReporter::shutdown);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        reconfigure(configs);
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return JmxReporter.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        JmxReporter.compilePredicate(configs);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        Predicate<String> mBeanPredicate = JmxReporter.compilePredicate(configs);
        jmxReporter.updatePredicate(metricName -> mBeanPredicate.test(metricName.getMBeanName()));
    }

    public static MetricName getMetricName(
        String group,
        String typeName,
        String name
    ) {
        return getMetricName(
            group,
            typeName,
            name,
            null
        );
    }

    public static MetricName getMetricName(
        String group,
        String typeName,
        String name,
        LinkedHashMap<String, String> tags
    ) {
        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(group);
        nameBuilder.append(":type=");
        nameBuilder.append(typeName);

        if (name.length() > 0) {
            nameBuilder.append(",name=");
            nameBuilder.append(name);
        }

        String scope = toScope(tags).orElse(null);
        Optional<String> tagsName = toMBeanName(tags);
        tagsName.ifPresent(nameBuilder::append);

        return new MetricName(group, typeName, name, scope, nameBuilder.toString());
    }

    private static Optional<String> toMBeanName(LinkedHashMap<String, String> tags) {
        if (tags == null) {
            return Optional.empty();
        }

        LinkedHashMap<String, String> nonEmptyTags = collectNonEmptyTags(tags, LinkedHashMap::new);
        if (nonEmptyTags.isEmpty()) {
            return Optional.empty();
        } else {
            StringBuilder tagsString = new StringBuilder();
            for (Map.Entry<String, String> tagEntry : nonEmptyTags.entrySet()) {
                String sanitizedValue = Sanitizer.jmxSanitize(tagEntry.getValue());
                tagsString.append(",");
                tagsString.append(tagEntry.getKey());
                tagsString.append("=");
                tagsString.append(sanitizedValue);
            }
            return Optional.of(tagsString.toString());
        }
    }

    private static <T extends Map<String, String>> T collectNonEmptyTags(
        Map<String, String> tags,
        Supplier<T> mapSupplier
    ) {
        T result = mapSupplier.get();
        for (Map.Entry<String, String> tagEntry : tags.entrySet()) {
            String tagValue = tagEntry.getValue();
            if (!"".equals(tagValue)) {
                result.put(tagEntry.getKey(), tagValue);
            }
        }
        return result;
    }

    private static Optional<String> toScope(Map<String, String> tags) {
        if (tags == null) {
            return Optional.empty();
        }

        SortedMap<String, String> nonEmptyTags = collectNonEmptyTags(tags, TreeMap::new);
        if (nonEmptyTags.isEmpty()) {
            return Optional.empty();
        } else {
            StringBuilder tagsString = new StringBuilder();

            for (Iterator<Map.Entry<String, String>> iterator = nonEmptyTags.entrySet().iterator(); iterator.hasNext();) {
                // convert dot to _ since reporters like Graphite typically use dot to represent hierarchy
                Map.Entry<String, String> tagEntry = iterator.next();
                String convertedValue = tagEntry.getValue().replaceAll("\\.", "_");
                tagsString.append(tagEntry.getKey());
                tagsString.append(".");
                tagsString.append(convertedValue);

                if (iterator.hasNext()) {
                    tagsString.append(".");
                }
            }
            return Optional.of(tagsString.toString());
        }
    }
}
