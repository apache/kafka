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
package org.apache.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public final class StreamsTestUtils {
    private StreamsTestUtils() {}

    public static Properties getStreamsConfig(final String applicationId,
                                              final String bootstrapServers,
                                              final String keySerdeClassName,
                                              final String valueSerdeClassName,
                                              final Properties additional) {

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerdeClassName);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerdeClassName);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.putAll(additional);
        return streamsConfiguration;

    }

    public static Properties minimalStreamsConfig() {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "anyserver:9092");
        return properties;
    }

    public static <K, V> List<KeyValue<K, V>> toList(final Iterator<KeyValue<K, V>> iterator) {
        final List<KeyValue<K, V>> results = new ArrayList<>();

        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }

    public static <K> void verifyKeyValueList(final List<KeyValue<K, byte[]>> expected, final List<KeyValue<K, byte[]>> actual) {
        assertThat(actual.size(), equalTo(expected.size()));
        for (int i = 0; i < actual.size(); i++) {
            final KeyValue<K, byte[]> expectedKv = expected.get(i);
            final KeyValue<K, byte[]> actualKv = actual.get(i);
            assertThat(actualKv.key, equalTo(expectedKv.key));
            assertThat(actualKv.value, equalTo(expectedKv.value));
        }
    }

    public static void verifyWindowedKeyValue(final KeyValue<Windowed<Bytes>, byte[]> actual,
                                              final Windowed<Bytes> expectedKey,
                                              final String expectedValue) {
        assertThat(actual.key.window(), equalTo(expectedKey.window()));
        assertThat(actual.key.key(), equalTo(expectedKey.key()));
        assertThat(actual.value, equalTo(expectedValue.getBytes()));
    }

    public static Metric getMetricByName(final Map<MetricName, ? extends Metric> metrics,
                                         final String name,
                                         final String group) {
        Metric metric = null;
        for (final Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            if (entry.getKey().name().equals(name) && entry.getKey().group().equals(group)) {
                if (metric == null) {
                    metric = entry.getValue();
                } else {
                    throw new IllegalStateException(
                        "Found two metrics with name=[" + name + "]: \n" +
                            metric.metricName().toString() +
                            " AND \n" +
                            entry.getKey().toString()
                    );
                }
            }
        }
        if (metric == null) {
            throw new IllegalStateException("Didn't find metric with name=[" + name + "]");
        } else {
            return metric;
        }
    }

    public static Metric getMetricByNameFilterByTags(final Map<MetricName, ? extends Metric> metrics,
                                                     final String name,
                                                     final String group,
                                                     final Map<String, String> filterTags) {
        Metric metric = null;
        for (final Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            if (entry.getKey().name().equals(name) && entry.getKey().group().equals(group)) {
                boolean filtersMatch = true;
                for (final Map.Entry<String, String> filter : filterTags.entrySet()) {
                    if (!filter.getValue().equals(entry.getKey().tags().get(filter.getKey()))) {
                        filtersMatch = false;
                    }
                }
                if (filtersMatch) {
                    if (metric == null) {
                        metric = entry.getValue();
                    } else {
                        throw new IllegalStateException(
                            "Found two metrics with name=[" + name + "] and tags=[" + filterTags + "]: \n" +
                                metric.metricName().toString() +
                                " AND \n" +
                                entry.getKey().toString()
                        );
                    }
                }
            }
        }
        if (metric == null) {
            throw new IllegalStateException("Didn't find metric with name=[" + name + "] and tags=[" + filterTags + "]");
        } else {
            return metric;
        }
    }
}
