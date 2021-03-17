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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.kafka.common.metrics.Sensor.RecordingLevel.DEBUG;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public final class StreamsTestUtils {
    private StreamsTestUtils() {}

    public static Properties getStreamsConfig(final String applicationId,
                                              final String bootstrapServers,
                                              final String keySerdeClassName,
                                              final String valueSerdeClassName,
                                              final Properties additional) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerdeClassName);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerdeClassName);
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, DEBUG.name);
        props.putAll(additional);
        return props;
    }

    public static Properties getStreamsConfig(final Serde<?> keyDeserializer,
                                              final Serde<?> valueDeserializer) {
        return getStreamsConfig(
                UUID.randomUUID().toString(),
                "localhost:9091",
                keyDeserializer.getClass().getName(),
                valueDeserializer.getClass().getName(),
                new Properties());
    }

    public static Properties getStreamsConfig(final String applicationId) {
        return getStreamsConfig(applicationId, new Properties());
    }

    public static Properties getStreamsConfig(final String applicationId, final Properties additional) {
        return getStreamsConfig(
            applicationId,
            "localhost:9091",
            Serdes.ByteArraySerde.class.getName(),
            Serdes.ByteArraySerde.class.getName(),
            additional);
    }

    public static Properties getStreamsConfig() {
        return getStreamsConfig(UUID.randomUUID().toString());
    }

    public static void startKafkaStreamsAndWaitForRunningState(final KafkaStreams kafkaStreams) throws InterruptedException {
        startKafkaStreamsAndWaitForRunningState(kafkaStreams, DEFAULT_MAX_WAIT_MS);
    }

    public static void startKafkaStreamsAndWaitForRunningState(final KafkaStreams kafkaStreams,
                                                               final long timeoutMs) throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                countDownLatch.countDown();
            }
        });

        kafkaStreams.start();
        assertThat(
            "KafkaStreams did not transit to RUNNING state within " + timeoutMs + " milli seconds.",
            countDownLatch.await(timeoutMs, TimeUnit.MILLISECONDS),
            equalTo(true)
        );
    }

    public static <K, V> List<KeyValue<K, V>> toList(final Iterator<KeyValue<K, V>> iterator) {
        final List<KeyValue<K, V>> results = new ArrayList<>();

        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }

    public static <K, V> Set<KeyValue<K, V>> toSet(final Iterator<KeyValue<K, V>> iterator) {
        final Set<KeyValue<K, V>> results = new LinkedHashSet<>();

        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }

    public static <K, V> Set<V> valuesToSet(final Iterator<KeyValue<K, V>> iterator) {
        final Set<V> results = new HashSet<>();

        while (iterator.hasNext()) {
            results.add(iterator.next().value);
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

    public static boolean containsMetric(final Metrics metrics,
                                         final String name,
                                         final String group,
                                         final Map<String, String> tags) {
        final MetricName metricName = metrics.metricName(name, group, tags);
        return metrics.metric(metricName) != null;
    }

    /**
     * Used to keep tests simple, and ignore calls from {@link org.apache.kafka.streams.internals.ApiUtils#checkSupplier(Supplier)} )}.
     * @return true if the the stack context is within a {@link org.apache.kafka.streams.internals.ApiUtils#checkSupplier(Supplier)} )} call
     */
    public static boolean isCheckSupplierCall() {
        return Arrays.stream(Thread.currentThread().getStackTrace())
                .anyMatch(caller -> "org.apache.kafka.streams.internals.ApiUtils".equals(caller.getClassName()) && "checkSupplier".equals(caller.getMethodName()));
    }
}
