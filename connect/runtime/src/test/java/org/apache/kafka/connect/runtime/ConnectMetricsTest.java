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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Monitorable;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroupId;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectMetricsTest {

    private static final Map<String, String> DEFAULT_WORKER_CONFIG = Map.of(
        WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter",
        WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    private static final ConnectorTaskId CONNECTOR_TASK_ID = new ConnectorTaskId("connector", 0);
    private static final Map<String, String> TAGS = Map.of("t1", "v1");

    private ConnectMetrics metrics;

    @BeforeEach
    public void setUp() {
        metrics = new ConnectMetrics("worker1", new WorkerConfig(WorkerConfig.baseConfigDef(), DEFAULT_WORKER_CONFIG), new MockTime(), "cluster-1");
    }

    @AfterEach
    public void tearDown() {
        if (metrics != null)
            metrics.stop();
    }

    @Test
    public void testKafkaMetricsNotNull() {
        assertNotNull(metrics.metrics());
    }

    @Test
    public void testGettingGroupWithOddNumberOfTags() {
        assertThrows(IllegalArgumentException.class,
            () -> metrics.group("name", "k1", "v1", "k2", "v2", "extra"));
    }

    @Test
    public void testGettingGroupWithTags() {
        MetricGroup group1 = metrics.group("name", "k1", "v1", "k2", "v2");
        assertEquals("v1", group1.tags().get("k1"));
        assertEquals("v2", group1.tags().get("k2"));
        assertEquals(2, group1.tags().size());
    }

    @Test
    public void testGettingGroupMultipleTimes() {
        MetricGroup group1 = metrics.group("name");
        MetricGroup group2 = metrics.group("name");
        assertNotNull(group1);
        assertSame(group1, group2);
        MetricGroup group3 = metrics.group("other");
        assertNotNull(group3);
        assertNotSame(group1, group3);

        // Now with tags
        MetricGroup group4 = metrics.group("name", "k1", "v1");
        assertNotNull(group4);
        assertNotSame(group1, group4);
        assertNotSame(group2, group4);
        assertNotSame(group3, group4);
        MetricGroup group5 = metrics.group("name", "k1", "v1");
        assertSame(group4, group5);
    }

    @Test
    public void testMetricGroupIdIdentity() {
        MetricGroupId id1 = metrics.groupId("name", "k1", "v1");
        MetricGroupId id2 = metrics.groupId("name", "k1", "v1");
        MetricGroupId id3 = metrics.groupId("name", "k1", "v1", "k2", "v2");

        assertEquals(id1.hashCode(), id2.hashCode());
        assertEquals(id1, id2);
        assertEquals(id1.toString(), id2.toString());
        assertEquals(id1.groupName(), id2.groupName());
        assertEquals(id1.tags(), id2.tags());
        assertNotNull(id1.tags());

        assertNotEquals(id1, id3);
    }

    @Test
    public void testMetricGroupIdWithoutTags() {
        MetricGroupId id1 = metrics.groupId("name");
        MetricGroupId id2 = metrics.groupId("name");

        assertEquals(id1.hashCode(), id2.hashCode());
        assertEquals(id1, id2);
        assertEquals(id1.toString(), id2.toString());
        assertEquals(id1.groupName(), id2.groupName());
        assertEquals(id1.tags(), id2.tags());
        assertNotNull(id1.tags());
        assertNotNull(id2.tags());
    }

    @Test
    public void testRecreateWithClose() {
        final Sensor originalSensor = addToGroup(metrics, false);
        final Sensor recreatedSensor = addToGroup(metrics, true);
        // because we closed the metricGroup, we get a brand-new sensor
        assertNotSame(originalSensor, recreatedSensor);
    }

    @Test
    public void testRecreateWithoutClose() {
        final Sensor originalSensor = addToGroup(metrics, false);
        final Sensor recreatedSensor = addToGroup(metrics, false);
        // since we didn't close the group, the second addToGroup is idempotent
        assertSame(originalSensor, recreatedSensor);
    }

    @Test
    public void testMetricReporters() {
        assertEquals(1, metrics.metrics().reporters().size());
    }

    @Test
    public void testDisableJmxReporter() {
        Map<String, String> props = new HashMap<>(DEFAULT_WORKER_CONFIG);
        props.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, "");
        ConnectMetrics cm = new ConnectMetrics("worker-testDisableJmxReporter", new WorkerConfig(WorkerConfig.baseConfigDef(), props), new MockTime(), "cluster-1");
        assertTrue(cm.metrics().reporters().isEmpty());
        cm.stop();
    }

    @Test
    public void testExplicitlyEnableJmxReporter() {
        Map<String, String> props = new HashMap<>(DEFAULT_WORKER_CONFIG);
        props.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, "org.apache.kafka.common.metrics.JmxReporter");
        ConnectMetrics cm = new ConnectMetrics("worker-testExplicitlyEnableJmxReporter", new WorkerConfig(WorkerConfig.baseConfigDef(), props), new MockTime(), "cluster-1");
        assertEquals(1, cm.metrics().reporters().size());
        cm.stop();
    }

    @Test
    public void testConnectorPluginMetrics() throws Exception {
        try (PluginMetricsImpl pluginMetrics = metrics.connectorPluginMetrics(CONNECTOR_TASK_ID.connector())) {
            MetricName metricName = pluginMetrics.metricName("name", "description", TAGS);
            Map<String, String> expectedTags = new LinkedHashMap<>();
            expectedTags.put("connector", CONNECTOR_TASK_ID.connector());
            expectedTags.putAll(TAGS);
            assertEquals(expectedTags, metricName.tags());
        }
    }

    @Test
    public void testTaskPluginMetrics() throws Exception {
        try (PluginMetricsImpl pluginMetrics = metrics.taskPluginMetrics(CONNECTOR_TASK_ID)) {
            MetricName metricName = pluginMetrics.metricName("name", "description", TAGS);
            Map<String, String> expectedTags = new LinkedHashMap<>();
            expectedTags.put("connector", CONNECTOR_TASK_ID.connector());
            expectedTags.put("task", String.valueOf(CONNECTOR_TASK_ID.task()));
            expectedTags.putAll(TAGS);
            assertEquals(expectedTags, metricName.tags());
        }
    }

    static final class MonitorableConverter implements Converter, HeaderConverter, Monitorable {

        private int calls = 0;
        private PluginMetrics pluginMetrics = null;
        private MetricName metricName = null;

        @Override
        public void withPluginMetrics(PluginMetrics pluginMetrics) {
            this.pluginMetrics = pluginMetrics;
            metricName = pluginMetrics.metricName("name", "description", TAGS);
            pluginMetrics.addMetric(metricName, (Measurable) (config, now) -> calls);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) { }

        @Override
        public byte[] fromConnectData(String topic, Schema schema, Object value) {
            calls++;
            return new byte[0];
        }

        @Override
        public SchemaAndValue toConnectData(String topic, byte[] value) {
            calls++;
            return null;
        }

        @Override
        public ConfigDef config() {
            return Converter.super.config();
        }

        @Override
        public void configure(Map<String, ?> configs) { }

        @Override
        public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
            calls++;
            return null;
        }

        @Override
        public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
            calls++;
            return new byte[0];
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testWrapConverter(boolean isKey) throws IOException {
        try (MonitorableConverter converter = new MonitorableConverter()) {
            metrics.wrap(converter, CONNECTOR_TASK_ID, isKey);
            assertNotNull(converter.pluginMetrics);
            MetricName metricName = converter.metricName;
            Map<String, String> expectedTags = new LinkedHashMap<>();
            expectedTags.put("connector", CONNECTOR_TASK_ID.connector());
            expectedTags.put("task", String.valueOf(CONNECTOR_TASK_ID.task()));
            expectedTags.put("converter", isKey ? "key" : "value");
            expectedTags.putAll(TAGS);
            assertEquals(expectedTags, metricName.tags());
            KafkaMetric metric = metrics.metrics().metrics().get(metricName);
            assertEquals(0.0, (double) metric.metricValue());
            converter.toConnectData("topic", new byte[]{});
            assertEquals(1.0, (double) metric.metricValue());
            converter.fromConnectData("topic", null, null);
            assertEquals(2.0, (double) metric.metricValue());
        }
    }

    @Test
    public void testWrapHeaderConverter() throws IOException {
        try (MonitorableConverter converter = new MonitorableConverter()) {
            metrics.wrap(converter, CONNECTOR_TASK_ID);
            assertNotNull(converter.pluginMetrics);
            MetricName metricName = converter.metricName;
            Map<String, String> expectedTags = new LinkedHashMap<>();
            expectedTags.put("connector", CONNECTOR_TASK_ID.connector());
            expectedTags.put("task", String.valueOf(CONNECTOR_TASK_ID.task()));
            expectedTags.put("converter", "header");
            expectedTags.putAll(TAGS);
            assertEquals(expectedTags, metricName.tags());
            KafkaMetric metric = metrics.metrics().metrics().get(metricName);
            assertEquals(0.0, (double) metric.metricValue());
            converter.toConnectHeader("topic", "header", new byte[]{});
            assertEquals(1.0, (double) metric.metricValue());
            converter.fromConnectHeader("topic", "header", null, null);
            assertEquals(2.0, (double) metric.metricValue());
        }
    }

    static final class MonitorableTransformation implements Transformation<SourceRecord>, Monitorable {

        private int calls = 0;
        private PluginMetrics pluginMetrics = null;
        private MetricName metricName = null;

        @Override
        public void withPluginMetrics(PluginMetrics pluginMetrics) {
            this.pluginMetrics = pluginMetrics;
            metricName = pluginMetrics.metricName("name", "description", TAGS);
            pluginMetrics.addMetric(metricName, (Measurable) (config, now) -> calls);
        }

        @Override
        public void configure(Map<String, ?> configs) { }

        @Override
        public SourceRecord apply(SourceRecord record) {
            calls++;
            return null;
        }

        @Override
        public ConfigDef config() {
            return null;
        }

        @Override
        public void close() { }
    }

    @Test
    public void testWrapTransformation() {
        try (MonitorableTransformation transformation = new MonitorableTransformation()) {
            metrics.wrap(transformation, CONNECTOR_TASK_ID, "alias");
            assertNotNull(transformation.pluginMetrics);
            MetricName metricName = transformation.metricName;
            Map<String, String> expectedTags = new LinkedHashMap<>();
            expectedTags.put("connector", CONNECTOR_TASK_ID.connector());
            expectedTags.put("task", String.valueOf(CONNECTOR_TASK_ID.task()));
            expectedTags.put("transformation", "alias");
            expectedTags.putAll(TAGS);
            assertEquals(expectedTags, metricName.tags());
            KafkaMetric metric = metrics.metrics().metrics().get(metricName);
            assertEquals(0.0, (double) metric.metricValue());
            transformation.apply(null);
            assertEquals(1.0, (double) metric.metricValue());
        }
    }

    static final class MonitorablePredicate implements Predicate<SourceRecord>, Monitorable {

        private int calls = 0;
        private PluginMetrics pluginMetrics = null;
        private MetricName metricName = null;

        @Override
        public void withPluginMetrics(PluginMetrics pluginMetrics) {
            this.pluginMetrics = pluginMetrics;
            metricName = pluginMetrics.metricName("name", "description", TAGS);
            pluginMetrics.addMetric(metricName, (Measurable) (config, now) -> calls);
        }

        @Override
        public void configure(Map<String, ?> configs) { }

        @Override
        public ConfigDef config() {
            return null;
        }

        @Override
        public boolean test(SourceRecord record) {
            calls++;
            return false;
        }

        @Override
        public void close() { }
    }

    @Test
    public void testWrapPredicate() {
        try (MonitorablePredicate predicate = new MonitorablePredicate()) {
            metrics.wrap(predicate, CONNECTOR_TASK_ID, "alias");
            assertNotNull(predicate.pluginMetrics);
            MetricName metricName = predicate.metricName;
            Map<String, String> expectedTags = new LinkedHashMap<>();
            expectedTags.put("connector", CONNECTOR_TASK_ID.connector());
            expectedTags.put("task", String.valueOf(CONNECTOR_TASK_ID.task()));
            expectedTags.put("predicate", "alias");
            expectedTags.putAll(TAGS);
            assertEquals(expectedTags, metricName.tags());
            KafkaMetric metric = metrics.metrics().metrics().get(metricName);
            assertEquals(0.0, (double) metric.metricValue());
            predicate.test(null);
            assertEquals(1.0, (double) metric.metricValue());
        }
    }

    private Sensor addToGroup(ConnectMetrics connectMetrics, boolean shouldClose) {
        ConnectMetricsRegistry registry = connectMetrics.registry();
        ConnectMetrics.MetricGroup metricGroup = connectMetrics.group(registry.taskGroupName(),
                registry.connectorTagName(), "conn_name");

        if (shouldClose) {
            metricGroup.close();
        }

        Sensor sensor = metricGroup.sensor("my_sensor");
        sensor.add(metricName("x1"), new Max());
        sensor.add(metricName("y2"), new Avg());

        return sensor;
    }

    static MetricName metricName(String name) {
        return new MetricName(name, "test_group", "metrics for testing", Map.of());
    }
}
