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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MetricNamingConventionTest {

    private MetricNamingStrategy<MetricName> metricNamingStrategy;

    @BeforeEach
    public void setUp() {
        metricNamingStrategy = MetricNamingConvention
            .getClientTelemetryMetricNamingStrategy("org.apache.kafka");
    }

    @Test
    public void testMetricKey() {
        MetricName metricName = new MetricName("name", "group", "description",
            Collections.emptyMap());
        MetricKey metricKey = metricNamingStrategy.metricKey(metricName);

        assertEquals("org.apache.kafka.group.name", metricKey.getName());
        assertEquals(Collections.emptyMap(), metricKey.tags());
    }

    @Test
    public void testMetricKeyWithHyphenNameAndTags() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        tags.put("tag2", "value2");

        MetricName metricName = new MetricName("test-name", "group-name", "description", tags);
        MetricKey metricKey = metricNamingStrategy.metricKey(metricName);

        assertEquals("org.apache.kafka.group.name.test.name", metricKey.getName());
        assertEquals(tags, metricKey.tags());
    }

    /**
     * Test metric key with mixed name and mixed tags where mixed represents combination of upper case,
     * lower case, numbers, hyphen, dot, underscore and special characters.
     */
    @Test
    public void testMetricKeyWithMixedNameAndMixedTags() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1-Ab-2_(", "value1");
        tags.put("tag2-HELLO.@", "value2");

        MetricName metricName = new MetricName("test-Name-1.$", "grouP-name-AB_&", "description", tags);
        MetricKey metricKey = metricNamingStrategy.metricKey(metricName);

        tags.clear();
        tags.put("tag1_ab_2_(", "value1");
        tags.put("tag2_hello.@", "value2");

        assertEquals("org.apache.kafka.group.name.ab_&.test.name.1.$", metricKey.getName());
        assertEquals(tags, metricKey.tags());
    }

    @Test
    public void testMetricKeyWithNullMetricName() {
        Exception e = assertThrows(NullPointerException.class, () -> metricNamingStrategy.metricKey(null));
        assertEquals("metric name cannot be null", e.getMessage());
    }

    @Test
    public void testMetricKeyWithEmptyName() {
        MetricName metricName = new MetricName("", "group-1A", "description",
            Collections.emptyMap());
        MetricKey metricKey = metricNamingStrategy.metricKey(metricName);

        // If there is no name, then the telemetry metric name will have dot in the end though
        // metric names always have a name.
        assertEquals("org.apache.kafka.group.1a.", metricKey.getName());
        assertEquals(Collections.emptyMap(), metricKey.tags());
    }

    @Test
    public void testMetricKeyWithEmptyGroup() {
        MetricName metricName = new MetricName("name", "", "description",
            Collections.emptyMap());
        MetricKey metricKey = metricNamingStrategy.metricKey(metricName);

        // If there is no group, then the telemetry metric name will have consecutive dots, though
        // metric names always have group name.
        assertEquals("org.apache.kafka..name", metricKey.getName());
        assertEquals(Collections.emptyMap(), metricKey.tags());
    }

    @Test
    public void testMetricKeyWithAdditionalMetricsSuffixInGroup() {
        MetricName metricName = new MetricName("name", "group-metrics", "description",
            Collections.emptyMap());
        MetricKey metricKey = metricNamingStrategy.metricKey(metricName);

        // '-metrics' gets removed from the group name.
        assertEquals("org.apache.kafka.group.name", metricKey.getName());
        assertEquals(Collections.emptyMap(), metricKey.tags());
    }

    @Test
    public void testMetricKeyWithMultipleMetricsSuffixInGroup() {
        MetricName metricName = new MetricName("name-metrics", "group-metrics-metrics", "description",
            Collections.emptyMap());
        MetricKey metricKey = metricNamingStrategy.metricKey(metricName);

        // '-metrics' gets removed from the group name.
        assertEquals("org.apache.kafka.group.name.metrics", metricKey.getName());
        assertEquals(Collections.emptyMap(), metricKey.tags());
    }

    @Test
    public void testMetricKeyWithNullTagKey() {
        MetricName metricName = new MetricName("name", "group", "description",
            Collections.singletonMap(null, "value1"));
        Exception e = assertThrows(NullPointerException.class, () -> metricNamingStrategy.metricKey(metricName));
        assertEquals("metric data cannot be null", e.getMessage());
    }

    @Test
    public void testMetricKeyWithBlankTagKey() {
        MetricName metricName = new MetricName("name", "group", "description",
            Collections.singletonMap("", "value1"));
        MetricKey metricKey = metricNamingStrategy.metricKey(metricName);

        assertEquals("org.apache.kafka.group.name", metricKey.getName());
        assertEquals(Collections.singletonMap("", "value1"), metricKey.tags());
    }

    @Test
    public void testDerivedMetricKey() {
        MetricName metricName = new MetricName("name", "group", "description",
            Collections.emptyMap());
        MetricKey metricKey = metricNamingStrategy.derivedMetricKey(
            metricNamingStrategy.metricKey(metricName), "delta");

        assertEquals("org.apache.kafka.group.name.delta", metricKey.getName());
        assertEquals(Collections.emptyMap(), metricKey.tags());
    }

    @Test
    public void testDerivedMetricKeyWithTags() {
        MetricName metricName = new MetricName("name", "group", "description",
            Collections.singletonMap("tag1", "value1"));
        MetricKey metricKey = metricNamingStrategy.derivedMetricKey(
            metricNamingStrategy.metricKey(metricName), "delta");

        assertEquals("org.apache.kafka.group.name.delta", metricKey.getName());
        assertEquals(Collections.singletonMap("tag1", "value1"), metricKey.tags());
    }

    @Test
    public void testDerivedMetricKeyWithNullComponent() {
        MetricName metricName = new MetricName("name", "group", "description",
            Collections.emptyMap());
        Exception e = assertThrows(NullPointerException.class, () -> metricNamingStrategy.derivedMetricKey(
            metricNamingStrategy.metricKey(metricName), null));
        assertEquals("derived component cannot be null", e.getMessage());
    }

    @Test
    public void testDerivedMetricKeyWithBlankComponent() {
        MetricName metricName = new MetricName("name", "group", "description",
            Collections.emptyMap());
        MetricKey metricKey = metricNamingStrategy.derivedMetricKey(
            metricNamingStrategy.metricKey(metricName), "");

        // Ends with dot, though derived component should
        assertEquals("org.apache.kafka.group.name.", metricKey.getName());
        assertEquals(Collections.emptyMap(), metricKey.tags());
    }
}
