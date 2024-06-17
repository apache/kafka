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
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class KafkaYammerMetricsTest {

    @Test
    public void testUntaggedMetric() {
        MetricName metricName = KafkaYammerMetrics.getMetricName(
            "kafka.metrics",
            "TestMetrics",
            "UntaggedMetric"
        );

        assertEquals("kafka.metrics", metricName.getGroup());
        assertEquals("TestMetrics", metricName.getType());
        assertEquals("UntaggedMetric", metricName.getName());
        assertEquals("kafka.metrics:type=TestMetrics,name=UntaggedMetric",
            metricName.getMBeanName());
        assertNull(metricName.getScope());
    }

    @Test
    public void testTaggedMetricName() {
        LinkedHashMap<String, String> tags = new LinkedHashMap<>();
        tags.put("foo", "bar");
        tags.put("bar", "baz");
        tags.put("baz", "raz.taz");

        MetricName metricName = KafkaYammerMetrics.getMetricName(
            "kafka.metrics",
            "TestMetrics",
            "TaggedMetric",
            tags
        );

        assertEquals("kafka.metrics", metricName.getGroup());
        assertEquals("TestMetrics", metricName.getType());
        assertEquals("TaggedMetric", metricName.getName());

        // MBean name should preserve initial ordering
        assertEquals("kafka.metrics:type=TestMetrics,name=TaggedMetric,foo=bar,bar=baz,baz=raz.taz",
            metricName.getMBeanName());

        // Scope should be sorted by key
        assertEquals("bar.baz.baz.raz_taz.foo.bar", metricName.getScope());
    }

    @Test
    public void testTaggedMetricNameWithEmptyValue() {
        LinkedHashMap<String, String> tags = new LinkedHashMap<>();
        tags.put("foo", "bar");
        tags.put("bar", "");
        tags.put("baz", "raz.taz");

        MetricName metricName = KafkaYammerMetrics.getMetricName(
            "kafka.metrics",
            "TestMetrics",
            "TaggedMetric",
            tags
        );

        assertEquals("kafka.metrics", metricName.getGroup());
        assertEquals("TestMetrics", metricName.getType());
        assertEquals("TaggedMetric", metricName.getName());

        // MBean name should preserve initial ordering (with empty key value removed)
        assertEquals("kafka.metrics:type=TestMetrics,name=TaggedMetric,foo=bar,baz=raz.taz",
            metricName.getMBeanName());

        // Scope should be sorted by key (with empty key value removed)
        assertEquals("baz.raz_taz.foo.bar", metricName.getScope());
    }

}