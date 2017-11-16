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

import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroupId;
import org.apache.kafka.connect.util.MockTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class ConnectMetricsTest {

    private static final Map<String, String> DEFAULT_WORKER_CONFIG = new HashMap<>();

    static {
        DEFAULT_WORKER_CONFIG.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_WORKER_CONFIG.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_WORKER_CONFIG.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_WORKER_CONFIG.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    }

    private ConnectMetrics metrics;

    @Before
    public void setUp() {
        metrics = new ConnectMetrics("worker1", new WorkerConfig(WorkerConfig.baseConfigDef(), DEFAULT_WORKER_CONFIG), new MockTime());
    }

    @After
    public void tearDown() {
        if (metrics != null)
            metrics.stop();
    }

    @Test
    public void testKafkaMetricsNotNull() {
        assertNotNull(metrics.metrics());
    }

    @Test
    public void testCreatingTags() {
        Map<String, String> tags = ConnectMetrics.tags("k1", "v1", "k2", "v2");
        assertEquals("v1", tags.get("k1"));
        assertEquals("v2", tags.get("k2"));
        assertEquals(2, tags.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatingTagsWithOddNumberOfTags() {
        ConnectMetrics.tags("k1", "v1", "k2", "v2", "extra");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGettingGroupWithOddNumberOfTags() {
        metrics.group("name", "k1", "v1", "k2", "v2", "extra");
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
}