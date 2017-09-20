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
import org.apache.kafka.connect.util.MockTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
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
        if (metrics != null) metrics.stop();
    }

    @Test
    public void testValidatingNameWithAllValidCharacters() {
        String name = "abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ-0123456789";
        assertEquals(name, ConnectMetrics.makeValidName(name));
    }

    @Test
    public void testValidatingEmptyName() {
        String name = "";
        assertSame(name, ConnectMetrics.makeValidName(name));
    }

    @Test(expected = NullPointerException.class)
    public void testValidatingNullName() {
        ConnectMetrics.makeValidName(null);
    }

    @Test
    public void testValidatingNameWithInvalidCharacters() {
        assertEquals("a-b-c-d-e-f-g-h-i-j-k", ConnectMetrics.makeValidName("a:b;c/d\\e,f*.--..;;g?h[i]j=k"));
        assertEquals("-a-b-c-d-e-f-g-h-", ConnectMetrics.makeValidName(":a:b;c/d\\e,f*g?[]=h:"));
        assertEquals("a-f-h", ConnectMetrics.makeValidName("a:;/\\,f*?h"));
    }

    @Test
    public void testKafkaMetricsNotNull() {
        assertNotNull(metrics.metrics());
    }

    @Test
    public void testCreatingTagsWithNonNullWorkerId() {
        Map<String, String> tags = ConnectMetrics.tags("name", "k1", "v1", "k2", "v2");
        assertEquals("v1", tags.get("k1"));
        assertEquals("v2", tags.get("k2"));
        assertEquals("name", tags.get(ConnectMetrics.WORKER_ID_TAG_NAME));
    }

    @Test
    public void testCreatingTagsWithNullWorkerId() {
        Map<String, String> tags = ConnectMetrics.tags(null, "k1", "v1", "k2", "v2");
        assertEquals("v1", tags.get("k1"));
        assertEquals("v2", tags.get("k2"));
        assertEquals(null, tags.get(ConnectMetrics.WORKER_ID_TAG_NAME));
    }

    @Test
    public void testCreatingTagsWithEmptyWorkerId() {
        Map<String, String> tags = ConnectMetrics.tags("", "k1", "v1", "k2", "v2");
        assertEquals("v1", tags.get("k1"));
        assertEquals("v2", tags.get("k2"));
        assertEquals(null, tags.get(ConnectMetrics.WORKER_ID_TAG_NAME));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatingTagsWithOddNumberOfTags() {
        ConnectMetrics.tags("name", "k1", "v1", "k2", "v2", "extra");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGettingGroupWithOddNumberOfTags() {
        metrics.group("name", false, "k1", "v1", "k2", "v2", "extra");
    }

    @Test
    public void testGettingGroupWithTags() {
        MetricGroup group1 = metrics.group("name", false, "k1", "v1", "k2", "v2");
        assertEquals("v1", group1.tags().get("k1"));
        assertEquals("v2", group1.tags().get("k2"));
        assertEquals(null, group1.tags().get(ConnectMetrics.WORKER_ID_TAG_NAME));
    }

    @Test
    public void testGettingGroupWithWorkerIdAndTags() {
        MetricGroup group1 = metrics.group("name", true, "k1", "v1", "k2", "v2");
        assertEquals("v1", group1.tags().get("k1"));
        assertEquals("v2", group1.tags().get("k2"));
        assertEquals(metrics.workerId(), group1.tags().get(ConnectMetrics.WORKER_ID_TAG_NAME));
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
    }

}