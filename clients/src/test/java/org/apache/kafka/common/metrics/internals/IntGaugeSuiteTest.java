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

package org.apache.kafka.common.metrics.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class IntGaugeSuiteTest {
    private static final Logger log = LoggerFactory.getLogger(IntGaugeSuiteTest.class);

    private static IntGaugeSuite<String> createIntGaugeSuite() {
        MetricConfig config = new MetricConfig();
        Metrics metrics = new Metrics(config);
        return new IntGaugeSuite<>(log,
            "mySuite",
            metrics,
            name -> new MetricName(name, "group", "myMetric", Collections.emptyMap()),
            3);
    }

    @Test
    public void testCreateAndClose() {
        IntGaugeSuite<String> suite = createIntGaugeSuite();
        assertEquals(3, suite.maxEntries());
        suite.close();
        suite.close();
        suite.metrics().close();
    }

    @Test
    public void testCreateMetrics() {
        IntGaugeSuite<String> suite = createIntGaugeSuite();
        suite.increment("foo");
        Map<String, Integer> values = suite.values();
        assertEquals(Integer.valueOf(1), values.get("foo"));
        assertEquals(1, values.size());
        suite.increment("foo");
        suite.increment("bar");
        suite.increment("baz");
        suite.increment("quux");
        values = suite.values();
        assertEquals(Integer.valueOf(2), values.get("foo"));
        assertEquals(Integer.valueOf(1), values.get("bar"));
        assertEquals(Integer.valueOf(1), values.get("baz"));
        assertEquals(3, values.size());
        assertFalse(values.containsKey("quux"));
        suite.close();
        suite.metrics().close();
    }

    @Test
    public void testCreateAndRemoveMetrics() {
        IntGaugeSuite<String> suite = createIntGaugeSuite();
        suite.increment("foo");
        suite.decrement("foo");
        suite.increment("foo");
        suite.increment("foo");
        suite.increment("bar");
        suite.decrement("bar");
        suite.increment("baz");
        suite.increment("quux");
        Map<String, Integer> values = suite.values();
        assertEquals(Integer.valueOf(2), values.get("foo"));
        assertFalse(values.containsKey("bar"));
        assertEquals(Integer.valueOf(1), values.get("baz"));
        assertEquals(Integer.valueOf(1), values.get("quux"));
        assertEquals(3, values.size());
        suite.close();
        suite.metrics().close();
    }
}
