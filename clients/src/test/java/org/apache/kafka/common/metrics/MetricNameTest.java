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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MetricNameTest {

    private Metrics metrics;

    private Map<String, String> asMap(String... tags) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < tags.length; i += 2)
            map.put(tags[i], tags[i + 1]);
        return map;
    }

    @Before
    public void setUp() {
        metrics = new Metrics();
    }

    private MetricName metricName(String... tags) {
        return metrics.metricName("name", "group", "description", tags);
    }

    private MetricName metricName(Map<String, String> tags) {
        return metrics.metricName("name", "group", "description", tags);
    }

    @Test
    public void equality() {
        MetricName varArgKeyValue = metricName("key1", "value1", "key2", "value2");
        MetricName mapBasedKeyValue = metricName(asMap("key1", "value1", "key2", "value2"));
        assertEquals(
                "metric names created in two different ways should be equal",
                varArgKeyValue,
                mapBasedKeyValue
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void immutability() {
        MetricName mutated = metricName("key1", "value1");
        mutated.tags().put("key2", "value2");
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsException_whenProvidedOddNumberOfKeyValueParameters() {
        metrics.metricName("name", "group", "description", "key1");
    }
}
