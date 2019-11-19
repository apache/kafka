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
import org.junit.Test;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class IntGaugeSuiteTest {
    private static final Logger log = LoggerFactory.getLogger(IntGaugeSuiteTest.class);

    @Test
    public void testCreateAndClose() {
        MetricConfig config = new MetricConfig();
        Metrics metrics = new Metrics(config);
        IntGaugeSuite<String> suite = new IntGaugeSuite<>(log,
            "mySuite",
            metrics,
            name -> new MetricName(name, "group", "myMetric", Collections.emptyMap()),
            3);
        Assert.assertEquals(3, suite.maxEntries());
        suite.close();
        suite.close();
        metrics.close();
    }

    @Test
    public void testCreateMetrics() {
        MetricConfig config = new MetricConfig();
        Metrics metrics = new Metrics(config);
        IntGaugeSuite<String> suite = new IntGaugeSuite<>(log,
            "mySuite",
            metrics,
            name -> new MetricName(name, "group", "myMetric", Collections.emptyMap()),
            3);
        suite.increment("foo");
        suite.increment("foo");
        suite.increment("bar");
        suite.increment("baz");
        suite.increment("quux");
        suite.visit(map -> {
            Assert.assertEquals(2, map.get("foo").value);
            Assert.assertEquals(1, map.get("bar").value);
            Assert.assertEquals(1, map.get("baz").value);
            Assert.assertFalse(map.containsKey("quux"));
        });
        suite.close();
        metrics.close();
    }
}
