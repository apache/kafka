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

package org.apache.kafka.metadata.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.metadata.config.ConfigRegistryTestConstants.TOPIC_BAAZ_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(120)
public final class AllResourcesConfigMonitorTest {
    static class TestContext {
        int numCallbacks = 0;

        Map<String, Boolean> values = new HashMap<>();

        AllResourcesConfigMonitor<Boolean> monitor =
            new AllResourcesConfigMonitor<>(TOPIC_BAAZ_ENABLED,
                (resourceName, newValue) -> this.setValue(resourceName, newValue),
                false);

        void setValue(String resourceName, Boolean newValue) {
            this.numCallbacks++;
            if (newValue == null) {
                this.values.remove(resourceName);
            } else {
                this.values.put(resourceName, newValue);
            }
        }
    }

    static Map<String, Boolean> createMap(
        Collection<String> trueKeys,
        Collection<String> falseKeys
    ) {
        Map<String, Boolean> result = new HashMap<>();
        trueKeys.forEach(k -> result.put(k, true));
        falseKeys.forEach(k -> result.put(k, false));
        return result;
    }

    @Test
    public void testKey() {
        TestContext context = new TestContext();
        assertEquals(TOPIC_BAAZ_ENABLED, context.monitor.key());
    }

    @Test
    public void testNoUpdates() {
        TestContext context = new TestContext();
        assertEquals(createMap(Arrays.asList(), Arrays.asList()), context.values);
        assertEquals(0, context.numCallbacks);
    }

    @Test
    public void testUpdateSpecificResources() {
        TestContext context = new TestContext();
        context.monitor.update("1", true);
        context.monitor.update("2", false);
        assertEquals(createMap(Arrays.asList("1"), Arrays.asList("2")), context.values);
        assertEquals(2, context.numCallbacks);
    }

    @Test
    public void testUpdateDefaultResource() {
        TestContext context = new TestContext();
        context.monitor.update("", true);
        assertEquals(createMap(Arrays.asList(""), Arrays.asList()), context.values);
        assertEquals(1, context.numCallbacks);
    }

    @Test
    public void testUpdateDefaultResourceAndRemove() {
        TestContext context = new TestContext();
        context.monitor.update("", true);
        context.monitor.update("", null);
        assertEquals(createMap(Arrays.asList(), Arrays.asList("")), context.values);
        assertEquals(2, context.numCallbacks);
    }

    @Test
    public void testUpdateSpecificResourcesAndRemove() {
        TestContext context = new TestContext();
        context.monitor.update("1", true);
        context.monitor.update("2", false);
        context.monitor.update("1", null);
        context.monitor.update("2", null);
        assertEquals(createMap(Arrays.asList(), Arrays.asList()), context.values);
        assertEquals(4, context.numCallbacks);
    }
}
