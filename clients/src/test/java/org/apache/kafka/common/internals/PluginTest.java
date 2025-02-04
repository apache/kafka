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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Monitorable;
import org.apache.kafka.common.metrics.PluginMetrics;

import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PluginTest {

    private static final String CONFIG = "some.config";
    private static final Metrics METRICS = new Metrics();

    static class SomePlugin implements Closeable {

        PluginMetrics pluginMetrics;
        boolean closed;
        boolean throwOnClose = false;

        @Override
        public void close() throws IOException {
            if (throwOnClose) throw new RuntimeException("throw on close");
            closed = true;
        }
    }

    static class SomeMonitorablePlugin extends SomePlugin implements Monitorable  {

        @Override
        public void withPluginMetrics(PluginMetrics metrics) {
            pluginMetrics = metrics;
        }
    }

    @Test
    void testWrapInstance() throws Exception {
        SomeMonitorablePlugin someMonitorablePlugin = new SomeMonitorablePlugin();
        Plugin<SomeMonitorablePlugin> pluginMonitorable = Plugin.wrapInstance(someMonitorablePlugin, METRICS, CONFIG);
        checkPlugin(pluginMonitorable, someMonitorablePlugin, true);

        someMonitorablePlugin = new SomeMonitorablePlugin();
        assertFalse(someMonitorablePlugin.closed);
        pluginMonitorable = Plugin.wrapInstance(someMonitorablePlugin, null, CONFIG);
        checkPlugin(pluginMonitorable, someMonitorablePlugin, false);

        SomePlugin somePlugin = new SomePlugin();
        assertFalse(somePlugin.closed);
        Plugin<SomePlugin> plugin = Plugin.wrapInstance(somePlugin, null, CONFIG);
        assertSame(somePlugin, plugin.get());
        assertNull(somePlugin.pluginMetrics);
        plugin.close();
        assertTrue(somePlugin.closed);
    }

    @Test
    void testWrapInstances() throws Exception {
        List<SomeMonitorablePlugin> someMonitorablePlugins = Arrays.asList(new SomeMonitorablePlugin(), new SomeMonitorablePlugin());
        List<Plugin<SomeMonitorablePlugin>> pluginsMonitorable = Plugin.wrapInstances(someMonitorablePlugins, METRICS, CONFIG);
        assertEquals(someMonitorablePlugins.size(), pluginsMonitorable.size());
        for (int i = 0; i < pluginsMonitorable.size(); i++) {
            Plugin<SomeMonitorablePlugin> plugin = pluginsMonitorable.get(i);
            SomeMonitorablePlugin somePlugin = someMonitorablePlugins.get(i);
            checkPlugin(plugin, somePlugin, true);
        }

        someMonitorablePlugins = Arrays.asList(new SomeMonitorablePlugin(), new SomeMonitorablePlugin());
        pluginsMonitorable = Plugin.wrapInstances(someMonitorablePlugins, null, CONFIG);
        assertEquals(someMonitorablePlugins.size(), pluginsMonitorable.size());
        for (int i = 0; i < pluginsMonitorable.size(); i++) {
            Plugin<SomeMonitorablePlugin> plugin = pluginsMonitorable.get(i);
            SomeMonitorablePlugin somePlugin = someMonitorablePlugins.get(i);
            checkPlugin(plugin, somePlugin, false);
        }

        List<SomePlugin> somePlugins = Arrays.asList(new SomePlugin(), new SomePlugin());
        List<Plugin<SomePlugin>> plugins = Plugin.wrapInstances(somePlugins, METRICS, CONFIG);
        assertEquals(somePlugins.size(), plugins.size());
        for (int i = 0; i < plugins.size(); i++) {
            Plugin<SomePlugin> plugin = plugins.get(i);
            SomePlugin somePlugin = somePlugins.get(i);
            assertSame(somePlugin, plugin.get());
            assertNull(somePlugin.pluginMetrics);
            plugin.close();
            assertTrue(somePlugin.closed);
        }
    }

    @Test
    public void testCloseThrows() {
        SomePlugin somePlugin = new SomePlugin();
        somePlugin.throwOnClose = true;
        Plugin<SomePlugin> plugin = Plugin.wrapInstance(somePlugin, METRICS, CONFIG);
        assertThrows(KafkaException.class, plugin::close);
    }

    @Test
    public void testUsePluginMetricsAfterClose() throws Exception {
        Plugin<SomeMonitorablePlugin> plugin = Plugin.wrapInstance(new SomeMonitorablePlugin(), METRICS, CONFIG);
        PluginMetrics pluginMetrics = plugin.get().pluginMetrics;
        plugin.close();
        assertThrows(IllegalStateException.class, () -> pluginMetrics.metricName("", "", Collections.emptyMap()));
        assertThrows(IllegalStateException.class, () -> pluginMetrics.addMetric(null, null));
        assertThrows(IllegalStateException.class, () -> pluginMetrics.removeMetric(null));
        assertThrows(IllegalStateException.class, () -> pluginMetrics.addSensor(""));
        assertThrows(IllegalStateException.class, () -> pluginMetrics.removeSensor(""));
    }

    private void checkPlugin(Plugin<SomeMonitorablePlugin> plugin, SomeMonitorablePlugin instance, boolean metricsSet) throws Exception {
        assertSame(instance, plugin.get());
        if (metricsSet) {
            assertNotNull(instance.pluginMetrics);
        } else {
            assertNull(instance.pluginMetrics);
        }
        plugin.close();
        assertTrue(instance.closed);
    }
}
