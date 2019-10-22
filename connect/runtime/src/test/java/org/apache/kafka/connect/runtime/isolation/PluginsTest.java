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

package org.apache.kafka.connect.runtime.isolation;

import java.util.Map.Entry;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PluginsTest {

    private Plugins plugins;
    private Map<String, String> props;
    private AbstractConfig config;

    @Before
    public void setup() {
        Map<String, String> pluginProps = new HashMap<>();

        // Set up the plugins with some test plugins to test isolation
        pluginProps.put(WorkerConfig.PLUGIN_PATH_CONFIG, Utils.join(TestPlugins.pluginPath(), ","));
        plugins = new Plugins(pluginProps);
        props = new HashMap<>(pluginProps);
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());
        props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());
        props.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());
        props.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());

        createConfig();
    }

    protected void createConfig() {
        this.config = new TestableWorkerConfig(props);
    }

    @Test(expected = ExceptionInInitializerError.class)
    public void shouldThrowIfPluginThrows() {
        TestPlugins.assertAvailable();
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, TestPlugins.ALWAYS_THROW_EXCEPTION);
        ClassLoader classLoader = plugins.delegatingLoader().pluginClassLoader(TestPlugins.ALWAYS_THROW_EXCEPTION);
        ClassLoader savedLoader = Plugins.compareAndSwapLoaders(classLoader);
        try {
            createConfig();
        } finally {
            Plugins.compareAndSwapLoaders(savedLoader);
        }
    }

    @Test
    public void shouldShareStaticValuesBetweenSamePlugin() {
        // Plugins are not isolated from other instances of their own class.
        TestPlugins.assertAvailable();

        Converter firstPlugin = plugins.newConverter(
            TestPlugins.ALIASED_STATIC_FIELD,
            config
        );

        assertInstanceOf(SamplingTestPlugin.class, firstPlugin, "Cannot collect samples");

        Converter secondPlugin = plugins.newConverter(
            TestPlugins.ALIASED_STATIC_FIELD,
            config
        );

        assertInstanceOf(SamplingTestPlugin.class, secondPlugin, "Cannot collect samples");
        assertSame(
            ((SamplingTestPlugin) firstPlugin).otherSamples(),
            ((SamplingTestPlugin) secondPlugin).otherSamples()
        );
    }

    @Test
    public void newPluginShouldServiceLoadWithPluginClassLoader() {
        TestPlugins.assertAvailable();
        Converter plugin = plugins.newConverter(
            TestPlugins.SERVICE_LOADER,
            config
        );

        assertInstanceOf(SamplingTestPlugin.class, plugin, "Cannot collect samples");
        Map<String, SamplingTestPlugin> samples = ((SamplingTestPlugin) plugin).flatten();
        // Assert that the service loaded subclass is found in both environments
        assertTrue(samples.containsKey("ServiceLoadedSubclass.static"));
        assertTrue(samples.containsKey("ServiceLoadedSubclass.dynamic"));
        assertPluginClassLoaderAlwaysActive(samples);
    }

    @Test
    public void newPluginShouldInstantiateWithPluginClassLoader() {
        TestPlugins.assertAvailable();
        Converter plugin = plugins.newConverter(
            TestPlugins.SERVICE_LOADER,
            config
        );

        assertInstanceOf(SamplingTestPlugin.class, plugin, "Cannot collect samples");
        Map<String, SamplingTestPlugin> samples = ((SamplingTestPlugin) plugin).flatten();
        assertPluginClassLoaderAlwaysActive(samples);
    }

    @Test(expected = ConfigException.class)
    public void shouldFailToFindConverterInCurrentClassloader() {
        TestPlugins.assertAvailable();
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, TestPlugins.SAMPLING_CONVERTER);
        createConfig();
    }

    public static void assertPluginClassLoaderAlwaysActive(Map<String, SamplingTestPlugin> samples) {
        for (Entry<String, SamplingTestPlugin> e : samples.entrySet()) {
            String sampleName = "\"" + e.getKey() + "\" (" + e.getValue() + ")";
            assertInstanceOf(
                PluginClassLoader.class,
                e.getValue().staticClassloader(),
                sampleName + " has incorrect static classloader"
            );
            assertInstanceOf(
                PluginClassLoader.class,
                e.getValue().classloader(),
                sampleName + " has incorrect dynamic classloader"
            );
        }
    }

    public static void assertInstanceOf(Class<?> expected, Object actual, String message) {
        assertTrue(
            "Expected an instance of " + expected.getSimpleName() + ", found " + actual + " instead: " + message,
            expected.isInstance(actual)
        );
    }

    public static class TestableWorkerConfig extends WorkerConfig {
        public TestableWorkerConfig(Map<String, String> props) {
            super(WorkerConfig.baseConfigDef(), props);
        }
    }

    public static class TestConverter implements Converter, Configurable {
        public Map<String, ?> configs;

        public ConfigDef config() {
            return null;
        }

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
            new JsonConverter().configure(configs, true); // requires the `converter.type` config be set
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.configs = configs;
        }

        @Override
        public byte[] fromConnectData(String topic, Schema schema, Object value) {
            return new byte[0];
        }

        @Override
        public SchemaAndValue toConnectData(String topic, byte[] value) {
            return null;
        }
    }
}