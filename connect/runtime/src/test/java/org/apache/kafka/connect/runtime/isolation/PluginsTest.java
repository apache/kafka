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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins.ClassLoaderUsage;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PluginsTest {

    private static Map<String, String> props;
    private static Plugins plugins;
    private AbstractConfig config;
    private TestConverter converter;
    private TestHeaderConverter headerConverter;

    @BeforeClass
    public static void beforeAll() {
        props = new HashMap<>();
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());
        props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());
        props.put("key.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        props.put("value.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        props.put("key.converter.extra.config", "foo1");
        props.put("value.converter.extra.config", "foo2");
        props.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());
        props.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());
        props.put("internal.key.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        props.put("internal.value.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        props.put("internal.key.converter.extra.config", "bar1");
        props.put("internal.value.converter.extra.config", "bar2");
        props.put(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG, TestHeaderConverter.class.getName());
        props.put("header.converter.extra.config", "baz");

        // Set up the plugins to have no additional plugin directories.
        // This won't allow us to test classpath isolation, but it will allow us to test some of the utility methods.
        props.put(WorkerConfig.PLUGIN_PATH_CONFIG, "");
        plugins = new Plugins(props);
    }

    @Before
    public void setup() {
        this.config = new TestableWorkerConfig(props);
    }

    @Test
    public void shouldInstantiateAndConfigureConverters() {
        instantiateAndConfigureConverter(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, ClassLoaderUsage.CURRENT_CLASSLOADER);
        // Validate extra configs got passed through to overridden converters
        assertEquals("true", converter.configs.get(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG));
        assertEquals("foo1", converter.configs.get("extra.config"));

        instantiateAndConfigureConverter(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, ClassLoaderUsage.PLUGINS);
        // Validate extra configs got passed through to overridden converters
        assertEquals("true", converter.configs.get(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG));
        assertEquals("foo2", converter.configs.get("extra.config"));
    }

    @Test
    public void shouldInstantiateAndConfigureInternalConverters() {
        instantiateAndConfigureConverter(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, ClassLoaderUsage.CURRENT_CLASSLOADER);
        // Validate extra configs got passed through to overridden converters
        assertEquals("false", converter.configs.get(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG));
        assertEquals("bar1", converter.configs.get("extra.config"));

        instantiateAndConfigureConverter(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, ClassLoaderUsage.PLUGINS);
        // Validate extra configs got passed through to overridden converters
        assertEquals("false", converter.configs.get(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG));
        assertEquals("bar2", converter.configs.get("extra.config"));
    }

    @Test
    public void shouldInstantiateAndConfigureHeaderConverter() {
        instantiateAndConfigureHeaderConverter(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG);
        // Validate extra configs got passed through to overridden converters
        assertConverterType(ConverterType.HEADER, headerConverter.configs);
        assertEquals("baz", headerConverter.configs.get("extra.config"));
    }

    protected void instantiateAndConfigureConverter(String configPropName, ClassLoaderUsage classLoaderUsage) {
        converter = (TestConverter) plugins.newConverter(config, configPropName, classLoaderUsage);
        assertNotNull(converter);
    }

    protected void instantiateAndConfigureHeaderConverter(String configPropName) {
        headerConverter = (TestHeaderConverter) plugins.newHeaderConverter(config, configPropName, ClassLoaderUsage.CURRENT_CLASSLOADER);
        assertNotNull(headerConverter);
    }

    protected void assertConverterType(ConverterType type, Map<String, ?> props) {
        assertEquals(type.getName(), props.get(ConverterConfig.TYPE_CONFIG));
    }

    public static class TestableWorkerConfig extends WorkerConfig {
        public TestableWorkerConfig(Map<String, String> props) {
            super(WorkerConfig.baseConfigDef(), props);
        }
    }

    public static class TestConverter implements Converter, Configurable {
        public Map<String, ?> configs;

        public ConfigDef config() {
            return JsonConverterConfig.configDef();
        }

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
            new JsonConverterConfig(configs); // requires the `converter.type` config be set
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

    public static class TestHeaderConverter implements HeaderConverter {
        public Map<String, ?> configs;

        @Override
        public ConfigDef config() {
            return JsonConverterConfig.configDef();
        }

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
            new JsonConverterConfig(configs); // requires the `converter.type` config be set
        }

        @Override
        public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
            return new byte[0];
        }

        @Override
        public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
            return null;
        }

        @Override
        public void close() throws IOException {
        }
    }
}