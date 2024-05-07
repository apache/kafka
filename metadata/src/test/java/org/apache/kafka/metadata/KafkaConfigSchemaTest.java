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

package org.apache.kafka.metadata;

import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.server.config.ConfigSynonym;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.server.config.ConfigSynonym.HOURS_TO_MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class KafkaConfigSchemaTest {
    public static final Map<ConfigResource.Type, ConfigDef> CONFIGS = new HashMap<>();

    static {
        CONFIGS.put(BROKER, new ConfigDef().
            define("foo.bar", ConfigDef.Type.LIST, "1", ConfigDef.Importance.HIGH, "foo bar doc").
            define("baz", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "baz doc").
            define("quux", ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "quux doc").
            define("quuux", ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "quuux doc").
            define("quuux2", ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "quuux2 doc"));
        CONFIGS.put(TOPIC, new ConfigDef().
            define("abc", ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "abc doc").
            define("def", ConfigDef.Type.LONG, ConfigDef.Importance.HIGH, "def doc").
            define("ghi", ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, "ghi doc").
            define("xyz", ConfigDef.Type.PASSWORD, "thedefault", ConfigDef.Importance.HIGH, "xyz doc"));
    }

    public static final Map<String, List<ConfigSynonym>> SYNONYMS = new HashMap<>();

    static {
        SYNONYMS.put("abc", Arrays.asList(new ConfigSynonym("foo.bar")));
        SYNONYMS.put("def", Arrays.asList(new ConfigSynonym("quux", HOURS_TO_MILLISECONDS)));
        SYNONYMS.put("ghi", Arrays.asList(new ConfigSynonym("ghi")));
        SYNONYMS.put("xyz", Arrays.asList(new ConfigSynonym("quuux"), new ConfigSynonym("quuux2")));
    }

    private static final KafkaConfigSchema SCHEMA = new KafkaConfigSchema(CONFIGS, SYNONYMS);

    @Test
    public void testTranslateConfigTypes() {
        testTranslateConfigType(ConfigDef.Type.BOOLEAN, ConfigEntry.ConfigType.BOOLEAN);
        testTranslateConfigType(ConfigDef.Type.STRING, ConfigEntry.ConfigType.STRING);
        testTranslateConfigType(ConfigDef.Type.INT, ConfigEntry.ConfigType.INT);
        testTranslateConfigType(ConfigDef.Type.SHORT, ConfigEntry.ConfigType.SHORT);
        testTranslateConfigType(ConfigDef.Type.LONG, ConfigEntry.ConfigType.LONG);
        testTranslateConfigType(ConfigDef.Type.DOUBLE, ConfigEntry.ConfigType.DOUBLE);
        testTranslateConfigType(ConfigDef.Type.LIST, ConfigEntry.ConfigType.LIST);
        testTranslateConfigType(ConfigDef.Type.CLASS, ConfigEntry.ConfigType.CLASS);
        testTranslateConfigType(ConfigDef.Type.PASSWORD, ConfigEntry.ConfigType.PASSWORD);
    }

    private static void testTranslateConfigType(ConfigDef.Type a, ConfigEntry.ConfigType b) {
        assertEquals(b, KafkaConfigSchema.translateConfigType(a));
    }

    @Test
    public void testTranslateConfigSources() {
        testTranslateConfigSource(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG,
            DescribeConfigsResponse.ConfigSource.TOPIC_CONFIG);
        testTranslateConfigSource(ConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG,
            DescribeConfigsResponse.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG);
        testTranslateConfigSource(ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
            DescribeConfigsResponse.ConfigSource.DYNAMIC_BROKER_CONFIG);
        testTranslateConfigSource(ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
            DescribeConfigsResponse.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG);
        testTranslateConfigSource(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG,
            DescribeConfigsResponse.ConfigSource.STATIC_BROKER_CONFIG);
        testTranslateConfigSource(ConfigEntry.ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG,
            DescribeConfigsResponse.ConfigSource.CLIENT_METRICS_CONFIG);
        testTranslateConfigSource(ConfigEntry.ConfigSource.DEFAULT_CONFIG,
            DescribeConfigsResponse.ConfigSource.DEFAULT_CONFIG);
    }

    private static void testTranslateConfigSource(ConfigEntry.ConfigSource a,
                                                  DescribeConfigsResponse.ConfigSource b) {
        assertEquals(b, KafkaConfigSchema.translateConfigSource(a));
    }

    @Test
    public void testIsSplittable() {
        assertTrue(SCHEMA.isSplittable(BROKER, "foo.bar"));
        assertFalse(SCHEMA.isSplittable(BROKER, "baz"));
        assertFalse(SCHEMA.isSplittable(BROKER, "foo.baz.quux"));
        assertFalse(SCHEMA.isSplittable(TOPIC, "baz"));
        assertTrue(SCHEMA.isSplittable(TOPIC, "abc"));
    }

    @Test
    public void testGetConfigValueDefault() {
        assertEquals("1", SCHEMA.getDefault(BROKER, "foo.bar"));
        assertNull(SCHEMA.getDefault(BROKER, "foo.baz.quux"));
        assertNull(SCHEMA.getDefault(TOPIC, "abc"));
        assertEquals("true", SCHEMA.getDefault(TOPIC, "ghi"));
    }

    @Test
    public void testIsSensitive() {
        assertFalse(SCHEMA.isSensitive(BROKER, "foo.bar"));
        assertTrue(SCHEMA.isSensitive(BROKER, "quuux"));
        assertTrue(SCHEMA.isSensitive(BROKER, "quuux2"));
        assertTrue(SCHEMA.isSensitive(BROKER, "unknown.config.key"));
        assertFalse(SCHEMA.isSensitive(TOPIC, "abc"));
    }

    @Test
    public void testResolveEffectiveTopicConfig() {
        Map<String, String> staticNodeConfig = new HashMap<>();
        staticNodeConfig.put("foo.bar", "the,static,value");
        staticNodeConfig.put("quux", "123");
        staticNodeConfig.put("ghi", "false");
        Map<String, String> dynamicClusterConfigs = new HashMap<>();
        dynamicClusterConfigs.put("foo.bar", "the,dynamic,cluster,config,value");
        dynamicClusterConfigs.put("quux", "456");
        Map<String, String> dynamicNodeConfigs = new HashMap<>();
        dynamicNodeConfigs.put("quux", "789");
        Map<String, String> dynamicTopicConfigs = new HashMap<>();
        dynamicTopicConfigs.put("ghi", "true");
        Map<String, ConfigEntry> expected = new HashMap<>();
        expected.put("abc", new ConfigEntry("abc", "the,dynamic,cluster,config,value",
            ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG, false, false, emptyList(),
                ConfigEntry.ConfigType.LIST, "abc doc"));
        expected.put("def", new ConfigEntry("def", "2840400000",
            ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, false, false, emptyList(),
            ConfigEntry.ConfigType.LONG, "def doc"));
        expected.put("ghi", new ConfigEntry("ghi", "true",
            ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG, false, false, emptyList(),
            ConfigEntry.ConfigType.BOOLEAN, "ghi doc"));
        expected.put("xyz", new ConfigEntry("xyz", "thedefault",
            ConfigEntry.ConfigSource.DEFAULT_CONFIG, true, false, emptyList(),
            ConfigEntry.ConfigType.PASSWORD, "xyz doc"));
        assertEquals(expected, SCHEMA.resolveEffectiveTopicConfigs(staticNodeConfig,
            dynamicClusterConfigs,
            dynamicNodeConfigs,
            dynamicTopicConfigs));
    }
}
