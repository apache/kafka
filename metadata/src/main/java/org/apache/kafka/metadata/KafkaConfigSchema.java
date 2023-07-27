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
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.requests.DescribeConfigsResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.server.config.ConfigSynonym;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;


/**
 * Tracks information about the schema of configuration keys for brokers, topics, and other
 * resources. Since this class does not depend on core, it is useful in the controller for
 * determining the type of config keys (string, int, password, etc.)
 */
public class KafkaConfigSchema {
    public static final KafkaConfigSchema EMPTY = new KafkaConfigSchema(emptyMap(), emptyMap());

    private static final ConfigDef EMPTY_CONFIG_DEF = new ConfigDef();

    /**
     * Translate a ConfigDef.Type to its equivalent for ConfigEntry.ConfigType.
     *
     * We do not want this code in ConfigEntry, since that is a public-facing API. On the
     * other hand, putting this code in ConfigDef.Type would introduce an unwanted dependency
     * from org.apache.kafka.common.config to org.apache.kafka.clients.admin. So it
     * makes sense to put it here.
     */
    public static ConfigEntry.ConfigType translateConfigType(ConfigDef.Type type) {
        switch (type) {
            case BOOLEAN:
                return ConfigEntry.ConfigType.BOOLEAN;
            case STRING:
                return ConfigEntry.ConfigType.STRING;
            case INT:
                return ConfigEntry.ConfigType.INT;
            case SHORT:
                return ConfigEntry.ConfigType.SHORT;
            case LONG:
                return ConfigEntry.ConfigType.LONG;
            case DOUBLE:
                return ConfigEntry.ConfigType.DOUBLE;
            case LIST:
                return ConfigEntry.ConfigType.LIST;
            case CLASS:
                return ConfigEntry.ConfigType.CLASS;
            case PASSWORD:
                return ConfigEntry.ConfigType.PASSWORD;
            default:
                return ConfigEntry.ConfigType.UNKNOWN;
        }
    }

    private static final Map<ConfigEntry.ConfigSource, DescribeConfigsResponse.ConfigSource> TRANSLATE_CONFIG_SOURCE_MAP;

    static {
        Map<ConfigEntry.ConfigSource, DescribeConfigsResponse.ConfigSource> map = new HashMap<>();
        for (DescribeConfigsResponse.ConfigSource source : DescribeConfigsResponse.ConfigSource.values()) {
            map.put(source.source(), source);
        }
        TRANSLATE_CONFIG_SOURCE_MAP = Collections.unmodifiableMap(map);
    }

    /**
     * Translate a ConfigEntry.ConfigSource enum to its equivalent for DescribeConfigsResponse.
     *
     * We do not want this code in ConfigEntry, since that is a public-facing API. On the
     * other hand, putting this code in DescribeConfigsResponse would introduce an unwanted
     * dependency from org.apache.kafka.common.requests to org.apache.kafka.clients.admin.
     * So it makes sense to put it here.
     */
    public static DescribeConfigsResponse.ConfigSource translateConfigSource(ConfigEntry.ConfigSource configSource) {
        DescribeConfigsResponse.ConfigSource result = TRANSLATE_CONFIG_SOURCE_MAP.get(configSource);
        if (result != null) return result;
        return DescribeConfigsResponse.ConfigSource.UNKNOWN;
    }

    private final Map<ConfigResource.Type, ConfigDef> configDefs;

    private final Map<String, List<ConfigSynonym>> logConfigSynonyms;

    public KafkaConfigSchema(Map<ConfigResource.Type, ConfigDef> configDefs,
                             Map<String, List<ConfigSynonym>> logConfigSynonyms) {
        this.configDefs = configDefs;
        this.logConfigSynonyms = logConfigSynonyms;
    }

    /**
     * Returns true if the configuration key specified is splittable (only lists are splittable.)
     */
    public boolean isSplittable(ConfigResource.Type type, String key) {
        ConfigDef configDef = configDefs.get(type);
        if (configDef == null) return false;
        ConfigDef.ConfigKey configKey = configDef.configKeys().get(key);
        if (configKey == null) return false;
        return configKey.type == ConfigDef.Type.LIST;
    }

    /**
     * Returns true if the configuration key specified in this ConfigRecord is sensitive, or if
     * we don't know whether it is sensitive.
     */
    public boolean isSensitive(ConfigRecord record) {
        ConfigResource.Type type = ConfigResource.Type.forId(record.resourceType());
        return isSensitive(type, record.name());
    }

    /**
     * Returns true if the configuration key specified is sensitive, or if we don't know whether
     * it is sensitive.
     */
    public boolean isSensitive(ConfigResource.Type type, String key) {
        ConfigDef configDef = configDefs.get(type);
        if (configDef == null) return true;
        ConfigDef.ConfigKey configKey = configDef.configKeys().get(key);
        if (configKey == null) return true;
        return configKey.type.isSensitive();
    }

    /**
     * Get the default value of the configuration key, or null if no default is specified.
     */
    public String getDefault(ConfigResource.Type type, String key) {
        ConfigDef configDef = configDefs.get(type);
        if (configDef == null) return null;
        ConfigDef.ConfigKey configKey = configDef.configKeys().get(key);
        if (configKey == null || !configKey.hasDefault()) {
            return null;
        }
        return ConfigDef.convertToString(configKey.defaultValue, configKey.type);
    }

    public Map<String, ConfigEntry> resolveEffectiveTopicConfigs(
            Map<String, ?> staticNodeConfig,
            Map<String, ?> dynamicClusterConfigs,
            Map<String, ?> dynamicNodeConfigs,
            Map<String, ?> dynamicTopicConfigs) {
        ConfigDef configDef = configDefs.getOrDefault(ConfigResource.Type.TOPIC, EMPTY_CONFIG_DEF);
        HashMap<String, ConfigEntry> effectiveConfigs = new HashMap<>();
        for (ConfigDef.ConfigKey configKey : configDef.configKeys().values()) {
            ConfigEntry entry = resolveEffectiveTopicConfig(configKey, staticNodeConfig,
                dynamicClusterConfigs, dynamicNodeConfigs, dynamicTopicConfigs);
            effectiveConfigs.put(entry.name(), entry);
        }
        return effectiveConfigs;
    }

    private ConfigEntry resolveEffectiveTopicConfig(ConfigDef.ConfigKey configKey,
            Map<String, ?> staticNodeConfig,
            Map<String, ?> dynamicClusterConfigs,
            Map<String, ?> dynamicNodeConfigs,
            Map<String, ?> dynamicTopicConfigs) {
        if (dynamicTopicConfigs.containsKey(configKey.name)) {
            return toConfigEntry(configKey,
                dynamicTopicConfigs.get(configKey.name),
                ConfigSource.DYNAMIC_TOPIC_CONFIG, Function.identity());
        }
        List<ConfigSynonym> synonyms = logConfigSynonyms.getOrDefault(configKey.name, emptyList());
        for (ConfigSynonym synonym : synonyms) {
            if (dynamicNodeConfigs.containsKey(synonym.name())) {
                return toConfigEntry(configKey, dynamicNodeConfigs.get(synonym.name()),
                    ConfigSource.DYNAMIC_BROKER_CONFIG, synonym.converter());
            }
        }
        for (ConfigSynonym synonym : synonyms) {
            if (dynamicClusterConfigs.containsKey(synonym.name())) {
                return toConfigEntry(configKey, dynamicClusterConfigs.get(synonym.name()),
                    ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG, synonym.converter());
            }
        }
        for (ConfigSynonym synonym : synonyms) {
            if (staticNodeConfig.containsKey(synonym.name())) {
                return toConfigEntry(configKey, staticNodeConfig.get(synonym.name()),
                    ConfigSource.STATIC_BROKER_CONFIG, synonym.converter());
            }
        }
        return toConfigEntry(configKey, configKey.hasDefault() ? configKey.defaultValue : null,
            ConfigSource.DEFAULT_CONFIG, Function.identity());
    }

    private ConfigEntry toConfigEntry(ConfigDef.ConfigKey configKey,
                                      Object value,
                                      ConfigSource source,
                                      Function<String, String> converter) {
        // Convert the value into a nullable string suitable for storing in ConfigEntry.
        String stringValue = null;
        if (value != null) {
            if (value instanceof String) {
                // The value may already be a string if it's coming from a Map<String, String>.
                // Then it doesn't need to be converted.
                stringValue = (String) value;
            } else if (value instanceof Password) {
                // We want the actual value here, not [hidden], which is what we'd get
                // from Password#toString. While we don't return sensitive config values
                // over the wire to users, we may need the real value internally.
                stringValue = ((Password) value).value();
            } else {
                try {
                    // Use the ConfigDef function here which will handle List, Class, etc.
                    stringValue = ConfigDef.convertToString(value, configKey.type);
                } catch (Exception e) {
                    throw new RuntimeException("Unable to convert " + configKey.name + " to string.", e);
                }
            }
        }
        if (stringValue != null) {
            stringValue = converter.apply(stringValue);
        }
        return new ConfigEntry(
            configKey.name,
            stringValue,
            source,
            configKey.type().isSensitive(),
            false, // "readonly" is always false, for now.
            emptyList(), // we don't populate synonyms, for now.
            translateConfigType(configKey.type()),
            configKey.documentation);
    }
}
