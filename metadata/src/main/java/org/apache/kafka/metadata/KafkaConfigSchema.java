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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.ConfigRecord;

import java.util.Map;

import static java.util.Collections.emptyMap;


/**
 * Tracks information about the schema of configuration keys for brokers, topics, and other
 * resources. Since this class does not depend on core, it is useful in the controller for
 * determining the type of config keys (string, int, password, etc.)
 */
public class KafkaConfigSchema {
    public static final KafkaConfigSchema EMPTY = new KafkaConfigSchema(emptyMap());

    private final Map<ConfigResource.Type, ConfigDef> configDefs;

    public KafkaConfigSchema(Map<ConfigResource.Type, ConfigDef> configDefs) {
        this.configDefs = configDefs;
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
}
