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


public class KafkaConfigSchema {
    public static final KafkaConfigSchema EMPTY = new KafkaConfigSchema(emptyMap());

    private final Map<ConfigResource.Type, ConfigDef> configDefs;

    public KafkaConfigSchema(Map<ConfigResource.Type, ConfigDef> configDefs) {
        this.configDefs = configDefs;
    }

    public boolean isSplittable(ConfigResource.Type type, String key) {
        ConfigDef configDef = configDefs.get(type);
        if (configDef == null) return false;
        ConfigDef.ConfigKey configKey = configDef.configKeys().get(key);
        if (configKey == null) return false;
        return configKey.type == ConfigDef.Type.LIST;
    }

    public boolean isSensitive(ConfigRecord record) {
        ConfigResource.Type type = ConfigResource.Type.forId(record.resourceType());
        return isSensitive(type, record.name());
    }

    public boolean isSensitive(ConfigResource.Type type, String key) {
        ConfigDef configDef = configDefs.get(type);
        if (configDef == null) return false;
        ConfigDef.ConfigKey configKey = configDef.configKeys().get(key);
        if (configKey == null) return false;
        return configKey.type.isSensitive();
    }

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
