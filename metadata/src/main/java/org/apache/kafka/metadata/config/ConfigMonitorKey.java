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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Objects;

public final class ConfigMonitorKey {
    private final ConfigResource.Type resourceType;
    private final String keyName;

    public ConfigMonitorKey(
        ConfigResource.Type resourceType,
        String keyName
    ) {
        this.resourceType = resourceType;
        this.keyName = keyName;
    }

    public ConfigResource.Type resourceType() {
        return resourceType;
    }

    public String keyName() {
        return keyName;
    }

    /**
     * Verify that this ConfigMonitorKey appears in the schema and has the value type we expect.
     *
     * @param configSchema          The configuration schema object
     * @param expectedValueType
     */
    public void verifyExpectedValueType(
        KafkaConfigSchema configSchema,
        ConfigDef.Type expectedValueType
    ) {
        ConfigDef.ConfigKey configKey = configSchema.configKey(resourceType, keyName);
        if (configKey == null) {
            throw new RuntimeException("No config schema entry found for resource type " +
                    resourceType + ", config keyName " + keyName);
        }
        if (!configKey.type.equals(expectedValueType)) {
            throw new RuntimeException("Unexpected value type for resource type " +
                    resourceType + ", config keyName " + keyName + "; " + "needed " + expectedValueType +
                    ", got " + configKey.type);
        }
    }

    /**
     * Check if the value type is sensitive.
     *
     * @return      True if the value type is sensitive, and cannot be logged.
     */
    public boolean isSensitive(KafkaConfigSchema configSchema) {
        ConfigDef.ConfigKey configKey = configSchema.configKey(resourceType, keyName);
        if (configKey == null) {
            throw new RuntimeException("No config schema entry found for resource type " +
                    resourceType + ", config keyName " + keyName);
        }
        return configKey.type.isSensitive();
    }

    /**
     * Get the statically configured value for this key.
     *
     * @param log           The log object to use if needed.
     * @param configMap     A map containing static configuration entries.
     * @param configSchema  The current configuration schema.
     *
     * @return              The static default value.
     */
    Object loadStaticValue(
        Logger log,
        Map<String, Object> configMap,
        KafkaConfigSchema configSchema
    ) {
        ConfigDef.ConfigKey configKey = configSchema.configKey(resourceType, keyName);
        if (configKey == null) {
            throw new RuntimeException("No config schema entry found for resource type " +
                resourceType + ", " + "config keyName " + keyName);
        }
        if (!configMap.containsKey(keyName)) {
            if (log.isTraceEnabled()) {
                log.trace("Loaded initial value of {} from hard-coded node configuration as {]",
                    keyName, configKey.type.isSensitive() ? "[redacted]" : configKey.defaultValue);
            }
            return configKey.defaultValue;
        }
        try {
            Object result = ConfigDef.parseType(keyName, configMap.get(keyName), configKey.type);
            if (log.isTraceEnabled()) {
                log.trace("Loaded initial value of {} from static node configuration as {]",
                        keyName, configKey.type.isSensitive() ? "[redacted]" : result);
            }
            return result;
        } catch (ConfigException e) {
            log.error("Unable to parse statically configured value for {}. Falling back on " +
                            "hard-coded value of {}.",
                    keyName, configKey.type.isSensitive() ? "[redacted]" : configKey.defaultValue, e);
            return configKey.defaultValue;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!o.getClass().equals(ConfigMonitorKey.class))) return false;
        ConfigMonitorKey other = (ConfigMonitorKey) o;
        return resourceType.equals(other.resourceType) && keyName.equals(other.keyName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, keyName);
    }

    @Override
    public String toString() {
        return "ConfigMonitorKey(resourceType=" + resourceType + ", keyName=" + keyName + ")";
    }
}
