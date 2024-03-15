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

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;

public final class ConfigRegistry {
    /**
     * Create a builder object which can be used to create a new ConfigMonitor.
     */
    public static class Builder {
        private final LogContext logContext;
        private final Logger log;
        private final KafkaConfigSchema configSchema;
        private final Map<String, Object> staticConfigMap;
        private final String nodeIdString;
        private final Map<ConfigMonitorKey, ConfigMonitor<?>> monitors = new HashMap<>();

        /**
         * Create the new Builder.
         *
         * @param configSchema  The configuration schema to use.
         */
        public Builder(
            LogContext logContext,
            KafkaConfigSchema configSchema,
            Map<String, Object> staticConfigMap,
            int nodeId
        ) {
            this.logContext = logContext;
            this.log = logContext.logger(ConfigRegistry.class);
            this.configSchema = configSchema;
            this.staticConfigMap = staticConfigMap;
            this.nodeIdString = Integer.toString(nodeId);
        }

        public Builder addShortNodeMonitor(String keyName, Consumer<Short> callback) {
            ConfigMonitorKey key = new ConfigMonitorKey(BROKER, keyName);
            key.verifyExpectedValueType(configSchema, SHORT);
            return addMonitor(new SpecificResourceConfigMonitor<>(key, false, nodeIdString,
                callback, (Short) key.loadStaticValue(log, staticConfigMap, configSchema)));
        }

        public Builder addIntNodeMonitor(String keyName, Consumer<Integer> callback) {
            ConfigMonitorKey key = new ConfigMonitorKey(BROKER, keyName);
            key.verifyExpectedValueType(configSchema, INT);
            return addMonitor(new SpecificResourceConfigMonitor<>(key, false, nodeIdString,
                callback, (Integer) key.loadStaticValue(log, staticConfigMap, configSchema)));
        }

        public Builder addMonitor(ConfigMonitor<?> monitor) {
            monitors.put(monitor.key(), monitor);
            return this;
        }

        public ConfigRegistry build() {
            return new ConfigRegistry(log,
                monitors,
                staticConfigMap,
                configSchema);
        }

        public short getStaticNodeShort(String keyName) {
            ConfigMonitorKey key = new ConfigMonitorKey(BROKER, keyName);
            key.verifyExpectedValueType(configSchema, SHORT);
            return (short) key.loadStaticValue(log, staticConfigMap, configSchema);
        }

        public int getStaticNodeInt(String keyName) {
            ConfigMonitorKey key = new ConfigMonitorKey(BROKER, keyName);
            key.verifyExpectedValueType(configSchema, INT);
            return (int) key.loadStaticValue(log, staticConfigMap, configSchema);
        }
    }

    /**
     * The slf4j logger object.
     */
    private final Logger log;

    /**
     * The monitors maintained by this registry.
     */
    private final Map<ConfigMonitorKey, ConfigMonitor<?>> monitors;

    @SuppressWarnings("unchecked")
    private ConfigRegistry(
        Logger log,
        Map<ConfigMonitorKey, ConfigMonitor<?>> monitors,
        Map<String, Object> staticConfigMap,
        KafkaConfigSchema configSchema
    ) {
        this.log = log;
        this.monitors = monitors;

        // Initialize defaults.
        for (ConfigMonitor monitor : monitors.values()) {
            monitor.update("", monitor.key().loadStaticValue(log, staticConfigMap, configSchema));
        }
    }

    @SuppressWarnings("unchecked")
    public void maybeUpdate(
        ConfigResource configResource,
        String configKey,
        String newValue
    ) {
        ConfigMonitorKey key = new ConfigMonitorKey(configResource.type(), configKey);
        ConfigMonitor monitor = monitors.get(key);
        if (monitor != null) {
            monitor.update(configResource.name(), newValue);
        }
    }
}
