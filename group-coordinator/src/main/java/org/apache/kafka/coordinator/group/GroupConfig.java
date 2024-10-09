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

package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;

/**
 * Group configuration related parameters and supporting methods like validation, etc. are
 * defined in this class.
 */
public class GroupConfig extends AbstractConfig {

    public static final String CONSUMER_SESSION_TIMEOUT_MS_CONFIG = "consumer.session.timeout.ms";

    public static final String CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG = "consumer.heartbeat.interval.ms";

    public final int consumerSessionTimeoutMs;

    public final int consumerHeartbeatIntervalMs;

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(CONSUMER_SESSION_TIMEOUT_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC)
        .define(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC);

    @SuppressWarnings("this-escape")
    public GroupConfig(Map<?, ?> props) {
        super(CONFIG, props, false);
        this.consumerSessionTimeoutMs = getInt(CONSUMER_SESSION_TIMEOUT_MS_CONFIG);
        this.consumerHeartbeatIntervalMs = getInt(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public static Optional<Type> configType(String configName) {
        return Optional.ofNullable(CONFIG.configKeys().get(configName)).map(c -> c.type);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    /**
     * Check that property names are valid
     */
    public static void validateNames(Properties props) {
        Set<String> names = configNames();
        for (Object name : props.keySet()) {
            if (!names.contains(name)) {
                throw new InvalidConfigurationException("Unknown group config name: " + name);
            }
        }
    }

    /**
     * Validates the values of the given properties.
     */
    private static void validateValues(Map<?, ?> valueMaps, GroupCoordinatorConfig groupCoordinatorConfig) {
        int consumerHeartbeatInterval = (Integer) valueMaps.get(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
        int consumerSessionTimeout = (Integer) valueMaps.get(CONSUMER_SESSION_TIMEOUT_MS_CONFIG);
        if (consumerHeartbeatInterval < groupCoordinatorConfig.consumerGroupMinHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG + " must be greater than or equals to " +
                GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (consumerHeartbeatInterval > groupCoordinatorConfig.consumerGroupMaxHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG + " must be less than or equals to " +
                GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (consumerSessionTimeout < groupCoordinatorConfig.consumerGroupMinSessionTimeoutMs()) {
            throw new InvalidConfigurationException(CONSUMER_SESSION_TIMEOUT_MS_CONFIG + " must be greater than or equals to " +
                GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (consumerSessionTimeout > groupCoordinatorConfig.consumerGroupMaxSessionTimeoutMs()) {
            throw new InvalidConfigurationException(CONSUMER_SESSION_TIMEOUT_MS_CONFIG + " must be greater than or equals to " +
                GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
        }

        if (consumerSessionTimeout <= consumerHeartbeatInterval) {
            throw new InvalidConfigurationException(CONSUMER_SESSION_TIMEOUT_MS_CONFIG + " must be greater than " +
                CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
    }

    /**
     * Check that the given properties contain only valid consumer group config names and that all values can be
     * parsed and are valid.
     */
    public static void validate(Properties props, GroupCoordinatorConfig groupCoordinatorConfig) {
        validateNames(props);
        Map<?, ?> valueMaps = CONFIG.parse(props);
        validateValues(valueMaps, groupCoordinatorConfig);
    }

    /**
     * Create a group config instance using the given properties and defaults.
     */
    public static GroupConfig fromProps(Map<?, ?> defaults, Properties overrides) {
        Properties props = new Properties();
        props.putAll(defaults);
        props.putAll(overrides);
        return new GroupConfig(props);
    }

    /**
     * The consumer group session timeout in milliseconds.
     */
    public int sessionTimeoutMs() {
        return consumerSessionTimeoutMs;
    }

    /**
     * The consumer group heartbeat interval in milliseconds.
     */
    public int heartbeatIntervalMs() {
        return consumerHeartbeatIntervalMs;
    }
}
