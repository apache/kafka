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
import org.apache.kafka.common.errors.InvalidConfigurationException;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.utils.Utils.require;

/**
 * Group configuration related parameters and supporting methods like validation, etc. are
 * defined in this class.
 */
public class GroupConfig extends AbstractConfig {

    public static final String CONSUMER_SESSION_TIMEOUT_MS_CONFIG = "consumer.session.timeout.ms";

    public static final String CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG = "consumer.heartbeat.interval.ms";

    private static final ConfigDef CONFIG = new ConfigDef()
            .define(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC)
            .define(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, INT, GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC);
    public GroupConfig(Map<?, ?> props) {
        super(CONFIG, props, false);
    }

    public static Set<String> configNames() {
        return new HashSet<>(CONFIG.names());
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
    public static void validateValues(Map<?, ?> valueMaps, Properties groupConfigBounds) {
        int consumerHeartbeatInterval = (int) valueMaps.get(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
        int consumerSessionTimeout = (int) valueMaps.get(CONSUMER_SESSION_TIMEOUT_MS_CONFIG);
        require(consumerHeartbeatInterval >= (int) groupConfigBounds.get(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG),
            CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG + "must be greater than or equals to" +
            GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
        require(consumerHeartbeatInterval <= (int) groupConfigBounds.get(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG),
            CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG + "must be less than or equals to" +
            GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);
        require(consumerSessionTimeout >= (int) groupConfigBounds.get(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG),
            CONSUMER_SESSION_TIMEOUT_MS_CONFIG + "must be greater than or equals to" +
            GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        require(consumerSessionTimeout <= (int) groupConfigBounds.get(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG),
            CONSUMER_SESSION_TIMEOUT_MS_CONFIG + "must be greater than or equals to" +
            GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);

    }

    public static void validate(Properties props, Properties groupConfigBounds) {
        validateNames(props);
        Map<?, ?> valueMaps = CONFIG.parse(props);
        validateValues(valueMaps, groupConfigBounds);
    }

    /**
     * Create a group config instance using the given properties and defaults
     */
    public static GroupConfig fromProps(Map<?, ?> defaults, Properties overrides) {
        Properties props = new Properties();
        props.putAll(defaults);
        props.putAll(overrides);
        return new GroupConfig(props);
    }

    public int sessionTimeoutMs() {
        return getInt(CONSUMER_SESSION_TIMEOUT_MS_CONFIG);
    }

    public int heartbeatIntervalMs() {
        return getInt(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
    }
}
