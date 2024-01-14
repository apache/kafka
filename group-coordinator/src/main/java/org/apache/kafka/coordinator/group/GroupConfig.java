/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Group configuration related parameters and supporting methods like validation, etc. are
 * defined in this class.
 */
public class GroupConfig extends AbstractConfig {

    public static final String CONSUMER_SESSION_TIMEOUT_MS_CONFIG = "consumer.session.timeout.ms";

    public static final String CONSUMER_SESSION_TIMEOUT_MS_DOC
        = "The timeout to detect client failures when using the consumer group protocol.";

    public static final String CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG = "consumer.heartbeat.interval.ms";

    public static final String CONSUMER_HEARTBEAT_INTERVAL_MS_DOC
        = "The heartbeat interval given to the members of a consumer group.";

    public static final int DEFAULT_CONSUMER_GROUP_SESSION_TIMEOUT_MS = 45 * 1000;

    public static final int DEFAULT_CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS = 5 * 1000;

    private static final ConfigDef CONFIG = new ConfigDef();

    static {
        CONFIG
            .define(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, INT, DEFAULT_CONSUMER_GROUP_SESSION_TIMEOUT_MS, atLeast(1),
                MEDIUM, CONSUMER_SESSION_TIMEOUT_MS_DOC)
            .define(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, INT, DEFAULT_CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS, atLeast(1),
                MEDIUM, CONSUMER_HEARTBEAT_INTERVAL_MS_DOC);
    }

    public final int sessionTimeoutMs;

    public final int heartbeatIntervalMs;

    public GroupConfig(Map<?, ?> props) {
        super(CONFIG, props, false);

        this.sessionTimeoutMs = getInt(CONSUMER_SESSION_TIMEOUT_MS_CONFIG);
        this.heartbeatIntervalMs = getInt(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
    }

    public static List<String> configNames() {
        return new ArrayList<>(CONFIG.names());
    }

    /**
     * Check that property names are valid
     */
    public static void validateNames(Properties props) {
        List<String> names = configNames();
        for (Object name : props.keySet()) {
            if (!names.contains(name)) {
                throw new InvalidConfigurationException("Unknown group config name: " + name);
            }
        }
    }

    public static void validate(Properties props) {
        validateNames(props);
        CONFIG.parse(props);
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
}
