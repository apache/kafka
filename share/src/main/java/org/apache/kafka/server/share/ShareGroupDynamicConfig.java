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
package org.apache.kafka.server.share;

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
public class ShareGroupDynamicConfig extends AbstractConfig {

    public static final String SHARE_RECORD_LOCK_DURATION_MS_CONFIG = "share.record.lock.duration.ms";

    public final int shareRecordLockDurationMs;

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(SHARE_RECORD_LOCK_DURATION_MS_CONFIG,
            INT,
            ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_DOC);

    @SuppressWarnings("this-escape")
    public ShareGroupDynamicConfig(Map<?, ?> props) {
        super(CONFIG, props, false);
        this.shareRecordLockDurationMs = getInt(SHARE_RECORD_LOCK_DURATION_MS_CONFIG);
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

    public static boolean isShareGroupConfig(String key) {
        return configNames().contains(key);
    }

    /**
     * Validates the values of the given properties.
     */
    private static void validateValues(Map<?, ?> valueMaps, ShareGroupConfig shareGroupConfig) {
        int shareRecordLockDurationMs = (Integer) valueMaps.get(SHARE_RECORD_LOCK_DURATION_MS_CONFIG);
        if (shareRecordLockDurationMs > shareGroupConfig.shareGroupMaxRecordLockDurationMs()) {
            throw new InvalidConfigurationException(SHARE_RECORD_LOCK_DURATION_MS_CONFIG + " must be less than or equals to " +
                ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG);
        }
        if (shareRecordLockDurationMs < shareGroupConfig.shareGroupMinRecordLockDurationMs()) {
            throw new InvalidConfigurationException(SHARE_RECORD_LOCK_DURATION_MS_CONFIG + " must be greater than or equals to " +
                ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG);
        }
    }

    /**
     * Check that the given properties contain only valid share group config names and that all values can be
     * parsed and are valid.
     */
    public static void validate(Properties props, ShareGroupConfig shareGroupConfig) {
        Map<?, ?> valueMaps = CONFIG.parse(props);
        validateValues(valueMaps, shareGroupConfig);
    }

    /**
     * Create a group config instance using the given properties and defaults.
     */
    public static ShareGroupDynamicConfig fromProps(Map<?, ?> defaults, Properties overrides) {
        Properties props = new Properties();
        props.putAll(defaults);
        props.putAll(overrides);
        return new ShareGroupDynamicConfig(props);
    }

    /**
     * The share group record lock duration in milliseconds
     */
    public int shareRecordLockDurationMs() {
        return shareRecordLockDurationMs;
    }
}
