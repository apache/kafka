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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.share.ShareGroupConfig;
import org.apache.kafka.server.share.ShareGroupDynamicConfig;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

public class DynamicGroupConfig {
    public static final ConfigDef CONFIG_DEF =  Utils.mergeConfigs(Arrays.asList(
        ConsumerGroupDynamicConfig.configDef(),
        ShareGroupDynamicConfig.configDef()
    ));

    ConsumerGroupDynamicConfig consumerGroupDynamicConfig;
    ShareGroupDynamicConfig shareGroupDynamicConfig;

    @SuppressWarnings("this-escape")
    public DynamicGroupConfig(
        ConsumerGroupDynamicConfig consumerGroupDynamicConfig,
        ShareGroupDynamicConfig shareGroupDynamicConfig
    ) {
        this.consumerGroupDynamicConfig = consumerGroupDynamicConfig;
        this.shareGroupDynamicConfig = shareGroupDynamicConfig;
    }

    public static ConfigDef configDef() {
        return CONFIG_DEF;
    }

    public static Optional<ConfigDef.Type> configType(String configName) {
        if (ConsumerGroupDynamicConfig.configType(configName).isPresent())
            return ConsumerGroupDynamicConfig.configType(configName);
        else
            return ShareGroupDynamicConfig.configType(configName);
    }

    /**
     * Check that the given properties contain only valid dynamic group config names and that all values can be
     * parsed and are valid.
     */
    public static void validate(
        Properties props,
        GroupCoordinatorConfig groupCoordinatorConfig,
        ShareGroupConfig shareGroupConfig
    ) {
        Properties consumerGroupProps = new Properties();
        Properties shareGroupProps = new Properties();
        props.forEach((key, value) -> {
            if (ConsumerGroupDynamicConfig.isConsumerGroupConfig((String) key)) {
                consumerGroupProps.put(key, value);
            } else if (ShareGroupDynamicConfig.isShareGroupConfig((String) key)) {
                shareGroupProps.put(key, value);
            } else {
                throw new InvalidConfigurationException("Unknown group config name: " + key);
            }
        });
        ConsumerGroupDynamicConfig.validate(consumerGroupProps, groupCoordinatorConfig);
        ShareGroupDynamicConfig.validate(shareGroupProps, shareGroupConfig);
    }

    public String documentationOf(String key) {
        if (ConsumerGroupDynamicConfig.isConsumerGroupConfig(key)) {
            return consumerGroupDynamicConfig.documentationOf(key);
        }
        return shareGroupDynamicConfig.documentationOf(key);
    }
}
