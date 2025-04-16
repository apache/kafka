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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class DynamicConfig {


    public static class Client {
        private static final ConfigDef CLIENT_CONFIGS = QuotaConfig.userAndClientQuotaConfigs();

        public static Map<String, ConfigDef.ConfigKey> configKeys() {
            return CLIENT_CONFIGS.configKeys();
        }

        public static Set<String> names() {
            return CLIENT_CONFIGS.names();
        }

        public static Map<String, Object> validate(Properties props) {
            return DynamicConfig.validate(CLIENT_CONFIGS, props, false);
        }
    }

    public static class User {
        private static final ConfigDef USER_CONFIGS = QuotaConfig.scramMechanismsPlusUserAndClientQuotaConfigs();

        public static Map<String, ConfigDef.ConfigKey> configKeys() {
            return USER_CONFIGS.configKeys();
        }

        public static Set<String> names() {
            return USER_CONFIGS.names();
        }

        public static Map<String, Object> validate(Properties props) {
            return DynamicConfig.validate(USER_CONFIGS, props, false);
        }
    }

    private static Map<String, Object> validate(ConfigDef configDef, Properties props, boolean customPropsAllowed) {
        // Validate Names
        Set<String> names = configDef.names();
        Set<String> propKeys = new HashSet<>();
        for (Object key : props.keySet()) {
            propKeys.add((String) key);
        }

        if (!customPropsAllowed) {
            Set<String> unknownKeys = new HashSet<>(propKeys);
            unknownKeys.removeAll(names);
            if (!unknownKeys.isEmpty()) {
                throw new IllegalArgumentException("Unknown Dynamic Configuration: " + unknownKeys);
            }
        }
        Properties propResolved = resolveVariableConfigs(props);
        // Validate Values
        return configDef.parse(propResolved);
    }

    private static Properties resolveVariableConfigs(Properties propsOriginal) {
        Properties props = new Properties();
        AbstractConfig config = new AbstractConfig(new ConfigDef(), propsOriginal,
            Utils.castToStringObjectMap(propsOriginal), false);
        config.originals().forEach((key, value) -> {
            if (!key.startsWith(AbstractConfig.CONFIG_PROVIDERS_CONFIG)) {
                props.put(key, value);
            }
        });
        return props;
    }
}
