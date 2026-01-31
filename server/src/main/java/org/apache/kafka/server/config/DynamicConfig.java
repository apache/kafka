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

import org.apache.kafka.common.config.ConfigDef;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Class used to hold dynamic configs. These are configs which have no physical manifestation in the server.properties
 * and can only be set dynamically.
 */
public class DynamicConfig {

    public static class Broker {

        private static final ConfigDef BROKER_CONFIGS;
        static {
            ConfigDef configs = QuotaConfig.brokerQuotaConfigs();
            // Filter and define all dynamic configurations
            AbstractKafkaConfig.CONFIG_DEF.configKeys().forEach((name, value) -> {
                if (DynamicBrokerConfig.ALL_DYNAMIC_CONFIGS.contains(name)) {
                    configs.define(value);
                }
            });
            BROKER_CONFIGS = configs;
        }

        // In order to avoid circular reference, all DynamicBrokerConfig's variables which are initialized by `DynamicConfig.Broker` should be moved to `DynamicConfig.Broker`.
        // Otherwise, those variables of DynamicBrokerConfig will see intermediate state of `DynamicConfig.Broker`, because `BROKER_CONFIGS` is created by `DynamicBrokerConfig.ALL_DYNAMIC_CONFIGS`
        public static Set<String> nonDynamicProps() {
            Set<String> nonDynamicProps = new HashSet<>(AbstractKafkaConfig.CONFIG_DEF.names());
            nonDynamicProps.removeAll(BROKER_CONFIGS.names());
            return nonDynamicProps;
        }

        public static Map<String, ConfigDef.ConfigKey> configKeys() {
            return BROKER_CONFIGS.configKeys();
        }

        public static Set<String> names() {
            return BROKER_CONFIGS.names();
        }

        public static Map<String, Object> validate(Properties props) {
            // Validate Names
            Properties propResolved = DynamicBrokerConfig.resolveVariableConfigs(props);
            // ValidateValues
            return BROKER_CONFIGS.parse(propResolved);
        }
    }

}
