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
import org.apache.kafka.storage.internals.log.LogConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;

public class DynamicConfig {
    protected static Map<String, ?> validate(ConfigDef configDef, Properties props, boolean customPropsAllowed) {
        // Validate Names
        Set<String> names = configDef.names();
        Set<String> propKeys = props.stringPropertyNames();

        if (!customPropsAllowed) {
            Set<String> unknownKeys = propKeys.stream()
                    .filter(key -> !names.contains(key))
                    .collect(Collectors.toSet());

            if (!unknownKeys.isEmpty()) {
                throw new IllegalArgumentException("Unknown Dynamic Configuration: " + unknownKeys);
            }
        }

        Properties propResolved = DynamicBrokerConfigBaseManager.resolveVariableConfigs(props);
        // ValidateValues
        Map<String, String> propResolvedMap = propResolved.entrySet().stream().collect(Collectors.toMap(e -> (String) e.getKey(), e -> (String) e.getValue()));
        return configDef.parse(propResolvedMap);
    }

    public static class Broker {
        // Properties
        public static final String LEADER_REPLICATION_THROTTLED_RATE_PROP = "leader.replication.throttled.rate";
        public static final String FOLLOWER_REPLICATION_THROTTLED_RATE_PROP = "follower.replication.throttled.rate";
        public static final String REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP = "replica.alter.log.dirs.io.max.bytes.per.second";

        // Defaults
        public static final long DEFAULT_REPLICATION_THROTTLED_RATE = ReplicationQuotaManagerConfig.DEFAULT_QUOTA_BYTES_PER_SECOND;

        // Documentation
        public static final String LEADER_REPLICATION_THROTTLED_RATE_DOC = "A long representing the upper bound (bytes/sec) on replication traffic for leaders enumerated in the "
                + "property " + LogConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG + " (for each topic). This property can be only set dynamically. It is suggested that the "
                + "limit be kept above 1MB/s for accurate behaviour.";
        public static final String FOLLOWER_REPLICATION_THROTTLED_RATE_DOC = "A long representing the upper bound (bytes/sec) on replication traffic for followers enumerated in the "
                + "property " + LogConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG + " (for each topic). This property can be only set dynamically. It is suggested that the "
                + "limit be kept above 1MB/s for accurate behaviour.";
        public static final String REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_DOC = "A long representing the upper bound (bytes/sec) on disk IO used for moving replica between log directories on the same broker. "
                + "This property can be only set dynamically. It is suggested that the limit be kept above 1MB/s for accurate behaviour.";

        // Definitions

        protected final static ConfigDef BROKER_CONFIG_DEF = DynamicBrokerConfigBaseManager.addDynamicConfigs(new ConfigDef()
                // Round minimum value down, to make it easier for users.
                .define(LEADER_REPLICATION_THROTTLED_RATE_PROP, LONG, DEFAULT_REPLICATION_THROTTLED_RATE, atLeast(0), MEDIUM, LEADER_REPLICATION_THROTTLED_RATE_DOC)
                .define(FOLLOWER_REPLICATION_THROTTLED_RATE_PROP, LONG, DEFAULT_REPLICATION_THROTTLED_RATE, atLeast(0), MEDIUM, FOLLOWER_REPLICATION_THROTTLED_RATE_DOC)
                .define(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP, LONG, DEFAULT_REPLICATION_THROTTLED_RATE, atLeast(0), MEDIUM, REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_DOC)
        );

        private static final Set<String> NON_DYNAMIC_PROPS = new HashSet<String>() {{
                addAll(KafkaConfig.configNames());
                removeAll(BROKER_CONFIG_DEF.names());
            }};

        public static Set<String> names() {
            return BROKER_CONFIG_DEF.names();
        }

        public static Map<String, ?> validate(Properties props) {
            return DynamicConfig.validate(BROKER_CONFIG_DEF, props, true);
        }

        public static Set<String> nonDynamicProps() {
            return KafkaConfig.configNames().stream()
                    .filter(name -> !BROKER_CONFIG_DEF.names().contains(name))
                    .collect(Collectors.toSet());
        }
    }

    public static class QuotaConfigs {
        public static Boolean isClientOrUserQuotaConfig(String name) {
            return org.apache.kafka.common.config.internals.QuotaConfigs.isClientOrUserConfig(name);  
        } 
    }

    public static class Client {
        private static ConfigDef clientConfigs = org.apache.kafka.common.config.internals.QuotaConfigs.userAndClientQuotaConfigs();

        public static Map<String, ConfigDef.ConfigKey> configKeys() {
            return clientConfigs.configKeys();
        }

        public static Set<String> names() {
            return clientConfigs.names();
        }

        public static Map<String, ?> validate(Properties props) {
            return DynamicConfig.validate(clientConfigs, props, false);
        }
    }

    public static class User {
        private static ConfigDef userConfigs = org.apache.kafka.common.config.internals.QuotaConfigs.scramMechanismsPlusUserAndClientQuotaConfigs();

        public static Map<String, ConfigDef.ConfigKey> configKeys() {
            return userConfigs.configKeys();
        }

        public static Set<String> names() {
            return userConfigs.names();
        }

        public static Map<String, ?> validate(Properties props) {
            return DynamicConfig.validate(userConfigs, props, false);
        }
    }

    public static class  Ip {
        private static ConfigDef ipConfigs = org.apache.kafka.common.config.internals.QuotaConfigs.ipConfigs();

        public static Map<String, ConfigDef.ConfigKey> configKeys() {
            return ipConfigs.configKeys();
        }

        public static Set<String> names() {
            return ipConfigs.names();
        }

        public static Map<String, ?> validate(Properties props) {
            return DynamicConfig.validate(ipConfigs, props, false);
        }

        public static Boolean isValidIpEntity(String ip) {
            // TODO: use ConfigEntityName.Default once it is moved out of core
            String defaultConfigEntityName = "<default>";
            if (!ip.equals(defaultConfigEntityName)) {
                try {
                    InetAddress.getByName(ip);
                } catch (UnknownHostException e) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class ClientMetrics {
        private static ConfigDef clientConfigs = org.apache.kafka.server.metrics.ClientMetricsConfigs.configDef();
        public static Set<String> names() {
            return clientConfigs.names();
        }
    }
}
