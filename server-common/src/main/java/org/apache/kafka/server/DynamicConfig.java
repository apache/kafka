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
package org.apache.kafka.server;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.server.config.ConfigEntityName;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;

/**
 * Class used to hold dynamic configs. These are configs which have no physical manifestation in the server.properties
 * and can only be set dynamically.
 */
public class DynamicConfig {
    public static class Broker {
        // Properties
        public static final String LEADER_REPLICATION_THROTTLED_RATE_PROP = "leader.replication.throttled.rate";
        public static final String FOLLOWER_REPLICATION_THROTTLED_RATE_PROP = "follower.replication.throttled.rate";
        public static final String REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP = "replica.alter.log.dirs.io.max.bytes.per.second";

        // Defaults
        //ReplicationQuotaManagerConfig.QuotaBytesPerSecondDefault
        public static final long DEFAULT_REPLICATION_THROTTLED_RATE = Long.MAX_VALUE;

        // Documentation
        public static final String LEADER_REPLICATION_THROTTLED_RATE_DOC = "A long representing the upper bound (bytes/sec) on replication traffic for leaders enumerated in the " +
            "property leader.replication.throttled.replicas (for each topic). This property can be only set dynamically. It is suggested that the " +
            "limit be kept above 1MB/s for accurate behaviour.";
        public static final String FOLLOWER_REPLICATION_THROTTLED_RATE_DOC = "A long representing the upper bound (bytes/sec) on replication traffic for followers enumerated in the " +
            "property follower.replication.throttled.replicas (for each topic). This property can be only set dynamically. It is suggested that the " +
            "limit be kept above 1MB/s for accurate behaviour.";
        public static final String REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_DOC = "A long representing the upper bound (bytes/sec) on disk IO used for moving replica between log directories on the same broker. " +
            "This property can be only set dynamically. It is suggested that the limit be kept above 1MB/s for accurate behaviour.";

        // Definitions
        public static final ConfigDef BROKER_CONFIG_DEF = new ConfigDef()
            // Round minimum value down, to make it easier for users.
            .define(LEADER_REPLICATION_THROTTLED_RATE_PROP, LONG, DEFAULT_REPLICATION_THROTTLED_RATE, atLeast(0), MEDIUM, LEADER_REPLICATION_THROTTLED_RATE_DOC)
            .define(FOLLOWER_REPLICATION_THROTTLED_RATE_PROP, LONG, DEFAULT_REPLICATION_THROTTLED_RATE, atLeast(0), MEDIUM, FOLLOWER_REPLICATION_THROTTLED_RATE_DOC)
            .define(REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_PROP, LONG, DEFAULT_REPLICATION_THROTTLED_RATE, atLeast(0), MEDIUM, REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_DOC);

        public static final Set<String> NAMES = BROKER_CONFIG_DEF.names();

        public static void validate(Properties props) {
            DynamicConfig.validate(BROKER_CONFIG_DEF, props, true);
        }
    }

    public static class QuotaConfigs {
        public static boolean isClientOrUserQuotaConfig(String name) {
            return org.apache.kafka.common.config.internals.QuotaConfigs.isClientOrUserConfig(name);
        }
    }

    public static class Client {
        public static final ConfigDef CLIENT_CONFIGS = org.apache.kafka.common.config.internals.QuotaConfigs.userAndClientQuotaConfigs();

        public static final Map<String, ConfigDef.ConfigKey> CONFIG_KEYS = CLIENT_CONFIGS.configKeys();

        public static final Set<String> NAMES = CLIENT_CONFIGS.names();

        public static void validate(Properties props) {
            DynamicConfig.validate(CLIENT_CONFIGS, props, false);
        }
    }

    public static class User {
        public static final ConfigDef USER_CONFIGS = org.apache.kafka.common.config.internals.QuotaConfigs.scramMechanismsPlusUserAndClientQuotaConfigs();

        public static final Map<String, ConfigDef.ConfigKey> CONFIG_KEYS = USER_CONFIGS.configKeys();

        public static final Set<String> NAMES = USER_CONFIGS.names();

        public static void validate(Properties props) {
            DynamicConfig.validate(USER_CONFIGS, props, false);
        }
    }

    public static class Ip {
        public static final ConfigDef IP_CONFIGS = org.apache.kafka.common.config.internals.QuotaConfigs.ipConfigs();

        public static final Map<String, ConfigDef.ConfigKey> CONFIG_KEYS = IP_CONFIGS.configKeys();

        public static final Set<String> NAMES = IP_CONFIGS.names();

        public static void validate(Properties props) {
            DynamicConfig.validate(IP_CONFIGS, props, false);
        }

        public static boolean isValidIpEntity(String ip) {
            if (!Objects.equals(ip, ConfigEntityName.DEFAULT)) {
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
        public static final ConfigDef CLIENT_CONFIGS = org.apache.kafka.server.metrics.ClientMetricsConfigs.configDef();

        public static final Set<String> NAMES = CLIENT_CONFIGS.names();
    }

    public static void validate(ConfigDef configDef, Properties props, boolean customPropsAllowed) {
        // Validate Names
        Set<String> names = configDef.names();
        Set<String> propKeys = props.keySet().stream().map(p -> (String) p).collect(Collectors.toSet());
        if (!customPropsAllowed) {
            Set<String> unknownKeys = propKeys.stream().filter(p -> !names.contains(p)).collect(Collectors.toSet());
            if (!unknownKeys.isEmpty())
                throw new IllegalArgumentException("Unknown Dynamic Configuration: " + unknownKeys + ".");
        }

        Properties propResolved = DynamicBrokerConfig.resolveVariableConfigs(props);
        // ValidateValues
        configDef.parse(propResolved);
    }
}
