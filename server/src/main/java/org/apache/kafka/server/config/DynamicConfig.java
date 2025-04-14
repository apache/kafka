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
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.DynamicThreadPool;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.storage.internals.log.LogCleaner;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public class DynamicConfig {
    public static final Set<String> ALL_DYNAMIC_CONFIGS;

    static {
        Set<String> allDynamicConfigs = new HashSet<>(SslConfigs.RECONFIGURABLE_CONFIGS);
        allDynamicConfigs.addAll(LogCleaner.RECONFIGURABLE_CONFIGS);
        allDynamicConfigs.addAll(ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.values());
        allDynamicConfigs.addAll(DynamicThreadPool.RECONFIGURABLE_CONFIGS);
        allDynamicConfigs.add(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG);
        allDynamicConfigs.addAll(Set.of(
            // Listener configs
            SocketServerConfigs.LISTENERS_CONFIG,
            SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,

            // SSL configs
            BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG,
            SslConfigs.SSL_PROTOCOL_CONFIG,
            SslConfigs.SSL_PROVIDER_CONFIG,
            SslConfigs.SSL_CIPHER_SUITES_CONFIG,
            SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_KEY_PASSWORD_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
            SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
            SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
            SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
            BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG,
            SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG,

            // SASL configs
            BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG,
            SaslConfigs.SASL_JAAS_CONFIG,
            BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
            SaslConfigs.SASL_KERBEROS_SERVICE_NAME,
            SaslConfigs.SASL_KERBEROS_KINIT_CMD,
            SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR,
            SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER,
            SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN,
            BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG,
            SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR,
            SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER,
            SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
            SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS,

            // Connection limit configs
            SocketServerConfigs.MAX_CONNECTIONS_CONFIG,
            SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG,

            // Network threads
            SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG
        ));
        allDynamicConfigs.addAll(Set.of(
            TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG,
            TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG
        ));
        allDynamicConfigs.addAll(Set.of(
            SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG,
            SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG,
            SocketServerConfigs.MAX_CONNECTIONS_CONFIG,
            SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG
        ));
        allDynamicConfigs.addAll(Set.of(
            RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP,
            RemoteLogManagerConfig.REMOTE_FETCH_MAX_WAIT_MS_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP,
            RemoteLogManagerConfig.REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP
        ));
        ALL_DYNAMIC_CONFIGS = Collections.unmodifiableSet(allDynamicConfigs);
    }

    public static class Broker {
        private static final ConfigDef BROKER_CONFIGS;

        static {
            ConfigDef configs = QuotaConfig.brokerQuotaConfigs();

            // Filter and define all dynamic configurations
            AbstractKafkaConfig.CONFIG_DEF.configKeys().forEach((configName, value) -> {
                if (ALL_DYNAMIC_CONFIGS.contains(configName)) {
                    configs.define(value);
                }
            });
            BROKER_CONFIGS = configs;
        }

        // In order to avoid circular reference, all DynamicBrokerConfig's variables which are initialized by
        // `DynamicConfig.Broker` should be moved to `DynamicConfig.Broker`.
        // Otherwise, those variables of DynamicBrokerConfig will see intermediate state of `DynamicConfig.Broker`,
        // because `brokerConfigs` is created by `DynamicBrokerConfig.AllDynamicConfigs`

        public static final Set<String> NON_DYNAMIC_PROPS;

        static {
            Set<String> props = new TreeSet<>(AbstractKafkaConfig.CONFIG_DEF.names());
            props.removeAll(BROKER_CONFIGS.names());
            NON_DYNAMIC_PROPS = Collections.unmodifiableSet(props);
        }

        public static Map<String, ConfigDef.ConfigKey> configKeys() {
            return BROKER_CONFIGS.configKeys();
        }

        public static Set<String> names() {
            return BROKER_CONFIGS.names();
        }

        public static Map<String, Object> validate(Properties props) {
            return DynamicConfig.validate(BROKER_CONFIGS, props, true);
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
