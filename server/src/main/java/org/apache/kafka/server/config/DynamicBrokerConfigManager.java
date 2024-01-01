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

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.ReconfigurableServer;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.metrics.ClientMetricsReceiverPlugin;
import org.apache.kafka.zk.KafkaZKClient;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

public interface DynamicBrokerConfigManager {
    Map<String, String> staticBrokerConfigs();

    Map<String, String> staticDefaultConfigs();

    void addReconfigurable(Reconfigurable reconfigurable);

    void addBrokerReconfigurable(BrokerReconfigurable reconfigurable);

    void addReconfigurables(ReconfigurableServer server);

    void removeReconfigurable(Reconfigurable reconfigurable);

    void updateDefaultConfig(Properties persistentProps, Boolean doLog);

    void updateBrokerConfig(int brokerId, Properties persistentProps, Boolean doLog);

    Map<String, String> currentDynamicDefaultConfigs();

    Map<String, String> currentDynamicBrokerConfigs();

    KafkaConfig currentKafkaConfig();

    void clear();

    Properties toPersistentProps(Properties configProps, Boolean perBrokerConfig);

    void reloadUpdatedFilesWithoutConfigChange(Properties newProps);

    void maybeReconfigure(Reconfigurable reconfigurable, KafkaConfig oldConfig, Map<String, ?> newConfig);

    void initialize(Optional<KafkaZKClient> zkClientOpt, Optional<ClientMetricsReceiverPlugin> clientMetricsReceiverPluginOpt);

    Properties fromPersistentProps(Properties persistentProps, Boolean perBrokerConfig);

    void validate(Properties props, Boolean perBrokerConfig);

    Optional<ClientMetricsReceiverPlugin> clientMetricsReceiverPlugin();

    abstract class JDynamicLogConfig implements BrokerReconfigurable {
        // Exclude message.format.version for now since we need to check that the version
        // is supported on all brokers in the cluster.
        @SuppressWarnings("deprecation")
        public static final Set<String> EXCLUDED_CONFIGS = Collections.singleton(KafkaConfig.LOG_MESSAGE_FORMAT_VERSION_PROP);
        private static final Set<String> RECONFIGURABLE_CONFIGS = new HashSet<>(ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.values());
        public static final Map<String, String> KAFKA_CONFIG_TO_LOG_CONFIG_NAME = ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS;

        public static final Set<String> getReconfigurableConfigs() {
            return RECONFIGURABLE_CONFIGS;
        }
    }

    abstract class JDynamicThreadPool implements BrokerReconfigurable {
        public static final Set<String> RECONFIGURABLE_CONFIGS = Utils.mkSet(
                KafkaConfig.NUM_IO_THREADS_PROP,
                KafkaConfig.NUM_REPLICA_FETCHERS_PROP,
                KafkaConfig.NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP,
                KafkaConfig.BACKGROUND_THREADS_PROP
        );

        public static void validateReconfiguration(KafkaConfig currentConfig, KafkaConfig newConfig) {
            newConfig.values().forEach((k, v) -> {
                if (RECONFIGURABLE_CONFIGS.contains(k)) {
                    int newValue = (int) v;
                    int oldValue = getValue(currentConfig, k);
                    if (newValue != oldValue) {
                        String errorMsg = String.format("Dynamic thread count update validation failed for %s=%s", k, v);
                        if (newValue <= 0) {
                            throw new ConfigException(String.format("%s, value should be at least 1", errorMsg));
                        }
                        if (newValue < oldValue / 2) {
                            throw new ConfigException(String.format("%s, value should be at least half the current value %s", errorMsg, oldValue));
                        }
                        if (newValue > oldValue * 2) {
                            throw new ConfigException(String.format("%s, value should not be greater than double the current value %s", errorMsg, oldValue));
                        }
                    }
                }
            });
        }

        public static int getValue(KafkaConfig config, String name) {
            switch (name) {
                case KafkaConfig.NUM_IO_THREADS_PROP:
                    return config.numIoThreads();
                case KafkaConfig.NUM_REPLICA_FETCHERS_PROP:
                    return config.numReplicaFetchers();
                case KafkaConfig.NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP:
                    return config.numRecoveryThreadsPerDataDir();
                case KafkaConfig.BACKGROUND_THREADS_PROP:
                    return config.backgroundThreads();
                default:
                    throw new IllegalStateException("Unexpected config: " + name);
            }
        }
    }

    abstract class JDynamicListenerConfig implements BrokerReconfigurable {
        /**
         * The set of configurations which the DynamicListenerConfig object listens for. Many of
         * these are also monitored by other objects such as ChannelBuilders and SocketServers.
         */
        public static final Set<String> RECONFIGURABLE_CONFIGS = Utils.mkSet(
                // Listener configs
                KafkaConfig.ADVERTISED_LISTENERS_PROP,
                KafkaConfig.LISTENERS_PROP,
                KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP,

                // SSL configs
                KafkaConfig.PRINCIPAL_BUILDER_CLASS_PROP,
                KafkaConfig.SSL_PROTOCOL_PROP,
                KafkaConfig.SSL_PROVIDER_PROP,
                KafkaConfig.SSL_CIPHER_SUITES_PROP,
                KafkaConfig.SSL_ENABLED_PROTOCOLS_PROP,
                KafkaConfig.SSL_KEYSTORE_TYPE_PROP,
                KafkaConfig.SSL_KEYSTORE_LOCATION_PROP,
                KafkaConfig.SSL_KEYSTORE_PASSWORD_PROP,
                KafkaConfig.SSL_KEY_PASSWORD_PROP,
                KafkaConfig.SSL_TRUSTSTORE_TYPE_PROP,
                KafkaConfig.SSL_TRUSTSTORE_LOCATION_PROP,
                KafkaConfig.SSL_TRUSTSTORE_PASSWORD_PROP,
                KafkaConfig.SSL_KEY_MANAGER_ALGORITHM_PROP,
                KafkaConfig.SSL_TRUST_MANAGER_ALGORITHM_PROP,
                KafkaConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP,
                KafkaConfig.SSL_SECURE_RANDOM_IMPLEMENTATION_PROP,
                KafkaConfig.SSL_CLIENT_AUTH_PROP,
                KafkaConfig.SSL_ENGINE_FACTORY_CLASS_PROP,

                // SASL configs
                KafkaConfig.SASL_MECHANISM_INTER_BROKER_PROTOCOL_PROP,
                KafkaConfig.SASL_JAAS_CONFIG_PROP,
                KafkaConfig.SASL_ENABLED_MECHANISMS_PROP,
                KafkaConfig.SASL_KERBEROS_SERVICE_NAME_PROP,
                KafkaConfig.SASL_KERBEROS_KINIT_CMD_PROP,
                KafkaConfig.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_PROP,
                KafkaConfig.SASL_KERBEROS_TICKET_RENEW_JITTER_PROP,
                KafkaConfig.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_PROP,
                KafkaConfig.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_PROP,
                KafkaConfig.SASL_LOGIN_REFRESH_WINDOW_FACTOR_PROP,
                KafkaConfig.SASL_LOGIN_REFRESH_WINDOW_JITTER_PROP,
                KafkaConfig.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_PROP,
                KafkaConfig.SASL_LOGIN_REFRESH_BUFFER_SECONDS_PROP,

                // Connection limit configs
                KafkaConfig.MAX_CONNECTIONS_PROP,
                KafkaConfig.MAX_CONNECTION_CREATION_RATE_PROP,

                // Network threads
                KafkaConfig.NUM_NETWORK_THREADS_PROP
        );

    }

    abstract class JDynamicRemoteLogConfig implements BrokerReconfigurable {
        public static final Set<String> RECONFIGURABLE_CONFIGS = Utils.mkSet(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP);
    }

    interface BrokerReconfigurable {
        Set<String> reconfigurableConfigs();

        void validateReconfiguration(KafkaConfig newConfig);

        void reconfigure(KafkaConfig oldConfig, KafkaConfig newConfig);
    }
}
