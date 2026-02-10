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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.network.SocketServer;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.DynamicThreadPool;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.storage.internals.log.LogCleaner;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DynamicBrokerConfig {

    public static final Set<String> DYNAMIC_SECURITY_CONFIGS = SslConfigs.RECONFIGURABLE_CONFIGS;

    private static final Set<String> DYNAMIC_PRODUCER_STATE_MANAGER_CONFIGS = Set.of(
            TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG,
            TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG);

    private static final Set<String> CLUSTER_LEVEL_LISTENER_CONFIGS = Set.of(
            SocketServerConfigs.MAX_CONNECTIONS_CONFIG,
            SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG,
            SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG);

    private static final Set<String> PER_BROKER_CONFIGS = Stream.of(
            DYNAMIC_SECURITY_CONFIGS,
            DynamicListenerConfig.RECONFIGURABLE_CONFIGS)
        .flatMap(Collection::stream)
        .filter(c -> !CLUSTER_LEVEL_LISTENER_CONFIGS.contains(c))
        .collect(Collectors.toUnmodifiableSet());

    public static final Set<String> ALL_DYNAMIC_CONFIGS = Stream.of(
            DYNAMIC_SECURITY_CONFIGS,
            LogCleaner.RECONFIGURABLE_CONFIGS,
            DynamicLogConfig.RECONFIGURABLE_CONFIGS,
            DynamicThreadPool.RECONFIGURABLE_CONFIGS,
            List.of(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG),
            DynamicListenerConfig.RECONFIGURABLE_CONFIGS,
            SocketServer.RECONFIGURABLE_CONFIGS,
            DYNAMIC_PRODUCER_STATE_MANAGER_CONFIGS,
            DynamicRemoteLogConfig.RECONFIGURABLE_CONFIGS,
            DynamicReplicationConfig.RECONFIGURABLE_CONFIGS,
            List.of(AbstractConfig.CONFIG_PROVIDERS_CONFIG),
            GroupCoordinatorConfig.RECONFIGURABLE_CONFIGS,
            ShareCoordinatorConfig.RECONFIGURABLE_CONFIGS, 
            QuotaConfig.BROKER_QUOTA_CONFIGS)
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableSet());

    private static final Set<String> LISTENER_MECHANISM_CONFIGS = Set.of(
            SaslConfigs.SASL_JAAS_CONFIG,
            SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
            SaslConfigs.SASL_LOGIN_CLASS,
            BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG,
            BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG);

    private static final Pattern LISTENER_CONFIG_REGEX = Pattern.compile("listener\\.name\\.[^.]*\\.(.*)");

    public static List<String> brokerConfigSynonyms(String name, boolean matchListenerOverride) {
        List<String> logRollConfigs = List.of(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG);
        List<String> logRollJitterConfigs = List.of(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG, ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG);
        List<String> logRetentionConfigs = List.of(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG);
        List<String> logFlushConfigs = List.of(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG, ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG);
        if (logRollConfigs.contains(name)) {
            return logRollConfigs;
        } else if (logRollJitterConfigs.contains(name)) {
            return logRollJitterConfigs;
        } else if (ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG.equals(name)) { // KafkaLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG is used as default
            return logFlushConfigs;
        } else if (logRetentionConfigs.contains(name)) {
            return logRetentionConfigs;
        } else if (matchListenerOverride) {
            Matcher matcher = LISTENER_CONFIG_REGEX.matcher(name);
            if (matcher.matches()) {
                String baseName = matcher.group(1);
                // `ListenerMechanismConfigs` are specified as listenerPrefix.mechanism.<configName>
                // and other listener configs are specified as listenerPrefix.<configName>
                // Add <configName> as a synonym in both cases.
                Optional<String> mechanismConfig = LISTENER_MECHANISM_CONFIGS.stream().filter(baseName::endsWith).findFirst();
                return List.of(name, mechanismConfig.orElse(baseName));
            }
        }
        return List.of(name);
    }

    private static void checkInvalidProps(Set<String> invalidPropNames, String errorMessage) {
        if (!invalidPropNames.isEmpty()) {
            throw new ConfigException(errorMessage + ": " + invalidPropNames);
        }
    }

    public static void validateConfigs(Properties props, boolean perBrokerConfig) {
        checkInvalidProps(nonDynamicConfigs(props), "Cannot update these configs dynamically");
        checkInvalidProps(securityConfigsWithoutListenerPrefix(props),
                "These security configs can be dynamically updated only per-listener using the listener prefix");
        validateConfigTypes(props);
        if (!perBrokerConfig) {
            checkInvalidProps(perBrokerConfigs(props),
                    "Cannot update these configs at default cluster level, broker id must be specified");
        }
    }

    public static Set<String> securityConfigsWithoutListenerPrefix(Properties props) {
        return DYNAMIC_SECURITY_CONFIGS.stream().filter(props::containsKey).collect(Collectors.toSet());
    }

    public static void validateConfigTypes(Properties props) {
        Properties baseProps = new Properties();
        props.forEach((name, value) -> {
            Matcher matcher = LISTENER_CONFIG_REGEX.matcher((String) name);
            if (matcher.matches()) {
                String baseName = matcher.group(1);
                baseProps.put(baseName, value);
            } else {
                baseProps.put(name, value);
            }
        });
        DynamicConfig.Broker.validate(baseProps);
    }

    public static Set<String> perBrokerConfigs(Properties props) {
        Set<String> configNames = props.stringPropertyNames();
        Set<String> perBrokerConfigs = new HashSet<>();
        for (String name : configNames) {
            if (PER_BROKER_CONFIGS.contains(name)) {
                perBrokerConfigs.add(name);
            } else {
                Matcher matcher = LISTENER_CONFIG_REGEX.matcher(name);
                if (matcher.matches()) {
                    String baseName = matcher.group(1);
                    if (!CLUSTER_LEVEL_LISTENER_CONFIGS.contains(baseName)) {
                        perBrokerConfigs.add(name);
                    }
                }
            }
        }
        return perBrokerConfigs;
    }

    public static Set<String> nonDynamicConfigs(Properties props) {
        Set<String> nonDynamicConfigs = new HashSet<>(props.stringPropertyNames());
        nonDynamicConfigs.retainAll(DynamicConfig.Broker.nonDynamicProps());
        return nonDynamicConfigs;
    }

    public static Properties resolveVariableConfigs(Properties propsOriginal) {
        Properties props = new Properties();
        AbstractConfig config = new AbstractConfig(new ConfigDef(), propsOriginal, Utils.castToStringObjectMap(propsOriginal), false);
        config.originals().forEach((key, value) -> {
            if (!key.startsWith(AbstractConfig.CONFIG_PROVIDERS_CONFIG)) {
                props.put(key, value);
            }
        });
        return props;
    }

    public static Map<String, String> dynamicConfigUpdateModes() {
        return ALL_DYNAMIC_CONFIGS.stream().collect(Collectors.toMap(
                Function.identity(),
                name -> PER_BROKER_CONFIGS.contains(name) ? "per-broker" : "cluster-wide"
            )
        );
    }

    public static class DynamicLogConfig {
        /**
         * The broker configurations pertaining to logs that are reconfigurable. This set contains
         * the names you would use when setting a static or dynamic broker configuration (not topic
         * configuration).
         */
        public static final Set<String> RECONFIGURABLE_CONFIGS = Set.copyOf(
                ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.values());
    }

    public static class DynamicListenerConfig {
        /**
         * The set of configurations which the DynamicListenerConfig object listens for. Many of
         * these are also monitored by other objects such as ChannelBuilders and SocketServers.
         */
        public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
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
                SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG);
    }

    public static class DynamicRemoteLogConfig {
        public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
                RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP,
                RemoteLogManagerConfig.REMOTE_FETCH_MAX_WAIT_MS_PROP,
                RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP,
                RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP,
                RemoteLogManagerConfig.REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_PROP,
                RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP,
                RemoteLogManagerConfig.REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP,
                RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_PROP,
                RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP);
    }

    public static class DynamicReplicationConfig {
        public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
                ReplicationConfigs.FOLLOWER_FETCH_LAST_TIERED_OFFSET_ENABLE_CONFIG);
    }
}
