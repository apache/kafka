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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.network.SocketServer;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Dynamic broker configurations are stored in ZooKeeper and may be defined at two levels:
 * <ul>
 *   <li>Per-broker configs persisted at <tt>/configs/brokers/{brokerId}</tt>: These can be described/altered
 *       using AdminClient using the resource name brokerId.</li>
 *   <li>Cluster-wide defaults persisted at <tt>/configs/brokers/&lt;default&gt;</tt>: These can be described/altered
 *       using AdminClient using an empty resource name.</li>
 * </ul>
 * The order of precedence for broker configs is:
 * <ol>
 *   <li>DYNAMIC_BROKER_CONFIG: stored in ZK at /configs/brokers/{brokerId}</li>
 *   <li>DYNAMIC_DEFAULT_BROKER_CONFIG: stored in ZK at /configs/brokers/&lt;default&gt;</li>
 *   <li>STATIC_BROKER_CONFIG: properties that broker is started up with, typically from server.properties file</li>
 *   <li>DEFAULT_CONFIG: Default configs defined in KafkaConfig</li>
 * </ol>
 * Log configs use topic config overrides if defined and fallback to broker defaults using the order of precedence above.
 * Topic config overrides may use a different config name from the default broker config.
 * See [[org.apache.kafka.storage.internals.log.LogConfig#TopicConfigSynonyms]] for the mapping.
 * <p>
 * AdminClient returns all config synonyms in the order of precedence when configs are described with
 * <code>includeSynonyms</code>. In addition to configs that may be defined with the same name at different levels,
 * some configs have additional synonyms.
 * </p>
 * <ul>
 *   <li>Listener configs may be defined using the prefix <tt>listener.name.{listenerName}.{configName}</tt>. These may be
 *       configured as dynamic or static broker configs. Listener configs have higher precedence than the base configs
 *       that don't specify the listener name. Listeners without a listener config use the base config. Base configs
 *       may be defined only as STATIC_BROKER_CONFIG or DEFAULT_CONFIG and cannot be updated dynamically.<li>
 *   <li>Some configs may be defined using multiple properties. For example, <tt>log.roll.ms</tt> and
 *       <tt>log.roll.hours</tt> refer to the same config that may be defined in milliseconds or hours. The order of
 *       precedence of these synonyms is described in the docs of these configs in [[kafka.server.KafkaConfig]].</li>
 * </ul>
 *
 */
public abstract class DynamicBrokerConfigBaseManager implements DynamicBrokerConfigManager {
    public final static Set<String> RECONFIGURABLE_CONFIGS = SslConfigs.RECONFIGURABLE_CONFIGS;
    public final static Set<String> allDynamicConfigs() {
        return ALL_DYNAMIC_CONFIGS;
    }
    private final static Set<String> ALL_DYNAMIC_CONFIGS = new HashSet<String>() {{
            addAll(RECONFIGURABLE_CONFIGS);
            addAll(LogCleanerConfig.RECONFIGURABLE_CONFIGS);
            addAll(JDynamicLogConfig.getReconfigurableConfigs());
            addAll(JDynamicThreadPool.RECONFIGURABLE_CONFIGS);
            add(KafkaConfig.METRIC_REPORTER_CLASSES_PROP);
            addAll(JDynamicListenerConfig.RECONFIGURABLE_CONFIGS);
            addAll(SocketServer.RECONFIGURABLE_CONFIGS);
            addAll(ProducerStateManagerConfig.RECONFIGURABLE_CONFIGS);
            addAll(JDynamicRemoteLogConfig.RECONFIGURABLE_CONFIGS);
        }};

    public final static Set<String> CLUSTER_LEVEL_LISTENER_CONFIGS = Utils.mkSet(
            KafkaConfig.MAX_CONNECTIONS_PROP,
            KafkaConfig.MAX_CONNECTION_CREATION_RATE_PROP,
            KafkaConfig.NUM_NETWORK_THREADS_PROP
    );

    private final static Set<String> PER_BROKER_CONFIGS = new HashSet<String>() {{
            addAll(RECONFIGURABLE_CONFIGS);
            addAll(JDynamicListenerConfig.RECONFIGURABLE_CONFIGS);
            removeAll(CLUSTER_LEVEL_LISTENER_CONFIGS);
        }};

    public final static Set<String> LISTENER_MECHANISM_CONFIGS = Utils.mkSet(
            KafkaConfig.SASL_JAAS_CONFIG_PROP,
            KafkaConfig.SASL_LOGIN_CALLBACK_HANDLER_CLASS_PROP,
            KafkaConfig.SASL_LOGIN_CLASS_PROP,
            KafkaConfig.SASL_SERVER_CALLBACK_HANDLER_CLASS_PROP,
            KafkaConfig.CONNECTIONS_MAX_REAUTH_MS_PROP
    );

    public final static Set<String> RELOADABLE_FILE_CONFIGS = Utils.mkSet(
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG
    );

    protected final static Pattern LISTENER_CONFIG_REGEX = Pattern.compile("listener\\.name\\.[^.]*\\.(.*)");

    private static final Set<String> DYNAMIC_PASSWORD_CONFIGS = new HashSet<String>(ALL_DYNAMIC_CONFIGS) {{
            retainAll(KafkaConfig.configKeys()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().type == ConfigDef.Type.PASSWORD)
                    .map(entry -> entry.getKey())
                    .collect(Collectors.toSet()));
        }};

    protected final CopyOnWriteArrayList<Reconfigurable> reconfigurables = new CopyOnWriteArrayList<>();

    public final CopyOnWriteArrayList<Reconfigurable> reconfigurables() {
        return reconfigurables;
    }

    public static Map<String, String> dynamicConfigUpdateModes() {
        return ALL_DYNAMIC_CONFIGS.stream().collect(Collectors.toMap(
                name -> name,
                name -> PER_BROKER_CONFIGS.contains(name) ? "per-broker" : "cluster-wide"
        ));
    }

    public static List<String> brokerConfigSynonyms(String name, boolean matchListenerOverride) {
        if (Arrays.asList(KafkaConfig.LOG_ROLL_TIME_MILLIS_PROP, KafkaConfig.LOG_ROLL_TIME_HOURS_PROP).contains(name)) {
            return Arrays.asList(KafkaConfig.LOG_ROLL_TIME_MILLIS_PROP, KafkaConfig.LOG_ROLL_TIME_HOURS_PROP);
        } else if (Arrays.asList(KafkaConfig.LOG_ROLL_TIME_JITTER_MILLIS_PROP, KafkaConfig.LOG_ROLL_TIME_JITTER_HOURS_PROP).contains(name)) {
            return Arrays.asList(KafkaConfig.LOG_ROLL_TIME_JITTER_MILLIS_PROP, KafkaConfig.LOG_ROLL_TIME_JITTER_HOURS_PROP);
        } else if (name.equals(KafkaConfig.LOG_FLUSH_INTERVAL_MS_PROP)) {
            return Arrays.asList(KafkaConfig.LOG_FLUSH_INTERVAL_MS_PROP, KafkaConfig.LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP);
        } else if (Arrays.asList(
                KafkaConfig.LOG_RETENTION_TIME_MILLIS_PROP,
                KafkaConfig.LOG_RETENTION_TIME_MINUTES_PROP,
                KafkaConfig.LOG_RETENTION_TIME_HOURS_PROP
        ).contains(name)) {
            return Arrays.asList(
                    KafkaConfig.LOG_RETENTION_TIME_MILLIS_PROP,
                    KafkaConfig.LOG_RETENTION_TIME_MINUTES_PROP,
                    KafkaConfig.LOG_RETENTION_TIME_HOURS_PROP
            );
        } else if (matchListenerOverride && name.matches(LISTENER_CONFIG_REGEX.pattern())) {
            Matcher matcher = LISTENER_CONFIG_REGEX.matcher(name);
            if (matcher.matches()) {
                String baseName = matcher.group(1);
                Optional<String> mechanismConfig = LISTENER_MECHANISM_CONFIGS.stream()
                        .filter(config -> baseName.endsWith(config))
                        .findFirst();
                return Arrays.asList(name, mechanismConfig.orElse(baseName));
            } else {
                return Collections.singletonList(name);
            }
        } else {
            return Collections.singletonList(name);
        }
    }

    public static ConfigDef addDynamicConfigs(ConfigDef configDef) {
        KafkaConfig.configKeys().entrySet().forEach(entry -> {
            String configName = entry.getKey();
            ConfigDef.ConfigKey config = entry.getValue();
            if (ALL_DYNAMIC_CONFIGS.contains(configName)) {
                configDef.define(config.name, config.type, config.defaultValue, config.validator, config.importance,
                        config.documentation, config.group, config.orderInGroup, config.width, config.displayName,
                        config.dependents, config.recommender);
            }
        });
        return configDef;
    }

    public static boolean isPasswordConfig(String name) {
        return DYNAMIC_PASSWORD_CONFIGS.stream().anyMatch(name::endsWith);
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

    public static void checkInvalidProps(Set<String> invalidPropNames, String errorMessage) {
        if (!invalidPropNames.isEmpty()) {
            throw new ConfigException(errorMessage + ": " + invalidPropNames);
        }
    }

    public static Set<String> nonDynamicConfigs(Properties props) {
        Set<String> dynamicConfigSet = DynamicConfig.Broker.nonDynamicProps();
        Set<String> propsSet = new HashSet<>(props.stringPropertyNames());
        propsSet.retainAll(dynamicConfigSet);

        return propsSet;
    }

    public static Set<String> securityConfigsWithoutListenerPrefix(Properties props) {
        return RECONFIGURABLE_CONFIGS.stream().filter(conf -> props.containsKey(conf)).collect(Collectors.toSet());
    }

    public static void validateConfigTypes(Properties props) {
        Properties baseProps = new Properties();

        props.forEach((k, v) -> {
            Matcher matcher = LISTENER_CONFIG_REGEX.matcher((String) k);
            if (matcher.matches()) {
                String baseName = matcher.group(1);
                baseProps.put(baseName, v);
            } else {
                baseProps.put(k, v);
            }
        });

        DynamicConfig.Broker.validate(baseProps);
    }

    public static Properties resolveVariableConfigs(Properties propsOriginal) {
        Properties props = new Properties();
        AbstractConfig config = new AbstractConfig(new ConfigDef(), propsOriginal, false);
        config.originals().entrySet().stream().forEach(entry -> {
            if (!entry.getKey().startsWith(AbstractConfig.CONFIG_PROVIDERS_CONFIG)) {
                props.put(entry.getKey(), entry.getValue());
            }
        });

        return props;
    }

    public static Set<String> perBrokerConfigs(Properties props) {
        Set<String> configNames = new HashSet<>(props.stringPropertyNames());
        Set<String> result = new HashSet<>();

        for (String name : configNames) {
            if (PER_BROKER_CONFIGS.contains(name)) {
                result.add(name);
            } else if (perBrokerListenerConfig(name)) {
                result.add(name);
            }
        }

        return result;
    }

    public static boolean perBrokerListenerConfig(String name) {
        // Implement logic to check if it's a per-broker listener config
        return name.matches(LISTENER_CONFIG_REGEX.pattern()) &&
                !CLUSTER_LEVEL_LISTENER_CONFIGS.contains(name);
    }
}

