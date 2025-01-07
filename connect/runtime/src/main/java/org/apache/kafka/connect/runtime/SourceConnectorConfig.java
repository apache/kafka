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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.enumOptions;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.ExactlyOnceSupportLevel.REQUESTED;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.ExactlyOnceSupportLevel.REQUIRED;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_GROUP;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.EXCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.INCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary;
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary.CONNECTOR;
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary.DEFAULT;
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary.INTERVAL;
import static org.apache.kafka.connect.source.SourceTask.TransactionBoundary.POLL;

public final class SourceConnectorConfig extends ConnectorConfig {

    private static final Logger log = LoggerFactory.getLogger(SourceConnectorConfig.class);

    static final String TOPIC_CREATION_GROUP = "Topic Creation";

    public static final String TOPIC_CREATION_PREFIX = "topic.creation.";

    public static final String TOPIC_CREATION_GROUPS_CONFIG = TOPIC_CREATION_PREFIX + "groups";
    private static final String TOPIC_CREATION_GROUPS_DOC = "Groups of configurations for topics "
            + "created by source connectors";
    private static final String TOPIC_CREATION_GROUPS_DISPLAY = "Topic Creation Groups";

    static final String EXACTLY_ONCE_SUPPORT_GROUP = "Exactly Once Support";

    public enum ExactlyOnceSupportLevel {
        REQUESTED,
        REQUIRED;

        public static ExactlyOnceSupportLevel fromProperty(String property) {
            return valueOf(property.toUpperCase(Locale.ROOT).trim());
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static final String EXACTLY_ONCE_SUPPORT_CONFIG = "exactly.once.support";
    private static final String EXACTLY_ONCE_SUPPORT_DOC = "Permitted values are " + String.join(", ", enumOptions(ExactlyOnceSupportLevel.class)) + ". "
            + "If set to \"" + REQUIRED + "\", forces a preflight check for the connector to ensure that it can provide exactly-once semantics "
            + "with the given configuration. Some connectors may be capable of providing exactly-once semantics but not signal to "
            + "Connect that they support this; in that case, documentation for the connector should be consulted carefully before "
            + "creating it, and the value for this property should be set to \"" + REQUESTED + "\". "
            + "Additionally, if the value is set to \"" + REQUIRED + "\" but the worker that performs preflight validation does not have "
            + "exactly-once support enabled for source connectors, requests to create or validate the connector will fail.";
    private static final String EXACTLY_ONCE_SUPPORT_DISPLAY = "Exactly once support";

    public static final String TRANSACTION_BOUNDARY_CONFIG = SourceTask.TRANSACTION_BOUNDARY_CONFIG;
    private static final String TRANSACTION_BOUNDARY_DOC = "Permitted values are: " + String.join(", ", enumOptions(TransactionBoundary.class)) + ". "
            + "If set to '" + POLL + "', a new producer transaction will be started and committed for every batch of records that each task from "
            + "this connector provides to Connect. If set to '" + CONNECTOR + "', relies on connector-defined transaction boundaries; note that "
            + "not all connectors are capable of defining their own transaction boundaries, and in that case, attempts to instantiate a connector with "
            + "this value will fail. Finally, if set to '" + INTERVAL + "', commits transactions only after a user-defined time interval has passed.";
    private static final String TRANSACTION_BOUNDARY_DISPLAY = "Transaction Boundary";

    public static final String TRANSACTION_BOUNDARY_INTERVAL_CONFIG = "transaction.boundary.interval.ms";
    private static final String TRANSACTION_BOUNDARY_INTERVAL_DOC = "If '" + TRANSACTION_BOUNDARY_CONFIG + "' is set to '" + INTERVAL
            + "', determines the interval for producer transaction commits by connector tasks. If unset, defaults to the value of the worker-level "
            + "'" + WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG + "' property. It has no effect if a different "
            + TRANSACTION_BOUNDARY_CONFIG + " is specified.";
    private static final String TRANSACTION_BOUNDARY_INTERVAL_DISPLAY = "Transaction boundary interval";

    static final String OFFSETS_TOPIC_GROUP = "offsets.topic";

    public static final String OFFSETS_TOPIC_CONFIG = "offsets.storage.topic";
    private static final String OFFSETS_TOPIC_DOC = "The name of a separate offsets topic to use for this connector. "
            + "If empty or not specified, the worker’s global offsets topic name will be used. "
            + "If specified, the offsets topic will be created if it does not already exist on the Kafka cluster targeted by this connector "
            + "(which may be different from the one used for the worker's global offsets topic if the bootstrap.servers property of the connector's producer "
            + "has been overridden from the worker's). Only applicable in distributed mode; in standalone mode, setting this property will have no effect.";
    private static final String OFFSETS_TOPIC_DISPLAY = "Offsets topic";

    private static class EnrichedSourceConnectorConfig extends ConnectorConfig {
        EnrichedSourceConnectorConfig(Plugins plugins, ConfigDef configDef, Map<String, String> props) {
            super(plugins, configDef, props);
        }

    }

    private final TransactionBoundary transactionBoundary;
    private final Long transactionBoundaryInterval;
    private final EnrichedSourceConnectorConfig enrichedSourceConfig;
    private final String offsetsTopic;

    public static ConfigDef configDef() {
        ConfigDef.Validator atLeastZero = ConfigDef.Range.atLeast(0);
        int orderInGroup = 0;
        return new ConfigDef(ConnectorConfig.configDef())
                .define(
                        TOPIC_CREATION_GROUPS_CONFIG,
                        ConfigDef.Type.LIST,
                        Collections.emptyList(),
                        ConfigDef.CompositeValidator.of(
                                new ConfigDef.NonNullValidator(),
                                ConfigDef.LambdaValidator.with(
                                    (name, value) -> {
                                        List<?> groupAliases = (List<?>) value;
                                        if (groupAliases.size() > new HashSet<>(groupAliases).size()) {
                                            throw new ConfigException(name, value, "Duplicate alias provided.");
                                        }
                                    },
                                    () -> "unique topic creation groups")),
                        ConfigDef.Importance.LOW,
                        TOPIC_CREATION_GROUPS_DOC,
                        TOPIC_CREATION_GROUP,
                        ++orderInGroup,
                        ConfigDef.Width.LONG,
                        TOPIC_CREATION_GROUPS_DISPLAY)
                .define(
                        EXACTLY_ONCE_SUPPORT_CONFIG,
                        ConfigDef.Type.STRING,
                        REQUESTED.toString(),
                        ConfigDef.CaseInsensitiveValidString.in(enumOptions(ExactlyOnceSupportLevel.class)),
                        ConfigDef.Importance.MEDIUM,
                        EXACTLY_ONCE_SUPPORT_DOC,
                        EXACTLY_ONCE_SUPPORT_GROUP,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        EXACTLY_ONCE_SUPPORT_DISPLAY)
                .define(
                        TRANSACTION_BOUNDARY_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT.toString(),
                        ConfigDef.CaseInsensitiveValidString.in(enumOptions(TransactionBoundary.class)),
                        ConfigDef.Importance.MEDIUM,
                        TRANSACTION_BOUNDARY_DOC,
                        EXACTLY_ONCE_SUPPORT_GROUP,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        TRANSACTION_BOUNDARY_DISPLAY)
                .define(
                        TRANSACTION_BOUNDARY_INTERVAL_CONFIG,
                        ConfigDef.Type.LONG,
                        null,
                        ConfigDef.LambdaValidator.with(
                            (name, value) -> {
                                if (value == null) {
                                    return;
                                }
                                atLeastZero.ensureValid(name, value);
                            },
                            atLeastZero::toString
                        ),
                        ConfigDef.Importance.LOW,
                        TRANSACTION_BOUNDARY_INTERVAL_DOC,
                        EXACTLY_ONCE_SUPPORT_GROUP,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        TRANSACTION_BOUNDARY_INTERVAL_DISPLAY)
                .define(
                        OFFSETS_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.LOW,
                        OFFSETS_TOPIC_DOC,
                        OFFSETS_TOPIC_GROUP,
                        orderInGroup = 1,
                        ConfigDef.Width.LONG,
                        OFFSETS_TOPIC_DISPLAY);
    }

    public static ConfigDef embedDefaultGroup(ConfigDef baseConfigDef) {
        String defaultGroup = "default";
        ConfigDef newDefaultDef = new ConfigDef(baseConfigDef);
        newDefaultDef.embed(DEFAULT_TOPIC_CREATION_PREFIX, defaultGroup, 0, TopicCreationConfig.defaultGroupConfigDef());
        return newDefaultDef;
    }

    /**
     * Returns an enriched {@link ConfigDef} building upon the {@code ConfigDef}, using the current configuration specified in {@code props} as an input.
     *
     * @param baseConfigDef the base configuration definition to be enriched
     * @param props the non parsed configuration properties
     * @return the enriched configuration definition
     */
    public static ConfigDef enrich(ConfigDef baseConfigDef, Map<String, String> props, AbstractConfig defaultGroupConfig) {
        List<Object> topicCreationGroups = new ArrayList<>();
        Object aliases = ConfigDef.parseType(TOPIC_CREATION_GROUPS_CONFIG, props.get(TOPIC_CREATION_GROUPS_CONFIG), ConfigDef.Type.LIST);
        if (aliases instanceof List) {
            topicCreationGroups.addAll((List<?>) aliases);
        }

        //Remove "topic.creation.groups" config if its present and the value is "default"
        if (topicCreationGroups.contains(DEFAULT_TOPIC_CREATION_GROUP)) {
            log.warn("'{}' topic creation group always exists and does not need to be listed explicitly",
                DEFAULT_TOPIC_CREATION_GROUP);
            topicCreationGroups.removeAll(Collections.singleton(DEFAULT_TOPIC_CREATION_GROUP));
        }

        ConfigDef newDef = new ConfigDef(baseConfigDef);
        String defaultGroupPrefix = TOPIC_CREATION_PREFIX + DEFAULT_TOPIC_CREATION_GROUP + ".";
        short defaultGroupReplicationFactor = defaultGroupConfig.getShort(defaultGroupPrefix + REPLICATION_FACTOR_CONFIG);
        int defaultGroupPartitions = defaultGroupConfig.getInt(defaultGroupPrefix + PARTITIONS_CONFIG);
        topicCreationGroups.stream().distinct().forEach(group -> {
            if (!(group instanceof String)) {
                throw new ConfigException("Item in " + TOPIC_CREATION_GROUPS_CONFIG + " property is not of type String");
            }
            String alias = (String) group;
            String prefix = TOPIC_CREATION_PREFIX + alias + ".";
            String configGroup = TOPIC_CREATION_GROUP + ": " + alias;
            newDef.embed(prefix, configGroup, 0,
                    TopicCreationConfig.configDef(configGroup, defaultGroupReplicationFactor, defaultGroupPartitions));
        });
        return newDef;
    }

    public SourceConnectorConfig(Plugins plugins, Map<String, String> props, boolean createTopics) {
        super(plugins, configDef(), props);
        if (createTopics && props.entrySet().stream().anyMatch(e -> e.getKey().startsWith(TOPIC_CREATION_PREFIX))) {
            ConfigDef defaultConfigDef = embedDefaultGroup(configDef());
            // This config is only used to set default values for partitions and replication
            // factor from the default group and otherwise it remains unused
            AbstractConfig defaultGroup = new AbstractConfig(defaultConfigDef, props, false);

            // If the user has added regex of include or exclude patterns in the default group,
            // they should be ignored.
            Map<String, String> propsWithoutRegexForDefaultGroup = new HashMap<>(props);
            propsWithoutRegexForDefaultGroup.entrySet()
                    .removeIf(e -> e.getKey().equals(DEFAULT_TOPIC_CREATION_PREFIX + INCLUDE_REGEX_CONFIG)
                            || e.getKey().equals(DEFAULT_TOPIC_CREATION_PREFIX + EXCLUDE_REGEX_CONFIG));
            enrichedSourceConfig = new EnrichedSourceConnectorConfig(plugins,
                    enrich(defaultConfigDef, props, defaultGroup),
                    propsWithoutRegexForDefaultGroup);
        } else {
            enrichedSourceConfig = null;
        }
        transactionBoundary = TransactionBoundary.fromProperty(getString(TRANSACTION_BOUNDARY_CONFIG));
        transactionBoundaryInterval = getLong(TRANSACTION_BOUNDARY_INTERVAL_CONFIG);
        offsetsTopic = getString(OFFSETS_TOPIC_CONFIG);
    }

    public static boolean usesTopicCreation(Map<String, String> props) {
        return props.entrySet().stream().anyMatch(e -> e.getKey().startsWith(TOPIC_CREATION_PREFIX));
    }

    @Override
    public Object get(String key) {
        return enrichedSourceConfig != null ? enrichedSourceConfig.get(key) : super.get(key);
    }

    public TransactionBoundary transactionBoundary() {
        return transactionBoundary;
    }

    public Long transactionBoundaryInterval() {
        return transactionBoundaryInterval;
    }

    public String offsetsTopic() {
        return offsetsTopic;
    }

    /**
     * Returns whether this configuration uses topic creation properties.
     *
     * @return true if the configuration should be validated and used for topic creation; false otherwise
     */
    public boolean usesTopicCreation() {
        return enrichedSourceConfig != null;
    }

    public List<String> topicCreationInclude(String group) {
        return getList(TOPIC_CREATION_PREFIX + group + '.' + INCLUDE_REGEX_CONFIG);
    }

    public List<String> topicCreationExclude(String group) {
        return getList(TOPIC_CREATION_PREFIX + group + '.' + EXCLUDE_REGEX_CONFIG);
    }

    public Short topicCreationReplicationFactor(String group) {
        return getShort(TOPIC_CREATION_PREFIX + group + '.' + REPLICATION_FACTOR_CONFIG);
    }

    public Integer topicCreationPartitions(String group) {
        return getInt(TOPIC_CREATION_PREFIX + group + '.' + PARTITIONS_CONFIG);
    }

    public Map<String, Object> topicCreationOtherConfigs(String group) {
        if (enrichedSourceConfig == null) {
            return Collections.emptyMap();
        }
        return enrichedSourceConfig.originalsWithPrefix(TOPIC_CREATION_PREFIX + group + '.').entrySet().stream()
                .filter(e -> {
                    String key = e.getKey();
                    return !(INCLUDE_REGEX_CONFIG.equals(key) || EXCLUDE_REGEX_CONFIG.equals(key)
                            || REPLICATION_FACTOR_CONFIG.equals(key) || PARTITIONS_CONFIG.equals(key));
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static void main(String[] args) {
        System.out.println(configDef().toHtml(4, config -> "sourceconnectorconfigs_" + config));
    }
}
