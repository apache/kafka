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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.transforms.util.RegexValidator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Configuration needed for all {@link org.apache.kafka.connect.sink.SinkConnector sink connectors}
 */

public class SinkConnectorConfig extends ConnectorConfig {

    public static final String TOPICS_CONFIG = SinkTask.TOPICS_CONFIG;
    private static final String TOPICS_DOC = "List of topics to consume, separated by commas";
    public static final List<String> TOPICS_DEFAULT = List.of();
    private static final String TOPICS_DISPLAY = "Topics";

    public static final String TOPICS_REGEX_CONFIG = SinkTask.TOPICS_REGEX_CONFIG;
    private static final String TOPICS_REGEX_DOC = "Regular expression giving topics to consume. " +
        "Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. " +
        "Only one of " + TOPICS_CONFIG + " or " + TOPICS_REGEX_CONFIG + " should be specified.";
    public static final String TOPICS_REGEX_DEFAULT = "";
    private static final String TOPICS_REGEX_DISPLAY = "Topics regex";

    public static final String CONSUMER_TYPE_CONFIG = "consumer.type";
    public static final String CONSUMER_TYPE_DEFAULT = "consumer";
    public static final String SHARE_CONSUMER_TYPE = "share";
    public static final String SHARE_GROUP_ID_CONFIG = "share.group.id";
    public static final String SHARE_PROCESSING_MAX_AGE_MS_CONFIG = "share.processing.max.age.ms";
    public static final String SHARE_PROCESSING_CANCEL_GRACE_MS_CONFIG = "share.processing.cancel.grace.ms";

    public static final String DLQ_PREFIX = "errors.deadletterqueue.";

    public static final String DLQ_TOPIC_NAME_CONFIG = DLQ_PREFIX + "topic.name";
    public static final String DLQ_TOPIC_NAME_DOC = "The name of the topic to be used as the dead letter queue (DLQ) for messages that " +
        "result in an error when processed by this sink connector, or its transformations or converters. The topic name is blank by default, " +
        "which means that no messages are to be recorded in the DLQ.";
    public static final String DLQ_TOPIC_DEFAULT = "";
    private static final String DLQ_TOPIC_DISPLAY = "Dead Letter Queue Topic Name";

    public static final String DLQ_TOPIC_REPLICATION_FACTOR_CONFIG = DLQ_PREFIX + "topic.replication.factor";
    private static final String DLQ_TOPIC_REPLICATION_FACTOR_CONFIG_DOC = "Replication factor used to create the dead letter queue topic when it doesn't already exist.";
    public static final short DLQ_TOPIC_REPLICATION_FACTOR_CONFIG_DEFAULT = 3;
    private static final String DLQ_TOPIC_REPLICATION_FACTOR_CONFIG_DISPLAY = "Dead Letter Queue Topic Replication Factor";

    public static final String DLQ_CONTEXT_HEADERS_ENABLE_CONFIG = DLQ_PREFIX + "context.headers.enable";
    public static final boolean DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT = false;
    public static final String DLQ_CONTEXT_HEADERS_ENABLE_DOC = "If true, add headers containing error context to the messages " +
            "written to the dead letter queue. To avoid clashing with headers from the original record, all error context header " +
            "keys, all error context header keys will start with <code>__connect.errors.</code>";
    private static final String DLQ_CONTEXT_HEADERS_ENABLE_DISPLAY = "Enable Error Context Headers";

    private static ConfigDef configDef(ConfigDef baseConfigs) {
        return baseConfigs
                .define(TOPICS_CONFIG, ConfigDef.Type.LIST, TOPICS_DEFAULT, ConfigDef.ValidList.anyNonDuplicateValues(true, false), ConfigDef.Importance.HIGH, TOPICS_DOC, COMMON_GROUP, 4, ConfigDef.Width.LONG, TOPICS_DISPLAY)
                .define(TOPICS_REGEX_CONFIG, ConfigDef.Type.STRING, TOPICS_REGEX_DEFAULT, new RegexValidator(), ConfigDef.Importance.HIGH, TOPICS_REGEX_DOC, COMMON_GROUP, 4, ConfigDef.Width.LONG, TOPICS_REGEX_DISPLAY)
                .define(CONSUMER_TYPE_CONFIG, Type.STRING, CONSUMER_TYPE_DEFAULT, ConfigDef.ValidString.in(CONSUMER_TYPE_DEFAULT, SHARE_CONSUMER_TYPE), Importance.HIGH,
                        "Consumer type for this sink: consumer or share. Share mode requires a ShareSinkTask with durable put completion.", COMMON_GROUP, 5, ConfigDef.Width.SHORT, "Consumer type")
                .define(SHARE_GROUP_ID_CONFIG, Type.STRING, null, Importance.HIGH,
                        "Share group identity. Defaults to connect-share- followed by the connector name. Ordinary consumer offsets are not transferred.", COMMON_GROUP, 6, ConfigDef.Width.MEDIUM, "Share group ID")
                .define(SHARE_PROCESSING_MAX_AGE_MS_CONFIG, Type.LONG, null, optionalRange(1), Importance.HIGH,
                        "Maximum acquisition-to-finalisation processing time in milliseconds. Required in share mode; must be positive.", COMMON_GROUP, 7, ConfigDef.Width.MEDIUM, "Share processing maximum age")
                .define(SHARE_PROCESSING_CANCEL_GRACE_MS_CONFIG, Type.LONG, null, optionalRange(0), Importance.HIGH,
                        "Time in milliseconds to wait for execution to exit after cancellation is requested. Required in share mode; zero is allowed. Cancellation cannot force a connector's external writes to stop.", COMMON_GROUP, 8, ConfigDef.Width.MEDIUM, "Share processing cancellation grace")
                .define(DLQ_TOPIC_NAME_CONFIG, ConfigDef.Type.STRING, DLQ_TOPIC_DEFAULT, Importance.MEDIUM, DLQ_TOPIC_NAME_DOC, ERROR_GROUP, 6, ConfigDef.Width.MEDIUM, DLQ_TOPIC_DISPLAY)
                .define(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, ConfigDef.Type.SHORT, DLQ_TOPIC_REPLICATION_FACTOR_CONFIG_DEFAULT, Importance.MEDIUM, DLQ_TOPIC_REPLICATION_FACTOR_CONFIG_DOC, ERROR_GROUP, 7, ConfigDef.Width.MEDIUM, DLQ_TOPIC_REPLICATION_FACTOR_CONFIG_DISPLAY)
                .define(DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT, Importance.MEDIUM, DLQ_CONTEXT_HEADERS_ENABLE_DOC, ERROR_GROUP, 8, ConfigDef.Width.MEDIUM, DLQ_CONTEXT_HEADERS_ENABLE_DISPLAY);
    }

    public static ConfigDef configDef() {
        return configDef(ConnectorConfig.configDef());
    }

    public static ConfigDef enrichedConfigDef(Plugins plugins, Map<String, String> connProps, WorkerConfig workerConfig) {
        return configDef(ConnectorConfig.enrichedConfigDef(plugins, connProps, workerConfig));
    }

    public static ConfigDef enrichedConfigDef(Plugins plugins, String connectorClass) {
        return configDef(ConnectorConfig.enrichedConfigDef(plugins, connectorClass));
    }

    public SinkConnectorConfig(Plugins plugins, Map<String, String> props) {
        super(plugins, configDef(), props);
        if (isShareMode(props)) {
            validate(props);
        }
    }

    /**
     * Throw an exception if the passed-in properties do not constitute a valid sink.
     * @param props sink configuration properties
     */
    public static void validate(Map<String, String> props) {
        Map<String, ConfigValue> validatedConfig = new LinkedHashMap<>();
        validate(props, validatedConfig);
        validatedConfig.values().stream()
                .filter(configValue -> !configValue.errorMessages().isEmpty())
                .findFirst()
                .ifPresent(configValue -> {
                    throw new ConfigException(configValue.name(), configValue.value(), configValue.errorMessages().get(0));
                });
    }

    /**
     * Perform preflight validation for the sink-specific properties for a connector.
     *
     * @param props           the configuration for the sink connector
     * @param validatedConfig any already-known {@link ConfigValue validation results} for the configuration.
     *                        May be empty, but may not be null. Any configuration errors discovered by this method will
     *                        be {@link ConfigValue#addErrorMessage(String) added} to a value in this map, adding a new
     *                        entry if one for the problematic property does not already exist.
     */
    public static void validate(Map<String, String> props, Map<String, ConfigValue> validatedConfig) {
        validateShareSettings(props, validatedConfig);
        final String topicsList = props.get(TOPICS_CONFIG);
        final String topicsRegex = props.get(TOPICS_REGEX_CONFIG);
        final String dlqTopic = props.getOrDefault(DLQ_TOPIC_NAME_CONFIG, "").trim();
        final boolean hasTopicsConfig = !Utils.isBlank(topicsList);
        final boolean hasTopicsRegexConfig = !Utils.isBlank(topicsRegex);
        final boolean hasDlqTopicConfig = !Utils.isBlank(dlqTopic);

        if (hasTopicsConfig && hasTopicsRegexConfig) {
            String errorMessage = TOPICS_CONFIG + " and " + TOPICS_REGEX_CONFIG + " are mutually exclusive options, but both are set.";
            addErrorMessage(validatedConfig, TOPICS_CONFIG, topicsList, errorMessage);
            addErrorMessage(validatedConfig, TOPICS_REGEX_CONFIG, topicsRegex, errorMessage);
        }

        if (!hasTopicsConfig && !hasTopicsRegexConfig) {
            String errorMessage = "Must configure one of " + TOPICS_CONFIG + " or " + TOPICS_REGEX_CONFIG;
            addErrorMessage(validatedConfig, TOPICS_CONFIG, topicsList, errorMessage);
            addErrorMessage(validatedConfig, TOPICS_REGEX_CONFIG, topicsRegex, errorMessage);
        }

        if (hasDlqTopicConfig) {
            if (hasTopicsConfig) {
                List<String> topics = parseTopicsList(props);
                if (topics.contains(dlqTopic)) {
                    String errorMessage = String.format(
                            "The DLQ topic '%s' may not be included in the list of topics ('%s=%s') consumed by the connector",
                            dlqTopic, TOPICS_CONFIG, topics
                    );
                    addErrorMessage(validatedConfig, TOPICS_CONFIG, topicsList, errorMessage);
                }
            }
            if (hasTopicsRegexConfig) {
                Pattern pattern = Pattern.compile(topicsRegex);
                if (pattern.matcher(dlqTopic).matches()) {
                    String errorMessage = String.format(
                            "The DLQ topic '%s' may not be matched by the regex for the topics ('%s=%s') consumed by the connector",
                            dlqTopic, TOPICS_REGEX_CONFIG, topicsRegex
                    );
                    addErrorMessage(validatedConfig, TOPICS_REGEX_CONFIG, topicsRegex, errorMessage);
                }
            }
        }
    }

    public static boolean isShareMode(Map<String, String> props) {
        String consumerType = props.get(CONSUMER_TYPE_CONFIG);
        return consumerType != null && SHARE_CONSUMER_TYPE.equals(consumerType.trim());
    }

    private static ConfigDef.Validator optionalRange(long minimum) {
        ConfigDef.Range range = ConfigDef.Range.atLeast(minimum);
        return ConfigDef.LambdaValidator.with((name, value) -> {
            if (value != null) {
                range.ensureValid(name, value);
            }
        }, () -> "null or " + range);
    }

    private static void validateShareSettings(Map<String, String> props, Map<String, ConfigValue> validatedConfig) {
        String consumerType = props.getOrDefault(CONSUMER_TYPE_CONFIG, CONSUMER_TYPE_DEFAULT);
        consumerType = consumerType == null ? null : consumerType.trim();
        if (!CONSUMER_TYPE_DEFAULT.equals(consumerType) && !SHARE_CONSUMER_TYPE.equals(consumerType)) {
            addErrorMessage(validatedConfig, CONSUMER_TYPE_CONFIG, consumerType, "Consumer type must be consumer or share");
        }
        if (!isShareMode(props)) {
            return;
        }
        String groupId = validateShareGroupId(props, validatedConfig);
        validateShareProcessingLimits(props, validatedConfig);
        validateShareDeliveryOptions(props, validatedConfig);
        Map<String, Object> overrides = new LinkedHashMap<>();
        props.forEach((key, value) -> {
            if (key.startsWith(CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX)) {
                overrides.put(key.substring(CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX.length()), value);
            }
        });
        ShareConsumerSettings.validationErrors(overrides, groupId).forEach((key, error) -> {
            String property = CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + key;
            addErrorMessage(validatedConfig, property, props.get(property), error);
        });
    }

    private static String validateShareGroupId(Map<String, String> props, Map<String, ConfigValue> validatedConfig) {
        try {
            return ShareConsumerSettings.resolveGroupId(props.get(NAME_CONFIG), props);
        } catch (ConfigException e) {
            String key = props.containsKey(SHARE_GROUP_ID_CONFIG) ? SHARE_GROUP_ID_CONFIG : NAME_CONFIG;
            addErrorMessage(validatedConfig, key, props.get(key), e.getMessage());
            return null;
        }
    }

    private static void validateShareProcessingLimits(Map<String, String> props, Map<String, ConfigValue> validatedConfig) {
        for (String key : List.of(SHARE_PROCESSING_MAX_AGE_MS_CONFIG, SHARE_PROCESSING_CANCEL_GRACE_MS_CONFIG)) {
            String rawValue = props.get(key);
            try {
                Object value = ConfigDef.parseType(key, rawValue, Type.LONG);
                if (value == null) {
                    throw new ConfigException(key, null, "An explicit processing limit is required in share mode");
                }
                optionalRange(key.equals(SHARE_PROCESSING_MAX_AGE_MS_CONFIG) ? 1 : 0).ensureValid(key, value);
            } catch (ConfigException e) {
                addErrorMessage(validatedConfig, key, rawValue, e.getMessage());
            }
        }
    }

    private static void validateShareDeliveryOptions(Map<String, String> props, Map<String, ConfigValue> validatedConfig) {
        String tolerance = props.getOrDefault(ERRORS_TOLERANCE_CONFIG, ERRORS_TOLERANCE_DEFAULT.value());
        if (!ERRORS_TOLERANCE_DEFAULT.value().equals(tolerance == null ? null : tolerance.trim())) {
            addErrorMessage(validatedConfig, ERRORS_TOLERANCE_CONFIG, props.get(ERRORS_TOLERANCE_CONFIG), "Share mode requires errors.tolerance=none");
        }
        if (hasDlqTopicConfig(props)) {
            addErrorMessage(validatedConfig, DLQ_TOPIC_NAME_CONFIG, props.get(DLQ_TOPIC_NAME_CONFIG), "Independent dead letter queue reporting is not supported in share mode");
        }
        if (!Utils.isBlank(props.get(TOPICS_REGEX_CONFIG))) {
            addErrorMessage(validatedConfig, TOPICS_REGEX_CONFIG, props.get(TOPICS_REGEX_CONFIG), "Share mode requires an explicit topics list; regex subscription is not supported");
        }
    }

    private static void addErrorMessage(Map<String, ConfigValue> validatedConfig, String name, String value, String errorMessage) {
        validatedConfig.computeIfAbsent(
                name,
                p -> new ConfigValue(name, value, List.of(), new ArrayList<>())
        ).addErrorMessage(
                errorMessage
        );
    }

    public static boolean hasTopicsConfig(Map<String, String> props) {
        String topicsStr = props.get(TOPICS_CONFIG);
        return !Utils.isBlank(topicsStr);
    }

    public static boolean hasDlqTopicConfig(Map<String, String> props) {
        String dqlTopicStr = props.get(DLQ_TOPIC_NAME_CONFIG);
        return !Utils.isBlank(dqlTopicStr);
    }

    @SuppressWarnings("unchecked")
    public static List<String> parseTopicsList(Map<String, String> props) {
        List<String> topics = (List<String>) ConfigDef.parseType(TOPICS_CONFIG, props.get(TOPICS_CONFIG), Type.LIST);
        if (topics == null) {
            return List.of();
        }
        return topics
                .stream()
                .filter(topic -> !topic.isEmpty())
                .distinct()
                .collect(Collectors.toList());
    }

    public String dlqTopicName() {
        return getString(DLQ_TOPIC_NAME_CONFIG);
    }

    public short dlqTopicReplicationFactor() {
        return getShort(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG);
    }

    public boolean isDlqContextHeadersEnabled() {
        return getBoolean(DLQ_CONTEXT_HEADERS_ENABLE_CONFIG);
    }

    public boolean enableErrantRecordReporter() {
        String dqlTopic = dlqTopicName();
        return !dqlTopic.isEmpty() || enableErrorLog();
    }

    public static void main(String[] args) {
        System.out.println(configDef().toHtml(4, config -> "sinkconnectorconfigs_" + config));
    }
}
