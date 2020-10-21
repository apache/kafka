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
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.transforms.util.RegexValidator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Configuration needed for all sink connectors
 */

public class SinkConnectorConfig extends ConnectorConfig {

    public static final String TOPICS_CONFIG = SinkTask.TOPICS_CONFIG;
    private static final String TOPICS_DOC = "List of topics to consume, separated by commas";
    public static final String TOPICS_DEFAULT = "";
    private static final String TOPICS_DISPLAY = "Topics";

    public static final String TOPICS_REGEX_CONFIG = SinkTask.TOPICS_REGEX_CONFIG;
    private static final String TOPICS_REGEX_DOC = "Regular expression giving topics to consume. " +
        "Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. " +
        "Only one of " + TOPICS_CONFIG + " or " + TOPICS_REGEX_CONFIG + " should be specified.";
    public static final String TOPICS_REGEX_DEFAULT = "";
    private static final String TOPICS_REGEX_DISPLAY = "Topics regex";

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

    static ConfigDef config = ConnectorConfig.configDef()
        .define(TOPICS_CONFIG, ConfigDef.Type.LIST, TOPICS_DEFAULT, ConfigDef.Importance.HIGH, TOPICS_DOC, COMMON_GROUP, 4, ConfigDef.Width.LONG, TOPICS_DISPLAY)
        .define(TOPICS_REGEX_CONFIG, ConfigDef.Type.STRING, TOPICS_REGEX_DEFAULT, new RegexValidator(), ConfigDef.Importance.HIGH, TOPICS_REGEX_DOC, COMMON_GROUP, 4, ConfigDef.Width.LONG, TOPICS_REGEX_DISPLAY)
        .define(DLQ_TOPIC_NAME_CONFIG, ConfigDef.Type.STRING, DLQ_TOPIC_DEFAULT, Importance.MEDIUM, DLQ_TOPIC_NAME_DOC, ERROR_GROUP, 6, ConfigDef.Width.MEDIUM, DLQ_TOPIC_DISPLAY)
        .define(DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, ConfigDef.Type.SHORT, DLQ_TOPIC_REPLICATION_FACTOR_CONFIG_DEFAULT, Importance.MEDIUM, DLQ_TOPIC_REPLICATION_FACTOR_CONFIG_DOC, ERROR_GROUP, 7, ConfigDef.Width.MEDIUM, DLQ_TOPIC_REPLICATION_FACTOR_CONFIG_DISPLAY)
        .define(DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN, DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT, Importance.MEDIUM, DLQ_CONTEXT_HEADERS_ENABLE_DOC, ERROR_GROUP, 8, ConfigDef.Width.MEDIUM, DLQ_CONTEXT_HEADERS_ENABLE_DISPLAY);

    public static ConfigDef configDef() {
        return config;
    }

    public SinkConnectorConfig(Plugins plugins, Map<String, String> props) {
        super(plugins, config, props);
    }

    /**
     * Throw an exception if the passed-in properties do not constitute a valid sink.
     * @param props sink configuration properties
     */
    public static void validate(Map<String, String> props) {
        final boolean hasTopicsConfig = hasTopicsConfig(props);
        final boolean hasTopicsRegexConfig = hasTopicsRegexConfig(props);
        final boolean hasDlqTopicConfig = hasDlqTopicConfig(props);

        if (hasTopicsConfig && hasTopicsRegexConfig) {
            throw new ConfigException(SinkTask.TOPICS_CONFIG + " and " + SinkTask.TOPICS_REGEX_CONFIG +
                " are mutually exclusive options, but both are set.");
        }

        if (!hasTopicsConfig && !hasTopicsRegexConfig) {
            throw new ConfigException("Must configure one of " +
                SinkTask.TOPICS_CONFIG + " or " + SinkTask.TOPICS_REGEX_CONFIG);
        }

        if (hasDlqTopicConfig) {
            String dlqTopic = props.get(DLQ_TOPIC_NAME_CONFIG).trim();
            if (hasTopicsConfig) {
                List<String> topics = parseTopicsList(props);
                if (topics.contains(dlqTopic)) {
                    throw new ConfigException(String.format("The DLQ topic '%s' may not be included in the list of "
                            + "topics ('%s=%s') consumed by the connector", dlqTopic, SinkTask.TOPICS_REGEX_CONFIG, topics));
                }
            }
            if (hasTopicsRegexConfig) {
                String topicsRegexStr = props.get(SinkTask.TOPICS_REGEX_CONFIG);
                Pattern pattern = Pattern.compile(topicsRegexStr);
                if (pattern.matcher(dlqTopic).matches()) {
                    throw new ConfigException(String.format("The DLQ topic '%s' may not be included in the regex matching the "
                            + "topics ('%s=%s') consumed by the connector", dlqTopic, SinkTask.TOPICS_REGEX_CONFIG, topicsRegexStr));
                }
            }
        }
    }

    public static boolean hasTopicsConfig(Map<String, String> props) {
        String topicsStr = props.get(TOPICS_CONFIG);
        return topicsStr != null && !topicsStr.trim().isEmpty();
    }

    public static boolean hasTopicsRegexConfig(Map<String, String> props) {
        String topicsRegexStr = props.get(TOPICS_REGEX_CONFIG);
        return topicsRegexStr != null && !topicsRegexStr.trim().isEmpty();
    }

    public static boolean hasDlqTopicConfig(Map<String, String> props) {
        String dqlTopicStr = props.get(DLQ_TOPIC_NAME_CONFIG);
        return dqlTopicStr != null && !dqlTopicStr.trim().isEmpty();
    }

    @SuppressWarnings("unchecked")
    public static List<String> parseTopicsList(Map<String, String> props) {
        List<String> topics = (List<String>) ConfigDef.parseType(TOPICS_CONFIG, props.get(TOPICS_CONFIG), Type.LIST);
        if (topics == null) {
            return Collections.emptyList();
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
        System.out.println(config.toHtml(4, config -> "sinkconnectorconfigs_" + config));
    }
}
