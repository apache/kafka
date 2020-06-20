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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.util.TopicAdmin;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class TopicCreationConfig {

    public static final String DEFAULT_TOPIC_CREATION_PREFIX = "topic.creation.default.";
    public static final String DEFAULT_TOPIC_CREATION_GROUP = "default";

    public static final String INCLUDE_REGEX_CONFIG = "include";
    private static final String INCLUDE_REGEX_DOC = "A list of regular expression literals "
            + "used to match the topic names used by the source connector. This list is used "
            + "to include topics that should be created using the topic settings defined by this group.";

    public static final String EXCLUDE_REGEX_CONFIG = "exclude";
    private static final String EXCLUDE_REGEX_DOC = "A list of regular expression literals "
            + "used to match the topic names used by the source connector. This list is used "
            + "to exclude topics from being created with the topic settings defined by this group. "
            + "Note that exclusion rules have precedent and override any inclusion rules for the topics.";

    public static final String REPLICATION_FACTOR_CONFIG = "replication.factor";
    private static final String REPLICATION_FACTOR_DOC = "The replication factor for new topics "
            + "created for this connector using this group. This value may be -1 to use the broker's"
            + "default replication factor, or may be a positive number not larger than the number of "
            + "brokers in the Kafka cluster. A value larger than the number of brokers in the Kafka cluster "
            + "will result in an error when the new topic is created. For the default group this configuration "
            + "is required. For any other group defined in topic.creation.groups this config is "
            + "optional and if it's missing it gets the value of the default group";

    public static final String PARTITIONS_CONFIG = "partitions";
    private static final String PARTITIONS_DOC = "The number of partitions new topics created for "
            + "this connector. This value may be -1 to use the broker's default number of partitions, "
            + "or a positive number representing the desired number of partitions. "
            + "For the default group this configuration is required. For any "
            + "other group defined in topic.creation.groups this config is optional and if it's "
            + "missing it gets the value of the default group";

    public static final ConfigDef.Validator REPLICATION_FACTOR_VALIDATOR = ConfigDef.LambdaValidator.with(
        (name, value) -> validateReplicationFactor(name, (short) value),
        () -> "Positive number not larger than the number of brokers in the Kafka cluster, or -1 to use the broker's default"
    );
    public static final ConfigDef.Validator PARTITIONS_VALIDATOR = ConfigDef.LambdaValidator.with(
        (name, value) -> validatePartitions(name, (int) value),
        () -> "Positive number, or -1 to use the broker's default"
    );
    @SuppressWarnings("unchecked")
    public static final ConfigDef.Validator REGEX_VALIDATOR = ConfigDef.LambdaValidator.with(
        (name, value) -> {
            try {
                ((List<String>) value).forEach(Pattern::compile);
            } catch (PatternSyntaxException e) {
                throw new ConfigException(name, value,
                        "Syntax error in regular expression: " + e.getMessage());
            }
        },
        () -> "Positive number, or -1 to use the broker's default"
    );

    private static void validatePartitions(String configName, int factor) {
        if (factor != TopicAdmin.NO_PARTITIONS && factor < 1) {
            throw new ConfigException(configName, factor,
                    "Number of partitions must be positive, or -1 to use the broker's default");
        }
    }

    private static void validateReplicationFactor(String configName, short factor) {
        if (factor != TopicAdmin.NO_REPLICATION_FACTOR && factor < 1) {
            throw new ConfigException(configName, factor,
                    "Replication factor must be positive and not larger than the number of brokers in the Kafka cluster, or -1 to use the broker's default");
        }
    }

    public static ConfigDef configDef(String group, short defaultReplicationFactor, int defaultParitionCount) {
        int orderInGroup = 0;
        ConfigDef configDef = new ConfigDef();
        configDef
                .define(INCLUDE_REGEX_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(),
                        REGEX_VALIDATOR, ConfigDef.Importance.LOW,
                        INCLUDE_REGEX_DOC, group, ++orderInGroup, ConfigDef.Width.LONG,
                        "Inclusion Topic Pattern for " + group)
                .define(EXCLUDE_REGEX_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(),
                        REGEX_VALIDATOR, ConfigDef.Importance.LOW,
                        EXCLUDE_REGEX_DOC, group, ++orderInGroup, ConfigDef.Width.LONG,
                        "Exclusion Topic Pattern for " + group)
                .define(REPLICATION_FACTOR_CONFIG, ConfigDef.Type.SHORT,
                        defaultReplicationFactor, REPLICATION_FACTOR_VALIDATOR,
                        ConfigDef.Importance.LOW, REPLICATION_FACTOR_DOC, group, ++orderInGroup,
                        ConfigDef.Width.LONG, "Replication Factor for Topics in " + group)
                .define(PARTITIONS_CONFIG, ConfigDef.Type.INT,
                        defaultParitionCount, PARTITIONS_VALIDATOR,
                        ConfigDef.Importance.LOW, PARTITIONS_DOC, group, ++orderInGroup,
                        ConfigDef.Width.LONG, "Partition Count for Topics in " + group);
        return configDef;
    }

    public static ConfigDef defaultGroupConfigDef() {
        int orderInGroup = 0;
        ConfigDef configDef = new ConfigDef();
        configDef
                .define(INCLUDE_REGEX_CONFIG, ConfigDef.Type.LIST, ".*",
                        new ConfigDef.NonNullValidator(), ConfigDef.Importance.LOW,
                        INCLUDE_REGEX_DOC, DEFAULT_TOPIC_CREATION_GROUP, ++orderInGroup, ConfigDef.Width.LONG,
                        "Inclusion Topic Pattern for " + DEFAULT_TOPIC_CREATION_GROUP)
                .define(EXCLUDE_REGEX_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(),
                        new ConfigDef.NonNullValidator(), ConfigDef.Importance.LOW,
                        EXCLUDE_REGEX_DOC, DEFAULT_TOPIC_CREATION_GROUP, ++orderInGroup, ConfigDef.Width.LONG,
                        "Exclusion Topic Pattern for " + DEFAULT_TOPIC_CREATION_GROUP)
                .define(REPLICATION_FACTOR_CONFIG, ConfigDef.Type.SHORT,
                        ConfigDef.NO_DEFAULT_VALUE, REPLICATION_FACTOR_VALIDATOR,
                        ConfigDef.Importance.LOW, REPLICATION_FACTOR_DOC, DEFAULT_TOPIC_CREATION_GROUP, ++orderInGroup,
                        ConfigDef.Width.LONG, "Replication Factor for Topics in " + DEFAULT_TOPIC_CREATION_GROUP)
                .define(PARTITIONS_CONFIG, ConfigDef.Type.INT,
                        ConfigDef.NO_DEFAULT_VALUE, PARTITIONS_VALIDATOR,
                        ConfigDef.Importance.LOW, PARTITIONS_DOC, DEFAULT_TOPIC_CREATION_GROUP, ++orderInGroup,
                        ConfigDef.Width.LONG, "Partition Count for Topics in " + DEFAULT_TOPIC_CREATION_GROUP);
        return configDef;
    }

}
