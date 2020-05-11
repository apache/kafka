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

import java.util.Collections;

public class TopicCreationConfig {

    public static final String DEFAULT_TOPIC_CREATION_PREFIX = "topic.creation.default.";
    public static final String DEFAULT_TOPIC_CREATION_GROUP = "default";

    public static final String INCLUDE_REGEX_CONFIG = "include";
    private static final String INCLUDE_REGEX_DOC = "A list of strings that represent regular "
            + "expressions that may match topic names. This list is used to include topics that "
            + "match their values and apply this group's specific configuration to the topics "
            + "that match this inclusion list. $alias applies to any group defined in topic"
            + ".creation.groups but not the default";

    public static final String EXCLUDE_REGEX_CONFIG = "exclude";
    private static final String EXCLUDE_REGEX_DOC = "A list of strings that represent regular "
            + "expressions that may match topic names. This list is used to exclude topics that "
            + "match their values and refrain from applying this group's specific configuration "
            + "to the topics that match this exclusion list. $alias applies to any group defined "
            + "in topic.creation.groups but not the default. Note that exclusion rules have "
            + "precedent and override any inclusion rules for topics. ";

    public static final String REPLICATION_FACTOR_CONFIG = "replication.factor";
    private static final String REPLICATION_FACTOR_DOC = "The replication factor for new topics "
            + "created for this connector. This value must not be larger than the number of "
            + "brokers in the Kafka cluster, or otherwise an error will be thrown when the "
            + "connector will attempt to create a topic. For the default group this configuration"
            + " is required. For any other group defined in topic.creation.groups this config is "
            + "optional and if it's missing it gets the value the default group";

    public static final String PARTITIONS_CONFIG = "partitions";
    private static final String PARTITIONS_DOC = "The number of partitions new topics created for"
            + " this connector. For the default group this configuration is required. For any "
            + "other group defined in topic.creation.groups this config is optional and if it's "
            + "missing it gets the value the default group";

    private static ConfigDef.LambdaValidator minusOneOrPositiveInt = ConfigDef.LambdaValidator.with(
        (name, value) -> {
            if (!(value instanceof Integer)) {
                throw new ConfigException(name, value, "Value is not of type Integer");
            }
            int num = (int) value;
            if (num != -1 && num < 1) {
                throw new ConfigException(name, num, "Value must be -1 or at least 1");
            }
        },
        () -> "-1 or [1,...]"
    );

    private static ConfigDef.LambdaValidator minusOneOrPositiveShort = ConfigDef.LambdaValidator.with(
        (name, value) -> {
            if (!(value instanceof Short)) {
                throw new ConfigException(name, value, "Value is not of type Integer");
            }
            short num = (short) value;
            if (num != -1 && num < 1) {
                throw new ConfigException(name, num, "Value must be -1 or at least 1");
            }
        },
        () -> "-1 or [1,...]"
    );

    public static ConfigDef configDef(String group, short defaultReplicationFactor, int defaultParitionCount) {
        int orderInGroup = 0;
        // TODO: add more specific validation
        ConfigDef configDef = new ConfigDef();
        configDef
                .define(INCLUDE_REGEX_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(),
                        new ConfigDef.NonNullValidator(), ConfigDef.Importance.LOW,
                        INCLUDE_REGEX_DOC, group, ++orderInGroup, ConfigDef.Width.LONG,
                        "Inclusion Topic Pattern for " + group)
                .define(EXCLUDE_REGEX_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(),
                        new ConfigDef.NonNullValidator(), ConfigDef.Importance.LOW,
                        EXCLUDE_REGEX_DOC, group, ++orderInGroup, ConfigDef.Width.LONG,
                        "Exclusion Topic Pattern for " + group)
                .define(REPLICATION_FACTOR_CONFIG, ConfigDef.Type.SHORT,
                        defaultReplicationFactor, minusOneOrPositiveShort,
                        ConfigDef.Importance.LOW, REPLICATION_FACTOR_DOC, group, ++orderInGroup,
                        ConfigDef.Width.LONG, "Replication Factor for Topics in " + group)
                .define(PARTITIONS_CONFIG, ConfigDef.Type.INT,
                        defaultParitionCount, minusOneOrPositiveInt,
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
                        ConfigDef.NO_DEFAULT_VALUE, minusOneOrPositiveShort,
                        ConfigDef.Importance.LOW, REPLICATION_FACTOR_DOC, DEFAULT_TOPIC_CREATION_GROUP, ++orderInGroup,
                        ConfigDef.Width.LONG, "Replication Factor for Topics in " + DEFAULT_TOPIC_CREATION_GROUP)
                .define(PARTITIONS_CONFIG, ConfigDef.Type.INT,
                        ConfigDef.NO_DEFAULT_VALUE, minusOneOrPositiveInt,
                        ConfigDef.Importance.LOW, PARTITIONS_DOC, DEFAULT_TOPIC_CREATION_GROUP, ++orderInGroup,
                        ConfigDef.Width.LONG, "Partition Count for Topics in " + DEFAULT_TOPIC_CREATION_GROUP);
        return configDef;
    }

}
