/*
 * Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
 */
package org.apache.kafka.controller;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

final class AivenTopicPolicyConfig extends AbstractConfig {
    private static final String PREFIX = "aiven.topic.policy.";
    private static final String MAX_USER_TOPICS = PREFIX + "max.user.topics";
    private static final String MAX_USER_PARTITIONS = PREFIX + "max.user.partitions";
    private static final String MAX_PARTITIONS_PER_USER_TOPIC = PREFIX + "max.partitions.per.user.topic";
    private static final String EXCLUDED_TOPICS = PREFIX + "excluded.topics";

    private static ConfigDef configDef() {
        return new ConfigDef()
            .define(
                MAX_USER_TOPICS,
                ConfigDef.Type.INT,
                null,
                new OptionalRange(ConfigDef.Range.atLeast(0)),
                ConfigDef.Importance.MEDIUM,
                "Maximum number of user topics (optional, must be >= 0 if provided)"
            )
            .define(
                MAX_USER_PARTITIONS,
                ConfigDef.Type.INT,
                null,
                new OptionalRange(ConfigDef.Range.atLeast(0)),
                ConfigDef.Importance.MEDIUM,
                "Maximum total number of user partitions (optional, must be >= 0 if provided)"
            )
            .define(
                MAX_PARTITIONS_PER_USER_TOPIC,
                ConfigDef.Type.INT,
                null,
                new OptionalRange(ConfigDef.Range.atLeast(1)),
                ConfigDef.Importance.MEDIUM,
                "Maximum number of partitions per user topic (optional, must be >= 1 if provided)"
            )
            .define(
                EXCLUDED_TOPICS,
                ConfigDef.Type.LIST,
                null,
                ConfigDef.Importance.MEDIUM,
                "Comma-separated list of topic names to exclude from policy checks"
            );
    }

    AivenTopicPolicyConfig(final Map<String, ?> props) {
        super(configDef(), props);
    }

    Optional<Integer> maxUserTopics() {
        return Optional.ofNullable(getInt(MAX_USER_TOPICS));
    }

    Optional<Integer> maxUserPartitions() {
        return Optional.ofNullable(getInt(MAX_USER_PARTITIONS));
    }

    Optional<Integer> maxPartitionsPerUserTopic() {
        return Optional.ofNullable(getInt(MAX_PARTITIONS_PER_USER_TOPIC));
    }

    Set<String> excludedTopics() {
        final List<String> excludedTopics = getList(EXCLUDED_TOPICS);
        if (excludedTopics == null) {
            return Collections.emptySet();
        } else {
            return new HashSet<>(excludedTopics);
        }
    }

    private static class OptionalRange implements ConfigDef.Validator {
        private final ConfigDef.Range internal;

        OptionalRange(final ConfigDef.Range internal) {
            this.internal = internal;
        }

        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                return;
            }
            internal.ensureValid(name, value);
        }
    }
}
