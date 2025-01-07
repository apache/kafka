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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.Topic;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * InternalTopicConfig captures the properties required for configuring
 * the internal topics we create for change-logs and repartitioning etc.
 */
public abstract class InternalTopicConfig {
    final String name;
    final Map<String, String> topicConfigs;
    final boolean enforceNumberOfPartitions;

    private Optional<Integer> numberOfPartitions = Optional.empty();

    static final Map<String, String> INTERNAL_TOPIC_DEFAULT_OVERRIDES = new HashMap<>();
    static {
        INTERNAL_TOPIC_DEFAULT_OVERRIDES.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime");
    }

    InternalTopicConfig(final String name, final Map<String, String> topicConfigs) {
        this.name = Objects.requireNonNull(name, "name can't be null");
        Topic.validate(name);
        this.topicConfigs = Objects.requireNonNull(topicConfigs, "topicConfigs can't be null");
        this.enforceNumberOfPartitions = false;
    }

    InternalTopicConfig(final String name,
                        final Map<String, String> topicConfigs,
                        final int numberOfPartitions,
                        final boolean enforceNumberOfPartitions) {
        this.name = Objects.requireNonNull(name, "name can't be null");
        Topic.validate(name);
        validateNumberOfPartitions(numberOfPartitions);
        this.topicConfigs = Objects.requireNonNull(topicConfigs, "topicConfigs can't be null");
        this.numberOfPartitions = Optional.of(numberOfPartitions);
        this.enforceNumberOfPartitions = enforceNumberOfPartitions;
    }

    /**
     * Get the configured properties for this topic. If retentionMs is set then
     * we add additionalRetentionMs to work out the desired retention when cleanup.policy=compact,delete
     *
     * @param additionalRetentionMs - added to retention to allow for clock drift etc
     * @return Properties to be used when creating the topic
     */
    public abstract Map<String, String> properties(final Map<String, String> defaultProperties, final long additionalRetentionMs);

    public boolean hasEnforcedNumberOfPartitions() {
        return enforceNumberOfPartitions;
    }

    public String name() {
        return name;
    }

    public Optional<Integer> numberOfPartitions() {
        return numberOfPartitions;
    }

    public void setNumberOfPartitions(final int numberOfPartitions) {
        if (hasEnforcedNumberOfPartitions()) {
            throw new UnsupportedOperationException("number of partitions are enforced on topic " + name() + " and can't be altered.");
        }

        validateNumberOfPartitions(numberOfPartitions);

        this.numberOfPartitions = Optional.of(numberOfPartitions);
    }

    private static void validateNumberOfPartitions(final int numberOfPartitions) {
        if (numberOfPartitions < 1) {
            throw new IllegalArgumentException("Number of partitions must be at least 1.");
        }
    }

    @Override
    public String toString() {
        return "InternalTopicConfig(" +
                "name=" + name +
                ", topicConfigs=" + topicConfigs +
                ", enforceNumberOfPartitions=" + enforceNumberOfPartitions +
                ")";
    }
}
