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
package org.apache.kafka.coordinator.group.streams.topics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.Topic;

/**
 * InternalTopicConfig captures the properties required for configuring
 * the internal topics we create for change-logs and repartitioning etc.
 */
public class InternalTopicConfig {

    private final String name;
    private final Map<String, String> topicConfigs;

    private Optional<Integer> numberOfPartitions;
    private Optional<Short> replicationFactor;

    static final Map<String, String> INTERNAL_TOPIC_DEFAULT_OVERRIDES = new HashMap<>();
    static {
        INTERNAL_TOPIC_DEFAULT_OVERRIDES.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime");
    }

    public InternalTopicConfig(final String name) {
        this.name = Objects.requireNonNull(name, "name can't be null");
        this.topicConfigs = Collections.emptyMap();
        this.numberOfPartitions = Optional.empty();
        this.replicationFactor =  Optional.empty();
    }

    public InternalTopicConfig(
        final String name,
        final Map<String, String> topicConfigs,
        final Optional<Integer> numberOfPartitions,
        final Optional<Short> replicationFactor) {
        this.name = Objects.requireNonNull(name, "name can't be null");
        Topic.validate(name);
        numberOfPartitions.ifPresent(InternalTopicConfig::validateNumberOfPartitions);
        this.topicConfigs = Objects.requireNonNull(topicConfigs, "topicConfigs can't be null");
        this.numberOfPartitions = numberOfPartitions;
        this.replicationFactor = replicationFactor;
    }

    public Map<String, String> topicConfigs() {
        return topicConfigs;
    }

    public String name() {
        return name;
    }

    public Optional<Integer> numberOfPartitions() {
        return numberOfPartitions;
    }

    public Optional<Short> replicationFactor() {
        return replicationFactor;
    }

    public void setNumberOfPartitions(final int numberOfPartitions) {
        if (this.numberOfPartitions.isPresent() && this.numberOfPartitions.get() != numberOfPartitions) {
            throw new UnsupportedOperationException(
                "number of partitions are enforced on topic " + name() + " and can't be altered.");
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
            ", numberOfPartitions=" + numberOfPartitions +
            ", replicationFactor=" + replicationFactor +
            ")";
    }
}
