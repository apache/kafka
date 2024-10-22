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

import org.apache.kafka.common.internals.Topic;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * ConfiguredInternalTopic captures the properties required for configuring the internal topics we create for change-logs and repartitioning
 * etc.
 * <p>
 * It is derived from the topology sent by the client, and the current state of the topics inside the broker. If the topics on the broker
 * changes, the internal topic may need to be reconfigured.
 */
public class ConfiguredInternalTopic {

    private final String name;
    private final Map<String, String> topicConfigs;
    private final Optional<Short> replicationFactor;
    private final boolean enforceNumberOfPartitions;
    private Optional<Integer> numberOfPartitions;

    public ConfiguredInternalTopic(final String name) {
        this(name, Collections.emptyMap(), Optional.empty(), Optional.empty());
    }

    public ConfiguredInternalTopic(final String name,
                                   final Map<String, String> topicConfigs) {
        this(name, topicConfigs, Optional.empty(), Optional.empty());
    }

    public ConfiguredInternalTopic(final String name,
                                   final Map<String, String> topicConfigs,
                                   final Optional<Integer> numberOfPartitions,
                                   final Optional<Short> replicationFactor) {
        this.name = Objects.requireNonNull(name, "name can't be null");
        Topic.validate(name);
        numberOfPartitions.ifPresent(ConfiguredInternalTopic::validateNumberOfPartitions);
        this.topicConfigs = Objects.requireNonNull(topicConfigs, "topicConfigs can't be null");
        this.numberOfPartitions = numberOfPartitions;
        this.replicationFactor = replicationFactor;
        this.enforceNumberOfPartitions = numberOfPartitions.isPresent();
    }

    private static void validateNumberOfPartitions(final int numberOfPartitions) {
        if (numberOfPartitions < 1) {
            throw new IllegalArgumentException("Number of partitions must be at least 1.");
        }
    }

    public Map<String, String> topicConfigs() {
        return topicConfigs;
    }

    public boolean hasEnforcedNumberOfPartitions() {
        return enforceNumberOfPartitions;
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

    public ConfiguredInternalTopic setNumberOfPartitions(final int numberOfPartitions) {
        if (this.hasEnforcedNumberOfPartitions()
            && this.numberOfPartitions.isPresent()
            && this.numberOfPartitions.get() != numberOfPartitions) {
            throw new UnsupportedOperationException(
                "number of partitions are enforced on topic " + name() + " and can't be altered.");
        }

        validateNumberOfPartitions(numberOfPartitions);

        this.numberOfPartitions = Optional.of(numberOfPartitions);
        return this;
    }

    @Override
    public String toString() {
        return "ConfiguredInternalTopic(" +
            "name=" + name +
            ", topicConfigs=" + topicConfigs +
            ", numberOfPartitions=" + numberOfPartitions +
            ", replicationFactor=" + replicationFactor +
            ", enforceNumberOfPartitions=" + enforceNumberOfPartitions +
            ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConfiguredInternalTopic that = (ConfiguredInternalTopic) o;
        return enforceNumberOfPartitions == that.enforceNumberOfPartitions
            && Objects.equals(name, that.name)
            && Objects.equals(topicConfigs, that.topicConfigs)
            && Objects.equals(numberOfPartitions, that.numberOfPartitions)
            && Objects.equals(replicationFactor, that.replicationFactor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name,
            topicConfigs,
            numberOfPartitions,
            replicationFactor,
            enforceNumberOfPartitions);
    }
}
