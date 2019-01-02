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

import org.apache.kafka.common.internals.Topic;

import java.util.Map;
import java.util.Objects;

/**
 * InternalTopicConfig captures the properties required for configuring
 * the internal topics we create for change-logs and repartitioning etc.
 */
public abstract class InternalTopicConfig {
    final String name;
    final Map<String, String> topicConfigs;

    private int numberOfPartitions = -1;

    InternalTopicConfig(final String name, final Map<String, String> topicConfigs) {
        Objects.requireNonNull(name, "name can't be null");
        Topic.validate(name);

        this.name = name;
        this.topicConfigs = topicConfigs;
    }

    /**
     * Get the configured properties for this topic. If retentionMs is set then
     * we add additionalRetentionMs to work out the desired retention when cleanup.policy=compact,delete
     *
     * @param additionalRetentionMs - added to retention to allow for clock drift etc
     * @return Properties to be used when creating the topic
     */
    abstract public Map<String, String> getProperties(final Map<String, String> defaultProperties, final long additionalRetentionMs);

    public String name() {
        return name;
    }

    public int numberOfPartitions() {
        if (numberOfPartitions == -1) {
            throw new IllegalStateException("Number of partitions not specified.");
        }
        return numberOfPartitions;
    }

    void setNumberOfPartitions(final int numberOfPartitions) {
        if (numberOfPartitions < 1) {
            throw new IllegalArgumentException("Number of partitions must be at least 1.");
        }
        this.numberOfPartitions = numberOfPartitions;
    }

    @Override
    public String toString() {
        return "InternalTopicConfig(" +
                "name=" + name +
                ", topicConfigs=" + topicConfigs +
                ")";
    }
}
