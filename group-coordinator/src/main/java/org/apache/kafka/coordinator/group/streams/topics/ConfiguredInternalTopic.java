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
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * ConfiguredInternalTopic captures the properties required for configuring the internal topics we create for change-logs and repartitioning
 * etc.
 * <p>
 * It is derived from the topology sent by the client, and the current state of the topics inside the broker. If the topics on the broker
 * changes, the internal topic may need to be reconfigured.
 */
public record ConfiguredInternalTopic(String name,
                                      Map<String, String> topicConfigs,
                                      int numberOfPartitions,
                                      Optional<Short> replicationFactor) {

    public ConfiguredInternalTopic {
        Objects.requireNonNull(name, "name can't be null");
        Topic.validate(name);
        validateNumberOfPartitions(numberOfPartitions);
        topicConfigs = Collections.unmodifiableMap(Objects.requireNonNull(topicConfigs, "topicConfigs can't be null"));
    }

    private static void validateNumberOfPartitions(final int numberOfPartitions) {
        if (numberOfPartitions < 1) {
            throw new IllegalArgumentException("Number of partitions must be at least 1.");
        }
    }

    public StreamsGroupDescribeResponseData.TopicInfo asStreamsGroupDescribeTopicInfo() {
        return new StreamsGroupDescribeResponseData.TopicInfo()
            .setName(name)
            .setPartitions(numberOfPartitions)
            .setReplicationFactor(replicationFactor.orElse((short) 0))
            .setTopicConfigs(
                topicConfigs != null ?
                    topicConfigs.entrySet().stream().map(
                        y -> new StreamsGroupDescribeResponseData.KeyValue()
                            .setKey(y.getKey())
                            .setValue(y.getValue())
                    ).collect(Collectors.toList()) : null
            );
    }

}
