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

import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Internal representation of a subtopology.
 * <p>
 * The subtopology is configured according to the number of partitions available in the source topics. It has regular expressions already
 * resolved and defined exactly the information that is being used by streams groups assignment reconciliation.
 * <p>
 * Configured subtopologies may be recreated every time the input topics used by the subtopology are modified.
 *
 * @param numberOfTasks           Precomputed number of tasks. Not that not every source topic may have a partition for
 *                                every task, in cases where there are multiple source topics with an unequal number of
 *                                partitions (e.g., one topic has 3 partitions and another has 5 and both are used in a
 *                                merge).
 * @param sourceTopics            The source topics of the subtopology.
 * @param repartitionSourceTopics The repartition source topics of the subtopology.
 * @param repartitionSinkTopics   The repartition sink topics of the subtopology.
 * @param stateChangelogTopics    The state changelog topics of the subtopology.
 */
public record ConfiguredSubtopology(int numberOfTasks,
                                    Set<String> sourceTopics,
                                    Map<String, ConfiguredInternalTopic> repartitionSourceTopics,
                                    Set<String> repartitionSinkTopics,
                                    Map<String, ConfiguredInternalTopic> stateChangelogTopics) {

    public ConfiguredSubtopology {
        if (numberOfTasks <= 0) {
            throw new IllegalArgumentException("Number of tasks must be positive");
        }
        Objects.requireNonNull(sourceTopics, "sourceTopics can't be null");
        Objects.requireNonNull(repartitionSourceTopics, "repartitionSourceTopics can't be null");
        Objects.requireNonNull(repartitionSinkTopics, "repartitionSinkTopics can't be null");
        Objects.requireNonNull(stateChangelogTopics, "stateChangelogTopics can't be null");
    }

    public StreamsGroupDescribeResponseData.Subtopology asStreamsGroupDescribeSubtopology(String subtopologyId) {
        return new StreamsGroupDescribeResponseData.Subtopology()
            .setSubtopologyId(subtopologyId)
            .setSourceTopics(sourceTopics.stream().sorted().toList())
            .setRepartitionSinkTopics(repartitionSinkTopics.stream().sorted().toList())
            .setRepartitionSourceTopics(repartitionSourceTopics.values().stream()
                .map(ConfiguredInternalTopic::asStreamsGroupDescribeTopicInfo)
                .sorted(Comparator.comparing(StreamsGroupDescribeResponseData.TopicInfo::name)).toList())
            .setStateChangelogTopics(stateChangelogTopics.values().stream()
                .map(ConfiguredInternalTopic::asStreamsGroupDescribeTopicInfo)
                .sorted(Comparator.comparing(StreamsGroupDescribeResponseData.TopicInfo::name)).toList());
    }



}