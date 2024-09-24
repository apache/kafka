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
package org.apache.kafka.coordinator.group.streams;

import java.util.List;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupInitializeRequestData;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.coordinator.group.streams.topics.InternalTopicConfig;
import org.apache.kafka.coordinator.group.streams.topics.TopicsInfo;

/**
 * Immutable topology metadata.
 */
public class StreamsTopology {

    private final String topologyId;

    private final Map<String, TopicsInfo> subtopologies;

    public StreamsTopology(final String topologyId,
        final Map<String, TopicsInfo> subtopologies) {
        this.topologyId = topologyId;
        this.subtopologies = subtopologies;
    }

    public String topologyId() {
        return topologyId;
    }

    public Map<String, TopicsInfo> subtopologies() {
        return subtopologies;
    }

    public Set<String> topicSubscription() {
        return subtopologies.values().stream()
            .flatMap(x -> Stream.concat(x.sourceTopics().stream(),
                x.repartitionSourceTopics().values().stream().map(
                    InternalTopicConfig::name))).collect(
                Collectors.toSet());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamsTopology that = (StreamsTopology) o;
        return Objects.deepEquals(topologyId, that.topologyId) && Objects.equals(subtopologies, that.subtopologies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topologyId, subtopologies);
    }

    @Override
    public String toString() {
        return "StreamsTopology{" +
            "topologyId=" + topologyId +
            ", subtopologies=" + subtopologies +
            '}';
    }

    public static StreamsTopology fromRecord(StreamsGroupTopologyValue record) {
        return new StreamsTopology(
            record.topologyId(),
            record.topology().stream().collect(
                Collectors.toMap(Subtopology::subtopologyId, TopicsInfo::fromPersistedSubtopology))
        );
    }

    public static StreamsTopology fromRecord(StreamsGroupInitializeRequestData record) {
        return new StreamsTopology(
            record.topologyId(),
            record.topology().stream().collect(Collectors.toMap(
                StreamsGroupInitializeRequestData.Subtopology::subtopologyId,
                TopicsInfo::fromInitializationSubtopology))
        );
    }

    public List<StreamsGroupDescribeResponseData.Subtopology> toStreamsGroupDescribeTopology() {
        return subtopologies.entrySet().stream().map(
            e -> TopicsInfo.toStreamsGroupDescribeSubtopology(e.getKey(), e.getValue())
        ).collect(Collectors.toList());
    }
}
