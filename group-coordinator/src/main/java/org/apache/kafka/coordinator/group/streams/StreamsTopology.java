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

import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupInitializeRequestData;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Immutable topology metadata.
 */
public class StreamsTopology {

    private final String topologyId;

    private final Map<String, Subtopology> subtopologies;

    public StreamsTopology(final String topologyId,
                           final Map<String, Subtopology> subtopologies) {
        this.topologyId = topologyId;
        this.subtopologies = subtopologies;
    }

    public StreamsTopology(final String topologyId,
                           final List<StreamsGroupInitializeRequestData.Subtopology> subtopologies) {
        this.topologyId = topologyId;
        this.subtopologies = new HashMap<>();
        subtopologies.forEach(subtopology -> {
            List<StreamsGroupTopologyValue.TopicInfo> repartitionSourceTopics = subtopology.repartitionSourceTopics().stream()
                .map(topicInfo -> {
                    List<StreamsGroupTopologyValue.TopicConfig> topicConfigs =  topicInfo.topicConfigs() != null ? topicInfo.topicConfigs().stream()
                        .map(config -> new StreamsGroupTopologyValue.TopicConfig().setKey(config.key()).setValue(config.value()))
                        .collect(Collectors.toList()) : null;
                    return new StreamsGroupTopologyValue.TopicInfo().setName(topicInfo.name()).setTopicConfigs(topicConfigs)
                        .setPartitions(topicInfo.partitions());
                }).collect(Collectors.toList());

            List<StreamsGroupTopologyValue.TopicInfo> stateChangelogTopics = subtopology.stateChangelogTopics().stream().map(topicInfo -> {
                List<StreamsGroupTopologyValue.TopicConfig> topicConfigs = topicInfo.topicConfigs() != null ? topicInfo.topicConfigs().stream()
                    .map(config -> new StreamsGroupTopologyValue.TopicConfig().setKey(config.key()).setValue(config.value()))
                    .collect(Collectors.toList()) : null;
                return new StreamsGroupTopologyValue.TopicInfo().setName(topicInfo.name()).setTopicConfigs(topicConfigs);
            }).collect(Collectors.toList());

            this.subtopologies.put(
                subtopology.subtopologyId(),
                new StreamsGroupTopologyValue.Subtopology().setSubtopologyId(subtopology.subtopologyId())
                    .setSourceTopics(subtopology.sourceTopics()).setRepartitionSinkTopics(subtopology.repartitionSinkTopics())
                    .setRepartitionSourceTopics(repartitionSourceTopics).setStateChangelogTopics(stateChangelogTopics));
        });
    }

    public String topologyId() {
        return topologyId;
    }

    public Map<String, Subtopology> subtopologies() {
        return subtopologies;
    }

    public Set<String> topicSubscription() {
        return subtopologies.values().stream()
            .flatMap(x -> Stream.concat(x.sourceTopics().stream(), x.repartitionSourceTopics().stream().map(
                TopicInfo::name))).collect(
                Collectors.toSet());
    }

    public static StreamsTopology fromRecord(StreamsGroupTopologyValue record) {
        return new StreamsTopology(
            record.topologyId(),
            record.topology().stream().collect(Collectors.toMap(Subtopology::subtopologyId, x -> x))
        );
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

    public List<StreamsGroupDescribeResponseData.Subtopology> asStreamsGroupDescribeTopology() {
        return subtopologies.values().stream().map(
            subtopology -> new StreamsGroupDescribeResponseData.Subtopology()
                .setSourceTopicRegex(subtopology.sourceTopicRegex())
                .setSubtopologyId(subtopology.subtopologyId())
                .setSourceTopics(subtopology.sourceTopics())
                .setRepartitionSinkTopics(subtopology.repartitionSinkTopics())
                .setRepartitionSourceTopics(
                    asStreamsGroupDescribeTopicInfo(subtopology.repartitionSourceTopics()))
                .setStateChangelogTopics(
                    asStreamsGroupDescribeTopicInfo(subtopology.stateChangelogTopics()))
        ).collect(Collectors.toList());
    }

    private static List<StreamsGroupDescribeResponseData.TopicInfo> asStreamsGroupDescribeTopicInfo(
        final List<TopicInfo> topicInfos) {
        return topicInfos.stream().map(x ->
            new StreamsGroupDescribeResponseData.TopicInfo()
                .setName(x.name())
                .setPartitions(x.partitions())
                .setTopicConfigs(
                    x.topicConfigs() != null ?
                        x.topicConfigs().stream().map(
                            y -> new StreamsGroupDescribeResponseData.KeyValue()
                                .setKey(y.key())
                                .setValue(y.value())
                        ).collect(Collectors.toList()) : null
                )
        ).collect(Collectors.toList());
    }
}
