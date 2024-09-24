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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupInitializeRequestData;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.TopicInfo;

/**
 * Internal representation of the topics used by a subtopology.
 */
public class TopicsInfo {

    private List<String> repartitionSinkTopics;
    private List<String> sourceTopics;
    private List<String> sourceTopicsRegex;
    private Map<String, InternalTopicConfig> stateChangelogTopics;
    private Map<String, InternalTopicConfig> repartitionSourceTopics;
    private Collection<Set<String>> copartitionGroups;

    public TopicsInfo() {
        this.repartitionSinkTopics = new ArrayList<>();
        this.sourceTopics = new ArrayList<>();
        this.sourceTopicsRegex = new ArrayList<>();
        this.stateChangelogTopics = new HashMap<>();
        this.repartitionSourceTopics = new HashMap<>();
        this.copartitionGroups = new ArrayList<>();
    }

    public TopicsInfo(
        final List<String> repartitionSinkTopics,
        final List<String> sourceTopics,
        final List<String> sourceTopicsRegex,
        final Map<String, InternalTopicConfig> repartitionSourceTopics,
        final Map<String, InternalTopicConfig> stateChangelogTopics,
        final Collection<Set<String>> copartitionGroups
    ) {
        this.repartitionSinkTopics = repartitionSinkTopics;
        this.sourceTopics = sourceTopics;
        this.sourceTopicsRegex = sourceTopicsRegex;
        this.stateChangelogTopics = stateChangelogTopics;
        this.repartitionSourceTopics = repartitionSourceTopics;
        this.copartitionGroups = copartitionGroups;

        if (this.sourceTopicsRegex != null && !this.sourceTopicsRegex.isEmpty()) {
            // TODO: Implement support for regular expressions
            throw new UnsupportedOperationException("Regular expressions are not supported");
        }
    }

    public List<String> sourceTopicsRegex() {
        return sourceTopicsRegex;
    }

    public List<String> repartitionSinkTopics() {
        return repartitionSinkTopics;
    }

    public List<String> sourceTopics() {
        return sourceTopics;
    }

    public Map<String, InternalTopicConfig> stateChangelogTopics() {
        return stateChangelogTopics;
    }

    public Map<String, InternalTopicConfig> repartitionSourceTopics() {
        return repartitionSourceTopics;
    }

    public Collection<Set<String>> copartitionGroups() {
        return copartitionGroups;
    }

    public TopicsInfo setRepartitionSinkTopics(final List<String> repartitionSinkTopics) {
        this.repartitionSinkTopics = repartitionSinkTopics;
        return this;
    }

    public TopicsInfo setSourceTopics(final List<String> sourceTopics) {
        this.sourceTopics = sourceTopics;
        return this;
    }

    public TopicsInfo setSourceTopicsRegex(final List<String> sourceTopicsRegex) {
        this.sourceTopicsRegex = sourceTopicsRegex;
        return this;
    }

    public TopicsInfo setStateChangelogTopics(
        final Map<String, InternalTopicConfig> stateChangelogTopics) {
        this.stateChangelogTopics = stateChangelogTopics;
        return this;
    }

    public TopicsInfo setRepartitionSourceTopics(
        final Map<String, InternalTopicConfig> repartitionSourceTopics) {
        this.repartitionSourceTopics = repartitionSourceTopics;
        return this;
    }

    public TopicsInfo setCopartitionGroups(final Collection<Set<String>> copartitionGroups) {
        this.copartitionGroups = copartitionGroups;
        return this;
    }

    /**
     * Returns the config for any changelogs that must be prepared for this topic group, ie
     * excluding any source topics that are reused as a changelog
     */
    public Set<InternalTopicConfig> nonSourceChangelogTopics() {
        final Set<InternalTopicConfig> topicConfigs = new HashSet<>();
        for (final Map.Entry<String, InternalTopicConfig> changelogTopicEntry : stateChangelogTopics.entrySet()) {
            if (!sourceTopics.contains(changelogTopicEntry.getKey())) {
                topicConfigs.add(changelogTopicEntry.getValue());
            }
        }
        return topicConfigs;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TopicsInfo that = (TopicsInfo) o;
        return Objects.equals(repartitionSinkTopics, that.repartitionSinkTopics)
            && Objects.equals(sourceTopics, that.sourceTopics)
            && Objects.equals(sourceTopicsRegex, that.sourceTopicsRegex)
            && Objects.equals(stateChangelogTopics, that.stateChangelogTopics)
            && Objects.equals(repartitionSourceTopics, that.repartitionSourceTopics)
            && Objects.equals(copartitionGroups, that.copartitionGroups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(repartitionSinkTopics, sourceTopics, sourceTopicsRegex,
            stateChangelogTopics, repartitionSourceTopics, copartitionGroups);
    }

    @Override
    public String toString() {
        return "TopicsInfo{" +
            "repartitionSinkTopics=" + repartitionSinkTopics +
            ", sourceTopics=" + sourceTopics +
            ", sourceTopicsRegex=" + sourceTopicsRegex +
            ", stateChangelogTopics=" + stateChangelogTopics +
            ", repartitionSourceTopics=" + repartitionSourceTopics +
            ", copartitionGroups=" + copartitionGroups +
            '}';
    }

    public static TopicsInfo fromInitializationSubtopology(
        final StreamsGroupInitializeRequestData.Subtopology subtopology) {
        return new TopicsInfo(
            subtopology.repartitionSinkTopics(),
            subtopology.sourceTopics(),
            subtopology.sourceTopicRegex(),
            subtopology.repartitionSourceTopics().stream()
                .map(TopicsInfo::fromInitializationTopicInfo)
                .collect(Collectors.toMap(InternalTopicConfig::name, x -> x)),
            subtopology.stateChangelogTopics().stream()
                .map(TopicsInfo::fromInitializationTopicInfo)
                .collect(Collectors.toMap(InternalTopicConfig::name, x -> x)),
            fromInitializationCopartitionGroups(subtopology)
        );
    }

    private static InternalTopicConfig fromInitializationTopicInfo(
        final StreamsGroupInitializeRequestData.TopicInfo topicInfo) {
        return new InternalTopicConfig(
            topicInfo.name(),
            topicInfo.topicConfigs() != null ? topicInfo.topicConfigs().stream()
                .collect(Collectors.toMap(StreamsGroupInitializeRequestData.TopicConfig::key,
                    StreamsGroupInitializeRequestData.TopicConfig::value))
                : Collections.emptyMap(),
            topicInfo.partitions() == 0 ? Optional.empty() : Optional.of(topicInfo.partitions()),
            topicInfo.replicationFactor() == 0 ? Optional.empty()
                : Optional.of(topicInfo.replicationFactor()));
    }

    private static Collection<Set<String>> fromInitializationCopartitionGroups(
        final StreamsGroupInitializeRequestData.Subtopology subtopology) {
        return subtopology.copartitionGroups().stream().map(copartitionGroup ->
            Stream.concat(
                copartitionGroup.sourceTopics().stream()
                    .map(i -> subtopology.sourceTopics().get(i)),
                copartitionGroup.repartitionSourceTopics().stream()
                    .map(i -> subtopology.repartitionSourceTopics().get(i).name())
            ).collect(Collectors.toSet())
        ).collect(Collectors.toList());
    }

    public static TopicsInfo fromPersistedSubtopology(
        final StreamsGroupTopologyValue.Subtopology subtopology) {
        return new TopicsInfo(
            subtopology.repartitionSinkTopics(),
            subtopology.sourceTopics(),
            subtopology.sourceTopicRegex(),
            subtopology.repartitionSourceTopics().stream()
                .map(TopicsInfo::fromPersistedTopicInfo)
                .collect(Collectors.toMap(InternalTopicConfig::name, x -> x)),
            subtopology.stateChangelogTopics().stream()
                .map(TopicsInfo::fromPersistedTopicInfo)
                .collect(Collectors.toMap(InternalTopicConfig::name, x -> x)),
            fromPersistedCopartitionGroups(subtopology)
        );
    }

    private static InternalTopicConfig fromPersistedTopicInfo(
        final StreamsGroupTopologyValue.TopicInfo topicInfo) {
        return new InternalTopicConfig(
            topicInfo.name(),
            topicInfo.topicConfigs() != null ? topicInfo.topicConfigs().stream()
                .collect(Collectors.toMap(StreamsGroupTopologyValue.TopicConfig::key,
                    StreamsGroupTopologyValue.TopicConfig::value))
                : Collections.emptyMap(),
            topicInfo.partitions() == 0 ? Optional.empty() : Optional.of(topicInfo.partitions()),
            topicInfo.replicationFactor() == 0 ? Optional.empty()
                : Optional.of(topicInfo.replicationFactor()));
    }

    private static Collection<Set<String>> fromPersistedCopartitionGroups(
        final StreamsGroupTopologyValue.Subtopology subtopology) {
        return subtopology.copartitionGroups().stream().map(copartitionGroup ->
            Stream.concat(
                copartitionGroup.sourceTopics().stream()
                    .map(i -> subtopology.sourceTopics().get(i)),
                copartitionGroup.repartitionSourceTopics().stream()
                    .map(i -> subtopology.repartitionSourceTopics().get(i).name())
            ).collect(Collectors.toSet())
        ).collect(Collectors.toList());
    }

    public static StreamsGroupDescribeResponseData.Subtopology toStreamsGroupDescribeSubtopology(
        String subtopologyId, TopicsInfo subtopology) {
        return new StreamsGroupDescribeResponseData.Subtopology()
            .setSourceTopicRegex(subtopology.sourceTopicsRegex())
            .setSubtopologyId(subtopologyId)
            .setSourceTopics(new ArrayList<>(subtopology.sourceTopics()))
            .setSourceTopicRegex(subtopology.sourceTopicsRegex())
            .setRepartitionSinkTopics(new ArrayList<>(subtopology.repartitionSinkTopics()))
            .setRepartitionSourceTopics(
                subtopology.repartitionSourceTopics().values().stream().map(
                    TopicsInfo::toStreamsGroupDescribeTopicInfo
                ).collect(Collectors.toList())
            )
            .setStateChangelogTopics(
                subtopology.stateChangelogTopics().values().stream().map(
                    TopicsInfo::toStreamsGroupDescribeTopicInfo
                ).collect(Collectors.toList())
            );
    }

    private static StreamsGroupDescribeResponseData.TopicInfo toStreamsGroupDescribeTopicInfo(
        final InternalTopicConfig internalTopicConfig) {
        return new StreamsGroupDescribeResponseData.TopicInfo()
            .setName(internalTopicConfig.name())
            .setPartitions(internalTopicConfig.numberOfPartitions().isPresent()
                ? internalTopicConfig.numberOfPartitions().get() : 0)
            .setReplicationFactor(internalTopicConfig.replicationFactor().isPresent()
                ? internalTopicConfig.replicationFactor().get() : 0)
            .setTopicConfigs(
                internalTopicConfig.topicConfigs() != null ?
                    internalTopicConfig.topicConfigs().entrySet().stream().map(
                        y -> new StreamsGroupDescribeResponseData.KeyValue()
                            .setKey(y.getKey())
                            .setValue(y.getValue())
                    ).collect(Collectors.toList()) : null
            );
    }

    public static StreamsGroupTopologyValue.Subtopology toStreamsGroupTopologyValueSubtopology(
        String subtopologyId, TopicsInfo subtopology) {

        final Map<String, Integer> sourceTopicsMap =
            IntStream.range(0, subtopology.sourceTopics.size())
                .boxed()
                .collect(Collectors.toMap(subtopology.sourceTopics::get, i -> i));

        final List<TopicInfo> repartitionSourceTopicList =
            subtopology.repartitionSourceTopics().values().stream().map(
                TopicsInfo::toStreamsGroupTopologyValueTopicInfo
            ).collect(Collectors.toList());

        final Map<String, Integer> repartitionSourceTopicsMap =
            IntStream.range(0, repartitionSourceTopicList.size())
                .boxed()
                .collect(Collectors.toMap(x -> repartitionSourceTopicList.get(x).name(), i -> i));

        return new StreamsGroupTopologyValue.Subtopology()
            .setSourceTopicRegex(subtopology.sourceTopicsRegex())
            .setSubtopologyId(subtopologyId)
            .setSourceTopics(subtopology.sourceTopics)
            .setSourceTopicRegex(subtopology.sourceTopicsRegex())
            .setRepartitionSinkTopics(subtopology.repartitionSinkTopics)
            .setRepartitionSourceTopics(repartitionSourceTopicList)
            .setStateChangelogTopics(
                subtopology.stateChangelogTopics().values().stream().map(
                    TopicsInfo::toStreamsGroupTopologyValueTopicInfo
                ).collect(Collectors.toList())
            ).setCopartitionGroups(
                subtopology.copartitionGroups().stream().map(
                    x -> toStreamsGroupTopologyValueSubtopologyCopartitionGroup(x, sourceTopicsMap,
                        repartitionSourceTopicsMap)
                ).collect(Collectors.toList())
            );
    }

    private static StreamsGroupTopologyValue.CopartitionGroup toStreamsGroupTopologyValueSubtopologyCopartitionGroup(
        final Set<String> topicNames,
        final Map<String, Integer> sourceTopicsMap,
        final Map<String, Integer> repartitionSourceTopics) {
        StreamsGroupTopologyValue.CopartitionGroup copartitionGroup = new StreamsGroupTopologyValue.CopartitionGroup();

        topicNames.forEach(topicName -> {
            if (sourceTopicsMap.containsKey(topicName)) {
                copartitionGroup.sourceTopics().add(sourceTopicsMap.get(topicName));
            } else if (repartitionSourceTopics.containsKey(topicName)) {
                copartitionGroup.repartitionSourceTopics()
                    .add(repartitionSourceTopics.get(topicName));
            } else {
                throw new IllegalStateException(
                    "Source topic not found in subtopology: " + topicName);
            }
        });

        return copartitionGroup;
    }

    private static StreamsGroupTopologyValue.TopicInfo toStreamsGroupTopologyValueTopicInfo(
        final InternalTopicConfig internalTopicConfig) {
        return new StreamsGroupTopologyValue.TopicInfo()
            .setName(internalTopicConfig.name())
            .setPartitions(internalTopicConfig.numberOfPartitions().isPresent()
                ? internalTopicConfig.numberOfPartitions().get() : 0)
            .setReplicationFactor(internalTopicConfig.replicationFactor().isPresent()
                ? internalTopicConfig.replicationFactor().get() : 0)
            .setTopicConfigs(
                internalTopicConfig.topicConfigs() != null ?
                    internalTopicConfig.topicConfigs().entrySet().stream().map(
                        y -> new StreamsGroupTopologyValue.TopicConfig()
                            .setKey(y.getKey())
                            .setValue(y.getValue())
                    ).collect(Collectors.toList()) : null
            );
    }
    
}