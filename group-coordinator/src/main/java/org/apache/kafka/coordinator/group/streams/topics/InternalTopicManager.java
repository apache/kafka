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

import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.coordinator.group.streams.TopicMetadata;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Responsible for configuring internal topics for a given topology.
 */
public class InternalTopicManager {

    /**
     * Configures the internal topics for the given topology. Given a topology and the topic metadata, this method determines the number of
     * partitions for all internal topics and returns a {@link ConfiguredTopology} object.
     *
     * @param logContext    The log context.
     * @param topology      The topology.
     * @param topicMetadata The topic metadata.
     * @return The configured topology.
     */
    public static ConfiguredTopology configureTopics(LogContext logContext,
                                                     StreamsTopology topology,
                                                     Map<String, TopicMetadata> topicMetadata) {
        final Logger log = logContext.logger(InternalTopicManager.class);
        final Collection<StreamsGroupTopologyValue.Subtopology> subtopologies = topology.subtopologies().values();

        final Map<String, Collection<Set<String>>> copartitionGroupsBySubtopology =
            subtopologies.stream()
                .collect(Collectors.toMap(
                    StreamsGroupTopologyValue.Subtopology::subtopologyId,
                    InternalTopicManager::copartitionGroupsFromPersistedSubtopology)
                );

        try {
            Optional<TopicConfigurationException> topicConfigurationException = Optional.empty();

            throwOnMissingSourceTopics(topology, topicMetadata);

            Map<String, Integer> decidedPartitionCountsForInternalTopics =
                decidePartitionCounts(logContext, topology, topicMetadata, copartitionGroupsBySubtopology, log);

            final SortedMap<String, ConfiguredSubtopology> configuredSubtopologies =
                subtopologies.stream()
                    .collect(Collectors.toMap(
                        StreamsGroupTopologyValue.Subtopology::subtopologyId,
                        x -> fromPersistedSubtopology(x, decidedPartitionCountsForInternalTopics),
                        (v1, v2) -> {
                            throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));
                        },
                        TreeMap::new
                    ));

            Map<String, CreatableTopic> internalTopicsToCreate = missingInternalTopics(configuredSubtopologies, topicMetadata);
            if (!internalTopicsToCreate.isEmpty()) {
                topicConfigurationException = Optional.of(TopicConfigurationException.missingInternalTopics(
                    "Internal topics are missing: " + internalTopicsToCreate.keySet()
                ));
                log.info("Valid topic configuration found, but internal topics are missing for topology epoch {}: {}",
                    topology.topologyEpoch(), topicConfigurationException.get().toString());
            } else {
                log.info("Valid topic configuration found, topology epoch {} is now initialized.", topology.topologyEpoch());
            }

            return new ConfiguredTopology(
                topology.topologyEpoch(),
                Optional.of(configuredSubtopologies),
                internalTopicsToCreate,
                topicConfigurationException
            );

        } catch (TopicConfigurationException e) {
            log.warn("Topic configuration failed for topology epoch {}: {} ",
                topology.topologyEpoch(), e.toString());
            return new ConfiguredTopology(
                topology.topologyEpoch(),
                Optional.empty(),
                Map.of(),
                Optional.of(e)
            );
        }
    }

    private static void throwOnMissingSourceTopics(final StreamsTopology topology,
                                                   final Map<String, TopicMetadata> topicMetadata) {
        TreeSet<String> sortedMissingTopics = new TreeSet<>();
        for (StreamsGroupTopologyValue.Subtopology subtopology : topology.subtopologies().values()) {
            for (String sourceTopic : subtopology.sourceTopics()) {
                if (!topicMetadata.containsKey(sourceTopic)) {
                    sortedMissingTopics.add(sourceTopic);
                }
            }
        }
        if (!sortedMissingTopics.isEmpty()) {
            throw TopicConfigurationException.missingSourceTopics(
                "Source topics " + String.join(", ", sortedMissingTopics) + " are missing.");
        }
    }

    private static Map<String, Integer> decidePartitionCounts(final LogContext logContext,
                                                              final StreamsTopology topology,
                                                              final Map<String, TopicMetadata> topicMetadata,
                                                              final Map<String, Collection<Set<String>>> copartitionGroupsBySubtopology,
                                                              final Logger log) {
        final Map<String, Integer> decidedPartitionCountsForInternalTopics = new HashMap<>();
        final Function<String, OptionalInt> topicPartitionCountProvider =
            topic -> getPartitionCount(topicMetadata, topic, decidedPartitionCountsForInternalTopics);
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            logContext,
            topology.subtopologies().values(),
            topicPartitionCountProvider);
        final CopartitionedTopicsEnforcer copartitionedTopicsEnforcer = new CopartitionedTopicsEnforcer(
            logContext,
            topicPartitionCountProvider);
        final ChangelogTopics changelogTopics = new ChangelogTopics(logContext,
            topology.subtopologies().values(),
            topicPartitionCountProvider);

        decidedPartitionCountsForInternalTopics.putAll(repartitionTopics.setup());

        enforceCopartitioning(
            topology,
            copartitionGroupsBySubtopology,
            log,
            decidedPartitionCountsForInternalTopics,
            copartitionedTopicsEnforcer
        );

        decidedPartitionCountsForInternalTopics.putAll(changelogTopics.setup());

        return decidedPartitionCountsForInternalTopics;
    }

    private static void enforceCopartitioning(final StreamsTopology topology,
                                              final Map<String, Collection<Set<String>>> copartitionGroupsBySubtopology,
                                              final Logger log,
                                              final Map<String, Integer> decidedPartitionCountsForInternalTopics,
                                              final CopartitionedTopicsEnforcer copartitionedTopicsEnforcer) {
        final Set<String> fixedRepartitionTopics =
            topology.subtopologies().values().stream().flatMap(x ->
                x.repartitionSourceTopics().stream().filter(y -> y.partitions() != 0)
            ).map(StreamsGroupTopologyValue.TopicInfo::name).collect(Collectors.toSet());
        final Set<String> flexibleRepartitionTopics =
            topology.subtopologies().values().stream().flatMap(x ->
                x.repartitionSourceTopics().stream().filter(y -> y.partitions() == 0)
            ).map(StreamsGroupTopologyValue.TopicInfo::name).collect(Collectors.toSet());

        if (fixedRepartitionTopics.isEmpty() && flexibleRepartitionTopics.isEmpty()) {
            log.info("Skipping the repartition topic validation since there are no repartition topics.");
        } else {
            // ensure the co-partitioning topics within the group have the same number of partitions,
            // and enforce the number of partitions for those repartition topics to be the same if they
            // are co-partitioned as well.
            for (Collection<Set<String>> copartitionGroups : copartitionGroupsBySubtopology.values()) {
                for (Set<String> copartitionGroup : copartitionGroups) {
                    decidedPartitionCountsForInternalTopics.putAll(
                        copartitionedTopicsEnforcer.enforce(copartitionGroup, fixedRepartitionTopics, flexibleRepartitionTopics));
                }
            }
        }
    }

    private static Map<String, CreatableTopic> missingInternalTopics(Map<String, ConfiguredSubtopology> subtopologyMap,
                                                                     Map<String, TopicMetadata> topicMetadata) {

        final Map<String, CreatableTopic> topicsToCreate = new HashMap<>();
        for (ConfiguredSubtopology subtopology : subtopologyMap.values()) {
            subtopology.repartitionSourceTopics().values()
                .forEach(x -> topicsToCreate.put(x.name(), toCreatableTopic(x)));
            subtopology.stateChangelogTopics().values()
                .forEach(x -> topicsToCreate.put(x.name(), toCreatableTopic(x)));
        }
        for (Map.Entry<String, TopicMetadata> topic : topicMetadata.entrySet()) {
            final TopicMetadata existingTopic = topic.getValue();
            final CreatableTopic expectedTopic = topicsToCreate.remove(topic.getKey());
            if (expectedTopic != null) {
                if (existingTopic.numPartitions() != expectedTopic.numPartitions()) {
                    throw TopicConfigurationException.incorrectlyPartitionedTopics("Existing topic " + topic.getKey() + " has different"
                        + " number of partitions: expected " + expectedTopic.numPartitions() + ", found " + existingTopic.numPartitions());
                }
            }
        }
        return topicsToCreate;
    }

    private static OptionalInt getPartitionCount(Map<String, TopicMetadata> topicMetadata,
                                                 String topic,
                                                 Map<String, Integer> decidedPartitionCountsForInternalTopics) {
        final TopicMetadata metadata = topicMetadata.get(topic);
        if (metadata == null) {
            if (decidedPartitionCountsForInternalTopics.containsKey(topic)) {
                return OptionalInt.of(decidedPartitionCountsForInternalTopics.get(topic));
            } else {
                return OptionalInt.empty();
            }
        } else {
            return OptionalInt.of(metadata.numPartitions());
        }
    }

    private static CreatableTopic toCreatableTopic(final ConfiguredInternalTopic config) {

        final CreatableTopic creatableTopic = new CreatableTopic();

        creatableTopic.setName(config.name());
        creatableTopic.setNumPartitions(config.numberOfPartitions());

        if (config.replicationFactor().isPresent() && config.replicationFactor().get() != 0) {
            creatableTopic.setReplicationFactor(config.replicationFactor().get());
        } else {
            creatableTopic.setReplicationFactor((short) -1);
        }

        final CreatableTopicConfigCollection topicConfigs = new CreatableTopicConfigCollection();

        config.topicConfigs().forEach((k, v) -> {
            final CreatableTopicConfig topicConfig = new CreatableTopicConfig();
            topicConfig.setName(k);
            topicConfig.setValue(v);
            topicConfigs.add(topicConfig);
        });

        creatableTopic.setConfigs(topicConfigs);

        return creatableTopic;
    }

    private static ConfiguredSubtopology fromPersistedSubtopology(final StreamsGroupTopologyValue.Subtopology subtopology,
                                                                  final Map<String, Integer> decidedPartitionCountsForInternalTopics
    ) {
        return new ConfiguredSubtopology(
            new HashSet<>(subtopology.sourceTopics()),
            subtopology.repartitionSourceTopics().stream()
                .map(x -> fromPersistedTopicInfo(x, decidedPartitionCountsForInternalTopics))
                .collect(Collectors.toMap(ConfiguredInternalTopic::name, x -> x)),
            new HashSet<>(subtopology.repartitionSinkTopics()),
            subtopology.stateChangelogTopics().stream()
                .map(x -> fromPersistedTopicInfo(x, decidedPartitionCountsForInternalTopics))
                .collect(Collectors.toMap(ConfiguredInternalTopic::name, x -> x))
        );
    }

    private static ConfiguredInternalTopic fromPersistedTopicInfo(final StreamsGroupTopologyValue.TopicInfo topicInfo,
                                                                  final Map<String, Integer> decidedPartitionCountsForInternalTopics) {
        if (topicInfo.partitions() == 0 && !decidedPartitionCountsForInternalTopics.containsKey(topicInfo.name())) {
            throw new IllegalStateException("Number of partitions must be set for topic " + topicInfo.name());
        }

        return new ConfiguredInternalTopic(
            topicInfo.name(),
            topicInfo.partitions() == 0 ? decidedPartitionCountsForInternalTopics.get(topicInfo.name()) : topicInfo.partitions(),
            topicInfo.replicationFactor() == 0 ? Optional.empty()
                : Optional.of(topicInfo.replicationFactor()),
            topicInfo.topicConfigs() != null ? topicInfo.topicConfigs().stream()
                .collect(Collectors.toMap(StreamsGroupTopologyValue.TopicConfig::key,
                    StreamsGroupTopologyValue.TopicConfig::value))
                : Collections.emptyMap()
        );
    }

    private static Collection<Set<String>> copartitionGroupsFromPersistedSubtopology(
        final StreamsGroupTopologyValue.Subtopology subtopology
    ) {
        return subtopology.copartitionGroups().stream().map(copartitionGroup ->
            Stream.concat(
                copartitionGroup.sourceTopics().stream()
                    .map(i -> subtopology.sourceTopics().get(i)),
                copartitionGroup.repartitionSourceTopics().stream()
                    .map(i -> subtopology.repartitionSourceTopics().get(i).name())
            ).collect(Collectors.toSet())
        ).collect(Collectors.toList());
    }
}
