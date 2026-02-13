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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;

import org.slf4j.Logger;

import java.util.Collection;
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
     * Configures the internal topics for the given topology. Given a topology and the metadata image, this method determines the number of
     * partitions for all internal topics and returns a {@link ConfiguredTopology} object.
     *
     * @param logContext    The log context.
     * @param metadataHash  The metadata hash of the group.
     * @param topology      The topology.
     * @param metadataImage The metadata image.
     * @return The configured topology.
     */
    public static ConfiguredTopology configureTopics(LogContext logContext,
                                                     long metadataHash,
                                                     StreamsTopology topology,
                                                     CoordinatorMetadataImage metadataImage,
                                                     Time time) {
        final Logger log = logContext.logger(InternalTopicManager.class);
        final long startTimeMs = time.milliseconds();
        final Collection<StreamsGroupTopologyValue.Subtopology> subtopologies = topology.subtopologies().values();

        final Map<String, Collection<Set<String>>> copartitionGroupsBySubtopology =
            subtopologies.stream()
                .collect(Collectors.toMap(
                    StreamsGroupTopologyValue.Subtopology::subtopologyId,
                    InternalTopicManager::copartitionGroupsFromPersistedSubtopology)
                );

        try {
            Optional<TopicConfigurationException> topicConfigurationException = Optional.empty();

            throwOnMissingSourceTopics(topology, metadataImage);

            Map<String, Integer> decidedPartitionCountsForInternalTopics =
                decidePartitionCounts(logContext, topology, metadataImage, copartitionGroupsBySubtopology);

            final SortedMap<String, ConfiguredSubtopology> configuredSubtopologies =
                subtopologies.stream()
                    .collect(Collectors.toMap(
                        StreamsGroupTopologyValue.Subtopology::subtopologyId,
                        x -> fromPersistedSubtopology(x, metadataImage, decidedPartitionCountsForInternalTopics),
                        (v1, v2) -> {
                            throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));
                        },
                        TreeMap::new
                    ));

            Map<String, CreatableTopic> internalTopicsToCreate = missingInternalTopics(configuredSubtopologies, topology, metadataImage);
            long elapsedMs = time.milliseconds() - startTimeMs;
            if (!internalTopicsToCreate.isEmpty()) {
                topicConfigurationException = Optional.of(TopicConfigurationException.missingInternalTopics(
                    "Internal topics are missing: " + summarizeTopics(internalTopicsToCreate.keySet())
                ));
                log.info("Valid topic configuration found in {}ms, but internal topics are missing for topology epoch {}: {}",
                    elapsedMs, topology.topologyEpoch(), summarizeTopics(internalTopicsToCreate.keySet()));
            } else {
                log.info("Valid topic configuration found in {}ms, topology epoch {} is now initialized.",
                    elapsedMs, topology.topologyEpoch());
            }

            return new ConfiguredTopology(
                topology.topologyEpoch(),
                metadataHash,
                Optional.of(configuredSubtopologies),
                internalTopicsToCreate,
                topicConfigurationException
            );

        } catch (TopicConfigurationException e) {
            long elapsedMs = time.milliseconds() - startTimeMs;
            log.warn("Topic configuration failed for topology epoch {} in {}ms: {}",
                topology.topologyEpoch(), elapsedMs, e.getMessage());
            return new ConfiguredTopology(
                topology.topologyEpoch(),
                metadataHash,
                Optional.empty(),
                Map.of(),
                Optional.of(e)
            );
        }
    }

    private static void throwOnMissingSourceTopics(final StreamsTopology topology,
                                                   final CoordinatorMetadataImage metadataImage) {
        TreeSet<String> sortedMissingTopics = new TreeSet<>();
        for (StreamsGroupTopologyValue.Subtopology subtopology : topology.subtopologies().values()) {
            for (String sourceTopic : subtopology.sourceTopics()) {
                if (metadataImage.topicMetadata(sourceTopic).isEmpty()) {
                    sortedMissingTopics.add(sourceTopic);
                }
            }
        }
        if (!sortedMissingTopics.isEmpty()) {
            throw TopicConfigurationException.missingSourceTopics(
                "Source topics " + summarizeTopics(sortedMissingTopics) + " are missing.");
        }
    }

    private static Map<String, Integer> decidePartitionCounts(final LogContext logContext,
                                                              final StreamsTopology topology,
                                                              final CoordinatorMetadataImage metadataImage,
                                                              final Map<String, Collection<Set<String>>> copartitionGroupsBySubtopology) {
        final Map<String, Integer> decidedPartitionCountsForInternalTopics = new HashMap<>();
        final Function<String, OptionalInt> topicPartitionCountProvider =
            topic -> getPartitionCount(metadataImage, topic, decidedPartitionCountsForInternalTopics);
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
            decidedPartitionCountsForInternalTopics,
            copartitionedTopicsEnforcer
        );

        decidedPartitionCountsForInternalTopics.putAll(changelogTopics.setup());

        return decidedPartitionCountsForInternalTopics;
    }

    private static void enforceCopartitioning(final StreamsTopology topology,
                                              final Map<String, Collection<Set<String>>> copartitionGroupsBySubtopology,
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

    private static Map<String, CreatableTopic> missingInternalTopics(Map<String, ConfiguredSubtopology> subtopologyMap,
                                                                     StreamsTopology topology,
                                                                     CoordinatorMetadataImage metadataImage) {

        final Map<String, CreatableTopic> topicsToCreate = new HashMap<>();
        for (ConfiguredSubtopology subtopology : subtopologyMap.values()) {
            subtopology.repartitionSourceTopics().values()
                .forEach(x -> topicsToCreate.put(x.name(), toCreatableTopic(x)));
            subtopology.stateChangelogTopics().values()
                .forEach(x -> topicsToCreate.put(x.name(), toCreatableTopic(x)));
        }
        for (String topic : topology.requiredTopics()) {
            metadataImage.topicMetadata(topic).ifPresent(topicMetadata -> {
                final CreatableTopic expectedTopic = topicsToCreate.remove(topic);
                if (expectedTopic != null) {
                    if (topicMetadata.partitionCount() != expectedTopic.numPartitions()) {
                        throw TopicConfigurationException.incorrectlyPartitionedTopics("Existing topic " + topic + " has different"
                            + " number of partitions: expected " + expectedTopic.numPartitions() + ", found " + topicMetadata.partitionCount());
                    }
                }
            });
        }
        return topicsToCreate;
    }

    private static OptionalInt getPartitionCount(CoordinatorMetadataImage metadataImage,
                                                 String topic,
                                                 Map<String, Integer> decidedPartitionCountsForInternalTopics) {
        Optional<CoordinatorMetadataImage.TopicMetadata> topicMetadata = metadataImage.topicMetadata(topic);
        if (topicMetadata.isEmpty()) {
            if (decidedPartitionCountsForInternalTopics.containsKey(topic)) {
                return OptionalInt.of(decidedPartitionCountsForInternalTopics.get(topic));
            } else {
                return OptionalInt.empty();
            }
        } else {
            return OptionalInt.of(topicMetadata.get().partitionCount());
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
                                                                  final CoordinatorMetadataImage metadataImage,
                                                                  final Map<String, Integer> decidedPartitionCountsForInternalTopics
    ) {
        return new ConfiguredSubtopology(
            computeNumberOfTasks(subtopology, metadataImage, decidedPartitionCountsForInternalTopics),
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

    private static int computeNumberOfTasks(final StreamsGroupTopologyValue.Subtopology subtopology,
                                            final CoordinatorMetadataImage metadataImage,
                                            final Map<String, Integer> decidedPartitionCountsForInternalTopics) {
        return Stream.concat(
            subtopology.sourceTopics().stream(),
            subtopology.repartitionSourceTopics().stream().map(StreamsGroupTopologyValue.TopicInfo::name)
        ).map(
            topic -> getPartitionCount(metadataImage, topic, decidedPartitionCountsForInternalTopics).orElseThrow(
                () -> new IllegalStateException("Number of partitions must be set for topic " + topic)
            )
        ).max(Integer::compareTo).orElseThrow(
            () -> new IllegalStateException("Subtopology does not contain any source topics")
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
                : Map.of()
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
        ).toList();
    }

    /**
     * Formats a collection of topic names for log and exception messages.
     * Includes up to 3 topic names, and if more are present, appends a summary.
     */
    private static String summarizeTopics(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) {
            return "<none>";
        }
        int maxToShow = 3;
        int size = topics.size();
        return topics.stream()
            .limit(maxToShow)
            .collect(Collectors.joining(", ")) +
            (size > maxToShow ? " and " + (size - maxToShow) + " additional topics" : "");
    }
}
