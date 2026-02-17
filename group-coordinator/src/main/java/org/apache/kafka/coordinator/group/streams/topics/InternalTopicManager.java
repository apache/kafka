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
     * partitions for all internal topics and returns a {@link TopologyValidationResult} object.
     *
     * @param logContext    The log context.
     * @param metadataHash  The metadata hash of the group.
     * @param topology      The topology.
     * @param metadataImage The metadata image.
     * @return The topology validation result.
     */
    public static TopologyValidationResult configureTopics(LogContext logContext,
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

            Map<String, CreatableTopic> internalTopicsToCreate =
                missingInternalTopics(topology, metadataImage, decidedPartitionCountsForInternalTopics);

            long elapsedMs = time.milliseconds() - startTimeMs;
            if (!internalTopicsToCreate.isEmpty()) {
                topicConfigurationException = Optional.of(TopicConfigurationException.missingInternalTopics(
                    "Internal topics are missing: " + summarizeTopics(internalTopicsToCreate.keySet())
                ));
                log.info("Valid topic configuration found in {}ms, but internal topics are missing for topology epoch {}: {}",
                    elapsedMs, topology.topologyEpoch(), summarizeTopics(internalTopicsToCreate.keySet()));

                return new TopologyValidationResult(
                    topology.topologyEpoch(),
                    metadataHash,
                    internalTopicsToCreate,
                    topicConfigurationException,
                    Optional.empty(),
                    decidedPartitionCountsForInternalTopics
                );
            } else {
                log.info("Valid topic configuration found in {}ms, topology epoch {} is now initialized.",
                    elapsedMs, topology.topologyEpoch());

                Map<String, Integer> numTasksBySubtopology = computeAllMaxPartitions(metadataImage, topology);
                return new TopologyValidationResult(
                    topology.topologyEpoch(),
                    metadataHash,
                    internalTopicsToCreate,
                    Optional.empty(),
                    Optional.of(numTasksBySubtopology),
                    decidedPartitionCountsForInternalTopics
                );
            }

        } catch (TopicConfigurationException e) {
            long elapsedMs = time.milliseconds() - startTimeMs;
            log.warn("Topic configuration failed for topology epoch {} in {}ms: {}",
                topology.topologyEpoch(), elapsedMs, e.getMessage());
            return new TopologyValidationResult(
                topology.topologyEpoch(),
                metadataHash,
                Map.of(),
                Optional.of(e),
                Optional.empty(),
                Map.of()
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

    private static Map<String, CreatableTopic> missingInternalTopics(StreamsTopology topology,
                                                                     CoordinatorMetadataImage metadataImage,
                                                                     Map<String, Integer> decidedPartitionCountsForInternalTopics) {
        final Map<String, CreatableTopic> topicsToCreate = new HashMap<>();

        for (StreamsGroupTopologyValue.Subtopology subtopology : topology.subtopologies().values()) {
            for (StreamsGroupTopologyValue.TopicInfo topicInfo : subtopology.repartitionSourceTopics()) {
                CreatableTopic creatableTopic = toCreatableTopic(topicInfo, decidedPartitionCountsForInternalTopics);
                topicsToCreate.put(topicInfo.name(), creatableTopic);
            }
            for (StreamsGroupTopologyValue.TopicInfo topicInfo : subtopology.stateChangelogTopics()) {
                CreatableTopic creatableTopic = toCreatableTopic(topicInfo, decidedPartitionCountsForInternalTopics);
                topicsToCreate.put(topicInfo.name(), creatableTopic);
            }
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

    private static CreatableTopic toCreatableTopic(final StreamsGroupTopologyValue.TopicInfo topicInfo,
                                                   final Map<String, Integer> decidedPartitionCountsForInternalTopics) {
        final CreatableTopic creatableTopic = new CreatableTopic();
        creatableTopic.setName(topicInfo.name());

        int numPartitions;
        if (topicInfo.partitions() == 0) {
            Integer decidedCount = decidedPartitionCountsForInternalTopics.get(topicInfo.name());
            if (decidedCount == null) {
                throw new IllegalStateException("Number of partitions must be set for topic " + topicInfo.name());
            }
            numPartitions = decidedCount;
        } else {
            numPartitions = topicInfo.partitions();
        }
        creatableTopic.setNumPartitions(numPartitions);

        if (topicInfo.replicationFactor() != 0) {
            creatableTopic.setReplicationFactor(topicInfo.replicationFactor());
        } else {
            creatableTopic.setReplicationFactor((short) -1);
        }

        final CreatableTopicConfigCollection topicConfigs = new CreatableTopicConfigCollection();

        if (topicInfo.topicConfigs() != null) {
            topicInfo.topicConfigs().forEach(config -> {
                final CreatableTopicConfig topicConfig = new CreatableTopicConfig();
                topicConfig.setName(config.key());
                topicConfig.setValue(config.value());
                topicConfigs.add(topicConfig);
            });
        }

        creatableTopic.setConfigs(topicConfigs);

        return creatableTopic;
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

    /**
     * Returns the set of source topics (including repartition source topics) for a subtopology.
     *
     * @param subtopology The subtopology.
     * @return The set of all source topic names.
     */
    public static Set<String> sourceTopicsForSubtopology(StreamsGroupTopologyValue.Subtopology subtopology) {
        Set<String> allSourceTopics = new HashSet<>(subtopology.sourceTopics());
        subtopology.repartitionSourceTopics().forEach(topicInfo -> allSourceTopics.add(topicInfo.name()));
        return allSourceTopics;
    }

    /**
     * Computes the maximum number of partitions for each subtopology based on source topics.
     *
     * @param metadataImage The metadata image containing topic information.
     * @param topology      The streams topology.
     * @return A map from subtopology ID to max partition count.
     */
    static Map<String, Integer> computeAllMaxPartitions(
        CoordinatorMetadataImage metadataImage,
        StreamsTopology topology
    ) {
        Map<String, Integer> result = new HashMap<>();
        for (Map.Entry<String, StreamsGroupTopologyValue.Subtopology> entry : topology.subtopologies().entrySet()) {
            String subtopologyId = entry.getKey();
            StreamsGroupTopologyValue.Subtopology subtopology = entry.getValue();
            int maxPartitions = Stream.concat(
                    subtopology.sourceTopics().stream(),
                    subtopology.repartitionSourceTopics().stream()
                        .map(StreamsGroupTopologyValue.TopicInfo::name)
                ).mapToInt(topic -> metadataImage.topicMetadata(topic)
                    .orElseThrow(() -> new IllegalStateException("Topic " + topic + " not found in metadata image"))
                    .partitionCount())
                .max()
                .orElseThrow(() -> new IllegalStateException("Subtopology " + subtopologyId + " does not contain any source topics"));
            result.put(subtopologyId, maxPartitions);
        }
        return result;
    }
}
