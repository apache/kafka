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
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalTopicManager {

    public static Map<String, CreatableTopic> missingTopics(Map<String, ConfiguredSubtopology> subtopologyMap,
                                                            MetadataImage metadataImage) {

        final Map<String, CreatableTopic> topicsToCreate = new HashMap<>();
        for (ConfiguredSubtopology subtopology : subtopologyMap.values()) {
            subtopology.repartitionSourceTopics().values()
                .forEach(x -> topicsToCreate.put(x.name(), toCreatableTopic(x)));
            subtopology.stateChangelogTopics().values()
                .forEach(x -> topicsToCreate.put(x.name(), toCreatableTopic(x)));
        }
        // TODO: Validate if existing topics are compatible with the new topics
        for (String topic : metadataImage.topics().topicsByName().keySet()) {
            topicsToCreate.remove(topic);
        }
        return topicsToCreate;
    }


    public static Map<String, ConfiguredSubtopology> configureTopics(LogContext logContext,
                                                          List<StreamsGroupTopologyValue.Subtopology> subtopologyList,
                                                          MetadataImage metadataImage) {

        final Logger log = logContext.logger(InternalTopicManager.class);

        final Map<String, ConfiguredSubtopology> configuredSubtopologies =
            subtopologyList.stream()
                .collect(Collectors.toMap(
                    StreamsGroupTopologyValue.Subtopology::subtopologyId,
                    InternalTopicManager::fromPersistedSubtopology)
                );

        final Map<String, Collection<Set<String>>> copartitionGroupsBySubtopology =
            subtopologyList.stream()
                .collect(Collectors.toMap(
                    StreamsGroupTopologyValue.Subtopology::subtopologyId,
                    InternalTopicManager::copartitionGroupsFromPersistedSubtopology)
                );

        final Map<String, ConfiguredInternalTopic> configuredInternalTopics =
            configuredSubtopologies.values().stream().flatMap(x ->
                Stream.concat(
                    x.repartitionSourceTopics().values().stream(),
                    x.stateChangelogTopics().values().stream()
                )
            ).collect(Collectors.toMap(ConfiguredInternalTopic::name, x -> x));

        final Function<String, Integer> topicPartitionCountProvider =
            x -> getPartitionCount(metadataImage, x, configuredInternalTopics);

        configureRepartitionTopics(logContext, configuredSubtopologies, topicPartitionCountProvider);
        enforceCopartitioning(logContext, configuredSubtopologies, copartitionGroupsBySubtopology, topicPartitionCountProvider, log);
        configureChangelogTopics(logContext, configuredSubtopologies, topicPartitionCountProvider);

        return configuredSubtopologies;
    }


    private static void configureRepartitionTopics(LogContext logContext,
                                                   Map<String, ConfiguredSubtopology> configuredSubtopologies,
                                                   Function<String, Integer> topicPartitionCountProvider) {
        final RepartitionTopics repartitionTopics = new RepartitionTopics(logContext,
            configuredSubtopologies,
            topicPartitionCountProvider);
        repartitionTopics.setup();
    }

    private static void enforceCopartitioning(LogContext logContext,
                                              Map<String, ConfiguredSubtopology> configuredSubtopologies,
                                              Map<String, Collection<Set<String>>> copartitionGroupsBySubtopology,
                                              Function<String, Integer> topicPartitionCountProvider,
                                              Logger log) {
        final CopartitionedTopicsEnforcer copartitionedTopicsEnforcer = new CopartitionedTopicsEnforcer(
            logContext, topicPartitionCountProvider);

        final Map<String, ConfiguredInternalTopic> repartitionTopicConfigs =
            configuredSubtopologies.values().stream().flatMap(x ->
                x.repartitionSourceTopics().values().stream()
            ).collect(Collectors.toMap(ConfiguredInternalTopic::name, x -> x));

        if (repartitionTopicConfigs.isEmpty()) {
            log.info("Skipping the repartition topic validation since there are no repartition topics.");
        } else {
            // ensure the co-partitioning topics within the group have the same number of partitions,
            // and enforce the number of partitions for those repartition topics to be the same if they
            // are co-partitioned as well.
            for (Collection<Set<String>> copartitionGroups : copartitionGroupsBySubtopology.values()) {
                for (Set<String> copartitionGroup : copartitionGroups) {
                    copartitionedTopicsEnforcer.enforce(copartitionGroup, repartitionTopicConfigs);
                }
            }
        }
    }

    private static void configureChangelogTopics(LogContext logContext,
                                                 Map<String, ConfiguredSubtopology> configuredSubtopologies,
                                                 Function<String, Integer> topicPartitionCountProvider) {
        final ChangelogTopics changelogTopics = new ChangelogTopics(logContext,
            configuredSubtopologies, topicPartitionCountProvider);
        changelogTopics.setup();
    }

    private static Integer getPartitionCount(MetadataImage metadataImage,
                                             String topic,
                                             Map<String, ConfiguredInternalTopic> configuredInternalTopics) {
        final TopicImage topicImage = metadataImage.topics().getTopic(topic);
        if (topicImage == null) {
            if (configuredInternalTopics.containsKey(topic) && configuredInternalTopics.get(topic).numberOfPartitions().isPresent()) {
                return configuredInternalTopics.get(topic).numberOfPartitions().get();
            } else {
                return null;
            }
        } else {
            return topicImage.partitions().size();
        }
    }

    private static CreatableTopic toCreatableTopic(final ConfiguredInternalTopic config) {

        final CreatableTopic creatableTopic = new CreatableTopic();

        creatableTopic.setName(config.name());

        if (!config.numberOfPartitions().isPresent()) {
            throw new IllegalStateException(
                "Number of partitions must be set for topic " + config.name());
        } else {
            creatableTopic.setNumPartitions(config.numberOfPartitions().get());
        }

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

    private static ConfiguredSubtopology fromPersistedSubtopology(
        final StreamsGroupTopologyValue.Subtopology subtopology) {
        // TODO: Need to resolve regular expressions here.
        return new ConfiguredSubtopology(
            new HashSet<>(subtopology.repartitionSinkTopics()),
            new HashSet<>(subtopology.sourceTopics()),
            subtopology.repartitionSourceTopics().stream()
                .map(InternalTopicManager::fromPersistedTopicInfo)
                .collect(Collectors.toMap(ConfiguredInternalTopic::name, x -> x)),
            subtopology.stateChangelogTopics().stream()
                .map(InternalTopicManager::fromPersistedTopicInfo)
                .collect(Collectors.toMap(ConfiguredInternalTopic::name, x -> x))
        );
    }

    private static ConfiguredInternalTopic fromPersistedTopicInfo(
        final StreamsGroupTopologyValue.TopicInfo topicInfo) {
        return new ConfiguredInternalTopic(
            topicInfo.name(),
            topicInfo.topicConfigs() != null ? topicInfo.topicConfigs().stream()
                .collect(Collectors.toMap(StreamsGroupTopologyValue.TopicConfig::key,
                    StreamsGroupTopologyValue.TopicConfig::value))
                : Collections.emptyMap(),
            topicInfo.partitions() == 0 ? Optional.empty() : Optional.of(topicInfo.partitions()),
            topicInfo.replicationFactor() == 0 ? Optional.empty()
                : Optional.of(topicInfo.replicationFactor()));
    }

    private static Collection<Set<String>> copartitionGroupsFromPersistedSubtopology(
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
}
