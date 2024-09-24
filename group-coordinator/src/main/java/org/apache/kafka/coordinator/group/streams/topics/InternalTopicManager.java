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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;

public class InternalTopicManager {

    public static Map<String, CreatableTopic> configureTopics(LogContext logContext,
        Map<String, TopicsInfo> subtopologyToTopicsInfo, MetadataImage metadataImage) {

        final Collection<Set<String>> copartitionGroups =
            subtopologyToTopicsInfo.values().stream()
                .flatMap(x -> x.copartitionGroups().stream())
                .collect(Collectors.toList());

        final Function<String, Integer> topicPartitionCountProvider = x -> getPartitionCount(metadataImage, x);
        final CopartitionedTopicsEnforcer copartitionedTopicsEnforcer = new CopartitionedTopicsEnforcer(
            logContext, topicPartitionCountProvider);
        final RepartitionTopics repartitionTopics = new RepartitionTopics(logContext,
            subtopologyToTopicsInfo, copartitionGroups, copartitionedTopicsEnforcer,
            topicPartitionCountProvider);
        final Map<String, InternalTopicConfig> repartitionTopicConfigs = repartitionTopics.setup();

        // Use a topic partition count provider that takes repartitionTopicConfigs into account
        final Function<String, Integer> topicPartitionCountProviderWithRepartition =
            x -> getPartitionCount(metadataImage, x, repartitionTopicConfigs);
        final ChangelogTopics changelogTopics = new ChangelogTopics(logContext,
            subtopologyToTopicsInfo, topicPartitionCountProviderWithRepartition);
        final Map<String, InternalTopicConfig> changelogTopicConfigs = changelogTopics.setup();

        final Map<String, CreatableTopic> topicsToCreate = new HashMap<>();
        repartitionTopicConfigs.values()
            .forEach(x -> topicsToCreate.put(x.name(), toCreatableTopic(x)));
        changelogTopicConfigs.values()
            .forEach(x -> topicsToCreate.put(x.name(), toCreatableTopic(x)));

        // TODO: Validate if existing topics are compatible with the new topics
        for (String topic : metadataImage.topics().topicsByName().keySet()) {
            topicsToCreate.remove(topic);
        }

        return topicsToCreate;
    }

    private static Integer getPartitionCount(MetadataImage metadataImage, String topic) {
        final TopicImage topicImage = metadataImage.topics().getTopic(topic);
        if (topicImage == null) {
            return null;
        } else {
            return topicImage.partitions().size();
        }
    }

    private static Integer getPartitionCount(MetadataImage metadataImage, String topic, Map<String, InternalTopicConfig> additionalTopics) {
        final TopicImage topicImage = metadataImage.topics().getTopic(topic);
        if (topicImage == null) {
            if (additionalTopics.containsKey(topic) && additionalTopics.get(topic).numberOfPartitions().isPresent()) {
                return additionalTopics.get(topic).numberOfPartitions().get();
            } else {
                return null;
            }
        } else {
            return topicImage.partitions().size();
        }
    }

    private static CreatableTopic toCreatableTopic(final InternalTopicConfig config) {

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

}
