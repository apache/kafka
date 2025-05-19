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

package org.apache.kafka.image;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.node.TopicsImageByNameNode;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.immutable.ImmutableMap;
import org.apache.kafka.server.util.TranslatedValueMapView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Represents the topics in the metadata image.
 * <p>
 * This class is thread-safe.
 */
public record TopicsImage(ImmutableMap<Uuid, TopicImage> topicsById, ImmutableMap<String, TopicImage> topicsByName) {
    public static final TopicsImage EMPTY = new TopicsImage(ImmutableMap.empty(), ImmutableMap.empty());
    private static final Logger LOG = LoggerFactory.getLogger(TopicsImage.class);

    public TopicsImage including(TopicImage topic) {
        return new TopicsImage(
            this.topicsById.updated(topic.id(), topic),
            this.topicsByName.updated(topic.name(), topic));
    }

    public boolean isEmpty() {
        return topicsById.isEmpty() && topicsByName.isEmpty();
    }

    public PartitionRegistration getPartition(Uuid id, int partitionId) {
        TopicImage topicImage = topicsById.get(id);
        if (topicImage == null) return null;
        return topicImage.partitions().get(partitionId);
    }

    public TopicImage getTopic(Uuid id) {
        return topicsById.get(id);
    }

    public TopicImage getTopic(String name) {
        return topicsByName.get(name);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        for (Map.Entry<Uuid, TopicImage> entry : topicsById.entrySet()) {
            entry.getValue().write(writer, options);
        }
    }

    /**
     * Expose a view of this TopicsImage as a map from topic names to IDs.
     * <p>
     * Like TopicsImage itself, this map is immutable.
     */
    public Map<String, Uuid> topicNameToIdView() {
        return new TranslatedValueMapView<>(topicsByName, TopicImage::id);
    }

    /**
     * Expose a view of this TopicsImage as a map from IDs to names.
     * <p>
     * Like TopicsImage itself, this map is immutable.
     */
    public Map<Uuid, String> topicIdToNameView() {
        return new TranslatedValueMapView<>(topicsById, TopicImage::name);
    }

    @Override
    public String toString() {
        return new TopicsImageByNameNode(this).stringify();
    }

    /**
     * Returns true if the given topic partition should not be on the current broker according to the metadata image.
     *
     * @param newTopicsImage The new topics image after broker has been reloaded
     * @param brokerId       The ID of the current broker.
     * @param topicId        The topic ID
     * @param partitionId    The partition ID
     * @param log            The log
     * @return true if the topic partition should not exist on the broker, false otherwise.
     */
    public static boolean isStrayReplica(TopicsImage newTopicsImage, int brokerId, Optional<Uuid> topicId, int partitionId, String log) {
        if (topicId.isEmpty()) {
            // Missing topic ID could result from storage failure or unclean shutdown after topic creation but before flushing
            // data to the `partition.metadata` file. And before appending data to the log, the `partition.metadata` is always
            // flushed to disk. So if the topic ID is missing, it mostly means no data was appended, and we can treat this as
            // a stray log.
            LOG.info("The topicId does not exist in {}, treat it as a stray log.", log);
            return true;
        }

        PartitionRegistration partition = newTopicsImage.getPartition(topicId.get(), partitionId);
        if (partition == null) {
            LOG.info("Found stray log dir {}: the topicId {} does not exist in the metadata image.", log, topicId);
            return true;
        } else {
            List<Integer> replicas = Arrays.stream(partition.replicas).boxed().toList();
            if (!replicas.contains(brokerId)) {
                LOG.info("Found stray log dir {}: the current replica assignment {} does not contain the local brokerId {}.",
                        log, replicas.stream().map(String::valueOf).collect(Collectors.joining(", ", "[", "]")), brokerId);
                return true;
            } else {
                return false;
            }
        }
    }
}
