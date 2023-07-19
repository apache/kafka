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

import java.util.Map;
import java.util.Objects;

/**
 * Represents the topics in the metadata image.
 *
 * This class is thread-safe.
 */
public final class TopicsImage {
    public static final TopicsImage EMPTY =  new TopicsImage(ImmutableMap.empty(), ImmutableMap.empty());

    private final ImmutableMap<Uuid, TopicImage> topicsById;
    private final ImmutableMap<String, TopicImage> topicsByName;

    public TopicsImage(
        ImmutableMap<Uuid, TopicImage> topicsById,
        ImmutableMap<String, TopicImage> topicsByName
    ) {
        this.topicsById = topicsById;
        this.topicsByName = topicsByName;
    }

    public TopicsImage including(TopicImage topic) {
        return new TopicsImage(
            this.topicsById.updated(topic.id(), topic),
            this.topicsByName.updated(topic.name(), topic));
    }

    public boolean isEmpty() {
        return topicsById.isEmpty() && topicsByName.isEmpty();
    }

    public ImmutableMap<Uuid, TopicImage> topicsById() {
        return topicsById;
    }

    public ImmutableMap<String, TopicImage> topicsByName() {
        return topicsByName;
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

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TopicsImage)) return false;
        TopicsImage other = (TopicsImage) o;
        return topicsById.equals(other.topicsById) &&
            topicsByName.equals(other.topicsByName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicsById, topicsByName);
    }

    /**
     * Expose a view of this TopicsImage as a map from topic names to IDs.
     *
     * Like TopicsImage itself, this map is immutable.
     */
    public Map<String, Uuid> topicNameToIdView() {
        return new TranslatedValueMapView<>(topicsByName, image -> image.id());
    }

    /**
     * Expose a view of this TopicsImage as a map from IDs to names.
     *
     * Like TopicsImage itself, this map is immutable.
     */
    public Map<Uuid, String> topicIdToNameView() {
        return new TranslatedValueMapView<>(topicsById, image -> image.name());
    }

    @Override
    public String toString() {
        return new TopicsImageByNameNode(this).stringify();
    }
}
