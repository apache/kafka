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
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.util.TranslatedValueMapView;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * Represents the topics in the metadata image.
 *
 * This class is thread-safe.
 */
public final class TopicsImage {
    public static final TopicsImage EMPTY =
        new TopicsImage(Collections.emptyMap(), Collections.emptyMap());

    private final Map<Uuid, TopicImage> topicsById;
    private final Map<String, TopicImage> topicsByName;

    public TopicsImage(Map<Uuid, TopicImage> topicsById,
                       Map<String, TopicImage> topicsByName) {
        this.topicsById = Collections.unmodifiableMap(topicsById);
        this.topicsByName = Collections.unmodifiableMap(topicsByName);
    }

    public boolean isEmpty() {
        return topicsById.isEmpty() && topicsByName.isEmpty();
    }

    public Map<Uuid, TopicImage> topicsById() {
        return topicsById;
    }

    public Map<String, TopicImage> topicsByName() {
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
        for (TopicImage topicImage : topicsById.values()) {
            topicImage.write(writer, options);
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
        return "TopicsImage(topicsById=" + topicsById.entrySet().stream().
            map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", ")) +
            ", topicsByName=" + topicsByName.entrySet().stream().
            map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", ")) +
            ")";
    }
}
