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
import org.apache.kafka.server.util.VavrMapAsJava;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Represents the topics in the metadata image.
 *
 * This class is thread-safe.
 */
public final class TopicsImage {
    public static final TopicsImage EMPTY =
        new TopicsImage(io.vavr.collection.HashMap.empty(), io.vavr.collection.HashMap.empty());

    final io.vavr.collection.Map<Uuid, TopicImage> topicsById;
    final io.vavr.collection.Map<String, TopicImage> topicsByName;

    public TopicsImage(io.vavr.collection.Map<Uuid, TopicImage> topicsById,
                       io.vavr.collection.Map<String, TopicImage> topicsByName) {
        this.topicsById = topicsById;
        this.topicsByName = topicsByName;
    }

    public TopicsImage including(TopicImage topic) {
        return new TopicsImage(
            this.topicsById.put(topic.id(), topic),
            this.topicsByName.put(topic.name(), topic));
    }

    public boolean isEmpty() {
        return topicsById.isEmpty() && topicsByName.isEmpty();
    }

    public Map<Uuid, TopicImage> topicsById() {
        return new VavrMapAsJava<>(topicsById, Function.identity());
    }

    public Map<String, TopicImage> topicsByName() {
        return new VavrMapAsJava<>(topicsByName, Function.identity());
    }

    public PartitionRegistration getPartition(Uuid id, int partitionId) {
        TopicImage topicImage = topicsById.get(id).getOrNull();
        if (topicImage == null) return null;
        return topicImage.partitions().get(partitionId);
    }

    public TopicImage getTopic(Uuid id) {
        return topicsById.get(id).getOrNull();
    }

    public TopicImage getTopic(String name) {
        return topicsByName.get(name).getOrNull();
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
        return new VavrMapAsJava<>(topicsByName, TopicImage::id);
    }

    /**
     * Expose a view of this TopicsImage as a map from IDs to names.
     *
     * Like TopicsImage itself, this map is immutable.
     */
    public Map<Uuid, String> topicIdToNameView() {
        return new VavrMapAsJava<>(topicsById, TopicImage::name);
    }

    @Override
    public String toString() {
        return "TopicsImage(topicsById=" + topicsById
            .toStream()
            .map(e -> e._1 + ":" + e._2)
            .collect(Collectors.joining(", ")) +
            ", topicsByName=" +
            topicsByName
            .toStream()
            .map(e -> e._1 + ":" + e._2)
            .collect(Collectors.joining(", ")) +
            ")";
    }
}
