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
import org.apache.kafka.common.metadata.MirrorTopicStateChangeRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.node.TopicImageNode;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;


/**
 * Represents a topic in the metadata image.
 *
 * This class is thread-safe.
 */
public final class TopicImage {

    /**
     * The mirror state of a mirrored topic.
     *
     * @param name          The name of the mirror. Never null.
     * @param desiredState  The desired mirror partition state.
     */
    public record Mirror(String name, byte desiredState) {
        public Mirror {
            Objects.requireNonNull(name, "mirror name must not be null");
        }
    }

    private final String name;
    private final Uuid id;
    private final Mirror mirror;
    private final Map<Integer, PartitionRegistration> partitions;

    /**
     * Creates an image of a topic which is not associated with a mirror.
     */
    public TopicImage(String name, Uuid id, Map<Integer, PartitionRegistration> partitions) {
        this(name, id, null, partitions);
    }

    /**
     * Creates an image of a topic.
     *
     * @param mirror The mirror state, or null if the topic is not associated with a mirror.
     */
    public TopicImage(String name, Uuid id, Mirror mirror, Map<Integer, PartitionRegistration> partitions) {
        this.name = name;
        this.id = id;
        this.mirror = mirror;
        this.partitions = Collections.unmodifiableMap(partitions);
    }

    public String name() {
        return name;
    }

    public Uuid id() {
        return id;
    }

    /**
     * Returns the mirror state, or empty if the topic is not associated with a mirror.
     */
    public Optional<Mirror> mirror() {
        return Optional.ofNullable(mirror);
    }

    public Map<Integer, PartitionRegistration> partitions() {
        return partitions;
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(0, new TopicRecord().
            setName(name).
            setTopicId(id));
        mirror().ifPresent(m -> {
            if (!options.metadataVersion().isClusterMirroringSupported()) {
                options.handleLoss("cluster mirror topic state change");
            } else {
                writer.write(0, new MirrorTopicStateChangeRecord().
                        setTopicName(name).
                        setMirrorName(m.name()).
                        setDesiredState(m.desiredState()));
            }
        });
        for (Entry<Integer, PartitionRegistration> entry : partitions.entrySet()) {
            int partitionId = entry.getKey();
            PartitionRegistration partition = entry.getValue();
            writer.write(partition.toRecord(id, partitionId, options));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicImage other)) return false;
        return Objects.equals(name, other.name) &&
            Objects.equals(id, other.id) &&
            Objects.equals(mirror, other.mirror) &&
            Objects.equals(partitions, other.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, mirror, partitions);
    }

    @Override
    public String toString() {
        return new TopicImageNode(this).stringify();
    }
}
