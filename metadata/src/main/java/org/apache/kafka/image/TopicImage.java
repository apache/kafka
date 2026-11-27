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
import org.apache.kafka.server.mirror.MirrorPartitionState;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;


/**
 * Represents a topic in the metadata image.
 *
 * This class is thread-safe.
 */
public record TopicImage(String name, Uuid id, Mirror mirror, Map<Integer, PartitionRegistration> partitions) {

    public sealed interface Mirror permits ActiveMirror, NoMirror {

    }

    /**
     * The mirror state of a mirrored topic.
     *
     * @param name         The name of the mirror. Never null.
     * @param desiredState The desired mirror partition state.
     */
    public record ActiveMirror(String name, MirrorPartitionState desiredState) implements Mirror {
        public ActiveMirror {
            Objects.requireNonNull(name, "mirror name must not be null");
        }
    }

    /**
     * The mirror state when no mirror exists on a topic.
     */
    public record NoMirror() implements Mirror {
    }

    /**
     * Creates an image of a topic which is not associated with a mirror.
     */
    public TopicImage(String name, Uuid id, Map<Integer, PartitionRegistration> partitions) {
        this(name, id, new NoMirror(), partitions);
    }

    /**
     * Creates an image of a topic.
     *
     * @param mirror The mirror state. Must not be null; use {@link NoMirror} if the topic is not
     *               associated with a mirror.
     */
    public TopicImage {
        Objects.requireNonNull(mirror);
        partitions = Collections.unmodifiableMap(partitions);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(0, new TopicRecord().
                setName(name).
                setTopicId(id));
        if (mirror instanceof ActiveMirror activeMirror) {
            if (!options.metadataVersion().isClusterMirroringSupported()) {
                options.handleLoss("cluster mirror topic state change");
            } else {
                writer.write(0, new MirrorTopicStateChangeRecord().
                        setTopicName(name).
                        setMirrorName(activeMirror.name()).
                        setDesiredState(activeMirror.desiredState().value()));
            }
        }
        for (Entry<Integer, PartitionRegistration> entry : partitions.entrySet()) {
            int partitionId = entry.getKey();
            PartitionRegistration partition = entry.getValue();
            writer.write(partition.toRecord(id, partitionId, options));
        }
    }

    @Override
    public String toString() {
        return new TopicImageNode(this).stringify();
    }
}
