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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * An implementation of {@link CoordinatorMetadataImage} that wraps the KRaft MetadataImage.
 */
public class KRaftCoordinatorMetadataImage implements CoordinatorMetadataImage {

    private final MetadataImage metadataImage;

    public KRaftCoordinatorMetadataImage(MetadataImage metadataImage) {
        this.metadataImage = Objects.requireNonNull(metadataImage, "metadataImage must be provided");
    }

    @Override
    public Set<Uuid> topicIds() {
        return Collections.unmodifiableSet(metadataImage.topics().topicsById().keySet());
    }

    @Override
    public Set<String> topicNames() {
        return Collections.unmodifiableSet(metadataImage.topics().topicsByName().keySet());
    }

    @Override
    public Optional<TopicMetadata> topicMetadata(Uuid topicId) {
        TopicImage topicImage = metadataImage.topics().getTopic(topicId);
        if (topicImage == null) return Optional.empty();

        ClusterImage clusterImage = metadataImage.cluster();
        if (clusterImage == null) return Optional.empty();

        return Optional.of(new KraftTopicMetadata(topicImage, clusterImage));
    }

    @Override
    public Optional<TopicMetadata> topicMetadata(String topicName) {
        TopicImage topicImage = metadataImage.topics().getTopic(topicName);
        if (topicImage == null) return Optional.empty();

        ClusterImage clusterImage = metadataImage.cluster();
        if (clusterImage == null) return Optional.empty();

        return Optional.of(new KraftTopicMetadata(topicImage, clusterImage));
    }

    @Override
    public CoordinatorMetadataDelta emptyDelta() {
        return new KRaftCoordinatorMetadataDelta(new MetadataDelta(metadataImage));
    }

    @Override
    public long version() {
        return metadataImage.offset();
    }

    @Override
    public boolean isEmpty() {
        return metadataImage.isEmpty();
    }

    @Override
    public String toString() {
        return metadataImage.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        KRaftCoordinatorMetadataImage other = (KRaftCoordinatorMetadataImage) o;
        return metadataImage.equals(other.metadataImage);
    }

    @Override
    public int hashCode() {
        return metadataImage.hashCode();
    }

    public static class KraftTopicMetadata implements TopicMetadata {
        private final TopicImage topicImage;
        private final ClusterImage clusterImage;

        public KraftTopicMetadata(TopicImage topicImage, ClusterImage clusterImage) {
            this.topicImage = topicImage;
            this.clusterImage = clusterImage;
        }

        @Override
        public String name() {
            return topicImage.name();
        }

        @Override
        public Uuid id() {
            return topicImage.id();
        }

        @Override
        public int partitionCount() {
            return topicImage.partitions().size();
        }

        @Override
        public List<String> partitionRacks(int partition) {
            List<String> racks = new ArrayList<>();
            PartitionRegistration partitionRegistration = topicImage.partitions().get(partition);
            if (partitionRegistration != null) {
                for (int replicaId : partitionRegistration.replicas) {
                    BrokerRegistration broker = clusterImage.broker(replicaId);
                    if (broker != null) {
                        broker.rack().ifPresent(racks::add);
                    }
                }
                return racks;
            } else {
                return List.of();
            }
        }
    }
}
