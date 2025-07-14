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

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Provides metadata to Coordinators (GroupCoordinator, ShareCoordinator, etc) such as topics, partitions, and their configurations.
 * Implementations should be thread-safe and immutable.
 */
public interface CoordinatorMetadataImage {
    CoordinatorMetadataImage EMPTY = emptyImage();

    Set<Uuid> topicIds();

    Set<String> topicNames();

    Optional<TopicMetadata> topicMetadata(String topicName);

    Optional<TopicMetadata> topicMetadata(Uuid topicId);

    CoordinatorMetadataDelta emptyDelta();

    long version();

    boolean isEmpty();

    /**
     * Metadata about a particular topic
     */
    interface TopicMetadata {
        String name();

        Uuid id();

        int partitionCount();

        List<String> partitionRacks(int partitionId);
    }

    private static CoordinatorMetadataImage emptyImage() {

        return new CoordinatorMetadataImage() {
            @Override
            public Set<Uuid> topicIds() {
                return Set.of();
            }

            @Override
            public Set<String> topicNames() {
                return Set.of();
            }

            @Override
            public Optional<TopicMetadata> topicMetadata(String topicName) {
                return Optional.empty();
            }

            @Override
            public Optional<TopicMetadata> topicMetadata(Uuid topicId) {
                return Optional.empty();
            }

            @Override
            public CoordinatorMetadataDelta emptyDelta() {
                return CoordinatorMetadataDelta.EMPTY;
            }

            @Override
            public long version() {
                return 0L;
            }

            @Override
            public boolean isEmpty() {
                return true;
            }
        };
    }

}
