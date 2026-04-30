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

import java.util.Collection;
import java.util.Set;

/**
 * Provides metadata deltas to Coordinators (GroupCoordinator, ShareCoordinator, etc) such as changed topics and deleted topics
 * Implementations should be immutable.
 */
public interface CoordinatorMetadataDelta {

    CoordinatorMetadataDelta EMPTY = emptyDelta();

    Collection<Uuid> createdTopicIds();

    Collection<Uuid> changedTopicIds();

    Set<Uuid> deletedTopicIds();

    /**
     * Returns the previous image of the coordinator metadata.
     * This image is a snapshot of the metadata before the delta occurred.
     */
    CoordinatorMetadataImage image();

    private static CoordinatorMetadataDelta emptyDelta() {
        return new CoordinatorMetadataDelta() {
            @Override
            public Collection<Uuid> createdTopicIds() {
                return Set.of();
            }

            @Override
            public Collection<Uuid> changedTopicIds() {
                return Set.of();
            }

            @Override
            public Set<Uuid> deletedTopicIds() {
                return Set.of();
            }

            @Override
            public CoordinatorMetadataImage image() {
                return CoordinatorMetadataImage.EMPTY;
            }
        };
    }
}
