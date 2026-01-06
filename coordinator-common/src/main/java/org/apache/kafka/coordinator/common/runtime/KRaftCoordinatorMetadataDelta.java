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
import org.apache.kafka.image.MetadataDelta;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * An implementation of {@link CoordinatorMetadataDelta} that wraps the KRaft MetadataDelta.
 */
public class KRaftCoordinatorMetadataDelta implements CoordinatorMetadataDelta {

    final MetadataDelta metadataDelta;

    public KRaftCoordinatorMetadataDelta(MetadataDelta metadataDelta) {
        this.metadataDelta = Objects.requireNonNull(metadataDelta, "metadataDelta must be provided");
    }

    @Override
    public Collection<Uuid> createdTopicIds() {
        if (metadataDelta.topicsDelta() == null) {
            return Set.of();
        }
        return metadataDelta.topicsDelta().createdTopicIds();
    }

    @Override
    public Collection<Uuid> changedTopicIds() {
        if (metadataDelta.topicsDelta() == null) {
            return Set.of();
        }
        return metadataDelta.topicsDelta().changedTopics().keySet();
    }

    @Override
    public Set<Uuid> deletedTopicIds() {
        if (metadataDelta.topicsDelta() == null) {
            return Set.of();
        }
        return metadataDelta.topicsDelta().deletedTopicIds();
    }

    @Override
    public CoordinatorMetadataImage image() {
        return new KRaftCoordinatorMetadataImage(metadataDelta.image());
    }

    @Override
    public String toString() {
        return metadataDelta.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        KRaftCoordinatorMetadataDelta other = (KRaftCoordinatorMetadataDelta) o;
        return metadataDelta.equals(other.metadataDelta);
    }

    @Override
    public int hashCode() {
        return metadataDelta.hashCode();
    }
}
