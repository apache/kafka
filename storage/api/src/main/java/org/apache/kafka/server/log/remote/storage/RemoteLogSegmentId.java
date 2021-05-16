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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;

/**
 * This class represents a universally unique identifier associated to a topic partition's log segment. This will be
 * regenerated for every attempt of copying a specific log segment in {@link RemoteStorageManager#copyLogSegmentData(RemoteLogSegmentMetadata, LogSegmentData)}.
 * Once it is stored in remote storage, it is used to access that segment later from remote log metadata storage.
 */
@InterfaceStability.Evolving
public class RemoteLogSegmentId {

    private final TopicIdPartition topicIdPartition;
    private final Uuid id;

    public RemoteLogSegmentId(TopicIdPartition topicIdPartition, Uuid id) {
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");
        this.id = Objects.requireNonNull(id, "id can not be null");
    }

    /**
     * @return TopicIdPartition of this remote log segment.
     */
    public TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    /**
     * @return Universally Unique Id of this remote log segment.
     */
    public Uuid id() {
        return id;
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentId{" +
               "topicIdPartition=" + topicIdPartition +
               ", id=" + id +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteLogSegmentId that = (RemoteLogSegmentId) o;
        return Objects.equals(topicIdPartition, that.topicIdPartition) && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicIdPartition, id);
    }

}
