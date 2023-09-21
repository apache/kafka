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
package org.apache.kafka.server.common;

import org.apache.kafka.common.Uuid;

/**
 * Represents a partition using its unique topic Id and partition number.
 */
public final class TopicIdPartition {
    private final Uuid topicId;
    private final Integer partitionId;

    public TopicIdPartition(Uuid topicId, int partitionId) {
        this.topicId = topicId;
        this.partitionId = partitionId;
    }

    /**
     * @return Universally unique Id representing this topic partition.
     */
    public Uuid topicId() {
        return topicId;
    }

    /**
     * @return The partition Id.
     */
    public int partitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicIdPartition that = (TopicIdPartition) o;
        return topicId.equals(that.topicId) &&
            partitionId.equals(that.partitionId);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + topicId.hashCode();
        result = prime * result + partitionId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TopicIdPartition(" +
            "topicId=" + topicId +
            ", partition=" + partitionId +
            ')';
    }
}