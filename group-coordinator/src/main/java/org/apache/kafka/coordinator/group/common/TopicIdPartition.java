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
package org.apache.kafka.coordinator.group.common;

import org.apache.kafka.common.Uuid;

import java.util.Objects;

public class TopicIdPartition {
    private final Uuid topicId;
    private final Integer partition;

    public TopicIdPartition(Uuid topicId, Integer topicPartition) {
        this.topicId = Objects.requireNonNull(topicId, "topicId can not be null");
        this.partition = Objects.requireNonNull(topicPartition, "topicPartition can not be null");
    }

    /**
     * @return Universally unique id representing this topic partition.
     */
    public Uuid topicId() {
        return topicId;
    }

    /**
     * @return the partition id.
     */
    public int partition() {
        return partition;
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
            partition.equals(that.partition);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + topicId.hashCode();
        result = prime * result + partition.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TopicIdPartition(" +
            "topicId=" + topicId +
            ", partition=" + partition +
            ')';
    }
}