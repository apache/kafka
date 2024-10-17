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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import java.util.Objects;
import java.util.Optional;

/**
 * This represents universally unique identifier with topic id for a topic partition. However, for this wrapper, we can
 * have an optional topic id with a not null topic partition to account for the functionalities that don't have topic id incorporated yet.
 */
public class TopicOptionalIdPartition {

    private final Optional<Uuid> topicId;
    private final TopicPartition topicPartition;

    /**
     * Create an instance with the provided parameters.
     *
     * @param topicId the topic id
     * @param topicPartition the topic partition
     */
    public TopicOptionalIdPartition(Optional<Uuid> topicId, TopicPartition topicPartition) {
        this.topicId = topicId;
        this.topicPartition = Objects.requireNonNull(topicPartition, "topicPartition can not be null");
    }

    /**
     * @return Universally unique id representing this topic partition.
     */
    public Optional<Uuid> topicId() {
        return topicId;
    }

    /**
     * @return the topic name.
     */
    public String topic() {
        return topicPartition.topic();
    }

    /**
     * @return the partition id.
     */
    public int partition() {
        return topicPartition.partition();
    }

    /**
     * @return Topic partition representing this instance.
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicOptionalIdPartition that = (TopicOptionalIdPartition) o;
        return topicId.equals(that.topicId) &&
            topicPartition.equals(that.topicPartition);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 0;
        if (topicId.isPresent()) {
            result = prime + topicId.get().hashCode();
        }
        result = prime * result + topicPartition.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return topicId.map(uuid -> uuid + ":" + topic() + "-" + partition()).orElseGet(() -> "none" + ":" + topic() + "-" + partition());
    }
}
