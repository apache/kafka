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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Specification of share group offsets to list using {@link Admin#listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)}.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListShareGroupOffsetsSpec {

    private Collection<TopicPartition> topicPartitions;

    /**
     * Set the topic partitions whose offsets are to be listed for a share group.
     *
     * @param topicPartitions List of topic partitions to include
     */
    public ListShareGroupOffsetsSpec topicPartitions(Collection<TopicPartition> topicPartitions) {
        this.topicPartitions = topicPartitions;
        return this;
    }

    /**
     * Returns the topic partitions whose offsets are to be listed for a share group.
     */
    public Collection<TopicPartition> topicPartitions() {
        return topicPartitions == null ? List.of() : topicPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ListShareGroupOffsetsSpec)) {
            return false;
        }
        ListShareGroupOffsetsSpec that = (ListShareGroupOffsetsSpec) o;
        return Objects.equals(topicPartitions, that.topicPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartitions);
    }

    @Override
    public String toString() {
        return "ListShareGroupOffsetsSpec(" +
            "topicPartitions=" + (topicPartitions != null ? topicPartitions : "null") +
            ')';
    }
}
