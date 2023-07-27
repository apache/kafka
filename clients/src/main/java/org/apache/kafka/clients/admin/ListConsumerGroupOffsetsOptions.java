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

import java.util.List;

/**
 * Options for {@link Admin#listConsumerGroupOffsets(java.util.Map)} and {@link Admin#listConsumerGroupOffsets(String)}.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListConsumerGroupOffsetsOptions extends AbstractOptions<ListConsumerGroupOffsetsOptions> {

    private List<TopicPartition> topicPartitions;
    private boolean requireStable = false;

    /**
     * Set the topic partitions to list as part of the result.
     * {@code null} includes all topic partitions.
     * <p>
     * @deprecated Since 3.3.
     * Use {@link Admin#listConsumerGroupOffsets(java.util.Map, ListConsumerGroupOffsetsOptions)}
     * to specify topic partitions.
     *
     * @param topicPartitions List of topic partitions to include
     * @return This ListGroupOffsetsOptions
     */
    @Deprecated
    public ListConsumerGroupOffsetsOptions topicPartitions(List<TopicPartition> topicPartitions) {
        this.topicPartitions = topicPartitions;
        return this;
    }

    /**
     * Sets an optional requireStable flag.
     */
    public ListConsumerGroupOffsetsOptions requireStable(final boolean requireStable) {
        this.requireStable = requireStable;
        return this;
    }

    /**
     * Returns a list of topic partitions to add as part of the result.
     * <p>
     * @deprecated Since 3.3.
     * Use {@link Admin#listConsumerGroupOffsets(java.util.Map, ListConsumerGroupOffsetsOptions)}
     * to specify topic partitions.
     */
    @Deprecated
    public List<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    public boolean requireStable() {
        return requireStable;
    }
}
