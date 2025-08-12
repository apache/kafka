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
package org.apache.kafka.server.purgatory;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

/**
 * Used by delayed-produce and delayed-fetch operations
 */
public class TopicPartitionOperationKey implements DelayedOperationKey {

    public final String topic;
    public final int partition;

    public TopicPartitionOperationKey(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public TopicPartitionOperationKey(TopicPartition tp) {
        this(tp.topic(), tp.partition());
    }

    public TopicPartitionOperationKey(TopicIdPartition tp) {
        this(tp.topic(), tp.partition());
    }

    @Override
    public String keyLabel() {
        return topic + "-" + partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartitionOperationKey that = (TopicPartitionOperationKey) o;
        return partition == that.partition && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }
}
