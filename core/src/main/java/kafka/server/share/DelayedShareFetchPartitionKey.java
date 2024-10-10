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
package kafka.server.share;

import kafka.server.DelayedOperationKey;

import java.util.Objects;

/**
 * A key for delayed share fetch purgatory that refers to the topic partition. Since the below replicaManager functionalities
 * use TopicPartition and not TopicIdPartition, hence we are using the same here.
 * 1. Determine if HWM has moved
 * 2. Determine if a replica becomes a follower
 * 3. Know if the replica is deleted from a broker
 */
public class DelayedShareFetchPartitionKey implements  DelayedShareFetchKey, DelayedOperationKey {
    private final String topic;
    private final int partition;

    public DelayedShareFetchPartitionKey(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelayedShareFetchPartitionKey that = (DelayedShareFetchPartitionKey) o;
        return topic.equals(that.topic) && partition == that.partition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }

    @Override
    public String toString() {
        return "DelayedShareFetchPartitionKey(topic=" + topic +
            ", partition=" + partition + ")";
    }

    @Override
    public String keyLabel() {
        return String.format("topic=%s, partition=%s", topic, partition);
    }
}
