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
package org.apache.kafka.common;

import java.io.Serializable;


/**
 * The topic name, partition number and the brokerId of the replica
 */
public final class TopicPartitionReplica implements Serializable {

    private int hash = 0;
    private final int brokerId;
    private final int partition;
    private final String topic;

    public TopicPartitionReplica(String topic, int partition, int brokerId) {
        this.topic = topic;
        this.partition = partition;
        this.brokerId = brokerId;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public int brokerId() {
        return brokerId;
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        final int prime = 31;
        int result = 1;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + partition;
        result = prime * result + brokerId;
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TopicPartitionReplica other = (TopicPartitionReplica) obj;
        if (partition != other.partition)
            return false;
        if (brokerId != other.brokerId)
            return false;
        if (topic == null) {
            if (other.topic != null) {
                return false;
            }
        } else if (!topic.equals(other.topic)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return String.format("%s-%d-%d", topic, partition, brokerId);
    }
}
