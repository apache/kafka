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
import org.apache.kafka.common.utils.Utils;

import java.util.List;

/**
 * A description of the assignments of a specific group member.
 */
public class MemberAssignment {
    private final List<TopicPartition> topicPartitions;

    /**
     * Creates an instance with the specified parameters.
     *
     * @param topicPartitions List of topic partitions
     */
    public MemberAssignment(List<TopicPartition> topicPartitions) {
        this.topicPartitions = topicPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemberAssignment that = (MemberAssignment) o;

        return topicPartitions != null ? topicPartitions.equals(that.topicPartitions) : that.topicPartitions == null;
    }

    @Override
    public int hashCode() {
        return topicPartitions != null ? topicPartitions.hashCode() : 0;
    }

    /**
     * The topic partitions assigned to a group member.
     */
    public List<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    @Override
    public String toString() {
        return "(topicPartitions=" + Utils.join(topicPartitions, ",") + ")";
    }
}
