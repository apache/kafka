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
package org.apache.kafka.coordinator.group.assignor;

import java.util.Objects;

/**
 * Metadata of a topic.
 */
public class AssignmentTopicMetadata {
    /**
     * The topic name.
     */
    final String topicName;

    /**
     * The number of partitions.
     */
    final int numPartitions;

    public AssignmentTopicMetadata(
        String topicName,
        int numPartitions
    ) {
        Objects.requireNonNull(topicName);
        this.topicName = topicName;
        this.numPartitions = numPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AssignmentTopicMetadata that = (AssignmentTopicMetadata) o;

        if (numPartitions != that.numPartitions) return false;
        return topicName.equals(that.topicName);
    }

    @Override
    public int hashCode() {
        int result = topicName.hashCode();
        result = 31 * result + numPartitions;
        return result;
    }

    @Override
    public String toString() {
        return "AssignmentTopicMetadata(topicName=" + topicName +
            ", numPartitions=" + numPartitions +
            ')';
    }
}
