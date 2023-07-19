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

/**
 * Metadata of a topic.
 */
public class AssignmentTopicMetadata {

    /**
     * The number of partitions.
     */
    private final int numPartitions;

    public AssignmentTopicMetadata(
        int numPartitions
    ) {
        this.numPartitions = numPartitions;
    }

    /**
     * @return The number of partitions present for the topic.
     */
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AssignmentTopicMetadata that = (AssignmentTopicMetadata) o;
        return numPartitions == that.numPartitions;
    }

    @Override
    public int hashCode() {
        return numPartitions;
    }

    @Override
    public String toString() {
        return "AssignmentTopicMetadata(numPartitions=" + numPartitions + ')';
    }
}
