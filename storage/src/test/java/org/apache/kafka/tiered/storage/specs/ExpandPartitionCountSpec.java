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
package org.apache.kafka.tiered.storage.specs;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ExpandPartitionCountSpec {

    private final String topicName;
    private final int partitionCount;
    private final Map<Integer, List<Integer>> assignment;

    public ExpandPartitionCountSpec(String topicName,
                                    int partitionCount) {
        this(topicName, partitionCount, null);
    }

    public ExpandPartitionCountSpec(String topicName,
                                    int partitionCount,
                                    Map<Integer, List<Integer>> assignment) {
        this.topicName = topicName;
        this.partitionCount = partitionCount;
        this.assignment = assignment;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public Map<Integer, List<Integer>> getAssignment() {
        return assignment;
    }

    @Override
    public String toString() {
        return String.format("ExpandPartitionCountSpec[topicName=%s, partitionCount=%d, assignment=%s]",
                topicName, partitionCount, assignment);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExpandPartitionCountSpec that = (ExpandPartitionCountSpec) o;
        return partitionCount == that.partitionCount
                && Objects.equals(topicName, that.topicName)
                && Objects.equals(assignment, that.assignment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName, partitionCount, assignment);
    }
}
