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

import org.apache.kafka.common.Uuid;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The partition assignment for a consumer group member.
 */
public class MemberAssignment {
    /**
     * The target partitions assigned to this member keyed by topicId.
     */
    private final Map<Uuid, Set<Integer>> targetPartitions;

    public MemberAssignment(Map<Uuid, Set<Integer>> targetPartitions) {
        Objects.requireNonNull(targetPartitions);
        this.targetPartitions = targetPartitions;
    }

    /**
     * @return Target partitions keyed by topic Ids.
     */
    public Map<Uuid, Set<Integer>> targetPartitions() {
        return this.targetPartitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberAssignment that = (MemberAssignment) o;
        return targetPartitions.equals(that.targetPartitions);
    }

    @Override
    public int hashCode() {
        return targetPartitions.hashCode();
    }

    @Override
    public String toString() {
        return "MemberAssignment(targetPartitions=" + targetPartitions + ')';
    }
}
