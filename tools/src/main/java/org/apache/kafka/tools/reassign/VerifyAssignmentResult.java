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

package org.apache.kafka.tools.reassign;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A result returned from verifyAssignment.
 */
public final class VerifyAssignmentResult {
    public final Map<TopicPartition, PartitionReassignmentState> partStates;
    public final boolean partsOngoing;
    public final Map<TopicPartitionReplica, LogDirMoveState> moveStates;
    public final boolean movesOngoing;

    public VerifyAssignmentResult(Map<TopicPartition, PartitionReassignmentState> partStates) {
        this(partStates, false, Collections.emptyMap(), false);
    }

    /**
     * @param partStates    A map from partitions to reassignment states.
     * @param partsOngoing  True if there are any ongoing partition reassignments.
     * @param moveStates    A map from log directories to movement states.
     * @param movesOngoing  True if there are any ongoing moves that we know about.
     */
    public VerifyAssignmentResult(
        Map<TopicPartition, PartitionReassignmentState> partStates,
        boolean partsOngoing,
        Map<TopicPartitionReplica, LogDirMoveState> moveStates,
        boolean movesOngoing
    ) {
        this.partStates = partStates;
        this.partsOngoing = partsOngoing;
        this.moveStates = moveStates;
        this.movesOngoing = movesOngoing;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VerifyAssignmentResult that = (VerifyAssignmentResult) o;
        return partsOngoing == that.partsOngoing && movesOngoing == that.movesOngoing && Objects.equals(partStates, that.partStates) && Objects.equals(moveStates, that.moveStates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partStates, partsOngoing, moveStates, movesOngoing);
    }
}
