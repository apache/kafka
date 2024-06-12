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

import java.util.List;
import java.util.Objects;

/**
 * The state of a partition reassignment.  The current replicas and target replicas
 * may overlap.
 */
final class PartitionReassignmentState {
    public final List<Integer> currentReplicas;

    public final List<Integer> targetReplicas;

    public final boolean done;

    /**
     * @param currentReplicas The current replicas.
     * @param targetReplicas  The target replicas.
     * @param done            True if the reassignment is done.
     */
    public PartitionReassignmentState(List<Integer> currentReplicas, List<Integer> targetReplicas, boolean done) {
        this.currentReplicas = currentReplicas;
        this.targetReplicas = targetReplicas;
        this.done = done;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionReassignmentState state = (PartitionReassignmentState) o;
        return done == state.done && Objects.equals(currentReplicas, state.currentReplicas) && Objects.equals(targetReplicas, state.targetReplicas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentReplicas, targetReplicas, done);
    }
}
