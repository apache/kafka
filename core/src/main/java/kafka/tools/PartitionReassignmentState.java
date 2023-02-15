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

package kafka.tools;

import java.util.List;
import java.util.Objects;

/**
 * The state of a partition reassignment.  The current replicas and target replicas
 * may overlap.
 */
public final class PartitionReassignmentState {
    private final List<Integer> currentReplicas;

    private final List<Integer> targetReplicas;

    private final boolean done;

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

    public List<Integer> currentReplicas() {
        return currentReplicas;
    }

    public List<Integer> targetReplicas() {
        return targetReplicas;
    }

    public boolean done() {
        return done;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionReassignmentState that = (PartitionReassignmentState) o;
        return done == that.done && Objects.equals(currentReplicas, that.currentReplicas) && Objects.equals(targetReplicas, that.targetReplicas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentReplicas, targetReplicas, done);
    }

    @Override
    public String toString() {
        return "PartitionReassignmentState{" +
            "currentReplicas=" + currentReplicas +
            ", targetReplicas=" + targetReplicas +
            ", done=" + done +
            '}';
    }
}
