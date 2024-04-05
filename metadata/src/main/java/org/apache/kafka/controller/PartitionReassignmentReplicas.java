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

package org.apache.kafka.controller;

import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.metadata.placement.PartitionAssignment;

import java.util.ArrayList;
import java.util.Set;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;


class PartitionReassignmentReplicas {
    private final List<Integer> removing;
    private final List<Integer> adding;
    private final List<Integer> replicas;

    public PartitionReassignmentReplicas(
        List<Integer> removing,
        List<Integer> adding,
        List<Integer> replicas
    ) {
        this.removing = removing;
        this.adding = adding;
        this.replicas = replicas;
    }

    private static Set<Integer> calculateDifference(List<Integer> a, List<Integer> b) {
        Set<Integer> result = new TreeSet<>(a);
        result.removeAll(b);
        return result;
    }

    PartitionReassignmentReplicas(
        PartitionAssignment currentAssignment,
        PartitionAssignment targetAssignment
    ) {
        Set<Integer> removing = calculateDifference(currentAssignment.replicas(), targetAssignment.replicas());
        this.removing = new ArrayList<>(removing);
        Set<Integer> adding = calculateDifference(targetAssignment.replicas(), currentAssignment.replicas());
        this.adding = new ArrayList<>(adding);
        this.replicas = new ArrayList<>(targetAssignment.replicas());
        this.replicas.addAll(removing);
    }

    List<Integer> removing() {
        return removing;
    }

    List<Integer> adding() {
        return adding;
    }

    List<Integer> replicas() {
        return replicas;
    }

    boolean isReassignmentInProgress() {
        return isReassignmentInProgress(
            removing,
            adding);
    }

    static boolean isReassignmentInProgress(PartitionRegistration part) {
        return isReassignmentInProgress(
            Replicas.toList(part.removingReplicas),
            Replicas.toList(part.addingReplicas));
    }

    private static boolean isReassignmentInProgress(
        List<Integer> removingReplicas,
        List<Integer> addingReplicas
    ) {
        return removingReplicas.size() > 0
            || addingReplicas.size() > 0;
    }


    Optional<CompletedReassignment> maybeCompleteReassignment(List<Integer> targetIsr) {
        // Check if there is a reassignment to complete.
        if (!isReassignmentInProgress()) {
            return Optional.empty();
        }

        List<Integer> newTargetIsr = new ArrayList<>(targetIsr);
        List<Integer> newTargetReplicas = replicas;
        if (!removing.isEmpty()) {
            newTargetIsr = new ArrayList<>(targetIsr.size());
            for (int replica : targetIsr) {
                if (!removing.contains(replica)) {
                    newTargetIsr.add(replica);
                }
            }
            if (newTargetIsr.isEmpty()) return Optional.empty();

            newTargetReplicas = new ArrayList<>(replicas.size());
            for (int replica : replicas) {
                if (!removing.contains(replica)) {
                    newTargetReplicas.add(replica);
                }
            }
            if (newTargetReplicas.isEmpty()) return Optional.empty();
        }
        if (!newTargetIsr.containsAll(newTargetReplicas)) return Optional.empty();

        return Optional.of(
            new CompletedReassignment(
                newTargetReplicas,
                newTargetIsr
            )
        );
    }

    static class CompletedReassignment {
        final List<Integer> replicas;
        final List<Integer> isr;

        public CompletedReassignment(List<Integer> replicas, List<Integer> isr) {
            this.replicas = replicas;
            this.isr = isr;
        }
    }

    List<Integer> originalReplicas() {
        List<Integer> replicas = new ArrayList<>(this.replicas);
        replicas.removeAll(adding);
        return replicas;
    }

    @Override
    public int hashCode() {
        return Objects.hash(removing, adding, replicas);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PartitionReassignmentReplicas)) return false;
        PartitionReassignmentReplicas other = (PartitionReassignmentReplicas) o;
        return removing.equals(other.removing) &&
            adding.equals(other.adding) &&
            replicas.equals(other.replicas);
    }

    @Override
    public String toString() {
        return "PartitionReassignmentReplicas(" +
            "removing=" + removing + ", " +
            "adding=" + adding + ", " +
            "replicas=" + replicas + ")";
    }
}
