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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
        this(currentAssignment, List.of(), targetAssignment);
    }

    /**
     * Compute the reassignment that moves a partition from its current assignment to the
     * target assignment.
     *
     * If a reassignment is already in progress, {@code currentAdding} holds the replicas it
     * is still adding. Those replicas are part of the current replica list, but they have not
     * necessarily joined the ISR yet. If the target keeps such a replica, it must remain an
     * adding replica so that the new reassignment still waits for it to catch up. Otherwise
     * the replica would silently become a regular replica, and the partition could complete
     * the reassignment with an out-of-sync replica in its final replica list. If the target
     * drops such a replica, it is treated like any other current replica that is not in the
     * target, and is removed once the reassignment completes.
     *
     * @param currentAssignment The current assignment, including any replicas that an
     *                          in-progress reassignment is adding.
     * @param currentAdding     The replicas that an in-progress reassignment is still adding,
     *                          or an empty list if no reassignment is in progress.
     * @param targetAssignment  The requested target assignment.
     */
    PartitionReassignmentReplicas(
        PartitionAssignment currentAssignment,
        List<Integer> currentAdding,
        PartitionAssignment targetAssignment
    ) {
        Set<Integer> removing = calculateDifference(currentAssignment.replicas(), targetAssignment.replicas());
        this.removing = new ArrayList<>(removing);
        List<Integer> originalReplicas = new ArrayList<>(currentAssignment.replicas());
        originalReplicas.removeAll(currentAdding);
        Set<Integer> adding = calculateDifference(targetAssignment.replicas(), originalReplicas);
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
        return !removingReplicas.isEmpty() || !addingReplicas.isEmpty();
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

        // Wait for all adding sync replicas to join the ISR, as new brokers could be unhealthy.
        if (!newTargetIsr.containsAll(adding)) return Optional.empty();

        // If the replication factor is being reduced, wait for all target sync replicas to be present in newTargetIsr to avoid
        // the potential negative impact on ISR. For example: We reassign [0,1,2,3,4] -> [2,3,4,5] when only 0, 1 and 5 are in
        // ISR. If we complete the reassignment, we end up with ISR of [5]. If we stay with the current assignment, ISR is
        // [0,1]. So, by completing the reassignment in this case, we are reducing the size of ISR unnecessarily.
        if (adding.size() < removing.size() && !newTargetIsr.containsAll(newTargetReplicas)) return Optional.empty();

        return Optional.of(
            new CompletedReassignment(
                newTargetReplicas,
                newTargetIsr
            )
        );
    }

    record CompletedReassignment(List<Integer> replicas, List<Integer> isr) {
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
        if (!(o instanceof PartitionReassignmentReplicas other)) return false;
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
