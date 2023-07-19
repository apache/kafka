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

import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;

import java.util.List;
import java.util.Objects;


class PartitionReassignmentRevert {
    private final List<Integer> replicas;
    private final List<Integer> isr;
    private final boolean unclean;

    PartitionReassignmentRevert(PartitionRegistration registration) {
        // Figure out the replica list and ISR that we will have after reverting the
        // reassignment. In general, we want to take out any replica that the reassignment
        // was adding, but keep the ones the reassignment was removing. (But see the
        // special case below.)

        PartitionReassignmentReplicas ongoingReassignment =
            new PartitionReassignmentReplicas(
                Replicas.toList(registration.removingReplicas),
                Replicas.toList(registration.addingReplicas),
                Replicas.toList(registration.replicas)
            );

        this.replicas = ongoingReassignment.originalReplicas();
        this.isr = Replicas.toList(registration.isr);
        this.isr.removeAll(ongoingReassignment.adding());

        if (isr.isEmpty()) {
            // In the special case that all the replicas that are in the ISR are also
            // contained in addingReplicas, we choose the first remaining replica and add
            // it to the ISR. This is considered an unclean leader election. Therefore,
            // calling code must check that unclean leader election is enabled before
            // accepting the new ISR.
            if (this.replicas.isEmpty()) {
                // This should not be reachable, since it would require a partition
                // starting with an empty replica set prior to the reassignment we are
                // trying to revert.
                throw new InvalidReplicaAssignmentException("Invalid replica " +
                    "assignment: addingReplicas contains all replicas.");
            }
            isr.add(replicas.get(0));
            this.unclean = true;
        } else {
            this.unclean = false;
        }
    }

    List<Integer> replicas() {
        return replicas;
    }

    List<Integer> isr() {
        return isr;
    }

    boolean unclean() {
        return unclean;
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicas, isr, unclean);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PartitionReassignmentRevert)) return false;
        PartitionReassignmentRevert other = (PartitionReassignmentRevert) o;
        return replicas.equals(other.replicas) &&
            isr.equals(other.isr) &&
            unclean == other.unclean;
    }

    @Override
    public String toString() {
        return "PartitionReassignmentRevert(" +
            "replicas=" + replicas + ", " +
            "isr=" + isr + ", " +
            "unclean=" + unclean + ")";
    }
}
