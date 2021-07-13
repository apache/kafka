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

import java.util.ArrayList;
import java.util.Set;
import java.util.List;
import java.util.Objects;


class PartitionReassignmentRevert {
    private final List<Integer> replicas;
    private final List<Integer> isr;

    PartitionReassignmentRevert(PartitionRegistration registration) {
        // Figure out the replica list and ISR that we will have after reverting the
        // reassignment. In general, we want to take out any replica that the reassignment
        // was adding, but keep the ones the reassignment was removing. (But see the
        // special case below.)
        Set<Integer> adding = Replicas.toSet(registration.addingReplicas);
        this.replicas = new ArrayList<>(registration.replicas.length);
        this.isr = new ArrayList<>(registration.isr.length);
        for (int i = 0; i < registration.isr.length; i++) {
            int replica = registration.isr[i];
            if (!adding.contains(replica)) {
                this.isr.add(replica);
            } else if (i == registration.isr.length - 1 && isr.isEmpty()) {
                // This is a special case where taking out all the "adding" replicas is
                // not possible. The reason it is not possible is that doing so would
                // create an empty ISR, which is not allowed.
                //
                // In this case, we leave in one of the adding replicas permanently.
                this.isr.add(replica);
                this.replicas.add(replica);
            }
        }
        for (int replica : registration.replicas) {
            if (!adding.contains(replica)) {
                this.replicas.add(replica);
            }
        }
    }

    List<Integer> replicas() {
        return replicas;
    }

    List<Integer> isr() {
        return isr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicas, isr);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PartitionReassignmentRevert)) return false;
        PartitionReassignmentRevert other = (PartitionReassignmentRevert) o;
        return replicas.equals(other.replicas) &&
            isr.equals(other.isr);
    }

    @Override
    public String toString() {
        return "PartitionReassignmentRevert(" +
            "replicas=" + replicas + ", " +
            "isr=" + isr + ")";
    }
}
