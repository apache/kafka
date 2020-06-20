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

package org.apache.kafka.clients.admin;

import java.util.Collections;
import java.util.List;

/**
 * A partition reassignment, which has been listed via {@link AdminClient#listPartitionReassignments()}.
 */
public class PartitionReassignment {

    private final List<Integer> replicas;
    private final List<Integer> addingReplicas;
    private final List<Integer> removingReplicas;

    public PartitionReassignment(List<Integer> replicas, List<Integer> addingReplicas, List<Integer> removingReplicas) {
        this.replicas = Collections.unmodifiableList(replicas);
        this.addingReplicas = Collections.unmodifiableList(addingReplicas);
        this.removingReplicas = Collections.unmodifiableList(removingReplicas);
    }

    /**
     * The brokers which this partition currently resides on.
     */
    public List<Integer> replicas() {
        return replicas;
    }

    /**
     * The brokers that we are adding this partition to as part of a reassignment.
     * A subset of replicas.
     */
    public List<Integer> addingReplicas() {
        return addingReplicas;
    }

    /**
     * The brokers that we are removing this partition from as part of a reassignment.
     * A subset of replicas.
     */
    public List<Integer> removingReplicas() {
        return removingReplicas;
    }

    @Override
    public String toString() {
        return "PartitionReassignment(" +
                "replicas=" + replicas +
                ", addingReplicas=" + addingReplicas +
                ", removingReplicas=" + removingReplicas +
                ')';
    }
}
