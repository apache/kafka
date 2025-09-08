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
package org.apache.kafka.storage.internals.log;

import java.util.List;

/**
 * Represents the current assignment of replicas for a partition. This can be a simple assignment
 * or an ongoing reassignment.
 */
public interface AssignmentState {

    /**
     * An ordered sequence of all the broker ids that were assigned to this topic partition.
     * @return the list of broker ids
     */
    List<Integer> replicas();

    /**
     * The number of replicas in the assignment.
     * @return the replication factor
     */
    int replicationFactor();

    /**
     * Check whether a replica is being added to the assignment.
     * The simple assignment returns false permanently.
     *
     * @param brokerId the broker id to check
     * @return true if the broker is being added
     */
    boolean isAddingReplica(int brokerId);
}
