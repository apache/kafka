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

import java.util.Iterator;
import java.util.List;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.metadata.UsableBroker;


/**
 * The interface which a Kafka replica placement policy must implement.
 */
@InterfaceStability.Unstable
interface ReplicaPlacer {
    /**
     * Create a new replica placement.
     *
     * @param startPartition        The partition ID to start with.
     * @param numPartitions         The number of partitions to create placements for.
     * @param numReplicas           The number of replicas to create for each partitions.
     *                              Must be positive.
     * @param iterator              An iterator that yields all the usable brokers.
     *
     * @return                      A list of replica lists.
     *
     * @throws InvalidReplicationFactorException    If too many replicas were requested.
     */
    List<List<Integer>> place(int startPartition,
                              int numPartitions,
                              short numReplicas,
                              Iterator<UsableBroker> iterator)
        throws InvalidReplicationFactorException;
}
