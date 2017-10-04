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

package org.apache.kafka.server.policy;

import java.util.List;
import java.util.Map;

/**
 * Represents the state of a topic either before, or as a result of,
 * an administrative request affecting a topic.
 */
public interface TopicState {
    /**
     * The number of partitions of the topic.
     */
    int numPartitions();

    /**
     * The replication factor of the topic. More precisely, the number of assigned replicas for partition 0.
     * // TODO what about during reassignment
     */
    Short replicationFactor();

    /**
     * A map of the replica assignments of the topic, with partition ids as keys and
     * the assigned brokers as the corresponding values.
     * // TODO what about during reassignment
     */
    Map<Integer, List<Integer>> replicasAssignments();

    /**
     * The topic config.
     */
    Map<String,String> configs();

    /**
     * Returns whether the topic is marked for deletion.
     */
    boolean markedForDeletion();

    /**
     * Returns whether the topic is an internal topic.
     */
    boolean internal();

}
