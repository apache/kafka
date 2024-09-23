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
package org.apache.kafka.streams.processor.assignment;


import org.apache.kafka.streams.processor.TaskId;

import java.util.Set;

/**
 * A simple container class corresponding to a given {@link TaskId}.
 * Includes metadata such as whether it's stateful and the names of all state stores
 * belonging to this task, the set of input topic partitions and changelog topic partitions
 * for all logged state stores, and the rack ids of all replicas of each topic partition
 * in the task.
 */
public interface TaskInfo {

    /**
     *
     * @return The {@code TaskId} of the underlying task.
     */
    TaskId id();

    /**
     *
     * @return true if the underlying task is stateful, and false otherwise.
     */
    boolean isStateful();

    /**
     *
     * @return the set of state store names that this task makes use of. In the case of stateless tasks,
     *         this set will be empty as no state stores are used.
     */
    Set<String> stateStoreNames();

    /**
     *
     * @return the set of topic partitions in use for this task.
     */
    Set<TaskTopicPartition> topicPartitions();
}
