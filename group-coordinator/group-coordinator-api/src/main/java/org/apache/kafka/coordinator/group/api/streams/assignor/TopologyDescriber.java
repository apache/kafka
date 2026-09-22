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
package org.apache.kafka.coordinator.group.api.streams.assignor;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * The topology describer is used by the {@link TaskAssignor} to get topic and task metadata of the group's topology.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TopologyDescriber {

    /**
     * The IDs of all subtopologies in the group's topology.
     *
     * @return The list of subtopology IDs.
     */
    List<String> subtopologies();

    /**
     * The maximal number of input partitions among all source topics for the given subtopology.
     *
     * @param subtopologyId String identifying the subtopology.
     *
     * @throws NoSuchElementException if the subtopology ID does not exist.
     * @throws IllegalStateException if the subtopology contains no source topics.
     * @return The maximal number of input partitions among all source topics for the given subtopology.
     */
    int maxNumInputPartitions(String subtopologyId) throws NoSuchElementException;

    /**
     * Checks whether the given subtopology is associated with a changelog topic.
     *
     * @param subtopologyId String identifying the subtopology.
     * @throws NoSuchElementException if the subtopology ID does not exist.
     * @return true if the subtopology is associated with a changelog topic, false otherwise.
     */
    boolean isStateful(String subtopologyId);

}
