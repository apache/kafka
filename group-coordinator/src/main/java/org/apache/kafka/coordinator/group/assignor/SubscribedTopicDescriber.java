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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Set;

/**
 * The subscribed topic describer is used by the {@link PartitionAssignor}
 * to obtain topic and partition metadata of the subscribed topics.
 *
 * The interface is kept in an internal module until KIP-848 is fully
 * implemented and ready to be released.
 */
@InterfaceStability.Unstable
public interface SubscribedTopicDescriber {
    /**
     * The number of partitions for the given topic Id.
     *
     * @param topicId   Uuid corresponding to the topic.
     * @return The number of partitions corresponding to the given topic Id,
     *         or -1 if the topic Id does not exist.
     */
    int numPartitions(Uuid topicId);

    /**
     * Returns all the available racks associated with the replicas of the given partition.
     *
     * @param topicId       Uuid corresponding to the partition's topic.
     * @param partition     Partition Id within topic.
     * @return The set of racks corresponding to the replicas of the topic's partition.
     *         If the topic Id does not exist, an empty set is returned.
     */
    Set<String> racksForPartition(Uuid topicId, int partition);
}
