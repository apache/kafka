/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

/**
 * Generic interface to perform partition assignment when using Kafka's automatic group management
 * protocol to divide topic consumption between members in a group.
 */
public interface PartitionAssignor {

    /**
     * Perform the assignment for the consumer.
     * @param subscriptions Map of all group members to their respective topic subscriptions
     * @param cluster Current metadata to use for partition counts
     * @return A map containing the respective assignment for each group member
     */
    Map<String, List<TopicPartition>> assign(Map<String, List<String>> subscriptions,
                                             Cluster cluster);

    /**
     * The name of the partition assignor (e.g. 'roundrobin')
     * @return The name (must not be null)
     */
    String name();


}
