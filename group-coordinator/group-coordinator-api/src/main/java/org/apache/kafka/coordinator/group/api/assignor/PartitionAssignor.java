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
package org.apache.kafka.coordinator.group.api.assignor;

/**
 * Server-side partition assignor used by the GroupCoordinator.
 */
public interface PartitionAssignor {
    /**
     * Unique name for this assignor.
     */
    String name();

    /**
     * Assigns partitions to group members based on the given assignment specification and topic metadata.
     *
     * @param groupSpec           The assignment spec which includes member metadata.
     * @param subscribedTopicDescriber The topic and partition metadata describer.
     * @return The new assignment for the group.
     */
    GroupAssignment assign(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException;
}
