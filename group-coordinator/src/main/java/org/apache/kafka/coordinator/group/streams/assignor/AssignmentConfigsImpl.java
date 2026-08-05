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
package org.apache.kafka.coordinator.group.streams.assignor;

import org.apache.kafka.coordinator.group.api.streams.assignor.AssignmentConfigs;

import java.util.List;
import java.util.Objects;

/**
 * The assignment configurations for a streams group.
 *
 * @param numStandbyReplicas      The number of standby replicas for each task.
 * @param rackAwareAssignmentTags The client tags used to distribute standby tasks across racks.
 */
public record AssignmentConfigsImpl(
    int numStandbyReplicas,
    List<String> rackAwareAssignmentTags
) implements AssignmentConfigs {

    public AssignmentConfigsImpl {
        // The list is exposed to a custom assignor through the public AssignmentConfigs interface.
        rackAwareAssignmentTags = List.copyOf(Objects.requireNonNull(rackAwareAssignmentTags));
    }
}
