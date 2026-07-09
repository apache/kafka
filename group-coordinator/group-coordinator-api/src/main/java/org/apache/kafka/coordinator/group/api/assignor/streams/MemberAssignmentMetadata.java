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
package org.apache.kafka.coordinator.group.api.assignor.streams;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;
import java.util.Optional;

/**
 * Interface representing the static metadata for a streams group member.
 *
 * <p>The metadata contains the per-member information that does not change during the assignment
 * computation. The member's current task assignment state is exposed separately through
 * {@link MemberAssignmentState}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface MemberAssignmentMetadata {

    /**
     * @return The instance ID if provided.
     */
    Optional<String> instanceId();

    /**
     * @return The rack ID if provided.
     */
    Optional<String> rackId();

    /**
     * @return The process ID.
     */
    String processId();

    /**
     * @return The client tags for a rack-aware assignment.
     */
    Map<String, String> clientTags();

}
