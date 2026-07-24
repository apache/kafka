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

import java.util.Collection;
import java.util.Map;

/**
 * The group metadata specifications required to compute the target assignment.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface GroupSpec {

    /**
     * @return The member Ids of all members in the group.
     */
    Collection<String> memberIds();

    /**
     * Gets the static metadata for a given member.
     *
     * @param memberId The member Id.
     * @return The static member metadata.
     */
    MemberAssignmentMetadata memberMetadata(String memberId);

    /**
     * Gets the current assignment state for a given member.
     *
     * @param memberId The member Id.
     * @return The current member assignment state.
     */
    MemberAssignmentState memberAssignmentState(String memberId);

    /**
     * @return Any configurations passed to the assignor.
     */
    Map<String, String> configs();

}
