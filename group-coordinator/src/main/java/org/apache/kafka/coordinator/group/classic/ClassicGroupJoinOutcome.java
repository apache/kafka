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
package org.apache.kafka.coordinator.group.classic;

/**
 * The outcome of handling a classic JoinGroup request in the group coordinator shard.
 *
 * <p>Usually the join is handled in a single pass and the outcome is {@link #COMPLETED}.
 * Joining a group of another type may convert it to a classic group. If that group holds
 * state outside the coordinator that must be released first, the shard changes nothing and
 * returns an outcome naming the step the caller must run before calling the join again.
 * A group type that needs such a step adds a value here and a case in the caller, so the
 * classic join logic itself stays free of group-type special cases.
 */
public enum ClassicGroupJoinOutcome {
    /**
     * The join was handled, and its response future was or will be completed by the shard.
     */
    COMPLETED,

    /**
     * The group is an empty streams group whose topology may still be held by the
     * topology-description plugin (KIP-1331). Nothing was changed and the response future
     * is still open. The caller must delete the topology from the plugin and then run the
     * join again, so the group can be converted to a classic group.
     */
    DELETE_STREAMS_TOPOLOGY_FIRST
}
