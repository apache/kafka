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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Options for {@link Admin#listConsumerGroups()}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListConsumerGroupsOptions extends AbstractOptions<ListConsumerGroupsOptions> {

    private Set<GroupState> groupStates = Collections.emptySet();
    private Set<GroupType> types = Collections.emptySet();

    /**
     * If groupStates is set, only groups in these states will be returned by listGroups().
     * Otherwise, all groups are returned.
     * This operation is supported by brokers with version 2.6.0 or later.
     */
    public ListConsumerGroupsOptions inGroupStates(Set<GroupState> groupStates) {
        this.groupStates = (groupStates == null || groupStates.isEmpty()) ? Collections.emptySet() : Set.copyOf(groupStates);
        return this;
    }

    /**
     * If states is set, only groups in these states will be returned by listConsumerGroups().
     * Otherwise, all groups are returned.
     * This operation is supported by brokers with version 2.6.0 or later.
     * @deprecated Since 4.0. Use {@link #inGroupStates(Set)}.
     */
    @Deprecated
    public ListConsumerGroupsOptions inStates(Set<ConsumerGroupState> states) {
        this.groupStates = (states == null || states.isEmpty())
            ? Collections.emptySet()
            : states.stream().map(state -> GroupState.parse(state.name())).collect(Collectors.toSet());
        return this;
    }

    /**
     * If types is set, only groups of these types will be returned by listConsumerGroups().
     * Otherwise, all groups are returned.
     */
    public ListConsumerGroupsOptions withTypes(Set<GroupType> types) {
        this.types = (types == null || types.isEmpty()) ? Collections.emptySet() : new HashSet<>(types);
        return this;
    }

    /**
     * Returns the list of group states that are requested or empty if no states have been specified.
     */
    public Set<GroupState> groupStates() {
        return groupStates;
    }

    /**
     * Returns the list of States that are requested or empty if no states have been specified.
     * @deprecated Since 4.0. Use {@link #inGroupStates(Set)}.
     */
    @Deprecated
    public Set<ConsumerGroupState> states() {
        return groupStates.stream().map(groupState -> ConsumerGroupState.parse(groupState.name())).collect(Collectors.toSet());
    }

    /**
     * Returns the list of group types that are requested or empty if no types have been specified.
     */
    public Set<GroupType> types() {
        return types;
    }
}
