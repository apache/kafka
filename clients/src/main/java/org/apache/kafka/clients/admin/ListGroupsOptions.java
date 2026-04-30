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

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;

import java.util.Set;

/**
 * Options for {@link Admin#listGroups()}.
 */
public class ListGroupsOptions extends AbstractOptions<ListGroupsOptions> {

    private Set<GroupState> groupStates = Set.of();
    private Set<GroupType> types = Set.of();
    private Set<String> protocolTypes = Set.of();

    /**
     * Only consumer groups will be returned by listGroups().
     * This operation sets filters on group type and protocol type which select consumer groups.
     */
    public static ListGroupsOptions forConsumerGroups() {
        return new ListGroupsOptions()
            .withTypes(Set.of(GroupType.CLASSIC, GroupType.CONSUMER))
            .withProtocolTypes(Set.of("", ConsumerProtocol.PROTOCOL_TYPE));
    }

    /**
     * Only share groups will be returned by listGroups().
     * This operation sets a filter on group type which select share groups.
     */
    public static ListGroupsOptions forShareGroups() {
        return new ListGroupsOptions()
            .withTypes(Set.of(GroupType.SHARE));
    }

    /**
     * Only streams groups will be returned by listGroups().
     * This operation sets a filter on group type which select streams groups.
     */
    public static ListGroupsOptions forStreamsGroups() {
        return new ListGroupsOptions()
            .withTypes(Set.of(GroupType.STREAMS));
    }

    /**
     * If groupStates is set, only groups in these states will be returned by listGroups().
     * Otherwise, all groups are returned.
     * This operation is supported by brokers with version 2.6.0 or later.
     */
    public ListGroupsOptions inGroupStates(Set<GroupState> groupStates) {
        this.groupStates = (groupStates == null || groupStates.isEmpty()) ? Set.of() : Set.copyOf(groupStates);
        return this;
    }

    /**
     * If protocol types is set, only groups of these protocol types will be returned by listGroups().
     * Otherwise, all groups are returned.
     */
    public ListGroupsOptions withProtocolTypes(Set<String> protocolTypes) {
        this.protocolTypes = (protocolTypes == null || protocolTypes.isEmpty()) ? Set.of() : Set.copyOf(protocolTypes);
        return this;
    }

    /**
     * If types is set, only groups of these types will be returned by listGroups().
     * Otherwise, all groups are returned.
     */
    public ListGroupsOptions withTypes(Set<GroupType> types) {
        this.types = (types == null || types.isEmpty()) ? Set.of() : Set.copyOf(types);
        return this;
    }

    /**
     * Returns the list of group states that are requested or empty if no states have been specified.
     */
    public Set<GroupState> groupStates() {
        return groupStates;
    }

    /**
     * Returns the list of protocol types that are requested or empty if no protocol types have been specified.
     */
    public Set<String> protocolTypes() {
        return protocolTypes;
    }

    /**
     * Returns the list of group types that are requested or empty if no types have been specified.
     */
    public Set<GroupType> types() {
        return types;
    }
}
