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

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ListGroupsOptionsTest {
    @Test
    public void testForConsumerGroups() {
        ListGroupsOptions options = ListGroupsOptions.forConsumerGroups();
        assertTrue(options.groupStates().isEmpty());
        assertEquals(Set.of(GroupType.CONSUMER, GroupType.CLASSIC), options.types());
        assertEquals(Set.of("", ConsumerProtocol.PROTOCOL_TYPE), options.protocolTypes());

        options.inGroupStates(Set.of(GroupState.STABLE));
        options.withTypes(Set.of(GroupType.CONSUMER));
        options.withProtocolTypes(Set.of(ConsumerProtocol.PROTOCOL_TYPE));
        assertEquals(Set.of(GroupState.STABLE), options.groupStates());
        assertEquals(Set.of(GroupType.CONSUMER), options.types());
        assertEquals(Set.of(ConsumerProtocol.PROTOCOL_TYPE), options.protocolTypes());
    }

    @Test
    public void testForShareGroups() {
        ListGroupsOptions options = ListGroupsOptions.forShareGroups();
        assertTrue(options.groupStates().isEmpty());
        assertEquals(Set.of(GroupType.SHARE), options.types());
        assertTrue(options.protocolTypes().isEmpty());

        options.inGroupStates(Set.of(GroupState.STABLE));
        options.withTypes(Set.of(GroupType.CONSUMER));
        options.withProtocolTypes(Set.of(ConsumerProtocol.PROTOCOL_TYPE));
        assertEquals(Set.of(GroupState.STABLE), options.groupStates());
        assertEquals(Set.of(GroupType.CONSUMER), options.types());
        assertEquals(Set.of(ConsumerProtocol.PROTOCOL_TYPE), options.protocolTypes());
    }

    @Test
    public void testForStreamsGroups() {
        ListGroupsOptions options = ListGroupsOptions.forStreamsGroups();
        assertTrue(options.groupStates().isEmpty());
        assertEquals(Set.of(GroupType.STREAMS), options.types());
        assertTrue(options.protocolTypes().isEmpty());

        options.inGroupStates(Set.of(GroupState.STABLE));
        options.withTypes(Set.of(GroupType.CONSUMER));
        options.withProtocolTypes(Set.of(ConsumerProtocol.PROTOCOL_TYPE));
        assertEquals(Set.of(GroupState.STABLE), options.groupStates());
        assertEquals(Set.of(GroupType.CONSUMER), options.types());
        assertEquals(Set.of(ConsumerProtocol.PROTOCOL_TYPE), options.protocolTypes());
    }

    @Test
    public void testGroupStates() {
        ListGroupsOptions options = new ListGroupsOptions();
        assertTrue(options.groupStates().isEmpty());

        options.inGroupStates(Set.of(GroupState.DEAD));
        assertEquals(Set.of(GroupState.DEAD), options.groupStates());

        Set<GroupState> groupStates = Set.of(GroupState.values());
        options = new ListGroupsOptions().inGroupStates(groupStates);
        assertEquals(groupStates, options.groupStates());
    }

    @Test
    public void testConsumerGroupStates() {
        ListGroupsOptions options = new ListGroupsOptions();
        assertTrue(options.groupStates().isEmpty());

        Set<GroupState> groupStates = GroupState.groupStatesForType(GroupType.CONSUMER);
        options = new ListGroupsOptions().inGroupStates(groupStates);
        assertEquals(groupStates, options.groupStates());
    }

    @Test
    public void testProtocolTypes() {
        ListGroupsOptions options = new ListGroupsOptions();
        assertTrue(options.protocolTypes().isEmpty());

        options.withProtocolTypes(Set.of(ConsumerProtocol.PROTOCOL_TYPE));
        assertEquals(Set.of(ConsumerProtocol.PROTOCOL_TYPE), options.protocolTypes());

        Set<String> protocolTypes = Set.of("", "consumer", "share");
        options = new ListGroupsOptions().withProtocolTypes(protocolTypes);
        assertEquals(protocolTypes, options.protocolTypes());
    }

    @Test
    public void testTypes() {
        ListGroupsOptions options = new ListGroupsOptions();
        assertTrue(options.types().isEmpty());

        options.withTypes(Set.of(GroupType.CLASSIC));
        assertEquals(Set.of(GroupType.CLASSIC), options.types());

        Set<GroupType> groupTypes = Set.of(GroupType.values());
        options = new ListGroupsOptions().withTypes(groupTypes);
        assertEquals(groupTypes, options.types());
    }
}