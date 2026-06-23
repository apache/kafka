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

import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MockAdminClientTest {

    @Test
    public void testDescribeStreamsGroupsReturnsRegisteredDescription() throws Exception {
        String groupId = "stream-group";
        StreamsGroupDescription description = newStreamsGroupDescription(groupId);

        try (MockAdminClient admin = new MockAdminClient()) {
            admin.addStreamsGroupDescription(description);

            StreamsGroupDescription result = admin
                .describeStreamsGroups(List.of(groupId))
                .all()
                .get()
                .get(groupId);

            assertEquals(description, result);
        }
    }

    @Test
    public void testDescribeStreamsGroupsReturnsTopologyDescriptionWhenRequested() throws Exception {
        String groupId = "stream-group";
        StreamsGroupDescription description = newStreamsGroupDescriptionWithTopology(groupId);

        try (MockAdminClient admin = new MockAdminClient()) {
            admin.addStreamsGroupDescription(description);

            StreamsGroupDescription result = admin
                .describeStreamsGroups(List.of(groupId), new DescribeStreamsGroupsOptions().includeTopologyDescription(true))
                .all()
                .get()
                .get(groupId);

            assertEquals(StreamsGroupTopologyDescriptionStatus.AVAILABLE, result.topologyDescriptionStatus());
            assertEquals(description.topologyDescription(), result.topologyDescription());
        }
    }

    @Test
    public void testDescribeStreamsGroupsOmitsTopologyDescriptionWhenNotRequested() throws Exception {
        String groupId = "stream-group";
        StreamsGroupDescription description = newStreamsGroupDescriptionWithTopology(groupId);

        try (MockAdminClient admin = new MockAdminClient()) {
            admin.addStreamsGroupDescription(description);

            StreamsGroupDescription result = admin
                .describeStreamsGroups(List.of(groupId))
                .all()
                .get()
                .get(groupId);

            assertEquals(StreamsGroupTopologyDescriptionStatus.NOT_REQUESTED, result.topologyDescriptionStatus());
            assertEquals(Optional.empty(), result.topologyDescription());
        }
    }

    @Test
    public void testDescribeStreamsGroupsUnknownGroupFails() {
        try (MockAdminClient admin = new MockAdminClient()) {
            ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> admin.describeStreamsGroups(List.of("missing-group")).all().get());
            assertInstanceOf(GroupIdNotFoundException.class, exception.getCause());
        }
    }

    private StreamsGroupDescription newStreamsGroupDescription(String groupId) {
        return new StreamsGroupDescription(
            groupId,
            0,
            0,
            0,
            List.of(),
            List.of(),
            GroupState.STABLE,
            new Node(0, "host", 0),
            Set.of(),
            Optional.empty(),
            StreamsGroupTopologyDescriptionStatus.NOT_REQUESTED);
    }

    private StreamsGroupDescription newStreamsGroupDescriptionWithTopology(String groupId) {
        StreamsGroupTopologyDescription topology = new StreamsGroupTopologyDescription(List.of(), List.of());
        return new StreamsGroupDescription(
            groupId,
            0,
            0,
            0,
            List.of(),
            List.of(),
            GroupState.STABLE,
            new Node(0, "host", 0),
            Set.of(),
            Optional.of(topology),
            StreamsGroupTopologyDescriptionStatus.AVAILABLE);
    }
}
