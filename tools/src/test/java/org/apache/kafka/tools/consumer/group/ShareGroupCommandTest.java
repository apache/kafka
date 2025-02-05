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
package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.DescribeShareGroupsOptions;
import org.apache.kafka.clients.admin.DescribeShareGroupsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberAssignment;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;
import org.apache.kafka.tools.consumer.group.ShareGroupCommand.ShareGroupService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import joptsimple.OptionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShareGroupCommandTest {
    private static final List<List<String>> DESCRIBE_TYPE_OFFSETS = List.of(List.of(""), List.of("--offsets"));
    private static final List<List<String>> DESCRIBE_TYPE_MEMBERS = List.of(List.of("--members"), List.of("--members", "--verbose"));
    private static final List<List<String>> DESCRIBE_TYPE_STATE = List.of(List.of("--state"), List.of("--state", "--verbose"));
    private static final List<List<String>> DESCRIBE_TYPES = Stream.of(DESCRIBE_TYPE_OFFSETS, DESCRIBE_TYPE_MEMBERS, DESCRIBE_TYPE_STATE).flatMap(Collection::stream).toList();

    @Test
    public void testListShareGroups() throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--list"};
        Admin adminClient = mock(KafkaAdminClient.class);
        ListGroupsResult result = mock(ListGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Arrays.asList(
                new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
                new GroupListing(secondGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.EMPTY))
        )));

        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(result);
        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            Set<String> expectedGroups = new HashSet<>(Arrays.asList(firstGroup, secondGroup));

            final Set[] foundGroups = new Set[]{Set.of()};
            TestUtils.waitForCondition(() -> {
                foundGroups[0] = new HashSet<>(service.listShareGroups());
                return Objects.equals(expectedGroups, foundGroups[0]);
            }, "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups[0] + ".");
        }
    }

    @Test
    public void testListShareGroupsWithStates() throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--list", "--state"};
        Admin adminClient = mock(KafkaAdminClient.class);
        ListGroupsResult resultWithAllStates = mock(ListGroupsResult.class);
        when(resultWithAllStates.all()).thenReturn(KafkaFuture.completedFuture(Arrays.asList(
            new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
            new GroupListing(secondGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.EMPTY))
        )));
        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(resultWithAllStates);
        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            Set<GroupListing> expectedListing = new HashSet<>(Arrays.asList(
                new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
                new GroupListing(secondGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.EMPTY))));

            final Set[] foundListing = new Set[]{Set.of()};
            TestUtils.waitForCondition(() -> {
                foundListing[0] = new HashSet<>(service.listShareGroupsInStates(new HashSet<>(Arrays.asList(GroupState.values()))));
                return Objects.equals(expectedListing, foundListing[0]);
            }, "Expected to show groups " + expectedListing + ", but found " + foundListing[0]);

            ListGroupsResult resultWithStableState = mock(ListGroupsResult.class);
            when(resultWithStableState.all()).thenReturn(KafkaFuture.completedFuture(List.of(
                new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE))
            )));
            when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(resultWithStableState);
            Set<GroupListing> expectedListingStable = Set.of(
                new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)));

            foundListing[0] = Set.of();

            TestUtils.waitForCondition(() -> {
                foundListing[0] = new HashSet<>(service.listShareGroupsInStates(Set.of(GroupState.STABLE)));
                return Objects.equals(expectedListingStable, foundListing[0]);
            }, "Expected to show groups " + expectedListingStable + ", but found " + foundListing[0]);
        }
    }

    @Test
    public void testDescribeOffsetsOfExistingGroup() throws Exception {
        String firstGroup = "group1";
        String bootstrapServer = "localhost:9092";

        for (List<String> describeType : DESCRIBE_TYPE_OFFSETS) {
            List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", bootstrapServer, "--describe", "--group", firstGroup));
            cgcArgs.addAll(describeType);
            Admin adminClient = mock(KafkaAdminClient.class);
            DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
            ShareGroupDescription exp = new ShareGroupDescription(
                firstGroup,
                List.of(new ShareMemberDescription("memid1", "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);
            ListShareGroupOffsetsResult listShareGroupOffsetsResult = AdminClientTestUtils.createListShareGroupOffsetsResult(
                Map.of(
                    firstGroup,
                    KafkaFuture.completedFuture(Map.of(new TopicPartition("topic1", 0), 0L))
                )
            );

            when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(firstGroup, KafkaFuture.completedFuture(exp)));
            when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
            when(adminClient.listShareGroupOffsets(ArgumentMatchers.anyMap())).thenReturn(listShareGroupOffsetsResult);
            try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
                TestUtils.waitForCondition(() -> {
                    Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                    String[] lines = res.getKey().trim().split("\n");
                    if (lines.length != 2 && !res.getValue().isEmpty()) {
                        return false;
                    }

                    List<String> expectedValues = List.of(firstGroup, "topic1", "0", "0");
                    return checkArgsHeaderOutput(cgcArgs, lines[0]) &&
                        Arrays.stream(lines[1].trim().split("\\s+")).toList().equals(expectedValues);
                }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
            }
        }
    }

    @Test
    public void testDescribeOffsetsOfAllExistingGroups() throws Exception {
        String firstGroup = "group1";
        String secondGroup = "group2";
        String bootstrapServer = "localhost:9092";

        for (List<String> describeType : DESCRIBE_TYPE_OFFSETS) {
            List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", bootstrapServer, "--describe", "--all-groups"));
            cgcArgs.addAll(describeType);
            Admin adminClient = mock(KafkaAdminClient.class);
            ListGroupsResult listGroupsResult = mock(ListGroupsResult.class);
            GroupListing firstGroupListing = new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE));
            GroupListing secondGroupListing = new GroupListing(secondGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE));
            DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
            ShareGroupDescription exp1 = new ShareGroupDescription(
                firstGroup,
                List.of(new ShareMemberDescription("memid1", "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);
            ShareGroupDescription exp2 = new ShareGroupDescription(
                secondGroup,
                List.of(new ShareMemberDescription("memid1", "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);
            ListShareGroupOffsetsResult listShareGroupOffsetsResult1 = AdminClientTestUtils.createListShareGroupOffsetsResult(
                Map.of(
                    firstGroup,
                    KafkaFuture.completedFuture(Map.of(new TopicPartition("topic1", 0), 0L))
                )
            );
            ListShareGroupOffsetsResult listShareGroupOffsetsResult2 = AdminClientTestUtils.createListShareGroupOffsetsResult(
                Map.of(
                    secondGroup,
                    KafkaFuture.completedFuture(Map.of(new TopicPartition("topic1", 0), 0L))
                )
            );

            when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(firstGroupListing, secondGroupListing)));
            when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(listGroupsResult);
            when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(firstGroup, KafkaFuture.completedFuture(exp1), secondGroup, KafkaFuture.completedFuture(exp2)));
            when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
            when(adminClient.listShareGroupOffsets(ArgumentMatchers.anyMap())).thenAnswer(
                invocation -> {
                    Map<String, Object> argument = invocation.getArgument(0);
                    if (argument.containsKey(firstGroup)) {
                        return listShareGroupOffsetsResult1;
                    } else if (argument.containsKey(secondGroup)) {
                        return listShareGroupOffsetsResult2;
                    }
                    return null;
                });
            try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
                TestUtils.waitForCondition(() -> {
                    Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                    String[] lines = res.getKey().trim().split("\n");
                    if (lines.length != 2 && !res.getValue().isEmpty()) {
                        return false;
                    }

                    List<String> expectedValues1 = List.of(firstGroup, "topic1", "0", "0");
                    List<String> expectedValues2 = List.of(secondGroup, "topic1", "0", "0");
                    return checkArgsHeaderOutput(cgcArgs, lines[0]) && checkArgsHeaderOutput(cgcArgs, lines[3]) &&
                        Arrays.stream(lines[1].trim().split("\\s+")).toList().equals(expectedValues1) &&
                        Arrays.stream(lines[4].trim().split("\\s+")).toList().equals(expectedValues2);
                }, "Expected 2 data rows and no error in describe results with describe type " + String.join(" ", describeType) + ".");
            }
        }
    }

    @Test
    public void testDescribeStateOfExistingGroup() throws Exception {
        String firstGroup = "group1";
        String bootstrapServer = "localhost:9092";

        for (List<String> describeType : DESCRIBE_TYPE_STATE) {
            List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", bootstrapServer, "--describe", "--group", firstGroup));
            cgcArgs.addAll(describeType);
            Admin adminClient = mock(KafkaAdminClient.class);
            DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
            ShareGroupDescription exp1 = new ShareGroupDescription(
                firstGroup,
                List.of(new ShareMemberDescription("memid1", "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);

            when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(firstGroup, KafkaFuture.completedFuture(exp1)));
            when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
            try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
                TestUtils.waitForCondition(() -> {
                    Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                    String[] lines = res.getKey().trim().split("\n");
                    if (lines.length != 2 && !res.getValue().isEmpty()) {
                        return false;
                    }

                    List<String> expectedValues1;
                    if (describeType.contains("--verbose")) {
                        expectedValues1 = List.of(firstGroup, "host1:9090", "(0)", "Stable", "0", "0", "1");

                    } else {
                        expectedValues1 = List.of(firstGroup, "host1:9090", "(0)", "Stable", "1");
                    }
                    return checkArgsHeaderOutput(cgcArgs, lines[0]) &&
                        Arrays.stream(lines[1].trim().split("\\s+")).toList().equals(expectedValues1);
                }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
            }
        }
    }

    @Test
    public void testDescribeStatesOfAllExistingGroups() throws Exception {
        String firstGroup = "group1";
        String secondGroup = "group2";
        String bootstrapServer = "localhost:9092";

        for (List<String> describeType : DESCRIBE_TYPE_STATE) {
            List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", bootstrapServer, "--describe", "--all-groups"));
            cgcArgs.addAll(describeType);
            Admin adminClient = mock(KafkaAdminClient.class);
            ListGroupsResult listGroupsResult = mock(ListGroupsResult.class);
            GroupListing firstGroupListing = new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE));
            GroupListing secondGroupListing = new GroupListing(secondGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE));
            DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
            ShareGroupDescription exp1 = new ShareGroupDescription(
                firstGroup,
                List.of(new ShareMemberDescription("memid1", "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);
            ShareGroupDescription exp2 = new ShareGroupDescription(
                secondGroup,
                List.of(new ShareMemberDescription("memid1", "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);

            when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(firstGroupListing, secondGroupListing)));
            when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(listGroupsResult);
            when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(firstGroup, KafkaFuture.completedFuture(exp1), secondGroup, KafkaFuture.completedFuture(exp2)));
            when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
            try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
                TestUtils.waitForCondition(() -> {
                    Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                    String[] lines = res.getKey().trim().split("\n");
                    if (lines.length != 2 && !res.getValue().isEmpty()) {
                        return false;
                    }

                    List<String> expectedValues1;
                    List<String> expectedValues2;
                    if (describeType.contains("--verbose")) {
                        expectedValues1 = List.of(firstGroup, "host1:9090", "(0)", "Stable", "0", "0", "1");
                        expectedValues2 = List.of(secondGroup, "host1:9090", "(0)", "Stable", "0", "0", "1");

                    } else {
                        expectedValues1 = List.of(firstGroup, "host1:9090", "(0)", "Stable", "1");
                        expectedValues2 = List.of(secondGroup, "host1:9090", "(0)", "Stable", "1");
                    }
                    return checkArgsHeaderOutput(cgcArgs, lines[0]) && checkArgsHeaderOutput(cgcArgs, lines[3]) &&
                        Arrays.stream(lines[1].trim().split("\\s+")).toList().equals(expectedValues1) &&
                        Arrays.stream(lines[4].trim().split("\\s+")).toList().equals(expectedValues2);
                }, "Expected 2 data rows and no error in describe results with describe type " + String.join(" ", describeType) + ".");
            }
        }
    }

    @Test
    public void testDescribeMembersOfExistingGroup() throws Exception {
        String firstGroup = "group1";
        String bootstrapServer = "localhost:9092";

        for (List<String> describeType : DESCRIBE_TYPE_MEMBERS) {
            List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", bootstrapServer, "--describe", "--group", firstGroup));
            cgcArgs.addAll(describeType);
            Admin adminClient = mock(KafkaAdminClient.class);
            DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
            ShareGroupDescription exp1 = new ShareGroupDescription(
                firstGroup,
                List.of(new ShareMemberDescription("memid1", "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0), new TopicPartition("topic1", 1), new TopicPartition("topic2", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);

            when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(firstGroup, KafkaFuture.completedFuture(exp1)));
            when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
            try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
                TestUtils.waitForCondition(() -> {
                    Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                    String[] lines = res.getKey().trim().split("\n");
                    if (lines.length != 2 && !res.getValue().isEmpty()) {
                        return false;
                    }

                    List<String> expectedValues1;
                    if (describeType.contains("--verbose")) {
                        expectedValues1 = List.of(firstGroup, "memid1", "host1", "clId1", "0", "topic1:0,1;topic2:0");

                    } else {
                        expectedValues1 = List.of(firstGroup, "memid1", "host1", "clId1", "topic1:0,1;topic2:0");
                    }
                    return checkArgsHeaderOutput(cgcArgs, lines[0]) &&
                        Arrays.stream(lines[1].trim().split("\\s+")).toList().equals(expectedValues1);
                }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
            }
        }
    }

    @Test
    public void testDescribeMembersOfAllExistingGroups() throws Exception {
        String firstGroup = "group1";
        String secondGroup = "group2";
        String bootstrapServer = "localhost:9092";

        for (List<String> describeType : DESCRIBE_TYPE_MEMBERS) {
            List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", bootstrapServer, "--describe", "--all-groups"));
            cgcArgs.addAll(describeType);
            Admin adminClient = mock(KafkaAdminClient.class);
            ListGroupsResult listGroupsResult = mock(ListGroupsResult.class);
            GroupListing firstGroupListing = new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE));
            GroupListing secondGroupListing = new GroupListing(secondGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE));
            DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
            ShareGroupDescription exp1 = new ShareGroupDescription(
                firstGroup,
                List.of(new ShareMemberDescription("memid1", "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0), new TopicPartition("topic1", 1), new TopicPartition("topic2", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);
            ShareGroupDescription exp2 = new ShareGroupDescription(
                secondGroup,
                List.of(new ShareMemberDescription("memid1", "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);

            when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(firstGroupListing, secondGroupListing)));
            when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(listGroupsResult);
            when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(firstGroup, KafkaFuture.completedFuture(exp1), secondGroup, KafkaFuture.completedFuture(exp2)));
            when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
            try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
                TestUtils.waitForCondition(() -> {
                    Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                    String[] lines = res.getKey().trim().split("\n");
                    if (lines.length != 2 && !res.getValue().isEmpty()) {
                        return false;
                    }

                    List<String> expectedValues1;
                    List<String> expectedValues2;
                    if (describeType.contains("--verbose")) {
                        expectedValues1 = List.of(firstGroup, "memid1", "host1", "clId1", "0", "topic1:0,1;topic2:0");
                        expectedValues2 = List.of(secondGroup, "memid1", "host1", "clId1", "0", "topic1:0");

                    } else {
                        expectedValues1 = List.of(firstGroup, "memid1", "host1", "clId1", "topic1:0,1;topic2:0");
                        expectedValues2 = List.of(secondGroup, "memid1", "host1", "clId1", "topic1:0");
                    }
                    return checkArgsHeaderOutput(cgcArgs, lines[0]) && checkArgsHeaderOutput(cgcArgs, lines[3]) &&
                        Arrays.stream(lines[1].trim().split("\\s+")).toList().equals(expectedValues1) &&
                        Arrays.stream(lines[4].trim().split("\\s+")).toList().equals(expectedValues2);
                }, "Expected 2 data rows and no error in describe results with describe type " + String.join(" ", describeType) + ".");
            }
        }
    }

    @Test
    public void testDescribeNonexistentGroup() {
        String missingGroup = "missing.group";
        String bootstrapServer = "localhost:9092";

        for (List<String> describeType : DESCRIBE_TYPES) {
            // note the group to be queried is a different (non-existing) group
            List<String> cgcArgs = new ArrayList<>(Arrays.asList("--bootstrap-server", bootstrapServer, "--describe", "--group", missingGroup));
            cgcArgs.addAll(describeType);
            Admin adminClient = mock(KafkaAdminClient.class);
            DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
            KafkaFutureImpl<ShareGroupDescription> missingGroupFuture = new KafkaFutureImpl<>();
            missingGroupFuture.completeExceptionally(new GroupIdNotFoundException("Group " + missingGroup + " not found."));
            when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(missingGroup, missingGroupFuture));
            when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
            try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
                service.describeGroups();
                fail("Expected error was not detected for describe option '" + String.join(" ", describeType) + "'");
            } catch (ExecutionException ee) {
                assertInstanceOf(GroupIdNotFoundException.class, ee.getCause());
                assertEquals("Group " + missingGroup + " not found.", ee.getCause().getMessage());
            } catch (Exception e) {
                fail("Expected error was not detected for describe option '" + String.join(" ", describeType) + "'");
            }
        }
    }

    @Test
    public void testDescribeShareGroupsInvalidVerboseOption() {
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--describe", "--group", "group1", "--verbose"};
        assertThrows(OptionException.class, () -> getShareGroupService(cgcArgs, new MockAdminClient()));
    }

    @Test
    public void testDescribeShareGroupsOffsetsInvalidVerboseOption() {
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--describe", "--group", "group1", "--offsets", "--verbose"};
        assertThrows(OptionException.class, () -> getShareGroupService(cgcArgs, new MockAdminClient()));
    }

    @Test
    public void testPrintEmptyGroupState() {
        assertFalse(ShareGroupService.maybePrintEmptyGroupState("group", GroupState.EMPTY, 0));
        assertFalse(ShareGroupService.maybePrintEmptyGroupState("group", GroupState.DEAD, 0));
        assertFalse(ShareGroupService.maybePrintEmptyGroupState("group", GroupState.STABLE, 0));
        assertTrue(ShareGroupService.maybePrintEmptyGroupState("group", GroupState.STABLE, 1));
        assertTrue(ShareGroupService.maybePrintEmptyGroupState("group", GroupState.UNKNOWN, 1));
    }

    @Test
    public void testListWithUnrecognizedOption() {
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--list", "--verbose"};
        assertThrows(OptionException.class, () -> getShareGroupService(cgcArgs, new MockAdminClient()));
    }

    @Test
    public void testGroupStatesFromString() {
        Set<GroupState> result = ShareGroupCommand.groupStatesFromString("Stable");
        assertEquals(Set.of(GroupState.STABLE), result);

        result = ShareGroupCommand.groupStatesFromString("stable");
        assertEquals(Set.of(GroupState.STABLE), result);

        result = ShareGroupCommand.groupStatesFromString("dead");
        assertEquals(Set.of(GroupState.DEAD), result);

        result = ShareGroupCommand.groupStatesFromString("empty");
        assertEquals(Set.of(GroupState.EMPTY), result);

        assertThrows(IllegalArgumentException.class, () -> ShareGroupCommand.groupStatesFromString("assigning"));

        assertThrows(IllegalArgumentException.class, () -> ShareGroupCommand.groupStatesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ShareGroupCommand.groupStatesFromString("  bad, Stable"));

        assertThrows(IllegalArgumentException.class, () -> ShareGroupCommand.groupStatesFromString("   ,   ,"));
    }

    ShareGroupService getShareGroupService(String[] args, Admin adminClient) {
        ShareGroupCommandOptions opts = new ShareGroupCommandOptions(args);
        return new ShareGroupService(opts, adminClient);
    }

    private Runnable describeGroups(ShareGroupCommand.ShareGroupService service) {
        return () -> Assertions.assertDoesNotThrow(service::describeGroups);
    }

    private boolean checkArgsHeaderOutput(List<String> args, String output) {
        if (!output.contains("GROUP")) {
            return false;
        }

        if (args.contains("--members")) {
            return checkMembersArgsHeaderOutput(output, args.contains("--verbose"));
        }

        if (args.contains("--state")) {
            return checkStateArgsHeaderOutput(output, args.contains("--verbose"));
        }

        // --offsets or no arguments
        return checkOffsetsArgsHeaderOutput(output);
    }

    private boolean checkOffsetsArgsHeaderOutput(String output) {
        List<String> expectedKeys = List.of("GROUP", "TOPIC", "PARTITION", "START-OFFSET");
        return Arrays.stream(output.trim().split("\\s+")).toList().equals(expectedKeys);
    }

    private boolean checkMembersArgsHeaderOutput(String output, boolean verbose) {
        List<String> expectedKeys = verbose ?
            List.of("GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "MEMBER-EPOCH", "ASSIGNMENT") :
            List.of("GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "ASSIGNMENT");
        return Arrays.stream(output.trim().split("\\s+")).toList().equals(expectedKeys);
    }

    private boolean checkStateArgsHeaderOutput(String output, boolean verbose) {
        List<String> expectedKeys = verbose ?
            List.of("GROUP", "COORDINATOR", "(ID)", "STATE", "GROUP-EPOCH", "ASSIGNMENT-EPOCH", "#MEMBERS") :
            List.of("GROUP", "COORDINATOR", "(ID)", "STATE", "#MEMBERS");
        return Arrays.stream(output.trim().split("\\s+")).toList().equals(expectedKeys);
    }
}
