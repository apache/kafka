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
import org.apache.kafka.clients.admin.AlterShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.AlterShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteShareGroupsResult;
import org.apache.kafka.clients.admin.DescribeShareGroupsOptions;
import org.apache.kafka.clients.admin.DescribeShareGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberAssignment;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;
import org.apache.kafka.tools.consumer.group.ShareGroupCommand.ShareGroupService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

import joptsimple.OptionException;

import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShareGroupCommandTest {
    private static final List<List<String>> DESCRIBE_TYPE_OFFSETS = List.of(List.of(""), List.of("--offsets"), List.of("--verbose"), List.of("--offsets", "--verbose"));
    private static final List<List<String>> DESCRIBE_TYPE_MEMBERS = List.of(List.of("--members"), List.of("--members", "--verbose"));
    private static final List<List<String>> DESCRIBE_TYPE_STATE = List.of(List.of("--state"), List.of("--state", "--verbose"));
    private static final List<List<String>> DESCRIBE_TYPES = Stream.of(DESCRIBE_TYPE_OFFSETS, DESCRIBE_TYPE_MEMBERS, DESCRIBE_TYPE_STATE).flatMap(Collection::stream).toList();

    @BeforeEach
    public void setup() {
        // nothing by default
        Exit.setExitProcedure(((statusCode, message) -> {
        }));
    }

    @AfterEach
    public void teardown() {
        Exit.resetExitProcedure();
    }

    @Test
    public void testListShareGroups() throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--list"};
        Admin adminClient = mock(KafkaAdminClient.class);
        ListGroupsResult result = mock(ListGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(List.of(
                new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
                new GroupListing(secondGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.EMPTY))
        )));

        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(result);
        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            Set<String> expectedGroups = Set.of(firstGroup, secondGroup);

            final Set[] foundGroups = new Set[]{Set.of()};
            TestUtils.waitForCondition(() -> {
                foundGroups[0] = new HashSet<>(service.listShareGroups());
                return Objects.equals(expectedGroups, foundGroups[0]);
            }, () -> "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups[0] + ".");
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
        when(resultWithAllStates.all()).thenReturn(KafkaFuture.completedFuture(List.of(
            new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
            new GroupListing(secondGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.EMPTY))
        )));
        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(resultWithAllStates);
        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            Set<GroupListing> expectedListing = Set.of(
                new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
                new GroupListing(secondGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.EMPTY)));

            final Set[] foundListing = new Set[]{Set.of()};
            TestUtils.waitForCondition(() -> {
                foundListing[0] = new HashSet<>(service.listShareGroupsInStates(Set.of(GroupState.values())));
                return Objects.equals(expectedListing, foundListing[0]);
            }, () -> "Expected to show groups " + expectedListing + ", but found " + foundListing[0]);

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
            }, () -> "Expected to show groups " + expectedListingStable + ", but found " + foundListing[0]);
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
                List.of(new ShareMemberDescription("memid1", Optional.of("rackId1"), "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);
            ListShareGroupOffsetsResult listShareGroupOffsetsResult = AdminClientTestUtils.createListShareGroupOffsetsResult(
                Map.of(
                    firstGroup,
                    KafkaFuture.completedFuture(
                            Map.of(new TopicPartition("topic1", 0), new SharePartitionOffsetInfo(0L, Optional.of(1), Optional.of(0L))))
                )
            );

            when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(firstGroup, KafkaFuture.completedFuture(exp)));
            when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
            when(adminClient.listShareGroupOffsets(ArgumentMatchers.anyMap(), any(ListShareGroupOffsetsOptions.class))).thenReturn(listShareGroupOffsetsResult);
            try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
                TestUtils.waitForCondition(() -> {
                    Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                    String[] lines = res.getKey().trim().split("\n");
                    if (lines.length != 2 && !res.getValue().isEmpty()) {
                        return false;
                    }

                    List<String> expectedValues;
                    if (describeType.contains("--verbose")) {
                        expectedValues = List.of(firstGroup, "topic1", "0", "1", "0", "0");
                    } else {
                        expectedValues = List.of(firstGroup, "topic1", "0", "0", "0");
                    }
                    return checkArgsHeaderOutput(cgcArgs, lines[0]) &&
                        Arrays.stream(lines[1].trim().split("\\s+")).toList().equals(expectedValues);
                }, "Expected a data row and no error in describe results with describe type " + String.join(" ", describeType) + ".");
            }
        }
    }

    @Test
    public void testDescribeOffsetsOfExistingGroupWithNulls() throws Exception {
        String firstGroup = "group1";
        String bootstrapServer = "localhost:9092";

        for (List<String> describeType : DESCRIBE_TYPE_OFFSETS) {
            List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", bootstrapServer, "--describe", "--group", firstGroup));
            cgcArgs.addAll(describeType);
            Admin adminClient = mock(KafkaAdminClient.class);
            DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
            ShareGroupDescription exp = new ShareGroupDescription(
                firstGroup,
                List.of(new ShareMemberDescription("memid1", Optional.empty(), "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);
            // The null here indicates a topic-partition for which offset information could not be retrieved, typically due to an error
            ListShareGroupOffsetsResult listShareGroupOffsetsResult = AdminClientTestUtils.createListShareGroupOffsetsResult(
                Map.of(
                    firstGroup,
                    KafkaFuture.completedFuture(Collections.singletonMap(new TopicPartition("topic1", 0), null))
                )
            );

            when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(firstGroup, KafkaFuture.completedFuture(exp)));
            when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
            when(adminClient.listShareGroupOffsets(ArgumentMatchers.anyMap(), any(ListShareGroupOffsetsOptions.class))).thenReturn(listShareGroupOffsetsResult);
            try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
                TestUtils.waitForCondition(() -> {
                    Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(describeGroups(service));
                    String[] lines = res.getKey().trim().split("\n");
                    if (lines.length != 2 && !res.getValue().isEmpty()) {
                        return false;
                    }

                    List<String> expectedValues;
                    if (describeType.contains("--verbose")) {
                        expectedValues = List.of(firstGroup, "topic1", "0", "-", "-", "-");
                    } else {
                        expectedValues = List.of(firstGroup, "topic1", "0", "-", "-");
                    }
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
                List.of(new ShareMemberDescription("memid1", Optional.of("rackId1"), "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);
            ShareGroupDescription exp2 = new ShareGroupDescription(
                secondGroup,
                List.of(new ShareMemberDescription("memid1", Optional.of("rackId1"), "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);
            ListShareGroupOffsetsResult listShareGroupOffsetsResult1 = AdminClientTestUtils.createListShareGroupOffsetsResult(
                Map.of(
                    firstGroup,
                    KafkaFuture.completedFuture(Map.of(new TopicPartition("topic1", 0), new SharePartitionOffsetInfo(0, Optional.of(1), Optional.empty())))
                )
            );
            ListShareGroupOffsetsResult listShareGroupOffsetsResult2 = AdminClientTestUtils.createListShareGroupOffsetsResult(
                Map.of(
                    secondGroup,
                    KafkaFuture.completedFuture(Map.of(new TopicPartition("topic1", 0), new SharePartitionOffsetInfo(0, Optional.of(1), Optional.of(0L))))
                )
            );

            when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(firstGroupListing, secondGroupListing)));
            when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(listGroupsResult);
            when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(firstGroup, KafkaFuture.completedFuture(exp1), secondGroup, KafkaFuture.completedFuture(exp2)));
            when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
            when(adminClient.listShareGroupOffsets(ArgumentMatchers.anyMap(), any(ListShareGroupOffsetsOptions.class))).thenAnswer(
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

                    List<String> expectedValues1, expectedValues2;
                    if (describeType.contains("--verbose")) {
                        expectedValues1 = List.of(firstGroup, "topic1", "0", "1", "0", "-");
                        expectedValues2 = List.of(secondGroup, "topic1", "0", "1", "0", "0");
                    } else {
                        expectedValues1 = List.of(firstGroup, "topic1", "0", "0", "-");
                        expectedValues2 = List.of(secondGroup, "topic1", "0", "0", "0");
                    }
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
                List.of(new ShareMemberDescription("memid1", Optional.of("rackId1"), "clId1", "host1", new ShareMemberAssignment(
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
                List.of(new ShareMemberDescription("memid1", Optional.of("rackId1"), "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);
            ShareGroupDescription exp2 = new ShareGroupDescription(
                secondGroup,
                List.of(new ShareMemberDescription("memid1", Optional.of("rackId1"), "clId1", "host1", new ShareMemberAssignment(
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
                List.of(new ShareMemberDescription("memid1", Optional.of("rackId1"), "clId1", "host1", new ShareMemberAssignment(
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
                        expectedValues1 = List.of(firstGroup, "memid1", "host1", "clId1", "3", "0", "topic1:0,1;topic2:0");

                    } else {
                        expectedValues1 = List.of(firstGroup, "memid1", "host1", "clId1", "3", "topic1:0,1;topic2:0");
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
                List.of(new ShareMemberDescription("memid1", Optional.of("rackId1"), "clId1", "host1", new ShareMemberAssignment(
                    Set.of(new TopicPartition("topic1", 0), new TopicPartition("topic1", 1), new TopicPartition("topic2", 0))
                ), 0)),
                GroupState.STABLE,
                new Node(0, "host1", 9090), 0, 0);
            ShareGroupDescription exp2 = new ShareGroupDescription(
                secondGroup,
                List.of(new ShareMemberDescription("memid1", Optional.of("rackId1"), "clId1", "host1", new ShareMemberAssignment(
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
                        expectedValues1 = List.of(firstGroup, "memid1", "host1", "clId1", "3", "0", "topic1:0,1;topic2:0");
                        expectedValues2 = List.of(secondGroup, "memid1", "host1", "clId1", "1", "0", "topic1:0");

                    } else {
                        expectedValues1 = List.of(firstGroup, "memid1", "host1", "clId1", "3", "topic1:0,1;topic2:0");
                        expectedValues2 = List.of(secondGroup, "memid1", "host1", "clId1", "1", "topic1:0");
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
            List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", bootstrapServer, "--describe", "--group", missingGroup));
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

    @Test
    public void testDeleteShareGroupOffsetsArgsWithoutTopic() {
        String bootstrapServer = "localhost:9092";
        Admin adminClient = mock(KafkaAdminClient.class);

        // no group spec args
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--delete-offsets", "--group", "groupId"};
        AtomicBoolean exited = new AtomicBoolean(false);
        Exit.setExitProcedure(((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Option [delete-offsets] takes the following options: [group], [topic]"));
            exited.set(true);
        }));
        try {
            getShareGroupService(cgcArgs, adminClient);
        } finally {
            assertTrue(exited.get());
        }
    }

    @Test
    public void testDeleteShareGroupOffsetsArgsWithoutGroup() {
        String bootstrapServer = "localhost:9092";
        Admin adminClient = mock(KafkaAdminClient.class);

        // no group spec args
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--delete-offsets", "--topic", "t1"};
        AtomicBoolean exited = new AtomicBoolean(false);
        Exit.setExitProcedure(((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Option [delete-offsets] takes the following options: [group], [topic]"));
            exited.set(true);
        }));
        try {
            getShareGroupService(cgcArgs, adminClient);
        } finally {
            assertTrue(exited.get());
        }
    }

    @Test
    public void testDeleteShareGroupOffsets() throws Exception {
        String firstGroup = "first-group";
        String firstTopic = "t1";
        String secondTopic = "t2";
        String bootstrapServer = "localhost:9092";

        List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", bootstrapServer, "--delete-offsets", "--group", firstGroup, "--topic", firstTopic, "--topic", secondTopic));
        Admin adminClient = mock(KafkaAdminClient.class);
        DeleteShareGroupOffsetsResult result = mock(DeleteShareGroupOffsetsResult.class);

        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));

        when(result.topicResult(eq(firstTopic))).thenReturn(KafkaFuture.completedFuture(null));
        when(result.topicResult(eq(secondTopic))).thenReturn(KafkaFuture.completedFuture(null));

        when(adminClient.deleteShareGroupOffsets(any(), any(), any())).thenReturn(result);

        try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
            TestUtils.waitForCondition(() -> {
                Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(deleteOffsets(service));
                String[] lines = res.getKey().trim().split("\n");
                if (lines.length != 3 && !res.getValue().isEmpty()) {
                    return false;
                }

                List<String> expectedResultHeader = List.of("TOPIC", "STATUS");
                List<String> expectedResultValues1 = List.of(firstTopic, "Successful");
                List<String> expectedResultValues2 = List.of(secondTopic, "Successful");

                return Arrays.stream(lines[0].trim().split("\\s+")).toList().equals(expectedResultHeader) &&
                    Arrays.stream(lines[1].trim().split("\\s+")).toList().equals(expectedResultValues1) &&
                    Arrays.stream(lines[2].trim().split("\\s+")).toList().equals(expectedResultValues2);
            }, "Expected a data row and no error in delete offsets result with group: " + firstGroup + " and topic: " + firstTopic);
        }
    }

    @Test
    public void testDeleteShareGroupOffsetsMultipleGroups() {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String firstTopic = "t1";
        String secondTopic = "t2";
        String bootstrapServer = "localhost:9092";

        List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", bootstrapServer, "--delete-offsets", "--group", firstGroup, "--group", secondGroup, "--topic", firstTopic, "--topic", secondTopic));
        Admin adminClient = mock(KafkaAdminClient.class);

        try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
            service.deleteOffsets();
            fail("Expected error was not detected while trying delete offsets multiple groups");
        } catch (Exception e) {
            String expectedErrorMessage = "Found multiple arguments for option group, but you asked for only one";
            assertEquals(expectedErrorMessage, e.getMessage());
        }
    }

    @Test
    public void testDeleteShareGroupOffsetsTopLevelError() throws Exception {
        String firstGroup = "first-group";
        String firstTopic = "t1";
        String secondTopic = "t2";
        String bootstrapServer = "localhost:9092";

        List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", bootstrapServer, "--delete-offsets", "--group", firstGroup, "--topic", firstTopic, "--topic", secondTopic));
        Admin adminClient = mock(KafkaAdminClient.class);
        DeleteShareGroupOffsetsResult result = mock(DeleteShareGroupOffsetsResult.class);

        KafkaFutureImpl<Void> resultFuture = new KafkaFutureImpl<>();
        String errorMessage = "Group g3 not found.";
        GroupIdNotFoundException exception = new GroupIdNotFoundException(errorMessage);

        resultFuture.completeExceptionally(exception);
        when(result.all()).thenReturn(resultFuture);

        when(result.topicResult(eq(firstTopic))).thenReturn(resultFuture);
        when(result.topicResult(eq(secondTopic))).thenReturn(resultFuture);

        when(adminClient.deleteShareGroupOffsets(any(), any(), any())).thenReturn(result);

        try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
            TestUtils.waitForCondition(() -> {
                Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(deleteOffsets(service));
                String[] lines = res.getKey().trim().split("\n");
                if (lines.length != 5 && !res.getValue().isEmpty()) {
                    return false;
                }

                List<String> error = Stream.concat(
                    Stream.of("Error:"),
                    Arrays.stream(errorMessage.trim().split("\\s+"))
                    ).toList();

                List<String> errorLine = new ArrayList<>(error);
                List<String> expectedResultHeader = List.of("TOPIC", "STATUS");
                List<String> expectedResultValue1 =  new ArrayList<>();
                expectedResultValue1.add(firstTopic);
                expectedResultValue1.addAll(error);
                List<String> expectedResultValue2 =  new ArrayList<>();
                expectedResultValue2.add(secondTopic);
                expectedResultValue2.addAll(error);

                return Arrays.stream(lines[0].trim().split("\\s+")).toList().equals(errorLine) &&
                    Arrays.stream(lines[2].trim().split("\\s+")).toList().equals(expectedResultHeader) &&
                    Arrays.stream(lines[3].trim().split("\\s+")).toList().equals(expectedResultValue1) &&
                    Arrays.stream(lines[4].trim().split("\\s+")).toList().equals(expectedResultValue2);
            }, "Expected a data row and no error in delete offsets result with group: " + firstGroup + " and topic: " + firstTopic);
        }
    }

    @Test
    public void testDeleteShareGroupOffsetsTopicLevelError() throws Exception {
        String firstGroup = "first-group";
        String firstTopic = "t1";
        String secondTopic = "t2";
        String bootstrapServer = "localhost:9092";

        List<String> cgcArgs = new ArrayList<>(List.of("--bootstrap-server", bootstrapServer, "--delete-offsets", "--group", firstGroup, "--topic", firstTopic, "--topic", secondTopic));
        Admin adminClient = mock(KafkaAdminClient.class);
        DeleteShareGroupOffsetsResult result = mock(DeleteShareGroupOffsetsResult.class);

        KafkaFutureImpl<Void> resultFuture = new KafkaFutureImpl<>();
        String errorMessage = Errors.UNKNOWN_TOPIC_OR_PARTITION.message();

        resultFuture.completeExceptionally(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception());
        when(result.all()).thenReturn(resultFuture);

        when(result.topicResult(eq(firstTopic))).thenReturn(KafkaFuture.completedFuture(null));
        when(result.topicResult(eq(secondTopic))).thenReturn(resultFuture);

        when(adminClient.deleteShareGroupOffsets(any(), any(), any())).thenReturn(result);

        try (ShareGroupService service = getShareGroupService(cgcArgs.toArray(new String[0]), adminClient)) {
            TestUtils.waitForCondition(() -> {
                Entry<String, String> res = ToolsTestUtils.grabConsoleOutputAndError(deleteOffsets(service));
                String[] lines = res.getKey().trim().split("\n");
                if (lines.length != 5 && !res.getValue().isEmpty()) {
                    return false;
                }

                List<String> error = Stream.concat(
                    Stream.of("Error:"),
                    Arrays.stream(errorMessage.trim().split("\\s+"))
                ).toList();

                List<String> expectedResultHeader = List.of("TOPIC", "STATUS");
                List<String> expectedResultValue1 =  List.of(firstTopic, "Successful");
                List<String> expectedResultValue2 =  new ArrayList<>();
                expectedResultValue2.add(secondTopic);
                expectedResultValue2.addAll(error);

                return Arrays.stream(lines[0].trim().split("\\s+")).toList().equals(expectedResultHeader) &&
                    Arrays.stream(lines[1].trim().split("\\s+")).toList().equals(expectedResultValue1) &&
                    Arrays.stream(lines[2].trim().split("\\s+")).toList().equals(expectedResultValue2);
            }, "Expected a data row and no error in delete offsets result with group: " + firstGroup + " and topic: " + firstTopic);
        }
    }

    @Test
    public void testDeleteShareGroupsArgs() {
        String bootstrapServer = "localhost:9092";
        Admin adminClient = mock(KafkaAdminClient.class);

        mockListShareGroups(adminClient, new LinkedHashMap<>());

        // no group spec args
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--delete"};
        AtomicBoolean exited = new AtomicBoolean(false);
        Exit.setExitProcedure(((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Option [delete] takes the options [group] or [all-groups]"));
            exited.set(true);
        }));
        try {
            getShareGroupService(cgcArgs, adminClient);
        } finally {
            assertTrue(exited.get());
        }
    }

    @Test
    public void testDeleteShareGroupsSuccess() {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--delete", "--group", firstGroup, "--group", secondGroup};
        Admin adminClient = mock(KafkaAdminClient.class);
        DeleteShareGroupsResult result = mock(DeleteShareGroupsResult.class);
        Map<String, KafkaFuture<Void>> deletedGroups = Map.of(
            firstGroup, KafkaFuture.completedFuture(null),
            secondGroup, KafkaFuture.completedFuture(null)
        );

        LinkedHashMap<String, GroupState> shareGroupMap = new LinkedHashMap<>();
        shareGroupMap.put(firstGroup, GroupState.EMPTY);
        shareGroupMap.put(secondGroup, GroupState.EMPTY);
        mockListShareGroups(adminClient, shareGroupMap);

        when(result.deletedGroups()).thenReturn(deletedGroups);

        Map<String, Throwable> expectedResults = new HashMap<>();
        expectedResults.put(firstGroup, null);
        expectedResults.put(secondGroup, null);

        when(adminClient.deleteShareGroups(anyList(), any())).thenReturn(result);

        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            assertEquals(expectedResults, service.deleteShareGroups());
        }
    }

    @Test
    public void testDeleteShareGroupsAllGroupsSuccess() {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--delete", "--all-groups"};
        Admin adminClient = mock(KafkaAdminClient.class);
        DeleteShareGroupsResult result = mock(DeleteShareGroupsResult.class);
        Map<String, KafkaFuture<Void>> deletedGroups = Map.of(
            firstGroup, KafkaFuture.completedFuture(null),
            secondGroup, KafkaFuture.completedFuture(null)
        );

        LinkedHashMap<String, GroupState> shareGroupMap = new LinkedHashMap<>();
        shareGroupMap.put(firstGroup, GroupState.EMPTY);
        shareGroupMap.put(secondGroup, GroupState.EMPTY);
        mockListShareGroups(adminClient, shareGroupMap);

        when(result.deletedGroups()).thenReturn(deletedGroups);

        Map<String, Throwable> expectedResults = new HashMap<>();
        expectedResults.put(firstGroup, null);
        expectedResults.put(secondGroup, null);

        when(adminClient.deleteShareGroups(anyList(), any())).thenReturn(result);

        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            assertEquals(expectedResults, service.deleteShareGroups());
        }
    }

    @Test
    public void testDeleteShareGroupsAllGroupsPartialFail() {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--delete", "--all-groups"};
        Admin adminClient = mock(KafkaAdminClient.class);
        DeleteShareGroupsResult result = mock(DeleteShareGroupsResult.class);
        KafkaFutureImpl<Void> future1 = new KafkaFutureImpl<>();
        KafkaFutureImpl<Void> future2 = new KafkaFutureImpl<>();
        future1.complete(null);
        Exception exp = new Exception("bad");
        future2.completeExceptionally(exp);
        Map<String, KafkaFuture<Void>> deletedGroups = Map.of(
            firstGroup, future1,
            secondGroup, future2
        );

        LinkedHashMap<String, GroupState> shareGroupMap = new LinkedHashMap<>();
        shareGroupMap.put(firstGroup, GroupState.EMPTY);
        shareGroupMap.put(secondGroup, GroupState.EMPTY);
        mockListShareGroups(adminClient, shareGroupMap);

        when(result.deletedGroups()).thenReturn(deletedGroups);

        Map<String, Throwable> expectedResults = new HashMap<>();
        expectedResults.put(firstGroup, null);
        expectedResults.put(secondGroup, exp);

        when(adminClient.deleteShareGroups(anyList(), any())).thenReturn(result);

        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            assertEquals(expectedResults, service.deleteShareGroups());
        }
    }

    @Test
    public void testDeleteShareGroupsDeleteFailure() {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--delete", "--group", firstGroup, "--group", secondGroup};
        Admin adminClient = mock(KafkaAdminClient.class);
        DeleteShareGroupsResult result = mock(DeleteShareGroupsResult.class);

        LinkedHashMap<String, GroupState> shareGroupMap = new LinkedHashMap<>();
        shareGroupMap.put(firstGroup, GroupState.EMPTY);
        shareGroupMap.put(secondGroup, GroupState.EMPTY);
        mockListShareGroups(adminClient, shareGroupMap);

        KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        Exception exp = new Exception("bad");
        future.completeExceptionally(exp);
        Map<String, KafkaFuture<Void>> deletedGroups = Map.of(
            firstGroup, future,
            secondGroup, future
        );

        when(result.deletedGroups()).thenReturn(deletedGroups);

        Map<String, Throwable> expectedResults = new HashMap<>();
        expectedResults.put(firstGroup, exp);
        expectedResults.put(secondGroup, exp);

        when(adminClient.deleteShareGroups(anyList(), any())).thenReturn(result);

        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            assertEquals(expectedResults, service.deleteShareGroups());
        }
    }

    @Test
    public void testDeleteShareGroupsFailureNonShareGroup() {
        String firstGroup = "first-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--delete", "--group", firstGroup};
        Admin adminClient = mock(KafkaAdminClient.class);
        DeleteShareGroupsResult result = mock(DeleteShareGroupsResult.class);
        mockListShareGroups(adminClient, new LinkedHashMap<>());

        when(result.deletedGroups()).thenReturn(Map.of());

        when(adminClient.deleteShareGroups(anyList(), any())).thenReturn(result);

        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            service.deleteShareGroups();
            verify(result, times(0)).deletedGroups();
            verify(adminClient, times(0)).deleteShareGroups(anyList());
        }
    }

    @Test
    public void testDeleteShareGroupsFailureNonEmptyGroup() {
        String firstGroup = "first-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--delete", "--group", firstGroup};
        Admin adminClient = mock(KafkaAdminClient.class);
        DeleteShareGroupsResult result = mock(DeleteShareGroupsResult.class);

        LinkedHashMap<String, GroupState> shareGroupMap = new LinkedHashMap<>();
        shareGroupMap.put(firstGroup, GroupState.STABLE);
        mockListShareGroups(adminClient, shareGroupMap);

        when(result.deletedGroups()).thenReturn(Map.of());

        when(adminClient.deleteShareGroups(anyList(), any())).thenReturn(result);

        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            service.deleteShareGroups();
            verify(result, times(0)).deletedGroups();
            verify(adminClient, times(0)).deleteShareGroups(anyList());
        }
    }

    @Test
    public void testDeleteShareGroupsPartialFailure() {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--delete", "--group", firstGroup, "--group", secondGroup};
        Admin adminClient = mock(KafkaAdminClient.class);
        DeleteShareGroupsResult result = mock(DeleteShareGroupsResult.class);
        LinkedHashMap<String, GroupState> shareGroupMap = new LinkedHashMap<>();
        shareGroupMap.put(firstGroup, GroupState.EMPTY);
        shareGroupMap.put(secondGroup, GroupState.EMPTY);
        mockListShareGroups(adminClient, shareGroupMap);
        KafkaFutureImpl<Void> future1 = new KafkaFutureImpl<>();
        KafkaFutureImpl<Void> future2 = new KafkaFutureImpl<>();
        future1.complete(null);
        Exception exp = new Exception("bad");
        future2.completeExceptionally(exp);
        Map<String, KafkaFuture<Void>> deletedGroups = Map.of(
            firstGroup, future1,
            secondGroup, future2
        );

        when(result.deletedGroups()).thenReturn(deletedGroups);

        when(adminClient.deleteShareGroups(anyList(), any())).thenReturn(result);
        Map<String, Throwable> expectedResults = new HashMap<>();
        expectedResults.put(firstGroup, null);
        expectedResults.put(secondGroup, exp);

        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            assertEquals(expectedResults, service.deleteShareGroups());
        }
    }
    
    @Test
    public void testAlterShareGroupMultipleTopicsSuccess() {
        String group = "share-group";
        String topic1 = "topic1";
        String topic2 = "topic2";
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--reset-offsets", "--to-earliest", "--execute", "--topic", topic1, "--topic", topic2, "--group", group};
        Admin adminClient = mock(KafkaAdminClient.class);

        ListShareGroupOffsetsResult listShareGroupOffsetsResult = AdminClientTestUtils.createListShareGroupOffsetsResult(
            Map.of(
                group,
                KafkaFuture.completedFuture(Map.of(
                    new TopicPartition(topic1, 0), new SharePartitionOffsetInfo(10L, Optional.empty(), Optional.empty()),
                    new TopicPartition(topic1, 1), new SharePartitionOffsetInfo(10L, Optional.empty(), Optional.empty()),
                    new TopicPartition(topic2, 0), new SharePartitionOffsetInfo(0L, Optional.empty(), Optional.empty())))
            )
        );
        when(adminClient.listShareGroupOffsets(any(), any(ListShareGroupOffsetsOptions.class))).thenReturn(listShareGroupOffsetsResult);
        
        AlterShareGroupOffsetsResult alterShareGroupOffsetsResult = mockAlterShareGroupOffsets(adminClient, group);
        TopicPartition tp0 = new TopicPartition(topic1, 0);
        TopicPartition tp1 = new TopicPartition(topic1, 1);
        TopicPartition tp2 = new TopicPartition(topic2, 0);
        Map<TopicPartition, OffsetAndMetadata> partitionOffsets = Map.of(tp0, new OffsetAndMetadata(0L), tp1, new OffsetAndMetadata(0L),
            tp2, new OffsetAndMetadata(0L));
        ListOffsetsResult listOffsetsResult = AdminClientTestUtils.createListOffsetsResult(partitionOffsets);
        when(adminClient.listOffsets(any(), any(ListOffsetsOptions.class))).thenReturn(listOffsetsResult);

        ShareGroupDescription exp = new ShareGroupDescription(
            group,
            List.of(),
            GroupState.EMPTY,
            new Node(0, "host1", 9090), 0, 0);
        DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
        when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(group, KafkaFuture.completedFuture(exp)));
        when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
        Map<String, TopicDescription> descriptions = Map.of(
            topic1, new TopicDescription(topic1, false, List.of(
                new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of()),
                new TopicPartitionInfo(1, Node.noNode(), List.of(), List.of()))
            ),
            topic2, new TopicDescription(topic2, false, List.of(
                new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of())
        )));
        DescribeTopicsResult topicsResult = mock(DescribeTopicsResult.class);
        when(topicsResult.allTopicNames()).thenReturn(completedFuture(descriptions));
        when(adminClient.describeTopics(anyCollection(), any(DescribeTopicsOptions.class))).thenReturn(topicsResult);
        when(adminClient.describeTopics(anyCollection())).thenReturn(topicsResult);
        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            service.resetOffsets();
            verify(adminClient).alterShareGroupOffsets(eq(group), anyMap(), any(AlterShareGroupOffsetsOptions.class));
            verify(adminClient, times(3)).describeTopics(anyCollection(), any(DescribeTopicsOptions.class));
            verify(alterShareGroupOffsetsResult, times(1)).all();
            verify(adminClient).describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class));
        }
    }

    @Test
    public void testAlterShareGroupToLatestSuccess() {
        String group = "share-group";
        String topic = "topic";
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--reset-offsets", "--to-latest", "--execute", "--topic", topic, "--group", group};
        Admin adminClient = mock(KafkaAdminClient.class);
        TopicPartition t1 = new TopicPartition(topic, 0);
        TopicPartition t2 = new TopicPartition(topic, 1);
        ListShareGroupOffsetsResult listShareGroupOffsetsResult = AdminClientTestUtils.createListShareGroupOffsetsResult(
            Map.of(
                group,
                KafkaFuture.completedFuture(Map.of(
                    t1, new SharePartitionOffsetInfo(10L, Optional.empty(), Optional.empty()),
                    t2, new SharePartitionOffsetInfo(10L, Optional.empty(), Optional.empty())))
            )
        );
        Map<String, TopicDescription> descriptions = Map.of(
            topic, new TopicDescription(topic, false, List.of(
                new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of()),
                new TopicPartitionInfo(1, Node.noNode(), List.of(), List.of()))
        ));
        DescribeTopicsResult describeTopicResult = mock(DescribeTopicsResult.class);
        when(describeTopicResult.allTopicNames()).thenReturn(completedFuture(descriptions));
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicResult);
        when(adminClient.describeTopics(anyCollection(), any(DescribeTopicsOptions.class))).thenReturn(describeTopicResult);
        when(adminClient.listShareGroupOffsets(any(), any(ListShareGroupOffsetsOptions.class))).thenReturn(listShareGroupOffsetsResult);

        AlterShareGroupOffsetsResult alterShareGroupOffsetsResult = mockAlterShareGroupOffsets(adminClient, group);
        Map<TopicPartition, OffsetAndMetadata> partitionOffsets = Map.of(t1, new OffsetAndMetadata(40L), t2, new OffsetAndMetadata(40L));
        ListOffsetsResult listOffsetsResult = AdminClientTestUtils.createListOffsetsResult(partitionOffsets);
        when(adminClient.listOffsets(any(), any(ListOffsetsOptions.class))).thenReturn(listOffsetsResult);

        ShareGroupDescription exp = new ShareGroupDescription(
            group,
            List.of(),
            GroupState.EMPTY,
            new Node(0, "host1", 9090), 0, 0);
        DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
        when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(group, KafkaFuture.completedFuture(exp)));
        when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
        Function<Collection<TopicPartition>, ArgumentMatcher<Map<TopicPartition, OffsetSpec>>> offsetsArgMatcher = expectedPartitions ->
            topicPartitionOffsets -> topicPartitionOffsets != null && topicPartitionOffsets.keySet().equals(expectedPartitions) &&
                topicPartitionOffsets.values().stream().allMatch(offsetSpec -> offsetSpec instanceof OffsetSpec.LatestSpec);
        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            service.resetOffsets();
            verify(adminClient).alterShareGroupOffsets(eq(group), anyMap(), any(AlterShareGroupOffsetsOptions.class));
            verify(adminClient, times(1)).listOffsets(ArgumentMatchers.argThat(offsetsArgMatcher.apply(Set.of(t1, t2))), any());
            verify(alterShareGroupOffsetsResult, times(1)).all();
            verify(adminClient).describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class));
        }
    }

    @Test
    public void testAlterShareGroupAllTopicsToDatetimeSuccess() {
        String group = "share-group";
        String topic1 = "topic1";
        String topic2 = "topic2";
        String topic3 = "topic3";
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--reset-offsets", "--to-datetime", "2025-07-20T01:20:38.198", "--execute", "--all-topics", "--group", group};
        Admin adminClient = mock(KafkaAdminClient.class);

        ListShareGroupOffsetsResult listShareGroupOffsetsResult = AdminClientTestUtils.createListShareGroupOffsetsResult(
            Map.of(
                group,
                KafkaFuture.completedFuture(Map.of(
                    new TopicPartition(topic1, 0), new SharePartitionOffsetInfo(5L, Optional.empty(), Optional.empty()),
                    new TopicPartition(topic1, 1), new SharePartitionOffsetInfo(10L, Optional.empty(), Optional.empty()),
                    new TopicPartition(topic2, 0), new SharePartitionOffsetInfo(10L, Optional.empty(), Optional.empty()),
                    new TopicPartition(topic3, 0), new SharePartitionOffsetInfo(10L, Optional.empty(), Optional.empty())))
            )
        );
        when(adminClient.listShareGroupOffsets(any(), any(ListShareGroupOffsetsOptions.class))).thenReturn(listShareGroupOffsetsResult);
        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        Set<String> topics = Set.of(topic1, topic2, topic3);
        when(listTopicsResult.names()).thenReturn(completedFuture(topics));
        when(adminClient.listTopics()).thenReturn(listTopicsResult);

        AlterShareGroupOffsetsResult alterShareGroupOffsetsResult = mockAlterShareGroupOffsets(adminClient, group);
        TopicPartition tp1 = new TopicPartition(topic1, 0);
        TopicPartition tp2 = new TopicPartition(topic1, 1);
        TopicPartition tp3 = new TopicPartition(topic2, 0);
        TopicPartition tp4 = new TopicPartition(topic3, 0);
        Map<TopicPartition, OffsetAndMetadata> partitionOffsets = Map.of(tp1, new OffsetAndMetadata(10L), tp2, new OffsetAndMetadata(15L),
            tp3, new OffsetAndMetadata(15L), tp4, new OffsetAndMetadata(15L));
        ListOffsetsResult listOffsetsResult = AdminClientTestUtils.createListOffsetsResult(partitionOffsets);
        when(adminClient.listOffsets(any(), any(ListOffsetsOptions.class))).thenReturn(listOffsetsResult);
        Map<String, TopicDescription> descriptions = Map.of(
            topic1, new TopicDescription(topic1, false, List.of(
                new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of()),
                new TopicPartitionInfo(1, Node.noNode(), List.of(), List.of())
            )),
            topic2, new TopicDescription(topic2, false, List.of(
                new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of())
            )),
            topic3, new TopicDescription(topic3, false, List.of(
                new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of()
            ))
        ));
        DescribeTopicsResult describeTopicResult = mock(DescribeTopicsResult.class);
        when(describeTopicResult.allTopicNames()).thenReturn(completedFuture(descriptions));
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicResult);
        when(adminClient.describeTopics(anyCollection(), any(DescribeTopicsOptions.class))).thenReturn(describeTopicResult);

        ShareGroupDescription exp = new ShareGroupDescription(
            group,
            List.of(),
            GroupState.EMPTY,
            new Node(0, "host1", 9090), 0, 0);
        DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
        when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(group, KafkaFuture.completedFuture(exp)));
        when(adminClient.describeShareGroups(anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
        Function<Collection<TopicPartition>, ArgumentMatcher<Map<TopicPartition, OffsetSpec>>> offsetsArgMatcher = expectedPartitions ->
            topicPartitionOffsets -> topicPartitionOffsets != null && topicPartitionOffsets.keySet().equals(expectedPartitions) &&
                topicPartitionOffsets.values().stream().allMatch(offsetSpec -> offsetSpec instanceof OffsetSpec.TimestampSpec);
        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            service.resetOffsets();
            verify(adminClient).alterShareGroupOffsets(eq(group), anyMap(), any(AlterShareGroupOffsetsOptions.class));
            verify(adminClient, times(1)).listOffsets(ArgumentMatchers.argThat(offsetsArgMatcher.apply(Set.of(tp1, tp2, tp3, tp4))), any());
            verify(alterShareGroupOffsetsResult, times(1)).all();
            verify(adminClient).describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class));
        }
    }

    @Test
    public void testResetOffsetsDryRunSuccess() {
        String group = "share-group";
        String topic = "topic";
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--reset-offsets", "--to-earliest", "--dry-run", "--topic", topic, "--group", group};
        Admin adminClient = mock(KafkaAdminClient.class);

        ListShareGroupOffsetsResult listShareGroupOffsetsResult = AdminClientTestUtils.createListShareGroupOffsetsResult(
            Map.of(
                group,
                KafkaFuture.completedFuture(Map.of(new TopicPartition(topic, 0), new SharePartitionOffsetInfo(10L, Optional.empty(), Optional.empty())))
            )
        );
        when(adminClient.listShareGroupOffsets(any(), any(ListShareGroupOffsetsOptions.class))).thenReturn(listShareGroupOffsetsResult);

        Map<TopicPartition, OffsetAndMetadata> partitionOffsets = Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(0L));
        ListOffsetsResult listOffsetsResult = AdminClientTestUtils.createListOffsetsResult(partitionOffsets);
        when(adminClient.listOffsets(any(), any(ListOffsetsOptions.class))).thenReturn(listOffsetsResult);

        AlterShareGroupOffsetsResult alterShareGroupOffsetsResult = mock(AlterShareGroupOffsetsResult.class);
        when(alterShareGroupOffsetsResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(adminClient.alterShareGroupOffsets(any(), any())).thenReturn(alterShareGroupOffsetsResult);
        Map<String, TopicDescription> descriptions = Map.of(
            topic, new TopicDescription(topic, false, List.of(
                new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of())
        )));
        DescribeTopicsResult describeTopicResult = mock(DescribeTopicsResult.class);
        when(describeTopicResult.allTopicNames()).thenReturn(completedFuture(descriptions));
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicResult);
        when(adminClient.describeTopics(anyCollection(), any(DescribeTopicsOptions.class))).thenReturn(describeTopicResult);

        ShareGroupDescription exp = new ShareGroupDescription(
            group,
            List.of(),
            GroupState.EMPTY,
            new Node(0, "host1", 9090), 0, 0);
        DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
        when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(group, KafkaFuture.completedFuture(exp)));
        when(adminClient.describeShareGroups(anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);

        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            service.resetOffsets();
            verify(adminClient, times(0)).alterShareGroupOffsets(any(), any(), any(AlterShareGroupOffsetsOptions.class));
        }
    }

    @Test
    public void testAlterShareGroupOffsetsFailureWithoutTopic() {
        String bootstrapServer = "localhost:9092";
        String group = "share-group";
        Admin adminClient = mock(KafkaAdminClient.class);
        // no group spec args
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--reset-offsets", "--to-earliest", "--execute", "--group", group};
        AtomicBoolean exited = new AtomicBoolean(false);
        Exit.setExitProcedure(((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Option [reset-offsets] takes one of these options: [all-topics], [topic]"));
            exited.set(true);
        }));
        try {
            getShareGroupService(cgcArgs, adminClient);
        } finally {
            assertTrue(exited.get());
        }
    }

    @Test
    public void testAlterShareGroupOffsetsFailureWithNonEmptyGroup() {
        String group = "share-group";
        String topic = "topic";
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--reset-offsets", "--to-earliest", "--execute", "--topic", topic, "--group", group};
        Admin adminClient = mock(KafkaAdminClient.class);

        ListShareGroupOffsetsResult listShareGroupOffsetsResult = AdminClientTestUtils.createListShareGroupOffsetsResult(
            Map.of(
                group,
                KafkaFuture.completedFuture(Map.of(new TopicPartition("topic", 0), new SharePartitionOffsetInfo(10L, Optional.empty(), Optional.empty())))
            )
        );
        when(adminClient.listShareGroupOffsets(any(), any(ListShareGroupOffsetsOptions.class))).thenReturn(listShareGroupOffsetsResult);
        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        Set<String> topics = Set.of("topic");
        when(listTopicsResult.names()).thenReturn(completedFuture(topics));
        when(adminClient.listTopics()).thenReturn(listTopicsResult);

        ShareGroupDescription exp = new ShareGroupDescription(
            group,
            List.of(new ShareMemberDescription("memid1", Optional.empty(), "clId1", "host1", new ShareMemberAssignment(
                Set.of(new TopicPartition("topic", 0))
            ), 0)),
            GroupState.STABLE,
            new Node(0, "host1", 9090), 0, 0);
        DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
        when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(group, KafkaFuture.completedFuture(exp)));
        when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);

        Exit.setExitProcedure((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Share group 'share-group' is not empty."));
            throw new IllegalArgumentException(message);
        });
        assertThrows(IllegalArgumentException.class, () -> getShareGroupService(cgcArgs, adminClient).resetOffsets());
    }

    @Test
    public void testAlterShareGroupOffsetsArgsFailureWithoutResetOffsetsArgs() {
        String bootstrapServer = "localhost:9092";
        String group = "share-group";
        Admin adminClient = mock(KafkaAdminClient.class);
        // no reset-offsets spec args
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--execute", "--reset-offsets", "--group", group, "--topic", "topic"};
        AtomicBoolean exited = new AtomicBoolean(false);
        Exit.setExitProcedure(((statusCode, message) -> {
            assertNotEquals(0, statusCode);
            assertTrue(message.contains("Option [reset-offsets] takes one of these options: [to-datetime], [to-earliest], [to-latest]"));
            exited.set(true);
        }));
        try {
            getShareGroupService(cgcArgs, adminClient);
        } finally {
            assertTrue(exited.get());
        }
    }
    
    @Test
    public void testAlterShareGroupUnsubscribedTopicSuccess() {
        String group = "share-group";
        String topic = "none";
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--reset-offsets", "--to-earliest", "--execute", "--topic", topic, "--group", group};
        Admin adminClient = mock(KafkaAdminClient.class);

        ListShareGroupOffsetsResult listShareGroupOffsetsResult = AdminClientTestUtils.createListShareGroupOffsetsResult(
            Map.of(
                group,
                KafkaFuture.completedFuture(Map.of(new TopicPartition("topic", 0), new SharePartitionOffsetInfo(10L, Optional.empty(), Optional.empty())))
            )
        );
        when(adminClient.listShareGroupOffsets(any(), any(ListShareGroupOffsetsOptions.class))).thenReturn(listShareGroupOffsetsResult);

        AlterShareGroupOffsetsResult alterShareGroupOffsetsResult = mockAlterShareGroupOffsets(adminClient, group);
        TopicPartition tp0 = new TopicPartition(topic, 0);
        Map<TopicPartition, OffsetAndMetadata> partitionOffsets = Map.of(tp0, new OffsetAndMetadata(0L));
        ListOffsetsResult listOffsetsResult = AdminClientTestUtils.createListOffsetsResult(partitionOffsets);
        when(adminClient.listOffsets(any(), any(ListOffsetsOptions.class))).thenReturn(listOffsetsResult);

        ShareGroupDescription exp = new ShareGroupDescription(
            group,
            List.of(),
            GroupState.EMPTY,
            new Node(0, "host1", 9090), 0, 0);
        DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
        when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(group, KafkaFuture.completedFuture(exp)));
        when(adminClient.describeShareGroups(any(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
        Map<String, TopicDescription> descriptions = Map.of(
            topic, new TopicDescription(topic, false, List.of(
                new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of())
        )));
        DescribeTopicsResult describeTopicResult = mock(DescribeTopicsResult.class);
        when(describeTopicResult.allTopicNames()).thenReturn(completedFuture(descriptions));
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicResult);
        when(adminClient.describeTopics(anyCollection(), any(DescribeTopicsOptions.class))).thenReturn(describeTopicResult);
        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            service.resetOffsets();
            verify(adminClient).alterShareGroupOffsets(eq(group), anyMap(), any(AlterShareGroupOffsetsOptions.class));
            verify(adminClient, times(3)).describeTopics(anyCollection(), any(DescribeTopicsOptions.class));
            verify(alterShareGroupOffsetsResult, times(1)).all();
            verify(adminClient).describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class));
        }
    }

    @Test
    public void testAlterShareGroupNonExistentGroupSuccess() {
        String group = "share-group";
        String topic = "none";
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--reset-offsets", "--to-earliest", "--execute", "--topic", topic, "--group", group};
        Admin adminClient = mock(KafkaAdminClient.class);

        ListShareGroupOffsetsResult listShareGroupOffsetsResult = AdminClientTestUtils.createListShareGroupOffsetsResult(
            Map.of(
                group,
                KafkaFuture.completedFuture(Map.of(new TopicPartition("topic", 0), new SharePartitionOffsetInfo(10L, Optional.empty(), Optional.empty())))
            )
        );
        when(adminClient.listShareGroupOffsets(any(), any(ListShareGroupOffsetsOptions.class))).thenReturn(listShareGroupOffsetsResult);

        AlterShareGroupOffsetsResult alterShareGroupOffsetsResult = mockAlterShareGroupOffsets(adminClient, group);
        TopicPartition tp0 = new TopicPartition(topic, 0);
        Map<TopicPartition, OffsetAndMetadata> partitionOffsets = Map.of(tp0, new OffsetAndMetadata(0L));
        ListOffsetsResult listOffsetsResult = AdminClientTestUtils.createListOffsetsResult(partitionOffsets);
        when(adminClient.listOffsets(any(), any(ListOffsetsOptions.class))).thenReturn(listOffsetsResult);

        KafkaFutureImpl<ShareGroupDescription> missingGroupFuture = new KafkaFutureImpl<>();
        missingGroupFuture.completeExceptionally(new GroupIdNotFoundException("Group " + group + " not found."));
        DescribeShareGroupsResult describeShareGroupsResult = mock(DescribeShareGroupsResult.class);
        when(describeShareGroupsResult.describedGroups()).thenReturn(Map.of(group, missingGroupFuture));
        when(adminClient.describeShareGroups(any(), any(DescribeShareGroupsOptions.class))).thenReturn(describeShareGroupsResult);
        Map<String, TopicDescription> descriptions = Map.of(
            topic, new TopicDescription(topic, false, List.of(
                new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of())
            )));
        DescribeTopicsResult describeTopicResult = mock(DescribeTopicsResult.class);
        when(describeTopicResult.allTopicNames()).thenReturn(completedFuture(descriptions));
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicResult);
        when(adminClient.describeTopics(anyCollection(), any(DescribeTopicsOptions.class))).thenReturn(describeTopicResult);
        try (ShareGroupService service = getShareGroupService(cgcArgs, adminClient)) {
            service.resetOffsets();
            verify(adminClient).alterShareGroupOffsets(eq(group), anyMap(), any(AlterShareGroupOffsetsOptions.class));
            verify(adminClient, times(3)).describeTopics(anyCollection(), any(DescribeTopicsOptions.class));
            verify(alterShareGroupOffsetsResult, times(1)).all();
            verify(adminClient).describeShareGroups(ArgumentMatchers.anyCollection(), any(DescribeShareGroupsOptions.class));
        }
    }

    private AlterShareGroupOffsetsResult mockAlterShareGroupOffsets(Admin client, String groupId) {
        AlterShareGroupOffsetsResult alterShareGroupOffsetsResult = mock(AlterShareGroupOffsetsResult.class);
        KafkaFutureImpl<Void> resultFuture = new KafkaFutureImpl<>();
        resultFuture.complete(null);
        when(alterShareGroupOffsetsResult.all()).thenReturn(resultFuture);
        when(client.alterShareGroupOffsets(eq(groupId), any(), any(AlterShareGroupOffsetsOptions.class))).thenReturn(alterShareGroupOffsetsResult);
        return alterShareGroupOffsetsResult;
    }

    private void mockListShareGroups(Admin client, LinkedHashMap<String, GroupState> groupIds) {
        ListGroupsResult listResult = mock(ListGroupsResult.class);
        KafkaFutureImpl<Collection<GroupListing>> listFuture = new KafkaFutureImpl<>();
        List<GroupListing> groupListings = new ArrayList<>();
        groupIds.forEach((groupId, state) -> groupListings.add(
            new GroupListing(groupId, Optional.of(GroupType.SHARE), "share", Optional.of(state))
        ));
        listFuture.complete(groupListings);
        when(listResult.all()).thenReturn(listFuture);
        when(client.listGroups(any())).thenReturn(listResult);
    }

    ShareGroupService getShareGroupService(String[] args, Admin adminClient) {
        ShareGroupCommandOptions opts = new ShareGroupCommandOptions(args);
        opts.checkArgs();
        return new ShareGroupService(opts, adminClient);
    }

    private Runnable describeGroups(ShareGroupCommand.ShareGroupService service) {
        return () -> Assertions.assertDoesNotThrow(service::describeGroups);
    }

    private Runnable deleteOffsets(ShareGroupCommand.ShareGroupService service) {
        return () -> Assertions.assertDoesNotThrow(service::deleteOffsets);
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
        return checkOffsetsArgsHeaderOutput(output, args.contains("--verbose"));
    }

    private boolean checkOffsetsArgsHeaderOutput(String output, boolean verbose) {
        List<String> expectedKeys = verbose ?
            List.of("GROUP", "TOPIC", "PARTITION", "LEADER-EPOCH", "START-OFFSET", "LAG") :
            List.of("GROUP", "TOPIC", "PARTITION", "START-OFFSET", "LAG");
        return Arrays.stream(output.trim().split("\\s+")).toList().equals(expectedKeys);
    }

    private boolean checkMembersArgsHeaderOutput(String output, boolean verbose) {
        List<String> expectedKeys = verbose ?
            List.of("GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS", "MEMBER-EPOCH", "ASSIGNMENT") :
            List.of("GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS", "ASSIGNMENT");
        return Arrays.stream(output.trim().split("\\s+")).toList().equals(expectedKeys);
    }

    private boolean checkStateArgsHeaderOutput(String output, boolean verbose) {
        List<String> expectedKeys = verbose ?
            List.of("GROUP", "COORDINATOR", "(ID)", "STATE", "GROUP-EPOCH", "ASSIGNMENT-EPOCH", "#MEMBERS") :
            List.of("GROUP", "COORDINATOR", "(ID)", "STATE", "#MEMBERS");
        return Arrays.stream(output.trim().split("\\s+")).toList().equals(expectedKeys);
    }
}
