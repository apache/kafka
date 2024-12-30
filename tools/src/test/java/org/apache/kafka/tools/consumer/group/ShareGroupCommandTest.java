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
import org.apache.kafka.clients.admin.DescribeShareGroupsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberAssignment;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.consumer.group.ShareGroupCommand.ShareGroupService;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import joptsimple.OptionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShareGroupCommandTest {

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
        ShareGroupService service = getShareGroupService(cgcArgs, adminClient);
        Set<String> expectedGroups = new HashSet<>(Arrays.asList(firstGroup, secondGroup));

        final Set[] foundGroups = new Set[]{Collections.emptySet()};
        TestUtils.waitForCondition(() -> {
            foundGroups[0] = new HashSet<>(service.listShareGroups());
            return Objects.equals(expectedGroups, foundGroups[0]);
        }, "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups[0] + ".");
        service.close();
    }

    @Test
    public void testDescribeShareGroups() throws Exception {
        String firstGroup = "group1";
        Admin adminClient = mock(KafkaAdminClient.class);
        DescribeShareGroupsResult result = mock(DescribeShareGroupsResult.class);
        Map<String, ShareGroupDescription> resultMap = new HashMap<>();
        ShareGroupDescription exp = new ShareGroupDescription(
                firstGroup,
                List.of(new ShareMemberDescription("memid1", "clId1", "host1", new ShareMemberAssignment(
                        Set.of(new TopicPartition("topic1", 0))
                ))),
                GroupState.STABLE,
                new Node(0, "host1", 9090));
        resultMap.put(firstGroup, exp);

        when(result.all()).thenReturn(KafkaFuture.completedFuture(resultMap));
        when(adminClient.describeShareGroups(ArgumentMatchers.anyCollection())).thenReturn(result);
        ShareGroupService service = new ShareGroupService(null, adminClient);
        assertEquals(exp, service.getDescribeGroup(firstGroup));
        service.close();
    }

    @Test
    public void testDescribeShareGroupsGetOffsets() throws Exception {
        Admin adminClient = mock(KafkaAdminClient.class);

        ListOffsetsResult startOffset = mock(ListOffsetsResult.class);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> startOffsetResultMap = new HashMap<>();
        startOffsetResultMap.put(new TopicPartition("topic1", 0), new ListOffsetsResult.ListOffsetsResultInfo(10, -1, Optional.empty()));

        ListOffsetsResult endOffset = mock(ListOffsetsResult.class);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsetResultMap = new HashMap<>();
        endOffsetResultMap.put(new TopicPartition("topic1", 0), new ListOffsetsResult.ListOffsetsResultInfo(30, -1, Optional.empty()));

        when(startOffset.all()).thenReturn(KafkaFuture.completedFuture(startOffsetResultMap));
        when(endOffset.all()).thenReturn(KafkaFuture.completedFuture(endOffsetResultMap));

        when(adminClient.listOffsets(ArgumentMatchers.anyMap())).thenReturn(startOffset, endOffset);

        ShareMemberDescription description = new ShareMemberDescription("", "", "",
                new ShareMemberAssignment(Set.of(new TopicPartition("topic1", 0))));
        ShareGroupService service = new ShareGroupService(null, adminClient);
        Map<TopicPartition, Long> lags = service.getOffsets(List.of(description));
        assertEquals(1, lags.size());
        assertEquals(20, lags.get(new TopicPartition("topic1", 0)));
        service.close();
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
        String[] cgcArgs = new String[]{"--frivolous-nonsense", "--bootstrap-server", bootstrapServer, "--list"};
        assertThrows(OptionException.class, () -> getShareGroupService(cgcArgs, new MockAdminClient()));
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
        ShareGroupService service = getShareGroupService(cgcArgs, adminClient);
        Set<GroupListing> expectedListing = new HashSet<>(Arrays.asList(
                new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)),
                new GroupListing(secondGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.EMPTY))));

        final Set[] foundListing = new Set[]{Collections.emptySet()};
        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listShareGroupsInStates(new HashSet<>(Arrays.asList(GroupState.values()))));
            return Objects.equals(expectedListing, foundListing[0]);
        }, "Expected to show groups " + expectedListing + ", but found " + foundListing[0]);

        ListGroupsResult resultWithStableState = mock(ListGroupsResult.class);
        when(resultWithStableState.all()).thenReturn(KafkaFuture.completedFuture(Collections.singletonList(
                new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE))
        )));
        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(resultWithStableState);
        Set<GroupListing> expectedListingStable = Collections.singleton(
                new GroupListing(firstGroup, Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)));

        foundListing[0] = Collections.emptySet();

        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listShareGroupsInStates(Collections.singleton(GroupState.STABLE)));
            return Objects.equals(expectedListingStable, foundListing[0]);
        }, "Expected to show groups " + expectedListingStable + ", but found " + foundListing[0]);
        service.close();
    }

    @Test
    public void testGroupStatesFromString() {
        Set<GroupState> result = ShareGroupCommand.groupStatesFromString("Stable");
        assertEquals(Collections.singleton(GroupState.STABLE), result);

        result = ShareGroupCommand.groupStatesFromString("stable");
        assertEquals(new HashSet<>(Collections.singletonList(GroupState.STABLE)), result);

        result = ShareGroupCommand.groupStatesFromString("dead");
        assertEquals(new HashSet<>(Collections.singletonList(GroupState.DEAD)), result);

        result = ShareGroupCommand.groupStatesFromString("empty");
        assertEquals(new HashSet<>(Collections.singletonList(GroupState.EMPTY)), result);

        assertThrows(IllegalArgumentException.class, () -> ShareGroupCommand.groupStatesFromString("assigning"));

        assertThrows(IllegalArgumentException.class, () -> ShareGroupCommand.groupStatesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ShareGroupCommand.groupStatesFromString("  bad, Stable"));

        assertThrows(IllegalArgumentException.class, () -> ShareGroupCommand.groupStatesFromString("   ,   ,"));
    }

    ShareGroupService getShareGroupService(String[] args, Admin adminClient) {
        ShareGroupCommandOptions opts = new ShareGroupCommandOptions(args);
        return new ShareGroupService(opts, adminClient);
    }
}
