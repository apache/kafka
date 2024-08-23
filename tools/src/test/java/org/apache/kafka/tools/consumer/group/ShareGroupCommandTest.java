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
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupsOptions;
import org.apache.kafka.clients.admin.ListShareGroupsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareGroupListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.ShareGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.consumer.group.ShareGroupCommand.ShareGroupService;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
        ListShareGroupsResult result = mock(ListShareGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Arrays.asList(
                new ShareGroupListing(firstGroup, Optional.of(ShareGroupState.STABLE)),
                new ShareGroupListing(secondGroup, Optional.of(ShareGroupState.EMPTY))
        )));
        when(adminClient.listShareGroups(any(ListShareGroupsOptions.class))).thenReturn(result);
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
                Collections.singletonList(new MemberDescription("memid1", "clId1", "host1", new MemberAssignment(
                        Collections.singleton(new TopicPartition("topic1", 0))
                ))),
                ShareGroupState.STABLE,
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

        MemberDescription description = new MemberDescription("", "", "",
                new MemberAssignment(Collections.singleton(new TopicPartition("topic1", 0))));
        ShareGroupService service = new ShareGroupService(null, adminClient);
        Map<TopicPartition, Long> lags = service.getOffsets(Collections.singletonList(description));
        assertEquals(1, lags.size());
        assertEquals(20, lags.get(new TopicPartition("topic1", 0)));
        service.close();
    }

    @Test
    public void testPrintEmptyGroupState() {
        assertFalse(ShareGroupService.maybePrintEmptyGroupState("group", ShareGroupState.EMPTY, 0));
        assertFalse(ShareGroupService.maybePrintEmptyGroupState("group", ShareGroupState.DEAD, 0));
        assertFalse(ShareGroupService.maybePrintEmptyGroupState("group", ShareGroupState.STABLE, 0));
        assertTrue(ShareGroupService.maybePrintEmptyGroupState("group", ShareGroupState.STABLE, 1));
        assertTrue(ShareGroupService.maybePrintEmptyGroupState("group", ShareGroupState.UNKNOWN, 1));
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
        ListShareGroupsResult resultWithAllStates = mock(ListShareGroupsResult.class);
        when(resultWithAllStates.all()).thenReturn(KafkaFuture.completedFuture(Arrays.asList(
                new ShareGroupListing(firstGroup, Optional.of(ShareGroupState.STABLE)),
                new ShareGroupListing(secondGroup, Optional.of(ShareGroupState.EMPTY))
        )));
        when(adminClient.listShareGroups(any(ListShareGroupsOptions.class))).thenReturn(resultWithAllStates);
        ShareGroupService service = getShareGroupService(cgcArgs, adminClient);
        Set<ShareGroupListing> expectedListing = new HashSet<>(Arrays.asList(
                new ShareGroupListing(firstGroup, Optional.of(ShareGroupState.STABLE)),
                new ShareGroupListing(secondGroup, Optional.of(ShareGroupState.EMPTY))));

        final Set[] foundListing = new Set[]{Collections.emptySet()};
        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listShareGroupsWithState(new HashSet<>(Arrays.asList(ShareGroupState.values()))));
            return Objects.equals(expectedListing, foundListing[0]);
        }, "Expected to show groups " + expectedListing + ", but found " + foundListing[0]);

        ListShareGroupsResult resultWithStableState = mock(ListShareGroupsResult.class);
        when(resultWithStableState.all()).thenReturn(KafkaFuture.completedFuture(Collections.singletonList(
                new ShareGroupListing(firstGroup, Optional.of(ShareGroupState.STABLE))
        )));
        when(adminClient.listShareGroups(any(ListShareGroupsOptions.class))).thenReturn(resultWithStableState);
        Set<ShareGroupListing> expectedListingStable = Collections.singleton(
                new ShareGroupListing(firstGroup, Optional.of(ShareGroupState.STABLE)));

        foundListing[0] = Collections.emptySet();

        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listShareGroupsWithState(Collections.singleton(ShareGroupState.STABLE)));
            return Objects.equals(expectedListingStable, foundListing[0]);
        }, "Expected to show groups " + expectedListingStable + ", but found " + foundListing[0]);
        service.close();
    }

    @Test
    public void testShareGroupStatesFromString() {
        Set<ShareGroupState> result = ShareGroupCommand.shareGroupStatesFromString("Stable");
        assertEquals(Collections.singleton(ShareGroupState.STABLE), result);

        result = ShareGroupCommand.shareGroupStatesFromString("stable");
        assertEquals(new HashSet<>(Collections.singletonList(ShareGroupState.STABLE)), result);

        result = ShareGroupCommand.shareGroupStatesFromString("dead");
        assertEquals(new HashSet<>(Collections.singletonList(ShareGroupState.DEAD)), result);

        result = ShareGroupCommand.shareGroupStatesFromString("empty");
        assertEquals(new HashSet<>(Collections.singletonList(ShareGroupState.EMPTY)), result);

        assertThrows(IllegalArgumentException.class, () -> ShareGroupCommand.shareGroupStatesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ShareGroupCommand.shareGroupStatesFromString("  bad, Stable"));

        assertThrows(IllegalArgumentException.class, () -> ShareGroupCommand.shareGroupStatesFromString("   ,   ,"));
    }

    ShareGroupService getShareGroupService(String[] args, Admin adminClient) {
        ShareGroupCommandOptions opts = new ShareGroupCommandOptions(args);
        return new ShareGroupService(opts, adminClient);
    }
}
