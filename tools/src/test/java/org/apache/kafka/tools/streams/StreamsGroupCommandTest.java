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
package org.apache.kafka.tools.streams;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteStreamsGroupsOptions;
import org.apache.kafka.clients.admin.DeleteStreamsGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsOptions;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsResult;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupMemberAssignment;
import org.apache.kafka.clients.admin.StreamsGroupMemberDescription;
import org.apache.kafka.clients.admin.StreamsGroupSubtopologyDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import joptsimple.OptionException;

import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamsGroupCommandTest {

    private static final Admin ADMIN_CLIENT = mock(KafkaAdminClient.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @Test
    public void testListStreamsGroups() throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";

        String[] cgcArgs = new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS, "--list"};
        ListGroupsResult result = mock(ListGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(List.of(
            new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)),
            new GroupListing(secondGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.EMPTY))
        )));
        when(ADMIN_CLIENT.listGroups(any(ListGroupsOptions.class))).thenReturn(result);
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(cgcArgs);
        Set<String> expectedGroups = Set.of(firstGroup, secondGroup);

        final Set[] foundGroups = new Set[]{Set.of()};
        TestUtils.waitForCondition(() -> {
            foundGroups[0] = new HashSet<>(service.listStreamsGroups());
            return Objects.equals(expectedGroups, foundGroups[0]);
        }, () -> "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups[0] + ".");

        service.close();
    }

    @Test
    public void testListWithUnrecognizedOption() {
        String[] cgcArgs = new String[]{"--frivolous-nonsense", "--bootstrap-server", BOOTSTRAP_SERVERS, "--list"};
        final Exception exception = assertThrows(OptionException.class, () -> getStreamsGroupService(cgcArgs));
        assertEquals("frivolous-nonsense is not a recognized option", exception.getMessage());
    }

    @Test
    public void testListStreamsGroupsWithStates() throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";

        String[] cgcArgs = new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS, "--list", "--state"};
        ListGroupsResult resultWithAllStates = mock(ListGroupsResult.class);
        when(resultWithAllStates.all()).thenReturn(KafkaFuture.completedFuture(List.of(
            new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)),
            new GroupListing(secondGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.EMPTY))
        )));
        when(ADMIN_CLIENT.listGroups(any(ListGroupsOptions.class))).thenReturn(resultWithAllStates);
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(cgcArgs);
        Set<GroupListing> expectedListing = Set.of(
            new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)),
            new GroupListing(secondGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.EMPTY)));

        final Set[] foundListing = new Set[]{Set.of()};
        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listStreamsGroupsInStates(Set.of(GroupState.values())));
            return Objects.equals(expectedListing, foundListing[0]);
        }, () -> "Expected to show groups " + expectedListing + ", but found " + foundListing[0]);

        ListGroupsResult resultWithStableState = mock(ListGroupsResult.class);
        when(resultWithStableState.all()).thenReturn(KafkaFuture.completedFuture(List.of(
            new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE))
        )));
        when(ADMIN_CLIENT.listGroups(any(ListGroupsOptions.class))).thenReturn(resultWithStableState);
        Set<GroupListing> expectedListingStable = Set.of(
            new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)));

        foundListing[0] = Set.of();

        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listStreamsGroupsInStates(Set.of(GroupState.STABLE)));
            return Objects.equals(expectedListingStable, foundListing[0]);
        }, () -> "Expected to show groups " + expectedListingStable + ", but found " + foundListing[0]);

        service.close();
    }

    @Test
    public void testDescribeStreamsGroups() throws Exception {
        String firstGroup = "foo-group";
        DescribeStreamsGroupsResult result = mock(DescribeStreamsGroupsResult.class);
        Map<String, StreamsGroupDescription> resultMap = new HashMap<>();
        StreamsGroupDescription exp = new StreamsGroupDescription(
            firstGroup,
            0,
            0,
            0,
            List.of(new StreamsGroupSubtopologyDescription("foo", List.of(), List.of(), Map.of(), Map.of())),
            List.of(),
            GroupState.STABLE,
            new Node(0, "bar", 0),
            null);
        resultMap.put(firstGroup, exp);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(resultMap));
        when(ADMIN_CLIENT.describeStreamsGroups(ArgumentMatchers.anyCollection(),  any(DescribeStreamsGroupsOptions.class))).thenReturn(result);

        StreamsGroupCommandOptions streamsGroupCommandOptions = new StreamsGroupCommandOptions(
            new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS, "--group", firstGroup, "--describe"});
        StreamsGroupCommand.StreamsGroupService service = new StreamsGroupCommand.StreamsGroupService(streamsGroupCommandOptions, ADMIN_CLIENT);

        assertEquals(exp, service.getDescribeGroup(firstGroup));

        service.close();
    }

    @Test
    public void testDescribeStreamsGroupsGetOffsets() throws Exception {
        String groupId = "group1";

        ListOffsetsResult startOffset = mock(ListOffsetsResult.class);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> startOffsetResultMap = new HashMap<>();
        startOffsetResultMap.put(new TopicPartition("topic1", 0), new ListOffsetsResult.ListOffsetsResultInfo(10, -1, Optional.empty()));

        ListOffsetsResult endOffset = mock(ListOffsetsResult.class);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsetResultMap = new HashMap<>();
        endOffsetResultMap.put(new TopicPartition("topic1", 0), new ListOffsetsResult.ListOffsetsResultInfo(30, -1, Optional.empty()));

        when(startOffset.all()).thenReturn(KafkaFuture.completedFuture(startOffsetResultMap));
        when(endOffset.all()).thenReturn(KafkaFuture.completedFuture(endOffsetResultMap));

        when(ADMIN_CLIENT.listOffsets(ArgumentMatchers.anyMap())).thenReturn(startOffset, endOffset);

        ListStreamsGroupOffsetsResult result = mock(ListStreamsGroupOffsetsResult.class);
        Map<TopicPartition, OffsetAndMetadata> committedOffsetsMap = new HashMap<>();
        committedOffsetsMap.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(12, Optional.of(0), ""));

        when(ADMIN_CLIENT.listStreamsGroupOffsets(ArgumentMatchers.anyMap())).thenReturn(result);
        when(result.partitionsToOffsetAndMetadata(ArgumentMatchers.anyString())).thenReturn(KafkaFuture.completedFuture(committedOffsetsMap));

        DescribeStreamsGroupsResult describeResult = mock(DescribeStreamsGroupsResult.class);
        StreamsGroupDescription groupDescription = mock(StreamsGroupDescription.class);
        StreamsGroupSubtopologyDescription subtopology = mock(StreamsGroupSubtopologyDescription.class);
        when(ADMIN_CLIENT.describeStreamsGroups(List.of(groupId))).thenReturn(describeResult);
        when(describeResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(groupId, groupDescription)));
        when(groupDescription.subtopologies()).thenReturn(List.of(subtopology));
        when(subtopology.sourceTopics()).thenReturn(List.of("topic1"));

        StreamsGroupMemberDescription description = new StreamsGroupMemberDescription("foo", 0, Optional.empty(),
            Optional.empty(), "bar", "baz", 0, "qux",
            Optional.empty(), Map.of(), List.of(), List.of(),
            new StreamsGroupMemberAssignment(List.of(), List.of(), List.of()), new StreamsGroupMemberAssignment(List.of(), List.of(), List.of()),
            false);
        StreamsGroupDescription x = new StreamsGroupDescription(
            groupId,
            0,
            0,
            0,
            List.of(new StreamsGroupSubtopologyDescription("id", List.of("topic1"), List.of(), Map.of(), Map.of())),
            List.of(description),
            GroupState.STABLE,
            new Node(0, "host", 0),
            null);
        StreamsGroupCommandOptions streamsGroupCommandOptions = new StreamsGroupCommandOptions(
            new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS, "--group", groupId, "--describe"});

        StreamsGroupCommand.StreamsGroupService service = new StreamsGroupCommand.StreamsGroupService(streamsGroupCommandOptions, ADMIN_CLIENT);
        Map<TopicPartition, StreamsGroupCommand.OffsetsInfo> lags = service.getOffsets(x);

        assertEquals(1, lags.size());
        assertEquals(new StreamsGroupCommand.OffsetsInfo(Optional.of(12L), Optional.of(0), 30L, 18L), lags.get(new TopicPartition("topic1", 0)));

        service.close();
    }

    @Test
    public void testPrintEmptyGroupState() {
        assertFalse(StreamsGroupCommand.StreamsGroupService.isGroupStateValid(GroupState.EMPTY, 0));
        assertFalse(StreamsGroupCommand.StreamsGroupService.isGroupStateValid(GroupState.DEAD, 0));
        assertFalse(StreamsGroupCommand.StreamsGroupService.isGroupStateValid(GroupState.STABLE, 0));
        assertTrue(StreamsGroupCommand.StreamsGroupService.isGroupStateValid(GroupState.STABLE, 1));
        assertTrue(StreamsGroupCommand.StreamsGroupService.isGroupStateValid(GroupState.UNKNOWN, 1));
    }

    @Test
    public void testGroupStatesFromString() {
        Set<GroupState> result = StreamsGroupCommand.groupStatesFromString("empty");
        assertEquals(Set.of(GroupState.EMPTY), result);
        result = StreamsGroupCommand.groupStatesFromString("EMPTY");
        assertEquals(Set.of(GroupState.EMPTY), result);

        result = StreamsGroupCommand.groupStatesFromString("notready");
        assertEquals(Set.of(GroupState.NOT_READY), result);
        result = StreamsGroupCommand.groupStatesFromString("notReady");
        assertEquals(Set.of(GroupState.NOT_READY), result);

        result = StreamsGroupCommand.groupStatesFromString("assigning");
        assertEquals(Set.of(GroupState.ASSIGNING), result);
        result = StreamsGroupCommand.groupStatesFromString("ASSIGNING");
        assertEquals(Set.of(GroupState.ASSIGNING), result);

        result = StreamsGroupCommand.groupStatesFromString("RECONCILING");
        assertEquals(Set.of(GroupState.RECONCILING), result);
        result = StreamsGroupCommand.groupStatesFromString("reconCILING");
        assertEquals(Set.of(GroupState.RECONCILING), result);

        result = StreamsGroupCommand.groupStatesFromString("STABLE");
        assertEquals(Set.of(GroupState.STABLE), result);
        result = StreamsGroupCommand.groupStatesFromString("stable");
        assertEquals(Set.of(GroupState.STABLE), result);

        result = StreamsGroupCommand.groupStatesFromString("DEAD");
        assertEquals(Set.of(GroupState.DEAD), result);
        result = StreamsGroupCommand.groupStatesFromString("dead");
        assertEquals(Set.of(GroupState.DEAD), result);

        assertThrow("preparingRebalance");
        assertThrow("completingRebalance");
        assertThrow("bad, wrong");
        assertThrow("  bad, Stable");
        assertThrow("   ,   ,");
    }

    @Test
    public void testAdminRequestsForResetOffsets() {
        Admin adminClient = mock(KafkaAdminClient.class);
        String topic = "topic1";
        String groupId = "foo-group";
        List<String> args = List.of("--bootstrap-server", "localhost:9092", "--group", groupId, "--reset-offsets", "--input-topic", "topic1", "--to-latest");
        List<String> topics = List.of(topic);

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(adminClient.describeStreamsGroups(List.of(groupId)))
            .thenReturn(describeStreamsResult(groupId, GroupState.DEAD));
        Map<String, TopicDescription> descriptions = Map.of(
            topic, new TopicDescription(topic, false, List.of(
                new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of()))
        ));
        when(adminClient.describeTopics(anyCollection()))
            .thenReturn(describeTopicsResult);
        when(adminClient.describeTopics(eq(topics), any(DescribeTopicsOptions.class)))
            .thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(completedFuture(descriptions));
        when(adminClient.listOffsets(any(), any()))
            .thenReturn(listOffsetsResult());
        ListGroupsResult listGroupsResult = listGroupResult(groupId);
        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(listGroupsResult);
        ListStreamsGroupOffsetsResult result = mock(ListStreamsGroupOffsetsResult.class);
        Map<TopicPartition, OffsetAndMetadata> committedOffsetsMap = new HashMap<>();
        committedOffsetsMap.put(new TopicPartition(topic, 0), mock(OffsetAndMetadata.class));
        when(adminClient.listStreamsGroupOffsets(ArgumentMatchers.anyMap())).thenReturn(result);
        when(result.partitionsToOffsetAndMetadata(ArgumentMatchers.anyString())).thenReturn(KafkaFuture.completedFuture(committedOffsetsMap));

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args.toArray(new String[0]), adminClient);
        Map<String, Map<TopicPartition, OffsetAndMetadata>> resetResult = service.resetOffsets();

        assertEquals(Set.of(groupId), resetResult.keySet());
        assertEquals(Set.of(new TopicPartition(topics.get(0), 0)),
            resetResult.get(groupId).keySet());

        verify(adminClient, times(1)).describeStreamsGroups(List.of(groupId));
        verify(adminClient, times(1)).describeTopics(eq(topics), any(DescribeTopicsOptions.class));
        verify(adminClient, times(1)).listOffsets(any(), any());
        verify(adminClient, times(1)).listStreamsGroupOffsets(any());

        service.close();
    }

    @Test
    public void testRetrieveInternalTopics() {
        String groupId = "foo-group";
        List<String> args = List.of("--bootstrap-server", "localhost:9092", "--group", groupId, "--delete");
        List<String> sourceTopics = List.of("source-topic1", "source-topic2");
        List<String> repartitionSinkTopics = List.of("rep-sink-topic1", "rep-sink-topic2");
        Map<String, StreamsGroupSubtopologyDescription.TopicInfo> stateChangelogTopics = Map.of(
            groupId + "-1-changelog", mock(StreamsGroupSubtopologyDescription.TopicInfo.class),
            "some-pre-fix" + "-changelog", mock(StreamsGroupSubtopologyDescription.TopicInfo.class),
            groupId + "-2-changelog", mock(StreamsGroupSubtopologyDescription.TopicInfo.class));
        Map<String, StreamsGroupSubtopologyDescription.TopicInfo> repartitionSourceTopics = Map.of(
            groupId + "-1-repartition", mock(StreamsGroupSubtopologyDescription.TopicInfo.class),
            groupId + "-some-thing", mock(StreamsGroupSubtopologyDescription.TopicInfo.class),
            groupId + "-2-repartition", mock(StreamsGroupSubtopologyDescription.TopicInfo.class));


        Map<String, StreamsGroupDescription> resultMap = new HashMap<>();
        resultMap.put(groupId, new StreamsGroupDescription(
            groupId,
            0,
            0,
            0,
            List.of(new StreamsGroupSubtopologyDescription("subtopology1", sourceTopics, repartitionSinkTopics, stateChangelogTopics, repartitionSourceTopics)),
            List.of(),
            GroupState.DEAD,
            new Node(0, "localhost", 9092),
            null));
        DescribeStreamsGroupsResult result = mock(DescribeStreamsGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(resultMap));
        when(ADMIN_CLIENT.describeStreamsGroups(ArgumentMatchers.anyCollection())).thenReturn(result);

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args.toArray(new String[0]));
        Map<String, List<String>> internalTopics = service.retrieveInternalTopics(List.of(groupId));

        assertNotNull(internalTopics.get(groupId));
        assertEquals(4, internalTopics.get(groupId).size());
        assertEquals(Set.of(groupId + "-1-changelog", groupId + "-2-changelog", groupId + "-1-repartition", groupId + "-2-repartition"),
            new HashSet<>(internalTopics.get(groupId)));
        assertFalse(internalTopics.get(groupId).stream().anyMatch(List.of("some-pre-fix-changelog", groupId + "-some-thing")::contains));
        assertFalse(internalTopics.get(groupId).stream().anyMatch(sourceTopics::contains));
        assertFalse(internalTopics.get(groupId).stream().anyMatch(repartitionSinkTopics::contains));

        service.close();
    }

    @Test
    public void testDeleteStreamsGroup() {
        Admin adminClient = mock(KafkaAdminClient.class);
        String groupId = "foo-group";
        List<String> args = List.of("--bootstrap-server", "localhost:9092", "--group", groupId, "--delete", "--delete-all-internal-topics");

        DeleteStreamsGroupsResult deleteStreamsGroupsResult = mock(DeleteStreamsGroupsResult.class);
        when(adminClient.deleteStreamsGroups(eq(List.of(groupId)), any(DeleteStreamsGroupsOptions.class))).thenReturn(deleteStreamsGroupsResult);
        when(deleteStreamsGroupsResult.deletedGroups()).thenReturn(Map.of(groupId, KafkaFuture.completedFuture(null)));
        DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
        when(deleteTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(adminClient.deleteTopics(ArgumentMatchers.anyCollection())).thenReturn(deleteTopicsResult);
        DescribeStreamsGroupsResult describeStreamsGroupsResult = mock(DescribeStreamsGroupsResult.class);
        when(describeStreamsGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(groupId, mock(StreamsGroupDescription.class))));
        when(adminClient.describeStreamsGroups(any())).thenReturn(describeStreamsGroupsResult);
        ListGroupsResult listGroupsResult = mock(ListGroupsResult.class);
        when(adminClient.listGroups(any())).thenReturn(listGroupsResult);
        when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(new GroupListing(groupId, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.EMPTY)))));

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args.toArray(new String[0]), adminClient);
        service.deleteGroups();

        verify(adminClient, times(1)).listGroups(any(ListGroupsOptions.class));
        verify(adminClient, times(1)).deleteStreamsGroups(eq(List.of(groupId)), any(DeleteStreamsGroupsOptions.class));
        verify(adminClient, times(1)).describeStreamsGroups(any());
        // because of having 0 internal topics, we do not expect deleteTopics to be called
        verify(adminClient, times(0)).deleteTopics(ArgumentMatchers.anyCollection());

        service.close();
    }

    @Test
    public void testDeleteNonStreamsGroup() {
        Admin adminClient = mock(KafkaAdminClient.class);
        String groupId = "foo-group";
        List<String> args = List.of("--bootstrap-server", "localhost:9092", "--group", groupId, "--delete");

        ListGroupsResult listGroupsResult = mock(ListGroupsResult.class);
        when(adminClient.listGroups(any())).thenReturn(listGroupsResult);
        when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of()));

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args.toArray(new String[0]), adminClient);
        Map<String, Throwable> result = service.deleteGroups();

        assertNotNull(result.get(groupId));
        assertEquals("Group '" + groupId + "' does not exist or is not a streams group.",
            result.get(groupId).getMessage());
        assertInstanceOf(IllegalArgumentException.class, result.get(groupId));
        verify(adminClient, times(1)).listGroups(any(ListGroupsOptions.class));
        // we do not expect any further API to be called
        verify(adminClient, times(0)).deleteStreamsGroups(eq(List.of(groupId)), any(DeleteStreamsGroupsOptions.class));
        verify(adminClient, times(0)).describeStreamsGroups(any());
        verify(adminClient, times(0)).deleteTopics(ArgumentMatchers.anyCollection());

        service.close();
    }
    
    @Test
    public void testResetOffsetsWithPartitionNotExist() {
        Admin adminClient = mock(KafkaAdminClient.class);
        String groupId = "foo-group";
        String topic = "topic";
        List<String> args = new ArrayList<>(Arrays.asList("--bootstrap-server", "localhost:9092", "--group", groupId, "--reset-offsets", "--input-topic", "topic:3", "--to-latest"));
        
        when(adminClient.describeStreamsGroups(List.of(groupId)))
            .thenReturn(describeStreamsResult(groupId, GroupState.DEAD));
        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);

        Map<String, TopicDescription> descriptions = Map.of(
            topic, new TopicDescription(topic, false, List.of(
                new TopicPartitionInfo(0, Node.noNode(), List.of(), List.of()))
        ));
        when(adminClient.describeTopics(anyCollection()))
            .thenReturn(describeTopicsResult);
        when(adminClient.describeTopics(eq(List.of(topic)), any(DescribeTopicsOptions.class)))
            .thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(completedFuture(descriptions));
        when(adminClient.listOffsets(any(), any()))
            .thenReturn(listOffsetsResult());
        ListStreamsGroupOffsetsResult result = mock(ListStreamsGroupOffsetsResult.class);
        Map<TopicPartition, OffsetAndMetadata> committedOffsetsMap = Map.of(
            new TopicPartition(topic, 0), 
            new OffsetAndMetadata(12, Optional.of(0), ""),
            new TopicPartition(topic, 1),
            new OffsetAndMetadata(12, Optional.of(0), "")  
        );
        
        when(adminClient.listStreamsGroupOffsets(ArgumentMatchers.anyMap())).thenReturn(result);
        when(result.partitionsToOffsetAndMetadata(ArgumentMatchers.anyString())).thenReturn(KafkaFuture.completedFuture(committedOffsetsMap));
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args.toArray(new String[0]), adminClient);
        assertThrows(UnknownTopicOrPartitionException.class, () -> service.resetOffsets());
        service.close();
    }

    @Test
    public void testExitCodeOnInvalidOption() {
        String[] args = new String[]{"--invalid-option"};
        assertEquals(1, StreamsGroupCommand.execute(args));
    }

    @Test
    public void testExitCodeOnIllegalArguments() {
        String[] args = new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS};
        assertEquals(1, StreamsGroupCommand.execute(args));
    }

    @Test
    public void testExitCodeOnExecutionException() {
        try (MockedStatic<StreamsGroupCommand> mockedStreamGroupCommand = mockStatic(StreamsGroupCommand.class)) {
            String[] args = new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS, "--list"};
            mockedStreamGroupCommand.when(() -> StreamsGroupCommand.execute(any(String[].class))).thenCallRealMethod();
            mockedStreamGroupCommand.when(() -> StreamsGroupCommand.run(any(StreamsGroupCommandOptions.class))).thenThrow(new ExecutionException("ExecutionException", new RuntimeException()));
            
            assertEquals(1, StreamsGroupCommand.execute(args));
            
            mockedStreamGroupCommand.verify(() -> StreamsGroupCommand.run(any(StreamsGroupCommandOptions.class)));
        }
    }

    @Test
    public void testExitCodeOnInterruptedException() {
        try (MockedStatic<StreamsGroupCommand> mockedStreamGroupCommand = mockStatic(StreamsGroupCommand.class)) {
            String[] args = new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS, "--list"};
            mockedStreamGroupCommand.when(() -> StreamsGroupCommand.execute(any(String[].class))).thenCallRealMethod();
            mockedStreamGroupCommand.when(() -> StreamsGroupCommand.run(any(StreamsGroupCommandOptions.class))).thenThrow(new InterruptedException("InterruptedException"));
            
            assertEquals(1, StreamsGroupCommand.execute(args));
            
            mockedStreamGroupCommand.verify(() -> StreamsGroupCommand.run(any(StreamsGroupCommandOptions.class)));
        }
    }

    private ListGroupsResult listGroupResult(String groupId) {
        ListGroupsResult listGroupsResult = mock(ListGroupsResult.class);
        when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(
            new GroupListing(groupId, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.DEAD))
        )));
        return listGroupsResult;
    }

    StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args) {
        StreamsGroupCommandOptions opts = new StreamsGroupCommandOptions(args);
        return new StreamsGroupCommand.StreamsGroupService(opts, ADMIN_CLIENT);
    }

    StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args, Admin adminClient) {
        StreamsGroupCommandOptions opts = new StreamsGroupCommandOptions(args);
        return new StreamsGroupCommand.StreamsGroupService(opts, adminClient);
    }

    private static void assertThrow(final String wrongState) {
        final Set<String> validStates = Set.of("Assigning", "Dead", "Empty", "Reconciling", "Stable", "NotReady");

        final Exception exception = assertThrows(IllegalArgumentException.class, () -> StreamsGroupCommand.groupStatesFromString(wrongState));

        assertTrue(exception.getMessage().contains(" Valid states are: "));

        final String[] exceptionMessage = exception.getMessage().split(" Valid states are: ");
        assertEquals("Invalid state list '" + wrongState + "'.", exceptionMessage[0]);
        assertEquals(Arrays.stream(exceptionMessage[1].split(","))
            .map(String::trim)
            .collect(Collectors.toSet()), validStates);
    }

    private DescribeStreamsGroupsResult describeStreamsResult(String groupId, GroupState groupState) {
        StreamsGroupMemberDescription memberDescription = new StreamsGroupMemberDescription("foo", 0, Optional.empty(),
            Optional.empty(), "bar", "baz", 0, "qux",
            Optional.empty(), Map.of(), List.of(), List.of(),
            new StreamsGroupMemberAssignment(List.of(), List.of(), List.of()), new StreamsGroupMemberAssignment(List.of(), List.of(), List.of()),
            false);
        StreamsGroupDescription description = new StreamsGroupDescription(groupId,
            0,
            0,
            0,
            List.of(new StreamsGroupSubtopologyDescription("subtopologyId", List.of(), List.of(), Map.of(), Map.of())),
            List.of(memberDescription),
            groupState,
            new Node(1, "localhost", 9092),
            Set.of());
        KafkaFutureImpl<StreamsGroupDescription> future = new KafkaFutureImpl<>();
        future.complete(description);
        return new DescribeStreamsGroupsResult(Map.of(groupId, future));
    }

    private ListOffsetsResult listOffsetsResult() {
        List<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(new TopicPartition("topic1", 0));
        ListOffsetsResult.ListOffsetsResultInfo resultInfo = new ListOffsetsResult.ListOffsetsResultInfo(100, System.currentTimeMillis(), Optional.of(1));
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures = topicPartitions.stream().collect(Collectors.toMap(
            Function.identity(),
            __ -> KafkaFuture.completedFuture(resultInfo)));
        return new ListOffsetsResult(futures);
    }
}