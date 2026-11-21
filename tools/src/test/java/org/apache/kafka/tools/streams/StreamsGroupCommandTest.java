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
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.DeleteStreamsGroupsOptions;
import org.apache.kafka.clients.admin.DeleteStreamsGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsOptions;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupMemberAssignment;
import org.apache.kafka.clients.admin.StreamsGroupMemberDescription;
import org.apache.kafka.clients.admin.StreamsGroupSubtopologyDescription;
import org.apache.kafka.clients.admin.StreamsGroupTopologyDescription;
import org.apache.kafka.clients.admin.StreamsGroupTopologyDescriptionStatus;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
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
import java.util.stream.Stream;

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
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
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
            null,
            Optional.empty(),
            StreamsGroupTopologyDescriptionStatus.NOT_REQUESTED,
            Optional.empty());
        resultMap.put(firstGroup, exp);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(resultMap));
        when(ADMIN_CLIENT.describeStreamsGroups(anyCollection(),  any(DescribeStreamsGroupsOptions.class))).thenReturn(result);

        StreamsGroupCommandOptions streamsGroupCommandOptions = new StreamsGroupCommandOptions(
            new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS, "--group", firstGroup, "--describe"});
        StreamsGroupCommand.StreamsGroupService service = new StreamsGroupCommand.StreamsGroupService(streamsGroupCommandOptions, ADMIN_CLIENT);

        assertEquals(exp, service.getDescribeGroup(firstGroup));

        service.close();
    }

    @Test
    public void testDescribeStreamsGroupsWithTopologyAvailable() throws Exception {
        String group = "foo-group";
        StreamsGroupTopologyDescription topology = new StreamsGroupTopologyDescription(
            List.of(new StreamsGroupTopologyDescription.Subtopology("0", List.of(
                new StreamsGroupTopologyDescription.Source("source", Set.of("input"), Set.of("sink"), Set.of()),
                new StreamsGroupTopologyDescription.Sink("sink", Optional.of("output"), Set.of(), Set.of("source"))))),
            List.of());
        StreamsGroupDescription exp = new StreamsGroupDescription(
            group, 0, 0, 0, List.of(), List.of(), GroupState.STABLE, new Node(0, "bar", 0), null,
            Optional.of(topology), StreamsGroupTopologyDescriptionStatus.AVAILABLE, Optional.empty());

        Admin admin = mock(KafkaAdminClient.class);
        DescribeStreamsGroupsResult result = mock(DescribeStreamsGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Map.of(group, exp)));
        ArgumentCaptor<DescribeStreamsGroupsOptions> optionsCaptor = ArgumentCaptor.forClass(DescribeStreamsGroupsOptions.class);
        when(admin.describeStreamsGroups(anyCollection(), optionsCaptor.capture())).thenReturn(result);

        StreamsGroupCommandOptions opts = new StreamsGroupCommandOptions(
            new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS, "--group", group, "--describe", "--topology"});
        StreamsGroupCommand.StreamsGroupService service = new StreamsGroupCommand.StreamsGroupService(opts, admin);

        String output = ToolsTestUtils.grabConsoleOutput(() -> {
            try {
                assertEquals(0, service.describeGroups());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(optionsCaptor.getValue().includeTopologyDescription(), "Topology description should be requested.");
        assertTrue(output.contains("Sub-topology: 0"), "Unexpected output: " + output);
        assertTrue(output.contains("Source: source (topics: [input])"), "Unexpected output: " + output);
        assertTrue(output.contains("Sink: sink (topic: output)"), "Unexpected output: " + output);
        service.close();
    }

    @ParameterizedTest
    @MethodSource("topologyDescriptionStatusErrors")
    public void testDescribeStreamsGroupsWithUnavailableTopology(
        StreamsGroupTopologyDescriptionStatus status,
        String expectedMessage
    ) throws Exception {
        String group = "foo-group";
        StreamsGroupDescription exp = new StreamsGroupDescription(
            group, 0, 0, 0, List.of(), List.of(), GroupState.STABLE, new Node(0, "bar", 0), null,
            Optional.empty(), status, Optional.empty());

        Admin admin = mock(KafkaAdminClient.class);
        DescribeStreamsGroupsResult result = mock(DescribeStreamsGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Map.of(group, exp)));
        when(admin.describeStreamsGroups(anyCollection(), any(DescribeStreamsGroupsOptions.class))).thenReturn(result);

        StreamsGroupCommandOptions opts = new StreamsGroupCommandOptions(
            new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS, "--group", group, "--describe", "--topology"});
        StreamsGroupCommand.StreamsGroupService service = new StreamsGroupCommand.StreamsGroupService(opts, admin);

        String output = ToolsTestUtils.grabConsoleOutput(() -> {
            try {
                // A missing or unavailable topology description must surface a non-zero exit code.
                assertEquals(1, service.describeGroups());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(output.contains(expectedMessage), "Unexpected output: " + output);
        service.close();
    }

    private static Stream<Arguments> topologyDescriptionStatusErrors() {
        return Stream.of(
            Arguments.of(StreamsGroupTopologyDescriptionStatus.NOT_STORED,
                "No topology description is stored for streams group 'foo-group'."),
            Arguments.of(StreamsGroupTopologyDescriptionStatus.ERROR,
                "The broker failed to fetch the topology description for streams group 'foo-group'. See the broker logs for details.")
        );
    }

    @Test
    public void testDescribeStreamsGroupsGetOffsets() throws Exception {
        String groupId = "group1";

        TopicPartition topic1 = new TopicPartition("topic1", 0);
        ListOffsetsResult startOffset = new ListOffsetsResult(Map.of(topic1, offsetFuture(10)));
        ListOffsetsResult endOffset = new ListOffsetsResult(Map.of(topic1, offsetFuture(30)));

        when(ADMIN_CLIENT.listOffsets(anyMap(), any(ListOffsetsOptions.class))).thenReturn(startOffset, endOffset);
        when(ADMIN_CLIENT.describeTopics(anyCollection(), any(DescribeTopicsOptions.class)))
            .thenReturn(AdminClientTestUtils.describeTopicsResult("topic1", topicWithPartitions("topic1", 1)));

        ListStreamsGroupOffsetsResult result = mock(ListStreamsGroupOffsetsResult.class);
        Map<TopicPartition, OffsetAndMetadata> committedOffsetsMap = new HashMap<>();
        committedOffsetsMap.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(12, Optional.of(0), ""));

        when(ADMIN_CLIENT.listStreamsGroupOffsets(anyMap(), any(ListStreamsGroupOffsetsOptions.class))).thenReturn(result);
        when(result.partitionsToOffsetAndMetadata(anyString())).thenReturn(KafkaFuture.completedFuture(committedOffsetsMap));

        DescribeStreamsGroupsResult describeResult = mock(DescribeStreamsGroupsResult.class);
        StreamsGroupDescription groupDescription = mock(StreamsGroupDescription.class);
        StreamsGroupSubtopologyDescription subtopology = mock(StreamsGroupSubtopologyDescription.class);
        when(ADMIN_CLIENT.describeStreamsGroups(eq(List.of(groupId)), any(DescribeStreamsGroupsOptions.class))).thenReturn(describeResult);
        when(describeResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(groupId, groupDescription)));
        when(groupDescription.subtopologies()).thenReturn(List.of(subtopology));
        when(subtopology.sourceTopics()).thenReturn(List.of("topic1"));

        StreamsGroupMemberDescription description = new StreamsGroupMemberDescription("foo", 0, Optional.empty(),
            Optional.empty(), "bar", "baz", 0, "qux",
            Optional.empty(), Map.of(), List.of(), List.of(),
            new StreamsGroupMemberAssignment(List.of(new StreamsGroupMemberAssignment.TaskIds("id", List.of(0))), List.of(), List.of()),
            new StreamsGroupMemberAssignment(List.of(), List.of(), List.of()),
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
            null,
            Optional.empty(),
            StreamsGroupTopologyDescriptionStatus.NOT_REQUESTED,
            Optional.empty());
        StreamsGroupCommandOptions streamsGroupCommandOptions = new StreamsGroupCommandOptions(
            new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS, "--group", groupId, "--describe"});

        StreamsGroupCommand.StreamsGroupService service = new StreamsGroupCommand.StreamsGroupService(streamsGroupCommandOptions, ADMIN_CLIENT);
        Map<TopicPartition, StreamsGroupCommand.OffsetsInfo> lags = service.getOffsets(x);

        assertEquals(1, lags.size());
        assertEquals(new StreamsGroupCommand.OffsetsInfo(Optional.of(12L), Optional.of(0), Optional.of(30L), Optional.of(18L)), lags.get(new TopicPartition("topic1", 0)));

        service.close();
    }

    @Test
    public void testDescribeStreamsGroupsGetOffsetsWithDeletedTopicAndPartition() throws Exception {
        Admin adminClient = mock(KafkaAdminClient.class);
        StreamsGroupDescription groupDescription = mockGroupWithStaleAssignment(adminClient);
        ArgumentCaptor<Map<TopicPartition, OffsetSpec>> listOffsetsCaptor = stubListOffsetsForLivePartitions(adminClient);

        try (StreamsGroupCommand.StreamsGroupService service = describeService(adminClient)) {
            Map<TopicPartition, StreamsGroupCommand.OffsetsInfo> offsets = service.getOffsets(groupDescription);

            assertEquals(Map.of(
                new TopicPartition("alive", 0), new StreamsGroupCommand.OffsetsInfo(Optional.of(12L), Optional.of(0), Optional.of(30L), Optional.of(18L)),
                new TopicPartition("alive", 1), new StreamsGroupCommand.OffsetsInfo(Optional.empty(), Optional.empty(), Optional.of(30L), Optional.of(20L)),
                new TopicPartition("recreated", 0), new StreamsGroupCommand.OffsetsInfo(Optional.empty(), Optional.empty(), Optional.of(30L), Optional.of(20L)),
                // deleted topic and missing partition: committed offset is kept, log-end offset and lag are unknown
                new TopicPartition("deleted", 0), new StreamsGroupCommand.OffsetsInfo(Optional.of(5L), Optional.of(1), Optional.empty(), Optional.empty()),
                new TopicPartition("deleted", 1), new StreamsGroupCommand.OffsetsInfo(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
                new TopicPartition("recreated", 1), new StreamsGroupCommand.OffsetsInfo(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
            ), offsets);
            // Querying a deleted topic or partition would fail or retry until the timeout, so they must be left out.
            assertEquals(2, listOffsetsCaptor.getAllValues().size());
            listOffsetsCaptor.getAllValues().forEach(spec -> assertEquals(LIVE_PARTITIONS, spec.keySet()));
        }
    }

    @Test
    public void testDescribeStreamsGroupsPrintsMissingValuesForDeletedTopicAndPartition() throws Exception {
        Admin adminClient = mock(KafkaAdminClient.class);
        mockGroupWithStaleAssignment(adminClient);
        stubListOffsetsForLivePartitions(adminClient);

        assertEquals(Set.of(
            List.of(STALE_GROUP, "alive", "0", "18"),
            List.of(STALE_GROUP, "alive", "1", "20"),
            List.of(STALE_GROUP, "recreated", "0", "20"),
            List.of(STALE_GROUP, "deleted", "0", "-"),
            List.of(STALE_GROUP, "deleted", "1", "-"),
            List.of(STALE_GROUP, "recreated", "1", "-")
        ), describeOutputRows(adminClient));

        // re-stub so the second describe again gets earliest then latest
        stubListOffsetsForLivePartitions(adminClient);
        assertEquals(Set.of(
            List.of(STALE_GROUP, "alive", "0", "12", "0", "30", "18"),
            List.of(STALE_GROUP, "alive", "1", "-", "-", "30", "20"),
            List.of(STALE_GROUP, "recreated", "0", "-", "-", "30", "20"),
            List.of(STALE_GROUP, "deleted", "0", "5", "1", "-", "-"),
            List.of(STALE_GROUP, "deleted", "1", "-", "-", "-", "-"),
            List.of(STALE_GROUP, "recreated", "1", "-", "-", "-", "-")
        ), describeOutputRows(adminClient, "--verbose"));
    }

    @Test
    public void testDescribeStreamsGroupsGetOffsetsFailsOnOtherDescribeTopicsErrors() {
        Admin adminClient = mock(KafkaAdminClient.class);
        StreamsGroupDescription groupDescription = mockGroupWithStaleAssignment(adminClient);
        // Only UnknownTopicOrPartitionException means "deleted"; any other error must still fail the describe.
        mockDescribeTopics(adminClient, Map.of(
            "alive", completedFuture(topicWithPartitions("alive", 2)),
            "deleted", failedFuture(new TopicAuthorizationException(Set.of("deleted"))),
            "recreated", completedFuture(topicWithPartitions("recreated", 1))));

        try (StreamsGroupCommand.StreamsGroupService service = describeService(adminClient)) {
            Throwable e = assertThrows(RuntimeException.class, () -> service.getOffsets(groupDescription));
            assertInstanceOf(TopicAuthorizationException.class, e.getCause().getCause());
            verify(adminClient, times(0)).listOffsets(anyMap(), any(ListOffsetsOptions.class));
        }
    }

    @Test
    public void testDescribeStreamsGroupsGetOffsetsFailsOnListOffsetsErrors() {
        Admin adminClient = mock(KafkaAdminClient.class);
        StreamsGroupDescription groupDescription = mockGroupWithStaleAssignment(adminClient);
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> failing = new HashMap<>();
        LIVE_PARTITIONS.forEach(tp -> failing.put(tp, offsetFuture(10)));
        failing.put(new TopicPartition("alive", 1), failedFuture(new TopicAuthorizationException(Set.of("alive"))));
        when(adminClient.listOffsets(anyMap(), any(ListOffsetsOptions.class))).thenReturn(new ListOffsetsResult(failing));

        try (StreamsGroupCommand.StreamsGroupService service = describeService(adminClient)) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> service.getOffsets(groupDescription));
            assertInstanceOf(TopicAuthorizationException.class, e.getCause());
        }
    }

    private static final String STALE_GROUP = "stale-group";
    private static final Set<TopicPartition> LIVE_PARTITIONS = Set.of(
        new TopicPartition("alive", 0), new TopicPartition("alive", 1), new TopicPartition("recreated", 0));

    /**
     * Mocks a streams group whose only member was killed without leaving the group, so it still holds tasks 0 and 1
     * for three source topics while its session has not timed out: "alive" still exists with both partitions,
     * "deleted" was deleted, and "recreated" was recreated with only partition 0. Shared by the tests above because
     * the deleted topic, the missing partition and the live partitions must all be handled within one describe.
     * {@code listOffsets} is not stubbed; see {@link #stubListOffsetsForLivePartitions}.
     */
    private static StreamsGroupDescription mockGroupWithStaleAssignment(Admin adminClient) {
        mockDescribeTopics(adminClient, Map.of(
            "alive", completedFuture(topicWithPartitions("alive", 2)),
            "deleted", failedFuture(new UnknownTopicOrPartitionException("This server does not host this topic-partition.")),
            "recreated", completedFuture(topicWithPartitions("recreated", 1))));

        ListStreamsGroupOffsetsResult committedResult = mock(ListStreamsGroupOffsetsResult.class);
        when(adminClient.listStreamsGroupOffsets(anyMap(), any(ListStreamsGroupOffsetsOptions.class))).thenReturn(committedResult);
        when(committedResult.partitionsToOffsetAndMetadata(anyString())).thenAnswer(invocation -> completedFuture(new HashMap<>(Map.of(
            new TopicPartition("alive", 0), new OffsetAndMetadata(12, Optional.of(0), ""),
            new TopicPartition("deleted", 0), new OffsetAndMetadata(5, Optional.of(1), "")))));

        StreamsGroupMemberDescription member = new StreamsGroupMemberDescription("foo", 0, Optional.empty(),
            Optional.empty(), "bar", "baz", 0, "qux",
            Optional.empty(), Map.of(), List.of(), List.of(),
            new StreamsGroupMemberAssignment(List.of(new StreamsGroupMemberAssignment.TaskIds("id", List.of(0, 1))), List.of(), List.of()),
            new StreamsGroupMemberAssignment(List.of(), List.of(), List.of()),
            false);
        StreamsGroupDescription groupDescription = new StreamsGroupDescription(
            STALE_GROUP,
            0,
            0,
            0,
            List.of(new StreamsGroupSubtopologyDescription("id", List.of("alive", "deleted", "recreated"), List.of(), Map.of(), Map.of())),
            List.of(member),
            GroupState.STABLE,
            new Node(0, "host", 0),
            null,
            Optional.empty(),
            StreamsGroupTopologyDescriptionStatus.NOT_REQUESTED,
            Optional.empty());
        DescribeStreamsGroupsResult describeResult = mock(DescribeStreamsGroupsResult.class);
        when(adminClient.describeStreamsGroups(eq(List.of(STALE_GROUP)), any(DescribeStreamsGroupsOptions.class))).thenReturn(describeResult);
        when(describeResult.all()).thenReturn(completedFuture(Map.of(STALE_GROUP, groupDescription)));
        return groupDescription;
    }

    /**
     * Stubs {@code listOffsets} to answer only for {@link #LIVE_PARTITIONS} (earliest 10, then latest 30), capturing
     * the requested partitions so a test can check that deleted topics and partitions were not queried.
     */
    private static ArgumentCaptor<Map<TopicPartition, OffsetSpec>> stubListOffsetsForLivePartitions(Admin adminClient) {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetSpec>> captor = ArgumentCaptor.forClass(Map.class);
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliest = new HashMap<>();
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> latest = new HashMap<>();
        LIVE_PARTITIONS.forEach(tp -> {
            earliest.put(tp, offsetFuture(10));
            latest.put(tp, offsetFuture(30));
        });
        when(adminClient.listOffsets(captor.capture(), any(ListOffsetsOptions.class)))
            .thenReturn(new ListOffsetsResult(earliest), new ListOffsetsResult(latest));
        return captor;
    }

    private static void mockDescribeTopics(Admin adminClient, Map<String, KafkaFuture<TopicDescription>> topicFutures) {
        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(describeTopicsResult.topicNameValues()).thenReturn(topicFutures);
        when(adminClient.describeTopics(anyCollection(), any(DescribeTopicsOptions.class))).thenReturn(describeTopicsResult);
    }

    private static TopicDescription topicWithPartitions(String topic, int numPartitions) {
        Node leader = new Node(0, "localhost", 9092);
        List<TopicPartitionInfo> partitions = new ArrayList<>();
        for (int p = 0; p < numPartitions; p++) {
            partitions.add(new TopicPartitionInfo(p, leader, List.of(leader), List.of(leader)));
        }
        return new TopicDescription(topic, false, partitions);
    }

    private static StreamsGroupCommand.StreamsGroupService describeService(Admin adminClient, String... extraArgs) {
        List<String> args = new ArrayList<>(List.of("--bootstrap-server", BOOTSTRAP_SERVERS, "--group", STALE_GROUP, "--describe"));
        args.addAll(List.of(extraArgs));
        return new StreamsGroupCommand.StreamsGroupService(new StreamsGroupCommandOptions(args.toArray(new String[0])), adminClient);
    }

    /** Runs {@code --describe} and returns its rows (header excluded) split into columns; row order is not defined. */
    private static Set<List<String>> describeOutputRows(Admin adminClient, String... extraArgs) {
        String output = ToolsTestUtils.grabConsoleOutput(() -> {
            try (StreamsGroupCommand.StreamsGroupService service = describeService(adminClient, extraArgs)) {
                assertEquals(0, service.describeGroups());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return Arrays.stream(output.trim().split("\n"))
            .skip(1)
            .map(line -> List.of(line.trim().split("\\s+")))
            .collect(Collectors.toSet());
    }

    private static KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> offsetFuture(long offset) {
        return completedFuture(new ListOffsetsResult.ListOffsetsResultInfo(offset, -1, Optional.empty()));
    }

    private static <T> KafkaFuture<T> failedFuture(Throwable error) {
        KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
        future.completeExceptionally(error);
        return future;
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
        when(adminClient.describeStreamsGroups(eq(List.of(groupId)), any(DescribeStreamsGroupsOptions.class)))
            .thenReturn(describeStreamsResult(groupId, GroupState.DEAD));
        Map<String, TopicDescription> descriptions = Map.of(
            topic, new TopicDescription(topic, false, List.of(
                new TopicPartitionInfo(0, new Node(0, "localhost", 9092), List.of(), List.of()))
        ));
        when(adminClient.describeTopics(anyCollection(), any(DescribeTopicsOptions.class)))
            .thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(completedFuture(descriptions));
        when(adminClient.listOffsets(anyMap(), any(ListOffsetsOptions.class)))
            .thenReturn(listOffsetsResult());
        ListGroupsResult listGroupsResult = listGroupResult(groupId);
        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(listGroupsResult);
        ListStreamsGroupOffsetsResult result = mock(ListStreamsGroupOffsetsResult.class);
        Map<TopicPartition, OffsetAndMetadata> committedOffsetsMap = new HashMap<>();
        committedOffsetsMap.put(new TopicPartition(topic, 0), mock(OffsetAndMetadata.class));
        when(adminClient.listStreamsGroupOffsets(anyMap(), any(ListStreamsGroupOffsetsOptions.class))).thenReturn(result);
        when(result.partitionsToOffsetAndMetadata(anyString())).thenReturn(KafkaFuture.completedFuture(committedOffsetsMap));

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args.toArray(new String[0]), adminClient);
        Map<String, Map<TopicPartition, OffsetAndMetadata>> resetResult = service.resetOffsets();

        assertEquals(Set.of(groupId), resetResult.keySet());
        assertEquals(Set.of(new TopicPartition(topics.get(0), 0)),
            resetResult.get(groupId).keySet());

        verify(adminClient, times(1)).describeStreamsGroups(eq(List.of(groupId)), any(DescribeStreamsGroupsOptions.class));
        verify(adminClient, times(1)).describeTopics(eq(topics), any(DescribeTopicsOptions.class));
        verify(adminClient, times(1)).listOffsets(anyMap(), any(ListOffsetsOptions.class));
        verify(adminClient, times(1)).listStreamsGroupOffsets(anyMap(), any(ListStreamsGroupOffsetsOptions.class));

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
            null,
            Optional.empty(),
            StreamsGroupTopologyDescriptionStatus.NOT_REQUESTED,
            Optional.empty()));
        DescribeStreamsGroupsResult result = mock(DescribeStreamsGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(resultMap));
        when(ADMIN_CLIENT.describeStreamsGroups(anyCollection(), any(DescribeStreamsGroupsOptions.class))).thenReturn(result);

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
        when(adminClient.deleteTopics(anyCollection(), any(DeleteTopicsOptions.class))).thenReturn(deleteTopicsResult);
        DescribeStreamsGroupsResult describeStreamsGroupsResult = mock(DescribeStreamsGroupsResult.class);
        when(describeStreamsGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(groupId, mock(StreamsGroupDescription.class))));
        when(adminClient.describeStreamsGroups(any(), any(DescribeStreamsGroupsOptions.class))).thenReturn(describeStreamsGroupsResult);
        ListGroupsResult listGroupsResult = mock(ListGroupsResult.class);
        when(adminClient.listGroups(any())).thenReturn(listGroupsResult);
        when(listGroupsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(new GroupListing(groupId, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.EMPTY)))));

        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args.toArray(new String[0]), adminClient);
        service.deleteGroups();

        verify(adminClient, times(1)).listGroups(any(ListGroupsOptions.class));
        verify(adminClient, times(1)).deleteStreamsGroups(eq(List.of(groupId)), any(DeleteStreamsGroupsOptions.class));
        verify(adminClient, times(1)).describeStreamsGroups(any(), any(DescribeStreamsGroupsOptions.class));
        // because of having 0 internal topics, we do not expect deleteTopics to be called
        verify(adminClient, times(0)).deleteTopics(anyCollection(), any(DeleteTopicsOptions.class));

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
        verify(adminClient, times(0)).describeStreamsGroups(any(), any(DescribeStreamsGroupsOptions.class));
        verify(adminClient, times(0)).deleteTopics(anyCollection(), any(DeleteTopicsOptions.class));

        service.close();
    }
    
    @Test
    public void testResetOffsetsWithPartitionNotExist() {
        Admin adminClient = mock(KafkaAdminClient.class);
        String groupId = "foo-group";
        String topic = "topic";
        List<String> args = new ArrayList<>(List.of("--bootstrap-server", "localhost:9092", "--group", groupId, "--reset-offsets", "--input-topic", "topic:3", "--to-latest"));
        
        when(adminClient.describeStreamsGroups(eq(List.of(groupId)), any(DescribeStreamsGroupsOptions.class)))
            .thenReturn(describeStreamsResult(groupId, GroupState.DEAD));
        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);

        Map<String, TopicDescription> descriptions = Map.of(
            topic, new TopicDescription(topic, false, List.of(
                new TopicPartitionInfo(0, new Node(0, "localhost", 9092), List.of(), List.of()))
        ));
        when(adminClient.describeTopics(anyCollection(), any(DescribeTopicsOptions.class)))
            .thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(completedFuture(descriptions));
        when(adminClient.listOffsets(anyMap(), any(ListOffsetsOptions.class)))
            .thenReturn(listOffsetsResult());
        ListStreamsGroupOffsetsResult result = mock(ListStreamsGroupOffsetsResult.class);
        Map<TopicPartition, OffsetAndMetadata> committedOffsetsMap = Map.of(
            new TopicPartition(topic, 0), 
            new OffsetAndMetadata(12, Optional.of(0), ""),
            new TopicPartition(topic, 1),
            new OffsetAndMetadata(12, Optional.of(0), "")  
        );
        
        when(adminClient.listStreamsGroupOffsets(anyMap(), any(ListStreamsGroupOffsetsOptions.class))).thenReturn(result);
        when(result.partitionsToOffsetAndMetadata(anyString())).thenReturn(KafkaFuture.completedFuture(committedOffsetsMap));
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(args.toArray(new String[0]), adminClient);
        assertThrows(UnknownTopicOrPartitionException.class, service::resetOffsets);
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

    @Test
    public void testGetCommittedOffsetsIncludesRepartitionTopics() {
        String groupId = "test-group";
        String sourceTopic = "source-topic";
        String repartitionTopic = "repartition-topic";
        String changelogTopic = "changelog-topic";
        String outputTopic = "output-topic";

        // Set up describe streams groups to return both source and repartition topics
        DescribeStreamsGroupsResult describeResult = mock(DescribeStreamsGroupsResult.class);
        StreamsGroupDescription groupDescription = new StreamsGroupDescription(
            groupId,
            0,
            0,
            0,
            List.of(new StreamsGroupSubtopologyDescription(
                "subtopology-0",
                List.of(sourceTopic),
                List.of(),
                Map.of(changelogTopic, mock(StreamsGroupSubtopologyDescription.TopicInfo.class)),
                Map.of(repartitionTopic, mock(StreamsGroupSubtopologyDescription.TopicInfo.class))
            )),
            List.of(),
            GroupState.STABLE,
            new Node(0, "localhost", 9092),
            null,
            Optional.empty(),
            StreamsGroupTopologyDescriptionStatus.NOT_REQUESTED,
            Optional.empty()
        );
        when(describeResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(groupId, groupDescription)));
        when(ADMIN_CLIENT.describeStreamsGroups(eq(List.of(groupId)), any(DescribeStreamsGroupsOptions.class)))
            .thenReturn(describeResult);

        // Set up list offsets to return offsets for all topics including those that should be filtered
        ListStreamsGroupOffsetsResult offsetsResult = mock(ListStreamsGroupOffsetsResult.class);
        Map<TopicPartition, OffsetAndMetadata> allOffsets = new HashMap<>();
        allOffsets.put(new TopicPartition(sourceTopic, 0), new OffsetAndMetadata(10, Optional.of(0), ""));
        allOffsets.put(new TopicPartition(repartitionTopic, 0), new OffsetAndMetadata(20, Optional.of(1), ""));
        allOffsets.put(new TopicPartition(changelogTopic, 0), new OffsetAndMetadata(30, Optional.of(2), ""));
        allOffsets.put(new TopicPartition(outputTopic, 0), new OffsetAndMetadata(40, Optional.of(3), ""));

        when(offsetsResult.partitionsToOffsetAndMetadata(groupId)).thenReturn(KafkaFuture.completedFuture(allOffsets));
        when(ADMIN_CLIENT.listStreamsGroupOffsets(anyMap(), any(ListStreamsGroupOffsetsOptions.class)))
            .thenReturn(offsetsResult);

        StreamsGroupCommandOptions opts = new StreamsGroupCommandOptions(
            new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS, "--group", groupId, "--describe"});
        StreamsGroupCommand.StreamsGroupService service = new StreamsGroupCommand.StreamsGroupService(opts, ADMIN_CLIENT);

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = service.getCommittedOffsets(groupId);

        // Should include source topic and repartition topic, but not changelog or output topics
        assertEquals(2, committedOffsets.size());
        assertTrue(committedOffsets.containsKey(new TopicPartition(sourceTopic, 0)));
        assertTrue(committedOffsets.containsKey(new TopicPartition(repartitionTopic, 0)));
        assertFalse(committedOffsets.containsKey(new TopicPartition(changelogTopic, 0)));
        assertFalse(committedOffsets.containsKey(new TopicPartition(outputTopic, 0)));

        assertEquals(10, committedOffsets.get(new TopicPartition(sourceTopic, 0)).offset());
        assertEquals(20, committedOffsets.get(new TopicPartition(repartitionTopic, 0)).offset());

        service.close();
    }

    @Test
    public void testGetCommittedOffsetsWithMultipleSubtopologies() {
        String groupId = "multi-subtopology-group";
        String source1 = "source-1";
        String source2 = "source-2";
        String repartition1 = "repartition-1";
        String repartition2 = "repartition-2";

        // Set up describe streams groups with multiple subtopologies
        DescribeStreamsGroupsResult describeResult = mock(DescribeStreamsGroupsResult.class);
        StreamsGroupDescription groupDescription = new StreamsGroupDescription(
            groupId,
            0,
            0,
            0,
            List.of(
                new StreamsGroupSubtopologyDescription(
                    "subtopology-0",
                    List.of(source1),
                    List.of(),
                    Map.of(),
                    Map.of(repartition1, mock(StreamsGroupSubtopologyDescription.TopicInfo.class))
                ),
                new StreamsGroupSubtopologyDescription(
                    "subtopology-1",
                    List.of(source2),
                    List.of(),
                    Map.of(),
                    Map.of(repartition2, mock(StreamsGroupSubtopologyDescription.TopicInfo.class))
                )
            ),
            List.of(),
            GroupState.STABLE,
            new Node(0, "localhost", 9092),
            null,
            Optional.empty(),
            StreamsGroupTopologyDescriptionStatus.NOT_REQUESTED,
            Optional.empty()
        );
        when(describeResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(groupId, groupDescription)));
        when(ADMIN_CLIENT.describeStreamsGroups(eq(List.of(groupId)), any(DescribeStreamsGroupsOptions.class)))
            .thenReturn(describeResult);

        // Set up list offsets to return offsets for all source and repartition topics
        ListStreamsGroupOffsetsResult offsetsResult = mock(ListStreamsGroupOffsetsResult.class);
        Map<TopicPartition, OffsetAndMetadata> allOffsets = new HashMap<>();
        allOffsets.put(new TopicPartition(source1, 0), new OffsetAndMetadata(10, Optional.of(0), ""));
        allOffsets.put(new TopicPartition(source2, 0), new OffsetAndMetadata(20, Optional.of(1), ""));
        allOffsets.put(new TopicPartition(repartition1, 0), new OffsetAndMetadata(30, Optional.of(2), ""));
        allOffsets.put(new TopicPartition(repartition2, 0), new OffsetAndMetadata(40, Optional.of(3), ""));

        when(offsetsResult.partitionsToOffsetAndMetadata(groupId)).thenReturn(KafkaFuture.completedFuture(allOffsets));
        when(ADMIN_CLIENT.listStreamsGroupOffsets(anyMap(), any(ListStreamsGroupOffsetsOptions.class)))
            .thenReturn(offsetsResult);

        StreamsGroupCommandOptions opts = new StreamsGroupCommandOptions(
            new String[]{"--bootstrap-server", BOOTSTRAP_SERVERS, "--group", groupId, "--describe"});
        StreamsGroupCommand.StreamsGroupService service = new StreamsGroupCommand.StreamsGroupService(opts, ADMIN_CLIENT);

        Map<TopicPartition, OffsetAndMetadata> committedOffsets = service.getCommittedOffsets(groupId);

        // Should include all source topics and repartition topics from both subtopologies
        assertEquals(4, committedOffsets.size());
        assertTrue(committedOffsets.containsKey(new TopicPartition(source1, 0)));
        assertTrue(committedOffsets.containsKey(new TopicPartition(source2, 0)));
        assertTrue(committedOffsets.containsKey(new TopicPartition(repartition1, 0)));
        assertTrue(committedOffsets.containsKey(new TopicPartition(repartition2, 0)));

        service.close();
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
        assertEquals(validStates, Arrays.stream(exceptionMessage[1].split(","))
            .map(String::trim)
            .collect(Collectors.toSet()));
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
            Set.of(),
            Optional.empty(),
            StreamsGroupTopologyDescriptionStatus.NOT_REQUESTED,
            Optional.empty());
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