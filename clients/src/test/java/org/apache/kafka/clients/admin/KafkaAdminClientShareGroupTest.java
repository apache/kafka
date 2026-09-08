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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AlterShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse;
import org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.DeleteShareGroupOffsetsResponse;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest;
import org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.ShareGroupDescribeResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaAdminClientShareGroupTest extends KafkaAdminClientTestBase {

    @Test
    public void testDescribeShareGroups() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            ShareGroupDescribeResponseData data = new ShareGroupDescribeResponseData();

            // Retriable errors should be retried
            data.groups().add(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code()));
            env.kafkaClient().prepareResponse(new ShareGroupDescribeResponse(data));

            /*
             * We need to return two responses here, one with NOT_COORDINATOR error when calling describe share group
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for COORDINATOR_NOT_AVAILABLE error response
             */
            data = new ShareGroupDescribeResponseData();
            data.groups().add(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.NOT_COORDINATOR.code()));
            env.kafkaClient().prepareResponse(new ShareGroupDescribeResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            data = new ShareGroupDescribeResponseData();
            data.groups().add(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()));
            env.kafkaClient().prepareResponse(new ShareGroupDescribeResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            data = new ShareGroupDescribeResponseData();
            ShareGroupDescribeResponseData.TopicPartitions topicPartitions = new ShareGroupDescribeResponseData.TopicPartitions()
                .setTopicName("my_topic")
                .setPartitions(asList(0, 1, 2));
            ShareGroupDescribeResponseData.Assignment memberAssignment = new ShareGroupDescribeResponseData.Assignment()
                .setTopicPartitions(asList(topicPartitions));
            ShareGroupDescribeResponseData.Member memberOne = new ShareGroupDescribeResponseData.Member()
                .setMemberId("0")
                .setClientId("clientId0")
                .setClientHost("clientHost")
                .setAssignment(memberAssignment);
            ShareGroupDescribeResponseData.Member memberTwo = new ShareGroupDescribeResponseData.Member()
                .setMemberId("1")
                .setClientId("clientId1")
                .setClientHost("clientHost")
                .setAssignment(memberAssignment);

            ShareGroupDescribeResponseData group0Data = new ShareGroupDescribeResponseData();
            group0Data.groups().add(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setGroupState(GroupState.STABLE.toString())
                .setMembers(asList(memberOne, memberTwo)));

            final List<TopicPartition> expectedTopicPartitions = new ArrayList<>();
            expectedTopicPartitions.add(0, new TopicPartition("my_topic", 0));
            expectedTopicPartitions.add(1, new TopicPartition("my_topic", 1));
            expectedTopicPartitions.add(2, new TopicPartition("my_topic", 2));

            List<ShareMemberDescription> expectedMemberDescriptions = new ArrayList<>();
            expectedMemberDescriptions.add(convertToShareMemberDescriptions(memberOne,
                new ShareMemberAssignment(new HashSet<>(expectedTopicPartitions))));
            expectedMemberDescriptions.add(convertToShareMemberDescriptions(memberTwo,
                new ShareMemberAssignment(new HashSet<>(expectedTopicPartitions))));
            data.groups().add(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setGroupState(GroupState.STABLE.toString())
                .setMembers(asList(memberOne, memberTwo)));

            env.kafkaClient().prepareResponse(new ShareGroupDescribeResponse(data));

            final DescribeShareGroupsResult result = env.adminClient().describeShareGroups(singletonList(GROUP_ID));
            final ShareGroupDescription groupDescription = result.describedGroups().get(GROUP_ID).get();

            assertEquals(1, result.describedGroups().size());
            assertEquals(GROUP_ID, groupDescription.groupId());
            assertEquals(2, groupDescription.members().size());
            assertEquals(expectedMemberDescriptions, groupDescription.members());
        }
    }

    @Test
    public void testDescribeShareGroupsGroupIdNotFound() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(asList(
                        FindCoordinatorResponse.prepareCoordinatorResponse(Errors.NONE, GROUP_ID, env.cluster().controller()),
                        FindCoordinatorResponse.prepareCoordinatorResponse(Errors.NONE, "group-1", env.cluster().controller())
                    ))
            ));

            ShareGroupDescribeResponseData.TopicPartitions topicPartitions = new ShareGroupDescribeResponseData.TopicPartitions()
                .setTopicName("my_topic")
                .setPartitions(asList(0, 1, 2));
            final ShareGroupDescribeResponseData.Assignment memberAssignment = new ShareGroupDescribeResponseData.Assignment()
                .setTopicPartitions(asList(topicPartitions));
            ShareGroupDescribeResponseData groupData = new ShareGroupDescribeResponseData();
            groupData.groups().add(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setGroupState(GroupState.STABLE.toString())
                .setMembers(asList(
                    new ShareGroupDescribeResponseData.Member()
                        .setMemberId("0")
                        .setClientId("clientId0")
                        .setClientHost("clientHost")
                        .setAssignment(memberAssignment),
                    new ShareGroupDescribeResponseData.Member()
                        .setMemberId("1")
                        .setClientId("clientId1")
                        .setClientHost("clientHost")
                        .setAssignment(memberAssignment))));
            groupData.groups().add(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId("group-1")
                .setGroupState(GroupState.DEAD.toString())
                .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                .setErrorMessage("Group group-1 not found."));

            env.kafkaClient().prepareResponse(new ShareGroupDescribeResponse(groupData));

            Collection<String> groups = new HashSet<>();
            groups.add(GROUP_ID);
            groups.add("group-1");
            final DescribeShareGroupsResult result = env.adminClient().describeShareGroups(groups);
            assertEquals(2, result.describedGroups().size());
            assertEquals(groups, result.describedGroups().keySet());
            KafkaFuture<Map<String, ShareGroupDescription>> allFuture = result.all();
            assertThrows(ExecutionException.class, allFuture::get);
            assertTrue(result.all().isCompletedExceptionally());
        }
    }

    @Test
    public void testDescribeShareGroupsWithAuthorizedOperationsOmitted() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            ShareGroupDescribeResponseData data = new ShareGroupDescribeResponseData();

            data.groups().add(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setAuthorizedOperations(MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED));

            env.kafkaClient().prepareResponse(new ShareGroupDescribeResponse(data));

            final DescribeShareGroupsResult result = env.adminClient().describeShareGroups(singletonList(GROUP_ID));
            final ShareGroupDescription groupDescription = result.describedGroups().get(GROUP_ID).get();

            assertNull(groupDescription.authorizedOperations());
        }
    }

    @Test
    public void testDescribeMultipleShareGroups() {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(asList(
                        FindCoordinatorResponse.prepareCoordinatorResponse(Errors.NONE, GROUP_ID, env.cluster().controller()),
                        FindCoordinatorResponse.prepareCoordinatorResponse(Errors.NONE, "group-1", env.cluster().controller())
                    ))
            ));

            ShareGroupDescribeResponseData.TopicPartitions topicPartitions = new ShareGroupDescribeResponseData.TopicPartitions()
                .setTopicName("my_topic")
                .setPartitions(asList(0, 1, 2));
            final ShareGroupDescribeResponseData.Assignment memberAssignment = new ShareGroupDescribeResponseData.Assignment()
                .setTopicPartitions(asList(topicPartitions));
            ShareGroupDescribeResponseData groupData = new ShareGroupDescribeResponseData();
            groupData.groups().add(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setGroupState(GroupState.STABLE.toString())
                .setMembers(asList(
                    new ShareGroupDescribeResponseData.Member()
                        .setMemberId("0")
                        .setClientId("clientId0")
                        .setClientHost("clientHost")
                        .setAssignment(memberAssignment),
                    new ShareGroupDescribeResponseData.Member()
                        .setMemberId("1")
                        .setClientId("clientId1")
                        .setClientHost("clientHost")
                        .setAssignment(memberAssignment))));
            groupData.groups().add(new ShareGroupDescribeResponseData.DescribedGroup()
                .setGroupId("group-1")
                .setGroupState(GroupState.STABLE.toString())
                .setMembers(asList(
                    new ShareGroupDescribeResponseData.Member()
                        .setMemberId("0")
                        .setClientId("clientId0")
                        .setClientHost("clientHost")
                        .setAssignment(memberAssignment),
                    new ShareGroupDescribeResponseData.Member()
                        .setMemberId("1")
                        .setClientId("clientId1")
                        .setClientHost("clientHost")
                        .setAssignment(memberAssignment))));

            env.kafkaClient().prepareResponse(new ShareGroupDescribeResponse(groupData));

            Collection<String> groups = new HashSet<>();
            groups.add(GROUP_ID);
            groups.add("group-1");
            final DescribeShareGroupsResult result = env.adminClient().describeShareGroups(groups);
            assertEquals(2, result.describedGroups().size());
            assertEquals(groups, result.describedGroups().keySet());
            KafkaFuture<Map<String, ShareGroupDescription>> allFuture = result.all();
            assertDoesNotThrow(() -> allFuture.get());
            assertFalse(allFuture.isCompletedExceptionally());
        }
    }

    @Test
    public void testListShareGroups() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(4, 0),
                AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Empty metadata response should be retried
            env.kafkaClient().prepareResponse(
                RequestTestUtils.metadataResponse(
                    Collections.emptyList(),
                    env.cluster().clusterResource().clusterId(),
                    -1,
                    Collections.emptyList()));

            env.kafkaClient().prepareResponse(
                RequestTestUtils.metadataResponse(
                    env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(),
                    env.cluster().controller().id(),
                    Collections.emptyList()));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGroups(Arrays.asList(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("share-group-1")
                                .setGroupType(GroupType.SHARE.toString())
                                .setGroupState("Stable")
                            ))),
                env.cluster().nodeById(0));

            // handle retriable errors
            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                        .setGroups(Collections.emptyList())
                ),
                env.cluster().nodeById(1));
            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                        .setGroups(Collections.emptyList())
                ),
                env.cluster().nodeById(1));
            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGroups(Arrays.asList(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("share-group-2")
                                .setGroupType(GroupType.SHARE.toString())
                                .setGroupState("Stable"),
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("share-group-3")
                                .setGroupType(GroupType.SHARE.toString())
                                .setGroupState("Stable")
                        ))),
                env.cluster().nodeById(1));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGroups(Arrays.asList(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("share-group-4")
                                .setGroupType(GroupType.SHARE.toString())
                                .setGroupState("Stable")
                        ))),
                env.cluster().nodeById(2));

            // fatal error
            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setGroups(Collections.emptyList())),
                env.cluster().nodeById(3));

            final ListGroupsResult result = env.adminClient().listGroups(ListGroupsOptions.forShareGroups());
            TestUtils.assertFutureThrows(UnknownServerException.class, result.all());

            Collection<GroupListing> listings = result.valid().get();
            assertEquals(4, listings.size());

            Set<String> groupIds = new HashSet<>();
            for (GroupListing listing : listings) {
                groupIds.add(listing.groupId());
                assertTrue(listing.groupState().isPresent());
            }

            assertEquals(Set.of("share-group-1", "share-group-2", "share-group-3", "share-group-4"), groupIds);
            assertEquals(1, result.errors().get().size());
        }
    }

    @Test
    public void testListShareGroupsMetadataFailure() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Empty metadata causes the request to fail since we have no list of brokers
            // to send the ListGroups requests to
            env.kafkaClient().prepareResponse(
                RequestTestUtils.metadataResponse(
                    Collections.emptyList(),
                    env.cluster().clusterResource().clusterId(),
                    -1,
                    Collections.emptyList()));

            final ListGroupsResult result = env.adminClient().listGroups(ListGroupsOptions.forShareGroups());
            TestUtils.assertFutureThrows(KafkaException.class, result.all());
        }
    }

    @Test
    public void testListShareGroupsWithStates() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(new ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGroups(Arrays.asList(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("share-group-1")
                                .setGroupType(GroupType.SHARE.toString())
                                .setProtocolType("share")
                                .setGroupState("Stable"),
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("share-group-2")
                                .setGroupType(GroupType.SHARE.toString())
                                .setProtocolType("share")
                                .setGroupState("Empty")))),
                    env.cluster().nodeById(0));

            final ListGroupsResult result = env.adminClient().listGroups(ListGroupsOptions.forShareGroups());
            Collection<GroupListing> listings = result.valid().get();

            assertEquals(2, listings.size());
            List<GroupListing> expected = new ArrayList<>();
            expected.add(new GroupListing("share-group-1", Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.STABLE)));
            expected.add(new GroupListing("share-group-2", Optional.of(GroupType.SHARE), "share", Optional.of(GroupState.EMPTY)));
            assertEquals(expected, listings);
            assertEquals(0, result.errors().get().size());
        }
    }

    @Test
    public void testListShareGroupsWithStatesOlderBrokerVersion() {
        ApiVersion listGroupV4 = new ApiVersion()
            .setApiKey(ApiKeys.LIST_GROUPS.id)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 4);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(Collections.singletonList(listGroupV4)));

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            // Check we should not be able to list share groups with broker having version < 5
            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(Collections.singletonList(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("share-group-1")))),
                env.cluster().nodeById(0));
            ListGroupsResult result = env.adminClient().listGroups(ListGroupsOptions.forShareGroups());
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.all());
        }
    }

    @Test
    public void testListShareGroupOffsetsOptionsWithBatchedApi() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final List<TopicPartition> partitions = Collections.singletonList(new TopicPartition("A", 0));
            final ListShareGroupOffsetsOptions options = new ListShareGroupOffsetsOptions();

            final ListShareGroupOffsetsSpec groupSpec = new ListShareGroupOffsetsSpec()
                .topicPartitions(partitions);
            Map<String, ListShareGroupOffsetsSpec> groupSpecs = new HashMap<>();
            groupSpecs.put(GROUP_ID, groupSpec);

            env.adminClient().listShareGroupOffsets(groupSpecs, options);

            final MockClient mockClient = env.kafkaClient();
            waitForRequest(mockClient, ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS);

            ClientRequest clientRequest = mockClient.requests().peek();
            assertNotNull(clientRequest);
            DescribeShareGroupOffsetsRequestData data = ((DescribeShareGroupOffsetsRequest.Builder) clientRequest.requestBuilder()).build().data();
            assertEquals(1, data.groups().size());
            assertEquals(GROUP_ID, data.groups().get(0).groupId());
            assertEquals(Collections.singletonList("A"),
                data.groups().get(0).topics().stream().map(DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic::topicName).collect(Collectors.toList()));
        }
    }

    @Test
    public void testListShareGroupOffsets() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);
            TopicPartition myTopicPartition3 = new TopicPartition("my_topic", 3);
            TopicPartition myTopicPartition4 = new TopicPartition("my_topic_1", 4);
            TopicPartition myTopicPartition5 = new TopicPartition("my_topic_2", 6);

            ListShareGroupOffsetsSpec groupSpec = new ListShareGroupOffsetsSpec();
            Map<String, ListShareGroupOffsetsSpec> groupSpecs = new HashMap<>();
            groupSpecs.put(GROUP_ID, groupSpec);

            DescribeShareGroupOffsetsResponseData data = new DescribeShareGroupOffsetsResponseData().setGroups(
                List.of(
                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup().setGroupId(GROUP_ID).setTopics(
                        List.of(
                            new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic().setTopicName("my_topic").setPartitions(
                                List.of(
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(0).setStartOffset(10).setLeaderEpoch(0),
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(1).setStartOffset(11).setLeaderEpoch(0),
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(2).setStartOffset(40).setLeaderEpoch(0),
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(3).setStartOffset(50).setLeaderEpoch(1)
                                )
                            ),
                            new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic().setTopicName("my_topic_1").setPartitions(
                                List.of(
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(4).setStartOffset(100).setLeaderEpoch(2)
                                )
                            ),
                            new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic().setTopicName("my_topic_2").setPartitions(
                                List.of(
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(6).setStartOffset(500).setLeaderEpoch(3)
                                )
                            )
                        )
                    )
                )
            );
            env.kafkaClient().prepareResponse(new DescribeShareGroupOffsetsResponse(data));

            final ListShareGroupOffsetsResult result = env.adminClient().listShareGroupOffsets(groupSpecs);
            final Map<TopicPartition, SharePartitionOffsetInfo> partitionToOffsetInfo = result.partitionsToOffsetInfo(GROUP_ID).get();

            assertEquals(6, partitionToOffsetInfo.size());
            assertEquals(new SharePartitionOffsetInfo(10, Optional.of(0), Optional.empty()), partitionToOffsetInfo.get(myTopicPartition0));
            assertEquals(new SharePartitionOffsetInfo(11, Optional.of(0), Optional.empty()), partitionToOffsetInfo.get(myTopicPartition1));
            assertEquals(new SharePartitionOffsetInfo(40, Optional.of(0), Optional.empty()), partitionToOffsetInfo.get(myTopicPartition2));
            assertEquals(new SharePartitionOffsetInfo(50, Optional.of(1), Optional.empty()), partitionToOffsetInfo.get(myTopicPartition3));
            assertEquals(new SharePartitionOffsetInfo(100, Optional.of(2), Optional.empty()), partitionToOffsetInfo.get(myTopicPartition4));
            assertEquals(new SharePartitionOffsetInfo(500, Optional.of(3), Optional.empty()), partitionToOffsetInfo.get(myTopicPartition5));
        }
    }

    @Test
    public void testListShareGroupOffsetsMultipleGroups() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareBatchedFindCoordinatorResponse(Errors.NONE, env.cluster().controller(), Set.of(GROUP_ID, "group-1")));

            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);
            TopicPartition myTopicPartition3 = new TopicPartition("my_topic", 3);
            TopicPartition myTopicPartition4 = new TopicPartition("my_topic_1", 4);
            TopicPartition myTopicPartition5 = new TopicPartition("my_topic_2", 6);

            ListShareGroupOffsetsSpec group0Specs = new ListShareGroupOffsetsSpec().topicPartitions(
                List.of(myTopicPartition0, myTopicPartition1, myTopicPartition2, myTopicPartition3)
            );
            ListShareGroupOffsetsSpec group1Specs = new ListShareGroupOffsetsSpec().topicPartitions(
                List.of(myTopicPartition4, myTopicPartition5)
            );
            Map<String, ListShareGroupOffsetsSpec> groupSpecs = new HashMap<>();
            groupSpecs.put(GROUP_ID, group0Specs);
            groupSpecs.put("group-1", group1Specs);

            DescribeShareGroupOffsetsResponseData data = new DescribeShareGroupOffsetsResponseData().setGroups(
                List.of(
                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup().setGroupId(GROUP_ID).setTopics(
                        List.of(
                            new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic().setTopicName("my_topic").setPartitions(
                                List.of(
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(0).setStartOffset(10).setLeaderEpoch(0),
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(1).setStartOffset(11).setLeaderEpoch(0),
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(2).setStartOffset(40).setLeaderEpoch(0),
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(3).setStartOffset(50).setLeaderEpoch(1)
                                )
                            )
                        )
                    ),
                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup().setGroupId("group-1").setTopics(
                        List.of(
                            new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic().setTopicName("my_topic_1").setPartitions(
                                List.of(
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(4).setStartOffset(100).setLeaderEpoch(2)
                                )
                            ),
                            new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic().setTopicName("my_topic_2").setPartitions(
                                List.of(
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(6).setStartOffset(500).setLeaderEpoch(2)
                                )
                            )
                        )
                    )
                )
            );
            env.kafkaClient().prepareResponse(new DescribeShareGroupOffsetsResponse(data));

            final ListShareGroupOffsetsResult result = env.adminClient().listShareGroupOffsets(groupSpecs);
            assertEquals(2, result.all().get().size());

            final Map<TopicPartition, SharePartitionOffsetInfo> partitionToOffsetInfoGroup0 = result.partitionsToOffsetInfo(GROUP_ID).get();
            assertEquals(4, partitionToOffsetInfoGroup0.size());
            assertEquals(new SharePartitionOffsetInfo(10, Optional.of(0), Optional.empty()), partitionToOffsetInfoGroup0.get(myTopicPartition0));
            assertEquals(new SharePartitionOffsetInfo(11, Optional.of(0), Optional.empty()), partitionToOffsetInfoGroup0.get(myTopicPartition1));
            assertEquals(new SharePartitionOffsetInfo(40, Optional.of(0), Optional.empty()), partitionToOffsetInfoGroup0.get(myTopicPartition2));
            assertEquals(new SharePartitionOffsetInfo(50, Optional.of(1), Optional.empty()), partitionToOffsetInfoGroup0.get(myTopicPartition3));

            final Map<TopicPartition, SharePartitionOffsetInfo> partitionToOffsetInfoGroup1 = result.partitionsToOffsetInfo("group-1").get();
            assertEquals(2, partitionToOffsetInfoGroup1.size());
            assertEquals(new SharePartitionOffsetInfo(100, Optional.of(2), Optional.empty()), partitionToOffsetInfoGroup1.get(myTopicPartition4));
            assertEquals(new SharePartitionOffsetInfo(500, Optional.of(2), Optional.empty()), partitionToOffsetInfoGroup1.get(myTopicPartition5));
        }
    }

    @Test
    public void testListShareGroupOffsetsEmpty() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            ListShareGroupOffsetsSpec groupSpec = new ListShareGroupOffsetsSpec();
            Map<String, ListShareGroupOffsetsSpec> groupSpecs = new HashMap<>();
            groupSpecs.put(GROUP_ID, groupSpec);

            DescribeShareGroupOffsetsResponseData data = new DescribeShareGroupOffsetsResponseData().setGroups(
                List.of(
                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup().setGroupId(GROUP_ID)
                )
            );
            env.kafkaClient().prepareResponse(new DescribeShareGroupOffsetsResponse(data));

            final ListShareGroupOffsetsResult result = env.adminClient().listShareGroupOffsets(groupSpecs);
            final Map<TopicPartition, SharePartitionOffsetInfo> partitionToOffsetInfo = result.partitionsToOffsetInfo(GROUP_ID).get();

            assertEquals(0, partitionToOffsetInfo.size());
        }
    }

    @Test
    public void testListShareGroupOffsetsWithErrorInOnePartition() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic_1", 4);
            TopicPartition myTopicPartition3 = new TopicPartition("my_topic_2", 6);

            ListShareGroupOffsetsSpec groupSpec = new ListShareGroupOffsetsSpec().topicPartitions(
                List.of(myTopicPartition0, myTopicPartition1, myTopicPartition2, myTopicPartition3)
            );
            Map<String, ListShareGroupOffsetsSpec> groupSpecs = new HashMap<>();
            groupSpecs.put(GROUP_ID, groupSpec);

            DescribeShareGroupOffsetsResponseData data = new DescribeShareGroupOffsetsResponseData().setGroups(
                List.of(
                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup().setGroupId(GROUP_ID).setTopics(
                        List.of(
                            new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic().setTopicName("my_topic").setPartitions(
                                List.of(
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(0).setStartOffset(10).setLeaderEpoch(0),
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(1).setStartOffset(11).setLeaderEpoch(1)
                                )
                            ),
                            new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic().setTopicName("my_topic_1").setPartitions(
                                List.of(
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(4).setErrorCode(Errors.NOT_COORDINATOR.code()).setErrorMessage("Not a Coordinator")
                                )
                            ),
                            new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic().setTopicName("my_topic_2").setPartitions(
                                List.of(
                                    new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition().setPartitionIndex(6).setStartOffset(500).setLeaderEpoch(2)
                                )
                            )
                        )
                    )
                )
            );
            env.kafkaClient().prepareResponse(new DescribeShareGroupOffsetsResponse(data));

            final ListShareGroupOffsetsResult result = env.adminClient().listShareGroupOffsets(groupSpecs);
            final Map<TopicPartition, SharePartitionOffsetInfo> partitionToOffsetInfo = result.partitionsToOffsetInfo(GROUP_ID).get();

            // For myTopicPartition2 we have set an error as the response. Thus, it should be skipped from the final result
            assertEquals(3, partitionToOffsetInfo.size());
            assertEquals(new SharePartitionOffsetInfo(10, Optional.of(0), Optional.empty()), partitionToOffsetInfo.get(myTopicPartition0));
            assertEquals(new SharePartitionOffsetInfo(11, Optional.of(1), Optional.empty()), partitionToOffsetInfo.get(myTopicPartition1));
            assertEquals(new SharePartitionOffsetInfo(500, Optional.of(2), Optional.empty()), partitionToOffsetInfo.get(myTopicPartition3));
        }
    }

    @Test
    public void testAlterShareGroupOffsets() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            AlterShareGroupOffsetsResponseData data = new AlterShareGroupOffsetsResponseData().setResponses(
                new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopicCollection(List.of(
                    new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic().setTopicName("foo").setPartitions(List.of(new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition().setPartitionIndex(0), new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition().setPartitionIndex(1))),
                    new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic().setTopicName("bar").setPartitions(List.of(new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition().setPartitionIndex(0)))
                ))
            );

            TopicPartition fooTopicPartition0 = new TopicPartition("foo", 0);
            TopicPartition fooTopicPartition1 = new TopicPartition("foo", 1);
            TopicPartition barPartition0 = new TopicPartition("bar", 0);
            TopicPartition zooTopicPartition0 = new TopicPartition("zoo", 0);

            env.kafkaClient().prepareResponse(new AlterShareGroupOffsetsResponse(data));
            final AlterShareGroupOffsetsResult result = env.adminClient().alterShareGroupOffsets(GROUP_ID, Map.of(fooTopicPartition0, 1L, fooTopicPartition1, 2L, barPartition0, 1L));

            assertNull(result.all().get());
            assertNull(result.partitionResult(fooTopicPartition0).get());
            assertNull(result.partitionResult(fooTopicPartition1).get());
            assertNull(result.partitionResult(barPartition0).get());
            TestUtils.assertFutureThrows(IllegalArgumentException.class, result.partitionResult(zooTopicPartition0));
        }
    }

    @Test
    public void testAlterShareGroupOffsetsWithTopLevelError() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            AlterShareGroupOffsetsResponseData data = new AlterShareGroupOffsetsResponseData().setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code()).setErrorMessage("Group authorization failed.");

            TopicPartition fooTopicPartition0 = new TopicPartition("foo", 0);
            TopicPartition fooTopicPartition1 = new TopicPartition("foo", 1);
            TopicPartition barPartition0 = new TopicPartition("bar", 0);
            TopicPartition zooTopicPartition0 = new TopicPartition("zoo", 0);

            env.kafkaClient().prepareResponse(new AlterShareGroupOffsetsResponse(data));
            final AlterShareGroupOffsetsResult result = env.adminClient().alterShareGroupOffsets(GROUP_ID, Map.of(fooTopicPartition0, 1L, fooTopicPartition1, 2L, barPartition0, 1L));

            TestUtils.assertFutureThrows(GroupAuthorizationException.class, result.all());
            TestUtils.assertFutureThrows(GroupAuthorizationException.class, result.partitionResult(fooTopicPartition1));
            TestUtils.assertFutureThrows(IllegalArgumentException.class, result.partitionResult(zooTopicPartition0));
        }
    }

    @Test
    public void testAlterShareGroupOffsetsWithErrorInOnePartition() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            AlterShareGroupOffsetsResponseData data = new AlterShareGroupOffsetsResponseData().setResponses(
                new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopicCollection(List.of(
                    new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic().setTopicName("foo").setPartitions(List.of(new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition().setPartitionIndex(0),
                        new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition().setPartitionIndex(1).setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()).setErrorMessage("Topic authorization failed."))),
                    new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic().setTopicName("bar").setPartitions(List.of(new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition().setPartitionIndex(0)))
                ))
            );

            TopicPartition fooTopicPartition0 = new TopicPartition("foo", 0);
            TopicPartition fooTopicPartition1 = new TopicPartition("foo", 1);
            TopicPartition barPartition0 = new TopicPartition("bar", 0);

            env.kafkaClient().prepareResponse(new AlterShareGroupOffsetsResponse(data));
            final AlterShareGroupOffsetsResult result = env.adminClient().alterShareGroupOffsets(GROUP_ID, Map.of(fooTopicPartition0, 1L, fooTopicPartition1, 2L, barPartition0, 1L));

            TestUtils.assertFutureThrows(TopicAuthorizationException.class, result.all());
            assertNull(result.partitionResult(fooTopicPartition0).get());
            TestUtils.assertFutureThrows(TopicAuthorizationException.class, result.partitionResult(fooTopicPartition1));
            assertNull(result.partitionResult(barPartition0).get());
        }
    }

    @Test
    public void testDeleteShareGroupOffsetsOptionsWithBatchedApi() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final Set<String> topics = Collections.singleton("A");
            final DeleteShareGroupOffsetsOptions options = new DeleteShareGroupOffsetsOptions();

            env.adminClient().deleteShareGroupOffsets(GROUP_ID, topics, options);

            final MockClient mockClient = env.kafkaClient();
            waitForRequest(mockClient, ApiKeys.DELETE_SHARE_GROUP_OFFSETS);

            ClientRequest clientRequest = mockClient.requests().peek();
            assertNotNull(clientRequest);
            DeleteShareGroupOffsetsRequestData data = ((DeleteShareGroupOffsetsRequest.Builder) clientRequest.requestBuilder()).build().data();
            assertEquals(GROUP_ID, data.groupId());
            assertEquals(1, data.topics().size());
            assertEquals(Collections.singletonList("A"),
                data.topics().stream().map(DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic::topicName).collect(Collectors.toList()));
        }
    }

    @Test
    public void testDeleteShareGroupOffsets() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            Uuid fooId = Uuid.randomUuid();
            String fooName = "foo";
            Uuid barId = Uuid.randomUuid();
            String barName = "bar";

            String zooName = "zoo";

            DeleteShareGroupOffsetsResponseData data = new DeleteShareGroupOffsetsResponseData().setResponses(
                List.of(
                    new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic().setTopicName(fooName).setTopicId(fooId),
                    new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic().setTopicName(barName).setTopicId(barId)
                )
            );

            env.kafkaClient().prepareResponse(new DeleteShareGroupOffsetsResponse(data));
            final DeleteShareGroupOffsetsResult result = env.adminClient().deleteShareGroupOffsets(GROUP_ID, Set.of(fooName, barName));

            assertNull(result.all().get());
            assertNull(result.topicResult(fooName).get());
            assertNull(result.topicResult(barName).get());
            assertThrows(IllegalArgumentException.class, () -> result.topicResult(zooName));
        }
    }

    @Test
    public void testDeleteShareGroupOffsetsEmpty() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DeleteShareGroupOffsetsResponseData data = new DeleteShareGroupOffsetsResponseData().setResponses(
                List.of()
            );
            env.kafkaClient().prepareResponse(new DeleteShareGroupOffsetsResponse(data));

            final DeleteShareGroupOffsetsResult result = env.adminClient().deleteShareGroupOffsets(GROUP_ID, Collections.emptySet());
            assertDoesNotThrow(() -> result.all().get());
        }
    }

    @Test
    public void testDeleteShareGroupOffsetsWithErrorInGroup() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DeleteShareGroupOffsetsResponseData data = new DeleteShareGroupOffsetsResponseData()
                .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code())
                .setErrorMessage(Errors.GROUP_AUTHORIZATION_FAILED.message());

            String fooName = "foo";
            String barName = "bar";

            env.kafkaClient().prepareResponse(new DeleteShareGroupOffsetsResponse(data));
            final DeleteShareGroupOffsetsResult result = env.adminClient().deleteShareGroupOffsets(GROUP_ID, Set.of(fooName, barName));

            TestUtils.assertFutureThrows(Errors.GROUP_AUTHORIZATION_FAILED.exception().getClass(), result.all());
        }
    }

    @Test
    public void testDeleteShareGroupOffsetsWithErrorInOneTopic() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            Uuid fooId = Uuid.randomUuid();
            String fooName = "foo";
            Uuid barId = Uuid.randomUuid();
            String barName = "bar";

            DeleteShareGroupOffsetsResponseData data = new DeleteShareGroupOffsetsResponseData().setResponses(
                List.of(
                    new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                        .setTopicName(fooName)
                        .setTopicId(fooId)
                        .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code())
                        .setErrorMessage(Errors.KAFKA_STORAGE_ERROR.message()),
                    new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                        .setTopicName(barName)
                        .setTopicId(barId)
                )
            );

            env.kafkaClient().prepareResponse(new DeleteShareGroupOffsetsResponse(data));
            final DeleteShareGroupOffsetsResult result = env.adminClient().deleteShareGroupOffsets(GROUP_ID, Set.of(fooName, barName));

            TestUtils.assertFutureThrows(Errors.KAFKA_STORAGE_ERROR.exception().getClass(), result.all());
            TestUtils.assertFutureThrows(Errors.KAFKA_STORAGE_ERROR.exception().getClass(), result.topicResult(fooName));
            assertNull(result.topicResult(barName).get());
        }
    }

    @Test
    public void testDeleteShareGroupOffsetsWithPartitionNotPresentInResult() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            Uuid fooId = Uuid.randomUuid();
            String fooName = "foo";

            String barName = "bar";

            DeleteShareGroupOffsetsResponseData data = new DeleteShareGroupOffsetsResponseData().setResponses(
                List.of(
                    new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
                        .setTopicName(fooName)
                        .setTopicId(fooId)
                )
            );

            env.kafkaClient().prepareResponse(new DeleteShareGroupOffsetsResponse(data));
            final DeleteShareGroupOffsetsResult result = env.adminClient().deleteShareGroupOffsets(GROUP_ID, Set.of(fooName));

            assertDoesNotThrow(() -> result.all().get());
            assertThrows(IllegalArgumentException.class, () -> result.topicResult(barName));
            assertNull(result.topicResult(fooName).get());
        }
    }

    private static ShareMemberDescription convertToShareMemberDescriptions(ShareGroupDescribeResponseData.Member member,
                                                                           ShareMemberAssignment assignment) {
        return new ShareMemberDescription(member.memberId(),
                                          Optional.ofNullable(member.rackId()),
                                          member.clientId(),
                                          member.clientHost(),
                                          assignment,
                                          member.memberEpoch());
    }
}
