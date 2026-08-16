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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.ClassicGroupState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.ConsumerGroupDescribeResponse;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.admin.KafkaAdminClient.DEFAULT_LEAVE_GROUP_REASON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaAdminClientConsumerGroupTest extends KafkaAdminClientTestBase {

    @Test
    public void testListGroups() throws Exception {
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
                        .setGroups(asList(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-1")
                                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                .setGroupType(GroupType.CONSUMER.toString())
                                .setGroupState("Stable"),
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-connect-1")
                                .setProtocolType("connector")
                                .setGroupType(GroupType.CLASSIC.toString())
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
                        .setGroups(asList(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-2")
                                .setProtocolType("anyproto")
                                .setGroupType(GroupType.CLASSIC.toString())
                                .setGroupState("Stable"),
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-connect-2")
                                .setProtocolType("connector")
                                .setGroupType(GroupType.CLASSIC.toString())
                                .setGroupState("Stable")
                        ))),
                env.cluster().nodeById(1));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGroups(asList(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-3")
                                .setProtocolType("share")
                                .setGroupType(GroupType.SHARE.toString())
                                .setGroupState("Stable"),
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-connect-3")
                                .setProtocolType("connector")
                                .setGroupType(GroupType.CLASSIC.toString())
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

            final ListGroupsResult result = env.adminClient().listGroups();
            TestUtils.assertFutureThrows(UnknownServerException.class, result.all());

            Collection<GroupListing> listings = result.valid().get();
            assertEquals(6, listings.size());

            Set<String> groupIds = new HashSet<>();
            for (GroupListing listing : listings) {
                groupIds.add(listing.groupId());
            }

            assertEquals(Set.of("group-1", "group-connect-1", "group-2", "group-connect-2", "group-3", "group-connect-3"), groupIds);
            assertEquals(1, result.errors().get().size());
        }
    }

    @Test
    public void testListGroupsMetadataFailure() throws Exception {
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

            final ListGroupsResult result = env.adminClient().listGroups();
            TestUtils.assertFutureThrows(KafkaException.class, result.all());
        }
    }

    @Test
    public void testListGroupsEmptyProtocol() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(asList(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupType(GroupType.CONSUMER.toString())
                            .setGroupState("Stable"),
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-2")
                            .setGroupType(GroupType.CLASSIC.toString())
                            .setGroupState("Empty")))),
                env.cluster().nodeById(0));

            final ListGroupsOptions options = new ListGroupsOptions();
            final ListGroupsResult result = env.adminClient().listGroups(options);
            Collection<GroupListing> listings = result.valid().get();

            assertEquals(2, listings.size());
            List<GroupListing> expected = new ArrayList<>();
            expected.add(new GroupListing("group-2", Optional.of(GroupType.CLASSIC), "", Optional.of(GroupState.EMPTY)));
            expected.add(new GroupListing("group-1", Optional.of(GroupType.CONSUMER), ConsumerProtocol.PROTOCOL_TYPE, Optional.of(GroupState.STABLE)));
            assertEquals(expected, listings);
            assertEquals(0, result.errors().get().size());
        }
    }

    @Test
    public void testListGroupsEmptyGroupType() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType("any")))),
                env.cluster().nodeById(0));

            final ListGroupsOptions options = new ListGroupsOptions();
            final ListGroupsResult result = env.adminClient().listGroups(options);
            Collection<GroupListing> listings = result.valid().get();

            assertEquals(1, listings.size());
            List<GroupListing> expected = new ArrayList<>();
            expected.add(new GroupListing("group-1", Optional.empty(), "any", Optional.empty()));
            assertEquals(expected, listings);
            assertEquals(0, result.errors().get().size());
        }
    }

    @Test
    public void testListGroupsWithProtocolTypes() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test with list group options.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                expectListGroupsRequestWithFilters(Set.of(), Set.of()),
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState("Stable")
                            .setGroupType(GroupType.CONSUMER.toString()),
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-2")
                            .setGroupState("Empty")
                            .setGroupType(GroupType.CONSUMER.toString())))),
                env.cluster().nodeById(0));

            final ListGroupsOptions options = new ListGroupsOptions().withProtocolTypes(Set.of(""));
            final ListGroupsResult result = env.adminClient().listGroups(options);
            Collection<GroupListing> listing = result.valid().get();

            assertEquals(1, listing.size());
            List<GroupListing> expected = new ArrayList<>();
            expected.add(new GroupListing("group-2", Optional.of(GroupType.CONSUMER), "", Optional.of(GroupState.EMPTY)));
            assertEquals(expected, listing);
            assertEquals(0, result.errors().get().size());
        }
    }

    @Test
    public void testListGroupsWithTypes() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test with list group options.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                expectListGroupsRequestWithFilters(Collections.emptySet(), singleton(GroupType.CONSUMER.toString())),
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(asList(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState("Stable")
                            .setGroupType(GroupType.CONSUMER.toString()),
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-2")
                            .setGroupState("Empty")
                            .setGroupType(GroupType.CONSUMER.toString())))),
                env.cluster().nodeById(0));

            final ListGroupsOptions options = new ListGroupsOptions().withTypes(singleton(GroupType.CONSUMER));
            final ListGroupsResult result = env.adminClient().listGroups(options);
            Collection<GroupListing> listing = result.valid().get();

            assertEquals(2, listing.size());
            List<GroupListing> expected = new ArrayList<>();
            expected.add(new GroupListing("group-2", Optional.of(GroupType.CONSUMER), "", Optional.of(GroupState.EMPTY)));
            expected.add(new GroupListing("group-1", Optional.of(GroupType.CONSUMER), ConsumerProtocol.PROTOCOL_TYPE, Optional.of(GroupState.STABLE)));
            assertEquals(expected, listing);
            assertEquals(0, result.errors().get().size());
        }
    }

    @Test
    public void testListGroupsWithTypesOlderBrokerVersion() throws Exception {
        ApiVersion listGroupV4 = new ApiVersion()
            .setApiKey(ApiKeys.LIST_GROUPS.id)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 4);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(List.of(listGroupV4)));

            // Check that we cannot set a type filter with an older broker.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));
            env.kafkaClient().prepareUnsupportedVersionResponse(request ->
                request instanceof ListGroupsRequest && !((ListGroupsRequest) request).data().typesFilter().isEmpty()
            );

            ListGroupsOptions options = new ListGroupsOptions().withTypes(Set.of(GroupType.SHARE));
            ListGroupsResult result = env.adminClient().listGroups(options);
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.all());

            // But a type filter which is just classic groups is permitted with an older broker, because they
            // only know about classic groups so the types filter can be omitted.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                expectListGroupsRequestWithFilters(Set.of(), Set.of()),
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState(GroupState.STABLE.toString())))),
                env.cluster().nodeById(0));

            options = new ListGroupsOptions().withTypes(Set.of(GroupType.CLASSIC));
            result = env.adminClient().listGroups(options);

            Collection<GroupListing> listing = result.all().get();
            assertEquals(1, listing.size());
            List<GroupListing> expected = List.of(
                new GroupListing("group-1", Optional.empty(), ConsumerProtocol.PROTOCOL_TYPE, Optional.of(GroupState.STABLE))
            );
            assertEquals(expected, listing);

            // But a type filter which is just consumer groups without classic groups is not permitted with an older broker.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));
            env.kafkaClient().prepareUnsupportedVersionResponse(request ->
                request instanceof ListGroupsRequest && !((ListGroupsRequest) request).data().typesFilter().isEmpty()
            );

            options = new ListGroupsOptions().withTypes(Set.of(GroupType.CONSUMER));
            result = env.adminClient().listGroups(options);
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.all());
        }
    }

    @Test
    public void testListConsumerGroups() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(4, 0),
                AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Empty metadata response should be retried
            env.kafkaClient().prepareResponse(
                 RequestTestUtils.metadataResponse(
                    List.of(),
                    env.cluster().clusterResource().clusterId(),
                    -1,
                    List.of()));

            env.kafkaClient().prepareResponse(
                 RequestTestUtils.metadataResponse(
                    env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(),
                    env.cluster().controller().id(),
                    List.of()));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState("Stable"),
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-connect-1")
                            .setProtocolType("connector")
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
                        .setGroups(asList(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-2")
                                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                .setGroupState("Stable"),
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-connect-2")
                                .setProtocolType("connector")
                                .setGroupState("Stable")
                    ))),
                env.cluster().nodeById(1));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGroups(List.of(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-3")
                                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                .setGroupState("Stable"),
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-connect-3")
                                .setProtocolType("connector")
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

            final ListGroupsResult result = env.adminClient().listGroups(ListGroupsOptions.forConsumerGroups());
            TestUtils.assertFutureThrows(UnknownServerException.class, result.all());

            Collection<GroupListing> listings = result.valid().get();
            assertEquals(3, listings.size());

            Set<String> groupIds = new HashSet<>();
            for (GroupListing listing : listings) {
                groupIds.add(listing.groupId());
                assertTrue(listing.groupState().isPresent());
            }

            assertEquals(Set.of("group-1", "group-2", "group-3"), groupIds);
            assertEquals(1, result.errors().get().size());
        }
    }

    @Test
    public void testListConsumerGroupsMetadataFailure() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Empty metadata causes the request to fail since we have no list of brokers
            // to send the ListGroups requests to
            env.kafkaClient().prepareResponse(
                 RequestTestUtils.metadataResponse(
                    List.of(),
                    env.cluster().clusterResource().clusterId(),
                    -1,
                    List.of()));

            final ListGroupsResult result = env.adminClient().listGroups(ListGroupsOptions.forConsumerGroups());
            TestUtils.assertFutureThrows(KafkaException.class, result.all());
        }
    }

    @Test
    public void testListConsumerGroupsWithStates() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState("Stable"),
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-2")
                            .setGroupState("Empty")))),
                env.cluster().nodeById(0));

            final ListGroupsOptions options = ListGroupsOptions.forConsumerGroups();
            final ListGroupsResult result = env.adminClient().listGroups(options);
            Collection<GroupListing> listings = result.valid().get();

            assertEquals(2, listings.size());
            List<GroupListing> expected = new ArrayList<>();
            expected.add(new GroupListing("group-2", Optional.empty(), "", Optional.of(GroupState.EMPTY)));
            expected.add(new GroupListing("group-1", Optional.empty(), ConsumerProtocol.PROTOCOL_TYPE, Optional.of(GroupState.STABLE)));
            assertEquals(expected, listings);
            assertEquals(0, result.errors().get().size());
        }
    }

    @Test
    public void testListConsumerGroupsWithProtocolTypes() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test with a specific protocol type filter in list consumer group options.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                expectListGroupsRequestWithFilters(Set.of(), Set.of(GroupType.CONSUMER.toString(), GroupType.CLASSIC.toString())),
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState("Stable")
                            .setGroupType(GroupType.CONSUMER.toString()),
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-2")
                            .setGroupState("Empty")
                            .setGroupType(GroupType.CONSUMER.toString())))),
                env.cluster().nodeById(0));

            final ListGroupsOptions options = ListGroupsOptions.forConsumerGroups().withProtocolTypes(Set.of(ConsumerProtocol.PROTOCOL_TYPE));
            final ListGroupsResult result = env.adminClient().listGroups(options);
            Collection<GroupListing> listings = result.valid().get();

            assertEquals(1, listings.size());
            List<GroupListing> expected = new ArrayList<>();
            expected.add(new GroupListing("group-1", Optional.of(GroupType.CONSUMER), ConsumerProtocol.PROTOCOL_TYPE, Optional.of(GroupState.STABLE)));
            assertEquals(expected, listings);
            assertEquals(0, result.errors().get().size());
        }
    }

    @Test
    public void testListConsumerGroupsWithTypes() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test with a specific state filter but no type filter in list consumer group options.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                expectListGroupsRequestWithFilters(Set.of(GroupState.STABLE.toString()), Set.of(GroupType.CONSUMER.toString(), GroupType.CLASSIC.toString())),
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState("Stable")
                            .setGroupType(GroupType.CLASSIC.toString())))),
                env.cluster().nodeById(0));

            final ListGroupsOptions options = ListGroupsOptions.forConsumerGroups().inGroupStates(Set.of(GroupState.STABLE));
            final ListGroupsResult result = env.adminClient().listGroups(options);
            Collection<GroupListing> listings = result.valid().get();

            assertEquals(1, listings.size());
            List<GroupListing> expected = new ArrayList<>();
            expected.add(new GroupListing("group-1", Optional.of(GroupType.CLASSIC), ConsumerProtocol.PROTOCOL_TYPE, Optional.of(GroupState.STABLE)));
            assertEquals(expected, listings);
            assertEquals(0, result.errors().get().size());

            // Test with list consumer group options.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                expectListGroupsRequestWithFilters(Set.of(), Set.of(GroupType.CONSUMER.toString())),
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState("Stable")
                            .setGroupType(GroupType.CONSUMER.toString()),
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-2")
                            .setGroupState("Empty")
                            .setGroupType(GroupType.CONSUMER.toString())))),
                env.cluster().nodeById(0));

            final ListGroupsOptions options2 = ListGroupsOptions.forConsumerGroups().withTypes(Set.of(GroupType.CONSUMER));
            final ListGroupsResult result2 = env.adminClient().listGroups(options2);
            Collection<GroupListing> listings2 = result2.valid().get();

            assertEquals(2, listings2.size());
            List<GroupListing> expected2 = new ArrayList<>();
            expected2.add(new GroupListing("group-2", Optional.of(GroupType.CONSUMER), "", Optional.of(GroupState.EMPTY)));
            expected2.add(new GroupListing("group-1", Optional.of(GroupType.CONSUMER), ConsumerProtocol.PROTOCOL_TYPE, Optional.of(GroupState.STABLE)));
            assertEquals(expected2, listings2);
            assertEquals(0, result.errors().get().size());
        }
    }

    @Test
    public void testListConsumerGroupsWithStatesOlderBrokerVersion() throws Exception {
        ApiVersion listGroupV3 = new ApiVersion()
                .setApiKey(ApiKeys.LIST_GROUPS.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 3);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(List.of(listGroupV3)));

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            // Check we can list groups v3 with older broker if we don't specify states, and use just consumer group types which can be omitted.
            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)))),
                env.cluster().nodeById(0));

            ListGroupsOptions options = ListGroupsOptions.forConsumerGroups();
            ListGroupsResult result = env.adminClient().listGroups(options);
            Collection<GroupListing> listing = result.all().get();
            assertEquals(1, listing.size());
            List<GroupListing> expected = List.of(new GroupListing("group-1", Optional.empty(), ConsumerProtocol.PROTOCOL_TYPE, Optional.empty()));
            assertEquals(expected, listing);

            // But we cannot set a state filter with older broker
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareUnsupportedVersionResponse(request ->
                request instanceof ListGroupsRequest &&
                    !((ListGroupsRequest) request).data().statesFilter().isEmpty()
            );

            options = ListGroupsOptions.forConsumerGroups().inGroupStates(Set.of(GroupState.STABLE));
            result = env.adminClient().listGroups(options);
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.all());
        }
    }

    @Test
    public void testListConsumerGroupsWithTypesOlderBrokerVersion() throws Exception {
        ApiVersion listGroupV4 = new ApiVersion()
            .setApiKey(ApiKeys.LIST_GROUPS.id)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 4);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(List.of(listGroupV4)));

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            // Check if we can list groups v4 with older broker if we specify states and don't specify types.
            env.kafkaClient().prepareResponseFrom(
                expectListGroupsRequestWithFilters(Set.of(GroupState.STABLE.toString()), Set.of()),
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState(GroupState.STABLE.toString())))),
                env.cluster().nodeById(0));

            ListGroupsOptions options = ListGroupsOptions.forConsumerGroups().inGroupStates(Set.of(GroupState.STABLE));
            ListGroupsResult result = env.adminClient().listGroups(options);

            Collection<GroupListing> listing = result.all().get();
            assertEquals(1, listing.size());
            List<GroupListing> expected = List.of(
                new GroupListing("group-1", Optional.empty(), ConsumerProtocol.PROTOCOL_TYPE, Optional.of(GroupState.STABLE))
            );
            assertEquals(expected, listing);

            // Check that we cannot set a type filter with an older broker.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));
            // First attempt to build request will require v5 (type filter), but the broker only supports v4
            env.kafkaClient().prepareUnsupportedVersionResponse(request ->
                request instanceof ListGroupsRequest && !((ListGroupsRequest) request).data().typesFilter().isEmpty()
            );

            options = ListGroupsOptions.forConsumerGroups().withTypes(Set.of(GroupType.SHARE));
            result = env.adminClient().listGroups(options);
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.all());
        }
    }

    @Test
    @SuppressWarnings("removal")
    public void testListConsumerGroupsDeprecated() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(4, 0),
            AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Empty metadata response should be retried
            env.kafkaClient().prepareResponse(
                RequestTestUtils.metadataResponse(
                    List.of(),
                    env.cluster().clusterResource().clusterId(),
                    -1,
                    List.of()));

            env.kafkaClient().prepareResponse(
                RequestTestUtils.metadataResponse(
                    env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(),
                    env.cluster().controller().id(),
                    List.of()));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGroups(List.of(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-1")
                                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                .setGroupState("Stable"),
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-connect-1")
                                .setProtocolType("connector")
                                .setGroupState("Stable")
                        ))),
                env.cluster().nodeById(0));

            // handle retriable errors
            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                        .setGroups(List.of())
                ),
                env.cluster().nodeById(1));
            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                        .setGroups(List.of())
                ),
                env.cluster().nodeById(1));
            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGroups(List.of(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-2")
                                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                .setGroupState("Stable"),
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-connect-2")
                                .setProtocolType("connector")
                                .setGroupState("Stable")
                        ))),
                env.cluster().nodeById(1));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGroups(List.of(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-3")
                                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                .setGroupState("Stable"),
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-connect-3")
                                .setProtocolType("connector")
                                .setGroupState("Stable")
                        ))),
                env.cluster().nodeById(2));

            // fatal error
            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setGroups(List.of())),
                env.cluster().nodeById(3));

            final ListConsumerGroupsResult result = env.adminClient().listConsumerGroups();
            TestUtils.assertFutureThrows(UnknownServerException.class, result.all());

            Collection<ConsumerGroupListing> listings = result.valid().get();
            assertEquals(3, listings.size());

            Set<String> groupIds = new HashSet<>();
            for (ConsumerGroupListing listing : listings) {
                groupIds.add(listing.groupId());
                assertTrue(listing.state().isPresent());
            }

            assertEquals(Set.of("group-1", "group-2", "group-3"), groupIds);
            assertEquals(1, result.errors().get().size());
        }
    }

    @Test
    @SuppressWarnings("removal")
    public void testListConsumerGroupsDeprecatedMetadataFailure() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Empty metadata causes the request to fail since we have no list of brokers
            // to send the ListGroups requests to
            env.kafkaClient().prepareResponse(
                RequestTestUtils.metadataResponse(
                    List.of(),
                    env.cluster().clusterResource().clusterId(),
                    -1,
                    List.of()));

            final ListConsumerGroupsResult result = env.adminClient().listConsumerGroups();
            TestUtils.assertFutureThrows(KafkaException.class, result.all());
        }
    }

    @Test
    @SuppressWarnings("removal")
    public void testListConsumerGroupsDeprecatedWithStates() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState("Stable"),
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-2")
                            .setGroupState("Empty")))),
                env.cluster().nodeById(0));

            final ListConsumerGroupsOptions options = new ListConsumerGroupsOptions();
            final ListConsumerGroupsResult result = env.adminClient().listConsumerGroups(options);
            Collection<ConsumerGroupListing> listings = result.valid().get();

            assertEquals(2, listings.size());
            List<ConsumerGroupListing> expected = new ArrayList<>();
            expected.add(new ConsumerGroupListing("group-2", Optional.of(GroupState.EMPTY), true));
            expected.add(new ConsumerGroupListing("group-1", Optional.of(GroupState.STABLE), false));
            assertEquals(expected, listings);
            assertEquals(0, result.errors().get().size());
        }
    }

    @Test
    @SuppressWarnings("removal")
    public void testListConsumerGroupsDeprecatedWithTypes() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test with a specific state filter but no type filter in list consumer group options.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                expectListGroupsRequestWithFilters(Set.of(GroupState.STABLE.toString()), Set.of()),
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState("Stable")
                            .setGroupType(GroupType.CLASSIC.toString())))),
                env.cluster().nodeById(0));

            final ListConsumerGroupsOptions options = new ListConsumerGroupsOptions().inGroupStates(Set.of(GroupState.STABLE));
            final ListConsumerGroupsResult result = env.adminClient().listConsumerGroups(options);
            Collection<ConsumerGroupListing> listings = result.valid().get();

            assertEquals(1, listings.size());
            List<ConsumerGroupListing> expected = new ArrayList<>();
            expected.add(new ConsumerGroupListing("group-1", Optional.of(GroupState.STABLE), Optional.of(GroupType.CLASSIC), false));
            assertEquals(expected, listings);
            assertEquals(0, result.errors().get().size());

            // Test with list consumer group options.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                expectListGroupsRequestWithFilters(Set.of(), Set.of(GroupType.CONSUMER.toString())),
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState("Stable")
                            .setGroupType(GroupType.CONSUMER.toString()),
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-2")
                            .setGroupState("Empty")
                            .setGroupType(GroupType.CONSUMER.toString())))),
                env.cluster().nodeById(0));

            final ListConsumerGroupsOptions options2 = new ListConsumerGroupsOptions().withTypes(singleton(GroupType.CONSUMER));
            final ListConsumerGroupsResult result2 = env.adminClient().listConsumerGroups(options2);
            Collection<ConsumerGroupListing> listings2 = result2.valid().get();

            assertEquals(2, listings2.size());
            List<ConsumerGroupListing> expected2 = new ArrayList<>();
            expected2.add(new ConsumerGroupListing("group-2", Optional.of(GroupState.EMPTY), Optional.of(GroupType.CONSUMER), true));
            expected2.add(new ConsumerGroupListing("group-1", Optional.of(GroupState.STABLE), Optional.of(GroupType.CONSUMER), false));
            assertEquals(expected2, listings2);
            assertEquals(0, result.errors().get().size());
        }
    }

    @Test
    @SuppressWarnings("removal")
    public void testListConsumerGroupsDeprecatedWithStatesOlderBrokerVersion() throws Exception {
        ApiVersion listGroupV3 = new ApiVersion()
            .setApiKey(ApiKeys.LIST_GROUPS.id)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 3);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(List.of(listGroupV3)));

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            // Check we can list groups with older broker if we don't specify states
            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)))),
                env.cluster().nodeById(0));

            ListConsumerGroupsOptions options = new ListConsumerGroupsOptions();
            ListConsumerGroupsResult result = env.adminClient().listConsumerGroups(options);
            Collection<ConsumerGroupListing> listing = result.all().get();
            assertEquals(1, listing.size());
            List<ConsumerGroupListing> expected = List.of(new ConsumerGroupListing("group-1", false));
            assertEquals(expected, listing);

            // But we cannot set a state filter with older broker
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));
            env.kafkaClient().prepareUnsupportedVersionResponse(
                body -> body instanceof ListGroupsRequest);

            options = new ListConsumerGroupsOptions().inGroupStates(Set.of(GroupState.STABLE));
            result = env.adminClient().listConsumerGroups(options);
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.all());
        }
    }

    @Test
    @SuppressWarnings("removal")
    public void testListConsumerGroupsDeprecatedWithTypesOlderBrokerVersion() throws Exception {
        ApiVersion listGroupV4 = new ApiVersion()
            .setApiKey(ApiKeys.LIST_GROUPS.id)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 4);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(List.of(listGroupV4)));

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            // Check if we can list groups with older broker if we specify states and don't specify types.
            env.kafkaClient().prepareResponseFrom(
                expectListGroupsRequestWithFilters(Set.of(GroupState.STABLE.toString()), Set.of()),
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState(GroupState.STABLE.toString())))),
                env.cluster().nodeById(0));

            ListConsumerGroupsOptions options = new ListConsumerGroupsOptions().inGroupStates(Set.of(GroupState.STABLE));
            ListConsumerGroupsResult result = env.adminClient().listConsumerGroups(options);

            Collection<ConsumerGroupListing> listing = result.all().get();
            assertEquals(1, listing.size());
            List<ConsumerGroupListing> expected = List.of(
                new ConsumerGroupListing("group-1", Optional.of(GroupState.STABLE), false)
            );
            assertEquals(expected, listing);

            // Check that we cannot set a type filter with an older broker.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));
            env.kafkaClient().prepareUnsupportedVersionResponse(request ->
                request instanceof ListGroupsRequest && !((ListGroupsRequest) request).data().typesFilter().isEmpty()
            );

            options = new ListConsumerGroupsOptions().withTypes(Set.of(GroupType.SHARE));
            result = env.adminClient().listConsumerGroups(options);
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.all());

            // But a type filter which is just classic groups is permitted with an older broker, because they
            // only know about classic groups so the types filter can be omitted.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                expectListGroupsRequestWithFilters(Set.of(), Set.of()),
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(List.of(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("group-1")
                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                            .setGroupState(GroupState.STABLE.toString())))),
                env.cluster().nodeById(0));

            options = new ListConsumerGroupsOptions().withTypes(Set.of(GroupType.CLASSIC));
            result = env.adminClient().listConsumerGroups(options);

            listing = result.all().get();
            assertEquals(1, listing.size());
            assertEquals(expected, listing);
        }
    }

    @Test
    public void testOffsetCommitNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final TopicPartition tp1 = new TopicPartition("foo", 0);

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(prepareOffsetCommitResponse(tp1, Errors.NOT_COORDINATOR));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1, new OffsetAndMetadata(123L));
            final AlterConsumerGroupOffsetsResult result = env.adminClient().alterConsumerGroupOffsets(GROUP_ID, offsets);

            TestUtils.assertFutureThrows(TimeoutException.class, result.all());
        }
    }

    @Test
    public void testOffsetCommitWithMultipleErrors() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final TopicPartition foo0 = new TopicPartition("foo", 0);
            final TopicPartition foo1 = new TopicPartition("foo", 1);

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            Map<TopicPartition, Errors> responseData = new HashMap<>();
            responseData.put(foo0, Errors.NONE);
            responseData.put(foo1, Errors.UNKNOWN_TOPIC_OR_PARTITION);
            env.kafkaClient().prepareResponse(new OffsetCommitResponse(0, responseData));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(foo0, new OffsetAndMetadata(123L));
            offsets.put(foo1, new OffsetAndMetadata(456L));
            final AlterConsumerGroupOffsetsResult result = env.adminClient()
                .alterConsumerGroupOffsets(GROUP_ID, offsets);

            assertNull(result.partitionResult(foo0).get());
            TestUtils.assertFutureThrows(UnknownTopicOrPartitionException.class, result.partitionResult(foo1));

            TestUtils.assertFutureThrows(UnknownTopicOrPartitionException.class, result.all());
        }
    }

    @Test
    public void testOffsetCommitRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            final TopicPartition tp1 = new TopicPartition("foo", 0);

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return true;
            }, prepareOffsetCommitResponse(tp1, Errors.NOT_COORDINATOR));

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return true;
            }, prepareOffsetCommitResponse(tp1, Errors.NONE));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1, new OffsetAndMetadata(123L));
            final KafkaFuture<Void> future = env.adminClient().alterConsumerGroupOffsets(GROUP_ID, offsets).all();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting CommitOffsets first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry CommitOffsets call on first failure");

            long lowerBoundBackoffMs = (long) (retryBackoff * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
            long upperBoundBackoffMs = (long) (retryBackoff * CommonClientConfigs.RETRY_BACKOFF_EXP_BASE * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
            time.sleep(upperBoundBackoffMs);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, upperBoundBackoffMs - lowerBoundBackoffMs, "CommitOffsets retry did not await expected backoff");
        }
    }

    @Test
    public void testDescribeConsumerGroupNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();

            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.NOT_COORDINATOR,
                "",
                "",
                "",
                Collections.emptyList(),
                Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList(GROUP_ID));

            TestUtils.assertFutureThrows(TimeoutException.class, result.all());
        }
    }

    @Test
    public void testDescribeConsumerGroupRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.NOT_COORDINATOR,
                "",
                "",
                "",
                Collections.emptyList(),
                Collections.emptySet()));

            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return true;
            }, new DescribeGroupsResponse(data));

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.NONE,
                "",
                ConsumerProtocol.PROTOCOL_TYPE,
                "",
                Collections.emptyList(),
                Collections.emptySet()));

            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return true;
            }, new DescribeGroupsResponse(data));

            final KafkaFuture<Map<String, ConsumerGroupDescription>> future =
                env.adminClient().describeConsumerGroups(singletonList(GROUP_ID)).all();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting DescribeConsumerGroup first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry DescribeConsumerGroup call on first failure");

            long lowerBoundBackoffMs = (long) (retryBackoff * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
            long upperBoundBackoffMs = (long) (retryBackoff * CommonClientConfigs.RETRY_BACKOFF_EXP_BASE * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
            time.sleep(upperBoundBackoffMs);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, upperBoundBackoffMs - lowerBoundBackoffMs, "DescribeConsumerGroup retry did not await expected backoff!");
        }
    }

    @Test
    public void testDescribeConsumerGroups() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS,  Node.noNode()));

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            // The first request sent will be a ConsumerGroupDescribe request. Let's
            // fail it in order to fail back to using the classic version.
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof ConsumerGroupDescribeRequest);

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();

            // Retriable errors should be retried
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.COORDINATOR_LOAD_IN_PROGRESS,
                "",
                "",
                "",
                Collections.emptyList(),
                Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            /*
             * We need to return two responses here, one with NOT_COORDINATOR error when calling describe consumer group
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for COORDINATOR_NOT_AVAILABLE error response
             */
            data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                    GROUP_ID,
                    Errors.NOT_COORDINATOR,
                    "",
                    "",
                    "",
                    Collections.emptyList(),
                    Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.COORDINATOR_NOT_AVAILABLE,
                "",
                "",
                "",
                Collections.emptyList(),
                Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            data = new DescribeGroupsResponseData();
            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);

            final List<TopicPartition> topicPartitions = new ArrayList<>();
            topicPartitions.add(0, myTopicPartition0);
            topicPartitions.add(1, myTopicPartition1);
            topicPartitions.add(2, myTopicPartition2);

            final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(topicPartitions));
            byte[] memberAssignmentBytes = new byte[memberAssignment.remaining()];
            memberAssignment.get(memberAssignmentBytes);

            DescribedGroupMember memberOne = DescribeGroupsResponse.groupMember("0", "instance1", "clientId0", "clientHost", memberAssignmentBytes, null);
            DescribedGroupMember memberTwo = DescribeGroupsResponse.groupMember("1", "instance2", "clientId1", "clientHost", memberAssignmentBytes, null);

            List<MemberDescription> expectedMemberDescriptions = new ArrayList<>();
            expectedMemberDescriptions.add(convertToMemberDescriptions(memberOne,
                                                                       new MemberAssignment(new HashSet<>(topicPartitions))));
            expectedMemberDescriptions.add(convertToMemberDescriptions(memberTwo,
                                                                       new MemberAssignment(new HashSet<>(topicPartitions))));
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                    GROUP_ID,
                    Errors.NONE,
                    "",
                    ConsumerProtocol.PROTOCOL_TYPE,
                    "",
                    asList(memberOne, memberTwo),
                    Collections.emptySet()));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList(GROUP_ID));
            final ConsumerGroupDescription groupDescription = result.describedGroups().get(GROUP_ID).get();

            assertEquals(1, result.describedGroups().size());
            assertEquals(GROUP_ID, groupDescription.groupId());
            assertEquals(2, groupDescription.members().size());
            assertEquals(expectedMemberDescriptions, groupDescription.members());
        }
    }

    @Test
    public void testDescribeMultipleConsumerGroups() {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(asList(
                        FindCoordinatorResponse.prepareCoordinatorResponse(Errors.NONE, GROUP_ID, env.cluster().controller()),
                        FindCoordinatorResponse.prepareCoordinatorResponse(Errors.NONE, "group-connect-0", env.cluster().controller())
                    ))
            ));

            // The first request sent will be a ConsumerGroupDescribe request. Let's
            // fail it in order to fail back to using the classic version.
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof ConsumerGroupDescribeRequest);

            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);

            final List<TopicPartition> topicPartitions = new ArrayList<>();
            topicPartitions.add(0, myTopicPartition0);
            topicPartitions.add(1, myTopicPartition1);
            topicPartitions.add(2, myTopicPartition2);

            final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(topicPartitions));
            byte[] memberAssignmentBytes = new byte[memberAssignment.remaining()];
            memberAssignment.get(memberAssignmentBytes);

            DescribeGroupsResponseData groupData = new DescribeGroupsResponseData();
            groupData.groups().add(DescribeGroupsResponse.groupMetadata(
                    GROUP_ID,
                    Errors.NONE,
                    "",
                    ConsumerProtocol.PROTOCOL_TYPE,
                    "",
                    asList(
                            DescribeGroupsResponse.groupMember("0", null, "clientId0", "clientHost", memberAssignmentBytes, null),
                            DescribeGroupsResponse.groupMember("1", null, "clientId1", "clientHost", memberAssignmentBytes, null)
                    ),
                    Collections.emptySet()));
            groupData.groups().add(DescribeGroupsResponse.groupMetadata(
                    "group-connect-0",
                    Errors.NONE,
                    "",
                    "connect",
                    "",
                    asList(
                            DescribeGroupsResponse.groupMember("0", null, "clientId0", "clientHost", memberAssignmentBytes, null),
                            DescribeGroupsResponse.groupMember("1", null, "clientId1", "clientHost", memberAssignmentBytes, null)
                    ),
                    Collections.emptySet()));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(groupData));

            Collection<String> groups = new HashSet<>();
            groups.add(GROUP_ID);
            groups.add("group-connect-0");
            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(groups);
            assertEquals(2, result.describedGroups().size());
            assertEquals(groups, result.describedGroups().keySet());
            KafkaFuture<Map<String, ConsumerGroupDescription>> allFuture = result.all();
            // This throws because the second group is a classic connect group, not a consumer group.
            assertThrows(ExecutionException.class, allFuture::get);
            assertTrue(allFuture.isCompletedExceptionally());
        }
    }

    @Test
    public void testDescribeConsumerGroupsGroupIdNotFound() {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(asList(
                        FindCoordinatorResponse.prepareCoordinatorResponse(Errors.NONE, GROUP_ID, env.cluster().controller()),
                        FindCoordinatorResponse.prepareCoordinatorResponse(Errors.NONE, "group-connect-0", env.cluster().controller())
                    ))
            ));

            // The first request sent will be a ConsumerGroupDescribe request. Let's
            // fail it in order to fail back to using the classic version.
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof ConsumerGroupDescribeRequest);

            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);

            final List<TopicPartition> topicPartitions = new ArrayList<>();
            topicPartitions.add(0, myTopicPartition0);
            topicPartitions.add(1, myTopicPartition1);
            topicPartitions.add(2, myTopicPartition2);

            final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(topicPartitions));
            byte[] memberAssignmentBytes = new byte[memberAssignment.remaining()];
            memberAssignment.get(memberAssignmentBytes);

            DescribeGroupsResponseData groupData = new DescribeGroupsResponseData();
            groupData.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.NONE,
                "",
                ConsumerProtocol.PROTOCOL_TYPE,
                "",
                asList(
                    DescribeGroupsResponse.groupMember("0", null, "clientId0", "clientHost", memberAssignmentBytes, null),
                    DescribeGroupsResponse.groupMember("1", null, "clientId1", "clientHost", memberAssignmentBytes, null)
                ),
                Collections.emptySet()));
            groupData.groups().add(DescribeGroupsResponse.groupError(
                "group-connect-0",
                Errors.GROUP_ID_NOT_FOUND,
                "Group group-connect-0 is not a classic group."));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(groupData));

            Collection<String> groups = new HashSet<>();
            groups.add(GROUP_ID);
            groups.add("group-connect-0");
            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(groups);
            assertEquals(2, result.describedGroups().size());
            assertEquals(groups, result.describedGroups().keySet());
            KafkaFuture<Map<String, ConsumerGroupDescription>> allFuture = result.all();
            assertThrows(ExecutionException.class, allFuture::get);
            assertTrue(result.all().isCompletedExceptionally());
        }
    }

    @Test
    public void testDescribeConsumerGroupsWithAuthorizedOperationsOmitted() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            // The first request sent will be a ConsumerGroupDescribe request. Let's
            // fail it in order to fail back to using the classic version.
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof ConsumerGroupDescribeRequest);

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.NONE,
                "",
                ConsumerProtocol.PROTOCOL_TYPE,
                "",
                Collections.emptyList(),
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList(GROUP_ID));
            final ConsumerGroupDescription groupDescription = result.describedGroups().get(GROUP_ID).get();

            assertNull(groupDescription.authorizedOperations());
        }
    }

    @Test
    public void testDescribeNonConsumerGroups() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            // The first request sent will be a ConsumerGroupDescribe request. Let's
            // fail it in order to fail back to using the classic version.
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof ConsumerGroupDescribeRequest);

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();

            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.NONE,
                "",
                "non-consumer",
                "",
                emptyList(),
                Collections.emptySet()));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList(GROUP_ID));

            TestUtils.assertFutureThrows(IllegalArgumentException.class, result.describedGroups().get(GROUP_ID));
        }
    }

    @Test
    public void testDescribeGroupsWithBothUnsupportedApis() throws InterruptedException {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            // The first request sent will be a ConsumerGroupDescribe request. Let's
            // fail it in order to fail back to using the classic version.
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof ConsumerGroupDescribeRequest);

            // Let's also fail the second one.
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof DescribeGroupsRequest);

            DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList(GROUP_ID));
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.describedGroups().get(GROUP_ID));
        }
    }

    @Test
    public void testDescribeOldAndNewConsumerGroups() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(new FindCoordinatorResponse(
                new FindCoordinatorResponseData()
                    .setCoordinators(asList(
                        FindCoordinatorResponse.prepareCoordinatorResponse(Errors.NONE, "grp1", env.cluster().controller()),
                        FindCoordinatorResponse.prepareCoordinatorResponse(Errors.NONE, "grp2", env.cluster().controller())
                    ))
            ));

            env.kafkaClient().prepareResponse(new ConsumerGroupDescribeResponse(
                new ConsumerGroupDescribeResponseData()
                    .setGroups(asList(
                        new ConsumerGroupDescribeResponseData.DescribedGroup()
                            .setGroupId("grp1")
                            .setGroupState("Stable")
                            .setGroupEpoch(10)
                            .setAssignmentEpoch(10)
                            .setAssignorName("range")
                            .setAuthorizedOperations(Utils.to32BitField(emptySet()))
                            .setMembers(singletonList(
                                new ConsumerGroupDescribeResponseData.Member()
                                    .setMemberId("memberId")
                                    .setInstanceId("instanceId")
                                    .setClientHost("host")
                                    .setClientId("clientId")
                                    .setMemberEpoch(10)
                                    .setRackId("rackId")
                                    .setSubscribedTopicNames(singletonList("foo"))
                                    .setSubscribedTopicRegex("regex")
                                    .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
                                        .setTopicPartitions(singletonList(
                                            new ConsumerGroupDescribeResponseData.TopicPartitions()
                                                .setTopicId(Uuid.randomUuid())
                                                .setTopicName("foo")
                                                .setPartitions(singletonList(0))
                                        )))
                                    .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
                                        .setTopicPartitions(singletonList(
                                            new ConsumerGroupDescribeResponseData.TopicPartitions()
                                                .setTopicId(Uuid.randomUuid())
                                                .setTopicName("foo")
                                                .setPartitions(singletonList(1))
                                        )))
                                    .setMemberType((byte) 1)
                            )),
                        new ConsumerGroupDescribeResponseData.DescribedGroup()
                            .setGroupId("grp2")
                            .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
                    ))
            ));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(
                new DescribeGroupsResponseData()
                    .setGroups(Collections.singletonList(
                        DescribeGroupsResponse.groupMetadata(
                            "grp2",
                            Errors.NONE,
                            "Stable",
                            ConsumerProtocol.PROTOCOL_TYPE,
                            "range",
                            singletonList(
                                DescribeGroupsResponse.groupMember(
                                    "0",
                                    null,
                                    "clientId0",
                                    "clientHost",
                                    ConsumerProtocol.serializeAssignment(
                                        new ConsumerPartitionAssignor.Assignment(
                                            Collections.singletonList(new TopicPartition("bar", 0))
                                        )
                                    ).array(),
                                    null
                                )
                            ),
                            Collections.emptySet()
                        )
                    ))
            ));

            DescribeConsumerGroupsResult result = env.adminClient()
                .describeConsumerGroups(asList("grp1", "grp2"));

            Map<String, ConsumerGroupDescription> expectedResult = new HashMap<>();
            expectedResult.put("grp1", new ConsumerGroupDescription(
                "grp1",
                false,
                Collections.singletonList(
                    new MemberDescription(
                        "memberId",
                        Optional.of("instanceId"),
                        Optional.of("rackId"),
                        "clientId",
                        "host",
                        new MemberAssignment(
                            Collections.singleton(new TopicPartition("foo", 0))
                        ),
                        Optional.of(new MemberAssignment(
                            Collections.singleton(new TopicPartition("foo", 1))
                        )),
                        Optional.of(10),
                        Optional.of(true)
                    )
                ),
                "range",
                GroupType.CONSUMER,
                GroupState.STABLE,
                env.cluster().controller(),
                Collections.emptySet(),
                Optional.of(10),
                Optional.of(10)
            ));
            expectedResult.put("grp2", new ConsumerGroupDescription(
                "grp2",
                false,
                Collections.singletonList(
                    new MemberDescription(
                        "0",
                        Optional.empty(),
                        Optional.empty(),
                        "clientId0",
                        "clientHost",
                        new MemberAssignment(
                            Collections.singleton(new TopicPartition("bar", 0))
                        ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()
                    )
                ),
                "range",
                GroupType.CLASSIC,
                GroupState.STABLE,
                env.cluster().controller(),
                Collections.emptySet(),
                Optional.empty(),
                Optional.empty()
            ));

            assertEquals(expectedResult, result.all().get());
        }
    }

    @Test
    public void testListConsumerGroupOffsetsOptionsWithBatchedApi() throws Exception {
        verifyListConsumerGroupOffsetsOptions();
    }

    @Test
    public void testListConsumerGroupOffsetsNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(offsetFetchResponse(Errors.NOT_COORDINATOR));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final ListConsumerGroupOffsetsResult result = env.adminClient().listConsumerGroupOffsets(GROUP_ID);

            TestUtils.assertFutureThrows(TimeoutException.class, result.partitionsToOffsetAndMetadata());
        }
    }

    @Test
    public void testListConsumerGroupOffsetsRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return true;
            }, offsetFetchResponse(Errors.NOT_COORDINATOR));

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return true;
            }, offsetFetchResponse(Errors.NONE));

            final KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> future = env.adminClient().listConsumerGroupOffsets(GROUP_ID).partitionsToOffsetAndMetadata();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting ListConsumerGroupOffsets first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry ListConsumerGroupOffsets call on first failure");

            long lowerBoundBackoffMs = (long) (retryBackoff * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
            long upperBoundBackoffMs = (long) (retryBackoff * CommonClientConfigs.RETRY_BACKOFF_EXP_BASE * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
            time.sleep(upperBoundBackoffMs);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, upperBoundBackoffMs - lowerBoundBackoffMs, "ListConsumerGroupOffsets retry did not await expected backoff!");
        }
    }

    @Test
    public void testListConsumerGroupOffsetsRetriableErrors() throws Exception {
        // Retriable errors should be retried

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                offsetFetchResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS));

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling list consumer offsets
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            env.kafkaClient().prepareResponse(
                offsetFetchResponse(Errors.NOT_COORDINATOR));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                offsetFetchResponse(Errors.COORDINATOR_NOT_AVAILABLE));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                offsetFetchResponse(Errors.NONE));

            final ListConsumerGroupOffsetsResult errorResult1 = env.adminClient().listConsumerGroupOffsets(GROUP_ID);

            assertEquals(Collections.emptyMap(), errorResult1.partitionsToOffsetAndMetadata().get());
        }
    }

    @Test
    public void testListConsumerGroupOffsetsNonRetriableErrors() throws Exception {
        // Non-retriable errors throw an exception
        final List<Errors> nonRetriableErrors = asList(
            Errors.GROUP_AUTHORIZATION_FAILED, Errors.INVALID_GROUP_ID, Errors.GROUP_ID_NOT_FOUND,
            Errors.UNKNOWN_MEMBER_ID, Errors.STALE_MEMBER_EPOCH);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            for (Errors error : nonRetriableErrors) {
                env.kafkaClient().prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

                env.kafkaClient().prepareResponse(offsetFetchResponse(error));

                ListConsumerGroupOffsetsResult errorResult = env.adminClient().listConsumerGroupOffsets(GROUP_ID);

                TestUtils.assertFutureThrows(error.exception().getClass(), errorResult.partitionsToOffsetAndMetadata());
            }
        }
    }

    @Test
    public void testListConsumerGroupOffsets() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode()));

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            // Retriable errors should be retried
            env.kafkaClient().prepareResponse(offsetFetchResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS));

            /*
             * We need to return two responses here, one for NOT_COORDINATOR error when calling list consumer group offsets
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            env.kafkaClient().prepareResponse(offsetFetchResponse(Errors.NOT_COORDINATOR));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(offsetFetchResponse(Errors.COORDINATOR_NOT_AVAILABLE));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);
            TopicPartition myTopicPartition3 = new TopicPartition("my_topic", 3);

            final OffsetFetchResponseData response = new OffsetFetchResponseData()
                .setGroups(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseGroup()
                        .setGroupId(GROUP_ID)
                        .setTopics(List.of(
                            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                                .setName("my_topic")
                                .setPartitions(List.of(
                                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                        .setPartitionIndex(myTopicPartition0.partition())
                                        .setCommittedOffset(10),
                                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                        .setPartitionIndex(myTopicPartition1.partition())
                                        .setCommittedOffset(0),
                                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                        .setPartitionIndex(myTopicPartition2.partition())
                                        .setCommittedOffset(20),
                                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                        .setPartitionIndex(myTopicPartition3.partition())
                                        .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                                ))
                        ))
                ));

            env.kafkaClient().prepareResponse(new OffsetFetchResponse(response, ApiKeys.OFFSET_FETCH.latestVersion()));

            final ListConsumerGroupOffsetsResult result = env.adminClient().listConsumerGroupOffsets(GROUP_ID);
            final Map<TopicPartition, OffsetAndMetadata> partitionToOffsetAndMetadata = result.partitionsToOffsetAndMetadata().get();

            assertEquals(4, partitionToOffsetAndMetadata.size());
            assertEquals(10, partitionToOffsetAndMetadata.get(myTopicPartition0).offset());
            assertEquals(0, partitionToOffsetAndMetadata.get(myTopicPartition1).offset());
            assertEquals(20, partitionToOffsetAndMetadata.get(myTopicPartition2).offset());
            assertTrue(partitionToOffsetAndMetadata.containsKey(myTopicPartition3));
            assertNull(partitionToOffsetAndMetadata.get(myTopicPartition3));
        }
    }

    @Test
    public void testBatchedListConsumerGroupOffsets() throws Exception {
        Cluster cluster = mockCluster(1, 0);
        Time time = new MockTime();
        Map<String, ListConsumerGroupOffsetsSpec> groupSpecs = batchedListConsumerGroupOffsetsSpec();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster, AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareBatchedFindCoordinatorResponse(Errors.NONE, env.cluster().controller(), groupSpecs.keySet()));

            ListConsumerGroupOffsetsResult result = env.adminClient().listConsumerGroupOffsets(groupSpecs, new ListConsumerGroupOffsetsOptions());
            sendOffsetFetchResponse(env.kafkaClient(), groupSpecs, true, Errors.NONE);

            verifyListOffsetsForMultipleGroups(groupSpecs, result);
        }
    }

    @Test
    public void testBatchedListConsumerGroupOffsetsWithNoFindCoordinatorBatching() throws Exception {
        Cluster cluster = mockCluster(1, 0);
        Time time = new MockTime();
        Map<String, ListConsumerGroupOffsetsSpec> groupSpecs = batchedListConsumerGroupOffsetsSpec();

        ApiVersion findCoordinatorV3 = new ApiVersion()
                .setApiKey(ApiKeys.FIND_COORDINATOR.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 3);
        ApiVersion offsetFetchV7 = new ApiVersion()
                .setApiKey(ApiKeys.OFFSET_FETCH.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 7);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster, AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(asList(findCoordinatorV3, offsetFetchV7)));
            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode()));
            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            ListConsumerGroupOffsetsResult result = env.adminClient().listConsumerGroupOffsets(groupSpecs);

            // Fail the first request in order to ensure that the group is not batched when retried.
            sendOffsetFetchResponse(env.kafkaClient(), groupSpecs, false, Errors.COORDINATOR_LOAD_IN_PROGRESS);

            sendOffsetFetchResponse(env.kafkaClient(), groupSpecs, false, Errors.NONE);
            sendOffsetFetchResponse(env.kafkaClient(), groupSpecs, false, Errors.NONE);

            verifyListOffsetsForMultipleGroups(groupSpecs, result);
        }
    }

    @Test
    public void testBatchedListConsumerGroupOffsetsWithNoOffsetFetchBatching() throws Exception {
        Cluster cluster = mockCluster(1, 0);
        Time time = new MockTime();
        Map<String, ListConsumerGroupOffsetsSpec> groupSpecs = batchedListConsumerGroupOffsetsSpec();

        ApiVersion offsetFetchV7 = new ApiVersion()
                .setApiKey(ApiKeys.OFFSET_FETCH.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 7);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster, AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(Collections.singleton(offsetFetchV7)));
            env.kafkaClient().prepareResponse(prepareBatchedFindCoordinatorResponse(Errors.NONE, env.cluster().controller(), groupSpecs.keySet()));
            // Prepare a response to force client to attempt batched request creation that throws
            // NoBatchedOffsetFetchRequestException. This triggers creation of non-batched requests.
            env.kafkaClient().prepareResponse(offsetFetchResponse(Errors.COORDINATOR_NOT_AVAILABLE));

            ListConsumerGroupOffsetsResult result = env.adminClient().listConsumerGroupOffsets(groupSpecs);

            // The request handler attempts both FindCoordinator and OffsetFetch requests. This seems
            // ok since we expect this scenario only during upgrades from versions < 3.0.0 where
            // some upgraded brokers could handle batched FindCoordinator while non-upgraded coordinators
            // rejected batched OffsetFetch requests.
            sendFindCoordinatorResponse(env.kafkaClient(), env.cluster().controller());
            sendFindCoordinatorResponse(env.kafkaClient(), env.cluster().controller());
            sendOffsetFetchResponse(env.kafkaClient(), groupSpecs, false, Errors.NONE);
            sendOffsetFetchResponse(env.kafkaClient(), groupSpecs, false, Errors.NONE);

            verifyListOffsetsForMultipleGroups(groupSpecs, result);
        }
    }

    @Test
    public void testDeleteConsumerGroupsNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();
        final List<String> groupIds = singletonList("groupId");

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            final DeletableGroupResultCollection validResponse = new DeletableGroupResultCollection();
            validResponse.add(new DeletableGroupResult()
                .setGroupId("groupId")
                .setErrorCode(Errors.NOT_COORDINATOR.code()));
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(validResponse)
            ));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final DeleteConsumerGroupsResult result = env.adminClient().deleteConsumerGroups(groupIds);

            TestUtils.assertFutureThrows(TimeoutException.class, result.all());
        }
    }

    @Test
    public void testDeleteConsumerGroupsRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;
        final List<String> groupIds = singletonList(GROUP_ID);

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DeletableGroupResultCollection validResponse = new DeletableGroupResultCollection();
            validResponse.add(new DeletableGroupResult()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.NOT_COORDINATOR.code()));

            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return true;
            }, new DeleteGroupsResponse(new DeleteGroupsResponseData().setResults(validResponse)));

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            validResponse = new DeletableGroupResultCollection();
            validResponse.add(new DeletableGroupResult()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.NONE.code()));

            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return true;
            }, new DeleteGroupsResponse(new DeleteGroupsResponseData().setResults(validResponse)));

            final KafkaFuture<Void> future = env.adminClient().deleteConsumerGroups(groupIds).all();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting DeleteConsumerGroups first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry DeleteConsumerGroups call on first failure");

            long lowerBoundBackoffMs = (long) (retryBackoff * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
            long upperBoundBackoffMs = (long) (retryBackoff * CommonClientConfigs.RETRY_BACKOFF_EXP_BASE * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
            time.sleep(upperBoundBackoffMs);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, upperBoundBackoffMs - lowerBoundBackoffMs, "DeleteConsumerGroups retry did not await expected backoff!");
        }
    }

    @Test
    public void testDeleteConsumerGroupsWithOlderBroker() throws Exception {
        final List<String> groupIds = singletonList("groupId");
        ApiVersion findCoordinatorV3 = new ApiVersion()
                .setApiKey(ApiKeys.FIND_COORDINATOR.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 3);
        ApiVersion describeGroups = new ApiVersion()
                .setApiKey(ApiKeys.DESCRIBE_GROUPS.id)
                .setMinVersion((short) 0)
                .setMaxVersion(ApiKeys.DELETE_GROUPS.latestVersion());

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(asList(findCoordinatorV3, describeGroups)));

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode()));

            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final DeletableGroupResultCollection validResponse = new DeletableGroupResultCollection();
            validResponse.add(new DeletableGroupResult()
                                  .setGroupId("groupId")
                                  .setErrorCode(Errors.NONE.code()));
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(validResponse)
            ));

            final DeleteConsumerGroupsResult result = env.adminClient().deleteConsumerGroups(groupIds);

            final KafkaFuture<Void> results = result.deletedGroups().get("groupId");
            assertNull(results.get());

            // should throw error for non-retriable errors
            env.kafkaClient().prepareResponse(
                prepareOldFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED, Node.noNode()));

            DeleteConsumerGroupsResult errorResult = env.adminClient().deleteConsumerGroups(groupIds);
            TestUtils.assertFutureThrows(GroupAuthorizationException.class, errorResult.deletedGroups().get("groupId"));

            // Retriable errors should be retried
            env.kafkaClient().prepareResponse(
                prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final DeletableGroupResultCollection errorResponse = new DeletableGroupResultCollection();
            errorResponse.add(new DeletableGroupResult()
                                   .setGroupId("groupId")
                                   .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            );
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(errorResponse)));

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling delete a consumer group
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */

            DeletableGroupResultCollection coordinatorMoved = new DeletableGroupResultCollection();
            coordinatorMoved.add(new DeletableGroupResult()
                                     .setGroupId("groupId")
                                     .setErrorCode(Errors.NOT_COORDINATOR.code())
            );

            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(coordinatorMoved)));
            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            coordinatorMoved = new DeletableGroupResultCollection();
            coordinatorMoved.add(new DeletableGroupResult()
                .setGroupId("groupId")
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );

            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(coordinatorMoved)));
            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(validResponse)));

            errorResult = env.adminClient().deleteConsumerGroups(groupIds);

            final KafkaFuture<Void> errorResults = errorResult.deletedGroups().get("groupId");
            assertNull(errorResults.get());
        }
    }

    @Test
    public void testDeleteMultipleConsumerGroupsWithOlderBroker() throws Exception {
        final List<String> groupIds = asList("group1", "group2");
        ApiVersion findCoordinatorV3 = new ApiVersion()
                .setApiKey(ApiKeys.FIND_COORDINATOR.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 3);
        ApiVersion describeGroups = new ApiVersion()
                .setApiKey(ApiKeys.DESCRIBE_GROUPS.id)
                .setMinVersion((short) 0)
                .setMaxVersion(ApiKeys.DELETE_GROUPS.latestVersion());

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(
                    NodeApiVersions.create(asList(findCoordinatorV3, describeGroups)));

            // Dummy response for MockClient to handle the UnsupportedVersionException correctly to switch from batched to un-batched
            env.kafkaClient().prepareResponse(null);
            // Retriable FindCoordinatorResponse errors should be retried
            for (int i = 0; i < groupIds.size(); i++) {
                env.kafkaClient().prepareResponse(
                        prepareOldFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode()));
            }
            for (int i = 0; i < groupIds.size(); i++) {
                env.kafkaClient().prepareResponse(
                        prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            }

            final DeletableGroupResultCollection validResponse = new DeletableGroupResultCollection();
            validResponse.add(new DeletableGroupResult()
                    .setGroupId("group1")
                    .setErrorCode(Errors.NONE.code()));
            validResponse.add(new DeletableGroupResult()
                    .setGroupId("group2")
                    .setErrorCode(Errors.NONE.code()));
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                    new DeleteGroupsResponseData()
                            .setResults(validResponse)
            ));

            final DeleteConsumerGroupsResult result = env.adminClient()
                    .deleteConsumerGroups(groupIds);

            final KafkaFuture<Void> results = result.deletedGroups().get("group1");
            assertNull(results.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            final TopicPartition tp1 = new TopicPartition("foo", 0);

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(prepareOffsetDeleteResponse(Errors.NOT_COORDINATOR));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final DeleteConsumerGroupOffsetsResult result = env.adminClient()
                .deleteConsumerGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

            TestUtils.assertFutureThrows(TimeoutException.class, result.all());
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            final TopicPartition tp1 = new TopicPartition("foo", 0);

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return true;
            }, prepareOffsetDeleteResponse(Errors.NOT_COORDINATOR));

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return true;
            }, prepareOffsetDeleteResponse("foo", 0, Errors.NONE));

            final KafkaFuture<Void> future = env.adminClient().deleteConsumerGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet())).all();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting DeleteConsumerGroupOffsets first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry DeleteConsumerGroupOffsets call on first failure");

            long lowerBoundBackoffMs = (long) (retryBackoff * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
            long upperBoundBackoffMs = (long) (retryBackoff * CommonClientConfigs.RETRY_BACKOFF_EXP_BASE * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
            time.sleep(upperBoundBackoffMs);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, upperBoundBackoffMs - lowerBoundBackoffMs, "DeleteConsumerGroupOffsets retry did not await expected backoff!");
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsets() throws Exception {
        // Happy path

        final TopicPartition tp1 = new TopicPartition("foo", 0);
        final TopicPartition tp2 = new TopicPartition("bar", 0);
        final TopicPartition tp3 = new TopicPartition("foobar", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(new OffsetDeleteResponse(
                new OffsetDeleteResponseData()
                    .setTopics(new OffsetDeleteResponseTopicCollection(Stream.of(
                        new OffsetDeleteResponseTopic()
                            .setName("foo")
                            .setPartitions(new OffsetDeleteResponsePartitionCollection(Collections.singletonList(
                                new OffsetDeleteResponsePartition()
                                    .setPartitionIndex(0)
                                    .setErrorCode(Errors.NONE.code())
                            ))),
                        new OffsetDeleteResponseTopic()
                            .setName("bar")
                            .setPartitions(new OffsetDeleteResponsePartitionCollection(Collections.singletonList(
                                new OffsetDeleteResponsePartition()
                                    .setPartitionIndex(0)
                                    .setErrorCode(Errors.GROUP_SUBSCRIBED_TO_TOPIC.code())
                            )))
                    ).collect(Collectors.toList())))
                )
            );

            final DeleteConsumerGroupOffsetsResult errorResult = env.adminClient().deleteConsumerGroupOffsets(
                GROUP_ID, Stream.of(tp1, tp2).collect(Collectors.toSet()));

            assertNull(errorResult.partitionResult(tp1).get());
            TestUtils.assertFutureThrows(GroupSubscribedToTopicException.class, errorResult.all());
            TestUtils.assertFutureThrows(GroupSubscribedToTopicException.class, errorResult.partitionResult(tp2));
            assertThrows(IllegalArgumentException.class, () -> errorResult.partitionResult(tp3));
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsRetriableErrors() throws Exception {
        // Retriable errors should be retried

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS));

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling delete a consumer group
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse(Errors.NOT_COORDINATOR));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse(Errors.COORDINATOR_NOT_AVAILABLE));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse("foo", 0, Errors.NONE));

            final DeleteConsumerGroupOffsetsResult errorResult1 = env.adminClient()
                .deleteConsumerGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

            assertNull(errorResult1.all().get());
            assertNull(errorResult1.partitionResult(tp1).get());
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsNonRetriableErrors() throws Exception {
        // Non-retriable errors throw an exception

        final TopicPartition tp1 = new TopicPartition("foo", 0);
        final List<Errors> nonRetriableErrors = asList(
            Errors.GROUP_AUTHORIZATION_FAILED, Errors.INVALID_GROUP_ID, Errors.GROUP_ID_NOT_FOUND);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            for (Errors error : nonRetriableErrors) {
                env.kafkaClient().prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

                env.kafkaClient().prepareResponse(
                    prepareOffsetDeleteResponse(error));

                DeleteConsumerGroupOffsetsResult errorResult = env.adminClient()
                    .deleteConsumerGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

                TestUtils.assertFutureThrows(error.exception().getClass(), errorResult.all());
                TestUtils.assertFutureThrows(error.exception().getClass(), errorResult.partitionResult(tp1));
            }
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsFindCoordinatorRetriableErrors() throws Exception {
        // Retriable FindCoordinatorResponse errors should be retried

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode()));
            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode()));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse("foo", 0, Errors.NONE));

            final DeleteConsumerGroupOffsetsResult result = env.adminClient()
                .deleteConsumerGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

            assertNull(result.all().get());
            assertNull(result.partitionResult(tp1).get());
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsFindCoordinatorNonRetriableErrors() throws Exception {
        // Non-retriable FindCoordinatorResponse errors throw an exception

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED,  Node.noNode()));

            final DeleteConsumerGroupOffsetsResult errorResult = env.adminClient()
                .deleteConsumerGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

            TestUtils.assertFutureThrows(GroupAuthorizationException.class, errorResult.all());
            TestUtils.assertFutureThrows(GroupAuthorizationException.class, errorResult.partitionResult(tp1));
        }
    }

    @Test
    public void testDescribeClassicGroups() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();

            // Retriable errors should be retried
            data.groups().add(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            /*
             * We need to return two responses here, one with NOT_COORDINATOR error when calling describe classic group
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for COORDINATOR_NOT_AVAILABLE error response
             */
            data = new DescribeGroupsResponseData();
            data.groups().add(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.NOT_COORDINATOR.code()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            data = new DescribeGroupsResponseData();
            data.groups().add(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final List<TopicPartition> topicPartitions = List.of(
                new TopicPartition("my_topic", 0),
                new TopicPartition("my_topic", 1),
                new TopicPartition("my_topic", 2));
            final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(topicPartitions));
            final byte[] memberAssignmentBytes = new byte[memberAssignment.remaining()];
            memberAssignment.get(memberAssignmentBytes);

            data = new DescribeGroupsResponseData();
            DescribeGroupsResponseData.DescribedGroupMember memberOne = new DescribeGroupsResponseData.DescribedGroupMember()
                .setMemberId("0")
                .setClientId("clientId0")
                .setClientHost("clientHost")
                .setMemberAssignment(memberAssignmentBytes);
            DescribeGroupsResponseData.DescribedGroupMember memberTwo = new DescribeGroupsResponseData.DescribedGroupMember()
                .setMemberId("1")
                .setClientId("clientId1")
                .setClientHost("clientHost")
                .setGroupInstanceId("static")
                .setMemberAssignment(memberAssignmentBytes);

            final List<TopicPartition> expectedTopicPartitions = new ArrayList<>();
            expectedTopicPartitions.add(0, new TopicPartition("my_topic", 0));
            expectedTopicPartitions.add(1, new TopicPartition("my_topic", 1));
            expectedTopicPartitions.add(2, new TopicPartition("my_topic", 2));

            List<MemberDescription> expectedMemberDescriptions = new ArrayList<>();
            expectedMemberDescriptions.add(convertToMemberDescriptions(memberOne,
                new MemberAssignment(new HashSet<>(expectedTopicPartitions))));
            expectedMemberDescriptions.add(convertToMemberDescriptions(memberTwo,
                new MemberAssignment(new HashSet<>(expectedTopicPartitions))));
            data.groups().add(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState(ClassicGroupState.STABLE.toString())
                .setMembers(List.of(memberOne, memberTwo)));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeClassicGroupsResult result = env.adminClient().describeClassicGroups(List.of(GROUP_ID));
            final ClassicGroupDescription groupDescription = result.describedGroups().get(GROUP_ID).get();

            assertEquals(1, result.describedGroups().size());
            assertEquals(GROUP_ID, groupDescription.groupId());
            assertEquals(2, groupDescription.members().size());
            assertEquals(expectedMemberDescriptions, groupDescription.members());
        }
    }

    @Test
    public void testDescribeClassicGroupsWithAuthorizedOperationsOmitted() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();

            data.groups().add(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setProtocolType("")
                .setAuthorizedOperations(MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeClassicGroupsResult result = env.adminClient().describeClassicGroups(List.of(GROUP_ID));
            final ClassicGroupDescription groupDescription = result.describedGroups().get(GROUP_ID).get();

            assertNull(groupDescription.authorizedOperations());
        }
    }

    @Test
    public void testDescribeMultipleClassicGroups() {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final List<TopicPartition> topicPartitions = List.of(
                new TopicPartition("my_topic", 0),
                new TopicPartition("my_topic", 1),
                new TopicPartition("my_topic", 2));
            final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(topicPartitions));
            final byte[] memberAssignmentBytes = new byte[memberAssignment.remaining()];
            memberAssignment.get(memberAssignmentBytes);

            DescribeGroupsResponseData group0Data = new DescribeGroupsResponseData();
            group0Data.groups().add(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                .setGroupState(ClassicGroupState.STABLE.toString())
                .setMembers(List.of(
                    new DescribeGroupsResponseData.DescribedGroupMember()
                        .setMemberId("0")
                        .setClientId("clientId0")
                        .setClientHost("clientHost")
                        .setMemberAssignment(memberAssignmentBytes),
                    new DescribeGroupsResponseData.DescribedGroupMember()
                        .setMemberId("1")
                        .setClientId("clientId1")
                        .setClientHost("clientHost")
                        .setMemberAssignment(memberAssignmentBytes))));

            DescribeGroupsResponseData group1Data = new DescribeGroupsResponseData();
            group1Data.groups().add(new DescribeGroupsResponseData.DescribedGroup()
                .setGroupId("group-1")
                .setProtocolType("other")
                .setGroupState(ClassicGroupState.STABLE.toString())
                .setMembers(List.of(
                    new DescribeGroupsResponseData.DescribedGroupMember()
                        .setMemberId("0")
                        .setClientId("clientId0")
                        .setClientHost("clientHost"),
                    new DescribeGroupsResponseData.DescribedGroupMember()
                        .setMemberId("1")
                        .setClientId("clientId1")
                        .setClientHost("clientHost"))));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(group0Data));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(group1Data));

            Collection<String> groups = new HashSet<>();
            groups.add(GROUP_ID);
            groups.add("group-1");
            final DescribeClassicGroupsResult result = env.adminClient().describeClassicGroups(groups);
            assertEquals(2, result.describedGroups().size());
            assertEquals(groups, result.describedGroups().keySet());
        }
    }

    @Test
    public void testRemoveMembersFromGroupNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NOT_COORDINATOR.code())));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            Collection<MemberToRemove> membersToRemove = asList(new MemberToRemove("instance-1"), new MemberToRemove("instance-2"));

            final RemoveMembersFromConsumerGroupResult result = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID, new RemoveMembersFromConsumerGroupOptions(membersToRemove));

            TestUtils.assertFutureThrows(TimeoutException.class, result.all());
        }
    }

    @Test
    public void testRemoveMembersFromGroupRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return true;
            }, new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NOT_COORDINATOR.code())));

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            MemberResponse responseOne = new MemberResponse()
                .setGroupInstanceId("instance-1")
                .setErrorCode(Errors.NONE.code());
            env.kafkaClient().prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return true;
            }, new LeaveGroupResponse(new LeaveGroupResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMembers(Collections.singletonList(responseOne))));

            Collection<MemberToRemove> membersToRemove = singletonList(new MemberToRemove("instance-1"));

            final KafkaFuture<Void> future = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID, new RemoveMembersFromConsumerGroupOptions(membersToRemove)).all();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting RemoveMembersFromGroup first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry RemoveMembersFromGroup call on first failure");

            long lowerBoundBackoffMs = (long) (retryBackoff * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
            long upperBoundBackoffMs = (long) (retryBackoff * CommonClientConfigs.RETRY_BACKOFF_EXP_BASE * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
            time.sleep(upperBoundBackoffMs);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, upperBoundBackoffMs - lowerBoundBackoffMs, "RemoveMembersFromGroup retry did not await expected backoff!");
        }
    }

    @Test
    public void testRemoveMembersFromGroupRetriableErrors() throws Exception {
        // Retriable errors should be retried

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                    new LeaveGroupResponse(new LeaveGroupResponseData()
                        .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())));

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling remove member
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            env.kafkaClient().prepareResponse(
                    new LeaveGroupResponse(new LeaveGroupResponseData()
                            .setErrorCode(Errors.NOT_COORDINATOR.code())));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                new LeaveGroupResponse(new LeaveGroupResponseData()
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            MemberResponse memberResponse = new MemberResponse()
                    .setGroupInstanceId("instance-1")
                    .setErrorCode(Errors.NONE.code());
            env.kafkaClient().prepareResponse(
                    new LeaveGroupResponse(new LeaveGroupResponseData()
                            .setErrorCode(Errors.NONE.code())
                            .setMembers(Collections.singletonList(memberResponse))));

            MemberToRemove memberToRemove = new MemberToRemove("instance-1");
            Collection<MemberToRemove> membersToRemove = singletonList(memberToRemove);

            final RemoveMembersFromConsumerGroupResult result = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID, new RemoveMembersFromConsumerGroupOptions(membersToRemove));

            assertNull(result.all().get());
            assertNull(result.memberResult(memberToRemove).get());
        }
    }

    @Test
    public void testRemoveMembersFromGroupNonRetriableErrors() throws Exception {
        // Non-retriable errors throw an exception

        final List<Errors> nonRetriableErrors = asList(
            Errors.GROUP_AUTHORIZATION_FAILED, Errors.INVALID_GROUP_ID, Errors.GROUP_ID_NOT_FOUND);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            for (Errors error : nonRetriableErrors) {
                env.kafkaClient().prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

                env.kafkaClient().prepareResponse(
                        new LeaveGroupResponse(new LeaveGroupResponseData()
                                .setErrorCode(error.code())));

                MemberToRemove memberToRemove = new MemberToRemove("instance-1");
                Collection<MemberToRemove> membersToRemove = singletonList(memberToRemove);

                final RemoveMembersFromConsumerGroupResult result = env.adminClient().removeMembersFromConsumerGroup(
                    GROUP_ID, new RemoveMembersFromConsumerGroupOptions(membersToRemove));

                TestUtils.assertFutureThrows(error.exception().getClass(), result.all());
                TestUtils.assertFutureThrows(error.exception().getClass(), result.memberResult(memberToRemove));
            }
        }
    }

    @Test
    public void testRemoveMembersFromGroup() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            final String instanceOne = "instance-1";
            final String instanceTwo = "instance-2";

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            // Retriable errors should be retried
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())));

            // Inject a top-level non-retriable error
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())));

            Collection<MemberToRemove> membersToRemove = asList(new MemberToRemove(instanceOne),
                                                                       new MemberToRemove(instanceTwo));
            final RemoveMembersFromConsumerGroupResult unknownErrorResult = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID,
                new RemoveMembersFromConsumerGroupOptions(membersToRemove)
            );

            MemberToRemove memberOne = new MemberToRemove(instanceOne);
            MemberToRemove memberTwo = new MemberToRemove(instanceTwo);

            TestUtils.assertFutureThrows(UnknownServerException.class, unknownErrorResult.memberResult(memberOne));
            TestUtils.assertFutureThrows(UnknownServerException.class, unknownErrorResult.memberResult(memberTwo));

            MemberResponse responseOne = new MemberResponse()
                                             .setGroupInstanceId(instanceOne)
                                             .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code());

            MemberResponse responseTwo = new MemberResponse()
                                             .setGroupInstanceId(instanceTwo)
                                             .setErrorCode(Errors.NONE.code());

            // Inject one member level error.
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.NONE.code())
                                                                         .setMembers(asList(responseOne, responseTwo))));

            final RemoveMembersFromConsumerGroupResult memberLevelErrorResult = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID,
                new RemoveMembersFromConsumerGroupOptions(membersToRemove)
            );

            TestUtils.assertFutureThrows(UnknownMemberIdException.class, memberLevelErrorResult.all());
            TestUtils.assertFutureThrows(UnknownMemberIdException.class, memberLevelErrorResult.memberResult(memberOne));
            assertNull(memberLevelErrorResult.memberResult(memberTwo).get());

            // Return with missing member.
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.NONE.code())
                                                                         .setMembers(Collections.singletonList(responseTwo))));

            final RemoveMembersFromConsumerGroupResult missingMemberResult = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID,
                new RemoveMembersFromConsumerGroupOptions(membersToRemove)
            );

            TestUtils.assertFutureThrows(IllegalArgumentException.class, missingMemberResult.all());
            // The memberOne was not included in the response.
            TestUtils.assertFutureThrows(IllegalArgumentException.class, missingMemberResult.memberResult(memberOne));
            assertNull(missingMemberResult.memberResult(memberTwo).get());

            // Return with success.
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(
                    new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()).setMembers(
                        asList(responseTwo,
                                      new MemberResponse().setGroupInstanceId(instanceOne).setErrorCode(Errors.NONE.code())
                        ))
            ));

            final RemoveMembersFromConsumerGroupResult noErrorResult = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID,
                new RemoveMembersFromConsumerGroupOptions(membersToRemove)
            );
            assertNull(noErrorResult.all().get());
            assertNull(noErrorResult.memberResult(memberOne).get());
            assertNull(noErrorResult.memberResult(memberTwo).get());

            // Test the "removeAll" scenario
            final List<TopicPartition> topicPartitions = Stream.of(1, 2, 3).map(partition -> new TopicPartition("my_topic", partition))
                    .collect(Collectors.toList());
            // construct the DescribeGroupsResponse
            DescribeGroupsResponseData data = prepareDescribeGroupsResponseData(GROUP_ID, asList(instanceOne, instanceTwo), topicPartitions);

            // Return with partial failure for "removeAll" scenario
            // 1 prepare response for AdminClient.describeConsumerGroups
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            // 2 KafkaAdminClient encounter partial failure when trying to delete all members
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(
                    new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()).setMembers(
                            asList(responseOne, responseTwo))
            ));
            final RemoveMembersFromConsumerGroupResult partialFailureResults = env.adminClient().removeMembersFromConsumerGroup(
                    GROUP_ID,
                    new RemoveMembersFromConsumerGroupOptions()
            );
            ExecutionException exception = assertThrows(ExecutionException.class, () -> partialFailureResults.all().get());
            assertInstanceOf(KafkaException.class, exception.getCause());
            assertInstanceOf(UnknownMemberIdException.class, exception.getCause().getCause());

            // Return with success for "removeAll" scenario
            // 1 prepare response for AdminClient.describeConsumerGroups
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            // 2. KafkaAdminClient should delete all members correctly
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(
                    new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()).setMembers(
                            asList(responseTwo,
                                    new MemberResponse().setGroupInstanceId(instanceOne).setErrorCode(Errors.NONE.code())
                            ))
            ));
            final RemoveMembersFromConsumerGroupResult successResult = env.adminClient().removeMembersFromConsumerGroup(
                    GROUP_ID,
                    new RemoveMembersFromConsumerGroupOptions()
            );
            assertNull(successResult.all().get());
        }
    }

    @Test
    public void testRemoveMembersFromGroupReason() throws Exception {
        testRemoveMembersFromGroup("testing remove members reason", "testing remove members reason");
    }

    @Test
    public void testRemoveMembersFromGroupTruncatesReason() throws Exception {
        final String reason = "Very looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong reason that is 271 characters long to make sure that length limit logic handles the scenario nicely";
        final String truncatedReason = reason.substring(0, 255);
        testRemoveMembersFromGroup(reason, truncatedReason);
    }

    @Test
    public void testRemoveMembersFromGroupDefaultReason() throws Exception {
        testRemoveMembersFromGroup(null, DEFAULT_LEAVE_GROUP_REASON);
        testRemoveMembersFromGroup("", DEFAULT_LEAVE_GROUP_REASON);
    }

    @Test
    public void testAlterConsumerGroupOffsets() throws Exception {
        // Happy path

        final TopicPartition tp1 = new TopicPartition("foo", 0);
        final TopicPartition tp2 = new TopicPartition("bar", 0);
        final TopicPartition tp3 = new TopicPartition("foobar", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            Map<TopicPartition, Errors> responseData = new HashMap<>();
            responseData.put(tp1, Errors.NONE);
            responseData.put(tp2, Errors.NONE);
            env.kafkaClient().prepareResponse(new OffsetCommitResponse(0, responseData));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1, new OffsetAndMetadata(123L));
            offsets.put(tp2, new OffsetAndMetadata(456L));
            final AlterConsumerGroupOffsetsResult result = env.adminClient().alterConsumerGroupOffsets(
                GROUP_ID, offsets);

            assertNull(result.all().get());
            assertNull(result.partitionResult(tp1).get());
            assertNull(result.partitionResult(tp2).get());
            TestUtils.assertFutureThrows(IllegalArgumentException.class, result.partitionResult(tp3));
        }
    }

    @Test
    public void testAlterConsumerGroupOffsetsRetriableErrors() throws Exception {
        // Retriable errors should be retried

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.COORDINATOR_NOT_AVAILABLE));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.COORDINATOR_LOAD_IN_PROGRESS));

            env.kafkaClient().prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.NOT_COORDINATOR));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.REBALANCE_IN_PROGRESS));

            env.kafkaClient().prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.NONE));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1, new OffsetAndMetadata(123L));
            final AlterConsumerGroupOffsetsResult result1 = env.adminClient()
                .alterConsumerGroupOffsets(GROUP_ID, offsets);

            assertNull(result1.all().get());
            assertNull(result1.partitionResult(tp1).get());
        }
    }

    @Test
    public void testAlterConsumerGroupOffsetsNonRetriableErrors() throws Exception {
        // Non-retriable errors throw an exception

        final TopicPartition tp1 = new TopicPartition("foo", 0);
        final List<Errors> nonRetriableErrors = asList(
            Errors.GROUP_AUTHORIZATION_FAILED, Errors.INVALID_GROUP_ID, Errors.GROUP_ID_NOT_FOUND, Errors.STALE_MEMBER_EPOCH);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            for (Errors error : nonRetriableErrors) {
                env.kafkaClient().prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

                env.kafkaClient().prepareResponse(prepareOffsetCommitResponse(tp1, error));

                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsets.put(tp1,  new OffsetAndMetadata(123L));
                AlterConsumerGroupOffsetsResult errorResult = env.adminClient()
                    .alterConsumerGroupOffsets(GROUP_ID, offsets);

                TestUtils.assertFutureThrows(error.exception().getClass(), errorResult.all());
                TestUtils.assertFutureThrows(error.exception().getClass(), errorResult.partitionResult(tp1));
            }
        }
    }

    @Test
    public void testAlterConsumerGroupOffsetsFindCoordinatorRetriableErrors() throws Exception {
        // Retriable FindCoordinatorResponse errors should be retried

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode()));
            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode()));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.NONE));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1,  new OffsetAndMetadata(123L));
            final AlterConsumerGroupOffsetsResult result = env.adminClient()
                .alterConsumerGroupOffsets(GROUP_ID, offsets);

            assertNull(result.all().get());
            assertNull(result.partitionResult(tp1).get());
        }
    }

    @Test
    public void testAlterConsumerGroupOffsetsFindCoordinatorNonRetriableErrors() throws Exception {
        // Non-retriable FindCoordinatorResponse errors throw an exception

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED,  Node.noNode()));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1,  new OffsetAndMetadata(123L));
            final AlterConsumerGroupOffsetsResult errorResult = env.adminClient()
                .alterConsumerGroupOffsets(GROUP_ID, offsets);

            TestUtils.assertFutureThrows(GroupAuthorizationException.class, errorResult.all());
            TestUtils.assertFutureThrows(GroupAuthorizationException.class, errorResult.partitionResult(tp1));
        }
    }

    private MockClient.RequestMatcher expectListGroupsRequestWithFilters(
        Set<String> expectedStates,
        Set<String> expectedTypes
    ) {
        return body -> {
            if (body instanceof ListGroupsRequest) {
                ListGroupsRequest request = (ListGroupsRequest) body;
                return Objects.equals(new HashSet<>(request.data().statesFilter()), expectedStates)
                    && Objects.equals(new HashSet<>(request.data().typesFilter()), expectedTypes);
            }
            return false;
        };
    }

    private void verifyListConsumerGroupOffsetsOptions() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final List<TopicPartition> partitions = Collections.singletonList(new TopicPartition("A", 0));
            final ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions()
                    .requireStable(true)
                    .timeoutMs(300);

            final ListConsumerGroupOffsetsSpec groupSpec = new ListConsumerGroupOffsetsSpec()
                    .topicPartitions(partitions);
            env.adminClient().listConsumerGroupOffsets(Collections.singletonMap(GROUP_ID, groupSpec), options);

            final MockClient mockClient = env.kafkaClient();
            waitForRequest(mockClient, ApiKeys.OFFSET_FETCH);

            ClientRequest clientRequest = mockClient.requests().peek();
            assertNotNull(clientRequest);
            assertEquals(300, clientRequest.requestTimeoutMs());
            OffsetFetchRequestData data = ((OffsetFetchRequest.Builder) clientRequest.requestBuilder()).build().data();
            assertTrue(data.requireStable());
            assertEquals(Collections.singletonList(GROUP_ID),
                    data.groups().stream().map(OffsetFetchRequestGroup::groupId).collect(Collectors.toList()));
            assertEquals(Collections.singletonList("A"),
                    data.groups().get(0).topics().stream().map(OffsetFetchRequestTopics::name).collect(Collectors.toList()));
            assertEquals(Collections.singletonList(0),
                    data.groups().get(0).topics().get(0).partitionIndexes());
        }
    }

    private Map<String, ListConsumerGroupOffsetsSpec> batchedListConsumerGroupOffsetsSpec() {
        Set<TopicPartition> groupAPartitions = Collections.singleton(new TopicPartition("A", 1));
        Set<TopicPartition> groupBPartitions =  Collections.singleton(new TopicPartition("B", 2));

        ListConsumerGroupOffsetsSpec groupASpec = new ListConsumerGroupOffsetsSpec().topicPartitions(groupAPartitions);
        ListConsumerGroupOffsetsSpec groupBSpec = new ListConsumerGroupOffsetsSpec().topicPartitions(groupBPartitions);
        return Map.of("groupA", groupASpec, "groupB", groupBSpec);
    }

    private void sendOffsetFetchResponse(MockClient mockClient, Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, boolean batched, Errors error) throws Exception {
        waitForRequest(mockClient, ApiKeys.OFFSET_FETCH);

        ClientRequest clientRequest = mockClient.requests().peek();
        OffsetFetchRequestData data = ((OffsetFetchRequest.Builder) clientRequest.requestBuilder()).build().data();

        if (!batched) {
            assertEquals(1, data.groups().size());
        }

        OffsetFetchResponseData response = new OffsetFetchResponseData()
            .setGroups(data.groups().stream().map(group ->
                new OffsetFetchResponseData.OffsetFetchResponseGroup()
                    .setGroupId(group.groupId())
                    .setErrorCode(error.code())
                    .setTopics(groupSpecs.get(group.groupId()).topicPartitions().stream()
                        .collect(Collectors.groupingBy(TopicPartition::topic)).entrySet().stream().map(entry ->
                            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                                .setName(entry.getKey())
                                .setPartitions(entry.getValue().stream().map(partition ->
                                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                        .setPartitionIndex(partition.partition())
                                        .setCommittedOffset(10)
                                ).collect(Collectors.toList()))
                        ).collect(Collectors.toList()))
            ).collect(Collectors.toList()));

        mockClient.respond(new OffsetFetchResponse(response, ApiKeys.OFFSET_FETCH.latestVersion()));
    }

    private void verifyListOffsetsForMultipleGroups(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs,
                                                    ListConsumerGroupOffsetsResult result) throws Exception {
        assertEquals(groupSpecs.size(), result.all().get(10, TimeUnit.SECONDS).size());
        for (Map.Entry<String, ListConsumerGroupOffsetsSpec> entry : groupSpecs.entrySet()) {
            assertEquals(entry.getValue().topicPartitions(),
                    result.partitionsToOffsetAndMetadata(entry.getKey()).get().keySet());
        }
    }

    private void testRemoveMembersFromGroup(String reason, String expectedReason) throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster)) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(body -> {
                if (!(body instanceof LeaveGroupRequest)) {
                    return false;
                }
                LeaveGroupRequestData leaveGroupRequest = ((LeaveGroupRequest) body).data();

                return leaveGroupRequest.members().stream().allMatch(
                    member -> member.reason().equals(expectedReason)
                );
            }, new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()).setMembers(
                asList(
                    new MemberResponse().setGroupInstanceId("instance-1"),
                    new MemberResponse().setGroupInstanceId("instance-2")
                ))
            ));

            MemberToRemove memberToRemove1 = new MemberToRemove("instance-1");
            MemberToRemove memberToRemove2 = new MemberToRemove("instance-2");

            RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions(asList(
                memberToRemove1,
                memberToRemove2
            ));
            options.reason(reason);

            final RemoveMembersFromConsumerGroupResult result = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID,
                options
            );

            assertNull(result.all().get());
            assertNull(result.memberResult(memberToRemove1).get());
            assertNull(result.memberResult(memberToRemove2).get());
        }
    }

    private static MemberDescription convertToMemberDescriptions(DescribedGroupMember member,
                                                                 MemberAssignment assignment) {
        return new MemberDescription(member.memberId(),
                                     Optional.ofNullable(member.groupInstanceId()),
                                     Optional.empty(),
                                     member.clientId(),
                                     member.clientHost(),
                                     assignment,
                                     Optional.empty(),
                                     Optional.empty(),
                                     Optional.empty());
    }

    private static DescribeGroupsResponseData prepareDescribeGroupsResponseData(String groupId,
                                                                                List<String> groupInstances,
                                                                                List<TopicPartition> topicPartitions) {
        final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(topicPartitions));
        List<DescribedGroupMember> describedGroupMembers = groupInstances.stream().map(groupInstance -> DescribeGroupsResponse.groupMember(JoinGroupRequest.UNKNOWN_MEMBER_ID,
                groupInstance, "clientId0", "clientHost", new byte[memberAssignment.remaining()], null)).collect(Collectors.toList());
        DescribeGroupsResponseData data = new DescribeGroupsResponseData();
        data.groups().add(DescribeGroupsResponse.groupMetadata(
                groupId,
                Errors.NONE,
                "",
                ConsumerProtocol.PROTOCOL_TYPE,
                "",
                describedGroupMembers,
                Collections.emptySet()));
        return data;
    }
}
