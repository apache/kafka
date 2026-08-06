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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData.ListedGroup;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.StreamsGroupDescribeResponse;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaAdminClientStreamsGroupTest extends KafkaAdminClientTestBase {

    @Test
    public void testStreamsOffsetCommitNumRetries() throws Exception {
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
            final AlterStreamsGroupOffsetsResult result = env.adminClient().alterStreamsGroupOffsets(GROUP_ID, offsets);

            TestUtils.assertFutureThrows(TimeoutException.class, result.all());
        }
    }

    @Test
    public void testStreamsOffsetCommitWithMultipleErrors() throws Exception {
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
            final AlterStreamsGroupOffsetsResult result = env.adminClient()
                .alterStreamsGroupOffsets(GROUP_ID, offsets);

            assertNull(result.partitionResult(foo0).get());
            TestUtils.assertFutureThrows(UnknownTopicOrPartitionException.class, result.partitionResult(foo1));

            TestUtils.assertFutureThrows(UnknownTopicOrPartitionException.class, result.all());
        }
    }

    @Test
    public void testStreamsOffsetCommitRetryBackoff() throws Exception {
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
            final KafkaFuture<Void> future = env.adminClient().alterStreamsGroupOffsets(GROUP_ID, offsets).all();

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
    public void testBatchedListStreamsGroupOffsets() throws Exception {
        Cluster cluster = mockCluster(1, 0);
        Time time = new MockTime();
        Map<String, ListStreamsGroupOffsetsSpec> groupSpecs = batchedListStreamsGroupOffsetsSpec();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster, AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareBatchedFindCoordinatorResponse(Errors.NONE, env.cluster().controller(), groupSpecs.keySet()));

            ListStreamsGroupOffsetsResult result = env.adminClient().listStreamsGroupOffsets(groupSpecs, new ListStreamsGroupOffsetsOptions());
            sendStreamsOffsetFetchResponse(env.kafkaClient(), groupSpecs, true, Errors.NONE);

            verifyListStreamsOffsetsForMultipleGroups(groupSpecs, result);
        }
    }

    @Test
    public void testBatchedListStreamsGroupOffsetsWithNoFindCoordinatorBatching() throws Exception {
        Cluster cluster = mockCluster(1, 0);
        Time time = new MockTime();
        Map<String, ListStreamsGroupOffsetsSpec> groupSpecs = batchedListStreamsGroupOffsetsSpec();

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

            ListStreamsGroupOffsetsResult result = env.adminClient().listStreamsGroupOffsets(groupSpecs);

            // Fail the first request in order to ensure that the group is not batched when retried.
            sendStreamsOffsetFetchResponse(env.kafkaClient(), groupSpecs, false, Errors.COORDINATOR_LOAD_IN_PROGRESS);

            sendStreamsOffsetFetchResponse(env.kafkaClient(), groupSpecs, false, Errors.NONE);
            sendStreamsOffsetFetchResponse(env.kafkaClient(), groupSpecs, false, Errors.NONE);

            verifyListStreamsOffsetsForMultipleGroups(groupSpecs, result);
        }
    }

    @Test
    public void testBatchedListStreamsGroupOffsetsWithNoOffsetFetchBatching() throws Exception {
        Cluster cluster = mockCluster(1, 0);
        Time time = new MockTime();
        Map<String, ListStreamsGroupOffsetsSpec> groupSpecs = batchedListStreamsGroupOffsetsSpec();

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

            ListStreamsGroupOffsetsResult result = env.adminClient().listStreamsGroupOffsets(groupSpecs);

            // The request handler attempts both FindCoordinator and OffsetFetch requests. This seems
            // ok since we expect this scenario only during upgrades from versions < 3.0.0 where
            // some upgraded brokers could handle batched FindCoordinator while non-upgraded coordinators
            // rejected batched OffsetFetch requests.
            sendFindCoordinatorResponse(env.kafkaClient(), env.cluster().controller());
            sendFindCoordinatorResponse(env.kafkaClient(), env.cluster().controller());
            sendStreamsOffsetFetchResponse(env.kafkaClient(), groupSpecs, false, Errors.NONE);
            sendStreamsOffsetFetchResponse(env.kafkaClient(), groupSpecs, false, Errors.NONE);

            verifyListStreamsOffsetsForMultipleGroups(groupSpecs, result);
        }
    }

    @Test
    public void testDeleteStreamsGroupsNumRetries() throws Exception {
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

            final DeleteStreamsGroupsResult result = env.adminClient().deleteStreamsGroups(groupIds);

            TestUtils.assertFutureThrows(TimeoutException.class, result.all());
        }
    }

    @Test
    public void testDeleteStreamsGroupsRetryBackoff() throws Exception {
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

            final KafkaFuture<Void> future = env.adminClient().deleteStreamsGroups(groupIds).all();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting DeleteStreamsGroups first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry DeleteStreamsGroups call on first failure");

            long lowerBoundBackoffMs = (long) (retryBackoff * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
            long upperBoundBackoffMs = (long) (retryBackoff * CommonClientConfigs.RETRY_BACKOFF_EXP_BASE * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
            time.sleep(upperBoundBackoffMs);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, upperBoundBackoffMs - lowerBoundBackoffMs, "DeleteConsumerGroups retry did not await expected backoff!");
        }
    }

    @Test
    public void testDeleteStreamsGroupsWithOlderBroker() throws Exception {
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

            final DeleteStreamsGroupsResult result = env.adminClient().deleteStreamsGroups(groupIds);

            final KafkaFuture<Void> results = result.deletedGroups().get("groupId");
            assertNull(results.get());

            // should throw error for non-retriable errors
            env.kafkaClient().prepareResponse(
                prepareOldFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED, Node.noNode()));

            DeleteStreamsGroupsResult errorResult = env.adminClient().deleteStreamsGroups(groupIds);
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

            errorResult = env.adminClient().deleteStreamsGroups(groupIds);

            final KafkaFuture<Void> errorResults = errorResult.deletedGroups().get("groupId");
            assertNull(errorResults.get());
        }
    }

    @Test
    public void testDeleteMultipleStreamsGroupsWithOlderBroker() throws Exception {
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

            final DeleteStreamsGroupsResult result = env.adminClient()
                .deleteStreamsGroups(groupIds);

            final KafkaFuture<Void> results = result.deletedGroups().get("group1");
            assertNull(results.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testDeleteStreamsGroupOffsetsNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            final TopicPartition tp1 = new TopicPartition("foo", 0);

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(prepareOffsetDeleteResponse(Errors.NOT_COORDINATOR));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final DeleteStreamsGroupOffsetsResult result = env.adminClient()
                .deleteStreamsGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

            TestUtils.assertFutureThrows(TimeoutException.class, result.all());
        }
    }

    @Test
    public void testDeleteStreamsGroupOffsetsRetryBackoff() throws Exception {
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

            final KafkaFuture<Void> future = env.adminClient().deleteStreamsGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet())).all();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting DeleteStreamsGroupOffsets first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry DeleteStreamsGroupOffsets call on first failure");

            long lowerBoundBackoffMs = (long) (retryBackoff * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
            long upperBoundBackoffMs = (long) (retryBackoff * CommonClientConfigs.RETRY_BACKOFF_EXP_BASE * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
            time.sleep(upperBoundBackoffMs);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, upperBoundBackoffMs - lowerBoundBackoffMs, "DeleteStreamsGroupOffsets retry did not await expected backoff!");
        }
    }

    @Test
    public void testDeleteStreamsGroupOffsets() throws Exception {
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

            final DeleteStreamsGroupOffsetsResult errorResult = env.adminClient().deleteStreamsGroupOffsets(
                GROUP_ID, Stream.of(tp1, tp2).collect(Collectors.toSet()));

            assertNull(errorResult.partitionResult(tp1).get());
            TestUtils.assertFutureThrows(GroupSubscribedToTopicException.class, errorResult.all());
            TestUtils.assertFutureThrows(GroupSubscribedToTopicException.class, errorResult.partitionResult(tp2));
            assertThrows(IllegalArgumentException.class, () -> errorResult.partitionResult(tp3));
        }
    }

    @Test
    public void testDeleteStreamsGroupOffsetsRetriableErrors() throws Exception {
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

            final DeleteStreamsGroupOffsetsResult errorResult1 = env.adminClient()
                .deleteStreamsGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

            assertNull(errorResult1.all().get());
            assertNull(errorResult1.partitionResult(tp1).get());
        }
    }

    @Test
    public void testDeleteStreamsGroupOffsetsNonRetriableErrors() throws Exception {
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

                DeleteStreamsGroupOffsetsResult errorResult = env.adminClient()
                    .deleteStreamsGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

                TestUtils.assertFutureThrows(error.exception().getClass(), errorResult.all());
                TestUtils.assertFutureThrows(error.exception().getClass(), errorResult.partitionResult(tp1));
            }
        }
    }

    @Test
    public void testDeleteStreamsGroupOffsetsFindCoordinatorRetriableErrors() throws Exception {
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

            final DeleteStreamsGroupOffsetsResult result = env.adminClient()
                .deleteStreamsGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

            assertNull(result.all().get());
            assertNull(result.partitionResult(tp1).get());
        }
    }

    @Test
    public void testDeleteStreamsGroupOffsetsFindCoordinatorNonRetriableErrors() throws Exception {
        // Non-retriable FindCoordinatorResponse errors throw an exception

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED,  Node.noNode()));

            final DeleteStreamsGroupOffsetsResult errorResult = env.adminClient()
                .deleteStreamsGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

            TestUtils.assertFutureThrows(GroupAuthorizationException.class, errorResult.all());
            TestUtils.assertFutureThrows(GroupAuthorizationException.class, errorResult.partitionResult(tp1));
        }
    }

    @Test
    public void testDescribeStreamsGroups() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            StreamsGroupDescribeResponseData data = new StreamsGroupDescribeResponseData();

            // Retriable errors should be retried
            data.groups().add(new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code()));
            env.kafkaClient().prepareResponse(new StreamsGroupDescribeResponse(data));

            // We need to return two responses here, one with NOT_COORDINATOR error when calling describe streams group
            // api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
            // FindCoordinatorResponse.
            //
            // And the same reason for COORDINATOR_NOT_AVAILABLE error response
            data = new StreamsGroupDescribeResponseData();
            data.groups().add(new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.NOT_COORDINATOR.code()));
            env.kafkaClient().prepareResponse(new StreamsGroupDescribeResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            data = new StreamsGroupDescribeResponseData();
            data.groups().add(new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()));
            env.kafkaClient().prepareResponse(new StreamsGroupDescribeResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            data = makeFullStreamsGroupDescribeResponse();

            env.kafkaClient().prepareResponse(new StreamsGroupDescribeResponse(data));

            final DescribeStreamsGroupsResult result = env.adminClient().describeStreamsGroups(singletonList(GROUP_ID));
            final StreamsGroupDescription groupDescription = result.describedGroups().get(GROUP_ID).get();

            final String subtopologyId = "my_subtopology";
            StreamsGroupMemberAssignment.TaskIds expectedActiveTasks1 =
                new StreamsGroupMemberAssignment.TaskIds(subtopologyId, asList(0, 1, 2));
            StreamsGroupMemberAssignment.TaskIds expectedStandbyTasks1 =
                new StreamsGroupMemberAssignment.TaskIds(subtopologyId, asList(3, 4, 5));
            StreamsGroupMemberAssignment.TaskIds expectedWarmupTasks1 =
                new StreamsGroupMemberAssignment.TaskIds(subtopologyId, asList(6, 7, 8));
            StreamsGroupMemberAssignment.TaskIds expectedActiveTasks2 =
                new StreamsGroupMemberAssignment.TaskIds(subtopologyId, asList(3, 4, 5));
            StreamsGroupMemberAssignment.TaskIds expectedStandbyTasks2 =
                new StreamsGroupMemberAssignment.TaskIds(subtopologyId, asList(6, 7, 8));
            StreamsGroupMemberAssignment.TaskIds expectedWarmupTasks2 =
                new StreamsGroupMemberAssignment.TaskIds(subtopologyId, asList(0, 1, 2));
            StreamsGroupMemberAssignment expectedMemberAssignment = new StreamsGroupMemberAssignment(
                singletonList(expectedActiveTasks1),
                singletonList(expectedStandbyTasks1),
                singletonList(expectedWarmupTasks1)
            );
            StreamsGroupMemberAssignment expectedTargetAssignment = new StreamsGroupMemberAssignment(
                singletonList(expectedActiveTasks2),
                singletonList(expectedStandbyTasks2),
                singletonList(expectedWarmupTasks2)
            );
            final String instanceId = "instance-id";
            final String rackId = "rack-id";
            StreamsGroupMemberDescription expectedMemberOne = new StreamsGroupMemberDescription(
                "0",
                1,
                Optional.of(instanceId),
                Optional.of(rackId),
                "clientId0",
                "clientHost",
                0,
                "processId",
                Optional.of(new StreamsGroupMemberDescription.Endpoint("localhost", 8080)),
                Collections.singletonMap("key", "value"),
                Collections.singletonList(new StreamsGroupMemberDescription.TaskOffset(subtopologyId, 0, 0)),
                Collections.singletonList(new StreamsGroupMemberDescription.TaskOffset(subtopologyId, 0, 1)),
                expectedMemberAssignment,
                expectedTargetAssignment,
                true
            );

            StreamsGroupMemberDescription expectedMemberTwo = new StreamsGroupMemberDescription(
                "1",
                2,
                Optional.empty(),
                Optional.empty(),
                "clientId1",
                "clientHost",
                1,
                "processId2",
                Optional.empty(),
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                new StreamsGroupMemberAssignment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
                new StreamsGroupMemberAssignment(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()),
                false
            );

            StreamsGroupSubtopologyDescription expectedSubtopologyDescription = new StreamsGroupSubtopologyDescription(
                subtopologyId,
                Collections.singletonList("my_source_topic"),
                Collections.singletonList("my_repartition_sink_topic"),
                Collections.singletonMap(
                    "my_changelog_topic",
                    new StreamsGroupSubtopologyDescription.TopicInfo(
                        0,
                        (short) 3,
                        Collections.singletonMap("key1", "value1")
                    )
                ),
                Collections.singletonMap(
                    "my_repartition_topic",
                    new StreamsGroupSubtopologyDescription.TopicInfo(
                        99,
                        (short) 0,
                        Collections.emptyMap()
                    )
                )
            );

            assertEquals(1, result.describedGroups().size());
            assertEquals(GROUP_ID, groupDescription.groupId());
            assertEquals(2, groupDescription.members().size());
            Iterator<StreamsGroupMemberDescription> members = groupDescription.members().iterator();
            assertEquals(expectedMemberOne, members.next());
            assertEquals(expectedMemberTwo, members.next());
            assertEquals(1, groupDescription.subtopologies().size());
            assertEquals(expectedSubtopologyDescription, groupDescription.subtopologies().iterator().next());
            assertEquals(2, groupDescription.groupEpoch());
            assertEquals(1, groupDescription.targetAssignmentEpoch());

        }
    }

    @Test
    public void testDescribeStreamsGroupsWithAuthorizedOperationsOmitted() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            StreamsGroupDescribeResponseData data = makeFullStreamsGroupDescribeResponse();

            data.groups().iterator().next()
                .setAuthorizedOperations(MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);

            env.kafkaClient().prepareResponse(new StreamsGroupDescribeResponse(data));

            final DescribeStreamsGroupsResult result = env.adminClient().describeStreamsGroups(singletonList(GROUP_ID));
            final StreamsGroupDescription groupDescription = result.describedGroups().get(GROUP_ID).get();

            assertNull(groupDescription.authorizedOperations());
        }
    }

    @Test
    public void testDescribeMultipleStreamsGroups() {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            StreamsGroupDescribeResponseData.TaskIds activeTasks = new StreamsGroupDescribeResponseData.TaskIds()
                .setSubtopologyId("my_subtopology")
                .setPartitions(asList(0, 1, 2));
            StreamsGroupDescribeResponseData.TaskIds standbyTasks = new StreamsGroupDescribeResponseData.TaskIds()
                .setSubtopologyId("my_subtopology")
                .setPartitions(asList(3, 4, 5));
            StreamsGroupDescribeResponseData.TaskIds warmupTasks = new StreamsGroupDescribeResponseData.TaskIds()
                .setSubtopologyId("my_subtopology")
                .setPartitions(asList(6, 7, 8));
            final StreamsGroupDescribeResponseData.Assignment memberAssignment = new StreamsGroupDescribeResponseData.Assignment()
                .setActiveTasks(singletonList(activeTasks))
                .setStandbyTasks(singletonList(standbyTasks))
                .setWarmupTasks(singletonList(warmupTasks));
            StreamsGroupDescribeResponseData group0Data = new StreamsGroupDescribeResponseData();
            group0Data.groups().add(new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupId(GROUP_ID)
                .setGroupState(GroupState.STABLE.toString())
                .setMembers(asList(
                    new StreamsGroupDescribeResponseData.Member()
                        .setMemberId("0")
                        .setClientId("clientId0")
                        .setClientHost("clientHost")
                        .setAssignment(memberAssignment),
                    new StreamsGroupDescribeResponseData.Member()
                        .setMemberId("1")
                        .setClientId("clientId1")
                        .setClientHost("clientHost")
                        .setAssignment(memberAssignment))));

            StreamsGroupDescribeResponseData group1Data = new StreamsGroupDescribeResponseData();
            group1Data.groups().add(new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupId("group-1")
                .setGroupState(GroupState.STABLE.toString())
                .setMembers(asList(
                    new StreamsGroupDescribeResponseData.Member()
                        .setMemberId("0")
                        .setClientId("clientId0")
                        .setClientHost("clientHost")
                        .setAssignment(memberAssignment),
                    new StreamsGroupDescribeResponseData.Member()
                        .setMemberId("1")
                        .setClientId("clientId1")
                        .setClientHost("clientHost")
                        .setAssignment(memberAssignment))));

            env.kafkaClient().prepareResponse(new StreamsGroupDescribeResponse(group0Data));
            env.kafkaClient().prepareResponse(new StreamsGroupDescribeResponse(group1Data));

            Collection<String> groups = new HashSet<>();
            groups.add(GROUP_ID);
            groups.add("group-1");
            final DescribeStreamsGroupsResult result = env.adminClient().describeStreamsGroups(groups);
            assertEquals(2, result.describedGroups().size());
            assertEquals(groups, result.describedGroups().keySet());
        }
    }

    @Test
    public void testListStreamsGroups() throws Exception {
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
                        .setGroups(singletonList(
                            new ListedGroup()
                                .setGroupId("streams-group-1")
                                .setGroupType(GroupType.STREAMS.toString())
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
                                .setGroupId("streams-group-2")
                                .setGroupType(GroupType.STREAMS.toString())
                                .setGroupState("Stable"),
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("streams-group-3")
                                .setGroupType(GroupType.STREAMS.toString())
                                .setGroupState("Stable")
                        ))),
                env.cluster().nodeById(1));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(
                    new ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGroups(singletonList(
                            new ListedGroup()
                                .setGroupId("streams-group-4")
                                .setGroupType(GroupType.STREAMS.toString())
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

            final ListGroupsResult result = env.adminClient().listGroups(ListGroupsOptions.forStreamsGroups());
            TestUtils.assertFutureThrows(UnknownServerException.class, result.all());

            Collection<GroupListing> listings = result.valid().get();
            assertEquals(4, listings.size());

            Set<String> groupIds = new HashSet<>();
            for (GroupListing listing : listings) {
                groupIds.add(listing.groupId());
                assertTrue(listing.groupState().isPresent());
            }

            assertEquals(Set.of("streams-group-1", "streams-group-2", "streams-group-3", "streams-group-4"), groupIds);
            assertEquals(1, result.errors().get().size());
        }
    }

    @Test
    public void testListStreamsGroupsMetadataFailure() throws Exception {
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

            final ListGroupsResult result = env.adminClient().listGroups(ListGroupsOptions.forStreamsGroups());
            TestUtils.assertFutureThrows(KafkaException.class, result.all());
        }
    }

    @Test
    public void testListStreamsGroupsWithStates() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(Arrays.asList(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("streams-group-1")
                            .setGroupType(GroupType.STREAMS.toString())
                            .setProtocolType("streams")
                            .setGroupState("Stable"),
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("streams-group-2")
                            .setGroupType(GroupType.STREAMS.toString())
                            .setProtocolType("streams")
                            .setGroupState("NotReady")))),
                env.cluster().nodeById(0));

            final ListGroupsResult result = env.adminClient().listGroups(ListGroupsOptions.forStreamsGroups());
            Collection<GroupListing> listings = result.valid().get();

            assertEquals(2, listings.size());
            List<GroupListing> expected = new ArrayList<>();
            expected.add(new GroupListing("streams-group-1", Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)));
            expected.add(new GroupListing("streams-group-2", Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.NOT_READY)));
            assertEquals(expected, listings);
            assertEquals(0, result.errors().get().size());
        }
    }

    @Test
    public void testListStreamsGroupsWithStatesOlderBrokerVersion() {
        ApiVersion listGroupV4 = new ApiVersion()
            .setApiKey(ApiKeys.LIST_GROUPS.id)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 4);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(Collections.singletonList(listGroupV4)));

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            // Check we should not be able to list streams groups with broker having version < 5
            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(Collections.singletonList(
                        new ListGroupsResponseData.ListedGroup()
                            .setGroupId("streams-group-1")))),
                env.cluster().nodeById(0));
            ListGroupsResult result = env.adminClient().listGroups(ListGroupsOptions.forStreamsGroups());
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.all());
        }
    }

    @Test
    public void testAlterStreamsGroupOffsets() throws Exception {
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
            final AlterStreamsGroupOffsetsResult result = env.adminClient().alterStreamsGroupOffsets(
                GROUP_ID, offsets);

            assertNull(result.all().get());
            assertNull(result.partitionResult(tp1).get());
            assertNull(result.partitionResult(tp2).get());
            TestUtils.assertFutureThrows(IllegalArgumentException.class, result.partitionResult(tp3));
        }
    }

    @Test
    public void testAlterStreamsGroupOffsetsRetriableErrors() throws Exception {
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
            final AlterStreamsGroupOffsetsResult result1 = env.adminClient()
                .alterStreamsGroupOffsets(GROUP_ID, offsets);

            assertNull(result1.all().get());
            assertNull(result1.partitionResult(tp1).get());
        }
    }

    @Test
    public void testAlterStreamsGroupOffsetsNonRetriableErrors() throws Exception {
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
                AlterStreamsGroupOffsetsResult errorResult = env.adminClient()
                    .alterStreamsGroupOffsets(GROUP_ID, offsets);

                TestUtils.assertFutureThrows(error.exception().getClass(), errorResult.all());
                TestUtils.assertFutureThrows(error.exception().getClass(), errorResult.partitionResult(tp1));
            }
        }
    }

    @Test
    public void testAlterStreamsGroupOffsetsFindCoordinatorRetriableErrors() throws Exception {
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
            final AlterStreamsGroupOffsetsResult result = env.adminClient()
                .alterStreamsGroupOffsets(GROUP_ID, offsets);

            assertNull(result.all().get());
            assertNull(result.partitionResult(tp1).get());
        }
    }

    @Test
    public void testAlterStreamsGroupOffsetsFindCoordinatorNonRetriableErrors() throws Exception {
        // Non-retriable FindCoordinatorResponse errors throw an exception

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED,  Node.noNode()));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1,  new OffsetAndMetadata(123L));
            final AlterStreamsGroupOffsetsResult errorResult = env.adminClient()
                .alterStreamsGroupOffsets(GROUP_ID, offsets);

            TestUtils.assertFutureThrows(GroupAuthorizationException.class, errorResult.all());
            TestUtils.assertFutureThrows(GroupAuthorizationException.class, errorResult.partitionResult(tp1));
        }
    }

    private Map<String, ListStreamsGroupOffsetsSpec> batchedListStreamsGroupOffsetsSpec() {
        Set<TopicPartition> groupAPartitions = Collections.singleton(new TopicPartition("A", 1));
        Set<TopicPartition> groupBPartitions =  Collections.singleton(new TopicPartition("B", 2));

        ListStreamsGroupOffsetsSpec groupASpec = new ListStreamsGroupOffsetsSpec().topicPartitions(groupAPartitions);
        ListStreamsGroupOffsetsSpec groupBSpec = new ListStreamsGroupOffsetsSpec().topicPartitions(groupBPartitions);
        return Map.of("groupA", groupASpec, "groupB", groupBSpec);
    }

    private void sendStreamsOffsetFetchResponse(MockClient mockClient, Map<String, ListStreamsGroupOffsetsSpec> groupSpecs, boolean batched, Errors error) throws Exception {
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

    private void verifyListStreamsOffsetsForMultipleGroups(Map<String, ListStreamsGroupOffsetsSpec> groupSpecs,
                                                           ListStreamsGroupOffsetsResult result) throws Exception {
        assertEquals(groupSpecs.size(), result.all().get(10, TimeUnit.SECONDS).size());
        for (Map.Entry<String, ListStreamsGroupOffsetsSpec> entry : groupSpecs.entrySet()) {
            assertEquals(entry.getValue().topicPartitions(),
                result.partitionsToOffsetAndMetadata(entry.getKey()).get().keySet());
        }
    }

    private static StreamsGroupDescribeResponseData makeFullStreamsGroupDescribeResponse() {
        StreamsGroupDescribeResponseData data;
        StreamsGroupDescribeResponseData.TaskIds activeTasks1 = new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("my_subtopology")
            .setPartitions(asList(0, 1, 2));
        StreamsGroupDescribeResponseData.TaskIds standbyTasks1 = new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("my_subtopology")
            .setPartitions(asList(3, 4, 5));
        StreamsGroupDescribeResponseData.TaskIds warmupTasks1 = new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("my_subtopology")
            .setPartitions(asList(6, 7, 8));
        StreamsGroupDescribeResponseData.TaskIds activeTasks2 = new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("my_subtopology")
            .setPartitions(asList(3, 4, 5));
        StreamsGroupDescribeResponseData.TaskIds standbyTasks2 = new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("my_subtopology")
            .setPartitions(asList(6, 7, 8));
        StreamsGroupDescribeResponseData.TaskIds warmupTasks2 = new StreamsGroupDescribeResponseData.TaskIds()
            .setSubtopologyId("my_subtopology")
            .setPartitions(asList(0, 1, 2));
        StreamsGroupDescribeResponseData.Assignment memberAssignment = new StreamsGroupDescribeResponseData.Assignment()
            .setActiveTasks(singletonList(activeTasks1))
            .setStandbyTasks(singletonList(standbyTasks1))
            .setWarmupTasks(singletonList(warmupTasks1));
        StreamsGroupDescribeResponseData.Assignment targetAssignment = new StreamsGroupDescribeResponseData.Assignment()
            .setActiveTasks(singletonList(activeTasks2))
            .setStandbyTasks(singletonList(standbyTasks2))
            .setWarmupTasks(singletonList(warmupTasks2));
        StreamsGroupDescribeResponseData.Member memberOne = new StreamsGroupDescribeResponseData.Member()
            .setMemberId("0")
            .setMemberEpoch(1)
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setClientId("clientId0")
            .setClientHost("clientHost")
            .setTopologyEpoch(0)
            .setProcessId("processId")
            .setUserEndpoint(new StreamsGroupDescribeResponseData.Endpoint()
                .setHost("localhost")
                .setPort(8080)
            )
            .setClientTags(Collections.singletonList(new StreamsGroupDescribeResponseData.KeyValue()
                .setKey("key")
                .setValue("value")
            ))
            .setTaskOffsets(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskOffset()
                .setSubtopologyId("my_subtopology")
                .setPartition(0)
                .setOffset(0)
            ))
            .setTaskEndOffsets(Collections.singletonList(new StreamsGroupDescribeResponseData.TaskOffset()
                .setSubtopologyId("my_subtopology")
                .setPartition(0)
                .setOffset(1)
            ))
            .setAssignment(memberAssignment)
            .setTargetAssignment(targetAssignment)
            .setIsClassic(true);

        StreamsGroupDescribeResponseData.Member memberTwo = new StreamsGroupDescribeResponseData.Member()
            .setMemberId("1")
            .setMemberEpoch(2)
            .setInstanceId(null)
            .setRackId(null)
            .setClientId("clientId1")
            .setClientHost("clientHost")
            .setTopologyEpoch(1)
            .setProcessId("processId2")
            .setUserEndpoint(null)
            .setClientTags(Collections.emptyList())
            .setTaskOffsets(Collections.emptyList())
            .setTaskEndOffsets(Collections.emptyList())
            .setAssignment(new StreamsGroupDescribeResponseData.Assignment())
            .setTargetAssignment(new StreamsGroupDescribeResponseData.Assignment())
            .setIsClassic(false);

        StreamsGroupDescribeResponseData.Subtopology subtopologyDescription = new StreamsGroupDescribeResponseData.Subtopology()
            .setSubtopologyId("my_subtopology")
            .setSourceTopics(Collections.singletonList("my_source_topic"))
            .setRepartitionSinkTopics(Collections.singletonList("my_repartition_sink_topic"))
            .setStateChangelogTopics(Collections.singletonList(
                new StreamsGroupDescribeResponseData.TopicInfo()
                    .setName("my_changelog_topic")
                    .setPartitions(0)
                    .setReplicationFactor((short) 3)
                    .setTopicConfigs(Collections.singletonList(new StreamsGroupDescribeResponseData.KeyValue()
                        .setKey("key1")
                        .setValue("value1")
                    ))
            ))
            .setRepartitionSourceTopics(Collections.singletonList(
                new StreamsGroupDescribeResponseData.TopicInfo()
                    .setName("my_repartition_topic")
                    .setPartitions(99)
                    .setReplicationFactor((short) 0)
                    .setTopicConfigs(Collections.emptyList())
            ));

        data = new StreamsGroupDescribeResponseData();
        data.groups().add(new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(GROUP_ID)
            .setGroupState(GroupState.STABLE.toString())
            .setMembers(asList(memberOne, memberTwo))
            .setTopology(new StreamsGroupDescribeResponseData.Topology()
                .setEpoch(1)
                .setSubtopologies(Collections.singletonList(subtopologyDescription))
            )
            .setGroupEpoch(2)
            .setAssignmentEpoch(1));
        return data;
    }
}
