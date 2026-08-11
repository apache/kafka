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
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResultCollection;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResultCollection;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsTopic;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.AlterReplicaLogDirsResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.requests.DescribeTopicPartitionsResponse;
import org.apache.kafka.common.requests.ElectLeadersResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.ListPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaAdminClientTopicTest extends KafkaAdminClientTestBase {

    @Test
    public void testCreateTopics() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("myTopic"),
                prepareCreateTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                    singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();
            future.get();
        }
    }

    @Test
    public void testCreateTopicsPartialResponse() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("myTopic", "myTopic2"),
                prepareCreateTopicsResponse("myTopic", Errors.NONE));
            CreateTopicsResult topicsResult = env.adminClient().createTopics(
                    asList(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2))),
                           new NewTopic("myTopic2", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000));
            topicsResult.values().get("myTopic").get();
            TestUtils.assertFutureThrows(ApiException.class, topicsResult.values().get("myTopic2"));
        }
    }

    @Test
    public void testCreateTopicsRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
                mockCluster(3, 0),
                newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return body instanceof CreateTopicsRequest;
            }, null, true);

            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return body instanceof CreateTopicsRequest;
            }, prepareCreateTopicsResponse("myTopic", Errors.NONE));

            KafkaFuture<Void> future = env.adminClient().createTopics(
                singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                new CreateTopicsOptions().timeoutMs(10000)).all();

            // Wait until the first attempt has failed, then advance the time
            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1,
                "Failed awaiting CreateTopics first request failure");

            // Wait until the retry call added to the queue in AdminClient
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1,
                "Failed to add retry CreateTopics call");

            long lowerBoundBackoffMs = (long) (retryBackoff * (1 - CommonClientConfigs.RETRY_BACKOFF_JITTER));
            long upperBoundBackoffMs = (long) (retryBackoff * CommonClientConfigs.RETRY_BACKOFF_EXP_BASE * (1 + CommonClientConfigs.RETRY_BACKOFF_JITTER));
            time.sleep(upperBoundBackoffMs);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, upperBoundBackoffMs - lowerBoundBackoffMs, "CreateTopics retry did not await expected backoff");
        }
    }

    @Test
    public void testCreateTopicsHandleNotControllerException() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(
                prepareCreateTopicsResponse("myTopic", Errors.NOT_CONTROLLER),
                env.cluster().nodeById(0));
            env.kafkaClient().prepareResponse(RequestTestUtils.metadataResponse(env.cluster().nodes(),
                env.cluster().clusterResource().clusterId(),
                1,
                Collections.emptyList()));
            env.kafkaClient().prepareResponseFrom(
                prepareCreateTopicsResponse("myTopic", Errors.NONE),
                env.cluster().nodeById(1));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                new CreateTopicsOptions().timeoutMs(10000)).all();
            future.get();
        }
    }

    @Test
    public void testCreateTopicsRetryThrottlingExceptionWhenEnabled() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareCreateTopicsResponse(1000,
                    creatableTopicResult("topic1", Errors.NONE),
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    creatableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("topic2"),
                prepareCreateTopicsResponse(1000,
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED)));

            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("topic2"),
                prepareCreateTopicsResponse(0,
                    creatableTopicResult("topic2", Errors.NONE)));

            CreateTopicsResult result = env.adminClient().createTopics(
                asList(
                    new NewTopic("topic1", 1, (short) 1),
                    new NewTopic("topic2", 1, (short) 1),
                    new NewTopic("topic3", 1, (short) 1)),
                new CreateTopicsOptions().retryOnQuotaViolation(true));

            assertNull(result.values().get("topic1").get());
            assertNull(result.values().get("topic2").get());
            TestUtils.assertFutureThrows(TopicExistsException.class, result.values().get("topic3"));
        }
    }

    @Test
    public void testCreateTopicsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut() throws Exception {
        long defaultApiTimeout = 60000;
        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = mockClientEnv(time,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(defaultApiTimeout))) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareCreateTopicsResponse(1000,
                    creatableTopicResult("topic1", Errors.NONE),
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    creatableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("topic2"),
                prepareCreateTopicsResponse(1000,
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED)));

            CreateTopicsResult result = env.adminClient().createTopics(
                asList(
                    new NewTopic("topic1", 1, (short) 1),
                    new NewTopic("topic2", 1, (short) 1),
                    new NewTopic("topic3", 1, (short) 1)),
                new CreateTopicsOptions().retryOnQuotaViolation(true));

            // Wait until the prepared attempts have consumed
            TestUtils.waitForCondition(() -> env.kafkaClient().numAwaitingResponses() == 0,
                "Failed awaiting CreateTopics requests");

            // Wait until the next request is sent out
            TestUtils.waitForCondition(() -> env.kafkaClient().inFlightRequestCount() == 1,
                "Failed awaiting next CreateTopics request");

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout + 1);

            assertNull(result.values().get("topic1").get());
            ThrottlingQuotaExceededException e = TestUtils.assertFutureThrows(ThrottlingQuotaExceededException.class, result.values().get("topic2"));
            assertEquals(0, e.throttleTimeMs());
            TestUtils.assertFutureThrows(TopicExistsException.class, result.values().get("topic3"));
        }
    }

    @Test
    public void testCreateTopicsDontRetryThrottlingExceptionWhenDisabled() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareCreateTopicsResponse(1000,
                    creatableTopicResult("topic1", Errors.NONE),
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    creatableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            CreateTopicsResult result = env.adminClient().createTopics(
                asList(
                    new NewTopic("topic1", 1, (short) 1),
                    new NewTopic("topic2", 1, (short) 1),
                    new NewTopic("topic3", 1, (short) 1)),
                new CreateTopicsOptions().retryOnQuotaViolation(false));

            assertNull(result.values().get("topic1").get());
            ThrottlingQuotaExceededException e = TestUtils.assertFutureThrows(ThrottlingQuotaExceededException.class, result.values().get("topic2"));
            assertEquals(1000, e.throttleTimeMs());
            TestUtils.assertFutureThrows(TopicExistsException.class, result.values().get("topic3"));
        }
    }

    @Test
    public void testDeleteTopics() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("myTopic"),
                prepareDeleteTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().deleteTopics(singletonList("myTopic"),
                new DeleteTopicsOptions()).all();
            assertNull(future.get());

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("myTopic"),
                prepareDeleteTopicsResponse("myTopic", Errors.TOPIC_DELETION_DISABLED));
            future = env.adminClient().deleteTopics(singletonList("myTopic"),
                new DeleteTopicsOptions()).all();
            TestUtils.assertFutureThrows(TopicDeletionDisabledException.class, future);

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("myTopic"),
                prepareDeleteTopicsResponse("myTopic", Errors.UNKNOWN_TOPIC_OR_PARTITION));
            future = env.adminClient().deleteTopics(singletonList("myTopic"),
                new DeleteTopicsOptions()).all();
            TestUtils.assertFutureThrows(UnknownTopicOrPartitionException.class, future);

            // With topic IDs
            Uuid topicId = Uuid.randomUuid();

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId),
                    prepareDeleteTopicsResponseWithTopicId(topicId, Errors.NONE));
            future = env.adminClient().deleteTopics(TopicCollection.ofTopicIds(singletonList(topicId)),
                    new DeleteTopicsOptions()).all();
            assertNull(future.get());

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId),
                    prepareDeleteTopicsResponseWithTopicId(topicId, Errors.TOPIC_DELETION_DISABLED));
            future = env.adminClient().deleteTopics(TopicCollection.ofTopicIds(singletonList(topicId)),
                    new DeleteTopicsOptions()).all();
            TestUtils.assertFutureThrows(TopicDeletionDisabledException.class, future);

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId),
                    prepareDeleteTopicsResponseWithTopicId(topicId, Errors.UNKNOWN_TOPIC_ID));
            future = env.adminClient().deleteTopics(TopicCollection.ofTopicIds(singletonList(topicId)),
                    new DeleteTopicsOptions()).all();
            TestUtils.assertFutureThrows(UnknownTopicIdException.class, future);
        }
    }

    @Test
    public void testDeleteTopicsPartialResponse() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("myTopic", "myOtherTopic"),
                prepareDeleteTopicsResponse(1000,
                    deletableTopicResult("myTopic", Errors.NONE)));

            DeleteTopicsResult result = env.adminClient().deleteTopics(
                asList("myTopic", "myOtherTopic"), new DeleteTopicsOptions());

            result.topicNameValues().get("myTopic").get();
            TestUtils.assertFutureThrows(ApiException.class, result.topicNameValues().get("myOtherTopic"));

            // With topic IDs
            Uuid topicId1 = Uuid.randomUuid();
            Uuid topicId2 = Uuid.randomUuid();
            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId1, topicId2),
                    prepareDeleteTopicsResponse(1000,
                            deletableTopicResultWithId(topicId1, Errors.NONE)));

            DeleteTopicsResult resultIds = env.adminClient().deleteTopics(
                    TopicCollection.ofTopicIds(asList(topicId1, topicId2)), new DeleteTopicsOptions());

            resultIds.topicIdValues().get(topicId1).get();
            TestUtils.assertFutureThrows(ApiException.class, resultIds.topicIdValues().get(topicId2));
        }
    }

    @Test
    public void testDeleteTopicsRetryThrottlingExceptionWhenEnabled() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareDeleteTopicsResponse(1000,
                    deletableTopicResult("topic1", Errors.NONE),
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    deletableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("topic2"),
                prepareDeleteTopicsResponse(1000,
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED)));

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("topic2"),
                prepareDeleteTopicsResponse(0,
                    deletableTopicResult("topic2", Errors.NONE)));

            DeleteTopicsResult result = env.adminClient().deleteTopics(
                asList("topic1", "topic2", "topic3"),
                new DeleteTopicsOptions().retryOnQuotaViolation(true));

            assertNull(result.topicNameValues().get("topic1").get());
            assertNull(result.topicNameValues().get("topic2").get());
            TestUtils.assertFutureThrows(TopicExistsException.class, result.topicNameValues().get("topic3"));

            // With topic IDs
            Uuid topicId1 = Uuid.randomUuid();
            Uuid topicId2 = Uuid.randomUuid();
            Uuid topicId3 = Uuid.randomUuid();

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId1, topicId2, topicId3),
                    prepareDeleteTopicsResponse(1000,
                            deletableTopicResultWithId(topicId1, Errors.NONE),
                            deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED),
                            deletableTopicResultWithId(topicId3, Errors.UNKNOWN_TOPIC_ID)));

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId2),
                    prepareDeleteTopicsResponse(1000,
                            deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED)));

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId2),
                    prepareDeleteTopicsResponse(0,
                            deletableTopicResultWithId(topicId2, Errors.NONE)));

            DeleteTopicsResult resultIds = env.adminClient().deleteTopics(
                    TopicCollection.ofTopicIds(asList(topicId1, topicId2, topicId3)),
                    new DeleteTopicsOptions().retryOnQuotaViolation(true));

            assertNull(resultIds.topicIdValues().get(topicId1).get());
            assertNull(resultIds.topicIdValues().get(topicId2).get());
            TestUtils.assertFutureThrows(UnknownTopicIdException.class, resultIds.topicIdValues().get(topicId3));
        }
    }

    @Test
    public void testDeleteTopicsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut() throws Exception {
        long defaultApiTimeout = 60000;
        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = mockClientEnv(time,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(defaultApiTimeout))) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareDeleteTopicsResponse(1000,
                    deletableTopicResult("topic1", Errors.NONE),
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    deletableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("topic2"),
                prepareDeleteTopicsResponse(1000,
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED)));

            DeleteTopicsResult result = env.adminClient().deleteTopics(
                asList("topic1", "topic2", "topic3"),
                new DeleteTopicsOptions().retryOnQuotaViolation(true));

            // Wait until the prepared attempts have consumed
            TestUtils.waitForCondition(() -> env.kafkaClient().numAwaitingResponses() == 0,
                "Failed awaiting DeleteTopics requests");

            // Wait until the next request is sent out
            TestUtils.waitForCondition(() -> env.kafkaClient().inFlightRequestCount() == 1,
                "Failed awaiting next DeleteTopics request");

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout + 1);

            assertNull(result.topicNameValues().get("topic1").get());
            ThrottlingQuotaExceededException e = TestUtils.assertFutureThrows(ThrottlingQuotaExceededException.class, result.topicNameValues().get("topic2"));
            assertEquals(0, e.throttleTimeMs());
            TestUtils.assertFutureThrows(TopicExistsException.class, result.topicNameValues().get("topic3"));

            // With topic IDs
            Uuid topicId1 = Uuid.randomUuid();
            Uuid topicId2 = Uuid.randomUuid();
            Uuid topicId3 = Uuid.randomUuid();
            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId1, topicId2, topicId3),
                    prepareDeleteTopicsResponse(1000,
                            deletableTopicResultWithId(topicId1, Errors.NONE),
                            deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED),
                            deletableTopicResultWithId(topicId3, Errors.UNKNOWN_TOPIC_ID)));

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId2),
                    prepareDeleteTopicsResponse(1000,
                            deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED)));

            DeleteTopicsResult resultIds = env.adminClient().deleteTopics(
                    TopicCollection.ofTopicIds(asList(topicId1, topicId2, topicId3)),
                    new DeleteTopicsOptions().retryOnQuotaViolation(true));

            // Wait until the prepared attempts have consumed
            TestUtils.waitForCondition(() -> env.kafkaClient().numAwaitingResponses() == 0,
                    "Failed awaiting DeleteTopics requests");

            // Wait until the next request is sent out
            TestUtils.waitForCondition(() -> env.kafkaClient().inFlightRequestCount() == 1,
                    "Failed awaiting next DeleteTopics request");

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout + 1);

            assertNull(resultIds.topicIdValues().get(topicId1).get());
            e = TestUtils.assertFutureThrows(ThrottlingQuotaExceededException.class, resultIds.topicIdValues().get(topicId2));
            assertEquals(0, e.throttleTimeMs());
            TestUtils.assertFutureThrows(UnknownTopicIdException.class, resultIds.topicIdValues().get(topicId3));
        }
    }

    @Test
    public void testDeleteTopicsDontRetryThrottlingExceptionWhenDisabled() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareDeleteTopicsResponse(1000,
                    deletableTopicResult("topic1", Errors.NONE),
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    deletableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            DeleteTopicsResult result = env.adminClient().deleteTopics(
                asList("topic1", "topic2", "topic3"),
                new DeleteTopicsOptions().retryOnQuotaViolation(false));

            assertNull(result.topicNameValues().get("topic1").get());
            ThrottlingQuotaExceededException e = TestUtils.assertFutureThrows(ThrottlingQuotaExceededException.class, result.topicNameValues().get("topic2"));
            assertEquals(1000, e.throttleTimeMs());
            TestUtils.assertFutureThrows(TopicExistsException.class, result.topicNameValues().get("topic3"));

            // With topic IDs
            Uuid topicId1 = Uuid.randomUuid();
            Uuid topicId2 = Uuid.randomUuid();
            Uuid topicId3 = Uuid.randomUuid();
            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId1, topicId2, topicId3),
                    prepareDeleteTopicsResponse(1000,
                            deletableTopicResultWithId(topicId1, Errors.NONE),
                            deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED),
                            deletableTopicResultWithId(topicId3, Errors.UNKNOWN_TOPIC_ID)));

            DeleteTopicsResult resultIds = env.adminClient().deleteTopics(
                    TopicCollection.ofTopicIds(asList(topicId1, topicId2, topicId3)),
                    new DeleteTopicsOptions().retryOnQuotaViolation(false));

            assertNull(resultIds.topicIdValues().get(topicId1).get());
            e = TestUtils.assertFutureThrows(ThrottlingQuotaExceededException.class, resultIds.topicIdValues().get(topicId2));
            assertEquals(1000, e.throttleTimeMs());
            TestUtils.assertFutureThrows(UnknownTopicIdException.class, resultIds.topicIdValues().get(topicId3));
        }
    }

    @Test
    public void testInvalidTopicNames() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            List<String> sillyTopicNames = asList("", null);
            Map<String, KafkaFuture<Void>> deleteFutures = env.adminClient().deleteTopics(sillyTopicNames).topicNameValues();
            for (String sillyTopicName : sillyTopicNames) {
                TestUtils.assertFutureThrows(InvalidTopicException.class, deleteFutures.get(sillyTopicName));
            }
            assertEquals(0, env.kafkaClient().inFlightRequestCount());

            Map<String, KafkaFuture<TopicDescription>> describeFutures =
                    env.adminClient().describeTopics(sillyTopicNames).topicNameValues();
            for (String sillyTopicName : sillyTopicNames) {
                TestUtils.assertFutureThrows(InvalidTopicException.class, describeFutures.get(sillyTopicName));
            }
            assertEquals(0, env.kafkaClient().inFlightRequestCount());

            List<NewTopic> newTopics = new ArrayList<>();
            for (String sillyTopicName : sillyTopicNames) {
                newTopics.add(new NewTopic(sillyTopicName, 1, (short) 1));
            }

            Map<String, KafkaFuture<Void>> createFutures = env.adminClient().createTopics(newTopics).values();
            for (String sillyTopicName : sillyTopicNames) {
                TestUtils.assertFutureThrows(InvalidTopicException.class, createFutures.get(sillyTopicName));
            }
            assertEquals(0, env.kafkaClient().inFlightRequestCount());
        }
    }

    @SuppressWarnings("NPathComplexity")
    @Test
    public void testDescribeTopicsWithDescribeTopicPartitionsApiBasic() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            String topicName0 = "test-0";
            String topicName1 = "test-1";
            Map<String, Uuid> topics = new HashMap<>();
            topics.put(topicName0, Uuid.randomUuid());
            topics.put(topicName1, Uuid.randomUuid());

            env.kafkaClient().prepareResponse(
                prepareDescribeClusterResponse(0,
                    env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(),
                    2,
                    MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                    false)
            );

            DescribeTopicPartitionsResponseData dataFirstPart = new DescribeTopicPartitionsResponseData();
            addPartitionToDescribeTopicPartitionsResponse(dataFirstPart, topicName0, topics.get(topicName0), singletonList(0));
            dataFirstPart.setNextCursor(new DescribeTopicPartitionsResponseData.Cursor()
                .setTopicName(topicName0)
                .setPartitionIndex(1));
            env.kafkaClient().prepareResponse(body -> {
                DescribeTopicPartitionsRequestData request = (DescribeTopicPartitionsRequestData) body.data();
                if (request.topics().size() != 2) return false;
                if (!request.topics().get(0).name().equals(topicName0)) return false;
                if (!request.topics().get(1).name().equals(topicName1)) return false;
                return request.cursor() == null;
            }, new DescribeTopicPartitionsResponse(dataFirstPart));

            DescribeTopicPartitionsResponseData dataSecondPart = new DescribeTopicPartitionsResponseData();
            addPartitionToDescribeTopicPartitionsResponse(dataSecondPart, topicName0, topics.get(topicName0), singletonList(1));
            addPartitionToDescribeTopicPartitionsResponse(dataSecondPart, topicName1, topics.get(topicName1), singletonList(0));
            env.kafkaClient().prepareResponse(body -> {
                DescribeTopicPartitionsRequestData request = (DescribeTopicPartitionsRequestData) body.data();
                if (request.topics().size() != 2) return false;
                if (!request.topics().get(0).name().equals(topicName0)) return false;
                if (!request.topics().get(1).name().equals(topicName1)) return false;

                DescribeTopicPartitionsRequestData.Cursor cursor = request.cursor();
                return cursor != null && cursor.topicName() == topicName0 && cursor.partitionIndex() == 1;
            }, new DescribeTopicPartitionsResponse(dataSecondPart));

            DescribeTopicsResult result = env.adminClient().describeTopics(
                asList(topicName0, topicName1), new DescribeTopicsOptions()
            );
            Map<String, TopicDescription> topicDescriptions = result.allTopicNames().get();
            assertEquals(2, topicDescriptions.size());
            TopicDescription topicDescription = topicDescriptions.get(topicName0);
            assertEquals(2, topicDescription.partitions().size());
            assertEquals(0, topicDescription.partitions().get(0).partition());
            assertEquals(1, topicDescription.partitions().get(1).partition());
            topicDescription = topicDescriptions.get(topicName1);
            assertEquals(1, topicDescription.partitions().size());
            assertNull(topicDescription.authorizedOperations());
        }
    }

    @Test
    public void testDescribeTopicPartitionsApiWithAuthorizedOps() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            String topicName0 = "test-0";
            Uuid topicId =  Uuid.randomUuid();

            int authorisedOperations = Utils.to32BitField(Set.of(AclOperation.DESCRIBE.code(), AclOperation.ALTER.code()));
            env.kafkaClient().prepareResponse(
                    prepareDescribeClusterResponse(0,
                            env.cluster().nodes(),
                            env.cluster().clusterResource().clusterId(),
                            2,
                            authorisedOperations,
                            false)
            );

            DescribeTopicPartitionsResponseData responseData = new DescribeTopicPartitionsResponseData();
            responseData.topics().add(new DescribeTopicPartitionsResponseTopic()
                    .setErrorCode((short) 0)
                    .setTopicId(topicId)
                    .setName(topicName0)
                    .setIsInternal(false)
                    .setTopicAuthorizedOperations(authorisedOperations));
            env.kafkaClient().prepareResponse(new DescribeTopicPartitionsResponse(responseData));

            DescribeTopicsResult result = env.adminClient().describeTopics(
                    singletonList(topicName0), new DescribeTopicsOptions().includeAuthorizedOperations(true)
            );

            Map<String, TopicDescription> topicDescriptions = result.allTopicNames().get();
            TopicDescription topicDescription = topicDescriptions.get(topicName0);
            assertEquals(Set.of(AclOperation.DESCRIBE, AclOperation.ALTER),
                    topicDescription.authorizedOperations());
        }
    }

    @Test
    public void testDescribeTopicPartitionsApiWithoutAuthorizedOps() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            String topicName0 = "test-0";
            Uuid topicId =  Uuid.randomUuid();

            int authorisedOperations = Utils.to32BitField(Set.of(AclOperation.DESCRIBE.code(), AclOperation.ALTER.code()));
            env.kafkaClient().prepareResponse(
                    prepareDescribeClusterResponse(0,
                            env.cluster().nodes(),
                            env.cluster().clusterResource().clusterId(),
                            2,
                            authorisedOperations,
                            false)
            );

            DescribeTopicPartitionsResponseData responseData = new DescribeTopicPartitionsResponseData();
            responseData.topics().add(new DescribeTopicPartitionsResponseTopic()
                    .setErrorCode((short) 0)
                    .setTopicId(topicId)
                    .setName(topicName0)
                    .setIsInternal(false)
                    .setTopicAuthorizedOperations(authorisedOperations));
            env.kafkaClient().prepareResponse(new DescribeTopicPartitionsResponse(responseData));

            DescribeTopicsResult result = env.adminClient().describeTopics(
                    singletonList(topicName0), new DescribeTopicsOptions().includeAuthorizedOperations(false)
            );

            Map<String, TopicDescription> topicDescriptions = result.allTopicNames().get();
            TopicDescription topicDescription = topicDescriptions.get(topicName0);
            assertNull(topicDescription.authorizedOperations());
        }
    }

    @SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
    @Test
    public void testDescribeTopicsWithDescribeTopicPartitionsApiEdgeCase() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            String topicName0 = "test-0";
            String topicName1 = "test-1";
            String topicName2 = "test-2";
            Map<String, Uuid> topics = new HashMap<>();
            topics.put(topicName0, Uuid.randomUuid());
            topics.put(topicName1, Uuid.randomUuid());
            topics.put(topicName2, Uuid.randomUuid());

            env.kafkaClient().prepareResponse(
                prepareDescribeClusterResponse(0,
                    env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(),
                    2,
                    MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                    false)
            );

            DescribeTopicPartitionsResponseData dataFirstPart = new DescribeTopicPartitionsResponseData();
            addPartitionToDescribeTopicPartitionsResponse(dataFirstPart, topicName0, topics.get(topicName0), singletonList(0));
            addPartitionToDescribeTopicPartitionsResponse(dataFirstPart, topicName1, topics.get(topicName1), singletonList(0));
            dataFirstPart.setNextCursor(new DescribeTopicPartitionsResponseData.Cursor()
                .setTopicName(topicName1)
                .setPartitionIndex(1));
            env.kafkaClient().prepareResponse(body -> {
                DescribeTopicPartitionsRequestData request = (DescribeTopicPartitionsRequestData) body.data();
                if (request.topics().size() != 3) return false;
                if (!request.topics().get(0).name().equals(topicName0)) return false;
                if (!request.topics().get(1).name().equals(topicName1)) return false;
                if (!request.topics().get(2).name().equals(topicName2)) return false;
                return request.cursor() == null;
            }, new DescribeTopicPartitionsResponse(dataFirstPart));

            DescribeTopicPartitionsResponseData dataSecondPart = new DescribeTopicPartitionsResponseData();
            addPartitionToDescribeTopicPartitionsResponse(dataSecondPart, topicName1, topics.get(topicName1), singletonList(1));
            addPartitionToDescribeTopicPartitionsResponse(dataSecondPart, topicName2, topics.get(topicName2), singletonList(0));
            dataSecondPart.setNextCursor(new DescribeTopicPartitionsResponseData.Cursor()
                .setTopicName(topicName2)
                .setPartitionIndex(1));
            env.kafkaClient().prepareResponse(body -> {
                DescribeTopicPartitionsRequestData request = (DescribeTopicPartitionsRequestData) body.data();
                if (request.topics().size() != 2) return false;
                if (!request.topics().get(0).name().equals(topicName1)) return false;
                if (!request.topics().get(1).name().equals(topicName2)) return false;
                DescribeTopicPartitionsRequestData.Cursor cursor = request.cursor();
                return cursor != null && cursor.topicName().equals(topicName1) && cursor.partitionIndex() == 1;
            }, new DescribeTopicPartitionsResponse(dataSecondPart));

            DescribeTopicPartitionsResponseData dataThirdPart = new DescribeTopicPartitionsResponseData();
            addPartitionToDescribeTopicPartitionsResponse(dataThirdPart, topicName2, topics.get(topicName2), singletonList(1));
            env.kafkaClient().prepareResponse(body -> {
                DescribeTopicPartitionsRequestData request = (DescribeTopicPartitionsRequestData) body.data();
                if (request.topics().size() != 1) return false;
                if (!request.topics().get(0).name().equals(topicName2)) return false;
                DescribeTopicPartitionsRequestData.Cursor cursor = request.cursor();
                return cursor != null && cursor.topicName().equals(topicName2) && cursor.partitionIndex() == 1;
            }, new DescribeTopicPartitionsResponse(dataThirdPart));

            DescribeTopicsResult result = env.adminClient().describeTopics(
                asList(topicName1, topicName0, topicName2), new DescribeTopicsOptions()
            );
            Map<String, TopicDescription> topicDescriptions = result.allTopicNames().get();
            assertEquals(3, topicDescriptions.size());
            TopicDescription topicDescription = topicDescriptions.get(topicName0);
            assertEquals(1, topicDescription.partitions().size());
            assertEquals(0, topicDescription.partitions().get(0).partition());
            topicDescription = topicDescriptions.get(topicName1);
            assertEquals(2, topicDescription.partitions().size());
            topicDescription = topicDescriptions.get(topicName2);
            assertEquals(2, topicDescription.partitions().size());
            assertNull(topicDescription.authorizedOperations());
        }
    }

    @SuppressWarnings("NPathComplexity")
    @Test
    public void testDescribeTopicsWithDescribeTopicPartitionsApiErrorHandling() throws InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            String topicName0 = "test-0";
            String topicName1 = "test-1";
            Map<String, Uuid> topics = new HashMap<>();
            topics.put(topicName0, Uuid.randomUuid());
            topics.put(topicName1, Uuid.randomUuid());

            env.kafkaClient().prepareResponse(
                prepareDescribeClusterResponse(0,
                    env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(),
                    2,
                    MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                    false)
            );

            DescribeTopicPartitionsResponseData dataFirstPart = new DescribeTopicPartitionsResponseData();
            dataFirstPart.topics().add(new DescribeTopicPartitionsResponseTopic()
                .setErrorCode((short) 0)
                .setTopicId(topics.get(topicName0))
                .setName(topicName0)
                .setIsInternal(false)
                .setPartitions(singletonList(new DescribeTopicPartitionsResponsePartition()
                    .setIsrNodes(singletonList(0))
                    .setErrorCode((short) 0)
                    .setLeaderEpoch(0)
                    .setLeaderId(0)
                    .setEligibleLeaderReplicas(singletonList(1))
                    .setLastKnownElr(singletonList(2))
                    .setPartitionIndex(0)
                    .setReplicaNodes(asList(0, 1, 2))))
            );
            dataFirstPart.topics().add(new DescribeTopicPartitionsResponseTopic()
                .setErrorCode((short) 29)
                .setTopicId(Uuid.ZERO_UUID)
                .setName(topicName1)
                .setIsInternal(false)
            );
            env.kafkaClient().prepareResponse(body -> {
                DescribeTopicPartitionsRequestData request = (DescribeTopicPartitionsRequestData) body.data();
                if (request.topics().size() != 2) return false;
                if (!request.topics().get(0).name().equals(topicName0)) return false;
                if (!request.topics().get(1).name().equals(topicName1)) return false;
                return request.cursor() == null;
            }, new DescribeTopicPartitionsResponse(dataFirstPart));
            DescribeTopicsResult result = env.adminClient().describeTopics(
                asList(topicName1, topicName0), new DescribeTopicsOptions()
            );

            TestUtils.assertFutureThrows(TopicAuthorizationException.class, result.allTopicNames());

        }
    }

    @Test
    public void testElectLeaders()  throws Exception {
        TopicPartition topic1 = new TopicPartition("topic", 0);
        TopicPartition topic2 = new TopicPartition("topic", 2);
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            for (ElectionType electionType : ElectionType.values()) {
                env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

                // Test a call where one partition has an error.
                ApiError value = ApiError.fromThrowable(new ClusterAuthorizationException(null));
                List<ReplicaElectionResult> electionResults = new ArrayList<>();
                ReplicaElectionResult electionResult = new ReplicaElectionResult();
                electionResult.setTopic(topic1.topic());
                // Add partition 1 result
                PartitionResult partition1Result = new PartitionResult();
                partition1Result.setPartitionId(topic1.partition());
                partition1Result.setErrorCode(value.error().code());
                partition1Result.setErrorMessage(value.message());
                electionResult.partitionResult().add(partition1Result);

                // Add partition 2 result
                PartitionResult partition2Result = new PartitionResult();
                partition2Result.setPartitionId(topic2.partition());
                partition2Result.setErrorCode(value.error().code());
                partition2Result.setErrorMessage(value.message());
                electionResult.partitionResult().add(partition2Result);

                electionResults.add(electionResult);

                env.kafkaClient().prepareResponse(new ElectLeadersResponse(0, Errors.NONE.code(),
                        electionResults, ApiKeys.ELECT_LEADERS.latestVersion()));
                ElectLeadersResult results = env.adminClient().electLeaders(
                        electionType,
                        Set.of(topic1, topic2));
                assertEquals(ClusterAuthorizationException.class, results.partitions().get().get(topic2).get().getClass());

                // Test a call where there are no errors. By mutating the internal of election results
                partition1Result.setErrorCode(ApiError.NONE.error().code());
                partition1Result.setErrorMessage(ApiError.NONE.message());

                partition2Result.setErrorCode(ApiError.NONE.error().code());
                partition2Result.setErrorMessage(ApiError.NONE.message());

                env.kafkaClient().prepareResponse(new ElectLeadersResponse(0, Errors.NONE.code(), electionResults,
                        ApiKeys.ELECT_LEADERS.latestVersion()));
                results = env.adminClient().electLeaders(electionType, Set.of(topic1, topic2));
                assertFalse(results.partitions().get().get(topic1).isPresent());
                assertFalse(results.partitions().get().get(topic2).isPresent());

                // Now try a timeout
                results = env.adminClient().electLeaders(
                        electionType,
                        Set.of(topic1, topic2),
                        new ElectLeadersOptions().timeoutMs(100));
                TestUtils.assertFutureThrows(TimeoutException.class, results.partitions());
            }
        }
    }

    @Test
    public void testDescribeLogDirs() throws ExecutionException, InterruptedException {
        Set<Integer> brokers = singleton(0);
        String logDir = "/var/data/kafka";
        TopicPartition tp = new TopicPartition("topic", 12);
        long partitionSize = 1234567890;
        long offsetLag = 24;

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(
                    prepareDescribeLogDirsResponse(Errors.NONE, logDir, tp, partitionSize, offsetLag),
                    env.cluster().nodeById(0));

            DescribeLogDirsResult result = env.adminClient().describeLogDirs(brokers);

            Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions = result.descriptions();
            assertEquals(brokers, descriptions.keySet());
            assertNotNull(descriptions.get(0));
            assertDescriptionContains(descriptions.get(0).get(), logDir, tp, partitionSize, offsetLag);

            Map<Integer, Map<String, LogDirDescription>> allDescriptions = result.allDescriptions().get();
            assertEquals(brokers, allDescriptions.keySet());
            assertDescriptionContains(allDescriptions.get(0), logDir, tp, partitionSize, offsetLag);

            // Empty results when not authorized with version < 3
            env.kafkaClient().prepareResponseFrom(
                    prepareEmptyDescribeLogDirsResponse(Optional.empty()),
                    env.cluster().nodeById(0));
            final DescribeLogDirsResult errorResult = env.adminClient().describeLogDirs(brokers);
            ExecutionException exception = assertThrows(ExecutionException.class, () -> errorResult.allDescriptions().get());
            assertInstanceOf(ClusterAuthorizationException.class, exception.getCause());

            // Empty results with an error with version >= 3
            env.kafkaClient().prepareResponseFrom(
                    prepareEmptyDescribeLogDirsResponse(Optional.of(Errors.UNKNOWN_SERVER_ERROR)),
                    env.cluster().nodeById(0));
            final DescribeLogDirsResult errorResult2 = env.adminClient().describeLogDirs(brokers);
            exception = assertThrows(ExecutionException.class, () -> errorResult2.allDescriptions().get());
            assertInstanceOf(UnknownServerException.class, exception.getCause());
        }
    }

    @Test
    public void testDescribeLogDirsWithVolumeBytes() throws ExecutionException, InterruptedException {
        Set<Integer> brokers = singleton(0);
        String logDir = "/var/data/kafka";
        TopicPartition tp = new TopicPartition("topic", 12);
        long partitionSize = 1234567890;
        long offsetLag = 24;
        long totalBytes = 123L;
        long usableBytes = 456L;

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(
                    prepareDescribeLogDirsResponse(Errors.NONE, logDir, tp, partitionSize, offsetLag, totalBytes, usableBytes),
                    env.cluster().nodeById(0));

            DescribeLogDirsResult result = env.adminClient().describeLogDirs(brokers);

            Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions = result.descriptions();
            assertEquals(brokers, descriptions.keySet());
            assertNotNull(descriptions.get(0));
            assertDescriptionContains(descriptions.get(0).get(), logDir, tp, partitionSize, offsetLag, OptionalLong.of(totalBytes), OptionalLong.of(usableBytes));

            Map<Integer, Map<String, LogDirDescription>> allDescriptions = result.allDescriptions().get();
            assertEquals(brokers, allDescriptions.keySet());
            assertDescriptionContains(allDescriptions.get(0), logDir, tp, partitionSize, offsetLag, OptionalLong.of(totalBytes), OptionalLong.of(usableBytes));

            // Empty results when not authorized with version < 3
            env.kafkaClient().prepareResponseFrom(
                    prepareEmptyDescribeLogDirsResponse(Optional.empty()),
                    env.cluster().nodeById(0));
            final DescribeLogDirsResult errorResult = env.adminClient().describeLogDirs(brokers);
            ExecutionException exception = assertThrows(ExecutionException.class, () -> errorResult.allDescriptions().get());
            assertInstanceOf(ClusterAuthorizationException.class, exception.getCause());

            // Empty results with an error with version >= 3
            env.kafkaClient().prepareResponseFrom(
                    prepareEmptyDescribeLogDirsResponse(Optional.of(Errors.UNKNOWN_SERVER_ERROR)),
                    env.cluster().nodeById(0));
            final DescribeLogDirsResult errorResult2 = env.adminClient().describeLogDirs(brokers);
            exception = assertThrows(ExecutionException.class, () -> errorResult2.allDescriptions().get());
            assertInstanceOf(UnknownServerException.class, exception.getCause());
        }
    }

    @Test
    public void testDescribeLogDirsOfflineDir() throws ExecutionException, InterruptedException {
        Set<Integer> brokers = singleton(0);
        String logDir = "/var/data/kafka";
        Errors error = Errors.KAFKA_STORAGE_ERROR;

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(
                    prepareDescribeLogDirsResponse(error, logDir, emptyList()),
                    env.cluster().nodeById(0));

            DescribeLogDirsResult result = env.adminClient().describeLogDirs(brokers);

            Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions = result.descriptions();
            assertEquals(brokers, descriptions.keySet());
            assertNotNull(descriptions.get(0));
            Map<String, LogDirDescription> descriptionsMap = descriptions.get(0).get();
            assertEquals(singleton(logDir), descriptionsMap.keySet());
            assertEquals(error.exception().getClass(), descriptionsMap.get(logDir).error().getClass());
            assertEquals(emptySet(), descriptionsMap.get(logDir).replicaInfos().keySet());

            Map<Integer, Map<String, LogDirDescription>> allDescriptions = result.allDescriptions().get();
            assertEquals(brokers, allDescriptions.keySet());
            Map<String, LogDirDescription> allMap = allDescriptions.get(0);
            assertNotNull(allMap);
            assertEquals(singleton(logDir), allMap.keySet());
            assertEquals(error.exception().getClass(), allMap.get(logDir).error().getClass());
            assertEquals(emptySet(), allMap.get(logDir).replicaInfos().keySet());
        }
    }

    @Test
    public void testDescribeLogDirsWithCordonedDir() throws ExecutionException, InterruptedException {
        Set<Integer> brokers = singleton(0);
        String logDir = "/var/data/kafka";
        TopicPartition tp = new TopicPartition("topic", 12);

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(
                    prepareDescribeLogDirsResponse(Errors.NONE, logDir, tp, 123, -1, -1, -1, true),
                    env.cluster().nodeById(0));

            DescribeLogDirsResult result = env.adminClient().describeLogDirs(brokers);

            Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions = result.descriptions();
            assertEquals(brokers, descriptions.keySet());
            assertNotNull(descriptions.get(0));
            Map<String, LogDirDescription> descriptionsMap = descriptions.get(0).get();
            assertEquals(singleton(logDir), descriptionsMap.keySet());
            assertTrue(descriptionsMap.get(logDir).isCordoned());
            assertEquals(Set.of(tp), descriptionsMap.get(logDir).replicaInfos().keySet());

            Map<Integer, Map<String, LogDirDescription>> allDescriptions = result.allDescriptions().get();
            assertEquals(brokers, allDescriptions.keySet());
            Map<String, LogDirDescription> allMap = allDescriptions.get(0);
            assertNotNull(allMap);
            assertEquals(singleton(logDir), allMap.keySet());
            assertTrue(allMap.get(logDir).isCordoned());
            assertEquals(Set.of(tp), allMap.get(logDir).replicaInfos().keySet());
        }
    }

    @Test
    public void testDescribeReplicaLogDirs() throws ExecutionException, InterruptedException {
        TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic", 12, 1);
        TopicPartitionReplica tpr2 = new TopicPartitionReplica("topic", 12, 2);

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            String broker1log0 = "/var/data/kafka0";
            String broker1log1 = "/var/data/kafka1";
            String broker2log0 = "/var/data/kafka2";
            int broker1Log0OffsetLag = 24;
            int broker1Log0PartitionSize = 987654321;
            int broker1Log1PartitionSize = 123456789;
            int broker1Log1OffsetLag = 4321;
            env.kafkaClient().prepareResponseFrom(
                    new DescribeLogDirsResponse(
                            new DescribeLogDirsResponseData().setResults(asList(
                                    prepareDescribeLogDirsResult(tpr1, broker1log0, broker1Log0PartitionSize, broker1Log0OffsetLag, false),
                                    prepareDescribeLogDirsResult(tpr1, broker1log1, broker1Log1PartitionSize, broker1Log1OffsetLag, true)))),
                    env.cluster().nodeById(tpr1.brokerId()));
            env.kafkaClient().prepareResponseFrom(
                    prepareDescribeLogDirsResponse(Errors.KAFKA_STORAGE_ERROR, broker2log0),
                    env.cluster().nodeById(tpr2.brokerId()));

            DescribeReplicaLogDirsResult result = env.adminClient().describeReplicaLogDirs(asList(tpr1, tpr2));

            Map<TopicPartitionReplica, KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> values = result.values();
            assertEquals(Set.of(tpr1, tpr2), values.keySet());

            assertNotNull(values.get(tpr1));
            assertEquals(broker1log0, values.get(tpr1).get().getCurrentReplicaLogDir());
            assertEquals(broker1Log0OffsetLag, values.get(tpr1).get().getCurrentReplicaOffsetLag());
            assertEquals(broker1log1, values.get(tpr1).get().getFutureReplicaLogDir());
            assertEquals(broker1Log1OffsetLag, values.get(tpr1).get().getFutureReplicaOffsetLag());

            assertNotNull(values.get(tpr2));
            assertNull(values.get(tpr2).get().getCurrentReplicaLogDir());
            assertEquals(-1, values.get(tpr2).get().getCurrentReplicaOffsetLag());
            assertNull(values.get(tpr2).get().getFutureReplicaLogDir());
            assertEquals(-1, values.get(tpr2).get().getFutureReplicaOffsetLag());
        }
    }

    @Test
    public void testDescribeReplicaLogDirsUnexpected() throws ExecutionException, InterruptedException {
        TopicPartitionReplica expected = new TopicPartitionReplica("topic", 12, 1);
        TopicPartitionReplica unexpected = new TopicPartitionReplica("topic", 12, 2);

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            String broker1log0 = "/var/data/kafka0";
            String broker1log1 = "/var/data/kafka1";
            int broker1Log0PartitionSize = 987654321;
            int broker1Log0OffsetLag = 24;
            int broker1Log1PartitionSize = 123456789;
            int broker1Log1OffsetLag = 4321;
            env.kafkaClient().prepareResponseFrom(
                    new DescribeLogDirsResponse(
                            new DescribeLogDirsResponseData().setResults(asList(
                                    prepareDescribeLogDirsResult(expected, broker1log0, broker1Log0PartitionSize, broker1Log0OffsetLag, false),
                                    prepareDescribeLogDirsResult(unexpected, broker1log1, broker1Log1PartitionSize, broker1Log1OffsetLag, true)))),
                    env.cluster().nodeById(expected.brokerId()));

            DescribeReplicaLogDirsResult result = env.adminClient().describeReplicaLogDirs(singletonList(expected));

            Map<TopicPartitionReplica, KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> values = result.values();
            assertEquals(Set.of(expected), values.keySet());

            assertNotNull(values.get(expected));
            assertEquals(broker1log0, values.get(expected).get().getCurrentReplicaLogDir());
            assertEquals(broker1Log0OffsetLag, values.get(expected).get().getCurrentReplicaOffsetLag());
            assertEquals(broker1log1, values.get(expected).get().getFutureReplicaLogDir());
            assertEquals(broker1Log1OffsetLag, values.get(expected).get().getFutureReplicaOffsetLag());
        }
    }

    @Test
    public void testCreatePartitions() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where one filter has an error.
            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("my_topic", "other_topic"),
                prepareCreatePartitionsResponse(1000,
                    createPartitionsTopicResult("my_topic", Errors.NONE),
                    createPartitionsTopicResult("other_topic", Errors.INVALID_TOPIC_EXCEPTION,
                        "some detailed reason")));

            Map<String, NewPartitions> counts = new HashMap<>();
            counts.put("my_topic", NewPartitions.increaseTo(3));
            counts.put("other_topic", NewPartitions.increaseTo(3, asList(singletonList(2), singletonList(3))));

            CreatePartitionsResult results = env.adminClient().createPartitions(counts);
            Map<String, KafkaFuture<Void>> values = results.values();
            KafkaFuture<Void> myTopicResult = values.get("my_topic");
            myTopicResult.get();
            KafkaFuture<Void> otherTopicResult = values.get("other_topic");
            assertEquals("some detailed reason",
                assertInstanceOf(InvalidTopicException.class,
                    assertThrows(ExecutionException.class, otherTopicResult::get).getCause()).getMessage());
        }
    }

    @Test
    public void testCreatePartitionsRetryThrottlingExceptionWhenEnabled() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareCreatePartitionsResponse(1000,
                    createPartitionsTopicResult("topic1", Errors.NONE),
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    createPartitionsTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("topic2"),
                prepareCreatePartitionsResponse(1000,
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED)));

            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("topic2"),
                prepareCreatePartitionsResponse(0,
                    createPartitionsTopicResult("topic2", Errors.NONE)));

            Map<String, NewPartitions> counts = new HashMap<>();
            counts.put("topic1", NewPartitions.increaseTo(1));
            counts.put("topic2", NewPartitions.increaseTo(2));
            counts.put("topic3", NewPartitions.increaseTo(3));

            CreatePartitionsResult result = env.adminClient().createPartitions(
                counts, new CreatePartitionsOptions().retryOnQuotaViolation(true));

            assertNull(result.values().get("topic1").get());
            assertNull(result.values().get("topic2").get());
            TestUtils.assertFutureThrows(TopicExistsException.class, result.values().get("topic3"));
        }
    }

    @Test
    public void testCreatePartitionsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut() throws Exception {
        long defaultApiTimeout = 60000;
        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = mockClientEnv(time,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(defaultApiTimeout))) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareCreatePartitionsResponse(1000,
                    createPartitionsTopicResult("topic1", Errors.NONE),
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    createPartitionsTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("topic2"),
                prepareCreatePartitionsResponse(1000,
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED)));

            Map<String, NewPartitions> counts = new HashMap<>();
            counts.put("topic1", NewPartitions.increaseTo(1));
            counts.put("topic2", NewPartitions.increaseTo(2));
            counts.put("topic3", NewPartitions.increaseTo(3));

            CreatePartitionsResult result = env.adminClient().createPartitions(
                counts, new CreatePartitionsOptions().retryOnQuotaViolation(true));

            // Wait until the prepared attempts have consumed
            TestUtils.waitForCondition(() -> env.kafkaClient().numAwaitingResponses() == 0,
                "Failed awaiting CreatePartitions requests");

            // Wait until the next request is sent out
            TestUtils.waitForCondition(() -> env.kafkaClient().inFlightRequestCount() == 1,
                "Failed awaiting next CreatePartitions request");

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout + 1);

            assertNull(result.values().get("topic1").get());
            ThrottlingQuotaExceededException e = TestUtils.assertFutureThrows(ThrottlingQuotaExceededException.class, result.values().get("topic2"));
            assertEquals(0, e.throttleTimeMs());
            TestUtils.assertFutureThrows(TopicExistsException.class, result.values().get("topic3"));
        }
    }

    @Test
    public void testCreatePartitionsDontRetryThrottlingExceptionWhenDisabled() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareCreatePartitionsResponse(1000,
                    createPartitionsTopicResult("topic1", Errors.NONE),
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    createPartitionsTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            Map<String, NewPartitions> counts = new HashMap<>();
            counts.put("topic1", NewPartitions.increaseTo(1));
            counts.put("topic2", NewPartitions.increaseTo(2));
            counts.put("topic3", NewPartitions.increaseTo(3));

            CreatePartitionsResult result = env.adminClient().createPartitions(
                counts, new CreatePartitionsOptions().retryOnQuotaViolation(false));

            assertNull(result.values().get("topic1").get());
            ThrottlingQuotaExceededException e = TestUtils.assertFutureThrows(ThrottlingQuotaExceededException.class, result.values().get("topic2"));
            assertEquals(1000, e.throttleTimeMs());
            TestUtils.assertFutureThrows(TopicExistsException.class, result.values().get("topic3"));
        }
    }

    @Test
    public void testDeleteRecordsTopicAuthorizationError() {
        String topic = "foo";
        TopicPartition partition = new TopicPartition(topic, 0);

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
            topics.add(new MetadataResponse.TopicMetadata(Errors.TOPIC_AUTHORIZATION_FAILED, topic, false,
                    Collections.emptyList()));

            env.kafkaClient().prepareResponse(RequestTestUtils.metadataResponse(env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(), env.cluster().controller().id(), topics));

            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            recordsToDelete.put(partition, RecordsToDelete.beforeOffset(10L));
            DeleteRecordsResult results = env.adminClient().deleteRecords(recordsToDelete);

            TestUtils.assertFutureThrows(TopicAuthorizationException.class, results.lowWatermarks().get(partition));
        }
    }

    @Test
    public void testDeleteRecordsMultipleSends() throws Exception {
        String topic = "foo";
        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);

        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, mockCluster(3, 0))) {
            List<Node> nodes = env.cluster().nodes();

            List<MetadataResponse.PartitionMetadata> partitionMetadata = new ArrayList<>();
            partitionMetadata.add(new MetadataResponse.PartitionMetadata(Errors.NONE, tp0,
                    Optional.of(nodes.get(0).id()), Optional.of(5), singletonList(nodes.get(0).id()),
                    singletonList(nodes.get(0).id()), Collections.emptyList()));
            partitionMetadata.add(new MetadataResponse.PartitionMetadata(Errors.NONE, tp1,
                    Optional.of(nodes.get(1).id()), Optional.of(5), singletonList(nodes.get(1).id()),
                    singletonList(nodes.get(1).id()), Collections.emptyList()));

            List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
            topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, topic, false, partitionMetadata));

            env.kafkaClient().prepareResponse(RequestTestUtils.metadataResponse(env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(), env.cluster().controller().id(), topicMetadata));

            env.kafkaClient().prepareResponseFrom(new DeleteRecordsResponse(new DeleteRecordsResponseData().setTopics(
                    new DeleteRecordsResponseData.DeleteRecordsTopicResultCollection(singletonList(new DeleteRecordsResponseData.DeleteRecordsTopicResult()
                            .setName(tp0.topic())
                            .setPartitions(new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection(singletonList(new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                                    .setPartitionIndex(tp0.partition())
                                    .setErrorCode(Errors.NONE.code())
                                    .setLowWatermark(3)))))))), nodes.get(0));

            env.kafkaClient().disconnect(nodes.get(1).idString());
            env.kafkaClient().createPendingAuthenticationError(nodes.get(1), 100);

            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            recordsToDelete.put(tp0, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(tp1, RecordsToDelete.beforeOffset(10L));
            DeleteRecordsResult results = env.adminClient().deleteRecords(recordsToDelete);

            assertEquals(3L, results.lowWatermarks().get(tp0).get().lowWatermark());
            TestUtils.assertFutureThrows(SaslAuthenticationException.class, results.lowWatermarks().get(tp1));
        }
    }

    @Test
    public void testDeleteRecords() throws Exception {
        HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        partitionInfos.add(new PartitionInfo("my_topic", 0, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 1, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 2, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 3, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));

        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                partitionInfos, Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
        TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
        TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);
        TopicPartition myTopicPartition3 = new TopicPartition("my_topic", 3);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.LEADER_NOT_AVAILABLE));
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.UNKNOWN_TOPIC_OR_PARTITION));
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            DeleteRecordsResponseData m = new DeleteRecordsResponseData();
            m.topics().add(new DeleteRecordsResponseData.DeleteRecordsTopicResult().setName(myTopicPartition0.topic())
                    .setPartitions(new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection(asList(
                        new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                            .setPartitionIndex(myTopicPartition0.partition())
                            .setLowWatermark(3)
                            .setErrorCode(Errors.NONE.code()),
                        new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                            .setPartitionIndex(myTopicPartition1.partition())
                            .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                            .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code()),
                        new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                            .setPartitionIndex(myTopicPartition2.partition())
                            .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
                    ))));
            env.kafkaClient().prepareResponse(new DeleteRecordsResponse(m));

            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            recordsToDelete.put(myTopicPartition0, RecordsToDelete.beforeOffset(3L));
            recordsToDelete.put(myTopicPartition1, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(myTopicPartition2, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(myTopicPartition3, RecordsToDelete.beforeOffset(10L));

            DeleteRecordsResult results = env.adminClient().deleteRecords(recordsToDelete);

            // success on records deletion for partition 0
            Map<TopicPartition, KafkaFuture<DeletedRecords>> values = results.lowWatermarks();
            KafkaFuture<DeletedRecords> myTopicPartition0Result = values.get(myTopicPartition0);
            long myTopicPartition0lowWatermark = myTopicPartition0Result.get().lowWatermark();
            assertEquals(3, myTopicPartition0lowWatermark);

            // "offset out of range" failure on records deletion for partition 1
            KafkaFuture<DeletedRecords> myTopicPartition1Result = values.get(myTopicPartition1);
            assertInstanceOf(OffsetOutOfRangeException.class,
                assertThrows(ExecutionException.class, myTopicPartition1Result::get).getCause());

            // not authorized to delete records for partition 2
            KafkaFuture<DeletedRecords> myTopicPartition2Result = values.get(myTopicPartition2);
            assertInstanceOf(TopicAuthorizationException.class,
                assertThrows(ExecutionException.class, myTopicPartition2Result::get).getCause());

            // the response does not contain a result for partition 3
            KafkaFuture<DeletedRecords> myTopicPartition3Result = values.get(myTopicPartition3);
            assertInstanceOf(ApiException.class,
                assertThrows(ExecutionException.class, myTopicPartition3Result::get).getCause());
        }
    }

    @Test
    public void testDescribeTopicsByIds() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Valid ID
            Uuid topicId = Uuid.randomUuid();
            String topicName = "test-topic";
            Node leader = env.cluster().nodes().get(0);
            MetadataResponse.PartitionMetadata partitionMetadata = new MetadataResponse.PartitionMetadata(
                    Errors.NONE,
                    new TopicPartition(topicName, 0),
                    Optional.of(leader.id()),
                    Optional.of(10),
                    singletonList(leader.id()),
                    singletonList(leader.id()),
                    singletonList(leader.id()));
            env.kafkaClient().prepareResponse(RequestTestUtils
                    .metadataResponse(
                            env.cluster().nodes(),
                            env.cluster().clusterResource().clusterId(),
                            env.cluster().controller().id(),
                            singletonList(new MetadataResponse.TopicMetadata(Errors.NONE, topicName, topicId, false,
                                    singletonList(partitionMetadata), MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED))));
            TopicCollection.TopicIdCollection topicIds = TopicCollection.ofTopicIds(
                    singletonList(topicId));

            DescribeTopicsResult describeTopicsresult = env.adminClient().describeTopics(topicIds);
            Map<Uuid, TopicDescription> allTopicIds = describeTopicsresult.allTopicIds().get();
            assertEquals(topicName, allTopicIds.get(topicId).name());

            // ID not exist in brokers
            Uuid nonExistID = Uuid.randomUuid();
            env.kafkaClient().prepareResponse(RequestTestUtils
                    .metadataResponse(
                            env.cluster().nodes(),
                            env.cluster().clusterResource().clusterId(),
                            env.cluster().controller().id(),
                            emptyList()));

            DescribeTopicsResult result1 = env.adminClient().describeTopics(
                    TopicCollection.ofTopicIds(singletonList(nonExistID)));
            TestUtils.assertFutureThrows(UnknownTopicIdException.class, result1.allTopicIds());
            Exception e = assertThrows(Exception.class, () -> result1.allTopicIds().get(), "describe with non-exist topic ID should throw exception");
            assertEquals(String.format("org.apache.kafka.common.errors.UnknownTopicIdException: TopicId %s not found.", nonExistID), e.getMessage());

            DescribeTopicsResult result2 = env.adminClient().describeTopics(
                    TopicCollection.ofTopicIds(singletonList(Uuid.ZERO_UUID)));
            TestUtils.assertFutureThrows(InvalidTopicException.class, result2.allTopicIds());
            e = assertThrows(Exception.class, () -> result2.allTopicIds().get(), "describe with non-exist topic ID should throw exception");
            assertEquals("The given topic id 'AAAAAAAAAAAAAAAAAAAAAA' cannot be represented in a request.", e.getCause().getMessage());

        }
    }

    @Test
    public void testAlterPartitionReassignments() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            TopicPartition tp1 = new TopicPartition("A", 0);
            TopicPartition tp2 = new TopicPartition("B", 0);
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>();
            reassignments.put(tp1, Optional.empty());
            reassignments.put(tp2, Optional.of(new NewPartitionReassignment(asList(1, 2, 3))));

            // 1. server returns less responses than number of partitions we sent
            AlterPartitionReassignmentsResponseData responseData1 = new AlterPartitionReassignmentsResponseData();
            ReassignablePartitionResponse normalPartitionResponse = new ReassignablePartitionResponse().setPartitionIndex(0);
            responseData1.setResponses(Collections.singletonList(
                    new ReassignableTopicResponse()
                            .setName("A")
                            .setPartitions(Collections.singletonList(normalPartitionResponse))));
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(responseData1));
            AlterPartitionReassignmentsResult result1 = env.adminClient().alterPartitionReassignments(reassignments);
            Future<Void> future1 = result1.all();
            Future<Void> future2 = result1.values().get(tp1);
            TestUtils.assertFutureThrows(UnknownServerException.class, future1);
            TestUtils.assertFutureThrows(UnknownServerException.class, future2);

            // 2. NOT_CONTROLLER error handling
            AlterPartitionReassignmentsResponseData controllerErrResponseData =
                    new AlterPartitionReassignmentsResponseData()
                            .setErrorCode(Errors.NOT_CONTROLLER.code())
                            .setErrorMessage(Errors.NOT_CONTROLLER.message())
                            .setResponses(asList(
                                new ReassignableTopicResponse()
                                        .setName("A")
                                        .setPartitions(Collections.singletonList(normalPartitionResponse)),
                                new ReassignableTopicResponse()
                                        .setName("B")
                                        .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            MetadataResponse controllerNodeResponse = RequestTestUtils.metadataResponse(env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(), 1, Collections.emptyList());
            AlterPartitionReassignmentsResponseData normalResponse =
                    new AlterPartitionReassignmentsResponseData()
                            .setResponses(asList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)),
                                    new ReassignableTopicResponse()
                                            .setName("B")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(controllerErrResponseData));
            env.kafkaClient().prepareResponse(controllerNodeResponse);
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(normalResponse));
            AlterPartitionReassignmentsResult controllerErrResult = env.adminClient().alterPartitionReassignments(reassignments);
            controllerErrResult.all().get();
            controllerErrResult.values().get(tp1).get();
            controllerErrResult.values().get(tp2).get();

            // 3. partition-level error
            AlterPartitionReassignmentsResponseData partitionLevelErrData =
                    new AlterPartitionReassignmentsResponseData()
                            .setResponses(asList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(new ReassignablePartitionResponse()
                                                .setPartitionIndex(0).setErrorMessage(Errors.INVALID_REPLICA_ASSIGNMENT.message())
                                                .setErrorCode(Errors.INVALID_REPLICA_ASSIGNMENT.code())
                                            )),
                                    new ReassignableTopicResponse()
                                            .setName("B")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(partitionLevelErrData));
            AlterPartitionReassignmentsResult partitionLevelErrResult = env.adminClient().alterPartitionReassignments(reassignments);
            TestUtils.assertFutureThrows(InvalidReplicaAssignmentException.class, partitionLevelErrResult.values().get(tp1));
            partitionLevelErrResult.values().get(tp2).get();

            // 4. top-level error
            String errorMessage = "this is custom error message";
            AlterPartitionReassignmentsResponseData topLevelErrResponseData =
                    new AlterPartitionReassignmentsResponseData()
                            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                            .setErrorMessage(errorMessage)
                            .setResponses(asList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)),
                                    new ReassignableTopicResponse()
                                            .setName("B")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(topLevelErrResponseData));
            AlterPartitionReassignmentsResult topLevelErrResult = env.adminClient().alterPartitionReassignments(reassignments);
            assertEquals(errorMessage, TestUtils.assertFutureThrows(ClusterAuthorizationException.class, topLevelErrResult.all()).getMessage());
            assertEquals(errorMessage, TestUtils.assertFutureThrows(ClusterAuthorizationException.class, topLevelErrResult.values().get(tp1)).getMessage());
            assertEquals(errorMessage, TestUtils.assertFutureThrows(ClusterAuthorizationException.class, topLevelErrResult.values().get(tp2)).getMessage());

            // 5. unrepresentable topic name error
            TopicPartition invalidTopicTP = new TopicPartition("", 0);
            TopicPartition invalidPartitionTP = new TopicPartition("ABC", -1);
            Map<TopicPartition, Optional<NewPartitionReassignment>> invalidTopicReassignments = new HashMap<>();
            invalidTopicReassignments.put(invalidPartitionTP, Optional.of(new NewPartitionReassignment(asList(1, 2, 3))));
            invalidTopicReassignments.put(invalidTopicTP, Optional.of(new NewPartitionReassignment(asList(1, 2, 3))));
            invalidTopicReassignments.put(tp1, Optional.of(new NewPartitionReassignment(asList(1, 2, 3))));

            AlterPartitionReassignmentsResponseData singlePartResponseData =
                    new AlterPartitionReassignmentsResponseData()
                            .setResponses(Collections.singletonList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(singlePartResponseData));
            AlterPartitionReassignmentsResult unrepresentableTopicResult = env.adminClient().alterPartitionReassignments(invalidTopicReassignments);
            TestUtils.assertFutureThrows(InvalidTopicException.class, unrepresentableTopicResult.values().get(invalidTopicTP));
            TestUtils.assertFutureThrows(InvalidTopicException.class, unrepresentableTopicResult.values().get(invalidPartitionTP));
            unrepresentableTopicResult.values().get(tp1).get();

            // Test success scenario
            AlterPartitionReassignmentsResponseData noErrResponseData =
                    new AlterPartitionReassignmentsResponseData()
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage(Errors.NONE.message())
                            .setResponses(asList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)),
                                    new ReassignableTopicResponse()
                                            .setName("B")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(noErrResponseData));
            AlterPartitionReassignmentsResult noErrResult = env.adminClient().alterPartitionReassignments(reassignments);
            noErrResult.all().get();
            noErrResult.values().get(tp1).get();
            noErrResult.values().get(tp2).get();
        }
    }

    @Test
    public void testListPartitionReassignments() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            TopicPartition tp1 = new TopicPartition("A", 0);
            OngoingPartitionReassignment tp1PartitionReassignment = new OngoingPartitionReassignment()
                    .setPartitionIndex(0)
                    .setRemovingReplicas(asList(1, 2, 3))
                    .setAddingReplicas(asList(4, 5, 6))
                    .setReplicas(asList(1, 2, 3, 4, 5, 6));
            OngoingTopicReassignment tp1Reassignment = new OngoingTopicReassignment().setName("A")
                    .setPartitions(Collections.singletonList(tp1PartitionReassignment));

            TopicPartition tp2 = new TopicPartition("B", 0);
            OngoingPartitionReassignment tp2PartitionReassignment = new OngoingPartitionReassignment()
                    .setPartitionIndex(0)
                    .setRemovingReplicas(asList(1, 2, 3))
                    .setAddingReplicas(asList(4, 5, 6))
                    .setReplicas(asList(1, 2, 3, 4, 5, 6));
            OngoingTopicReassignment tp2Reassignment = new OngoingTopicReassignment().setName("B")
                    .setPartitions(Collections.singletonList(tp2PartitionReassignment));

            // 1. NOT_CONTROLLER error handling
            ListPartitionReassignmentsResponseData notControllerData = new ListPartitionReassignmentsResponseData()
                    .setErrorCode(Errors.NOT_CONTROLLER.code())
                    .setErrorMessage(Errors.NOT_CONTROLLER.message());
            MetadataResponse controllerNodeResponse = RequestTestUtils.metadataResponse(env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(), 1, Collections.emptyList());
            ListPartitionReassignmentsResponseData reassignmentsData = new ListPartitionReassignmentsResponseData()
                    .setTopics(asList(tp1Reassignment, tp2Reassignment));
            env.kafkaClient().prepareResponse(new ListPartitionReassignmentsResponse(notControllerData));
            env.kafkaClient().prepareResponse(controllerNodeResponse);
            env.kafkaClient().prepareResponse(new ListPartitionReassignmentsResponse(reassignmentsData));

            ListPartitionReassignmentsResult noControllerResult = env.adminClient().listPartitionReassignments();
            noControllerResult.reassignments().get(); // no error

            // 2. UNKNOWN_TOPIC_OR_EXCEPTION_ERROR
            ListPartitionReassignmentsResponseData unknownTpData = new ListPartitionReassignmentsResponseData()
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
            env.kafkaClient().prepareResponse(new ListPartitionReassignmentsResponse(unknownTpData));

            ListPartitionReassignmentsResult unknownTpResult = env.adminClient().listPartitionReassignments(Set.of(tp1, tp2));
            TestUtils.assertFutureThrows(UnknownTopicOrPartitionException.class, unknownTpResult.reassignments());

            // 3. Success
            ListPartitionReassignmentsResponseData responseData = new ListPartitionReassignmentsResponseData()
                    .setTopics(asList(tp1Reassignment, tp2Reassignment));
            env.kafkaClient().prepareResponse(new ListPartitionReassignmentsResponse(responseData));
            ListPartitionReassignmentsResult responseResult = env.adminClient().listPartitionReassignments();

            Map<TopicPartition, PartitionReassignment> reassignments = responseResult.reassignments().get();

            PartitionReassignment tp1Result = reassignments.get(tp1);
            assertEquals(tp1PartitionReassignment.addingReplicas(), tp1Result.addingReplicas());
            assertEquals(tp1PartitionReassignment.removingReplicas(), tp1Result.removingReplicas());
            assertEquals(tp1PartitionReassignment.replicas(), tp1Result.replicas());
            assertEquals(tp1PartitionReassignment.replicas(), tp1Result.replicas());
            PartitionReassignment tp2Result = reassignments.get(tp2);
            assertEquals(tp2PartitionReassignment.addingReplicas(), tp2Result.addingReplicas());
            assertEquals(tp2PartitionReassignment.removingReplicas(), tp2Result.removingReplicas());
            assertEquals(tp2PartitionReassignment.replicas(), tp2Result.replicas());
            assertEquals(tp2PartitionReassignment.replicas(), tp2Result.replicas());
        }
    }

    @Test
    public void testListOffsets() throws Exception {
        // Happy path

        Node node0 = new Node(0, "localhost", 8120);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node0, new Node[]{node0}, new Node[]{node0}));
        pInfos.add(new PartitionInfo("bar", 0, node0, new Node[]{node0}, new Node[]{node0}));
        pInfos.add(new PartitionInfo("baz", 0, node0, new Node[]{node0}, new Node[]{node0}));
        pInfos.add(new PartitionInfo("qux", 0, node0, new Node[]{node0}, new Node[]{node0}));
        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                singletonList(node0),
                pInfos,
                Collections.emptySet(),
                Collections.emptySet(),
                node0);

        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("bar", 0);
        final TopicPartition tp2 = new TopicPartition("baz", 0);
        final TopicPartition tp3 = new TopicPartition("qux", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -1L, 123L, 321);
            ListOffsetsTopicResponse t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.NONE, -1L, 234L, 432);
            ListOffsetsTopicResponse t2 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp2, Errors.NONE, 123456789L, 345L, 543);
            ListOffsetsTopicResponse t3 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp3, Errors.NONE, 234567890L, 456L, 654);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(asList(t0, t1, t2, t3));
            env.kafkaClient().prepareResponse(new ListOffsetsResponse(responseData));

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            partitions.put(tp1, OffsetSpec.earliest());
            partitions.put(tp2, OffsetSpec.forTimestamp(System.currentTimeMillis()));
            partitions.put(tp3, OffsetSpec.maxTimestamp());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);

            Map<TopicPartition, ListOffsetsResultInfo> offsets = result.all().get();
            assertFalse(offsets.isEmpty());
            assertEquals(123L, offsets.get(tp0).offset());
            assertEquals(321, offsets.get(tp0).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp0).timestamp());
            assertEquals(234L, offsets.get(tp1).offset());
            assertEquals(432, offsets.get(tp1).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp1).timestamp());
            assertEquals(345L, offsets.get(tp2).offset());
            assertEquals(543, offsets.get(tp2).leaderEpoch().get().intValue());
            assertEquals(123456789L, offsets.get(tp2).timestamp());
            assertEquals(456L, offsets.get(tp3).offset());
            assertEquals(654, offsets.get(tp3).leaderEpoch().get().intValue());
            assertEquals(234567890L, offsets.get(tp3).timestamp());
            assertEquals(offsets.get(tp0), result.partitionResult(tp0).get());
            assertEquals(offsets.get(tp1), result.partitionResult(tp1).get());
            assertEquals(offsets.get(tp2), result.partitionResult(tp2).get());
            assertEquals(offsets.get(tp3), result.partitionResult(tp3).get());
            assertThrows(IllegalArgumentException.class, () -> result.partitionResult(new TopicPartition("unknown", 0)).get());
        }
    }

    /**
     * Reproduces the scenario where the partition leader cache holds an entry pointing at a broker
     * that has since left the cluster (for example after a broker is recycled with a new id). The
     * cached leader sends the request straight to the fulfillment stage, but the admin client can
     * never route it because the broker is no longer in the metadata. Without re-running the lookup,
     * the call would sit unassigned until the request deadline expires and fail with
     * "Timed out waiting for a node assignment". The admin client should instead re-resolve the
     * leader and complete the request.
     */
    @Test
    public void testListOffsetsRetriesLookupWhenCachedLeaderLeavesCluster() throws Exception {
        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        final TopicPartition tp0 = new TopicPartition("foo", 0);

        // Initially foo-0 is led by node1.
        final Cluster initialCluster = new Cluster("mockClusterId", asList(node0, node1),
            singletonList(new PartitionInfo("foo", 0, node1, new Node[]{node0, node1}, new Node[]{node0, node1})),
            emptySet(), emptySet(), node0);
        // After node1 leaves the cluster, foo-0 is led by node0.
        final Cluster shrunkCluster = new Cluster("mockClusterId", singletonList(node0),
            singletonList(new PartitionInfo("foo", 0, node0, new Node[]{node0}, new Node[]{node0})),
            emptySet(), emptySet(), node0);

        MockTime time = new MockTime();
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, initialCluster,
                newStrMap(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000",
                          AdminClientConfig.METADATA_MAX_AGE_CONFIG, "50"))) {
            MockClient mockClient = env.kafkaClient();
            mockClient.setNodeApiVersions(NodeApiVersions.create());

            // First call: the lookup resolves foo-0 to node1 and caches it, then the offsets fetch
            // succeeds on node1.
            mockClient.prepareResponse(body -> body instanceof MetadataRequest,
                prepareMetadataResponse(initialCluster, Errors.NONE));
            mockClient.prepareResponseFrom(listOffsetsResponse(tp0, 100L), node1);
            assertEquals(100L, env.adminClient().listOffsets(singletonMap(tp0, OffsetSpec.latest()))
                .all().get().get(tp0).offset());

            // Drop node1 from the admin client's metadata via the periodic broker-info refresh.
            // Waiting for a second refresh guarantees the first one has been fully processed.
            AtomicInteger refreshes = new AtomicInteger();
            TestUtils.waitForCondition(() -> {
                time.sleep(20);
                if (respondToBrokerInfoRefresh(mockClient, shrunkCluster))
                    refreshes.incrementAndGet();
                return refreshes.get() >= 2;
            }, "Timed out waiting for the broker-info metadata refresh to drop node1");

            // Second call: the cache still points foo-0 at node1, which is gone. The admin client
            // must re-resolve the leader (now node0) rather than getting stuck until the deadline.
            ListOffsetsResult result = env.adminClient().listOffsets(singletonMap(tp0, OffsetSpec.latest()));
            TestUtils.waitForCondition(() -> {
                time.sleep(20);
                // Keep node1 out of the metadata, re-resolve foo-0 to node0, and satisfy the fetch.
                respondToBrokerInfoRefresh(mockClient, shrunkCluster);
                respondToTopicMetadata(mockClient, "foo", shrunkCluster);
                respondToListOffsets(mockClient, tp0, 200L, node0);
                return result.all().isDone();
            }, "Timed out waiting for listOffsets to recover after the cached leader left the cluster");

            assertEquals(200L, result.all().get().get(tp0).offset());
        }
    }

    private static ListOffsetsResponse listOffsetsResponse(TopicPartition tp, long offset) {
        return new ListOffsetsResponse(new ListOffsetsResponseData().setTopics(singletonList(
            ListOffsetsResponse.singletonListOffsetsTopicResponse(tp, Errors.NONE, -1L, offset, 5))));
    }

    /**
     * Respond out of order to the first in-flight request matching {@code matcher} with
     * {@code response}. Returns true if a matching request was found and answered.
     */
    private static boolean respondToInFlightRequest(MockClient mockClient,
                                                    Predicate<ClientRequest> matcher,
                                                    AbstractResponse response) {
        for (ClientRequest request : mockClient.requests()) {
            if (matcher.test(request)) {
                mockClient.respondToRequest(request, response);
                return true;
            }
        }
        return false;
    }

    private static boolean respondToBrokerInfoRefresh(MockClient mockClient, Cluster cluster) {
        return respondToInFlightRequest(mockClient, request -> {
            AbstractRequest body = request.requestBuilder().build();
            return body instanceof MetadataRequest && ((MetadataRequest) body).topics().isEmpty();
        }, prepareMetadataResponse(cluster, Errors.NONE));
    }

    private static boolean respondToTopicMetadata(MockClient mockClient, String topic, Cluster cluster) {
        return respondToInFlightRequest(mockClient, request -> {
            AbstractRequest body = request.requestBuilder().build();
            return body instanceof MetadataRequest && ((MetadataRequest) body).topics().contains(topic);
        }, prepareMetadataResponse(cluster, Errors.NONE));
    }

    private static boolean respondToListOffsets(MockClient mockClient, TopicPartition tp, long offset, Node node) {
        return respondToInFlightRequest(mockClient,
            request -> request.requestBuilder().build() instanceof ListOffsetsRequest
                && request.destination().equals(node.idString()),
            listOffsetsResponse(tp, offset));
    }

    @Test
    public void testListOffsetsRetriableErrors() throws Exception {

        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        List<Node> nodes = asList(node0, node1);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node0, new Node[]{node0, node1}, new Node[]{node0, node1}));
        pInfos.add(new PartitionInfo("foo", 1, node0, new Node[]{node0, node1}, new Node[]{node0, node1}));
        pInfos.add(new PartitionInfo("bar", 0, node1, new Node[]{node1, node0}, new Node[]{node1, node0}));
        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.emptySet(),
                Collections.emptySet(),
                node0);

        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("foo", 1);
        final TopicPartition tp2 = new TopicPartition("bar", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));
            // listoffsets response from broker 0
            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.LEADER_NOT_AVAILABLE, -1L, 123L, 321);
            ListOffsetsTopicResponse t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.NONE, -1L, 987L, 789);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(asList(t0, t1));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node0);
            // listoffsets response from broker 1
            ListOffsetsTopicResponse t2 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp2, Errors.NONE, -1L, 456L, 654);
            responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(singletonList(t2));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node1);

            // metadata refresh because of LEADER_NOT_AVAILABLE
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));
            // listoffsets response from broker 0
            t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -1L, 345L, 543);
            responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(singletonList(t0));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node0);

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            partitions.put(tp1, OffsetSpec.latest());
            partitions.put(tp2, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);

            Map<TopicPartition, ListOffsetsResultInfo> offsets = result.all().get();
            assertFalse(offsets.isEmpty());
            assertEquals(345L, offsets.get(tp0).offset());
            assertEquals(543, offsets.get(tp0).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp0).timestamp());
            assertEquals(987L, offsets.get(tp1).offset());
            assertEquals(789, offsets.get(tp1).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp1).timestamp());
            assertEquals(456L, offsets.get(tp2).offset());
            assertEquals(654, offsets.get(tp2).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp2).timestamp());
        }
    }

    @Test
    public void testListOffsetsNonRetriableErrors() throws Exception {

        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        List<Node> nodes = asList(node0, node1);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node0, new Node[]{node0, node1}, new Node[]{node0, node1}));
        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.emptySet(),
                Collections.emptySet(),
                node0);

        final TopicPartition tp0 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.TOPIC_AUTHORIZATION_FAILED, -1L, -1L, -1);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(singletonList(t0));
            env.kafkaClient().prepareResponse(new ListOffsetsResponse(responseData));

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);

            TestUtils.assertFutureThrows(TopicAuthorizationException.class, result.all());
        }
    }

    @Test
    public void testListOffsetsMaxTimestampUnsupportedSingleOffsetSpec() {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        final Cluster cluster = new Cluster(
            "mockClusterId",
            nodes,
            Collections.singleton(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node})),
            Collections.emptySet(),
            Collections.emptySet(),
            node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster, AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(
                    ApiKeys.LIST_OFFSETS.id, (short) 0, (short) 6));
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            // listoffsets response from broker 0
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof ListOffsetsRequest);

            ListOffsetsResult result = env.adminClient().listOffsets(Collections.singletonMap(tp0, OffsetSpec.maxTimestamp()));

            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.all());
        }
    }

    @Test
    public void testListOffsetsMaxTimestampUnsupportedMultipleOffsetSpec() throws Exception {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node}));
        pInfos.add(new PartitionInfo("foo", 1, node, new Node[]{node}, new Node[]{node}));
        final Cluster cluster = new Cluster(
            "mockClusterId",
            nodes,
            pInfos,
            Collections.emptySet(),
            Collections.emptySet(),
            node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("foo", 1);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster,
            AdminClientConfig.RETRIES_CONFIG, "2")) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(
                    ApiKeys.LIST_OFFSETS.id, (short) 0, (short) 6));
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            // listoffsets response from broker 0
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof ListOffsetsRequest);

            ListOffsetsTopicResponse topicResponse = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.NONE, -1L, 345L, 543);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(singletonList(topicResponse));
            env.kafkaClient().prepareResponseFrom(
                // ensure that no max timestamp requests are retried
                request -> request instanceof ListOffsetsRequest && ((ListOffsetsRequest) request).topics().stream()
                    .flatMap(t -> t.partitions().stream())
                    .noneMatch(p -> p.timestamp() == ListOffsetsRequest.MAX_TIMESTAMP),
                new ListOffsetsResponse(responseData), node);

            ListOffsetsResult result = env.adminClient().listOffsets(new HashMap<>() {{
                    put(tp0, OffsetSpec.maxTimestamp());
                    put(tp1, OffsetSpec.latest());
                }});

            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.partitionResult(tp0));

            ListOffsetsResultInfo tp1Offset = result.partitionResult(tp1).get();
            assertEquals(345L, tp1Offset.offset());
            assertEquals(543, tp1Offset.leaderEpoch().get().intValue());
            assertEquals(-1L, tp1Offset.timestamp());
        }
    }

    @Test
    public void testListOffsetsHandlesFulfillmentTimeouts() throws Exception {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node}));
        pInfos.add(new PartitionInfo("foo", 1, node, new Node[]{node}, new Node[]{node}));
        final Cluster cluster = new Cluster(
            "mockClusterId",
            nodes,
            pInfos,
            Collections.emptySet(),
            Collections.emptySet(),
            node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("foo", 1);

        int numRetries = 2;
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster,
            AdminClientConfig.RETRIES_CONFIG, Integer.toString(numRetries))) {

            ListOffsetsTopicResponse tp0ErrorResponse =
                ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.REQUEST_TIMED_OUT, -1L, -1L, -1);
            ListOffsetsTopicResponse tp1Response =
                ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.NONE, -1L, 345L, 543);
            ListOffsetsResponseData responseDataWithError = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(asList(tp0ErrorResponse, tp1Response));

            ListOffsetsTopicResponse tp0Response =
                ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -1L, 789L, 987);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(asList(tp0Response, tp1Response));

            // Test that one-too-many timeouts for partition 0 result in partial success overall -
            // timeout for partition 0 and success for partition 1.

            // It might be desirable to have the AdminApiDriver mechanism also handle all retriable
            // exceptions like TimeoutException during the lookup stage (it currently doesn't).
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));
            for (int i = 0; i < numRetries + 1; i++) {
                env.kafkaClient().prepareResponseFrom(
                    request -> request instanceof ListOffsetsRequest,
                    new ListOffsetsResponse(responseDataWithError), node);
            }
            ListOffsetsResult result = env.adminClient().listOffsets(
                new HashMap<>() {
                    {
                        put(tp0, OffsetSpec.latest());
                        put(tp1, OffsetSpec.latest());
                    }
                });
            TestUtils.assertFutureThrows(TimeoutException.class, result.partitionResult(tp0));
            ListOffsetsResultInfo tp1Result = result.partitionResult(tp1).get();
            assertEquals(345L, tp1Result.offset());
            assertEquals(543, tp1Result.leaderEpoch().get().intValue());
            assertEquals(-1L, tp1Result.timestamp());

            // Now test that only numRetries timeouts for partition 0 result in success for both
            // partition 0 and partition 1.
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));
            for (int i = 0; i < numRetries; i++) {
                env.kafkaClient().prepareResponseFrom(
                    request -> request instanceof ListOffsetsRequest,
                    new ListOffsetsResponse(responseDataWithError), node);
            }
            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof ListOffsetsRequest, new ListOffsetsResponse(responseData), node);
            result = env.adminClient().listOffsets(
                new HashMap<>() {
                    {
                        put(tp0, OffsetSpec.latest());
                        put(tp1, OffsetSpec.latest());
                    }
                });
            ListOffsetsResultInfo tp0Result = result.partitionResult(tp0).get();
            assertEquals(789L, tp0Result.offset());
            assertEquals(987, tp0Result.leaderEpoch().get().intValue());
            assertEquals(-1L, tp0Result.timestamp());
            tp1Result = result.partitionResult(tp1).get();
            assertEquals(345L, tp1Result.offset());
            assertEquals(543, tp1Result.leaderEpoch().get().intValue());
            assertEquals(-1L, tp1Result.timestamp());
        }
    }

    @Test
    public void testListOffsetsUnsupportedNonMaxTimestamp() {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node}));
        final Cluster cluster = new Cluster(
            "mockClusterId",
            nodes,
            pInfos,
            Collections.emptySet(),
            Collections.emptySet(),
            node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster,
            AdminClientConfig.RETRIES_CONFIG, "2")) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(
                    ApiKeys.LIST_OFFSETS.id, (short) 0, (short) 0));
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            // listoffsets response from broker 0
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof ListOffsetsRequest);

            ListOffsetsResult result = env.adminClient().listOffsets(
                Collections.singletonMap(tp0, OffsetSpec.latest()));

            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.partitionResult(tp0));
        }
    }

    @Test
    public void testListOffsetsNonMaxTimestampDowngradedImmediately() throws Exception {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node}));
        final Cluster cluster = new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.emptySet(),
                Collections.emptySet(),
                node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster,
                AdminClientConfig.RETRIES_CONFIG, "2")) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(
                    ApiKeys.LIST_OFFSETS.id, (short) 0, (short) 6));

            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -1L, 123L, 321);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(singletonList(t0));

            // listoffsets response from broker 0
            env.kafkaClient().prepareResponse(
                    request -> request instanceof ListOffsetsRequest,
                    new ListOffsetsResponse(responseData));

            ListOffsetsResult result = env.adminClient().listOffsets(
                    Collections.singletonMap(tp0, OffsetSpec.latest()));

            ListOffsetsResultInfo tp0Offset = result.partitionResult(tp0).get();
            assertEquals(123L, tp0Offset.offset());
            assertEquals(321, tp0Offset.leaderEpoch().get().intValue());
            assertEquals(-1L, tp0Offset.timestamp());
        }
    }

    @Test
    public void testListOffsetsEarliestLocalSpecMinVersion() throws Exception {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node}));
        final Cluster cluster = new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.emptySet(),
                Collections.emptySet(),
                node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster,
                AdminClientConfig.RETRIES_CONFIG, "2")) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.adminClient().listOffsets(Collections.singletonMap(tp0, OffsetSpec.earliestLocal()));

            TestUtils.waitForCondition(() -> env.kafkaClient().requests().stream().anyMatch(request ->
                request.requestBuilder().apiKey().messageType == ApiMessageType.LIST_OFFSETS && request.requestBuilder().oldestAllowedVersion() == 8
            ), "no listOffsets request has the expected oldestAllowedVersion");
        }
    }

    @Test
    public void testListOffsetsLatestTierSpecSpecMinVersion() throws Exception {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node}));
        final Cluster cluster = new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.emptySet(),
                Collections.emptySet(),
                node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster,
                AdminClientConfig.RETRIES_CONFIG, "2")) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.adminClient().listOffsets(Collections.singletonMap(tp0, OffsetSpec.latestTiered()));

            TestUtils.waitForCondition(() -> env.kafkaClient().requests().stream().anyMatch(request ->
                    request.requestBuilder().apiKey().messageType == ApiMessageType.LIST_OFFSETS && request.requestBuilder().oldestAllowedVersion() == 9
            ), "no listOffsets request has the expected oldestAllowedVersion");
        }
    }

    @Test
    public void testListOffsetsEarliestPendingUploadSpecSpecMinVersion() throws Exception {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node}));
        final Cluster cluster = new Cluster(
            "mockClusterId",
            nodes,
            pInfos,
            Collections.emptySet(),
            Collections.emptySet(),
            node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster,
            AdminClientConfig.RETRIES_CONFIG, "2")) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.adminClient().listOffsets(Collections.singletonMap(tp0, OffsetSpec.earliestPendingUpload()));

            TestUtils.waitForCondition(() -> env.kafkaClient().requests().stream().anyMatch(request ->
                request.requestBuilder().apiKey().messageType == ApiMessageType.LIST_OFFSETS && request.requestBuilder().oldestAllowedVersion() == 11
            ), "no listOffsets request has the expected oldestAllowedVersion");
        }
    }

    @Test
    public void testListOffsetsMetadataRetriableErrors() throws Exception {
        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        List<Node> nodes = asList(node0, node1);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node0, new Node[]{node0}, new Node[]{node0}));
        pInfos.add(new PartitionInfo("foo", 1, node1, new Node[]{node1}, new Node[]{node1}));
        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.emptySet(),
                Collections.emptySet(),
                node0);

        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("foo", 1);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.LEADER_NOT_AVAILABLE));
            // We retry when a partition of a topic (but not the topic itself) is unknown
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE, Errors.UNKNOWN_TOPIC_OR_PARTITION));
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            // listoffsets response from broker 0
            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -1L, 345L, 543);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(singletonList(t0));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node0);
            // listoffsets response from broker 1
            ListOffsetsTopicResponse t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.NONE, -1L, 789L, 987);
            responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(singletonList(t1));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node1);

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            partitions.put(tp1, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);

            Map<TopicPartition, ListOffsetsResultInfo> offsets = result.all().get();
            assertFalse(offsets.isEmpty());
            assertEquals(345L, offsets.get(tp0).offset());
            assertEquals(543, offsets.get(tp0).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp0).timestamp());
            assertEquals(789L, offsets.get(tp1).offset());
            assertEquals(987, offsets.get(tp1).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp1).timestamp());
        }
    }

    @Test
    public void testListOffsetsWithMultiplePartitionsLeaderChange() throws Exception {
        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        Node node2 = new Node(2, "localhost", 8122);
        List<Node> nodes = asList(node0, node1, node2);

        final PartitionInfo oldPInfo1 = new PartitionInfo("foo", 0, node0,
            new Node[]{node0, node1, node2}, new Node[]{node0, node1, node2});
        final PartitionInfo oldPnfo2 = new PartitionInfo("foo", 1, node0,
            new Node[]{node0, node1, node2}, new Node[]{node0, node1, node2});
        List<PartitionInfo> oldPInfos = asList(oldPInfo1, oldPnfo2);

        final Cluster oldCluster = new Cluster("mockClusterId", nodes, oldPInfos,
            Collections.emptySet(), Collections.emptySet(), node0);
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("foo", 1);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(oldCluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(oldCluster, Errors.NONE));

            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NOT_LEADER_OR_FOLLOWER, -1L, 345L, 543);
            ListOffsetsTopicResponse t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.LEADER_NOT_AVAILABLE, -2L, 123L, 456);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(asList(t0, t1));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node0);

            final PartitionInfo newPInfo1 = new PartitionInfo("foo", 0, node1,
                new Node[]{node0, node1, node2}, new Node[]{node0, node1, node2});
            final PartitionInfo newPInfo2 = new PartitionInfo("foo", 1, node2,
                new Node[]{node0, node1, node2}, new Node[]{node0, node1, node2});
            List<PartitionInfo> newPInfos = asList(newPInfo1, newPInfo2);

            final Cluster newCluster = new Cluster("mockClusterId", nodes, newPInfos,
                Collections.emptySet(), Collections.emptySet(), node0);

            env.kafkaClient().prepareResponse(prepareMetadataResponse(newCluster, Errors.NONE));

            t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -1L, 345L, 543);
            responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(singletonList(t0));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node1);

            t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.NONE, -2L, 123L, 456);
            responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(singletonList(t1));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node2);

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            partitions.put(tp1, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);
            Map<TopicPartition, ListOffsetsResultInfo> offsets = result.all().get();

            assertFalse(offsets.isEmpty());
            assertEquals(345L, offsets.get(tp0).offset());
            assertEquals(543, offsets.get(tp0).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp0).timestamp());
            assertEquals(123L, offsets.get(tp1).offset());
            assertEquals(456, offsets.get(tp1).leaderEpoch().get().intValue());
            assertEquals(-2L, offsets.get(tp1).timestamp());
        }
    }

    @Test
    public void testListOffsetsWithLeaderChange() throws Exception {
        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        Node node2 = new Node(2, "localhost", 8122);
        List<Node> nodes = asList(node0, node1, node2);

        final PartitionInfo oldPartitionInfo = new PartitionInfo("foo", 0, node0,
            new Node[]{node0, node1, node2}, new Node[]{node0, node1, node2});
        final Cluster oldCluster = new Cluster("mockClusterId", nodes, singletonList(oldPartitionInfo),
            Collections.emptySet(), Collections.emptySet(), node0);
        final TopicPartition tp0 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(oldCluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(oldCluster, Errors.NONE));

            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NOT_LEADER_OR_FOLLOWER, -1L, 345L, 543);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(singletonList(t0));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node0);

            // updating leader from node0 to node1 and metadata refresh because of NOT_LEADER_OR_FOLLOWER
            final PartitionInfo newPartitionInfo = new PartitionInfo("foo", 0, node1,
                new Node[]{node0, node1, node2}, new Node[]{node0, node1, node2});
            final Cluster newCluster = new Cluster("mockClusterId", nodes, singletonList(newPartitionInfo),
                Collections.emptySet(), Collections.emptySet(), node0);

            env.kafkaClient().prepareResponse(prepareMetadataResponse(newCluster, Errors.NONE));

            t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -2L, 123L, 456);
            responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(singletonList(t0));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node1);

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);
            Map<TopicPartition, ListOffsetsResultInfo> offsets = result.all().get();

            assertFalse(offsets.isEmpty());
            assertEquals(123L, offsets.get(tp0).offset());
            assertEquals(456, offsets.get(tp0).leaderEpoch().get().intValue());
            assertEquals(-2L, offsets.get(tp0).timestamp());
        }
    }

    @ParameterizedTest
    @MethodSource("listOffsetsMetadataNonRetriableErrors")
    public void testListOffsetsMetadataNonRetriableErrors(
            Errors topicMetadataError,
            Errors partitionMetadataError,
            Class<? extends Throwable> expectedFailure
    ) throws Exception {
        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        List<Node> nodes = asList(node0, node1);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node0, new Node[]{node0, node1}, new Node[]{node0, node1}));
        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.emptySet(),
                Collections.emptySet(),
                node0);

        final TopicPartition tp1 = new TopicPartition("foo", 0);
        final MetadataResponse preparedResponse = prepareMetadataResponse(
                cluster, topicMetadataError, partitionMetadataError
        );

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(preparedResponse);

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp1, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);

            TestUtils.assertFutureThrows(expectedFailure, result.all());
        }
    }

    @Test
    public void testListOffsetsPartialResponse() throws Exception {
        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        List<Node> nodes = asList(node0, node1);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node0, new Node[]{node0, node1}, new Node[]{node0, node1}));
        pInfos.add(new PartitionInfo("foo", 1, node0, new Node[]{node0, node1}, new Node[]{node0, node1}));
        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.emptySet(),
                Collections.emptySet(),
                node0);

        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("foo", 1);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -2L, 123L, 456);
            ListOffsetsResponseData data = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(singletonList(t0));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(data), node0);

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            partitions.put(tp1, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);
            assertNotNull(result.partitionResult(tp0).get());
            TestUtils.assertFutureThrows(ApiException.class, result.partitionResult(tp1));
            TestUtils.assertFutureThrows(ApiException.class, result.all());
        }
    }

    @Test
    public void testAlterReplicaLogDirsSuccess() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            createAlterLogDirsResponse(env, env.cluster().nodeById(0), Errors.NONE, 0);
            createAlterLogDirsResponse(env, env.cluster().nodeById(1), Errors.NONE, 0);

            TopicPartitionReplica tpr0 = new TopicPartitionReplica("topic", 0, 0);
            TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic", 0, 1);

            Map<TopicPartitionReplica, String> logDirs = new HashMap<>();
            logDirs.put(tpr0, "/data0");
            logDirs.put(tpr1, "/data1");
            AlterReplicaLogDirsResult result = env.adminClient().alterReplicaLogDirs(logDirs);
            assertNull(result.values().get(tpr0).get());
            assertNull(result.values().get(tpr1).get());
        }
    }

    @Test
    public void testAlterReplicaLogDirsLogDirNotFound() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            createAlterLogDirsResponse(env, env.cluster().nodeById(0), Errors.NONE, 0);
            createAlterLogDirsResponse(env, env.cluster().nodeById(1), Errors.LOG_DIR_NOT_FOUND, 0);

            TopicPartitionReplica tpr0 = new TopicPartitionReplica("topic", 0, 0);
            TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic", 0, 1);

            Map<TopicPartitionReplica, String> logDirs = new HashMap<>();
            logDirs.put(tpr0, "/data0");
            logDirs.put(tpr1, "/data1");
            AlterReplicaLogDirsResult result = env.adminClient().alterReplicaLogDirs(logDirs);
            assertNull(result.values().get(tpr0).get());
            TestUtils.assertFutureThrows(LogDirNotFoundException.class, result.values().get(tpr1));
        }
    }

    @Test
    public void testAlterReplicaLogDirsUnrequested() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            createAlterLogDirsResponse(env, env.cluster().nodeById(0), Errors.NONE, 1, 2);

            TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic", 1, 0);

            Map<TopicPartitionReplica, String> logDirs = new HashMap<>();
            logDirs.put(tpr1, "/data1");
            AlterReplicaLogDirsResult result = env.adminClient().alterReplicaLogDirs(logDirs);
            assertNull(result.values().get(tpr1).get());
        }
    }

    @Test
    public void testAlterReplicaLogDirsPartialResponse() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            createAlterLogDirsResponse(env, env.cluster().nodeById(0), Errors.NONE, 1);

            TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic", 1, 0);
            TopicPartitionReplica tpr2 = new TopicPartitionReplica("topic", 2, 0);

            Map<TopicPartitionReplica, String> logDirs = new HashMap<>();
            logDirs.put(tpr1, "/data1");
            logDirs.put(tpr2, "/data1");
            AlterReplicaLogDirsResult result = env.adminClient().alterReplicaLogDirs(logDirs);
            assertNull(result.values().get(tpr1).get());
            TestUtils.assertFutureThrows(ApiException.class, result.values().get(tpr2));
        }
    }

    @Test
    public void testAlterReplicaLogDirsPartialFailure() throws Exception {
        long defaultApiTimeout = 60000;
        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = mockClientEnv(time, AdminClientConfig.RETRIES_CONFIG, "0")) {

            // Provide only one prepared response from node 1
            env.kafkaClient().prepareResponseFrom(
                prepareAlterLogDirsResponse(Errors.NONE, "topic", 2),
                env.cluster().nodeById(1));

            TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic", 1, 0);
            TopicPartitionReplica tpr2 = new TopicPartitionReplica("topic", 2, 1);

            Map<TopicPartitionReplica, String> logDirs = new HashMap<>();
            logDirs.put(tpr1, "/data1");
            logDirs.put(tpr2, "/data1");

            AlterReplicaLogDirsResult result = env.adminClient().alterReplicaLogDirs(logDirs);

            // Wait until the prepared attempt has been consumed
            TestUtils.waitForCondition(() -> env.kafkaClient().numAwaitingResponses() == 0,
                "Failed awaiting requests");

            // Wait until the request is sent out
            TestUtils.waitForCondition(() -> env.kafkaClient().inFlightRequestCount() == 1,
                "Failed awaiting request");

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout + 1);

            TestUtils.assertFutureThrows(TimeoutException.class, result.values().get(tpr1));
            assertNull(result.values().get(tpr2).get());
        }
    }

    @Test
    public void testDescribeLogDirsPartialFailure() throws Exception {
        long defaultApiTimeout = 60000;
        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = mockClientEnv(time, AdminClientConfig.RETRIES_CONFIG, "0")) {

            env.kafkaClient().prepareResponseFrom(
                prepareDescribeLogDirsResponse(Errors.NONE, "/data"),
                env.cluster().nodeById(1));

            DescribeLogDirsResult result = env.adminClient().describeLogDirs(asList(0, 1));

            // Wait until the prepared attempt has been consumed
            TestUtils.waitForCondition(() -> env.kafkaClient().numAwaitingResponses() == 0,
                "Failed awaiting requests");

            // Wait until the request is sent out
            TestUtils.waitForCondition(() -> env.kafkaClient().inFlightRequestCount() == 1,
                "Failed awaiting request");

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout + 1);

            TestUtils.assertFutureThrows(TimeoutException.class, result.descriptions().get(0));
            assertNotNull(result.descriptions().get(1).get());
        }
    }

    @Test
    public void testDescribeReplicaLogDirsWithNonExistReplica() throws Exception {
        int brokerId = 0;
        TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic1", 12, brokerId);
        TopicPartitionReplica tpr2 = new TopicPartitionReplica("topic2", 12, brokerId);
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            String logDir = "/var/data/kafka0";
            int offsetLag = 1;
            int defaultOffsetLag = -1;
            env.kafkaClient().prepareResponseFrom(
                    new DescribeLogDirsResponse(
                            new DescribeLogDirsResponseData().setResults(singletonList(
                                    prepareDescribeLogDirsResult(tpr1, logDir, 123456, offsetLag, false)))),
                    env.cluster().nodeById(brokerId));

            DescribeReplicaLogDirsResult result = env.adminClient().describeReplicaLogDirs(asList(tpr1, tpr2));
            Map<TopicPartitionReplica, KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> values = result.values();

            assertEquals(logDir, values.get(tpr1).get().getCurrentReplicaLogDir());
            assertNull(values.get(tpr1).get().getFutureReplicaLogDir());
            assertEquals(offsetLag, values.get(tpr1).get().getCurrentReplicaOffsetLag());
            assertEquals(defaultOffsetLag, values.get(tpr1).get().getFutureReplicaOffsetLag());
            assertNull(values.get(tpr2).get().getCurrentReplicaLogDir());
            assertNull(values.get(tpr2).get().getFutureReplicaLogDir());
            assertEquals(defaultOffsetLag, values.get(tpr2).get().getCurrentReplicaOffsetLag());
            assertEquals(defaultOffsetLag, values.get(tpr2).get().getFutureReplicaOffsetLag());
        }
    }

    @Test
    @Timeout(30)
    public void testDescribeTopicsTimeoutWhenNoBrokerResponds() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(
            mockCluster(1, 0),
            AdminClientConfig.RETRIES_CONFIG, "0",
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Not using prepareResponse is equivalent to "no brokers respond".
            long start = System.currentTimeMillis();
            DescribeTopicsResult result = env.adminClient().describeTopics(List.of("test-topic"), new DescribeTopicsOptions().timeoutMs(200));
            Map<String, KafkaFuture<TopicDescription>> topicDescriptionMap = result.topicNameValues();
            KafkaFuture<TopicDescription> topicDescription = topicDescriptionMap.get("test-topic");
            ExecutionException exception = assertThrows(ExecutionException.class, topicDescription::get);
            // Duration should be greater than or equal to 200 ms but less than 30000 ms.
            long duration = System.currentTimeMillis() - start;

            assertInstanceOf(TimeoutException.class, exception.getCause());
            assertTrue(duration >= 150L && duration < 30000);
        }
    }

    private MockClient.RequestMatcher expectCreateTopicsRequestWithTopics(final String... topics) {
        return body -> {
            if (body instanceof CreateTopicsRequest) {
                CreateTopicsRequest request = (CreateTopicsRequest) body;
                for (String topic : topics) {
                    if (request.data().topics().find(topic) == null)
                        return false;
                }
                return topics.length == request.data().topics().size();
            }
            return false;
        };
    }

    private MockClient.RequestMatcher expectDeleteTopicsRequestWithTopics(final String... topics) {
        return body -> {
            if (body instanceof DeleteTopicsRequest) {
                DeleteTopicsRequest request = (DeleteTopicsRequest) body;
                return request.topicNames().equals(asList(topics));
            }
            return false;
        };
    }

    private MockClient.RequestMatcher expectDeleteTopicsRequestWithTopicIds(final Uuid... topicIds) {
        return body -> {
            if (body instanceof DeleteTopicsRequest) {
                DeleteTopicsRequest request = (DeleteTopicsRequest) body;
                return request.topicIds().equals(asList(topicIds));
            }
            return false;
        };
    }

    private void addPartitionToDescribeTopicPartitionsResponse(
        DescribeTopicPartitionsResponseData data,
        String topicName,
        Uuid topicId,
        List<Integer> partitions) {
        List<DescribeTopicPartitionsResponsePartition> addingPartitions = new ArrayList<>();
        partitions.forEach(partition ->
            addingPartitions.add(new DescribeTopicPartitionsResponsePartition()
                .setIsrNodes(singletonList(0))
                .setErrorCode((short) 0)
                .setLeaderEpoch(0)
                .setLeaderId(0)
                .setEligibleLeaderReplicas(singletonList(1))
                .setLastKnownElr(singletonList(2))
                .setPartitionIndex(partition)
                .setReplicaNodes(asList(0, 1, 2)))
        );
        data.topics().add(new DescribeTopicPartitionsResponseTopic()
                .setErrorCode((short) 0)
                .setTopicId(topicId)
                .setName(topicName)
                .setIsInternal(false)
                .setPartitions(addingPartitions));
    }

    private static DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir, TopicPartition tp, long partitionSize, long offsetLag) {
        return prepareDescribeLogDirsResponse(error, logDir,
                prepareDescribeLogDirsTopics(partitionSize, offsetLag, tp.topic(), tp.partition(), false));
    }

    private static DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir, TopicPartition tp, long partitionSize, long offsetLag, long totalBytes, long usableBytes) {
        return prepareDescribeLogDirsResponse(error, logDir,
                prepareDescribeLogDirsTopics(partitionSize, offsetLag, tp.topic(), tp.partition(), false), totalBytes, usableBytes, false);
    }

    private static DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir, TopicPartition tp, long partitionSize, long offsetLag, long totalBytes, long usableBytes, boolean isCordoned) {
        return prepareDescribeLogDirsResponse(error, logDir,
                prepareDescribeLogDirsTopics(partitionSize, offsetLag, tp.topic(), tp.partition(), false), totalBytes, usableBytes, isCordoned);
    }

    private static DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir,
                                                                   List<DescribeLogDirsTopic> topics) {
        return new DescribeLogDirsResponse(
                new DescribeLogDirsResponseData().setResults(singletonList(new DescribeLogDirsResponseData.DescribeLogDirsResult()
                        .setErrorCode(error.code())
                        .setLogDir(logDir)
                        .setTopics(topics)
                )));
    }

    private static DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir,
                                                                          List<DescribeLogDirsTopic> topics,
                                                                          long totalBytes, long usableBytes,
                                                                          boolean isCordoned) {
        return new DescribeLogDirsResponse(
                new DescribeLogDirsResponseData().setResults(singletonList(new DescribeLogDirsResponseData.DescribeLogDirsResult()
                        .setErrorCode(error.code())
                        .setLogDir(logDir)
                        .setTopics(topics)
                        .setTotalBytes(totalBytes)
                        .setUsableBytes(usableBytes)
                        .setIsCordoned(isCordoned)
                )));
    }

    private DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir) {
        return new DescribeLogDirsResponse(new DescribeLogDirsResponseData()
            .setResults(Collections.singletonList(
                new DescribeLogDirsResponseData.DescribeLogDirsResult()
                    .setErrorCode(error.code())
                    .setLogDir(logDir))));
    }

    private static List<DescribeLogDirsTopic> prepareDescribeLogDirsTopics(
            long partitionSize, long offsetLag, String topic, int partition, boolean isFuture) {
        return singletonList(new DescribeLogDirsTopic()
                .setName(topic)
                .setPartitions(singletonList(new DescribeLogDirsResponseData.DescribeLogDirsPartition()
                        .setPartitionIndex(partition)
                        .setPartitionSize(partitionSize)
                        .setIsFutureKey(isFuture)
                        .setOffsetLag(offsetLag))));
    }

    private static DescribeLogDirsResponse prepareEmptyDescribeLogDirsResponse(Optional<Errors> error) {
        DescribeLogDirsResponseData data = new DescribeLogDirsResponseData();
        error.ifPresent(e -> data.setErrorCode(e.code()));
        return new DescribeLogDirsResponse(data);
    }

    private static void assertDescriptionContains(Map<String, LogDirDescription> descriptionsMap, String logDir,
                                           TopicPartition tp, long partitionSize, long offsetLag) {
        assertDescriptionContains(descriptionsMap, logDir, tp, partitionSize, offsetLag, OptionalLong.empty(), OptionalLong.empty());
    }

    private static void assertDescriptionContains(Map<String, LogDirDescription> descriptionsMap, String logDir,
                                                  TopicPartition tp, long partitionSize, long offsetLag, OptionalLong totalBytes, OptionalLong usableBytes) {
        assertNotNull(descriptionsMap);
        assertEquals(singleton(logDir), descriptionsMap.keySet());
        assertNull(descriptionsMap.get(logDir).error());
        Map<TopicPartition, ReplicaInfo> descriptionsReplicaInfos = descriptionsMap.get(logDir).replicaInfos();
        assertEquals(singleton(tp), descriptionsReplicaInfos.keySet());
        assertEquals(partitionSize, descriptionsReplicaInfos.get(tp).size());
        assertEquals(offsetLag, descriptionsReplicaInfos.get(tp).offsetLag());
        assertFalse(descriptionsReplicaInfos.get(tp).isFuture());
        assertEquals(totalBytes, descriptionsMap.get(logDir).totalBytes());
        assertEquals(usableBytes, descriptionsMap.get(logDir).usableBytes());
        assertFalse(descriptionsMap.get(logDir).isCordoned());
    }

    private static DescribeLogDirsResponseData.DescribeLogDirsResult prepareDescribeLogDirsResult(TopicPartitionReplica tpr, String logDir, int partitionSize, int offsetLag, boolean isFuture) {
        return new DescribeLogDirsResponseData.DescribeLogDirsResult()
                .setErrorCode(Errors.NONE.code())
                .setLogDir(logDir)
                .setTopics(prepareDescribeLogDirsTopics(partitionSize, offsetLag, tpr.topic(), tpr.partition(), isFuture));
    }

    private MockClient.RequestMatcher expectCreatePartitionsRequestWithTopics(final String... topics) {
        return body -> {
            if (body instanceof CreatePartitionsRequest) {
                CreatePartitionsRequest request = (CreatePartitionsRequest) body;
                for (String topic : topics) {
                    if (request.data().topics().find(topic) == null)
                        return false;
                }
                return topics.length == request.data().topics().size();
            }
            return false;
        };
    }

    private static Stream<Arguments> listOffsetsMetadataNonRetriableErrors() {
        return Stream.of(
                Arguments.of(
                        Errors.TOPIC_AUTHORIZATION_FAILED,
                        Errors.TOPIC_AUTHORIZATION_FAILED,
                        TopicAuthorizationException.class
                ),
                Arguments.of(
                        // We fail fast when the entire topic is unknown...
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        Errors.NONE,
                        UnknownTopicOrPartitionException.class
                ),
                Arguments.of(
                        // ... even if a partition in the topic is also somehow reported as unknown...
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        UnknownTopicOrPartitionException.class
                ),
                Arguments.of(
                        // ... or a partition in the topic has a different, otherwise-retriable error
                        Errors.UNKNOWN_TOPIC_OR_PARTITION,
                        Errors.LEADER_NOT_AVAILABLE,
                        UnknownTopicOrPartitionException.class
                )
        );
    }

    private void createAlterLogDirsResponse(AdminClientUnitTestEnv env, Node node, Errors error, int... partitions) {
        env.kafkaClient().prepareResponseFrom(
            prepareAlterLogDirsResponse(error, "topic", partitions), node);
    }

    private AlterReplicaLogDirsResponse prepareAlterLogDirsResponse(Errors error, String topic, int... partitions) {
        return new AlterReplicaLogDirsResponse(
            new AlterReplicaLogDirsResponseData().setResults(singletonList(
                new AlterReplicaLogDirTopicResult()
                    .setTopicName(topic)
                    .setPartitions(Arrays.stream(partitions).boxed().map(partitionId ->
                        new AlterReplicaLogDirPartitionResult()
                            .setPartitionIndex(partitionId)
                            .setErrorCode(error.code())).collect(Collectors.toList())))));
    }

    public static DeleteTopicsResponse prepareDeleteTopicsResponse(int throttleTimeMs, DeletableTopicResult... topics) {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setResponses(new DeletableTopicResultCollection(Arrays.asList(topics)));
        return new DeleteTopicsResponse(data);
    }

    private static DeleteTopicsResponse prepareDeleteTopicsResponse(String topicName, Errors error) {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData();
        data.responses().add(new DeletableTopicResult()
            .setName(topicName)
            .setErrorCode(error.code()));
        return new DeleteTopicsResponse(data);
    }

    private static DeleteTopicsResponse prepareDeleteTopicsResponseWithTopicId(Uuid id, Errors error) {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData();
        data.responses().add(new DeletableTopicResult()
                .setTopicId(id)
                .setErrorCode(error.code()));
        return new DeleteTopicsResponse(data);
    }

    private static CreateTopicsResponse prepareCreateTopicsResponse(int throttleTimeMs, CreatableTopicResult... topics) {
        CreateTopicsResponseData data = new CreateTopicsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setTopics(new CreatableTopicResultCollection(Arrays.asList(topics)));
        return new CreateTopicsResponse(data);
    }

    private static CreatableTopicResult creatableTopicResult(String name, Errors error) {
        return new CreatableTopicResult()
            .setName(name)
            .setErrorCode(error.code());
    }

    private static DeletableTopicResult deletableTopicResult(String topicName, Errors error) {
        return new DeletableTopicResult()
            .setName(topicName)
            .setErrorCode(error.code());
    }

    private static DeletableTopicResult deletableTopicResultWithId(Uuid topicId, Errors error) {
        return new DeletableTopicResult()
                .setTopicId(topicId)
                .setErrorCode(error.code());
    }

    private static CreatePartitionsResponse prepareCreatePartitionsResponse(int throttleTimeMs, CreatePartitionsTopicResult... topics) {
        CreatePartitionsResponseData data = new CreatePartitionsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setResults(asList(topics));
        return new CreatePartitionsResponse(data);
    }

    private static CreatePartitionsTopicResult createPartitionsTopicResult(String name, Errors error) {
        return createPartitionsTopicResult(name, error, null);
    }

    private static CreatePartitionsTopicResult createPartitionsTopicResult(String name, Errors error, String errorMessage) {
        return new CreatePartitionsTopicResult()
            .setName(name)
            .setErrorCode(error.code())
            .setErrorMessage(errorMessage);
    }
}
