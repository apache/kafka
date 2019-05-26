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

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.ElectPreferredLeadersResponseData;
import org.apache.kafka.common.message.ElectPreferredLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectPreferredLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.CreateAclsResponse.AclCreationResponse;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DeleteAclsResponse.AclDeletionResult;
import org.apache.kafka.common.requests.DeleteAclsResponse.AclFilterResponse;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.ElectPreferredLeadersResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A unit test for KafkaAdminClient.
 *
 * See AdminClientIntegrationTest for an integration test.
 */
public class KafkaAdminClientTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaAdminClientTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testGetOrCreateListValue() {
        Map<String, List<String>> map = new HashMap<>();
        List<String> fooList = KafkaAdminClient.getOrCreateListValue(map, "foo");
        assertNotNull(fooList);
        fooList.add("a");
        fooList.add("b");
        List<String> fooList2 = KafkaAdminClient.getOrCreateListValue(map, "foo");
        assertEquals(fooList, fooList2);
        assertTrue(fooList2.contains("a"));
        assertTrue(fooList2.contains("b"));
        List<String> barList = KafkaAdminClient.getOrCreateListValue(map, "bar");
        assertNotNull(barList);
        assertTrue(barList.isEmpty());
    }

    @Test
    public void testCalcTimeoutMsRemainingAsInt() {
        assertEquals(0, KafkaAdminClient.calcTimeoutMsRemainingAsInt(1000, 1000));
        assertEquals(100, KafkaAdminClient.calcTimeoutMsRemainingAsInt(1000, 1100));
        assertEquals(Integer.MAX_VALUE, KafkaAdminClient.calcTimeoutMsRemainingAsInt(0, Long.MAX_VALUE));
        assertEquals(Integer.MIN_VALUE, KafkaAdminClient.calcTimeoutMsRemainingAsInt(Long.MAX_VALUE, 0));
    }

    @Test
    public void testPrettyPrintException() {
        assertEquals("Null exception.", KafkaAdminClient.prettyPrintException(null));
        assertEquals("TimeoutException", KafkaAdminClient.prettyPrintException(new TimeoutException()));
        assertEquals("TimeoutException: The foobar timed out.",
                KafkaAdminClient.prettyPrintException(new TimeoutException("The foobar timed out.")));
    }

    private static Map<String, Object> newStrMap(String... vals) {
        Map<String, Object> map = new HashMap<>();
        map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8121");
        map.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        if (vals.length % 2 != 0) {
            throw new IllegalStateException();
        }
        for (int i = 0; i < vals.length; i += 2) {
            map.put(vals[i], vals[i + 1]);
        }
        return map;
    }

    private static AdminClientConfig newConfMap(String... vals) {
        return new AdminClientConfig(newStrMap(vals));
    }

    @Test
    public void testGenerateClientId() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String id = KafkaAdminClient.generateClientId(newConfMap(AdminClientConfig.CLIENT_ID_CONFIG, ""));
            assertTrue("Got duplicate id " + id, !ids.contains(id));
            ids.add(id);
        }
        assertEquals("myCustomId",
                KafkaAdminClient.generateClientId(newConfMap(AdminClientConfig.CLIENT_ID_CONFIG, "myCustomId")));
    }

    private static Cluster mockCluster(int controllerIndex) {
        HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));
        nodes.put(1, new Node(1, "localhost", 8122));
        nodes.put(2, new Node(2, "localhost", 8123));
        return new Cluster("mockClusterId", nodes.values(),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(controllerIndex));
    }

    private static Cluster mockBootstrapCluster() {
        return Cluster.bootstrap(ClientUtils.parseAndValidateAddresses(
                Collections.singletonList("localhost:8121"), ClientDnsLookup.DEFAULT));
    }

    private static AdminClientUnitTestEnv mockClientEnv(String... configVals) {
        return new AdminClientUnitTestEnv(mockCluster(0), configVals);
    }

    @Test
    public void testCloseAdminClient() {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
        }
    }

    private static CreateTopicsResponse prepareCreateTopicsResponse(String topicName, Errors error) {
        CreateTopicsResponseData data = new CreateTopicsResponseData();
        data.topics().add(new CreatableTopicResult().
            setName(topicName).setErrorCode(error.code()));
        return new CreateTopicsResponse(data);
    }

    private static DeleteTopicsResponse prepareDeleteTopicsResponse(String topicName, Errors error) {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData();
        data.responses().add(new DeletableTopicResult()
                .setName(topicName)
                .setErrorCode(error.code()));
        return new DeleteTopicsResponse(data);
    }

    private static FindCoordinatorResponse prepareFindCoordinatorResponse(Errors error, Node node) {
        return FindCoordinatorResponse.prepareResponse(error, node);
    }

    /**
     * Test that the client properly times out when we don't receive any metadata.
     */
    @Test
    public void testTimeoutWithoutMetadata() throws Exception {
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, mockBootstrapCluster(),
                newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareCreateTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(1000)).all();
            TestUtils.assertFutureError(future, TimeoutException.class);
        }
    }

    @Test
    public void testConnectionFailureOnMetadataUpdate() throws Exception {
        // This tests the scenario in which we successfully connect to the bootstrap server, but
        // the server disconnects before sending the full response

        Cluster cluster = mockBootstrapCluster();
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster)) {
            Cluster discoveredCluster = mockCluster(0);
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(request -> request instanceof MetadataRequest, null, true);
            env.kafkaClient().prepareResponse(request -> request instanceof MetadataRequest,
                    MetadataResponse.prepareResponse(discoveredCluster.nodes(), discoveredCluster.clusterResource().clusterId(),
                            1, Collections.emptyList()));
            env.kafkaClient().prepareResponse(body -> body instanceof CreateTopicsRequest,
                    prepareCreateTopicsResponse("myTopic", Errors.NONE));

            KafkaFuture<Void> future = env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();

            future.get();
        }
    }

    @Test
    public void testUnreachableBootstrapServer() throws Exception {
        // This tests the scenario in which the bootstrap server is unreachable for a short while,
        // which prevents AdminClient from being able to send the initial metadata request

        Cluster cluster = Cluster.bootstrap(Collections.singletonList(new InetSocketAddress("localhost", 8121)));
        Map<Node, Long> unreachableNodes = Collections.singletonMap(cluster.nodes().get(0), 200L);
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster,
                AdminClientUnitTestEnv.clientConfigs(), unreachableNodes)) {
            Cluster discoveredCluster = mockCluster(0);
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(body -> body instanceof MetadataRequest,
                    MetadataResponse.prepareResponse(discoveredCluster.nodes(), discoveredCluster.clusterResource().clusterId(),
                            1, Collections.emptyList()));
            env.kafkaClient().prepareResponse(body -> body instanceof CreateTopicsRequest,
                prepareCreateTopicsResponse("myTopic", Errors.NONE));

            KafkaFuture<Void> future = env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();

            future.get();
        }
    }

    /**
     * Test that we propagate exceptions encountered when fetching metadata.
     */
    @Test
    public void testPropagatedMetadataFetchException() throws Exception {
        Cluster cluster = mockCluster(0);
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster,
                newStrMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8121",
                    AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().createPendingAuthenticationError(cluster.nodeById(0),
                    TimeUnit.DAYS.toMillis(1));
            env.kafkaClient().prepareResponse(prepareCreateTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                new CreateTopicsOptions().timeoutMs(1000)).all();
            TestUtils.assertFutureError(future, SaslAuthenticationException.class);
        }
    }

    @Test
    public void testCreateTopics() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(body -> body instanceof CreateTopicsRequest,
                    prepareCreateTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();
            future.get();
        }
    }

    @Test
    public void testCreateTopicsRetryBackoff() throws Exception {
        Cluster cluster = mockCluster(0);
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
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
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();

            // Wait until the first attempt has failed, then advance the time
            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1,
                    "Failed awaiting CreateTopics first request failure");

            // Wait until the retry call added to the queue in AdminClient
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1,
                "Failed to add retry CreateTopics call");

            time.sleep(retryBackoff);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals("CreateTopics retry did not await expected backoff",
                    retryBackoff, actualRetryBackoff);
        }
    }

    @Test
    public void testCreateTopicsHandleNotControllerException() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(
                prepareCreateTopicsResponse("myTopic", Errors.NOT_CONTROLLER),
                env.cluster().nodeById(0));
            env.kafkaClient().prepareResponse(MetadataResponse.prepareResponse(env.cluster().nodes(),
                env.cluster().clusterResource().clusterId(),
                1,
                Collections.<MetadataResponse.TopicMetadata>emptyList()));
            env.kafkaClient().prepareResponseFrom(
                prepareCreateTopicsResponse("myTopic", Errors.NONE),
                env.cluster().nodeById(1));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();
            future.get();
        }
    }

    @Test
    public void testDeleteTopics() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(body -> body instanceof DeleteTopicsRequest,
                    prepareDeleteTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().deleteTopics(Collections.singletonList("myTopic"),
                    new DeleteTopicsOptions()).all();
            future.get();

            env.kafkaClient().prepareResponse(body -> body instanceof DeleteTopicsRequest,
                    prepareDeleteTopicsResponse("myTopic", Errors.TOPIC_DELETION_DISABLED));
            future = env.adminClient().deleteTopics(Collections.singletonList("myTopic"),
                    new DeleteTopicsOptions()).all();
            TestUtils.assertFutureError(future, TopicDeletionDisabledException.class);

            env.kafkaClient().prepareResponse(body -> body instanceof DeleteTopicsRequest,
                    prepareDeleteTopicsResponse("myTopic", Errors.UNKNOWN_TOPIC_OR_PARTITION));
            future = env.adminClient().deleteTopics(Collections.singletonList("myTopic"),
                    new DeleteTopicsOptions()).all();
            TestUtils.assertFutureError(future, UnknownTopicOrPartitionException.class);
        }
    }

    @Test
    public void testInvalidTopicNames() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            List<String> sillyTopicNames = asList("", null);
            Map<String, KafkaFuture<Void>> deleteFutures = env.adminClient().deleteTopics(sillyTopicNames).values();
            for (String sillyTopicName : sillyTopicNames) {
                TestUtils.assertFutureError(deleteFutures.get(sillyTopicName), InvalidTopicException.class);
            }
            assertEquals(0, env.kafkaClient().inFlightRequestCount());

            Map<String, KafkaFuture<TopicDescription>> describeFutures =
                    env.adminClient().describeTopics(sillyTopicNames).values();
            for (String sillyTopicName : sillyTopicNames) {
                TestUtils.assertFutureError(describeFutures.get(sillyTopicName), InvalidTopicException.class);
            }
            assertEquals(0, env.kafkaClient().inFlightRequestCount());

            List<NewTopic> newTopics = new ArrayList<>();
            for (String sillyTopicName : sillyTopicNames) {
                newTopics.add(new NewTopic(sillyTopicName, 1, (short) 1));
            }

            Map<String, KafkaFuture<Void>> createFutures = env.adminClient().createTopics(newTopics).values();
            for (String sillyTopicName : sillyTopicNames) {
                TestUtils.assertFutureError(createFutures .get(sillyTopicName), InvalidTopicException.class);
            }
            assertEquals(0, env.kafkaClient().inFlightRequestCount());
        }
    }

    @Test
    public void testMetadataRetries() throws Exception {
        // We should continue retrying on metadata update failures in spite of retry configuration

        String topic = "topic";
        Cluster bootstrapCluster = Cluster.bootstrap(singletonList(new InetSocketAddress("localhost", 9999)));
        Cluster initializedCluster = mockCluster(0);

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, bootstrapCluster,
                newStrMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999",
                        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000000",
                        AdminClientConfig.RETRIES_CONFIG, "0"))) {

            // The first request fails with a disconnect
            env.kafkaClient().prepareResponse(null, true);

            // The next one succeeds and gives us the controller id
            env.kafkaClient().prepareResponse(MetadataResponse.prepareResponse(initializedCluster.nodes(),
                    initializedCluster.clusterResource().clusterId(),
                    initializedCluster.controller().id(),
                    Collections.emptyList()));

            // Then we respond to the DescribeTopic request
            Node leader = initializedCluster.nodes().get(0);
            MetadataResponse.PartitionMetadata partitionMetadata = new MetadataResponse.PartitionMetadata(
                    Errors.NONE, 0, leader, Optional.of(10), singletonList(leader),
                    singletonList(leader), singletonList(leader));
            env.kafkaClient().prepareResponse(MetadataResponse.prepareResponse(initializedCluster.nodes(),
                    initializedCluster.clusterResource().clusterId(), 1,
                    singletonList(new MetadataResponse.TopicMetadata(Errors.NONE, topic, false,
                            singletonList(partitionMetadata), MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED))));

            DescribeTopicsResult result = env.adminClient().describeTopics(Collections.singleton(topic));
            Map<String, TopicDescription> topicDescriptions = result.all().get();
            assertEquals(leader, topicDescriptions.get(topic).partitions().get(0).leader());
            assertEquals(null, topicDescriptions.get(topic).authorizedOperations());
        }
    }

    @Test
    public void testAdminClientApisAuthenticationFailure() throws Exception {
        Cluster cluster = mockBootstrapCluster();
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster,
                newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().createPendingAuthenticationError(cluster.nodes().get(0),
                    TimeUnit.DAYS.toMillis(1));
            callAdminClientApisAndExpectAnAuthenticationError(env);
        }
    }

    private void callAdminClientApisAndExpectAnAuthenticationError(AdminClientUnitTestEnv env) throws InterruptedException {
        try {
            env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }

        try {
            Map<String, NewPartitions> counts = new HashMap<>();
            counts.put("my_topic", NewPartitions.increaseTo(3));
            counts.put("other_topic", NewPartitions.increaseTo(3, asList(asList(2), asList(3))));
            env.adminClient().createPartitions(counts).all().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }

        try {
            env.adminClient().createAcls(asList(ACL1, ACL2)).all().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }

        try {
            env.adminClient().describeAcls(FILTER1).values().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }

        try {
            env.adminClient().deleteAcls(asList(FILTER1, FILTER2)).all().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }

        try {
            env.adminClient().describeConfigs(Collections.singleton(new ConfigResource(ConfigResource.Type.BROKER, "0"))).all().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }
    }

    private static final AclBinding ACL1 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW));
    private static final AclBinding ACL2 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic4", PatternType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.DENY));
    private static final AclBindingFilter FILTER1 = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, null, PatternType.LITERAL),
        new AccessControlEntryFilter("User:ANONYMOUS", null, AclOperation.ANY, AclPermissionType.ANY));
    private static final AclBindingFilter FILTER2 = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, null, PatternType.LITERAL),
        new AccessControlEntryFilter("User:bob", null, AclOperation.ANY, AclPermissionType.ANY));
    private static final AclBindingFilter UNKNOWN_FILTER = new AclBindingFilter(
        new ResourcePatternFilter(ResourceType.UNKNOWN, null, PatternType.LITERAL),
        new AccessControlEntryFilter("User:bob", null, AclOperation.ANY, AclPermissionType.ANY));

    @Test
    public void testDescribeAcls() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where we get back ACL1 and ACL2.
            env.kafkaClient().prepareResponse(new DescribeAclsResponse(0, ApiError.NONE,
                    asList(ACL1, ACL2)));
            assertCollectionIs(env.adminClient().describeAcls(FILTER1).values().get(), ACL1, ACL2);

            // Test a call where we get back no results.
            env.kafkaClient().prepareResponse(new DescribeAclsResponse(0, ApiError.NONE,
                Collections.<AclBinding>emptySet()));
            assertTrue(env.adminClient().describeAcls(FILTER2).values().get().isEmpty());

            // Test a call where we get back an error.
            env.kafkaClient().prepareResponse(new DescribeAclsResponse(0,
                new ApiError(Errors.SECURITY_DISABLED, "Security is disabled"), Collections.<AclBinding>emptySet()));
            TestUtils.assertFutureError(env.adminClient().describeAcls(FILTER2).values(), SecurityDisabledException.class);

            // Test a call where we supply an invalid filter.
            TestUtils.assertFutureError(env.adminClient().describeAcls(UNKNOWN_FILTER).values(),
                InvalidRequestException.class);
        }
    }

    @Test
    public void testCreateAcls() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where we successfully create two ACLs.
            env.kafkaClient().prepareResponse(new CreateAclsResponse(0,
                asList(new AclCreationResponse(ApiError.NONE), new AclCreationResponse(ApiError.NONE))));
            CreateAclsResult results = env.adminClient().createAcls(asList(ACL1, ACL2));
            assertCollectionIs(results.values().keySet(), ACL1, ACL2);
            for (KafkaFuture<Void> future : results.values().values())
                future.get();
            results.all().get();

            // Test a call where we fail to create one ACL.
            env.kafkaClient().prepareResponse(new CreateAclsResponse(0, asList(
                new AclCreationResponse(new ApiError(Errors.SECURITY_DISABLED, "Security is disabled")),
                new AclCreationResponse(ApiError.NONE))
            ));
            results = env.adminClient().createAcls(asList(ACL1, ACL2));
            assertCollectionIs(results.values().keySet(), ACL1, ACL2);
            TestUtils.assertFutureError(results.values().get(ACL1), SecurityDisabledException.class);
            results.values().get(ACL2).get();
            TestUtils.assertFutureError(results.all(), SecurityDisabledException.class);
        }
    }

    @Test
    public void testDeleteAcls() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where one filter has an error.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(0, asList(
                    new AclFilterResponse(asList(new AclDeletionResult(ACL1), new AclDeletionResult(ACL2))),
                    new AclFilterResponse(new ApiError(Errors.SECURITY_DISABLED, "No security"),
                            Collections.<AclDeletionResult>emptySet()))));
            DeleteAclsResult results = env.adminClient().deleteAcls(asList(FILTER1, FILTER2));
            Map<AclBindingFilter, KafkaFuture<FilterResults>> filterResults = results.values();
            FilterResults filter1Results = filterResults.get(FILTER1).get();
            assertEquals(null, filter1Results.values().get(0).exception());
            assertEquals(ACL1, filter1Results.values().get(0).binding());
            assertEquals(null, filter1Results.values().get(1).exception());
            assertEquals(ACL2, filter1Results.values().get(1).binding());
            TestUtils.assertFutureError(filterResults.get(FILTER2), SecurityDisabledException.class);
            TestUtils.assertFutureError(results.all(), SecurityDisabledException.class);

            // Test a call where one deletion result has an error.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(0, asList(
                    new AclFilterResponse(asList(new AclDeletionResult(ACL1),
                            new AclDeletionResult(new ApiError(Errors.SECURITY_DISABLED, "No security"), ACL2))),
                    new AclFilterResponse(Collections.<AclDeletionResult>emptySet()))));
            results = env.adminClient().deleteAcls(asList(FILTER1, FILTER2));
            assertTrue(results.values().get(FILTER2).get().values().isEmpty());
            TestUtils.assertFutureError(results.all(), SecurityDisabledException.class);

            // Test a call where there are no errors.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(0, asList(
                    new AclFilterResponse(asList(new AclDeletionResult(ACL1))),
                    new AclFilterResponse(asList(new AclDeletionResult(ACL2))))));
            results = env.adminClient().deleteAcls(asList(FILTER1, FILTER2));
            Collection<AclBinding> deleted = results.all().get();
            assertCollectionIs(deleted, ACL1, ACL2);
        }
    }

    @Test
    public void testElectPreferredLeaders()  throws Exception {
        TopicPartition topic1 = new TopicPartition("topic", 0);
        TopicPartition topic2 = new TopicPartition("topic", 2);
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where one partition has an error.
            ApiError value = ApiError.fromThrowable(new ClusterAuthorizationException(null));
            ElectPreferredLeadersResponseData responseData = new ElectPreferredLeadersResponseData();
            ReplicaElectionResult r = new ReplicaElectionResult().setTopic(topic1.topic());
            r.partitionResult().add(new PartitionResult()
                    .setPartitionId(topic1.partition())
                    .setErrorCode(ApiError.NONE.error().code())
                    .setErrorMessage(ApiError.NONE.message()));
            r.partitionResult().add(new PartitionResult()
                    .setPartitionId(topic2.partition())
                    .setErrorCode(value.error().code())
                    .setErrorMessage(value.message()));
            responseData.replicaElectionResults().add(r);
            env.kafkaClient().prepareResponse(new ElectPreferredLeadersResponse(responseData));
            ElectPreferredLeadersResult results = env.adminClient().electPreferredLeaders(asList(topic1, topic2));
            results.partitionResult(topic1).get();
            TestUtils.assertFutureError(results.partitionResult(topic2), ClusterAuthorizationException.class);
            TestUtils.assertFutureError(results.all(), ClusterAuthorizationException.class);

            // Test a call where there are no errors.
            r.partitionResult().clear();
            r.partitionResult().add(new PartitionResult()
                    .setPartitionId(topic1.partition())
                    .setErrorCode(ApiError.NONE.error().code())
                    .setErrorMessage(ApiError.NONE.message()));
            r.partitionResult().add(new PartitionResult()
                    .setPartitionId(topic2.partition())
                    .setErrorCode(ApiError.NONE.error().code())
                    .setErrorMessage(ApiError.NONE.message()));
            env.kafkaClient().prepareResponse(new ElectPreferredLeadersResponse(responseData));

            results = env.adminClient().electPreferredLeaders(asList(topic1, topic2));
            results.partitionResult(topic1).get();
            results.partitionResult(topic2).get();

            // Now try a timeout
            results = env.adminClient().electPreferredLeaders(asList(topic1, topic2), new ElectPreferredLeadersOptions().timeoutMs(100));
            TestUtils.assertFutureError(results.partitionResult(topic1), TimeoutException.class);
            TestUtils.assertFutureError(results.partitionResult(topic2), TimeoutException.class);
        }
    }

    /**
     * Test handling timeouts.
     */
    @Ignore // The test is flaky. Should be renabled when this JIRA is fixed: https://issues.apache.org/jira/browse/KAFKA-5792
    @Test
    public void testHandleTimeout() throws Exception {
        HashMap<Integer, Node> nodes = new HashMap<>();
        MockTime time = new MockTime();
        nodes.put(0, new Node(0, "localhost", 8121));
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
            Collections.<PartitionInfo>emptySet(), Collections.<String>emptySet(),
            Collections.<String>emptySet(), nodes.get(0));
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "1",
                AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, "1")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            assertEquals(time, env.time());
            assertEquals(env.time(), ((KafkaAdminClient) env.adminClient()).time());

            // Make a request with an extremely short timeout.
            // Then wait for it to fail by not supplying any response.
            log.info("Starting AdminClient#listTopics...");
            final ListTopicsResult result = env.adminClient().listTopics(new ListTopicsOptions().timeoutMs(1000));
            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return env.kafkaClient().hasInFlightRequests();
                }
            }, "Timed out waiting for inFlightRequests");
            time.sleep(5000);
            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return result.listings().isDone();
                }
            }, "Timed out waiting for listTopics to complete");
            TestUtils.assertFutureError(result.listings(), TimeoutException.class);
            log.info("Verified the error result of AdminClient#listTopics");

            // The next request should succeed.
            time.sleep(5000);
            env.kafkaClient().prepareResponse(new DescribeConfigsResponse(0,
                Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, "foo"),
                    new DescribeConfigsResponse.Config(ApiError.NONE,
                        Collections.emptySet()))));
            DescribeConfigsResult result2 = env.adminClient().describeConfigs(Collections.singleton(
                new ConfigResource(ConfigResource.Type.TOPIC, "foo")));
            time.sleep(5000);
            result2.values().get(new ConfigResource(ConfigResource.Type.TOPIC, "foo")).get();
        }
    }

    @Test
    public void testDescribeConfigs() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(new DescribeConfigsResponse(0,
                Collections.singletonMap(new ConfigResource(ConfigResource.Type.BROKER, "0"),
                    new DescribeConfigsResponse.Config(ApiError.NONE, Collections.emptySet()))));
            DescribeConfigsResult result2 = env.adminClient().describeConfigs(Collections.singleton(
                new ConfigResource(ConfigResource.Type.BROKER, "0")));
            result2.all().get();
        }
    }

    @Test
    public void testCreatePartitions() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            Map<String, ApiError> m = new HashMap<>();
            m.put("my_topic", ApiError.NONE);
            m.put("other_topic", ApiError.fromThrowable(new InvalidTopicException("some detailed reason")));

            // Test a call where one filter has an error.
            env.kafkaClient().prepareResponse(new CreatePartitionsResponse(0, m));

            Map<String, NewPartitions> counts = new HashMap<>();
            counts.put("my_topic", NewPartitions.increaseTo(3));
            counts.put("other_topic", NewPartitions.increaseTo(3, asList(asList(2), asList(3))));

            CreatePartitionsResult results = env.adminClient().createPartitions(counts);
            Map<String, KafkaFuture<Void>> values = results.values();
            KafkaFuture<Void> myTopicResult = values.get("my_topic");
            myTopicResult.get();
            KafkaFuture<Void> otherTopicResult = values.get("other_topic");
            try {
                otherTopicResult.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e0) {
                assertTrue(e0.getCause() instanceof InvalidTopicException);
                InvalidTopicException e = (InvalidTopicException) e0.getCause();
                assertEquals("some detailed reason", e.getMessage());
            }
        }
    }

    @Test
    public void testDeleteRecords() throws Exception {

        HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        partitionInfos.add(new PartitionInfo("my_topic", 0, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 1, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 2, null, new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 3, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 4, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                partitionInfos, Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
        TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
        TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);
        TopicPartition myTopicPartition3 = new TopicPartition("my_topic", 3);
        TopicPartition myTopicPartition4 = new TopicPartition("my_topic", 4);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            Map<TopicPartition, DeleteRecordsResponse.PartitionResponse> m = new HashMap<>();
            m.put(myTopicPartition0,
                    new DeleteRecordsResponse.PartitionResponse(3, Errors.NONE));
            m.put(myTopicPartition1,
                    new DeleteRecordsResponse.PartitionResponse(DeleteRecordsResponse.INVALID_LOW_WATERMARK, Errors.OFFSET_OUT_OF_RANGE));
            m.put(myTopicPartition3,
                    new DeleteRecordsResponse.PartitionResponse(DeleteRecordsResponse.INVALID_LOW_WATERMARK, Errors.NOT_LEADER_FOR_PARTITION));
            m.put(myTopicPartition4,
                    new DeleteRecordsResponse.PartitionResponse(DeleteRecordsResponse.INVALID_LOW_WATERMARK, Errors.UNKNOWN_TOPIC_OR_PARTITION));

            List<MetadataResponse.TopicMetadata> t = new ArrayList<>();
            List<MetadataResponse.PartitionMetadata> p = new ArrayList<>();
            p.add(new MetadataResponse.PartitionMetadata(Errors.NONE, 0, nodes.get(0), Optional.of(5),
                    singletonList(nodes.get(0)), singletonList(nodes.get(0)), Collections.emptyList()));
            p.add(new MetadataResponse.PartitionMetadata(Errors.NONE, 1, nodes.get(0), Optional.of(5),
                    singletonList(nodes.get(0)), singletonList(nodes.get(0)), Collections.emptyList()));
            p.add(new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE, 2, null,
                    Optional.empty(), singletonList(nodes.get(0)), singletonList(nodes.get(0)),
                    Collections.emptyList()));
            p.add(new MetadataResponse.PartitionMetadata(Errors.NONE, 3, nodes.get(0), Optional.of(5),
                    singletonList(nodes.get(0)), singletonList(nodes.get(0)), Collections.emptyList()));
            p.add(new MetadataResponse.PartitionMetadata(Errors.NONE, 4, nodes.get(0), Optional.of(5),
                    singletonList(nodes.get(0)), singletonList(nodes.get(0)), Collections.emptyList()));

            t.add(new MetadataResponse.TopicMetadata(Errors.NONE, "my_topic", false, p));

            env.kafkaClient().prepareResponse(MetadataResponse.prepareResponse(cluster.nodes(), cluster.clusterResource().clusterId(), cluster.controller().id(), t));
            env.kafkaClient().prepareResponse(new DeleteRecordsResponse(0, m));

            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            recordsToDelete.put(myTopicPartition0, RecordsToDelete.beforeOffset(3L));
            recordsToDelete.put(myTopicPartition1, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(myTopicPartition2, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(myTopicPartition3, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(myTopicPartition4, RecordsToDelete.beforeOffset(10L));

            DeleteRecordsResult results = env.adminClient().deleteRecords(recordsToDelete);

            // success on records deletion for partition 0
            Map<TopicPartition, KafkaFuture<DeletedRecords>> values = results.lowWatermarks();
            KafkaFuture<DeletedRecords> myTopicPartition0Result = values.get(myTopicPartition0);
            long lowWatermark = myTopicPartition0Result.get().lowWatermark();
            assertEquals(lowWatermark, 3);

            // "offset out of range" failure on records deletion for partition 1
            KafkaFuture<DeletedRecords> myTopicPartition1Result = values.get(myTopicPartition1);
            try {
                myTopicPartition1Result.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e0) {
                assertTrue(e0.getCause() instanceof OffsetOutOfRangeException);
            }

            // "leader not available" failure on metadata request for partition 2
            KafkaFuture<DeletedRecords> myTopicPartition2Result = values.get(myTopicPartition2);
            try {
                myTopicPartition2Result.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e1) {
                assertTrue(e1.getCause() instanceof LeaderNotAvailableException);
            }

            // "not leader for partition" failure on records deletion for partition 3
            KafkaFuture<DeletedRecords> myTopicPartition3Result = values.get(myTopicPartition3);
            try {
                myTopicPartition3Result.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e1) {
                assertTrue(e1.getCause() instanceof NotLeaderForPartitionException);
            }

            // "unknown topic or partition" failure on records deletion for partition 4
            KafkaFuture<DeletedRecords> myTopicPartition4Result = values.get(myTopicPartition4);
            try {
                myTopicPartition4Result.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e1) {
                assertTrue(e1.getCause() instanceof UnknownTopicOrPartitionException);
            }
        }
    }

    @Test
    public void testDescribeCluster() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        Node node0 = new Node(0, "localhost", 8121);
        Node node1 = new Node(1, "localhost", 8122);
        Node node2 = new Node(2, "localhost", 8123);
        Node node3 = new Node(3, "localhost", 8124);
        nodes.put(0, node0);
        nodes.put(1, node1);
        nodes.put(2, node2);
        nodes.put(3, node3);

        final Cluster cluster = new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster, AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Prepare the metadata response used for the first describe cluster
            MetadataResponse response = MetadataResponse.prepareResponse(0,
                    new ArrayList<>(nodes.values()),
                    env.cluster().clusterResource().clusterId(),
                    2,
                    Collections.emptyList(),
                    MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
            env.kafkaClient().prepareResponse(response);

            // Prepare the metadata response used for the second describe cluster
            MetadataResponse response2 = MetadataResponse.prepareResponse(0,
                    new ArrayList<>(nodes.values()),
                    env.cluster().clusterResource().clusterId(),
                    3,
                    Collections.emptyList(),
                    1 << AclOperation.DESCRIBE.code() | 1 << AclOperation.ALTER.code());
            env.kafkaClient().prepareResponse(response2);

            // Test DescribeCluster with the authorized operations omitted.
            final DescribeClusterResult result = env.adminClient().describeCluster();
            assertEquals(env.cluster().clusterResource().clusterId(), result.clusterId().get());
            assertEquals(2, result.controller().get().id());
            assertEquals(null, result.authorizedOperations().get());

            // Test DescribeCluster with the authorized operations included.
            final DescribeClusterResult result2 = env.adminClient().describeCluster();
            assertEquals(env.cluster().clusterResource().clusterId(), result2.clusterId().get());
            assertEquals(3, result2.controller().get().id());
            assertEquals(new HashSet<>(Arrays.asList(AclOperation.DESCRIBE, AclOperation.ALTER)),
                    result2.authorizedOperations().get());
        }
    }

    @Test
    public void testListConsumerGroups() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        Node node0 = new Node(0, "localhost", 8121);
        Node node1 = new Node(1, "localhost", 8122);
        Node node2 = new Node(2, "localhost", 8123);
        Node node3 = new Node(3, "localhost", 8124);
        nodes.put(0, node0);
        nodes.put(1, node1);
        nodes.put(2, node2);
        nodes.put(3, node3);

        final Cluster cluster = new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster, AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Empty metadata response should be retried
            env.kafkaClient().prepareResponse(
                     MetadataResponse.prepareResponse(
                            Collections.emptyList(),
                            env.cluster().clusterResource().clusterId(),
                            -1,
                            Collections.emptyList()));

            env.kafkaClient().prepareResponse(
                     MetadataResponse.prepareResponse(
                            env.cluster().nodes(),
                            env.cluster().clusterResource().clusterId(),
                            env.cluster().controller().id(),
                            Collections.emptyList()));

            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            Errors.NONE,
                            asList(
                                    new ListGroupsResponse.Group("group-1", ConsumerProtocol.PROTOCOL_TYPE),
                                    new ListGroupsResponse.Group("group-connect-1", "connector")
                            )),
                    node0);

            // handle retriable errors
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                        Errors.COORDINATOR_NOT_AVAILABLE,
                        Collections.emptyList()
                    ),
                    node1);
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            Errors.COORDINATOR_LOAD_IN_PROGRESS,
                            Collections.emptyList()
                    ),
                    node1);
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            Errors.NONE,
                            asList(
                                    new ListGroupsResponse.Group("group-2", ConsumerProtocol.PROTOCOL_TYPE),
                                    new ListGroupsResponse.Group("group-connect-2", "connector")
                            )),
                    node1);

            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            Errors.NONE,
                            asList(
                                    new ListGroupsResponse.Group("group-3", ConsumerProtocol.PROTOCOL_TYPE),
                                    new ListGroupsResponse.Group("group-connect-3", "connector")
                            )),
                    node2);

            // fatal error
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            Errors.UNKNOWN_SERVER_ERROR,
                            Collections.emptyList()),
                    node3);


            final ListConsumerGroupsResult result = env.adminClient().listConsumerGroups();
            TestUtils.assertFutureError(result.all(), UnknownServerException.class);

            Collection<ConsumerGroupListing> listings = result.valid().get();
            assertEquals(3, listings.size());

            Set<String> groupIds = new HashSet<>();
            for (ConsumerGroupListing listing : listings) {
                groupIds.add(listing.groupId());
            }

            assertEquals(Utils.mkSet("group-1", "group-2", "group-3"), groupIds);
            assertEquals(1, result.errors().get().size());
        }
    }

    @Test
    public void testListConsumerGroupsMetadataFailure() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        Node node0 = new Node(0, "localhost", 8121);
        Node node1 = new Node(1, "localhost", 8122);
        Node node2 = new Node(2, "localhost", 8123);
        nodes.put(0, node0);
        nodes.put(1, node1);
        nodes.put(2, node2);

        final Cluster cluster = new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Empty metadata causes the request to fail since we have no list of brokers
            // to send the ListGroups requests to
            env.kafkaClient().prepareResponse(
                     MetadataResponse.prepareResponse(
                            Collections.emptyList(),
                            env.cluster().clusterResource().clusterId(),
                            -1,
                            Collections.emptyList()));

            final ListConsumerGroupsResult result = env.adminClient().listConsumerGroups();
            TestUtils.assertFutureError(result.all(), KafkaException.class);
        }
    }

    @Test
    public void testDescribeConsumerGroups() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.<PartitionInfo>emptyList(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            //Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS,  Node.noNode()));

            env.kafkaClient().prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();

            //Retriable  errors should be retried
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                "group-0",
                Errors.COORDINATOR_LOAD_IN_PROGRESS,
                "",
                "",
                "",
                Collections.emptyList(),
                Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                "group-0",
                Errors.COORDINATOR_NOT_AVAILABLE,
                "",
                "",
                "",
                Collections.emptyList(),
                Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            data = new DescribeGroupsResponseData();
            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);

            final List<TopicPartition> topicPartitions = new ArrayList<>();
            topicPartitions.add(0, myTopicPartition0);
            topicPartitions.add(1, myTopicPartition1);
            topicPartitions.add(2, myTopicPartition2);

            final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new PartitionAssignor.Assignment(topicPartitions));
            byte[] memberAssignmentBytes = new byte[memberAssignment.remaining()];
            memberAssignment.get(memberAssignmentBytes);

            data.groups().add(DescribeGroupsResponse.groupMetadata(
                    "group-0",
                    Errors.NONE,
                    "",
                    ConsumerProtocol.PROTOCOL_TYPE,
                    "",
                    asList(
                        DescribeGroupsResponse.groupMember("0", "clientId0", "clientHost", memberAssignmentBytes, null),
                        DescribeGroupsResponse.groupMember("1", "clientId1", "clientHost", memberAssignmentBytes, null)
                    ),
                    Collections.emptySet()));

            data.groups().add(DescribeGroupsResponse.groupMetadata(
                    "group-connect-0",
                    Errors.NONE,
                    "",
                    "connect",
                    "",
                    asList(
                        DescribeGroupsResponse.groupMember("0", "clientId0", "clientHost", memberAssignmentBytes, null),
                        DescribeGroupsResponse.groupMember("1", "clientId1", "clientHost", memberAssignmentBytes, null)
                    ),
                    Collections.emptySet()));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList("group-0"));
            final ConsumerGroupDescription groupDescription = result.describedGroups().get("group-0").get();

            assertEquals(1, result.describedGroups().size());
            assertEquals("group-0", groupDescription.groupId());
            assertEquals(2, groupDescription.members().size());
        }
    }

    @Test
    public void testDescribeConsumerGroupsWithAuthorizedOperationsOmitted() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.<PartitionInfo>emptyList(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                "group-0",
                Errors.NONE,
                "",
                ConsumerProtocol.PROTOCOL_TYPE,
                "",
                Collections.emptyList(),
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList("group-0"));
            final ConsumerGroupDescription groupDescription = result.describedGroups().get("group-0").get();

            assertNull(groupDescription.authorizedOperations());
        }
    }

    @Test
    public void testDescribeConsumerGroupOffsets() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            //Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));

            env.kafkaClient().prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, env.cluster().controller()));

            //Retriable  errors should be retried
            env.kafkaClient().prepareResponse(new OffsetFetchResponse(Errors.COORDINATOR_NOT_AVAILABLE, Collections.emptyMap()));
            env.kafkaClient().prepareResponse(new OffsetFetchResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Collections.emptyMap()));

            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);

            final Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = new HashMap<>();
            responseData.put(myTopicPartition0, new OffsetFetchResponse.PartitionData(10,
                    Optional.empty(), "", Errors.NONE));
            responseData.put(myTopicPartition1, new OffsetFetchResponse.PartitionData(0,
                    Optional.empty(), "", Errors.NONE));
            responseData.put(myTopicPartition2, new OffsetFetchResponse.PartitionData(20,
                    Optional.empty(), "", Errors.NONE));
            env.kafkaClient().prepareResponse(new OffsetFetchResponse(Errors.NONE, responseData));

            final ListConsumerGroupOffsetsResult result = env.adminClient().listConsumerGroupOffsets("group-0");
            final Map<TopicPartition, OffsetAndMetadata> partitionToOffsetAndMetadata = result.partitionsToOffsetAndMetadata().get();

            assertEquals(3, partitionToOffsetAndMetadata.size());
            assertEquals(10, partitionToOffsetAndMetadata.get(myTopicPartition0).offset());
            assertEquals(0, partitionToOffsetAndMetadata.get(myTopicPartition1).offset());
            assertEquals(20, partitionToOffsetAndMetadata.get(myTopicPartition2).offset());
        }
    }

    @Test
    public void testDeleteConsumerGroups() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.<PartitionInfo>emptyList(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        final List<String> groupIds = singletonList("group-0");

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            //Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode()));

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final Map<String, Errors> validResponse = new HashMap<>();
            validResponse.put("group-0", Errors.NONE);
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(validResponse));

            final DeleteConsumerGroupsResult result = env.adminClient().deleteConsumerGroups(groupIds);

            final KafkaFuture<Void> results = result.deletedGroups().get("group-0");
            assertNull(results.get());

            //should throw error for non-retriable errors
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED,  Node.noNode()));

            final DeleteConsumerGroupsResult errorResult = env.adminClient().deleteConsumerGroups(groupIds);
            TestUtils.assertFutureError(errorResult.deletedGroups().get("group-0"), GroupAuthorizationException.class);

            //Retriable  errors should be retried
            env.kafkaClient().prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, env.cluster().controller()));

            final Map<String, Errors> errorResponse1 = new HashMap<>();
            errorResponse1.put("group-0", Errors.COORDINATOR_NOT_AVAILABLE);
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(errorResponse1));

            final Map<String, Errors> errorResponse2 = new HashMap<>();
            errorResponse2.put("group-0", Errors.COORDINATOR_LOAD_IN_PROGRESS);
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(errorResponse2));

            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(validResponse));

            final DeleteConsumerGroupsResult errorResult1 = env.adminClient().deleteConsumerGroups(groupIds);

            final KafkaFuture<Void> errorResults = errorResult1.deletedGroups().get("group-0");
            assertNull(errorResults.get());
        }
    }

    @Test
    public void testIncrementalAlterConfigs()  throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            //test error scenarios
            IncrementalAlterConfigsResponseData responseData =  new IncrementalAlterConfigsResponseData();
            responseData.responses().add(new AlterConfigsResourceResult()
                    .setResourceName("")
                    .setResourceType(ConfigResource.Type.BROKER.id())
                    .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                    .setErrorMessage("authorization error"));

            responseData.responses().add(new AlterConfigsResourceResult()
                    .setResourceName("topic1")
                    .setResourceType(ConfigResource.Type.TOPIC.id())
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("Config value append is not allowed for config"));

            env.kafkaClient().prepareResponse(new IncrementalAlterConfigsResponse(responseData));

            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "");
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "topic1");

            AlterConfigOp alterConfigOp1 = new AlterConfigOp(
                    new ConfigEntry("log.segment.bytes", "1073741"),
                    AlterConfigOp.OpType.SET);

            AlterConfigOp alterConfigOp2 = new AlterConfigOp(
                    new ConfigEntry("compression.type", "gzip"),
                    AlterConfigOp.OpType.APPEND);

            final Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(brokerResource, Collections.singletonList(alterConfigOp1));
            configs.put(topicResource, Collections.singletonList(alterConfigOp2));

            AlterConfigsResult result = env.adminClient().incrementalAlterConfigs(configs);
            TestUtils.assertFutureError(result.values().get(brokerResource), ClusterAuthorizationException.class);
            TestUtils.assertFutureError(result.values().get(topicResource), InvalidRequestException.class);

            // Test a call where there are no errors.
            responseData =  new IncrementalAlterConfigsResponseData();
            responseData.responses().add(new AlterConfigsResourceResult()
                    .setResourceName("")
                    .setResourceType(ConfigResource.Type.BROKER.id())
                    .setErrorCode(Errors.NONE.code())
                    .setErrorMessage(ApiError.NONE.message()));

            env.kafkaClient().prepareResponse(new IncrementalAlterConfigsResponse(responseData));
            env.adminClient().incrementalAlterConfigs(Collections.singletonMap(brokerResource, asList(alterConfigOp1))).all().get();
        }
    }

    @SafeVarargs
    private static <T> void assertCollectionIs(Collection<T> collection, T... elements) {
        for (T element : elements) {
            assertTrue("Did not find " + element, collection.contains(element));
        }
        assertEquals("There are unexpected extra elements in the collection.",
            elements.length, collection.size());
    }

    public static KafkaAdminClient createInternal(AdminClientConfig config, KafkaAdminClient.TimeoutProcessorFactory timeoutProcessorFactory) {
        return KafkaAdminClient.createInternal(config, timeoutProcessorFactory);
    }

    public static class FailureInjectingTimeoutProcessorFactory extends KafkaAdminClient.TimeoutProcessorFactory {

        private int numTries = 0;

        private int failuresInjected = 0;

        @Override
        public KafkaAdminClient.TimeoutProcessor create(long now) {
            return new FailureInjectingTimeoutProcessor(now);
        }

        synchronized boolean shouldInjectFailure() {
            numTries++;
            if (numTries == 1) {
                failuresInjected++;
                return true;
            }
            return false;
        }

        public synchronized int failuresInjected() {
            return failuresInjected;
        }

        public final class FailureInjectingTimeoutProcessor extends KafkaAdminClient.TimeoutProcessor {
            public FailureInjectingTimeoutProcessor(long now) {
                super(now);
            }

            boolean callHasExpired(KafkaAdminClient.Call call) {
                if ((!call.isInternal()) && shouldInjectFailure()) {
                    log.debug("Injecting timeout for {}.", call);
                    return true;
                } else {
                    boolean ret = super.callHasExpired(call);
                    log.debug("callHasExpired({}) = {}", call, ret);
                    return ret;
                }
            }
        }

    }

}
