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
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.apache.kafka.clients.admin.internals.InternalDescribeFeaturesResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.BootstrapResolutionException;
import org.apache.kafka.common.errors.DuplicateVoterException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.MismatchedEndpointTypeException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.errors.VoterNotFoundException;
import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.ListConfigResourcesResponseData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.message.UnregisterBrokerResponseData;
import org.apache.kafka.common.message.UnregisterControllerResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddRaftVoterRequest;
import org.apache.kafka.common.requests.AddRaftVoterResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.ListConfigResourcesRequest;
import org.apache.kafka.common.requests.ListConfigResourcesResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RemoveRaftVoterRequest;
import org.apache.kafka.common.requests.RemoveRaftVoterResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.UnregisterBrokerResponse;
import org.apache.kafka.common.requests.UnregisterControllerResponse;
import org.apache.kafka.common.requests.UpdateFeaturesRequest;
import org.apache.kafka.common.requests.UpdateFeaturesResponse;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class KafkaAdminClientTest extends KafkaAdminClientTestBase {

    private static final Uuid REPLICA_DIRECTORY_ID = Uuid.randomUuid();

    @Test
    public void testDefaultApiTimeoutAndRequestTimeoutConflicts() {
        final AdminClientConfig config = newConfMap(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "500");
        KafkaException exception = assertThrows(KafkaException.class,
            () -> KafkaAdminClient.createInternal(config, null));
        assertInstanceOf(ConfigException.class, exception.getCause());
    }

    @Test
    public void testParseDescribeClusterResponseWithError() {
        assertThrows(MismatchedEndpointTypeException.class,
            () -> KafkaAdminClient.parseDescribeClusterResponse(new DescribeClusterResponseData().
                setErrorCode(Errors.MISMATCHED_ENDPOINT_TYPE.code()).
                setErrorMessage("The request was sent to an endpoint of type BROKER, " +
                        "but we wanted an endpoint of type CONTROLLER")));
    }

    @Test
    public void testParseDescribeClusterResponseWithUnexpectedEndpointType() {
        assertThrows(MismatchedEndpointTypeException.class,
            () -> KafkaAdminClient.parseDescribeClusterResponse(new DescribeClusterResponseData().
                    setEndpointType(EndpointType.BROKER.id())));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testParseSuccessfulDescribeClusterResponse(boolean includeController) {
        Cluster cluster = KafkaAdminClient.parseDescribeClusterResponse(new DescribeClusterResponseData().
            setControllerId(includeController ? 0 : -1).
            setEndpointType(EndpointType.CONTROLLER.id()).
            setClusterId("Ek8tjqq1QBWfnaoyHFZqDg").
            setBrokers(new DescribeClusterResponseData.DescribeClusterBrokerCollection(asList(
                new DescribeClusterBroker().
                    setBrokerId(0).
                    setHost("controller0.com").
                    setPort(9092),
                new DescribeClusterBroker().
                    setBrokerId(1).
                    setHost("controller1.com").
                    setPort(9092),
                new DescribeClusterBroker().
                    setBrokerId(2).
                    setHost("controller2.com").
                    setPort(9092)))));
        if (includeController) {
            assertNotNull(cluster.controller());
            assertEquals(0, cluster.controller().id());
        } else {
            assertNull(cluster.controller());
        }
        assertEquals("Ek8tjqq1QBWfnaoyHFZqDg", cluster.clusterResource().clusterId());
        assertEquals(Set.of(
            new Node(0, "controller0.com", 9092),
            new Node(1, "controller1.com", 9092),
            new Node(2, "controller2.com", 9092)), new HashSet<>(cluster.nodes()));
    }

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

    @Test
    public void testGenerateClientId() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String id = KafkaAdminClient.generateClientId(newConfMap(AdminClientConfig.CLIENT_ID_CONFIG, ""));
            assertFalse(ids.contains(id), "Got duplicate id " + id);
            ids.add(id);
        }
        assertEquals("myCustomId",
                KafkaAdminClient.generateClientId(newConfMap(AdminClientConfig.CLIENT_ID_CONFIG, "myCustomId")));
    }

    @Test
    public void testMetricsReporterAutoGeneratedClientId() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        KafkaAdminClient admin = (KafkaAdminClient) AdminClient.create(props);

        MockMetricsReporter mockMetricsReporter = (MockMetricsReporter) admin.metrics.reporters().get(0);

        assertEquals(admin.getClientId(), mockMetricsReporter.clientId);
        assertEquals(1, admin.metrics.reporters().size());
        admin.close();
    }

    @Test
    public void testDisableJmxReporter() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG, "");
        KafkaAdminClient admin = (KafkaAdminClient) AdminClient.create(props);
        assertTrue(admin.metrics.reporters().isEmpty());
        admin.close();
    }

    @Test
    public void testExplicitlyEnableJmxReporter() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG, "org.apache.kafka.common.metrics.JmxReporter");
        KafkaAdminClient admin = (KafkaAdminClient) AdminClient.create(props);
        assertEquals(1, admin.metrics.reporters().size());
        admin.close();
    }

    @Test
    public void testExplicitlyEnableTelemetryReporter() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, "true");
        try (KafkaAdminClient admin = (KafkaAdminClient) AdminClient.create(props)) {
            List<ClientTelemetryReporter> telemetryReporterList = admin.metrics.reporters().stream()
                    .filter(r -> r instanceof ClientTelemetryReporter)
                    .map(r -> (ClientTelemetryReporter) r)
                    .collect(Collectors.toList());

            assertEquals(1, telemetryReporterList.size());
        }
    }

    @Test
    public void testTelemetryReporterIsDisabledByDefault() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        try (KafkaAdminClient admin = (KafkaAdminClient) AdminClient.create(props)) {
            List<ClientTelemetryReporter> telemetryReporterList = admin.metrics.reporters().stream()
                    .filter(r -> r instanceof ClientTelemetryReporter)
                    .map(r -> (ClientTelemetryReporter) r)
                    .collect(Collectors.toList());

            assertTrue(telemetryReporterList.isEmpty());
        }
    }

    @Test
    public void testCloseAdminClient() {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
        }
    }

    /**
     * Test if admin client can be closed in the callback invoked when
     * an api call completes. If calling {@link Admin#close()} in callback, AdminClient thread hangs
     */
    @Test
    @Timeout(10)
    public void testCloseAdminClientInCallback() throws InterruptedException {
        MockTime time = new MockTime();
        AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, mockCluster(3, 0));

        final ListTopicsResult result = env.adminClient().listTopics(new ListTopicsOptions().timeoutMs(1000));
        final KafkaFuture<Collection<TopicListing>> kafkaFuture = result.listings();
        final Semaphore callbackCalled = new Semaphore(0);
        kafkaFuture.whenComplete((topicListings, throwable) -> {
            env.close();
            callbackCalled.release();
        });

        time.sleep(2000); // Advance time to timeout and complete listTopics request
        callbackCalled.acquire();
    }

    @Test
    public void testAdminClientFailureWhenClosed() {
        MockTime time = new MockTime();
        AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, mockCluster(3, 0));
        env.adminClient().close();
        ExecutionException e = assertThrows(ExecutionException.class, () -> env.adminClient().createTopics(
                singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                new CreateTopicsOptions().timeoutMs(10000)).all().get());
        assertInstanceOf(IllegalStateException.class, e.getCause(),
                "Expected an IllegalStateException error, but got " + Utils.stackTrace(e));
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
                    singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(1000)).all();
            TestUtils.assertFutureThrows(TimeoutException.class, future);
        }
    }

    @Test
    public void testConnectionFailureOnMetadataUpdate() throws Exception {
        // This tests the scenario in which we successfully connect to the bootstrap server, but
        // the server disconnects before sending the full response

        Cluster cluster = mockBootstrapCluster();
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster)) {
            Cluster discoveredCluster = mockCluster(3, 0);
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(request -> request instanceof MetadataRequest, null, true);
            env.kafkaClient().prepareResponse(request -> request instanceof MetadataRequest,
                    RequestTestUtils.metadataResponse(discoveredCluster.nodes(), discoveredCluster.clusterResource().clusterId(),
                            1, Collections.emptyList()));
            env.kafkaClient().prepareResponse(body -> body instanceof CreateTopicsRequest,
                    prepareCreateTopicsResponse("myTopic", Errors.NONE));

            KafkaFuture<Void> future = env.adminClient().createTopics(
                    singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();

            future.get();
        }
    }

    @Test
    public void testUnreachableBootstrapServer() throws Exception {
        verifyUnreachableBootstrapServer(MetadataRecoveryStrategy.REBOOTSTRAP);
    }

    @Test
    public void testUnreachableBootstrapServerNoRebootstrap() throws Exception {
        verifyUnreachableBootstrapServer(MetadataRecoveryStrategy.NONE);
    }

    /**
     * Test that we propagate exceptions encountered when fetching metadata.
     */
    @Test
    public void testPropagatedMetadataFetchException() throws Exception {
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM,
                mockCluster(3, 0),
                newStrMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8121",
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().createPendingAuthenticationError(env.cluster().nodeById(0),
                    TimeUnit.DAYS.toMillis(1));
            env.kafkaClient().prepareResponse(prepareCreateTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                new CreateTopicsOptions().timeoutMs(1000)).all();
            TestUtils.assertFutureThrows(SaslAuthenticationException.class, future);
        }
    }

    @Test
    public void testAdminClientApisAuthenticationFailure() {
        Cluster cluster = mockBootstrapCluster();
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster,
                newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000",
                    // Default "metadata.recovery.strategy" is rebootstrap. If it meets "retry.backoff.ms" (default is 100L),
                    // following assertion will fail. Set it to none to avoid authentication error cleanup.
                    AdminClientConfig.METADATA_RECOVERY_STRATEGY_CONFIG, "none"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().createPendingAuthenticationError(cluster.nodes().get(0),
                    TimeUnit.DAYS.toMillis(1));
            callAdminClientApisAndExpectAnAuthenticationError(env);
            callClientQuotasApisAndExpectAnAuthenticationError(env);
        }
    }

    @Test
    public void testDescribeCluster() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(4, 0),
            AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Prepare the describe cluster response used for the first describe cluster
            env.kafkaClient().prepareResponse(
                prepareDescribeClusterResponse(0,
                    env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(),
                    2,
                    MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                    false));

            // Prepare the describe cluster response used for the second describe cluster
            env.kafkaClient().prepareResponse(
                prepareDescribeClusterResponse(0,
                    env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(),
                    3,
                    1 << AclOperation.DESCRIBE.code() | 1 << AclOperation.ALTER.code(),
                    false));

            // Test DescribeCluster with the authorized operations omitted.
            final DescribeClusterResult result = env.adminClient().describeCluster();
            assertEquals(env.cluster().clusterResource().clusterId(), result.clusterId().get());
            assertEquals(new HashSet<>(env.cluster().nodes()), new HashSet<>(result.nodes().get()));
            assertEquals(2, result.controller().get().id());
            assertNull(result.authorizedOperations().get());

            // Test DescribeCluster with the authorized operations included.
            final DescribeClusterResult result2 = env.adminClient().describeCluster();
            assertEquals(env.cluster().clusterResource().clusterId(), result2.clusterId().get());
            assertEquals(new HashSet<>(env.cluster().nodes()), new HashSet<>(result2.nodes().get()));
            assertEquals(3, result2.controller().get().id());
            assertEquals(Set.of(AclOperation.DESCRIBE, AclOperation.ALTER),
                result2.authorizedOperations().get());
        }
    }

    @Test
    public void testDescribeClusterHandleError() {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(4, 0),
            AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Prepare the describe cluster response used for the first describe cluster
            String errorMessage = "my error";
            env.kafkaClient().prepareResponse(
                new DescribeClusterResponse(new DescribeClusterResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage(errorMessage)));

            final DescribeClusterResult result = env.adminClient().describeCluster();
            TestUtils.assertFutureThrows(InvalidRequestException.class, result.clusterId(), errorMessage);
            TestUtils.assertFutureThrows(InvalidRequestException.class, result.controller(), errorMessage);
            TestUtils.assertFutureThrows(InvalidRequestException.class, result.nodes(), errorMessage);
            TestUtils.assertFutureThrows(InvalidRequestException.class, result.authorizedOperations(), errorMessage);
        }
    }

    @Test
    public void testDescribeClusterFailBack() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(4, 0),
            AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Reject the describe cluster request with an unsupported exception
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof DescribeClusterRequest);

            // Prepare the metadata response used for the first describe cluster
            env.kafkaClient().prepareResponse(
                RequestTestUtils.metadataResponse(
                    0,
                    env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(),
                    2,
                    Collections.emptyList(),
                    MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                    ApiKeys.METADATA.latestVersion()));

            final DescribeClusterResult result = env.adminClient().describeCluster();
            assertEquals(env.cluster().clusterResource().clusterId(), result.clusterId().get());
            assertEquals(new HashSet<>(env.cluster().nodes()), new HashSet<>(result.nodes().get()));
            assertEquals(2, result.controller().get().id());
            assertNull(result.authorizedOperations().get());
        }
    }

    @Test
    public void testDescribeClusterHandleUnsupportedVersionForIncludingFencedBrokers() {
        ApiVersion describeClusterV1 = new ApiVersion()
            .setApiKey(ApiKeys.DESCRIBE_CLUSTER.id)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 1);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(Collections.singletonList(describeClusterV1)));

            env.kafkaClient().prepareUnsupportedVersionResponse(
                    request -> request instanceof DescribeClusterRequest);

            final DescribeClusterResult result = env.adminClient().describeCluster(new DescribeClusterOptions().includeFencedBrokers(true));
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.nodes());
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {1, 2})
    public void testUpdateFeaturesDuringSuccess(short version) throws Exception {
        final Map<String, FeatureUpdate> updates = makeTestFeatureUpdates();
        // Only v1 and below specifies error codes per feature for NONE error.
        Set<String> features = version <= 1 ? updates.keySet() : Set.of();
        testUpdateFeatures(updates, ApiError.NONE, features);
    }

    @Test
    public void testUpdateFeaturesTopLevelError() throws Exception {
        final Map<String, FeatureUpdate> updates = makeTestFeatureUpdates();
        testUpdateFeatures(updates, new ApiError(Errors.INVALID_REQUEST), Set.of());
    }

    @ParameterizedTest
    @ValueSource(shorts = {1, 2})
    public void testUpdateFeaturesHandleNotControllerException(short version) throws Exception {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof UpdateFeaturesRequest,
                UpdateFeaturesResponse.createWithErrors(
                    new ApiError(Errors.NOT_CONTROLLER),
                    Set.of(),
                    0),
                env.cluster().nodeById(0));
            final int controllerId = 1;
            env.kafkaClient().prepareResponse(RequestTestUtils.metadataResponse(env.cluster().nodes(),
                env.cluster().clusterResource().clusterId(),
                controllerId,
                Collections.emptyList()));
            // Only v1 and below specifies error codes per feature for NONE error.
            Set<String> features = version <= 1 ? Set.of("test_feature_1", "test_feature_2") : Set.of();
            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof UpdateFeaturesRequest,
                UpdateFeaturesResponse.createWithErrors(
                    ApiError.NONE,
                    features,
                    0),
                env.cluster().nodeById(controllerId));
            final KafkaFuture<Void> future = env.adminClient().updateFeatures(
                Map.of(
                    "test_feature_1", new FeatureUpdate((short) 2,  FeatureUpdate.UpgradeType.UPGRADE),
                    "test_feature_2", new FeatureUpdate((short) 3,  FeatureUpdate.UpgradeType.SAFE_DOWNGRADE)),
                new UpdateFeaturesOptions().timeoutMs(10000)
            ).all();
            future.get();
        }
    }

    @Test
    public void testUpdateFeaturesShouldFailRequestForEmptyUpdates() {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            assertThrows(
                IllegalArgumentException.class,
                () -> env.adminClient().updateFeatures(new HashMap<>()));
        }
    }

    @Test
    public void testUpdateFeaturesShouldFailRequestForInvalidFeatureName() {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            assertThrows(
                IllegalArgumentException.class,
                () -> env.adminClient().updateFeatures(
                    Map.of("feature", new FeatureUpdate((short) 2,  FeatureUpdate.UpgradeType.UPGRADE),
                        "", new FeatureUpdate((short) 2,  FeatureUpdate.UpgradeType.UPGRADE))));
        }
    }

    @Test
    public void testUpdateFeaturesShouldFailRequestInClientWhenDowngradeFlagIsNotSetDuringDeletion() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new FeatureUpdate((short) 0,  FeatureUpdate.UpgradeType.UPGRADE));
    }

    @Test
    public void testDescribeFeaturesSuccess() throws Exception {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponse(
                body -> body instanceof ApiVersionsRequest,
                prepareApiVersionsResponseForDescribeFeatures(Errors.NONE));
            final var result = (InternalDescribeFeaturesResult) env.adminClient().describeFeatures(
                new DescribeFeaturesOptions().timeoutMs(10000));
            assertEquals(defaultFeatureMetadata(), result.featureMetadata().get());
            assertNotNull(result.nodeApiVersions().get().apiVersion(ApiKeys.API_VERSIONS));
        }
    }

    @Test
    public void testDescribeFeaturesFailure() {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponse(
                body -> body instanceof ApiVersionsRequest,
                prepareApiVersionsResponseForDescribeFeatures(Errors.INVALID_REQUEST));
            final DescribeFeaturesOptions options = new DescribeFeaturesOptions();
            options.timeoutMs(10000);
            final var result = (InternalDescribeFeaturesResult) env.adminClient().describeFeatures(options);
            TestUtils.assertFutureThrows(InvalidRequestException.class, result.featureMetadata());
            TestUtils.assertFutureThrows(InvalidRequestException.class, result.nodeApiVersions());
        }
    }

    @Test
    public void testDescribeFeaturesWithNodeSuccess() throws Exception {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponseFrom(
                body -> body instanceof ApiVersionsRequest,
                prepareApiVersionsResponseForDescribeFeatures(Errors.NONE),
                env.cluster().nodeById(0));
            final KafkaFuture<FeatureMetadata> future = env.adminClient().describeFeatures(
                new DescribeFeaturesOptions().timeoutMs(10000).nodeId(0)).featureMetadata();
            final FeatureMetadata metadata = future.get();
            assertEquals(defaultFeatureMetadata(), metadata);
        }
    }

    @Test
    public void testDescribeFeaturesWithNodeFailure() throws Exception {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponseFrom(
                body -> body instanceof ApiVersionsRequest,
                prepareApiVersionsResponseForDescribeFeatures(Errors.NONE),
                env.cluster().nodeById(1));
            final var result = (InternalDescribeFeaturesResult) env.adminClient().describeFeatures(
                new DescribeFeaturesOptions().timeoutMs(1000).nodeId(0));
            TestUtils.assertFutureThrows(TimeoutException.class, result.featureMetadata());
            TestUtils.assertFutureThrows(TimeoutException.class, result.nodeApiVersions());
        }
    }

    @Test
    public void testDescribeMetadataQuorumSuccess() throws Exception {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(ApiKeys.DESCRIBE_QUORUM.id,
                    ApiKeys.DESCRIBE_QUORUM.oldestVersion(),
                    ApiKeys.DESCRIBE_QUORUM.latestVersion()));

            // Test with optional fields set
            env.kafkaClient().prepareResponse(
                    body -> body instanceof DescribeQuorumRequest,
                    prepareDescribeQuorumResponse(Errors.NONE, Errors.NONE, false, false, false, false, false));
            KafkaFuture<QuorumInfo> future = env.adminClient().describeMetadataQuorum().quorumInfo();
            QuorumInfo quorumInfo = future.get();
            assertEquals(defaultQuorumInfo(false), quorumInfo);

            // Test with optional fields empty
            env.kafkaClient().prepareResponse(
                    body -> body instanceof DescribeQuorumRequest,
                    prepareDescribeQuorumResponse(Errors.NONE, Errors.NONE, false, false, false, false, true));
            future = env.adminClient().describeMetadataQuorum().quorumInfo();
            quorumInfo = future.get();
            assertEquals(defaultQuorumInfo(true), quorumInfo);
        }
    }

    @Test
    public void testDescribeMetadataQuorumRetriableError() throws Exception {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(ApiKeys.DESCRIBE_QUORUM.id,
                ApiKeys.DESCRIBE_QUORUM.oldestVersion(),
                ApiKeys.DESCRIBE_QUORUM.latestVersion()));

            // First request fails with a NOT_LEADER_OR_FOLLOWER error (which is retriable)
            env.kafkaClient().prepareResponse(
                body -> body instanceof DescribeQuorumRequest,
                prepareDescribeQuorumResponse(Errors.NONE, Errors.NOT_LEADER_OR_FOLLOWER, false, false, false, false, false));

            // The second request succeeds
            env.kafkaClient().prepareResponse(
                body -> body instanceof DescribeQuorumRequest,
                prepareDescribeQuorumResponse(Errors.NONE, Errors.NONE, false, false, false, false, false));

            KafkaFuture<QuorumInfo> future = env.adminClient().describeMetadataQuorum().quorumInfo();
            QuorumInfo quorumInfo = future.get();
            assertEquals(defaultQuorumInfo(false), quorumInfo);
        }
    }

    @Test
    public void testDescribeMetadataQuorumFailure() {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(ApiKeys.DESCRIBE_QUORUM.id,
                        ApiKeys.DESCRIBE_QUORUM.oldestVersion(),
                        ApiKeys.DESCRIBE_QUORUM.latestVersion()));

            // Test top level error
            env.kafkaClient().prepareResponse(
                    body -> body instanceof DescribeQuorumRequest,
                    prepareDescribeQuorumResponse(Errors.INVALID_REQUEST, Errors.NONE, false, false, false, false, false));
            KafkaFuture<QuorumInfo> future = env.adminClient().describeMetadataQuorum().quorumInfo();
            TestUtils.assertFutureThrows(InvalidRequestException.class, future);

            // Test incorrect topic count
            env.kafkaClient().prepareResponse(
                    body -> body instanceof DescribeQuorumRequest,
                    prepareDescribeQuorumResponse(Errors.NONE, Errors.NONE, true, false, false, false, false));
            future = env.adminClient().describeMetadataQuorum().quorumInfo();
            TestUtils.assertFutureThrows(UnknownServerException.class, future);

            // Test incorrect topic name
            env.kafkaClient().prepareResponse(
                    body -> body instanceof DescribeQuorumRequest,
                    prepareDescribeQuorumResponse(Errors.NONE, Errors.NONE, false, true, false, false, false));
            future = env.adminClient().describeMetadataQuorum().quorumInfo();
            TestUtils.assertFutureThrows(UnknownServerException.class, future);

            // Test incorrect partition count
            env.kafkaClient().prepareResponse(
                    body -> body instanceof DescribeQuorumRequest,
                    prepareDescribeQuorumResponse(Errors.NONE, Errors.NONE, false, false, true, false, false));
            future = env.adminClient().describeMetadataQuorum().quorumInfo();
            TestUtils.assertFutureThrows(UnknownServerException.class, future);

            // Test incorrect partition index
            env.kafkaClient().prepareResponse(
                    body -> body instanceof DescribeQuorumRequest,
                    prepareDescribeQuorumResponse(Errors.NONE, Errors.NONE, false, false, false, true, false));
            future = env.adminClient().describeMetadataQuorum().quorumInfo();
            TestUtils.assertFutureThrows(UnknownServerException.class, future);

            // Test partition level error
            env.kafkaClient().prepareResponse(
                    body -> body instanceof DescribeQuorumRequest,
                    prepareDescribeQuorumResponse(Errors.NONE, Errors.INVALID_REQUEST, false, false, false, false, false));
            future = env.adminClient().describeMetadataQuorum().quorumInfo();
            TestUtils.assertFutureThrows(InvalidRequestException.class, future);

            // Test all incorrect and no errors
            env.kafkaClient().prepareResponse(
                    body -> body instanceof DescribeQuorumRequest,
                    prepareDescribeQuorumResponse(Errors.NONE, Errors.NONE, true, true, true, true, false));
            future = env.adminClient().describeMetadataQuorum().quorumInfo();
            TestUtils.assertFutureThrows(UnknownServerException.class, future);

            // Test all incorrect and both errors
            env.kafkaClient().prepareResponse(
                    body -> body instanceof DescribeQuorumRequest,
                    prepareDescribeQuorumResponse(Errors.INVALID_REQUEST, Errors.INVALID_REQUEST, true, true, true, true, false));
            future = env.adminClient().describeMetadataQuorum().quorumInfo();
            TestUtils.assertFutureThrows(InvalidRequestException.class, future);
        }
    }

    @Test
    public void testGetSubLevelError() {
        List<MemberIdentity> memberIdentities = asList(
            new MemberIdentity().setGroupInstanceId("instance-0"),
            new MemberIdentity().setGroupInstanceId("instance-1"));
        Map<MemberIdentity, Errors> errorsMap = new HashMap<>();
        errorsMap.put(memberIdentities.get(0), Errors.NONE);
        errorsMap.put(memberIdentities.get(1), Errors.FENCED_INSTANCE_ID);
        assertEquals(IllegalArgumentException.class, KafkaAdminClient.getSubLevelError(errorsMap,
                                                                                       new MemberIdentity().setGroupInstanceId("non-exist-id"), "For unit test").getClass());
        assertNull(KafkaAdminClient.getSubLevelError(errorsMap, memberIdentities.get(0), "For unit test"));
        assertEquals(FencedInstanceIdException.class, KafkaAdminClient.getSubLevelError(
            errorsMap, memberIdentities.get(1), "For unit test").getClass());
    }

    @Test
    public void testSuccessfulRetryAfterRequestTimeout() throws Exception {
        HashMap<Integer, Node> nodes = new HashMap<>();
        MockTime time = new MockTime();
        Node node0 = new Node(0, "localhost", 8121);
        nodes.put(0, node0);
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                singletonList(new PartitionInfo("foo", 0, node0, new Node[]{node0}, new Node[]{node0})),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        final int requestTimeoutMs = 1000;
        final int retryBackoffMs = 100;
        final int apiTimeoutMs = 3000;

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs),
                AdminClientConfig.RETRY_BACKOFF_MAX_MS_CONFIG, String.valueOf(retryBackoffMs),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final ListTopicsResult result = env.adminClient()
                    .listTopics(new ListTopicsOptions().timeoutMs(apiTimeoutMs));

            // Wait until the first attempt has been sent, then advance the time
            TestUtils.waitForCondition(() -> env.kafkaClient().hasInFlightRequests(),
                    "Timed out waiting for Metadata request to be sent");
            time.sleep(requestTimeoutMs + 1);

            // Wait for the request to be timed out before backing off
            TestUtils.waitForCondition(() -> !env.kafkaClient().hasInFlightRequests(),
                    "Timed out waiting for inFlightRequests to be timed out");
            time.sleep(retryBackoffMs + 1);

            // Since api timeout bound is not hit, AdminClient should retry
            TestUtils.waitForCondition(() -> env.kafkaClient().hasInFlightRequests(),
                    "Failed to retry Metadata request");
            env.kafkaClient().respond(prepareMetadataResponse(cluster, Errors.NONE));

            assertEquals(1, result.listings().get().size());
            assertEquals("foo", result.listings().get().iterator().next().name());
        }
    }

    @Test
    public void testDefaultApiTimeout() throws Exception {
        testApiTimeout(1500, 3000, OptionalInt.empty());
    }

    @Test
    public void testDefaultApiTimeoutOverride() throws Exception {
        testApiTimeout(1500, 10000, OptionalInt.of(3000));
    }

    @Test
    public void testRequestTimeoutExceedingDefaultApiTimeout() throws Exception {
        HashMap<Integer, Node> nodes = new HashMap<>();
        MockTime time = new MockTime();
        Node node0 = new Node(0, "localhost", 8121);
        nodes.put(0, node0);
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                singletonList(new PartitionInfo("foo", 0, node0, new Node[]{node0}, new Node[]{node0})),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        // This test assumes the default api timeout value of 60000. When the request timeout
        // is set to something larger, we should adjust the api timeout accordingly for compatibility.

        final int retryBackoffMs = 100;
        final int requestTimeoutMs = 120000;

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            ListTopicsOptions options = new ListTopicsOptions();

            final ListTopicsResult result = env.adminClient().listTopics(options);

            // Wait until the first attempt has been sent, then advance the time by the default api timeout
            TestUtils.waitForCondition(() -> env.kafkaClient().hasInFlightRequests(),
                    "Timed out waiting for Metadata request to be sent");
            time.sleep(60001);

            // The in-flight request should not be cancelled
            assertTrue(env.kafkaClient().hasInFlightRequests());

            // Now sleep the remaining time for the request timeout to expire
            time.sleep(60000);
            TestUtils.assertFutureThrows(TimeoutException.class, result.future);
        }
    }

    private static final int UNREGISTER_NODE_ID = 1;

    private static final Function<Errors, AbstractResponse> BROKER_RESPONSE_FACTORY =
        error -> new UnregisterBrokerResponse(new UnregisterBrokerResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(error.message()));

    private static final Function<Errors, AbstractResponse> CONTROLLER_RESPONSE_FACTORY =
        error -> new UnregisterControllerResponse(new UnregisterControllerResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(error.message()));

    private static final Function<Admin, KafkaFuture<Void>> UNREGISTER_BROKER_CALL =
        admin -> admin.unregisterBroker(UNREGISTER_NODE_ID).all();

    private static final Function<Admin, KafkaFuture<Void>> UNREGISTER_CONTROLLER_CALL =
        admin -> admin.unregisterController(UNREGISTER_NODE_ID).all();

    private void runUnregisterScenario(
        AdminClientUnitTestEnv env,
        ApiKeys apiKey,
        Function<Errors, AbstractResponse> responseFactory,
        Function<Admin, KafkaFuture<Void>> adminCall,
        List<Errors> responsesToPrepare,
        Class<? extends Throwable> expectedException
    ) throws ExecutionException, InterruptedException {
        env.kafkaClient().setNodeApiVersions(
            NodeApiVersions.create(apiKey.id, (short) 0, (short) 0));
        for (Errors error : responsesToPrepare) {
            env.kafkaClient().prepareResponse(responseFactory.apply(error));
        }
        KafkaFuture<Void> future = adminCall.apply(env.adminClient());
        assertNotNull(future);
        if (expectedException == null) {
            future.get();
        } else {
            TestUtils.assertFutureThrows(expectedException, future);
        }
    }

    @Test
    public void testUnregisterBrokerSuccess() throws InterruptedException, ExecutionException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            runUnregisterScenario(env, ApiKeys.UNREGISTER_BROKER, BROKER_RESPONSE_FACTORY,
                UNREGISTER_BROKER_CALL, List.of(Errors.NONE), null);
        }
    }

    @Test
    public void testUnregisterBrokerFailure() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            runUnregisterScenario(env, ApiKeys.UNREGISTER_BROKER, BROKER_RESPONSE_FACTORY,
                UNREGISTER_BROKER_CALL, List.of(Errors.UNKNOWN_SERVER_ERROR), UnknownServerException.class);
        }
    }

    @Test
    public void testUnregisterBrokerTimeoutAndSuccessRetry() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            runUnregisterScenario(env, ApiKeys.UNREGISTER_BROKER, BROKER_RESPONSE_FACTORY,
                UNREGISTER_BROKER_CALL, List.of(Errors.REQUEST_TIMED_OUT, Errors.NONE), null);
        }
    }

    @Test
    public void testUnregisterBrokerTimeoutAndFailureRetry() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            runUnregisterScenario(env, ApiKeys.UNREGISTER_BROKER, BROKER_RESPONSE_FACTORY,
                UNREGISTER_BROKER_CALL, List.of(Errors.REQUEST_TIMED_OUT, Errors.UNKNOWN_SERVER_ERROR),
                UnknownServerException.class);
        }
    }

    @Test
    public void testUnregisterBrokerTimeoutMaxRetry() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv(Time.SYSTEM, AdminClientConfig.RETRIES_CONFIG, "1")) {
            runUnregisterScenario(env, ApiKeys.UNREGISTER_BROKER, BROKER_RESPONSE_FACTORY,
                UNREGISTER_BROKER_CALL, List.of(Errors.REQUEST_TIMED_OUT, Errors.REQUEST_TIMED_OUT),
                TimeoutException.class);
        }
    }

    @Test
    public void testUnregisterBrokerTimeoutMaxWait() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            runUnregisterScenario(env, ApiKeys.UNREGISTER_BROKER, BROKER_RESPONSE_FACTORY,
                admin -> admin.unregisterBroker(UNREGISTER_NODE_ID,
                    new UnregisterBrokerOptions().timeoutMs(10)).all(),
                List.of(), TimeoutException.class);
        }
    }

    @Test
    public void testUnregisterControllerSuccess() throws InterruptedException, ExecutionException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            runUnregisterScenario(env, ApiKeys.UNREGISTER_CONTROLLER, CONTROLLER_RESPONSE_FACTORY,
                UNREGISTER_CONTROLLER_CALL, List.of(Errors.NONE), null);
        }
    }

    @Test
    public void testUnregisterControllerFailure() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            runUnregisterScenario(env, ApiKeys.UNREGISTER_CONTROLLER, CONTROLLER_RESPONSE_FACTORY,
                UNREGISTER_CONTROLLER_CALL, List.of(Errors.UNKNOWN_SERVER_ERROR), UnknownServerException.class);
        }
    }

    @Test
    public void testUnregisterControllerTimeoutAndSuccessRetry() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            runUnregisterScenario(env, ApiKeys.UNREGISTER_CONTROLLER, CONTROLLER_RESPONSE_FACTORY,
                UNREGISTER_CONTROLLER_CALL, List.of(Errors.REQUEST_TIMED_OUT, Errors.NONE), null);
        }
    }

    @Test
    public void testUnregisterControllerTimeoutAndFailureRetry() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            runUnregisterScenario(env, ApiKeys.UNREGISTER_CONTROLLER, CONTROLLER_RESPONSE_FACTORY,
                UNREGISTER_CONTROLLER_CALL, List.of(Errors.REQUEST_TIMED_OUT, Errors.UNKNOWN_SERVER_ERROR),
                UnknownServerException.class);
        }
    }

    @Test
    public void testUnregisterControllerTimeoutMaxRetry() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv(Time.SYSTEM, AdminClientConfig.RETRIES_CONFIG, "1")) {
            runUnregisterScenario(env, ApiKeys.UNREGISTER_CONTROLLER, CONTROLLER_RESPONSE_FACTORY,
                UNREGISTER_CONTROLLER_CALL, List.of(Errors.REQUEST_TIMED_OUT, Errors.REQUEST_TIMED_OUT),
                TimeoutException.class);
        }
    }

    @Test
    public void testUnregisterControllerTimeoutMaxWait() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            runUnregisterScenario(env, ApiKeys.UNREGISTER_CONTROLLER, CONTROLLER_RESPONSE_FACTORY,
                admin -> admin.unregisterController(UNREGISTER_NODE_ID,
                    new UnregisterControllerOptions().timeoutMs(10)).all(),
                List.of(), TimeoutException.class);
        }
    }

    /**
     * Test that if the client can obtain a node assignment, but can't send to the given
     * node, it will disconnect and try a different node.
     */
    @Test
    public void testClientSideTimeoutAfterFailureToSend() throws Exception {
        Cluster cluster = mockCluster(3, 0);
        CompletableFuture<String> disconnectFuture = new CompletableFuture<>();
        MockTime time = new MockTime();
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1",
                          AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "100000",
                          AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "1"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            for (Node node : cluster.nodes()) {
                env.kafkaClient().delayReady(node, 100);
            }

            // We use a countdown latch to ensure that we get to the first
            // call to `ready` before we increment the time below to trigger
            // the disconnect.
            CountDownLatch readyLatch = new CountDownLatch(2);

            env.kafkaClient().setDisconnectFuture(disconnectFuture);
            env.kafkaClient().setReadyCallback(node -> readyLatch.countDown());
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            final ListTopicsResult result = env.adminClient().listTopics();

            readyLatch.await(TestUtils.DEFAULT_MAX_WAIT_MS, TimeUnit.MILLISECONDS);
            log.debug("Advancing clock by 25 ms to trigger client-side disconnect.");
            time.sleep(25);
            disconnectFuture.get();

            log.debug("Enabling nodes to send requests again.");
            for (Node node : cluster.nodes()) {
                env.kafkaClient().delayReady(node, 0);
            }
            time.sleep(5);
            log.info("Waiting for result.");
            assertEquals(0, result.listings().get().size());
        }
    }

    /**
     * Test that if the client can send to a node, but doesn't receive a response, it will
     * disconnect and try a different node.
     */
    @Test
    public void testClientSideTimeoutAfterFailureToReceiveResponse() throws Exception {
        Cluster cluster = mockCluster(3, 0);
        CompletableFuture<String> disconnectFuture = new CompletableFuture<>();
        MockTime time = new MockTime();
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1",
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "100000",
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "0"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().setDisconnectFuture(disconnectFuture);
            final ListTopicsResult result = env.adminClient().listTopics();
            TestUtils.waitForCondition(() -> {
                time.sleep(1);
                return disconnectFuture.isDone();
            }, 5000, 1, () -> "Timed out waiting for expected disconnect");
            assertFalse(disconnectFuture.isCompletedExceptionally());
            assertFalse(result.future.isDone());
            TestUtils.waitForCondition(env.kafkaClient()::hasInFlightRequests,
                "Timed out waiting for retry");
            env.kafkaClient().respond(prepareMetadataResponse(cluster, Errors.NONE));
            assertEquals(0, result.listings().get().size());
        }
    }

    @Test
    public void testClientInstanceId() {

        try (MockedStatic<CommonClientConfigs> mockedCommonClientConfigs = mockStatic(CommonClientConfigs.class, new CallsRealMethods())) {
            ClientTelemetryReporter clientTelemetryReporter = mock(ClientTelemetryReporter.class);
            clientTelemetryReporter.configure(any());
            mockedCommonClientConfigs.when(() -> CommonClientConfigs.telemetryReporter(anyString(), any())).thenReturn(Optional.of(clientTelemetryReporter));
            
            try (AdminClientUnitTestEnv env = mockClientEnv(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, "true")) {
                ClientTelemetrySender clientTelemetrySender = mock(ClientTelemetrySender.class);
                Uuid expectedUuid = Uuid.randomUuid();
                when(clientTelemetryReporter.telemetrySender()).thenReturn(clientTelemetrySender);
                when(clientTelemetrySender.clientInstanceId(any())).thenReturn(Optional.of(expectedUuid));

                Uuid result = env.adminClient().clientInstanceId(Duration.ofSeconds(1));
                assertEquals(expectedUuid, result);
            }
        }
    }

    @Test
    public void testClientInstanceIdInvalidTimeout() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");

        KafkaAdminClient admin = (KafkaAdminClient) AdminClient.create(props);
        Exception exception = assertThrows(IllegalArgumentException.class, () -> admin.clientInstanceId(Duration.ofMillis(-1)));
        assertEquals("The timeout cannot be negative.", exception.getMessage());

        admin.close();
    }

    @Test
    public void testClientInstanceIdNoTelemetryReporterRegistered() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, "false");

        KafkaAdminClient admin = (KafkaAdminClient) AdminClient.create(props);
        Exception exception = assertThrows(IllegalStateException.class, () -> admin.clientInstanceId(Duration.ofMillis(0)));
        assertEquals("Telemetry is not enabled. Set config `enable.metrics.push` to `true`.", exception.getMessage());

        admin.close();
    }

    @SuppressWarnings({"deprecation", "removal"})
    @Test
    public void testListClientMetricsResources() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            List<ClientMetricsResourceListing> expected = asList(
                new ClientMetricsResourceListing("one"),
                new ClientMetricsResourceListing("two")
            );

            ListConfigResourcesResponseData responseData =
                new ListConfigResourcesResponseData().setErrorCode(Errors.NONE.code());

            responseData.configResources()
                .add(new ListConfigResourcesResponseData
                    .ConfigResource()
                    .setResourceName("one")
                    .setResourceType(ConfigResource.Type.CLIENT_METRICS.id())
                );
            responseData.configResources()
                .add(new ListConfigResourcesResponseData
                    .ConfigResource()
                    .setResourceName("two")
                    .setResourceType(ConfigResource.Type.CLIENT_METRICS.id())
                );

            env.kafkaClient().prepareResponse(
                request -> request instanceof ListConfigResourcesRequest,
                new ListConfigResourcesResponse(responseData));

            ListClientMetricsResourcesResult result = env.adminClient().listClientMetricsResources();
            assertEquals(new HashSet<>(expected), new HashSet<>(result.all().get()));
        }
    }

    @SuppressWarnings({"deprecation", "removal"})
    @Test
    public void testListClientMetricsResourcesEmpty() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            List<ClientMetricsResourceListing> expected = Collections.emptyList();

            ListConfigResourcesResponseData responseData =
                new ListConfigResourcesResponseData().setErrorCode(Errors.NONE.code());

            env.kafkaClient().prepareResponse(
                request -> request instanceof ListConfigResourcesRequest,
                new ListConfigResourcesResponse(responseData));

            ListClientMetricsResourcesResult result = env.adminClient().listClientMetricsResources();
            assertEquals(new HashSet<>(expected), new HashSet<>(result.all().get()));
        }
    }

    @SuppressWarnings({"deprecation", "removal"})
    @Test
    public void testListClientMetricsResourcesNotSupported() {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponse(
                request -> request instanceof ListConfigResourcesRequest,
                prepareListClientMetricsResourcesResponse(Errors.UNSUPPORTED_VERSION));

            ListClientMetricsResourcesResult result = env.adminClient().listClientMetricsResources();

            // Validate response
            assertNotNull(result.all());
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.all());
        }
    }

    @Test
    public void testListConfigResources() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            List<ConfigResource> expected = List.of(
                new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "client-metrics"),
                new ConfigResource(ConfigResource.Type.BROKER, "1"),
                new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "1"),
                new ConfigResource(ConfigResource.Type.TOPIC, "topic"),
                new ConfigResource(ConfigResource.Type.GROUP, "group")
            );

            ListConfigResourcesResponseData responseData =
                new ListConfigResourcesResponseData().setErrorCode(Errors.NONE.code());

            expected.forEach(c ->
                responseData.configResources()
                    .add(new ListConfigResourcesResponseData
                        .ConfigResource()
                        .setResourceName(c.name())
                        .setResourceType(c.type().id())
                    )
            );

            env.kafkaClient().prepareResponse(
                request -> request instanceof ListConfigResourcesRequest,
                new ListConfigResourcesResponse(responseData));

            ListConfigResourcesResult result = env.adminClient().listConfigResources();
            assertEquals(expected.size(), result.all().get().size());
            assertEquals(new HashSet<>(expected), new HashSet<>(result.all().get()));
        }
    }

    @Test
    public void testListConfigResourcesEmpty() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            ListConfigResourcesResponseData responseData =
                new ListConfigResourcesResponseData().setErrorCode(Errors.NONE.code());

            env.kafkaClient().prepareResponse(
                request -> request instanceof ListConfigResourcesRequest,
                new ListConfigResourcesResponse(responseData));

            ListConfigResourcesResult result = env.adminClient().listConfigResources();
            assertTrue(result.all().get().isEmpty());
        }
    }

    @Test
    public void testListConfigResourcesNotSupported() {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponse(
                request -> request instanceof ListConfigResourcesRequest,
                new ListConfigResourcesResponse(new ListConfigResourcesResponseData()
                    .setErrorCode(Errors.UNSUPPORTED_VERSION.code())));

            ListConfigResourcesResult result = env.adminClient().listConfigResources(
                Set.of(ConfigResource.Type.UNKNOWN), new ListConfigResourcesOptions());

            assertNotNull(result.all());
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, result.all());
        }
    }

    @Test
    public void testCallFailWithUnsupportedVersionExceptionDoesNotHaveConcurrentModificationException() throws InterruptedException {
        Cluster cluster = mockCluster(1, 0);
        try (MockClient mockClient = new MockClient(Time.SYSTEM, new MockClient.MockMetadataUpdater() {
            @Override
            public List<Node> fetchNodes() {
                return cluster.nodes();
            }

            @Override
            public boolean isUpdateNeeded() {
                return false;
            }

            @Override
            public void update(Time time, MockClient.MetadataUpdate update) {
                throw new UnsupportedOperationException();
            }
        })) {
            AdminMetadataManager metadataManager = mock(AdminMetadataManager.class);

            // first false result make sure LeastLoadedBrokerOrActiveKController#provide can go to requestUpdate
            // second true result make sure LeastLoadedBrokerOrActiveKController#provide can get a node
            doReturn(false).doReturn(true).when(metadataManager).isReady();

            // make maybeDrainPendingCall throw UnsupportedVersionException and go to Call#fail
            doThrow(new UnsupportedVersionException("Unsupported version")).doNothing().when(metadataManager).requestUpdate();

            // make sure describeCluster handleUnsupportedVersionException doesn't always return false
            doReturn(false).when(metadataManager).usingBootstrapControllers();
            // avoid sending fetchMetadata request
            doReturn(1L).when(metadataManager).metadataFetchDelayMs(anyLong());

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            try (KafkaAdminClient admin = KafkaAdminClient.createInternal(
                    new AdminClientConfig(Collections.emptyMap()), metadataManager, mockClient, Time.SYSTEM)) {
                DescribeClusterResult result = admin.describeCluster(new DescribeClusterOptions());

                // make sure maybeDrainPendingCalls doesn't remove duplicate pending calls
                // the listNodes call will be added again in call.fail and remove one in maybeDrainPendingCalls
                TestUtils.waitForCondition(() -> mockClient.inFlightRequestCount() != 0,
                        "Timed out waiting for listNodes request");

                // after handleUnsupportedVersionException, describe cluster use MetadataRequest
                ClientRequest request = mockClient.requests().peek();
                assertEquals(ApiKeys.METADATA, request.apiKey());

                // clear active external request
                mockClient.respondToRequest(request, prepareMetadataResponse(cluster, Errors.NONE));
                assertEquals(cluster.clusterResource().clusterId(), assertDoesNotThrow(() -> result.clusterId().get()));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false, false", "false, true", "true, false", "true, true" })
    public void testAddRaftVoterRequest(boolean fail, boolean sendClusterId) throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, "dummy")) {
            AddRaftVoterResponseData responseData = new AddRaftVoterResponseData();
            if (fail) {
                responseData.
                    setErrorCode(Errors.DUPLICATE_VOTER.code()).
                    setErrorMessage("duplicate");
            }
            AtomicReference<AddRaftVoterRequestData> requestData = new AtomicReference<>();
            env.kafkaClient().prepareResponse(
                request -> {
                    if (!(request instanceof AddRaftVoterRequest)) return false;
                    requestData.set((AddRaftVoterRequestData) request.data());
                    return true;
                },
                new AddRaftVoterResponse(responseData));
            AddRaftVoterOptions options = new AddRaftVoterOptions();
            if (sendClusterId) {
                options.setClusterId(Optional.of("_o_GnDGwQaWu4r-NMzmkTw"));
            }
            AddRaftVoterResult result = env.adminClient().addRaftVoter(1,
                    Uuid.fromString("YAfa4HClT3SIIW2klIUspg"),
                    Collections.singleton(new RaftVoterEndpoint("CONTROLLER", "example.com", 8080)),
                    options);
            assertNotNull(result.all());
            if (fail) {
                TestUtils.assertFutureThrows(DuplicateVoterException.class, result.all());
            } else {
                result.all().get();
            }
            if (sendClusterId) {
                assertEquals("_o_GnDGwQaWu4r-NMzmkTw", requestData.get().clusterId());
            } else {
                assertNull(requestData.get().clusterId());
            }
            assertEquals(1000, requestData.get().timeoutMs());
            assertEquals(1, requestData.get().voterId());
            assertEquals(Uuid.fromString("YAfa4HClT3SIIW2klIUspg"), requestData.get().voterDirectoryId());
            assertEquals(new AddRaftVoterRequestData.Listener().
                    setName("CONTROLLER").
                    setHost("example.com").
                    setPort(8080), requestData.get().listeners().find("CONTROLLER"));

            // In the fail case, we continue to test the `NOT_LEADER_OR_FOLLOWER` error case
            if (fail && !sendClusterId) {
                responseData.
                        setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code()).
                        setErrorMessage("test");
                env.kafkaClient().prepareResponse(
                        request -> {
                            if (!(request instanceof AddRaftVoterRequest)) return false;
                            requestData.set((AddRaftVoterRequestData) request.data());
                            return true;
                        },
                        new AddRaftVoterResponse(responseData));

                // should retry the describe cluster to update the metadata
                env.kafkaClient().prepareResponse(
                        prepareDescribeClusterResponse(0,
                                env.cluster().nodes(),
                                env.cluster().clusterResource().clusterId(),
                                2,
                                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                                true)
                );

                AddRaftVoterResponseData responseData2 = new AddRaftVoterResponseData();
                env.kafkaClient().prepareResponse(
                        request -> {
                            if (!(request instanceof AddRaftVoterRequest)) return false;
                            requestData.set((AddRaftVoterRequestData) request.data());
                            return true;
                        },
                        new AddRaftVoterResponse(responseData2));

                AddRaftVoterResult result2 = env.adminClient().addRaftVoter(1,
                        Uuid.fromString("YAfa4HClT3SIIW2klIUspg"),
                        Collections.singleton(new RaftVoterEndpoint("CONTROLLER", "example.com", 8080)),
                        options);
                result2.all().get();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false, false", "false, true", "true, false", "true, true" })
    public void testRemoveRaftVoterRequest(boolean fail, boolean sendClusterId) throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, "dummy")) {
            RemoveRaftVoterResponseData responseData = new RemoveRaftVoterResponseData();
            if (fail) {
                responseData.
                    setErrorCode(Errors.VOTER_NOT_FOUND.code()).
                    setErrorMessage("not found");
            }
            AtomicReference<RemoveRaftVoterRequestData> requestData = new AtomicReference<>();
            env.kafkaClient().prepareResponse(
                    request -> {
                        if (!(request instanceof RemoveRaftVoterRequest)) return false;
                        requestData.set((RemoveRaftVoterRequestData) request.data());
                        return true;
                    },
                    new RemoveRaftVoterResponse(responseData));
            RemoveRaftVoterOptions options = new RemoveRaftVoterOptions();
            if (sendClusterId) {
                options.setClusterId(Optional.of("_o_GnDGwQaWu4r-NMzmkTw"));
            }
            RemoveRaftVoterResult result = env.adminClient().removeRaftVoter(1,
                Uuid.fromString("YAfa4HClT3SIIW2klIUspg"),
                options);
            assertNotNull(result.all());
            if (fail) {
                TestUtils.assertFutureThrows(VoterNotFoundException.class, result.all());
            } else {
                result.all().get();
            }
            if (sendClusterId) {
                assertEquals("_o_GnDGwQaWu4r-NMzmkTw", requestData.get().clusterId());
            } else {
                assertNull(requestData.get().clusterId());
            }
            assertEquals(1, requestData.get().voterId());
            assertEquals(Uuid.fromString("YAfa4HClT3SIIW2klIUspg"), requestData.get().voterDirectoryId());

            // In the fail case, we continue to test the `NOT_LEADER_OR_FOLLOWER` error case
            if (fail && !sendClusterId) {
                responseData.
                        setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code()).
                        setErrorMessage("test");
                env.kafkaClient().prepareResponse(
                        request -> {
                            if (!(request instanceof RemoveRaftVoterRequest)) return false;
                            requestData.set((RemoveRaftVoterRequestData) request.data());
                            return true;
                        },
                        new RemoveRaftVoterResponse(responseData));

                // should retry the describe cluster to update the metadata
                env.kafkaClient().prepareResponse(
                        prepareDescribeClusterResponse(0,
                                env.cluster().nodes(),
                                env.cluster().clusterResource().clusterId(),
                                2,
                                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                                true)
                );

                RemoveRaftVoterResponseData responseData2 = new RemoveRaftVoterResponseData();
                env.kafkaClient().prepareResponse(
                        request -> {
                            if (!(request instanceof RemoveRaftVoterRequest)) return false;
                            requestData.set((RemoveRaftVoterRequestData) request.data());
                            return true;
                        },
                        new RemoveRaftVoterResponse(responseData2));

                RemoveRaftVoterResult result2 = env.adminClient().removeRaftVoter(1,
                        Uuid.fromString("YAfa4HClT3SIIW2klIUspg"),
                        options);
                result2.all().get();
            }
        }
    }

    /**
     * Test that OutOfMemoryError is properly propagated and not masked as TimeoutException.
     * This test simulates an OOM error during response processing and verifies it propagates
     * without being wrapped. This is a regression test for KAFKA-19932.
     */
    @Test
    public void testOutOfMemoryErrorPropagation() throws Exception {
        MockTime time = new MockTime();
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, mockCluster(1, 0),
                AdminClientConfig.RETRIES_CONFIG, "2",
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "100")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            OutOfMemoryError oomError = new OutOfMemoryError("Simulated OOM during response handling");
            MetadataResponse mockResponse = mock(MetadataResponse.class);
            doThrow(oomError).when(mockResponse).topicMetadata();

            env.kafkaClient().prepareResponse(mockResponse);

            // Make the listTopics call - this will internally trigger a metadata request
            ListTopicsResult result = env.adminClient().listTopics(new ListTopicsOptions().timeoutMs(10000));

            TestUtils.assertFutureThrows(OutOfMemoryError.class, result.names());
        }
    }

    private void verifyUnreachableBootstrapServer(MetadataRecoveryStrategy metadataRecoveryStrategy) throws Exception {
        // This tests the scenario in which the bootstrap server is unreachable for a short while,
        // which prevents AdminClient from being able to send the initial metadata request

        Cluster cluster = Cluster.bootstrap(singletonList(new InetSocketAddress("localhost", 8121)));
        Node bootstrapNode = cluster.nodes().get(0);
        Map<Node, Long> unreachableNodes = Collections.singletonMap(bootstrapNode, 200L);
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster,
                AdminClientUnitTestEnv.clientConfigs(AdminClientConfig.METADATA_RECOVERY_STRATEGY_CONFIG, metadataRecoveryStrategy.name), unreachableNodes)) {
            Cluster discoveredCluster = mockCluster(3, 0);
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            // Bind responses to specific destinations so MockClient delivery does not depend on
            // the iteration order of AdminClient's callsToSend map (which is keyed by Node).
            env.kafkaClient().prepareResponseFrom(body -> body instanceof MetadataRequest,
                    RequestTestUtils.metadataResponse(discoveredCluster.nodes(), discoveredCluster.clusterResource().clusterId(),
                            1, Collections.emptyList()), bootstrapNode);
            if (metadataRecoveryStrategy == MetadataRecoveryStrategy.REBOOTSTRAP) {
                env.kafkaClient().prepareResponseFrom(body -> body instanceof MetadataRequest,
                        RequestTestUtils.metadataResponse(discoveredCluster.nodes(), discoveredCluster.clusterResource().clusterId(),
                                1, Collections.emptyList()), bootstrapNode);
            }
            env.kafkaClient().prepareResponseFrom(body -> body instanceof CreateTopicsRequest,
                prepareCreateTopicsResponse("myTopic", Errors.NONE), discoveredCluster.nodeById(1));

            KafkaFuture<Void> future = env.adminClient().createTopics(
                    singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();

            future.get();
        }
    }

    private void callAdminClientApisAndExpectAnAuthenticationError(AdminClientUnitTestEnv env) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> env.adminClient().createTopics(
            singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
            new CreateTopicsOptions().timeoutMs(10000)).all().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        Map<String, NewPartitions> counts = new HashMap<>();
        counts.put("my_topic", NewPartitions.increaseTo(3));
        counts.put("other_topic", NewPartitions.increaseTo(3, asList(singletonList(2), singletonList(3))));
        e = assertThrows(ExecutionException.class, () -> env.adminClient().createPartitions(counts).all().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        e = assertThrows(ExecutionException.class, () -> env.adminClient().createAcls(asList(ACL1, ACL2)).all().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        e = assertThrows(ExecutionException.class, () -> env.adminClient().describeAcls(FILTER1).values().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        e = assertThrows(ExecutionException.class, () -> env.adminClient().deleteAcls(asList(FILTER1, FILTER2)).all().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        e = assertThrows(ExecutionException.class, () -> env.adminClient().describeConfigs(
            singleton(new ConfigResource(ConfigResource.Type.BROKER, "0"))).all().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));
    }

    private void callClientQuotasApisAndExpectAnAuthenticationError(AdminClientUnitTestEnv env) {
        ExecutionException e = assertThrows(ExecutionException.class,
            () -> env.adminClient().describeClientQuotas(ClientQuotaFilter.all()).entities().get());
        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        ClientQuotaEntity entity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, "user"));
        ClientQuotaAlteration alteration = new ClientQuotaAlteration(entity, singletonList(new ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0)));
        e = assertThrows(ExecutionException.class,
            () -> env.adminClient().alterClientQuotas(singletonList(alteration)).all().get());

        assertInstanceOf(AuthenticationException.class, e.getCause(),
            "Expected an authentication error, but got " + Utils.stackTrace(e));
    }

    private static AdminClientConfig newConfMap(String... vals) {
        return new AdminClientConfig(newStrMap(vals));
    }

    private static Cluster mockBootstrapCluster() {
        return Cluster.bootstrap(singletonList(InetSocketAddress.createUnresolved("localhost", 8121)));
    }

    private Map<String, FeatureUpdate> makeTestFeatureUpdates() {
        return Map.of(
            "test_feature_1", new FeatureUpdate((short) 2,  FeatureUpdate.UpgradeType.UPGRADE),
            "test_feature_2", new FeatureUpdate((short) 3,  FeatureUpdate.UpgradeType.SAFE_DOWNGRADE));
    }

    private void testUpdateFeatures(Map<String, FeatureUpdate> featureUpdates,
                                    ApiError topLevelError,
                                    Set<String> updates) throws Exception {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponse(
                body -> body instanceof UpdateFeaturesRequest,
                UpdateFeaturesResponse.createWithErrors(topLevelError, updates, 0));
            final Map<String, KafkaFuture<Void>> futures = env.adminClient().updateFeatures(
                featureUpdates,
                new UpdateFeaturesOptions().timeoutMs(10000)).values();
            for (final Map.Entry<String, KafkaFuture<Void>> entry : futures.entrySet()) {
                final KafkaFuture<Void> future = entry.getValue();
                if (topLevelError.error() == Errors.NONE) {
                    // Since the top level error was NONE, each future should be successful.
                    future.get();
                } else {
                    final ExecutionException e = assertThrows(ExecutionException.class, future::get);
                    assertEquals(e.getCause().getClass(), topLevelError.exception().getClass());
                    assertEquals(e.getCause().getMessage(), topLevelError.exception().getMessage());
                }
            }
        }
    }

    private void testApiTimeout(int requestTimeoutMs,
                                int defaultApiTimeoutMs,
                                OptionalInt overrideApiTimeoutMs) throws Exception {
        HashMap<Integer, Node> nodes = new HashMap<>();
        MockTime time = new MockTime();
        Node node0 = new Node(0, "localhost", 8121);
        nodes.put(0, node0);
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                singletonList(new PartitionInfo("foo", 0, node0, new Node[]{node0}, new Node[]{node0})),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        final int retryBackoffMs = 100;
        final int effectiveTimeoutMs = overrideApiTimeoutMs.orElse(defaultApiTimeoutMs);
        assertEquals(2 * requestTimeoutMs, effectiveTimeoutMs,
            "This test expects the effective timeout to be twice the request timeout");

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs),
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(defaultApiTimeoutMs))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            ListTopicsOptions options = new ListTopicsOptions();
            overrideApiTimeoutMs.ifPresent(options::timeoutMs);

            final ListTopicsResult result = env.adminClient().listTopics(options);

            // Wait until the first attempt has been sent, then advance the time
            TestUtils.waitForCondition(() -> env.kafkaClient().hasInFlightRequests(),
                    "Timed out waiting for Metadata request to be sent");
            time.sleep(requestTimeoutMs + 1);

            // Wait for the request to be timed out before backing off
            TestUtils.waitForCondition(() -> !env.kafkaClient().hasInFlightRequests(),
                    "Timed out waiting for inFlightRequests to be timed out");

            // Since api timeout bound is not hit, AdminClient should retry
            TestUtils.waitForCondition(() -> {
                boolean hasInflightRequests = env.kafkaClient().hasInFlightRequests();
                if (!hasInflightRequests)
                    time.sleep(retryBackoffMs);
                return hasInflightRequests;
            }, "Timed out waiting for Metadata request to be sent");
            time.sleep(requestTimeoutMs + 1);

            TestUtils.assertFutureThrows(TimeoutException.class, result.future);
        }
    }

    private UnregisterBrokerResponse prepareUnregisterBrokerResponse(Errors error, int throttleTimeMs) {
        return new UnregisterBrokerResponse(new UnregisterBrokerResponseData()
                .setErrorCode(error.code())
                .setErrorMessage(error.message())
                .setThrottleTimeMs(throttleTimeMs));
    }

    private static ListConfigResourcesResponse prepareListClientMetricsResourcesResponse(Errors error) {
        return new ListConfigResourcesResponse(new ListConfigResourcesResponseData()
                .setErrorCode(error.code()));
    }

    private static FeatureMetadata defaultFeatureMetadata() {
        return new FeatureMetadata(
            Map.of("test_feature_1", new FinalizedVersionRange((short) 2, (short) 2)),
            Optional.of(1L),
            Map.of("test_feature_1", new SupportedVersionRange((short) 1, (short) 5)));
    }

    private static Features<org.apache.kafka.common.feature.SupportedVersionRange> convertSupportedFeaturesMap(Map<String, SupportedVersionRange> features) {
        final Map<String, org.apache.kafka.common.feature.SupportedVersionRange> featuresMap = new HashMap<>();
        for (final Map.Entry<String, SupportedVersionRange> entry : features.entrySet()) {
            final SupportedVersionRange versionRange = entry.getValue();
            featuresMap.put(
                entry.getKey(),
                new org.apache.kafka.common.feature.SupportedVersionRange(versionRange.minVersion(),
                                                                          versionRange.maxVersion()));
        }

        return Features.supportedFeatures(featuresMap);
    }

    private static ApiVersionsResponse prepareApiVersionsResponseForDescribeFeatures(Errors error) {
        if (error == Errors.NONE) {
            return new ApiVersionsResponse.Builder().
                setApiVersions(ApiVersionsResponse.filterApis(
                    ApiMessageType.ListenerType.BROKER, false, false)).
                setSupportedFeatures(
                    convertSupportedFeaturesMap(defaultFeatureMetadata().supportedFeatures())).
                setFinalizedFeatures(
                    Collections.singletonMap("test_feature_1", (short) 2)).
                setFinalizedFeaturesEpoch(
                    defaultFeatureMetadata().finalizedFeaturesEpoch().get()).
                build();
        }
        return new ApiVersionsResponse(
            new ApiVersionsResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode(error.code()));
    }

    private static QuorumInfo defaultQuorumInfo(boolean emptyOptionals) {
        return new QuorumInfo(1, 1, 1L,
                singletonList(new QuorumInfo.ReplicaState(1,
                        emptyOptionals ? Uuid.ZERO_UUID : REPLICA_DIRECTORY_ID,
                        100,
                        emptyOptionals ? OptionalLong.empty() : OptionalLong.of(1000),
                        emptyOptionals ? OptionalLong.empty() : OptionalLong.of(1000))),
                singletonList(new QuorumInfo.ReplicaState(1,
                        emptyOptionals ? Uuid.ZERO_UUID : REPLICA_DIRECTORY_ID,
                        100,
                        emptyOptionals ? OptionalLong.empty() : OptionalLong.of(1000),
                        emptyOptionals ? OptionalLong.empty() : OptionalLong.of(1000))),
                singletonMap(1, new QuorumInfo.Node(1, Collections.emptyList())));
    }

    private static DescribeQuorumResponse prepareDescribeQuorumResponse(
            Errors topLevelError,
            Errors partitionLevelError,
            Boolean topicCountError,
            Boolean topicNameError,
            Boolean partitionCountError,
            Boolean partitionIndexError,
            Boolean emptyOptionals) {
        String topicName = topicNameError ? "RANDOM" : Topic.CLUSTER_METADATA_TOPIC_NAME;
        int partitionIndex = partitionIndexError ? 1 : Topic.CLUSTER_METADATA_TOPIC_PARTITION.partition();
        List<DescribeQuorumResponseData.TopicData> topics = new ArrayList<>();
        List<DescribeQuorumResponseData.PartitionData> partitions = new ArrayList<>();
        for (int i = 0; i < (partitionCountError ? 2 : 1); i++) {
            DescribeQuorumResponseData.ReplicaState replica = new DescribeQuorumResponseData.ReplicaState()
                    .setReplicaId(1)
                    .setReplicaDirectoryId(emptyOptionals ? Uuid.ZERO_UUID : REPLICA_DIRECTORY_ID)
                    .setLogEndOffset(100);
            replica.setLastFetchTimestamp(emptyOptionals ? -1 : 1000);
            replica.setLastCaughtUpTimestamp(emptyOptionals ? -1 : 1000);
            partitions.add(new DescribeQuorumResponseData.PartitionData().setPartitionIndex(partitionIndex)
                    .setLeaderId(1)
                    .setLeaderEpoch(1)
                    .setHighWatermark(1)
                    .setCurrentVoters(singletonList(replica))
                    .setObservers(singletonList(replica))
                    .setErrorCode(partitionLevelError.code())
                    .setErrorMessage(partitionLevelError.message()));
        }
        for (int i = 0; i < (topicCountError ? 2 : 1); i++) {
            topics.add(new DescribeQuorumResponseData.TopicData().setTopicName(topicName).setPartitions(partitions));
        }
        return new DescribeQuorumResponse(new DescribeQuorumResponseData()
            .setTopics(topics)
            .setErrorCode(topLevelError.code())
            .setErrorMessage(topLevelError.message())
            .setNodes(new DescribeQuorumResponseData.NodeCollection(Collections.singleton(new DescribeQuorumResponseData.Node().setNodeId(1)))));
    }

    @Test
    public void testAdminBootstrapResolutionExceptionPropagated() throws Exception {
        String invalidHost = "unresolvable.invalid:9092";
        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, invalidHost);
        configs.put(CommonClientConfigs.BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG, "3000");

        try (Admin admin = Admin.create(configs)) {
            assertThrows(BootstrapResolutionException.class, () -> {
                long startTime = System.currentTimeMillis();
                long maxWaitTime = 15000;
                while (System.currentTimeMillis() - startTime < maxWaitTime) {
                    try {
                        admin.listTopics().names().get();
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof BootstrapResolutionException) {
                            throw (BootstrapResolutionException) e.getCause();
                        }
                    }
                }
                fail("Expected BootstrapResolutionException to be thrown within " + maxWaitTime + "ms");
            });

            // After the first failure, any further API call must also surface the bootstrap error.
            ExecutionException e = assertThrows(ExecutionException.class,
                () -> admin.listTopics().names().get());
            assertInstanceOf(BootstrapResolutionException.class, e.getCause());
        }
    }
}
