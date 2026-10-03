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

package org.apache.kafka.common.test;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.server.IntegrationTestUtils.connectAndReceive;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("integration")
@Timeout(120)
public class KafkaClusterTestKitTest {
    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    public void testCreateClusterWithBadNumDisksThrows(int disks) {
        IllegalArgumentException e = assertThrowsExactly(IllegalArgumentException.class, () -> new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumDisksPerBroker(disks)
                .setNumControllerNodes(1)
                .build())
        );
        assertEquals("Invalid value for numDisksPerBroker", e.getMessage());
    }

    @Test
    public void testCreateClusterWithBadNumOfControllers() {
        IllegalArgumentException e = assertThrowsExactly(IllegalArgumentException.class, () -> new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(-1)
                .build())
        );
        assertEquals("Invalid negative value for numControllerNodes", e.getMessage());
    }

    @Test
    public void testCreateClusterWithBadNumOfBrokers() {
        IllegalArgumentException e = assertThrowsExactly(IllegalArgumentException.class, () -> new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(-1)
                .setNumControllerNodes(1)
                .build())
        );
        assertEquals("Invalid negative value for numBrokerNodes", e.getMessage());
    }

    @Test
    public void testCreateClusterWithBadPerServerProperties() {
        Map<Integer, Map<String, String>> perServerProperties = new HashMap<>();
        perServerProperties.put(100, Map.of("foo", "foo1"));
        perServerProperties.put(200, Map.of("bar", "bar1"));

        IllegalArgumentException e = assertThrowsExactly(IllegalArgumentException.class, () -> new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .setPerServerProperties(perServerProperties)
                .build())
        );
        assertEquals("Unknown server id 100, 200 in perServerProperties, the existent server ids are 0, 3000", e.getMessage());
    }

    @ParameterizedTest
    @CsvSource({
        "true,1,1,2", /* 1 combined node */
        "true,5,7,2", /* 5 combined nodes + 2 controllers */
        "true,7,5,2", /* 7 combined nodes */
        "false,1,1,2", /* 1 broker + 1 controller */
        "false,5,7,2", /* 5 brokers + 7 controllers */
        "false,7,5,2", /* 7 brokers + 5 controllers */
    })
    public void testCreateClusterFormatAndCloseWithMultipleLogDirs(boolean combined, int numBrokers, int numControllers, int numDisks) throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder().
                setNumBrokerNodes(numBrokers).
                setNumDisksPerBroker(numDisks).
                setCombined(combined).
                setNumControllerNodes(numControllers).build()).build()) {

            TestKitNodes nodes = cluster.nodes();
            assertEquals(numBrokers, nodes.brokerNodes().size());
            assertEquals(numControllers, nodes.controllerNodes().size());

            Set<String> logDirs = new HashSet<>();
            nodes.brokerNodes().forEach((brokerId, node) -> {
                assertEquals(numDisks, node.logDataDirectories().size());
                Set<String> expectedDisks = IntStream.range(0, numDisks)
                        .mapToObj(i -> {
                            if (nodes.isCombined(node.id())) {
                                return String.format("combined_%d_%d", brokerId, i);
                            } else {
                                return String.format("broker_%d_data%d", brokerId, i);
                            }
                        }).collect(Collectors.toSet());
                assertEquals(
                    expectedDisks,
                    node.logDataDirectories().stream()
                        .map(p -> Paths.get(p).getFileName().toString())
                        .collect(Collectors.toSet())
                );
                logDirs.addAll(node.logDataDirectories());
            });

            nodes.controllerNodes().forEach((controllerId, node) -> {
                String expected = nodes.isCombined(node.id()) ? String.format("combined_%d_0", controllerId) : String.format("controller_%d", controllerId);
                assertEquals(expected, Paths.get(node.metadataDirectory()).getFileName().toString());
                logDirs.addAll(node.logDataDirectories());
            });

            cluster.format();
            logDirs.forEach(logDir ->
                assertTrue(Files.exists(Paths.get(logDir, MetaPropertiesEnsemble.META_PROPERTIES_NAME)))
            );
        }
    }

    @Test
    public void testCreateClusterWithSpecificBaseDir() throws Exception {
        Path baseDirectory = TestUtils.tempDirectory().toPath();
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder().
                setBaseDirectory(baseDirectory).
                setNumBrokerNodes(1).
                setCombined(true).
                setNumControllerNodes(1).build()).build()) {
            assertEquals(cluster.nodes().baseDirectory(), baseDirectory.toFile().getAbsolutePath());
            cluster.nodes().controllerNodes().values().forEach(controller ->
                assertTrue(Paths.get(controller.metadataDirectory()).startsWith(baseDirectory)));
            cluster.nodes().brokerNodes().values().forEach(broker ->
                assertTrue(Paths.get(broker.metadataDirectory()).startsWith(baseDirectory)));
        }
    }

    @Test
    public void testExposedFaultHandlers() {
        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(1)
            .setNumControllerNodes(1)
            .build();
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes).build()) {
            assertNotNull(cluster.fatalFaultHandler(), "Fatal fault handler should not be null");
            assertNotNull(cluster.nonFatalFaultHandler(), "Non-fatal fault handler should not be null");
        } catch (Exception e) {
            fail("Failed to initialize cluster", e);
        }
    }

    @Test
    public void testCreateClusterAndClose() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build())
            .build()) {
            cluster.format();
            cluster.startup();
        }
    }

    /**
     * Test a single broker, single controller cluster at the minimum bootstrap level. This tests
     * that we can function without having periodic NoOpRecords written.
     */
    @Test
    public void testSingleControllerSingleBrokerCluster() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setBootstrapMetadataVersion(MetadataVersion.MINIMUM_VERSION)
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
        }
    }

    @Test
    public void testCreateClusterAndRestartBrokerNode() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build())
            .build()) {
            cluster.format();
            cluster.startup();
            var broker = cluster.brokers().values().iterator().next();
            broker.shutdown();
            broker.startup();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateClusterAndRestartControllerNode() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(3)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            var controller = cluster.controllers().values().stream()
                .filter(c -> c.controller().isActive())
                .findFirst()
                .get();
            var port = controller.socketServer().boundPort(
                ListenerName.normalised(controller.config().controllerListeners().head().listener()));

            // shutdown active controller
            controller.shutdown();
            // Rewrite The `listeners` config to avoid controller socket server init using different port
            var config = controller.sharedServer().controllerConfig().props();
            ((Map<String, String>) config).put(SocketServerConfigs.LISTENERS_CONFIG,
                "CONTROLLER://localhost:" + port);
            controller.sharedServer().controllerConfig().updateCurrentConfig(config);

            // restart controller
            controller.startup();
            TestUtils.waitForCondition(() -> cluster.controllers().values().stream()
                .anyMatch(c -> c.controller().isActive()),
                "Timeout waiting for new controller election");
        }
    }

    @Test
    public void testCreateClusterAndWaitForBrokerInRunningState() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build())
            .build()) {
            cluster.format();
            cluster.startup();
            TestUtils.waitForCondition(() -> cluster.brokers().get(0).brokerState() == BrokerState.RUNNING,
                "Broker never made it to RUNNING state.");
            TestUtils.waitForCondition(() -> cluster.raftManagers().get(0).client().leaderAndEpoch().leaderId().isPresent(),
                "RaftManager was not initialized.");
            try (Admin admin = cluster.admin()) {
                assertEquals(cluster.nodes().clusterId(),
                    admin.describeCluster().clusterId().get());
            }
        }
    }

    @Test
    public void testCreateClusterWithAdvertisedPortZero() throws Exception {
        Map<Integer, Map<String, String>> brokerPropertyOverrides = new HashMap<>();
        for (int brokerId = 0; brokerId < 3; brokerId++) {
            Map<String, String> props = new HashMap<>();
            props.put(SocketServerConfigs.LISTENERS_CONFIG, "EXTERNAL://localhost:0");
            props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "EXTERNAL://localhost:0");
            brokerPropertyOverrides.put(brokerId, props);
        }

        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumControllerNodes(1)
            .setNumBrokerNodes(3)
            .setPerServerProperties(brokerPropertyOverrides)
            .build();

        doOnStartedKafkaCluster(nodes, cluster ->
            sendDescribeClusterRequestToBoundPortUntilAllBrokersPropagated(cluster.nodes().brokerListenerName(), Duration.ofSeconds(15), cluster)
                .nodes().values().forEach(broker -> {
                    assertEquals("localhost", broker.host(),
                        "Did not advertise configured advertised host");
                    assertEquals(cluster.brokers().get(broker.id()).socketServer().boundPort(cluster.nodes().brokerListenerName()), broker.port(),
                        "Did not advertise bound socket port");
                })
        );
    }

    @Test
    public void testCreateClusterWithAdvertisedHostAndPortDifferentFromSocketServer() throws Exception {
        var brokerPropertyOverrides = IntStream.range(0, 3).boxed().collect(Collectors.toMap(brokerId -> brokerId, brokerId -> Map.of(
            SocketServerConfigs.LISTENERS_CONFIG, "EXTERNAL://localhost:0",
            SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "EXTERNAL://advertised-host-" + brokerId + ":" + (brokerId + 100)
        )));

        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumControllerNodes(1)
            .setNumBrokerNodes(3)
            .setNumDisksPerBroker(1)
            .setPerServerProperties(brokerPropertyOverrides)
            .build();

        doOnStartedKafkaCluster(nodes, cluster ->
            sendDescribeClusterRequestToBoundPortUntilAllBrokersPropagated(cluster.nodes().brokerListenerName(), Duration.ofSeconds(15), cluster)
                .nodes().values().forEach(broker -> {
                    assertEquals("advertised-host-" + broker.id(), broker.host(), "Did not advertise configured advertised host");
                    assertEquals(broker.id() + 100, broker.port(), "Did not advertise configured advertised port");
                })
        );
    }

    private void doOnStartedKafkaCluster(TestKitNodes nodes, Consumer<KafkaClusterTestKit> action) throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes).build()) {
            cluster.format();
            cluster.startup();
            action.accept(cluster);
        }
    }

    private DescribeClusterResponse sendDescribeClusterRequestToBoundPortUntilAllBrokersPropagated(
        ListenerName listenerName,
        Duration waitTime,
        KafkaClusterTestKit cluster
    ) throws RuntimeException {
        try {
            long startTime = System.currentTimeMillis();
            TestUtils.waitForCondition(() -> cluster.brokers().get(0).brokerState() == BrokerState.RUNNING,
                "Broker never made it to RUNNING state.");
            TestUtils.waitForCondition(() -> cluster.raftManagers().get(0).client().leaderAndEpoch().leaderId().isPresent(),
                "RaftManager was not initialized.");

            Duration remainingWaitTime = waitTime.minus(Duration.ofMillis(System.currentTimeMillis() - startTime));

            final DescribeClusterResponse[] currentResponse = new DescribeClusterResponse[1];
            int expectedBrokerCount = cluster.nodes().brokerNodes().size();
            TestUtils.waitForCondition(
                () -> {
                    currentResponse[0] = connectAndReceive(
                        new DescribeClusterRequest.Builder(new DescribeClusterRequestData()).build(),
                        cluster.brokers().get(0).socketServer().boundPort(listenerName)
                    );
                    return currentResponse[0].nodes().size() == expectedBrokerCount;
                },
                remainingWaitTime.toMillis(),
                String.format("After %s ms Broker is only aware of %s brokers, but %s are expected", remainingWaitTime.toMillis(), expectedBrokerCount, expectedBrokerCount)
            );
            return currentResponse[0];
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testClusterWithLowerCaseListeners() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setBrokerListenerName(new ListenerName("external"))
                .setNumControllerNodes(3)
                .build())
            .build()) {
            cluster.format();
            cluster.startup();
            cluster.brokers().forEach((brokerId, broker) -> {
                assertEquals(List.of("external://localhost:0"), broker.config().get(SocketServerConfigs.LISTENERS_CONFIG));
                assertEquals("external", broker.config().get(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG));
                assertEquals("external:PLAINTEXT,CONTROLLER:PLAINTEXT", broker.config().get(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG));
            });
            TestUtils.waitForCondition(() -> cluster.brokers().get(0).brokerState() == BrokerState.RUNNING,
                "Broker never made it to RUNNING state.");
            TestUtils.waitForCondition(() -> cluster.raftManagers().get(0).client().leaderAndEpoch().leaderId().isPresent(),
                "RaftManager was not initialized.");
            try (Admin admin = cluster.admin()) {
                assertEquals(cluster.nodes().clusterId(),
                    admin.describeCluster().clusterId().get());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testUnregisterController(boolean usingBootstrapControllers) throws Exception {
        final var nodes = new TestKitNodes.Builder().
            setNumBrokerNodes(3).
            setNumControllerNodes(3).
            build();
        final Map<Integer, Uuid> initialVoters = new HashMap<>();
        for (final var controllerNode : nodes.controllerNodes().values()) {
            initialVoters.put(
                controllerNode.id(),
                controllerNode.metadataDirectoryId()
            );
        }

        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes).
            setInitialVoterSet(initialVoters).
            build()
        ) {
            cluster.format();
            cluster.startup();
            int controllerIdToUnregister = cluster.controllers().keySet().iterator().next();
            cluster.controllers().get(controllerIdToUnregister).shutdown();
            cluster.waitForActiveController();

            try (Admin admin = cluster.admin(Map.of(AdminClientConfig.CLIENT_ID_CONFIG, getClass().getName()), usingBootstrapControllers)) {
                // The controller is still part of the voter set, so it can't be unregistered yet
                assertFutureThrows(
                    InvalidRequestException.class,
                    admin.unregisterController(controllerIdToUnregister).all(),
                    "Cannot unregister controller " + controllerIdToUnregister +
                        " because it is part of the voter set."
                );

                admin.removeRaftVoter(
                    controllerIdToUnregister,
                    initialVoters.get(controllerIdToUnregister)
                ).all().get();

                assertDoesNotThrow(() -> admin.unregisterController(controllerIdToUnregister).all().get());
            }

            TestUtils.waitForCondition(() -> !cluster.brokers().get(1).metadataCache().currentImage().cluster().controllers().containsKey(controllerIdToUnregister),
                    "Timed out waiting for controller to be unregistered.");
        }
    }

    @Test
    public void testStartupWithNonDefaultKControllerDynamicConfiguration() throws Exception {
        var bootstrapRecords = List.of(
            new ApiMessageAndVersion(new FeatureLevelRecord()
                .setName(MetadataVersion.FEATURE_NAME)
                .setFeatureLevel(MetadataVersion.IBP_3_7_IV0.featureLevel()), (short) 0),
            new ApiMessageAndVersion(new ConfigRecord()
                .setResourceType(ConfigResource.Type.BROKER.id())
                .setResourceName("")
                .setName("num.io.threads")
                .setValue("9"), (short) 0));
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder(BootstrapMetadata.fromRecords(bootstrapRecords, "testRecords"))
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build())
            .build()) {
            cluster.format();
            cluster.startup();
            var controller = cluster.controllers().values().iterator().next();
            TestUtils.retryOnExceptionWithTimeout(60000, () -> {
                assertNotNull(controller.controllerApisHandlerPool());
                assertEquals(9, controller.controllerApisHandlerPool().threadPoolSize().get());
            });
        }
    }

    /**
     * Test that once a cluster is formatted, a bootstrap.metadata file that contains an unsupported
     * MetadataVersion is not a problem. This is a regression test for KAFKA-19192.
     */
    @Test
    public void testOldBootstrapMetadataFile() throws Exception {
        var baseDirectory = TestUtils.tempDirectory().toPath();
        try (var cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .setBaseDirectory(baseDirectory)
                .build())
            .setDeleteOnClose(false)
            .build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
        }
        var oldBootstrapMetadata = BootstrapMetadata.fromRecords(
            List.of(
                new ApiMessageAndVersion(
                    new FeatureLevelRecord()
                        .setName(MetadataVersion.FEATURE_NAME)
                        .setFeatureLevel((short) 1),
                    (short) 0)
            ),
            "oldBootstrapMetadata");
        // Re-create the cluster using the same directory structure as above.
        // Since we do not need to use the bootstrap metadata, the fact that
        // it specifies an obsolete metadata.version should not be a problem.
        try (var cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .setBaseDirectory(baseDirectory)
                .setBootstrapMetadata(oldBootstrapMetadata)
                .build()).build()) {
            cluster.startup();
            cluster.waitForReadyBrokers();
        }
    }

    @Test
    public void testAuthorizerFailureFoundInControllerStartup() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumControllerNodes(3).build())
            .setConfigProp("authorizer.class.name", BadAuthorizer.class.getName())
            .build()) {
            cluster.format();
            ExecutionException exception = assertThrows(ExecutionException.class,
                cluster::startup);
            assertEquals("java.lang.IllegalStateException: test authorizer exception",
                exception.getMessage());
            cluster.fatalFaultHandler().setIgnore(true);
        }
    }

    public static class BadAuthorizer implements Authorizer {
        // Default constructor needed for reflection object creation
        public BadAuthorizer() {
        }

        @Override
        public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
            throw new IllegalStateException("test authorizer exception");
        }

        @Override
        public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
            return null;
        }

        @Override
        public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext,
            List<AclBinding> aclBindings) {
            return null;
        }

        @Override
        public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext,
            List<AclBindingFilter> aclBindingFilters) {
            return null;
        }

        @Override
        public Iterable<AclBinding> acls(AclBindingFilter filter) {
            return null;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }
}
