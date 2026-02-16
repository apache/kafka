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
package org.apache.kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.SupportedVersionRange;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.server.IntegrationTestUtils.connectAndReceive;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(120)
@Tag("integration")
public class KRaftClusterTest {
    private static final Logger LOG = LoggerFactory.getLogger(KRaftClusterTest.class);
    private static final Logger LOG_2 = LoggerFactory.getLogger(KRaftClusterTest.class.getCanonicalName() + "2");

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
    public void testRemoteLogManagerInstantiation() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build())
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true)
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
                "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
                "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
            .build()) {
            cluster.format();
            cluster.startup();
            cluster.brokers().forEach((brokerId, broker) -> {
                assertFalse(broker.remoteLogManagerOpt().isEmpty(), "RemoteLogManager should be initialized");
            });
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReconfigureControllerClientQuotas(boolean combinedController) throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setCombined(combinedController)
                .setNumControllerNodes(1)
                .build())
            .setConfigProp("client.quota.callback.class", DummyClientQuotaCallback.class.getName())
            .setConfigProp(DummyClientQuotaCallback.DUMMY_CLIENT_QUOTA_CALLBACK_VALUE_CONFIG_KEY, "0")
            .build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            assertConfigValue(cluster, 0);

            try (Admin admin = cluster.admin()) {
                admin.incrementalAlterConfigs(
                    Map.of(new ConfigResource(Type.BROKER, ""),
                        List.of(new AlterConfigOp(
                            new ConfigEntry(DummyClientQuotaCallback.DUMMY_CLIENT_QUOTA_CALLBACK_VALUE_CONFIG_KEY, "1"), OpType.SET))))
                        .all().get();
            }
            assertConfigValue(cluster, 1);
        }
    }

    private void assertConfigValue(KafkaClusterTestKit cluster, int expected) throws InterruptedException {
        TestUtils.retryOnExceptionWithTimeout(60000, () -> {
            Object controllerCallback = cluster.controllers().values().iterator().next()
                .quotaManagers().clientQuotaCallbackPlugin().get().get();
            assertEquals(expected, ((DummyClientQuotaCallback) controllerCallback).value);

            Object brokerCallback = cluster.brokers().values().iterator().next()
                .quotaManagers().clientQuotaCallbackPlugin().get().get();
            assertEquals(expected, ((DummyClientQuotaCallback) brokerCallback).value);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReconfigureControllerAuthorizer(boolean combinedMode) throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setCombined(combinedMode)
                .setNumControllerNodes(1)
                .build())
            .setConfigProp("authorizer.class.name", FakeConfigurableAuthorizer.class.getName())
            .build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            assertFoobarValue(cluster, 0);

            try (Admin admin = cluster.admin()) {
                admin.incrementalAlterConfigs(
                    Map.of(new ConfigResource(Type.BROKER, ""),
                        List.of(new AlterConfigOp(
                            new ConfigEntry(FakeConfigurableAuthorizer.FOOBAR_CONFIG_KEY, "123"), OpType.SET))))
                    .all().get();
            }

            assertFoobarValue(cluster, 123);
        }
    }

    private void assertFoobarValue(KafkaClusterTestKit cluster, int expected) throws InterruptedException {
        TestUtils.retryOnExceptionWithTimeout(60000, () -> {
            Object controllerAuthorizer = cluster.controllers().values().iterator().next()
                .authorizerPlugin().get().get();
            assertEquals(expected, ((FakeConfigurableAuthorizer) controllerAuthorizer).foobar.get());

            Object brokerAuthorizer = cluster.brokers().values().iterator().next()
                .authorizerPlugin().get().get();
            assertEquals(expected, ((FakeConfigurableAuthorizer) brokerAuthorizer).foobar.get());
        });
    }

    @Test
    public void testCreateClusterAndCreateListDeleteTopic() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(3)
                .setNumControllerNodes(3)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            TestUtils.waitForCondition(() -> cluster.brokers().get(0).brokerState() == BrokerState.RUNNING,
                "Broker never made it to RUNNING state.");
            TestUtils.waitForCondition(() -> cluster.raftManagers().get(0).client().leaderAndEpoch().leaderId().isPresent(),
                "RaftManager was not initialized.");

            String testTopic = "test-topic";
            try (Admin admin = cluster.admin()) {
                // Create a test topic
                List<NewTopic> newTopic = List.of(new NewTopic(testTopic, 1, (short) 3));
                CreateTopicsResult createTopicResult = admin.createTopics(newTopic);
                createTopicResult.all().get();
                waitForTopicListing(admin, List.of(testTopic), List.of());

                // Delete topic
                DeleteTopicsResult deleteResult = admin.deleteTopics(List.of(testTopic));
                deleteResult.all().get();

                // List again
                waitForTopicListing(admin, List.of(), List.of(testTopic));
            }
        }
    }

    @Test
    public void testCreateClusterAndCreateAndManyTopics() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(3)
                .setNumControllerNodes(3)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            TestUtils.waitForCondition(() -> cluster.brokers().get(0).brokerState() == BrokerState.RUNNING,
                "Broker never made it to RUNNING state.");
            TestUtils.waitForCondition(() -> cluster.raftManagers().get(0).client().leaderAndEpoch().leaderId().isPresent(),
                "RaftManager was not initialized.");

            try (Admin admin = cluster.admin()) {
                // Create many topics
                List<NewTopic> newTopics = List.of(
                    new NewTopic("test-topic-1", 2, (short) 3),
                    new NewTopic("test-topic-2", 2, (short) 3),
                    new NewTopic("test-topic-3", 2, (short) 3)
                );
                CreateTopicsResult createTopicResult = admin.createTopics(newTopics);
                createTopicResult.all().get();

                // List created topics
                waitForTopicListing(admin, List.of("test-topic-1", "test-topic-2", "test-topic-3"), List.of());
            }
        }
    }

    private Map<ClientQuotaEntity, Map<String, Double>> alterThenDescribe(
        Admin admin,
        ClientQuotaEntity entity,
        List<ClientQuotaAlteration.Op> quotas,
        ClientQuotaFilter filter,
        int expectCount
    ) throws Exception {
        AlterClientQuotasResult alterResult = admin.alterClientQuotas(List.of(new ClientQuotaAlteration(entity, quotas)));
        alterResult.all().get();

        TestUtils.waitForCondition(() -> {
            Map<ClientQuotaEntity, Map<String, Double>> results = admin.describeClientQuotas(filter).entities().get();
            return results.getOrDefault(entity, Map.of()).size() == expectCount;
        }, "Broker never saw new client quotas");

        return admin.describeClientQuotas(filter).entities().get();
    }

    @Test
    public void testClientQuotas() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            TestUtils.waitForCondition(() -> cluster.brokers().get(0).brokerState() == BrokerState.RUNNING,
                "Broker never made it to RUNNING state.");

            try (Admin admin = cluster.admin()) {
                ClientQuotaEntity entity = new ClientQuotaEntity(Map.of("user", "testkit"));
                ClientQuotaFilter filter = ClientQuotaFilter.containsOnly(
                    List.of(ClientQuotaFilterComponent.ofEntity("user", "testkit")));

                Map<ClientQuotaEntity, Map<String, Double>> describeResult = alterThenDescribe(admin, entity,
                    List.of(new ClientQuotaAlteration.Op("request_percentage", 0.99)), filter, 1);
                assertEquals(0.99, describeResult.get(entity).get("request_percentage"), 1e-6);

                describeResult = alterThenDescribe(admin, entity, List.of(
                    new ClientQuotaAlteration.Op("request_percentage", 0.97),
                    new ClientQuotaAlteration.Op("producer_byte_rate", 10000.0),
                    new ClientQuotaAlteration.Op("consumer_byte_rate", 10001.0)
                ), filter, 3);
                assertEquals(0.97, describeResult.get(entity).get("request_percentage"), 1e-6);
                assertEquals(10000.0, describeResult.get(entity).get("producer_byte_rate"), 1e-6);
                assertEquals(10001.0, describeResult.get(entity).get("consumer_byte_rate"), 1e-6);

                describeResult = alterThenDescribe(admin, entity, List.of(
                    new ClientQuotaAlteration.Op("request_percentage", 0.95),
                    new ClientQuotaAlteration.Op("producer_byte_rate", null),
                    new ClientQuotaAlteration.Op("consumer_byte_rate", null)
                ), filter, 1);
                assertEquals(0.95, describeResult.get(entity).get("request_percentage"), 1e-6);

                alterThenDescribe(admin, entity, List.of(
                    new ClientQuotaAlteration.Op("request_percentage", null)), filter, 0);

                describeResult = alterThenDescribe(admin, entity,
                    List.of(new ClientQuotaAlteration.Op("producer_byte_rate", 9999.0)), filter, 1);
                assertEquals(9999.0, describeResult.get(entity).get("producer_byte_rate"), 1e-6);

                ClientQuotaEntity entity2 = new ClientQuotaEntity(Map.of("user", "testkit", "client-id", "some-client"));
                filter = ClientQuotaFilter.containsOnly(
                    List.of(
                        ClientQuotaFilterComponent.ofEntity("user", "testkit"),
                        ClientQuotaFilterComponent.ofEntity("client-id", "some-client")
                    ));
                describeResult = alterThenDescribe(admin, entity2,
                    List.of(new ClientQuotaAlteration.Op("producer_byte_rate", 9998.0)), filter, 1);
                assertEquals(9998.0, describeResult.get(entity2).get("producer_byte_rate"), 1e-6);

                final ClientQuotaFilter finalFilter = ClientQuotaFilter.contains(
                    List.of(ClientQuotaFilterComponent.ofEntity("user", "testkit")));

                TestUtils.waitForCondition(() -> {
                    Map<ClientQuotaEntity, Map<String, Double>> results = admin.describeClientQuotas(finalFilter).entities().get();
                    if (results.size() != 2) {
                        return false;
                    }
                    assertEquals(9999.0, results.get(entity).get("producer_byte_rate"), 1e-6);
                    assertEquals(9998.0, results.get(entity2).get("producer_byte_rate"), 1e-6);
                    return true;
                }, "Broker did not see two client quotas");
            }
        }
    }

    private void setConsumerByteRate(Admin admin, ClientQuotaEntity entity, Long value) throws Exception {
        admin.alterClientQuotas(List.of(
            new ClientQuotaAlteration(entity, List.of(
                new ClientQuotaAlteration.Op("consumer_byte_rate", value.doubleValue())))
        )).all().get();
    }

    private Map<ClientQuotaEntity, Long> getConsumerByteRates(Admin admin) throws Exception {
        return admin.describeClientQuotas(ClientQuotaFilter.contains(List.of()))
            .entities().get()
            .entrySet().stream()
            .filter(entry -> entry.getValue().containsKey("consumer_byte_rate"))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().get("consumer_byte_rate").longValue()
            ));
    }

    @Test
    public void testDefaultClientQuotas() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            TestUtils.waitForCondition(() -> cluster.brokers().get(0).brokerState() == BrokerState.RUNNING,
                "Broker never made it to RUNNING state.");

            try (Admin admin = cluster.admin()) {
                ClientQuotaEntity defaultUser = new ClientQuotaEntity(Collections.singletonMap("user", null));
                ClientQuotaEntity bobUser = new ClientQuotaEntity(Map.of("user", "bob"));

                TestUtils.waitForCondition(
                    () -> getConsumerByteRates(admin).isEmpty(),
                    "Initial consumer byte rates should be empty");

                setConsumerByteRate(admin, defaultUser, 100L);
                TestUtils.waitForCondition(() -> {
                    Map<ClientQuotaEntity, Long> rates = getConsumerByteRates(admin);
                    return rates.size() == 1 &&
                        rates.get(defaultUser) == 100L;
                }, "Default user rate should be 100");

                setConsumerByteRate(admin, bobUser, 1000L);
                TestUtils.waitForCondition(() -> {
                    Map<ClientQuotaEntity, Long> rates = getConsumerByteRates(admin);
                    return rates.size() == 2 &&
                        rates.get(defaultUser) == 100L &&
                        rates.get(bobUser) == 1000L;
                }, "Should have both default and bob user rates");
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

    private void waitForTopicListing(Admin admin, List<String> expectedPresent, List<String> expectedAbsent)
        throws InterruptedException {
        Set<String> topicsNotFound = new HashSet<>(expectedPresent);
        Set<String> extraTopics = new HashSet<>();
        TestUtils.waitForCondition(() -> {
            Set<String> topicNames = admin.listTopics().names().get();
            topicsNotFound.removeAll(topicNames);
            extraTopics.clear();
            extraTopics.addAll(topicNames.stream().filter(expectedAbsent::contains).collect(Collectors.toSet()));
            return topicsNotFound.isEmpty() && extraTopics.isEmpty();
        }, String.format("Failed to find topic(s): %s and NOT find topic(s): %s", topicsNotFound, extraTopics));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testUnregisterBroker(boolean usingBootstrapControllers) throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(3)
                .setNumControllerNodes(3)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            TestUtils.waitForCondition(() -> brokerIsUnfenced(clusterImage(cluster, 1), 0),
                "Timed out waiting for broker 0 to be unfenced.");
            cluster.brokers().get(0).shutdown();
            TestUtils.waitForCondition(() -> !brokerIsUnfenced(clusterImage(cluster, 1), 0),
                "Timed out waiting for broker 0 to be fenced.");

            try (Admin admin = createAdminClient(cluster, usingBootstrapControllers)) {
                admin.unregisterBroker(0);
            }

            TestUtils.waitForCondition(() -> brokerIsAbsent(clusterImage(cluster, 1), 0),
                "Timed out waiting for broker 0 to be fenced.");
        }
    }

    private ClusterImage clusterImage(KafkaClusterTestKit cluster, int brokerId) {
        return cluster.brokers().get(brokerId).metadataCache().currentImage().cluster();
    }

    private boolean brokerIsUnfenced(ClusterImage image, int brokerId) {
        BrokerRegistration registration = image.brokers().get(brokerId);
        if (registration == null) {
            return false;
        }
        return !registration.fenced();
    }

    private boolean brokerIsAbsent(ClusterImage image, int brokerId) {
        return !image.brokers().containsKey(brokerId);
    }

    private Admin createAdminClient(KafkaClusterTestKit cluster, boolean usingBootstrapControllers) {
        return cluster.admin(Map.of(AdminClientConfig.CLIENT_ID_CONFIG, this.getClass().getName()), usingBootstrapControllers);
    }

    @Test
    public void testCreateClusterAndPerformReassignment() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(4)
                .setNumControllerNodes(3)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            try (Admin admin = cluster.admin()) {
                // Create the topic.
                Map<Integer, List<Integer>> assignments = Map.of(
                    0, List.of(0, 1, 2),
                    1, List.of(1, 2, 3),
                    2, List.of(2, 3, 0),
                    3, List.of(3, 2, 1)
                );

                CreateTopicsResult createTopicResult = admin.createTopics(List.of(
                    new NewTopic("foo", assignments)));
                createTopicResult.all().get();
                waitForTopicListing(admin, List.of("foo"), List.of());

                // Start some reassignments.
                assertEquals(Map.of(), admin.listPartitionReassignments().reassignments().get());
                Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = Map.of(
                    new TopicPartition("foo", 0), Optional.of(new NewPartitionReassignment(List.of(2, 1, 0))),
                    new TopicPartition("foo", 1), Optional.of(new NewPartitionReassignment(List.of(0, 1, 2))),
                    new TopicPartition("foo", 2), Optional.of(new NewPartitionReassignment(List.of(2, 3))),
                    new TopicPartition("foo", 3), Optional.of(new NewPartitionReassignment(List.of(3, 2, 0, 1)))
                );
                admin.alterPartitionReassignments(reassignments).all().get();
                TestUtils.waitForCondition(
                    () -> admin.listPartitionReassignments().reassignments().get().isEmpty(),
                    "The reassignment never completed."
                );
                AtomicReference<List<List<Integer>>> currentMapping = new AtomicReference<>(List.of());
                List<List<Integer>> expectedMapping = List.of(
                    List.of(2, 1, 0),
                    List.of(0, 1, 2),
                    List.of(2, 3),
                    List.of(3, 2, 0, 1)
                );
                TestUtils.waitForCondition(() -> {
                    Map<String, TopicDescription> topicInfoMap = admin.describeTopics(Set.of("foo")).allTopicNames().get();
                    if (topicInfoMap.containsKey("foo")) {
                        currentMapping.set(translatePartitionInfoToSeq(topicInfoMap.get("foo").partitions()));
                        return expectedMapping.equals(currentMapping.get());
                    } else {
                        return false;
                    }
                }, () -> "Timed out waiting for replica assignments for topic foo. " +
                    "Wanted: " + expectedMapping + ". Got: " + currentMapping.get());

                TestUtils.retryOnExceptionWithTimeout(60000, () -> checkReplicaManager(
                    cluster,
                    Map.of(
                        0, List.of(true, true, false, true),
                        1, List.of(true, true, false, true),
                        2, List.of(true, true, true, true),
                        3, List.of(false, false, true, true)
                    )
                ));
            }
        }
    }

    private void checkReplicaManager(KafkaClusterTestKit cluster, Map<Integer, List<Boolean>> expectedHosting) {
        for (Map.Entry<Integer, List<Boolean>> entry : expectedHosting.entrySet()) {
            int brokerId = entry.getKey();
            List<Boolean> partitionsIsHosted = entry.getValue();
            var broker = cluster.brokers().get(brokerId);

            for (int partitionId = 0; partitionId < partitionsIsHosted.size(); partitionId++) {
                boolean isHosted = partitionsIsHosted.get(partitionId);
                TopicPartition topicPartition = new TopicPartition("foo", partitionId);
                var partition = broker.replicaManager().getPartition(topicPartition);
                if (isHosted) {
                    assertNotEquals(kafka.server.HostedPartition.None$.MODULE$, partition, "topicPartition = " + topicPartition);
                } else {
                    assertEquals(kafka.server.HostedPartition.None$.MODULE$, partition, "topicPartition = " + topicPartition);
                }
            }
        }
    }

    private List<List<Integer>> translatePartitionInfoToSeq(List<TopicPartitionInfo> partitions) {
        return partitions.stream()
            .map(partition -> partition.replicas().stream()
                .map(Node::id)
                .collect(Collectors.toList()))
            .collect(Collectors.toList());
    }

    @Test
    public void testIncrementalAlterConfigs() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(3)
                .setNumControllerNodes(3)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            try (Admin admin = cluster.admin()) {
                Map<ConfigResource, Collection<AlterConfigOp>> brokerConfigs = Map.of(
                    new ConfigResource(Type.BROKER, ""),
                    List.of(
                        new AlterConfigOp(new ConfigEntry("log.roll.ms", "1234567"), AlterConfigOp.OpType.SET),
                        new AlterConfigOp(new ConfigEntry("max.connections.per.ip", "60"), AlterConfigOp.OpType.SET)
                    )
                );
                assertEquals(List.of(ApiError.NONE), incrementalAlter(admin, brokerConfigs));

                validateConfigs(admin, Map.of(
                    new ConfigResource(Type.BROKER, ""), Map.of(
                        "log.roll.ms", "1234567",
                        "max.connections.per.ip", "60",
                        "min.insync.replicas", "1"
                    )), true);

                admin.createTopics(List.of(
                    new NewTopic("foo", 2, (short) 3),
                    new NewTopic("bar", 2, (short) 3)
                )).all().get();
                waitForAllPartitions(cluster, "foo", 2);
                waitForAllPartitions(cluster, "bar", 2);

                validateConfigs(admin, Map.of(
                    new ConfigResource(Type.TOPIC, "bar"), Map.of()
                ), false);

                assertListEquals(List.of(ApiError.NONE,
                    new ApiError(Errors.INVALID_CONFIG, "Unknown topic config name: not.a.real.topic.config"),
                    new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The topic 'baz' does not exist.")),
                    incrementalAlter(admin, Map.of(
                        new ConfigResource(Type.TOPIC, "foo"),
                        List.of(new AlterConfigOp(new ConfigEntry("segment.jitter.ms", "345"), AlterConfigOp.OpType.SET)),
                        new ConfigResource(Type.TOPIC, "bar"),
                        List.of(new AlterConfigOp(new ConfigEntry("not.a.real.topic.config", "789"), AlterConfigOp.OpType.SET)),
                        new ConfigResource(Type.TOPIC, "baz"),
                        List.of(new AlterConfigOp(new ConfigEntry("segment.jitter.ms", "678"), AlterConfigOp.OpType.SET))
                    )));

                validateConfigs(admin, Map.of(
                    new ConfigResource(Type.TOPIC, "foo"), Map.of("segment.jitter.ms", "345")
                ), false);
                assertEquals(List.of(ApiError.NONE), incrementalAlter(admin, Map.of(
                    new ConfigResource(Type.BROKER, "2"),
                    List.of(new AlterConfigOp(new ConfigEntry("max.connections.per.ip", "7"), AlterConfigOp.OpType.SET))
                )));
                validateConfigs(admin, Map.of(
                    new ConfigResource(Type.BROKER, "2"), Map.of("max.connections.per.ip", "7")
                ), false);
            }
        }
    }

    private void waitForAllPartitions(KafkaClusterTestKit cluster, String topic, int expectedNumPartitions)
        throws InterruptedException {
        TestUtils.waitForCondition(() -> cluster.brokers().values().stream().allMatch(broker -> {
            Optional<Integer> numPartitionsOpt = broker.metadataCache().numPartitions(topic);
            if (expectedNumPartitions == 0) {
                return numPartitionsOpt.isEmpty();
            } else {
                return numPartitionsOpt.isPresent() && numPartitionsOpt.get() == expectedNumPartitions;
            }
        }), 60000L, "Topic [" + topic + "] metadata not propagated after 60000 ms");
    }

    private List<ApiError> incrementalAlter(Admin admin, Map<ConfigResource, Collection<AlterConfigOp>> changes) {
        Map<ConfigResource, KafkaFuture<Void>> values = admin.incrementalAlterConfigs(changes).values();
        return changes.keySet().stream().map(resource -> {
            try {
                values.get(resource).get();
                return ApiError.NONE;
            } catch (Throwable t) {
                return ApiError.fromThrowable(t);
            }
        }).collect(Collectors.toList());
    }

    private Map<ConfigResource, Map<String, String>> validateConfigs(
        Admin admin,
        Map<ConfigResource, Map<String, String>> expected,
        boolean exhaustive
    ) throws Exception {
        Map<ConfigResource, Map<String, String>> results = new HashMap<>();
        TestUtils.retryOnExceptionWithTimeout(60000, () -> {
            var values = admin.describeConfigs(expected.keySet()).values();
            results.clear();
            assertEquals(expected.keySet(), values.keySet());
            for (Map.Entry<ConfigResource, Map<String, String>> entry : expected.entrySet()) {
                ConfigResource resource = entry.getKey();
                Map<String, String> expectedPairs = entry.getValue();
                var config = values.get(resource).get();
                Map<String, String> actualMap = new TreeMap<>();
                Map<String, String> expectedMap = new TreeMap<>();
                config.entries().forEach(configEntry -> {
                    actualMap.put(configEntry.name(), configEntry.value());
                    if (!exhaustive) {
                        expectedMap.put(configEntry.name(), configEntry.value());
                    }
                });
                expectedMap.putAll(expectedPairs);
                assertEquals(expectedMap, actualMap);
                results.put(resource, actualMap);
            }
        });
        return results;
    }

    @Test
    public void testSetLog4jConfigurations() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(3)
                .setNumControllerNodes(3)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            try (Admin admin = cluster.admin()) {
                LOG.debug("setting log4j");
                LOG_2.debug("setting log4j");

                ConfigResource broker1 = new ConfigResource(Type.BROKER_LOGGER, "1");
                ConfigResource broker2 = new ConfigResource(Type.BROKER_LOGGER, "2");
                var initialLog4j = validateConfigs(admin, Map.of(broker1, Map.of()), false);

                assertListEquals(List.of(ApiError.NONE,
                    new ApiError(Errors.INVALID_REQUEST, "APPEND operation is not allowed for the BROKER_LOGGER resource")),
                    incrementalAlter(admin, Map.of(
                        broker1, List.of(
                            new AlterConfigOp(new ConfigEntry(LOG.getName(), "TRACE"), OpType.SET),
                            new AlterConfigOp(new ConfigEntry(LOG_2.getName(), "TRACE"), OpType.SET)),
                        broker2, List.of(
                            new AlterConfigOp(new ConfigEntry(LOG.getName(), "TRACE"), OpType.APPEND),
                            new AlterConfigOp(new ConfigEntry(LOG_2.getName(), "TRACE"), OpType.APPEND)))
                    )
                );

                validateConfigs(admin, Map.of(
                    broker1, Map.of(
                        LOG.getName(), "TRACE",
                        LOG_2.getName(), "TRACE"
                    )
                ), false);

                assertListEquals(List.of(ApiError.NONE,
                    new ApiError(Errors.INVALID_REQUEST, "SUBTRACT operation is not allowed for the BROKER_LOGGER resource")),
                    incrementalAlter(admin, Map.of(
                        broker1, List.of(
                            new AlterConfigOp(new ConfigEntry(LOG.getName(), ""), OpType.DELETE),
                            new AlterConfigOp(new ConfigEntry(LOG_2.getName(), ""), OpType.DELETE)),
                        broker2, List.of(
                            new AlterConfigOp(new ConfigEntry(LOG.getName(), "TRACE"), OpType.SUBTRACT),
                            new AlterConfigOp(new ConfigEntry(LOG_2.getName(), "TRACE"), OpType.SUBTRACT)))
                    )
                );

                validateConfigs(admin, Map.of(
                    broker1, Map.of(
                        LOG.getName(), initialLog4j.get(broker1).get(LOG.getName()),
                        LOG_2.getName(), initialLog4j.get(broker1).get(LOG_2.getName())
                    )
                ), false);
            }
        }
    }

    private void assertListEquals(List<ApiError> expected, List<ApiError> actual) {
        for (ApiError expectedError : expected) {
            if (!actual.contains(expectedError)) {
                fail("Failed to find expected error " + expectedError);
            }
        }
        for (ApiError actualError : actual) {
            if (!expected.contains(actualError)) {
                fail("Found unexpected error " + actualError);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"3.7-IV0", "3.7-IV2"})
    public void testCreatePartitions(String metadataVersionString) throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(3)
                .setBootstrapMetadataVersion(MetadataVersion.fromVersionString(metadataVersionString, true))
                .setNumControllerNodes(3)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            try (Admin admin = cluster.admin()) {
                Map<String, KafkaFuture<Void>> createResults = admin.createTopics(List.of(
                    new NewTopic("foo", 1, (short) 3),
                    new NewTopic("bar", 2, (short) 3)
                )).values();
                createResults.get("foo").get();
                createResults.get("bar").get();
                Map<String, KafkaFuture<Void>> increaseResults = admin.createPartitions(Map.of(
                    "foo", NewPartitions.increaseTo(3),
                    "bar", NewPartitions.increaseTo(2)
                )).values();

                increaseResults.get("foo").get();
                ExecutionException exception = assertThrows(ExecutionException.class, () -> increaseResults.get("bar").get());
                assertEquals(InvalidPartitionsException.class, exception.getCause().getClass());
            }
        }
    }

    @Test
    public void testDescribeQuorumRequestToBrokers() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(3)
                .setNumControllerNodes(3)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            try (Admin admin = createAdminClient(cluster, false)) {
                QuorumInfo quorumInfo = admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions()).quorumInfo().get();

                Set<Integer> controllerIds = cluster.controllers().keySet();
                Set<Integer> voterIds = quorumInfo.voters().stream()
                    .map(QuorumInfo.ReplicaState::replicaId)
                    .collect(Collectors.toSet());
                assertEquals(controllerIds, voterIds);
                assertTrue(controllerIds.contains(quorumInfo.leaderId()),
                    "Leader ID " + quorumInfo.leaderId() + " was not a controller ID.");

                AtomicReference<List<QuorumInfo.ReplicaState>> currentVotersRef = new AtomicReference<>();
                TestUtils.waitForCondition(() -> {
                    try {
                        List<QuorumInfo.ReplicaState> voters = admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions())
                            .quorumInfo().get().voters();
                        currentVotersRef.set(voters);
                        return voters.stream().allMatch(voter ->
                            voter.logEndOffset() > 0
                                && voter.lastFetchTimestamp().isPresent()
                                && voter.lastCaughtUpTimestamp().isPresent()
                        );
                    } catch (Exception e) {
                        return false;
                    }
                }, () -> "At least one voter did not return the expected state within timeout. " +
                    "The responses gathered for all the voters: " + currentVotersRef.get());

                AtomicReference<List<QuorumInfo.ReplicaState>> currentObserversRef = new AtomicReference<>();
                TestUtils.waitForCondition(() -> {
                    try {
                        List<QuorumInfo.ReplicaState> observers = admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions())
                            .quorumInfo().get().observers();
                        currentObserversRef.set(observers);
                        Set<Integer> brokerIds = cluster.brokers().keySet();
                        Set<Integer> observerIds = observers.stream()
                            .map(QuorumInfo.ReplicaState::replicaId)
                            .collect(Collectors.toSet());

                        boolean idsMatch = brokerIds.equals(observerIds);
                        boolean stateValid = observers.stream().allMatch(observer ->
                            observer.logEndOffset() > 0
                                && observer.lastFetchTimestamp().isPresent()
                                && observer.lastCaughtUpTimestamp().isPresent()
                        );
                        return idsMatch && stateValid;
                    } catch (Exception e) {
                        return false;
                    }
                }, () -> "At least one observer did not return the expected state within timeout. " +
                    "The responses gathered for all the observers: " + currentObserversRef.get());
            }
        }
    }

    @Test
    public void testDescribeQuorumRequestToControllers() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(3)
                .setNumControllerNodes(3)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            try (Admin admin = createAdminClient(cluster, true)) {
                QuorumInfo quorumInfo = admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions()).quorumInfo().get();

                Set<Integer> controllerIds = cluster.controllers().keySet();
                Set<Integer> voterIds = quorumInfo.voters().stream()
                    .map(QuorumInfo.ReplicaState::replicaId)
                    .collect(Collectors.toSet());

                assertEquals(controllerIds, voterIds);
                assertTrue(controllerIds.contains(quorumInfo.leaderId()),
                    "Leader ID " + quorumInfo.leaderId() + " was not a controller ID.");

                // Try to bring down the raft client in the active controller node to force the leader election.
                // Stop raft client but not the controller, because we would like to get NOT_LEADER_OR_FOLLOWER error first.
                // If the controller is shutdown, the client can't send request to the original leader.
                cluster.controllers().get(quorumInfo.leaderId()).sharedServer().raftManager().client().shutdown(1000);
                // Send another describe metadata quorum request, it'll get NOT_LEADER_OR_FOLLOWER error first and then re-retrieve the metadata update
                // and send to the correct active controller.
                KafkaFuture<QuorumInfo> quorumInfo2Future = admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions()).quorumInfo();
                // If raft client finishes shutdown before returning NOT_LEADER_OR_FOLLOWER error, the request will not be handled.
                // This makes test fail. Shutdown the controller to make sure the request is handled by another controller.
                cluster.controllers().get(quorumInfo.leaderId()).shutdown();
                QuorumInfo quorumInfo2 = quorumInfo2Future.get();
                // Make sure the leader has changed
                assertTrue(quorumInfo.leaderId() != quorumInfo2.leaderId());

                assertEquals(controllerIds, voterIds);
                assertTrue(controllerIds.contains(quorumInfo.leaderId()),
                    "Leader ID " + quorumInfo.leaderId() + " was not a controller ID.");
            }
        }
    }

    @Test
    public void testUpdateMetadataVersion() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setBootstrapMetadataVersion(MetadataVersion.MINIMUM_VERSION)
                .setNumBrokerNodes(3)
                .setNumControllerNodes(3)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            try (Admin admin = cluster.admin()) {
                admin.updateFeatures(
                    Map.of(MetadataVersion.FEATURE_NAME,
                        new FeatureUpdate(MetadataVersion.latestTesting().featureLevel(), FeatureUpdate.UpgradeType.UPGRADE))
                );
                assertEquals(new SupportedVersionRange((short) 0, (short) 1), admin.describeFeatures().featureMetadata().get()
                    .supportedFeatures().get(KRaftVersion.FEATURE_NAME));
            }
            TestUtils.waitForCondition(() -> cluster.brokers().get(0).metadataCache().currentImage().features().metadataVersion()
                .equals(Optional.of(MetadataVersion.latestTesting())), "Timed out waiting for metadata.version update");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDescribeKRaftVersion(boolean usingBootstrapControllers) throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build())
            .setStandalone(true)
            .build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            try (Admin admin = createAdminClient(cluster, usingBootstrapControllers)) {
                FeatureMetadata featureMetadata = admin.describeFeatures().featureMetadata().get();
                assertEquals(new SupportedVersionRange((short) 0, (short) 1),
                    featureMetadata.supportedFeatures().get(KRaftVersion.FEATURE_NAME));
                assertEquals(new FinalizedVersionRange((short) 1, (short) 1),
                    featureMetadata.finalizedFeatures().get(KRaftVersion.FEATURE_NAME));
            }
        }
    }

    @Test
    public void testCreateClusterAndCreateTopicWithRemoteLogManagerInstantiation() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build())
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
                "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
                "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
            .build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            TestUtils.waitForCondition(() -> cluster.brokers().get(0).brokerState() == BrokerState.RUNNING,
                "Broker never made it to RUNNING state.");
            TestUtils.waitForCondition(() -> cluster.raftManagers().get(0).client().leaderAndEpoch().leaderId().isPresent(),
                "RaftManager was not initialized.");

            try (Admin admin = cluster.admin()) {
                // Create a test topic
                List<NewTopic> newTopic = List.of(new NewTopic("test-topic", 1, (short) 1));
                CreateTopicsResult createTopicResult = admin.createTopics(newTopic);
                createTopicResult.all().get();
                waitForTopicListing(admin, List.of("test-topic"), List.of());

                // Delete topic
                DeleteTopicsResult deleteResult = admin.deleteTopics(List.of("test-topic"));
                deleteResult.all().get();

                // List again
                waitForTopicListing(admin, List.of(), List.of("test-topic"));
            }
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
    public void testSnapshotCount() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(0)
                .setNumControllerNodes(1)
                .build())
            .setConfigProp("metadata.log.max.snapshot.interval.ms", "500")
            .setConfigProp("metadata.max.idle.interval.ms", "50") // Set this low to generate metadata
            .build()) {
            cluster.format();
            cluster.startup();
            var metaLog = FileSystems.getDefault().getPath(
                cluster.controllers().get(3000).config().metadataLogDir(),
                "__cluster_metadata-0");
            TestUtils.waitForCondition(() -> {
                var files = metaLog.toFile().listFiles((dir, name) ->
                    name.toLowerCase(Locale.ROOT).endsWith("checkpoint")
                );
                return files != null && files.length > 0;
            }, "Failed to see at least one snapshot");
            Thread.sleep(500 * 10); // Sleep for 10 snapshot intervals
            var filesAfterTenIntervals = metaLog.toFile().listFiles((dir, name) ->
                name.toLowerCase(Locale.ROOT).endsWith("checkpoint")
            );
            int countAfterTenIntervals = filesAfterTenIntervals != null ? filesAfterTenIntervals.length : 0;
            assertTrue(countAfterTenIntervals > 1,
                "Expected to see at least one more snapshot, saw " + countAfterTenIntervals);
            assertTrue(countAfterTenIntervals < 20,
                "Did not expect to see more than twice as many snapshots as snapshot intervals, saw " + countAfterTenIntervals);
            TestUtils.waitForCondition(() -> {
                var emitterMetrics = cluster.controllers().values().iterator().next()
                    .sharedServer().snapshotEmitter().metrics();
                return emitterMetrics.latestSnapshotGeneratedBytes() > 0;
            }, "Failed to see latestSnapshotGeneratedBytes > 0");
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
    public void testOverlyLargeCreateTopics() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build()).build()) {
            cluster.format();
            cluster.startup();
            try (Admin admin = cluster.admin()) {
                var newTopics = new ArrayList<NewTopic>();
                for (int i = 0; i <= 10000; i++) {
                    newTopics.add(new NewTopic("foo" + i, 100000, (short) 1));
                }
                var executionException = assertThrows(ExecutionException.class,
                    () -> admin.createTopics(newTopics).all().get());
                assertNotNull(executionException.getCause());
                assertEquals(PolicyViolationException.class, executionException.getCause().getClass());
                assertEquals("Excessively large number of partitions per request.",
                    executionException.getCause().getMessage());
            }
        }
    }

    @Test
    public void testTimedOutHeartbeats() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(3)
                .setNumControllerNodes(1)
                .build())
            .setConfigProp(KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG, "10")
            .setConfigProp(KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG, "1000")
            .build()) {
            cluster.format();
            cluster.startup();
            var controller = cluster.controllers().values().iterator().next();
            controller.controller().waitForReadyBrokers(3).get();
            TestUtils.retryOnExceptionWithTimeout(60000, () -> {
                var latch = pause((QuorumController) controller.controller());
                Thread.sleep(1001);
                latch.countDown();
                assertEquals(0, controller.sharedServer().controllerServerMetrics().fencedBrokerCount());
                assertTrue(controller.quorumControllerMetrics().timedOutHeartbeats() > 0,
                    "Expected timedOutHeartbeats to be greater than 0.");
            });
        }
    }

    // Duplicate method to decouple the dependency on the metadata module.
    private CountDownLatch pause(QuorumController controller) {
        final CountDownLatch latch = new CountDownLatch(1);
        controller.appendControlEvent("pause", () -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                LOG.info("Interrupted while waiting for unpause.", e);
            }
        });
        return latch;
    }

    @Test
    public void testRegisteredControllerEndpoints() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(3)
                .build())
            .build()) {
            cluster.format();
            cluster.startup();
            TestUtils.retryOnExceptionWithTimeout(60000, () -> {
                var controller = cluster.controllers().values().iterator().next();
                var registeredControllers = controller.registrationsPublisher().controllers();
                assertEquals(3, registeredControllers.size(), "Expected 3 controller registrations");
                registeredControllers.values().forEach(registration -> {
                    assertNotNull(registration.listeners().get("CONTROLLER"));
                    assertNotEquals(0, registration.listeners().get("CONTROLLER").port());
                });
            });
        }
    }

    @Test
    public void testDirectToControllerCommunicationFailsOnOlderMetadataVersion() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setBootstrapMetadataVersion(MetadataVersion.IBP_3_6_IV2)
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .build())
            .build()) {
            cluster.format();
            cluster.startup();
            try (Admin admin = cluster.admin(Map.of(), true)) {
                var exception = assertThrows(ExecutionException.class,
                    () -> admin.describeCluster().clusterId().get(1, TimeUnit.MINUTES));
                assertNotNull(exception.getCause());
                assertEquals(UnsupportedVersionException.class, exception.getCause().getClass());
            }
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

    @Test
    public void testTopicDeletedAndRecreatedWhileBrokerIsDown() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setBootstrapMetadataVersion(MetadataVersion.IBP_3_6_IV2)
                .setNumBrokerNodes(3)
                .setNumControllerNodes(1)
                .build())
            .build()) {
            cluster.format();
            cluster.startup();
            try (Admin admin = cluster.admin()) {
                var broker0 = cluster.brokers().get(0);
                var broker1 = cluster.brokers().get(1);
                var foo0 = new TopicPartition("foo", 0);

                admin.createTopics(List.of(
                    new NewTopic("foo", 3, (short) 3))).all().get();

                // Wait until foo-0 is created on broker0.
                TestUtils.retryOnExceptionWithTimeout(60000, () -> {
                    assertTrue(broker0.logManager().getLog(foo0, false).isDefined());
                });

                // Shut down broker0 and wait until the ISR of foo-0 is set to [1, 2]
                broker0.shutdown();
                TestUtils.retryOnExceptionWithTimeout(60000, () -> {
                    var info = broker1.metadataCache().getLeaderAndIsr("foo", 0);
                    assertTrue(info.isPresent());
                    assertEquals(Set.of(1, 2), new HashSet<>(info.get().isr()));
                });

                // Modify foo-0 so that it has the wrong topic ID.
                var logDir = broker0.logManager().getLog(foo0, false).get().dir();
                var partitionMetadataFile = new File(logDir, "partition.metadata");
                Files.write(partitionMetadataFile.toPath(),
                    "version: 0\ntopic_id: AAAAAAAAAAAAA7SrBWaJ7g\n".getBytes(StandardCharsets.UTF_8));

                // Start up broker0 and wait until the ISR of foo-0 is set to [0, 1, 2]
                broker0.startup();
                TestUtils.retryOnExceptionWithTimeout(60000, () -> {
                    var info = broker1.metadataCache().getLeaderAndIsr("foo", 0);
                    assertTrue(info.isPresent());
                    assertEquals(Set.of(0, 1, 2), new HashSet<>(info.get().isr()));
                });
            }
        }
    }

    @Test
    public void testAbandonedFutureReplicaRecovered_mainReplicaInOfflineLogDir() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setBootstrapMetadataVersion(MetadataVersion.IBP_3_7_IV2)
                .setNumBrokerNodes(3)
                .setNumDisksPerBroker(2)
                .setNumControllerNodes(1)
                .build())
            .build()) {
            cluster.format();
            cluster.startup();
            try (Admin admin = cluster.admin()) {
                var broker0 = cluster.brokers().get(0);
                var broker1 = cluster.brokers().get(1);
                var foo0 = new TopicPartition("foo", 0);

                admin.createTopics(List.of(
                    new NewTopic("foo", 3, (short) 3))).all().get();

                // Wait until foo-0 is created on broker0.
                TestUtils.retryOnExceptionWithTimeout(60000, () -> 
                    assertTrue(broker0.logManager().getLog(foo0, false).isDefined()));

                // Shut down broker0 and wait until the ISR of foo-0 is set to [1, 2]
                broker0.shutdown();
                TestUtils.retryOnExceptionWithTimeout(60000, () -> {
                    var info = broker1.metadataCache().getLeaderAndIsr("foo", 0);
                    assertTrue(info.isPresent());
                    assertEquals(Set.of(1, 2), new HashSet<>(info.get().isr()));
                });

                // Modify foo-0 so that it refers to a future replica.
                // This is equivalent to a failure during the promotion of the future replica and a restart with directory for
                // the main replica being offline
                var log = broker0.logManager().getLog(foo0, false).get();
                log.renameDir(UnifiedLog.logFutureDirName(foo0), false);

                // Start up broker0 and wait until the ISR of foo-0 is set to [0, 1, 2]
                broker0.startup();
                TestUtils.retryOnExceptionWithTimeout(60000, () -> {
                    var info = broker1.metadataCache().getLeaderAndIsr("foo", 0);
                    assertTrue(info.isPresent());
                    assertEquals(Set.of(0, 1, 2), new HashSet<>(info.get().isr()));
                    assertTrue(broker0.logManager().getLog(foo0, true).isEmpty());
                });
            }
        }
    }

    @Test
    public void testAbandonedFutureReplicaRecovered_mainReplicaInOnlineLogDir() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setBootstrapMetadataVersion(MetadataVersion.IBP_3_7_IV2)
                .setNumBrokerNodes(3)
                .setNumDisksPerBroker(2)
                .setNumControllerNodes(1)
                .build())
            .build()) {
            cluster.format();
            cluster.startup();
            try (Admin admin = cluster.admin()) {
                var broker0 = cluster.brokers().get(0);
                var broker1 = cluster.brokers().get(1);
                var foo0 = new TopicPartition("foo", 0);

                admin.createTopics(List.of(
                    new NewTopic("foo", 3, (short) 3))).all().get();

                // Wait until foo-0 is created on broker0.
                TestUtils.retryOnExceptionWithTimeout(60000, () ->
                    assertTrue(broker0.logManager().getLog(foo0, false).isDefined()));

                // Shut down broker0 and wait until the ISR of foo-0 is set to [1, 2]
                broker0.shutdown();
                TestUtils.retryOnExceptionWithTimeout(60000, () -> {
                    var info = broker1.metadataCache().getLeaderAndIsr("foo", 0);
                    assertTrue(info.isPresent());
                    assertEquals(Set.of(1, 2), new HashSet<>(info.get().isr()));
                });

                var log = broker0.logManager().getLog(foo0, false).get();

                // Copy foo-0 to targetParentDir
                // This is so that we can rename the main replica to a future down below
                var parentDir = log.parentDir();
                var targetParentDir = broker0.config().logDirs().stream()
                    .filter(l -> !l.equals(parentDir))
                    .findFirst()
                    .orElseThrow();
                var targetDirFile = new File(targetParentDir, log.dir().getName());
                targetDirFile.mkdir();
                try (Stream<Path> stream = Files.walk(Paths.get(log.dir().toString()))) {
                    stream.forEach(p -> {
                        var out = Paths.get(targetDirFile.toString(),
                            p.toString().substring(log.dir().toString().length()));
                        if (!p.toString().equals(log.dir().toString())) {
                            assertDoesNotThrow(() -> Files.copy(p, out));
                        }
                    });
                }
                assertTrue(targetDirFile.exists());

                // Rename original log to a future
                // This is equivalent to a failure during the promotion of the future replica and a restart with directory for
                // the main replica being online
                var originalLogFile = log.dir();
                log.renameDir(UnifiedLog.logFutureDirName(foo0), false);
                assertFalse(originalLogFile.exists());

                // Start up broker0 and wait until the ISR of foo-0 is set to [0, 1, 2]
                broker0.startup();
                TestUtils.retryOnExceptionWithTimeout(60000, () -> {
                    var info = broker1.metadataCache().getLeaderAndIsr("foo", 0);
                    assertTrue(info.isPresent());
                    assertEquals(Set.of(0, 1, 2), new HashSet<>(info.get().isr()));
                    assertTrue(broker0.logManager().getLog(foo0, true).isEmpty());
                    assertFalse(targetDirFile.exists());
                    assertTrue(originalLogFile.exists());
                });
            }
        }
    }

    @Test
    public void testControllerFailover() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(3).build()).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            TestUtils.waitForCondition(() -> cluster.brokers().get(0).brokerState() == BrokerState.RUNNING,
                "Broker never made it to RUNNING state.");
            TestUtils.waitForCondition(() -> cluster.raftManagers().get(0).client().leaderAndEpoch().leaderId().isPresent(),
                "RaftManager was not initialized.");

            try (Admin admin = cluster.admin()) {
                // Create a test topic
                admin.createTopics(List.of(
                    new NewTopic("test-topic", 1, (short) 1))).all().get();
                waitForTopicListing(admin, List.of("test-topic"), List.of());

                // Shut down active controller
                var active = cluster.waitForActiveController();
                cluster.raftManagers().get(((QuorumController) active).nodeId()).shutdown();

                // Create a test topic on the new active controller
                admin.createTopics(List.of(
                    new NewTopic("test-topic2", 1, (short) 1))).all().get();
                waitForTopicListing(admin, List.of("test-topic2"), List.of());
            }
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
    public void testIncreaseNumIoThreads() throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1).build())
            .setConfigProp(ServerConfigs.NUM_IO_THREADS_CONFIG, "4")
            .build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            try (Admin admin = cluster.admin()) {
                admin.incrementalAlterConfigs(
                    Map.of(new ConfigResource(Type.BROKER, ""),
                        List.of(new AlterConfigOp(
                            new ConfigEntry(ServerConfigs.NUM_IO_THREADS_CONFIG, "8"), OpType.SET)))).all().get();
                var newTopic = List.of(new NewTopic("test-topic", 1, (short) 1));
                var createTopicResult = admin.createTopics(newTopic);
                createTopicResult.all().get();
                waitForTopicListing(admin, List.of("test-topic"), List.of());
            }
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

    public static class DummyClientQuotaCallback implements ClientQuotaCallback, Reconfigurable {
        // Default constructor needed for reflection object creation
        public DummyClientQuotaCallback() {
        }

        public static final String DUMMY_CLIENT_QUOTA_CALLBACK_VALUE_CONFIG_KEY = "dummy.client.quota.callback.value";

        private int value = 0;

        @Override
        public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
            return Map.of();
        }

        @Override
        public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
            return 1.0;
        }

        @Override
        public void updateQuota(ClientQuotaType quotaType, org.apache.kafka.server.quota.ClientQuotaEntity quotaEntity, double newValue) {
        }

        @Override
        public void removeQuota(ClientQuotaType quotaType, org.apache.kafka.server.quota.ClientQuotaEntity quotaEntity) {
        }

        @Override
        public boolean quotaResetRequired(ClientQuotaType quotaType) {
            return true;
        }

        @Override
        public boolean updateClusterMetadata(Cluster cluster) {
            return false;
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
            Object newValue = configs.get(DUMMY_CLIENT_QUOTA_CALLBACK_VALUE_CONFIG_KEY);
            if (newValue != null) {
                value = Integer.parseInt(newValue.toString());
            }
        }

        @Override
        public Set<String> reconfigurableConfigs() {
            return Set.of(DUMMY_CLIENT_QUOTA_CALLBACK_VALUE_CONFIG_KEY);
        }

        @Override
        public void validateReconfiguration(Map<String, ?> configs) {
        }

        @Override
        public void reconfigure(Map<String, ?> configs) {
            configure(configs);
        }
    }

    public static class FakeConfigurableAuthorizer implements Authorizer, Reconfigurable {
        // Default constructor needed for reflection object creation
        public FakeConfigurableAuthorizer() {
        }

        public static final String FOOBAR_CONFIG_KEY = "fake.configurable.authorizer.foobar.config";

        private final AtomicInteger foobar = new AtomicInteger(0);

        @Override
        public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
            return serverInfo.endpoints().stream()
                .collect(Collectors.toMap(
                    endpoint -> endpoint,
                    endpoint -> {
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        future.complete(null);
                        return future;
                    }
                ));
        }

        @Override
        public Set<String> reconfigurableConfigs() {
            return Set.of(FOOBAR_CONFIG_KEY);
        }

        @Override
        public void validateReconfiguration(Map<String, ?> configs) {
            fakeConfigurableAuthorizerConfigToInt(configs);
        }

        @Override
        public void reconfigure(Map<String, ?> configs) {
            foobar.set(fakeConfigurableAuthorizerConfigToInt(configs));
        }

        @Override
        public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext,
            List<Action> actions
        ) {
            return actions.stream()
                .map(action -> AuthorizationResult.ALLOWED)
                .collect(Collectors.toList());
        }

        @Override
        public Iterable<AclBinding> acls(AclBindingFilter filter) {
            return List.of();
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
            foobar.set(fakeConfigurableAuthorizerConfigToInt(configs));
        }

        @Override
        public List<? extends CompletionStage<AclCreateResult>> createAcls(
            AuthorizableRequestContext requestContext,
            List<AclBinding> aclBindings
        ) {
            return List.of();
        }

        @Override
        public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(
            AuthorizableRequestContext requestContext,
            List<AclBindingFilter> aclBindingFilters
        ) {
            return List.of();
        }

        private int fakeConfigurableAuthorizerConfigToInt(Map<String, ?> configs) {
            Object value = configs.get(FOOBAR_CONFIG_KEY);
            if (value == null) {
                return 0;
            } else {
                return Integer.parseInt(value.toString());
            }
        }
    }
}
