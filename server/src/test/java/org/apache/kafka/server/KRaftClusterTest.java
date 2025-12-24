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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.server.IntegrationTestUtils.connectAndReceive;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(120)
@Tag("integration")
public class KRaftClusterTest {

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
                .setNumBrokerNodes(4)
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
